use std::env;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures_util::TryStreamExt;
use tiberius::{AuthMethod, Client, ColumnData, Config};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[derive(Clone)]
struct Settings {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    partitions: usize,
    rows: i64,
    batch_size: usize,
    decode_only: bool,
    binary_strings: bool,
}

impl Settings {
    fn from_env() -> Result<Self, String> {
        fn value(name: &str, default: &str) -> String {
            env::var(name).unwrap_or_else(|_| default.to_owned())
        }

        Ok(Self {
            host: value("PIOT_MSSQL_HOST", "mssql"),
            port: value("PIOT_MSSQL_PORT", "1433")
                .parse()
                .map_err(|error| format!("invalid PIOT_MSSQL_PORT: {error}"))?,
            database: value("PIOT_MSSQL_DATABASE", "benchmark"),
            user: value("PIOT_MSSQL_USER", "sa"),
            password: value("PIOT_MSSQL_PASSWORD", "BenchMark2026!"),
            partitions: value("PIOT_MSSQL_PARTITIONS", "4")
                .parse()
                .map_err(|error| format!("invalid PIOT_MSSQL_PARTITIONS: {error}"))?,
            rows: value("PIOT_MSSQL_ROWS", "8000000")
                .parse()
                .map_err(|error| format!("invalid PIOT_MSSQL_ROWS: {error}"))?,
            batch_size: value("PIOT_MSSQL_BATCH_SIZE", "65536")
                .parse()
                .map_err(|error| format!("invalid PIOT_MSSQL_BATCH_SIZE: {error}"))?,
            decode_only: value("PIOT_MSSQL_DECODE_ONLY", "false") == "true",
            binary_strings: value("PIOT_MSSQL_BINARY_STRINGS", "false") == "true",
        })
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("nullable_value", DataType::Int64, true),
    ]))
}

struct Builders {
    id: Int64Builder,
    created_at: TimestampNanosecondBuilder,
    amount: Float64Builder,
    category: StringBuilder,
    payload: StringBuilder,
    active: BooleanBuilder,
    nullable_value: Int64Builder,
    rows: usize,
    capacity: usize,
}

impl Builders {
    fn new(capacity: usize) -> Self {
        Self {
            id: Int64Builder::with_capacity(capacity),
            created_at: TimestampNanosecondBuilder::with_capacity(capacity),
            amount: Float64Builder::with_capacity(capacity),
            category: StringBuilder::with_capacity(capacity, capacity * 12),
            payload: StringBuilder::with_capacity(capacity, capacity * 64),
            active: BooleanBuilder::with_capacity(capacity),
            nullable_value: Int64Builder::with_capacity(capacity),
            rows: 0,
            capacity,
        }
    }

    fn append(&mut self, row: &tiberius::Row) -> Result<(), String> {
        let id = row
            .get::<i64, _>(0)
            .ok_or_else(|| "id was null or had the wrong type".to_owned())?;
        let created_at = match row.values()[1] {
            ColumnData::DateTime2(Some(value)) => {
                const UNIX_EPOCH_DAYS: i64 = 719_162;
                const DAY_NS: i64 = 86_400_000_000_000;
                (i64::from(value.date().days()) - UNIX_EPOCH_DAYS) * DAY_NS
                    + i64::try_from(value.time().increments()).unwrap()
                        * 10_i64.pow(9 - u32::from(value.time().scale()))
            }
            _ => return Err("created_at was null or had the wrong type".to_owned()),
        };
        let amount = row
            .get::<f64, _>(2)
            .ok_or_else(|| "amount was null or had the wrong type".to_owned())?;
        let category = row
            .get::<&str, _>(3)
            .ok_or_else(|| "category was null or had the wrong type".to_owned())?;
        let payload = row
            .get::<&str, _>(4)
            .ok_or_else(|| "payload was null or had the wrong type".to_owned())?;
        let active = row
            .get::<bool, _>(5)
            .ok_or_else(|| "active was null or had the wrong type".to_owned())?;

        self.id.append_value(id);
        self.created_at.append_value(created_at);
        self.amount.append_value(amount);
        self.category.append_value(category);
        self.payload.append_value(payload);
        self.active.append_value(active);
        self.nullable_value.append_option(row.get::<i64, _>(6));
        self.rows += 1;
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.rows == self.capacity
    }

    fn finish(&mut self, schema: Arc<Schema>) -> Result<RecordBatch, String> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.created_at.finish()),
            Arc::new(self.amount.finish()),
            Arc::new(self.category.finish()),
            Arc::new(self.payload.finish()),
            Arc::new(self.active.finish()),
            Arc::new(self.nullable_value.finish()),
        ];
        self.rows = 0;
        RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
    }
}

async fn connect(
    settings: &Settings,
) -> Result<Client<tokio_util::compat::Compat<TcpStream>>, String> {
    let mut config = Config::new();
    config.host(&settings.host);
    config.port(settings.port);
    config.database(&settings.database);
    config.authentication(AuthMethod::sql_server(&settings.user, &settings.password));
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|error| error.to_string())?;
    tcp.set_nodelay(true).map_err(|error| error.to_string())?;
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|error| error.to_string())
}

async fn read_partition(
    settings: Settings,
    start_id: i64,
    end_id: i64,
) -> Result<(Vec<RecordBatch>, usize), String> {
    let mut client = connect(&settings).await?;
    let projection = if settings.binary_strings {
        "id, created_at, amount, CONVERT(VARBINARY(20), category), \
         CONVERT(VARBINARY(64), payload), active, nullable_value"
    } else {
        "id, created_at, amount, category, payload, active, nullable_value"
    };
    let sql = if settings.partitions == 1 {
        format!("SELECT {projection} FROM dbo.benchmark_events")
    } else {
        format!(
            "SELECT {projection} \
             FROM dbo.benchmark_events WHERE id >= {start_id} AND id < {end_id}"
        )
    };
    let mut rows = client
        .query(sql, &[])
        .await
        .map_err(|error| error.to_string())?
        .into_row_stream();
    let output_schema = schema();
    let mut builders = Builders::new(settings.batch_size);
    let mut batches = Vec::new();
    let mut row_count = 0;

    while let Some(row) = rows.try_next().await.map_err(|error| error.to_string())? {
        row_count += 1;
        if settings.decode_only {
            std::hint::black_box(row);
            continue;
        }
        builders.append(&row)?;
        if builders.is_full() {
            batches.push(builders.finish(output_schema.clone())?);
        }
    }
    if builders.rows > 0 {
        batches.push(builders.finish(output_schema)?);
    }
    Ok((batches, row_count))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), String> {
    let settings = Settings::from_env()?;
    if settings.partitions == 0 || settings.rows <= 0 || settings.batch_size == 0 {
        return Err("partitions, rows, and batch size must be positive".to_owned());
    }

    let started = Instant::now();
    let width = (settings.rows + settings.partitions as i64 - 1) / settings.partitions as i64;
    let mut tasks = JoinSet::new();
    for partition in 0..settings.partitions {
        let start_id = 1 + partition as i64 * width;
        let end_id = (start_id + width).min(settings.rows + 1);
        if start_id < end_id {
            tasks.spawn(read_partition(settings.clone(), start_id, end_id));
        }
    }

    let mut all_batches = Vec::new();
    let mut rows = 0;
    while let Some(result) = tasks.join_next().await {
        let (batches, partition_rows) = result.map_err(|error| error.to_string())??;
        all_batches.extend(batches);
        rows += partition_rows;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let bytes: usize = all_batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum();
    let id_sum: i128 = all_batches
        .iter()
        .map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("id must remain Int64")
                .values()
                .iter()
                .map(|value| i128::from(*value))
                .sum::<i128>()
        })
        .sum();

    println!(
        "{{\"seconds\":{elapsed:.9},\"rows\":{rows},\"bytes\":{bytes},\"rows_per_second\":{:.3},\"mib_per_second\":{:.3},\"batches\":{},\"partitions\":{},\"id_sum\":{id_sum}}}",
        rows as f64 / elapsed,
        bytes as f64 / 1024.0 / 1024.0 / elapsed,
        all_batches.len(),
        settings.partitions,
    );
    Ok(())
}
