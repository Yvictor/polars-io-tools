use std::sync::{mpsc as std_mpsc, Arc};
use std::thread;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use chrono::NaiveDateTime;
use tiberius::{AuthMethod, Client, Column, ColumnData, ColumnType, Config, FromSqlOwned, RowSink};
use tokio::net::TcpStream;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::JoinSet;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

#[derive(Clone, Debug)]
pub struct MssqlReadOptions {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub trust_server_certificate: bool,
    pub query: String,
    pub partition_on: Option<String>,
    pub partition_min: Option<i64>,
    pub partition_max: Option<i64>,
    pub partition_num: usize,
    pub batch_size: usize,
    pub channel_capacity: usize,
}

#[derive(Clone, Debug)]
enum SupportedType {
    UInt8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Utf8,
    Binary,
    TimestampNanosecond,
}

#[derive(Clone, Debug)]
struct ColumnSpec {
    name: String,
    data_type: SupportedType,
}

impl SupportedType {
    fn from_tiberius(value: ColumnType) -> Result<Self, String> {
        match value {
            ColumnType::Int1 => Ok(Self::UInt8),
            ColumnType::Int2 => Ok(Self::Int16),
            ColumnType::Int4 => Ok(Self::Int32),
            ColumnType::Int8 => Ok(Self::Int64),
            ColumnType::Float4 => Ok(Self::Float32),
            ColumnType::Float8 => Ok(Self::Float64),
            ColumnType::Bit | ColumnType::Bitn => Ok(Self::Boolean),
            ColumnType::BigVarChar
            | ColumnType::BigChar
            | ColumnType::NVarchar
            | ColumnType::NChar
            | ColumnType::Text
            | ColumnType::NText
            | ColumnType::Xml => Ok(Self::Utf8),
            ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => Ok(Self::Binary),
            ColumnType::Datetime4
            | ColumnType::Datetime
            | ColumnType::Datetimen
            | ColumnType::Datetime2 => Ok(Self::TimestampNanosecond),
            other => Err(format!("unsupported SQL Server type: {other:?}")),
        }
    }

    fn arrow_type(&self) -> DataType {
        match self {
            Self::UInt8 => DataType::UInt8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Utf8 => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::TimestampNanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),
        }
    }
}

enum DynamicBuilder {
    UInt8(UInt8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
}

impl DynamicBuilder {
    fn new(data_type: &SupportedType, capacity: usize) -> Self {
        match data_type {
            SupportedType::UInt8 => Self::UInt8(UInt8Builder::with_capacity(capacity)),
            SupportedType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            SupportedType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            SupportedType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            SupportedType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            SupportedType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            SupportedType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            SupportedType::Utf8 => Self::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(32),
            )),
            SupportedType::Binary => Self::Binary(BinaryBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(32),
            )),
            SupportedType::TimestampNanosecond => {
                Self::TimestampNanosecond(TimestampNanosecondBuilder::with_capacity(capacity))
            }
        }
    }

    fn append_owned(&mut self, value: ColumnData<'static>, index: usize) -> Result<(), String> {
        match (self, value) {
            (Self::UInt8(builder), ColumnData::U8(value)) => builder.append_option(value),
            (Self::Int16(builder), ColumnData::I16(value)) => builder.append_option(value),
            (Self::Int32(builder), ColumnData::I32(value)) => builder.append_option(value),
            (Self::Int64(builder), ColumnData::I64(value)) => builder.append_option(value),
            (Self::Float32(builder), ColumnData::F32(value)) => builder.append_option(value),
            (Self::Float64(builder), ColumnData::F64(value)) => builder.append_option(value),
            (Self::Boolean(builder), ColumnData::Bit(value)) => builder.append_option(value),
            (Self::Utf8(builder), ColumnData::String(value)) => {
                builder.append_option(value.as_deref());
            }
            (Self::Binary(builder), ColumnData::Binary(value)) => {
                builder.append_option(value.as_deref());
            }
            (Self::TimestampNanosecond(builder), ColumnData::DateTime2(value)) => {
                const UNIX_EPOCH_DAYS: i64 = 719_162;
                const DAY_NS: i64 = 86_400_000_000_000;
                let value = value
                    .map(|value| {
                        let days = i64::from(value.date().days()) - UNIX_EPOCH_DAYS;
                        let factor = 10_i64.pow(9 - u32::from(value.time().scale()));
                        let time = i64::try_from(value.time().increments())
                            .map_err(|_| "datetime2 increments exceed i64".to_owned())?;
                        days.checked_mul(DAY_NS)
                            .and_then(|days| {
                                time.checked_mul(factor)
                                    .and_then(|time| days.checked_add(time))
                            })
                            .ok_or_else(|| "timestamp is outside Arrow nanosecond range".to_owned())
                    })
                    .transpose()?;
                builder.append_option(value);
            }
            (Self::TimestampNanosecond(builder), value) => {
                let value = NaiveDateTime::from_sql_owned(value)
                    .map_err(|error| error.to_string())?
                    .map(|value| {
                        value
                            .and_utc()
                            .timestamp_nanos_opt()
                            .ok_or_else(|| "timestamp is outside Arrow nanosecond range".to_owned())
                    })
                    .transpose()?;
                builder.append_option(value);
            }
            (_, value) => {
                return Err(format!(
                    "column {index} decoded to unexpected value {value:?}"
                ));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::UInt8(builder) => Arc::new(builder.finish()),
            Self::Int16(builder) => Arc::new(builder.finish()),
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float32(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::TimestampNanosecond(builder) => Arc::new(builder.finish()),
        }
    }
}

struct DirectArrowSink {
    builders: DynamicBuilders,
    schema: SchemaRef,
    sender: std_mpsc::SyncSender<Result<RecordBatch, ArrowError>>,
}

impl DirectArrowSink {
    fn new(
        specs: &[ColumnSpec],
        batch_size: usize,
        schema: SchemaRef,
        sender: std_mpsc::SyncSender<Result<RecordBatch, ArrowError>>,
    ) -> Self {
        Self {
            builders: DynamicBuilders::new(specs, batch_size),
            schema,
            sender,
        }
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.builders.rows == 0 {
            return Ok(());
        }
        let batch = self
            .builders
            .finish(self.schema.clone())
            .map_err(|error| error.to_string())?;
        self.sender
            .send(Ok(batch))
            .map_err(|_| "MSSQL Arrow stream consumer stopped".to_owned())
    }
}

impl RowSink for DirectArrowSink {
    fn start_row(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn append_cell(&mut self, index: usize, value: ColumnData<'static>) -> Result<(), String> {
        self.builders.columns[index].append_owned(value, index)
    }

    fn finish_row(&mut self) -> Result<(), String> {
        self.builders.rows += 1;
        if self.builders.rows == self.builders.capacity {
            self.flush()?;
        }
        Ok(())
    }
}

struct DynamicBuilders {
    columns: Vec<DynamicBuilder>,
    rows: usize,
    capacity: usize,
}

impl DynamicBuilders {
    fn new(specs: &[ColumnSpec], capacity: usize) -> Self {
        Self {
            columns: specs
                .iter()
                .map(|spec| DynamicBuilder::new(&spec.data_type, capacity))
                .collect(),
            rows: 0,
            capacity,
        }
    }

    fn finish(&mut self, schema: SchemaRef) -> Result<RecordBatch, ArrowError> {
        self.rows = 0;
        RecordBatch::try_new(
            schema,
            self.columns
                .iter_mut()
                .map(DynamicBuilder::finish)
                .collect(),
        )
    }
}

async fn connect(options: &MssqlReadOptions) -> Result<Client<Compat<TcpStream>>, String> {
    let mut config = Config::new();
    config.host(&options.host);
    config.port(options.port);
    config.database(&options.database);
    config.authentication(AuthMethod::sql_server(&options.user, &options.password));
    if options.trust_server_certificate {
        config.trust_cert();
    }
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|error| error.to_string())?;
    tcp.set_nodelay(true).map_err(|error| error.to_string())?;
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|error| error.to_string())
}

fn specs_from_columns(columns: &[Column]) -> Result<Vec<ColumnSpec>, String> {
    columns
        .iter()
        .map(|column| {
            Ok(ColumnSpec {
                name: column.name().to_owned(),
                data_type: SupportedType::from_tiberius(column.column_type())?,
            })
        })
        .collect()
}

async fn discover_schema(
    options: &MssqlReadOptions,
) -> Result<(Vec<ColumnSpec>, SchemaRef), String> {
    let mut client = connect(options).await?;
    let query = options.query.trim().trim_end_matches(';');
    let metadata_query = format!("SELECT TOP (0) * FROM ({query}) AS _piot_metadata");
    let mut stream = client
        .query(metadata_query, &[])
        .await
        .map_err(|error| error.to_string())?;
    let columns = stream
        .columns()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "query returned no result metadata".to_owned())?;
    let specs = specs_from_columns(columns)?;
    let schema = Arc::new(Schema::new(
        specs
            .iter()
            .map(|spec| Field::new(&spec.name, spec.data_type.arrow_type(), true))
            .collect::<Vec<_>>(),
    ));
    Ok((specs, schema))
}

fn partition_queries(options: &MssqlReadOptions) -> Result<Vec<String>, String> {
    let query = options.query.trim().trim_end_matches(';');
    if options.partition_num <= 1 {
        return Ok(vec![query.to_owned()]);
    }
    let column = options
        .partition_on
        .as_ref()
        .ok_or_else(|| {
            "partition_on is required when partition_num is greater than one".to_owned()
        })?
        .replace(']', "]]");
    let minimum = options.partition_min.ok_or_else(|| {
        "partition_min is required when partition_num is greater than one".to_owned()
    })?;
    let maximum = options.partition_max.ok_or_else(|| {
        "partition_max is required when partition_num is greater than one".to_owned()
    })?;
    if maximum <= minimum {
        return Err("partition_max must be greater than partition_min".to_owned());
    }
    let width =
        (maximum - minimum + options.partition_num as i64 - 1) / options.partition_num as i64;
    let mut queries = Vec::with_capacity(options.partition_num);
    for partition in 0..options.partition_num {
        let start = minimum + partition as i64 * width;
        let end = (start + width).min(maximum);
        if start < end {
            queries.push(format!(
                "SELECT * FROM ({query}) AS _piot_partition WHERE [{column}] >= {start} AND [{column}] < {end}"
            ));
        }
    }
    Ok(queries)
}

async fn read_partition(
    options: MssqlReadOptions,
    query: String,
    specs: Arc<Vec<ColumnSpec>>,
    schema: SchemaRef,
    sender: std_mpsc::SyncSender<Result<RecordBatch, ArrowError>>,
) -> Result<(), String> {
    let mut client = connect(&options).await?;
    let mut sink = DirectArrowSink::new(&specs, options.batch_size, schema, sender);
    client
        .query_into(query, &[], &mut sink)
        .await
        .map_err(|error| error.to_string())?;
    sink.flush()
}

struct MssqlBatchReader {
    schema: SchemaRef,
    receiver: std_mpsc::Receiver<Result<RecordBatch, ArrowError>>,
}

impl Iterator for MssqlBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl RecordBatchReader for MssqlBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub fn mssql_arrow_stream(options: MssqlReadOptions) -> Result<FFI_ArrowArrayStream, String> {
    if options.partition_num == 0 || options.batch_size == 0 || options.channel_capacity == 0 {
        return Err("partition_num, batch_size, and channel_capacity must be positive".to_owned());
    }
    let metadata_runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let (specs, schema) = metadata_runtime.block_on(discover_schema(&options))?;
    let queries = partition_queries(&options)?;
    let (sender, receiver) = std_mpsc::sync_channel(options.channel_capacity);
    let thread_schema = schema.clone();
    let specs = Arc::new(specs);

    thread::Builder::new()
        .name("polars-io-tools-mssql".to_owned())
        .spawn(move || {
            let runtime = match RuntimeBuilder::new_multi_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = sender.send(Err(ArrowError::ExternalError(Box::new(error))));
                    return;
                }
            };
            runtime.block_on(async move {
                let mut tasks = JoinSet::new();
                let error_sender = sender.clone();
                for query in queries {
                    tasks.spawn(read_partition(
                        options.clone(),
                        query,
                        specs.clone(),
                        thread_schema.clone(),
                        sender.clone(),
                    ));
                }
                drop(sender);
                while let Some(result) = tasks.join_next().await {
                    let error = match result {
                        Ok(Ok(())) => continue,
                        Ok(Err(error)) => error,
                        Err(error) => error.to_string(),
                    };
                    let _ = error_sender.send(Err(ArrowError::ExternalError(Box::new(
                        std::io::Error::other(format!("native MSSQL partition failed: {error}")),
                    ))));
                    tasks.abort_all();
                    break;
                }
            });
        })
        .map_err(|error| error.to_string())?;

    Ok(FFI_ArrowArrayStream::new(Box::new(MssqlBatchReader {
        schema,
        receiver,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use std::borrow::Cow;

    fn options() -> MssqlReadOptions {
        MssqlReadOptions {
            host: "localhost".to_owned(),
            port: 1433,
            database: "test".to_owned(),
            user: "reader".to_owned(),
            password: "secret".to_owned(),
            trust_server_certificate: false,
            query: "SELECT id, value FROM dbo.items;".to_owned(),
            partition_on: Some("id".to_owned()),
            partition_min: Some(1),
            partition_max: Some(11),
            partition_num: 3,
            batch_size: 1024,
            channel_capacity: 6,
        }
    }

    #[test]
    fn partitions_cover_half_open_range_without_overlap() {
        let queries = partition_queries(&options()).unwrap();
        assert_eq!(queries.len(), 3);
        assert!(queries[0].ends_with("WHERE [id] >= 1 AND [id] < 5"));
        assert!(queries[1].ends_with("WHERE [id] >= 5 AND [id] < 9"));
        assert!(queries[2].ends_with("WHERE [id] >= 9 AND [id] < 11"));
    }

    #[test]
    fn one_partition_preserves_original_query() {
        let mut options = options();
        options.partition_num = 1;
        assert_eq!(
            partition_queries(&options).unwrap(),
            vec!["SELECT id, value FROM dbo.items"]
        );
    }

    #[test]
    fn rejects_missing_or_invalid_partition_range() {
        let mut missing = options();
        missing.partition_on = None;
        assert!(partition_queries(&missing)
            .unwrap_err()
            .contains("partition_on"));

        let mut reversed = options();
        reversed.partition_min = Some(11);
        reversed.partition_max = Some(1);
        assert!(partition_queries(&reversed)
            .unwrap_err()
            .contains("must be greater"));
    }

    #[test]
    fn maps_common_tds_types_to_arrow() {
        assert_eq!(
            SupportedType::from_tiberius(ColumnType::Int8)
                .unwrap()
                .arrow_type(),
            DataType::Int64
        );
        assert_eq!(
            SupportedType::from_tiberius(ColumnType::NVarchar)
                .unwrap()
                .arrow_type(),
            DataType::Utf8
        );
        assert!(SupportedType::from_tiberius(ColumnType::Decimaln).is_err());
    }

    #[test]
    fn direct_sink_builds_arrow_without_intermediate_rows() {
        let specs = vec![
            ColumnSpec {
                name: "id".to_owned(),
                data_type: SupportedType::Int64,
            },
            ColumnSpec {
                name: "value".to_owned(),
                data_type: SupportedType::Utf8,
            },
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]));
        let (sender, receiver) = std_mpsc::sync_channel(2);
        let mut sink = DirectArrowSink::new(&specs, 2, schema, sender);

        for (id, value) in [(1, "one"), (2, "two")] {
            sink.start_row().unwrap();
            sink.append_cell(0, ColumnData::I64(Some(id))).unwrap();
            sink.append_cell(1, ColumnData::String(Some(Cow::Owned(value.to_owned()))))
                .unwrap();
            sink.finish_row().unwrap();
        }

        let batch = receiver.recv().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(1),
            2
        );
    }
}
