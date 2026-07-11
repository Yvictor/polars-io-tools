# SQL backend benchmark

`benchmark_sql_backends.py` executes the same query through ODBC and ConnectorX,
checks that each run has the same schema, row count, estimated result size, and row
fingerprint, then reports median throughput and the ConnectorX speedup ratio.
The requested fetch size is forced for both connectors so the comparison uses matching
batch sizes rather than Polars' internally selected source batch size.

By default the benchmark first reads ConnectorX's Arrow schema and passes it to
`arrow-odbc` as an explicit schema. This prevents driver metadata differences such as
`float32` versus `float64` or string versus boolean from turning the comparison into
different workloads. Pass `--no-align-odbc-schema` to inspect the drivers' default type
mapping instead; result validation will stop the run if they differ.

Install the ConnectorX extra first:

```bash
uv pip install -e '.[connectorx,develop]'
```

Keep credentials out of shell history by using environment variables:

```bash
export PIOT_ODBC_CONNECTION='Driver={PostgreSQL Unicode};Server=localhost;...'
export PIOT_CONNECTORX_URI='postgresql://user:password@localhost/database'

python3 benchmarks/benchmark_sql_backends.py \
  --query-file benchmarks/query.sql \
  --fetch-size 65536 \
  --warmups 1 \
  --runs 5 \
  --output benchmark-result.json
```

Connector-specific settings can be supplied as JSON. For example, this partitions a
PostgreSQL query across four native connections:

```bash
python3 benchmarks/benchmark_sql_backends.py \
  --query 'SELECT id, created_at, amount FROM benchmark_events' \
  --connectorx-options '{"partition_on":"id","partition_num":4}'
```

Run both an unpartitioned comparison and a partitioned comparison. Partitioning can
improve client throughput substantially, but it also changes database load and should
not be attributed solely to the native protocol.

## Reproducible PostgreSQL Docker benchmark

The Docker setup creates a PostgreSQL 17 database with eight million mixed-type rows.
Both transports run inside the same client container, so they share the same Python,
network, CPU allocation, and result validation path.

Build and initialize the database:

```bash
docker compose -f benchmarks/docker/compose.yml build benchmark
docker compose -f benchmarks/docker/compose.yml up -d postgres
```

Run the single-connection protocol comparison:

```bash
docker compose -f benchmarks/docker/compose.yml run --rm benchmark \
  --query-file benchmarks/docker/query.sql \
  --fetch-size 65536 \
  --warmups 1 \
  --runs 5 \
  --output /results/postgres-8m-single.json
```

Then measure ConnectorX with four query partitions:

```bash
docker compose -f benchmarks/docker/compose.yml run --rm benchmark \
  --query-file benchmarks/docker/query.sql \
  --fetch-size 65536 \
  --warmups 1 \
  --runs 5 \
  --connectorx-options '{"partition_on":"id","partition_num":4}' \
  --output /results/postgres-8m-partitioned.json
```

Export the same result as compressed and uncompressed Parquet, then benchmark warm or
per-file cache-evicted scans:

```bash
docker compose -f benchmarks/docker/compose.yml run --rm \
  --entrypoint python benchmark benchmarks/export_sql_parquet.py \
  --query-file benchmarks/docker/query.sql --output-prefix /results/postgres-8m

docker compose -f benchmarks/docker/compose.yml run --rm --no-deps \
  --entrypoint python benchmark benchmarks/benchmark_parquet.py \
  /results/postgres-8m-zstd.parquet --warmups 1 --runs 5

docker compose -f benchmarks/docker/compose.yml run --rm --no-deps \
  --entrypoint python benchmark benchmarks/benchmark_parquet.py \
  /results/postgres-8m-zstd.parquet --warmups 0 --runs 5 \
  --evict-page-cache
```

Remove the generated database when finished:

```bash
docker compose -f benchmarks/docker/compose.yml down --volumes
```

### MySQL and SQL Server

MySQL 8.4 and SQL Server 2022 use the same client image and create eight million rows
each. Start them and run their one-connection comparisons with:

```bash
docker compose -f benchmarks/docker/compose.yml up -d mysql mssql
docker compose -f benchmarks/docker/compose.yml up mssql-init

docker compose -f benchmarks/docker/compose.yml run --rm benchmark-mysql \
  --query-file benchmarks/docker/query-mysql.sql --fetch-size 65536 \
  --warmups 1 --runs 5 --output /results/mysql-single.json

docker compose -f benchmarks/docker/compose.yml run --rm benchmark-mssql \
  --query-file benchmarks/docker/query-mssql.sql --fetch-size 65536 \
  --warmups 1 --runs 5 --output /results/mssql-single.json
```

Add `--connectorx-options '{"partition_on":"id","partition_num":4}'` for the
partitioned comparison. The MySQL ODBC baseline uses MariaDB Connector/ODBC from
Debian; the SQL Server baseline uses Microsoft ODBC Driver 18.
