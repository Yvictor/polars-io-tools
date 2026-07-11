# ODBC versus ConnectorX Docker benchmarks

Run date: 2026-07-10

## Environment

- Docker host: 64 logical CPUs and 251.6 GiB memory
- Python 3.13, Polars 1.42.1, PyArrow 25.0.0
- ConnectorX 0.4.5 and arrow-odbc 10.4.2
- Batch size 65,536 rows, one warm-up, and five measured runs
- Database and client containers used the same Docker bridge network
- No CPU or memory limits were applied; the host was shared with other containers

The PostgreSQL, MySQL, and SQL Server datasets each contain 8 million rows. All
datasets have seven columns covering integer,
timestamp, double, short/long text, boolean, and nullable integer values.

Each run materialized the complete result. ODBC was given ConnectorX's Arrow schema so
both paths produced the same Polars schema and complete row fingerprint. Database-
specific mapping exceptions are documented below.

## Results

### PostgreSQL 17.10

Dataset: 8,000,000 rows, 1,250 MiB table, 817.5 MiB Polars result. ODBC driver:
psqlODBC 17.00.0004.

| Transport                   | Median time | Median rows/s | Median MiB/s | Relative to ODBC |
| --------------------------- | ----------: | ------------: | -----------: | ---------------: |
| ODBC, one connection        |    15.545 s |       514,636 |         52.6 |            1.00x |
| ConnectorX, one connection  |     7.459 s |     1,072,470 |        109.6 |        **2.08x** |
| ConnectorX, four partitions |     2.045 s |     3,912,536 |        399.8 |        **7.54x** |

Single-connection ConnectorX reduced elapsed time by 52.0%. Four partitions were 3.65x
faster than single-connection ConnectorX.

### Parquet from the PostgreSQL result

Both files contain the exact same 8,000,000 rows and fingerprint as the database
benchmarks. Warm measurements used one warm-up and five measured runs. Cold-ish
measurements used five runs with `POSIX_FADV_DONTNEED` before each scan; this is a
per-file kernel cache hint rather than a machine-wide cache reset.

| Encoding     | File size | Warm median | Cold-ish median | Write time |
| ------------ | --------: | ----------: | --------------: | ---------: |
| Zstd         | 324.8 MiB |     0.064 s |         0.116 s |    0.507 s |
| Uncompressed | 766.8 MiB |     0.058 s |         0.174 s |    0.809 s |

Zstd was smaller and faster in the cold-ish test because it read 57.7% fewer bytes.
With the file already in the OS cache, uncompressed Parquet was marginally faster.
Creating the Zstd file from PostgreSQL took about 2.80 seconds end-to-end: 2.29 seconds
to collect with four ConnectorX partitions plus 0.51 seconds to encode and write.

### MySQL 8.4.10

Dataset: 8,000,000 rows, 2,772 MiB InnoDB table, 877.6 MiB Polars result. ODBC driver:
MariaDB Connector/ODBC 3.1.15, because Debian does not package Oracle MySQL
Connector/ODBC.

| Transport                   | Median time | Median rows/s | Median MiB/s | Relative to ODBC |
| --------------------------- | ----------: | ------------: | -----------: | ---------------: |
| ODBC, one connection        |    17.562 s |       455,518 |         50.0 |            1.00x |
| ConnectorX, one connection  |    14.468 s |       552,957 |         60.7 |        **1.21x** |
| ConnectorX, four partitions |     5.812 s |     1,376,555 |        151.0 |        **3.02x** |

Single-connection ConnectorX reduced elapsed time by 17.6%. Four partitions were 2.49x
faster than single-connection ConnectorX.

### SQL Server 2022

Dataset: 8,000,000 rows, 963 MiB table, 817.5 MiB Polars result. ODBC driver: Microsoft
ODBC Driver 18.6.2.1 for SQL Server.

| Transport                       | Median time | Median rows/s | Median MiB/s | Relative to ODBC |
| ------------------------------- | ----------: | ------------: | -----------: | ---------------: |
| ODBC, one connection            |     7.232 s |     1,106,121 |        113.0 |            1.00x |
| ConnectorX, one connection      |    33.223 s |       240,799 |         24.6 |        **0.22x** |
| Rust native, one connection     |     6.804 s |     1,175,816 |        120.2 |        **1.06x** |
| ODBC reference in partition run |     6.953 s |     1,150,643 |        117.6 |            1.00x |
| ConnectorX, four partitions     |     8.622 s |       927,851 |         94.8 |        **0.81x** |
| Rust native, four partitions    |     4.818 s |     1,660,446 |        169.7 |        **1.50x** |
| Rust native, eight partitions   |     2.528 s |     3,164,723 |        323.4 |        **2.86x** |

Microsoft ODBC was 4.59x faster than single-connection ConnectorX. The optimized
single-connection Rust-native backend was 6.3% faster than a contemporaneous five-run
ODBC control (6.804 versus 7.232 seconds median), and 7.0% faster than the original
7.283-second ODBC baseline. All native samples were 6.622--6.866 seconds and produced
the same 8,000,000-row fingerprint. Four partitions made ConnectorX 3.85x faster than
its single-connection path, but it remained 24.0% slower than the ODBC reference from
the same run.

The Rust-native backend requests 32 KiB TDS packets, bulk-reads variable-length
payloads instead of awaiting one byte at a time, reuses ASCII wire buffers without a
second string allocation, and converts `datetime2` directly to Arrow nanoseconds. Its
direct row sink appends decoded cells to 65,536-row Arrow builders without allocating
an intermediate `TokenRow`, cloning column metadata per row, or calling the generic
`Row::get` path. Completed batches cross a bounded channel to Polars. Four and eight
partitions still provide additional throughput, but the direct sink now also exceeds
Microsoft ODBC with exactly one SQL Server connection.

## Type mapping findings

- PostgreSQL psqlODBC inferred `double precision` as `Float32` and boolean as text.
  This caused precision loss until the ConnectorX Arrow schema was supplied.
- MySQL through MariaDB ODBC inferred boolean as `Int8` and timestamp as microseconds.
  When forced directly to Arrow Boolean, the ODBC driver silently returned all-null
  values. The throughput query therefore explicitly casts boolean to `SIGNED`, making
  both transports return `Int64`. This driver behavior is recorded separately from the
  timing result.
- SQL Server ODBC and ConnectorX values matched by default; only timestamp resolution
  differed (`us` versus `ns`). Schema alignment produced identical fingerprints.

## Why performance differs by database

Calling a connector "native" does not imply a shared transfer implementation.
ConnectorX 0.4.5 uses a separate Rust source for every database, while arrow-odbc uses
ODBC rowset binding into column-oriented transit buffers. The ODBC benchmark used
65,536-row batches and arrow-odbc's default concurrent prefetch. ConnectorX's three
sources all expose row-major data and internally buffer only 32 decoded rows at a time.

### PostgreSQL

ConnectorX's default protocol wraps the query in `COPY (...) TO STDOUT WITH BINARY`.
However, a targeted three-run control using ConnectorX's cursor protocol measured
7.403 seconds, essentially the same as the 7.459-second binary result. Therefore binary
COPY alone does not explain the 2.08x advantage in this workload. The evidence points
instead to the typed Rust/PostgreSQL decode path avoiding overhead in the psqlODBC
stack. Four partitions then add real query and decode parallelism, producing most of
the 7.54x result.

### MySQL

ConnectorX executes a prepared statement for its binary protocol, iterates MySQL `Row`
objects, and takes each cell into Arrow builders. It has no PostgreSQL-like bulk COPY
path. An immediate three-run control measured 15.037 seconds for binary protocol and
18.759 seconds for text protocol, so binary encoding helped by about 20%, but did not
remove the dominant per-row/per-cell materialization and string allocation work.
Partitioning is therefore the larger optimization. The ODBC comparison is also
driver-specific: it used MariaDB Connector/ODBC 3.1.15 against MySQL 8.4, not Oracle's
MySQL Connector/ODBC.

### SQL Server

ConnectorX has no alternative MSSQL bulk protocol. It always uses Tiberius
`QueryStream`, calls `block_on(next())` for each stream item, stores rows in a 32-row
buffer, then reads every field again into Arrow builders. In contrast, arrow-odbc binds
65,536-row column-oriented arrays that Microsoft ODBC fills in bulk. Disabling
arrow-odbc concurrent prefetch increased the ODBC median from the formal 7.283 seconds
to 8.094 seconds in a focused two-run control, while ConnectorX in that control was
33.784 seconds. Prefetch accounts for only about 11% of ODBC's result; bulk column
binding and the mature Microsoft driver explain the larger structural advantage.

## Raw measurements

- PostgreSQL 8M: [`postgres-8m-single.json`](postgres-8m-single.json),
  [`postgres-8m-partitioned.json`](postgres-8m-partitioned.json)
- PostgreSQL prior 3M run: [`postgres-single.json`](postgres-single.json),
  [`postgres-partitioned.json`](postgres-partitioned.json)
- Parquet warm: [`postgres-8m-parquet-zstd.json`](postgres-8m-parquet-zstd.json),
  [`postgres-8m-parquet-uncompressed.json`](postgres-8m-parquet-uncompressed.json)
- Parquet cold-ish:
  [`postgres-8m-parquet-zstd-cold.json`](postgres-8m-parquet-zstd-cold.json),
  [`postgres-8m-parquet-uncompressed-cold.json`](postgres-8m-parquet-uncompressed-cold.json)
- Protocol controls: [`postgres-8m-cursor-research.json`](postgres-8m-cursor-research.json),
  [`mysql-8m-text-research.json`](mysql-8m-text-research.json),
  [`mysql-8m-binary-research.json`](mysql-8m-binary-research.json), and
  [`mssql-8m-no-concurrent-odbc-research.json`](mssql-8m-no-concurrent-odbc-research.json)
- MySQL: [`mysql-single.json`](mysql-single.json),
  [`mysql-partitioned.json`](mysql-partitioned.json)
- SQL Server: [`mssql-single.json`](mssql-single.json),
  [`mssql-odbc-current-8m-single.json`](mssql-odbc-current-8m-single.json), and
  [`mssql-partitioned.json`](mssql-partitioned.json)
- Rust-native SQL Server:
  [`mssql-native-direct-8m-single.json`](mssql-native-direct-8m-single.json),
  [`mssql-native-8m-single.json`](mssql-native-8m-single.json),
  [`mssql-native-8m-4part.json`](mssql-native-8m-4part.json), and
  [`mssql-native-8m.json`](mssql-native-8m.json)

## Recommendation

- Prefer ConnectorX for PostgreSQL.
- Prefer ConnectorX for large MySQL reads when native MySQL binary protocol is
  available; partitioning materially improves throughput. Keep an ODBC fallback and
  test boolean/decimal mappings with the actual production ODBC driver.
- Prefer the Rust-native backend for supported SQL Server analytical reads. Its direct
  sink exceeds Microsoft ODBC with one connection, and numeric partitioning can add
  throughput when higher database concurrency is acceptable. Keep Microsoft ODBC as
  the compatibility fallback for unsupported types and driver-specific behavior; use
  one native connection when query ordering must be preserved.

The results still do not support replacing ODBC globally. Backend selection should be
database- and workload-specific. Replacing ConnectorX's MSSQL source architecture,
rather than copying it unchanged, is what produced the SQL Server improvement.
