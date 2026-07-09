# API Reference Notes

Use this file as a quick public-surface map before editing functions, docs, examples,
or tests. The full source of truth remains the implementation and `docs/wiki/`.

## Namespace and Top-Level Functions

Importing `polars_io_tools` registers the `piot` namespace on every Polars
`LazyFrame`. Many operations exist both as `lf.piot.method(...)` and as a top-level
function taking the `LazyFrame` as its first argument.

## LazyFrame Namespace

- `lf.piot.cache(cache=None, *, order_by, partition_cols=(), cache_mode="cache",
  validate=True, log_explain=False, **kwargs)`: maintain a per-column cache. `order_by`
  is required and must uniquely identify rows.
- `lf.piot.cache_parquet(cache_path, date_column=None, *, time_unit="monthly",
  partition_format=None, cache_mode=CacheMode.CACHE, aws_profile=None, write_kwargs=None,
  read_kwargs=None, extra_partition_cols=None, schema=None,
  write_bounding_columns=None)`: materialize date-partitioned Parquet locally or on S3.
- `lf.piot.debug(log_level=None)`: log or print projection, predicate, row limit, and
  optimized plan passed to a source.
- `lf.piot.filtered_join(lf2, on=None, how="inner", *, left_on=None, right_on=None,
  nulls_equal=False, log_explain=False, **join_kwargs)`: materialize the left side and
  push join keys to the right side before joining.
- `lf.piot.filtered_join_asof(lf2, *, left_on=None, right_on=None, on=None, by=None,
  by_left=None, by_right=None, strategy="backward", tolerance=None,
  log_explain=True, **join_kwargs)`: asof join with right-side filter pushdown.
- `lf.piot.ts_with_columns(*exprs, index_col=None, linked_cols=None, lookback=None,
  lookahead=None, log_explain=False)`: run time-window expressions while preserving
  filter pushdown by widening then trimming time predicates.
- `lf.piot.with_columns_topo(exprs)`: add dependent expressions in topological order.
- `lf.piot.filter_no_pushdown(expressions)`: apply filters that should not be pushed
  into a source.
- `lf.piot.execute_on_ray(*, date_column, time_unit, return_as="arrow",
  remote_options=None, max_concurrency=100)`: split by calendar periods and execute on
  an existing Ray cluster.
- `lf.piot.sink_delta(target, *, mode="error", overwrite_schema=None,
  storage_options=None, credential_provider="auto", delta_write_options=None,
  delta_merge_options=None, translate_logical_types=True, chunk_size=None,
  aws_profile=None)`: write a lazy frame to Delta Lake with logical type translation.
- `lf.piot.sink_clickhouse(table, url, params, *, chunk_size=None)`: write a lazy frame
  to an existing ClickHouse table over HTTP Arrow IPC.
- `lf.piot.iter_rows(*, named=False, buffer_size=512, maintain_order=True)`: collect in
  batches and yield rows without materializing the whole frame at once.

## Readers

- `scan_db(query, connection, fetch_size=10000, **kwargs)`: ODBC SQL source with
  predicate and projection pushdown.
- `scan_clickhouse(query, url, params, fetch_size=10000)`: ClickHouse HTTP Arrow IPC
  source with SQL predicate/projection folding.
- `scan_datadog(query, api_key, app_key, max_chunk_duration_seconds=86400,
  dd_interval=None, additional_schema={}, overwrite_schema=False)`: Datadog metrics
  source. Requires a bounded `timestamp` predicate.
- `scan_delta(source, *, version=None, storage_options=None, credential_provider="auto",
  delta_table_options=None, use_pyarrow=False, pyarrow_options=None, rechunk=None,
  aws_profile=None, pushdown_predicate_deltalake=True)`: wraps `pl.scan_delta`, adds
  partition pruning and logical type recovery.
- `from_narwhals(obj, fetch_size=10_000)`: convert Narwhals eager or lazy frames into
  Polars, preserving lazy bridge behavior where possible.

## Composition Helpers

- `multi_source(sources, combine, *, combine_kwargs=None, sources_as_kwargs=False,
  log_explain=False)`: construct a lazy frame from multiple sources and transform output
  predicates into per-source filters.
- `FilterSpec(source_col=None, lookback=timedelta(), lookahead=timedelta(),
  value_mapping=None)`: maps output filters to a source column, widened time range, or
  remapped values.
- `concat_named(lf_dict, identifier_cols, *, log_explain=False, **kwargs)`: concatenate
  keyed frames and prune branches using identifier-column filters.
- `join_between(left, right, left_on, right_on_start, right_on_end, by=None,
  how="left")`: join each point to the non-overlapping right interval containing it.
- `CacheMode.CACHE`, `CacheMode.IGNORE`, `CacheMode.REBUILD`: cache behavior for
  `cache_parquet`.
- `disable_optimizations()`: context manager that swaps optimized helpers for
  plain-Polars equivalents for explainability and comparisons.
