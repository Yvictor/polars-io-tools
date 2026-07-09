# Pushdown and Query Patterns

## Core Model

Polars custom lazy sources receive optimizer hints: requested columns, a predicate
expression, row limit, and batch size. `polars-io-tools` translates those hints into
backend-specific work, such as SQL `WHERE` clauses, Datadog time ranges, Delta partition
pruning, or reduced column reads.

If translation is not sound, prefer returning a weaker pushdown and re-applying the
original predicate later. The optimization should change where filtering happens, not the
answer.

## Predicate Visitors

The implementation uses expression visitors to answer narrowly scoped questions:

- Normalize filters into disjunctive normal form.
- Extract ranges from date/datetime columns.
- Extract allowed or excluded values for `IN`-style filters.
- Restrict predicates to a subset of columns.
- Translate filters into backend query syntax.

When modifying a visitor, test unsupported or partially supported expressions. The
correct fallback is usually no pushdown or weaker pushdown, not a guessed translation.

## Source Readers

- SQL and ClickHouse readers should fold supported predicates into SQL and narrow the
  selected columns.
- Datadog reads require a bounded `timestamp` predicate because the API request needs a
  concrete time range.
- Delta reads should preserve partition pruning and logical type restoration written by
  `sink_delta`.
- Narwhals lazy bridges should keep filters lazy where the wrapped backend supports it.

## Pushdown-Preserving Operations

`filtered_join` and `filtered_join_asof` intentionally materialize the left side, derive
join keys or temporal ranges, push those constraints into the right side, then run a
normal join. This is worthwhile when the left side is smaller than the remote right side.

`multi_source` maps an output predicate into per-source predicates with `FilterSpec`.
Use `source_col` for renamed fields, `lookback`/`lookahead` for temporal widening, and
`value_mapping` when output values differ from source values.

`concat_named` adds identifier columns from dictionary keys and uses filters on those
columns to skip irrelevant branches before materialization.

`ts_with_columns` widens time filters by lookback/lookahead, computes cumulative,
rolling, or forward-fill expressions over the widened set, then trims back to the
original predicate.

## Caching

`cache` stores columns independently and reassembles them by position. Preserve the
`order_by` requirement because source order instability can misalign columns. With
`partition_cols`, filters on partition columns should restrict the cache scope.

`cache_parquet` stores date partitions and should fetch/write only missing partitions in
query scope unless `CacheMode.REBUILD` or `CacheMode.IGNORE` says otherwise.

## Testing Strategy

Prefer tests that prove both:

1. Results match the plain or expected Polars behavior.
2. The upstream source receives a narrowed predicate, projection, branch set, or
   partition set.

Use `lf.piot.debug()` manually while exploring optimizer behavior. Use
`disable_optimizations()` for tests that compare optimized helpers against their plain
equivalents.
