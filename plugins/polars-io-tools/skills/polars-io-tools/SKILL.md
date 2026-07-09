---
name: polars-io-tools
description: Work on the polars-io-tools Python/Rust project. Use when implementing, debugging, reviewing, or documenting lazy Polars I/O sources, predicate/projection pushdown, SQL/ClickHouse/Datadog/Delta readers and sinks, cache/cache_parquet, filtered joins, multi_source, concat_named, Ray execution, expression visitors, or the `piot` LazyFrame namespace.
---

# polars-io-tools

## Overview

`polars-io-tools` extends Polars lazy execution with custom I/O sources and operations
that preserve predicate and projection pushdown across remote systems and query
composition. The repo is a Python package with a small Rust extension built through
`hatch-rs`.

Answer users directly from the bundled references and repo source. Do not route users to
external documentation pages as a substitute for answering. When exact behavior matters,
confirm against the implementation and nearby tests.

## Repo Map

- `polars_io_tools/io_sources/`: lazy readers, writers, predicate visitors, cache,
  source composition, and `piot` namespace implementation.
- `polars_io_tools/testing/`: helper utilities for asserting pushdown behavior.
- `polars_io_tools/tests/`: unit tests, grouped by `io_sources/` and `pushdown/`.
- `rust/`: Rust extension code exposed to Python.
- `docs/wiki/`: user-facing docs that often contain the clearest public API behavior.

## First Steps

1. Read the relevant implementation and tests before changing behavior. Start with
   `rg` for the API name, then inspect matching tests under `polars_io_tools/tests`.
2. Preserve lazy semantics. Avoid eager `collect()` unless the feature intentionally
   materializes a small side input, such as `filtered_join`.
3. Keep transformed predicates conservative. If a predicate cannot be translated
   soundly, leave it to Polars or re-apply the original predicate after an optimized
   pushdown.
4. Add or update focused tests for pushdown behavior, not just final row equality.

## How to Use References

For most tasks, load only 1-2 files. Choose the functional reference first, then inspect
the matching source and tests.

| Task | Load File |
|------|-----------|
| Public API, method signatures, examples, namespace/top-level function alignment | [api-reference.md](references/api-reference.md) |
| Predicate visitors, source readers, pushdown semantics, joins, caches, `multi_source`, `concat_named`, time-series windows | [pushdown-patterns.md](references/pushdown-patterns.md) |
| Build, setup, tests, linting, packaging, contribution workflow | [development.md](references/development.md) |

Token-efficient lookup: search the repo for the symbol first, read the smallest matching
implementation/test section, then load the reference file that explains the behavior.

## Installation

```bash
codex plugin marketplace add /Users/ec666/yvictor/polars-io-tools
codex plugin add polars-io-tools@polars-io-tools
```

The repo-local marketplace lives at `.agents/plugins/marketplace.json` and points to
`./plugins/polars-io-tools`, matching the rshioaji plugin layout.

After editing the skill, validate it with `uv`:

```bash
uv run python /Users/ec666/.codex/skills/.system/skill-creator/scripts/quick_validate.py plugins/polars-io-tools/skills/polars-io-tools
```

## Behavioral Guardrails

- Importing `polars_io_tools` registers the `.piot` namespace on Polars `LazyFrame`.
- Most namespace methods also have top-level function forms; keep both surfaces aligned.
- Source readers should use projection, predicate, row limit, and batch-size hints when
  Polars provides them.
- Writers should avoid silently losing logical type information. Delta logical type
  metadata must round-trip through `sink_delta` and `scan_delta`.
- Cache code must preserve row alignment. `order_by` exists because independent column
  caches are unsafe when source ordering is unstable.
- Prefer `disable_optimizations()` in tests when comparing optimized behavior against
  plain-Polars equivalents.

## Public Usage Pattern

```python
import polars as pl
import polars_io_tools  # registers .piot

left = pl.LazyFrame({"x": [1, 2, 3]})
right = pl.LazyFrame({"x": [3, 4], "value": [10, 20]})

out = left.piot.filtered_join(right, on="x").collect()
```
