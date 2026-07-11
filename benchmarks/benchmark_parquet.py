"""Measure repeated Polars scans of one Parquet result with content validation."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True)
class Sample:
    run: int
    seconds: float
    rows: int
    columns: int
    result_bytes: int
    rows_per_second: float
    mib_per_second: float
    fingerprint: int
    schema: str


def evict_page_cache(path: Path) -> None:
    """Ask the kernel to discard cached pages for this file before a sample."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(descriptor)


def collect_sample(path: Path, run: int, *, evict_cache: bool = False) -> Sample:
    if evict_cache:
        evict_page_cache(path)
    started = time.perf_counter()
    frame = pl.scan_parquet(path).collect()
    elapsed = time.perf_counter() - started
    result_bytes = int(frame.estimated_size())
    return Sample(
        run=run,
        seconds=elapsed,
        rows=frame.height,
        columns=frame.width,
        result_bytes=result_bytes,
        rows_per_second=frame.height / elapsed,
        mib_per_second=(result_bytes / 1024**2) / elapsed,
        fingerprint=int(frame.hash_rows(seed=0).sum() or 0),
        schema=str(frame.schema),
    )


def validate(samples: list[Sample]) -> None:
    expected = samples[0]
    fields = ("rows", "columns", "result_bytes", "fingerprint", "schema")
    for sample in samples[1:]:
        mismatches = [field for field in fields if getattr(sample, field) != getattr(expected, field)]
        if mismatches:
            raise RuntimeError(f"Parquet result changed between runs: {', '.join(mismatches)}")


def summarize(samples: list[Sample]) -> dict[str, Any]:
    seconds = [sample.seconds for sample in samples]
    return {
        "runs": len(samples),
        "median_seconds": statistics.median(seconds),
        "mean_seconds": statistics.fmean(seconds),
        "min_seconds": min(seconds),
        "median_rows_per_second": statistics.median(sample.rows_per_second for sample in samples),
        "median_mib_per_second": statistics.median(sample.mib_per_second for sample in samples),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--evict-page-cache", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.warmups < 0 or args.runs <= 0:
        raise SystemExit("runs must be positive; warmups cannot be negative")

    for _ in range(args.warmups):
        collect_sample(args.path, -1, evict_cache=args.evict_page_cache)
    samples = [
        collect_sample(args.path, run, evict_cache=args.evict_page_cache)
        for run in range(args.runs)
    ]
    validate(samples)
    report = {
        "path": str(args.path),
        "file_bytes": args.path.stat().st_size,
        "evict_page_cache": args.evict_page_cache,
        "warmups": args.warmups,
        "samples": [asdict(sample) for sample in samples],
        "summary": summarize(samples),
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
