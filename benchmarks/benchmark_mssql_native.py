"""Benchmark the Rust-native MSSQL Arrow stream through its Python/Polars API."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from dataclasses import asdict, dataclass

from polars_io_tools import scan_db


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


QUERY = """
SELECT id, created_at, amount, category, payload, active, nullable_value
FROM dbo.benchmark_events
"""


def collect_sample(run: int, partitions: int, batch_size: int) -> Sample:
    started = time.perf_counter()
    host = os.environ.get("PIOT_MSSQL_HOST", "mssql")
    port = int(os.environ.get("PIOT_MSSQL_PORT", "1433"))
    database = os.environ.get("PIOT_MSSQL_DATABASE", "benchmark")
    user = os.environ.get("PIOT_MSSQL_USER", "sa")
    password = os.environ.get("PIOT_MSSQL_PASSWORD_ENCODED", "BenchMark2026%21")
    frame = scan_db(
        QUERY,
        f"mssql://{user}:{password}@{host}:{port}/{database}?trust_server_certificate=true",
        engine="mssql_native",
        batch_size_override=batch_size,
        partition_on="id" if partitions > 1 else None,
        partition_range=(1, 8_000_001) if partitions > 1 else None,
        partition_num=partitions,
        channel_capacity=partitions * 2,
    ).collect()
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=65_536)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.partitions <= 0 or args.batch_size <= 0 or args.warmups < 0 or args.runs <= 0:
        raise SystemExit("partitions, batch size, and runs must be positive; warmups cannot be negative")

    for _ in range(args.warmups):
        collect_sample(-1, args.partitions, args.batch_size)
    samples = [collect_sample(run, args.partitions, args.batch_size) for run in range(args.runs)]
    expected = samples[0]
    for sample in samples[1:]:
        if (sample.rows, sample.columns, sample.result_bytes, sample.fingerprint, sample.schema) != (
            expected.rows,
            expected.columns,
            expected.result_bytes,
            expected.fingerprint,
            expected.schema,
        ):
            raise RuntimeError("native MSSQL result changed between runs")
    seconds = [sample.seconds for sample in samples]
    report = {
        "partitions": args.partitions,
        "batch_size": args.batch_size,
        "warmups": args.warmups,
        "samples": [asdict(sample) for sample in samples],
        "summary": {
            "runs": len(samples),
            "median_seconds": statistics.median(seconds),
            "mean_seconds": statistics.fmean(seconds),
            "min_seconds": min(seconds),
            "median_rows_per_second": statistics.median(sample.rows_per_second for sample in samples),
            "median_mib_per_second": statistics.median(sample.mib_per_second for sample in samples),
        },
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output:
            output.write(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
