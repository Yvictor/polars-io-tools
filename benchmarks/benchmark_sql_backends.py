"""Compare the ODBC and ConnectorX transports against the same SQL result."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from sqlglot import parse_one

from polars_io_tools import scan_db
from polars_io_tools.io_sources.lazy_sql_reader import (
    _connectorx_arrow_reader,
    get_sqlglot_dialect_connectorx,
)


@dataclass(frozen=True)
class Sample:
    engine: str
    run: int
    seconds: float
    rows: int
    columns: int
    result_bytes: int
    rows_per_second: float
    mib_per_second: float
    fingerprint: int
    schema: str


def parse_options(value: str) -> dict[str, Any]:
    options = json.loads(value)
    if not isinstance(options, dict):
        raise argparse.ArgumentTypeError("engine options must be a JSON object")
    return options


def load_query(args: argparse.Namespace) -> str:
    if args.query is not None:
        return args.query
    return args.query_file.read_text()


def get_connectorx_arrow_schema(
    query: str,
    connection: str,
    options: dict[str, Any],
):
    dialect = get_sqlglot_dialect_connectorx(connection)
    parsed_query = parse_one(query, dialect=dialect)
    schema_query = parsed_query.limit(0, dialect=dialect).sql(dialect=dialect)
    schema_options = {key: value for key, value in options.items() if key not in {"partition_on", "partition_range", "partition_num"}}
    return _connectorx_arrow_reader(
        query=schema_query,
        connection=connection,
        batch_size=1,
        **schema_options,
    ).schema


def collect_sample(
    *,
    engine: Literal["odbc", "connectorx"],
    connection: str,
    query: str,
    fetch_size: int,
    run: int,
    options: dict[str, Any],
) -> Sample:
    started = time.perf_counter()
    frame = scan_db(
        query,
        connection,
        fetch_size=fetch_size,
        engine=engine,
        batch_size_override=fetch_size,
        **options,
    ).collect()
    elapsed = time.perf_counter() - started
    result_bytes = int(frame.estimated_size())
    fingerprint = int(frame.hash_rows(seed=0).sum() or 0)
    return Sample(
        engine=engine,
        run=run,
        seconds=elapsed,
        rows=frame.height,
        columns=frame.width,
        result_bytes=result_bytes,
        rows_per_second=frame.height / elapsed,
        mib_per_second=(result_bytes / 1024**2) / elapsed,
        fingerprint=fingerprint,
        schema=str(frame.schema),
    )


def validate_results(samples: list[Sample]) -> None:
    expected = samples[0]
    for sample in samples[1:]:
        fields = ("rows", "columns", "fingerprint", "schema")
        mismatches = [field for field in fields if getattr(sample, field) != getattr(expected, field)]
        if mismatches:
            names = ", ".join(mismatches)
            raise RuntimeError(f"Result mismatch between {expected.engine} and {sample.engine}: {names}")


def summarize(samples: list[Sample]) -> dict[str, Any]:
    by_engine: dict[str, list[Sample]] = {}
    for sample in samples:
        by_engine.setdefault(sample.engine, []).append(sample)

    summary: dict[str, Any] = {}
    for engine, engine_samples in by_engine.items():
        seconds = [sample.seconds for sample in engine_samples]
        summary[engine] = {
            "runs": len(seconds),
            "median_seconds": statistics.median(seconds),
            "mean_seconds": statistics.fmean(seconds),
            "min_seconds": min(seconds),
            "median_rows_per_second": statistics.median(sample.rows_per_second for sample in engine_samples),
            "median_mib_per_second": statistics.median(sample.mib_per_second for sample in engine_samples),
        }

    if "odbc" in summary and "connectorx" in summary:
        summary["connectorx_speedup"] = summary["odbc"]["median_seconds"] / summary["connectorx"]["median_seconds"]
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--odbc-connection",
        default=os.environ.get("PIOT_ODBC_CONNECTION"),
        help="ODBC connection string; defaults to PIOT_ODBC_CONNECTION",
    )
    parser.add_argument(
        "--connectorx-uri",
        default=os.environ.get("PIOT_CONNECTORX_URI"),
        help="ConnectorX URI; defaults to PIOT_CONNECTORX_URI",
    )
    query = parser.add_mutually_exclusive_group(required=True)
    query.add_argument("--query")
    query.add_argument("--query-file", type=Path)
    parser.add_argument("--fetch-size", type=int, default=65_536)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--engines",
        nargs="+",
        choices=("odbc", "connectorx"),
        default=("odbc", "connectorx"),
    )
    parser.add_argument("--odbc-options", type=parse_options, default={})
    parser.add_argument("--connectorx-options", type=parse_options, default={})
    parser.add_argument(
        "--no-align-odbc-schema",
        action="store_false",
        dest="align_odbc_schema",
        help="Do not force ODBC to produce ConnectorX's Arrow schema",
    )
    parser.add_argument("--output", type=Path)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if not args.odbc_connection or not args.connectorx_uri:
        raise SystemExit("Both connections are required; pass flags or set PIOT_ODBC_CONNECTION and PIOT_CONNECTORX_URI")
    if args.fetch_size <= 0 or args.warmups < 0 or args.runs <= 0:
        raise SystemExit("fetch-size and runs must be positive; warmups cannot be negative")

    query = load_query(args)
    odbc_options = dict(args.odbc_options)
    aligned_schema = None
    if args.align_odbc_schema:
        aligned_schema = get_connectorx_arrow_schema(
            query,
            args.connectorx_uri,
            args.connectorx_options,
        )
        odbc_options["schema"] = aligned_schema

    engines: dict[Literal["odbc", "connectorx"], tuple[str, dict[str, Any]]] = {
        "odbc": (args.odbc_connection, odbc_options),
        "connectorx": (args.connectorx_uri, args.connectorx_options),
    }
    engines = {engine: engines[engine] for engine in args.engines}

    for _ in range(args.warmups):
        for engine, (connection, options) in engines.items():
            collect_sample(
                engine=engine,
                connection=connection,
                query=query,
                fetch_size=args.fetch_size,
                run=-1,
                options=options,
            )

    samples: list[Sample] = []
    for run in range(args.runs):
        # Alternate which backend runs first to reduce ordering bias.
        order = list(engines)
        if run % 2:
            order.reverse()
        run_samples = []
        for engine in order:
            connection, options = engines[engine]
            run_samples.append(
                collect_sample(
                    engine=engine,
                    connection=connection,
                    query=query,
                    fetch_size=args.fetch_size,
                    run=run,
                    options=options,
                )
            )
        validate_results(run_samples)
        samples.extend(run_samples)

    report = {
        "fetch_size": args.fetch_size,
        "warmups": args.warmups,
        "aligned_odbc_schema": str(aligned_schema) if aligned_schema is not None else None,
        "samples": [asdict(sample) for sample in samples],
        "summary": summarize(samples),
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
