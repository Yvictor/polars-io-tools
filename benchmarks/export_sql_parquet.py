"""Export a ConnectorX query result to comparable Parquet encodings."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from polars_io_tools import scan_db


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-file", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    args = parser.parse_args()
    uri = os.environ.get("PIOT_CONNECTORX_URI")
    if not uri:
        raise SystemExit("PIOT_CONNECTORX_URI is required")

    started = time.perf_counter()
    frame = scan_db(
        args.query_file.read_text(),
        uri,
        engine="connectorx",
        batch_size_override=65_536,
        partition_on="id",
        partition_num=4,
    ).collect()
    collect_seconds = time.perf_counter() - started
    outputs = {}
    for name, compression in (("zstd", "zstd"), ("uncompressed", "uncompressed")):
        path = args.output_prefix.with_name(f"{args.output_prefix.name}-{name}.parquet")
        write_started = time.perf_counter()
        frame.write_parquet(path, compression=compression)
        outputs[name] = {
            "path": str(path),
            "file_bytes": path.stat().st_size,
            "write_seconds": time.perf_counter() - write_started,
        }
    print(
        json.dumps(
            {
                "rows": frame.height,
                "columns": frame.width,
                "result_bytes": frame.estimated_size(),
                "fingerprint": int(frame.hash_rows(seed=0).sum() or 0),
                "schema": str(frame.schema),
                "collect_seconds": collect_seconds,
                "outputs": outputs,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
