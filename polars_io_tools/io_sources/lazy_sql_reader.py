import logging
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Literal, Optional, Union
from urllib.parse import parse_qs, unquote, urlsplit

import polars as pl
from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import Dialect

from .sql_dialects import MSSQL
from .sql_utils import (
    apply_polars_io_source_exprs,
    fix_three_part_identifiers,
)
from .util import register_io_source_with_is_pure

__all__ = ["scan_db"]


# Configure logging
log = logging.getLogger(__name__)

DatabaseEngine = Literal["odbc", "connectorx", "mssql_native"]


def get_sqlglot_dialect_connectorx(connection: str) -> Optional[Union[str, type[Dialect]]]:
    """Infer a SQLGlot dialect from a ConnectorX connection URI."""

    scheme = urlsplit(connection).scheme.lower().split("+", 1)[0]
    dialects: dict[str, Union[str, type[Dialect]]] = {
        "postgres": "postgres",
        "postgresql": "postgres",
        "redshift": "redshift",
        "mysql": "mysql",
        "mariadb": "mysql",
        "mssql": MSSQL,
        "sqlserver": MSSQL,
        "oracle": "oracle",
        "sqlite": "sqlite",
        "clickhouse": "clickhouse",
        "trino": "trino",
        "bigquery": "bigquery",
    }
    return dialects.get(scheme)


def _connectorx_arrow_reader(query: str, connection: str, batch_size: int, **kwargs: Any):
    try:
        import connectorx as cx
    except ImportError as e:
        raise ImportError("The ConnectorX backend requires the optional dependency; install polars-io-tools[connectorx] or connectorx>=0.4.4") from e

    reserved = {"conn", "query", "return_type", "batch_size"}.intersection(kwargs)
    if reserved:
        names = ", ".join(sorted(reserved))
        raise ValueError(f"ConnectorX options must not override managed arguments: {names}")

    stream = cx.read_sql(
        conn=connection,
        query=query,
        return_type="arrow_stream",
        batch_size=batch_size,
        **kwargs,
    )

    # ConnectorX versions may expose the Arrow C stream as a PyCapsule or as an
    # object implementing the Arrow PyCapsule protocol. Normalize both to a
    # PyArrow RecordBatchReader while also accepting reader-like test doubles.
    if hasattr(stream, "schema") and hasattr(stream, "__iter__"):
        return stream

    import pyarrow as pa

    if hasattr(stream, "__arrow_c_stream__"):
        return pa.RecordBatchReader.from_stream(stream)
    try:
        return pa.RecordBatchReader._import_from_c_capsule(stream)
    except Exception as e:
        raise TypeError("ConnectorX returned an unsupported Arrow stream object") from e


def _mssql_native_arrow_reader(query: str, connection: str, batch_size: int, **kwargs: Any):
    """Create a PyArrow reader backed by the Rust-native TDS implementation."""

    parsed = urlsplit(connection)
    if parsed.scheme.lower().split("+", 1)[0] not in {"mssql", "sqlserver"}:
        raise ValueError("The mssql_native backend requires an mssql:// or sqlserver:// URI")
    if not parsed.hostname or parsed.username is None or parsed.password is None:
        raise ValueError("The mssql_native URI must include host, username, and password")

    partition_range = kwargs.pop("partition_range", None)
    partition_min = kwargs.pop("partition_min", None)
    partition_max = kwargs.pop("partition_max", None)
    if partition_range is not None:
        if partition_min is not None or partition_max is not None:
            raise ValueError("Use partition_range or partition_min/partition_max, not both")
        if not isinstance(partition_range, (tuple, list)) or len(partition_range) != 2:
            raise ValueError("partition_range must contain exactly (minimum, exclusive_maximum)")
        partition_min, partition_max = partition_range
    partition_on = kwargs.pop("partition_on", None)
    partition_num = kwargs.pop("partition_num", 1)
    channel_capacity = kwargs.pop("channel_capacity", max(2, int(partition_num) * 2))
    if kwargs:
        names = ", ".join(sorted(kwargs))
        raise TypeError(f"Unsupported mssql_native options: {names}")

    try:
        mssql_native_arrow_stream = import_module("polars_io_tools.polars_io_tools").mssql_native_arrow_stream
    except ImportError as e:
        raise ImportError("The mssql_native backend requires a wheel built with the Rust extension") from e

    uri_options = parse_qs(parsed.query)
    trust_value = (uri_options.get("trust_server_certificate") or uri_options.get("trustServerCertificate") or ["false"])[-1]
    trust_server_certificate = trust_value.lower() in {"1", "true", "yes"}
    capsule = mssql_native_arrow_stream(
        parsed.hostname,
        parsed.port or 1433,
        parsed.path.lstrip("/") or "master",
        unquote(parsed.username),
        unquote(parsed.password),
        query,
        partition_on,
        partition_min,
        partition_max,
        int(partition_num),
        batch_size,
        int(channel_capacity),
        trust_server_certificate,
    )
    import pyarrow as pa

    return pa.RecordBatchReader._import_from_c_capsule(capsule)


@lru_cache(None)
def get_sqlglot_dialect_odbc(conn_string: str) -> Optional[Union[str, type[Dialect]]]:
    import pyodbc

    DIALECT_MAP: dict[str, Union[str, type[Dialect]]] = {
        "microsoft sql server": MSSQL,
        "postgresql": "postgres",
        "oracle": "oracle",
        "mysql": "mysql",
        "snowflake": "snowflake",
        "sqlite": "sqlite",
        "amazon redshift": "redshift",
    }
    with pyodbc.connect(conn_string) as conn:
        try:
            return DIALECT_MAP[conn.getinfo(pyodbc.SQL_DBMS_NAME).lower()]
        except Exception as e:
            log.warning(f"Got exception when trying to find dialect: {e}")
            return None


def get_schema_from_query_odbc(
    query: exp.Expression,
    connection: Union[str, Any],
    dialect: Optional[Union[str, type[Dialect]]],
    **kwargs: Any,
) -> Dict[str, pl.DataType]:
    """
    Get the schema for a SQL query using arrow-odbc.

    Args:
        query (str): The SQL query
        connection (Union[str, Any]): Database connection or connection string
        dialect (str): SQL dialect
        **kwargs: Additional arguments to pass to arrow_odbc's read_arrow_batches_from_odbc.
            These are passed through to ensure consistency between schema detection
            and data fetching (e.g., query_timeout_sec, schema overrides, etc.).

    Returns:
        Dict[str, pl.DataType]: Schema mapping column names to Polars data types
    """

    try:
        from arrow_odbc import read_arrow_batches_from_odbc

        # Create connection string if not already a string
        conn_string = connection if isinstance(connection, str) else str(connection)
        schema_query_parsed = query.limit(0, dialect=dialect)  # type: ignore[union-attr]
        identifier_parsed = schema_query_parsed.transform(fix_three_part_identifiers)
        schema_query = identifier_parsed.sql(dialect=dialect)

        # Use arrow_odbc to get schema information directly
        # The batch reader provides schema information even for empty result sets
        batch_reader = read_arrow_batches_from_odbc(
            query=schema_query,
            batch_size=1,
            connection_string=conn_string,
            **kwargs,
        )

        # We can access the PyArrow schema directly from the batch reader
        import pyarrow as pa

        arrow_schema = batch_reader.schema
        df = pl.DataFrame(pa.Table.from_pylist([], schema=arrow_schema))
        return dict(df.schema)
    except Exception as e:
        raise ValueError(f"Could not determine schema for query: {query}, with error: {e}") from e


def get_schema_from_query_connectorx(
    query: exp.Expression,
    connection: str,
    dialect: Optional[Union[str, type[Dialect]]],
    **kwargs: Any,
) -> Dict[str, pl.DataType]:
    """Get query schema through ConnectorX without materializing result rows."""

    try:
        schema_query = query.limit(0, dialect=dialect).sql(dialect=dialect)  # type: ignore[union-attr]
        # Partition settings are data-fetch optimizations and can make no sense
        # for the zero-row schema query. Other settings, such as protocol and
        # pre-execution queries, must stay consistent with the data query.
        schema_kwargs = {key: value for key, value in kwargs.items() if key not in {"partition_on", "partition_range", "partition_num"}}
        batch_reader = _connectorx_arrow_reader(
            query=schema_query,
            connection=connection,
            batch_size=1,
            **schema_kwargs,
        )

        import pyarrow as pa

        df = pl.DataFrame(pa.Table.from_pylist([], schema=batch_reader.schema))
        return dict(df.schema)
    except Exception as e:
        raise ValueError(f"Could not determine schema for query: {query}, with error: {e}") from e


def get_schema_from_query_mssql_native(
    query: exp.Expression,
    connection: str,
    dialect: Optional[Union[str, type[Dialect]]],
    **kwargs: Any,
) -> Dict[str, pl.DataType]:
    """Get query schema through the native TDS reader without result rows."""

    try:
        schema_query = query.limit(0, dialect=dialect).sql(dialect=dialect)  # type: ignore[union-attr]
        schema_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key not in {"partition_on", "partition_range", "partition_min", "partition_max", "partition_num"}
        }
        batch_reader = _mssql_native_arrow_reader(
            query=schema_query,
            connection=connection,
            batch_size=1,
            **schema_kwargs,
        )
        import pyarrow as pa

        return dict(pl.DataFrame(pa.Table.from_pylist([], schema=batch_reader.schema)).schema)
    except Exception as e:
        raise ValueError(f"Could not determine native MSSQL schema for query: {query}, with error: {e}") from e


def scan_db(
    query: str,
    connection: str,
    fetch_size: int = 10000,
    engine: DatabaseEngine = "odbc",
    batch_size_override: Optional[int] = None,
    **kwargs,
) -> pl.LazyFrame:
    """
    Create a LazyFrame from a SQL query with predicate pushdown support.

    This is the primary user-facing function in this module.

    This function creates a LazyFrame that will execute SQL queries against the provided
    connection with optimized predicate pushdown. Filters applied to the LazyFrame will
    be translated back to SQL and pushed to the database.

    Args:
        query (str): The SQL query to execute
        connection (str): A connection string (*not* a database connection object)
        fetch_size (int, default 10000): Number of rows to fetch at a time. This is a default needed by the \
            source generator function that scan_db wraps (because it is required \
            by the Polars IO plugins API). This value will only be used if Polars \
            does not pass a value for batch size; if it does, that will be used instead.
        engine (str, default "odbc"): Database transport. Use ``"connectorx"``
            with a ConnectorX URI, or ``"mssql_native"`` with an MSSQL URI for
            the optimized Rust TDS-to-Arrow implementation.
        batch_size_override (int, optional): Force the connector batch size even
            when Polars supplies one. Primarily useful for controlled benchmarks.
        **kwargs: Additional arguments for the database connector

    Returns:
        pl.LazyFrame: A Polars LazyFrame with predicate pushdown support
    """

    if batch_size_override is not None and batch_size_override <= 0:
        raise ValueError("batch_size_override must be positive")

    def _fetch_info_needing_connection() -> tuple[
        dict[str, pl.DataType],
        exp.Expression,
        Optional[Union[str, type[Dialect]]],
    ]:
        if engine == "odbc":
            dialect = get_sqlglot_dialect_odbc(conn_string=connection)
        elif engine == "connectorx":
            dialect = get_sqlglot_dialect_connectorx(connection)
            if dialect is None:
                scheme = urlsplit(connection).scheme or "<missing>"
                raise ValueError(f"Could not infer SQL dialect from ConnectorX URI scheme: {scheme!r}")
        elif engine == "mssql_native":
            dialect = MSSQL
        else:
            raise ValueError(f"Unsupported database engine: {engine!r}")

        # Parse the original query
        parsed_query = parse_one(query, dialect=dialect)
        if engine == "odbc":
            schema = get_schema_from_query_odbc(parsed_query.copy(), connection, dialect=dialect, **kwargs)
        elif engine == "connectorx":
            schema = get_schema_from_query_connectorx(parsed_query.copy(), connection, dialect=dialect, **kwargs)
        else:
            schema = get_schema_from_query_mssql_native(parsed_query.copy(), connection, dialect=dialect, **kwargs)
        return (
            schema,
            parsed_query,
            dialect,
        )

    schema, parsed_query, dialect = _fetch_info_needing_connection()

    # Create the generator function for our custom IO source
    def source_generator(
        with_columns: Optional[List[str]],
        predicate: Optional[pl.Expr],
        n_rows: Optional[int],
        batch_size: Optional[int],
    ):
        # Short-circuit: if the caller already knows zero rows are needed
        # (e.g. from head(0) on a contradictory filter), skip the query entirely.
        if n_rows == 0:
            empty = pl.DataFrame({}, schema=schema)
            if with_columns is not None:
                empty = empty.select(col for col in schema if col in set(with_columns))
            yield empty
            return

        # Generate a new SQL query by combining the original query with the predicate
        query_copy = parsed_query.copy()
        final_query_expr = apply_polars_io_source_exprs(query_copy, dialect, with_columns, predicate, n_rows, batch_size)
        # Convert back to SQL string
        final_sql = final_query_expr.sql(dialect=dialect)
        log.debug(f"Executing SQL with pushdown: {final_sql}")

        # Create a connection string if needed
        conn_string = connection if isinstance(connection, str) else str(connection)
        try:
            effective_batch_size = batch_size_override or (fetch_size if batch_size is None else batch_size)
            if engine == "odbc":
                from arrow_odbc import read_arrow_batches_from_odbc

                batch_reader = read_arrow_batches_from_odbc(
                    query=final_sql,
                    batch_size=effective_batch_size,
                    connection_string=conn_string,
                    **kwargs,
                )
            elif engine == "connectorx":
                batch_reader = _connectorx_arrow_reader(
                    query=final_sql,
                    connection=conn_string,
                    batch_size=effective_batch_size,
                    **kwargs,
                )
            else:
                batch_reader = _mssql_native_arrow_reader(
                    query=final_sql,
                    connection=conn_string,
                    batch_size=effective_batch_size,
                    **kwargs,
                )

            # Track if we've yielded any batches yet
            # This is necessary in case the query yields
            # no records
            count = 0

            def select_cols(df) -> pl.DataFrame:
                if with_columns is not None:
                    with_cols_set = set(with_columns)
                    return df.select(col for col in schema.keys() if col in with_cols_set)
                return df

            for record_batch in batch_reader:
                df = pl.DataFrame(record_batch)
                if predicate is not None:
                    df = df.filter(predicate)
                yield select_cols(df)
                count += 1

            if count == 0:
                yield select_cols(pl.DataFrame({}, schema=schema))

        except Exception as e:
            err_msg = f"Failed to execute SQL query: {final_sql}\nPredicate:\n{predicate}\n The `with_columns` used: {with_columns}\n"
            err_msg += f"\n\nWhile running the above, received error: {e.__class__.__name__}:{e}"
            raise RuntimeError(err_msg) from e

    return register_io_source_with_is_pure(source_generator, schema=schema)
