"""Spatial lookup: UPRN coordinates → LSOA via point-in-polygon."""

import os
import pathlib

import duckdb
import pandas as pd


def _configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """Apply resource limits from environment variables to a DuckDB connection.

    Reads ``DUCKDB_MEMORY_LIMIT`` and ``DUCKDB_THREADS`` from the environment.
    See pipeline._configure_duckdb for full documentation.
    """
    memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT")
    threads = os.environ.get("DUCKDB_THREADS")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if threads:
        con.execute(f"SET threads = {int(threads)}")


def build_uprn_lsoa(
    uprn_path: str | pathlib.Path,
    boundary_path: str | pathlib.Path,
) -> pd.DataFrame:
    """Join UPRN coordinates to LSOA boundaries via point-in-polygon.

    Returns a DataFrame with columns: UPRN, LSOA21CD, LSOA21NM.
    Only UPRNs that fall within a boundary polygon are included.
    """
    con = duckdb.connect()
    _configure_duckdb(con)
    con.execute("INSTALL spatial; LOAD spatial;")

    uprn = str(uprn_path)
    if uprn.endswith(".parquet"):
        uprn_src = f"read_parquet('{uprn}')"
    else:
        uprn_src = f"read_csv('{uprn}')"
    boundary = str(boundary_path)

    return con.execute(f"""
        SELECT
            u.UPRN,
            l.LSOA21CD,
            l.LSOA21NM
        FROM {uprn_src} AS u
        JOIN ST_Read('{boundary}') AS l
          ON ST_Within(
              ST_Point(u.X_COORDINATE, u.Y_COORDINATE),
              l.geom
          )
    """).df()
