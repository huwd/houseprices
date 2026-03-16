"""Spatial lookup: UPRN coordinates → LSOA via point-in-polygon."""

import pathlib

import duckdb
import pandas as pd


def build_uprn_lsoa(
    uprn_path: str | pathlib.Path,
    boundary_path: str | pathlib.Path,
) -> pd.DataFrame:
    """Join UPRN coordinates to LSOA boundaries via point-in-polygon.

    Returns a DataFrame with columns: UPRN, LSOA21CD, LSOA21NM.
    Only UPRNs that fall within a boundary polygon are included.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    uprn = str(uprn_path)
    uprn_src = f"read_parquet('{uprn}')" if uprn.endswith(".parquet") else f"read_csv('{uprn}')"
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
