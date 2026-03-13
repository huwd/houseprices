"""Spatial lookup: UPRN coordinates → LSOA via point-in-polygon."""

import pathlib

import pandas as pd


def build_uprn_lsoa(
    uprn_path: str | pathlib.Path,
    boundary_path: str | pathlib.Path,
) -> pd.DataFrame:
    """Join UPRN coordinates to LSOA boundaries via point-in-polygon.

    Returns a DataFrame with columns: UPRN, LSOA21CD, LSOA21NM.
    Only UPRNs that fall within a boundary polygon are included.
    """
    raise NotImplementedError
