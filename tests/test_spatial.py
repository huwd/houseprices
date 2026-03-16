"""Tests for spatial.py: UPRN → LSOA point-in-polygon lookup."""

import pathlib

import pandas as pd
import pytest

from houseprices.pipeline import prepare_uprn
from houseprices.spatial import build_uprn_lsoa

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def uprn_path() -> pathlib.Path:
    return FIXTURES / "uprn_sample.csv"


@pytest.fixture
def lsoa_path() -> pathlib.Path:
    return FIXTURES / "lsoa_sample.geojson"


@pytest.fixture
def uprn_df(uprn_path: pathlib.Path) -> pd.DataFrame:
    return pd.read_csv(uprn_path)


def test_point_in_polygon(uprn_path: pathlib.Path, lsoa_path: pathlib.Path) -> None:
    """Known UPRN coordinate should resolve to the expected LSOA."""
    result = build_uprn_lsoa(uprn_path, lsoa_path)
    assert result.loc[result["UPRN"] == 12345678, "LSOA21CD"].iloc[0] == "SD0000001"


def test_uprn_outside_boundary_excluded(
    uprn_path: pathlib.Path, lsoa_path: pathlib.Path
) -> None:
    """UPRNs that fall outside all boundary polygons should not appear in the result."""
    result = build_uprn_lsoa(uprn_path, lsoa_path)
    assert 87654321 not in result["UPRN"].values


def test_build_uprn_lsoa_accepts_prepared_parquet(
    tmp_path: pathlib.Path, lsoa_path: pathlib.Path
) -> None:
    """build_uprn_lsoa must work when passed a column-pruned Parquet UPRN file."""
    uprn_slim = tmp_path / "uprn_slim.parquet"
    prepare_uprn(FIXTURES / "uprn_sample.csv", uprn_slim)
    result = build_uprn_lsoa(uprn_slim, lsoa_path)
    assert result.loc[result["UPRN"] == 12345678, "LSOA21CD"].iloc[0] == "SD0000001"


def test_build_uprn_lsoa_respects_duckdb_memory_limit(
    monkeypatch: pytest.MonkeyPatch,
    uprn_path: pathlib.Path,
    lsoa_path: pathlib.Path,
) -> None:
    """build_uprn_lsoa must complete correctly when DUCKDB_MEMORY_LIMIT is set."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "512MB")
    result = build_uprn_lsoa(uprn_path, lsoa_path)
    assert result.loc[result["UPRN"] == 12345678, "LSOA21CD"].iloc[0] == "SD0000001"


def test_build_uprn_lsoa_respects_duckdb_threads(
    monkeypatch: pytest.MonkeyPatch,
    uprn_path: pathlib.Path,
    lsoa_path: pathlib.Path,
) -> None:
    """build_uprn_lsoa must complete correctly when DUCKDB_THREADS is set."""
    monkeypatch.setenv("DUCKDB_THREADS", "1")
    result = build_uprn_lsoa(uprn_path, lsoa_path)
    assert result.loc[result["UPRN"] == 12345678, "LSOA21CD"].iloc[0] == "SD0000001"


def test_bng_coordinates_not_swapped(uprn_df: pd.DataFrame) -> None:
    """Easting (X) should be ~100k–700k, Northing (Y) ~0–1300k for England & Wales.

    Guards against accidental X/Y swap when loading OS Open UPRN data.
    """
    assert uprn_df["X_COORDINATE"].between(100_000, 700_000).all()
    assert uprn_df["Y_COORDINATE"].between(0, 1_300_000).all()
