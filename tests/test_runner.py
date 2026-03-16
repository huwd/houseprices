"""Tests for the top-level pipeline runner: checkpoint helper and run()."""

import pathlib
import shutil
from unittest.mock import patch

import pandas as pd
import pytest

from houseprices.pipeline import _checkpoint, run

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def _fixture_data(tmp_path: pathlib.Path) -> dict[str, pathlib.Path]:
    """Copy fixture CSVs into tmp_path/data/ so run() can delete them safely."""
    data = tmp_path / "data"
    data.mkdir()
    for name in [
        "ppd_sample.csv",
        "epc_sample.csv",
        "ubdc_sample.csv",
        "uprn_sample.csv",
    ]:
        shutil.copy(FIXTURES / name, data / name)
    shutil.copy(FIXTURES / "lsoa_sample.geojson", data / "lsoa_sample.geojson")
    return {
        "ppd_path": data / "ppd_sample.csv",
        "epc_path": data / "epc_sample.csv",
        "ubdc_path": data / "ubdc_sample.csv",
        "uprn_path": data / "uprn_sample.csv",
        "boundary_path": data / "lsoa_sample.geojson",
    }


# ---------------------------------------------------------------------------
# _checkpoint
# ---------------------------------------------------------------------------


def test_checkpoint_calls_compute_and_saves(tmp_path: pathlib.Path) -> None:
    calls = 0

    def compute() -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return pd.DataFrame({"x": [1, 2, 3]})

    result = _checkpoint("test", tmp_path, compute)

    assert calls == 1
    assert (tmp_path / "test.parquet").exists()
    assert list(result["x"]) == [1, 2, 3]


def test_checkpoint_skips_compute_if_cached(tmp_path: pathlib.Path) -> None:
    pd.DataFrame({"x": [99]}).to_parquet(tmp_path / "test.parquet", index=False)
    calls = 0

    def compute() -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return pd.DataFrame({"x": [1]})

    result = _checkpoint("test", tmp_path, compute)

    assert calls == 0
    assert list(result["x"]) == [99]


def test_checkpoint_creates_cache_dir(tmp_path: pathlib.Path) -> None:
    nested = tmp_path / "a" / "b" / "cache"
    _checkpoint("test", nested, lambda: pd.DataFrame({"x": [1]}))
    assert nested.exists()


# ---------------------------------------------------------------------------
# run — uses fixture data; results are small but structurally correct
# ---------------------------------------------------------------------------


@pytest.fixture
def run_result(tmp_path: pathlib.Path) -> pathlib.Path:
    """Run the full pipeline against fixture data; return tmp_path.

    tmp_path/data/   — copies of fixture CSVs (may be deleted by prepare steps)
    tmp_path/cache/  — Parquet checkpoints
    tmp_path/output/ — output CSVs
    """
    run(
        **_fixture_data(tmp_path),
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        min_sales=1,
    )
    return tmp_path


def test_run_writes_postcode_district_csv(run_result: pathlib.Path) -> None:
    assert (run_result / "output" / "price_per_sqm_postcode_district.csv").exists()


def test_run_writes_lsoa_csv(run_result: pathlib.Path) -> None:
    assert (run_result / "output" / "price_per_sqm_lsoa.csv").exists()


def test_run_creates_matched_checkpoint(run_result: pathlib.Path) -> None:
    assert (run_result / "cache" / "matched.parquet").exists()


def test_run_creates_uprn_lsoa_checkpoint(run_result: pathlib.Path) -> None:
    assert (run_result / "cache" / "uprn_lsoa.parquet").exists()


def test_run_postcode_district_has_expected_districts(
    run_result: pathlib.Path,
) -> None:
    df = pd.read_csv(run_result / "output" / "price_per_sqm_postcode_district.csv")
    assert set(df["postcode_district"]) == {"SD1", "SD2"}


def test_run_deletes_source_csvs_after_prepare(tmp_path: pathlib.Path) -> None:
    """EPC, UBDC, and UPRN raw CSVs must be deleted once slim Parquets are written.

    PPD is not prepared to Parquet, so it must be left in place.
    """
    paths = _fixture_data(tmp_path)
    run(
        **paths,
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "output",
        min_sales=1,
    )
    assert not paths["epc_path"].exists()
    assert not paths["ubdc_path"].exists()
    assert not paths["uprn_path"].exists()
    assert paths["ppd_path"].exists()


def test_run_skips_join_on_second_call(tmp_path: pathlib.Path) -> None:
    """Second run re-uses matched.parquet; join_datasets is not called again."""
    paths = _fixture_data(tmp_path)
    kwargs: dict[str, object] = {
        **paths,
        "cache_dir": tmp_path / "cache",
        "output_dir": tmp_path / "output",
        "min_sales": 1,
    }
    run(**kwargs)  # type: ignore[arg-type]
    with patch("houseprices.pipeline.join_datasets") as mock_join:
        run(**kwargs)  # type: ignore[arg-type]
    mock_join.assert_not_called()
