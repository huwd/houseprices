"""Tests for CPI deflation support — issue #67.

Covers:
  load_cpi        — parse an ONS-format monthly CPI CSV into a lookup dict
  deflate_price   — apply a deflator ratio to convert a nominal price to real terms
  aggregate_by_geography — new price_col parameter selects nominal or real price
"""

import datetime
import pathlib

import pandas as pd
import pytest

from houseprices.pipeline import (
    Geography,
    aggregate_by_geography,
    deflate_price,
    load_cpi,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
CPI_FIXTURE = FIXTURES / "cpi_sample.csv"


# ---------------------------------------------------------------------------
# load_cpi
#
# cpi_sample.csv covers the months used by ppd_sample.csv (2020-05 through
# 2023-09) plus the base month (2026-01).
# ---------------------------------------------------------------------------


def test_load_cpi_returns_dict() -> None:
    cpi = load_cpi(CPI_FIXTURE)
    assert isinstance(cpi, dict)


def test_load_cpi_keys_are_year_month_tuples() -> None:
    cpi = load_cpi(CPI_FIXTURE)
    for key in cpi:
        assert isinstance(key, tuple), f"Expected tuple key, got {type(key)}"
        assert len(key) == 2
        year, month = key
        assert isinstance(year, int)
        assert isinstance(month, int)
        assert 1 <= month <= 12


def test_load_cpi_values_are_floats() -> None:
    cpi = load_cpi(CPI_FIXTURE)
    for v in cpi.values():
        assert isinstance(v, float), f"Expected float value, got {type(v)}"


def test_load_cpi_parses_known_month() -> None:
    """(2021, 6) must map to 116.5 as in the fixture."""
    cpi = load_cpi(CPI_FIXTURE)
    assert cpi[(2021, 6)] == pytest.approx(116.5)


def test_load_cpi_base_month_present() -> None:
    """The base month (2026, 1) must be present so deflation can be applied."""
    cpi = load_cpi(CPI_FIXTURE)
    assert (2026, 1) in cpi
    assert cpi[(2026, 1)] == pytest.approx(140.0)


def test_load_cpi_all_fixture_months_loaded() -> None:
    """All 13 rows in the fixture must be parsed."""
    cpi = load_cpi(CPI_FIXTURE)
    assert len(cpi) == 13


# ---------------------------------------------------------------------------
# deflate_price
#
# Formula: adjusted = price × (cpi[base] / cpi[(year, month)])
# ---------------------------------------------------------------------------

_CPI: dict[tuple[int, int], float] = {
    (2021, 6): 116.5,
    (2026, 1): 140.0,
}


def test_deflate_price_older_sale_increases_price() -> None:
    """A 2021-06 price adjusted to 2026-01 pounds must exceed the nominal value."""
    adjusted = deflate_price(200_000, datetime.date(2021, 6, 1), _CPI, base=(2026, 1))
    assert adjusted > 200_000


def test_deflate_price_base_month_returns_original_price() -> None:
    """Deflating a sale in the base month must return the original price exactly."""
    cpi: dict[tuple[int, int], float] = {(2026, 1): 140.0}
    adjusted = deflate_price(300_000, datetime.date(2026, 1, 15), cpi, base=(2026, 1))
    assert adjusted == pytest.approx(300_000)


def test_deflate_price_known_arithmetic() -> None:
    """200,000 × (140.0 / 116.5) ≈ 240,343.35"""
    adjusted = deflate_price(200_000, datetime.date(2021, 6, 1), _CPI, base=(2026, 1))
    assert adjusted == pytest.approx(200_000 * (140.0 / 116.5))


def test_deflate_price_missing_month_raises() -> None:
    """A sale month absent from the CPI table must raise KeyError."""
    cpi: dict[tuple[int, int], float] = {(2026, 1): 140.0}
    with pytest.raises(KeyError):
        deflate_price(100_000, datetime.date(2019, 3, 1), cpi, base=(2026, 1))


def test_deflate_price_uses_year_and_month_not_day() -> None:
    """Two dates in the same month must produce identical adjusted prices."""
    adjusted_1st = deflate_price(
        200_000, datetime.date(2021, 6, 1), _CPI, base=(2026, 1)
    )
    adjusted_30th = deflate_price(
        200_000, datetime.date(2021, 6, 30), _CPI, base=(2026, 1)
    )
    assert adjusted_1st == pytest.approx(adjusted_30th)


# ---------------------------------------------------------------------------
# aggregate_by_geography — price_col parameter (issue #67)
#
# Fixture data:
#   SD1: adjusted_price = [240_000, 220_000, 380_000]  area = [80, 55, 95]
#        total = 840_000 / 230.0  → price_per_sqm = 3_652
#   SD2: adjusted_price = [170_000]                    area = [65]
#        total = 170_000 / 65.0   → price_per_sqm = 2_615
#
#   SD1 nominal: [200_000, 180_000, 320_000] / 230.0 → 3_043
# ---------------------------------------------------------------------------


@pytest.fixture
def matched_with_adjusted() -> pd.DataFrame:
    """Matched DataFrame carrying both price and adjusted_price columns."""
    return pd.DataFrame(
        {
            "postcode": ["SD1 1AA", "SD1 2AA", "SD1 3AA", "SD2 1AA"],
            "price": [200_000, 180_000, 320_000, 150_000],
            "adjusted_price": [240_000, 220_000, 380_000, 170_000],
            "TOTAL_FLOOR_AREA": [80.0, 55.0, 95.0, 65.0],
            "match_tier": [1, 2, 2, 2],
        }
    )


def test_aggregate_by_geography_accepts_price_col_param(
    matched_with_adjusted: pd.DataFrame,
) -> None:
    """aggregate_by_geography must accept a price_col keyword argument."""
    result = aggregate_by_geography(
        matched_with_adjusted,
        Geography.POSTCODE_DISTRICT,
        min_sales=1,
        price_col="price",
    )
    assert len(result) > 0


def test_aggregate_by_geography_default_price_col_unchanged(
    matched_with_adjusted: pd.DataFrame,
) -> None:
    """Explicit price_col='price' must produce the same result as the default."""
    without = aggregate_by_geography(
        matched_with_adjusted, Geography.POSTCODE_DISTRICT, min_sales=1
    )
    with_explicit = aggregate_by_geography(
        matched_with_adjusted,
        Geography.POSTCODE_DISTRICT,
        min_sales=1,
        price_col="price",
    )
    pd.testing.assert_frame_equal(without, with_explicit)


def test_aggregate_by_geography_price_col_adjusted_produces_correct_values(
    matched_with_adjusted: pd.DataFrame,
) -> None:
    """Using price_col='adjusted_price' must aggregate on the adjusted column.

    SD1: 840_000 / 230.0 = 3652 (rounded)
    SD2: 170_000 / 65.0  = 2615 (rounded)
    """
    result = aggregate_by_geography(
        matched_with_adjusted,
        Geography.POSTCODE_DISTRICT,
        min_sales=1,
        price_col="adjusted_price",
    )
    sd1 = result[result["postcode_district"] == "SD1"]
    sd2 = result[result["postcode_district"] == "SD2"]
    assert sd1.iloc[0]["price_per_sqm"] == round(840_000 / 230.0)
    assert sd2.iloc[0]["price_per_sqm"] == round(170_000 / 65.0)


def test_aggregate_by_geography_price_col_adjusted_higher_than_nominal(
    matched_with_adjusted: pd.DataFrame,
) -> None:
    """price_per_sqm with adjusted_price must exceed that with nominal price."""
    nominal = aggregate_by_geography(
        matched_with_adjusted,
        Geography.POSTCODE_DISTRICT,
        min_sales=1,
        price_col="price",
    )
    real = aggregate_by_geography(
        matched_with_adjusted,
        Geography.POSTCODE_DISTRICT,
        min_sales=1,
        price_col="adjusted_price",
    )
    merged = nominal.merge(real, on="postcode_district", suffixes=("_nom", "_real"))
    assert (merged["price_per_sqm_real"] > merged["price_per_sqm_nom"]).all()
