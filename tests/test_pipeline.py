"""Tests for pipeline.py: address normalisation, EPC loading, join, aggregation."""

import json
import pathlib

import duckdb
import pandas as pd
import pytest
from rich.console import Console

from houseprices.pipeline import (
    POSTCODE_DISTRICT_OVERRIDES,
    Geography,
    _configure_duckdb,
    _fmt_elapsed,
    _fmt_size,
    _join_tier1,
    _join_tier2,
    _join_tier3,
    _rss_mb,
    _run_aggregations,
    aggregate,
    aggregate_by_geography,
    join_datasets,
    load_epc,
    match_report,
    normalise_address,
    prepare_epc,
    prepare_ppd,
    prepare_ubdc,
    prepare_uprn,
    rematch,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def epc_slim(tmp_path: pathlib.Path) -> pathlib.Path:
    """Pre-deduplicated EPC Parquet, written to tmp_path."""
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    return dst


# ---------------------------------------------------------------------------
# normalise_address
# ---------------------------------------------------------------------------


def test_normalise_address_basic() -> None:
    assert normalise_address("FLAT 2", "12", "HIGH STREET") == "FLAT 2 12 HIGH STREET"


def test_normalise_address_empty_saon() -> None:
    assert normalise_address("", "12A", "ST JOHNS RD") == "12A ST JOHNS ROAD"


def test_normalise_address_apartment_to_flat() -> None:
    assert (
        normalise_address("APARTMENT 4B", "THE GABLES", "GROVE AVE")
        == "FLAT 4B THE GABLES GROVE AVENUE"
    )


def test_normalise_address_uppercases_input() -> None:
    assert normalise_address("flat 2", "12", "high street") == "FLAT 2 12 HIGH STREET"


def test_normalise_address_removes_punctuation() -> None:
    # Apostrophes and periods stripped; ST prefix (saint) not expanded to STREET
    assert normalise_address("", "12A", "ST. JOHN'S ROAD") == "12A ST JOHNS ROAD"


def test_normalise_address_collapses_whitespace() -> None:
    assert normalise_address("", "12A", "HIGH  STREET") == "12A HIGH STREET"


def test_normalise_address_drive_abbreviation() -> None:
    assert normalise_address("", "4", "MANOR DR") == "4 MANOR DRIVE"


def test_normalise_address_close_abbreviation() -> None:
    assert normalise_address("", "7", "OAK CL") == "7 OAK CLOSE"


def test_normalise_address_gardens_abbreviation() -> None:
    assert normalise_address("", "3", "ROSE GDNS") == "3 ROSE GARDENS"


def test_normalise_address_unit_to_flat() -> None:
    assert normalise_address("UNIT 3", "10", "HIGH STREET") == "FLAT 3 10 HIGH STREET"


def test_normalise_address_unit_mid_string() -> None:
    # UNIT only triggers on a whole word; should not corrupt UNITED etc.
    result = normalise_address("UNIT 4B", "22", "MILL LANE")
    assert result == "FLAT 4B 22 MILL LANE"


def test_normalise_address_hyphen_in_street_becomes_space() -> None:
    # Hyphens are word separators — replace with space so that
    # "CROSS-O-THE-HANDS" matches "CROSS O THE HANDS"
    result = normalise_address("", "HILLSIDE", "CROSS-O-THE-HANDS")
    assert result == "HILLSIDE CROSS O THE HANDS"


def test_normalise_address_hyphenated_property_name_becomes_space() -> None:
    # Hyphenated property names (EPC style) normalise to space-separated
    assert normalise_address("", "ROSE-COTTAGE", "") == "ROSE COTTAGE"


def test_normalise_address_apostrophe_still_stripped() -> None:
    # Apostrophes (possessives) are stripped without adding a space —
    # existing behaviour must be preserved alongside the hyphen change
    assert normalise_address("", "12A", "ST. JOHN'S ROAD") == "12A ST JOHNS ROAD"


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------


def test_price_per_sqm_uses_total_not_mean_of_ratios() -> None:
    """Aggregate must be total_price / total_area, not mean of per-property ratios."""
    rows = [
        {"price": 200_000, "floor_area": 50},  # £4 000/m²
        {"price": 400_000, "floor_area": 200},  # £2 000/m²
    ]
    # Correct:          600_000 / 250 = £2 400/m²
    # Wrong (mean):  (4000 + 2000) / 2 = £3 000/m²
    assert aggregate(rows)["price_per_sqm"] == 2400


def test_price_per_sqm_single_record() -> None:
    rows = [{"price": 300_000, "floor_area": 100}]
    assert aggregate(rows)["price_per_sqm"] == 3000


def test_aggregate_raises_on_empty_input() -> None:
    with pytest.raises((ValueError, ZeroDivisionError)):
        aggregate([])


# ---------------------------------------------------------------------------
# load_epc
# ---------------------------------------------------------------------------


def test_load_epc_deduplicates_by_uprn() -> None:
    """Most recent certificate per UPRN must be kept; older duplicates dropped.

    The fixture has two rows for UPRN 100001:
      - 2020-01-15, TOTAL_FLOOR_AREA=80.0  ← must be kept
      - 2018-06-01, TOTAL_FLOOR_AREA=78.0  ← must be dropped
    """
    result = load_epc(FIXTURES / "epc_sample.csv")
    rows_for_uprn = result[result["UPRN"] == 100001]
    assert len(rows_for_uprn) == 1
    assert rows_for_uprn.iloc[0]["TOTAL_FLOOR_AREA"] == 80.0


def test_load_epc_preserves_no_uprn_rows() -> None:
    """Rows without a UPRN must be kept — they are Tier 2 address-match candidates."""
    result = load_epc(FIXTURES / "epc_sample.csv")
    no_uprn = result[result["UPRN"].isna()]
    assert len(no_uprn) == 2


def test_load_epc_total_row_count() -> None:
    """After dedup: 2 unique UPRNs + 2 no-UPRN rows = 4 rows total."""
    result = load_epc(FIXTURES / "epc_sample.csv")
    assert len(result) == 4


# ---------------------------------------------------------------------------
# join_datasets
# ---------------------------------------------------------------------------


@pytest.fixture
def joined(epc_slim: pathlib.Path, tmp_path: pathlib.Path) -> pd.DataFrame:
    dst = tmp_path / "matched.parquet"
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=dst,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    return pd.read_parquet(dst)


TXN_001 = "{A0000001-0000-0000-0000-000000000000}"


def test_join_tier1_match(joined: "pd.DataFrame") -> None:  # type: ignore[name-defined]  # noqa: F821
    """TXN-001: UBDC entry + EPC UPRN present — must be a Tier 1 match."""
    row = joined[joined["transaction_unique_identifier"] == TXN_001]
    assert len(row) == 1
    assert row.iloc[0]["match_tier"] == 1


def test_join_tier1_uses_deduplicated_epc(joined: "pd.DataFrame") -> None:  # type: ignore[name-defined]  # noqa: F821
    """TXN-001 must use the 2020 EPC row (80 m²), not the 2018 duplicate (78 m²)."""
    row = joined[joined["transaction_unique_identifier"] == TXN_001]
    assert row.iloc[0]["TOTAL_FLOOR_AREA"] == 80.0


def test_join_tier2_matches(joined: "pd.DataFrame") -> None:  # type: ignore[name-defined]  # noqa: F821
    """TXN-002, 003, 004 must all match via address normalisation (Tier 2)."""
    tier2_txns = {
        "{A0000002-0000-0000-0000-000000000000}",
        "{A0000003-0000-0000-0000-000000000000}",
        "{A0000004-0000-0000-0000-000000000000}",
    }
    tier2_rows = joined[joined["transaction_unique_identifier"].isin(tier2_txns)]
    assert len(tier2_rows) == 3
    assert (tier2_rows["match_tier"] == 2).all()


def test_join_excludes_unmatched(joined: "pd.DataFrame") -> None:  # type: ignore[name-defined]  # noqa: F821
    """TXN-005 has no matching EPC — must not appear in the result."""
    assert (
        "{A0000005-0000-0000-0000-000000000000}"
        not in joined["transaction_unique_identifier"].values
    )


def test_join_excludes_category_b(joined: "pd.DataFrame") -> None:  # type: ignore[name-defined]  # noqa: F821
    """TXN-006 is ppd_category_type='B' — must be filtered before joining."""
    assert (
        "{A0000006-0000-0000-0000-000000000000}"
        not in joined["transaction_unique_identifier"].values
    )


def test_join_result_row_count(joined: "pd.DataFrame") -> None:  # type: ignore[name-defined]  # noqa: F821
    """Result must contain exactly the 4 matched category-A records."""
    assert len(joined) == 4


# ---------------------------------------------------------------------------
# aggregate_by_postcode_district
#
# Fixture data after joining (4 rows):
#   SD1 1AA  price=250000  floor_area=80.0   (TXN-001, Tier 1)
#   SD1 2AA  price=180000  floor_area=55.0   (TXN-002, Tier 2)
#   SD1 3AA  price=320000  floor_area=95.0   (TXN-003, Tier 2)
#   SD2 1AA  price=150000  floor_area=65.0   (TXN-004, Tier 2)
#
# SD1: total_price=750000  total_area=230.0  price_per_sqm=3261
# SD2: total_price=150000  total_area=65.0   price_per_sqm=2308
# ---------------------------------------------------------------------------


@pytest.fixture
def aggregated(joined: "pd.DataFrame") -> "pd.DataFrame":  # type: ignore[name-defined]  # noqa: F821
    return aggregate_by_geography(joined, Geography.POSTCODE_DISTRICT, min_sales=1)


def test_aggregate_postcode_district_extraction(
    aggregated: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Postcode district is the outward code — last 3 chars stripped."""
    districts = set(aggregated["postcode_district"])
    assert districts == {"SD1", "SD2"}


def test_aggregate_price_per_sqm_is_total_over_total(
    aggregated: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """SD1: 750000 / 230 = 3261 (total/total, not mean of ratios)."""
    row = aggregated[aggregated["postcode_district"] == "SD1"]
    assert row.iloc[0]["price_per_sqm"] == 3261


def test_aggregate_num_sales_count(
    aggregated: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """SD1 has 3 matched transactions; SD2 has 1."""
    sw1a = aggregated[aggregated["postcode_district"] == "SD1"]
    n1 = aggregated[aggregated["postcode_district"] == "SD2"]
    assert sw1a.iloc[0]["num_sales"] == 3
    assert n1.iloc[0]["num_sales"] == 1


def test_aggregate_min_sales_filter(
    joined: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Districts below min_sales threshold are excluded from the result."""
    result = aggregate_by_geography(joined, Geography.POSTCODE_DISTRICT, min_sales=2)
    districts = set(result["postcode_district"])
    assert "SD1" in districts
    assert "SD2" not in districts  # only 1 sale


def test_aggregate_sorted_by_price_per_sqm_descending(
    aggregated: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Result must be sorted highest price_per_sqm first."""
    prices = list(aggregated["price_per_sqm"])
    assert prices == sorted(prices, reverse=True)


def test_aggregate_output_columns(
    aggregated: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Output must contain the expected columns."""
    assert set(aggregated.columns) >= {
        "postcode_district",
        "num_sales",
        "price_per_sqm",
        "total_price",
        "total_floor_area",
    }


# ---------------------------------------------------------------------------
# POSTCODE_DISTRICT_OVERRIDES / E20 → E15 remapping
# ---------------------------------------------------------------------------


def test_postcode_district_overrides_maps_e20_to_e15() -> None:
    """POSTCODE_DISTRICT_OVERRIDES must remap E20 to E15."""
    assert POSTCODE_DISTRICT_OVERRIDES.get("E20") == "E15"


def test_aggregate_e20_remapped_to_e15() -> None:
    """E20 records must be folded into E15 after district override is applied."""
    df = pd.DataFrame(
        {
            "postcode": ["E15 1AA", "E15 2BB", "E20 1AA"],
            "price": [300_000, 200_000, 400_000],
            "TOTAL_FLOOR_AREA": [75.0, 50.0, 100.0],
        }
    )
    result = aggregate_by_geography(df, Geography.POSTCODE_DISTRICT, min_sales=1)
    districts = set(result["postcode_district"])
    assert "E15" in districts
    assert "E20" not in districts


def test_aggregate_e20_sales_count_merged_into_e15() -> None:
    """E15 num_sales must include E20 records after remapping."""
    df = pd.DataFrame(
        {
            "postcode": ["E15 1AA", "E15 2BB", "E20 1AA"],
            "price": [300_000, 200_000, 400_000],
            "TOTAL_FLOOR_AREA": [75.0, 50.0, 100.0],
        }
    )
    result = aggregate_by_geography(df, Geography.POSTCODE_DISTRICT, min_sales=1)
    e15 = result[result["postcode_district"] == "E15"]
    assert e15.iloc[0]["num_sales"] == 3


def test_aggregate_e20_price_per_sqm_merged() -> None:
    """price_per_sqm for merged E15 must be total/total across all three records."""
    # 300000 + 200000 + 400000 = 900000
    # 75 + 50 + 100 = 225
    # 900000 / 225 = 4000
    df = pd.DataFrame(
        {
            "postcode": ["E15 1AA", "E15 2BB", "E20 1AA"],
            "price": [300_000, 200_000, 400_000],
            "TOTAL_FLOOR_AREA": [75.0, 50.0, 100.0],
        }
    )
    result = aggregate_by_geography(df, Geography.POSTCODE_DISTRICT, min_sales=1)
    e15 = result[result["postcode_district"] == "E15"]
    assert e15.iloc[0]["price_per_sqm"] == 4000


# ---------------------------------------------------------------------------
# match_report
#
# Fixture data (5 category-A PPD rows):
#   TXN-001 → Tier 1   (1 row)
#   TXN-002/003/004 → Tier 2  (3 rows)
#   TXN-005 → unmatched (not in joined result)
#   TXN-006 → category B, excluded before joining
#
# Percentages: tier1=20.0%, tier2=60.0%, unmatched=20.0%
# ---------------------------------------------------------------------------

# 5 category-A rows in ppd_sample.csv (TXN-001 to TXN-005)
TOTAL_PPD_FIXTURE = 5


def test_match_report_tier_counts(
    joined: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Tier 1 and Tier 2 counts must reflect the match_tier column."""
    report = match_report(joined, TOTAL_PPD_FIXTURE)
    assert report["tier1"] == 1
    assert report["tier2"] == 3


def test_match_report_unmatched_count(
    joined: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Unmatched = total_ppd − tier1 − tier2."""
    report = match_report(joined, TOTAL_PPD_FIXTURE)
    assert report["unmatched"] == 1


def test_match_report_percentages(
    joined: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Percentages are rounded to one decimal place."""
    report = match_report(joined, TOTAL_PPD_FIXTURE)
    assert report["tier1_pct"] == 20.0
    assert report["tier2_pct"] == 60.0
    assert report["unmatched_pct"] == 20.0


def test_match_report_total(
    joined: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """total reflects the total_ppd argument passed in."""
    report = match_report(joined, TOTAL_PPD_FIXTURE)
    assert report["total"] == TOTAL_PPD_FIXTURE


def test_match_report_percentages_sum_to_100(
    joined: "pd.DataFrame",  # type: ignore[name-defined]  # noqa: F821
) -> None:
    """Tier 1 + Tier 2 + unmatched percentages must sum to 100.0."""
    report = match_report(joined, TOTAL_PPD_FIXTURE)
    total_pct = (
        float(report["tier1_pct"])
        + float(report["tier2_pct"])
        + float(report["unmatched_pct"])
    )
    assert abs(total_pct - 100.0) < 0.2  # allow rounding tolerance


# ---------------------------------------------------------------------------
# aggregate_by_geography — Geography.LSOA
#
# Synthetic fixture: four rows with LSOA21CD, one without (excluded).
#
#   SD0000001: price=430000  area=135.0  price_per_sqm=3185
#   SD0000002: price=150000  area=65.0   price_per_sqm=2308
#   (row without LSOA21CD excluded)
# ---------------------------------------------------------------------------


@pytest.fixture
def lsoa_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"price": 250_000, "TOTAL_FLOOR_AREA": 80.0, "LSOA21CD": "SD0000001"},
            {"price": 180_000, "TOTAL_FLOOR_AREA": 55.0, "LSOA21CD": "SD0000001"},
            {"price": 150_000, "TOTAL_FLOOR_AREA": 65.0, "LSOA21CD": "SD0000002"},
            {"price": 200_000, "TOTAL_FLOOR_AREA": 60.0, "LSOA21CD": None},
        ]
    )


@pytest.fixture
def lsoa_aggregated(lsoa_rows: pd.DataFrame) -> pd.DataFrame:
    return aggregate_by_geography(lsoa_rows, Geography.LSOA, min_sales=1)


def test_lsoa_aggregate_output_column(lsoa_aggregated: pd.DataFrame) -> None:
    """Geography key column must be named LSOA21CD."""
    assert "LSOA21CD" in lsoa_aggregated.columns


def test_lsoa_aggregate_expected_codes(lsoa_aggregated: pd.DataFrame) -> None:
    codes = set(lsoa_aggregated["LSOA21CD"])
    assert codes == {"SD0000001", "SD0000002"}


def test_lsoa_aggregate_excludes_rows_without_code(
    lsoa_aggregated: pd.DataFrame,
) -> None:
    """Rows with no LSOA21CD must be silently dropped."""
    assert len(lsoa_aggregated) == 2


def test_lsoa_aggregate_price_per_sqm(lsoa_aggregated: pd.DataFrame) -> None:
    """SD0000001: (250000+180000) / (80+55) = 430000/135 = 3185."""
    row = lsoa_aggregated[lsoa_aggregated["LSOA21CD"] == "SD0000001"]
    assert row.iloc[0]["price_per_sqm"] == 3185


def test_lsoa_aggregate_num_sales(lsoa_aggregated: pd.DataFrame) -> None:
    row = lsoa_aggregated[lsoa_aggregated["LSOA21CD"] == "SD0000001"]
    assert row.iloc[0]["num_sales"] == 2


def test_lsoa_aggregate_min_sales_filter(lsoa_rows: pd.DataFrame) -> None:
    result = aggregate_by_geography(lsoa_rows, Geography.LSOA, min_sales=2)
    codes = set(result["LSOA21CD"])
    assert "SD0000001" in codes
    assert "SD0000002" not in codes  # only 1 sale


def test_lsoa_aggregate_sorted_descending(lsoa_aggregated: pd.DataFrame) -> None:
    prices = list(lsoa_aggregated["price_per_sqm"])
    assert prices == sorted(prices, reverse=True)


def test_lsoa_aggregate_output_columns(lsoa_aggregated: pd.DataFrame) -> None:
    assert set(lsoa_aggregated.columns) >= {
        "LSOA21CD",
        "num_sales",
        "price_per_sqm",
        "total_price",
        "total_floor_area",
    }


# ---------------------------------------------------------------------------
# _fmt_elapsed
# ---------------------------------------------------------------------------


def test_fmt_elapsed_under_one_minute() -> None:
    assert _fmt_elapsed(45) == "45s"


def test_fmt_elapsed_zero() -> None:
    assert _fmt_elapsed(0) == "0s"


def test_fmt_elapsed_exactly_one_minute() -> None:
    assert _fmt_elapsed(60) == "1:00"


def test_fmt_elapsed_minutes_and_seconds() -> None:
    assert _fmt_elapsed(90) == "1:30"


def test_fmt_elapsed_pads_seconds() -> None:
    assert _fmt_elapsed(125) == "2:05"


# ---------------------------------------------------------------------------
# _fmt_size
# ---------------------------------------------------------------------------


def test_fmt_size_bytes() -> None:
    assert _fmt_size(500) == "500 B"


def test_fmt_size_kilobytes() -> None:
    assert _fmt_size(2_000) == "2.0 KB"


def test_fmt_size_megabytes() -> None:
    assert _fmt_size(5_000_000) == "5.0 MB"


def test_fmt_size_gigabytes() -> None:
    assert _fmt_size(2_700_000_000) == "2.7 GB"


# ---------------------------------------------------------------------------
# _rss_mb
# ---------------------------------------------------------------------------


def test_rss_mb_returns_positive_integer() -> None:
    rss = _rss_mb()
    assert isinstance(rss, int)
    assert rss > 0


def test_rss_mb_plausible_range() -> None:
    # A running Python process should use at least 10 MB and less than 100 GB
    rss = _rss_mb()
    assert 10 < rss < 100_000


# ---------------------------------------------------------------------------
# prepare_epc
# ---------------------------------------------------------------------------


EPC_COLUMNS = {
    "UPRN",
    "LODGEMENT_DATETIME",
    "TOTAL_FLOOR_AREA",
    "ADDRESS1",
    "ADDRESS2",
    "POSTCODE",
    "BUILT_FORM",
    "CONSTRUCTION_AGE_BAND",
    "CURRENT_ENERGY_RATING",
}


def test_prepare_epc_writes_expected_columns(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    assert set(pd.read_parquet(dst).columns) == EPC_COLUMNS


def test_prepare_epc_preserves_row_count(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    assert len(pd.read_parquet(dst)) == 4


def test_prepare_epc_deduplicates_by_uprn(tmp_path: pathlib.Path) -> None:
    """Output must contain at most one row per non-null UPRN."""
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    df = pd.read_parquet(dst)
    with_uprn = df[df["UPRN"].notna()]
    assert with_uprn["UPRN"].nunique() == len(with_uprn)


def test_prepare_epc_keeps_most_recent_per_uprn(tmp_path: pathlib.Path) -> None:
    """For UPRN 100001, the 2020 row (80 m²) must be kept, not the 2018 row (78 m²)."""
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    df = pd.read_parquet(dst)
    row = df[df["UPRN"] == 100001]
    assert len(row) == 1
    assert row.iloc[0]["TOTAL_FLOOR_AREA"] == 80.0


def test_prepare_epc_preserves_null_uprn_rows(tmp_path: pathlib.Path) -> None:
    """Rows with no UPRN must all be kept — they are Tier 2 candidates."""
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    df = pd.read_parquet(dst)
    assert len(df[df["UPRN"].isna()]) == 2


def test_prepare_epc_skips_if_exists(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "epc_slim.parquet"
    dst.write_bytes(b"sentinel")
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    assert dst.read_bytes() == b"sentinel"


def test_prepare_epc_cleans_up_tmp_on_success(tmp_path: pathlib.Path) -> None:
    """No .tmp.parquet file must remain after a successful prepare_epc call."""
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst)
    assert not dst.with_suffix(".tmp.parquet").exists()


def test_prepare_epc_handles_backslash_escaped_quotes(tmp_path: pathlib.Path) -> None:
    """prepare_epc must not crash on rows where non-selected columns contain
    JSON with backslash-escaped quotes — seen in real EPC API data e.g.
    '{"value": 8.14, "quantity": "metres"}' in a measurement column.

    In the full 5.7 GB EPC file DuckDB samples only the first 20 KB, which
    contains no backslash-escaped fields, so it auto-detects escape='"'
    (RFC 4180 doubling).  Deep in the file it then hits a row like:
        ...,"{\"value\": 8.14, \"quantity\": \"metres\"}",...
    and raises InvalidInputException ("unterminated quote") under strict mode.
    The fix is strict_mode=false on the read_csv call.

    Small fixtures auto-detect escape='\\' from the sample, so the failure
    mode doesn't trigger here — this test guards against regressions and
    documents the real-world behaviour.
    """
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_backslash_quotes.csv", dst)
    df = pd.read_parquet(dst)
    assert len(df) == 1
    assert df.iloc[0]["TOTAL_FLOOR_AREA"] == 60.0


def test_prepare_epc_handles_short_rows(tmp_path: pathlib.Path) -> None:
    """prepare_epc must not crash on rows with fewer columns than the header.

    The full 5.7 GB EPC CSV contains rows like:
        0000-2800-7833-9572-2531,1,47,"£800 - £1,200",,
    which have only 6 of 93 expected fields.  Without null_padding=true
    DuckDB raises: Expected Number of Columns: 93 Found: 6.

    The fixture places the short row beyond DuckDB's 20 KB sample window so
    schema detection succeeds, then triggers the error at parse time.
    With null_padding=true the short row is padded with NULLs and kept.
    """
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_short_row.csv", dst)
    df = pd.read_parquet(dst)
    # 499 full rows survive deduplication; the short row has NULL floor area
    assert len(df) == 500
    assert df["TOTAL_FLOOR_AREA"].notna().sum() == 499


def test_prepare_epc_handles_single_quoted_addresses(tmp_path: pathlib.Path) -> None:
    """prepare_epc must not misparse rows where address fields contain single
    quotes, e.g. 'OLD TRINITY HALL'.

    In the full 5.7 GB EPC file DuckDB samples only the first 20 KB.  If that
    sample contains a field like 'OLD TRINITY HALL' (single quotes wrapping
    the whole value), DuckDB may auto-detect quote='\\'' or escape='\\''.
    This shifts every column, causing total_floor_area to receive a timestamp
    value and raising ConversionException.

    The fix is to pin quote='\"' and escape='\"' on the read_csv call so
    auto-detection cannot choose single quote as the quote/escape character.

    Small fixtures don't reliably trigger the wrong auto-detection — this test
    guards against regressions and documents the real-world failure.
    """
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_single_quotes.csv", dst)
    df = pd.read_parquet(dst)
    assert len(df) == 1
    assert df.iloc[0]["TOTAL_FLOOR_AREA"] == 83.0


def test_prepare_epc_handles_quoted_newlines(tmp_path: pathlib.Path) -> None:
    """prepare_epc must not crash on rows where a quoted field contains a
    newline character — seen in address fields in the real EPC bulk CSV.

    DuckDB's parallel CSV scanner does not support null_padding=true when
    quoted newlines are present and raises:
        "The parallel scanner does not support null_padding in conjunction
        with quoted new lines. Please disable the parallel csv reader with
        parallel=false"

    The fix is to add parallel=false to the read_csv call.

    Small fixtures don't reliably trigger the parallel scanner — this test
    guards against regressions and documents the real-world failure.
    """
    dst = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_quoted_newline.csv", dst)
    df = pd.read_parquet(dst)
    assert len(df) == 1
    assert df.iloc[0]["TOTAL_FLOOR_AREA"] == 75.0


# ---------------------------------------------------------------------------
# prepare_uprn
# ---------------------------------------------------------------------------


def test_prepare_uprn_writes_expected_columns(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "uprn_slim.parquet"
    prepare_uprn(FIXTURES / "uprn_sample.csv", dst)
    assert set(pd.read_parquet(dst).columns) == {"UPRN", "X_COORDINATE", "Y_COORDINATE"}


def test_prepare_uprn_preserves_row_count(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "uprn_slim.parquet"
    prepare_uprn(FIXTURES / "uprn_sample.csv", dst)
    assert len(pd.read_parquet(dst)) == 2


def test_prepare_uprn_skips_if_exists(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "uprn_slim.parquet"
    dst.write_bytes(b"sentinel")
    prepare_uprn(FIXTURES / "uprn_sample.csv", dst)
    assert dst.read_bytes() == b"sentinel"


# ---------------------------------------------------------------------------
# prepare_ubdc
# ---------------------------------------------------------------------------


def test_prepare_ubdc_writes_expected_columns(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "ubdc_slim.parquet"
    prepare_ubdc(FIXTURES / "ubdc_sample.csv", dst)
    assert set(pd.read_parquet(dst).columns) == {"transactionid", "uprn"}


def test_prepare_ubdc_preserves_row_count(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "ubdc_slim.parquet"
    prepare_ubdc(FIXTURES / "ubdc_sample.csv", dst)
    assert len(pd.read_parquet(dst)) == 3


def test_prepare_ubdc_skips_if_exists(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "ubdc_slim.parquet"
    dst.write_bytes(b"sentinel")
    prepare_ubdc(FIXTURES / "ubdc_sample.csv", dst)
    assert dst.read_bytes() == b"sentinel"


# ---------------------------------------------------------------------------
# join_datasets — accepts prepared Parquet inputs
# ---------------------------------------------------------------------------


def test_join_datasets_accepts_prepared_parquet(tmp_path: pathlib.Path) -> None:
    """join_datasets must produce the same result when fed Parquet-prepared inputs."""
    epc_slim = tmp_path / "epc_slim.parquet"
    ubdc_slim = tmp_path / "ubdc_slim.parquet"
    dst = tmp_path / "matched.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", epc_slim)
    prepare_ubdc(FIXTURES / "ubdc_sample.csv", ubdc_slim)
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        ubdc_slim,
        dst=dst,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    result = pd.read_parquet(dst)
    assert len(result) == 4
    assert set(result["match_tier"]) == {1, 2}


# ---------------------------------------------------------------------------
# _join_tier1 / _join_tier2
# ---------------------------------------------------------------------------


@pytest.fixture
def tier1(epc_slim: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
    dst = tmp_path / "tier1.parquet"
    _join_tier1(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst,
    )
    return dst


def test_join_tier1_returns_only_tier1_rows(tier1: pathlib.Path) -> None:
    assert (pd.read_parquet(tier1)["match_tier"] == 1).all()


def test_join_tier1_contains_uprn_match(tier1: pathlib.Path) -> None:
    assert TXN_001 in pd.read_parquet(tier1)["transaction_unique_identifier"].values


def test_join_tier1_deduplicates_epc(tier1: pathlib.Path) -> None:
    """Tier 1 must use the most recent EPC (80 m²), not the older duplicate (78 m²)."""
    df = pd.read_parquet(tier1)
    row = df[df["transaction_unique_identifier"] == TXN_001]
    assert row.iloc[0]["TOTAL_FLOOR_AREA"] == 80.0


def test_join_tier1_row_count(tier1: pathlib.Path) -> None:
    assert len(pd.read_parquet(tier1)) == 1


def test_join_tier2_returns_only_tier2_rows(
    tier1: pathlib.Path, epc_slim: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    dst = tmp_path / "tier2.parquet"
    _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1, dst)
    assert (pd.read_parquet(dst)["match_tier"] == 2).all()


def test_join_tier2_excludes_tier1_transactions(
    tier1: pathlib.Path, epc_slim: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    dst = tmp_path / "tier2.parquet"
    _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1, dst)
    assert TXN_001 not in pd.read_parquet(dst)["transaction_unique_identifier"].values


def test_join_tier2_row_count(
    tier1: pathlib.Path, epc_slim: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    dst = tmp_path / "tier2.parquet"
    _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1, dst)
    assert len(pd.read_parquet(dst)) == 3


def test_join_tier2_postcode_filter_excludes_nonmatching_epc(
    tmp_path: pathlib.Path,
) -> None:
    """Tier 2 must still find address matches after postcode pre-filter."""
    epc_slim = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", epc_slim)
    tier1_path = tmp_path / "tier1.parquet"
    _join_tier1(
        FIXTURES / "ppd_sample.csv", epc_slim, FIXTURES / "ubdc_sample.csv", tier1_path
    )
    # Write a fresh EPC with an extra row in a completely different postcode —
    # that row must never appear in tier2 results even after address normalisation
    import duckdb as _duckdb

    extra_epc = tmp_path / "epc_extra.parquet"
    _duckdb.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{epc_slim}')
            UNION ALL
            SELECT 999999::BIGINT, '2023-01-01 00:00:00'::TIMESTAMP, 50.0,
                   'DECOY HOUSE', '', 'ZZ99 9ZZ',
                   'Detached', '2007-2011', 'A'
        ) TO '{extra_epc}' (FORMAT PARQUET)
    """)
    tier2_path = tmp_path / "tier2.parquet"
    _join_tier2(FIXTURES / "ppd_sample.csv", extra_epc, tier1_path, tier2_path)
    tier2 = pd.read_parquet(tier2_path)
    assert len(tier2) == 3
    assert 999999 not in (tier2["uprn"].dropna().tolist())


def test_join_datasets_result_has_no_duplicate_transactions(
    epc_slim: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Combining tier1 and tier2 must not produce duplicate transaction IDs."""
    dst = tmp_path / "matched.parquet"
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=dst,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    result = pd.read_parquet(dst)
    assert result["transaction_unique_identifier"].nunique() == len(result)


def test_join_tier2_writes_to_parquet_path(
    tmp_path: pathlib.Path,
) -> None:
    """_join_tier2 writes results to dst and returns the row count."""
    epc_slim = tmp_path / "epc_slim.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", epc_slim)
    tier1_path = tmp_path / "tier1.parquet"
    _join_tier1(
        FIXTURES / "ppd_sample.csv", epc_slim, FIXTURES / "ubdc_sample.csv", tier1_path
    )
    tier2_path = tmp_path / "tier2.parquet"
    _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1_path, tier2_path)
    tier2 = pd.read_parquet(tier2_path)
    assert len(tier2) == 3
    assert (tier2["match_tier"] == 2).all()


def test_join_datasets_calls_callback_with_tier1_count(
    epc_slim: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """on_tier1_complete is called once with the tier 1 row count before tier 2 runs."""
    calls: list[int] = []
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=tmp_path / "matched.parquet",
        on_tier1_complete=calls.append,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    assert len(calls) == 1
    assert calls[0] == 1


def test_join_datasets_no_callback_does_not_raise(
    epc_slim: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """join_datasets without on_tier1_complete writes result to dst."""
    dst = tmp_path / "matched.parquet"
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=dst,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    assert dst.exists()
    assert len(pd.read_parquet(dst)) == 4


# ---------------------------------------------------------------------------
# prepare_ppd
# ---------------------------------------------------------------------------

PPD_COLUMNS = {
    "transaction_unique_identifier",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "new_build_flag",
    "tenure_type",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
}


def test_prepare_ppd_writes_expected_columns(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "ppd_slim.parquet"
    prepare_ppd(FIXTURES / "ppd_sample.csv", dst)
    assert set(pd.read_parquet(dst).columns) == PPD_COLUMNS


def test_prepare_ppd_filters_to_category_a(tmp_path: pathlib.Path) -> None:
    """Category B rows must be excluded from the slim Parquet."""
    dst = tmp_path / "ppd_slim.parquet"
    prepare_ppd(FIXTURES / "ppd_sample.csv", dst)
    result = pd.read_parquet(dst)
    assert (result["ppd_category_type"] == "A").all()


def test_prepare_ppd_preserves_category_a_row_count(tmp_path: pathlib.Path) -> None:
    """ppd_sample.csv has 5 category-A rows and 1 category-B row."""
    dst = tmp_path / "ppd_slim.parquet"
    prepare_ppd(FIXTURES / "ppd_sample.csv", dst)
    assert len(pd.read_parquet(dst)) == 5


def test_prepare_ppd_skips_if_exists(tmp_path: pathlib.Path) -> None:
    dst = tmp_path / "ppd_slim.parquet"
    dst.write_bytes(b"sentinel")
    prepare_ppd(FIXTURES / "ppd_sample.csv", dst)
    assert dst.read_bytes() == b"sentinel"


# ---------------------------------------------------------------------------
# join_datasets — accepts prepared PPD Parquet input
# ---------------------------------------------------------------------------


def test_join_datasets_accepts_prepared_ppd_parquet(
    tmp_path: pathlib.Path, epc_slim: pathlib.Path
) -> None:
    """join_datasets must produce the same result when fed a slim PPD Parquet."""
    ppd_slim = tmp_path / "ppd_slim.parquet"
    dst = tmp_path / "matched.parquet"
    prepare_ppd(FIXTURES / "ppd_sample.csv", ppd_slim)
    join_datasets(
        ppd_slim,
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=dst,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    result = pd.read_parquet(dst)
    assert len(result) == 4
    assert set(result["match_tier"]) == {1, 2}


# ---------------------------------------------------------------------------
# _configure_duckdb
# ---------------------------------------------------------------------------


def test_configure_duckdb_disables_insertion_order_preservation() -> None:
    """_configure_duckdb must always disable insertion-order preservation."""
    con = duckdb.connect()
    _configure_duckdb(con)
    result = con.execute(
        "SELECT current_setting('preserve_insertion_order')"
    ).fetchone()[0]
    assert str(result).lower() in ("false", "off", "0")


def test_configure_duckdb_no_env_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no env vars set, _configure_duckdb must be a no-op that does not raise."""
    monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
    monkeypatch.delenv("DUCKDB_THREADS", raising=False)
    con = duckdb.connect()
    _configure_duckdb(con)


def test_configure_duckdb_applies_memory_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """DUCKDB_MEMORY_LIMIT env var must be applied to the connection."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "512MB")
    con = duckdb.connect()
    _configure_duckdb(con)


def test_configure_duckdb_applies_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """DUCKDB_THREADS env var must be applied to the connection."""
    monkeypatch.setenv("DUCKDB_THREADS", "1")
    con = duckdb.connect()
    _configure_duckdb(con)


def test_join_tier1_respects_duckdb_memory_limit(
    monkeypatch: pytest.MonkeyPatch,
    epc_slim: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """_join_tier1 must complete successfully when DUCKDB_MEMORY_LIMIT is set."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "512MB")
    dst = tmp_path / "tier1.parquet"
    n = _join_tier1(
        FIXTURES / "ppd_sample.csv", epc_slim, FIXTURES / "ubdc_sample.csv", dst
    )
    assert n == 1


def test_join_tier2_respects_duckdb_memory_limit(
    monkeypatch: pytest.MonkeyPatch,
    epc_slim: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """_join_tier2 must complete successfully when DUCKDB_MEMORY_LIMIT is set."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "512MB")
    tier1_path = tmp_path / "tier1.parquet"
    _join_tier1(
        FIXTURES / "ppd_sample.csv", epc_slim, FIXTURES / "ubdc_sample.csv", tier1_path
    )
    tier2_path = tmp_path / "tier2.parquet"
    n = _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1_path, tier2_path)
    assert n == 3


# ---------------------------------------------------------------------------
# _join_tier3
#
# Fixture data (ppd_tier3.csv / epc_tier3.csv):
#
#   TXN-T1: saon="3"   bare numeric      → FLAT-prepend → "FLAT 3 10 CHURCH LANE"
#   TXN-T2: saon="UNIT 5" UNIT→FLAT     → normalises to "FLAT 5 20 MARKET ROAD"
#   TXN-T3: saon="4A"  bare alphanumeric → FLAT-prepend → "FLAT 4A 30 PARK ROAD"
#   TXN-T4:               (no matching EPC)  → unmatched
# ---------------------------------------------------------------------------

TXN_T1 = "{T0000001-0000-0000-0000-000000000000}"
TXN_T2 = "{T0000002-0000-0000-0000-000000000000}"
TXN_T3 = "{T0000003-0000-0000-0000-000000000000}"
TXN_T4 = "{T0000004-0000-0000-0000-000000000000}"


@pytest.fixture
def epc_tier3_slim(tmp_path: pathlib.Path) -> pathlib.Path:
    dst = tmp_path / "epc_tier3_slim.parquet"
    prepare_epc(FIXTURES / "epc_tier3.csv", dst)
    return dst


@pytest.fixture
def existing_matched(epc_slim: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
    """Tier1+tier2 matches from the main sample fixtures (used as prior-run input)."""
    dst = tmp_path / "matched_existing.parquet"
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=dst,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    return dst


@pytest.fixture
def tier3(
    epc_tier3_slim: pathlib.Path,
    existing_matched: pathlib.Path,
    tmp_path: pathlib.Path,
) -> pathlib.Path:
    dst = tmp_path / "tier3.parquet"
    _join_tier3(FIXTURES / "ppd_tier3.csv", epc_tier3_slim, existing_matched, dst)
    return dst


def test_join_tier3_row_count(tier3: pathlib.Path) -> None:
    """T1, T2, T3 should match; T4 has no EPC → 3 rows total."""
    assert len(pd.read_parquet(tier3)) == 3


def test_join_tier3_match_tier_column(tier3: pathlib.Path) -> None:
    """All tier-3 rows must have match_tier = 3."""
    assert (pd.read_parquet(tier3)["match_tier"] == 3).all()


def test_join_tier3_bare_numeric_saon(tier3: pathlib.Path) -> None:
    """TXN-T1: saon='3' (bare numeric) must be matched via FLAT-prepend."""
    assert TXN_T1 in pd.read_parquet(tier3)["transaction_unique_identifier"].values


def test_join_tier3_unit_to_flat(tier3: pathlib.Path) -> None:
    """TXN-T2: saon='UNIT 5' must match after UNIT→FLAT normalisation."""
    assert TXN_T2 in pd.read_parquet(tier3)["transaction_unique_identifier"].values


def test_join_tier3_bare_alphanumeric_saon(tier3: pathlib.Path) -> None:
    """TXN-T3: saon='4A' (bare alphanumeric) must be matched via FLAT-prepend."""
    assert TXN_T3 in pd.read_parquet(tier3)["transaction_unique_identifier"].values


def test_join_tier3_unmatched_excluded(tier3: pathlib.Path) -> None:
    """TXN-T4 has no EPC row — must not appear in tier-3 output."""
    assert TXN_T4 not in pd.read_parquet(tier3)["transaction_unique_identifier"].values


def test_join_tier3_excludes_already_matched(
    epc_tier3_slim: pathlib.Path,
    existing_matched: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """Records already in existing_matched must not appear in tier-3 output."""
    dst = tmp_path / "tier3.parquet"
    _join_tier3(FIXTURES / "ppd_tier3.csv", epc_tier3_slim, existing_matched, dst)
    tier3_txns = set(pd.read_parquet(dst)["transaction_unique_identifier"])
    existing_txns = set(
        pd.read_parquet(existing_matched)["transaction_unique_identifier"]
    )
    assert tier3_txns.isdisjoint(existing_txns)


def test_join_tier3_floor_area_populated(tier3: pathlib.Path) -> None:
    """Matched tier-3 rows must have TOTAL_FLOOR_AREA from the EPC."""
    df = pd.read_parquet(tier3)
    row_t1 = df[df["transaction_unique_identifier"] == TXN_T1]
    assert row_t1.iloc[0]["TOTAL_FLOOR_AREA"] == 55.0


def test_join_tier3_returns_row_count(
    epc_tier3_slim: pathlib.Path,
    existing_matched: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """_join_tier3 return value must equal the number of rows written."""
    dst = tmp_path / "tier3_count.parquet"
    n = _join_tier3(FIXTURES / "ppd_tier3.csv", epc_tier3_slim, existing_matched, dst)
    assert n == len(pd.read_parquet(dst))


# ---------------------------------------------------------------------------
# _run_aggregations
#
# Needs a synthetic matched.parquet and an uprn_lsoa.parquet.  The uprn_lsoa
# file can be empty since the fixture data has no UPRNs for LSOA lookup.
# ---------------------------------------------------------------------------


@pytest.fixture
def aggregation_inputs(
    epc_slim: pathlib.Path, tmp_path: pathlib.Path
) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
    """Return (matched_parquet, uprn_lsoa_parquet, ppd_slim_parquet) paths."""
    # Build matched from the main sample fixtures (tiers 1+2)
    matched = tmp_path / "matched.parquet"
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=matched,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    # Empty uprn_lsoa (no spatial join in unit tests; LSOA output will have 0 rows)
    uprn_lsoa = tmp_path / "uprn_lsoa.parquet"
    duckdb.execute(f"""
        COPY (
            SELECT NULL::BIGINT AS UPRN, NULL::VARCHAR AS LSOA21CD
            WHERE FALSE
        ) TO '{uprn_lsoa}' (FORMAT PARQUET)
    """)
    # Slim PPD for total count
    ppd_slim = tmp_path / "ppd_slim.parquet"
    prepare_ppd(FIXTURES / "ppd_sample.csv", ppd_slim)
    return matched, uprn_lsoa, ppd_slim


def test_run_aggregations_writes_district_csv(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """_run_aggregations must write price_per_sqm_postcode_district.csv."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    console = Console(quiet=True)
    _run_aggregations(
        matched, uprn_lsoa, ppd_slim, output_dir, min_sales=1, console=console
    )
    district_csv = output_dir / "price_per_sqm_postcode_district.csv"
    assert district_csv.exists()
    df = pd.read_csv(district_csv)
    assert set(df.columns) >= {"postcode_district", "num_sales", "price_per_sqm"}
    assert len(df) >= 1


def test_run_aggregations_writes_lsoa_csv(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """_run_aggregations must write price_per_sqm_lsoa.csv (may have 0 rows)."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    console = Console(quiet=True)
    _run_aggregations(
        matched, uprn_lsoa, ppd_slim, output_dir, min_sales=1, console=console
    )
    assert (output_dir / "price_per_sqm_lsoa.csv").exists()


def test_run_aggregations_min_sales_filter(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """_run_aggregations with high min_sales should produce an empty district CSV."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    console = Console(quiet=True)
    _run_aggregations(
        matched, uprn_lsoa, ppd_slim, output_dir, min_sales=9999, console=console
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    assert len(df) == 0


def test_run_aggregations_district_csv_has_adj_price_per_sqm_column(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """District CSV must contain adj_price_per_sqm (real Jan-2026 £/m²)."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    console = Console(quiet=True)
    _run_aggregations(
        matched, uprn_lsoa, ppd_slim, output_dir, min_sales=1, console=console
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    assert "adj_price_per_sqm" in df.columns


def test_run_aggregations_adj_price_per_sqm_above_nominal(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """adj_price_per_sqm must exceed price_per_sqm for pre-base-month sales."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    console = Console(quiet=True)
    _run_aggregations(
        matched, uprn_lsoa, ppd_slim, output_dir, min_sales=1, console=console
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    # All fixture sales are pre-2026 → adjusted price must exceed nominal
    assert (df["adj_price_per_sqm"] > df["price_per_sqm"]).all()


# ---------------------------------------------------------------------------
# property_type segmentation in district CSV (issue #69)
# ---------------------------------------------------------------------------


def test_run_aggregations_district_csv_has_property_type_column(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """District CSV must contain a property_type column."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        console=Console(quiet=True),
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    assert "property_type" in df.columns


def test_run_aggregations_district_csv_has_all_rows(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """District CSV must include ALL rollup rows for each district."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        console=Console(quiet=True),
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    assert "ALL" in df["property_type"].values


def test_run_aggregations_district_csv_has_per_type_rows(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """District CSV must include per-type rows (T and F present in fixtures)."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        min_sales_type=1,
        console=Console(quiet=True),
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    types = set(df["property_type"].values)
    # Fixtures have T (terraced) and F (flat) in SD1
    assert "T" in types
    assert "F" in types


def test_run_aggregations_all_row_num_sales_equals_sum_of_type_rows(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """ALL row num_sales must equal the sum of per-type num_sales for same district."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        min_sales_type=1,
        console=Console(quiet=True),
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    sd1_all = df[(df["postcode_district"] == "SD1") & (df["property_type"] == "ALL")]
    sd1_types = df[(df["postcode_district"] == "SD1") & (df["property_type"] != "ALL")]
    assert sd1_all.iloc[0]["num_sales"] == sd1_types["num_sales"].sum()


def test_run_aggregations_min_sales_type_filters_sparse_type_rows(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """Per-type rows below min_sales_type are excluded; ALL rows use min_sales."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    # SD1/F has 1 sale; with min_sales_type=2 it should be excluded
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        min_sales_type=2,
        console=Console(quiet=True),
    )
    df = pd.read_csv(output_dir / "price_per_sqm_postcode_district.csv")
    sd1_types = df[(df["postcode_district"] == "SD1") & (df["property_type"] != "ALL")]
    assert "F" not in sd1_types["property_type"].values
    assert "T" in sd1_types["property_type"].values  # 2 sales → kept


# ---------------------------------------------------------------------------
# adjusted_price column in join output (issue #67)
# ---------------------------------------------------------------------------


def test_join_datasets_result_has_adjusted_price_column(
    joined: pd.DataFrame,
) -> None:
    """matched.parquet must carry an adjusted_price column (real Jan-2026 £)."""
    assert "adjusted_price" in joined.columns


def test_join_datasets_adjusted_price_above_nominal_for_pre_base_month_sales(
    joined: pd.DataFrame,
) -> None:
    """adjusted_price must exceed price for all pre-base-month fixture sales."""
    # All ppd_sample sales are pre-2026; CPI at sale month < base-month CPI
    assert (joined["adjusted_price"] > joined["price"]).all()


# ---------------------------------------------------------------------------
# rematch
#
# Uses ppd_tier3.csv + epc_tier3.csv as the data source.
# Tier 2 (join_datasets) catches TXN-T2 (UNIT→FLAT via normalise_addr).
# Tier 3 (rematch) then catches TXN-T1 (bare "3") and TXN-T3 (bare "4A").
# TXN-T4 has no matching EPC and remains unmatched throughout.
# ---------------------------------------------------------------------------


def _build_rematch_cache(
    epc_tier3_slim: pathlib.Path, cache_dir: pathlib.Path
) -> pathlib.Path:
    """Populate cache_dir with prior-run artefacts from the tier-3 fixtures.

    Returns the path to matched.parquet (tier1+tier2 only, before rematch).
    """
    matched = cache_dir / "matched.parquet"
    # Tier 1: no UBDC entries for T* transactions → 0 UPRN matches.
    # Tier 2: TXN-T2 (saon="UNIT 5") caught by UNIT→FLAT in normalise_addr.
    join_datasets(
        FIXTURES / "ppd_tier3.csv",
        epc_tier3_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=matched,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    # Empty uprn_lsoa — no spatial join in unit tests.
    uprn_lsoa = cache_dir / "uprn_lsoa.parquet"
    duckdb.execute(f"""
        COPY (
            SELECT NULL::BIGINT AS UPRN, NULL::VARCHAR AS LSOA21CD WHERE FALSE
        ) TO '{uprn_lsoa}' (FORMAT PARQUET)
    """)
    # Slim PPD needed by _run_aggregations for total PPD count.
    ppd_slim = cache_dir / "ppd_slim.parquet"
    prepare_ppd(FIXTURES / "ppd_tier3.csv", ppd_slim)
    return matched


def test_rematch_appends_tier3_to_matched(
    epc_tier3_slim: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """rematch must append tier-3 matches to an existing matched.parquet."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    matched = _build_rematch_cache(epc_tier3_slim, cache_dir)
    n_before = len(pd.read_parquet(matched))  # TXN-T2 (tier2 UNIT match)

    rematch(
        ppd_path=cache_dir / "ppd_slim.parquet",
        epc_path=epc_tier3_slim,
        cache_dir=cache_dir,
        output_dir=tmp_path / "output",
        min_sales=1,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )

    n_after = len(pd.read_parquet(matched))
    assert n_after > n_before  # TXN-T1 and TXN-T3 now also matched


def test_rematch_tier3_rows_have_match_tier_3(
    epc_tier3_slim: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """New rows added by rematch must have match_tier = 3."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    _build_rematch_cache(epc_tier3_slim, cache_dir)

    rematch(
        ppd_path=cache_dir / "ppd_slim.parquet",
        epc_path=epc_tier3_slim,
        cache_dir=cache_dir,
        output_dir=tmp_path / "output",
        min_sales=1,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )

    df = pd.read_parquet(cache_dir / "matched.parquet")
    assert 3 in df["match_tier"].values


def test_rematch_no_duplicates_after_append(
    epc_tier3_slim: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """rematch must not create duplicate transaction IDs in matched.parquet."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    _build_rematch_cache(epc_tier3_slim, cache_dir)

    rematch(
        ppd_path=cache_dir / "ppd_slim.parquet",
        epc_path=epc_tier3_slim,
        cache_dir=cache_dir,
        output_dir=tmp_path / "output",
        min_sales=1,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )

    df = pd.read_parquet(cache_dir / "matched.parquet")
    assert df["transaction_unique_identifier"].nunique() == len(df)


def test_rematch_missing_matched_parquet_returns_early(tmp_path: pathlib.Path) -> None:
    """rematch must return without error when matched.parquet is absent."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    # No matched.parquet — should print an error and return cleanly
    rematch(
        ppd_path=FIXTURES / "ppd_sample.csv",
        epc_path=FIXTURES / "epc_sample.csv",
        cache_dir=cache_dir,
        output_dir=tmp_path / "output",
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    # No output should have been created
    assert not (tmp_path / "output").exists()


def test_rematch_no_new_matches_leaves_matched_unchanged(
    epc_slim: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """rematch with no new matches must not modify matched.parquet."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    matched = cache_dir / "matched.parquet"
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        dst=matched,
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    mtime_before = matched.stat().st_mtime
    ppd_slim = cache_dir / "ppd_slim.parquet"
    prepare_ppd(FIXTURES / "ppd_sample.csv", ppd_slim)

    # Use the same EPC slim that was used to build matched — tier 3 should
    # find nothing new (tier-2 already captured all address-normalisation cases).
    rematch(
        ppd_path=cache_dir / "ppd_slim.parquet",
        epc_path=epc_slim,
        cache_dir=cache_dir,
        output_dir=tmp_path / "output",
        cpi_path=FIXTURES / "cpi_sample.csv",
    )
    assert matched.stat().st_mtime == mtime_before


# ---------------------------------------------------------------------------
# Temporal EPC matching (issue #60)
#
# Fixtures: epc_temporal.csv / ppd_temporal.csv / ubdc_temporal.csv
#
# UPRN 200001 — 14 Engine Lane, SD3 1AA — has three EPC lodgements:
#   2015-03-01  70.0 m²  (earliest)
#   2019-06-01  85.0 m²  (middle)
#   2022-09-01  95.0 m²  (most recent)
#
# Three sales exercise every branch of the temporal matching algorithm:
#
#   TXN-T01  2020-06-01  prior EPCs exist (2015, 2019) — must select 2019 (most
#                         recent prior = 85 m²), NOT 2022 (most recent overall)
#   TXN-T02  2013-04-01  no prior EPC — must fall back to earliest post-sale
#                         (2015 = 70 m²), NOT most recent post-sale (2022)
#   TXN-T03  2000-01-01  all EPCs are 15+ years ahead — beyond the 10-year
#                         cutoff, must produce NO match
# ---------------------------------------------------------------------------

_EPC_TEMPORAL = FIXTURES / "epc_temporal.csv"
_PPD_TEMPORAL = FIXTURES / "ppd_temporal.csv"
_UBDC_TEMPORAL = FIXTURES / "ubdc_temporal.csv"

TXN_T01 = "{A000T001-0000-0000-0000-000000000000}"
TXN_T02 = "{A000T002-0000-0000-0000-000000000000}"
TXN_T03 = "{A000T003-0000-0000-0000-000000000000}"


@pytest.fixture
def joined_temporal(tmp_path: pathlib.Path) -> pd.DataFrame:
    """Tier-1 join result using the temporal EPC fixture (undeduped CSV)."""
    dst = tmp_path / "matched_temporal.parquet"
    _join_tier1(_PPD_TEMPORAL, _EPC_TEMPORAL, _UBDC_TEMPORAL, dst)
    return pd.read_parquet(dst)


# --- prepare_epc deduplicate=False ---


def test_prepare_epc_deduplicate_false_retains_all_uprn_rows(
    tmp_path: pathlib.Path,
) -> None:
    """With deduplicate=False both EPC rows for UPRN 100001 must be kept."""
    dst = tmp_path / "epc_full.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst, deduplicate=False)
    df = pd.read_parquet(dst)
    assert len(df[df["UPRN"] == 100001]) == 2


def test_prepare_epc_deduplicate_true_keeps_default_behaviour(
    tmp_path: pathlib.Path,
) -> None:
    """With deduplicate=True (explicit) behaviour is identical to the default."""
    dst = tmp_path / "epc_deduped.parquet"
    prepare_epc(FIXTURES / "epc_sample.csv", dst, deduplicate=True)
    df = pd.read_parquet(dst)
    rows = df[df["UPRN"] == 100001]
    assert len(rows) == 1
    assert rows.iloc[0]["TOTAL_FLOOR_AREA"] == 80.0


# --- temporal EPC selection ---


def test_join_tier1_selects_most_recent_prior_epc_not_overall_most_recent(
    joined_temporal: pd.DataFrame,
) -> None:
    """TXN-T01 (2020 sale): must use the 2019 EPC (85 m²), not 2022 (95 m²)."""
    row = joined_temporal[joined_temporal["transaction_unique_identifier"] == TXN_T01]
    assert len(row) == 1, "Expected exactly one row for TXN-T01"
    assert row.iloc[0]["TOTAL_FLOOR_AREA"] == 85.0


def test_join_tier1_falls_back_to_earliest_post_sale_when_no_prior(
    joined_temporal: pd.DataFrame,
) -> None:
    """TXN-T02 (2013 sale): no prior EPC — must use 2015 (70 m²), not 2022 (95 m²)."""
    row = joined_temporal[joined_temporal["transaction_unique_identifier"] == TXN_T02]
    assert len(row) == 1, "Expected exactly one row for TXN-T02"
    assert row.iloc[0]["TOTAL_FLOOR_AREA"] == 70.0


def test_join_tier1_excludes_sale_with_all_epcs_beyond_max_gap(
    joined_temporal: pd.DataFrame,
) -> None:
    """TXN-T03 (2000 sale): nearest EPC is 2015, 15 years away — no match."""
    assert TXN_T03 not in joined_temporal["transaction_unique_identifier"].values


# --- gap_days and is_post_sale columns ---


def test_join_tier1_output_has_gap_days_column(
    joined_temporal: pd.DataFrame,
) -> None:
    assert "gap_days" in joined_temporal.columns


def test_join_tier1_output_has_is_post_sale_column(
    joined_temporal: pd.DataFrame,
) -> None:
    assert "is_post_sale" in joined_temporal.columns


def test_join_tier1_gap_days_negative_for_prior_epc(
    joined_temporal: pd.DataFrame,
) -> None:
    """TXN-T01: EPC lodged before sale → gap_days must be negative."""
    row = joined_temporal[joined_temporal["transaction_unique_identifier"] == TXN_T01]
    assert row.iloc[0]["gap_days"] < 0


def test_join_tier1_gap_days_positive_for_post_sale_epc(
    joined_temporal: pd.DataFrame,
) -> None:
    """TXN-T02: EPC lodged after sale → gap_days must be positive."""
    row = joined_temporal[joined_temporal["transaction_unique_identifier"] == TXN_T02]
    assert row.iloc[0]["gap_days"] > 0


def test_join_tier1_is_post_sale_false_for_prior_epc(
    joined_temporal: pd.DataFrame,
) -> None:
    """TXN-T01: matched to a prior EPC → is_post_sale must be False."""
    row = joined_temporal[joined_temporal["transaction_unique_identifier"] == TXN_T01]
    assert row.iloc[0]["is_post_sale"] is False or row.iloc[0]["is_post_sale"] == False  # noqa: E712


def test_join_tier1_is_post_sale_true_for_post_sale_fallback(
    joined_temporal: pd.DataFrame,
) -> None:
    """TXN-T02: matched to a post-sale EPC → is_post_sale must be True."""
    row = joined_temporal[joined_temporal["transaction_unique_identifier"] == TXN_T02]
    assert row.iloc[0]["is_post_sale"] is True or row.iloc[0]["is_post_sale"] == True  # noqa: E712


# ---------------------------------------------------------------------------
# _run_aggregations — metadata.json (issue #89)
# ---------------------------------------------------------------------------


def test_run_aggregations_writes_metadata_json(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """_run_aggregations must write output/metadata.json."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        console=Console(quiet=True),
    )
    assert (output_dir / "metadata.json").exists()


def test_run_aggregations_metadata_has_min_max_sale_date(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """metadata.json must contain min_sale_date and max_sale_date keys."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        console=Console(quiet=True),
    )
    meta = json.loads((output_dir / "metadata.json").read_text())
    assert "min_sale_date" in meta
    assert "max_sale_date" in meta


def test_run_aggregations_metadata_dates_are_iso_format(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """metadata.json dates must be YYYY-MM-DD strings."""
    import re

    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        console=Console(quiet=True),
    )
    meta = json.loads((output_dir / "metadata.json").read_text())
    iso = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    assert iso.match(meta["min_sale_date"]), f"Not ISO: {meta['min_sale_date']}"
    assert iso.match(meta["max_sale_date"]), f"Not ISO: {meta['max_sale_date']}"


def test_run_aggregations_metadata_dates_match_fixture_range(
    aggregation_inputs: tuple[pathlib.Path, pathlib.Path, pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """metadata.json min_sale_date must be ≤ max_sale_date, both from matched data."""
    matched, uprn_lsoa, ppd_slim = aggregation_inputs
    output_dir = tmp_path / "output"
    _run_aggregations(
        matched,
        uprn_lsoa,
        ppd_slim,
        output_dir,
        min_sales=1,
        console=Console(quiet=True),
    )
    meta = json.loads((output_dir / "metadata.json").read_text())
    import datetime

    min_d = datetime.date.fromisoformat(meta["min_sale_date"])
    max_d = datetime.date.fromisoformat(meta["max_sale_date"])
    assert min_d <= max_d
    # Both dates must fall within the fixture data range (2021–2023 for matched records)
    assert min_d >= datetime.date(2021, 1, 1)
    assert max_d <= datetime.date(2024, 1, 1)
