"""Tests for pipeline.py: address normalisation, EPC loading, join, aggregation."""

import pathlib

import duckdb
import pandas as pd
import pytest

from houseprices.pipeline import (
    Geography,
    _configure_duckdb,
    _fmt_elapsed,
    _fmt_size,
    _join_tier1,
    _join_tier2,
    _rss_mb,
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
def joined(epc_slim: pathlib.Path) -> "pd.DataFrame":  # type: ignore[name-defined]  # noqa: F821
    import pandas as pd  # noqa: F401

    return join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
    )


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
    prepare_epc(FIXTURES / "epc_sample.csv", epc_slim)
    prepare_ubdc(FIXTURES / "ubdc_sample.csv", ubdc_slim)
    result = join_datasets(FIXTURES / "ppd_sample.csv", epc_slim, ubdc_slim)
    assert len(result) == 4
    assert set(result["match_tier"]) == {1, 2}


# ---------------------------------------------------------------------------
# _join_tier1 / _join_tier2
# ---------------------------------------------------------------------------


@pytest.fixture
def tier1(epc_slim: pathlib.Path) -> pd.DataFrame:
    return _join_tier1(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
    )


def test_join_tier1_returns_only_tier1_rows(tier1: pd.DataFrame) -> None:
    assert (tier1["match_tier"] == 1).all()


def test_join_tier1_contains_uprn_match(tier1: pd.DataFrame) -> None:
    assert TXN_001 in tier1["transaction_unique_identifier"].values


def test_join_tier1_deduplicates_epc(tier1: pd.DataFrame) -> None:
    """Tier 1 must use the most recent EPC (80 m²), not the older duplicate (78 m²)."""
    row = tier1[tier1["transaction_unique_identifier"] == TXN_001]
    assert row.iloc[0]["TOTAL_FLOOR_AREA"] == 80.0


def test_join_tier1_row_count(tier1: pd.DataFrame) -> None:
    assert len(tier1) == 1


def test_join_tier2_returns_only_tier2_rows(
    tier1: pd.DataFrame, epc_slim: pathlib.Path
) -> None:
    tier2 = _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1)
    assert (tier2["match_tier"] == 2).all()


def test_join_tier2_excludes_tier1_transactions(
    tier1: pd.DataFrame, epc_slim: pathlib.Path
) -> None:
    tier2 = _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1)
    assert TXN_001 not in tier2["transaction_unique_identifier"].values


def test_join_tier2_row_count(tier1: pd.DataFrame, epc_slim: pathlib.Path) -> None:
    tier2 = _join_tier2(FIXTURES / "ppd_sample.csv", epc_slim, tier1)
    assert len(tier2) == 3


def test_join_datasets_calls_callback_with_tier1_dataframe(
    epc_slim: pathlib.Path,
) -> None:
    """on_tier1_complete is called once with the tier 1 DataFrame before tier 2 runs."""
    calls: list[pd.DataFrame] = []
    join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
        on_tier1_complete=calls.append,
    )
    assert len(calls) == 1
    assert (calls[0]["match_tier"] == 1).all()


def test_join_datasets_no_callback_does_not_raise(epc_slim: pathlib.Path) -> None:
    """join_datasets without on_tier1_complete runs normally."""
    result = join_datasets(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
    )
    assert len(result) == 4


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
    prepare_ppd(FIXTURES / "ppd_sample.csv", ppd_slim)
    result = join_datasets(
        ppd_slim,
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
    )
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
) -> None:
    """_join_tier1 must complete successfully when DUCKDB_MEMORY_LIMIT is set."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "512MB")
    result = _join_tier1(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
    )
    assert len(result) == 1


def test_join_tier2_respects_duckdb_memory_limit(
    monkeypatch: pytest.MonkeyPatch,
    epc_slim: pathlib.Path,
) -> None:
    """_join_tier2 must complete successfully when DUCKDB_MEMORY_LIMIT is set."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "512MB")
    tier1 = _join_tier1(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        FIXTURES / "ubdc_sample.csv",
    )
    result = _join_tier2(
        FIXTURES / "ppd_sample.csv",
        epc_slim,
        tier1,
    )
    assert len(result) == 3
