"""Tests for build_page.py: data.json generation and changelog HTML."""

from __future__ import annotations

import csv
import pathlib
import sys

# build_page.py lives in scripts/ — add it to sys.path so we can import it.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "scripts"))
import build_page  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(path: pathlib.Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# build_data_json
# ---------------------------------------------------------------------------


def test_build_data_json_returns_datasets_list(tmp_path: pathlib.Path) -> None:
    district_csv = tmp_path / "price_per_sqm_postcode_district.csv"
    lsoa_csv = tmp_path / "price_per_sqm_lsoa.csv"
    _write_csv(
        district_csv,
        [
            {
                "postcode_district": "SW1A",
                "num_sales": "100",
                "total_floor_area": "10000.0",
                "total_price": "50000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )
    _write_csv(
        lsoa_csv,
        [
            {
                "LSOA21CD": "E01000001",
                "num_sales": "50",
                "total_floor_area": "5000.0",
                "total_price": "25000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )

    result = build_page.build_data_json(tmp_path, version="v0.1.0")

    assert isinstance(result, dict)
    assert "datasets" in result
    assert len(result["datasets"]) == 2


def test_build_data_json_dataset_has_required_fields(tmp_path: pathlib.Path) -> None:
    district_csv = tmp_path / "price_per_sqm_postcode_district.csv"
    lsoa_csv = tmp_path / "price_per_sqm_lsoa.csv"
    _write_csv(
        district_csv,
        [
            {
                "postcode_district": "SW1A",
                "num_sales": "100",
                "total_floor_area": "10000.0",
                "total_price": "50000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )
    _write_csv(
        lsoa_csv,
        [
            {
                "LSOA21CD": "E01000001",
                "num_sales": "50",
                "total_floor_area": "5000.0",
                "total_price": "25000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )

    result = build_page.build_data_json(tmp_path, version="v0.1.0")
    dataset = result["datasets"][0]

    for field in ("name", "description", "filename", "rows", "size_bytes", "schema"):
        assert field in dataset, f"Missing field: {field}"


def test_build_data_json_row_counts_correct(tmp_path: pathlib.Path) -> None:
    district_csv = tmp_path / "price_per_sqm_postcode_district.csv"
    lsoa_csv = tmp_path / "price_per_sqm_lsoa.csv"
    district_rows = [
        {
            "postcode_district": f"SW{i}",
            "num_sales": "10",
            "total_floor_area": "1000.0",
            "total_price": "5000000.0",
            "price_per_sqm": "5000",
            "adj_price_per_sqm": "5200",
        }
        for i in range(5)
    ]
    _write_csv(district_csv, district_rows)
    lsoa_rows = [
        {
            "LSOA21CD": f"E0100000{i}",
            "num_sales": "10",
            "total_floor_area": "1000.0",
            "total_price": "5000000.0",
            "price_per_sqm": "5000",
            "adj_price_per_sqm": "5200",
        }
        for i in range(3)
    ]
    _write_csv(lsoa_csv, lsoa_rows)

    result = build_page.build_data_json(tmp_path, version="v0.1.0")

    district_ds = next(
        d for d in result["datasets"] if "postcode_district" in d["filename"]
    )
    lsoa_ds = next(d for d in result["datasets"] if "lsoa" in d["filename"])

    assert district_ds["rows"] == 5
    assert lsoa_ds["rows"] == 3


def test_build_data_json_schema_columns_documented(tmp_path: pathlib.Path) -> None:
    district_csv = tmp_path / "price_per_sqm_postcode_district.csv"
    lsoa_csv = tmp_path / "price_per_sqm_lsoa.csv"
    _write_csv(
        district_csv,
        [
            {
                "postcode_district": "SW1A",
                "num_sales": "100",
                "total_floor_area": "10000.0",
                "total_price": "50000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )
    _write_csv(
        lsoa_csv,
        [
            {
                "LSOA21CD": "E01000001",
                "num_sales": "50",
                "total_floor_area": "5000.0",
                "total_price": "25000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )

    result = build_page.build_data_json(tmp_path, version="v0.1.0")

    for dataset in result["datasets"]:
        columns = {col["name"] for col in dataset["schema"]}
        for expected in ("num_sales", "price_per_sqm", "adj_price_per_sqm"):
            assert expected in columns, (
                f"{expected} missing from schema of {dataset['filename']}"
            )
        # Every column entry must have a description
        for col in dataset["schema"]:
            assert "description" in col, f"Column {col['name']} has no description"


def test_build_data_json_size_bytes_nonzero(tmp_path: pathlib.Path) -> None:
    district_csv = tmp_path / "price_per_sqm_postcode_district.csv"
    lsoa_csv = tmp_path / "price_per_sqm_lsoa.csv"
    _write_csv(
        district_csv,
        [
            {
                "postcode_district": "SW1A",
                "num_sales": "100",
                "total_floor_area": "10000.0",
                "total_price": "50000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )
    _write_csv(
        lsoa_csv,
        [
            {
                "LSOA21CD": "E01000001",
                "num_sales": "50",
                "total_floor_area": "5000.0",
                "total_price": "25000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )

    result = build_page.build_data_json(tmp_path, version="v0.1.0")

    for dataset in result["datasets"]:
        assert dataset["size_bytes"] > 0


def test_build_data_json_version_included(tmp_path: pathlib.Path) -> None:
    district_csv = tmp_path / "price_per_sqm_postcode_district.csv"
    lsoa_csv = tmp_path / "price_per_sqm_lsoa.csv"
    _write_csv(
        district_csv,
        [
            {
                "postcode_district": "SW1A",
                "num_sales": "100",
                "total_floor_area": "10000.0",
                "total_price": "50000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )
    _write_csv(
        lsoa_csv,
        [
            {
                "LSOA21CD": "E01000001",
                "num_sales": "50",
                "total_floor_area": "5000.0",
                "total_price": "25000000.0",
                "price_per_sqm": "5000",
                "adj_price_per_sqm": "5200",
            }
        ],
    )

    result = build_page.build_data_json(tmp_path, version="v1.2.3")

    assert result.get("version") == "v1.2.3"


# ---------------------------------------------------------------------------
# load_metadata (issue #89)
# ---------------------------------------------------------------------------


def test_load_metadata_returns_min_max_dates(tmp_path: pathlib.Path) -> None:
    """load_metadata reads min_sale_date and max_sale_date from metadata.json."""
    import json

    (tmp_path / "metadata.json").write_text(
        json.dumps({"min_sale_date": "1995-01-01", "max_sale_date": "2026-03-01"})
    )
    meta = build_page.load_metadata(tmp_path)
    assert meta["min_sale_date"] == "1995-01-01"
    assert meta["max_sale_date"] == "2026-03-01"


def test_load_metadata_missing_file_returns_empty_dict(tmp_path: pathlib.Path) -> None:
    """load_metadata returns {} when metadata.json does not exist."""
    meta = build_page.load_metadata(tmp_path)
    assert meta == {}


def test_compute_stats_date_range_uses_metadata_dates(tmp_path: pathlib.Path) -> None:
    """compute_stats must build date_range from metadata min/max, not hardcoded text."""
    import json

    (tmp_path / "metadata.json").write_text(
        json.dumps({"min_sale_date": "1995-04-01", "max_sale_date": "2026-02-01"})
    )
    meta = build_page.load_metadata(tmp_path)
    price_data = {
        "SW1A": {"price_per_sqm": 5000, "adj_price_per_sqm": 5200, "num_sales": 50}
    }
    stats = build_page.compute_stats(price_data, meta)
    assert stats["date_range"] == "Apr 1995–Feb 2026"


def test_compute_stats_date_range_fallback_when_no_metadata(
    tmp_path: pathlib.Path,
) -> None:
    """compute_stats falls back gracefully when metadata is empty."""
    price_data = {
        "SW1A": {"price_per_sqm": 5000, "adj_price_per_sqm": 5200, "num_sales": 50}
    }
    stats = build_page.compute_stats(price_data, {})
    assert stats["date_range"] == ""


# ---------------------------------------------------------------------------
# load_price_data — property_type filtering (issue #69)
# ---------------------------------------------------------------------------


def test_load_price_data_returns_only_all_rows(tmp_path: pathlib.Path) -> None:
    """load_price_data must return only ALL rollup rows, not per-type rows."""
    csv_path = tmp_path / "price_per_sqm_postcode_district.csv"
    _write_csv(
        csv_path,
        [
            {
                "postcode_district": "SW1A",
                "property_type": "ALL",
                "num_sales": 100,
                "total_price": 50000000,
                "total_floor_area": 10000,
                "price_per_sqm": 5000,
                "adj_price_per_sqm": 5200,
            },
            {
                "postcode_district": "SW1A",
                "property_type": "F",
                "num_sales": 60,
                "total_price": 30000000,
                "total_floor_area": 5000,
                "price_per_sqm": 6000,
                "adj_price_per_sqm": 6240,
            },
            {
                "postcode_district": "SW1A",
                "property_type": "T",
                "num_sales": 40,
                "total_price": 20000000,
                "total_floor_area": 5000,
                "price_per_sqm": 4000,
                "adj_price_per_sqm": 4160,
            },
        ],
    )

    import unittest.mock as mock

    with mock.patch.object(build_page, "CSV_PATH", csv_path):
        data = build_page.load_price_data()

    assert set(data.keys()) == {"SW1A"}
    assert data["SW1A"]["num_sales"] == 100
    assert data["SW1A"]["price_per_sqm"] == 5000


def test_load_price_data_no_duplicate_keys_from_type_rows(
    tmp_path: pathlib.Path,
) -> None:
    """Per-type rows must not overwrite ALL row in the returned dict."""
    csv_path = tmp_path / "price_per_sqm_postcode_district.csv"
    _write_csv(
        csv_path,
        [
            {
                "postcode_district": "E1",
                "property_type": "ALL",
                "num_sales": 200,
                "total_price": 80000000,
                "total_floor_area": 20000,
                "price_per_sqm": 4000,
                "adj_price_per_sqm": 4200,
            },
            {
                "postcode_district": "E1",
                "property_type": "F",
                "num_sales": 150,
                "total_price": 65000000,
                "total_floor_area": 13000,
                "price_per_sqm": 5000,
                "adj_price_per_sqm": 5250,
            },
        ],
    )

    import unittest.mock as mock

    with mock.patch.object(build_page, "CSV_PATH", csv_path):
        data = build_page.load_price_data()

    assert data["E1"]["num_sales"] == 200
    assert data["E1"]["price_per_sqm"] == 4000


# ---------------------------------------------------------------------------
# build_yearly_totals (issue #61)
# ---------------------------------------------------------------------------


def test_build_yearly_totals_returns_dict(tmp_path: pathlib.Path) -> None:
    """build_yearly_totals must return a dict with 'min_year' and 'districts' keys."""
    yearly_csv = tmp_path / "price_per_sqm_yearly_postcode_district.csv"
    _write_csv(
        yearly_csv,
        [
            {
                "year": 2020,
                "postcode_district": "SW1A",
                "num_sales": 15,
                "total_floor_area": 1500,
                "adj_price_per_sqm": 5000,
            }
        ],
    )
    result = build_page.build_yearly_totals(tmp_path)
    assert isinstance(result, dict)
    assert "min_year" in result
    assert "districts" in result


def test_build_yearly_totals_min_year_from_data(tmp_path: pathlib.Path) -> None:
    """min_year must equal the earliest year present in the CSV."""
    yearly_csv = tmp_path / "price_per_sqm_yearly_postcode_district.csv"
    _write_csv(
        yearly_csv,
        [
            {
                "year": 2012,
                "postcode_district": "E1",
                "num_sales": 10,
                "total_floor_area": 800,
                "adj_price_per_sqm": 3000,
            },
            {
                "year": 2020,
                "postcode_district": "E1",
                "num_sales": 20,
                "total_floor_area": 1600,
                "adj_price_per_sqm": 4000,
            },
        ],
    )
    result = build_page.build_yearly_totals(tmp_path)
    assert result["min_year"] == 2012


def test_build_yearly_totals_district_keyed(tmp_path: pathlib.Path) -> None:
    """districts must be keyed by postcode district string."""
    yearly_csv = tmp_path / "price_per_sqm_yearly_postcode_district.csv"
    _write_csv(
        yearly_csv,
        [
            {
                "year": 2020,
                "postcode_district": "SW1A",
                "num_sales": 15,
                "total_floor_area": 1500,
                "adj_price_per_sqm": 5000,
            }
        ],
    )
    result = build_page.build_yearly_totals(tmp_path)
    assert "SW1A" in result["districts"]


def test_build_yearly_totals_year_keyed_within_district(tmp_path: pathlib.Path) -> None:
    """Each district must contain integer-year-keyed entries."""
    yearly_csv = tmp_path / "price_per_sqm_yearly_postcode_district.csv"
    _write_csv(
        yearly_csv,
        [
            {
                "year": 2020,
                "postcode_district": "SW1A",
                "num_sales": 15,
                "total_floor_area": 1500,
                "adj_price_per_sqm": 5000,
            },
            {
                "year": 2021,
                "postcode_district": "SW1A",
                "num_sales": 20,
                "total_floor_area": 2000,
                "adj_price_per_sqm": 5200,
            },
        ],
    )
    result = build_page.build_yearly_totals(tmp_path)
    assert 2020 in result["districts"]["SW1A"]
    assert 2021 in result["districts"]["SW1A"]


def test_build_yearly_totals_entry_has_compact_keys(tmp_path: pathlib.Path) -> None:
    """Each year entry must have exactly 'p', 'fa', and 'n' compact keys."""
    yearly_csv = tmp_path / "price_per_sqm_yearly_postcode_district.csv"
    _write_csv(
        yearly_csv,
        [
            {
                "year": 2020,
                "postcode_district": "SW1A",
                "num_sales": 15,
                "total_floor_area": 1500,
                "adj_price_per_sqm": 5000,
            }
        ],
    )
    result = build_page.build_yearly_totals(tmp_path)
    entry = result["districts"]["SW1A"][2020]
    assert entry == {"p": 5000, "fa": 1500, "n": 15}


def test_build_yearly_totals_missing_file_returns_empty(tmp_path: pathlib.Path) -> None:
    """build_yearly_totals must return {} when the CSV does not exist."""
    result = build_page.build_yearly_totals(tmp_path)
    assert result == {}


# ---------------------------------------------------------------------------
# compute_stats — adj_price_per_sqm alignment (issue #122)
# ---------------------------------------------------------------------------


def _price_data_two_districts() -> dict[str, dict]:
    """Two districts where nominal and adj rankings diverge.

    District A: nominal=3000, adj=6000 (high adj — old sale, big CPI lift)
    District B: nominal=5000, adj=2000 (low adj — recent sale, low CPI lift)
    By nominal: B > A.  By adj: A > B.
    """
    return {
        "SW1A": {
            "price_per_sqm": 3000,
            "adj_price_per_sqm": 6000,
            "num_sales": 50,
        },
        "E1": {
            "price_per_sqm": 5000,
            "adj_price_per_sqm": 2000,
            "num_sales": 50,
        },
    }


def test_compute_stats_median_uses_adj_price_per_sqm() -> None:
    """median_price_per_sqm must use adj_price_per_sqm, not price_per_sqm."""
    price_data = _price_data_two_districts()
    # Add a third district so adj and nominal medians diverge
    price_data["N1"] = {
        "price_per_sqm": 4000,
        "adj_price_per_sqm": 1000,
        "num_sales": 50,
    }
    stats = build_page.compute_stats(price_data, {})
    # adj values sorted: [1000, 2000, 6000] → median = 2000
    # nominal values sorted: [3000, 4000, 5000] → median = 4000
    assert stats["median_price_per_sqm"] == 2000


def test_compute_stats_top10_has_adj_price_per_sqm_key() -> None:
    """top10 entries must expose 'adj_price_per_sqm', not 'price_per_sqm'."""
    price_data = _price_data_two_districts()
    stats = build_page.compute_stats(price_data, {})
    for entry in stats["top10"]:
        assert "adj_price_per_sqm" in entry, (
            f"top10 entry for {entry['district']} missing adj_price_per_sqm"
        )
        assert "price_per_sqm" not in entry, (
            f"top10 entry for {entry['district']} should not have price_per_sqm"
        )


def test_compute_stats_bottom10_has_adj_price_per_sqm_key() -> None:
    """bottom10 entries must expose 'adj_price_per_sqm', not 'price_per_sqm'."""
    price_data = _price_data_two_districts()
    stats = build_page.compute_stats(price_data, {})
    for entry in stats["bottom10"]:
        assert "adj_price_per_sqm" in entry, (
            f"bottom10 entry for {entry['district']} missing adj_price_per_sqm"
        )
        assert "price_per_sqm" not in entry, (
            f"bottom10 entry for {entry['district']} should not have price_per_sqm"
        )


def test_compute_stats_ranking_order_uses_adj_price_per_sqm() -> None:
    """top10 must be ranked by adj_price_per_sqm descending (not nominal)."""
    price_data = _price_data_two_districts()
    stats = build_page.compute_stats(price_data, {})
    # By adj: SW1A (6000) > E1 (2000) → SW1A should be first in top10
    assert stats["top10"][0]["district"] == "SW1A"
    assert stats["bottom10"][0]["district"] == "E1"


def test_compute_stats_first_non_london_has_adj_price_per_sqm_key() -> None:
    """first_non_london fact must expose 'adj_price_per_sqm', not 'price_per_sqm'."""
    # Need enough districts and a non-London one ranked highly
    price_data = {
        "SW1A": {
            "price_per_sqm": 10000,
            "adj_price_per_sqm": 10000,
            "num_sales": 50,
        },
        "OX1": {
            "price_per_sqm": 8000,
            "adj_price_per_sqm": 8000,
            "num_sales": 50,
        },
    }
    stats = build_page.compute_stats(price_data, {})
    fnl = stats["facts"]["first_non_london"]
    assert fnl is not None
    assert "adj_price_per_sqm" in fnl, "first_non_london missing adj_price_per_sqm"
    assert "price_per_sqm" not in fnl, "first_non_london should not have price_per_sqm"
