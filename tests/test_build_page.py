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
