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
