"""Tests for build_page.py: data.json generation and changelog HTML."""

from __future__ import annotations

import csv
import io
import json
import pathlib
import sys
import unittest.mock

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
# fetch_ons_geometry
# ---------------------------------------------------------------------------

_ONS_E20_FEATURE = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[0.01, 51.5], [0.02, 51.5], [0.02, 51.51], [0.01, 51.5]]],
    },
    "properties": {"PostDist": "E20", "GlobalID": "some-guid"},
}

_ONS_RESPONSE_FOUND = json.dumps(
    {"type": "FeatureCollection", "features": [_ONS_E20_FEATURE]}
).encode()

_ONS_RESPONSE_EMPTY = json.dumps({"type": "FeatureCollection", "features": []}).encode()


def _mock_urlopen(payload: bytes):
    """Return a context-manager mock that yields a file-like with *payload*."""
    cm = unittest.mock.MagicMock()
    cm.__enter__ = unittest.mock.Mock(return_value=io.BytesIO(payload))
    cm.__exit__ = unittest.mock.Mock(return_value=False)
    return unittest.mock.Mock(return_value=cm)


def test_fetch_ons_geometry_returns_feature_when_found() -> None:
    mock = _mock_urlopen(_ONS_RESPONSE_FOUND)
    with unittest.mock.patch("urllib.request.urlopen", mock):
        result = build_page.fetch_ons_geometry("E20")
    assert result is not None
    assert result["type"] == "Feature"
    assert result["geometry"]["type"] == "Polygon"  # type: ignore[index]


def test_fetch_ons_geometry_sets_postdist_property() -> None:
    mock = _mock_urlopen(_ONS_RESPONSE_FOUND)
    with unittest.mock.patch("urllib.request.urlopen", mock):
        result = build_page.fetch_ons_geometry("E20")
    assert result is not None
    props = result["properties"]
    assert isinstance(props, dict)
    assert props["PostDist"] == "E20"


def test_fetch_ons_geometry_returns_none_when_no_features() -> None:
    mock = _mock_urlopen(_ONS_RESPONSE_EMPTY)
    with unittest.mock.patch("urllib.request.urlopen", mock):
        result = build_page.fetch_ons_geometry("ZZ99")
    assert result is None


def test_fetch_ons_geometry_returns_none_on_network_error() -> None:
    with unittest.mock.patch(
        "urllib.request.urlopen", side_effect=OSError("network error")
    ):
        result = build_page.fetch_ons_geometry("E20")
    assert result is None


def test_fetch_ons_geometry_returns_none_on_bad_json() -> None:
    with unittest.mock.patch("urllib.request.urlopen", _mock_urlopen(b"not-json")):
        result = build_page.fetch_ons_geometry("E20")
    assert result is None
