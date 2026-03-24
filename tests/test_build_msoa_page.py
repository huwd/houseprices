"""Tests for build_msoa_page.py: LSOA→MSOA aggregation and page rendering."""

from __future__ import annotations

import csv
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "scripts"))
import build_msoa_page  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(path: pathlib.Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _lsoa_rows() -> list[dict]:
    """Two LSOAs in one MSOA, one LSOA in another."""
    return [
        {
            "LSOA21CD": "E01000001",
            "num_sales": 20,
            "total_floor_area": 2000.0,
            "total_price": 6000000.0,
            "price_per_sqm": 3000,
            "adj_price_per_sqm": 3300,
        },
        {
            "LSOA21CD": "E01000002",
            "num_sales": 30,
            "total_floor_area": 3000.0,
            "total_price": 12000000.0,
            "price_per_sqm": 4000,
            "adj_price_per_sqm": 4400,
        },
        {
            "LSOA21CD": "E01000003",
            "num_sales": 10,
            "total_floor_area": 1000.0,
            "total_price": 5000000.0,
            "price_per_sqm": 5000,
            "adj_price_per_sqm": 5500,
        },
    ]


def _lookup() -> dict[str, str]:
    """LSOA21CD → MSOA21CD."""
    return {
        "E01000001": "E02000001",
        "E01000002": "E02000001",
        "E01000003": "E02000002",
    }


# ---------------------------------------------------------------------------
# load_lsoa_data
# ---------------------------------------------------------------------------


def test_load_lsoa_data_returns_dict_keyed_by_lsoa(tmp_path: pathlib.Path) -> None:
    csv_path = tmp_path / "price_per_sqm_lsoa.csv"
    _write_csv(csv_path, _lsoa_rows())
    data = build_msoa_page.load_lsoa_data(csv_path)
    assert "E01000001" in data
    assert "E01000002" in data


def test_load_lsoa_data_has_required_fields(tmp_path: pathlib.Path) -> None:
    csv_path = tmp_path / "price_per_sqm_lsoa.csv"
    _write_csv(csv_path, _lsoa_rows())
    data = build_msoa_page.load_lsoa_data(csv_path)
    row = data["E01000001"]
    for field in ("num_sales", "total_floor_area", "total_price", "adj_price_per_sqm"):
        assert field in row, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# aggregate_to_msoa
# ---------------------------------------------------------------------------


def test_aggregate_to_msoa_groups_lsoas_by_msoa() -> None:
    """Two LSOAs in E02000001 must produce one MSOA row."""
    result = build_msoa_page.aggregate_to_msoa(_lsoa_rows_as_dict(), _lookup())
    assert "E02000001" in result
    assert "E02000002" in result


def test_aggregate_to_msoa_sums_num_sales() -> None:
    result = build_msoa_page.aggregate_to_msoa(_lsoa_rows_as_dict(), _lookup())
    # E02000001: 20 + 30 = 50
    assert result["E02000001"]["num_sales"] == 50


def test_aggregate_to_msoa_price_per_sqm_is_total_divided_by_area() -> None:
    """price_per_sqm = Σtotal_price / Σtotal_floor_area (not mean of ratios)."""
    result = build_msoa_page.aggregate_to_msoa(_lsoa_rows_as_dict(), _lookup())
    # E02000001: (6_000_000 + 12_000_000) / (2000 + 3000) = 18_000_000 / 5000 = 3600
    assert result["E02000001"]["price_per_sqm"] == 3600


def test_aggregate_to_msoa_adj_price_per_sqm_is_weighted_by_floor_area() -> None:
    """adj_price_per_sqm = Σ(adj * floor_area) / Σfloor_area."""
    result = build_msoa_page.aggregate_to_msoa(_lsoa_rows_as_dict(), _lookup())
    # E02000001: (3300*2000 + 4400*3000) / 5000 = (6_600_000 + 13_200_000) / 5000 = 3960
    assert result["E02000001"]["adj_price_per_sqm"] == 3960


def test_aggregate_to_msoa_single_lsoa_msoa() -> None:
    result = build_msoa_page.aggregate_to_msoa(_lsoa_rows_as_dict(), _lookup())
    # E02000002: only E01000003 → price_per_sqm = 5000
    assert result["E02000002"]["price_per_sqm"] == 5000
    assert result["E02000002"]["adj_price_per_sqm"] == 5500


def test_aggregate_to_msoa_excludes_lsoas_not_in_lookup() -> None:
    lookup = {"E01000001": "E02000001"}  # only one LSOA mapped
    data = {
        "E01000001": {
            "num_sales": 20,
            "total_floor_area": 2000.0,
            "total_price": 6000000.0,
            "adj_price_per_sqm": 3300,
        },
        "E01000099": {  # not in lookup
            "num_sales": 5,
            "total_floor_area": 500.0,
            "total_price": 1000000.0,
            "adj_price_per_sqm": 2000,
        },
    }
    result = build_msoa_page.aggregate_to_msoa(data, lookup)
    assert "E02000001" in result
    # E01000099 has no MSOA — should not create a mystery key
    assert len(result) == 1


def test_aggregate_to_msoa_excludes_below_min_sales() -> None:
    """MSOAs with fewer than 10 sales must be excluded."""
    data = {
        "E01000001": {
            "num_sales": 5,
            "total_floor_area": 500.0,
            "total_price": 1500000.0,
            "adj_price_per_sqm": 3000,
        },
    }
    lookup = {"E01000001": "E02000001"}
    result = build_msoa_page.aggregate_to_msoa(data, lookup, min_sales=10)
    assert "E02000001" not in result


def test_aggregate_to_msoa_min_sales_default_is_10() -> None:
    """Default min_sales threshold must be 10."""
    import inspect

    sig = inspect.signature(build_msoa_page.aggregate_to_msoa)
    assert sig.parameters["min_sales"].default == 10


# ---------------------------------------------------------------------------
# compute_msoa_stats
# ---------------------------------------------------------------------------


def test_compute_msoa_stats_returns_required_keys() -> None:
    msoa_data = {
        "E02000001": {
            "num_sales": 50,
            "price_per_sqm": 3600,
            "adj_price_per_sqm": 3960,
        },
        "E02000002": {
            "num_sales": 10,
            "price_per_sqm": 5000,
            "adj_price_per_sqm": 5500,
        },
    }
    stats = build_msoa_page.compute_msoa_stats(msoa_data, metadata={})
    for key in ("median_price_per_sqm", "num_areas", "total_sales", "date_range"):
        assert key in stats, f"Missing key: {key}"


def test_compute_msoa_stats_median_uses_adj_price() -> None:
    msoa_data = {
        "E02000001": {
            "num_sales": 50,
            "price_per_sqm": 3000,
            "adj_price_per_sqm": 4000,
        },
        "E02000002": {
            "num_sales": 10,
            "price_per_sqm": 5000,
            "adj_price_per_sqm": 2000,
        },
        "E02000003": {
            "num_sales": 20,
            "price_per_sqm": 4000,
            "adj_price_per_sqm": 1000,
        },
    }
    stats = build_msoa_page.compute_msoa_stats(msoa_data, metadata={})
    # adj sorted: [1000, 2000, 4000] → median = 2000
    assert stats["median_price_per_sqm"] == 2000


def test_compute_msoa_stats_top10_ranked_by_adj() -> None:
    msoa_data = {
        f"E020000{i:02d}": {
            "num_sales": 20,
            "price_per_sqm": i * 1000,
            "adj_price_per_sqm": (15 - i) * 1000,  # inverted — adj differs from nominal
        }
        for i in range(1, 13)
    }
    stats = build_msoa_page.compute_msoa_stats(msoa_data, metadata={})
    # adj rankings are inverted vs nominal — verify adj is used
    top_adj = sorted(
        msoa_data.items(), key=lambda kv: kv[1]["adj_price_per_sqm"], reverse=True
    )
    assert stats["top10"][0]["msoa"] == top_adj[0][0]


def test_compute_msoa_stats_top10_has_adj_key() -> None:
    msoa_data = {
        "E02000001": {
            "num_sales": 50,
            "price_per_sqm": 3600,
            "adj_price_per_sqm": 3960,
        },
    }
    stats = build_msoa_page.compute_msoa_stats(msoa_data, metadata={})
    for entry in stats["top10"]:
        assert "adj_price_per_sqm" in entry
        assert "price_per_sqm" not in entry


# ---------------------------------------------------------------------------
# write_msoa_csv
# ---------------------------------------------------------------------------


def test_write_msoa_csv_creates_file(tmp_path: pathlib.Path) -> None:
    msoa_data = {
        "E02000001": {
            "num_sales": 50,
            "total_floor_area": 5000.0,
            "total_price": 18000000.0,
            "price_per_sqm": 3600,
            "adj_price_per_sqm": 3960,
        },
    }
    msoa_names = {"E02000001": "City of London 001"}
    out = tmp_path / "price_per_sqm_msoa.csv"
    build_msoa_page.write_msoa_csv(msoa_data, msoa_names, out)
    assert out.exists()


def test_write_msoa_csv_headers(tmp_path: pathlib.Path) -> None:
    msoa_data = {
        "E02000001": {
            "num_sales": 50,
            "total_floor_area": 5000.0,
            "total_price": 18000000.0,
            "price_per_sqm": 3600,
            "adj_price_per_sqm": 3960,
        },
    }
    out = tmp_path / "price_per_sqm_msoa.csv"
    build_msoa_page.write_msoa_csv(msoa_data, {}, out)
    with open(out) as f:
        headers = csv.DictReader(f).fieldnames
    assert headers == [
        "msoa21cd",
        "msoa21nm",
        "num_sales",
        "total_floor_area",
        "total_price",
        "price_per_sqm",
        "adj_price_per_sqm",
    ]


def test_write_msoa_csv_row_values(tmp_path: pathlib.Path) -> None:
    msoa_data = {
        "E02000001": {
            "num_sales": 50,
            "total_floor_area": 5000.0,
            "total_price": 18000000.0,
            "price_per_sqm": 3600,
            "adj_price_per_sqm": 3960,
        },
    }
    msoa_names = {"E02000001": "City of London 001"}
    out = tmp_path / "price_per_sqm_msoa.csv"
    build_msoa_page.write_msoa_csv(msoa_data, msoa_names, out)
    with open(out) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert row["msoa21cd"] == "E02000001"
    assert row["msoa21nm"] == "City of London 001"
    assert int(row["num_sales"]) == 50
    assert int(row["price_per_sqm"]) == 3600
    assert int(row["adj_price_per_sqm"]) == 3960


def test_write_msoa_csv_missing_name_falls_back_to_code(tmp_path: pathlib.Path) -> None:
    msoa_data = {
        "E02000001": {
            "num_sales": 50,
            "total_floor_area": 5000.0,
            "total_price": 18000000.0,
            "price_per_sqm": 3600,
            "adj_price_per_sqm": 3960,
        },
    }
    out = tmp_path / "price_per_sqm_msoa.csv"
    build_msoa_page.write_msoa_csv(msoa_data, {}, out)
    with open(out) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["msoa21nm"] == "E02000001"


def test_write_msoa_csv_sorted_by_code(tmp_path: pathlib.Path) -> None:
    msoa_data = {
        "E02000002": {
            "num_sales": 10,
            "total_floor_area": 1000.0,
            "total_price": 5000000.0,
            "price_per_sqm": 5000,
            "adj_price_per_sqm": 5500,
        },
        "E02000001": {
            "num_sales": 50,
            "total_floor_area": 5000.0,
            "total_price": 18000000.0,
            "price_per_sqm": 3600,
            "adj_price_per_sqm": 3960,
        },
    }
    out = tmp_path / "price_per_sqm_msoa.csv"
    build_msoa_page.write_msoa_csv(msoa_data, {}, out)
    with open(out) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["msoa21cd"] == "E02000001"
    assert rows[1]["msoa21cd"] == "E02000002"


# ---------------------------------------------------------------------------
# Helpers for tests above
# ---------------------------------------------------------------------------


def _lsoa_rows_as_dict() -> dict:
    return {
        row["LSOA21CD"]: {
            "num_sales": int(row["num_sales"]),
            "total_floor_area": float(row["total_floor_area"]),
            "total_price": float(row["total_price"]),
            "adj_price_per_sqm": int(row["adj_price_per_sqm"]),
        }
        for row in _lsoa_rows()
    }
