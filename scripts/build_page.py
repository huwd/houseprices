#!/usr/bin/env python3
"""Build output/index.html from pipeline outputs and cached boundary GeoJSON.

Run the pipeline first, then fetch boundaries, then run this:
    uv run python src/houseprices/pipeline.py
    uv run scripts/fetch_boundaries.py
    uv run python scripts/build_page.py

Or via Makefile:
    make boundaries   # fetch + cache postcode district polygons
    make page         # build output/index.html
"""

import csv
import json
import pathlib
import statistics
import sys

ROOT = pathlib.Path(__file__).parent.parent
OUTPUT = ROOT / "output"
DATA = ROOT / "data"
SCRIPTS = ROOT / "scripts"

BOUNDARIES_PATH = DATA / "postcode_districts.geojson"
CSV_PATH = OUTPUT / "price_per_sqm_postcode_district.csv"
TEMPLATE_PATH = SCRIPTS / "page_template.html"
OUT_HTML = OUTPUT / "index.html"

MIN_SALES_FOR_RANKING = 20  # exclude very thin districts from top/bottom tables


def load_price_data() -> dict[str, dict]:
    data: dict[str, dict] = {}
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            data[row["postcode_district"]] = {
                "price_per_sqm": int(row["price_per_sqm"]),
                "num_sales": int(row["num_sales"]),
            }
    return data


def compute_stats(price_data: dict[str, dict]) -> dict:
    prices = sorted(r["price_per_sqm"] for r in price_data.values())
    median = int(statistics.median(prices))
    total_sales = sum(r["num_sales"] for r in price_data.values())

    ranked = [
        {"district": d, **v}
        for d, v in price_data.items()
        if v["num_sales"] >= MIN_SALES_FOR_RANKING
    ]
    ranked.sort(key=lambda r: r["price_per_sqm"])

    return {
        "median_price_per_sqm": median,
        "num_districts": len(price_data),
        "date_range": "Aug 2007–Jan 2026",
        "total_sales": total_sales,
        "top10": [
            {"district": r["district"], "price_per_sqm": r["price_per_sqm"]}
            for r in ranked[-10:][::-1]
        ],
        "bottom10": [
            {"district": r["district"], "price_per_sqm": r["price_per_sqm"]}
            for r in ranked[:10]
        ],
    }


def build_geojson(boundaries: dict, price_data: dict[str, dict]) -> dict:
    """Join price data into the boundary GeoJSON features."""
    matched = 0
    for feature in boundaries["features"]:
        dist = feature["properties"].get("PostDist")
        if dist and dist in price_data:
            feature["properties"].update(price_data[dist])
            matched += 1
    print(
        f"  Joined {matched} / {len(boundaries['features'])} boundary features "
        f"to {len(price_data)} price records"
    )
    return boundaries


def main() -> None:
    # Validate inputs
    missing = [p for p in (BOUNDARIES_PATH, CSV_PATH, TEMPLATE_PATH) if not p.exists()]
    if missing:
        for p in missing:
            print(f"Missing: {p}", file=sys.stderr)
        if BOUNDARIES_PATH in missing:
            print(
                "  Run: uv run scripts/fetch_boundaries.py", file=sys.stderr
            )
        if CSV_PATH in missing:
            print(
                "  Run the pipeline first: uv run python src/houseprices/pipeline.py",
                file=sys.stderr,
            )
        sys.exit(1)

    print("Loading data…")
    boundaries = json.loads(BOUNDARIES_PATH.read_text())
    price_data = load_price_data()
    print(
        f"  {len(boundaries['features'])} boundary features, "
        f"{len(price_data)} price records"
    )

    print("Joining…")
    geojson = build_geojson(boundaries, price_data)
    stats = compute_stats(price_data)

    print("Rendering…")
    template = TEMPLATE_PATH.read_text()
    html = template.replace(
        "__GEOJSON__", json.dumps(geojson, separators=(",", ":"))
    ).replace(
        "__STATS__", json.dumps(stats, separators=(",", ":"))
    )

    OUT_HTML.write_text(html)
    size_kb = OUT_HTML.stat().st_size // 1024
    print(f"Written → {OUT_HTML} ({size_kb:,} KB)")
    print(f"  Median: £{stats['median_price_per_sqm']:,}/m²")
    print(f"  Districts: {stats['num_districts']:,}")
    print(f"  Sales: {stats['total_sales']:,}")


if __name__ == "__main__":
    main()
