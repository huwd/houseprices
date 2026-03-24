#!/usr/bin/env python3
"""Build output/msoa.html from LSOA pipeline outputs and ONS MSOA boundaries.

Run after the main pipeline has produced price_per_sqm_lsoa.csv:
    uv run python scripts/build_msoa_page.py

Or via Makefile:
    make page   (builds both index.html and msoa.html)

MSOA data is derived entirely from the existing LSOA output — no pipeline
re-run is needed.  The LSOA→MSOA lookup and MSOA boundary GeoJSON are
fetched from the ONS Open Geography Portal and cached in data/.

Coverage caveat: LSOA-level data is tier-1 UPRN-matched only (~60 % of
all sales, covering 1995–2022).  Post-2022 address-normalised matches are
not included.  The page carries a prominent caveat to this effect.
"""

from __future__ import annotations

import csv
import json
import pathlib
import shutil
import statistics
import sys
import urllib.request

ROOT = pathlib.Path(__file__).parent.parent
OUTPUT = ROOT / "output"
DATA = ROOT / "data"
SCRIPTS = ROOT / "scripts"

LSOA_CSV_PATH = OUTPUT / "price_per_sqm_lsoa.csv"
MSOA_BOUNDARIES_PATH = DATA / "msoa_boundaries.geojson"
LSOA_MSOA_LOOKUP_PATH = DATA / "lsoa_msoa_lookup.csv"

TEMPLATE_PATH = SCRIPTS / "msoa_template.html"
CSS_PATH = SCRIPTS / "page.css"
SHARED_JS_PATH = SCRIPTS / "shared.js"
MSOA_JS_PATH = SCRIPTS / "msoa.js"
VERSION_PATH = OUTPUT / "VERSION.txt"
CHANGELOG_PATH = OUTPUT / "CHANGELOG.md"

OUT_HTML = OUTPUT / "msoa.html"
OUT_GEOJSON = OUTPUT / "msoa_areas.geojson"
OUT_MSOA_CSV = OUTPUT / "price_per_sqm_msoa.csv"
OUT_CSS = OUTPUT / "page.css"
OUT_SHARED_JS = OUTPUT / "shared.js"
OUT_MSOA_JS = OUTPUT / "msoa.js"

MIN_SALES = 10
MIN_SALES_FOR_RANKING = 20

# ONS Open Geography Portal — MSOA 2021 boundaries (England & Wales), BSC tier.
# BSC (Super Generalised Clipped) is designed for display at scales above
# 1:500,000 — appropriate for a national choropleth.  Produces a smaller file
# than BGC (~8 MB vs ~19 MB) at the cost of less detail at high zoom levels.
_MSOA_BOUNDARY_SERVICE = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "MSOA_2021_EW_BSC_V3_RUC/FeatureServer/0"
)
# ONS LSOA21→MSOA21 lookup (England & Wales).
# The standalone LSOA21_MSOA21_EW_LU_V2 service was retired; the lookup is
# now part of the combined OA21/LAD22 geography lookup service.
_LSOA_MSOA_LOOKUP_SERVICE = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "OA21_LAD22_LSOA21_MSOA21_LEP22_EN_LU_V2/FeatureServer/0"
)
_PAGE_SIZE = 1000  # ONS services cap at maxRecordCount=1000


def load_lsoa_data(csv_path: pathlib.Path) -> dict[str, dict]:
    """Load price_per_sqm_lsoa.csv into a dict keyed by LSOA21CD."""
    data: dict[str, dict] = {}
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            data[row["LSOA21CD"]] = {
                "num_sales": int(row["num_sales"]),
                "total_floor_area": float(row["total_floor_area"]),
                "total_price": float(row["total_price"]),
                "adj_price_per_sqm": int(row["adj_price_per_sqm"]),
            }
    return data


def aggregate_to_msoa(
    lsoa_data: dict[str, dict],
    lsoa_msoa_lookup: dict[str, str],
    min_sales: int = 10,
) -> dict[str, dict]:
    """Aggregate LSOA-level data to MSOA level.

    Groups LSOAs by their MSOA, summing total_price, total_floor_area, and
    num_sales.  price_per_sqm = Σtotal_price / Σtotal_floor_area (not a mean
    of ratios).  adj_price_per_sqm is a floor-area-weighted average of the
    per-LSOA adj values.  MSOAs with fewer than min_sales are excluded.
    """
    totals: dict[str, dict] = {}
    for lsoa_code, row in lsoa_data.items():
        msoa_code = lsoa_msoa_lookup.get(lsoa_code)
        if not msoa_code:
            continue
        if msoa_code not in totals:
            totals[msoa_code] = {
                "num_sales": 0,
                "total_floor_area": 0.0,
                "total_price": 0.0,
                "adj_price_x_fa": 0.0,
            }
        t = totals[msoa_code]
        t["num_sales"] += row["num_sales"]
        t["total_floor_area"] += row["total_floor_area"]
        t["total_price"] += row["total_price"]
        t["adj_price_x_fa"] += row["adj_price_per_sqm"] * row["total_floor_area"]

    result: dict[str, dict] = {}
    for msoa_code, t in totals.items():
        if t["num_sales"] < min_sales:
            continue
        fa = t["total_floor_area"]
        result[msoa_code] = {
            "num_sales": t["num_sales"],
            "total_floor_area": fa,
            "total_price": t["total_price"],
            "price_per_sqm": round(t["total_price"] / fa),
            "adj_price_per_sqm": round(t["adj_price_x_fa"] / fa),
        }
    return result


def compute_msoa_stats(
    msoa_data: dict[str, dict],
    metadata: dict[str, str],
    msoa_names: dict[str, str] | None = None,
) -> dict:
    """Compute summary statistics for the MSOA page STATS object."""
    import datetime

    adj_prices = sorted(r["adj_price_per_sqm"] for r in msoa_data.values())
    median = int(statistics.median(adj_prices))
    total_sales = sum(r["num_sales"] for r in msoa_data.values())

    ranked = [
        {"msoa": code, **v}
        for code, v in msoa_data.items()
        if v["num_sales"] >= MIN_SALES_FOR_RANKING
    ]
    ranked.sort(key=lambda r: r["adj_price_per_sqm"])
    ranked_desc = ranked[::-1]

    if metadata.get("min_sale_date") and metadata.get("max_sale_date"):
        min_d = datetime.date.fromisoformat(metadata["min_sale_date"])
        max_d = datetime.date.fromisoformat(metadata["max_sale_date"])
        date_range = f"{min_d.strftime('%b %Y')}–{max_d.strftime('%b %Y')}"
    else:
        date_range = ""

    return {
        "median_price_per_sqm": median,
        "num_areas": len(msoa_data),
        "total_sales": total_sales,
        "date_range": date_range,
        "cpi_base": "January 2026",
        "top10": [
            {
                "msoa": r["msoa"],
                "name": (msoa_names or {}).get(r["msoa"], r["msoa"]),
                "adj_price_per_sqm": r["adj_price_per_sqm"],
            }
            for r in ranked_desc[:10]
        ],
        "bottom10": [
            {
                "msoa": r["msoa"],
                "name": (msoa_names or {}).get(r["msoa"], r["msoa"]),
                "adj_price_per_sqm": r["adj_price_per_sqm"],
            }
            for r in ranked[:10]
        ],
    }


def _fetch_paginated(
    service_url: str,
    fields: list[str],
    extra_params: list[str] | None = None,
) -> list[dict]:
    """Fetch all records from an ArcGIS FeatureServer via offset pagination."""
    records = []
    offset = 0
    base_params = [
        "where=1%3D1",
        f"outFields={','.join(fields)}",
    ] + (extra_params or ["f=json"])
    while True:
        params = "&".join(
            base_params
            + [
                f"resultOffset={offset}",
                f"resultRecordCount={_PAGE_SIZE}",
            ]
        )
        url = f"{service_url}/query?{params}"
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = json.loads(resp.read())
        batch = data.get("features", [])
        records.extend(batch)
        if not batch:
            break
        offset += len(batch)
    return records


def fetch_lsoa_msoa_lookup(cache_path: pathlib.Path) -> dict[str, str]:
    """Return LSOA21CD→MSOA21CD lookup, fetching from ONS if not cached."""
    if cache_path.exists():
        lookup: dict[str, str] = {}
        with open(cache_path) as f:
            for row in csv.DictReader(f):
                lookup[row["LSOA21CD"]] = row["MSOA21CD"]
        return lookup

    print("  Fetching LSOA→MSOA lookup from ONS…")
    records = _fetch_paginated(_LSOA_MSOA_LOOKUP_SERVICE, ["LSOA21CD", "MSOA21CD"])
    lookup = {r["attributes"]["LSOA21CD"]: r["attributes"]["MSOA21CD"] for r in records}

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["LSOA21CD", "MSOA21CD"])
        writer.writeheader()
        for lsoa, msoa in sorted(lookup.items()):
            writer.writerow({"LSOA21CD": lsoa, "MSOA21CD": msoa})
    print(f"  Cached → {cache_path} ({len(lookup):,} rows)")
    return lookup


def fetch_msoa_boundaries(cache_path: pathlib.Path) -> dict:
    """Return MSOA boundary GeoJSON, fetching from ONS if not cached."""
    if cache_path.exists():
        return json.loads(cache_path.read_text())

    print("  Fetching MSOA boundaries from ONS…")
    # f=geojson returns valid GeoJSON geometry (WGS84) rather than the ArcGIS
    # rings format returned by f=json.  outSR=4326 ensures WGS84 output.
    # geometryPrecision=5 truncates coordinates to 5 decimal places (~1 m),
    # keeping the file well under Cloudflare Pages' 25 MB file size limit.
    records = _fetch_paginated(
        _MSOA_BOUNDARY_SERVICE,
        ["MSOA21CD", "MSOA21NM", "LAT", "LONG"],
        extra_params=["f=geojson", "outSR=4326", "geometryPrecision=4"],
    )
    features = [
        {
            "type": "Feature",
            "properties": {
                "MSOA21CD": r["properties"]["MSOA21CD"],
                "MSOA21NM": r["properties"]["MSOA21NM"],
                "LAT": r["properties"]["LAT"],
                "LONG": r["properties"]["LONG"],
            },
            "geometry": r.get("geometry"),
        }
        for r in records
    ]
    geojson = {"type": "FeatureCollection", "features": features}

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(geojson, separators=(",", ":")))
    print(f"  Cached → {cache_path} ({len(features):,} features)")
    return geojson


def build_msoa_geojson(
    boundaries: dict, msoa_data: dict[str, dict], msoa_names: dict[str, str]
) -> dict:
    """Join MSOA price data into boundary GeoJSON features."""
    matched = 0
    for feature in boundaries["features"]:
        code = feature["properties"].get("MSOA21CD")
        if code and code in msoa_data:
            feature["properties"].update(msoa_data[code])
            matched += 1
    print(
        f"  Joined {matched} / {len(boundaries['features'])} boundary features "
        f"to {len(msoa_data)} MSOA price records"
    )
    return boundaries


def write_msoa_csv(
    msoa_data: dict[str, dict],
    msoa_names: dict[str, str],
    out_path: pathlib.Path,
) -> None:
    """Write price_per_sqm_msoa.csv sorted by MSOA code."""
    fieldnames = [
        "msoa21cd",
        "msoa21nm",
        "num_sales",
        "total_floor_area",
        "total_price",
        "price_per_sqm",
        "adj_price_per_sqm",
    ]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for code in sorted(msoa_data):
            row = msoa_data[code]
            writer.writerow(
                {
                    "msoa21cd": code,
                    "msoa21nm": msoa_names.get(code, code),
                    "num_sales": row["num_sales"],
                    "total_floor_area": row["total_floor_area"],
                    "total_price": row["total_price"],
                    "price_per_sqm": row["price_per_sqm"],
                    "adj_price_per_sqm": row["adj_price_per_sqm"],
                }
            )


def load_version() -> str:
    if not VERSION_PATH.exists():
        return ""
    return "v" + VERSION_PATH.read_text().strip()


def load_metadata() -> dict[str, str]:
    path = OUTPUT / "metadata.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text())  # type: ignore[no-any-return]


def load_changelog_html() -> str:
    """Reuse the changelog-to-HTML converter from build_page."""
    sys.path.insert(0, str(SCRIPTS))
    from build_page import changelog_to_html  # type: ignore[import]

    if not CHANGELOG_PATH.exists():
        return ""
    return changelog_to_html(CHANGELOG_PATH.read_text())


def main() -> None:
    missing = [p for p in (LSOA_CSV_PATH, TEMPLATE_PATH) if not p.exists()]
    if missing:
        for p in missing:
            print(f"Missing: {p}", file=sys.stderr)
        if LSOA_CSV_PATH in missing:
            print(
                "  Run the pipeline first: uv run python src/houseprices/pipeline.py",
                file=sys.stderr,
            )
        sys.exit(1)

    version = load_version()
    metadata = load_metadata()

    print("Loading LSOA data…")
    lsoa_data = load_lsoa_data(LSOA_CSV_PATH)
    print(f"  {len(lsoa_data):,} LSOAs loaded")

    print("Loading LSOA→MSOA lookup…")
    lookup = fetch_lsoa_msoa_lookup(LSOA_MSOA_LOOKUP_PATH)

    print("Aggregating to MSOA…")
    msoa_data = aggregate_to_msoa(lsoa_data, lookup, min_sales=MIN_SALES)
    print(f"  {len(msoa_data):,} MSOAs with ≥{MIN_SALES} sales")

    print("Loading MSOA boundaries…")
    boundaries = fetch_msoa_boundaries(MSOA_BOUNDARIES_PATH)
    msoa_names = {
        f["properties"]["MSOA21CD"]: f["properties"]["MSOA21NM"]
        for f in boundaries["features"]
    }

    print("Joining…")
    geojson = build_msoa_geojson(boundaries, msoa_data, msoa_names)
    stats = compute_msoa_stats(msoa_data, metadata, msoa_names)

    print("Writing CSV…")
    write_msoa_csv(msoa_data, msoa_names, OUT_MSOA_CSV)
    csv_kb = OUT_MSOA_CSV.stat().st_size // 1024
    print(f"  Written → {OUT_MSOA_CSV} ({csv_kb:,} KB)")

    print("Writing GeoJSON…")
    OUT_GEOJSON.write_text(json.dumps(geojson, separators=(",", ":")))
    geojson_kb = OUT_GEOJSON.stat().st_size // 1024
    print(f"  Written → {OUT_GEOJSON} ({geojson_kb:,} KB)")

    print("Rendering…")
    import re

    changelog_html = load_changelog_html()
    data_date = ""
    if CHANGELOG_PATH.exists():
        pattern = re.compile(
            r"^## \[\d+\.\d+\.\d+\] — (\d{4}-\d{2}-\d{2})", re.MULTILINE
        )
        m = pattern.search(CHANGELOG_PATH.read_text())
        if m:
            import datetime

            data_date = datetime.date.fromisoformat(m.group(1)).strftime("%b %Y")

    template = TEMPLATE_PATH.read_text()
    rendered = (
        template.replace("__STATS__", json.dumps(stats, separators=(",", ":")))
        .replace("__VERSION__", version)
        .replace("__DATA_DATE__", data_date)
        .replace("__NUM_AREAS__", f"{stats['num_areas']:,}")
        .replace("__CHANGELOG_HTML__", changelog_html)
    )

    OUT_HTML.write_text(rendered)
    size_kb = OUT_HTML.stat().st_size // 1024
    print(f"  Written → {OUT_HTML} ({size_kb:,} KB)")

    shutil.copy2(CSS_PATH, OUT_CSS)
    shutil.copy2(SHARED_JS_PATH, OUT_SHARED_JS)
    shutil.copy2(MSOA_JS_PATH, OUT_MSOA_JS)
    print(f"  Copied  → {OUT_CSS}")
    print(f"  Copied  → {OUT_SHARED_JS}")
    print(f"  Copied  → {OUT_MSOA_JS}")

    print(f"  Median: £{stats['median_price_per_sqm']:,}/m²")
    print(f"  Areas:  {stats['num_areas']:,}")
    print(f"  Sales:  {stats['total_sales']:,}")


if __name__ == "__main__":
    main()
