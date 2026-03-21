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
import html
import json
import pathlib
import re
import statistics
import sys

ROOT = pathlib.Path(__file__).parent.parent
OUTPUT = ROOT / "output"
DATA = ROOT / "data"
SCRIPTS = ROOT / "scripts"

BOUNDARIES_PATH = DATA / "postcode_districts.geojson"
CSV_PATH = OUTPUT / "price_per_sqm_postcode_district.csv"
TEMPLATE_PATH = SCRIPTS / "page_template.html"
VERSION_PATH = OUTPUT / "VERSION.txt"
CHANGELOG_PATH = OUTPUT / "CHANGELOG.md"
OUT_HTML = OUTPUT / "index.html"
# Joined GeoJSON written alongside index.html so the page can fetch() it.
# Serving separately enables browser caching and CDN gzip compression.
OUT_GEOJSON = OUTPUT / "postcode_districts.geojson"

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


def compute_stats(price_data: dict[str, dict], data_date: str) -> dict:
    prices = sorted(r["price_per_sqm"] for r in price_data.values())
    median = int(statistics.median(prices))
    total_sales = sum(r["num_sales"] for r in price_data.values())

    ranked = [
        {"district": d, **v}
        for d, v in price_data.items()
        if v["num_sales"] >= MIN_SALES_FOR_RANKING
    ]
    ranked.sort(key=lambda r: r["price_per_sqm"])

    date_range = f"Jan 1995–{data_date}" if data_date else "Jan 1995–present"

    return {
        "median_price_per_sqm": median,
        "num_districts": len(price_data),
        "date_range": date_range,
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


def _strip_points(geometry: dict) -> dict:
    """Remove Point/MultiPoint sub-geometries from a GeometryCollection.

    The Geolytix source data includes stray Point geometries inside some
    GeometryCollections.  Leaflet renders these as default pin markers.
    This keeps only Polygon/MultiPolygon parts and promotes to Polygon or
    MultiPolygon when a single sub-geometry remains.
    """
    if geometry["type"] != "GeometryCollection":
        return geometry
    keep = ("Polygon", "MultiPolygon")
    polys = [g for g in geometry["geometries"] if g["type"] in keep]
    if len(polys) == 1:
        return polys[0]
    if polys:
        return {"type": "GeometryCollection", "geometries": polys}
    return geometry  # no polygons found — leave unchanged


def build_geojson(boundaries: dict, price_data: dict[str, dict]) -> dict:
    """Join price data into the boundary GeoJSON features."""
    matched = 0
    for feature in boundaries["features"]:
        feature["geometry"] = _strip_points(feature["geometry"])
        dist = feature["properties"].get("PostDist")
        if dist and dist in price_data:
            feature["properties"].update(price_data[dist])
            matched += 1
    print(
        f"  Joined {matched} / {len(boundaries['features'])} boundary features "
        f"to {len(price_data)} price records"
    )
    return boundaries


def load_version() -> str:
    """Read VERSION.txt and return a version string like 'v0.1.0'."""
    if not VERSION_PATH.exists():
        return ""
    return "v" + VERSION_PATH.read_text().strip()


def load_data_date() -> str:
    """Parse the most recent release date from CHANGELOG.md.

    Looks for the first '## [x.y.z] — YYYY-MM-DD' heading and returns the
    date formatted as 'Month YYYY' (e.g. 'March 2026').  Falls back to an
    empty string if CHANGELOG.md is missing or no dated release is found.
    """
    if not CHANGELOG_PATH.exists():
        return ""
    import datetime

    pattern = re.compile(r"^## \[\d+\.\d+\.\d+\] — (\d{4}-\d{2}-\d{2})", re.MULTILINE)
    m = pattern.search(CHANGELOG_PATH.read_text())
    if not m:
        return ""
    date = datetime.date.fromisoformat(m.group(1))
    return date.strftime("%B %Y")


def _inline(text: str) -> str:
    """Apply inline markdown transforms: escape HTML, then bold/italic/code/links."""
    text = html.escape(text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    return text


def changelog_to_html(md: str) -> str:
    """Convert our CHANGELOG.md subset of Markdown to an HTML fragment.

    Handles: ATX headings (#/##/###/####), pipe tables, unordered lists (-),
    indented code blocks (4 spaces), inline bold/italic/code/links, and
    paragraphs (consecutive plain lines are joined into one <p>).
    The top-level # heading is skipped (redundant inside the <details>).
    """
    lines = md.splitlines()
    parts: list[str] = []
    in_table = False
    in_list = False
    para_buf: list[str] = []
    i = 0

    def flush_para() -> None:
        if para_buf:
            parts.append(f"<p>{' '.join(para_buf)}</p>")
            para_buf.clear()

    def close_open() -> None:
        nonlocal in_table, in_list
        flush_para()
        if in_table:
            parts.append("</tbody></table>")
            in_table = False
        if in_list:
            parts.append("</ul>")
            in_list = False

    while i < len(lines):
        line = lines[i]

        # ATX headings
        m = re.match(r"^(#{1,6}) (.+)", line)
        if m:
            close_open()
            level = len(m.group(1))
            text = _inline(m.group(2))
            # Flatten all heading levels to h4 for the compact changelog view;
            # skip the top-level # title (level 1) entirely.
            if level == 1:
                i += 1
                continue
            parts.append(f"<h4>{text}</h4>")
            i += 1
            continue

        # Indented code block (4 spaces)
        if line.startswith("    ") and line.strip():
            close_open()
            parts.append(f"<pre><code>{html.escape(line[4:])}</code></pre>")
            i += 1
            continue

        # Table row
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if not in_table:
                # Close anything else that was open before starting the table
                flush_para()
                if in_list:
                    parts.append("</ul>")
                    in_list = False
                in_table = True
                parts.append("<table><thead><tr>")
                for c in cells:
                    parts.append(f"<th>{_inline(c)}</th>")
                parts.append("</tr></thead><tbody>")
                # Skip separator row (|---|---|)
                i += 1
                if i < len(lines) and re.match(r"\|[-| :]+\|", lines[i]):
                    i += 1
            else:
                parts.append("<tr>")
                for c in cells:
                    parts.append(f"<td>{_inline(c)}</td>")
                parts.append("</tr>")
                i += 1
            continue

        # List item
        if line.startswith("- "):
            flush_para()
            if in_table:
                parts.append("</tbody></table>")
                in_table = False
            if not in_list:
                in_list = True
                parts.append("<ul>")
            parts.append(f"<li>{_inline(line[2:])}</li>")
            i += 1
            continue

        # Blank line — flush paragraph, close list/table
        if not line.strip():
            close_open()
            i += 1
            continue

        # Plain text — accumulate into paragraph buffer
        if in_table or in_list:
            close_open()
        para_buf.append(_inline(line))
        i += 1

    close_open()
    return "\n        ".join(parts)


def main() -> None:
    # Validate inputs
    missing = [p for p in (BOUNDARIES_PATH, CSV_PATH, TEMPLATE_PATH) if not p.exists()]
    if missing:
        for p in missing:
            print(f"Missing: {p}", file=sys.stderr)
        if BOUNDARIES_PATH in missing:
            print("  Run: uv run scripts/fetch_boundaries.py", file=sys.stderr)
        if CSV_PATH in missing:
            print(
                "  Run the pipeline first: uv run python src/houseprices/pipeline.py",
                file=sys.stderr,
            )
        sys.exit(1)

    version = load_version()
    data_date = load_data_date()

    print("Loading data…")
    boundaries = json.loads(BOUNDARIES_PATH.read_text())
    price_data = load_price_data()
    print(
        f"  {len(boundaries['features'])} boundary features, "
        f"{len(price_data)} price records"
    )

    print("Joining…")
    geojson = build_geojson(boundaries, price_data)
    stats = compute_stats(price_data, data_date)

    print("Writing GeoJSON…")
    OUT_GEOJSON.write_text(json.dumps(geojson, separators=(",", ":")))
    geojson_kb = OUT_GEOJSON.stat().st_size // 1024
    print(f"  Written → {OUT_GEOJSON} ({geojson_kb:,} KB)")

    print("Rendering…")
    changelog_html = (
        changelog_to_html(CHANGELOG_PATH.read_text()) if CHANGELOG_PATH.exists() else ""
    )

    template = TEMPLATE_PATH.read_text()
    rendered = (
        template.replace("__STATS__", json.dumps(stats, separators=(",", ":")))
        .replace("__VERSION__", version)
        .replace("__DATA_DATE__", data_date)
        .replace("__CHANGELOG_HTML__", changelog_html)
    )

    OUT_HTML.write_text(rendered)
    size_kb = OUT_HTML.stat().st_size // 1024
    print(f"  Written → {OUT_HTML} ({size_kb:,} KB)")
    print(f"  Median: £{stats['median_price_per_sqm']:,}/m²")
    print(f"  Districts: {stats['num_districts']:,}")
    print(f"  Sales: {stats['total_sales']:,}")


if __name__ == "__main__":
    main()
