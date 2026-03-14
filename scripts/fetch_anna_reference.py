#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["mapbox-vector-tile", "mercantile", "requests"]
# ///
"""Fetch Anna Powell-Smith's postcode district price-per-sqm data from
her Mapbox tileset and write it to data/anna_reference.json.

Uses the public access token and tileset ID embedded in houseprices.anna.ps.

Usage:
    uv run scripts/fetch_anna_reference.py
"""

import json
import pathlib

import mapbox_vector_tile
import mercantile
import requests

# ---------------------------------------------------------------------------
# Source — public token and tileset ID from houseprices.anna.ps
# ---------------------------------------------------------------------------

TOKEN = (
    "pk.eyJ1Ijoid2hvb3duc2VuZ2xhbmQiLCJhIjoiY2l6ZDcwNW1uMDAzdjMyb3llczN6bDh6"
    "ZyJ9.laaDJGqsBHQLIZRy9dWlxA"
)
TILESET = "annapowellsmith.2kq8mrxg"
LAYER = "postcode_sectors_englandgeojson"

# England and Wales bounding box (from tileset metadata)
BOUNDS = (-6.418537, 49.863213, 1.763537, 55.830803)

# z=7 gives 20 tiles — small enough to be fast, high enough that all
# postcode districts are represented in the tile data
ZOOM = 6

OUTPUT = pathlib.Path(__file__).parent.parent / "data" / "anna_reference.json"

# ---------------------------------------------------------------------------


def tile_url(z: int, x: int, y: int) -> str:
    return f"https://api.mapbox.com/v4/{TILESET}/{z}/{x}/{y}.mvt?access_token={TOKEN}"


def fetch_tile(z: int, x: int, y: int) -> bytes | None:
    """Return tile bytes, or None if the tile doesn't exist (ocean/out of bounds)."""
    url = tile_url(z, x, y)
    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.content


def main() -> None:
    tiles = list(mercantile.tiles(*BOUNDS, zooms=ZOOM))
    print(f"Fetching {len(tiles)} tiles at zoom {ZOOM} …")

    districts: dict[str, dict[str, object]] = {}

    for i, tile in enumerate(tiles, 1):
        print(f"  [{i}/{len(tiles)}] z={tile.z} x={tile.x} y={tile.y}", end=" ")
        data = fetch_tile(tile.z, tile.x, tile.y)
        if data is None:
            print("(empty)")
            continue
        print(f"({len(data):,} bytes)")
        decoded = mapbox_vector_tile.decode(data)

        layer = decoded.get(LAYER, {})
        for feature in layer.get("features", []):
            props = feature.get("properties", {})
            dist = props.get("PostDist")
            price = props.get("price_by_postcode_district_price_per_sq_m")
            if not dist or price is None:
                continue
            # Keep first occurrence — value is the same across tiles
            if dist not in districts:
                districts[dist] = {
                    "price_per_sqm": round(price),
                    "num_transactions": props.get(
                        "price_by_postcode_district_num_transactions"
                    ),
                    "total_price": props.get("price_by_postcode_district_total_price"),
                    "total_area": props.get("price_by_postcode_district_total_area"),
                }

    print(f"\nFound {len(districts)} postcode districts")

    result = {
        "_note": (
            "Extracted from Anna Powell-Smith's houseprices.anna.ps Mapbox tileset "
            f"({TILESET}). Price Paid data produced by HM Land Registry © Crown "
            "copyright 2017. EPC data © Crown copyright."
        ),
        "tileset": TILESET,
        "postcode_districts": {
            dist: info["price_per_sqm"] for dist, info in sorted(districts.items())
        },
    }

    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(json.dumps(result, indent=2))
    print(f"Written → {OUTPUT}")

    # Quick sanity check
    prices = [v for v in result["postcode_districts"].values() if v]
    if prices:
        median = sorted(prices)[len(prices) // 2]
        print(f"\nSanity check: median price/m² across all districts = £{median:,}")
        print(f"Min: £{min(prices):,}  Max: £{max(prices):,}")


if __name__ == "__main__":
    main()
