#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["mapbox-vector-tile", "mercantile", "requests", "shapely"]
# ///
"""Fetch postcode district polygon boundaries from Anna Powell-Smith's public
Mapbox tileset and write to data/postcode_districts.geojson.

The postcode district geometry is sourced from OS/Royal Mail open data served
via the tileset. Licence: OGL (Open Government Licence).

Usage:
    uv run scripts/fetch_boundaries.py
    uv run scripts/fetch_boundaries.py --force   # re-fetch even if cached
"""

import json
import pathlib
import sys

import mapbox_vector_tile
import mercantile
import requests
from shapely.geometry import mapping
from shapely.geometry import shape as shapely_shape
from shapely.ops import unary_union

# Same public token and tileset as fetch_anna_reference.py
TOKEN = (
    "pk.eyJ1Ijoid2hvb3duc2VuZ2xhbmQiLCJhIjoiY2l6ZDcwNW1uMDAzdjMyb3llczN6bDh6"
    "ZyJ9.laaDJGqsBHQLIZRy9dWlxA"
)
TILESET = "annapowellsmith.2kq8mrxg"
LAYER = "postcode_sectors_englandgeojson"
BOUNDS = (-6.418537, 49.863213, 1.763537, 55.830803)
ZOOM = 8  # ~48 tiles; good balance of geometry quality vs download size

OUTPUT = pathlib.Path(__file__).parent.parent / "data" / "postcode_districts.geojson"


def tile_url(z: int, x: int, y: int) -> str:
    return f"https://api.mapbox.com/v4/{TILESET}/{z}/{x}/{y}.mvt?access_token={TOKEN}"


def fetch_tile(z: int, x: int, y: int) -> bytes | None:
    resp = requests.get(tile_url(z, x, y), timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.content


def mvt_coords_to_wgs84(
    coords: list, tile_bounds: tuple, extent: int = 4096
) -> list:
    """Recursively convert MVT tile coordinates to WGS84 [lon, lat]."""
    if not coords:
        return coords
    if isinstance(coords[0], (int, float)):
        x, y = coords
        lon = tile_bounds.west + (x / extent) * (tile_bounds.east - tile_bounds.west)
        lat = tile_bounds.north - (y / extent) * (tile_bounds.north - tile_bounds.south)
        return [lon, lat]
    return [mvt_coords_to_wgs84(c, tile_bounds, extent) for c in coords]


def transform_geometry(geom: dict, tile_bounds: tuple) -> dict:
    return {
        "type": geom["type"],
        "coordinates": mvt_coords_to_wgs84(geom["coordinates"], tile_bounds),
    }


def main(force: bool = False) -> None:
    if OUTPUT.exists() and not force:
        print(f"Already cached: {OUTPUT}")
        print("Pass --force to re-fetch.")
        return

    tiles = list(mercantile.tiles(*BOUNDS, zooms=ZOOM))
    print(f"Fetching {len(tiles)} tiles at zoom {ZOOM}…")

    district_geoms: dict[str, list] = {}
    errors = 0

    for i, tile in enumerate(tiles, 1):
        print(f"  [{i:>2}/{len(tiles)}] {tile.z}/{tile.x}/{tile.y}", end="  ")
        data = fetch_tile(tile.z, tile.x, tile.y)
        if data is None:
            print("(empty)")
            continue
        print(f"{len(data):>8,} bytes")

        tile_bounds = mercantile.bounds(tile)
        decoded = mapbox_vector_tile.decode(data)

        layer = decoded.get(LAYER, {})
        for feature in layer.get("features", []):
            props = feature.get("properties", {})
            dist = props.get("PostDist")
            if not dist:
                continue
            try:
                raw_geom = transform_geometry(feature["geometry"], tile_bounds)
                geom = shapely_shape(raw_geom)
                if geom.is_valid and not geom.is_empty:
                    district_geoms.setdefault(dist, []).append(geom)
            except Exception:
                errors += 1

    print(f"\nMerging geometries for {len(district_geoms)} districts…")
    features = []
    for dist, geoms in sorted(district_geoms.items()):
        try:
            merged = unary_union(geoms) if len(geoms) > 1 else geoms[0]
            features.append(
                {
                    "type": "Feature",
                    "properties": {"PostDist": dist},
                    "geometry": mapping(merged),
                }
            )
        except Exception:
            errors += 1

    geojson = {"type": "FeatureCollection", "features": features}
    OUTPUT.write_text(json.dumps(geojson))
    size_kb = OUTPUT.stat().st_size // 1024
    print(f"Written → {OUTPUT} ({len(features)} districts, {size_kb:,} KB)")
    if errors:
        print(f"  {errors} geometry errors skipped")


if __name__ == "__main__":
    main(force="--force" in sys.argv)
