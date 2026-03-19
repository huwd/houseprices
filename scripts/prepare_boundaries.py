#!/usr/bin/env python3
"""Convert Geolytix PostalBoundariesOpen SHP to WGS84 GeoJSON.

Reads PostalDistrict.shp from the Geolytix ZIP, reprojects from BNG
(Airy 1830) to WGS84 EPSG:4326, and writes data/postcode_districts.geojson.

Looks for the SHP in two places (first match wins):
  1. data/geolytix_postal_boundaries.zip — downloaded by `make download`
     (outer ZIP → PostalBoundariesSHP.zip → PostalDistrict.shp)
  2. data/GEOLYTIX - PostalBoundariesOpen2012/PostalBoundariesSHP.zip
     — manually unpacked fallback

After conversion, reports any postcode districts present in the existing
output that are absent from the Geolytix data (e.g. E20 — created 2012,
not in this 2012 vintage dataset).

Requires: ogr2ogr (GDAL) on PATH.

Usage:
    uv run scripts/prepare_boundaries.py
    uv run scripts/prepare_boundaries.py --force   # overwrite if exists
"""

import argparse
import json
import pathlib
import subprocess
import sys
import tempfile
import zipfile

ROOT = pathlib.Path(__file__).parent.parent
DATA = ROOT / "data"

_OUTER_ZIP = DATA / "geolytix_postal_boundaries.zip"
_INNER_ZIP_NAME = "PostalBoundariesSHP.zip"
_MANUAL_ZIP = DATA / "GEOLYTIX - PostalBoundariesOpen2012" / "PostalBoundariesSHP.zip"
_SHP_NAME = "PostalDistrict.shp"
_OUTPUT = DATA / "postcode_districts.geojson"


def _find_shp_vsipath() -> str:
    """Return a GDAL /vsizip/ path to PostalDistrict.shp.

    Tries the downloaded outer ZIP first, then the manual unpacked directory.
    Raises SystemExit with a helpful message if neither is found.
    """
    if _OUTER_ZIP.exists():
        # Check whether it's a flat ZIP (SHP files at root) or nested ZIP
        with zipfile.ZipFile(_OUTER_ZIP) as zf:
            names = zf.namelist()
        if _SHP_NAME in names:
            return f"/vsizip/{_OUTER_ZIP}/{_SHP_NAME}"
        # Look for the inner SHP zip
        inner = next(
            (n for n in names if n.endswith(_INNER_ZIP_NAME)),
            None,
        )
        if inner:
            return f"/vsizip//vsizip/{_OUTER_ZIP}/{inner}/{_SHP_NAME}"

    if _MANUAL_ZIP.exists():
        return f"/vsizip/{_MANUAL_ZIP}/{_SHP_NAME}"

    print(
        "ERROR: Geolytix SHP data not found.\n"
        f"  Expected: {_OUTER_ZIP}\n"
        f"       or: {_MANUAL_ZIP}\n"
        "  Run: make download",
        file=sys.stderr,
    )
    sys.exit(1)


def _report_missing(output: pathlib.Path) -> None:
    """Print any postcode districts present in the existing GeoJSON but absent
    from the newly written file.  Typical output: E20 (post-2012 creation).
    """
    if not output.exists():
        return

    existing_path = DATA / "postcode_districts.geojson"
    if not existing_path.exists() or existing_path == output:
        return

    existing = {
        f["properties"].get("PostDist")
        for f in json.loads(existing_path.read_text())["features"]
    }
    new = {
        f["properties"].get("PostDist")
        for f in json.loads(output.read_text())["features"]
    }

    missing = sorted(d for d in existing - new if d)
    if missing:
        print(
            f"  Note: {len(missing)} district(s) in old GeoJSON"
            " absent from Geolytix data:"
        )
        for d in missing:
            print(f"    {d}")
        print(
            "  These districts post-date the 2012 Geolytix vintage"
            " (e.g. E20 — Olympic Park)."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force", action="store_true", help="Overwrite output if it already exists."
    )
    args = parser.parse_args()

    if _OUTPUT.exists() and not args.force:
        print(f"  ⊘  {_OUTPUT.name} already exists (use --force to overwrite)")
        return

    vsi_path = _find_shp_vsipath()
    print(f"  → Converting {vsi_path}")
    print(f"     to {_OUTPUT}")

    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
        tmp_path = pathlib.Path(tmp.name)
    tmp_path.unlink()  # ogr2ogr refuses to overwrite an existing file

    try:
        result = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "GeoJSON",
                "-t_srs",
                "EPSG:4326",
                str(tmp_path),
                vsi_path,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"ERROR: ogr2ogr failed:\n{result.stderr}", file=sys.stderr)
            tmp_path.unlink(missing_ok=True)
            sys.exit(1)

        _report_missing(tmp_path)
        tmp_path.replace(_OUTPUT)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    data = json.loads(_OUTPUT.read_text())
    print(f"  ✓  {len(data['features'])} districts written to {_OUTPUT.name}")


if __name__ == "__main__":
    main()
