"""Tests for scripts/prepare_boundaries.py."""

from __future__ import annotations

import importlib.util
import io
import json
import pathlib
from unittest.mock import MagicMock, patch


def _load_prepare_boundaries_module():
    path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "scripts"
        / "prepare_boundaries.py"
    )
    spec = importlib.util.spec_from_file_location("prepare_boundaries", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pb = _load_prepare_boundaries_module()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_E15_FEATURE = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[0.0, 51.5], [0.05, 51.5], [0.05, 51.55], [0.0, 51.5]]],
    },
    "properties": {"PostDist": "E15"},
}

_E20_FEATURE = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[0.01, 51.5], [0.02, 51.5], [0.02, 51.51], [0.01, 51.5]]],
    },
    "properties": {"PostDist": "E20"},
}


def _ons_response(features: list[dict]) -> bytes:
    return json.dumps({"type": "FeatureCollection", "features": features}).encode()


def _mock_urlopen(payload: bytes):
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=io.BytesIO(payload))
    cm.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=cm)


# ---------------------------------------------------------------------------
# _fetch_ons_district_area
# ---------------------------------------------------------------------------


def test_fetch_ons_district_area_returns_features_for_area() -> None:
    payload = _ons_response([_E15_FEATURE, _E20_FEATURE])
    with patch("urllib.request.urlopen", _mock_urlopen(payload)):
        result = pb._fetch_ons_district_area("E")
    assert len(result) == 2
    districts = {f["properties"]["PostDist"] for f in result}
    assert districts == {"E15", "E20"}


def test_fetch_ons_district_area_returns_empty_on_network_error() -> None:
    with patch("urllib.request.urlopen", side_effect=OSError("no network")):
        result = pb._fetch_ons_district_area("E")
    assert result == []


def test_fetch_ons_district_area_returns_empty_on_bad_json() -> None:
    with patch("urllib.request.urlopen", _mock_urlopen(b"not-json")):
        result = pb._fetch_ons_district_area("E")
    assert result == []


# ---------------------------------------------------------------------------
# _augment_with_ons
# ---------------------------------------------------------------------------


def test_augment_with_ons_adds_missing_district(tmp_path: pathlib.Path) -> None:
    # GeoJSON without E20 (only E15)
    geojson_path = tmp_path / "postcode_districts.geojson"
    geojson_path.write_text(
        json.dumps({"type": "FeatureCollection", "features": [_E15_FEATURE]})
    )

    # ONS returns E15 + E20
    payload = _ons_response([_E15_FEATURE, _E20_FEATURE])

    def _fake_mapshaper(src: pathlib.Path, dst: pathlib.Path, retain: str) -> None:
        # Pass features through unchanged (mock simplification)
        dst.write_text(src.read_text())

    with (
        patch("urllib.request.urlopen", _mock_urlopen(payload)),
        patch.object(pb, "_mapshaper_geojson", side_effect=_fake_mapshaper),
    ):
        pb._augment_with_ons(geojson_path, ["E20"], pb._MAPSHAPER_DEFAULT_RETAIN)

    result = json.loads(geojson_path.read_text())
    districts = {f["properties"]["PostDist"] for f in result["features"]}
    assert "E20" in districts


def test_augment_with_ons_replaces_existing_district_with_ons_version(
    tmp_path: pathlib.Path,
) -> None:
    old_e15 = {**_E15_FEATURE, "properties": {"PostDist": "E15", "stale": True}}
    geojson_path = tmp_path / "postcode_districts.geojson"
    geojson_path.write_text(
        json.dumps({"type": "FeatureCollection", "features": [old_e15]})
    )

    new_e15 = {**_E15_FEATURE, "properties": {"PostDist": "E15", "stale": False}}
    payload = _ons_response([new_e15, _E20_FEATURE])

    def _fake_mapshaper(src: pathlib.Path, dst: pathlib.Path, retain: str) -> None:
        dst.write_text(src.read_text())

    with (
        patch("urllib.request.urlopen", _mock_urlopen(payload)),
        patch.object(pb, "_mapshaper_geojson", side_effect=_fake_mapshaper),
    ):
        pb._augment_with_ons(geojson_path, ["E20"], pb._MAPSHAPER_DEFAULT_RETAIN)

    result = json.loads(geojson_path.read_text())
    e15_features = [
        f for f in result["features"] if f["properties"]["PostDist"] == "E15"
    ]
    assert len(e15_features) == 1
    assert e15_features[0]["properties"].get("stale") is False


def test_augment_with_ons_is_noop_when_ons_returns_no_features(
    tmp_path: pathlib.Path,
) -> None:
    geojson_path = tmp_path / "postcode_districts.geojson"
    original = json.dumps({"type": "FeatureCollection", "features": [_E15_FEATURE]})
    geojson_path.write_text(original)

    with patch("urllib.request.urlopen", _mock_urlopen(_ons_response([]))):
        pb._augment_with_ons(geojson_path, ["E20"], pb._MAPSHAPER_DEFAULT_RETAIN)

    # File should be unchanged
    assert geojson_path.read_text() == original

    with patch.object(
        pb.subprocess,
        "run",
        return_value=MagicMock(returncode=0, stderr=""),
    ) as mock_run:
        pb._ogr2ogr_geojson("/tmp/source.shp", pathlib.Path("/tmp/out.geojson"), "100")

    args = mock_run.call_args.args[0]
    assert args[0] == "ogr2ogr"
    assert "-simplify" in args
    assert args[args.index("-simplify") + 1] == "100"
    assert args[args.index("-lco") + 1] == "COORDINATE_PRECISION=5"


def test_mapshaper_geojson_uses_topology_preserving_options(
    tmp_path: pathlib.Path,
) -> None:
    temp_dir = MagicMock()
    temp_dir.__enter__.return_value = str(tmp_path)
    temp_dir.__exit__.return_value = False

    def _fake_run(command: list[str], _: str) -> None:
        (tmp_path / "out1.geojson").write_text('{"features": []}')
        (tmp_path / "out2.geojson").write_text('{"features": [{"id": 1}]}')

    with (
        patch.object(pb.shutil, "which", return_value="/usr/bin/npx"),
        patch.object(pb.tempfile, "TemporaryDirectory", return_value=temp_dir),
        patch.object(pb, "_run_checked", side_effect=_fake_run) as mock_run,
    ):
        dst = tmp_path / "chosen.geojson"
        pb._mapshaper_geojson(
            pathlib.Path("/tmp/raw.geojson"),
            dst,
            "10%",
        )

    args = mock_run.call_args.args[0]
    assert args[:3] == ["npx", "--yes", "mapshaper"]
    assert "-simplify" in args
    assert "10%" in args
    assert "weighted" in args
    assert "keep-shapes" in args
    assert "-clean" in args
    assert "rewind" in args
    assert f"precision={pb._WGS84_PRECISION}" in args
    assert "fix-geometry" in args
    assert "extension=.geojson" in args
    assert dst.exists()


def test_main_uses_unsimplified_intermediate_for_mapshaper(
    tmp_path: pathlib.Path,
) -> None:
    output = tmp_path / "postcode_districts.geojson"

    def _write_output(_: pathlib.Path, dst: pathlib.Path, __: str) -> None:
        dst.write_text('{"features": []}')

    with (
        patch.object(pb, "_OUTPUT", output),
        patch.object(pb, "_find_shp_vsipath", return_value="/tmp/source.shp"),
        patch.object(pb, "_ogr2ogr_geojson") as mock_ogr,
        patch.object(pb, "_mapshaper_geojson", side_effect=_write_output) as mock_map,
        patch.object(pb, "_report_missing"),
        patch.object(
            pb.sys,
            "argv",
            ["prepare_boundaries.py", "--engine", "mapshaper"],
        ),
    ):
        pb.main()

    assert mock_ogr.call_args.kwargs["simplify_meters"] is None
    assert mock_map.call_args.args[2] == pb._MAPSHAPER_DEFAULT_RETAIN
    assert output.exists()
