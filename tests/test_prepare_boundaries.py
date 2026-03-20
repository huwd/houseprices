"""Tests for scripts/prepare_boundaries.py."""

from __future__ import annotations

import importlib.util
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


def test_ogr2ogr_geojson_uses_meter_tolerance() -> None:
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
