"""Tests for site_config.render_shared_js: CARTO key injection."""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "scripts"))
import site_config  # noqa: E402


def test_render_shared_js_injects_key(tmp_path, monkeypatch):
    monkeypatch.setenv("CARTO_API_KEY", "abc123")
    src, dest = tmp_path / "in.js", tmp_path / "out.js"
    src.write_text('const K = "__CARTO_API_KEY__";')
    site_config.render_shared_js(src, dest)
    assert dest.read_text() == 'const K = "abc123";'


def test_render_shared_js_fails_without_key(tmp_path, monkeypatch):
    monkeypatch.delenv("CARTO_API_KEY", raising=False)
    monkeypatch.setattr(site_config, "load_dotenv", lambda: None)
    src = tmp_path / "in.js"
    src.write_text("x")
    with pytest.raises(SystemExit):
        site_config.render_shared_js(src, tmp_path / "out.js")
