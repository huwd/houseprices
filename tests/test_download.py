"""Tests for download.py: skip-if-exists, streaming, and auth headers."""

from __future__ import annotations

import base64
import pathlib
import zipfile
from unittest.mock import MagicMock, patch

import pytest

import houseprices.download as dl


def _mock_response(chunks: list[bytes] | None = None) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.iter_content.return_value = iter(chunks or [b"data"])
    return resp


# ---------------------------------------------------------------------------
# _stream_to_file internals
# ---------------------------------------------------------------------------


def test_skips_download_if_file_exists(tmp_path: pathlib.Path) -> None:
    dest = tmp_path / "file.csv"
    dest.write_bytes(b"existing")
    with patch("houseprices.download.requests.get") as mock_get:
        result = dl._stream_to_file("http://example.com/f", dest)
    mock_get.assert_not_called()
    assert result == dest


def test_writes_response_chunks(tmp_path: pathlib.Path) -> None:
    dest = tmp_path / "file.csv"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response([b"hello ", b"world"]),
    ):
        dl._stream_to_file("http://example.com/f", dest)
    assert dest.read_bytes() == b"hello world"


def test_raises_on_http_error(tmp_path: pathlib.Path) -> None:
    dest = tmp_path / "file.csv"
    resp = MagicMock()
    resp.raise_for_status.side_effect = Exception("404 Not Found")
    with (
        patch("houseprices.download.requests.get", return_value=resp),
        pytest.raises(Exception, match="404"),
    ):
        dl._stream_to_file("http://example.com/f", dest)


def test_creates_parent_directories(tmp_path: pathlib.Path) -> None:
    dest = tmp_path / "nested" / "dir" / "file.csv"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        dl._stream_to_file("http://example.com/f", dest)
    assert dest.exists()


# ---------------------------------------------------------------------------
# download_ppd
# ---------------------------------------------------------------------------


def test_download_ppd_uses_ppd_url(tmp_path: pathlib.Path) -> None:
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_ppd(tmp_path)
    assert mock_get.call_args.args[0] == dl.PPD_URL


def test_download_ppd_saves_as_csv(tmp_path: pathlib.Path) -> None:
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_ppd(tmp_path)
    assert result.name == "pp-complete.csv"


# ---------------------------------------------------------------------------
# download_epc
# ---------------------------------------------------------------------------


def test_download_epc_uses_basic_auth(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EPC_EMAIL", "user@example.com")
    monkeypatch.setenv("EPC_API_KEY", "s3cr3t")
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_epc(tmp_path)
    headers = mock_get.call_args.kwargs["headers"]
    expected = base64.b64encode(b"user@example.com:s3cr3t").decode()
    assert headers["Authorization"] == f"Basic {expected}"


def test_download_epc_saves_as_zip(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EPC_EMAIL", "u@e.com")
    monkeypatch.setenv("EPC_API_KEY", "k")
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_epc(tmp_path)
    assert result.name == "epc-domestic-all.zip"


def test_download_epc_raises_if_env_missing(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """KeyError if EPC credentials are not set in the environment."""
    monkeypatch.delenv("EPC_EMAIL", raising=False)
    monkeypatch.delenv("EPC_API_KEY", raising=False)
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with pytest.raises(KeyError):
        dl.download_epc(tmp_path)


# ---------------------------------------------------------------------------
# download_ubdc
# ---------------------------------------------------------------------------


def test_download_ubdc_saves_as_zip(tmp_path: pathlib.Path) -> None:
    dl.UBDC_URL = "http://example.com/ubdc.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_ubdc(tmp_path)
    assert result.name == "ppd-uprn-lookup.zip"


# ---------------------------------------------------------------------------
# download_os_open_uprn
# ---------------------------------------------------------------------------


def test_download_os_open_uprn_uses_os_open_uprn_url(tmp_path: pathlib.Path) -> None:
    """No API key required; URL is used as-is."""
    dl.OS_OPEN_UPRN_URL = "http://example.com/uprn"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_os_open_uprn(tmp_path)
    assert mock_get.call_args.args[0] == dl.OS_OPEN_UPRN_URL


def test_download_os_open_uprn_saves_as_zip(tmp_path: pathlib.Path) -> None:
    dl.OS_OPEN_UPRN_URL = "http://example.com/uprn"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_os_open_uprn(tmp_path)
    assert result.name == "os-open-uprn.zip"


# ---------------------------------------------------------------------------
# download_lsoa_boundaries
# ---------------------------------------------------------------------------


def _make_fake_fgdb_zip(path: pathlib.Path) -> None:
    """Write a minimal ZIP containing a fake .gdb directory."""
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("fake.gdb/placeholder", "")


def test_download_lsoa_skips_if_gpkg_exists(tmp_path: pathlib.Path) -> None:
    """If lsoa_boundaries.gpkg already exists the download is skipped."""
    (tmp_path / "lsoa_boundaries.gpkg").write_bytes(b"existing")
    with patch("houseprices.download.requests.get") as mock_get:
        result = dl.download_lsoa_boundaries(tmp_path)
    mock_get.assert_not_called()
    assert result.name == "lsoa_boundaries.gpkg"


def test_download_lsoa_calls_ogr2ogr(tmp_path: pathlib.Path) -> None:
    """ogr2ogr must be called with GPKG output and EPSG:27700 reprojection."""
    dl.LSOA_BGC_URL = "http://example.com/lsoa.fgdb.zip"

    fake_zip = tmp_path / "fake.zip"
    _make_fake_fgdb_zip(fake_zip)

    with (
        patch(
            "houseprices.download.requests.get",
            return_value=_mock_response(chunks=[fake_zip.read_bytes()]),
        ),
        patch("houseprices.download.subprocess.run") as mock_run,
    ):
        dl.download_lsoa_boundaries(tmp_path)

    args = mock_run.call_args.args[0]
    assert args[0] == "ogr2ogr"
    assert "-t_srs" in args
    assert "EPSG:27700" in args
    assert args[args.index("-f") + 1] == "GPKG"


def test_download_lsoa_raises_if_no_gdb_in_zip(tmp_path: pathlib.Path) -> None:
    """FileNotFoundError if the ZIP contains no .gdb directory."""
    dl.LSOA_BGC_URL = "http://example.com/lsoa.fgdb.zip"

    empty_zip = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty_zip, "w") as zf:
        zf.writestr("not_a_gdb/placeholder", "")

    with (
        patch(
            "houseprices.download.requests.get",
            return_value=_mock_response(chunks=[empty_zip.read_bytes()]),
        ),
        pytest.raises(FileNotFoundError, match=".gdb"),
    ):
        dl.download_lsoa_boundaries(tmp_path)


def test_download_lsoa_cleans_up_fgdb_zip(tmp_path: pathlib.Path) -> None:
    """The intermediate FGDB ZIP must be removed after conversion."""
    dl.LSOA_BGC_URL = "http://example.com/lsoa.fgdb.zip"

    fake_zip = tmp_path / "fake.zip"
    _make_fake_fgdb_zip(fake_zip)

    with (
        patch(
            "houseprices.download.requests.get",
            return_value=_mock_response(chunks=[fake_zip.read_bytes()]),
        ),
        patch("houseprices.download.subprocess.run"),
    ):
        dl.download_lsoa_boundaries(tmp_path)

    assert not (tmp_path / "lsoa_boundaries.fgdb.zip").exists()
