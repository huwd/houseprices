"""Tests for download.py: skip-if-exists, streaming, and auth headers."""

from __future__ import annotations

import base64
import pathlib
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


def test_download_epc_uses_basic_auth(tmp_path: pathlib.Path) -> None:
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_epc(tmp_path, email="user@example.com", api_key="s3cr3t")
    headers = mock_get.call_args.kwargs["headers"]
    expected = base64.b64encode(b"user@example.com:s3cr3t").decode()
    assert headers["Authorization"] == f"Basic {expected}"


def test_download_epc_saves_as_zip(tmp_path: pathlib.Path) -> None:
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_epc(tmp_path, email="u@e.com", api_key="k")
    assert result.name == "epc-domestic-all.zip"


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


def test_download_os_open_uprn_passes_api_key(tmp_path: pathlib.Path) -> None:
    dl.OS_OPEN_UPRN_URL = "http://example.com/uprn"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_os_open_uprn(tmp_path, api_key="mykey")
    called_url: str = mock_get.call_args.args[0]
    assert "mykey" in called_url


def test_download_os_open_uprn_saves_as_zip(tmp_path: pathlib.Path) -> None:
    dl.OS_OPEN_UPRN_URL = "http://example.com/uprn"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_os_open_uprn(tmp_path, api_key="k")
    assert result.name == "os-open-uprn.zip"


# ---------------------------------------------------------------------------
# download_lsoa_boundaries
# ---------------------------------------------------------------------------


def test_download_lsoa_saves_as_gpkg(tmp_path: pathlib.Path) -> None:
    dl.LSOA_BGC_URL = "http://example.com/lsoa.gpkg"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_lsoa_boundaries(tmp_path)
    assert result.name == "lsoa_boundaries.gpkg"
