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


def test_download_ubdc_resolves_signed_url(tmp_path: pathlib.Path) -> None:
    """download_ubdc must call the API to resolve the signed URL, then stream it."""
    dl.UBDC_URL = "http://example.com/ubdc-api"
    signed_url = "http://blob.example.com/signed?token=abc"

    api_response = MagicMock()
    api_response.raise_for_status.return_value = None
    api_response.json.return_value = {"download": {"url": signed_url}}

    with patch(
        "houseprices.download.requests.get",
        side_effect=[api_response, _mock_response()],
    ) as mock_get:
        result = dl.download_ubdc(tmp_path)

    assert mock_get.call_args_list[0].args[0] == dl.UBDC_URL
    assert mock_get.call_args_list[1].args[0] == signed_url
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


# ---------------------------------------------------------------------------
# extract_epc
# ---------------------------------------------------------------------------


def _make_epc_zip(path: pathlib.Path, las: dict[str, list[str]]) -> None:
    """Write a minimal EPC-style ZIP.

    *las* maps LA name to a list of data rows (without header).
    A recommendations.csv is also written for each LA to confirm it is ignored.
    """
    header = "LMK_KEY,ADDRESS1,POSTCODE,TOTAL_FLOOR_AREA,UPRN,LODGEMENT_DATETIME\n"
    with zipfile.ZipFile(path, "w") as zf:
        for la, rows in las.items():
            certs = header + "".join(rows)
            zf.writestr(f"domestic-{la}/certificates.csv", certs)
            zf.writestr(
                f"domestic-{la}/recommendations.csv", "LMK_KEY,IMPROVEMENT_ITEM\n"
            )
        zf.writestr("LICENCE.txt", "OGL")


def test_extract_epc_skips_if_csv_exists(tmp_path: pathlib.Path) -> None:
    (tmp_path / "epc-domestic-all.csv").write_text("existing")
    epc_zip = tmp_path / "epc-domestic-all.zip"
    _make_epc_zip(epc_zip, {"LA1": []})
    result = dl.extract_epc(tmp_path)
    assert result.read_text() == "existing"


def test_extract_epc_writes_header_once(tmp_path: pathlib.Path) -> None:
    _make_epc_zip(
        tmp_path / "epc-domestic-all.zip",
        {
            "LA1": ["k1,a,SW1A1AA,80,100,2022-01-01\n"],
            "LA2": ["k2,b,SW1A2AA,90,200,2022-02-01\n"],
        },
    )
    result = dl.extract_epc(tmp_path)
    lines = result.read_text().splitlines()
    header = "LMK_KEY,ADDRESS1,POSTCODE,TOTAL_FLOOR_AREA,UPRN,LODGEMENT_DATETIME"
    assert lines[0] == header
    assert lines.count(header) == 1


def test_extract_epc_concatenates_all_la_rows(tmp_path: pathlib.Path) -> None:
    _make_epc_zip(
        tmp_path / "epc-domestic-all.zip",
        {
            "LA1": [
                "k1,a,SW1A1AA,80,100,2022-01-01\n",
                "k2,b,SW1A1AB,70,101,2022-01-02\n",
            ],
            "LA2": ["k3,c,SW1A2AA,90,200,2022-02-01\n"],
        },
    )
    result = dl.extract_epc(tmp_path)
    lines = result.read_text().splitlines()
    assert len(lines) == 4  # 1 header + 3 data rows


def test_extract_epc_skips_recommendations(tmp_path: pathlib.Path) -> None:
    _make_epc_zip(
        tmp_path / "epc-domestic-all.zip",
        {"LA1": ["k1,a,SW1A1AA,80,100,2022-01-01\n"]},
    )
    result = dl.extract_epc(tmp_path)
    assert "IMPROVEMENT_ITEM" not in result.read_text()


def test_extract_epc_deletes_zip(tmp_path: pathlib.Path) -> None:
    epc_zip = tmp_path / "epc-domestic-all.zip"
    _make_epc_zip(epc_zip, {"LA1": ["k1,a,SW1A1AA,80,100,2022-01-01\n"]})
    dl.extract_epc(tmp_path)
    assert not epc_zip.exists()


# ---------------------------------------------------------------------------
# extract_os_open_uprn
# ---------------------------------------------------------------------------


def _make_uprn_zip(path: pathlib.Path, content: str) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("osopenuprn_202602.csv", content)
        zf.writestr("licence.txt", "OGL")


def test_extract_os_open_uprn_skips_if_csv_exists(tmp_path: pathlib.Path) -> None:
    (tmp_path / "os-open-uprn.csv").write_text("existing")
    _make_uprn_zip(
        tmp_path / "os-open-uprn.zip", "UPRN,X_COORDINATE,Y_COORDINATE\n1,100,200\n"
    )
    result = dl.extract_os_open_uprn(tmp_path)
    assert result.read_text() == "existing"


def test_extract_os_open_uprn_extracts_csv(tmp_path: pathlib.Path) -> None:
    _make_uprn_zip(
        tmp_path / "os-open-uprn.zip",
        "UPRN,X_COORDINATE,Y_COORDINATE\n1,358260,172796\n",
    )
    result = dl.extract_os_open_uprn(tmp_path)
    assert result.name == "os-open-uprn.csv"
    assert "UPRN" in result.read_text()
    assert "358260" in result.read_text()


def test_extract_os_open_uprn_strips_bom(tmp_path: pathlib.Path) -> None:
    """The OS UPRN CSV ships with a UTF-8 BOM that must be stripped."""
    _make_uprn_zip(
        tmp_path / "os-open-uprn.zip",
        "\ufeffUPRN,X_COORDINATE,Y_COORDINATE\n1,358260,172796\n",
    )
    result = dl.extract_os_open_uprn(tmp_path)
    assert not result.read_text().startswith("\ufeff")
    assert result.read_text().startswith("UPRN")


def test_extract_os_open_uprn_deletes_zip(tmp_path: pathlib.Path) -> None:
    uprn_zip = tmp_path / "os-open-uprn.zip"
    _make_uprn_zip(uprn_zip, "UPRN,X_COORDINATE,Y_COORDINATE\n1,358260,172796\n")
    dl.extract_os_open_uprn(tmp_path)
    assert not uprn_zip.exists()


def test_extract_os_open_uprn_multi_chunk(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All chunks after the first are written correctly."""
    monkeypatch.setattr(dl, "_CHUNK_SIZE", 10)  # force multi-chunk read
    content = "UPRN,X_COORDINATE,Y_COORDINATE\n" + "1,358260,172796\n" * 5
    _make_uprn_zip(tmp_path / "os-open-uprn.zip", content)
    result = dl.extract_os_open_uprn(tmp_path)
    assert result.read_text() == content


# ---------------------------------------------------------------------------
# extract_ubdc
# ---------------------------------------------------------------------------


def _make_ubdc_zip(path: pathlib.Path, content: str) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("ppdid_uprn_usrn.csv", content)


def test_extract_ubdc_skips_if_csv_exists(tmp_path: pathlib.Path) -> None:
    (tmp_path / "ppd-uprn-lookup.csv").write_text("existing")
    _make_ubdc_zip(tmp_path / "ppd-uprn-lookup.zip", "uprn,transactionid\n")
    result = dl.extract_ubdc(tmp_path)
    assert result.read_text() == "existing"


def test_extract_ubdc_extracts_csv(tmp_path: pathlib.Path) -> None:
    content = "uprn,transactionid,parentuprn,usrn\n30,{ABC},,12345\n"
    _make_ubdc_zip(tmp_path / "ppd-uprn-lookup.zip", content)
    result = dl.extract_ubdc(tmp_path)
    assert result.name == "ppd-uprn-lookup.csv"
    assert result.read_text() == content


def test_extract_ubdc_deletes_zip(tmp_path: pathlib.Path) -> None:
    ubdc_zip = tmp_path / "ppd-uprn-lookup.zip"
    _make_ubdc_zip(ubdc_zip, "uprn,transactionid\n30,{ABC}\n")
    dl.extract_ubdc(tmp_path)
    assert not ubdc_zip.exists()
