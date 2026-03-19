"""Tests for download.py: skip-if-exists, streaming, and auth headers."""

from __future__ import annotations

import pathlib
import zipfile
from unittest.mock import MagicMock, patch

import jsonschema
import pytest

import houseprices.download as dl

# ---------------------------------------------------------------------------
# OAS 3.0 schemas (from communitiesuk/epb-data-warehouse api.yml)
# Used to validate that mock responses match the documented API contract.
# ---------------------------------------------------------------------------

_FILE_INFO_SCHEMA = {
    "type": "object",
    "required": ["data"],
    "properties": {
        "data": {
            "type": "object",
            "required": ["fileSize", "lastUpdated"],
            "properties": {
                "fileSize": {"type": "integer"},
                "lastUpdated": {"type": "string"},
            },
        }
    },
}

_ERROR_SCHEMA = {
    "oneOf": [
        {
            "type": "object",
            "required": ["error"],
            "properties": {"error": {"type": "string"}},
        },
        {
            "type": "object",
            "required": ["errors"],
            "properties": {
                "errors": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"code": {"type": "string"}},
                    },
                }
            },
        },
    ]
}


def _mock_response(
    chunks: list[bytes] | None = None,
    content_length: int | None = None,
) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.iter_content.return_value = iter(chunks or [b"data"])
    resp.headers = (
        {"Content-Length": str(content_length)} if content_length is not None else {}
    )
    return resp


# ---------------------------------------------------------------------------
# _stream_to_file internals
# ---------------------------------------------------------------------------


def test_stream_to_file_uses_content_length_when_present(
    tmp_path: pathlib.Path,
) -> None:
    """Content-Length header is consumed and data is written correctly."""
    dest = tmp_path / "file.csv"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response([b"hello ", b"world"], content_length=11),
    ):
        dl._stream_to_file("http://example.com/f", dest)
    assert dest.read_bytes() == b"hello world"


def test_stream_to_file_handles_missing_content_length(tmp_path: pathlib.Path) -> None:
    """Missing Content-Length falls back gracefully (no total shown)."""
    dest = tmp_path / "file.csv"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response([b"data"]),  # headers={}, no Content-Length
    ):
        dl._stream_to_file("http://example.com/f", dest)
    assert dest.exists()


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
# download_epc — GOV.UK One Login bearer token auth
# ---------------------------------------------------------------------------


def test_download_epc_uses_bearer_token(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Authorization header must use Bearer token, not Basic auth."""
    monkeypatch.setenv("EPC_BEARER_TOKEN", "my-token-abc")
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_epc(tmp_path)
    headers = mock_get.call_args.kwargs["headers"]
    assert headers["Authorization"] == "Bearer my-token-abc"
    assert "Basic" not in headers["Authorization"]


def test_download_epc_saves_as_zip(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EPC_BEARER_TOKEN", "tok")
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_epc(tmp_path)
    assert result.name == "epc-domestic-all.zip"


def test_download_epc_raises_if_bearer_token_missing(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """KeyError if EPC_BEARER_TOKEN is not set in the environment."""
    monkeypatch.delenv("EPC_BEARER_TOKEN", raising=False)
    dl.EPC_BULK_URL = "http://example.com/epc.zip"
    with pytest.raises(KeyError):
        dl.download_epc(tmp_path)


# ---------------------------------------------------------------------------
# download_geolytix
# ---------------------------------------------------------------------------


def test_download_geolytix_uses_geolytix_url(tmp_path: pathlib.Path) -> None:
    """download_geolytix must GET GEOLYTIX_URL with no auth headers."""
    dl.GEOLYTIX_URL = "http://example.com/geolytix.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_geolytix(tmp_path)
    assert mock_get.call_args.args[0] == dl.GEOLYTIX_URL


def test_download_geolytix_saves_as_zip(tmp_path: pathlib.Path) -> None:
    dl.GEOLYTIX_URL = "http://example.com/geolytix.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ):
        result = dl.download_geolytix(tmp_path)
    assert result.name == "geolytix_postal_boundaries.zip"


def test_download_geolytix_skips_if_exists(tmp_path: pathlib.Path) -> None:
    (tmp_path / "geolytix_postal_boundaries.zip").write_bytes(b"existing")
    with patch("houseprices.download.requests.get") as mock_get:
        result = dl.download_geolytix(tmp_path)
    mock_get.assert_not_called()
    assert result.name == "geolytix_postal_boundaries.zip"


def test_download_geolytix_requires_no_auth(tmp_path: pathlib.Path) -> None:
    """No Authorization header should be sent — Google Drive is public."""
    dl.GEOLYTIX_URL = "http://example.com/geolytix.zip"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_response(),
    ) as mock_get:
        dl.download_geolytix(tmp_path)
    headers = mock_get.call_args.kwargs.get("headers", {})
    assert "Authorization" not in headers


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


def _make_epc_zip_by_la(path: pathlib.Path, las: dict[str, list[str]]) -> None:
    """Write a minimal EPC-style ZIP in the OLD per-LA format.

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


def _make_epc_zip_by_year(path: pathlib.Path, years: dict[str, list[str]]) -> None:
    """Write a minimal EPC-style ZIP in the NEW year-split format.

    *years* maps year string (e.g. '2023') to a list of data rows (without header).
    """
    header = "LMK_KEY,ADDRESS1,POSTCODE,TOTAL_FLOOR_AREA,UPRN,LODGEMENT_DATETIME\n"
    with zipfile.ZipFile(path, "w") as zf:
        for year, rows in years.items():
            certs = header + "".join(rows)
            zf.writestr(f"domestic-{year}.csv", certs)
        zf.writestr("LICENCE.txt", "OGL")


# Keep old name as alias for existing tests
_make_epc_zip = _make_epc_zip_by_la


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


# ---------------------------------------------------------------------------
# _meta_path / _save_meta / _load_meta
# ---------------------------------------------------------------------------


def test_meta_path_derives_json_sidecar(tmp_path: pathlib.Path) -> None:
    slim = tmp_path / "epc_slim.parquet"
    assert dl._meta_path(slim) == tmp_path / "epc_slim.meta.json"


def test_save_and_load_meta_round_trips(tmp_path: pathlib.Path) -> None:
    slim = tmp_path / "epc_slim.parquet"
    meta = {"ETag": '"abc123"', "Content-Length": "6400000000"}
    dl._save_meta(slim, meta)
    assert dl._load_meta(slim) == meta


def test_load_meta_returns_empty_when_file_absent(tmp_path: pathlib.Path) -> None:
    assert dl._load_meta(tmp_path / "nonexistent.parquet") == {}


def test_save_meta_is_noop_for_empty_dict(tmp_path: pathlib.Path) -> None:
    slim = tmp_path / "epc_slim.parquet"
    dl._save_meta(slim, {})
    assert not dl._meta_path(slim).exists()


# ---------------------------------------------------------------------------
# _meta_matches
# ---------------------------------------------------------------------------


def test_meta_matches_equal_etags() -> None:
    assert dl._meta_matches({"ETag": '"abc"'}, {"ETag": '"abc"'}) is True


def test_meta_matches_different_etags() -> None:
    assert dl._meta_matches({"ETag": '"abc"'}, {"ETag": '"xyz"'}) is False


def test_meta_matches_etag_priority_over_content_length() -> None:
    """ETag match wins even when Content-Length would differ."""
    stored = {"ETag": '"abc"', "Content-Length": "99"}
    remote = {"ETag": '"abc"', "Content-Length": "100"}
    assert dl._meta_matches(stored, remote) is True


def test_meta_matches_falls_back_to_last_modified() -> None:
    ts = "Mon, 01 Jan 2024 00:00:00 GMT"
    assert dl._meta_matches({"Last-Modified": ts}, {"Last-Modified": ts}) is True


def test_meta_matches_last_modified_change() -> None:
    assert (
        dl._meta_matches(
            {"Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"},
            {"Last-Modified": "Tue, 02 Jan 2024 00:00:00 GMT"},
        )
        is False
    )


def test_meta_matches_falls_back_to_content_length() -> None:
    assert dl._meta_matches({"Content-Length": "99"}, {"Content-Length": "99"}) is True


def test_meta_matches_content_length_change() -> None:
    assert (
        dl._meta_matches({"Content-Length": "99"}, {"Content-Length": "100"}) is False
    )


def test_meta_matches_no_common_keys_returns_false() -> None:
    assert dl._meta_matches({"ETag": '"x"'}, {"Last-Modified": "Mon..."}) is False


def test_meta_matches_empty_dicts_returns_false() -> None:
    assert dl._meta_matches({}, {}) is False


# ---------------------------------------------------------------------------
# _http_meta
# ---------------------------------------------------------------------------


def _fake_head_response(headers: dict[str, str]) -> MagicMock:
    r = MagicMock()
    r.headers = headers
    r.raise_for_status = MagicMock()
    return r


def test_http_meta_extracts_etag_last_modified_content_length(
    tmp_path: pathlib.Path,
) -> None:
    r = _fake_head_response(
        {
            "ETag": '"abc123"',
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "Content-Length": "12345",
            "Content-Type": "application/zip",  # excluded
        }
    )
    with patch("houseprices.download.requests.head", return_value=r):
        meta = dl._http_meta("https://example.com/file.zip")

    assert meta == {
        "ETag": '"abc123"',
        "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        "Content-Length": "12345",
    }


def test_http_meta_returns_empty_on_request_exception() -> None:
    import requests as _req

    with patch(
        "houseprices.download.requests.head",
        side_effect=_req.RequestException("timeout"),
    ):
        assert dl._http_meta("https://example.com/") == {}


def test_http_meta_returns_empty_on_unexpected_exception() -> None:
    with patch(
        "houseprices.download.requests.head",
        side_effect=Exception("SSL error"),
    ):
        assert dl._http_meta("https://example.com/") == {}


def test_http_meta_passes_auth_headers() -> None:
    r = _fake_head_response({"ETag": '"x"'})
    with patch("houseprices.download.requests.head", return_value=r) as mock_head:
        dl._http_meta("https://example.com/", headers={"Authorization": "Basic abc"})
    mock_head.assert_called_once_with(
        "https://example.com/",
        headers={"Authorization": "Basic abc"},
        timeout=30,
        allow_redirects=True,
    )


# ---------------------------------------------------------------------------
# _check_freshness
# ---------------------------------------------------------------------------


def test_check_freshness_slim_absent_returns_not_fresh(
    tmp_path: pathlib.Path,
) -> None:
    slim = tmp_path / "ppd_slim.parquet"
    with patch("houseprices.download._http_meta", return_value={"ETag": '"abc"'}):
        is_fresh, meta = dl._check_freshness(slim, "https://example.com/")
    assert is_fresh is False
    assert meta == {"ETag": '"abc"'}


def test_check_freshness_present_and_matching_etag(tmp_path: pathlib.Path) -> None:
    slim = tmp_path / "ppd_slim.parquet"
    slim.write_bytes(b"data")
    dl._save_meta(slim, {"ETag": '"abc"'})
    with patch("houseprices.download._http_meta", return_value={"ETag": '"abc"'}):
        is_fresh, _ = dl._check_freshness(slim, "https://example.com/")
    assert is_fresh is True


def test_check_freshness_present_and_stale(tmp_path: pathlib.Path) -> None:
    slim = tmp_path / "ppd_slim.parquet"
    slim.write_bytes(b"data")
    dl._save_meta(slim, {"ETag": '"old"'})
    with patch("houseprices.download._http_meta", return_value={"ETag": '"new"'}):
        is_fresh, meta = dl._check_freshness(slim, "https://example.com/")
    assert is_fresh is False
    assert meta["ETag"] == '"new"'


def test_check_freshness_present_no_stored_meta_treated_as_stale(
    tmp_path: pathlib.Path,
) -> None:
    """Parquet present, no .meta.json → stale so meta is written on next prepare."""
    slim = tmp_path / "ppd_slim.parquet"
    slim.write_bytes(b"data")
    with patch("houseprices.download._http_meta", return_value={"ETag": '"abc"'}):
        is_fresh, meta = dl._check_freshness(slim, "https://example.com/")
    assert is_fresh is False
    assert meta == {"ETag": '"abc"'}


def test_check_freshness_head_fails_trusts_existing_parquet(
    tmp_path: pathlib.Path,
) -> None:
    """Network failure when parquet exists → keep it, don't force a re-download."""
    slim = tmp_path / "ppd_slim.parquet"
    slim.write_bytes(b"data")
    dl._save_meta(slim, {"ETag": '"abc"'})
    with patch("houseprices.download._http_meta", return_value={}):
        is_fresh, meta = dl._check_freshness(slim, "https://example.com/")
    assert is_fresh is True
    assert meta == {}


def test_check_freshness_head_fails_slim_absent_returns_not_fresh(
    tmp_path: pathlib.Path,
) -> None:
    """Network failure with no parquet → return not-fresh so download proceeds."""
    slim = tmp_path / "ppd_slim.parquet"
    with patch("houseprices.download._http_meta", return_value={}):
        is_fresh, meta = dl._check_freshness(slim, "https://example.com/")
    assert is_fresh is False


# ---------------------------------------------------------------------------
# download_cpi
# ---------------------------------------------------------------------------

# ONS generator returns a quoted CSV: 8 metadata rows, then annual/quarterly/monthly.
# The parser must skip the header and annual/quarterly rows, keeping only monthly ones.
_ONS_CSV_RESPONSE = (
    '"Title","CPI INDEX 00: ALL ITEMS 2015=100"\n'
    '"CDID","D7BT"\n'
    '"Source dataset ID","MM23"\n'
    '"PreUnit",""\n'
    '"Unit","Index, base year = 100"\n'
    '"Release date","18-02-2026"\n'
    '"Next release","25 March 2026"\n'
    '"Important notes",\n'
    '"2021","114.3"\n'  # annual row — must be skipped
    '"2021 Q2","116.0"\n'  # quarterly row — must be skipped
    '"2021 JUN","116.5"\n'  # monthly — must be kept
    '"2026 JAN","140.0"\n'  # monthly — must be kept
)


def _mock_csv_response(text: str) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = text
    return resp


def test_download_cpi_uses_ons_cpi_url(tmp_path: pathlib.Path) -> None:
    """download_cpi must GET the ONS_CPI_URL constant."""
    dl.ONS_CPI_URL = "http://example.com/cpi"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_csv_response(_ONS_CSV_RESPONSE),
    ) as mock_get:
        dl.download_cpi(tmp_path)
    assert mock_get.call_args.args[0] == dl.ONS_CPI_URL


def test_download_cpi_saves_as_cpi_csv(tmp_path: pathlib.Path) -> None:
    """Output file must be named cpi.csv."""
    dl.ONS_CPI_URL = "http://example.com/cpi"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_csv_response(_ONS_CSV_RESPONSE),
    ):
        result = dl.download_cpi(tmp_path)
    assert result.name == "cpi.csv"


def test_download_cpi_skips_if_csv_exists(tmp_path: pathlib.Path) -> None:
    """Skip download if cpi.csv already exists."""
    existing = tmp_path / "cpi.csv"
    existing.write_text("date,cpi\n2021-06,116.5\n")
    with patch("houseprices.download.requests.get") as mock_get:
        result = dl.download_cpi(tmp_path)
    mock_get.assert_not_called()
    assert result == existing


def test_download_cpi_parses_months_to_csv_rows(tmp_path: pathlib.Path) -> None:
    """Monthly rows from the ONS CSV are written as date,cpi rows."""
    dl.ONS_CPI_URL = "http://example.com/cpi"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_csv_response(_ONS_CSV_RESPONSE),
    ):
        result = dl.download_cpi(tmp_path)
    lines = result.read_text().splitlines()
    assert lines[0] == "date,cpi"
    assert "2021-06" in lines[1]
    assert "116.5" in lines[1]
    assert "2026-01" in lines[2]
    assert "140.0" in lines[2]


def test_download_cpi_skips_annual_and_quarterly_rows(tmp_path: pathlib.Path) -> None:
    """Annual ("2021") and quarterly ("2021 Q2") rows must not appear in output."""
    dl.ONS_CPI_URL = "http://example.com/cpi"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_csv_response(_ONS_CSV_RESPONSE),
    ):
        result = dl.download_cpi(tmp_path)
    content = result.read_text()
    assert "2021," not in content  # annual row date would be bare year
    assert "Q2" not in content  # quarterly label must not appear


def test_download_cpi_date_format_is_yyyy_mm(tmp_path: pathlib.Path) -> None:
    """All date values in the output must match YYYY-MM."""
    import re

    dl.ONS_CPI_URL = "http://example.com/cpi"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_csv_response(_ONS_CSV_RESPONSE),
    ):
        result = dl.download_cpi(tmp_path)
    for line in result.read_text().splitlines()[1:]:
        date_part = line.split(",")[0]
        assert re.fullmatch(r"\d{4}-\d{2}", date_part), f"Bad date format: {date_part}"


def test_download_cpi_raises_on_http_error(tmp_path: pathlib.Path) -> None:
    """HTTP error from the ONS endpoint must propagate."""
    dl.ONS_CPI_URL = "http://example.com/cpi"
    resp = MagicMock()
    resp.raise_for_status.side_effect = Exception("503 Service Unavailable")
    with (
        patch("houseprices.download.requests.get", return_value=resp),
        pytest.raises(Exception, match="503"),
    ):
        dl.download_cpi(tmp_path)


# ---------------------------------------------------------------------------
# EPC info endpoint and freshness — OAS-validated
# ---------------------------------------------------------------------------

_VALID_FILE_INFO_RESPONSE = {
    "data": {
        "fileSize": 2923946932,
        "lastUpdated": "2026-03-01T00:31:19.000+00:00",
    }
}


def _mock_json_response(
    payload: dict,  # type: ignore[type-arg]
    status_code: int = 200,
) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    resp.json.return_value = payload
    return resp


def test_file_info_response_matches_oas_schema() -> None:
    """Verify our test fixture itself conforms to the OAS FileInfoResponse schema."""
    jsonschema.validate(_VALID_FILE_INFO_RESPONSE, _FILE_INFO_SCHEMA)


def test_error_response_matches_oas_schema_single_error() -> None:
    payload = {"error": "File not found"}
    jsonschema.validate(payload, _ERROR_SCHEMA)


def test_error_response_matches_oas_schema_errors_array() -> None:
    payload = {"errors": [{"code": "Unexpected error message here"}]}
    jsonschema.validate(payload, _ERROR_SCHEMA)


def test_epc_last_updated_returns_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_epc_last_updated makes a GET to the info endpoint and returns lastUpdated."""
    monkeypatch.setenv("EPC_BEARER_TOKEN", "tok")
    dl.EPC_INFO_URL = "http://example.com/epc/info"
    with patch(
        "houseprices.download.requests.get",
        return_value=_mock_json_response(_VALID_FILE_INFO_RESPONSE),
    ) as mock_get:
        result = dl._epc_last_updated("tok")
    assert result == "2026-03-01T00:31:19.000+00:00"
    assert mock_get.call_args.kwargs["headers"]["Authorization"] == "Bearer tok"


def test_epc_last_updated_returns_empty_on_network_error() -> None:
    """Network failure must return '' so callers treat it as unknown."""
    import requests as _req

    dl.EPC_INFO_URL = "http://example.com/epc/info"
    with patch(
        "houseprices.download.requests.get",
        side_effect=_req.RequestException("timeout"),
    ):
        assert dl._epc_last_updated("tok") == ""


def test_epc_last_updated_returns_empty_on_401() -> None:
    """401 Unauthorized must return '' (bad token — don't crash)."""
    resp = MagicMock()
    resp.status_code = 401
    import requests as _req

    resp.raise_for_status.side_effect = _req.HTTPError("401")
    dl.EPC_INFO_URL = "http://example.com/epc/info"
    with patch("houseprices.download.requests.get", return_value=resp):
        assert dl._epc_last_updated("bad-token") == ""


def test_check_epc_freshness_slim_absent_returns_not_fresh(
    tmp_path: pathlib.Path,
) -> None:
    slim = tmp_path / "epc_slim.parquet"
    dl.EPC_INFO_URL = "http://example.com/epc/info"
    with patch(
        "houseprices.download._epc_last_updated",
        return_value="2026-03-01T00:00:00.000+00:00",
    ):
        is_fresh, meta = dl._check_epc_freshness(slim, "tok")
    assert is_fresh is False
    assert meta["lastUpdated"] == "2026-03-01T00:00:00.000+00:00"


def test_check_epc_freshness_matching_timestamp_is_fresh(
    tmp_path: pathlib.Path,
) -> None:
    slim = tmp_path / "epc_slim.parquet"
    slim.write_bytes(b"data")
    ts = "2026-03-01T00:00:00.000+00:00"
    dl._save_meta(slim, {"lastUpdated": ts})
    with patch("houseprices.download._epc_last_updated", return_value=ts):
        is_fresh, _ = dl._check_epc_freshness(slim, "tok")
    assert is_fresh is True


def test_check_epc_freshness_changed_timestamp_is_stale(
    tmp_path: pathlib.Path,
) -> None:
    slim = tmp_path / "epc_slim.parquet"
    slim.write_bytes(b"data")
    dl._save_meta(slim, {"lastUpdated": "2026-02-01T00:00:00.000+00:00"})
    with patch(
        "houseprices.download._epc_last_updated",
        return_value="2026-03-01T00:00:00.000+00:00",
    ):
        is_fresh, meta = dl._check_epc_freshness(slim, "tok")
    assert is_fresh is False
    assert meta["lastUpdated"] == "2026-03-01T00:00:00.000+00:00"


def test_check_epc_freshness_network_failure_trusts_existing(
    tmp_path: pathlib.Path,
) -> None:
    """Info endpoint unreachable + slim present → keep existing, return fresh."""
    slim = tmp_path / "epc_slim.parquet"
    slim.write_bytes(b"data")
    dl._save_meta(slim, {"lastUpdated": "2026-02-01T00:00:00.000+00:00"})
    with patch("houseprices.download._epc_last_updated", return_value=""):
        is_fresh, meta = dl._check_epc_freshness(slim, "tok")
    assert is_fresh is True
    assert meta == {}


def test_check_epc_freshness_no_stored_meta_treated_as_stale(
    tmp_path: pathlib.Path,
) -> None:
    """Slim present, no meta.json → stale so lastUpdated gets written this run."""
    slim = tmp_path / "epc_slim.parquet"
    slim.write_bytes(b"data")
    with patch(
        "houseprices.download._epc_last_updated",
        return_value="2026-03-01T00:00:00.000+00:00",
    ):
        is_fresh, meta = dl._check_epc_freshness(slim, "tok")
    assert is_fresh is False
    assert "lastUpdated" in meta


# ---------------------------------------------------------------------------
# 429 retry logic
# ---------------------------------------------------------------------------


def test_stream_to_file_retries_on_429(tmp_path: pathlib.Path) -> None:
    """A 429 response triggers a retry; the second attempt succeeds."""
    dest = tmp_path / "file.zip"
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.raise_for_status.side_effect = None

    ok = _mock_response([b"data"])
    ok.status_code = 200

    with (
        patch(
            "houseprices.download.requests.get",
            side_effect=[rate_limited, ok],
        ),
        patch("houseprices.download.time.sleep") as mock_sleep,
    ):
        dl._stream_to_file("http://example.com/f", dest)

    mock_sleep.assert_called_once()
    assert dest.read_bytes() == b"data"


def test_stream_to_file_raises_after_max_retries(tmp_path: pathlib.Path) -> None:
    """Persistent 429 beyond max_retries raises an exception."""
    import requests as _req

    dest = tmp_path / "file.zip"
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.raise_for_status.side_effect = _req.HTTPError("429")

    with (
        patch(
            "houseprices.download.requests.get",
            return_value=rate_limited,
        ),
        patch("houseprices.download.time.sleep"),
        pytest.raises(_req.HTTPError, match="429"),
    ):
        dl._stream_to_file("http://example.com/f", dest, max_retries=2)


def test_stream_to_file_does_not_retry_on_other_errors(
    tmp_path: pathlib.Path,
) -> None:
    """Non-429 HTTP errors are not retried."""
    import requests as _req

    dest = tmp_path / "file.zip"
    resp = MagicMock()
    resp.status_code = 500
    resp.raise_for_status.side_effect = _req.HTTPError("500")

    with (
        patch("houseprices.download.requests.get", return_value=resp),
        patch("houseprices.download.time.sleep") as mock_sleep,
        pytest.raises(_req.HTTPError, match="500"),
    ):
        dl._stream_to_file("http://example.com/f", dest, max_retries=3)

    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# extract_epc — year-split ZIP format (new MHCLG service)
# ---------------------------------------------------------------------------


def test_extract_epc_handles_year_split_zip(tmp_path: pathlib.Path) -> None:
    """New year-split ZIP format: domestic-YYYY.csv files are concatenated."""
    _make_epc_zip_by_year(
        tmp_path / "epc-domestic-all.zip",
        {
            "2022": ["k1,a,SW1A1AA,80,100,2022-01-01\n"],
            "2023": ["k2,b,SW1A2AA,90,200,2023-06-01\n"],
        },
    )
    result = dl.extract_epc(tmp_path)
    lines = result.read_text().splitlines()
    header = "LMK_KEY,ADDRESS1,POSTCODE,TOTAL_FLOOR_AREA,UPRN,LODGEMENT_DATETIME"
    assert lines[0] == header
    assert lines.count(header) == 1
    assert len(lines) == 3  # 1 header + 2 data rows


def test_extract_epc_year_split_excludes_licence_file(tmp_path: pathlib.Path) -> None:
    """LICENCE.txt (non-CSV) in the new ZIP must not be processed."""
    _make_epc_zip_by_year(
        tmp_path / "epc-domestic-all.zip",
        {"2023": ["k1,a,SW1A1AA,80,100,2023-01-01\n"]},
    )
    result = dl.extract_epc(tmp_path)
    assert "OGL" not in result.read_text()


def test_extract_epc_both_formats_produce_same_schema(
    tmp_path: pathlib.Path,
) -> None:
    """Old per-LA and new year-split ZIPs produce the same output schema."""
    row = "k1,a,SW1A1AA,80,100,2022-01-01\n"

    la_zip = tmp_path / "la.zip"
    _make_epc_zip_by_la(la_zip, {"LA1": [row]})
    la_zip.rename(tmp_path / "epc-domestic-all.zip")
    la_result = dl.extract_epc(tmp_path)
    la_header = la_result.read_text().splitlines()[0]
    la_result.unlink()

    yr_zip = tmp_path / "yr.zip"
    _make_epc_zip_by_year(yr_zip, {"2022": [row]})
    yr_zip.rename(tmp_path / "epc-domestic-all.zip")
    yr_result = dl.extract_epc(tmp_path)
    yr_header = yr_result.read_text().splitlines()[0]

    assert la_header == yr_header
