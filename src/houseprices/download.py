"""Download raw data files from upstream sources.

URL constants are declared at module level so they can be inspected and
overridden without touching the download functions.  Fill in the TODO
entries once you have confirmed the direct-download URLs.
"""

import base64
import pathlib

import requests

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------

# HM Land Registry Price Paid Data — complete CSV (OGL).
# Updated monthly; currently includes sales through January 2026.
PPD_URL = (
    "http://prod.publicdata.landregistry.gov.uk"
    ".s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
)

# EPC bulk download — ZIP of all domestic certificates (OGL).
# Requires free registration at https://epc.opendatacommunities.org/
# Authenticates via HTTP Basic Auth: email address + API key.
# TODO: confirm the exact bulk-download endpoint URL after registration.
EPC_BULK_URL: str = ""

# UBDC PPD → UPRN lookup — ZIP containing CSV (OGL).
# Dataset page: https://data.ubdc.ac.uk/dataset/a999fd05-e7fe-4243-ab9a-95ce98132956
# TODO: confirm the direct-download URL from the dataset page.
UBDC_URL: str = ""

# OS Open UPRN — ZIP of all UPRNs with BNG coordinates (OGL).
# Requires a free API key from https://osdatahub.os.uk/
# TODO: confirm the download endpoint; key is appended as ?key={api_key}.
OS_OPEN_UPRN_URL: str = ""

# ONS LSOA December 2021 Boundaries EW BGC — GeoPackage, BNG (OGL).
# See research/ons-boundary-format.md for format and CRS guidance.
# TODO: confirm the direct-download URL from https://geoportal.statistics.gov.uk/
LSOA_BGC_URL: str = ""

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 1024 * 1024  # 1 MB


def _stream_to_file(
    url: str,
    dest: pathlib.Path,
    *,
    headers: dict[str, str] | None = None,
) -> pathlib.Path:
    """Stream *url* to *dest*, skipping if the file already exists."""
    if dest.exists():
        print(f"  [skip] {dest.name} (already downloaded)")
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  [get]  {url} → {dest}")

    response = requests.get(url, headers=headers or {}, stream=True, timeout=120)
    response.raise_for_status()

    with dest.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
            fh.write(chunk)

    return dest


# ---------------------------------------------------------------------------
# Public download functions
# ---------------------------------------------------------------------------


def download_ppd(data_dir: pathlib.Path) -> pathlib.Path:
    """Download the complete Price Paid Data CSV."""
    return _stream_to_file(PPD_URL, data_dir / "pp-complete.csv")


def download_epc(
    data_dir: pathlib.Path,
    *,
    email: str,
    api_key: str,
) -> pathlib.Path:
    """Download the EPC bulk ZIP.

    Credentials are from the free epc.opendatacommunities.org registration.
    Authenticates via HTTP Basic Auth (email address + API key).
    """
    token = base64.b64encode(f"{email}:{api_key}".encode()).decode()
    return _stream_to_file(
        EPC_BULK_URL,
        data_dir / "epc-domestic-all.zip",
        headers={"Authorization": f"Basic {token}"},
    )


def download_ubdc(data_dir: pathlib.Path) -> pathlib.Path:
    """Download the UBDC PPD → UPRN lookup ZIP."""
    return _stream_to_file(UBDC_URL, data_dir / "ppd-uprn-lookup.zip")


def download_os_open_uprn(
    data_dir: pathlib.Path,
    *,
    api_key: str,
) -> pathlib.Path:
    """Download OS Open UPRN ZIP.

    Requires a free API key from https://osdatahub.os.uk/
    The key is appended as a query parameter — confirm the exact mechanism
    against the OS Data Hub download documentation before running.
    """
    url = f"{OS_OPEN_UPRN_URL}?key={api_key}"
    return _stream_to_file(url, data_dir / "os-open-uprn.zip")


def download_lsoa_boundaries(data_dir: pathlib.Path) -> pathlib.Path:
    """Download ONS LSOA December 2021 BGC boundaries as a GeoPackage."""
    return _stream_to_file(LSOA_BGC_URL, data_dir / "lsoa_boundaries.gpkg")
