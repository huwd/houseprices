"""Download raw data files from upstream sources.

URL constants are declared at module level so they can be inspected and
overridden without touching the download functions.  Fill in the TODO
entries once you have confirmed the direct-download URLs.

Credentials are read from environment variables at call time.  Copy
.env.example to .env and fill in your values; python-dotenv loads the
file automatically when this module is imported.
"""

import base64
import os
import pathlib
import shutil
import subprocess
import zipfile

import requests
from dotenv import load_dotenv

load_dotenv()

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
# Authenticates via HTTP Basic Auth (EPC_EMAIL + EPC_API_KEY).
# List available files: GET https://epc.opendatacommunities.org/api/v1/files
EPC_BULK_URL = (
    "https://epc.opendatacommunities.org/api/v1/files/all-domestic-certificates.zip"
)

# UBDC PPD → UPRN lookup — ZIP containing CSV (OGL).
# Dataset page: https://data.ubdc.ac.uk/datasets/a999fd05-e7fe-4243-ab9a-95ce98132956
# Returns JSON {"download": {"url": "<signed-azure-url>"}} — URL is time-limited.
# download_ubdc() resolves the signed URL at call time before streaming.
UBDC_URL = "https://data.ubdc.ac.uk/api/resources/download?file_id=37&dataset_id=13"

# OS Open UPRN — ZIP of all UPRNs with BNG coordinates (OGL).
# Free bulk download via OS Data Hub Downloads API; no API key or account required.
# CRS: BNG EPSG:27700 (X_COORDINATE, Y_COORDINATE columns).
# Updated February 2026; ~616 MB zipped.
OS_OPEN_UPRN_URL = (
    "https://api.os.uk/downloads/v1/products/OpenUPRN/downloads"
    "?area=GB&format=CSV&redirect"
)

# ONS LSOA December 2021 Boundaries EW BGC V5 — FGDB (OGL).
# Source: ONS Open Geography Portal (ArcGIS Hub), item 68515293204e43ca8ab56fa13ae8a547.
# Only FGDB is pre-cached; GeoPackage/Shapefile generation returns 500.
# download_lsoa_boundaries() downloads this and converts to GeoPackage via ogr2ogr,
# reprojecting to BNG EPSG:27700 to match OS Open UPRN. ~18 MB zipped.
LSOA_BGC_URL = (
    "https://opendata.arcgis.com/api/v3/datasets"
    "/68515293204e43ca8ab56fa13ae8a547_0/downloads/data"
    "?format=fgdb&spatialRefId=4326"
)

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


def download_epc(data_dir: pathlib.Path) -> pathlib.Path:
    """Download the EPC bulk ZIP.

    Reads EPC_EMAIL and EPC_API_KEY from the environment (.env or shell).
    Authenticates via HTTP Basic Auth per the EPC open data API documentation:
    https://epc.opendatacommunities.org/docs/api/domestic#downloads
    """
    email = os.environ["EPC_EMAIL"]
    api_key = os.environ["EPC_API_KEY"]
    token = base64.b64encode(f"{email}:{api_key}".encode()).decode()
    return _stream_to_file(
        EPC_BULK_URL,
        data_dir / "epc-domestic-all.zip",
        headers={"Authorization": f"Basic {token}"},
    )


def download_ubdc(data_dir: pathlib.Path) -> pathlib.Path:
    """Download the UBDC PPD → UPRN lookup ZIP.

    The UBDC API returns a time-limited pre-signed Azure blob URL. This
    function resolves that URL first, then streams the ZIP.
    """
    response = requests.get(UBDC_URL, timeout=30)
    response.raise_for_status()
    signed_url: str = response.json()["download"]["url"]
    return _stream_to_file(signed_url, data_dir / "ppd-uprn-lookup.zip")


def download_os_open_uprn(data_dir: pathlib.Path) -> pathlib.Path:
    """Download OS Open UPRN ZIP.

    No API key required — OS Open UPRN is free open data under OGL.
    A free OS OpenData Plan account is needed to obtain the download URL
    (sign up at https://osdatahub.os.uk/plans, then visit the download page
    at https://osdatahub.os.uk/downloads/open/OpenUPRN and select CSV format).
    Set OS_OPEN_UPRN_URL to the URL shown on that page before calling this.
    """
    return _stream_to_file(OS_OPEN_UPRN_URL, data_dir / "os-open-uprn.zip")


def download_lsoa_boundaries(data_dir: pathlib.Path) -> pathlib.Path:
    """Download ONS LSOA December 2021 BGC boundaries as a GeoPackage.

    The ArcGIS Hub only pre-caches this dataset as FGDB. This function:
      1. Downloads the FGDB ZIP (~18 MB)
      2. Extracts it to a temporary directory
      3. Converts to GeoPackage in BNG EPSG:27700 via ogr2ogr
      4. Removes the ZIP and temporary directory

    The output matches the CRS of OS Open UPRN (BNG EPSG:27700), so no
    reprojection is needed in spatial.py.

    Requires ogr2ogr (GDAL) to be available on PATH.
    Skips all steps if lsoa_boundaries.gpkg already exists.
    """
    dest = data_dir / "lsoa_boundaries.gpkg"
    if dest.exists():
        print(f"  [skip] {dest.name} (already downloaded)")
        return dest

    fgdb_zip = _stream_to_file(LSOA_BGC_URL, data_dir / "lsoa_boundaries.fgdb.zip")

    print(f"  [convert] {fgdb_zip.name} → {dest.name}")
    tmp_dir = data_dir / "_lsoa_fgdb_tmp"
    try:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(fgdb_zip, "r") as zf:
            zf.extractall(tmp_dir)

        gdb_dirs = list(tmp_dir.glob("*.gdb"))
        if not gdb_dirs:
            raise FileNotFoundError(f"No .gdb directory found in {fgdb_zip.name}")

        subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "GPKG",
                str(dest),
                str(gdb_dirs[0]),
                "-t_srs",
                "EPSG:27700",
                "-select",
                "LSOA21CD,LSOA21NM",
            ],
            check=True,
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        fgdb_zip.unlink(missing_ok=True)

    return dest


# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------


def extract_epc(data_dir: pathlib.Path) -> pathlib.Path:
    """Concatenate all per-LA certificates.csv files from the EPC ZIP.

    The EPC bulk ZIP contains one folder per local authority, each with a
    certificates.csv and a recommendations.csv.  This function streams all
    347 certificates.csv files into a single epc-domestic-all.csv, writing
    the header once and skipping recommendations files entirely.

    The source ZIP is deleted after successful extraction.
    Skips if epc-domestic-all.csv already exists.
    """
    dest = data_dir / "epc-domestic-all.csv"
    if dest.exists():
        print(f"  [skip] {dest.name} (already extracted)")
        return dest

    src = data_dir / "epc-domestic-all.zip"
    print(f"  [extract] {src.name} → {dest.name}")

    with zipfile.ZipFile(src, "r") as zf:
        cert_files = [n for n in zf.namelist() if n.endswith("certificates.csv")]
        with dest.open("wb") as out:
            header_written = False
            for name in cert_files:
                with zf.open(name) as f:
                    header = f.readline()
                    if not header_written:
                        out.write(header)
                        header_written = True
                    for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                        out.write(chunk)

    src.unlink()
    return dest


def extract_os_open_uprn(data_dir: pathlib.Path) -> pathlib.Path:
    """Extract the OS Open UPRN CSV from its ZIP, stripping the UTF-8 BOM.

    The ZIP contains a single versioned CSV (e.g. osopenuprn_202602.csv).
    It is extracted and renamed to os-open-uprn.csv.  The UTF-8 BOM that OS
    ships in the file is stripped so downstream readers don't see a mangled
    first column name.

    The source ZIP is deleted after successful extraction.
    Skips if os-open-uprn.csv already exists.
    """
    dest = data_dir / "os-open-uprn.csv"
    if dest.exists():
        print(f"  [skip] {dest.name} (already extracted)")
        return dest

    src = data_dir / "os-open-uprn.zip"
    print(f"  [extract] {src.name} → {dest.name}")

    with zipfile.ZipFile(src, "r") as zf:
        csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
        with zf.open(csv_files[0]) as f, dest.open("wb") as out:
            first_chunk = f.read(_CHUNK_SIZE)
            out.write(first_chunk.lstrip(b"\xef\xbb\xbf"))  # strip UTF-8 BOM
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                out.write(chunk)

    src.unlink()
    return dest


def extract_ubdc(data_dir: pathlib.Path) -> pathlib.Path:
    """Extract the UBDC PPD → UPRN lookup CSV from its ZIP.

    The ZIP contains a single CSV (ppdid_uprn_usrn.csv). It is extracted
    and renamed to ppd-uprn-lookup.csv.

    The source ZIP is deleted after successful extraction.
    Skips if ppd-uprn-lookup.csv already exists.
    """
    dest = data_dir / "ppd-uprn-lookup.csv"
    if dest.exists():
        print(f"  [skip] {dest.name} (already extracted)")
        return dest

    src = data_dir / "ppd-uprn-lookup.zip"
    print(f"  [extract] {src.name} → {dest.name}")

    with zipfile.ZipFile(src, "r") as zf:
        csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
        with zf.open(csv_files[0]) as f, dest.open("wb") as out:
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                out.write(chunk)

    src.unlink()
    return dest


if __name__ == "__main__":  # pragma: no cover
    data = pathlib.Path("data")
    data.mkdir(exist_ok=True)
    download_ppd(data)
    download_epc(data)
    extract_epc(data)
    download_ubdc(data)
    extract_ubdc(data)
    download_os_open_uprn(data)
    extract_os_open_uprn(data)
    download_lsoa_boundaries(data)
