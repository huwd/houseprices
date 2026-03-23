"""Main pipeline: download → join → aggregate."""

import datetime
import json
import os
import pathlib
import re
import sys
import tempfile
import time
from collections.abc import Callable
from enum import Enum

import duckdb
import pandas as pd
from dotenv import load_dotenv
from rich.console import Console

from houseprices.spatial import build_uprn_lsoa

load_dotenv()

# ---------------------------------------------------------------------------
# Default paths (relative to project root)
# ---------------------------------------------------------------------------

DATA = pathlib.Path("data")
CACHE = pathlib.Path("cache")
OUTPUT = pathlib.Path("output")

# Base month for CPI deflation — all sale prices are converted to real terms
# relative to this month.  January 2026 chosen to match the pipeline's
# first production run.
CPI_BASE: tuple[int, int] = (2026, 1)


class Geography(Enum):
    """Contiguous geographies supported for price-per-sqm aggregation.

    The enum value is the output column name in the aggregated DataFrame.
    For POSTCODE_DISTRICT the column is derived from the ``postcode`` field.
    For all other geographies the column must already be present in the
    matched DataFrame (populated by the spatial join step).
    """

    POSTCODE_DISTRICT = "postcode_district"
    LSOA = "LSOA21CD"


# Postcode districts that have no boundary polygon in our boundary file and
# must be folded into a neighbouring district before aggregation.
#
# E20 (Queen Elizabeth Olympic Park / East Village) was carved out of E15 by
# Royal Mail circa 2012 — after our Geolytix boundary snapshot.  Remapping to
# E15 is a reasonable interim approximation; remove once issue #81 (a proper
# E20 polygon) is resolved.
POSTCODE_DISTRICT_OVERRIDES: dict[str, str] = {
    "E20": "E15",  # Olympic Park — carved out of E15 circa 2012; no boundary polygon
}

_ABBREVIATIONS: list[tuple[str, str]] = [
    (r"\bAPARTMENT\b", "FLAT"),
    (r"\bUNIT\b", "FLAT"),
    (r"\bRD\b", "ROAD"),
    (r"\bAVE?\b", "AVENUE"),
    (r"\bDR\b", "DRIVE"),
    (r"\bCL\b", "CLOSE"),
    (r"\bCT\b", "COURT"),
    (r"\bGDNS\b", "GARDENS"),
    (r"\bHSE\b", "HOUSE"),
]


def normalise_address(saon: str, paon: str, street: str) -> str:
    """Normalise address components into a single uppercase string for matching."""
    parts = " ".join(filter(None, [saon, paon, street]))
    parts = parts.upper()
    parts = re.sub(r"-", " ", parts)
    parts = re.sub(r"[^\w\s]", "", parts)
    parts = re.sub(r"\s+", " ", parts).strip()
    parts = re.sub(r"\bTHE\b", "", parts)
    parts = re.sub(r"\s+", " ", parts).strip()
    for pattern, replacement in _ABBREVIATIONS:
        parts = re.sub(pattern, replacement, parts)
    return parts


def _sql_source(path: str | pathlib.Path) -> str:
    """Return a DuckDB table expression for *path* (CSV or Parquet)."""
    p = str(path)
    return f"read_parquet('{p}')" if p.endswith(".parquet") else f"read_csv('{p}')"


_PPD_NAMES = [
    "transaction_unique_identifier",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "new_build_flag",
    "tenure_type",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
]


def _ppd_source(path: str | pathlib.Path) -> str:
    """Return a DuckDB table expression for a PPD file (CSV or Parquet).

    Parquet files are read directly.  CSV files are read with ``header=false``
    and the canonical PPD column names assigned, matching the schema HMLR
    publishes (no header row, 16 columns).
    """
    p = str(path)
    if p.endswith(".parquet"):
        return f"read_parquet('{p}')"
    names = ", ".join(f"'{n}'" for n in _PPD_NAMES)
    return f"read_csv('{p}', header=false, ignore_errors=true, names=[{names}])"


def _configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """Apply resource limits from environment variables to a DuckDB connection.

    Reads ``DUCKDB_MEMORY_LIMIT`` and ``DUCKDB_THREADS`` from the environment.
    When set, the corresponding DuckDB ``SET`` statements are issued so the
    engine spills to disk rather than exhausting system RAM.  If a variable is
    absent or empty the DuckDB default is preserved (no limit / all cores).

    Typical ``.env`` values for an 8 GB laptop::

        DUCKDB_MEMORY_LIMIT=4GB
        DUCKDB_THREADS=2
    """
    con.execute("SET preserve_insertion_order = false")
    memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT")
    threads = os.environ.get("DUCKDB_THREADS")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if threads:
        con.execute(f"SET threads = {int(threads)}")


def prepare_ppd(src: str | pathlib.Path, dst: pathlib.Path) -> None:
    """Write a category-A-only, column-named Parquet from the PPD CSV.

    Filters to ppd_category_type = 'A' (standard residential sales) and
    retains all 16 PPD columns.  No-ops if *dst* already exists.
    """
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    _configure_duckdb(con)
    src_expr = _ppd_source(src)
    con.execute(f"""
        COPY (
            SELECT * FROM {src_expr}
            WHERE ppd_category_type = 'A'
        ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


def prepare_epc(
    src: str | pathlib.Path,
    dst: pathlib.Path,
    *,
    deduplicate: bool = True,
) -> None:
    """Write a column-pruned Parquet from the EPC CSV.

    Selects the 9 columns used by the pipeline.  When *deduplicate* is
    ``True`` (default) the output keeps only the most recent certificate per
    UPRN (by LODGEMENT_DATETIME DESC); rows without a UPRN are kept as-is
    (Tier 2 candidates).  When *deduplicate* is ``False`` all rows are kept,
    which is required for Tier 1 temporal matching.

    No-ops if *dst* already exists.

    Uses a two-step approach to stay within the DuckDB memory limit:

    1. Stream the 60-column CSV down to a 9-column slim Parquet (no sort).
    2. When *deduplicate* is True: deduplicate the slim Parquet with GROUP BY
       + MAX_BY, which only keeps one hash-table entry per UPRN —
       O(unique UPRNs) memory rather than O(total rows).
       When *deduplicate* is False: step 2 is skipped; the slim Parquet is
       renamed to *dst* directly.
    """
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    src_str = str(src)
    tmp = dst.with_suffix(".tmp.parquet")
    con = duckdb.connect()
    _configure_duckdb(con)
    try:
        # Step 1: column projection — pure streaming, no window or sort.
        con.execute(f"""
            COPY (
                SELECT
                    UPRN, LODGEMENT_DATETIME, TOTAL_FLOOR_AREA,
                    ADDRESS1, ADDRESS2, POSTCODE,
                    BUILT_FORM, CONSTRUCTION_AGE_BAND, CURRENT_ENERGY_RATING
                FROM read_csv(
                    '{src_str}',
                    quote='"',
                    escape='"',
                    strict_mode=false,
                    null_padding=true,
                    parallel=false
                )
            ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        if deduplicate:
            # Step 2: deduplicate from the slim Parquet.
            # Split into two passes to avoid a hash table that is too large
            # for the memory limit.  A single GROUP BY UPRN with 8× MAX_BY
            # columns holds ~15 M groups × ~125 bytes each ≈ 2+ GB — at or
            # beyond DUCKDB_MEMORY_LIMIT on a 2 GB config.
            #
            # Pass 2a: aggregate only UPRN → max datetime (hash table ≈ 400 MB).
            con.execute(f"""
                CREATE TEMP TABLE _uprn_max AS
                SELECT UPRN, MAX(LODGEMENT_DATETIME) AS max_dt
                FROM read_parquet('{tmp}')
                WHERE UPRN IS NOT NULL
                GROUP BY UPRN
            """)
            # Pass 2b: equijoin back to fetch all columns for the winning row.
            # The build side is _uprn_max (~400 MB); the probe side streams.
            # ROW_NUMBER handles the rare case of two certs sharing the same
            # max datetime for the same UPRN (ties get an arbitrary winner).
            con.execute(f"""
                COPY (
                    SELECT
                        UPRN, LODGEMENT_DATETIME, TOTAL_FLOOR_AREA,
                        ADDRESS1, ADDRESS2, POSTCODE,
                        BUILT_FORM, CONSTRUCTION_AGE_BAND, CURRENT_ENERGY_RATING
                    FROM (
                        SELECT t.*,
                            ROW_NUMBER() OVER (PARTITION BY t.UPRN) AS rn
                        FROM read_parquet('{tmp}') t
                        JOIN _uprn_max m
                            ON t.UPRN = m.UPRN
                            AND t.LODGEMENT_DATETIME IS NOT DISTINCT FROM m.max_dt
                    ) WHERE rn = 1
                    UNION ALL
                    SELECT * FROM read_parquet('{tmp}') WHERE UPRN IS NULL
                ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        else:
            # No deduplication: the column-projected slim Parquet is the output.
            tmp.rename(dst)
    finally:
        tmp.unlink(missing_ok=True)


def prepare_uprn(src: str | pathlib.Path, dst: pathlib.Path) -> None:
    """Write a column-pruned Parquet from the OS Open UPRN CSV.

    Selects UPRN, X_COORDINATE, Y_COORDINATE. No-ops if *dst* already exists.
    """
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    _configure_duckdb(con)
    con.execute(f"""
        COPY (
            SELECT UPRN, X_COORDINATE, Y_COORDINATE
            FROM read_csv('{src}')
        ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


def prepare_ubdc(src: str | pathlib.Path, dst: pathlib.Path) -> None:
    """Write a column-pruned Parquet from the UBDC PPD→UPRN lookup CSV.

    Selects transactionid and uprn. No-ops if *dst* already exists.
    """
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    _configure_duckdb(con)
    con.execute(f"""
        COPY (
            SELECT transactionid, uprn
            FROM read_csv('{src}')
        ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


def load_epc(epc_path: str | pathlib.Path) -> pd.DataFrame:
    """Load EPC CSV and deduplicate to the most recent certificate per UPRN.

    Rows without a UPRN are kept as-is (they are candidates for Tier 2
    address-normalisation matching and cannot be deduplicated by UPRN).
    """
    path = str(epc_path)
    return duckdb.execute(f"""
        WITH
        epc_raw AS (SELECT * FROM read_csv('{path}')),
        with_uprn AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY UPRN
                    ORDER BY LODGEMENT_DATETIME DESC
                ) AS _rn
            FROM epc_raw
            WHERE UPRN IS NOT NULL
        )
        SELECT * EXCLUDE (_rn) FROM with_uprn WHERE _rn = 1
        UNION ALL
        SELECT * FROM epc_raw WHERE UPRN IS NULL
    """).df()


_NORMALISE_MACRO = r"""
    CREATE OR REPLACE MACRO normalise_addr(s) AS (
        trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                regexp_replace(
                                                    regexp_replace(
                                                        regexp_replace(
                                                            regexp_replace(
                                                                upper(s),
                                                                '-', ' ', 'g'
                                                            ),
                                                            '[^\w\s]', '', 'g'
                                                        ),
                                                        '\s+', ' ', 'g'
                                                    ),
                                                    '\bTHE\b', '', 'g'
                                                ),
                                                '\s+', ' ', 'g'
                                            ),
                                            '\bAPARTMENT\b', 'FLAT', 'g'
                                        ),
                                        '\bUNIT\b', 'FLAT', 'g'
                                    ),
                                    '\bRD\b', 'ROAD', 'g'
                                ),
                                '\bAVE?\b', 'AVENUE', 'g'
                            ),
                            '\bDR\b', 'DRIVE', 'g'
                        ),
                        '\bCL\b', 'CLOSE', 'g'
                    ),
                    '\bCT\b', 'COURT', 'g'
                ),
                '\bGDNS\b', 'GARDENS', 'g'
            ),
            '\bHSE\b', 'HOUSE', 'g'
        ))
    )
"""


def _join_tier1(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    ubdc_path: str | pathlib.Path,
    dst: pathlib.Path,
    *,
    max_gap_years: int = 10,
) -> int:
    """UPRN-based join with temporal EPC selection.

    Writes category-A PPD rows matched via the UBDC lookup to *dst*.  For each
    sale, all EPC certificates for the matched UPRN within *max_gap_years* of
    the sale date are considered as candidates.  The best candidate is chosen
    by the following rule:

    1. The most recent EPC lodged **before or on** the sale date (prior EPC).
    2. If no prior EPC exists, the **earliest** EPC lodged after the sale
       date (post-sale fallback).
    3. If all EPCs are more than *max_gap_years* away from the sale date, the
       sale is excluded from the output.

    Two new columns are added to the output:

    ``gap_days``     — Days from the sale date to the selected EPC
                       (negative = EPC before sale, positive = EPC after sale).
    ``is_post_sale`` — True if the selected EPC was lodged after the sale.

    *epc_path* must contain all EPC rows (not deduplicated) so that multiple
    certificates for the same UPRN are available for temporal selection.
    Provide the ``epc_full.parquet`` checkpoint produced by
    ``prepare_epc(…, deduplicate=False)``.

    Returns the number of rows written.  Uses DuckDB COPY to stream directly
    to Parquet, avoiding materialising the join result into Python heap memory.

    Memory note: this function performs a window-function ranking over the
    candidate set (PPD × EPC on UPRN, pre-filtered by max_gap_years).  The
    fan-out before window reduction is bounded by the average number of EPC
    certificates per UPRN (~1.4–2×), so peak working-set is ~1.5–2× the
    tier-1 PPD row count.  DuckDB spills to disk when the memory limit is hit.
    """
    con = duckdb.connect()
    _configure_duckdb(con)
    ppd_src = _ppd_source(ppd_path)
    epc_src = _sql_source(epc_path)
    ubdc_src = _sql_source(ubdc_path)
    con.execute(f"""
        COPY (
            WITH
            epc AS (SELECT * FROM {epc_src}),
            ppd AS (
                SELECT * FROM {ppd_src}
                WHERE ppd_category_type = 'A'
            ),
            ubdc AS (SELECT * FROM {ubdc_src}),
            -- All (PPD, EPC) candidate pairs within the temporal gap window.
            candidates AS (
                SELECT
                    ppd.transaction_unique_identifier,
                    ppd.price, ppd.date_of_transfer, ppd.postcode,
                    ppd.property_type, ppd.new_build_flag, ppd.tenure_type,
                    ppd.paon, ppd.saon, ppd.street, ppd.locality, ppd.town_city,
                    ppd.district, ppd.county,
                    ppd.ppd_category_type, ppd.record_status,
                    CAST(ubdc.uprn AS BIGINT) AS uprn,
                    epc.TOTAL_FLOOR_AREA, epc.LODGEMENT_DATETIME,
                    epc.ADDRESS1, epc.ADDRESS2,
                    epc.BUILT_FORM, epc.CONSTRUCTION_AGE_BAND,
                    epc.CURRENT_ENERGY_RATING,
                    DATEDIFF('day', ppd.date_of_transfer, epc.LODGEMENT_DATETIME)
                        AS gap_days,
                    (epc.LODGEMENT_DATETIME > ppd.date_of_transfer) AS is_post_sale
                FROM ppd
                JOIN ubdc ON ppd.transaction_unique_identifier = ubdc.transactionid
                JOIN epc ON CAST(ubdc.uprn AS BIGINT) = CAST(epc.UPRN AS BIGINT)
                WHERE ABS(DATEDIFF('year',
                          epc.LODGEMENT_DATETIME, ppd.date_of_transfer))
                          <= {max_gap_years}
            ),
            -- Rank candidates: prior EPCs (is_post_sale=False) first, then
            -- post-sale.  Within priors: most recent first.
            -- Within post-sales: earliest first.
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY transaction_unique_identifier
                        ORDER BY
                            CASE WHEN is_post_sale THEN 1 ELSE 0 END ASC,
                            CASE WHEN NOT is_post_sale
                                 THEN LODGEMENT_DATETIME END DESC NULLS LAST,
                            CASE WHEN is_post_sale
                                 THEN LODGEMENT_DATETIME END ASC NULLS LAST
                    ) AS _rn
                FROM candidates
            )
            SELECT
                transaction_unique_identifier,
                price, date_of_transfer, postcode,
                property_type, new_build_flag, tenure_type,
                paon, saon, street, locality, town_city,
                district, county, ppd_category_type, record_status,
                uprn,
                TOTAL_FLOOR_AREA, LODGEMENT_DATETIME,
                ADDRESS1, ADDRESS2,
                BUILT_FORM, CONSTRUCTION_AGE_BAND, CURRENT_ENERGY_RATING,
                gap_days, is_post_sale,
                1 AS match_tier
            FROM ranked
            WHERE _rn = 1
        ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{dst}')").fetchone()
    result: int = row[0]  # type: ignore[index]
    return result


def _join_tier2(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    tier1_path: pathlib.Path,
    dst: pathlib.Path,
) -> int:
    """Address-normalisation join. Writes PPD rows not in tier1 to dst.

    Returns the number of rows written. Uses DuckDB COPY to stream directly to
    Parquet, avoiding materialising the join result into Python heap memory.
    """
    con = duckdb.connect()
    _configure_duckdb(con)
    con.execute(_NORMALISE_MACRO)
    tier1_expr = f"read_parquet('{tier1_path}')"
    ppd_src = _ppd_source(ppd_path)
    epc_src = _sql_source(epc_path)
    con.execute(f"""
        COPY (
            WITH
            epc AS (SELECT * FROM {epc_src}),
            ppd AS (
                SELECT * FROM {ppd_src}
                WHERE ppd_category_type = 'A'
            ),
            ppd_remaining AS (
                SELECT * FROM ppd
                WHERE transaction_unique_identifier NOT IN (
                    SELECT transaction_unique_identifier FROM {tier1_expr}
                )
            ),
            ppd_norm AS (
                SELECT *,
                    normalise_addr(concat_ws(' ',
                        NULLIF(COALESCE(CAST(saon AS VARCHAR), ''), ''),
                        NULLIF(COALESCE(CAST(paon AS VARCHAR), ''), ''),
                        NULLIF(COALESCE(CAST(street AS VARCHAR), ''), '')
                    )) AS norm_addr,
                    upper(trim(postcode)) AS postcode_norm
                FROM ppd_remaining
            ),
            epc_norm AS (
                SELECT *,
                    normalise_addr(concat_ws(' ',
                        NULLIF(COALESCE(ADDRESS1, ''), ''),
                        NULLIF(COALESCE(ADDRESS2, ''), '')
                    )) AS norm_addr,
                    upper(trim(POSTCODE)) AS postcode_norm
                FROM epc
                WHERE upper(trim(POSTCODE)) IN (
                    SELECT DISTINCT postcode_norm FROM ppd_norm
                )
            )
            SELECT
                p.transaction_unique_identifier,
                p.price, p.date_of_transfer, p.postcode,
                p.property_type, p.new_build_flag, p.tenure_type,
                p.paon, p.saon, p.street, p.locality, p.town_city,
                p.district, p.county, p.ppd_category_type, p.record_status,
                NULL::BIGINT AS uprn,
                e.TOTAL_FLOOR_AREA, e.LODGEMENT_DATETIME,
                e.ADDRESS1, e.ADDRESS2,
                e.BUILT_FORM, e.CONSTRUCTION_AGE_BAND, e.CURRENT_ENERGY_RATING,
                NULL::INT AS gap_days,
                NULL::BOOLEAN AS is_post_sale,
                2 AS match_tier
            FROM ppd_norm AS p
            JOIN epc_norm AS e
                ON p.postcode_norm = e.postcode_norm
               AND p.norm_addr = e.norm_addr
        ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{dst}')").fetchone()
    result: int = row[0]  # type: ignore[index]
    return result


def _join_tier3(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    existing_matched_path: pathlib.Path,
    dst: pathlib.Path,
) -> int:
    """Enhanced normalisation join for records unmatched in prior tiers.

    Extends tier 2 with two additional normalisations:
    - Bare numeric/alphanumeric SAONs get ``FLAT `` prepended before concatenation
      (e.g. saon ``"3"`` → ``"FLAT 3"`` so it matches EPC ``"FLAT 3 10 HIGH ST"``).
    - Both sides already benefit from ``UNIT → FLAT`` added to the shared
      ``normalise_addr`` macro.

    Records already present in *existing_matched_path* are excluded so this
    function can be run incrementally on top of an existing matched.parquet.
    Uses DuckDB COPY to stream directly to Parquet (no Python heap materialisation).
    Returns the number of new rows written.
    """
    con = duckdb.connect()
    _configure_duckdb(con)
    con.execute(_NORMALISE_MACRO)
    ppd_src = _ppd_source(ppd_path)
    epc_src = _sql_source(epc_path)
    # Pre-extract just the transaction IDs to a narrow single-column Parquet.
    # This separates the anti-join hash-table build from the EPC+PPD join so
    # the two scans do not compete for the same DuckDB memory budget.  DuckDB
    # performs column pruning on Parquet, but making it explicit here also
    # avoids any risk of reading wide rows from matched.parquet during the join.
    excluded_ids_path = dst.with_suffix(".excluded_ids.parquet")
    try:
        con.execute(f"""
            COPY (
                SELECT transaction_unique_identifier
                FROM read_parquet('{existing_matched_path}')
            ) TO '{excluded_ids_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        excluded_expr = f"read_parquet('{excluded_ids_path}')"
        con.execute(f"""
            COPY (
                WITH
                epc AS (SELECT * FROM {epc_src}),
                ppd AS (
                    SELECT * FROM {ppd_src}
                    WHERE ppd_category_type = 'A'
                ),
                ppd_remaining AS (
                    SELECT * FROM ppd
                    WHERE transaction_unique_identifier NOT IN (
                        SELECT transaction_unique_identifier FROM {excluded_expr}
                    )
                ),
                ppd_norm AS (
                    SELECT *,
                        normalise_addr(concat_ws(' ',
                            CASE
                                WHEN regexp_matches(
                                    trim(upper(coalesce(cast(saon AS VARCHAR), ''))),
                                    '^\\d+[A-Z]?$'
                                )
                                THEN 'FLAT ' || trim(upper(cast(saon AS VARCHAR)))
                                ELSE nullif(
                                    trim(coalesce(cast(saon AS VARCHAR), '')), ''
                                )
                            END,
                            nullif(trim(coalesce(cast(paon AS VARCHAR), '')), ''),
                            nullif(trim(coalesce(cast(street AS VARCHAR), '')), '')
                        )) AS norm_addr,
                        upper(trim(postcode)) AS postcode_norm
                    FROM ppd_remaining
                ),
                epc_norm AS (
                    SELECT *,
                        normalise_addr(concat_ws(' ',
                            NULLIF(COALESCE(ADDRESS1, ''), ''),
                            NULLIF(COALESCE(ADDRESS2, ''), '')
                        )) AS norm_addr,
                        upper(trim(POSTCODE)) AS postcode_norm
                    FROM epc
                    WHERE upper(trim(POSTCODE)) IN (
                        SELECT DISTINCT postcode_norm FROM ppd_norm
                    )
                )
                SELECT
                    p.transaction_unique_identifier,
                    p.price, p.date_of_transfer, p.postcode,
                    p.property_type, p.new_build_flag, p.tenure_type,
                    p.paon, p.saon, p.street, p.locality, p.town_city,
                    p.district, p.county, p.ppd_category_type, p.record_status,
                    NULL::BIGINT AS uprn,
                    e.TOTAL_FLOOR_AREA, e.LODGEMENT_DATETIME,
                    e.ADDRESS1, e.ADDRESS2,
                    e.BUILT_FORM, e.CONSTRUCTION_AGE_BAND, e.CURRENT_ENERGY_RATING,
                    NULL::INT AS gap_days,
                    NULL::BOOLEAN AS is_post_sale,
                    3 AS match_tier
                FROM ppd_norm AS p
                JOIN epc_norm AS e
                    ON p.postcode_norm = e.postcode_norm
                   AND p.norm_addr = e.norm_addr
            ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        excluded_ids_path.unlink(missing_ok=True)
    row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{dst}')").fetchone()
    result: int = row[0]  # type: ignore[index]
    return result


def _cpi_ctes(cpi_path: pathlib.Path) -> str:
    """Return SQL CTE block for CPI deflation.

    Produces two CTEs for use inside a WITH clause:

    ``cpi``      — monthly index values keyed by (yr, mo).
    ``base_cpi`` — scalar subquery alias for the base-month index value.

    Usage in a SELECT: ``price * ((SELECT idx FROM base_cpi) / cpi.idx)``
    with a JOIN on ``cpi.yr = YEAR(date_col) AND cpi.mo = MONTH(date_col)``.
    """
    return f"""
    cpi AS (
        SELECT
            CAST(split_part(date, '-', 1) AS INTEGER) AS yr,
            CAST(split_part(date, '-', 2) AS INTEGER) AS mo,
            cpi AS idx
        FROM read_csv('{cpi_path}',
                      columns={{'date': 'VARCHAR', 'cpi': 'DOUBLE'}})
    ),
    base_cpi AS (
        SELECT idx FROM cpi WHERE yr = {CPI_BASE[0]} AND mo = {CPI_BASE[1]}
    )"""


def join_datasets(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    ubdc_path: str | pathlib.Path,
    dst: pathlib.Path,
    *,
    epc_full_path: pathlib.Path | None = None,
    on_tier1_complete: Callable[[int], None] | None = None,
    cpi_path: pathlib.Path,
) -> None:
    """Join PPD to EPC using a tiered strategy, writing result to *dst*.

    Tier 1 — temporal UPRN join via the UBDC lookup table.  For each sale,
    selects the most recent EPC lodged before the sale date, or the earliest
    EPC lodged after if no prior certificate exists, within a 10-year window.
    Tier 2 — address normalisation fallback for records without a UPRN match.

    Writes a Parquet file to *dst* containing matched records with a
    ``match_tier`` column (1 or 2).  Tier 1 rows also carry ``gap_days``
    (signed day count from sale to EPC) and ``is_post_sale`` (bool); Tier 2
    rows carry NULL for those columns.  PPD records with
    ``ppd_category_type != 'A'`` are excluded before joining.  Unmatched PPD
    records are not included.

    Each row also carries an ``adjusted_price`` column: the nominal sale price
    deflated to real ``CPI_BASE`` pounds using the ONS CPI monthly index
    loaded from *cpi_path*.

    *epc_full_path* — path to the undeduped EPC Parquet
    (``prepare_epc(…, deduplicate=False)``).  When provided, Tier 1 uses it
    for temporal matching across multiple certificates per UPRN.  When ``None``
    *epc_path* is used instead (legacy behaviour: single deduplicated EPC per
    UPRN, no temporal selection).

    All intermediate results are streamed to temp Parquet files via DuckDB
    COPY, so no join result is materialised into Python heap memory.

    If *on_tier1_complete* is provided it is called with the tier-1 row count
    before tier-2 begins, allowing callers to report intermediate progress.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    epc_tier1 = epc_full_path if epc_full_path is not None else epc_path
    with tempfile.TemporaryDirectory() as _tmp:
        tmp = pathlib.Path(_tmp)
        tier1_path = tmp / "tier1.parquet"
        tier2_path = tmp / "tier2.parquet"

        n_tier1 = _join_tier1(ppd_path, epc_tier1, ubdc_path, tier1_path)
        if on_tier1_complete is not None:
            on_tier1_complete(n_tier1)

        _join_tier2(ppd_path, epc_path, tier1_path, tier2_path)

        duckdb.execute(f"""
            COPY (
                WITH
                {_cpi_ctes(cpi_path)},
                combined AS (
                    SELECT * FROM read_parquet('{tier1_path}')
                    UNION ALL
                    SELECT * FROM read_parquet('{tier2_path}')
                )
                SELECT
                    c.*,
                    c.price * (
                        (SELECT idx FROM base_cpi) / cpi.idx
                    ) AS adjusted_price
                FROM combined AS c
                JOIN cpi
                  ON cpi.yr = YEAR(c.date_of_transfer)
                 AND cpi.mo = MONTH(c.date_of_transfer)
            ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)


def match_report(matched: pd.DataFrame, total_ppd: int) -> dict[str, int | float]:
    """Compute match-rate statistics from the joined dataset.

    Args:
        matched:   DataFrame returned by join_datasets, with a match_tier column.
        total_ppd: Total number of category-A PPD records before joining.

    Returns a dict with tier1, tier2, unmatched counts and their percentages.
    """
    tier1 = int((matched["match_tier"] == 1).sum())
    tier2 = int((matched["match_tier"] == 2).sum())
    unmatched = total_ppd - tier1 - tier2
    return {
        "tier1": tier1,
        "tier2": tier2,
        "unmatched": unmatched,
        "total": total_ppd,
        "tier1_pct": round(100 * tier1 / total_ppd, 1),
        "tier2_pct": round(100 * tier2 / total_ppd, 1),
        "unmatched_pct": round(100 * unmatched / total_ppd, 1),
    }


def load_cpi(path: pathlib.Path) -> dict[tuple[int, int], float]:
    """Load an ONS-format monthly CPI CSV into a (year, month) → index dict.

    The CSV must have columns ``date`` (``YYYY-MM``) and ``cpi`` (numeric).
    """
    df = pd.read_csv(path, dtype={"date": str, "cpi": float})
    result: dict[tuple[int, int], float] = {}
    for _, row in df.iterrows():
        year, month = (int(part) for part in str(row["date"]).split("-"))
        result[(year, month)] = float(row["cpi"])
    return result


def deflate_price(
    price: float,
    sale_date: datetime.date,
    cpi: dict[tuple[int, int], float],
    base: tuple[int, int],
) -> float:
    """Convert a nominal *price* to real terms relative to *base* month.

    Formula: ``price × (cpi[base] / cpi[(year, month)])``.
    Raises ``KeyError`` if the sale month is not present in *cpi*.
    """
    sale_key = (sale_date.year, sale_date.month)
    return price * (cpi[base] / cpi[sale_key])


def aggregate_by_geography(
    matched: pd.DataFrame,
    geography: Geography,
    min_sales: int = 10,
    price_col: str = "price",
) -> pd.DataFrame:
    """Aggregate matched records to price per m² by the given geography.

    For ``Geography.POSTCODE_DISTRICT`` the district is derived from the
    ``postcode`` column (last three characters stripped).  For all other
    geographies the column named by ``geography.value`` must already be
    present in *matched* — it is populated by the spatial join step.

    Rows without a value for the target geography are excluded.
    Geographies with fewer than *min_sales* transactions are excluded.
    Result is sorted by price_per_sqm descending.
    """
    col = geography.value

    # Select only the columns needed for aggregation to avoid a full copy.
    if geography is Geography.POSTCODE_DISTRICT:
        df = matched[["postcode", price_col, "TOTAL_FLOOR_AREA"]].copy()
        df[col] = df["postcode"].str[:-3].str.strip()
        df[col] = df[col].replace(POSTCODE_DISTRICT_OVERRIDES)
        df.drop(columns=["postcode"], inplace=True)
    else:
        df = matched[[col, price_col, "TOTAL_FLOOR_AREA"]].copy()

    df = df[df[col].notna()]

    grouped = (
        df.groupby(col)
        .agg(
            num_sales=(price_col, "count"),
            total_price=(price_col, "sum"),
            total_floor_area=("TOTAL_FLOOR_AREA", "sum"),
        )
        .reset_index()
    )

    grouped = grouped[grouped["num_sales"] >= min_sales].copy()
    grouped["price_per_sqm"] = (
        (grouped["total_price"] / grouped["total_floor_area"]).round().astype(int)
    )

    return grouped.sort_values("price_per_sqm", ascending=False).reset_index(drop=True)


def aggregate(rows: list[dict[str, float]]) -> dict[str, int]:
    """Aggregate sales rows to price per m² (total price / total area)."""
    total_price = sum(r["price"] for r in rows)
    total_area = sum(r["floor_area"] for r in rows)
    return {"price_per_sqm": round(total_price / total_area)}


# ---------------------------------------------------------------------------
# Checkpoint helper
# ---------------------------------------------------------------------------


def _checkpoint(
    name: str,
    cache_dir: pathlib.Path,
    compute: Callable[[], pd.DataFrame],
) -> pd.DataFrame:
    """Return cached Parquet if present; otherwise compute, save, and return.

    Creates *cache_dir* if it does not exist.
    """
    path = cache_dir / f"{name}.parquet"
    if path.exists():
        print(f"  [skip] {name}")
        return pd.read_parquet(path)
    print(f"  [run]  {name}")
    cache_dir.mkdir(parents=True, exist_ok=True)
    df = compute()
    df.to_parquet(path, index=False)
    return df


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}:{s % 60:02d}"


def _fmt_size(num_bytes: int) -> str:
    for unit, threshold in [("GB", 1_000_000_000), ("MB", 1_000_000), ("KB", 1_000)]:
        if num_bytes >= threshold:
            return f"{num_bytes / threshold:.1f} {unit}"
    return f"{num_bytes} B"


def _rss_mb() -> int:
    """Return the current resident set size of this process in megabytes."""
    status = pathlib.Path("/proc/self/status").read_text()
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) // 1024
    return 0


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------


def run(
    ppd_path: pathlib.Path,
    epc_path: pathlib.Path,
    ubdc_path: pathlib.Path,
    uprn_path: pathlib.Path,
    boundary_path: pathlib.Path,
    *,
    epc_full_path: pathlib.Path | None = None,
    cache_dir: pathlib.Path = CACHE,
    output_dir: pathlib.Path = OUTPUT,
    min_sales: int = 10,
    cpi_path: pathlib.Path = DATA / "cpi.csv",
) -> None:
    """Run the full pipeline: join → spatial → aggregate → write CSVs.

    Each heavy step is checkpointed to *cache_dir* as Parquet.  Re-running
    skips any step whose checkpoint already exists, so iterating on later
    stages does not require re-processing the raw data.

    Args:
        ppd_path:      Path to the slim PPD Parquet (from ``make download``).
        epc_path:      Path to the slim deduplicated EPC Parquet
                       (``epc_slim.parquet`` from ``make download``).
        ubdc_path:     Path to the slim UBDC PPD→UPRN Parquet (from ``make download``).
        uprn_path:     Path to the slim OS Open UPRN Parquet (from ``make download``).
        boundary_path: Path to the LSOA boundary file (GeoPackage or GeoJSON).
        epc_full_path: Path to the undeduped EPC Parquet
                       (``epc_full.parquet`` from ``make download``).
                       When provided, Tier 1 uses temporal EPC selection
                       (most recent prior / earliest post-sale within 10 years).
                       When ``None``, Tier 1 falls back to the single
                       deduplicated EPC per UPRN in *epc_path*.
        cache_dir:     Directory for Parquet checkpoints (default: cache/).
        output_dir:    Directory for output CSVs (default: output/).
        min_sales:     Minimum sales per geography unit to include in output.
    """
    console = Console()

    # --- Input file summary --------------------------------------------------
    console.print()
    console.print("[bold]House prices pipeline[/bold]")
    console.print()
    inputs = [
        ("PPD", ppd_path),
        ("EPC", epc_path),
        ("UBDC", ubdc_path),
        ("UPRN", uprn_path),
        ("Boundary", boundary_path),
    ]
    for label, path in inputs:
        try:
            size_str = _fmt_size(path.stat().st_size)
        except FileNotFoundError:  # pragma: no cover
            size_str = "not found"
        console.print(f"  {label:<10} {path.name:<44} [dim]{size_str}[/dim]")
    memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "unlimited")
    threads = os.environ.get("DUCKDB_THREADS", "all")
    console.print(
        f"\n  [dim]DuckDB     memory_limit={memory_limit}  threads={threads}[/dim]"
    )
    console.print()

    # --- Step helper ---------------------------------------------------------
    # Inlines checkpoint logic so the display owns the full lifecycle.
    def step(name: str, compute: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        parquet = cache_dir / f"{name}.parquet"
        if parquet.exists():
            console.print(f"  [dim]⊘  {name:<18} skipped (cached)[/dim]")
            return pd.read_parquet(parquet)
        t0 = time.monotonic()
        with console.status(f"  [yellow]⏳  {name}…[/yellow]"):
            df = compute()
        cache_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet, index=False)
        elapsed = _fmt_elapsed(time.monotonic() - t0)
        rss = _rss_mb()
        console.print(
            f"  [green]✓[/green]  {name:<18} {elapsed:<8} {len(df):>14,} rows"
            f"  [dim]RSS {rss} MB[/dim]"
        )
        return df

    # --- Steps ---------------------------------------------------------------
    def _on_tier1(n: int) -> None:
        console.print(f"      [dim]tier 1: {n:,} UPRN matches[/dim]")

    matched_parquet = cache_dir / "matched.parquet"
    if matched_parquet.exists():
        console.print(f"  [dim]⊘  {'matched':<18} skipped (cached)[/dim]")
    else:
        t0 = time.monotonic()
        with console.status("  [yellow]⏳  matched…[/yellow]"):
            join_datasets(
                ppd_path,
                epc_path,
                ubdc_path,
                dst=matched_parquet,
                epc_full_path=epc_full_path,
                on_tier1_complete=_on_tier1,
                cpi_path=cpi_path,
            )
        elapsed = _fmt_elapsed(time.monotonic() - t0)
        n_rows = duckdb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{matched_parquet}')"
        ).fetchone()[0]  # type: ignore[index]
        rss = _rss_mb()
        console.print(
            f"  [green]✓[/green]  {'matched':<18} {elapsed:<8} {n_rows:>14,} rows"
            f"  [dim]RSS {rss} MB[/dim]"
        )
    # Extract UPRNs via a lightweight DuckDB query so the full matched
    # DataFrame is not in Python heap while the spatial join runs.
    # Loading both simultaneously caused the cgroup OOM kill.
    matched_uprns_df = duckdb.execute(
        f"SELECT CAST(uprn AS BIGINT) AS uprn"
        f" FROM read_parquet('{matched_parquet}')"
        f" WHERE uprn IS NOT NULL"
    ).df()
    matched_uprns = set(matched_uprns_df["uprn"])
    del matched_uprns_df

    step(
        "uprn_lsoa",
        lambda: build_uprn_lsoa(uprn_path, boundary_path, matched_uprns),
    )
    console.print()

    uprn_lsoa_parquet = cache_dir / "uprn_lsoa.parquet"
    _run_aggregations(
        matched_parquet, uprn_lsoa_parquet, ppd_path, output_dir, min_sales, console
    )


def _run_aggregations(
    matched_parquet: pathlib.Path,
    uprn_lsoa_parquet: pathlib.Path,
    ppd_path: pathlib.Path | str,
    output_dir: pathlib.Path,
    min_sales: int,
    console: Console,
    min_sales_type: int = 5,
) -> None:
    """Emit match report, district CSV, and LSOA CSV from existing Parquet files.

    Runs entirely inside DuckDB — never loads the full matched DataFrame into
    Python heap.  Peak RSS is bounded by the small aggregation result sets
    (~3 k district rows, ~35 k LSOA rows).

    Called by both ``run()`` (after the join steps) and ``rematch()`` (after
    appending tier-3 matches to an existing matched.parquet).
    """
    # Aggregate directly from Parquet files — never load the full matched
    # DataFrame into Python heap.  Peak RSS stays under 1 GB even with a
    # 1+ GB matched.parquet, because DuckDB streams the scans and only
    # materialises small aggregation states.
    con = duckdb.connect()
    _configure_duckdb(con)

    # Match report — tier counts via DuckDB scan.
    is_parquet = str(ppd_path).endswith(".parquet")
    total_ppd: int = duckdb.execute(
        f"SELECT COUNT(*) FROM {_ppd_source(ppd_path)}"
        + ("" if is_parquet else " WHERE ppd_category_type = 'A'")
    ).fetchone()[0]  # type: ignore[index]
    tier_counts = con.execute(f"""
        SELECT match_tier, COUNT(*) AS n
        FROM read_parquet('{matched_parquet}')
        GROUP BY match_tier
        ORDER BY match_tier
    """).fetchall()
    tier_map: dict[int, int] = {int(t): int(n) for t, n in tier_counts}
    matched_total = sum(tier_map.values())
    unmatched = total_ppd - matched_total
    tier_parts = "  ".join(
        f"tier{t} [{c}]{n:,}[/{c}] ({round(100 * n / total_ppd, 1)}%)"
        for t, n in sorted(tier_map.items())
        for c in ["green" if t == 1 else "yellow"]
    )
    console.print(
        f"  Match  {tier_parts}  "
        f"unmatched [red]{unmatched:,}[/red] ({round(100 * unmatched / total_ppd, 1)}%)"
    )
    console.print()

    # Aggregate by postcode district.
    # Postcode district = postcode minus the last 3 characters (the inward code),
    # matching the Python logic: postcode.str[:-3].str.strip().
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build a SQL CASE expression that applies POSTCODE_DISTRICT_OVERRIDES so
    # the DuckDB aggregation and the Python aggregate_by_geography path stay in sync.
    _district_expr = "TRIM(LEFT(postcode, LENGTH(postcode) - 3))"
    if POSTCODE_DISTRICT_OVERRIDES:
        _when_clauses = " ".join(
            f"WHEN '{src}' THEN '{dst}'"
            for src, dst in POSTCODE_DISTRICT_OVERRIDES.items()
        )
        _district_expr = (
            f"CASE {_district_expr} {_when_clauses} ELSE {_district_expr} END"
        )

    district_df = con.execute(f"""
        -- Per-type rows
        SELECT
            {_district_expr} AS postcode_district,
            property_type,
            COUNT(*) AS num_sales,
            SUM(TOTAL_FLOOR_AREA) AS total_floor_area,
            SUM(price) AS total_price,
            CAST(ROUND(SUM(price) / SUM(TOTAL_FLOOR_AREA)) AS INTEGER)
                AS price_per_sqm,
            CAST(ROUND(SUM(adjusted_price) / SUM(TOTAL_FLOOR_AREA)) AS INTEGER)
                AS adj_price_per_sqm
        FROM read_parquet('{matched_parquet}')
        WHERE TOTAL_FLOOR_AREA IS NOT NULL
          AND TOTAL_FLOOR_AREA > 0
          AND postcode IS NOT NULL
          AND property_type IS NOT NULL
        GROUP BY postcode_district, property_type
        HAVING COUNT(*) >= {min_sales_type}

        UNION ALL

        -- ALL rollup
        SELECT
            {_district_expr} AS postcode_district,
            'ALL' AS property_type,
            COUNT(*) AS num_sales,
            SUM(TOTAL_FLOOR_AREA) AS total_floor_area,
            SUM(price) AS total_price,
            CAST(ROUND(SUM(price) / SUM(TOTAL_FLOOR_AREA)) AS INTEGER)
                AS price_per_sqm,
            CAST(ROUND(SUM(adjusted_price) / SUM(TOTAL_FLOOR_AREA)) AS INTEGER)
                AS adj_price_per_sqm
        FROM read_parquet('{matched_parquet}')
        WHERE TOTAL_FLOOR_AREA IS NOT NULL
          AND TOTAL_FLOOR_AREA > 0
          AND postcode IS NOT NULL
        GROUP BY postcode_district
        HAVING COUNT(*) >= {min_sales}

        ORDER BY postcode_district, property_type
    """).df()
    district_path = output_dir / "price_per_sqm_postcode_district.csv"
    district_df.to_csv(district_path, index=False)
    console.print(
        f"  [green]✓[/green]  {len(district_df):,} postcode districts"
        f"  →  {district_path}"
    )
    del district_df

    # Aggregate by LSOA — join matched ← uprn_lsoa on disk.
    # Only Tier 1 records have a UPRN; address-matched records are excluded here
    # but still appear in the postcode district output above.
    lsoa_df = con.execute(f"""
        SELECT
            l.LSOA21CD,
            COUNT(*) AS num_sales,
            SUM(m.TOTAL_FLOOR_AREA) AS total_floor_area,
            SUM(m.price) AS total_price,
            CAST(ROUND(SUM(m.price) / SUM(m.TOTAL_FLOOR_AREA)) AS INTEGER)
                AS price_per_sqm,
            CAST(ROUND(SUM(m.adjusted_price) / SUM(m.TOTAL_FLOOR_AREA)) AS INTEGER)
                AS adj_price_per_sqm
        FROM read_parquet('{matched_parquet}') AS m
        JOIN read_parquet('{uprn_lsoa_parquet}') AS l
          ON CAST(m.uprn AS BIGINT) = CAST(l.UPRN AS BIGINT)
        WHERE m.TOTAL_FLOOR_AREA IS NOT NULL
          AND m.TOTAL_FLOOR_AREA > 0
        GROUP BY l.LSOA21CD
        HAVING COUNT(*) >= {min_sales}
        ORDER BY adj_price_per_sqm DESC
    """).df()
    lsoa_path = output_dir / "price_per_sqm_lsoa.csv"
    lsoa_df.to_csv(lsoa_path, index=False)
    console.print(f"  [green]✓[/green]  {len(lsoa_df):,} LSOAs  →  {lsoa_path}")
    console.print()

    # Write metadata.json with actual min/max sale dates from matched data.
    row = con.execute(f"""
        SELECT
            CAST(MIN(date_of_transfer) AS DATE) AS min_sale_date,
            CAST(MAX(date_of_transfer) AS DATE) AS max_sale_date
        FROM read_parquet('{matched_parquet}')
    """).fetchone()
    metadata: dict[str, str] = {}
    if row and row[0] is not None and row[1] is not None:
        metadata["min_sale_date"] = str(row[0])
        metadata["max_sale_date"] = str(row[1])
    metadata_path = output_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2))
    console.print(f"  [green]✓[/green]  metadata  →  {metadata_path}")


def rematch(
    ppd_path: pathlib.Path,
    epc_path: pathlib.Path,
    *,
    cache_dir: pathlib.Path = CACHE,
    output_dir: pathlib.Path = OUTPUT,
    min_sales: int = 10,
    cpi_path: pathlib.Path = DATA / "cpi.csv",
) -> None:
    """Extend existing matches with tier-3 normalisation, then re-aggregate.

    Requires ``cache/matched.parquet`` from a prior ``run()``.  Finds all
    unmatched category-A PPD records and applies enhanced address normalisation
    (bare numeric SAON prepend).  New matches are appended to
    ``matched.parquet`` in place; output CSVs are regenerated.

    The ``uprn_lsoa.parquet`` spatial checkpoint is not rebuilt — tier-3
    matches carry no UPRN so the LSOA aggregation is unchanged.

    Memory-safe: all heavy steps use DuckDB COPY to Parquet; the matched
    DataFrame is never loaded into Python heap.

    Args:
        ppd_path:  Path to the slim PPD Parquet (from ``make run``).
        epc_path:  Path to the slim EPC Parquet (from ``make run``).
        cache_dir: Directory containing Parquet checkpoints.
        output_dir: Directory for output CSVs.
        min_sales:  Minimum sales per geography to include in output.
        cpi_path:  Path to the ONS CPI monthly CSV (for adjusted_price).
    """
    console = Console()
    console.print()
    console.print("[bold]House prices pipeline — rematch[/bold]")
    console.print()

    matched_parquet = cache_dir / "matched.parquet"
    uprn_lsoa_parquet = cache_dir / "uprn_lsoa.parquet"

    if not matched_parquet.exists():
        console.print(
            "[red]Error:[/red] cache/matched.parquet not found."
            " Run [bold]make run[/bold] first."
        )
        return

    with tempfile.TemporaryDirectory() as _tmp:
        tmp = pathlib.Path(_tmp)
        tier3_path = tmp / "tier3.parquet"

        t0 = time.monotonic()
        with console.status("  [yellow]⏳  tier3 normalisation…[/yellow]"):
            n_tier3 = _join_tier3(ppd_path, epc_path, matched_parquet, tier3_path)
        elapsed = _fmt_elapsed(time.monotonic() - t0)
        rss = _rss_mb()
        console.print(
            f"  [green]✓[/green]  tier3               {elapsed:<8} {n_tier3:>14,} rows"
            f"  [dim]RSS {rss} MB[/dim]"
        )

        if n_tier3 == 0:
            console.print(
                "  [dim]No new matches found — matched.parquet unchanged.[/dim]"
            )
            console.print()
            return

        # Append tier-3 results to matched.parquet atomically.
        # Deflate tier-3 prices before appending so adjusted_price is
        # consistent with the tier-1/2 rows already in matched.parquet.
        # DuckDB reads both files without loading either into Python heap.
        tmp_merged = matched_parquet.with_suffix(".tmp.parquet")
        try:
            duckdb.execute(f"""
                COPY (
                    WITH
                    {_cpi_ctes(cpi_path)},
                    tier3 AS (SELECT * FROM read_parquet('{tier3_path}'))
                    SELECT * FROM read_parquet('{matched_parquet}')
                    UNION ALL
                    SELECT
                        t.*,
                        t.price * (
                            (SELECT idx FROM base_cpi) / cpi.idx
                        ) AS adjusted_price
                    FROM tier3 AS t
                    JOIN cpi
                      ON cpi.yr = YEAR(t.date_of_transfer)
                     AND cpi.mo = MONTH(t.date_of_transfer)
                ) TO '{tmp_merged}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            tmp_merged.rename(matched_parquet)
        finally:
            tmp_merged.unlink(missing_ok=True)

        rss = _rss_mb()
        console.print(
            f"  [green]✓[/green]  matched.parquet updated  [dim]RSS {rss} MB[/dim]"
        )
        console.print()

    _run_aggregations(
        matched_parquet, uprn_lsoa_parquet, ppd_path, output_dir, min_sales, console
    )


if __name__ == "__main__":  # pragma: no cover
    if "--rematch" in sys.argv:
        rematch(
            ppd_path=CACHE / "ppd_slim.parquet",
            epc_path=CACHE / "epc_slim.parquet",
        )
    else:
        _epc_full = CACHE / "epc_full.parquet"
        run(
            ppd_path=CACHE / "ppd_slim.parquet",
            epc_path=CACHE / "epc_slim.parquet",
            ubdc_path=CACHE / "ubdc_slim.parquet",
            uprn_path=CACHE / "uprn_slim.parquet",
            boundary_path=DATA / "lsoa_boundaries.gpkg",
            epc_full_path=_epc_full if _epc_full.exists() else None,
        )
