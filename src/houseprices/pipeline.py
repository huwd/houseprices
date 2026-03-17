"""Main pipeline: download → join → aggregate."""

import os
import pathlib
import re
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


class Geography(Enum):
    """Contiguous geographies supported for price-per-sqm aggregation.

    The enum value is the output column name in the aggregated DataFrame.
    For POSTCODE_DISTRICT the column is derived from the ``postcode`` field.
    For all other geographies the column must already be present in the
    matched DataFrame (populated by the spatial join step).
    """

    POSTCODE_DISTRICT = "postcode_district"
    LSOA = "LSOA21CD"


_ABBREVIATIONS: list[tuple[str, str]] = [
    (r"\bAPARTMENT\b", "FLAT"),
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
    parts = re.sub(r"[^\w\s]", "", parts)
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


def prepare_epc(src: str | pathlib.Path, dst: pathlib.Path) -> None:
    """Write a column-pruned, deduplicated Parquet from the EPC CSV.

    Selects the 9 columns used by the pipeline and deduplicates by UPRN,
    keeping only the most recent certificate per UPRN (by LODGEMENT_DATETIME
    DESC).  Rows without a UPRN are kept as-is (Tier 2 candidates).
    No-ops if *dst* already exists.

    Uses a two-step approach to stay within the DuckDB memory limit:

    1. Stream the 60-column CSV down to a 9-column slim Parquet (no sort).
    2. Deduplicate the slim Parquet with GROUP BY + MAX_BY, which only keeps
       one hash-table entry per UPRN — O(unique UPRNs) memory rather than
       O(total rows) as a window function would require.
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
                FROM read_csv('{src_str}')
            ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        # Step 2: deduplicate from the slim Parquet.
        # MAX_BY(value, key) returns the value from the row with the maximum
        # key — equivalent to the most-recent-certificate logic above but
        # implemented as a GROUP BY aggregate, not a window function.
        con.execute(f"""
            COPY (
                SELECT
                    UPRN,
                    MAX(LODGEMENT_DATETIME) AS LODGEMENT_DATETIME,
                    MAX_BY(TOTAL_FLOOR_AREA, LODGEMENT_DATETIME)
                        AS TOTAL_FLOOR_AREA,
                    MAX_BY(ADDRESS1, LODGEMENT_DATETIME) AS ADDRESS1,
                    MAX_BY(ADDRESS2, LODGEMENT_DATETIME) AS ADDRESS2,
                    MAX_BY(POSTCODE, LODGEMENT_DATETIME) AS POSTCODE,
                    MAX_BY(BUILT_FORM, LODGEMENT_DATETIME) AS BUILT_FORM,
                    MAX_BY(CONSTRUCTION_AGE_BAND, LODGEMENT_DATETIME)
                        AS CONSTRUCTION_AGE_BAND,
                    MAX_BY(CURRENT_ENERGY_RATING, LODGEMENT_DATETIME)
                        AS CURRENT_ENERGY_RATING
                FROM read_parquet('{tmp}')
                WHERE UPRN IS NOT NULL
                GROUP BY UPRN
                UNION ALL
                SELECT * FROM read_parquet('{tmp}') WHERE UPRN IS NULL
            ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
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
                                                upper(s),
                                                '[^\w\s]', '', 'g'
                                            ),
                                            '\s+', ' ', 'g'
                                        ),
                                        '\bAPARTMENT\b', 'FLAT', 'g'
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
) -> int:
    """UPRN-based join. Writes category-A PPD rows matched via the UBDC lookup to dst.

    Returns the number of rows written. Uses DuckDB COPY to stream directly to
    Parquet, avoiding materialising the join result into Python heap memory.
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
            ubdc AS (SELECT * FROM {ubdc_src})
            SELECT
                ppd.transaction_unique_identifier,
                ppd.price, ppd.date_of_transfer, ppd.postcode,
                ppd.property_type, ppd.new_build_flag, ppd.tenure_type,
                ppd.paon, ppd.saon, ppd.street, ppd.locality, ppd.town_city,
                ppd.district, ppd.county, ppd.ppd_category_type, ppd.record_status,
                CAST(ubdc.uprn AS BIGINT) AS uprn,
                epc.TOTAL_FLOOR_AREA, epc.LODGEMENT_DATETIME,
                epc.ADDRESS1, epc.ADDRESS2,
                epc.BUILT_FORM, epc.CONSTRUCTION_AGE_BAND, epc.CURRENT_ENERGY_RATING,
                1 AS match_tier
            FROM ppd
            JOIN ubdc ON ppd.transaction_unique_identifier = ubdc.transactionid
            JOIN epc ON CAST(ubdc.uprn AS BIGINT) = CAST(epc.UPRN AS BIGINT)
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


def join_datasets(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    ubdc_path: str | pathlib.Path,
    dst: pathlib.Path,
    *,
    on_tier1_complete: Callable[[int], None] | None = None,
) -> None:
    """Join PPD to EPC using a tiered strategy, writing result to *dst*.

    Tier 1 — exact UPRN join via the UBDC lookup table.
    Tier 2 — address normalisation fallback for records without a UPRN match.

    Writes a Parquet file to *dst* containing matched records with a
    `match_tier` column (1 or 2).  PPD records with ppd_category_type != 'A'
    are excluded before joining.  Unmatched PPD records are not included.

    All intermediate results are streamed to temp Parquet files via DuckDB
    COPY, so no join result is materialised into Python heap memory.

    If *on_tier1_complete* is provided it is called with the tier-1 row count
    before tier-2 begins, allowing callers to report intermediate progress.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as _tmp:
        tmp = pathlib.Path(_tmp)
        tier1_path = tmp / "tier1.parquet"
        tier2_path = tmp / "tier2.parquet"

        n_tier1 = _join_tier1(ppd_path, epc_path, ubdc_path, tier1_path)
        if on_tier1_complete is not None:
            on_tier1_complete(n_tier1)

        _join_tier2(ppd_path, epc_path, tier1_path, tier2_path)

        duckdb.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{tier1_path}')
                UNION ALL
                SELECT * FROM read_parquet('{tier2_path}')
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


def aggregate_by_geography(
    matched: pd.DataFrame,
    geography: Geography,
    min_sales: int = 10,
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
        df = matched[["postcode", "price", "TOTAL_FLOOR_AREA"]].copy()
        df[col] = df["postcode"].str[:-3].str.strip()
        df.drop(columns=["postcode"], inplace=True)
    else:
        df = matched[[col, "price", "TOTAL_FLOOR_AREA"]].copy()

    df = df[df[col].notna()]

    grouped = (
        df.groupby(col)
        .agg(
            num_sales=("price", "count"),
            total_price=("price", "sum"),
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
    cache_dir: pathlib.Path = CACHE,
    output_dir: pathlib.Path = OUTPUT,
    min_sales: int = 10,
) -> None:
    """Run the full pipeline: join → spatial → aggregate → write CSVs.

    Each heavy step is checkpointed to *cache_dir* as Parquet.  Re-running
    skips any step whose checkpoint already exists, so iterating on later
    stages does not require re-processing the raw data.

    Args:
        ppd_path:      Path to the Price Paid Data CSV.
        epc_path:      Path to the EPC CSV (extracted from bulk ZIP).
        ubdc_path:     Path to the slim UBDC PPD→UPRN Parquet (from make download).
        uprn_path:     Path to the slim OS Open UPRN Parquet (from make download).
        boundary_path: Path to the LSOA boundary file (GeoPackage or GeoJSON).
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
                on_tier1_complete=_on_tier1,
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

    # Steps 3–5: aggregate directly from Parquet files — never load the full
    # matched DataFrame into Python heap.  Peak RSS stays under 1 GB even with
    # a 1+ GB matched.parquet, because DuckDB streams the scans and only
    # materialises small aggregation states (≤35 k LSOA rows, ≤3 k district
    # rows) rather than the full matched result.
    uprn_lsoa_parquet = cache_dir / "uprn_lsoa.parquet"
    con = duckdb.connect()
    _configure_duckdb(con)

    # Step 3: match report — tier counts via DuckDB scan.
    is_parquet = str(ppd_path).endswith(".parquet")
    total_ppd: int = duckdb.execute(
        f"SELECT COUNT(*) FROM {_ppd_source(ppd_path)}"
        + ("" if is_parquet else " WHERE ppd_category_type = 'A'")
    ).fetchone()[0]  # type: ignore[index]
    tier_counts = con.execute(f"""
        SELECT match_tier, COUNT(*) AS n
        FROM read_parquet('{matched_parquet}')
        GROUP BY match_tier
    """).fetchall()
    tier_map: dict[int, int] = {int(t): int(n) for t, n in tier_counts}
    tier1 = tier_map.get(1, 0)
    tier2 = tier_map.get(2, 0)
    unmatched = total_ppd - tier1 - tier2
    console.print(
        f"  Match  "
        f"tier1 [green]{tier1:,}[/green] ({round(100 * tier1 / total_ppd, 1)}%)  "
        f"tier2 [yellow]{tier2:,}[/yellow] ({round(100 * tier2 / total_ppd, 1)}%)  "
        f"unmatched [red]{unmatched:,}[/red] ({round(100 * unmatched / total_ppd, 1)}%)"
    )
    console.print()

    # Step 4: aggregate by postcode district.
    # Postcode district = postcode minus the last 3 characters (the inward code),
    # matching the Python logic: postcode.str[:-3].str.strip().
    output_dir.mkdir(parents=True, exist_ok=True)

    district_df = con.execute(f"""
        SELECT
            TRIM(LEFT(postcode, LENGTH(postcode) - 3)) AS postcode_district,
            COUNT(*) AS num_sales,
            SUM(price) AS total_price,
            SUM(TOTAL_FLOOR_AREA) AS total_floor_area,
            CAST(ROUND(SUM(price) / SUM(TOTAL_FLOOR_AREA)) AS INTEGER)
                AS price_per_sqm
        FROM read_parquet('{matched_parquet}')
        WHERE TOTAL_FLOOR_AREA IS NOT NULL
          AND TOTAL_FLOOR_AREA > 0
          AND postcode IS NOT NULL
        GROUP BY postcode_district
        HAVING COUNT(*) >= {min_sales}
        ORDER BY price_per_sqm DESC
    """).df()
    district_path = output_dir / "price_per_sqm_postcode_district.csv"
    district_df.to_csv(district_path, index=False)
    console.print(
        f"  [green]✓[/green]  {len(district_df):,} postcode districts"
        f"  →  {district_path}"
    )
    del district_df

    # Step 5: aggregate by LSOA — join matched ← uprn_lsoa on disk.
    # Only Tier 1 records have a UPRN; Tier 2 records are excluded here but
    # still appear in the postcode district output above.
    lsoa_df = con.execute(f"""
        SELECT
            l.LSOA21CD,
            COUNT(*) AS num_sales,
            SUM(m.price) AS total_price,
            SUM(m.TOTAL_FLOOR_AREA) AS total_floor_area,
            CAST(ROUND(SUM(m.price) / SUM(m.TOTAL_FLOOR_AREA)) AS INTEGER)
                AS price_per_sqm
        FROM read_parquet('{matched_parquet}') AS m
        JOIN read_parquet('{uprn_lsoa_parquet}') AS l
          ON CAST(m.uprn AS BIGINT) = CAST(l.UPRN AS BIGINT)
        WHERE m.TOTAL_FLOOR_AREA IS NOT NULL
          AND m.TOTAL_FLOOR_AREA > 0
        GROUP BY l.LSOA21CD
        HAVING COUNT(*) >= {min_sales}
        ORDER BY price_per_sqm DESC
    """).df()
    lsoa_path = output_dir / "price_per_sqm_lsoa.csv"
    lsoa_df.to_csv(lsoa_path, index=False)
    console.print(f"  [green]✓[/green]  {len(lsoa_df):,} LSOAs  →  {lsoa_path}")
    console.print()


if __name__ == "__main__":  # pragma: no cover
    run(
        ppd_path=CACHE / "ppd_slim.parquet",
        epc_path=CACHE / "epc_slim.parquet",
        ubdc_path=CACHE / "ubdc_slim.parquet",
        uprn_path=CACHE / "uprn_slim.parquet",
        boundary_path=DATA / "lsoa_boundaries.gpkg",
    )
