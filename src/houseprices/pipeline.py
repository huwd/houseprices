"""Main pipeline: download → join → aggregate."""

import os
import pathlib
import re
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
    """
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    _configure_duckdb(con)
    con.execute(f"""
        COPY (
            WITH
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPRN
                        ORDER BY LODGEMENT_DATETIME DESC
                    ) AS _rn
                FROM read_csv('{src}')
                WHERE UPRN IS NOT NULL
            )
            SELECT
                UPRN, LODGEMENT_DATETIME, TOTAL_FLOOR_AREA,
                ADDRESS1, ADDRESS2, POSTCODE,
                BUILT_FORM, CONSTRUCTION_AGE_BAND, CURRENT_ENERGY_RATING
            FROM ranked WHERE _rn = 1
            UNION ALL
            SELECT
                UPRN, LODGEMENT_DATETIME, TOTAL_FLOOR_AREA,
                ADDRESS1, ADDRESS2, POSTCODE,
                BUILT_FORM, CONSTRUCTION_AGE_BAND, CURRENT_ENERGY_RATING
            FROM read_csv('{src}')
            WHERE UPRN IS NULL
        ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


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
) -> pd.DataFrame:
    """UPRN-based join. Returns category-A PPD rows matched via the UBDC lookup."""
    con = duckdb.connect()
    _configure_duckdb(con)
    ppd_src = _ppd_source(ppd_path)
    epc_src = _sql_source(epc_path)
    ubdc_src = _sql_source(ubdc_path)
    return con.execute(f"""
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
    """).df()


def _join_tier2(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    tier1: pd.DataFrame,
) -> pd.DataFrame:
    """Address-normalisation join. Returns PPD rows not already matched in tier1."""
    con = duckdb.connect()
    _configure_duckdb(con)
    con.execute(_NORMALISE_MACRO)
    con.register("_tier1", tier1)
    ppd_src = _ppd_source(ppd_path)
    epc_src = _sql_source(epc_path)
    return con.execute(f"""
        WITH
        epc AS (SELECT * FROM {epc_src}),
        ppd AS (
            SELECT * FROM {ppd_src}
            WHERE ppd_category_type = 'A'
        ),
        ppd_remaining AS (
            SELECT * FROM ppd
            WHERE transaction_unique_identifier NOT IN (
                SELECT transaction_unique_identifier FROM _tier1
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
    """).df()


def join_datasets(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    ubdc_path: str | pathlib.Path,
    *,
    on_tier1_complete: Callable[[pd.DataFrame], None] | None = None,
) -> pd.DataFrame:
    """Join PPD to EPC using a tiered strategy.

    Tier 1 — exact UPRN join via the UBDC lookup table.
    Tier 2 — address normalisation fallback for records without a UPRN match.

    Returns a DataFrame of matched records with a `match_tier` column (1 or 2).
    PPD records with ppd_category_type != 'A' are excluded before joining.
    Unmatched PPD records are not included in the result.

    If *on_tier1_complete* is provided it is called with the tier-1 DataFrame
    before tier-2 begins, allowing callers to report intermediate progress.
    """
    tier1 = _join_tier1(ppd_path, epc_path, ubdc_path)
    if on_tier1_complete is not None:
        on_tier1_complete(tier1)
    tier2 = _join_tier2(ppd_path, epc_path, tier1)
    return pd.concat([tier1, tier2], ignore_index=True)


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
    df = matched.copy()
    col = geography.value

    if geography is Geography.POSTCODE_DISTRICT:
        df[col] = df["postcode"].str[:-3].str.strip()

    df = df[df[col].notna()].copy()

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
    def _on_tier1(df: pd.DataFrame) -> None:
        console.print(f"      [dim]tier 1: {len(df):,} UPRN matches[/dim]")

    matched = step(
        "matched",
        lambda: join_datasets(
            ppd_path, epc_path, ubdc_path, on_tier1_complete=_on_tier1
        ),
    )
    uprn_lsoa = step("uprn_lsoa", lambda: build_uprn_lsoa(uprn_path, boundary_path))
    console.print()

    # Step 3: attach LSOA codes to matched records via UPRN.
    # Only Tier 1 records have a UBDC-confirmed UPRN (`uprn` column);
    # Tier 2 address-matched records get LSOA21CD = NaN and are excluded
    # from the LSOA output but still appear in the postcode district output.
    uprn_to_lsoa: pd.Series = uprn_lsoa.set_index("UPRN")["LSOA21CD"]
    matched["LSOA21CD"] = matched["uprn"].map(uprn_to_lsoa)

    # Step 4: match report — count rows in the slim PPD (all category A).
    is_parquet = str(ppd_path).endswith(".parquet")
    total_ppd = duckdb.execute(
        f"SELECT COUNT(*) FROM {_ppd_source(ppd_path)}"
        + ("" if is_parquet else " WHERE ppd_category_type = 'A'")
    ).fetchone()[0]  # type: ignore[index]
    report = match_report(matched, total_ppd)
    console.print(
        f"  Match  "
        f"tier1 [green]{report['tier1']:,}[/green] ({report['tier1_pct']}%)  "
        f"tier2 [yellow]{report['tier2']:,}[/yellow] ({report['tier2_pct']}%)  "
        f"unmatched [red]{report['unmatched']:,}[/red] ({report['unmatched_pct']}%)"
    )
    console.print()

    # Step 5: aggregate and write outputs
    output_dir.mkdir(parents=True, exist_ok=True)

    district_df = aggregate_by_geography(
        matched, Geography.POSTCODE_DISTRICT, min_sales=min_sales
    )
    district_path = output_dir / "price_per_sqm_postcode_district.csv"
    district_df.to_csv(district_path, index=False)
    console.print(
        f"  [green]✓[/green]  {len(district_df):,} postcode districts"
        f"  →  {district_path}"
    )

    lsoa_df = aggregate_by_geography(matched, Geography.LSOA, min_sales=min_sales)
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
