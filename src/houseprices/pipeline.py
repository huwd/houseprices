"""Main pipeline: download → join → aggregate."""

import pathlib
import re
from collections.abc import Callable
from enum import Enum

import duckdb
import pandas as pd

from houseprices.spatial import build_uprn_lsoa

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


def join_datasets(
    ppd_path: str | pathlib.Path,
    epc_path: str | pathlib.Path,
    ubdc_path: str | pathlib.Path,
) -> pd.DataFrame:
    """Join PPD to EPC using a tiered strategy.

    Tier 1 — exact UPRN join via the UBDC lookup table.
    Tier 2 — address normalisation fallback for records without a UPRN match.

    Returns a DataFrame of matched records with a `match_tier` column (1 or 2).
    PPD records with ppd_category_type != 'A' are excluded before joining.
    Unmatched PPD records are not included in the result.
    """
    con = duckdb.connect()
    con.execute(_NORMALISE_MACRO)

    ppd = str(ppd_path)
    epc = str(epc_path)
    ubdc = str(ubdc_path)

    return con.execute(f"""
        WITH
        -- EPC: deduplicate to most recent certificate per UPRN
        epc_raw AS (SELECT * FROM read_csv('{epc}')),
        epc_ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY UPRN
                    ORDER BY LODGEMENT_DATETIME DESC
                ) AS _rn
            FROM epc_raw
            WHERE UPRN IS NOT NULL
        ),
        epc AS (
            SELECT * EXCLUDE (_rn) FROM epc_ranked WHERE _rn = 1
            UNION ALL
            SELECT * FROM epc_raw WHERE UPRN IS NULL
        ),
        -- PPD: category A only (HMLR full download has no header row)
        ppd AS (
            SELECT * FROM read_csv('{ppd}', header=false, names=[
                'transaction_unique_identifier', 'price', 'date_of_transfer',
                'postcode', 'property_type', 'new_build_flag', 'tenure_type',
                'paon', 'saon', 'street', 'locality', 'town_city',
                'district', 'county', 'ppd_category_type', 'record_status'
            ])
            WHERE ppd_category_type = 'A'
        ),
        ubdc AS (SELECT * FROM read_csv('{ubdc}')),

        -- Tier 1: exact UPRN match via UBDC lookup
        tier1 AS (
            SELECT
                ppd.transaction_unique_identifier,
                ppd.price,
                ppd.date_of_transfer,
                ppd.postcode,
                ppd.property_type,
                ppd.new_build_flag,
                ppd.tenure_type,
                ppd.paon,
                ppd.saon,
                ppd.street,
                ppd.locality,
                ppd.town_city,
                ppd.district,
                ppd.county,
                ppd.ppd_category_type,
                ppd.record_status,
                CAST(ubdc.uprn AS BIGINT) AS uprn,
                epc.TOTAL_FLOOR_AREA,
                epc.LODGEMENT_DATETIME,
                epc.ADDRESS1,
                epc.ADDRESS2,
                epc.BUILT_FORM,
                epc.CONSTRUCTION_AGE_BAND,
                epc.CURRENT_ENERGY_RATING,
                1 AS match_tier
            FROM ppd
            JOIN ubdc ON ppd.transaction_unique_identifier = ubdc.transactionid
            JOIN epc ON CAST(ubdc.uprn AS BIGINT) = CAST(epc.UPRN AS BIGINT)
        ),

        -- Tier 2: address-normalisation fallback for unmatched PPD records
        ppd_remaining AS (
            SELECT * FROM ppd
            WHERE transaction_unique_identifier NOT IN (
                SELECT transaction_unique_identifier FROM tier1
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
        ),
        tier2 AS (
            SELECT
                p.transaction_unique_identifier,
                p.price,
                p.date_of_transfer,
                p.postcode,
                p.property_type,
                p.new_build_flag,
                p.tenure_type,
                p.paon,
                p.saon,
                p.street,
                p.locality,
                p.town_city,
                p.district,
                p.county,
                p.ppd_category_type,
                p.record_status,
                NULL::BIGINT AS uprn,
                e.TOTAL_FLOOR_AREA,
                e.LODGEMENT_DATETIME,
                e.ADDRESS1,
                e.ADDRESS2,
                e.BUILT_FORM,
                e.CONSTRUCTION_AGE_BAND,
                e.CURRENT_ENERGY_RATING,
                2 AS match_tier
            FROM ppd_norm AS p
            JOIN epc_norm AS e
                ON p.postcode_norm = e.postcode_norm
               AND p.norm_addr = e.norm_addr
        )
        SELECT * FROM tier1
        UNION ALL
        SELECT * FROM tier2
    """).df()


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
        ubdc_path:     Path to the UBDC PPD→UPRN lookup CSV.
        uprn_path:     Path to the OS Open UPRN CSV.
        boundary_path: Path to the LSOA boundary file (GeoPackage or GeoJSON).
        cache_dir:     Directory for Parquet checkpoints (default: cache/).
        output_dir:    Directory for output CSVs (default: output/).
        min_sales:     Minimum sales per geography unit to include in output.
    """
    # Step 1: join PPD + EPC via UBDC UPRN lookup + address normalisation
    matched = _checkpoint(
        "matched",
        cache_dir,
        lambda: join_datasets(ppd_path, epc_path, ubdc_path),
    )

    # Step 2: build UPRN → LSOA spatial lookup
    uprn_lsoa = _checkpoint(
        "uprn_lsoa",
        cache_dir,
        lambda: build_uprn_lsoa(uprn_path, boundary_path),
    )

    # Step 3: attach LSOA codes to matched records via UPRN.
    # Only Tier 1 records have a UBDC-confirmed UPRN (`uprn` column);
    # Tier 2 address-matched records get LSOA21CD = NaN and are excluded
    # from the LSOA output but still appear in the postcode district output.
    uprn_to_lsoa: pd.Series = uprn_lsoa.set_index("UPRN")["LSOA21CD"]
    matched["LSOA21CD"] = matched["uprn"].map(uprn_to_lsoa)

    # Step 4: match report
    ppd_meta = pd.read_csv(
        ppd_path,
        header=None,
        usecols=[14],
        names=["ppd_category_type"],
        engine="python",
        on_bad_lines="warn",
    )
    total_ppd = int((ppd_meta["ppd_category_type"] == "A").sum())
    report = match_report(matched, total_ppd)
    print(
        f"\nMatch report — "
        f"tier1: {report['tier1']} ({report['tier1_pct']}%), "
        f"tier2: {report['tier2']} ({report['tier2_pct']}%), "
        f"unmatched: {report['unmatched']} ({report['unmatched_pct']}%)\n"
    )

    # Step 5: aggregate and write outputs
    output_dir.mkdir(parents=True, exist_ok=True)

    district_df = aggregate_by_geography(
        matched, Geography.POSTCODE_DISTRICT, min_sales=min_sales
    )
    district_path = output_dir / "price_per_sqm_postcode_district.csv"
    district_df.to_csv(district_path, index=False)
    print(f"  wrote {len(district_df)} postcode districts → {district_path}")

    lsoa_df = aggregate_by_geography(matched, Geography.LSOA, min_sales=min_sales)
    lsoa_path = output_dir / "price_per_sqm_lsoa.csv"
    lsoa_df.to_csv(lsoa_path, index=False)
    print(f"  wrote {len(lsoa_df)} LSOAs → {lsoa_path}")


if __name__ == "__main__":  # pragma: no cover
    run(
        ppd_path=DATA / "pp-complete.csv",
        epc_path=DATA / "epc-domestic-all.csv",
        ubdc_path=DATA / "ppd-uprn-lookup.csv",
        uprn_path=DATA / "os-open-uprn.csv",
        boundary_path=DATA / "lsoa_boundaries.gpkg",
    )
