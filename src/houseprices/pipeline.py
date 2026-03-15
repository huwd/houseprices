"""Main pipeline: download → join → aggregate."""

import pathlib
import re
from collections.abc import Callable
from enum import Enum

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
    df = pd.read_csv(epc_path)
    df["LODGEMENT_DATETIME"] = pd.to_datetime(df["LODGEMENT_DATETIME"])

    with_uprn = df[df["UPRN"].notna()].copy()
    without_uprn = df[df["UPRN"].isna()].copy()

    with_uprn = with_uprn.sort_values("LODGEMENT_DATETIME", ascending=False)
    with_uprn = with_uprn.drop_duplicates(subset=["UPRN"], keep="first")

    return pd.concat([with_uprn, without_uprn], ignore_index=True)


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
    ppd = pd.read_csv(
        ppd_path,
        header=None,
        names=[
            "transaction_unique_identifier",
            "price",
            "date",
            "postcode",
            "property_type",
            "old_new",
            "duration",
            "paon",
            "saon",
            "street",
            "locality",
            "town",
            "district",
            "county",
            "ppd_category_type",
            "record_status",
        ],
        engine="python",
        on_bad_lines="warn",
    )
    ppd = ppd[ppd["ppd_category_type"] == "A"].copy()

    epc = load_epc(epc_path)
    ubdc = pd.read_csv(ubdc_path)

    # Tier 1: exact UPRN join via UBDC lookup
    epc_with_uprn = epc[epc["UPRN"].notna()].copy()
    epc_with_uprn["UPRN"] = epc_with_uprn["UPRN"].astype(int)

    tier1 = (
        ppd.merge(
            ubdc,
            left_on="transaction_unique_identifier",
            right_on="transactionid",
        )
        .merge(epc_with_uprn, left_on="uprn", right_on="UPRN")
        .assign(match_tier=1)
    )

    # Tier 2: address normalisation for records not matched in Tier 1
    matched = set(tier1["transaction_unique_identifier"])
    ppd_remaining = ppd[~ppd["transaction_unique_identifier"].isin(matched)].copy()

    ppd_remaining["norm_addr"] = ppd_remaining.apply(
        lambda r: normalise_address(
            str(r["saon"]) if pd.notna(r["saon"]) else "",
            str(r["paon"]) if pd.notna(r["paon"]) else "",
            str(r["street"]) if pd.notna(r["street"]) else "",
        ),
        axis=1,
    )
    ppd_remaining["postcode_norm"] = ppd_remaining["postcode"].str.strip().str.upper()

    epc_tier2 = epc.copy()
    epc_tier2["norm_addr"] = epc_tier2.apply(
        lambda r: normalise_address(
            str(r["ADDRESS1"]) if pd.notna(r["ADDRESS1"]) else "",
            str(r["ADDRESS2"]) if pd.notna(r["ADDRESS2"]) else "",
            "",
        ),
        axis=1,
    )
    epc_tier2["postcode_norm"] = epc_tier2["POSTCODE"].str.strip().str.upper()

    tier2 = ppd_remaining.merge(epc_tier2, on=["postcode_norm", "norm_addr"]).assign(
        match_tier=2
    )

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
