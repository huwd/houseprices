"""Main pipeline: download → join → aggregate."""

import pathlib
import re

import pandas as pd

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
    ppd = pd.read_csv(ppd_path)
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


def aggregate_by_postcode_district(
    matched: pd.DataFrame,
    min_sales: int = 10,
) -> pd.DataFrame:
    """Aggregate matched records to price per m² by postcode district.

    Postcode district is the outward code: last three characters (the inward
    code) are stripped, e.g. "SW1A 1AA" → "SW1A", "N1 1AA" → "N1".

    Districts with fewer than min_sales transactions are excluded.
    Result is sorted by price_per_sqm descending.
    """
    df = matched.copy()
    df["postcode_district"] = df["postcode"].str[:-3].str.strip()

    grouped = (
        df.groupby("postcode_district")
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
