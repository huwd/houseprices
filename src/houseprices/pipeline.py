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
    raise NotImplementedError


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
    raise NotImplementedError


def aggregate(rows: list[dict[str, float]]) -> dict[str, int]:
    """Aggregate sales rows to price per m² (total price / total area)."""
    total_price = sum(r["price"] for r in rows)
    total_area = sum(r["floor_area"] for r in rows)
    return {"price_per_sqm": round(total_price / total_area)}
