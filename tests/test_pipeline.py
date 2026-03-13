"""Tests for pipeline.py: address normalisation and aggregation."""

import pytest

from houseprices.pipeline import aggregate, normalise_address

# ---------------------------------------------------------------------------
# normalise_address
# ---------------------------------------------------------------------------


def test_normalise_address_basic() -> None:
    assert normalise_address("FLAT 2", "12", "HIGH STREET") == "FLAT 2 12 HIGH STREET"


def test_normalise_address_empty_saon() -> None:
    assert normalise_address("", "12A", "ST JOHNS RD") == "12A ST JOHNS ROAD"


def test_normalise_address_apartment_to_flat() -> None:
    assert (
        normalise_address("APARTMENT 4B", "THE GABLES", "GROVE AVE")
        == "FLAT 4B THE GABLES GROVE AVENUE"
    )


def test_normalise_address_uppercases_input() -> None:
    assert normalise_address("flat 2", "12", "high street") == "FLAT 2 12 HIGH STREET"


def test_normalise_address_removes_punctuation() -> None:
    # Apostrophes and periods stripped; ST prefix (saint) not expanded to STREET
    assert normalise_address("", "12A", "ST. JOHN'S ROAD") == "12A ST JOHNS ROAD"


def test_normalise_address_collapses_whitespace() -> None:
    assert normalise_address("", "12A", "HIGH  STREET") == "12A HIGH STREET"


def test_normalise_address_drive_abbreviation() -> None:
    assert normalise_address("", "4", "MANOR DR") == "4 MANOR DRIVE"


def test_normalise_address_close_abbreviation() -> None:
    assert normalise_address("", "7", "OAK CL") == "7 OAK CLOSE"


def test_normalise_address_gardens_abbreviation() -> None:
    assert normalise_address("", "3", "ROSE GDNS") == "3 ROSE GARDENS"


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------


def test_price_per_sqm_uses_total_not_mean_of_ratios() -> None:
    """Aggregate must be total_price / total_area, not mean of per-property ratios."""
    rows = [
        {"price": 200_000, "floor_area": 50},  # £4 000/m²
        {"price": 400_000, "floor_area": 200},  # £2 000/m²
    ]
    # Correct:          600_000 / 250 = £2 400/m²
    # Wrong (mean):  (4000 + 2000) / 2 = £3 000/m²
    assert aggregate(rows)["price_per_sqm"] == 2400


def test_price_per_sqm_single_record() -> None:
    rows = [{"price": 300_000, "floor_area": 100}]
    assert aggregate(rows)["price_per_sqm"] == 3000


def test_aggregate_raises_on_empty_input() -> None:
    with pytest.raises((ValueError, ZeroDivisionError)):
        aggregate([])
