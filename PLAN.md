# UK House Prices per Square Metre — Analysis Plan

## What Anna Did (Reference)

Anna Powell-Smith's analysis at houseprices.anna.ps:

> Sale prices from Land Registry's Price Paid dataset of residential property sales to
> individuals since August 2007, with floor area in m² per property taken from Energy
> Performance Certificates. Each property sale is joined to the property's most recent EPC
> using normalised addresses, finding a match 79% of the time for around 6.2 million property
> sales. The aggregate price per m² for each postcode district is calculated as the total price
> of all sales, divided by the total floor area of all properties.

Our repeat: same methodology, latest data (PPD through January 2026, EPC current), with an
output comparison document noting what has changed.

---

## Data Sources

### 1. HM Land Registry Price Paid Data (PPD)

- **URL**: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
- **Format**: CSV, ~4.3GB complete file
- **Coverage**: England and Wales, all residential sales since 1995
- **Latest**: Updated monthly; January 2026 data added March 2026
- **Licence**: OGL (Open Government Licence)
- **Download**: `http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv`

Key fields:
```
transaction_id, price, date_of_transfer, postcode, property_type,
new_build_flag, tenure_type, paon, saon, street, locality,
town_city, district, county, ppd_category_type, record_status
```

Filter to: `ppd_category_type = 'A'` (standard residential, excludes bulk/company sales)

### 2. EPC Domestic Energy Performance Certificates

- **URL**: https://epc.opendatacommunities.org/
- **Format**: ZIP bundles of CSVs, ~5.6GB total (all certificates)
- **Coverage**: England and Wales, ~30 million certificates
- **Licence**: OGL
- **Download**: Bulk download by local authority or full dataset
- **Account required**: Free registration at epc.opendatacommunities.org

Key fields (from CSVW schema):
```
UPRN, ADDRESS1, ADDRESS2, POSTCODE, LODGEMENT_DATETIME,
TOTAL_FLOOR_AREA,   ← primary field: m² internal floor area
PROPERTY_TYPE, BUILT_FORM, CONSTRUCTION_AGE_BAND,
CURRENT_ENERGY_RATING, MAIN_HEAT_DESCRIPTION,
SOLAR_WATER_HEATING_FLAG, MECHANICAL_VENTILATION
```

**Per-property deduplication**: keep only the most recent EPC per address (by
`LODGEMENT_DATETIME`). A property may have multiple EPCs across years.

---

## Methodology

### Address normalisation (the hard part)

Anna matched at 79%. The academic linkage paper (bin-chi.github.io) achieved 93%+ using a
251-rule matching process but required manual correction for historical data. We target
≥80% match rate using a pragmatic approach:

```python
import re

ABBREVIATIONS = {
    r"\bFLAT\b": "FLAT",
    r"\bAPARTMENT\b": "FLAT",
    r"\bST\b": "STREET",
    r"\bRD\b": "ROAD",
    r"\bAVE?\b": "AVENUE",
    r"\bDR\b": "DRIVE",
    r"\bCL\b": "CLOSE",
    r"\bCT\b": "COURT",
    r"\bGDNS\b": "GARDENS",
    r"\bHSE\b": "HOUSE",
}

def normalise_address(paon: str, saon: str, street: str) -> str:
    """Produce a normalised join key from PPD address components."""
    parts = " ".join(filter(None, [saon, paon, street]))
    parts = parts.upper()
    parts = re.sub(r"[^\w\s]", " ", parts)   # strip punctuation
    parts = re.sub(r"\s+", " ", parts).strip()
    for pattern, replacement in ABBREVIATIONS.items():
        parts = re.sub(pattern, replacement, parts)
    return parts

def normalise_epc_address(addr1: str, addr2: str) -> str:
    """Normalise EPC ADDRESS1 + ADDRESS2."""
    parts = " ".join(filter(None, [addr1, addr2]))
    # same normalisation as above
    ...
```

**Join strategy** (applied in order, stop at first match):

1. `postcode` (exact) + `normalised_full_address` (exact) — primary key
2. `postcode` (exact) + house number extracted from both sides
3. `postcode sector` (first 5 chars) + full normalised address — for postcode entry errors

### Aggregation

```sql
-- Per postcode district (e.g. "SW1A")
SELECT
    LEFT(ppd.postcode, LENGTH(ppd.postcode) - 3) AS postcode_district,
    COUNT(*) AS num_sales,
    SUM(ppd.price) AS total_price,
    SUM(epc.TOTAL_FLOOR_AREA) AS total_floor_area,
    ROUND(SUM(ppd.price) / SUM(epc.TOTAL_FLOOR_AREA)) AS price_per_sqm
FROM ppd
JOIN epc_matched ON ppd.transaction_id = epc_matched.transaction_id
WHERE ppd.date_of_transfer >= '2019-01-01'   -- configurable lookback window
GROUP BY postcode_district
HAVING COUNT(*) >= 10   -- minimum sample size
ORDER BY price_per_sqm DESC
```

Anna used all data since 2007 for her aggregate. Consider offering two outputs:
- **All-time** (Aug 2007 → Jan 2026): comparable to Anna's methodology
- **Recent** (2022 → Jan 2026): reflects current market, removes pandemic-era noise

---

## Implementation: DuckDB

DuckDB is the right tool for this. It reads multi-GB CSVs in parallel without loading into
memory, has SQL joins, and outputs Parquet. No database setup required.

```python
import duckdb

con = duckdb.connect()

# Load PPD (stream from CSV, no full load)
con.execute("""
    CREATE VIEW ppd AS
    SELECT * FROM read_csv_auto('pp-complete.csv',
        columns={
            'transaction_id': 'VARCHAR',
            'price': 'INTEGER',
            'date_of_transfer': 'DATE',
            'postcode': 'VARCHAR',
            'property_type': 'VARCHAR',
            'new_build_flag': 'VARCHAR',
            'tenure_type': 'VARCHAR',
            'paon': 'VARCHAR',
            'saon': 'VARCHAR',
            'street': 'VARCHAR',
            'locality': 'VARCHAR',
            'town_city': 'VARCHAR',
            'district': 'VARCHAR',
            'county': 'VARCHAR',
            'ppd_category_type': 'VARCHAR',
            'record_status': 'VARCHAR',
        },
        header=False
    )
    WHERE ppd_category_type = 'A'
""")

# Load EPC (deduplicated — latest per address)
con.execute("""
    CREATE VIEW epc_latest AS
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY POSTCODE, ADDRESS1, ADDRESS2
                   ORDER BY LODGEMENT_DATETIME DESC
               ) AS rn
        FROM read_csv_auto('epc/*.csv')
    ) WHERE rn = 1
    AND TOTAL_FLOOR_AREA > 0
    AND TOTAL_FLOOR_AREA < 2000   -- sanity filter: exclude data errors
""")
```

For performance, materialise normalised keys as Parquet after first run:

```python
# ~5 mins first run, then <30s on subsequent runs
con.execute("""
    COPY (
        SELECT
            transaction_id,
            postcode,
            upper(regexp_replace(
                concat_ws(' ', saon, paon, street),
                '[^\\w\\s]', ' '
            )) AS norm_key,
            price,
            date_of_transfer
        FROM ppd
    ) TO 'ppd_normalised.parquet' (FORMAT PARQUET)
""")
```

---

## Project Structure

```
houseprices/
  data/                    # gitignored — raw downloaded data
    pp-complete.csv
    epc/                   # extracted from bulk download ZIPs
  cache/                   # gitignored — intermediate parquet files
    ppd_normalised.parquet
    epc_normalised.parquet
    matched.parquet
  output/
    price_per_sqm.csv      # final output — committed
    comparison.md          # writeup vs Anna's data — committed
  src/
    normalise.py           # address normalisation functions
    join.py                # join orchestration
    aggregate.py           # postcode district aggregation
    compare.py             # comparison vs Anna's data
    download.py            # data download helpers
  tests/
    test_normalise.py
    test_join.py
    test_aggregate.py
    fixtures/
      ppd_sample.csv       # 500-row sample for tests
      epc_sample.csv       # matched sample for tests
  Makefile                 # download → normalise → join → aggregate → compare
  requirements.txt
  README.md
```

---

## Tests

### Unit: address normalisation

```python
# tests/test_normalise.py
import pytest
from src.normalise import normalise_ppd, normalise_epc, make_join_key

@pytest.mark.parametrize("saon, paon, street, expected", [
    ("FLAT 2", "12", "HIGH STREET", "FLAT 2 12 HIGH STREET"),
    ("", "12A", "ST JOHNS RD", "12A ST JOHNS ROAD"),       # RD expansion
    ("APARTMENT 4B", "THE GABLES", "GROVE AVE", "FLAT 4B THE GABLES GROVE AVENUE"),
    ("", "Rose Cottage", "Church Lane", "ROSE COTTAGE CHURCH LANE"),
])
def test_normalise_ppd(saon, paon, street, expected):
    assert normalise_ppd(saon, paon, street) == expected

def test_join_key_stable():
    """Same property should produce same key regardless of whitespace."""
    k1 = make_join_key("SW1A 1AA", "FLAT 1", "10", "DOWNING ST")
    k2 = make_join_key("SW1A1AA", "flat 1", "10", "Downing Street")
    assert k1 == k2
```

### Unit: aggregation correctness

```python
# tests/test_aggregate.py
def test_price_per_sqm_calculation():
    """Aggregate is total price / total area, not mean of per-property ratios."""
    rows = [
        {"price": 200_000, "floor_area": 50},   # £4000/m²
        {"price": 400_000, "floor_area": 200},  # £2000/m²
    ]
    # Correct: (600,000) / (250) = £2400/m²
    # Wrong (mean of ratios): (4000 + 2000) / 2 = £3000/m²
    assert aggregate(rows)["price_per_sqm"] == 2400
```

### Integration: join rate

```python
# tests/test_join.py
def test_join_rate_above_threshold(sample_ppd, sample_epc):
    """Join rate should exceed Anna's 79% on a clean sample."""
    matched, unmatched = run_join(sample_ppd, sample_epc)
    rate = len(matched) / (len(matched) + len(unmatched))
    assert rate >= 0.79, f"Join rate {rate:.1%} below threshold"
```

### Output validation

```python
def test_output_sanity(output_csv):
    df = pd.read_csv(output_csv)
    assert (df["price_per_sqm"] > 0).all()
    assert (df["price_per_sqm"] < 50_000).all()   # no outlier districts
    assert df["postcode_district"].nunique() > 2000
    assert df["num_sales"].min() >= 10   # minimum sample filter applied
```

---

## Makefile

```makefile
.PHONY: download normalise join aggregate compare test

download:
	python src/download.py

normalise:
	python src/normalise.py  # → cache/ppd_normalised.parquet + cache/epc_normalised.parquet

join:
	python src/join.py       # → cache/matched.parquet

aggregate:
	python src/aggregate.py  # → output/price_per_sqm.csv

compare:
	python src/compare.py    # → output/comparison.md

test:
	pytest tests/ -v

all: normalise join aggregate compare
```

---

## Comparison Output: What's Changed Since Anna's Analysis

`compare.py` reads `output/price_per_sqm.csv` and Anna's published data (scraped or from her
GitHub if available) and produces `output/comparison.md` covering:

```markdown
## Headline changes

- National median price per m²: £X,XXX (Anna: £X,XXX, +X%)
- London median: £X,XXX (Anna: £X,XXX, +X%)
- Cheapest district: XX (£X,XXX/m²)

## Biggest increases since Anna's analysis

| Postcode district | Anna £/m² | Now £/m² | Change |
|---|---|---|---|
| SW1A | £12,000 | £15,200 | +27% |
...

## Biggest decreases

...

## Areas that changed rank significantly

Districts that moved 100+ positions in the national ranking.

## Geographic observations

[written narrative]
```

Note: if Anna's underlying data isn't programmatically accessible, record her published
headline numbers manually in a `data/anna_reference.json` file and use that for comparison.

---

## Performance Notes

On a modern laptop:
- PPD CSV load + normalise: ~3 minutes (DuckDB parallel read)
- EPC CSV load + dedup + normalise: ~8 minutes (larger dataset)
- Join: ~5 minutes
- Aggregation: <1 minute
- Total first run: ~20 minutes
- Subsequent runs using cached Parquet: ~2 minutes

All times are estimates; DuckDB performance varies by disk I/O speed. Running on the NAS
directly (if needed) is viable but SSH + local execution on the laptop with NAS-mounted data
is faster.

---

## References

- [Land Registry Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)
- [EPC Open Data Communities](https://epc.opendatacommunities.org/)
- [Academic linkage methodology](https://bin-chi.github.io/Link-LR-PPD-and-Domestic-EPCs/)
- [Anna's analysis](http://houseprices.anna.ps/)
- [DuckDB](https://duckdb.org/)
