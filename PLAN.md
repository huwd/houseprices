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

**Note**: Base PPD does not include UPRNs. The UBDC PPD→UPRN lookup table
(see Data Source 5) provides a pre-built OGL lookup covering 96% of PPD records
from 1995–January 2022. This is the primary path for the join.

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

EPC records include a `UPRN` field; coverage is partial and must be measured
empirically on the actual dataset.

### 3. OS Open UPRN

- **URL**: https://www.ordnancesurvey.co.uk/products/os-open-uprn
- **Format**: CSV, one row per UPRN
- **Coverage**: All addressable properties in Great Britain
- **Licence**: OGL
- **Key fields**: `UPRN, X_COORDINATE, Y_COORDINATE` (British National Grid eastings/northings)

Used to resolve a UPRN to a point coordinate. BNG coordinates can be converted
to WGS84 (lat/lon) or used directly with projected boundary files.

### 4. UBDC Price Paid Data → UPRN Lookup

- **URL**: https://data.ubdc.ac.uk/dataset/a999fd05-e7fe-4243-ab9a-95ce98132956
- **Format**: zip (CSV inside)
- **Coverage**: PPD records January 1995 to January 2022
- **Match rate**: 96% of PPD records successfully linked
- **Licence**: OGL
- **Citation**: Urban Big Data Centre (2023). https://doi.org/10.20394/agu7hprj
- **Last updated**: 10 March 2026 — may extend beyond January 2022, verify on download

Key fields: `lmk` (PPD transaction identifier), `UPRN`, `USRN`

Produced by University of Glasgow using a 142-rule rules-based methodology
matching PPD address fields to OS AddressBase Plus. Resolves the need for
address normalisation on the PPD side for the covered date range.

**Note**: verify that `lmk` maps to the `transaction_id` field in the PPD CSV
before joining.

### 5. ONS Output Area / LSOA / MSOA Boundaries

- **URL**: https://geoportal.statistics.gov.uk/
- **Format**: GeoJSON or Shapefile
- **Coverage**: England and Wales
- **Licence**: OGL
- **Variants**: Output Areas (OA, ~40k), Lower Super Output Areas (LSOA, ~33k),
  Middle Super Output Areas (MSOA, ~7k)

Used for point-in-polygon lookups: assign each UPRN coordinate to its containing
OA/LSOA/MSOA. This gives a cleaner, population-normalised geography than postcode
district string truncation.

Note: postcode district output is still produced for comparability with Anna's
analysis (using Code Point Open postcode centroids or the postcode field directly).

### 6. UBDC EPC → UPRN Linkage (reference only)

The same OS-funded project also produced an EPC→UPRN linkage using 446 matching
rules. However, DLUHC has already incorporated UPRN matching into the published
EPC dataset (~92% coverage back to 2008), so this is not needed as a data source.
The methodology (see https://github.com/urbanbigdatacentre/os_epc_ppd_linkage)
may inform our Tier 2 address normalisation fallback.

---

## Methodology

### Join strategy: UPRN-first, address normalisation as fallback

Anna matched at 79% using address normalisation. We use a tiered approach that
prioritises exact UPRN matching, falling back to normalisation only where UPRNs
are unavailable.

**Tier 1 — UPRN direct join** (exact, no fuzzy logic):

PPD records are resolved to a UPRN via the UBDC lookup table (96% coverage,
1995–January 2022). EPC records carry a UPRN via DLUHC's backfilled matching
(~92% coverage back to 2008). Both UPRNs are derived from OS AddressBase so
they are consistent identifiers — a direct join is valid.

```python
# Load UBDC lookup: lmk → UPRN
ubdc = con.execute("SELECT lmk, UPRN FROM ubdc_lookup").df()

# Tier 1: join PPD → UBDC → UPRN, then UPRN → EPC
matched_uprn = (
    ppd
    .merge(ubdc, left_on="transaction_id", right_on="lmk")
    .merge(epc[epc.UPRN.notna()], on="UPRN")
    .assign(match_tier=1)
)
```

Estimated Tier 1 reach: ~96% (UBDC) × ~92% (EPC UPRN) ≈ **~88% of 1995–2022
sales**. PPD records from 2022–2026 are not in the UBDC lookup and go straight
to Tier 2.

**Tier 2 — Address normalisation fallback** (for records missing a UPRN on
either side):

Applied in order, stop at first match:

1. `postcode` (exact) + `normalised_full_address` (exact) — primary key
2. `postcode` (exact) + house number extracted from both sides
3. `postcode sector` (first 5 chars) + full normalised address — for postcode entry errors

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
    parts = " ".join(filter(None, [saon, paon, street]))
    parts = parts.upper()
    parts = re.sub(r"[^\w\s]", " ", parts)
    parts = re.sub(r"\s+", " ", parts).strip()
    for pattern, replacement in ABBREVIATIONS.items():
        parts = re.sub(pattern, replacement, parts)
    return parts
```

**Match rate reporting**: the join pipeline records which tier each match came
from, so we can see UPRN match rate vs normalisation match rate vs unmatched,
and understand where investment in the fallback logic pays off.

### Geography assignment via spatial lookup

For EPC records with a UPRN, use OS Open UPRN to resolve to a coordinate, then
do a point-in-polygon lookup against ONS boundary files to assign OA/LSOA/MSOA.
DuckDB's spatial extension handles this without needing geopandas:

```sql
-- Install once: INSTALL spatial; LOAD spatial;

-- Assign each UPRN to its LSOA
CREATE TABLE uprn_lsoa AS
SELECT
    u.UPRN,
    l.LSOA21CD,
    l.LSOA21NM
FROM os_open_uprn u
JOIN lsoa_boundaries l
  ON ST_Within(
      ST_Point(u.X_COORDINATE, u.Y_COORDINATE),
      ST_GeomFromWKB(l.geometry)
  );
```

This produces two output geographies:
- **Postcode district** — via `LEFT(postcode, ...)` truncation, for Anna comparison
- **LSOA/MSOA** — via spatial lookup, for richer analysis

Records without a UPRN (i.e. fallback normalisation matches) use postcode only
and are excluded from the LSOA output.

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

## Implementation

### Tools

- **DuckDB** — reads multi-GB CSVs in parallel without loading into memory, handles
  SQL joins, spatial extension for point-in-polygon, outputs Parquet. No database
  setup required.
- **Jupyter notebook** — for the analysis and comparison step, where interactive
  exploration is the right mode.

### Structure: pipeline script + notebook

The heavy data processing lives in `src/houseprices/pipeline.py`, a single script that runs
top-to-bottom and checkpoints at each stage as Parquet. The analysis and writeup
live in `notebooks/analysis.ipynb`, which reads from those Parquet files.

`src/houseprices/pipeline.py` is organised as a sequence of clearly-named functions:

```python
def download_data(): ...        # fetch PPD, EPC, OS Open UPRN, ONS boundaries
def load_ppd(): ...             # stream CSV → DuckDB view, filter to category A
def load_epc(): ...             # stream CSVs → deduplicate to latest per address
def build_uprn_lsoa(): ...      # spatial join: UPRN coordinates → LSOA (checkpoint)
def join_datasets(): ...        # Tier 1 UPRN join, Tier 2 normalisation fallback (checkpoint)
def aggregate(): ...            # postcode district + LSOA outputs (checkpoint)
```

Each checkpoint writes a Parquet file to `cache/`. On rerun, `pipeline.py` skips
steps whose output already exists — so iterating on the join logic doesn't require
re-downloading 10GB of CSVs.

```python
import duckdb, pathlib

CACHE = pathlib.Path("cache")

def checkpoint(name: str, con, query: str) -> str:
    """Write query result to cache/{name}.parquet if not already present."""
    path = CACHE / f"{name}.parquet"
    if path.exists():
        print(f"  [skip] {name} (cached)")
        return str(path)
    print(f"  [run]  {name}")
    con.execute(f"COPY ({query}) TO '{path}' (FORMAT PARQUET)")
    return str(path)
```

`notebooks/analysis.ipynb` reads `cache/matched.parquet` and
`output/price_per_sqm_*.csv`, produces the comparison writeup, and any
charts. This is the only place where exploration happens interactively.

---

## Project Structure

```
houseprices/
  data/                    # gitignored — raw downloaded data
    pp-complete.csv
    epc/                   # extracted from bulk download ZIPs
    os_open_uprn.csv       # OS Open UPRN (UPRN → BNG coordinates)
    lsoa_boundaries.*      # ONS LSOA boundary file (GeoJSON or GeoParquet)
    ppd_uprn.csv           # HMLR UPRN-linked PPD (if available/OGL — TBC)
  cache/                   # gitignored — pipeline checkpoints
    uprn_lsoa.parquet      # UPRN → LSOA spatial join result
    matched.parquet        # joined PPD+EPC with match_tier column
  output/                  # committed
    price_per_sqm_postcode_district.csv
    price_per_sqm_lsoa.csv
  src/houseprices/
    __init__.py
    pipeline.py            # main pipeline: download → join → aggregate
    spatial.py             # UPRN coordinate → LSOA point-in-polygon (used by pipeline)
  notebooks/
    analysis.ipynb         # reads cache/ + output/; comparison vs Anna, charts
  tests/
    test_spatial.py        # unit tests for spatial lookup
    test_pipeline.py       # aggregation maths, join tier logic
    fixtures/
      ppd_sample.csv
      epc_sample.csv
      uprn_sample.csv
  research/                # notes and findings — committed
  Makefile
  pyproject.toml
  README.md
  TODO.md                  # pending GitHub issues (not committed to main)
```

---

## Tests

Tests cover the two parts of the codebase that are genuinely unit-testable:
spatial lookup correctness and aggregation maths. The pipeline itself is
validated by running it and inspecting the match-rate report.

### Unit: aggregation correctness

```python
# tests/test_pipeline.py
def test_price_per_sqm_calculation():
    """Aggregate is total price / total area, not mean of per-property ratios."""
    rows = [
        {"price": 200_000, "floor_area": 50},   # £4000/m²
        {"price": 400_000, "floor_area": 200},  # £2000/m²
    ]
    # Correct: (600,000) / (250) = £2400/m²
    # Wrong (mean of ratios): (4000 + 2000) / 2 = £3000/m²
    assert aggregate(rows)["price_per_sqm"] == 2400

def test_address_normalisation():
    cases = [
        (("FLAT 2", "12", "HIGH STREET"), "FLAT 2 12 HIGH STREET"),
        (("", "12A", "ST JOHNS RD"), "12A ST JOHNS ROAD"),
        (("APARTMENT 4B", "THE GABLES", "GROVE AVE"), "FLAT 4B THE GABLES GROVE AVENUE"),
    ]
    for (saon, paon, street), expected in cases:
        assert normalise_address(saon, paon, street) == expected
```

### Unit: spatial lookup

```python
# tests/test_spatial.py
def test_point_in_polygon(fixture_uprn, fixture_lsoa_boundaries):
    """Known UPRN coordinate should resolve to expected LSOA."""
    result = build_uprn_lsoa(fixture_uprn, fixture_lsoa_boundaries)
    assert result.loc[result.UPRN == 12345678, "LSOA21CD"].iloc[0] == "E01000001"

def test_bng_coordinates_not_swapped(fixture_uprn):
    """Easting should be ~100k–700k, Northing ~0–1300k for England & Wales."""
    assert fixture_uprn["X_COORDINATE"].between(100_000, 700_000).all()
    assert fixture_uprn["Y_COORDINATE"].between(0, 1_300_000).all()
```

### Output sanity (run after full pipeline)

```python
def test_output_sanity(output_csv):
    df = pd.read_csv(output_csv)
    assert (df["price_per_sqm"] > 0).all()
    assert (df["price_per_sqm"] < 50_000).all()
    assert df["postcode_district"].nunique() > 2000
    assert df["num_sales"].min() >= 10
```

---

## Makefile

```makefile
.PHONY: pipeline notebook test clean

pipeline:
	python src/houseprices/pipeline.py   # download → join → aggregate → output/

notebook:
	jupyter nbconvert --to notebook --execute notebooks/analysis.ipynb

test:
	pytest tests/ -v

clean:
	rm -rf cache/            # force full rerun from raw data

all: pipeline notebook
```

`pipeline.py` skips stages whose Parquet checkpoint already exists, so
partial reruns are fast. `make clean` forces a full rerun from raw data.

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
