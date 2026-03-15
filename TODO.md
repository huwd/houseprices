# Pending GitHub Issues

Local cache of issues to open once signal coverage allows.
Format: `[ ] <title>` with body notes below each entry.

---

## Data / Research

- [ ] **Download and validate UBDC PPD→UPRN lookup**

  The UBDC lookup table (OGL, 96% match rate, 1995–January 2022) is the primary
  source of UPRNs for PPD records. Needs downloading and validating before use.

  URL: https://data.ubdc.ac.uk/dataset/a999fd05-e7fe-4243-ab9a-95ce98132956
  DOI: https://doi.org/10.20394/agu7hprj

  Acceptance criteria:
  - Download zip, inspect fields (`lmk`, `UPRN`, `USRN`)
  - Confirm `lmk` maps to the `transaction_id` / `transaction_unique_identifier`
    field in the PPD CSV (verify column name match)
  - Check whether the updated March 2026 version extends coverage beyond
    January 2022
  - Count rows; confirm ~96% coverage against PPD record count for same period
  - Add to `src/houseprices/pipeline.py` download step

- [ ] **Measure EPC UPRN population rate by lodgement quarter**

  Research (Boswarva, 2022) shows DLUHC retroactively applied UPRN matching back
  to 2008, achieving ~92% overall coverage. Rates vary by quarter: 90–96% in
  2010–2019, dropping to 82–86% in 2020–2021 (new-build lag). Coverage in the
  2022–2026 portion of the dataset is unknown and should be measured empirically.

  Acceptance criteria:
  - Pipeline prints UPRN coverage % broken down by lodgement year
  - Result appended to `research/uprn-coverage-in-epc-data.md`

- [ ] **Confirm ONS boundary file format for DuckDB spatial extension**

  DuckDB spatial can read GeoJSON and GeoParquet natively. The ONS geoportal
  publishes boundaries in multiple formats. Identify which format works best
  with DuckDB's `ST_Within` / `ST_GeomFromWKB` for LSOA point-in-polygon
  lookups, and document the download URL and any CRS considerations
  (OS Open UPRN uses BNG EPSG:27700; ONS boundaries may be WGS84 EPSG:4326).

---

## Implementation

- [x] **Build pipeline.py: tiered join with match-rate reporting**

  Implemented in `src/houseprices/pipeline.py`. Full test coverage.

- [x] **Build spatial.py: UPRN → LSOA point-in-polygon**

  Implemented in `src/houseprices/spatial.py` using DuckDB spatial extension.
  Result materialised as `cache/uprn_lsoa.parquet`. Full test coverage.

- [x] **Produce dual-geography output**

  `run()` in `pipeline.py` emits:
  - `output/price_per_sqm_postcode_district.csv`
  - `output/price_per_sqm_lsoa.csv`

- [ ] **Build analysis.ipynb**

  Notebook that reads `cache/matched.parquet` and `output/` CSVs and produces:
  - Comparison table vs Anna's published figures
  - Biggest movers (up and down) by postcode district
  - Optional choropleth if LSOA output is viable
  - Narrative writeup exported to `output/comparison.md`

---

## Stretch goals (open only after primary pipeline is working)

- [ ] **Address normalisation: improve fallback match rate**

  Once we know what proportion of records fall through to Tier 2 (normalisation),
  assess whether investing in a better normalisation approach is worthwhile.
  Reference: bin-chi.github.io 251-rule methodology achieving 93%+.

- [ ] **MSOA-level output and choropleth map**

  Aggregate to MSOA (7k areas) for a map-friendly output. MSOAs are the right
  granularity for a choropleth: not so granular that sample sizes become thin,
  not so coarse as to hide intra-city variation.
