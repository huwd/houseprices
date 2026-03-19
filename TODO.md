# Pending GitHub Issues

Local cache of issues to open once signal coverage allows.
Format: `[ ] <title>` with body notes below each entry.

---

## Data acquisition

- [x] **EPC bulk download: confirm auth and URL**

  HTTP Basic Auth (`EPC_EMAIL:EPC_API_KEY`, base64-encoded). Credentials
  confirmed working. `EPC_BULK_URL` set in `download.py`.
  File: `all-domestic-certificates.zip` (~6.4 GB).

- [x] **EPC bulk download: actually download and extract**

  Run `download_epc(data_dir)` once on a good connection. Extract to
  `data/epc-domestic-all.csv` (the pipeline expects a single flat CSV).

- [x] **OS Open UPRN: confirm auth and URL**

  No API key or account required. Direct URL confirmed via OS Data Hub
  Downloads API (`api.os.uk/downloads/v1/products/OpenUPRN/downloads`).
  `OS_OPEN_UPRN_URL` set in `download.py`. ~616 MB zipped, Feb 2026 build.

- [x] **OS Open UPRN: download**

  Run `download_os_open_uprn(data_dir)`. (~616 MB zipped.)

- [x] **UBDC PPD→UPRN lookup: confirm URL**

  API endpoint confirmed: `https://data.ubdc.ac.uk/api/resources/download?file_id=37&dataset_id=13`
  Returns JSON with a time-limited pre-signed Azure blob URL. `UBDC_URL` set
  in `download.py`; `download_ubdc()` resolves the signed URL at call time.

- [x] **UBDC PPD→UPRN lookup: download and validate**

  - Inspect fields — confirm `lmk` maps to `transaction_unique_identifier` in PPD
  - Check whether coverage extends beyond January 2022
  - Count rows; confirm ~96% match rate against PPD for the same period

- [x] **ONS LSOA boundaries: confirm format, CRS, and URL**

  GeoPackage confirmed. CRS is BNG EPSG:27700 — matches OS Open UPRN,
  no reprojection needed in `spatial.py`. `LSOA_BGC_URL` set in `download.py`.
  Dataset: LSOA Dec 2021 BGC V5, item 68515293204e43ca8ab56fa13ae8a547. ~79 MB.

- [x] **ONS LSOA boundaries: download**

  Run `download_lsoa_boundaries(data_dir)`. (~79 MB.)

---

## Data / Research

- [ ] **Measure EPC UPRN population rate by lodgement quarter**

  Research (Boswarva, 2022) shows DLUHC retroactively applied UPRN matching back
  to 2008, achieving ~92% overall coverage. Rates vary by quarter: 90–96% in
  2010–2019, dropping to 82–86% in 2020–2021 (new-build lag). Coverage in the
  2022–2026 portion of the dataset is unknown and should be measured empirically.

  Acceptance criteria:
  - Pipeline prints UPRN coverage % broken down by lodgement year
  - Result appended to `research/uprn-coverage-in-epc-data.md`

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

## Closed GitHub issues (recently resolved)

- [x] **#67 — Inflation-adjust prices to real Jan-2026 £/m² (CPI)**

  Implemented in PR #72. CPI download, deflation helpers, `adj_price_per_sqm`
  in both output CSVs, research note at `research/cpi-deflator-choice.md`.

- [x] **#60 — Improve sale-EPC temporal matching**

  Implemented in PR #72. `_join_tier1` now uses a window-function CTE to select
  the most recent prior EPC (or earliest post-sale fallback within 2 years) per
  sale, with a 10-year gap ceiling. Diagnostic columns `gap_days` and
  `is_post_sale` added to `matched.parquet`. Research note at
  `research/sale-epc-temporal-matching.md`.

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
