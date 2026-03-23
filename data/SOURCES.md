# Data Sources

All datasets used by this pipeline. Files themselves are gitignored.
This document records where each file came from, its licence, and how to obtain it.

---

## 1. HM Land Registry Price Paid Data (PPD)

| | |
|---|---|
| **File** | `pp-complete.csv` |
| **Licence** | Open Government Licence v3.0 |
| **Provider** | HM Land Registry |
| **URL** | https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads |
| **Direct download** | http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv |
| **Coverage** | England and Wales, all residential sales since 1995 |
| **Updated** | Monthly |
| **Format** | CSV, ~4.3 GB |

Attribution: Contains HM Land Registry data © Crown copyright and database right 2024.
This data is licensed under the Open Government Licence v3.0.

---

## 2. Domestic Energy Performance Certificates (EPC)

| | |
|---|---|
| **File** | `epc-domestic-all.csv` (extracted from bulk ZIP) |
| **Licence** | Split — see below |
| **Provider** | Ministry of Housing, Communities & Local Government (MHCLG) |
| **URL** | https://get-energy-performance-data.communities.gov.uk/ |
| **API docs** | https://get-energy-performance-data.communities.gov.uk/api-technical-documentation |
| **OAS spec** | https://raw.githubusercontent.com/communitiesuk/epb-data-warehouse/main/api/api.yml |
| **Bulk endpoint** | `GET /api/files/domestic/csv` → HTTP 302 → pre-signed S3 ZIP |
| **Coverage** | England and Wales, ~30 million certificates from 2008 |
| **Updated** | Monthly, regenerated on the 1st |
| **Format** | ZIP of year-split CSVs (~2.9 GB) |
| **Account required** | GOV.UK One Login at get-energy-performance-data.communities.gov.uk |
| **Auth** | Bearer token (`EPC_BEARER_TOKEN` env var); retrieve from `/api/my-account` |

**Licence split:**
- **Non-address fields** (`UPRN`, `TOTAL_FLOOR_AREA`, `LODGEMENT_DATETIME`,
  `BUILT_FORM`, `CONSTRUCTION_AGE_BAND`, `CURRENT_ENERGY_RATING`, etc.) —
  Open Government Licence v3.0
- **Address fields** (`ADDRESS1`, `ADDRESS2`, `ADDRESS3`, `POSTCODE`) —
  OS AddressBase Premium / Royal Mail PAF copyright; use permitted for energy
  efficiency analysis and property market transparency (our use case); raw
  address data must not be published at record level

This pipeline uses postcode only as a join/grouping key and publishes only
aggregate statistics — no raw address strings appear in output CSVs.

Note: records include a `UPRN` field backfilled by DLUHC via address-matching
against OS AddressBase (~92% coverage back to 2008). See `research/uprn-coverage-in-epc-data.md`.

Migration note: the previous platform (`epc.opendatacommunities.org`) used
HTTP Basic Auth. Replaced by GOV.UK One Login bearer token as of 2026-03.
See `research/epc-api-migration.md` for full migration details.

---

## 3. OS Open UPRN

| | |
|---|---|
| **File** | `os_open_uprn.csv` |
| **Licence** | Open Government Licence v3.0 |
| **Provider** | Ordnance Survey |
| **URL** | https://www.ordnancesurvey.co.uk/products/os-open-uprn |
| **Coverage** | All addressable properties in Great Britain |
| **Format** | CSV |
| **Key fields** | `UPRN`, `X_COORDINATE`, `Y_COORDINATE` (British National Grid, EPSG:27700) |

Used to resolve a UPRN to a point coordinate for spatial geography assignment.

---

## 4. UBDC Price Paid Data → UPRN Lookup

| | |
|---|---|
| **File** | `ubdc_ppd_uprn.csv` (or similar, check on download) |
| **Licence** | Open Government Licence v3.0 |
| **Provider** | Urban Big Data Centre, University of Glasgow |
| **URL** | https://data.ubdc.ac.uk/dataset/a999fd05-e7fe-4243-ab9a-95ce98132956 |
| **DOI** | https://doi.org/10.20394/agu7hprj |
| **Coverage** | PPD records January 1995 to January 2022 (verify current extent on download) |
| **Match rate** | 96% of PPD records |
| **Format** | zip |
| **Key fields** | `lmk` (PPD transaction ID), `UPRN`, `USRN` |

Citation: Urban Big Data Centre (2023). Price paid data to UPRN lookup [Data set].
University of Glasgow. https://doi.org/10.20394/agu7hprj

Note: confirm that `lmk` maps to the `transaction_unique_identifier` field in the
PPD CSV before joining. See `research/ubdc-ppd-uprn-lookup.md`.

---

## 5. ONS Output Area / LSOA / MSOA Boundaries

| | |
|---|---|
| **File** | `lsoa_boundaries.*` |
| **Licence** | Open Government Licence v3.0 |
| **Provider** | Office for National Statistics (ONS) |
| **URL** | https://geoportal.statistics.gov.uk/ |
| **Coverage** | England and Wales |
| **Format** | GeoJSON or GeoParquet (TBC — see TODO) |
| **Variants** | Output Areas (~40k), LSOAs (~33k), MSOAs (~7k) |

Used for point-in-polygon lookups: assign each UPRN coordinate to its containing
LSOA/MSOA. CRS may be WGS84 (EPSG:4326) — confirm compatibility with OS Open UPRN
BNG coordinates (EPSG:27700) before running spatial join.

---

## 6. ONS Consumer Price Index (CPI)

| | |
|---|---|
| **File** | `cpi.csv` (committed — small, ~300 rows) |
| **Licence** | Open Government Licence v3.0 |
| **Provider** | Office for National Statistics (ONS) |
| **Dataset** | Consumer Price Inflation |
| **URL** | https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceinflation |
| **Download URL** | https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/d7bt/mm23 |
| **Series** | D7BT — CPI All Items Index, not seasonally adjusted (2015=100) |
| **Coverage** | Monthly from January 1988 |
| **Format** | Fetched as JSON via ONS API; written to CSV with columns `date` (YYYY-MM) and `cpi` (float) |
| **Fetched by** | `download_cpi()` in `download.py` |

Used to convert nominal sale prices to real January-2026 pounds before
aggregation.  See `research/cpi-deflator-choice.md` for the rationale.

Note: `cpi.csv` is committed to the repository (unlike other `data/` files)
because it is small and the deflation produces different output CSVs if the
base values change.  Refresh with `make download` when ONS publish new months.

Attribution: Contains National Statistics data © Crown copyright and database
right 2026.  Licensed under the Open Government Licence v3.0.

---

## Disk space

Estimated storage requirements:

| | Size |
|---|---|
| PPD CSV | ~4.3 GB |
| EPC ZIPs | ~5.6 GB |
| EPC extracted CSVs | ~11 GB (rough 2× estimate) |
| OS Open UPRN | ~1 GB |
| UBDC PPD→UPRN lookup | <500 MB |
| ONS LSOA boundaries | ~100 MB |
| Cache (Parquet checkpoints) | ~3–5 GB |
| **Total** | **~25–30 GB** |

**Note**: EPC ZIPs and extracted CSVs will coexist briefly during extraction,
peaking at ~17 GB for that dataset alone. Delete the ZIPs after extraction.

As of March 2026, the laptop has ~51 GB free. This is workable but tight.
If space becomes an issue, point `data/` at the NAS instead.

---

## 7. Geolytix PostalBoundariesOpen

| | |
|---|---|
| **File** | `postcode_districts.geojson` (generated from `geolytix_postal_boundaries.zip`) |
| **Licence** | OGL + Geolytix attribution — see below |
| **Provider** | Geolytix Ltd |
| **URL** | https://geolytix.com/blog/postal-boundaries/ |
| **Direct download** | Google Drive (see `download.py` `GEOLYTIX_URL`) |
| **Coverage** | Great Britain, 2736 postcode districts (2012 vintage) |
| **Format** | ZIP of ZIPs — contains `PostalBoundariesSHP.zip` with `PostalDistrict.shp` (BNG Airy 1830) |
| **Key field** | `PostDist` (4-char string, e.g. `SW1A`) |
| **Prepared by** | `scripts/prepare_boundaries.py` reprojects BNG→WGS84 and writes `data/postcode_districts.geojson` |

**Licence:**
Open Government Licence v3.0 for the Ordnance Survey boundary data.
Additional attribution required: "Postal Boundaries © GeoLytix copyright and
database right 2012; Contains Ordnance Survey data © Crown copyright and
database right 2012."

**Note on E20 (Olympic Park):** E20 was created in late 2012, after this
dataset was compiled. It is absent from `PostalDistrict.shp` (0 features).
`build_page.py` detects districts with price data but no Geolytix geometry
and writes them to `output/missing_districts.txt`; the page explains why
they are absent from the map. The ONS ArcGIS FeatureServer that was intended
as a boundary fallback for E20 was confirmed retired in March 2026 — see
`research/ons-postcode-boundary-service-retired.md` and issue [#81][i81]
for options. The E20 → E15 remap (issue [#80][i80]) remains under consideration.

[i80]: https://github.com/huwd/houseprices/issues/80
[i81]: https://github.com/huwd/houseprices/issues/81

This replaces the previous boundary source (`scripts/fetch_boundaries.py`)
which scraped Anna Powell-Smith's Mapbox tileset and is no longer maintained.

---

## Licence summary

Most datasets are published under the Open Government Licence v3.0 (OGL):
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

The OGL permits use, adaptation, and redistribution with attribution. It is
compatible with Creative Commons Attribution Licence 4.0.

Exception: the Geolytix PostalBoundariesOpen data (§7) requires additional
attribution beyond the OGL — see §7 for the required attribution statement.
