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
| **File** | `epc/` (bulk download ZIPs, extracted) |
| **Licence** | Open Government Licence v3.0 |
| **Provider** | Department for Energy Security and Net Zero (DESNZ) |
| **URL** | https://epc.opendatacommunities.org/ |
| **Coverage** | England and Wales, ~30 million certificates from 2008 |
| **Updated** | Monthly |
| **Format** | ZIP bundles of CSVs, ~5.6 GB total |
| **Account required** | Free registration at epc.opendatacommunities.org |

Note: records include a `UPRN` field backfilled by DLUHC via address-matching
against OS AddressBase (~92% coverage back to 2008). See `research/uprn-coverage-in-epc-data.md`.

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

## Licence summary

All datasets are published under the Open Government Licence v3.0 (OGL):
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

The OGL permits use, adaptation, and redistribution with attribution. It is
compatible with Creative Commons Attribution Licence 4.0.
