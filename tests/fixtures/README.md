# Test Fixtures

Small, internally consistent CSV fixtures for unit testing the pipeline join
logic. All data is entirely synthetic — fictional transaction IDs, invented
prices and floor areas, and postcodes in the "SD" area (Sodor), which is not
a real Royal Mail postcode area. Place names (Knapford, Tidmouth, Wellsworth,
Arlesburgh) are from the fictional Island of Sodor. No real property, address,
or geographic data is reproduced here.

---

## Join case matrix

Each PPD row exercises a specific path through the tiered join:

| PPD row | Transaction ID | Expected outcome | Why |
|---|---|---|---|
| 1 | `{A0000001-...}` | **Tier 1 match** | UBDC lookup → UPRN 100001; EPC has UPRN 100001 |
| 2 | `{A0000002-...}` | **Tier 2 match** (address) | UBDC lookup → UPRN 100002; EPC has no UPRN → fall to address normalisation |
| 3 | `{A0000003-...}` | **Tier 2 match** (post-2022) | Not in UBDC (2023 date); EPC has UPRN 100003 but PPD can't resolve → address normalisation |
| 4 | `{A0000004-...}` | **Tier 2 match** (no UPRNs either side) | Not in UBDC; EPC has no UPRN → address normalisation |
| 5 | `{A0000005-...}` | **Unmatched** | Not in UBDC; no EPC row for this address |
| 6 | `{A0000006-...}` | **Filtered out** | `ppd_category_type = 'B'` — excluded before join |

---

## Address normalisation consistency

Tier 2 matches rely on `normalise_address` producing the same string from both
sides. Confirmed mappings:

| PPD (saon, paon, street) | Normalised | EPC ADDRESS1 | Normalised |
|---|---|---|---|
| `"FLAT 2"`, `"5"`, `"TIDMOUTH RD"` | `FLAT 2 5 TIDMOUTH ROAD` | `FLAT 2 5 TIDMOUTH RD` | `FLAT 2 5 TIDMOUTH ROAD` |
| `""`, `"22"`, `"HARBOUR AVE"` | `22 HARBOUR AVENUE` | `22 HARBOUR AVE` | `22 HARBOUR AVENUE` |
| `""`, `"3"`, `"VIADUCT CL"` | `3 VIADUCT CLOSE` | `3 VIADUCT CL` | `3 VIADUCT CLOSE` |

---

## EPC deduplication

EPC rows 1 and 5 share UPRN 100001 (same property, two certificates):

| Row | UPRN | LODGEMENT_DATETIME | TOTAL_FLOOR_AREA | CURRENT_ENERGY_RATING |
|---|---|---|---|---|
| 1 | 100001 | 2020-01-15 | 80.0 | C |
| 5 | 100001 | 2018-06-01 | 78.0 | D |

The deduplication step must keep row 1 (most recent lodgement). Tests should
assert that the matched floor area is 80.0, not 78.0.

---

## UBDC lookup

| transactionid | uprn | method | Notes |
|---|---|---|---|
| `{A0000001-...}` | 100001 | method1 | Used in Tier 1 match |
| `{A0000002-...}` | 100002 | method3 | EPC has no UPRN → Tier 2 |
| `{A0000006-...}` | 100099 | method1 | Cat B row — filtered before join reaches UBDC |

Rows 3, 4, 5 intentionally absent — they are post-2022 or otherwise not
covered by the UBDC lookup.

---

## Postcode districts (for aggregation tests)

| District | Rows | total_price | total_floor_area | price_per_sqm |
|---|---|---|---|---|
| SD1 | 1, 2, 3 | 750,000 | 230.0 | 3,261 |
| SD2 | 4 | 150,000 | 65.0 | 2,308 |

---

## Temporal matching fixtures (issue #60)

`epc_temporal.csv`, `ppd_temporal.csv`, `ubdc_temporal.csv` exercise the
temporal EPC selection logic introduced in issue #60.  All data is synthetic;
the postcode district SD3 and place name Suddery are fictional (Sodor).

### EPC history for UPRN 200001 — 14 Engine Lane, SD3 1AA

| Lodgement date | TOTAL_FLOOR_AREA | CURRENT_ENERGY_RATING | Notes |
|---|---|---|---|
| 2015-03-01 | 70.0 m² | E | Earliest certificate |
| 2019-06-01 | 85.0 m² | D | Middle certificate |
| 2022-09-01 | 95.0 m² | C | Most recent certificate |

### PPD sales of UPRN 200001 — expected temporal match

| Transaction | Sale date | Expected EPC | Floor area | Reason |
|---|---|---|---|---|
| TXN-T01 | 2020-06-01 | 2019-06-01 | 85.0 m² | Most recent **prior** EPC (not 2022, which is post-sale) |
| TXN-T02 | 2013-04-01 | 2015-03-01 | 70.0 m² | No prior EPC → earliest **post-sale** fallback |
| TXN-T03 | 2000-01-01 | — | — | All EPCs > 10 years after sale — **excluded** by max-gap cutoff |

### UBDC lookup

All three transactions map to UPRN 200001 (method1).
