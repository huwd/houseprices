# Test Fixtures

Small, internally consistent CSV fixtures for unit testing the pipeline join
logic. All data is synthetic — no real addresses or transaction IDs.

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
| `"FLAT 2"`, `"5"`, `"BAKER RD"` | `FLAT 2 5 BAKER ROAD` | `FLAT 2 5 BAKER RD` | `FLAT 2 5 BAKER ROAD` |
| `""`, `"22"`, `"GROVE AVE"` | `22 GROVE AVENUE` | `22 GROVE AVE` | `22 GROVE AVENUE` |
| `""`, `"3"`, `"OAK CL"` | `3 OAK CLOSE` | `3 OAK CL` | `3 OAK CLOSE` |

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
