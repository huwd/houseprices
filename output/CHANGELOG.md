# Analysis Changelog

## [Unreleased]

### Methodology improvements

#### Address normalisation: hyphens treated as word separators

Hyphens in PPD and EPC address fields are now replaced with a space
before matching, rather than silently removed. Previously, a hyphenated
address such as `CROSS-O-THE-HANDS` was collapsed to
`CROSSOTHEHANDS`, which no longer matched the same address written with
spaces in the other source.

The fix applies to both the Python normaliser and the DuckDB macro used
in tier-2 and tier-3 joins. Non-hyphen punctuation (apostrophes,
periods) is still stripped without adding a space.

Measured gain on the March 2026 dataset: **+2,428 matched records**
across all property types.

- Issue: [#114](https://github.com/huwd/houseprices/issues/114)

#### Address normalisation: article "THE" removed before matching

The word "THE" is now stripped from both PPD and EPC addresses before
the normalised match key is computed. "THE OLD RECTORY" and
"OLD RECTORY" — the same property in different sources — previously
produced different keys and went unmatched.

The removal uses a whole-word match (`\bTHE\b`) so property names that
contain "THE" as a substring (e.g. `THETFORD`, `THEYDON`) are
unaffected. Both sides receive the same transformation, so false
positives require two distinct properties to have the same name
minus the article at the same postcode — structurally very unlikely.

Measured gain on the March 2026 dataset: **+4,995 matched records**
across all property types.

- Issue: [#113](https://github.com/huwd/houseprices/issues/113)

#### Address normalisation: compound property-name words canonicalised

EPC assessors inconsistently split or join compound words in property
names. The following pairs are now collapsed to a single canonical
one-word form before matching:

| Two-word form | One-word canonical |
|---|---|
| FARM HOUSE | FARMHOUSE |
| GATE HOUSE | GATEHOUSE |
| SCHOOL HOUSE | SCHOOLHOUSE |
| MILL HOUSE | MILLHOUSE |
| ALMS HOUSE | ALMSHOUSE |

Unmatched detached counts in the March 2026 data show significant
splits: SCHOOL HOUSE vs SCHOOLHOUSE (3,947 vs 130), MILL HOUSE vs
MILLHOUSE (2,218 vs 54), FARM HOUSE vs FARMHOUSE (3,364 vs 4,048).

Measured gain on the March 2026 dataset: **+342 matched records**
across all property types.

- Issue: [#115](https://github.com/huwd/houseprices/issues/115)

### Workarounds

#### E20 postcode district remapped to E15

E20 (Queen Elizabeth Olympic Park / East Village, Stratford) was created by
Royal Mail circa 2012 — after our Geolytix boundary snapshot — by carving it
out of E15. Because no polygon exists for E20 in our boundary file, any PPD or
EPC record with an E20 postcode was previously silently dropped at aggregation.

As an interim fix, all E20 records are now folded into E15 before aggregation.
E20 and E15 are geographically contiguous and were administered as a single
district before 2012, so this is a reasonable approximation. The workaround
will be removed once a proper E20 polygon is available (issue #81).

Affected output: `price_per_sqm_postcode_district.csv` — E15 figures now
include 884 previously lost E20 sales.

## [0.2.0] — 2026-03-19

### Data vintages

| Source               | v0.1.0                                   | v0.2.0                                                                             |
| -------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------- |
| HM Land Registry PPD | to Jan 2026 (~22.5M rows)                | to March 2026 (~29.3M rows)                                                        |
| EPC bulk export      | per-LA ZIPs, epc.opendatacommunities.org | single monolithic CSV, get-energy-performance-data.communities.gov.uk (March 2026) |
| UBDC PPD→UPRN lookup | unchanged — covers to Jan 2022           | unchanged                                                                          |
| ONS CPI              | D7BT Jan 1988–Jan 2026                   | unchanged                                                                          |

The EPC source changed to the new MHCLG GOV.UK One Login API which delivers
a single 5.7 GB CSV assembled from all local authority feeds. Four CSV
parsing issues were encountered and fixed; see
[`research/epc-csv-data-quality.md`](../research/epc-csv-data-quality.md).

### Methodology changes

#### CPI price deflation — real Jan-2026 £/m²

All sale prices are now inflation-adjusted to January 2026 pounds before
aggregation, using the ONS CPI All Items monthly series (series ID: D7BT,
Jan 1988–Jan 2026, OGL v3.0). The adjustment converts each transaction price
to real terms: `adjusted_price = price × CPI[Jan-2026] / CPI[sale_month]`.

This avoids mixing 1995 pounds with 2024 pounds in the same aggregate.
Postcode districts where most sales occurred before 2010 were systematically
understated in nominal terms; the real-terms figure makes districts comparable
regardless of when their housing stock turned over.

The headline column in both output CSVs is now `adj_price_per_sqm`
(real Jan-2026 £/m²). The nominal `price_per_sqm` is retained as a reference
column.

- Research note: [`research/cpi-deflator-choice.md`](../research/cpi-deflator-choice.md)
- Issue: [#67](https://github.com/huwd/houseprices/issues/67) | PR: [#72](https://github.com/huwd/houseprices/pull/72)

### New and changed output columns

| Column              | Type | Description                                           |
| ------------------- | ---- | ----------------------------------------------------- |
| `adj_price_per_sqm` | int  | **Headline.** Real Jan-2026 £/m² (CPI-adjusted)       |
| `price_per_sqm`     | int  | Nominal £/m² at time of sale — retained for reference |

### Match statistics

| Tier                           | Count          | Share |
| ------------------------------ | -------------- | ----- |
| Tier 1 — UPRN exact match      | 9,255,768      | 31.6% |
| Tier 2 — address normalisation | 7,321,554      | 25.0% |
| Unmatched                      | 12,705,953     | 43.4% |
| **Total PPD sales**            | **29,283,275** |       |

The match rate dropped from 76.9% (v0.1.0) to 56.6%. This is structural:
the PPD gained ~6.8M rows (all post-Jan 2022 transactions) that fall outside
the UBDC UPRN lookup's coverage window. The absolute number of matched
records is broadly unchanged (~16.6M vs ~17.3M). See
[`research/uprn-coverage-in-epc-data.md`](../research/uprn-coverage-in-epc-data.md)
for a detailed breakdown.

### Summary statistics (postcode district, 2,277 districts)

| Metric                     | Value                                        |
| -------------------------- | -------------------------------------------- |
| Districts included         | 2,277                                        |
| Total matched sales        | 16,577,322                                   |
| Median real adj price      | £3,058/m²                                    |
| Most expensive district    | W1S — £35,462/m² (real Jan-2026)             |
| Least expensive district   | TS2 — £733/m² (real Jan-2026)                |
| Top 5 district rankings    | W1S, WC2A, WC2R, W1B, W1K — stable vs v0.1.0 |
| Bottom 5 district rankings | TS2, TS1, BD3, CF43, DN31 — stable vs v0.1.0 |

### Key commits and PRs

| Commit / PR                                                     | Description                                                       |
| --------------------------------------------------------------- | ----------------------------------------------------------------- |
| PR [#72](https://github.com/huwd/houseprices/pull/72) `ebe8619` | CPI inflation adjustment                                          |
| PR [#78](https://github.com/huwd/houseprices/pull/78)           | EPC download: migrate to new MHCLG API                            |
| `4e09bf1`                                                       | fix: atomic download — no partial file on interrupt               |
| `bc2e1a5`                                                       | fix: strict_mode=false — handle backslash-escaped JSON in EPC CSV |
| `943efbe`                                                       | fix: pin quote/escape — prevent single-quote column misdetection  |
| `6630a73`                                                       | fix: null_padding=true — handle short/stub rows                   |
| `fed70dd`                                                       | fix: parallel=false — allow null_padding with quoted newlines     |
| `f6ff4ab`                                                       | perf: two-pass EPC dedup to stay within 2 GB memory limit         |

---

## [0.1.0] — 2026-03-19

**First versioned release of the UK house price per m² analysis.**

### Data sources

| Source                                | Coverage                                                     | Licence                          |
| ------------------------------------- | ------------------------------------------------------------ | -------------------------------- |
| HM Land Registry Price Paid Data      | Standard residential sales to individuals, Aug 2007–Jan 2026 | OGL v3.0                         |
| DLUHC Energy Performance Certificates | All domestic lodgements, England & Wales                     | OGL v3.0                         |
| UBDC PPD–UPRN lookup                  | Transactions up to Jan 2022                                  | Open data, University of Glasgow |
| GeoLytix postcode district boundaries | —                                                            | OGL v3.0                         |

### Methodology

#### 1. Sale selection

Transactions are filtered to standard residential sales to private individuals
(`transaction_category = 'A'`). New-build and established property are both
included. Leasehold and freehold are both included.

#### 2. EPC–sale matching (three tiers)

**Tier 1 — UPRN exact match**
Each sale is joined to the UBDC Price Paid–UPRN lookup table
([doi:10.20394/agu7hprj](https://doi.org/10.20394/agu7hprj)), which provides
a Unique Property Reference Number for transactions up to January 2022. That
UPRN is then joined to the EPC register to retrieve the most recent certificate
lodged _before or on the sale date_. This is the highest-confidence tier.

**Tier 2 — Address normalisation match**
For sales after January 2022 (where no UPRN link exists) and for any earlier
sales that failed tier 1, the sale address is normalised: sub-address, building
name/number, and street name are concatenated and lowercased, punctuation
stripped. The normalised string is matched against the same normalised form in
the EPC register, restricted to the same postcode. The most recent EPC before
the sale date is selected.

**Tier 3 — Postcode fallback**
Remaining unmatched sales are assigned the median floor area of all EPCs within
the same postcode. This tier is lower confidence and flagged in the data.

_Temporal selection note:_ in all tiers, the EPC selected is the most recent
certificate lodged on or before the sale date. This avoids using post-sale
improvements to infer the floor area at the time of sale.

#### 3. Aggregation

Price per m² for each postcode district is computed as:

    price_per_sqm = Σ(sale_price) / Σ(floor_area_m²)

This is a value-weighted aggregate — not a mean of per-property ratios — so
that large properties contribute proportionally to the result.

Districts with fewer than 10 matched sales are excluded from the output.

#### 4. Output

- `price_per_sqm_postcode_district.csv` — primary output: one row per postcode district
- `price_per_sqm_lsoa.csv` — LSOA-level aggregation (experimental)
- `postcode_districts.geojson` — boundary-joined GeoJSON for the web map

### Key commits (reproducibility)

To reproduce this analysis, check out commit `da82e13` (the HEAD at time of
release) and follow the pipeline instructions in `PLAN.md` and `CLAUDE.md`.

| Commit    | Description                                            |
| --------- | ------------------------------------------------------ |
| `91920ab` | Initial pipeline setup                                 |
| `31aeb42` | First full pipeline run — outputs committed            |
| `cee3740` | Add tier-3 normalisation (address-string matching)     |
| `8929d3b` | Regenerate CSVs incorporating tier-3 matches           |
| `30ef34a` | Confirm UBDC lookup coverage ends at Jan 2022          |
| `049aa57` | Add output page with choropleth map                    |
| `da82e13` | Temporal EPC selection in tier-1 join (HEAD at v0.1.0) |

### Summary statistics (at release)

| Metric                   | Value             |
| ------------------------ | ----------------- |
| Total sales              | 22,503,694        |
| Matched with floor area  | ~77%              |
| Districts included       | 2,279             |
| Median price per m²      | £1,986            |
| Data range               | Aug 2007–Jan 2026 |
| Most expensive district  | W1S — £24,184/m²  |
| Least expensive district | TS2 — £504/m²     |
