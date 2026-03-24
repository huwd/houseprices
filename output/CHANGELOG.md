# Analysis Changelog

## [1.0.0] — 2026-03-24

### Breaking change — output schema

The district CSV now includes a `property_type` column. Each postcode district
has one `ALL` row (the previous single row) plus one row per property type:
`D` detached, `F` flat/maisonette, `S` semi-detached, `T` terraced, `O` other.
Consumers relying on one-row-per-district must filter to `property_type = ALL`.

- Issue: [#69](https://github.com/huwd/houseprices/issues/69)

### New outputs

- **`price_per_sqm_yearly_postcode_district.csv`** — annual aggregates per
  district, enabling the year-range filter on the web map.
- **`price_per_sqm_lsoa.csv`** — LSOA-level aggregation (promoted from
  experimental); used to derive the new MSOA choropleth.

### Matching improvements

#### Tier 4 — ADDRESS1-only join for named properties

A fourth matching tier recovers rural named properties where PPD has no
`street` field. When `paon` contains no digits, PPD is keyed on
`(postcode, normalise_addr(paon))` and matched against EPC
`(postcode, normalise_addr(ADDRESS1))`, ignoring ADDRESS2. Only 1:1 matches
on both sides are accepted to limit false positives.

- Issue: [#116](https://github.com/huwd/houseprices/issues/116)

#### Address normaliser improvements

Three fixes applied to both the Python normaliser and the DuckDB macro:

| Fix | Gain | Issue |
|-----|------|-------|
| Hyphens treated as word separators (`CROSS-O-THE-HANDS` → `CROSS O THE HANDS`) | +2,428 records | [#114](https://github.com/huwd/houseprices/issues/114) |
| Article "THE" stripped as whole word (`THE OLD RECTORY` → `OLD RECTORY`) | +4,995 records | [#113](https://github.com/huwd/houseprices/issues/113) |
| Compound words canonicalised (FARM HOUSE → FARMHOUSE, etc.) | +342 records | [#115](https://github.com/huwd/houseprices/issues/115) |

Gains measured on the March 2026 dataset.

### Workaround — E20 remapped to E15

E20 (Queen Elizabeth Olympic Park / East Village, Stratford) was created by
Royal Mail after our Geolytix boundary snapshot and has no polygon. All E20
records are now folded into the geographically contiguous E15, recovering 884
previously lost sales.

- Issue: [#80](https://github.com/huwd/houseprices/issues/80)

### Summary statistics (2,276 districts)

| Metric | Value |
|--------|-------|
| Districts included | 2,276 |
| Total matched sales | 16,577,000 |
| Median real adj price | £3,058/m² |
| Most expensive | W1S — £35,462/m² (real Jan-2026 £) |
| Least expensive | TS2 — £733/m² (real Jan-2026 £) |
| Top 5 | W1S, WC2A, WC2R, W1B, W1K |
| Bottom 5 | DN31, CF43, BD3, TS1, TS2 |

---

## [0.2.0] — 2026-03-19

### Data vintages

| Source | v0.1.0 | v0.2.0 |
|--------|--------|--------|
| HM Land Registry PPD | to Jan 2026 (~22.5M rows) | to March 2026 (~29.3M rows) |
| EPC bulk export | per-LA ZIPs, epc.opendatacommunities.org | single monolithic CSV, get-energy-performance-data.communities.gov.uk (March 2026) |
| UBDC PPD→UPRN lookup | unchanged — covers to Jan 2022 | unchanged |
| ONS CPI | D7BT Jan 1988–Jan 2026 | unchanged |

The EPC source changed to the new MHCLG GOV.UK One Login API (single 5.7 GB
CSV). Four CSV parsing issues were encountered and fixed; see
[`research/epc-csv-data-quality.md`](../research/epc-csv-data-quality.md).

### CPI price deflation — real Jan-2026 £/m²

All sale prices are now inflation-adjusted to January 2026 pounds before
aggregation, using ONS CPI All Items monthly series D7BT. The headline column
is `adj_price_per_sqm`; nominal `price_per_sqm` is retained as a reference.

- Issue: [#67](https://github.com/huwd/houseprices/issues/67) | PR: [#72](https://github.com/huwd/houseprices/pull/72)

### Match statistics

| Tier | Count | Share |
|------|-------|-------|
| Tier 1 — UPRN exact match | 9,255,768 | 31.6% |
| Tier 2 — address normalisation | 7,321,554 | 25.0% |
| Unmatched | 12,705,953 | 43.4% |
| **Total PPD sales** | **29,283,275** | |

Match rate dropped from 76.9% (v0.1.0) to 56.6% because the PPD gained
~6.8M post-Jan-2022 rows outside the UBDC lookup window. The absolute
matched count is broadly unchanged (~16.6M vs ~17.3M).

### Summary statistics (2,277 districts)

| Metric | Value |
|--------|-------|
| Districts included | 2,277 |
| Total matched sales | 16,577,322 |
| Median real adj price | £3,058/m² |
| Most expensive | W1S — £35,462/m² |
| Least expensive | TS2 — £733/m² |

---

## [0.1.0] — 2026-03-19

**First versioned release.**

### Data sources

| Source | Coverage | Licence |
|--------|----------|---------|
| HM Land Registry Price Paid Data | Standard residential sales, Aug 2007–Jan 2026 | OGL v3.0 |
| DLUHC Energy Performance Certificates | All domestic lodgements, England & Wales | OGL v3.0 |
| UBDC PPD–UPRN lookup | Transactions to Jan 2022 | Open data, University of Glasgow |
| GeoLytix postcode district boundaries | — | OGL v3.0 |

### Methodology

Price per m² = Σ(sale_price) / Σ(floor_area_m²) per postcode district.
Sales matched to EPCs via UPRN (tier 1), address normalisation (tier 2),
then postcode-median floor area fallback (tier 3). Districts with fewer than
10 matched sales excluded.

### Summary statistics

| Metric | Value |
|--------|-------|
| Total sales | 22,503,694 |
| Matched with floor area | ~77% |
| Districts included | 2,279 |
| Median price per m² | £1,986 |
| Data range | Aug 2007–Jan 2026 |
| Most expensive | W1S — £24,184/m² |
| Least expensive | TS2 — £504/m² |
