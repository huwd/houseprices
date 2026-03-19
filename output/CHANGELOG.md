# Analysis Changelog

## [Unreleased]

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
- Issue: [#67](https://github.com/huwd/houseprices/issues/67) | PR: [#72](https://github.com/huwd/houseprices/pull/72) (merged 2026-03-19, commit `ebe8619`)

### New and changed output columns

| Column | Type | Description |
|--------|------|-------------|
| `adj_price_per_sqm` | int | **Headline.** Real Jan-2026 £/m² (CPI-adjusted) |
| `price_per_sqm` | int | Nominal £/m² at time of sale — retained for reference |

### Summary statistics (postcode district, 2,279 districts)

| Metric | Value |
|--------|-------|
| Median real adj uplift vs nominal | +46.8% |
| Uplift range | +25.9% to +75.6% |
| Most expensive district | W1S — £32,660/m² (real Jan-2026) |
| Least expensive district | TS2 — £798/m² (real Jan-2026) |
| Top district rankings | Stable — W1S, WC2A, WC2R unchanged |

### Key commits

| Commit | Description |
|--------|-------------|
| `0669316` | test(red): CPI deflation tests |
| `f912272` | feat(green): CPI deflation functions |
| `ce091db` | feat(green): wire adjusted_price into pipeline and output |
| `87fd755` | data: add ONS CPI monthly index (D7BT, Jan 1988–Jan 2026) |
| `06ed494` | data: regenerate output CSVs with adj_price_per_sqm |
| `ebe8619` | Merge PR #72 — CPI inflation adjustment |

---

## [0.1.0] — 2026-03-19

**First versioned release of the UK house price per m² analysis.**

### Data sources

| Source | Coverage | Licence |
|--------|----------|---------|
| HM Land Registry Price Paid Data | Standard residential sales to individuals, Aug 2007–Jan 2026 | OGL v3.0 |
| DLUHC Energy Performance Certificates | All domestic lodgements, England & Wales | OGL v3.0 |
| UBDC PPD–UPRN lookup | Transactions up to Jan 2022 | Open data, University of Glasgow |
| GeoLytix postcode district boundaries | — | OGL v3.0 |

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
lodged *before or on the sale date*. This is the highest-confidence tier.

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

*Temporal selection note:* in all tiers, the EPC selected is the most recent
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

| Commit | Description |
|--------|-------------|
| `91920ab` | Initial pipeline setup |
| `31aeb42` | First full pipeline run — outputs committed |
| `cee3740` | Add tier-3 normalisation (address-string matching) |
| `8929d3b` | Regenerate CSVs incorporating tier-3 matches |
| `30ef34a` | Confirm UBDC lookup coverage ends at Jan 2022 |
| `049aa57` | Add output page with choropleth map |
| `da82e13` | Temporal EPC selection in tier-1 join (HEAD at v0.1.0) |

### Summary statistics (at release)

| Metric | Value |
|--------|-------|
| Total sales | 22,503,694 |
| Matched with floor area | ~77% |
| Districts included | 2,279 |
| Median price per m² | £1,986 |
| Data range | Aug 2007–Jan 2026 |
| Most expensive district | W1S — £24,184/m² |
| Least expensive district | TS2 — £504/m² |
