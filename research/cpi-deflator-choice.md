# CPI Deflator Choice — Issue #67

**Decision: CPI All Items (series D7BT), base January 2026.**

---

## The problem

The Price Paid dataset spans 1995 to the present. Aggregating
`total_price / total_floor_area` across all years treats a 1995 pound as
identical to a 2024 pound. General consumer prices roughly doubled over that
period, so a district where most sales occurred in the early 2000s will appear
cheap relative to one where most sales are recent — even if the real purchasing
power per m² is similar. The distortion is largest when comparing districts
with very different temporal distributions of sales.

The fix is to deflate each sale price to real terms relative to a common base
month before summing. The formula applied per row is:

```
adjusted_price = price × (cpi[base_month] / cpi[sale_month])
```

with `base_month = January 2026`. The denominator (floor area in m²) is a
physical quantity and is not adjusted.

---

## Candidate indices

Three ONS series were considered:

| Index | Series ID | History | Status | Notes |
|---|---|---|---|---|
| **CPI** (Consumer Prices Index) | `D7BT` | From Jan 1988 | National Statistic | Excludes owner-occupier housing costs |
| **CPIH** (CPI incl. owner-occupier housing costs) | `L55O` | From Jan 2005 | ONS preferred measure since 2017 | Includes housing via rental equivalence; shorter history |
| **RPI** (Retail Prices Index) | `CHAW` | From Jan 1947 | Legacy measure only | Known upward bias (Carli formula); no longer a National Statistic |

---

## Decision: CPI

CPI was chosen on pragmatic grounds:

- **History**: runs from January 1988, covering the full PPD range (1995–present)
  without any bridging or splicing.
- **Status**: a proper National Statistic with a clear and stable methodology.
  ONS publishes it monthly, it is widely cited, and its licensing is
  unambiguous OGL v3.
- **Licensing**: ONS CPI is published under the Open Government Licence v3.0,
  consistent with PPD and EPC. No additional attribution friction.

CPIH would be more theoretically defensible — it is ONS's preferred headline
measure and includes housing costs — but its series starts in January 2005,
leaving 1995–2004 sales (roughly 6 million transactions) without a native
index value. Bridging with CPI or RPI for that period would introduce a splice
and complicate the methodology. Given that the deflation is applied uniformly
across all districts, CPI's known omission of housing costs does not
systematically bias one district relative to another: it affects the level of
the adjusted figures but not the relative ordering, which is what the
pipeline's output is primarily used for.

RPI was rejected. It is no longer a National Statistic, carries a known
upward bias from the Carli formula, and ONS has recommended against new uses
of it since 2013. UKSA formally ruled that RPI is not a good measure of
inflation in 2012.

---

## Implementation

- **Source**: ONS Consumer Price Inflation dataset
  (`https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceinflation`)
- **Series**: D7BT — CPI All Items Index, not seasonally adjusted (2015=100)
- **Frequency**: Monthly
- **Fetched via**: `download_cpi()` in `download.py`, using the ONS API
  (`api.ons.gov.uk/v1`). Result written to `data/cpi.csv` with columns
  `date` (YYYY-MM) and `cpi` (float).
- **Base month**: January 2026 (`CPI_BASE = (2026, 1)` in `pipeline.py`)
- **Where applied**: in `join_datasets()` at the final UNION ALL step;
  `rematch()` applies the same deflation when appending tier-3 rows.
- **Output**: `adjusted_price` column in `matched.parquet`; `adj_price_per_sqm`
  (headline, sorted on) alongside `price_per_sqm` (nominal reference) in the
  output CSVs.

---

## Interaction with temporal EPC matching (issue #60)

Issue #60 matched each sale to its temporally nearest EPC rather than the
single most-recent certificate per UPRN. These two changes are complementary:

- Issue #60 corrects the **floor area** side of the ratio — using the EPC
  closest in time to the sale improves the accuracy of the m² figure.
- Issue #67 corrects the **price** side — converting all prices to the same
  real-terms base removes inter-year mixing.

Both changes were implemented on the same branch.

---

## Known limitations

- **Relative not absolute**: the adjustment removes the effect of general
  consumer price inflation. It does not adjust for changes in the housing
  market itself — if real house prices rose or fell over the period, those
  changes are preserved in the output.
- **Floor area unadjusted**: we adjust the numerator (price) but not the
  denominator (m²), which is correct — floor area is physical, not monetary.
  The output is labelled "real January-2026 £/m²" to make the base explicit.
- **Materiality**: the distortion only matters where districts have
  systematically different temporal distributions of sales. In practice the
  effect is modest for most districts but can be significant for areas that
  saw heavy turnover in the early 2000s relative to areas with mostly recent
  sales. The empirical analysis (see notebook) quantifies the ranking shifts.
