# Draft GitHub issue: Property type segmentation in price-per-sqm output

**Title**: Property type segmentation in price-per-sqm output

**Labels**: enhancement, output, analysis

---

## Summary

Add `property_type` as a segmentation dimension in the aggregated output,
producing per-geography, per-type price-per-sqm figures alongside the existing
all-types aggregate.

## Motivation

The current aggregate mixes property types that have systematically different
price-per-sqm characteristics. The turnover-frequency bias identified in
issue #60 is most visible across property types: frequently-traded smaller
properties (flats, terraced houses) typically command *higher* price per m²
than large rarely-traded properties (detached houses), particularly in urban
areas where land cost dominates.

This creates a composition effect: a postcode district with a high flat/terraced
ratio will show a different aggregate price-per-sqm than one with a high
detached ratio, independent of the actual market level.

A concrete testable hypothesis: London studio flats have higher price per m²
than 5-bedroom detached houses in the same postcode district, because land
cost is largely fixed per plot regardless of house size. Property type
segmentation would let us confirm or refute this directly from the data.

Segmenting by property type lets users:

- Compare like-for-like across geographies (detached vs detached, not a mix)
- Examine the composition effect directly — how much of the district-level
  variation is property type mix vs genuine price level?
- Control for the turnover-frequency bias from issue #60 by filtering to a
  single type

## Data available

Both data sources carry property type:

- **PPD**: `property_type` — D (detached), S (semi-detached), T (terraced),
  F (flats/maisonettes), O (other)
- **EPC**: `PROPERTY_TYPE` (free-text: "Flat", "House", "Bungalow",
  "Maisonette", "Park home") and `BUILT_FORM` ("Detached", "Semi-Detached",
  "Mid-Terrace", "End-Terrace", "Enclosed Mid-Terrace", "Enclosed End-Terrace")

PPD `property_type` is already carried through to `matched.parquet`. EPC
`BUILT_FORM` adds finer-grained structure type on top of PPD's classification.
PPD `property_type` is sufficient for a first implementation; EPC `BUILT_FORM`
can be added later for sub-type breakdowns (e.g. mid-terrace vs end-terrace).

## Proposed output structure

Single output file with `property_type` as a column, with `ALL` as a rollup
row for backward compatibility:

```
postcode_district, property_type, num_sales, total_price, total_floor_area, price_per_sqm
SW1A,             ALL,            1420,      ...
SW1A,             D,              84,        ...
SW1A,             F,              892,       ...
SW1A,             S,              183,       ...
SW1A,             T,              261,       ...
```

For issue #61 (time-range slider), the natural extension is a year × district
× type cube:

```
postcode_district, year, property_type, total_price, total_floor_area
```

This enables a slider that also filters by property type — showing how the
flat market and the detached market in a geography move independently over time.

## Minimum sample size

The existing `HAVING COUNT(*) >= 10` guard applies at the all-types level.
Per-type aggregates will be sparser; a separate threshold (e.g. `>= 5`) or
suppression of low-count cells (return NULL) should be applied to avoid
publishing noise as signal.

## Implementation sketch

Aggregation query change only — no join changes needed, `property_type` is
already in `matched.parquet`:

```sql
SELECT
    LEFT(postcode, ...) AS postcode_district,
    property_type,
    COUNT(*) AS num_sales,
    SUM(price) AS total_price,
    SUM(TOTAL_FLOOR_AREA) AS total_floor_area,
    ROUND(SUM(price) / SUM(TOTAL_FLOOR_AREA)) AS price_per_sqm
FROM matched
WHERE date_of_transfer >= '2019-01-01'
GROUP BY postcode_district, property_type
HAVING COUNT(*) >= 5

UNION ALL

SELECT
    LEFT(postcode, ...) AS postcode_district,
    'ALL' AS property_type,
    COUNT(*) AS num_sales,
    SUM(price) AS total_price,
    SUM(TOTAL_FLOOR_AREA) AS total_floor_area,
    ROUND(SUM(price) / SUM(TOTAL_FLOOR_AREA)) AS price_per_sqm
FROM matched
WHERE date_of_transfer >= '2019-01-01'
GROUP BY postcode_district
HAVING COUNT(*) >= 10
```

Low-risk: additive to the existing aggregate, no join layer changes required.

## Dependencies

- Requires Option B (all sales retained) from issue #60 to be implemented
  first, so historical depth is present for sparse property types in sparse
  geographies.
- Issue #61 (time-range slider) naturally extends to a property-type filter
  once this segmentation exists in the output.
- Issue #67 (inflation adjustment) applies equally here for cross-year
  type-level comparisons.
