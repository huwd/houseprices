# Sale-EPC Temporal Matching and PPD Methodology

**Issue**: https://github.com/huwd/houseprices/issues/60
**Researched**: 2026-03-18

---

## What are we measuring and why

The core output — price per square metre by geography — serves two distinct use
cases, each with different validity:

**Valuing an unsold property via comparables** ("what would my house sell for?")
This is the strongest use case. You want to know what comparable properties that
*actually entered the market* went for. Transaction data is the right data for
this. The starter home that sold three times is a rich source of market signal;
the mansion that last sold in 2003 is a poor comparable for most valuations
precisely because its last price signal is old and its type is illiquid.

**Characterising a geography** ("what is this area worth per m²?")
Useful but inherently limited by selection bias: only sold properties appear in
the dataset. The Georgian townhouse that hasn't changed hands since 1987 is
absent. Acknowledge this limitation rather than try to design around it — you
cannot observe prices for unsold properties.

The metric is honestly described as: **price per m² among properties that traded
in this area over a given period.** It is not a measure of the whole housing
stock.

---

## Unit of analysis: all sales vs most-recent-only

### Option A — most recent sale per property only

Deduplicate PPD to the latest transaction per UPRN. One data point per property.

**Data quality**: throws away historical signal. In a sparse postcode district
with 2 sales last year, properties that sold in 2019–2021 are discarded. For the
time-range slider (issue #61), historical buckets become empty or unreliable.

**EPC matching**: simple — one sale date per property, find the preceding EPC.

**Memory**: smaller intermediate data. PPD shrinks from ~29M to ~15M unique UPRNs.

**Bias introduced**: recency bias. Only properties with a sale in the lookback
window appear. For the time slider, a property that sold in 2019 and not since
contributes nothing to a 2022 bucket even though it is a valid comparable.

### Option B — all sales contribute (chosen approach)

Every PPD transaction is retained. Each sale is independently matched to the
temporally closest EPC.

**Data quality**: sparse areas benefit from historical sales filling thin recent
buckets. This is essential for issue #61 (time-range slider) — each year's
bucket reflects actual transactions from that year, not just properties whose
most recent sale happened to fall in that year.

**EPC matching**: per-sale temporal match. The 2015 sale of a house gets the
2014 EPC; the 2022 sale of the same house gets the 2021 EPC. If the property
was extended in 2018, these two EPC floor areas will differ — which is correct.

**Memory**: EPC table cannot be globally deduplicated before the join. ~30M rows
vs ~15M after deduplication. Roughly 2× EPC cache size. DuckDB handles the
per-sale temporal join efficiently via streaming window functions; the final
matched output remains 1:1 with PPD rows (one EPC selected per sale).

**Bias introduced**: turnover-frequency bias. Frequently-traded properties are
over-represented relative to their share of the housing stock. Discussed below.

### Why Option B despite the turnover bias

The turnover-frequency bias is smaller than intuition suggests, and directionally
correct for the valuation use case.

Because the aggregation is `SUM(price) / SUM(floor_area)`, a property appearing
N times contributes N× the price *and* N× the area. If its price-per-sqm is
stable over the window, it does not move the aggregate at all regardless of how
many times it sold. The bias only materialises when frequently-traded property
types have *different* price-per-sqm from infrequently-traded ones — and that
difference is then weighted by transaction volume rather than stock share.

In practice, small frequently-traded properties (starter homes, flats) often
command *higher* price per m² than large rarely-traded properties, because the
land cost is fixed per plot regardless of house size. So Option B pulls the
aggregate toward the liquid end of the market. This is arguably the correct
signal for valuations: the liquid market is where you find comparables.

The deeper structural issue — that illiquid property types (rarely-sold mansions,
long-let HMOs) are under-represented in *all* transaction-based analysis — is
irreducible. Option A and Option B share this limitation equally. Option B simply
makes the composition of what you do measure more transparent.

The metric under Option B is honestly described as: **transaction-frequency-
weighted price per m² among properties that traded.** A downstream user can
control for this by filtering to a specific property type (see property type
segmentation, issue TBD).

---

## The selection bias is fundamental and not fixable here

Every transaction-based analysis shares this property. Non-transaction sources
that could in principle supplement it (VOA rateable values, council tax band
implied prices, mortgage valuations) are either not open data or not at the
resolution needed. The right response is to document the limitation clearly in
the output, not to discard transaction data in pursuit of a representativeness
that cannot be achieved.

---

## Dependency on issue #61 (time-range slider)

Option B is a prerequisite for issue #61. The slider needs per-year, per-district
aggregates:

```sql
SELECT
    LEFT(postcode, ...) AS postcode_district,
    YEAR(date_of_transfer) AS year,
    SUM(price) AS total_price,
    SUM(TOTAL_FLOOR_AREA) AS total_floor_area
FROM matched
GROUP BY 1, 2
```

Under Option A, each property appears once at most — a 2015 sale discarded
because the same house sold in 2022 leaves the 2015 year-bucket without that
data point. Under Option B, every year gets its own pool of transactions.

Without temporal EPC matching (this issue), the historical year-buckets are
also incorrect: the 2015 row uses the 2023 EPC floor area (post-extension), so
`price / floor_area` is wrong even when the sale is present. Temporal EPC
matching is therefore also a prerequisite for issue #61 to be meaningful.

The dependency chain:

```
Option B (all sales retained)
    └── Issue #60 (temporal EPC matching, per-sale)
            └── Issue #61 (time-range slider, per-year aggregates)
                    └── Issue #67 (inflation adjustment for cross-year comparison)
```

Issue #67 is not required for #61 to ship — a 5-year window slider is useful in
nominal prices — but without it a "2008 to 2025" slider conflates nominal changes
with real price changes.

---

## Temporal EPC matching: how to pick the right EPC per sale

The current pipeline deduplicates EPCs before the join:

```python
# prepare_epc(): one EPC per UPRN — the most recently lodged
GROUP BY UPRN
MAX(LODGEMENT_DATETIME), MAX_BY(TOTAL_FLOOR_AREA, LODGEMENT_DATETIME) ...
```

Then `_join_tier1` joins every PPD sale for that UPRN to this single row.
A 2009 sale and a 2024 sale of the same property both use the 2024 EPC.
If the 2024 EPC reflects a rear extension added in 2018, the 2009 floor area
is wrong and `price_per_sqm` for that sale is systematically understated.

### Why EPCs are mostly lodged before a sale

EPCs in England and Wales must by law be **commissioned before a property is
put on the market** (Energy Performance of Buildings Regulations 2012). The
certificate must appear in the listing; the obligation attaches at the marketing
stage, typically 4–16 weeks before exchange. Fines of up to £500 apply for
non-compliance.

Practical consequence: for a resale, the EPC lodgement date should precede the
PPD `date_of_transfer` by days to a few months in most cases. A "prior EPC
exists" fallback is not an edge case — it is the structural norm for post-2012
resales.

### When no prior EPC exists

- **New builds**: EPC lodged at construction completion, which is the first sale.
  Temporally contemporaneous; no prior exists by definition. Use post-sale EPC
  within a tight cutoff (e.g. ≤6 months).
- **Pre-2009 sales**: EPC scheme did not exist until August 2007; sparse until
  ~2010. A 2006 sale cannot have a prior EPC. Use earliest available EPC for
  the property, or flag as low-quality match.
- **Inherited/probate/exempt sales**: less predictable; treat same as above.

### Matching algorithm (per sale)

```
For each (sale_uprn, sale_date):
    1. Most recent EPC with lodgement_date ≤ sale_date AND gap ≤ 10 years
       → contemporaneous match, highest quality
    2. Earliest EPC with lodgement_date > sale_date AND gap ≤ 2 years
       → post-sale fallback (new build; no prior commissioned)
    3. Any EPC for the UPRN (no direction preference, gap ≤ 10 years)
       → last resort; flag with stale_epc = true in output
    4. No match → unmatched (excluded from price_per_sqm aggregation)
```

The 10-year ceiling matches EPC legal validity (certificates expire after 10
years). Using an EPC older than 10 years as a floor area proxy is unreliable
because the certificate was no longer valid for its original purpose at that age.

### DuckDB implementation: window function per-sale

```sql
WITH candidates AS (
    SELECT
        ppd.transaction_unique_identifier,
        ppd.date_of_transfer,
        epc.UPRN,
        epc.LODGEMENT_DATETIME,
        epc.TOTAL_FLOOR_AREA,
        -- Gap in days, negative = EPC before sale (preferred)
        DATEDIFF('day', ppd.date_of_transfer, epc.LODGEMENT_DATETIME)
            AS gap_days,
        CASE
            WHEN epc.LODGEMENT_DATETIME <= ppd.date_of_transfer THEN 0
            ELSE 1
        END AS is_post_sale
    FROM ppd
    JOIN ubdc ON ppd.transaction_unique_identifier = ubdc.transactionid
    JOIN epc_all ON CAST(ubdc.uprn AS BIGINT) = epc_all.UPRN
    WHERE ABS(DATEDIFF('year',
              epc_all.LODGEMENT_DATETIME, ppd.date_of_transfer)) <= 10
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_unique_identifier
            ORDER BY
                is_post_sale,            -- prior EPCs first
                CASE
                    WHEN is_post_sale = 0
                    THEN -EPOCH(LODGEMENT_DATETIME)   -- most recent prior
                    ELSE  EPOCH(LODGEMENT_DATETIME)   -- earliest post
                END
        ) AS rn
    FROM candidates
)
SELECT * FROM ranked WHERE rn = 1
```

The matched output keeps one row per sale (1:1 with PPD) and adds `gap_days`
and `is_post_sale` as diagnostic columns. Downstream analysis can apply quality
filters (e.g. exclude `is_post_sale = 1 AND ABS(gap_days) > 180`).

---

## Pipeline changes required

### 1. `prepare_epc`: do not deduplicate by UPRN

Remove the `GROUP BY UPRN / MAX_BY` step. The slim-column Parquet (step 1 of
the current two-step prepare) is retained; the deduplication step (step 2) is
dropped.

Impact: `cache/epc.parquet` roughly doubles in size (~3GB vs ~1.5GB).

### 2. `_join_tier1`: replace equijoin with window-function temporal join

Replace the current:

```sql
JOIN epc ON CAST(ubdc.uprn AS BIGINT) = CAST(epc.UPRN AS BIGINT)
```

...with the candidates + ranked CTE pattern above.

### 3. `_join_tier2`: deferred

Tier 2 (address normalisation) deduplicates EPCs for a different reason: to
prevent fan-out when the normalised address matches multiple EPC rows. Changing
this requires more care — the deduplicated EPC is used as a de-facto 1:1 join
key. Tier 2 temporal matching is deferred; the gain is smaller (~8% of matches)
and the risk of introducing spurious many:many joins is higher.

### 4. Add diagnostic columns to matched output

`gap_days INT, is_post_sale BOOLEAN` on every row. These allow:
- Flagging stale matches in downstream analysis
- Measuring the actual gap distribution without re-running the pipeline
- Quality filtering at the aggregation step

---

## Recommended sequencing

1. **Diagnostic first (no pipeline changes)**: query the existing
   `matched.parquet` against the full undeduped EPC to measure the actual gap
   distribution. Establishes whether the bias is empirically significant and
   what cutoff values are appropriate.

2. **Implement Option B pipeline changes**: remove EPC global deduplication,
   add window-function temporal join in Tier 1, add diagnostic columns.

3. **Verify**: check that match rates do not decrease and that `gap_days`
   distribution looks as expected from the diagnostic step.

4. **Property type segmentation** (separate issue): once Option B is in place,
   add `property_type` stratification to the aggregation output. This exposes
   the per-type composition of each geography and allows users to control for
   the turnover-frequency bias by filtering to a single property type.

---

## References

- Energy Performance of Buildings (England and Wales) Regulations 2012 —
  EPC mandatory before marketing.
- DLUHC EPC technical notes:
  <https://www.gov.uk/government/publications/energy-performance-of-buildings-certificates-in-england-and-wales-technical-notes/energy-performance-of-buildings-certificates-in-england-and-wales-technical-notes>
- DLUHC: "Energy performance certificates now include the UPRN":
  <https://news.opendatacommunities.org/energy-performance-certificates-now-include-uprn/>
- Owen Boswarva, "Allocating UPRNs to Energy Performance Certificates" (2022):
  <https://www.owenboswarva.com/blog/post-hou3.htm>
- EPC ratings and transaction prices, England (2014):
  <https://www.sciencedirect.com/science/article/abs/pii/S0140988314003296>
