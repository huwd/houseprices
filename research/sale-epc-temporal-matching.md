# Sale-EPC Temporal Matching

**Issue**: https://github.com/huwd/houseprices/issues/60
**Researched**: 2026-03-18

---

## The problem in concrete terms

The current pipeline deduplicates EPCs before the join:

```python
# prepare_epc(): keeps one EPC per UPRN — the most recently lodged
MAX(LODGEMENT_DATETIME) AS LODGEMENT_DATETIME,
MAX_BY(TOTAL_FLOOR_AREA, LODGEMENT_DATETIME) AS TOTAL_FLOOR_AREA
```

Then `_join_tier1` joins every PPD sale for that UPRN to this single
deduplicated row. The result: a 2009 sale and a 2024 sale of the same property
are both matched to the 2024 EPC.

If the 2024 EPC reflects a rear extension added in 2018, the 2009 sale's
`price_per_sqm` is computed against a floor area that didn't exist at the time
— it is systematically understated. Conversely, if a loft conversion was done in
2011 and the property burned down (hypothetically) in 2023, the 2009 sale gets
the post-conversion area anyway. The direction of bias depends on what changed,
but the bias is real and applies asymmetrically to older sales.

### Why EPCs are mostly lodged *before* a sale

EPCs in England and Wales must by law be **commissioned before a property is
put on the market** (Energy Performance of Buildings Regulations 2012). This is
enforced at the listing stage, not completion — the EPC must be commissioned when
marketing begins, typically 4–16 weeks before exchange. Fines of up to £500
apply for non-compliance.

**Practical consequence**: for a property that is sold, the EPC lodgement date
should precede the PPD `date_of_transfer` by a few days to a few months in
most straightforward cases. This means the "prior EPC" will usually exist and
will often be very recent relative to the sale date.

### When a prior EPC does *not* exist

- **New builds** lodge their first EPC at construction, which appears in PPD as
  the first sale. For new builds the EPC is contemporaneous (same day to a few
  weeks before completion), and temporal matching is unproblematic.
- **Pre-2009 sales**: EPC was not introduced until August 2007 and coverage was
  sparse until ~2010. A 2006 sale cannot have a prior EPC by definition.
- **Inherited/probate sales**: the property may not have been marketed, and the
  EPC obligation may have been waived or ignored.
- **Exempt properties**: listed buildings and some conservation area properties
  are EPC-exempt; they should already fall out at the matching stage.

---

## What changes in the join architecture

Currently: one EPC row per UPRN → join all sales to it.

With temporal matching: no pre-deduplication by UPRN. Instead, for each
`(sale_uprn, sale_date)` pair, select the EPC row from that UPRN's history
that is closest to the sale date subject to the preference rule.

The join becomes:

```sql
-- For each sale, find the best available EPC:
--   1. Most recent EPC with LODGEMENT_DATETIME <= date_of_transfer (prior EPC)
--   2. If none: earliest EPC with LODGEMENT_DATETIME > date_of_transfer (post EPC)
--   3. Exclude matches where |gap| > N years

WITH ranked AS (
    SELECT
        ppd.transaction_unique_identifier,
        epc.LODGEMENT_DATETIME,
        epc.TOTAL_FLOOR_AREA,
        -- negative gap means EPC was lodged BEFORE the sale (preferred)
        DATEDIFF('day', epc.LODGEMENT_DATETIME, ppd.date_of_transfer) AS gap_days,
        CASE
            WHEN epc.LODGEMENT_DATETIME <= ppd.date_of_transfer THEN 0
            ELSE 1
        END AS is_post_sale,
        ROW_NUMBER() OVER (
            PARTITION BY ppd.transaction_unique_identifier
            ORDER BY
                -- prefer prior EPCs over post-sale
                CASE WHEN epc.LODGEMENT_DATETIME <= ppd.date_of_transfer THEN 0 ELSE 1 END,
                -- within each group: prior → most recent; post → earliest
                CASE
                    WHEN epc.LODGEMENT_DATETIME <= ppd.date_of_transfer
                    THEN -EPOCH(epc.LODGEMENT_DATETIME)
                    ELSE  EPOCH(epc.LODGEMENT_DATETIME)
                END
        ) AS rn
    FROM ppd
    JOIN epc ON epc.UPRN = ppd_uprn
    WHERE ABS(DATEDIFF('year', epc.LODGEMENT_DATETIME, ppd.date_of_transfer)) <= 10
)
SELECT * FROM ranked WHERE rn = 1
```

This is a significant pipeline change:

1. `prepare_epc` must retain **all** EPC rows per UPRN (not deduplicate), or at
   minimum keep one per `(UPRN, lodgement_year)` to bound memory.
2. The join becomes a per-sale window function rather than a simple equijoin.
3. Tier 2 (address normalisation) currently deduplicates EPCs for the same
   reason — that too would need rethinking.
4. The matched Parquet grows: currently one row per sale, still one row per sale
   but selected from a wider candidate set.

---

## Expected empirical distribution of gaps

Based on what we know about the dataset:

**EPC obligation**: EPC must precede marketing. Typical marketing-to-completion
period is 8–16 weeks. So for a straightforward resale, the gap between EPC
lodgement and PPD transfer date should be **0–6 months** for the vast majority.

**EPC validity**: 10 years. A property may have an EPC from a previous sale or
let that is still valid; the seller may choose not to commission a new one.
Under those circumstances the gap can be up to 10 years (prior EPC still valid
from last transaction).

**Postulated gap distribution** (to be verified empirically):

| Gap bucket | Expected share | Rationale |
|---|---|---|
| EPC 0–6 months before sale | ~50–65% | Freshly commissioned for sale |
| EPC 6 months–5 years before sale | ~20–30% | Valid EPC from previous let or sale |
| EPC 5–10 years before sale | ~5–15% | Old but still-valid certificate |
| No prior EPC (post-sale only) | ~5–15% | New builds; pre-EPC era sales; gaps in data |
| Gap > 10 years either direction | <5% | Structural mismatch; exclude |

These are estimates only. The empirical distribution can be computed directly
from the full EPC dataset without running the full pipeline:

```sql
-- Requires: epc_all (undeduped), matched.parquet (existing tier-1 matches)
SELECT
    FLOOR(DATEDIFF('month', e.LODGEMENT_DATETIME, m.date_of_transfer) / 6) * 6 AS gap_bucket_months,
    COUNT(*) AS n,
    COUNT_IF(e.LODGEMENT_DATETIME <= m.date_of_transfer) AS n_prior,
    COUNT_IF(e.LODGEMENT_DATETIME >  m.date_of_transfer) AS n_post
FROM matched m
JOIN epc_all e ON e.UPRN = m.uprn
GROUP BY 1
ORDER BY 1
```

---

## Impact on match rates

Temporal matching should **not reduce match rates** in the UPRN-linked tier.
The pool of candidate EPCs per UPRN is larger (undeduped), so if anything a
match exists where the pre-deduped approach might have had only a post-sale EPC.

The only way match rates drop is if we apply a strict prior-EPC-only cutoff
with no post-sale fallback for sales where no prior EPC exists. The issue
sensibly proposes using the earliest post-sale EPC as fallback, which preserves
match rates.

For Tier 2 (address normalisation), deduplication currently serves a different
purpose — it ensures a 1:1 join rather than a fan-out. If we retain all EPCs
per address, Tier 2 must either:
- Apply the same window-function logic (matching on `(postcode, norm_addr, date)`), or
- Retain deduplication for Tier 2 and only apply temporal matching to Tier 1
  (UPRN-matched) rows where the EPC history is unambiguous.

The simpler approach for an initial implementation is **Tier 1 temporal matching
only**, leaving Tier 2 with its existing deduplicated join. This covers ~69% of
all matches (the UPRN tier) with no risk of introducing fan-out joins in the
address-normalisation path.

---

## Impact on price per square metre figures

The bias from the current approach disproportionately affects:

- **Older sales** (pre-2015) where the property may have had significant
  alterations since the sale
- **Extended/converted properties** where the most recent EPC reflects a
  materially larger or smaller footprint than at time of sale
- **Postcode districts with older housing stock** where extensions are common

The effect on *national medians* is likely small, because:
1. The most recent EPC and the sale-contemporaneous EPC often agree for most
   sales (especially post-2015 where the market is liquid and EPCs are refreshed
   frequently)
2. For pre-2009 sales there is no prior EPC regardless of approach

For **longitudinal analysis** (tracking the same district's price per m² over
time) the distortion is more significant: a 2009 figure computed using 2024 EPC
floor areas is systematically different from one computed using 2009 EPC floor
areas.

---

## Implementation options

### Option A: Temporal matching for Tier 1 only (recommended first step)

Scope: ~69% of all matched records (the UPRN tier).

Changes:
1. `prepare_epc`: add a mode that retains all rows per UPRN rather than
   deduplicating. Probably a new function `prepare_epc_full()` or a parameter.
2. `_join_tier1`: replace the equijoin against a deduped EPC table with a
   window-function join against the full EPC table.
3. Keep `_join_tier2` exactly as is (still deduped, not temporally matched).
4. Add `gap_days` and `is_post_sale` columns to the matched output for
   diagnostics.

### Option B: Temporal matching for both Tier 1 and Tier 2

Scope: all matched records.

Additional changes beyond Option A:
- Tier 2 must become a per-sale window join on `(postcode_norm, norm_addr)`;
  a normalised address can match multiple EPC records (e.g. after a property
  is converted), so a fan-out is possible.
- More complex, and Tier 2 addresses are inherently noisier, so the temporal
  signal is less reliable than for UPRN-matched records.
- Deferred to a later iteration.

### Option C: Research-only — measure the gap distribution first

Before changing anything in the pipeline, run a diagnostic query against the
existing matched.parquet and the full undeduped EPC to measure the actual
gap distribution. This costs nothing in terms of pipeline changes and tells us
whether the bias is empirically significant enough to warrant Option A/B.

---

## Recommendation

1. **Start with Option C** — run the diagnostic gap distribution query on the
   existing matched output. This takes a few minutes and can be done in the
   notebook without touching the pipeline.

2. If the distribution shows a meaningful fraction of sales matched to EPCs
   lodged >2 years *after* the sale date, proceed with **Option A**.

3. A cutoff of **10 years** (matching the EPC validity period) seems like the
   natural maximum gap — beyond 10 years the EPC was no longer valid even for
   the purposes it was lodged for.

4. **Do not remove the post-sale fallback** — for new builds and pre-2009 sales
   there will genuinely be no prior EPC. A hard prior-only requirement would
   drop those sales entirely, which is worse than a slightly stale EPC.

5. **Output the gap diagnostics** (`gap_days`, `is_post_sale`) as columns in
   matched.parquet so downstream analysis can filter by temporal quality.

---

## Dependencies and sequencing

- This work requires reliable UPRN coverage at scale; the UPRN join
  improvements (issue #50) should be stable before tackling temporal matching.
- Temporal matching only applies to Tier 1 (UPRN-matched); if UPRN coverage
  improves further, more sales benefit from temporal matching automatically.
- The diagnostic query (Option C) has no dependencies and can be run now
  against the existing matched output.

---

## References

- Energy Performance of Buildings (England and Wales) Regulations 2012 —
  EPC mandatory before marketing
- DLUHC EPC technical notes:
  <https://www.gov.uk/government/publications/energy-performance-of-buildings-certificates-in-england-and-wales-technical-notes/energy-performance-of-buildings-certificates-in-england-and-wales-technical-notes>
- DLUHC: "Energy performance certificates now include the UPRN":
  <https://news.opendatacommunities.org/energy-performance-certificates-now-include-uprn/>
- Owen Boswarva, "Allocating UPRNs to Energy Performance Certificates" (2022):
  <https://www.owenboswarva.com/blog/post-hou3.htm>
- Sirmans et al. / DECC repeat-sales study on EPC ratings and price per m²
  (2014): <https://www.sciencedirect.com/science/article/abs/pii/S0140988314003296>
