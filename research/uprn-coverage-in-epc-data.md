# UPRN Coverage in EPC Data

Research into how well the open EPC dataset is populated with UPRNs, and what
this means for our UPRN-first join strategy.

---

## Overall picture: 92% across the full dataset back to 2008

Owen Boswarva analysed the Q4 2021 release of the domestic EPC dataset[^boswarva]:

- **22,243,396** total domestic EPC records (2008–end of 2021)
- **20,551,337** records have a UPRN — **92% overall**
- **1,716,164** (8%) UPRNs were added by energy assessors at submission time
- **18,835,173** (85%) UPRNs were added retrospectively by DLUHC's address-matching algorithm

**This is the most important finding**: UPRN coverage is NOT limited to post-2021
records. DLUHC ran a retrospective address-matching algorithm (rules-based + ML,
using OS AddressBase[^addressbase]) going all the way back to 2008. The 92% figure
applies across the full dataset history.

---

## Correcting the Kamma framing

Kamma (2024) stated: "Until 2021 EPCs were not issued with a Unique Property
Reference Number (UPRN)."[^kamma] This is technically true for assessor-submitted
UPRNs, but misleading in practice. DLUHC subsequently backfilled UPRNs for the
historic record via address matching[^dluhc-blog], so the current dataset has high
UPRN coverage going back to 2008.

The Kamma statement likely refers to the fact that assessors didn't routinely
include UPRNs in their submissions before 2021, not that the current published
dataset lacks them.

---

## Quarter-by-quarter variation: 82–96%

UPRN match rates are not uniform across lodgement quarters[^boswarva]:

- **2009–2019**: consistently 90–96%, peaking around 2013–2014
- **2020–2021**: declining, dropping to ~82–86% in recent quarters (2021 Q3/Q4)

The drop in 2020–2021 is primarily driven by **new builds**: there is a lag
between when a new UPRN is created for a new dwelling and when it appears in OS
AddressBase[^boswarva]. New-build EPCs lodged before the UPRN enters AddressBase
cannot be matched.

The unmatched-by-type chart in Boswarva[^boswarva] shows "new dwellings" account
for the majority of the spike in unmatched records in 2020–2021, while "other EPCs"
remain at a low base rate of ~4–6%.

---

## Backfilling is slow

Only 45 of the 1,635,380 unmatched records from Q3 2021 were matched in the Q4
2021 release[^boswarva]. DLUHC has committed to continuing address matching[^dluhc-blog]
but progress on backfilling existing unmatched records is slow. This means the
~8% without UPRNs is likely to remain substantial in the current dataset.

---

## Unique properties: 15.7 million

The Q4 2021 dataset contains **15,678,307 unique UPRNs**, of which **25% appear
on more than one EPC record**[^boswarva] (i.e. the same property has had multiple
EPCs over time). This is useful for tracking changes to a property's attributes.

Deduplication must therefore use UPRN as the partition key (take most recent
`LODGEMENT_DATETIME` per UPRN), not just `(POSTCODE, ADDRESS1, ADDRESS2)`.

---

## Rosie Winn (2022): local authority samples 87–96%

Winn independently found 87–96% UPRN coverage analysing several local authority
bulk EPC extracts[^winn], consistent with Boswarva's overall 92% figure. She notes:

- Missing UPRNs are predominantly new builds (not yet in AddressBase) and
  complex addresses (flats, annexes, house names)[^winn]
- Removing duplicate UPRNs drops apparent *stock coverage* estimates from
  65–75% to 55–70% of local authority housing stock[^winn]

Areas with high concentrations of flats (e.g. urban centres, London) will see
lower UPRN population rates than areas with predominantly houses.

---

## DLUHC matching method

> "The address-matching algorithm uses a combination of rules-based and
> machine-learning approaches using data from AddressBase."[^dluhc-blog]

DLUHC only assigns UPRNs that pass a confidence score threshold[^dluhc-blog].
UPRNs that fail the threshold are left blank rather than assigned incorrectly.
This means missing UPRNs are genuinely ambiguous cases — they are not a random
omission.

---

## Summary: implications for join strategy

The picture is substantially better than initially feared:

| Coverage | Rate | Source |
|---|---|---|
| Overall UPRN coverage (2008–2021 dataset) | ~92% | [^boswarva] |
| Coverage in stable mid-period (2010–2019) | 90–96% | [^boswarva] |
| Coverage in recent quarters (new-build lag) | 82–86% | [^boswarva] |
| Local authority samples | 87–96% | [^winn] |
| Records without UPRN (unresolvable or slow backfill) | ~8% | [^boswarva] |

**Tier 1 (UPRN direct join) is viable across the full dataset history**, not
just post-2021 records. For the ~8% of EPC records without a UPRN, Tier 2
(address normalisation) remains the fallback.

The address normalisation fallback is still needed but is a genuine fallback for
an 8% edge case, not the primary path.

### Remaining unknown

How has UPRN coverage evolved in the 2022–2026 portion of the dataset (post
Boswarva's Q4 2021 snapshot)? Expect continued improvement for 2019–2020 records
as AddressBase catches up on new builds from that period, but 2021–2024 new builds
may still have a lag. Measure empirically once data is downloaded.

---

## Empirical results from first pipeline run (March 2026)

Pipeline version: bd0e663. EPC bulk export downloaded March 2026.

### UPRN coverage by lodgement year

| Year | Total certs | With UPRN | UPRN % |
|---|---|---|---|
| 2008 | 158,170 | 140,249 | 88.7% |
| 2009 | 623,327 | 575,572 | 92.3% |
| 2010 | 606,942 | 577,581 | 95.2% |
| 2011 | 613,758 | 587,782 | 95.8% |
| 2012 | 675,706 | 652,355 | 96.5% |
| 2013 | 972,364 | 949,372 | 97.6% |
| 2014 | 1,204,705 | 1,181,904 | 98.1% |
| 2015 | 1,152,743 | 1,130,306 | 98.1% |
| 2016 | 1,165,431 | 1,144,890 | 98.2% |
| 2017 | 1,002,083 | 983,698 | 98.2% |
| 2018 | 1,191,235 | 1,166,306 | 97.9% |
| 2019 | 1,385,540 | 1,357,827 | 98.0% |
| 2020 | 1,335,454 | 1,310,659 | 98.1% |
| 2021 | 1,472,818 | 1,446,173 | 98.2% |
| 2022 | 1,589,025 | 1,553,629 | 97.8% |
| 2023 | 1,539,677 | 1,506,309 | 97.8% |
| 2024 | 1,574,581 | 1,518,330 | 96.4% |
| 2025 | 1,714,804 | 1,533,399 | 89.4% |
| 2026 | 121,768 | 107,171 | 88.0% |

Key observations:

- **2010–2024**: 95–98% UPRN coverage — substantially better than Boswarva's
  Q4 2021 figure of 92%. The DLUHC backfill has continued to improve coverage
  since 2022.
- **2025–2026**: drops to 88–89%, consistent with the new-build UPRN lag
  (AddressBase doesn't yet contain UPRNs for recently constructed properties).
- **2008**: 88.7% — some early records not yet backfilled by DLUHC.

The earlier research prediction of 82–86% for 2020–2021 records is **not borne
out** by the current dataset: 2020–2021 show 98%+ coverage. Either the DLUHC
backfill has substantially closed the new-build gap since Boswarva's 2022
analysis, or the bulk export has been updated with improved matching.

### The 2022–2026 unmatched gap is a matching problem, not a data gap

Cross-referencing unmatched PPD records (2022–2026) against EPC postcode
coverage:

| Year | Unmatched PPD | Postcode has EPC data | % |
|---|---|---|---|
| 2022 | 371,896 | 370,149 | 99.5% |
| 2023 | 282,510 | 281,350 | 99.6% |
| 2024 | 302,658 | 301,274 | 99.5% |
| 2025 | 265,441 | 264,750 | 99.7% |
| 2026 | 5,624 | 5,606 | 99.7% |

**99.5–99.7% of unmatched 2022–2026 PPD records are in postcodes where EPC
data exists.** Combined with 97–98% EPC UPRN coverage for 2022–2024, the
EPC records are present and have UPRNs — they are simply not being linked
because the UBDC lookup does not cover post-2022 PPD transactions and address
normalisation (tier 2) achieves only ~60% for those years.

This strongly motivates improving the tier 2 address normalisation (see
GitHub issue #50). Nearly all of the ~1.2M unmatched 2022–2026 records are
theoretically recoverable.

---

## Anatomy of the 23.1% unmatched (March 2026 pipeline run)

After running all three tiers (overall 76.9% match), the unmatched 23.1%
decomposes into two structural groups:

### Pre-2009: no EPC exists (~3.5M records, largely unrecoverable)

EPC was introduced in England and Wales from August 2007, but coverage was
sparse until 2009–2010. For a pre-EPC sale to match, the same property must
have lodged a certificate at some later point (e.g. a 2001 sale of a house
re-sold in 2016 and assessed then). Properties that have never been assessed
— common for older housing stock that hasn't changed hands recently — have no
EPC record and cannot be matched.

| Period | Typical unmatched/year | Cause |
|---|---|---|
| 1995–2007 | 280–410k | EPC scheme didn't exist; no certificate for most properties |
| 2008 | 193k | First year of EPC; sparse early coverage |
| 2009–2019 | 30–100k | Residual — named properties and tier-2 failures |

### Post-2021: UBDC coverage gap (~1.2M records, theoretically recoverable)

The UBDC PPD→UPRN lookup covers transactions up to approximately early 2022.
From 2023 onwards tier 1 contributes nothing. Tier 2 fills in heavily
(536k of 538k matched for 2022 come via tier 2) but still leaves 300–370k
per year unmatched. As the postcode cross-reference above shows, 99.5%+ of
these are in postcodes with EPC data — the gap is purely address normalisation
quality, not missing EPC records.

### By property type

| Type | Match % | Notes |
|---|---|---|
| Flats (F) | 82.2% | Highest — tier 3 SAON normalisation disproportionately helps |
| Terraced (T) | 80.1% | Straightforward numbered street addresses match well |
| Semi-detached (S) | 75.0% | — |
| Detached (D) | 70.8% | Lowest — named properties resist normalisation |

Detached houses being the hardest type is counterintuitive but explained by
named properties ("Rose Cottage", "The Old Rectory") that have no house number.
Without OS AddressBase as a canonical reference, named property addresses cannot
be reliably matched by string normalisation alone.

---

## Second pipeline run: v0.2.0 (March 2026, new EPC vintage)

### Overall match rate dropped from 76.9% → 56.6%

| | v0.1.0 | v0.2.0 | Change |
|---|---|---|---|
| Total PPD sales | 22,503,694 | 29,283,275 | +6,779,581 (+30%) |
| Tier 1 (UPRN) | — | 9,255,768 (31.6%) | — |
| Tier 2 (address) | — | 7,321,554 (25.0%) | — |
| Matched total | ~17.3M (76.9%) | 16,577,322 (56.6%) | −0.7M abs; −20pp rate |
| Unmatched | ~5.2M (23.1%) | 12,705,953 (43.4%) | +7.5M |

The rate drop is almost entirely structural: the PPD gained ~6.8M rows (all
post-2022 transactions in the updated Land Registry vintage), and the UBDC
lookup covers only up to January 2022. Every one of those 6.8M new rows
enters at best tier 2 (address normalisation) and at worst unmatched.
The absolute number of matched records barely changed (~17.3M → 16.6M),
so the denominator grew faster than the numerator.

This is not a regression in methodology — it is the expected consequence of
the data range expanding while the UBDC lookup remains fixed at 2022.
Improving tier-2 address normalisation (issue #50) remains the highest-
leverage intervention for the post-2022 gap.

### Headline comparison (real Jan-2026 £/m²)

| | v0.1.0 (nominal) | [Unreleased] (CPI est.) | v0.2.0 (new vintage) |
|---|---|---|---|
| Most expensive | W1S £24,184 | W1S £32,660 | W1S £35,462 |
| Least expensive | TS2 £504 | TS2 ~£740 est. | TS2 £733 |
| Median | £1,986 | ~£2,917 est. | £3,058 |
| Districts | 2,279 | 2,279 | 2,277 |

W1S increased by 8.6% vs the CPI-only estimate from the same data.
This reflects the new EPC vintage and updated PPD rather than any
methodology change — more recent transactions in central London at
2024–2025 prices shift the aggregate upward. The median uplift (+4.8%)
is more modest and consistent with general price trends.

TS2 moved marginally (£740 est. → £733), within rounding noise; it
remains the least expensive district.

### Rankings are stable

Top 5: W1S → WC2A → WC2R → W1B → W1K (unchanged order)
Bottom 5: TS2 → TS1 → BD3 → CF43 → DN31 (unchanged order)

Stability across both the CPI methodology change and the new data vintage
is a positive signal for the robustness of the methodology.

### New EPC data source

The v0.2.0 run used the new MHCLG GOV.UK One Login API
(`get-energy-performance-data.communities.gov.uk`) which delivers a
single monolithic 5.7 GB CSV (~23M rows) assembled from all local
authority feeds. The old API (`epc.opendatacommunities.org`) delivered
per-local-authority ZIP files that were individually smaller and more
consistently formatted. Four DuckDB CSV parsing issues were encountered
in the new file (documented in `research/epc-csv-data-quality.md`),
all attributable to the heterogeneous assembly of local authority
submissions with different formatting conventions.

---

## References

[^boswarva]: Owen Boswarva, "Allocating UPRNs to Energy Performance Certificates" (early 2022). <https://www.owenboswarva.com/blog/post-hou3.htm>

[^kamma]: Kamma Climate, "EPC Open Data vs Kamma's Enhanced EPC Data" (1 July 2024). <https://www.kammaclimate.com/news/2024/07/epc-open-data-vs-kammas-enhanced-epc-data/>

[^winn]: Rosie Winn, "Sharing my recent findings on the latest update to bulk EPC data" (13 January 2022). <https://www.linkedin.com/pulse/sharing-my-recent-findings-latest-update-bulk-epc-data-rosie-winn>

[^dluhc-blog]: DLUHC / Open Data Communities, "Energy performance certificates now include the Unique Property Reference Number (UPRN)". <https://news.opendatacommunities.org/energy-performance-certificates-now-include-uprn/>

[^addressbase]: Ordnance Survey, "OS Open UPRN". <https://www.ordnancesurvey.co.uk/products/os-open-uprn>
