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

## References

[^boswarva]: Owen Boswarva, "Allocating UPRNs to Energy Performance Certificates" (early 2022). <https://www.owenboswarva.com/blog/post-hou3.htm>

[^kamma]: Kamma Climate, "EPC Open Data vs Kamma's Enhanced EPC Data" (1 July 2024). <https://www.kammaclimate.com/news/2024/07/epc-open-data-vs-kammas-enhanced-epc-data/>

[^winn]: Rosie Winn, "Sharing my recent findings on the latest update to bulk EPC data" (13 January 2022). <https://www.linkedin.com/pulse/sharing-my-recent-findings-latest-update-bulk-epc-data-rosie-winn>

[^dluhc-blog]: DLUHC / Open Data Communities, "Energy performance certificates now include the Unique Property Reference Number (UPRN)". <https://news.opendatacommunities.org/energy-performance-certificates-now-include-uprn/>

[^addressbase]: Ordnance Survey, "OS Open UPRN". <https://www.ordnancesurvey.co.uk/products/os-open-uprn>
