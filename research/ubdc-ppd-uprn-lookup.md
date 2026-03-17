# UBDC Price Paid Data → UPRN Lookup

A pre-built OGL lookup table linking PPD transaction IDs to UPRNs, produced by
the Urban Big Data Centre (University of Glasgow) as part of an OS-funded project.

**Citation**: Urban Big Data Centre (2023). Price paid data to UPRN lookup [Data
set]. University of Glasgow. https://doi.org/10.20394/agu7hprj

---

## What it is

A lookup table with three fields:

| Field | Description |
|---|---|
| `transactionid` | PPD unique transaction identifier — matches `transaction_unique_identifier` in HMLR PPD CSV |
| `uprn` | Unique Property Reference Number |
| `method` | Which of the 142 matching rules produced the link (e.g. `"method1"`) |

- **Match rate**: 96% of PPD records successfully linked to a UPRN[^ubdc]
- **Coverage**: January 1995 to January 2022[^ubdc]
- **Licence**: Open Government Licence[^ubdc]
- **Format**: zip / CSV (confirm on download — earlier versions were xlsx)
- **Last updated**: 10 March 2026[^ubdc]

The linkage was produced using a 142-rule rules-based methodology[^github] —
the same methodology as the companion EPC linkage, but applied to PPD, which
the authors describe as "easier than Domestic EPCs."[^github]

---

## Field name correction (March 2026)

**Earlier versions of this document (and PLAN.md) used `lmk` as the PPD
identifier field name. This was incorrect.**

The correct field name is `transactionid`, confirmed by inspecting the UBDC
linkage source code (`lrppd_os_final.R`)[^rscript]. Key evidence:

```r
# From lrppd_os_final.R — the function that filters already-matched records:
matchleft <- function(x, y) {
  next0 <- x[!(x$transactionid %in% y$transactionid), ]
  return(next0)
}

# The final output column list written to file:
needlistf <- c("transactionid", "uprn", "method")
```

The variable `tran` (loaded from HMLR PPD) is also keyed on `transactionid`
throughout the script. This field corresponds to HMLR's
`transaction_unique_identifier` in the PPD CSV — a curly-braced UUID string,
e.g. `{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}`.

The `lmk` and `lmk_key` names appear in the companion EPC linkage and in some
secondary documentation, which is likely the source of the earlier confusion.
The PPD linkage consistently uses `transactionid`.

**Action on download**: confirm the column is named `transactionid` in the
published CSV. If the March 2026 release has renamed it, update the join code
in `pipeline.py` accordingly.

---

## Why this matters for our pipeline

This resolves the key unknown in our join strategy. We no longer need to:
- Wait for HMLR to publish a UPRN-linked PPD variant
- Do address normalisation on the PPD side for 1995–2022 records

The Tier 1 join becomes:

```
PPD transaction_unique_identifier
  → UBDC lookup (transactionid) → UPRN
  → EPC UPRN (from DLUHC backfill)
  → direct UPRN join
```

The `method` column in the lookup records which matching rule produced each
link. This could be used to flag lower-confidence matches (high rule numbers
tend to be fuzzier), but for our purposes we treat all UBDC matches as Tier 1.

### Estimated coverage

| Step | Coverage |
|---|---|
| PPD records with a UPRN via UBDC lookup (1995–2022) | ~96% |
| EPC records with a UPRN via DLUHC backfill | ~92% |
| Both sides with UPRN (approximate Tier 1 reach) | ~88% |
| Remaining — address normalisation fallback | ~12% |

The ~12% fallback figure is approximate. Some properties will have a UPRN on
one side only, and whether the normalisation fallback recovers them depends on
address data quality.

---

## Coverage gap: 2022–2026 PPD records

The UBDC lookup covers PPD to January 2022. Records from 2022–2026 are not
included. For those:

- The EPC UPRN should be available for most properties (post-2021 EPCs have
  good UPRN coverage[^boswarva])
- But we cannot resolve the PPD transaction to a UPRN via the lookup table
- Fallback: address normalisation between PPD address fields and EPC address

**Confirmed (March 2026):** The dataset was updated 10 March 2026 but coverage
has **not** extended beyond January 2022. Inspection of the three published
dataset records on the UBDC data portal confirms no post-2022 transactions are
included:

- https://data.ubdc.ac.uk/datasets/5cfce5ed-59d4-4690-8e83-1b6dc86f55a2
- https://data.ubdc.ac.uk/datasets/fc1179f2-e13a-47b2-b92e-189dd62b5460
- https://data.ubdc.ac.uk/datasets/a999fd05-e7fe-4243-ab9a-95ce98132956

This is consistent with our first full pipeline run, which showed essentially
zero tier 1 matches from 2022 onwards (see match rates by year below). The
coverage cliff is a **structural limitation** of the current release, not a
stale download issue. The 2022–2026 gap remains dependent on address
normalisation (Tier 2) until UBDC publish an extended release.

**Match rates by year (pipeline run @ bd0e663):**

| Year | Tier 1 | Tier 2 | Overall |
|---|---|---|---|
| 2019 | 95.4% | 1.1% | 96.4% |
| 2020 | 89.9% | 4.0% | 94.0% |
| 2021 | 64.4% | 17.0% | 81.4% |
| 2022 | 0.1% | 59.1% | 59.2% |
| 2023 | 0.0% | 60.1% | 60.1% |
| 2024 | 0.0% | 60.2% | 60.2% |
| 2025 | 0.0% | 59.6% | 59.6% |

The 2021 partial coverage (64.4%) likely reflects the UBDC data lagging the
PPD cut-off by several months — transactions registered late in 2021 may not
have been included in the linkage run.

---

## Licence note

OGL — compatible with our use. No commercial restriction (unlike the GitHub
repo[^github] which is CC-BY-NC). Cite as:

> Urban Big Data Centre (2023). Price paid data to UPRN lookup [Data set].
> University of Glasgow. https://doi.org/10.20394/agu7hprj

---

## References

[^ubdc]: Urban Big Data Centre, "Price paid data to UPRN lookup" (updated 10 March 2026). <https://data.ubdc.ac.uk/dataset/a999fd05-e7fe-4243-ab9a-95ce98132956>

[^github]: Urban Big Data Centre, `os_epc_ppd_linkage` GitHub repository. <https://github.com/urbanbigdatacentre/os_epc_ppd_linkage>

[^rscript]: Bin Chi (UBDC), `lrppd_os_final.R` — PPD to UPRN linkage source code. <https://github.com/urbanbigdatacentre/os_epc_ppd_linkage/blob/main/PPD/lrppd_os_final.R>

[^boswarva]: Owen Boswarva, "Allocating UPRNs to Energy Performance Certificates" (early 2022). <https://www.owenboswarva.com/blog/post-hou3.htm>
