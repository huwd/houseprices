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
| `lmk` | PPD unique transaction identifier |
| `UPRN` | Unique Property Reference Number |
| `USRN` | Unique Street Reference Number |

- **Match rate**: 96% of PPD records successfully linked to a UPRN[^ubdc]
- **Coverage**: January 1995 to January 2022[^ubdc]
- **Licence**: Open Government Licence[^ubdc]
- **Format**: zip / xlsx
- **Last updated**: 10 March 2026[^ubdc]

The linkage was produced using a 142-rule rules-based methodology[^github] —
the same methodology as the companion EPC linkage, but applied to PPD, which
the authors describe as "easier than Domestic EPCs."[^github]

---

## Why this matters for our pipeline

This resolves the key unknown in our join strategy. We no longer need to:
- Wait for HMLR to publish a UPRN-linked PPD variant
- Do address normalisation on the PPD side for 1995–2022 records

The Tier 1 join becomes:

```
PPD lmk → UBDC lookup → UPRN
EPC UPRN (from DLUHC backfill)
→ direct UPRN join
```

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

Note: the dataset was updated 10 March 2026 and may now extend beyond January
2022 — verify on download.

---

## `lmk` field mapping

The lookup uses `lmk` as the PPD identifier. Confirm this maps to the
`transaction_id` field in the PPD CSV before joining. In Land Registry
documentation this field is sometimes called `transaction_unique_identifier`
or `lmk_key` — verify the exact column name on download.

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

[^boswarva]: Owen Boswarva, "Allocating UPRNs to Energy Performance Certificates" (early 2022). <https://www.owenboswarva.com/blog/post-hou3.htm>
