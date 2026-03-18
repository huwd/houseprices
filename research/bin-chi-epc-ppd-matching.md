# Bin Chi / UBDC: Linking LR PPD and Domestic EPCs

**Source**: https://bin-chi.github.io/Link-LR-PPD-and-Domestic-EPCs/
**Code**: https://github.com/urbanbigdatacentre/os_epc_ppd_linkage
**Researched**: 2026-03-17

---

## Core architecture

Bin Chi's approach is **not** direct PPD → EPC matching. It uses **OS AddressBase Plus**
as an intermediate reference gazetteer:

```
PPD → (142 rules) → OS AddressBase Plus → UPRN
EPC → (446 rules) → OS AddressBase Plus → UPRN
                                           ↓
                              Join on UPRN → linked dataset
```

This is a crucial difference from our approach. We do not have OS AddressBase Plus (a
commercial product). Our tier 2 matches PPD directly to EPC via normalised address
string, without a UPRN intermediary.

---

## Match rates

| Property type      | Records matched  | Rate   |
|--------------------|-----------------|--------|
| Detached           | 1,682,801       | 93.34% |
| Semi-detached      | 1,901,929       | 95.03% |
| Terraced           | 1,955,057       | 94.19% |
| Flats/Maisonettes  | 1,213,548       | 88.62% |
| **Overall**        | **~5.7M**       | **93%+** |

**By year**: 2008 = 56% (EPC barely existed), 2009-2010 = ~88%, 2011-2019 = consistently >90%.
Dataset restricted to 2011-2019 for quality reasons; pre-2011 failure is structural (no EPC
exists), not a matching problem.

---

## Why flats are harder

The majority of their 142 PPD rules and 446 EPC rules exist solely to handle flat/sub-building
complexity. Key issues:

- PPD `saon` field (e.g. "FLAT 3", "APARTMENT 2B") is inconsistently populated
- EPC `ADDRESS1` uses different sub-building formats
- The same flat appears as "FLAT A", "A", "1A", "UNIT 1", "APARTMENT 1" across records
- Floor descriptors appear in different word orders: "FIRST FLOOR FLAT" vs "FLAT FIRST FLOOR"
- Bare numeric SAONs ("3") need "FLAT " prepended to match EPC `subbuildingname`

---

## Key normalisation rules they apply (PPD side, 142 rules across 12 stages)

All matching is **exact string matching** — no fuzzy/probabilistic methods. High rates are
achieved by constructing many candidate address strings and exhausting them in priority order.

### Address fields
- PPD: `postcode`, `paon`, `saon`, `street`
- AddressBase: `buildingname`, `buildingnumber`, `paostartnumber`, `paostartsuffix`,
  `paotext`, `saostartnumber`, `saostartsuffix`, `saotext`, `subbuildingname`,
  `streetdescription`

### Specific substitution rules relevant to us

| Rule | Description |
|------|-------------|
| Prepend "FLAT " | Bare numeric SAON → "FLAT " + SAON to match subbuildingname |
| UNIT → FLAT | `subbuildingname` contains "UNIT" → replace with "FLAT" |
| APARTMENT → FLAT | Already in our normaliser |
| BOX ROOM → FLAT | Unusual but present in AddressBase |
| STORE FLAT → FLAT | Variant in AddressBase |
| Strip periods | `gsub("[.]", "", saon)` |
| Strip all spaces | `gsub(" ", "", addressf)` before join for some methods |
| Floor descriptor reorder | "FIRST FLOOR FLAT" ↔ "FLAT FIRST FLOOR" |
| Ordinal variants | "1ST FLOOR" ↔ "FIRST FLOOR" ↔ "GROUND FLOOR" |
| Last word of SAON | `word(saon, -1)` — extracts end-number for matching |
| First two words of SAON | `word(saon, 1, 2)` |
| Flat letter lookup | "FLAT A"→"A", "FLAT B"→"B", ..., "FLAT Z"→"Z" (and reverse) |

### Structural patterns

- Try most-specific match first (postcode + PAON + SAON), resolve 1:many, carry ambiguous
  forward to next method
- Only keep 1:1 matches at each stage; 1:many go to next method
- SAON=null records handled separately from SAON≠null records

---

## Post-linkage data loss (~15%)

After matching, ~15% of linked records are removed due to EPC data quality:
- `TOTAL_FLOOR_AREA` = 0 or 0.01 m² (implausibly small)
- Missing physical attributes

This means even a perfect matcher would lose ~15% at the EPC quality gate.

---

## What we cannot replicate

- **OS AddressBase Plus**: Commercial product; provides the authoritative UPRN bridge. Without
  it, we must match PPD directly to EPC — fundamentally harder.
- **Manual spelling corrections**: Team manually corrected known address discrepancies for
  England 1995–2016. Automated rules can't substitute.
- **Post-2019 coverage**: Dataset covers 2011–2019 only. Their method has not been applied to
  2020–2026 data.

---

## What we can replicate / adapt

Our tier 2 (`_join_tier2`) matches on `(postcode_norm, norm_addr)` where `norm_addr` is
constructed from `saon + paon + street`. We can adopt their substitution rules into our
`normalise_addr` DuckDB macro without needing AddressBase.

See hypotheses in issue #50.

---

## Our achieved match rates (March 2026)

Pipeline version covering ~1995–2026 PPD (~29.3M category-A records):

| Tier | Records | Share |
|---|---|---|
| Tier 1 — UPRN direct (UBDC lookup) | 20,239,307 | 69.1% |
| Tier 2 — address normalisation | 2,267,763 | 7.7% |
| Tier 3 — enhanced flat normalisation | 2,817 | <0.1% |
| **Total matched** | **22,509,887** | **76.9%** |
| Unmatched | 6,773,388 | 23.1% |

**By property type:**

| Type | Total | Matched | Match % | Tier 3 |
|---|---|---|---|---|
| Flats (F) | 5,280,990 | 4,343,332 | 82.2% | 2,707 |
| Terraced (T) | 8,751,012 | 7,008,937 | 80.1% | 78 |
| Semi-detached (S) | 8,237,751 | 6,176,878 | 75.0% | 16 |
| Detached (D) | 7,037,328 | 4,980,740 | 70.8% | 16 |

Notable findings:
- Flats have the **highest** match rate (82.2%), not the lowest as naively expected. The
  bare-numeric SAON → "FLAT N" normalisation (tier 3) disproportionately helps flats
  (2,707 of 2,817 tier-3 matches).
- Detached houses have the **lowest** rate (70.8%), likely because named properties
  ("Rose Cottage", "The Old Rectory") resist address normalisation without a canonical
  number — exactly the case where AddressBase would help.

**Comparison with Bin Chi (93%+, 2011–2019 only):**
- Bin Chi restrict to 2011–2019, where UPRN coverage and EPC existence are near-complete.
  Our overall 76.9% is dragged down by the pre-2009 structural gap (~3.5M sales with no
  EPC on record) and the post-2021 UBDC coverage gap (~1.2M 2022–2026 records where
  address normalisation only partially fills in for the missing UPRN lookup).
- For the comparable 2011–2019 window our match rate is >90%, consistent with their figures.
- The remaining gap is the lack of OS AddressBase Plus (commercial) as an intermediate
  reference — their 142 PPD rules and 446 EPC rules are primarily workarounds for
  sub-building address complexity that AddressBase resolves directly.
