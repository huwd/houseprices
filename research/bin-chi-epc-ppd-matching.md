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
