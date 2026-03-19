# EPC Bulk CSV — Data Quality Issues

The domestic EPC bulk download from
`get-energy-performance-data.communities.gov.uk` is a 5.7 GB CSV with
93 columns and ~23 million rows (as of the March 2026 vintage).
The file is internally inconsistent in ways that trip up strict CSV
parsers. This note documents each failure mode encountered during the
March 2026 pipeline run, the specific lines that surfaced the issue,
the probable cause, and the mitigation applied.

---

## Issue 1 — Backslash-escaped JSON in a non-selected column

**Line:** 3 111 654

**Original line (truncated):**
```
2918-1908-7282-3614-1974,"30, Wood Close",,,"30, Wood Close",SO19 0SG,
2014-12-29,100060745001,60,…,electricity (not community),
"{\"value\": 8.14, \"quantity\": \"metres\"}",3,Y,…
```

**Error:**
```
_duckdb.InvalidInputException: Invalid Input Error: CSV Error on Line: 3111654
Value with unterminated quote found.
```

**Cause:**
The `unheated_corridor_length` column (not one of the nine columns we
select) contains a JSON object with backslash-escaped inner quotes:
`"{\"value\": 8.14, \"quantity\": \"metres\"}"`. This is not valid RFC
4180 CSV — standard CSV escapes an embedded double quote by doubling it
(`""`), not by prefixing a backslash. DuckDB's sniffer sampled the first
20 KB of the file, found no backslash-escaped fields, and auto-detected
`escape='"'` (RFC 4180 doubling). When it hit this row 3 million lines
in it treated the `\"` as an end-of-quote followed by stray text, and
raised an unterminated-quote error under strict mode.

**Mitigation:** `strict_mode=false` on the `read_csv` call. DuckDB
attempts a best-effort parse of non-conforming rows rather than aborting.
Because `unheated_corridor_length` is not in our nine-column projection,
any misparsing of that field does not affect output values.

---

## Issue 2 — Single-quoted address field triggers wrong quote/escape detection

**Line:** 13 441 320

**Original line (truncated):**
```
9380-2546-5000-2320-7011,'OLD TRINITY HALL',CHARLTON VILLAGE ROAD,
CHARLTON,"'OLD TRINITY HALL', CHARLTON VILLAGE ROAD, CHARLTON",OX12 7HW,
2020-10-26,10014020903,83,…,2020-10-27 00:00:00,…
```

**Error:**
```
_duckdb.ConversionException: Conversion Error: CSV Error on Line: 13441320
Could not convert string "2020-10-27 00:00:00" to 'BIGINT'
Column total_floor_area is being converted as type BIGINT
  escape = ' (Auto-Detected)
```

**Cause:**
`ADDRESS1` for this property is `'OLD TRINITY HALL'` — the address is
wrapped in single quotes in the raw data. DuckDB's sniffer interpreted the
leading `'` as a quote character and auto-detected `escape='\''`. With
single quote as the active quote/escape character, the field boundaries
shifted: what is actually `lodgement_datetime` (`2020-10-27 00:00:00`)
ended up being read into the `total_floor_area` slot, causing the integer
conversion failure. The real floor area (83 m²) was somewhere else.

**Mitigation:** Pin `quote='"'` and `escape='"'` explicitly on the
`read_csv` call. Single quotes are then treated as literal text characters,
which is the correct interpretation — the value `'OLD TRINITY HALL'` is an
idiosyncratic way of styling the name, not a CSV quoting construct. All
column boundaries are now stable regardless of what the sniffer might
choose from the 20 KB sample.

---

## Issue 3 — Row with far fewer columns than the header

**Line:** 22 981 804

**Original line:**
```
0000-2800-7833-9572-2531,1,47,"£800 - £1,200",,
```

**Error:**
```
_duckdb.InvalidInputException: Invalid Input Error: CSV Error on Line: 22981804
Expected Number of Columns: 93 Found: 6
```

**Cause:**
This row has only six fields. The certificate number
`0000-2800-7833-9572-2531` looks plausible but the remaining fields
(`1`, `47`, `"£800 - £1,200"`, two empty strings) do not correspond to
the 93-column schema. The row appears to be a fragment or stub record —
possibly an artifact of how the MHCLG system generates the bulk export
when a certificate is partially submitted, or a formatting bug in the
new API's CSV assembly. DuckDB, having correctly detected the 93-column
schema from the header and the first ~20 KB of full rows, raises an error
when it encounters a row with only six fields.

**Mitigation:** `null_padding=true` on the `read_csv` call. Missing
fields in a short row are padded with `NULL` rather than treated as an
error. This row ends up with `NULL` for `UPRN` and `NULL` for
`TOTAL_FLOOR_AREA`, so it contributes nothing to price-per-sqm
calculations. It is retained in the output rather than silently dropped,
which is the safer default — if it had a valid UPRN we would not want to
lose it entirely.

---

## Issue 4 — Quoted newlines incompatible with parallel scanner + null_padding

**Line:** 106 397 288

**Error:**
```
_duckdb.Error: CSV Error on Line: 106397288
 The parallel scanner does not support null_padding in conjunction with
 quoted new lines. Please disable the parallel csv reader with parallel=false
```

**Cause:**
Some address fields in the EPC CSV contain embedded newline characters
inside a double-quoted field — a valid RFC 4180 construct but one that
DuckDB's parallel CSV scanner cannot handle when `null_padding=true` is
also active. The parallel scanner splits the file across worker threads;
a quoted newline that straddles a split boundary causes the scanner to
lose track of field boundaries and abort. Note that line 106 397 288 is
a raw newline count, not a record count — embedded newlines in quoted
fields inflate the line counter well beyond the ~23 million record count.

**Mitigation:** `parallel=false` on the `read_csv` call. This forces
single-threaded sequential scanning, which handles quoted newlines
correctly at the cost of some throughput. Given that the bottleneck for
this file is I/O (5.7 GB read from disk), the single-threaded mode has
negligible impact on wall-clock time.

---

## Summary of mitigations

All four issues are addressed by five parameters on the single
`read_csv(…)` call in `prepare_epc`:

```sql
FROM read_csv(
    'data/epc-domestic-all.csv',
    quote='"',          -- prevent single-quote misdetection (issue 2)
    escape='"',         -- prevent single-quote misdetection (issue 2)
    strict_mode=false,  -- tolerate non-RFC-4180 quoting (issue 1)
    null_padding=true,  -- pad short rows rather than erroring (issue 3)
    parallel=false      -- allow null_padding with quoted newlines (issue 4)
)
```

The pipeline logs a row count at each stage; any material loss of records
relative to previous vintages would surface as an anomaly there.

---

## Notes on the new API

All four issues appeared for the first time in the March 2026 vintage,
which is the first bulk download from the new MHCLG API
(`get-energy-performance-data.communities.gov.uk`). The old
`epc.opendatacommunities.org` endpoint produced per-local-authority ZIPs
that were individually smaller and apparently more consistently formatted.
The new API delivers a single monolithic CSV assembled from all
local-authority feeds, and the inconsistencies may reflect differences in
how individual local authorities or assessment software vendors format
their submissions before they reach the central register.

The four failure lines (3 111 654, 13 441 320, 22 981 804, 106 397 288)
are spread across the file, suggesting these are not isolated to one local
authority or assessment period.
