# Memory Challenges — Running Log

This document records the memory / OOM problems encountered running the
pipeline on a low-RAM machine (8 GB laptop), and the mitigations applied.
It is a working reference, not a changelog — update it when new problems
or fixes are found.

---

## The core problem

The pipeline processes large datasets:

- HM Land Registry PPD — ~27 M rows
- EPC bulk export — ~25 M rows (deduped to ~15 M)
- UBDC UPRN lookup — ~30 M rows

On an 8 GB laptop, naively joining these in-memory causes DuckDB and/or
Python to exhaust RAM + swap, either crashing the Python process or
(worse) triggering `systemd-oomd` to kill the whole user session,
logging the user out mid-run.

---

## Timeline of mitigations (oldest → newest)

### 1. `systemd-run` cgroup cap (`make run`) — `844c16e`

**Problem:** OOM kills were taking out the whole user session, not just
the pipeline process.

**Fix:** Wrapped `make run` with `systemd-run --user --scope
-p MemoryMax=...` so the kernel enforces a hard ceiling on the pipeline's
cgroup. Any OOM within the cgroup kills only the pipeline, not the desktop.

**Config:** `MEM_MAX` Makefile variable (default 4 G, was 7 G → 5 G →
4 G as we tuned).

---

### 2. DuckDB env-var limits (`DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS`) — `c468f4d`

**Problem:** Without an internal limit, DuckDB grows unconstrained until
it hits the cgroup ceiling and gets OOM-killed hard, with no graceful
spill to disk.

**Fix:** Added `_configure_duckdb()` called at startup in both
`pipeline.py` and `spatial.py`. Reads `DUCKDB_MEMORY_LIMIT` and
`DUCKDB_THREADS` from environment (loaded from `.env` via
`python-dotenv`). When set, DuckDB spills to disk rather than
continuously allocating.

**Recommended values for 8 GB:**
```
DUCKDB_MEMORY_LIMIT=3GB
DUCKDB_THREADS=2
```

**Gotcha (fixed `f40b3ec`):** `.env` was not being loaded in the pipeline
entry point, so `DUCKDB_MEMORY_LIMIT` was silently ignored on the first
attempt.

**Gotcha (fixed `d870bf9`):** `make run` passes the variable via `env`
on the command line, but the `.env` file must also set it for direct
`python pipeline.py` invocations. Without both, oomd could still log the
user out.

---

### 3. `prepare_epc` streaming via temp file — `2787f52`

**Problem:** `prepare_epc()` read the entire ~25 M-row EPC CSV into a
DuckDB relation, deduplicated it, then materialised it as a pandas
DataFrame before writing Parquet. Peak RSS spiked on the materialise step.

**Fix:** Rewrote to use `COPY … TO … (FORMAT PARQUET)` to stream from
DuckDB directly to disk without going through Python/pandas heap.
A temporary intermediate Parquet is written then renamed atomically.
The temp file is cleaned up even on failure.

---

### 4. Postcode semi-join pre-filter in tier-2 — `cd3a94d`

**Problem:** Tier-2 (address normalisation) was loading the full ~15 M-row
deduplicated EPC dataset for every join attempt, even though most EPC rows
cannot possibly match the PPD records in scope (different postcodes).

**Fix:** Added a postcode semi-join CTE (`epc_norm`) that filters the EPC
dataset to only postcodes present in the PPD input before the expensive
address normalisation. Dramatically reduces the working set for tier-2.

---

### 5. EPC deduplication moved into `prepare_epc` — `ab82e29`

**Problem:** Duplicate EPC records (multiple certificates for the same
address) were being handled at join time, bloating intermediate results.

**Fix:** `prepare_epc()` now deduplicates on `(address, postcode)` keeping
the most recent certificate before writing the slim Parquet. Downstream
join queries simplified as a result.

---

### 6. `build_uprn_lsoa` UPRN filter — `9e3778b`

**Problem:** The spatial join read the full UBDC UPRN dataset (~30 M rows)
even though only the subset of UPRNs that matched in tier-1 are needed.

**Fix:** `run()` now passes `matched_uprns` (a Python `set`) to
`build_uprn_lsoa`, which filters the UBDC input to that set before the
point-in-polygon join. Reduces the spatial join working set to matched
records only.

---

### 7. Avoid double-materialising tier-1 + tier-2 — `cab7b09`

**Problem:** `join_datasets()` wrote tier-1 and tier-2 to temp Parquet
files but then re-read both with `read_parquet(…)` into a UNION ALL query
that was fully materialised as a DataFrame before returning.

**Fix:** Intermediate results stay in temp Parquet files; the final UNION
ALL uses DuckDB `COPY … TO … (FORMAT PARQUET)` to stream to disk. The
function return type changed from `pd.DataFrame` to `None`; callers read
back from disk with `pd.read_parquet()`.

**Signature change:** `join_datasets(…, dst: pathlib.Path)` — PR #45,
commit `92b5713`.

---

### 8. Per-step RSS reporting — `8e57bba`

**Observability:** Added `_rss_mb()` helper (reads `/proc/self/status`)
and wired it into the pipeline's progress output so each step prints its
RSS after completing. Allows profiling of which step is the biggest
offender without a separate memory profiler.

---

### 9. Stream tier-1 and tier-2 joins to Parquet — current

**Problem:** `_join_tier1` and `_join_tier2` called `.df()` to materialise
the join result into a Python/pandas DataFrame before writing it to a temp
Parquet file.  With a DuckDB memory limit of 3 G and the join result
potentially spanning millions of rows, the `.df()` call pushed total RSS past
the 4 G cgroup ceiling (Error 137 from `make run`).

**Fix:** Both functions now use DuckDB `COPY … TO … (FORMAT PARQUET)` to
stream the result directly to disk without any Python-heap materialisation.
Return type changed from `pd.DataFrame` to `int` (row count).
`join_datasets` callback type updated to `Callable[[int], None]`.

---

### 10. Defer `matched` load until after spatial step — current

**Problem:** `run()` loaded `pd.read_parquet(matched_parquet)` into Python
heap immediately before starting the spatial join.  At that point DuckDB was
also running a point-in-polygon join against the LSOA boundary file.  The two
competed for the same 4 G cgroup, tipping the process past `MEM_MAX` and
triggering an OOM kill (Error 137 from `make run`).

**Fix:** Extract `matched_uprns` via a lightweight DuckDB column-select
(no DataFrame materialisation) before the spatial step, then load the full
`matched` DataFrame only *after* the spatial step's DuckDB connection has been
released.  Also `del uprn_lsoa` / `del uprn_to_lsoa` immediately after the
LSOA attachment to free them before aggregation.

---

### 10. Slim column copy in `aggregate_by_geography` — `<hash>`

**Problem:** `aggregate_by_geography` did `df = matched.copy()` — a full copy
of all 25+ columns — then grouped on just two columns (`price`,
`TOTAL_FLOOR_AREA`).  Called twice consecutively, this created up to 3×
matched-DataFrame-size of Python heap simultaneously.

**Fix:** Copy only the 3 columns needed (`postcode`/geography col + `price` +
`TOTAL_FLOOR_AREA`) before the groupby.  Added `del district_df` between the
two aggregation calls so the first result is freed before the second copy is
made.

---

### 11. Push aggregation into DuckDB — current

**Problem:** After the spatial step, `run()` called `pd.read_parquet(matched_parquet)`
to load the full matched DataFrame (~1.1 GB compressed, ~4+ GB in Python heap)
for LSOA attachment and aggregation.  `systemd-oomd` killed the pipeline at
exactly the 4 GB cgroup ceiling, 1 minute after `uprn_lsoa.parquet` completed.

**Fix:** Removed `pd.read_parquet(matched_parquet)` from `run()` entirely.
Match report, postcode district aggregation, and LSOA aggregation are now all
DuckDB SQL queries that read `matched.parquet` and `uprn_lsoa.parquet` by path.
Peak RSS during aggregation is now <1 GB (only the tiny result DataFrames —
~3 k district rows, ~35 k LSOA rows — ever enter Python heap).
The `aggregate_by_geography` and `match_report` pandas functions are unchanged
and still used by tests and the notebook.

---

### 12. Load only needed columns from `matched.parquet` in notebook — current

**Problem:** `analysis.ipynb` called `pd.read_parquet(CACHE / "matched.parquet")`
with no column filter, loading the full 1.1 GB Parquet file (~4+ GB in Python
heap) into the notebook kernel.  The kernel was killed by OOM before any cells
could complete.

**Fix:** Added `columns=["match_tier", "LSOA21CD"]` to the `read_parquet`
call — the only two columns the notebook ever uses from the matched dataset.
All other statistics (row counts, district and LSOA aggregates) come from the
small CSV outputs in `output/`, not from the full matched file.

---

### 13. Temporal fan-out in `_join_tier1` — issue #60

**Context:** Temporal EPC selection (issue #60) replaced the simple
UPRN equijoin in `_join_tier1` with a window-function ranking over
*all* EPC certificates for each matched UPRN.  This requires reading
`epc_full.parquet` (undeduped, ~1–2 GB compressed vs. ~0.5–1 GB for
`epc_slim.parquet`) and creating a candidate set before filtering to
one row per sale with `ROW_NUMBER() = 1`.

**Fan-out:** The intermediate candidate set is `PPD_tier1_rows ×
avg_EPCs_per_UPRN`.  Empirically ~75% of UPRNs have a single certificate
and ~25% have 2–3, giving an average fan-out of ~1.4–2×.  For ~20M
tier-1 PPD rows that is ~28–40M candidate rows, comfortably within
DuckDB's spill-to-disk capability at `DUCKDB_MEMORY_LIMIT=2G`.

**EPC source size:** `epc_full.parquet` adds ~1–2 GB to disk and
requires DuckDB to scan more rows than `epc_slim.parquet`.  The scan is
bounded to the ±10-year window filter before the window function
evaluates, so rows further from the sale date are pruned early.

**No new mitigations needed** at current cgroup limits; the fan-out is
already within the headroom.  If OOMs recur here, the same postcode
pre-filter pattern used in tier-2 (mitigation #4) could be applied to
reduce the EPC scan before the window.

---

## Current configuration (`MEM_MAX` / `DUCKDB_MEMORY_LIMIT`)

| Setting | Value | Where |
|---|---|---|
| `MEM_HIGH` (cgroup soft throttle) | `2500M` | Makefile default |
| `MEM_MAX` (cgroup hard limit) | `3G` | Makefile default |
| `DUCKDB_MEMORY_LIMIT` | `2G` | `.env` + Makefile default |
| `DUCKDB_THREADS` | `2` | `.env` |

Rule of thumb: `DUCKDB_MEMORY_LIMIT` + ~1 GB Python overhead must be
comfortably below `MEM_HIGH` (not `MEM_MAX`), and `MEM_MAX` must leave
~3 GB for the desktop.

`MEM_HIGH` is the soft throttle: once the pipeline's cgroup crosses this
threshold the kernel begins reclaiming pages and throttling allocations
gradually.  This prevents the PSI spike that causes `systemd-oomd` to
cascade-kill the whole user session — a hard `MEM_MAX` wall alone causes
rapid heavy paging that raises PSI far above oomd's 50% session threshold
before the pipeline is terminated.

---

## Remaining risk areas

- **`_join_tier1` temporal window function (issue #60):** ~28–40M candidate
  rows before `ROW_NUMBER() = 1` filter.  Currently within cgroup limits;
  see mitigation #13 for details.  Postcode pre-filter (mitigation #4
  pattern) available if needed.

- **Spatial join (point-in-polygon):** The DuckDB spatial extension
  performs the polygon intersection in-process. With the UPRN filter in
  place the working set is small, but boundary file size (ONS LSOAs) is
  ~200 MB. Watch RSS here on larger runs.

- **`_join_tier3` / `make rematch` (OOM 2026-03-18):** The tier-3 join
  reads EPC slim (~15 M rows) + PPD slim (~20 M+ remaining) in a single
  DuckDB query. With `DUCKDB_MEMORY_LIMIT=3G` this caused heavy disk
  spilling, a PSI spike to 69%, and a session-level oomd cascade that
  killed gnome-shell, dbus, VS Code and Firefox.  Mitigations applied:
  (a) lowered limits to `MEM_MAX=3G` / `MEM_HIGH=2500M` / `DUCKDB_MEMORY_LIMIT=2G`,
  (b) added `MemoryHigh` soft throttle to `systemd-run`,
  (c) pre-extract excluded transaction IDs to a narrow single-column
  Parquet before the main join so the anti-join hash table build is
  separated from the EPC+PPD join scan.  If it OOMs again, batch by
  postcode area letter.

---

## Lessons learned

1. **DuckDB's internal limit ≠ process RSS.** Python, pyarrow, and pandas
   allocations sit on top of DuckDB's limit. Always leave 1–2 GB of
   headroom between `DUCKDB_MEMORY_LIMIT` and `MEM_MAX`.

2. **`systemd-oomd` kills at the session level.** Without a cgroup
   boundary, oomd attributes the runaway RSS to the user slice and kills
   the entire login session. `systemd-run --scope` creates an isolated
   cgroup so only the pipeline dies.

3. **Materialising to DataFrame is the danger point.** DuckDB can handle
   data larger than RAM via spilling, but the moment you call `.df()` or
   `pd.read_parquet()` on a large result you move it fully into Python
   heap. Prefer `COPY … TO … (FORMAT PARQUET)` for large intermediates
   and only materialise small final outputs.

4. **Temp files need cleanup on failure.** Several OOM bugs were masked
   by partial temp files being left behind. Added `try/finally` cleanup
   in `prepare_epc` after the first incident.

5. **`MemoryMax` alone does not prevent PSI cascades.** A hard cgroup
   ceiling causes rapid heavy paging when exceeded, driving PSI above
   oomd's 50% user-session threshold and triggering cascade kills.  Add
   `MemoryHigh` below `MemoryMax` so the kernel throttles gradually and
   PSI stays low even while the pipeline is memory-constrained.
