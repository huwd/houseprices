# houseprices

A data pipeline that joins [HM Land Registry Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) (PPD) to domestic [Energy Performance Certificates](https://epc.opendatacommunities.org/) (EPC) to produce **price-per-square-metre** figures by postcode district and LSOA. Inspired by and comparable with [Anna Powell-Smith's analysis](https://houseprices.anna.ps).

All source data is Open Government Licence v3.0.

---

## How it works

1. **Download** — fetches ~25 GB of raw data (PPD, EPC, OS Open UPRN, UBDC lookup, ONS LSOA boundaries)
2. **Join** — links property sales to floor area via a three-tier strategy:
   - Tier 1: UPRN match via the [UBDC PPD→UPRN lookup](https://data.ubdc.ac.uk/dataset/hm-land-registry-price-paid-data-with-uprns) with **temporal EPC selection** — for each sale, picks the most recent EPC lodged before the sale date, or the earliest post-sale EPC if no prior certificate exists, within a ±10-year window
   - Tier 2: normalised address fallback (postcode + street address) — primary path for 2022+
   - Tier 3: enhanced normalisation for flat sub-building addresses (bare numeric SAON → "FLAT N")
3. **Spatial** — maps each UPRN to its LSOA using a DuckDB point-in-polygon join
4. **Aggregate** — computes `total_price / total_floor_area` per postcode district and LSOA
5. **Output** — writes two CSVs to `output/`

**Match rates (March 2026 run, ~29.3M category-A PPD records):**

| Tier | Records | Share |
|---|---|---|
| Tier 1 — UPRN direct | 20,239,307 | 69.1% |
| Tier 2 — address normalisation | 2,267,763 | 7.7% |
| Tier 3 — enhanced flat normalisation | 2,817 | <0.1% |
| **Total matched** | **22,509,887** | **76.9%** |
| Unmatched | 6,773,388 | 23.1% |

The unmatched 23.1% has two structural causes: ~3.5M pre-2009 sales for properties that have never had an EPC lodged (no certificate exists to match), and ~1.2M 2022–2026 sales where the UBDC lookup has no coverage and address normalisation fails (see [`research/uprn-coverage-in-epc-data.md`](research/uprn-coverage-in-epc-data.md)).

Intermediate results are checkpointed to `cache/` as Parquet files so re-runs skip already-completed steps.

---

## Quick start

### 1. Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- GDAL (`ogr2ogr` on PATH) — for converting the LSOA boundary file

```bash
# Ubuntu / Debian
sudo apt install gdal-bin

# macOS
brew install gdal
```

- ~35 GB free disk space during download; ~2 GB after the first pipeline run (see [System resources](#system-resources))

### 2. Clone and install

```bash
git clone https://github.com/huwd/houseprices.git
cd houseprices
make install
```

### 3. Set up credentials

The EPC bulk download requires a free account at [epc.opendatacommunities.org](https://epc.opendatacommunities.org/login). Once registered, your API key is shown in account settings.

```bash
cp .env.example .env
# edit .env — add EPC_EMAIL and EPC_API_KEY
```

Everything else downloads without credentials.

### 4. Download the data

```bash
tmux new -s download
make download
```

~25 GB total. Each file is skipped if it already exists, so safe to re-run if interrupted.

### 5. Run the pipeline

```bash
tmux new -s pipeline
make run
```

The pipeline prints live progress — a spinner per step, elapsed time, and row counts on completion. First run takes ~20 minutes; subsequent runs use cached Parquet checkpoints and complete in ~2 minutes.

Output files are written to `output/`:

| File | Description |
|---|---|
| `price_per_sqm_postcode_district.csv` | Price per m² by postcode district (e.g. SW1A) |
| `price_per_sqm_lsoa.csv` | Price per m² by LSOA |

Both CSVs share this schema:

| Column | Type | Description |
|---|---|---|
| `postcode_district` / `LSOA21CD` | string | Geography identifier |
| `num_sales` | int | Matched sales contributing to the aggregate |
| `total_floor_area` | float | Sum of EPC floor areas (m²) |
| `total_price` | float | Sum of sale prices (£ nominal) |
| `adj_price_per_sqm` | int | **Headline.** Real Jan-2026 £/m² (CPI-adjusted) |
| `price_per_sqm` | int | Nominal £/m² — retained for reference |

All prices are CPI-adjusted to January 2026 pounds using the ONS CPI All Items
monthly series (D7BT). See [`research/cpi-deflator-choice.md`](research/cpi-deflator-choice.md)
for the deflator choice rationale.

`matched.parquet` in `cache/` includes two additional columns for Tier 1 rows:

| Column | Type | Description |
|---|---|---|
| `gap_days` | int | Days from sale date to selected EPC (negative = EPC before sale, positive = post-sale fallback); NULL for Tier 2/3 |
| `is_post_sale` | bool | True if the selected EPC was lodged after the sale date; NULL for Tier 2/3 |

To re-run the join and spatial steps without re-downloading:

```bash
make clean-cache && make run
```

To force a complete reset (wipes all cache and data, then re-downloads):

```bash
make dump-cache && make clean-data && make download && make run
```

---

## Notebook

The notebook at `notebooks/analysis.ipynb` compares the pipeline output against Anna Powell-Smith's reference figures.

```bash
make install                   # includes notebook extras
uv run jupyter lab             # then open notebooks/analysis.ipynb
```

---

## Development

```bash
make test        # run tests
make test-cov    # tests with coverage report
make lint        # ruff check + format check
make fmt         # auto-fix lint and formatting
make typecheck   # mypy
make check       # everything (lint + types + tests) — mirrors CI
make clean-cache # delete pipeline checkpoints (keeps slim Parquets)
make dump-cache  # delete all cache contents (requires re-download to re-run)
make clean-data  # remove downloaded data files (preserves committed files)
```

See [`PLAN.md`](PLAN.md) for full methodology and [`data/SOURCES.md`](data/SOURCES.md) for dataset details.

---

## System resources

### Disk space

The pipeline manages disk space aggressively to stay viable on modest machines.

| Stage | Peak disk use | After completion |
|---|---|---|
| `make download` | ~35 GB | ~30 GB (raw CSVs, ZIPs already deleted) |
| `make run` (prepare phase) | ~32 GB | ~2.5 GB (raw CSVs deleted once slim Parquets written) |
| `make run` (join phase) | ~12 GB (DuckDB temp) | ~2.5 GB |

**What the pipeline deletes automatically:**

- ZIP files are deleted by `make download` immediately after extraction
- `epc-domestic-all.csv` (26 GB), `os-open-uprn.csv` (2.2 GB), and `ppd-uprn-lookup.csv` (1.6 GB) are deleted by `make run` as soon as their column-pruned Parquet equivalents are written to `cache/`

**What stays on disk after a full run:**

| File | Size | Notes |
|---|---|---|
| `data/pp-complete.csv` | ~400 MB | Price Paid Data — kept as CSV |
| `data/lsoa_boundaries.gpkg` | ~45 MB | LSOA boundary polygons |
| `cache/epc_slim.parquet` | ~0.5–1 GB | 9-column EPC subset, deduplicated, ZSTD compressed |
| `cache/epc_full.parquet` | ~1–2 GB | 9-column EPC subset, all rows (for temporal Tier 1 matching) |
| `cache/uprn_slim.parquet` | ~300 MB | 3-column UPRN subset |
| `cache/ubdc_slim.parquet` | ~100 MB | 2-column UBDC lookup |
| `cache/matched.parquet` | ~0.5 GB | Joined PPD–EPC records |
| `cache/uprn_lsoa.parquet` | ~0.5 GB | UPRN→LSOA lookup |
| `output/*.csv` | ~5 MB | Final results |

**Total after a complete run: ~4–5 GB** (increased by ~1–2 GB for `epc_full.parquet`).

### `make clean` vs `make clean-all`

- `make clean-cache` — deletes `matched.parquet` and `uprn_lsoa.parquet` only. Slim Parquets are preserved, so re-running does not require re-downloading the raw data.
- `make dump-cache` — deletes all files in `cache/` including slim Parquets. Pair with `make clean-data` and `make download` for a full reset.
- `make clean-data` — removes all downloaded files from `data/`, preserving committed files (`SOURCES.md`, `anna_reference.json.example`) and dotfiles (`.gitkeep`).

### RAM

The tier-1 temporal join, tier-2, and tier-3 address-normalisation joins are the most memory-intensive steps — they scan the full EPC and PPD datasets simultaneously.  The tier-1 window function creates a temporary fan-out (~1.4–2× the matched PPD row count) before reducing to one row per sale; DuckDB spills this to disk when the memory limit is reached.  By default DuckDB uses all available RAM, which on a machine with 8 GB can exhaust RAM and swap and hard-freeze the OS.

**Set `DUCKDB_MEMORY_LIMIT` and `DUCKDB_THREADS` in your `.env` before running.** When the memory limit is reached, DuckDB spills temporary data to disk rather than crashing the system — the pipeline runs slower but completes safely.

```bash
# .env — recommended for an 8 GB laptop
DUCKDB_MEMORY_LIMIT=2GB   # leaves ~1 GB headroom for Python on top of DuckDB
DUCKDB_THREADS=2          # matches physical core count; reduces peak load
```

`make run` and `make rematch` wrap the pipeline in a `systemd-run --scope` cgroup with a hard memory ceiling (`MEM_MAX=3G`) and a soft throttle (`MEM_HIGH=2500M`) that causes gradual kernel reclaim before the ceiling is hit, preventing the PSI spike that would otherwise cause `systemd-oomd` to kill the whole desktop session. See [`MEMORY_CHALLENGES.md`](MEMORY_CHALLENGES.md) for the full history.

The active DuckDB values are printed at the start of each `make run` so you can confirm the config is being picked up. If neither variable is set, DuckDB defaults apply (no memory limit, all CPU threads) — safe on machines with 16 GB or more.
