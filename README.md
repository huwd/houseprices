# houseprices

A data pipeline that joins [HM Land Registry Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) (PPD) to domestic [Energy Performance Certificates](https://epc.opendatacommunities.org/) (EPC) to produce **price-per-square-metre** figures by postcode district and LSOA. Inspired by and comparable with [Anna Powell-Smith's analysis](https://houseprices.anna.ps).

All source data is Open Government Licence v3.0.

---

## How it works

1. **Download** — fetches ~25 GB of raw data (PPD, EPC, OS Open UPRN, UBDC lookup, ONS LSOA boundaries)
2. **Join** — links property sales to floor area via a two-tier strategy:
   - Tier 1: direct UPRN match via the [UBDC PPD→UPRN lookup](https://data.ubdc.ac.uk/dataset/hm-land-registry-price-paid-data-with-uprns)
   - Tier 2: normalised address fallback (postcode + street address)
3. **Spatial** — maps each UPRN to its LSOA using a DuckDB point-in-polygon join
4. **Aggregate** — computes `total_price / total_floor_area` per postcode district and LSOA
5. **Output** — writes two CSVs to `output/`

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

- ~30 GB free disk space

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

To force a full re-run from scratch:

```bash
make clean && make run
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
```

See [`PLAN.md`](PLAN.md) for full methodology and [`data/SOURCES.md`](data/SOURCES.md) for dataset details.
