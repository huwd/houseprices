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
uv sync
```

### 3. Set up credentials

The EPC bulk download requires a free account at [epc.opendatacommunities.org](https://epc.opendatacommunities.org/login). Once registered, your API key is shown in account settings.

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
EPC_EMAIL=your-email@example.com
EPC_API_KEY=your-epc-api-key
```

Everything else downloads without credentials.

### 4. Download the data

This takes a while (~25 GB total). Run it in a tmux session so it survives disconnects:

```bash
tmux new -s download
```

```bash
uv run python - << 'EOF'
import pathlib
from houseprices.download import (
    download_ppd,
    download_epc, extract_epc,
    download_ubdc, extract_ubdc,
    download_os_open_uprn, extract_os_open_uprn,
    download_lsoa_boundaries,
)

data = pathlib.Path("data")
download_ppd(data)
download_epc(data);   extract_epc(data)
download_ubdc(data);  extract_ubdc(data)
download_os_open_uprn(data); extract_os_open_uprn(data)
download_lsoa_boundaries(data)
EOF
```

Each download skips files that already exist, so it is safe to re-run if interrupted.

### 5. Run the pipeline

```bash
tmux new -s pipeline
```

```bash
uv run python src/houseprices/pipeline.py
```

The pipeline prints live progress — a spinner per step, elapsed time, and row counts on completion. First run takes ~20 minutes; subsequent runs use cached Parquet checkpoints and complete in ~2 minutes.

Output files are written to `output/`:

| File | Description |
|---|---|
| `price_per_sqm_postcode_district.csv` | Price per m² by postcode district (e.g. SW1A) |
| `price_per_sqm_lsoa.csv` | Price per m² by LSOA |

To force a full re-run from scratch:

```bash
make clean
uv run python src/houseprices/pipeline.py
```

---

## Notebook

The notebook at `notebooks/analysis.ipynb` compares the pipeline output against Anna Powell-Smith's reference figures.

```bash
uv sync --all-extras   # install notebook dependencies
uv run jupyter lab     # then open notebooks/analysis.ipynb
```

---

## Development

```bash
uv sync --all-extras          # install dev + notebook dependencies
uv run pytest                 # run tests
uv run pytest --cov           # tests with coverage
uv run ruff check .           # lint
uv run ruff format --check .  # check formatting
uv run mypy src/              # type checking
```

Full CI check (mirrors what runs on pull requests):

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest --cov
```

See [`PLAN.md`](PLAN.md) for full methodology and [`data/SOURCES.md`](data/SOURCES.md) for dataset details.
