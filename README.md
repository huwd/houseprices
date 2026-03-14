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

Intermediate results are checkpointed to `cache/` as Parquet files so re-runs are fast (~2 min vs ~20 min from scratch).

---

## First-time setup

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- ~30 GB free disk space for data

### 1. Clone and install dependencies

```bash
git clone https://github.com/huw/houseprices.git
cd houseprices
uv sync --all-extras
```

### 2. Set up environment variables

Copy the example env file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
EPC_EMAIL=your-email@example.com
EPC_API_KEY=your-epc-api-key
OS_DATA_HUB_API_KEY=your-os-data-hub-api-key
```

Where to get each key:

| Variable | Where to get it |
|---|---|
| `EPC_EMAIL` | Register free at [epc.opendatacommunities.org](https://epc.opendatacommunities.org/login) |
| `EPC_API_KEY` | Shown in your account settings after registering |
| `OS_DATA_HUB_API_KEY` | Register free at [osdatahub.os.uk](https://osdatahub.os.uk/) and create an API key |

### 3. Download the data

```bash
uv run python -c "
from houseprices.download import download_ppd, download_epc, download_os_open_uprn, download_lsoa_boundaries
download_ppd('data')
download_epc('data')
download_os_open_uprn('data')
download_lsoa_boundaries('data')
"
```

The UBDC PPD→UPRN lookup must be downloaded manually — see [`data/SOURCES.md`](data/SOURCES.md) for the DOI link and instructions.

> Downloads skip files that already exist, so it is safe to re-run if interrupted.

---

## Running the pipeline

```bash
uv run python src/houseprices/pipeline.py
```

On first run this takes ~20 minutes. Subsequent runs use cached Parquet checkpoints and complete in ~2 minutes.

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

## Running the notebook

The notebook at `notebooks/analysis.ipynb` compares the pipeline output against Anna Powell-Smith's reference figures.

**Interactive:**

```bash
uv run jupyter lab
```

Then open `notebooks/analysis.ipynb` in the browser.

**Headless:**

```bash
uv run jupyter nbconvert --to notebook --execute notebooks/analysis.ipynb
```

---

## Development

```bash
uv run pytest                  # run tests
uv run pytest --cov            # tests with coverage
uv run ruff check .            # lint
uv run ruff format --check .   # check formatting
uv run mypy src/               # type checking
```

Full CI check (mirrors what runs on pull requests):

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest --cov
```

See [`PLAN.md`](PLAN.md) for the full methodology.
