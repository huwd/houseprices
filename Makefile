.DEFAULT_GOAL := help

# ── Setup ──────────────────────────────────────────────────────────────────

.PHONY: install
install:  ## Install all dependencies (dev + notebook extras)
	uv sync --all-extras

# ── Data ───────────────────────────────────────────────────────────────────

.PHONY: download
download:  ## Download and extract all raw data (~25 GB). Requires .env credentials.
	uv run python src/houseprices/download.py

.PHONY: clean-data
clean-data:  ## Remove downloaded data files from data/ (preserves committed files and dotfiles)
	find data/ -maxdepth 1 -type f ! -name '.*' ! -name 'SOURCES.md' ! -name 'anna_reference.json.example' -delete

.PHONY: clean-cache
clean-cache:  ## Delete pipeline checkpoints (keeps slim Parquets; safe to re-run without re-downloading)
	rm -f cache/matched.parquet cache/uprn_lsoa.parquet

.PHONY: dump-cache
dump-cache:  ## Delete all cache/ contents and slim Parquets (pair with clean-data + download for full reset)
	find cache/ -maxdepth 1 -type f ! -name '.*' -delete

# ── Pipeline ───────────────────────────────────────────────────────────────

# MEM_MAX: hard cgroup ceiling for the pipeline process.
# MEM_HIGH: soft throttle limit — cgroup is slowed before hitting MEM_MAX,
#   reducing the PSI spike that causes systemd-oomd to cascade-kill the desktop.
#   Set ~0.5 G below MEM_MAX so there is a gradual ramp-up rather than a hard wall.
# DUCKDB_MEMORY_LIMIT: DuckDB's internal cap — must be set so DuckDB self-limits
#   before hitting MEM_HIGH. Without this, DuckDB grows unconstrained and the total
#   process RSS can exceed MEM_MAX, tipping systemd-oomd into killing the whole
#   user session. Rule of thumb: DUCKDB_MEMORY_LIMIT + ~1 GB Python overhead must
#   be comfortably below MEM_HIGH, and MEM_MAX must leave ~3 GB for the desktop.
MEM_MAX ?= 3G
MEM_HIGH ?= 2500M
DUCKDB_MEMORY_LIMIT ?= 2G

.PHONY: run
run:  ## Run the full pipeline with a hard memory cap (join → spatial → aggregate → output CSVs)
	systemd-run --user --scope -p MemoryHigh=$(MEM_HIGH) -p MemoryMax=$(MEM_MAX) -- \
		env DUCKDB_MEMORY_LIMIT=$(DUCKDB_MEMORY_LIMIT) \
		uv run python src/houseprices/pipeline.py

.PHONY: rematch
rematch:  ## Apply tier-3 normalisation to unmatched records (no re-download; requires prior run)
	systemd-run --user --scope -p MemoryHigh=$(MEM_HIGH) -p MemoryMax=$(MEM_MAX) -- \
		env DUCKDB_MEMORY_LIMIT=$(DUCKDB_MEMORY_LIMIT) \
		uv run python src/houseprices/pipeline.py --rematch

# ── Output page ────────────────────────────────────────────────────────────

.PHONY: boundaries
boundaries:  ## Fetch + cache postcode district boundary GeoJSON (one-off, ~48 tile downloads)
	uv run scripts/fetch_boundaries.py

.PHONY: page
page:  ## Build output/index.html from pipeline outputs (run boundaries + pipeline first)
	uv run python scripts/build_page.py

# ── Notebook ───────────────────────────────────────────────────────────────

.PHONY: explore
explore:  ## Open the analysis notebook in Jupyter Lab
	uv run jupyter lab notebooks/analysis.ipynb

# ── Development ────────────────────────────────────────────────────────────

.PHONY: test
test:  ## Run tests
	uv run pytest

.PHONY: test-cov
test-cov:  ## Run tests with coverage report
	uv run pytest --cov

.PHONY: lint
lint:  ## Check linting and formatting
	uv run ruff check .
	uv run ruff format --check .

.PHONY: fmt
fmt:  ## Auto-fix lint and formatting issues
	uv run ruff check . --fix
	uv run ruff format .

.PHONY: typecheck
typecheck:  ## Run mypy type checker
	uv run mypy src/

.PHONY: check
check: lint typecheck test-cov  ## Full CI check (lint + types + tests with coverage)

# ── Help ───────────────────────────────────────────────────────────────────

.PHONY: help
help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
