# houseprices — Project Claude Standard

Extends the global standard at `~/.claude/CLAUDE.md`. Everything there applies.
This file adds project-specific context and overrides.

---

## What this project is

A data pipeline that joins HM Land Registry Price Paid Data (PPD) to domestic
Energy Performance Certificates (EPC) to produce price-per-square-metre figures
by postcode district and LSOA. See `PLAN.md` for full methodology.

---

## Project structure

```
src/houseprices/
  pipeline.py       # main pipeline: download → join → aggregate
  spatial.py        # UPRN coordinate → LSOA point-in-polygon
notebooks/
  analysis.ipynb    # comparison vs Anna's data, charts
tests/
  test_pipeline.py  # aggregation maths, join tier logic, normalisation
  test_spatial.py   # UPRN → LSOA lookup correctness
  fixtures/         # small CSV fixtures for unit tests
research/           # findings and source notes — committed
data/               # gitignored — raw downloaded data (multi-GB)
cache/              # gitignored — intermediate Parquet checkpoints
output/             # committed — final CSVs
TODO.md             # pending GitHub issues (local cache, not for main)
```

---

## Commands

### Setup

```bash
uv sync --all-extras          # install all dependencies including dev + notebook
```

### Development

```bash
uv run pytest                 # run tests
uv run pytest --cov           # tests with coverage report
uv run ruff check .           # lint
uv run ruff format --check .  # check formatting
uv run mypy src/              # type checking
```

### All checks (mirrors CI)

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest --cov
```

### Pipeline

```bash
uv run python src/houseprices/pipeline.py   # full pipeline run (checkpoints to cache/)
make clean                                  # wipe cache/ to force full rerun
```

### Notebook

```bash
uv run jupyter lab                                        # interactive
uv run jupyter nbconvert --to notebook --execute notebooks/analysis.ipynb  # headless
```

---

## Development workflow (TDD)

Follow a strict red-green-refactor cycle. Commit between each step.

1. **Red** — write failing tests. Verify `uv run pytest` fails on new cases,
   `uv run ruff check .` passes. Commit and push.
2. **Green** — write minimal code to make tests pass. Verify `uv run pytest`
   passes. Commit and push.
3. **Refactor** — improve code while keeping tests green. Commit and push.
4. **Type-check** — add type annotations, verify `uv run mypy src/` passes.
   Commit and push.
5. **Lint** — `uv run ruff check .` and `uv run ruff format .`, fix issues.
   Commit and push.

---

## Testing conventions

- **pytest** with descriptive test names
- **pytest-cov** — target ≥80% line coverage, ≥75% branch coverage
  (`fail_under = 80` enforced in `pyproject.toml`)
- Tests live in `tests/` mirroring `src/houseprices/` structure
- Small CSV fixtures in `tests/fixtures/` for unit tests — never use real data

```python
# Good: descriptive name, clear arrange/act/assert
def test_price_per_sqm_uses_total_not_mean_of_ratios():
    rows = [
        {"price": 200_000, "floor_area": 50},   # £4000/m²
        {"price": 400_000, "floor_area": 200},  # £2000/m²
    ]
    # Correct: 600_000 / 250 = £2400/m²
    # Wrong (mean of ratios): (4000 + 2000) / 2 = £3000/m²
    assert aggregate(rows)["price_per_sqm"] == 2400
```

---

## Code style

- **Python 3.12+**
- **uv** for dependency management
- **ruff** for linting and formatting (replaces black + isort + flake8)
  - Rule set: `E F I UP B SIM C4`
  - Always run `uv run ruff check .` against the whole project, not a single
    file — ruff's isort needs full project context to resolve first-party imports
- **mypy** in strict mode
- **Double quotes** for strings
- Public functions get docstrings; internal helpers do not unless non-obvious

---

## Python tooling

| Tool | Purpose |
|---|---|
| `uv` | Dependency management and virtual env |
| `ruff` | Linting and formatting |
| `mypy` | Static type checking (strict) |
| `pytest` + `pytest-cov` | Tests and coverage |
| `duckdb` | Data engine (spatial extension installed via `INSTALL spatial` at runtime) |
| `pyarrow` | Parquet read/write |
| `pandas` | DataFrame work in notebook |

---

## Data files

`data/` and `cache/` are gitignored — they contain multi-GB raw CSVs and
intermediate Parquet files. Never commit them. `output/` CSVs are committed.

The EPC bulk download requires free registration at epc.opendatacommunities.org.

---

## Versioning and release strategy

Analysis outputs are **semantically versioned**. Any change to the methodology,
data sources, or aggregation logic that produces different output CSVs is a
versioned release.

- Version is stored in `output/VERSION.txt`
- Changelog is `output/CHANGELOG.md` (Keep a Changelog format)
- Unreleased changes accumulate under `## [Unreleased]`; bump the version when
  shipping a release

**Versioning rules:**

| Change | Version bump |
|--------|-------------|
| New data vintage (same methodology) | patch (`0.1.x`) |
| Methodology change affecting rankings or figures | minor (`0.x.0`) |
| Breaking change to output schema or geography | major (`x.0.0`) |

**Release checklist** (see issue [#73](https://github.com/huwd/houseprices/issues/73)):

1. `make download && make run` from a clean state produces correct output
2. Bump version in `output/VERSION.txt`
3. Move `[Unreleased]` to `[x.y.z] — YYYY-MM-DD` in `output/CHANGELOG.md`;
   link to PRs, commits, and research notes
4. Rebuild `make page` and push

---

## Pull request standards

Include a **Preview** section in every PR description with the Cloudflare Pages
preview URL for that branch. Cloudflare Pages automatically builds each branch
and exposes it at:

```
https://<deployment-hash>.<project-name>.pages.dev/
```

where `<deployment-hash>` is an 8-character hex string (Cloudflare's deployment
ID, not the git commit SHA) and `<project-name>` is `houseprices-6r0`.

Example PR body:

```markdown
## Preview

https://387d19e5.houseprices-6r0.pages.dev/
```

The URL appears in the Cloudflare Pages dashboard under the deployment for the
branch, and as a commit status check on the PR.

---

## Key decisions (resolved)

- **ONS boundary CRS**: BNG EPSG:27700 — matches OS Open UPRN; no reprojection
  needed in `spatial.py`
- **EPC UPRN coverage**: ~92% overall (Boswarva 2022); UBDC lookup ends Jan 2022;
  2022–2026 coverage falls to 0% for UPRN tier, addressed by Tier 2 address
  normalisation
- **Deflator choice**: CPI (D7BT) chosen over CPIH (shorter history) and RPI
  (legacy measure); base month January 2026; see `research/cpi-deflator-choice.md`
- **HMLR UPRN-linked PPD**: not pursued — UBDC lookup covers same need with
  clearer OGL licensing
