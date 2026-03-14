# ONS LSOA Boundary File Format

Research into which format of the ONS LSOA 2021 boundary file works best
with DuckDB's spatial extension, and what CRS considerations apply.

---

## Available formats

The ONS Open Geography Portal[^geoportal] publishes LSOA 2021 boundaries in
multiple formats:

| Format | Extension | Notes |
|---|---|---|
| Shapefile | `.shp` + sidecar files | Legacy format; 2 GB file size limit; multiple files |
| GeoJSON | `.geojson` | Single file; human-readable; large on disk |
| GeoPackage | `.gpkg` | Single binary file; spatially indexed; no size limit |

GeoParquet is not offered directly by the ONS geoportal (as of March 2026).

**Recommended format: GeoPackage (`.gpkg`).**

DuckDB's spatial extension reads GeoPackage via `ST_Read` with no additional
configuration. The single-file format is simpler to manage than Shapefile's
sidecar files, and its spatial index makes point-in-polygon queries faster
than scanning an unindexed GeoJSON.

---

## Generalised vs full resolution

The ONS publishes several clipping/generalisation variants:

| Code | Description | Use case |
|---|---|---|
| BFE | Full Extent | Includes coastal/estuarine areas; largest file |
| BFC | Full Clipped | Clipped to coastline; large |
| BGC | Generalised Clipped | 20 m generalisation; **recommended for joins** |
| BSC | Super Generalised Clipped | 200 m generalisation; for small-scale maps only |
| BUC | Ultra Generalised Clipped | 500 m generalisation; for national overview maps |

For point-in-polygon joins, the **BGC (Generalised Clipped, 20 m)** variant
is the right choice. Full-resolution boundaries add no accuracy benefit for
assigning a point to its containing polygon, and the smaller file size speeds
up the DuckDB spatial join considerably.

The V4 BSC dataset URL found during research:
`https://geoportal.statistics.gov.uk/datasets/ons::lower-layer-super-output-areas-december-2021-boundaries-ew-bsc-v4-2/about`

Search for the BGC variant on the portal — URL pattern will be similar with
`bgc` in the dataset slug.

---

## CRS

ONS boundary files use **BNG (British National Grid, EPSG:27700)** as the
standard CRS for England and Wales datasets. A WGS84 (EPSG:4326) variant is
sometimes published separately (labelled "WGS84" in the dataset title).

**Use the BNG variant.** OS Open UPRN coordinates are also in BNG
(EPSG:27700), so no reprojection is needed for the spatial join:

```sql
-- Both UPRN coordinates and boundary polygons are in EPSG:27700
-- ST_Point(easting, northing) is directly comparable to l.geom
ST_Within(
    ST_Point(u.X_COORDINATE, u.Y_COORDINATE),
    l.geom
)
```

If only the WGS84 variant is available, reproject the UPRN point:

```sql
-- Reproject BNG point to WGS84 before comparing to WGS84 boundary
ST_Within(
    ST_Transform(ST_Point(u.X_COORDINATE, u.Y_COORDINATE), 'EPSG:27700', 'EPSG:4326'),
    l.geom
)
```

---

## Column names

Confirmed from dataset documentation and consistent with our fixture GeoJSON:

| Column | Type | Description |
|---|---|---|
| `LSOA21CD` | VARCHAR | LSOA 2021 code (e.g. `E01000001`) |
| `LSOA21NM` | VARCHAR | LSOA 2021 name (e.g. `City of London 001A`) |
| `LSOA21NMW` | VARCHAR | Welsh name (Wales only; NULL for England) |
| `geom` | GEOMETRY | Polygon geometry (DuckDB ST_Read column name) |

These match the column names already used in `tests/fixtures/lsoa_sample.geojson`
and `src/houseprices/spatial.py`. No changes needed to the join query.

---

## Download instructions

1. Go to: `https://geoportal.statistics.gov.uk/`
2. Search for: `LSOA December 2021 Boundaries BGC`
3. Select the England and Wales (EW) dataset
4. Click "Download" → choose GeoPackage
5. Save as `data/lsoa_boundaries.gpkg`

The pipeline's `download_data()` function should automate this. The ONS
geoportal provides a direct download API URL — confirm the current URL when
implementing the download step, as the geoportal occasionally changes slugs.

---

## DuckDB ST_Read usage

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

# Works with both GeoPackage and GeoJSON
result = con.execute("""
    SELECT LSOA21CD, LSOA21NM, geom
    FROM ST_Read('data/lsoa_boundaries.gpkg')
    LIMIT 5
""").df()
```

---

## References

[^geoportal]: ONS Open Geography Portal. <https://geoportal.statistics.gov.uk/>

[^lsoa-bsc]: ONS, "Lower layer Super Output Areas (December 2021) Boundaries EW BSC (V4)". <https://geoportal.statistics.gov.uk/datasets/ons::lower-layer-super-output-areas-december-2021-boundaries-ew-bsc-v4-2/about>
