# Postcode District Boundary Sources

Research into candidate sources for postcode district boundary geometry,
conducted March 2026 as part of replacing the Anna Powell-Smith Mapbox
boundary scrape (issue [#77](https://github.com/huwd/houseprices/issues/77)).

---

## Candidates evaluated

### 1. Anna Powell-Smith Mapbox tileset (current approach)

Postcode district boundaries reconstructed by fetching ~725 MVT tiles at zoom 10
from Anna's public Mapbox tileset (`annapowellsmith.2kq8mrxg`), using her
hardcoded API token. The tileset is described as OGL-licensed OS/Royal Mail data.

**Problems:**

- Uses a third party's Mapbox quota without permission
- Token could be rotated at any time
- Tile-derived geometry has quantisation artefacts at tile seams
- No programmatic update mechanism

**Output size:** ~6.5 MB GeoJSON after MVT reconstruction + simplification.

---

### 2. missinglink/uk-postcode-polygons (GitHub)

Repository: <https://github.com/missinglink/uk-postcode-polygons>

GeoJSON files, one per postcode area, each containing the constituent districts
as features. Covers postcode areas and districts across the UK.

**How it was made:**

The polygons are sourced from KML files attached to Wikipedia templates on the
"List of postcode districts in the United Kingdom" Wikipedia page. The repo
converts these with `@mapbox/togeojson` and `@mapbox/geojson-rewind`. The
README explicitly states the repo "should be considered read-only" — edits
must go to Wikipedia directly.

**Licence:** Creative Commons Attribution ShareAlike 3.0 Unported (CC BY-SA
3.0). All geometry is "© Wikipedia contributors."

**Assessment:**

| Property | Finding |
|---|---|
| Source authority | Wikipedia contributors — hand-drawn KML |
| Coord count (E1) | 350 |
| Coord count (E11) | 256 |
| Contiguity (E1↔E2) | Clean shared border (distance = 0.0°) |
| E20 coverage | Yes — but only 11 vertices; rough convex hull |
| Licence | CC BY-SA 3.0 — ShareAlike requirement is a problem |

The geometry is low-resolution and hand-drawn. E20 is present but is an
11-vertex convex hull approximation, not a real boundary. The CC BY-SA
ShareAlike clause would require derivative works to be released under the same
terms, which is incompatible with our OGL-licensed pipeline outputs.

**Not suitable as a primary or fallback source.**

---

### 3. Geolytix PostalBoundariesOpen (chosen)

Blog: <https://geolytix.com/blog/uk-postal/>
Download: Google Drive ZIP — `GEOLYTIX - PostalBoundariesOpen2012.zip`

Postal boundaries for areas (124), districts (2,736), and sectors across the
UK. Originally derived from OS open data, created October 2012, with manual
updates for accuracy and cartographic quality.

**Licence:** OS Open Data licence with attribution to Geolytix Ltd. (full terms
in `PostalBoundariesLicence.pdf` inside the data pack). Compatible with our
pipeline outputs — no ShareAlike restriction.

**Format:** Shapefile (`.shp`) inside `PostalBoundariesSHP.zip`. CRS: BNG
EPSG:27700 — matches OS Open UPRN and our existing spatial pipeline; no
reprojection needed at the join step.

**Key field:** `PostDist` (4-char string) — identical to the property name
already used by `fetch_boundaries.py` and `build_page.py`. Drop-in replacement.

**Assessment:**

| Property | Finding |
|---|---|
| Source authority | OS-derived, manually maintained by Geolytix |
| Coord count (E1) | 397 |
| Coord count (E11) | 1,240 |
| Contiguity (E1↔E2) | Clean shared border (distance = 0.0°) |
| E20 coverage | **Missing** — post-2012 district |
| Districts in our output | 2,278 / 2,279 (99.96%) |
| Licence | OS Open Data + Geolytix attribution |

The higher coordinate counts versus missinglink reflect higher-quality
OS-derived source geometry, not worse simplification. At 250 m
Douglas-Peucker simplification (applied in BNG before reprojecting to WGS84),
output is **5.3 MB** — comparable to the 6.5 MB Anna output.

**Chosen.** See [#77](https://github.com/huwd/houseprices/issues/77).

---

## Contiguity

Both missinglink and Geolytix have clean contiguous borders (zero-gap shared
edges between adjacent districts in the E area tested). Contiguity is not a
differentiator; source authority and licence are.

---

## The E20 gap

E20 (Stratford/Olympic Park) was created after the October 2012 Geolytix
freeze. It is the only district in our current pipeline output missing from
Geolytix: **922 sales, £553M total transaction value**.

missinglink has E20 but its geometry is an 11-vertex rough convex hull — not
suitable as an authoritative fallback.

The ONS Geography Portal maintains a current postcode district layer under OGL,
making it the correct fallback source for post-2012 districts. See issue
[#79](https://github.com/huwd/houseprices/issues/79) for the gap-detection and
ONS fallback implementation plan.

---

## Download automation

The Geolytix ZIP is only available via Google Drive. Large Google Drive files
trigger a virus-scan confirmation page that breaks scripted downloads (`gdown`
and similar tools are regularly broken by Google as they update the flow).

Since the dataset is genuinely static (no updates since 2012), the right
approach is to convert it once and **commit the resulting GeoJSON** to
`data/`, alongside `cpi.csv`, as a static reference artefact. This removes
the download dependency entirely.

---

## References

- Geolytix blog: <https://geolytix.com/blog/uk-postal/>
- missinglink repo: <https://github.com/missinglink/uk-postcode-polygons>
- ONS Geography Portal (postcode districts): <https://geoportal.statistics.gov.uk/>
