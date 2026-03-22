# District Count Gap vs Anna Powell-Smith

Investigation into why our pipeline produces a different number of postcode
districts than Anna Powell-Smith's equivalent analysis at
[houseprices.anna.ps](http://houseprices.anna.ps/).

Conducted March 2026 in response to issue
[#91](https://github.com/huwd/houseprices/issues/91).

---

## Current counts (v0.2.0, PPD through January 2026)

| Source | Districts |
|---|---|
| This pipeline | 2,277 |
| Anna Powell-Smith | 2,278 |

The net gap is **1 district**. The issue was originally filed with a gap of 3;
that gap narrowed as the pipeline was updated. The raw difference is larger in
both directions (see below).

---

## Method

Anna's per-district data is not published as a flat file. Her figures were
extracted by querying her public Mapbox tileset
(`annapowellsmith.2kq8mrxg`) at zoom levels 6 and 9 using
`scripts/fetch_anna_reference.py`. Results were stable across both zoom levels
(2,278 districts in each case).

---

## Districts Anna has; we don't (4)

All four have residential sales in the PPD but fall below our `min_sales = 10`
pipeline filter.

| District | Anna's sales | Anna's price/m² | Notes |
|---|---|---|---|
| EC2R | 5 | £11,289 | Bank / City of London — predominantly commercial |
| EC3V | 2 | £8,235 | Monument / City of London — predominantly commercial |
| TR23 | 4 | £4,394 | Bryher, Isles of Scilly — remote, very sparse residential market |
| W1C | 3 | £11,457 | Oxford St / Mayfair fringe — predominantly commercial |

**Conclusion:** Anna publishes districts with as few as 2 transactions. Our
`min_sales = 10` filter is more conservative. These four districts are thin
but real; the figures would be unreliable with so few data points.

---

## Districts we have; Anna doesn't (3)

| District | Our sales | Our price/m² | Reason absent from Anna |
|---|---|---|---|
| E20 | 884 | £6,690 | Post-2012 creation (Olympic Park / East Village); likely absent from Anna's boundary file and possibly her older data vintage |
| EC3A | 12 | £9,245 | Present in Anna's tileset but `price = null, num = null` — she suppresses it, reason unclear |
| TD9 | 38 | £1,338 | Hawick / Scottish Borders — Anna covers England & Wales; TD9 is in Scotland |

---

## Relationship to issue #79

E20 is the district described in issue
[#79](https://github.com/huwd/houseprices/issues/79): it exists in our PPD+EPC
data (884 sales, £535M total value) but has **no geometry** in the Geolytix
PostalBoundariesOpen 2012 file, which predates the district's creation. E20
therefore appears in `price_per_sqm_postcode_district.csv` but is absent from
`postcode_districts.geojson` and the choropleth map.

Issue #79 proposes fetching E20's geometry from the ONS Geography Portal as a
build-time fallback. Anna's analysis similarly lacks E20, suggesting her
boundary source also predates the 2012 Olympic development.

---

## Summary

The gap has three structural causes:

1. **Minimum sales filter** — our `min_sales = 10` excludes four districts
   (EC2R, EC3V, TR23, W1C) that Anna publishes with 2–5 transactions each.
   These are real districts but statistically unreliable at that sample size.

2. **Post-2012 boundary gap** — E20 (Olympic Park) postdates the Geolytix
   boundary freeze. We have the sales data but no geometry; Anna appears to
   have neither. Resolved by #79.

3. **Geography scope** — TD9 is in Scotland. Anna covers England & Wales only;
   our pipeline currently includes it.

---

## References

- Issue [#91](https://github.com/huwd/houseprices/issues/91) — original investigation
- Issue [#79](https://github.com/huwd/houseprices/issues/79) — E20 geometry gap
- `scripts/fetch_anna_reference.py` — tileset extraction script
- `research/postcode-boundary-sources.md` — Geolytix boundary file notes
