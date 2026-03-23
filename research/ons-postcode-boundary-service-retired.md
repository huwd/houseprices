# ONS Postcode District Boundary Service — Retired

Investigated March 2026 while attempting to bake E20 boundary geometry into
`data/postcode_districts.geojson` (issues [#79][i79], [#81][i81]).

## What we tried

`scripts/prepare_boundaries.py --augment-ons E20` was written to query the ONS
Open Geography Portal ArcGIS FeatureServer for postcode district boundaries:

```
https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/
  Postcode_Districts_December_2023_Boundaries_UK_BGC/FeatureServer/0/query
```

When run in March 2026, the service returns `{"error":{"code":400,"message":"Invalid URL"}}`.
Browsing the full services index at
`https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services?f=json`
confirms no service with `Postcode_Districts` in the name exists. The ONS have
published December 2024 updates for Local Authority Districts, Counties, and
other administrative geographies, but **no postcode district boundary product
exists on the portal as of March 2026**.

This appears to be a permanent retirement rather than a temporary outage — the
service name is absent from the index entirely.

## Options for E20 boundary

| Option | Quality | Licence | Effort |
|---|---|---|---|
| Convex hull from ONSPD centroids | Poor — overextends into adjacent districts | OGL | Low |
| OS Code-Point with Polygons | Good | Licensed (paid OS product) | Low |
| Draw by hand from OS map | Reasonable | Depends on reference used | Medium |
| Remap E20 → E15 in pipeline (issue [#80][i80]) | Loses E20 as distinct district | OGL | Low |
| Leave as missing, surface in UI | N/A | N/A | None (already done) |

The convex hull approach is not materially better than the 11-vertex
missinglink polygon already ruled out in `research/postcode-boundary-sources.md`.

## Current state

E20 is absent from `data/postcode_districts.geojson` and will remain so until
a better source is identified. The pipeline already surfaces this correctly:
`build_page.py` writes `output/missing_districts.txt` and the page explains
why E20 is absent from the map (existing `missing_geometry` logic, merged in
[#79][i79]).

The interim E20 → E15 remap (issue [#80][i80]) remains a viable workaround if
losing E20 as a distinct district is acceptable.

[i79]: https://github.com/huwd/houseprices/issues/79
[i80]: https://github.com/huwd/houseprices/issues/80
[i81]: https://github.com/huwd/houseprices/issues/81
