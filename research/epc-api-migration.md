# EPC API Migration: epc.opendatacommunities.org → get-energy-performance-data.communities.gov.uk

**Issue**: https://github.com/huwd/houseprices/issues/76
**Researched**: 2026-03-19

---

## Background

The legacy EPC open data platform at `epc.opendatacommunities.org` (built on
the PublishMyData / Linked Data Mart platform) is being retired as part of
MHCLG's migration to a new cloud-based Open Data Communities platform. The EPC
team has simultaneously launched a dedicated replacement service.

**References:**
- Blog: https://mhclgdigital.blog.gov.uk/2026/03/04/shaping-the-future-of-open-data-with-open-data-communities/
- New service: https://get-energy-performance-data.communities.gov.uk/
- API technical docs: https://get-energy-performance-data.communities.gov.uk/api-technical-documentation
- OAS 3.0 spec (GitHub): https://raw.githubusercontent.com/communitiesuk/epb-data-warehouse/main/api/api.yml
- Swagger UI: https://get-energy-performance-data.communities.gov.uk/api-documentation/index.html
- Licensing: https://get-energy-performance-data.communities.gov.uk/guidance/licensing-restrictions

**Timeline:** ODC platform migration completing end of March 2026. No explicit
shutdown date for the old platform is published, but "interim" redirects suggest
it is not permanent. Treat this as urgent.

---

## What changed

### Authentication

| | Old | New |
|---|---|---|
| Method | HTTP Basic Auth | GOV.UK One Login bearer token |
| Env vars | `EPC_EMAIL`, `EPC_API_KEY` | `EPC_BEARER_TOKEN` |
| Header | `Authorization: Basic <base64(email:key)>` | `Authorization: Bearer <token>` |
| Token acquisition | Free registration at epc.opendatacommunities.org | Sign in at get-energy-performance-data.communities.gov.uk → copy from `/api/my-account` |
| Programmatic? | Yes (email + key scripted) | Token is long-lived static string — store in `.env` as `EPC_BEARER_TOKEN` |

No OAuth client credentials flow is documented. The bearer token is retrieved
once from the account UI and stored as an environment variable. Functionally
equivalent to the old API key model.

### Base URL

```
Old: https://epc.opendatacommunities.org/api/v1/
New: https://api.get-energy-performance-data.communities.gov.uk/
```

### Bulk download

| | Old | New |
|---|---|---|
| Endpoint | `/api/v1/files/all-domestic-certificates.zip` | `/api/files/domestic/csv` |
| Response | Direct ZIP stream | HTTP 302 → temporary AWS S3 signed URL |
| ZIP contents | Per-LA folders: `domestic-{LA}/certificates.csv` | **CSV files split by year** (verify on download) |
| File size | ~6.4 GB | ~2.9 GB (per info endpoint) |
| Update cadence | Monthly | Monthly, regenerated on the 1st |

The 302 redirect is followed transparently by `requests` with
`allow_redirects=True`. No code change needed for redirect handling.

**ZIP structure — to verify on first download.** The documentation describes
"a series of CSV files by year." Likely filenames: `domestic-YYYY.csv` or
`YYYY.csv`. The old `extract_epc()` scans for `certificates.csv` filenames and
will find nothing in the new ZIP. The updated implementation looks for all
`.csv` files excluding `recommendations.csv`, which handles both formats.

### Info endpoint (staleness detection)

New endpoint:

```
GET /api/files/domestic/csv/info
Authorization: Bearer <token>
Accept: application/json

→ {"data": {"fileSize": 2923946932, "lastUpdated": "2025-08-01T00:31:19.000+00:00"}}
```

This is a better staleness signal than the ETag-based `_check_freshness()` used
for the old URL:
- The `lastUpdated` timestamp directly encodes the upstream refresh date
- No HEAD request needed against the bulk file URL (the bulk URL returns 302;
  a HEAD on a redirect is less reliable than a dedicated info endpoint)
- Store `lastUpdated` in `epc_slim.meta.json` after a successful download;
  compare on next run to decide whether to re-download

### Rate limits

3 requests per second per application. HTTP 429 on excess. Relevant primarily
for search API calls; bulk download is a single request. Retry with exponential
backoff on 429.

---

## Licensing — split licence, no action required for aggregated outputs

The new service explicitly separates licensing:

**Non-address fields** (floor area, UPRN, energy ratings, `BUILT_FORM`,
`CONSTRUCTION_AGE_BAND`, `LODGEMENT_DATETIME`, etc.):
→ Open Government Licence v3.0, freely usable.

**Address fields** (`ADDRESS1`, `ADDRESS2`, `ADDRESS3`, `POSTCODE`):
→ OS AddressBase Premium / Royal Mail PAF copyright. Use permitted for:
energy efficiency analysis, property market transparency, enforcement of energy
regulations, research. **Raw address data must not be published at record level.**

**Impact on this project:** None. The pipeline uses postcode only as a join key
and groups addresses for normalised matching — no raw address strings appear in
the output CSVs. The use case (property market price-per-sqm analysis) falls
within the permitted categories. The `data/SOURCES.md` entry documents this.

---

## OAS 3.0 specification

The full spec is hosted publicly on GitHub:
```
https://raw.githubusercontent.com/communitiesuk/epb-data-warehouse/main/api/api.yml
```

Relevant schemas for this pipeline:

```yaml
FileInfoResponse:
  type: object
  required: [data]
  properties:
    data:
      type: object
      required: [fileSize, lastUpdated]
      properties:
        fileSize:     {type: integer}
        lastUpdated:  {type: string, format: date-time}

ErrorResponse:
  type: object
  oneOf:
    - required: [error]
      properties:
        error: {type: string}
    - required: [errors]
      properties:
        errors:
          type: array
          items:
            type: object
            properties:
              code: {type: string}
```

Error codes the pipeline must handle:
- **401** — bad/missing bearer token
- **404** — file not found (info endpoint or bulk download)
- **429** — rate limit exceeded → retry with backoff
- **500** — server error → propagate

---

## Changes required in `download.py`

1. `EPC_BULK_URL` — update to new endpoint
2. New `EPC_INFO_URL` constant for the info endpoint
3. `download_epc()` — replace Basic Auth with bearer token; read
   `EPC_BEARER_TOKEN` from env; remove `EPC_EMAIL`/`EPC_API_KEY`
4. New `_epc_last_updated()` helper — calls info endpoint, returns
   `lastUpdated` string
5. New `_check_epc_freshness()` — replaces `_check_freshness(epc_slim, EPC_BULK_URL)`
   for the EPC case; compares `lastUpdated` against stored meta
6. `_stream_to_file()` — add retry wrapper for 429 responses
7. `extract_epc()` — update ZIP traversal from `certificates.csv` scan to
   all-CSV scan (handles both old per-LA and new year-split formats)

## Changes required in `tests/test_download.py`

Replace `test_download_epc_uses_basic_auth` with bearer token test.
Replace `test_download_epc_raises_if_env_missing` for new env var name.
Add: info endpoint tests, freshness comparison via `lastUpdated`, 429 retry,
new ZIP structure extraction. Use `jsonschema` to validate mock responses
match OAS schemas.

## Changes required elsewhere

- `.env.example` — replace `EPC_EMAIL`/`EPC_API_KEY` with `EPC_BEARER_TOKEN`
- `data/SOURCES.md` — new URL, bearer auth, split licensing
- `README.md` — EPC setup instructions reference GOV.UK One Login
