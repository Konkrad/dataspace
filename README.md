# s1meta – Sentinel-1 catalogue metadata as GeoParquet

A small worker that turns the daily Sentinel-1 catalogue CSVs from
[csv.dataspace.copernicus.eu](https://csv.dataspace.copernicus.eu/Sentinel-1/) into
GeoParquet files with **all** OData metadata, and stores them in an OCI
registry (GHCR) with [ORAS](https://oras.land).

```
CSV listing ──► CSV ids ──► OData (100 ids/request) ──► GeoParquet ──► oras push ghcr.io/…:<csv name>
```

- One GeoParquet per CSV, with the same name:
  `S1A_20240101_COPERNICUS_catalogue_20260901.csv` becomes tag
  `S1A_20240101_COPERNICUS_catalogue_20260901` containing
  `S1A_20240101_COPERNICUS_catalogue_20260901.parquet`.
- **The registry is the state.** Each pass lists all CSVs, lists the registry
  tags, and processes whatever is missing. There is no local database, so a
  crash or restart just continues. A file is only pushed after it has been
  fully built and checked, so there are no partial uploads.
- CDSE regenerates recent days. A regenerated day gets a new catalogue date in
  its file name, so it shows up as new work. The merge step only uses the
  newest version of each day.
- When everything is done, the worker sleeps (`POLL_INTERVAL`, 6 h by default)
  and then picks up new daily CSVs.

## Data sources

| What | Where |
|---|---|
| CSV files | `https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/S1{A,B,C,D}/YYYY/MM/*.csv`. This is the public Swift container behind csv.dataspace.copernicus.eu; its JSON listing gives the md5 of each file. |
| Metadata | `https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Id eq … or …&$expand=Attributes&$expand=Assets&$expand=Locations` |

In September 2026 there were about 7,500 CSVs with roughly 2,500 products each.
One CSV takes about 30 s with the default settings, so a full run takes about
3 days.

## Output columns

| Column(s) | Notes |
|---|---|
| `id`, `name`, `s3_path`, `content_type`, `content_length`, `online` | from OData |
| `content_start`, `content_end`, `origin_date`, `publication_date`, `modification_date`, `eviction_date` | `timestamp[us, UTC]` |
| `ingestion_date` | from the CSV |
| `checksum_md5`, `checksum_blake3`, `quicklook_url` | |
| one column per OData attribute | `orbitNumber`, `relativeOrbitNumber`, `orbitDirection`, `productType`, `polarisationChannels`, `swathIdentifier`, `timeliness`, … Types follow the OData `ValueType`. |
| `attributes_json`, `assets_json`, `locations_json`, `checksum_json` | raw OData data, so nothing is lost |
| `odata_found` | `false` for products listed in the CSV but unknown to OData (see below) |
| `geometry` | WKB, OGC:CRS84, from `GeoFootprint` (falls back to `Footprint`, then the CSV `Bbox`) |
| `bbox` | GeoParquet 1.1 bbox covering column |

Rows are sorted by `content_start`.

**Products not found in OData.** About 1–4% of the CSV rows are not returned
by OData, whether you look them up by Id or by Name. So far these are all
`CARD_BS` / `CARD-COH12` analysis-ready products. They are kept as rows built
from the CSV alone (name, S3 path, dates, size, md5, and the bbox polygon as
geometry) with `odata_found = false`. The count is recorded in the
`s1meta.odata_missing` manifest annotation.

## Running it

1. Create a GitHub classic personal access token with `write:packages`.
2. Set up the config:
   ```sh
   cp .env.example .env   # fill in GHCR_USER / GHCR_TOKEN / OCI_REPO
   ```
3. Build and check what is left to do:
   ```sh
   docker compose build
   docker compose run --rm worker python -m s1meta.worker --dry-run
   ```
4. Start the worker:
   ```sh
   docker compose up -d
   docker compose logs -f worker
   ```

Useful one-offs:

```sh
# Process a single CSV into /work without pushing
docker compose run --rm worker python -m s1meta.worker \
    --only S1A_20240101_COPERNICUS_catalogue_20260901 --no-push --out /work/out

# See what is in the registry
oras repo tags ghcr.io/konkrad/dataspace
oras pull ghcr.io/konkrad/dataspace:S1A_20240101_COPERNICUS_catalogue_20260901
```

Everything goes into this repository's own package, `ghcr.io/konkrad/dataspace`.
Every push carries the annotation
`org.opencontainers.image.source=https://github.com/konkrad/dataspace`, so GHCR
links the package to the repo: it shows up under the repo's **Packages**, and
access follows the repo. If a `dataspace` package already existed before the
first push and isn't linked, connect it once under Package settings →
"Connect repository".

### Settings (`.env`)

| Variable | Default | |
|---|---|---|
| `OCI_REPO` | `ghcr.io/konkrad/dataspace` | target repository (this repo's package) |
| `GHCR_USER`, `GHCR_TOKEN` | | used for `oras login` at container start |
| `PLATFORMS` | `S1A,S1B,S1C,S1D` | |
| `ODATA_BATCH` | `100` | ids per request (200 fails with HTTP 414) |
| `ODATA_WORKERS` | `4` | parallel requests |
| `ODATA_RPS` | `3` | maximum requests started per second |
| `ORDER` | `asc` | `asc` = oldest day first, `desc` = newest first |
| `POLL_INTERVAL` | `21600` | seconds to sleep after each pass |
| `RUN_ONCE` | `0` | `1` = exit after one pass |
| `ORAS_EXTRA_ARGS` | | e.g. `--plain-http` for a local test registry |

### Errors

- HTTP 429 and 5xx responses, and connection errors, are retried with
  exponential backoff that respects `Retry-After`.
- If OData rejects a batch with some other 4xx error, the batch is split in
  half until the bad id is isolated, and that id is treated as not found.
- If a file still fails, the error is logged and the worker moves on. The file
  is still missing from the registry, so the next pass retries it.
- On SIGTERM (`docker compose stop`) the worker stops right away. The file it
  was working on is not pushed and gets redone later.

## One big file

Once everything is in the registry:

```sh
docker compose run --rm merge
# which runs: python -m s1meta.merge --out /work/s1_all.parquet --cache /work/cache --push
```

This does four things:

1. Pulls the newest version of every (platform, day) into `/work/cache`.
   Already-pulled files are skipped, so it can be rerun.
2. Merges them with DuckDB (`union_by_name`, so attribute columns that only
   some product types have are fine).
3. Sorts the result by `content_start`.
4. Writes GeoParquet metadata and pushes the file as tag `all-YYYYMMDD`.

Use `--memory-limit 4GB` on small machines.

## Development

```sh
pip install -e '.[test]'
pytest
```
