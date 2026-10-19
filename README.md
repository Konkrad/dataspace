# cdsemeta – Copernicus catalogue metadata as GeoParquet

A small worker that turns the daily Sentinel catalogue CSVs from
[csv.dataspace.copernicus.eu](https://csv.dataspace.copernicus.eu/) into
GeoParquet files with **all** OData metadata, and stores them in an OCI
registry (GHCR) with [ORAS](https://oras.land).

```
CSV listing ──► CSV ids ──► OData ──► GeoParquet ──► oras push ghcr.io/konkrad/dataspace/<mission>:<platform>_<day>
```

- One package per mission, e.g. `ghcr.io/konkrad/dataspace/sentinel-1`
  (see [Missions](#missions)).
- One GeoParquet per (satellite, sensing day), tagged `S1A_20240101`. The file
  inside keeps the CSV's name and folder structure: the CSV
  `S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20261001.csv` becomes
  `S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20261001.parquet`, so the
  file name says which catalogue generation it was built from (also in the
  `eu.copernicus.csv.generation` annotation). `oras pull` recreates that
  folder tree.
- **The tag is mutable.** CDSE regenerates the last 30 days every day at
  03:00 UTC and every CSV on the 2nd of each month, with the generation date
  in the file name. A changed day is updated in place under the same tag;
  only its added/modified rows are rebuilt (see [Updates](#updates)).
  Older artifacts were tagged with the full CSV name
  (`S1A_20240101_COPERNICUS_catalogue_20260901`); `cdsemeta-update retag`
  gives them their per-day tags, and the per-day tag always wins.
- **The registry is the state.** The worker lists all CSVs and the registry
  tags and builds whatever day is missing; `cdsemeta-update sync` finds
  changed days through the [ID files](#updates). There is no local database,
  so a crash or restart just continues.
  A file is only pushed after it has been fully built and checked, so there
  are no partial uploads.

## Missions

| `MISSION` | Satellites | Package |
|---|---|---|
| `sentinel-1` | S1A, S1B, S1C, S1D | `ghcr.io/konkrad/dataspace/sentinel-1` |
| `sentinel-2` | S2A, S2B, S2C | `ghcr.io/konkrad/dataspace/sentinel-2` |
| `sentinel-3` | S3A, S3B | `ghcr.io/konkrad/dataspace/sentinel-3` |
| `sentinel-5p` | S5P | `ghcr.io/konkrad/dataspace/sentinel-5p` |

All of these use the same CSV layout and header in the catalogue, so the code
is the same for each; only `MISSION` changes. Each mission runs as its own
container (see `docker-compose.yml`). OData attributes differ per mission
(`cloudCover` and `tileId` for Sentinel-2, for example), and they simply become
different columns. The table lives in `cdsemeta/config.py`.

The CLMS part of the catalogue (`bio-geophysical/…`, `landcover_landuse/…`)
has one CSV per collection rather than per day, and is not supported.

## Data sources

| What | Where |
|---|---|
| CSV files | `https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/<PLATFORM>/YYYY/MM/*.csv`. This is the public Swift container behind csv.dataspace.copernicus.eu; its JSON listing gives the md5 of each file. |
| All CSVs, zipped | `…/CatalogueCSV/CopernicusCatalogueCSV.zip` (13 GB, every mission, rebuilt daily ~03:00 UTC). Read with Range requests, so only the wanted CSVs are downloaded: S1 1.9 GB, S2 7.9 GB (vs 7.8 / 35.9 GB as single CSVs). |
| Metadata | `https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Id eq … or …&$expand=Attributes&$expand=Assets&$expand=Locations` |

For Sentinel-1, in September 2026 there were about 7,500 CSVs with roughly
2,500 products each. One CSV takes about 30 s with the default settings, so a
full run takes about 3 days. Sentinel-2 has many more products per day, so
expect it to take proportionally longer.

## Output columns

| Column(s) | Notes |
|---|---|
| `id`, `name`, `s3_path`, `content_type`, `content_length`, `online` | from OData |
| `content_start`, `content_end`, `origin_date`, `publication_date`, `modification_date`, `eviction_date` | `timestamp[us, UTC]` |
| `ingestion_date` | from the CSV |
| `checksum_md5`, `checksum_blake3`, `quicklook_url` | |
| one column per OData attribute | for Sentinel-1: `orbitNumber`, `relativeOrbitNumber`, `orbitDirection`, `productType`, `polarisationChannels`, `swathIdentifier`, `timeliness`, … Types follow the OData `ValueType`. |
| `attributes_json`, `assets_json`, `locations_json`, `checksum_json` | raw OData data, so nothing is lost |
| `odata_found` | `false` for products listed in the CSV but unknown to OData (see below) |
| `geometry` | WKB, OGC:CRS84, from `GeoFootprint` (falls back to `Footprint`, then the CSV `Bbox`) |
| `bbox` | GeoParquet 1.1 bbox covering column |

Rows are sorted by `content_start`.

**Products not found in OData.** About 1–4% of the CSV rows are not returned
by OData, whether you look them up by Id or by Name. For Sentinel-1, so far
these are all `CARD_BS` / `CARD-COH12` analysis-ready products. They are kept as rows built
from the CSV alone (name, S3 path, dates, size, md5, and the bbox polygon as
geometry) with `odata_found = false`. The count is recorded in the
`cdsemeta.odata_missing` manifest annotation.

## Running it

1. Create a GitHub classic personal access token with `write:packages`.
2. Set up the config:
   ```sh
   cp .env.example .env   # fill in GHCR_USER / GHCR_TOKEN
   ```
3. Build and check what is left to do:
   ```sh
   docker compose build
   docker compose run --rm sentinel-1 python -m cdsemeta.worker --dry-run
   ```
4. Start the worker:
   ```sh
   docker compose up -d
   docker compose logs -f sentinel-1
   ```

Useful one-offs:

```sh
# Process a single day into /work without pushing
docker compose run --rm sentinel-1 python -m cdsemeta.worker \
    --only S1A_20240101 --no-push --out /work/out

# See what is in the registry
oras repo tags ghcr.io/konkrad/dataspace/sentinel-1
oras pull ghcr.io/konkrad/dataspace/sentinel-1:S1A_20240101 -o s1
# -> s1/S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20261001.parquet
```

Everything goes into this repository's registry, one package per mission.
Every push carries the annotation
`org.opencontainers.image.source=https://github.com/konkrad/dataspace`, so GHCR
links each package to the repo: it shows up under the repo's **Packages**, and
access follows the repo. If a package already existed before the
first push and isn't linked, connect it once under Package settings →
"Connect repository".

### Settings (`.env`)

| Variable | Default | |
|---|---|---|
| `MISSION` | `sentinel-1` | set per service in `docker-compose.yml` |
| `OCI_BASE` | `ghcr.io/konkrad/dataspace` | the package is `${OCI_BASE}/${MISSION}` |
| `OCI_REPO` | | overrides the whole package name |
| `GHCR_USER`, `GHCR_TOKEN` | | used for `oras login` at container start |
| `PLATFORMS` | all of the mission's satellites | e.g. `S1C,S1D` to only do those |
| `ODATA_BATCH` | `100` | ids per request (200 fails with HTTP 414) |
| `ODATA_WORKERS` | `4` | parallel requests |
| `ODATA_RPS` | `3` | maximum requests started per second |
| `ORDER` | `asc` | `asc` = oldest day first, `desc` = newest first |
| `POLL_INTERVAL` | `21600` | seconds to sleep after each pass |
| `RUN_ONCE` | `0` | `1` = exit after one pass |
| `RECENT_DAYS` | `0` | only look at days sensed in the last N days (`0` = all) |
| `ORAS_EXTRA_ARGS` | | e.g. `--plain-http` for a local test registry |

### Errors

- HTTP 429 and 5xx responses, and connection errors, are retried with
  exponential backoff that respects `Retry-After`.
- If OData rejects a batch with some other 4xx error, the batch is split in
  half until the bad id is isolated, and that id is treated as not found.
- If a file still fails, the error is logged and the worker moves on. The day
  is still missing, so the next pass retries it.
- On SIGTERM (`docker compose stop`) the worker stops right away. The file it
  was working on is not pushed and gets redone later.

## Monthly files

The merge writes one file per collection and sensing month,
`<mission>_<collection>/<YYYY>-<MM>.parquet` (tag `<collection>-<YYYY>-<MM>`),
so an update only rewrites the months that changed. Existing files are
skipped except the current and previous month; use `--year`, `--month` or
`--force` to rebuild others. Once everything is in the registry:

```sh
cdsemeta-merge --out-dir out --cache cache [--push] [--upload-r2]
```

This does five things:

1. Pulls the newest version of every (platform, day) into
   `/work/sentinel-1/cache/S1X/YYYY/MM/`.
   Already-pulled files are skipped, so it can be rerun.
2. Merges them with DuckDB (`union_by_name`, so attribute columns that only
   some product types have are fine).
3. Sorts the result by `content_start`.
4. Writes GeoParquet metadata.
5. Reorders it into a [COGP](https://github.com/Kanahiro/cloud-optimized-geoparquet)
   layout with the `cogp` CLI (vendored in the image like `oras`), so viewers
   can progressively load it, then pushes the file as tag `all-YYYYMMDD`. A
   COGP file is still ordinary GeoParquet, so this is transparent to
   non-COGP-aware readers.

Use `--memory-limit 4GB` on small machines.

## Updates

CDSE regenerates every CSV on the 2nd of each month, and the last 30 days
daily. A regeneration changes few rows of most days. Measured for
Sentinel-1, Sept artifacts vs the Oct dump: 2014–2025 changed on 5,590 of
~7,100 days, by 52k added and 22k modified rows out of 13.5M, almost all
CSV-only `CARD_BS`; the whole archive up to 2025 needed 10 OData lookups.
Removals are mostly the temporary orbit files (`AUX_RESORB`/`PREORB`/
`MOEORB`), which CDSE deletes ~3–4 months after the sensing day. So a
changed day is updated per product, not rebuilt.

**ID files.** Next to the per-day data, one small Parquet per platform and
year, `<mission>_ids/<PLATFORM>/<year>.parquet` (tag `ids-<year>-<s1>-<s1a>`,
e.g. `ids-2024-s1-s1a`), with three columns:

| Column | From | |
|---|---|---|
| `Name` | the CSV | the product name: unique like `Id`, and smaller |
| `ModificationDate` | the CSV | a changed product gets a new one (true for all 21,866 modified rows in Sept→Oct) |
| `day` | the CSV's file name | needed to find the day file of a removed product; the name doesn't say it reliably (AUX files carry their creation date first, S5P orbits crossing midnight the previous day) |

About 15 MB per platform-year for Sentinel-1 (S1A 2024: 1.27M rows).

**`cdsemeta-update sync`**, per platform and year:

1. stream that year's CSVs out of the catalogue zip;
2. diff their `(Name, ModificationDate, day)` against the ID file: a day
   with any added, removed or modified name is looked at;
3. pull the day's artifact and compare it with the CSV by name: drop names
   the CSV no longer lists, rebuild added names and names with a new
   `ModificationDate`, keep every other row as it is. OData is only asked
   about rows that can be in it (not `CARD_BS`/`CARD-COH12`, not rows that
   were already CSV-only), all of a chunk of 200 days together;
4. push the day to its per-day tag; once every day of the platform-year went
   through, save and push the new ID file. If a day failed, the ID file is
   left as it was, so a rerun looks at the same days again.

Days without an artifact are left to the worker. The first run (no ID files
yet) compares every day's artifact with its CSV; S1A 2024 took under a
minute with local artifacts.

```sh
cdsemeta-update sync                      # monthly: everything
cdsemeta-update sync --recent-days 31     # daily: only the regenerated recent days
cdsemeta-update sync --year 2024 --artifacts cache --ids ids --out out --no-push   # local dry run
```

If CDSE rewrites the zip during a run (every read is pinned to its ETag),
the run stops with `ZipChanged`; just start it again.

### Moving to per-day tags

Needs `oras` logged in to GHCR (e.g. inside the image, with `GHCR_TOKEN`).

- **The worker** counts a day as done if it has *any* tag, per-day or legacy
  of whatever generation, so a full dump is no new work for it. The worker
  deployed before this change still compares exact file names: redeploy it
  **before the 2nd**, or it starts over.
- **Retag** only adds tags (no upload, no CDSE calls), so it is safe while a
  backfill runs. Days pushed by an old worker afterwards get theirs on the
  next `retag`.
- **First ID files**: `init-ids` takes a day from the zip only if its
  artifact was built from exactly that CSV (a legacy tag with the same
  generation). Sentinel-1's artifacts are from the Sept dump, whose CSVs are
  gone, so it gets no ID files up front: its first `sync` compares every
  day's artifact with the current CSV (one pull per day, ~9 GB).

```sh
MISSION=sentinel-1 cdsemeta-update retag --dry-run && MISSION=sentinel-1 cdsemeta-update retag
MISSION=sentinel-2 cdsemeta-update init-ids     # while the Oct CSVs are still current
MISSION=sentinel-1 cdsemeta-update sync         # first run: S1 up to the Oct dump, writes its ID files
MISSION=sentinel-2 cdsemeta-update retag        # once the S2 backfill is done
```

## STAC GeoParquet

```sh
docker compose run --rm sentinel-1-merge cdsemeta-stac /work/sentinel-1/sentinel-1_all.parquet \
    --out /work/sentinel-1/sentinel-1_stac.parquet --push
```

Maps cdsemeta's raw OData attribute columns (`orbitNumber`, `orbitDirection`,
`polarisationChannels`, `productType`, ...) into the STAC properties CDSE's
own STAC API uses (https://stac.dataspace.copernicus.eu/), producing a
[STAC GeoParquet](https://github.com/radiantearth/stac-geoparquet-spec) file:
the `sar`/`sat`/`product`/`processing`/`eopf` STAC extensions, `id`/
`datetime`/`collection`/`links`/`assets`. Each `assets.data.href` is the
product's `s3://eodata/...` directory (cdsemeta only has the product-level S3
path, not individual measurement files). Only mapped when the mission's
attribute columns are actually present, so it degrades gracefully for
missions other than Sentinel-1 — the mapping itself has only been verified
against Sentinel-1.

Like the merged file, this is run through `cogp convert` (rerun on this file
specifically, since adding columns after a COGP conversion would leave its
`geo.lod` metadata pointing at stale row groups) and pushed as its own tag,
`stac-YYYYMMDD`.

## Development

```sh
pip install -e '.[test]'
pytest
```
