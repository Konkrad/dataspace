# Copernicus Data Space catalogue as GeoParquet

The product metadata of every Sentinel-1, -2, -3 and -5P product in the
[Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu/) (CDSE), as
[STAC GeoParquet](https://github.com/radiantearth/stac-geoparquet-spec) with
the item ids, collection ids and property names of CDSE's own
[STAC API](https://stac.dataspace.copernicus.eu/v1/): footprints, sensing
times, orbits, product types, S3 paths, checksums, and the raw OData record of
every product. One file per collection and sensing month, updated daily,
public, no account needed.

**https://dataspace.konrad.online**: a map of every collection and month,
the list of files, and how to use them.

Query it in place with DuckDB, Python, QGIS or anything that reads Parquet
over HTTP, or download all of it. Only the parts of a file a query needs are
fetched.

```sql
ATTACH 'https://dataspace.konrad.online/dataspace.duckdb' AS ds (READ_ONLY);

-- Sentinel-1 GRD scenes over Berlin in May 2024
SELECT id, datetime, "sat:orbit_state", assets.product.href
FROM ds.sentinel_1_grd
WHERE datetime >= '2024-05-01' AND datetime < '2024-06-01'
  AND bbox.xmin < 13.8 AND bbox.xmax > 13.0 AND bbox.ymin < 52.7 AND bbox.ymax > 52.3;
```

## Collections

| Mission | Satellites | Collections |
|---|---|---|
| Sentinel-1 | S1A, S1B, S1C, S1D | `grd`, `slc`, `ocn`, `etad`, `raw`, `aux`, `other` |
| Sentinel-2 | S2A, S2B, S2C | `l1c`, `l2a`, `aux`, `other` |
| Sentinel-3 | S3A, S3B | `olci`, `slstr`, `sral`, `synergy`, `mwr`, `aux`, `other` |
| Sentinel-5P | S5P | `l1b`, `l2`, `aux`, `other` |

Files are grouped by `product:type`: `grd` holds every GRD product type
(`IW_GRDH_1S`, `EW_GRDM_1S`, ...), `l2a` is `S2MSI2A`, `olci` is every `OL_*`
type, `l2` is every `L2__*` type, `aux` the auxiliary files (orbits,
calibration), `other` whatever fits nowhere else. Each row's `collection` is
CDSE's finer STAC collection (`sentinel-3-olci-2-lfr-ntc`,
`sentinel-5p-l2-no2-offl`, ...), or the file's own (`sentinel-1-raw`) for
product types CDSE has no STAC collection for. The landing page lists what
exists, with months, file counts and sizes.

## Files

```
https://dataspace.konrad.online/<mission>-<collection>.parquet/<YYYY-MM>.parquet
https://dataspace.konrad.online/sentinel-2-l2a.parquet/2024-05.parquet
```

The month is the sensing month (`datetime`, UTC). Each file is
[GeoParquet](https://geoparquet.org/) 1.1 with a `bbox` covering column,
written in the [Cloud-Optimized GeoParquet](https://github.com/Kanahiro/cloud-optimized-geoparquet)
layout, so map viewers can load it progressively; to any other reader it is
ordinary GeoParquet.

Next to the data:

| File | What |
|---|---|
| [`/`](https://dataspace.konrad.online/) | landing page: a map of the footprints per collection and month, every collection with links to its months |
| `<mission>-<collection>.parquet/` | the months of one collection |
| [`dataspace.duckdb`](https://dataspace.konrad.online/dataspace.duckdb) | a DuckDB catalogue: a view per collection (`sentinel_1_grd`, `sentinel_2_l2a`, ...), plus the tables `datasets` and `files` |
| [`files.txt`](https://dataspace.konrad.online/files.txt) | every file's URL, one per line |
| [`files.tsv`](https://dataspace.konrad.online/files.tsv) | URL, size, ETag and last-modified of every file |

## When to use it

CDSE's [OData](https://documentation.dataspace.copernicus.eu/APIs/OData.html)
and [STAC](https://stac.dataspace.copernicus.eu/v1/) APIs are built to find
products and download them. This dataset is built for questions about many
products at once.

Use this dataset for:

- **Statistics over the archive**: scenes per tile and year, cloud-free
  revisits, data volume per month, coverage gaps of a satellite. One SQL query
  instead of thousands of paged API requests.
- **Large result sets**: no paging limit (CDSE's APIs stop at 10,000 results
  per query), no rate limit, no account.
- **Comparisons and joins**: Sentinel-1 and Sentinel-2 over the same place
  within a day, products whose processing baseline changed, a stack of one
  relative orbit for InSAR.
- **Reproducible analyses**: a file only changes when the catalogue does, and
  its ETag tells you when.

Use CDSE's APIs for:

- **The newest products**: near-real-time products appear there within hours;
  here after the next daily update (see [Freshness](#freshness)).
- **Downloading products**: this dataset has their S3 paths, download links
  and checksums, but the data itself comes from CDSE, with your account.
- **What is read from inside the products**: per-file assets (`vv`, `vh`,
  the manifest, ...) and viewing angles (`view:*`) are not in the catalogue
  (see [What is left out](#what-is-left-out)).

### Online or downloaded?

Queries over HTTP fetch only the columns and row groups they need, not whole
files:

- **Online** suits questions over a few months: put them in the path
  (`2024-*.parquet`) so other files aren't opened, and select only the
  columns you need. Filters on area or cloud cover don't reduce what is
  fetched: every part of a file covers the whole world, ordered for map
  display rather than by place or value.
- **Download** for scans over many months or all years that you run more
  than once. Fetch only the collections you need, or only the columns (see
  [Download](#download)), then query locally in seconds.

## Access

### DuckDB

```sql
ATTACH 'https://dataspace.konrad.online/dataspace.duckdb' AS ds (READ_ONLY);
SHOW ALL TABLES;
FROM ds.datasets;                          -- collections, months, sizes

-- Sentinel-2 L2A over an area, low cloud cover
SELECT id, datetime, "eo:cloud_cover", "grid:code", assets.product.href
FROM ds.sentinel_2_l2a
WHERE datetime >= '2024-06-01' AND datetime < '2024-09-01'
  AND "eo:cloud_cover" < 20
  AND bbox.xmin < 13.8 AND bbox.xmax > 13.0 AND bbox.ymin < 52.7 AND bbox.ymax > 52.3;

-- one month directly, without the catalogue
SELECT count(*) FROM 'https://dataspace.konrad.online/sentinel-1-grd.parquet/2024-05.parquet';
```

Filter on `datetime` so only the matching months are read, and on `bbox` so
only the matching parts of them. Property names with a `:` need double quotes
in SQL. To find a product, its sensing date is in its id, so read that month:

```sql
FROM 'https://dataspace.konrad.online/sentinel-1-grd.parquet/2016-10.parquet'
WHERE id = 'S1A_IW_GRDH_1SDV_20161001T002657_20161001T002726_013289_0152C9_731D_COG';
```

A local copy of a collection:

```sql
COPY (FROM ds.sentinel_1_grd WHERE datetime >= '2024-01-01')
TO 's1_grd_2024.parquet';
```

### Python

```python
import duckdb

con = duckdb.connect()
con.sql("ATTACH 'https://dataspace.konrad.online/dataspace.duckdb' AS ds (READ_ONLY)")
df = con.sql("FROM ds.sentinel_1_grd WHERE datetime >= '2024-05-01' LIMIT 1000").df()
```

```python
import geopandas as gpd

gdf = gpd.read_parquet("https://dataspace.konrad.online/sentinel-1-grd.parquet/2024-05.parquet")
```

Several months without the catalogue, since the file names are predictable:

```python
import pandas as pd

base = "https://dataspace.konrad.online/sentinel-1-grd.parquet"
months = pd.period_range("2024-01", "2024-06", freq="M").strftime("%Y-%m")
duckdb.sql(f"SELECT count(*) FROM read_parquet({[f'{base}/{m}.parquet' for m in months]})")
```

### QGIS and GDAL

Layer → Add Layer → Add Vector Layer → Protocol: HTTP(S), with a file's URL. Or:

```sh
ogrinfo -so /vsicurl/https://dataspace.konrad.online/sentinel-1-grd.parquet/2024-05.parquet
```

### In the browser

The bucket allows cross-origin requests, so browser tools such as
[shell.duckdb.org](https://shell.duckdb.org/) can read the files directly.

### Download

Everything, or one collection, with [rclone](https://rclone.org/). Rerunning
fetches only what changed:

```sh
rclone sync --http-url https://dataspace.konrad.online :http: ./dataspace
rclone sync --http-url https://dataspace.konrad.online/sentinel-2-l2a.parquet/ :http: ./dataspace/sentinel-2-l2a.parquet
```

Or from the file list:

```sh
wget -x -nH -N -i https://dataspace.konrad.online/files.txt
aria2c -x 4 -j 4 --auto-file-renaming=false -i https://dataspace.konrad.online/files.txt
```

`files.tsv` has each file's ETag and last-modified, to find what changed
since a previous download.

Or only the columns you need, which is often a small part of a collection.
DuckDB reads just those columns over HTTP, once, and writes a local file:

```sql
COPY (
  SELECT id, datetime, "grid:code", "eo:cloud_cover", bbox
  FROM read_parquet('https://dataspace.konrad.online/sentinel-2-l2a.parquet/*.parquet')
) TO 'sentinel-2-l2a-light.parquet';

-- then, locally: cloud-free scenes per tile and year
SELECT "grid:code", year(datetime) AS year, count(*) AS scenes
FROM 'sentinel-2-l2a-light.parquet'
WHERE "eo:cloud_cover" < 10
GROUP BY ALL ORDER BY ALL;
```

Redo the `COPY` after the monthly update to pick up changed months.

## Columns

Each row is a STAC item, with the names and values CDSE's STAC API uses for
the same product. Checked value by value against CDSE items; the exceptions
are noted below.

| Column(s) | Notes |
|---|---|
| `type`, `stac_version`, `stac_extensions`, `links` | STAC item fields; `links` has a `via` link to the product in CDSE's OData API |
| `id` | the product name without `.SAFE` / `.SEN3` / `.nc`, as CDSE's item id |
| `collection` | CDSE's STAC collection id (see [Collections](#collections)) |
| `geometry`, `bbox` | the footprint (WKB, OGC:CRS84) and its `xmin`/`ymin`/`xmax`/`ymax`, for fast spatial filters |
| `datetime`, `start_datetime`, `end_datetime` | sensing start and end, UTC; `datetime` is the start |
| `created`, `published`, `updated`, `expires` | from OData's origin, publication, modification and eviction dates; `expires` is 2262-01-01 for products that don't expire, as in CDSE |
| `constellation`, `platform`, `instruments` | `sentinel-2`, `sentinel-2b`, `["msi"]` |
| `sat:*` | `sat:orbit_state`, `sat:absolute_orbit`, `sat:relative_orbit`, `sat:orbit_cycle`, `sat:platform_international_designator`, `sat:anx_datetime` (Sentinel-1) |
| `sar:*` | Sentinel-1: `sar:instrument_mode`, `sar:polarizations`, `sar:beam_ids`, `sar:frequency_band`, `sar:center_frequency`, `sar:observation_direction` |
| `eo:cloud_cover` | Sentinel-2 and -3 |
| `grid:code`, `gsd` | Sentinel-2: `MGRS-32UQD`, 10 |
| `product:type`, `product:timeliness`, `product:timeliness_category` | `IW_GRDH_1S`, `PT24H`, `Fast-24h` |
| `processing:level`, `processing:datetime`, `processing:facility`, `processing:version`, `processing:software` | `L1`, ...; `processing:software` is `{processor: version}` |
| `eopf:*` | `eopf:origin_datetime`; Sentinel-1 `eopf:datatake_id`, `eopf:instrument_configuration_id`; Sentinel-2 `eopf:datatake_id`, `eopf:datastrip_id`, `eopf:instrument_mode` |
| `assets` | `product`: the product in CDSE's `s3://eodata/...`, with `file:size`; `thumbnail`: the quicklook |
| `cdse:*` | what STAC has no place for: `cdse:id` (OData product id), `cdse:online`, `cdse:odata_found`, `cdse:ingestion_date`, `cdse:checksum_md5`, `cdse:checksum_blake3`, and the raw OData record as JSON: `cdse:attributes`, `cdse:assets`, `cdse:locations` |

Not included: properties CDSE reads from the product files rather than
OData (`view:*`, `sar:pixel_spacing_*`, `sar:resolution_*`, `eo:snow_cover`,
`gsd` outside Sentinel-2). Where CDSE's own items differ from OData, the
OData value is kept: CDSE's `processing:software` adds its indexing tool,
its Sentinel-1 GRD COG items describe the COG conversion rather than the
processing, and its Sentinel-5P L1B times differ from OData's.

## Freshness

CDSE regenerates its catalogue for the last 30 days every day, and for the
whole archive on the 2nd of each month. This dataset follows: daily at
06:00 UTC for the recent days, and in full after the 2nd. Changed months are
replaced in place under the same URL, so a file's ETag tells whether it
changed.

## What is left out

- **CARD products** of Sentinel-1 (`CARD_BS`, `CARD-COH12`): a prototype,
  not in the CDSE browser or OData. ETAD stays.
- **Products not in OData:** about 1–4% of the products the CDSE catalogue
  CSVs list are not returned by OData. They are kept, built from the CSV
  alone (id, S3 path, dates, size, md5, bbox as geometry), with
  `cdse:odata_found = false`.
- **CLMS** (Copernicus Land Monitoring Service) collections are not included.

## Sources and licence

Built from the CDSE catalogue CSVs at
[csv.dataspace.copernicus.eu](https://csv.dataspace.copernicus.eu/) and the
[CDSE OData API](https://documentation.dataspace.copernicus.eu/APIs/OData.html).
Copernicus Sentinel data and metadata are free and open under the
[Copernicus data policy](https://dataspace.copernicus.eu/terms-and-conditions);
please credit "Contains modified Copernicus Sentinel data [year]".

This is an independent project, not run by ESA or the European Commission.

## Contributing

How the files are built (the worker, the update and merge steps, the
schedule, the public index) is in [CONTRIBUTING.md](CONTRIBUTING.md).
