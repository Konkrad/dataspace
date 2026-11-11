# Copernicus Data Space catalogue as GeoParquet

The product metadata of every Sentinel-1, -2, -3 and -5P product in the
[Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu/) (CDSE), as
GeoParquet: footprints, sensing times, orbits, product types, S3 paths,
checksums and **every** OData attribute. One file per collection and sensing
month, updated daily, public, no account needed.

**https://dataspace.konrad.online**

Query it in place with DuckDB, Python, QGIS or anything that reads Parquet
over HTTP, or download all of it. Only the parts of a file a query needs are
fetched.

```sql
ATTACH 'https://dataspace.konrad.online/dataspace.duckdb' AS ds (READ_ONLY);

-- Sentinel-1 GRD scenes over Berlin in May 2024
SELECT name, content_start, orbitDirection, s3_path
FROM ds.sentinel_1_grd
WHERE content_start >= '2024-05-01' AND content_start < '2024-06-01'
  AND bbox.xmin < 13.8 AND bbox.xmax > 13.0 AND bbox.ymin < 52.7 AND bbox.ymax > 52.3;
```

## Collections

| Mission | Satellites | Collections |
|---|---|---|
| Sentinel-1 | S1A, S1B, S1C, S1D | `grd`, `slc`, `ocn`, `etad`, `raw`, `aux`, `other` |
| Sentinel-2 | S2A, S2B, S2C | `l1c`, `l2a`, `aux`, `other` |
| Sentinel-3 | S3A, S3B | `olci`, `slstr`, `sral`, `synergy`, `mwr`, `aux`, `other` |
| Sentinel-5P | S5P | `l1b`, `l2`, `aux`, `other` |

Collections are grouped by `productType`: `grd` holds every GRD product type
(`IW_GRDH_1S`, `EW_GRDM_1S`, ...), `l2a` is `S2MSI2A`, `olci` is every `OL_*`
type, `l2` is every `L2__*` type, `aux` the auxiliary files (orbits,
calibration), `other` whatever fits nowhere else. The landing page lists what
exists, with months, file counts and sizes.

## Files

```
https://dataspace.konrad.online/<mission>-<collection>.parquet/<YYYY-MM>.parquet
https://dataspace.konrad.online/sentinel-2-l2a.parquet/2024-05.parquet
```

The month is the sensing month (`content_start`, UTC). Each file is
[GeoParquet](https://geoparquet.org/) 1.1 with a `bbox` covering column,
written in the [Cloud-Optimized GeoParquet](https://github.com/Kanahiro/cloud-optimized-geoparquet)
layout, so map viewers can load it progressively; to any other reader it is
ordinary GeoParquet.

Next to the data:

| File | What |
|---|---|
| [`/`](https://dataspace.konrad.online/) | landing page: every collection, with links to its months |
| `<mission>-<collection>.parquet/` | the months of one collection |
| [`dataspace.duckdb`](https://dataspace.konrad.online/dataspace.duckdb) | a DuckDB catalogue: a view per collection (`sentinel_1_grd`, `sentinel_2_l2a`, ...), plus the tables `datasets` and `files` |
| [`files.txt`](https://dataspace.konrad.online/files.txt) | every file's URL, one per line |
| [`files.tsv`](https://dataspace.konrad.online/files.tsv) | URL, size, ETag and last-modified of every file |

## Access

### DuckDB

```sql
ATTACH 'https://dataspace.konrad.online/dataspace.duckdb' AS ds (READ_ONLY);
SHOW ALL TABLES;
FROM ds.datasets;                          -- collections, months, sizes

-- Sentinel-2 L2A over an area, low cloud cover
SELECT name, content_start, cloudCover, s3_path
FROM ds.sentinel_2_l2a
WHERE content_start >= '2024-06-01' AND content_start < '2024-09-01'
  AND cloudCover < 20
  AND bbox.xmin < 13.8 AND bbox.xmax > 13.0 AND bbox.ymin < 52.7 AND bbox.ymax > 52.3;

-- one month directly, without the catalogue
SELECT count(*) FROM 'https://dataspace.konrad.online/sentinel-1-grd.parquet/2024-05.parquet';
```

Filter on `content_start` so only the matching months are read, and on
`bbox` so only the matching parts of them. To find a product by name, its
sensing date is in the name, so read that month:

```sql
FROM 'https://dataspace.konrad.online/sentinel-1-grd.parquet/2016-10.parquet'
WHERE name = 'S1A_IW_GRDH_1SDV_20161001T002657_20161001T002726_013289_0152C9_731D_COG.SAFE';
```

A local copy of a collection:

```sql
COPY (FROM ds.sentinel_1_grd WHERE content_start >= '2024-01-01')
TO 's1_grd_2024.parquet';
```

### Python

```python
import duckdb

con = duckdb.connect()
con.sql("ATTACH 'https://dataspace.konrad.online/dataspace.duckdb' AS ds (READ_ONLY)")
df = con.sql("FROM ds.sentinel_1_grd WHERE content_start >= '2024-05-01' LIMIT 1000").df()
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

## Columns

| Column(s) | Notes |
|---|---|
| `id`, `name` | the CDSE product id and name |
| `s3_path` | the product in CDSE's `s3://eodata/...` |
| `content_start`, `content_end` | sensing start and end, `timestamp with time zone`, UTC |
| `origin_date`, `publication_date`, `modification_date`, `eviction_date`, `ingestion_date` | |
| `content_type`, `content_length`, `online` | |
| `checksum_md5`, `checksum_blake3`, `quicklook_url` | |
| one column per OData attribute | e.g. Sentinel-1: `orbitNumber`, `relativeOrbitNumber`, `orbitDirection`, `productType`, `polarisationChannels`, `swathIdentifier`, `timeliness`; Sentinel-2: `cloudCover`, `tileId`, ... Types follow OData. Columns differ per mission and can differ between months. |
| `attributes_json`, `assets_json`, `locations_json`, `checksum_json` | the raw OData records, so nothing is lost |
| `odata_found` | `false` for products in the CDSE catalogue CSVs that OData doesn't return |
| `geometry` | the footprint, WKB, OGC:CRS84 (from `GeoFootprint`, else `Footprint`, else the CSV's bbox) |
| `bbox` | `xmin`, `ymin`, `xmax`, `ymax` of the footprint, for fast spatial filters |

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
  alone (name, S3 path, dates, size, md5, bbox as geometry), with
  `odata_found = false`.
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
