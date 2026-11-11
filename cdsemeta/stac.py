"""STAC GeoParquet with the property names and values of CDSE's own STAC API.

The merged files hold the raw OData attributes (``orbitNumber``,
``orbitDirection``, ``polarisationChannels``, ...). This maps them, as one
DuckDB ``SELECT`` per file, to the STAC properties
https://stac.dataspace.copernicus.eu/v1/ uses for the same products, following
https://github.com/radiantearth/stac-geoparquet-spec:

- ``id`` is the product name without ``.SAFE``/``.SEN3``/``.nc``, as CDSE's
  item id; ``collection`` is CDSE's (fine-grained) collection id, derived from
  the product type and timeliness (``sentinel-3-olci-2-lfr-ntc``), or our own
  ``<mission>-<bucket>`` for product types CDSE has no STAC collection for
  (Sentinel-1 RAW, auxiliary files, some Sentinel-5P products).
- ``sat:``, ``sar:``, ``eo:``, ``grid:``, ``product:``, ``processing:`` and
  ``eopf:`` properties, ``created``/``published``/``updated``/``expires``.
  Properties CDSE derives from the product files themselves (``view:*``,
  ``sar:pixel_spacing_*``, ``gsd`` outside Sentinel-2) are not in OData and
  are left out.
- An asset ``product`` (the product's ``s3://eodata/...`` path, size) and
  ``thumbnail`` (the quicklook), a ``via`` link to the OData product.
- What STAC has no place for stays under ``cdse:`` (OData id, online, the raw
  attribute/asset/location JSON), so nothing is lost.

Checked value by value against CDSE STAC items of the same products (see
tests/test_stac.py). Rows CDSE's OData didn't return (``cdse:odata_found =
false``) are kept, with only what the catalogue CSV has.

The result has to go through ``cogp convert`` again (``cdsemeta.cogp``):
COGP's ``geo.lod`` metadata describes row groups, which change on rewrite.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from . import __version__, r2
from .cogp import convert_to_cogp

log = logging.getLogger("cdsemeta.stac")

STAC_VERSION = "1.1.0"
STAC_GEOPARQUET_VERSION = "1.1.0"

EXTENSIONS = {
    "sat": "https://stac-extensions.github.io/sat/v1.1.0/schema.json",
    "sar": "https://stac-extensions.github.io/sar/v1.3.0/schema.json",
    "eo": "https://stac-extensions.github.io/eo/v2.0.0/schema.json",
    "grid": "https://stac-extensions.github.io/grid/v1.1.0/schema.json",
    "product": "https://stac-extensions.github.io/product/v1.0.0/schema.json",
    "processing": "https://stac-extensions.github.io/processing/v1.2.0/schema.json",
    "eopf": "https://cs-si.github.io/eopf-stac-extension/v1.2.0/schema.json",
    "file": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
    "timestamps": "https://stac-extensions.github.io/timestamps/v1.1.0/schema.json",
}
MISSION_EXTENSIONS = {
    "sentinel-1": ("sat", "sar", "product", "processing", "eopf", "file", "timestamps"),
    "sentinel-2": ("sat", "eo", "grid", "product", "processing", "eopf", "file", "timestamps"),
    "sentinel-3": ("sat", "eo", "product", "processing", "eopf", "file", "timestamps"),
    "sentinel-5p": ("sat", "product", "processing", "eopf", "file", "timestamps"),
}

# COSPAR ids, as CDSE's sat:platform_international_designator (checked per satellite).
DESIGNATORS = {
    "sentinel-1a": "2014-016A", "sentinel-1b": "2016-025A", "sentinel-1c": "2024-235A", "sentinel-1d": "2025-251A",
    "sentinel-2a": "2015-028A", "sentinel-2b": "2017-013A", "sentinel-2c": "2024-157A",
    "sentinel-3a": "2016-011A", "sentinel-3b": "2018-039A",
    "sentinel-5p": "2017-064A",
}

# CDSE's Sentinel STAC collections that a product type can map to. A derived
# id outside this set falls back to our own bucket id.
CDSE_COLLECTIONS = frozenset("""
sentinel-1-etad sentinel-1-grd sentinel-1-ocn sentinel-1-ocn-wv sentinel-1-slc sentinel-1-slc-wv
sentinel-2-l1c sentinel-2-l2a
sentinel-3-olci-1-efr-nrt sentinel-3-olci-1-efr-ntc sentinel-3-olci-1-err-nrt sentinel-3-olci-1-err-ntc
sentinel-3-olci-2-lfr-nrt sentinel-3-olci-2-lfr-ntc sentinel-3-olci-2-lrr-nrt sentinel-3-olci-2-lrr-ntc
sentinel-3-olci-2-wfr-nrt sentinel-3-olci-2-wfr-ntc sentinel-3-olci-2-wrr-nrt sentinel-3-olci-2-wrr-ntc
sentinel-3-sl-1-rbt-nrt sentinel-3-sl-1-rbt-ntc sentinel-3-sl-2-aod-nrt sentinel-3-sl-2-frp-nrt sentinel-3-sl-2-frp-ntc
sentinel-3-sl-2-lst-nrt sentinel-3-sl-2-lst-ntc sentinel-3-sl-2-wst-nrt sentinel-3-sl-2-wst-ntc
sentinel-3-sr-1-sra-a-nrt sentinel-3-sr-1-sra-a-ntc sentinel-3-sr-1-sra-a-stc sentinel-3-sr-1-sra-ntc
sentinel-3-sr-2-lan-hy-nrt sentinel-3-sr-2-lan-hy-ntc sentinel-3-sr-2-lan-hy-stc
sentinel-3-sr-2-lan-li-nrt sentinel-3-sr-2-lan-li-ntc sentinel-3-sr-2-lan-li-stc sentinel-3-sr-2-lan-ntc
sentinel-3-sr-2-lan-si-nrt sentinel-3-sr-2-lan-si-ntc sentinel-3-sr-2-lan-si-stc
sentinel-3-sr-2-wat-nrt sentinel-3-sr-2-wat-ntc sentinel-3-sr-2-wat-stc
sentinel-3-syn-2-aod-ntc sentinel-3-syn-2-syn-ntc sentinel-3-syn-2-syn-stc sentinel-3-syn-2-v10-ntc
sentinel-3-syn-2-v10-stc sentinel-3-syn-2-vg1-ntc sentinel-3-syn-2-vg1-stc sentinel-3-syn-2-vgp-ntc sentinel-3-syn-2-vgp-stc
sentinel-5p-l1-ra-bd1-nrti sentinel-5p-l1-ra-bd1-offl sentinel-5p-l1-ra-bd1-rpro sentinel-5p-l1-ra-bd2-nrti
sentinel-5p-l1-ra-bd2-offl sentinel-5p-l1-ra-bd2-rpro sentinel-5p-l1-ra-bd3-nrti sentinel-5p-l1-ra-bd3-offl
sentinel-5p-l1-ra-bd3-rpro sentinel-5p-l1-ra-bd4-nrti sentinel-5p-l1-ra-bd4-offl sentinel-5p-l1-ra-bd4-rpro
sentinel-5p-l1-ra-bd5-nrti sentinel-5p-l1-ra-bd5-offl sentinel-5p-l1-ra-bd5-rpro sentinel-5p-l1-ra-bd6-nrti
sentinel-5p-l1-ra-bd6-offl sentinel-5p-l1-ra-bd6-rpro sentinel-5p-l1-ra-bd7-nrti sentinel-5p-l1-ra-bd7-offl
sentinel-5p-l1-ra-bd7-rpro sentinel-5p-l1-ra-bd8-nrti sentinel-5p-l1-ra-bd8-offl sentinel-5p-l1-ra-bd8-rpro
sentinel-5p-l2-aer-ai-nrti sentinel-5p-l2-aer-ai-offl sentinel-5p-l2-aer-ai-rpro sentinel-5p-l2-aer-lh-nrti
sentinel-5p-l2-aer-lh-offl sentinel-5p-l2-aer-lh-rpro sentinel-5p-l2-ch4-nrti sentinel-5p-l2-ch4-offl
sentinel-5p-l2-ch4-rpro sentinel-5p-l2-cloud-nrti sentinel-5p-l2-cloud-offl sentinel-5p-l2-cloud-rpro
sentinel-5p-l2-co-nrti sentinel-5p-l2-co-offl sentinel-5p-l2-co-rpro sentinel-5p-l2-hcho-nrti sentinel-5p-l2-hcho-offl
sentinel-5p-l2-hcho-rpro sentinel-5p-l2-no2-nrti sentinel-5p-l2-no2-offl sentinel-5p-l2-no2-rpro
sentinel-5p-l2-np-bd3-offl sentinel-5p-l2-np-bd3-rpro sentinel-5p-l2-np-bd6-offl sentinel-5p-l2-np-bd6-rpro
sentinel-5p-l2-np-bd7-offl sentinel-5p-l2-np-bd7-rpro sentinel-5p-l2-o3-nrti sentinel-5p-l2-o3-offl
sentinel-5p-l2-o3-pr-nrti sentinel-5p-l2-o3-pr-offl sentinel-5p-l2-o3-pr-rpro sentinel-5p-l2-o3-rpro
sentinel-5p-l2-o3-tcl-nrti sentinel-5p-l2-o3-tcl-offl sentinel-5p-l2-o3-tcl-rpro sentinel-5p-l2-so2-nrti
sentinel-5p-l2-so2-offl sentinel-5p-l2-so2-rpro
""".split())

# Columns of the merged files that become STAC fields or cdse:* and aren't copied as they are.
RAW_ONLY = {"overviews"}  # COGP's, rebuilt by cogp convert


def _q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _in_list(values) -> str:
    return "[" + ", ".join(_q(v) for v in sorted(values)) + "]"


def select_sql(columns: set[str], mission: str, bucket_id: str) -> tuple[str, list[str]]:
    """The SELECT list mapping one merged file to STAC, and the extensions it uses.

    ``columns`` are the input's columns (they differ per mission and month);
    a missing source column gives NULL.
    """
    def c(name: str, cast: str = "VARCHAR") -> str:
        return f'"{name}"' if name in columns else f"NULL::{cast}"

    pt = c("productType")
    platform = f"lower({c('platformShortName')}) || lower(coalesce({c('platformSerialIdentifier')}, ''))"
    designators = "MAP {" + ", ".join(f"{_q(k)}: {_q(v)}" for k, v in DESIGNATORS.items()) + "}"
    rest = "lower(regexp_replace(trim({x}, '_'), '_+', '-', 'g'))"

    if mission == "sentinel-1":
        derived = f"""CASE
            WHEN {pt} LIKE '%GRD%' THEN 'sentinel-1-grd'
            WHEN {pt} LIKE 'WV_SLC%' THEN 'sentinel-1-slc-wv'
            WHEN {pt} LIKE '%SLC%' THEN 'sentinel-1-slc'
            WHEN {pt} LIKE 'WV_OCN%' THEN 'sentinel-1-ocn-wv'
            WHEN {pt} LIKE '%OCN%' THEN 'sentinel-1-ocn'
            WHEN {pt} LIKE '%ETA%' THEN 'sentinel-1-etad' END"""
        level = f"CASE WHEN {pt} LIKE '%ETA%' THEN 'L1' ELSE 'L' || nullif(regexp_extract({pt}, '_(\\d)S', 1), '') END"
        timeliness = {"NRT-3h": "PT3H", "Fast-24h": "PT24H"}
        tl_dur = "CASE " + " ".join(f"WHEN {c('timeliness')} = {_q(k)} THEN {_q(v)}" for k, v in timeliness.items()) + " END"
        tl_cat = f"CASE WHEN {c('timeliness')} IN {tuple(timeliness)} THEN {c('timeliness')} END"
        instruments = "['sar']"
    elif mission == "sentinel-2":
        derived = f"CASE {pt} WHEN 'S2MSI1C' THEN 'sentinel-2-l1c' WHEN 'S2MSI2A' THEN 'sentinel-2-l2a' END"
        level = f"'L' || nullif(regexp_extract({pt}, 'MSI(\\d)', 1), '')"
        tl_dur = f"CASE WHEN {pt} LIKE 'S2MSI%' THEN 'PT24H' END"
        tl_cat = f"CASE WHEN {pt} LIKE 'S2MSI%' THEN 'NRT' END"
        instruments = "['msi']"
    elif mission == "sentinel-3":
        m = f"regexp_extract({pt}, '^(OL|SL|SR|SY)_(\\d)_(.+)$', ['ins', 'lvl', 'rest'])"
        suffix = f"CASE {c('timeliness')} WHEN 'NR' THEN 'nrt' WHEN 'NT' THEN 'ntc' WHEN 'ST' THEN 'stc' END"
        ins = f"CASE {m}.ins WHEN 'OL' THEN 'olci' WHEN 'SL' THEN 'sl' WHEN 'SR' THEN 'sr' WHEN 'SY' THEN 'syn' END"
        derived = (f"'sentinel-3-' || {ins} || '-' || {m}.lvl || '-' || "
                   f"{rest.format(x=f'{m}.rest')} || '-' || {suffix}")
        level = f"'L' || nullif(regexp_extract({pt}, '^[A-Z]{{2}}_(\\d)_', 1), '')"
        tl_dur = f"CASE {c('timeliness')} WHEN 'NR' THEN 'PT3H' WHEN 'NT' THEN 'P1M' WHEN 'ST' THEN 'PT48H' END"
        tl_cat = f"CASE WHEN {c('timeliness')} IN ('NR', 'NT', 'ST') THEN {c('timeliness')} END"
        instruments = (f"CASE WHEN {pt} LIKE 'SY\\_%' ESCAPE '\\' THEN ['olci', 'slstr'] "
                       f"ELSE [lower({c('instrumentShortName')})] END")
    elif mission == "sentinel-5p":
        m = f"regexp_extract({pt}, '^(L1B|L2_)_(.+)$', ['lvl', 'rest'])"
        derived = (f"'sentinel-5p-' || CASE {m}.lvl WHEN 'L1B' THEN 'l1' ELSE 'l2' END || '-' || "
                   f"{rest.format(x=f'{m}.rest')} || '-' || lower({c('processingMode')})")
        level = f"'L' || nullif(regexp_extract({pt}, '^L(\\d)', 1), '')"
        tl_dur = f"CASE {c('processingMode')} WHEN 'OFFL' THEN 'PT12H' WHEN 'NRTI' THEN 'PT3H' END"
        tl_cat = f"CASE WHEN {c('processingMode')} IN ('OFFL', 'NRTI') THEN {c('processingMode')} END"
        instruments = "['tropomi']"
    else:
        raise ValueError(f"no STAC mapping for {mission}")

    collection = (f"CASE WHEN ({derived}) IN (SELECT unnest({_in_list(CDSE_COLLECTIONS)})) "
                  f"THEN ({derived}) ELSE {_q(bucket_id)} END")
    version = c("processorVersion")
    if mission == "sentinel-1":  # 003.71 -> 03.71, as CDSE
        version = f"regexp_replace({version}, '^0(\\d\\d\\.)', '\\1')"
    if mission == "sentinel-5p":  # 020901 -> 02.09.01, as CDSE
        version = (f"CASE WHEN regexp_matches({version}, '^\\d{{6}}$') THEN substr({version}, 1, 2) || '.' || "
                   f"substr({version}, 3, 2) || '.' || substr({version}, 5, 2) ELSE {version} END")
    s3 = c("s3_path")
    href = (f"CASE WHEN {s3} LIKE 's3://%' THEN {s3} WHEN {s3} LIKE '/%' THEN 's3:/' || {s3} "
            f"WHEN {s3} IS NOT NULL THEN 's3://' || {s3} END")
    beam_ids = (f"CASE WHEN {pt} LIKE '%GRD%' OR {pt} LIKE '%RAW%' OR {c('swathIdentifier')} IS NULL THEN NULL "
                f"WHEN NOT regexp_matches({c('swathIdentifier')}, '\\d') THEN NULL "
                f"ELSE list_transform(string_split_regex(trim({c('swathIdentifier')}), '[ ,]+'), x -> lower(x)) END")
    facility = f"coalesce({c('processingCenter')}, {c('origin')})" if mission == "sentinel-2" else c("processingCenter")
    is_s1 = mission == "sentinel-1"
    is_s2 = mission == "sentinel-2"

    fields = [
        "'Feature' AS type",
        f"{_q(STAC_VERSION)} AS stac_version",
        f"{_in_list(EXTENSIONS[e] for e in MISSION_EXTENSIONS[mission])} AS stac_extensions",
        f"regexp_replace({c('name')}, '\\.(SAFE|SEN3|nc|NC|zip|EOF|TGZ|tgz)$', '') AS id",
        f"{collection} AS collection",
        "geometry",
        "bbox",
        f"{c('content_start', 'TIMESTAMPTZ')} AS datetime",
        f"{c('content_start', 'TIMESTAMPTZ')} AS start_datetime",
        f"{c('content_end', 'TIMESTAMPTZ')} AS end_datetime",
        f"{c('origin_date', 'TIMESTAMPTZ')} AS created",
        f"{c('publication_date', 'TIMESTAMPTZ')} AS published",
        f"{c('modification_date', 'TIMESTAMPTZ')} AS updated",
        # 9999-12-31 means "never"; CDSE writes 2262-01-01 (the latest date pandas can hold).
        f"CASE WHEN year({c('eviction_date', 'TIMESTAMPTZ')}) >= 2262 THEN TIMESTAMPTZ '2262-01-01 00:00:00+00' "
        f"ELSE {c('eviction_date', 'TIMESTAMPTZ')} END AS expires",
        f"lower({c('platformShortName')}) AS constellation",
        f"nullif({platform}, '') AS platform",
        f"{instruments} AS instruments",
        f"lower({c('orbitDirection')}) AS \"sat:orbit_state\"",
        f"{c('orbitNumber', 'BIGINT')} AS \"sat:absolute_orbit\"",
        f"{c('relativeOrbitNumber', 'BIGINT')} AS \"sat:relative_orbit\"",
        f"{c('cycleNumber', 'BIGINT')} AS \"sat:orbit_cycle\"",
        f"{designators}[{platform}] AS \"sat:platform_international_designator\"",
        f"regexp_replace({pt}, '-COG$', '') AS \"product:type\"",
        f"{tl_dur} AS \"product:timeliness\"",
        f"{tl_cat} AS \"product:timeliness_category\"",
        f"{level} AS \"processing:level\"",
        f"{c('processingDate', 'TIMESTAMPTZ')} AS \"processing:datetime\"",
        f"nullif(nullif({facility}, '[]'), '') AS \"processing:facility\"",
        f"{version} AS \"processing:version\"",
        f"CASE WHEN {c('processorName')} IS NOT NULL AND {c('processorVersion')} IS NOT NULL "
        f"THEN MAP([{c('processorName')}], [{version}]) END AS \"processing:software\"",
        f"{c('origin_date', 'TIMESTAMPTZ')} AS \"eopf:origin_datetime\"",
    ]
    if is_s1:
        mode = c("operationalMode")
        fields += [
            f"{mode} AS \"sar:instrument_mode\"",
            f"string_split({c('polarisationChannels')}, '&') AS \"sar:polarizations\"",
            f"CASE WHEN {mode} IS NOT NULL THEN 'C' END AS \"sar:frequency_band\"",
            f"CASE WHEN {mode} IS NOT NULL THEN 5.405::DOUBLE END AS \"sar:center_frequency\"",
            # startTimeFromAscendingNode is in ms (matches CDSE to < 1 ms).
            f"{c('content_start', 'TIMESTAMPTZ')} - to_milliseconds(CAST({c('startTimeFromAscendingNode', 'DOUBLE')} AS BIGINT)) "
            f"AS \"sat:anx_datetime\"",
            f"CASE WHEN {mode} IS NOT NULL THEN 'right' END AS \"sar:observation_direction\"",
            f"{beam_ids} AS \"sar:beam_ids\"",
            f"CAST({c('datatakeID', 'BIGINT')} AS VARCHAR) AS \"eopf:datatake_id\"",
            f"{c('instrumentConfigurationID', 'BIGINT')} AS \"eopf:instrument_configuration_id\"",
        ]
    if is_s2:
        fields += [
            f"{c('productGroupId')} AS \"eopf:datatake_id\"",
            f"{c('datastripId')} AS \"eopf:datastrip_id\"",
            f"{c('operationalMode')} AS \"eopf:instrument_mode\"",
            f"'MGRS-' || {c('tileId')} AS \"grid:code\"",
            f"CASE WHEN {pt} LIKE 'S2MSI%' THEN 10 END AS gsd",
        ]
    if mission in ("sentinel-2", "sentinel-3"):
        cc = c("cloudCover", "DOUBLE")
        fields.append(f"{'round(' + cc + ', 2)' if is_s2 else cc} AS \"eo:cloud_cover\"")
    fields += [
        f"""{{'product': {{'href': {href}, 'title': 'Product', 'type': 'application/x-directory',
              'roles': ['data'], 'file:size': {c('content_length', 'BIGINT')}}},
             'thumbnail': CASE WHEN {c('quicklook_url')} IS NOT NULL THEN
              {{'href': {c('quicklook_url')}, 'title': 'Quicklook', 'type': 'image/jpeg', 'roles': ['thumbnail']}} END
           }} AS assets""",
        f"""CASE WHEN {c('id')} IS NOT NULL THEN [{{'rel': 'via', 'type': 'application/json', 'title': 'CDSE OData product',
              'href': 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products(' || {c('id')} || ')'}}]
           ELSE [] END AS links""",
        f"{c('id')} AS \"cdse:id\"",
        f"{c('odata_found', 'BOOLEAN')} AS \"cdse:odata_found\"",
        f"{c('online', 'BOOLEAN')} AS \"cdse:online\"",
        f"{c('ingestion_date', 'TIMESTAMPTZ')} AS \"cdse:ingestion_date\"",
        f"{c('checksum_md5')} AS \"cdse:checksum_md5\"",
        f"{c('checksum_blake3')} AS \"cdse:checksum_blake3\"",
        f"{c('attributes_json')} AS \"cdse:attributes\"",
        f"{c('assets_json')} AS \"cdse:assets\"",
        f"{c('locations_json')} AS \"cdse:locations\"",
    ]
    return ",\n  ".join(fields), list(MISSION_EXTENSIONS[mission])


def mission_of(bucket_id: str) -> str:
    """``sentinel-5p-l2`` -> ``sentinel-5p``."""
    return bucket_id.rsplit("-", 1)[0]


def convert(src: str | Path, out: Path, bucket_id: str, *, memory_limit: str | None = None,
            tmp_dir: Path | None = None, cogp: bool = True) -> int:
    """Write the STAC GeoParquet of one merged file (a path or URL) to ``out``; return its rows."""
    mission = mission_of(bucket_id)
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if tmp_dir:
        con.execute(f"SET temp_directory = '{tmp_dir}'")
    src_sql = _q(str(src))
    columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({src_sql})").fetchall()}
    geo_raw = con.execute(
        f"SELECT value FROM parquet_kv_metadata({src_sql}) WHERE decode(key) = 'geo'").fetchone()
    if geo_raw is None:
        raise ValueError(f"{src}: no GeoParquet metadata")
    geo = json.loads(bytes(geo_raw[0]).decode())
    geo.pop("lod", None)  # COGP's, rebuilt by cogp convert
    geo["creator"] = {"library": "cdsemeta", "version": __version__}
    select, _ = select_sql(columns - RAW_ONLY, mission, bucket_id)
    stac_meta = {"version": STAC_GEOPARQUET_VERSION}
    kv = {"geo": json.dumps(geo), "stac-geoparquet": json.dumps(stac_meta)}
    kv_sql = "{" + ", ".join(f"{k.replace('-', '_')}: {_q(v)}" for k, v in kv.items()) + "}"
    out.parent.mkdir(parents=True, exist_ok=True)
    part = out.with_suffix(out.suffix + ".part")
    con.execute(f"""
        COPY (SELECT {select} FROM read_parquet({src_sql}))
        TO {_q(str(part))} (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000, KV_METADATA {kv_sql})
    """)
    con.close()
    _rename_kv(part, {"stac_geoparquet": "stac-geoparquet"})
    part.replace(out)
    if cogp:
        convert_to_cogp(out)
    return pq.ParquetFile(out).metadata.num_rows


def _rename_kv(path: Path, renames: dict[str, str]) -> None:
    """DuckDB's KV_METADATA keys are identifiers (no '-'); fix them up in the footer."""
    pf = pq.ParquetFile(path)
    meta = dict(pf.metadata.metadata or {})
    if not any(k.encode() in meta for k in renames):
        return
    table = pf.read()
    for old, new in renames.items():
        if old.encode() in meta:
            meta[new.encode()] = meta.pop(old.encode())
    tmp = path.with_suffix(path.suffix + ".kv")
    pq.write_table(table.replace_schema_metadata(meta), tmp, compression="zstd", row_group_size=100_000)
    tmp.replace(path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", nargs="+",
                    help="merged files or URLs, named <mission>-<bucket>.parquet/<YYYY-MM>.parquet")
    ap.add_argument("--out-dir", type=Path, default=Path("stac"), help="written to <out-dir>/<same key>")
    ap.add_argument("--upload-r2", action="store_true", help="upload each result under the same key")
    ap.add_argument("--delete-after-upload", action="store_true")
    ap.add_argument("--memory-limit", default=None)
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    failed = 0
    for src in args.source:
        parts = src.rstrip("/").split("/")
        key = "/".join(parts[-2:])
        bucket_id = parts[-2].removesuffix(".parquet")
        out = args.out_dir / key
        try:
            rows = convert(src, out, bucket_id, memory_limit=args.memory_limit, tmp_dir=args.out_dir)
        except Exception as e:  # keep going; the month stays as it was
            log.error("%s: %s", key, e)
            failed += 1
            continue
        log.info("%s: %d rows", key, rows)
        if args.upload_r2:
            r2.upload(out, key)
            if args.delete_after_upload:
                out.unlink()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
