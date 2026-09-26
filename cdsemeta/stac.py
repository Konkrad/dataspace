"""Convert a merged cdsemeta GeoParquet into STAC GeoParquet.

Maps the raw OData attribute columns cdsemeta already stores (``orbitNumber``,
``orbitDirection``, ``polarisationChannels``, ``productType``, ...) into the
STAC properties used by CDSE's own STAC API
(https://stac.dataspace.copernicus.eu/), following
https://github.com/radiantearth/stac-geoparquet-spec.

Everything here runs as vectorized ``pyarrow.compute`` kernels over whole
columns (never a per-row Python callback), so it scales to the full ~18M-row
Sentinel-1 merge the same way ``transform.py`` and ``geoparquet.py`` do for
the per-CSV files. The one exception, ``processing:software``, builds a small
per-row dict because its *keys* vary by row -- there's no column-level
operation that expresses "each row gets an object with a different key name".

Each extension block below only runs when its source columns are present, so
this degrades gracefully for missions other than Sentinel-1 instead of
crashing -- but the SAR/sat/product/processing/EOPF mapping has only been
verified against real Sentinel-1 OData attributes and CDSE STAC items.

The output is run back through ``cogp convert`` (see ``cdsemeta.cogp``), since
adding columns after COGP layout was applied would leave its ``geo.lod``
metadata describing stale row-group boundaries.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import __version__
from .cogp import convert_to_cogp
from .config import Config
from .registry import Registry

log = logging.getLogger("cdsemeta.stac")

STAC_VERSION = "1.1.0"
STAC_GEOPARQUET_VERSION = "1.1.0"

# Sentinel-1A/B/C/D COSPAR/international designators. Fixed per satellite and
# not present in the OData Attributes cdsemeta stores; confirmed against
# real items from https://stac.dataspace.copernicus.eu/v1/.
PLATFORM_DESIGNATORS = {
    "A": "2014-016A",
    "B": "2016-025A",
    "C": "2024-235A",
    "D": "2025-251A",
}

# OData `timeliness` category -> ISO 8601 duration. Confirmed against real
# CDSE STAC items; only these two categories occur for Sentinel-1 GRD/SLC/OCN.
TIMELINESS_DURATION = {
    "NRT-3h": "PT3H",
    "Fast-24h": "PT24H",
}

PROCESSING_LEVEL = {"LEVEL0": "L0", "LEVEL1": "L1", "LEVEL2": "L2"}

STAC_EXTENSION_SCHEMAS = {
    "sar": "https://stac-extensions.github.io/sar/v1.3.0/schema.json",
    "sat": "https://stac-extensions.github.io/sat/v1.1.0/schema.json",
    "product": "https://stac-extensions.github.io/product/v1.0.0/schema.json",
    "processing": "https://stac-extensions.github.io/processing/v1.2.0/schema.json",
    "eopf": "https://cs-si.github.io/eopf-stac-extension/v1.2.0/schema.json",
}

ASSET_STRUCT = pa.struct([
    ("href", pa.string()),
    ("title", pa.string()),
    ("description", pa.string()),
    ("type", pa.string()),
    ("roles", pa.list_(pa.string())),
])
LINK_STRUCT = pa.struct([
    ("href", pa.string()),
    ("rel", pa.string()),
    ("type", pa.string()),
    ("title", pa.string()),
])


def _lookup(col: pa.Array, mapping: dict, default: pa.Array | pa.Scalar | None = None) -> pa.Array:
    """Chained ``if_else`` over a small fixed mapping (few keys, any row count).

    Vectorized per key -- the Python loop is over ``mapping``, not over rows.
    """
    result = default if default is not None else pa.scalar(None, type=pa.string())
    for key, value in mapping.items():
        result = pc.if_else(pc.equal(col, key), pa.scalar(value), result)
    return result


def _combine(arr: pa.Array | pa.ChunkedArray) -> pa.Array:
    return arr.combine_chunks() if isinstance(arr, pa.ChunkedArray) else arr


def _list_wrap(values: pa.Array, valid: pa.Array) -> pa.Array:
    """One-element list per valid row, null list per invalid row."""
    valid_np = _combine(valid).to_numpy(zero_copy_only=False)
    offsets = np.zeros(len(valid_np) + 1, dtype=np.int32)
    np.cumsum(valid_np.astype(np.int32), out=offsets[1:])
    filtered = _combine(values.filter(valid))
    return pa.ListArray.from_arrays(pa.array(offsets), filtered, mask=pa.array(~valid_np))


def _s3_uri(col: pa.Array) -> pa.Array:
    """Vectorized equivalent of ``transform.s3_uri`` for historical data.

    New rows are already normalized at ingestion time; this just protects
    against merging in older artifacts pushed before that fix.
    """
    has_scheme = pc.starts_with(col, "s3://")
    has_slash = pc.starts_with(col, "/")
    return pc.if_else(
        has_scheme, col,
        pc.if_else(has_slash,
                   pc.binary_join_element_wise(pa.scalar("s3:/"), col, ""),
                   pc.binary_join_element_wise(pa.scalar("s3://"), col, "")),
    )


def _add_common(t: pa.Table, cols: dict) -> None:
    """Fields common to any STAC item, when the source columns exist."""
    if "platformShortName" in t.column_names and "platformSerialIdentifier" in t.column_names:
        short = pc.utf8_lower(t["platformShortName"])
        serial = pc.utf8_lower(t["platformSerialIdentifier"])
        cols["constellation"] = short
        cols["platform"] = pc.binary_join_element_wise(short, serial, "")
    if "instrumentShortName" in t.column_names:
        col = t["instrumentShortName"]
        cols["instruments"] = _list_wrap(pc.utf8_lower(col), pc.is_valid(col))


def _add_sar(t: pa.Table, cols: dict, extensions: set) -> None:
    if "polarisationChannels" not in t.column_names or "operationalMode" not in t.column_names:
        return
    mode = t["operationalMode"]
    has_mode = pc.is_valid(mode)
    cols["sar:instrument_mode"] = mode
    cols["sar:polarizations"] = pc.split_pattern(t["polarisationChannels"], "&")
    # Fixed for the whole Sentinel-1 mission (C-band, right-looking antenna).
    cols["sar:frequency_band"] = pc.if_else(has_mode, pa.scalar("C"), pa.scalar(None, type=pa.string()))
    cols["sar:center_frequency"] = pc.if_else(has_mode, pa.scalar(5.405), pa.scalar(None, type=pa.float64()))
    cols["sar:observation_direction"] = pc.if_else(has_mode, pa.scalar("right"), pa.scalar(None, type=pa.string()))
    extensions.add("sar")


def _add_sat(t: pa.Table, cols: dict, extensions: set) -> None:
    have = False
    if "orbitDirection" in t.column_names:
        cols["sat:orbit_state"] = pc.utf8_lower(t["orbitDirection"])
        have = True
    if "orbitNumber" in t.column_names:
        cols["sat:absolute_orbit"] = t["orbitNumber"]
        have = True
    if "relativeOrbitNumber" in t.column_names:
        cols["sat:relative_orbit"] = t["relativeOrbitNumber"]
        have = True
    if "cycleNumber" in t.column_names:
        cols["sat:orbit_cycle"] = t["cycleNumber"]
        have = True
    if "platformSerialIdentifier" in t.column_names:
        cols["sat:platform_international_designator"] = _lookup(
            pc.utf8_upper(t["platformSerialIdentifier"]), PLATFORM_DESIGNATORS)
        have = True
    if have:
        extensions.add("sat")


def _add_product(t: pa.Table, cols: dict, extensions: set) -> None:
    have = False
    if "productType" in t.column_names:
        cols["product:type"] = t["productType"]
        have = True
    if "timeliness" in t.column_names:
        cols["product:timeliness_category"] = t["timeliness"]
        cols["product:timeliness"] = _lookup(t["timeliness"], TIMELINESS_DURATION)
        have = True
    if have:
        extensions.add("product")


def _add_processing(t: pa.Table, cols: dict, extensions: set) -> None:
    have = False
    if "processingLevel" in t.column_names:
        level = t["processingLevel"]
        cols["processing:level"] = _lookup(level, PROCESSING_LEVEL, default=level)
        have = True
    if "processingDate" in t.column_names:
        cols["processing:datetime"] = t["processingDate"]
        have = True
    if "processingCenter" in t.column_names:
        cols["processing:facility"] = t["processingCenter"]
        have = True
    if "processorName" in t.column_names and "processorVersion" in t.column_names:
        # A map, not a struct: processorName (the map key) varies per row, and
        # Arrow structs need a fixed field set. Keys vary per row, so this is
        # the one field built with a per-row Python loop -- see module docstring.
        names = t["processorName"].to_pylist()
        versions = t["processorVersion"].to_pylist()
        cols["processing:software"] = pa.array(
            [[(n, v)] if isinstance(n, str) else None for n, v in zip(names, versions)],
            type=pa.map_(pa.string(), pa.string()),
        )
        have = True
    if have:
        extensions.add("processing")


def _add_eopf(t: pa.Table, cols: dict, extensions: set) -> None:
    if "datatakeID" not in t.column_names:
        return
    cols["eopf:datatake_id"] = pc.cast(t["datatakeID"], pa.string())
    extensions.add("eopf")


def _assets(t: pa.Table) -> pa.Array:
    """A single ``data`` asset per item, pointing at its S3 directory.

    cdsemeta only has the product-level S3 directory (not the individual
    band/measurement files inside it), so that directory is the whole asset.
    """
    href = _s3_uri(t["s3_path"])
    has_href = pc.is_valid(href)
    n = t.num_rows
    data = pa.StructArray.from_arrays(
        [
            _combine(href),
            _combine(t["name"]),
            pa.nulls(n, type=pa.string()),
            _combine(pc.if_else(has_href, pa.scalar("application/x-directory"), pa.scalar(None, type=pa.string()))),
            _list_wrap(pa.array(["data"] * n), has_href),
        ],
        fields=list(ASSET_STRUCT),
    )
    return pa.StructArray.from_arrays([data], fields=[pa.field("data", ASSET_STRUCT)])


def build_stac_table(table: pa.Table, mission: str) -> pa.Table:
    """Turn a merged cdsemeta GeoParquet table into a STAC GeoParquet table."""
    n = table.num_rows
    extensions: set[str] = set()
    cols: dict = {}

    cols["id"] = pc.replace_substring_regex(table["name"], r"\.SAFE$", "")
    cols["collection"] = pa.array([mission] * n, type=pa.string())
    cols["datetime"] = table["content_start"]
    cols["start_datetime"] = table["content_start"]
    cols["end_datetime"] = table["content_end"]

    _add_common(table, cols)
    _add_sar(table, cols, extensions)
    _add_sat(table, cols, extensions)
    _add_product(table, cols, extensions)
    _add_processing(table, cols, extensions)
    _add_eopf(table, cols, extensions)

    out = pa.table(cols)
    out = out.append_column("geometry", table.column("geometry"))
    out = out.append_column("bbox", table.column("bbox"))
    out = out.append_column("links", pa.array([[]] * n, type=pa.list_(LINK_STRUCT)))
    out = out.append_column("assets", _assets(table))
    ext_list = sorted(STAC_EXTENSION_SCHEMAS[e] for e in extensions)
    out = out.append_column("stac_extensions", pa.array([ext_list] * n, type=pa.list_(pa.string())))
    return out


def collection_json(mission: str) -> dict:
    """A minimal STAC Collection object, for the parquet-level metadata."""
    return {
        "type": "Collection",
        "stac_version": STAC_VERSION,
        "id": mission,
        "description": f"cdsemeta {mission} catalogue mirror",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
    }


def write(table: pa.Table, geo_meta: bytes, mission: str, path: Path) -> Path:
    meta = dict(table.schema.metadata or {})
    meta[b"geo"] = geo_meta
    meta[b"stac-geoparquet"] = json.dumps({
        "version": STAC_GEOPARQUET_VERSION,
        "collections": {mission: collection_json(mission)},
    }).encode()
    table = table.replace_schema_metadata(meta)
    tmp = path.with_suffix(path.suffix + ".part")
    pq.write_table(table, tmp, compression="zstd", compression_level=9, row_group_size=100_000,
                   write_statistics=True)
    tmp.replace(path)
    return path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input", type=Path, help="merged GeoParquet, e.g. sentinel-1_all.parquet")
    ap.add_argument("--out", type=Path, default=None, help="default: <mission>_stac.parquet")
    ap.add_argument("--push", action="store_true", help="push the result as tag stac-YYYYMMDD")
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = Config.from_env()
    args.out = args.out or Path(f"{cfg.mission}_stac.parquet")

    src = pq.read_table(args.input)
    geo_meta = (src.schema.metadata or {}).get(b"geo")
    if geo_meta is None:
        log.error("%s is missing GeoParquet metadata", args.input)
        return 1
    stac_table = build_stac_table(src, cfg.mission)
    write(stac_table, geo_meta, cfg.mission, args.out)
    log.info("wrote %s: %d rows", args.out, stac_table.num_rows)
    log.info("converting %s to COGP layout", args.out)
    convert_to_cogp(args.out)

    if args.push:
        registry = Registry(cfg.oci_repo)
        tag = f"stac-{date.today():%Y%m%d}"
        registry.push(tag, args.out, {
            "org.opencontainers.image.title": args.out.name,
            "cdsemeta.rows": str(stac_table.num_rows),
            "cdsemeta.mission": cfg.mission,
            "cdsemeta.version": __version__,
        })
        log.info("pushed %s:%s", cfg.oci_repo, tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
