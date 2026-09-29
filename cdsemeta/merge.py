"""Merge all per-day GeoParquet artifacts into one file.

Only the newest catalogue generation of each (platform, day) is used, so days
that were regenerated are not counted twice.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from . import __version__
from .cogp import convert_to_cogp
from .config import Config
from .naming import latest_per_day, relpath_for_tag
from .registry import Registry

log = logging.getLogger("cdsemeta.merge")

# Bucketed from the real distinct `productType` values seen across the full
# Sentinel-1 archive (IW_GRDH_1S, IW_GRDH_1S-COG, EW_GRDM_1S, S1..S6_GRDH_1S
# for GRD; similarly for SLC/OCN; *_ETA__AX for ETAD; RAW and *_RAW__0S for
# RAW; AUX_* for orbit/calibration files, which aren't SAR imagery). Also
# splits the single combined merge into per-collection files -- the full
# archive merged into one file measured ~9GB, uncomfortably close to GHCR's
# 10GB per-layer limit, and only grows; GRD alone measured ~3.7GB.
COLLECTION_CASE = """
    CASE
      WHEN productType IS NULL THEN 'other'
      WHEN productType LIKE 'AUX_%' THEN 'aux'
      WHEN productType LIKE '%GRD%' THEN 'grd'
      WHEN productType LIKE '%SLC%' THEN 'slc'
      WHEN productType LIKE '%OCN%' THEN 'ocn'
      WHEN productType LIKE '%ETA%' THEN 'etad'
      WHEN productType LIKE '%RAW%' THEN 'raw'
      ELSE 'other'
    END
"""
COLLECTIONS = ("grd", "slc", "ocn", "etad", "raw", "aux", "other")


def pull_all(registry: Registry, tags: list[str], cache: Path, workers: int = 8) -> list[Path]:
    def one(tag: str) -> Path:
        f = cache / relpath_for_tag(tag)
        if not f.exists():
            registry.pull(tag, cache)
        return f

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(one, tags))


def combined_geo(files: list[Path]) -> dict:
    types: set[str] = set()
    bbox = [float("inf"), float("inf"), float("-inf"), float("-inf")]
    geo: dict | None = None
    for f in files:
        meta = pq.read_schema(f).metadata or {}
        g = json.loads(meta[b"geo"])
        col = g["columns"]["geometry"]
        types.update(col.get("geometry_types", []))
        if "bbox" in col:
            b = col["bbox"]
            bbox = [min(bbox[0], b[0]), min(bbox[1], b[1]), max(bbox[2], b[2]), max(bbox[3], b[3])]
        geo = geo or g
    assert geo is not None
    col = geo["columns"]["geometry"]
    col["geometry_types"] = sorted(types)
    if bbox[0] != float("inf"):
        col["bbox"] = bbox
    else:
        col.pop("bbox", None)
    geo["creator"] = {"library": "cdsemeta", "version": __version__}
    return geo


def merge(files: list[Path], out: Path, tmp_dir: Path, memory_limit: str | None = None,
          collection: str | None = None) -> int:
    """``files`` must already be in ascending (platform, day) order (as
    ``latest_per_day`` returns them). If ``collection`` is given (one of
    ``COLLECTIONS``), only rows bucketed into it are written.

    No explicit sort here: each CSV's embedded date is the *sensing* date
    (confirmed against live CDSE catalogues, not just ingestion order), and
    each file's own rows are already sorted by ``content_start``
    (``transform.build_table``). So concatenating files in the given order,
    with insertion order preserved, already yields a file that's sorted for
    all practical purposes -- without DuckDB's external sort, which needs
    tens of GB of disk spill at the full Sentinel-1 archive's scale (measured
    empirically: ~34GB spilled and still ran out of space with an explicit
    ``ORDER BY content_start``).

    Note this doesn't use DuckDB's ``PARTITION_BY`` to write every collection
    in one pass: combined with ``preserve_insertion_order``, that needs
    proportionally more buffering per simultaneous output stream and hit the
    same out-of-memory wall the sort did. One filtered pass per collection
    re-scans the files, but reuses the exact single-output-stream shape
    that's already proven to fit in a few GB of RAM at full archive scale.
    """
    geo = combined_geo(files)  # approximate for a single collection: the bbox/
    # geometry_types come from every file's full metadata, not just this
    # collection's rows. In practice this mission only ever has Polygon
    # geometries, and collections share roughly the same global coverage, so
    # this doesn't produce a meaningfully wrong bbox -- just a possibly
    # slightly looser one than recomputing from the filtered rows would.
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{tmp_dir}'")
    con.execute("SET preserve_insertion_order = true")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    file_list = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
    kv = json.dumps(geo).replace("'", "''")
    where = f"WHERE ({COLLECTION_CASE}) = '{collection}'" if collection else ""
    part = out.with_suffix(out.suffix + ".part")
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet([{file_list}], union_by_name = true)
            {where}
        ) TO '{part}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000, KV_METADATA {{geo: '{kv}'}})
    """)
    part.replace(out)
    return pq.ParquetFile(out).metadata.num_rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", type=Path, default=Path("."),
                    help="directory for <mission>_<collection>.parquet files")
    ap.add_argument("--collection", choices=COLLECTIONS, default=None,
                    help="only merge this one collection (default: all of them)")
    ap.add_argument("--cache", type=Path, default=Path("cache"), help="where pulled artifacts are kept")
    ap.add_argument("--no-pull", action="store_true", help="only use files already in --cache")
    ap.add_argument("--push", action="store_true", help="push each result as tag <collection>-YYYYMMDD")
    ap.add_argument("--memory-limit", default=None, help="DuckDB memory limit, e.g. 4GB")
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = Config.from_env()
    registry = Registry(cfg.oci_repo)
    args.cache.mkdir(parents=True, exist_ok=True)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    if args.no_pull:
        tags = latest_per_day([p.stem for p in args.cache.rglob("*.parquet")])
    else:
        tags = latest_per_day(sorted(registry.tags()))
        log.info("pulling %d artifacts into %s", len(tags), args.cache)
        pull_all(registry, tags, args.cache)
    files = [args.cache / relpath_for_tag(t) for t in tags]
    if not files:
        log.error("nothing to merge")
        return 1

    for collection in (args.collection,) if args.collection else COLLECTIONS:
        out = args.out_dir / f"{cfg.mission}_{collection}.parquet"
        log.info("merging %d files into %s", len(files), out)
        rows = merge(files, out, args.cache, args.memory_limit, collection=collection)
        if rows == 0:
            out.unlink()
            log.info("%s: no rows, skipping", collection)
            continue
        log.info("wrote %s: %d rows", out, rows)
        log.info("converting %s to COGP layout", out)
        convert_to_cogp(out)
        if args.push:
            tag = f"{collection}-{date.today():%Y%m%d}"
            registry.push(tag, out, {
                "org.opencontainers.image.title": out.name,
                "cdsemeta.rows": str(rows),
                "cdsemeta.files": str(len(files)),
                "cdsemeta.collection": collection,
                "cdsemeta.mission": cfg.mission,
                "cdsemeta.version": __version__,
            })
            log.info("pushed %s:%s", cfg.oci_repo, tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
