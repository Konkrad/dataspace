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


def merge(files: list[Path], out: Path, tmp_dir: Path, memory_limit: str | None = None) -> int:
    geo = combined_geo(files)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{tmp_dir}'")
    con.execute("SET preserve_insertion_order = false")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    file_list = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
    kv = json.dumps(geo).replace("'", "''")
    part = out.with_suffix(out.suffix + ".part")
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet([{file_list}], union_by_name = true)
            ORDER BY content_start, name
        ) TO '{part}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000, KV_METADATA {{geo: '{kv}'}})
    """)
    part.replace(out)
    return pq.ParquetFile(out).metadata.num_rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None, help="default: <mission>_all.parquet")
    ap.add_argument("--cache", type=Path, default=Path("cache"), help="where pulled artifacts are kept")
    ap.add_argument("--no-pull", action="store_true", help="only use files already in --cache")
    ap.add_argument("--push", action="store_true", help="push the result as tag all-YYYYMMDD")
    ap.add_argument("--memory-limit", default=None, help="DuckDB memory limit, e.g. 4GB")
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = Config.from_env()
    args.out = args.out or Path(f"{cfg.mission}_all.parquet")
    registry = Registry(cfg.oci_repo)
    args.cache.mkdir(parents=True, exist_ok=True)
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
    log.info("merging %d files", len(files))
    rows = merge(files, args.out, args.cache, args.memory_limit)
    log.info("wrote %s: %d rows", args.out, rows)
    log.info("converting %s to COGP layout", args.out)
    convert_to_cogp(args.out)
    if args.push:
        tag = f"all-{date.today():%Y%m%d}"
        registry.push(tag, args.out, {
            "org.opencontainers.image.title": args.out.name,
            "cdsemeta.rows": str(rows),
            "cdsemeta.files": str(len(files)),
            "cdsemeta.mission": cfg.mission,
            "cdsemeta.version": __version__,
        })
        log.info("pushed %s:%s", cfg.oci_repo, tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
