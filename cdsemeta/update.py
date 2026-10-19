"""Keep the per-day artifacts in step with CDSE's catalogue regenerations.

CDSE regenerates every CSV on the 2nd of each month, and the last 30 days
daily. A regeneration changes few rows of most days (Sentinel-1, Sept vs Oct
2026: 2014-2025 changed on 5,590 days by 52k added and 22k modified rows out
of 13.5M, almost all CSV-only CARD_BS; the temporary orbit files RESORB/
PREORB/MOEORB of the days ~3-4 months back were removed). So a changed day is
not rebuilt: its artifact is pulled, rows the CSV no longer lists are
dropped, added and modified rows are rebuilt, every other row is kept as is.

``sync`` (monthly; ``--recent-days 31`` daily), per platform and year:

1. stream that year's CSVs out of the catalogue zip (``catzip``);
2. diff their (Name, ModificationDate, day) against the ID file
   (``ids-<year>-<s1>-<s1a>``) -> the days to look at;
3. for each such day, compare the artifact with the CSV by product name
   (added / removed / ModificationDate changed), update and push it;
4. push the new ID file, only if every day went through.

Rows named like ``Config.exclude`` (Sentinel-1 CARD products) are dropped
on reading, so they leave the artifacts on the next sync; CSVs dated before
their satellite's launch are skipped. Days without an artifact are left to
the worker. ``init-ids`` writes the
first ID files from the zip, for the days whose artifact was built from
exactly the CSV in the zip (only known from legacy tags, which carry the
generation); without them the first ``sync`` compares every day's artifact
with its CSV, which also works, just with one pull per day. ``retag`` gives legacy
artifacts (``S1A_20240101_COPERNICUS_catalogue_20260901``) their per-day tag
(``S1A_20240101``) without re-uploading; ``pull`` fills a local artifact tree.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapely

from . import __version__, csvfile, geoparquet, ids
from .catzip import CatalogueZip
from .config import Config, before_launch
from .http import make_session
from .naming import day_tag, find_day_file, latest_per_day, parquet_relpath, parse_tag, stem
from .odata import Blocked, ODataClient
from .registry import Registry
from .transform import FIXED_SCHEMA, build_table, parse_ts

log = logging.getLogger("cdsemeta.update")

CHUNK = 200  # days per OData round: their lookups share 100-ID batches
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def annotations(csv_key: str, rows: int, missing: int, mission: str, md5: str = "",
                last_modified: str = "") -> dict[str, str]:
    ann = {
        "org.opencontainers.image.title": parquet_relpath(csv_key),
        "eu.copernicus.csv.key": csv_key,
        "eu.copernicus.csv.generation": parse_tag(stem(csv_key)).generated,
        "cdsemeta.rows": str(rows),
        "cdsemeta.odata_found": str(rows - missing),
        "cdsemeta.odata_missing": str(missing),
        "cdsemeta.mission": mission,
        "cdsemeta.version": __version__,
    }
    if md5:
        ann["eu.copernicus.csv.md5"] = md5
    if last_modified:
        ann["eu.copernicus.csv.last_modified"] = last_modified
    return ann


def read_artifact(path: Path) -> tuple[pa.Table, list]:
    """An artifact's table without geometry/bbox, plus its geometries."""
    t = pq.read_table(path)
    geoms = list(shapely.from_wkb(t.column("geometry").to_numpy(zero_copy_only=False)))
    return t.drop_columns(["geometry", "bbox"]), geoms


def _ms(v) -> int | None:
    """Timestamp at millisecond precision: OData has µs, the CSV ms."""
    if v is not None and not isinstance(v, datetime):
        try:
            v = parse_ts(v)
        except ValueError:
            return None
    return None if v is None else (v - EPOCH) // timedelta(milliseconds=1)


def plan_day(old: pa.Table, new_rows: pd.DataFrame) -> tuple[set[str], set[str], Counter]:
    """Compare an artifact with its new CSV, by product name.

    Returns (names to rebuild, ids that need an OData lookup, counts).
    Rebuilt: names the artifact lacks, and names whose ModificationDate
    changed. Names the CSV no longer lists are dropped by ``update_day``.
    Rows that were CSV-only before stay CSV-only (no lookup).
    """
    old_names = old.column("name").to_pylist()
    old_mod = dict(zip(old_names, (_ms(v) for v in old.column("modification_date").to_pylist())))
    csv_only = {n for n, found in zip(old_names, old.column("odata_found").to_pylist()) if not found}
    new = dict(zip(new_rows["Name"], zip(new_rows["Id"], new_rows["ModificationDate"])))
    added = set(new) - set(old_names)
    modified = {n for n in set(new) & set(old_names) if _ms(new[n][1]) != old_mod[n]}
    removed = set(old_names) - set(new)
    redo = added | modified
    lookup = {new[n][0] for n in redo if n not in csv_only}
    return redo, lookup, Counter(added=len(added), modified=len(modified), removed=len(removed))


def _concat(a: pa.Table, b: pa.Table) -> pa.Table:
    """Concatenate two artifact tables whose attribute columns may differ."""
    for name in set(a.column_names) & set(b.column_names):
        ta, tb = a.schema.field(name).type, b.schema.field(name).type
        if ta != tb and not (pa.types.is_null(ta) or pa.types.is_null(tb)):
            # Same attribute, different OData ValueType: keep it as text.
            a = a.set_column(a.column_names.index(name), name, pc.cast(a.column(name), pa.string()))
            b = b.set_column(b.column_names.index(name), name, pc.cast(b.column(name), pa.string()))
    t = pa.concat_tables([a, b], promote_options="permissive")
    fixed = [f.name for f in FIXED_SCHEMA]
    return t.select(fixed + sorted(c for c in t.column_names if c not in fixed))


def update_day(old: pa.Table, old_geoms: list, new_rows: pd.DataFrame, redo: set[str],
               products: dict[str, dict]) -> tuple[pa.Table, list, int]:
    """New (table, geometries, missing) for one day: rows of ``old`` that the
    new CSV still lists and that are not in ``redo`` (names) are kept
    unchanged; rows in ``redo`` are rebuilt from ``products`` / the CSV."""
    new_names = set(new_rows["Name"])
    keep = [n in new_names and n not in redo for n in old.column("name").to_pylist()]
    kept = old.filter(pa.array(keep))
    geoms = [g for g, k in zip(old_geoms, keep) if k]
    rebuilt, new_geoms, _ = build_table(new_rows[new_rows["Name"].isin(redo)].reset_index(drop=True), products)
    table = _concat(kept, rebuilt)
    geoms += new_geoms
    # Same order as build_table: content_start (missing last), then name.
    far_future = pa.scalar(datetime.max.replace(tzinfo=timezone.utc), type=table.schema.field("content_start").type)
    keys = pa.table({"s": pc.fill_null(table.column("content_start"), far_future),
                     "n": pc.fill_null(table.column("name"), "")})
    order = pc.sort_indices(keys, sort_keys=[("s", "ascending"), ("n", "ascending")]).to_pylist()
    table = table.take(order)
    geoms = [geoms[i] for i in order]
    missing = table.num_rows - pc.sum(table.column("odata_found")).as_py() if table.num_rows else 0
    return table, geoms, missing


def write_day(table: pa.Table, geoms: list, rows: int, out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    geoparquet.write(table, geoms, out)
    geoparquet.check(out, rows)
    return out


class Artifacts:
    """Where the current per-day artifacts come from: a local tree laid out
    like ``oras pull`` (e.g. a merge cache), or the registry."""

    def __init__(self, registry: Registry | None = None, local: Path | None = None,
                 tags: set[str] | None = None):
        self.registry = registry
        self.local = local
        self.by_day: dict[tuple[str, str], str] = {}
        if local is None and registry is not None:
            for t in latest_per_day(sorted(tags if tags is not None else registry.tags())):
                info = parse_tag(t)
                self.by_day[(info.platform, info.day)] = t

    def fetch(self, platform: str, day: str, dest: Path) -> Path | None:
        if self.local is not None:
            return find_day_file(self.local, platform, day)
        tag = self.by_day.get((platform, day))
        if tag is None:
            return None
        self.registry.pull(tag, dest)
        return find_day_file(dest, platform, day)


def apply_days(days: list[tuple[str, Path]], platform: str, artifacts: Artifacts, odata: ODataClient | None,
               cfg: Config, registry: Registry | None, out_dir: Path | None, workers: int = 4) -> Counter:
    """Bring the artifacts of ``days`` (``[(csv_key, csv_path)]``) in line
    with their CSVs. In chunks: pull and plan a chunk, look up all of its
    rows that need OData together (100 per request, instead of at least one
    request per day), then build and push."""
    stats: Counter = Counter()
    work_root = cfg.work_dir / "update"
    work_root.mkdir(parents=True, exist_ok=True)

    def prepare(item, work: Path):
        key, csv_path = item
        old_path = artifacts.fetch(platform, parse_tag(stem(key)).day, work / "old")
        if old_path is None:
            return "no-artifact"
        old, old_geoms = read_artifact(old_path)
        new_rows = csvfile.read_csv(csv_path, cfg.exclude)
        redo, lookup, counts = plan_day(old, new_rows)
        if not redo and not counts["removed"]:
            return "unchanged"
        return key, old, old_geoms, new_rows, redo, lookup, counts

    def finish(prepared, products: dict, work: Path) -> str:
        key, old, old_geoms, new_rows, redo, _, counts = prepared
        table, geoms, missing = update_day(old, old_geoms, new_rows, redo, products)
        out = write_day(table, geoms, len(new_rows), work / "new" / parquet_relpath(key))
        if out_dir is not None:
            dest = out_dir / parquet_relpath(key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(out, dest)
        tag = day_tag(platform, parse_tag(stem(key)).day)
        if registry is not None:
            registry.push(tag, out, annotations(key, len(new_rows), missing, cfg.mission), root=work / "new")
        log.info("%s: %d rows; %d added, %d modified, %d removed; %d not in OData", tag, len(new_rows),
                 counts["added"], counts["modified"], counts["removed"], missing)
        return "updated"

    def guarded(fn):
        def run(*a):
            try:
                return fn(*a)
            except Blocked:
                raise
            except Exception:  # noqa: BLE001 - one bad day must not stop the rest; a rerun retries it
                log.exception("update failed")
                return "failed"
        return run

    prepare, finish = guarded(prepare), guarded(finish)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for c in range(0, len(days), CHUNK):
            chunk = days[c:c + CHUNK]
            works = [Path(tempfile.mkdtemp(prefix="upd-", dir=work_root)) for _ in chunk]
            try:
                prepared = list(pool.map(prepare, chunk, works))
                ready = [(pr, w) for pr, w in zip(prepared, works) if not isinstance(pr, str)]
                stats.update(pr for pr in prepared if isinstance(pr, str))
                lookup = sorted(set().union(*(pr[5] for pr, _ in ready))) if ready else []
                products = odata.fetch(lookup) if (lookup and odata is not None) else {}
                if lookup:
                    log.info("OData: %d of %d looked-up products found", len(products), len(lookup))
                stats.update(pool.map(lambda rw: finish(rw[0], products, rw[1]), ready))
            finally:
                for w in works:
                    shutil.rmtree(w, ignore_errors=True)
    return stats


def read_year(catalogue: CatalogueZip, keys: list[str], csv_dir: Path, exclude: tuple[str, ...] = ()
              ) -> tuple[dict[str, tuple[str, Path]], pa.Table]:
    """Extract ``keys`` (one platform-year) to ``csv_dir``. Returns
    ``({day: (key, csv_path)}, ID rows of all of them)``."""
    csvs: dict[str, tuple[str, Path]] = {}
    parts = [ids.empty()]
    for key in keys:
        path = catalogue.extract(key, csv_dir / Path(key).name)
        day = parse_tag(stem(key)).day
        csvs[day] = (key, path)
        parts.append(ids.table_for(csvfile.read_csv(path, exclude), day))
    return csvs, pa.concat_tables(parts)


def keys_by_year(catalogue: CatalogueZip, platform: str, since: str = "",
                 years: list[str] | None = None) -> dict[str, list[str]]:
    by_year: dict[str, list[str]] = {}
    for key in catalogue.keys(platform):
        info = parse_tag(stem(key))
        if info is None or info.day < since or (years and info.day[:4] not in years):
            continue
        if before_launch(info.platform, info.day):
            continue
        by_year.setdefault(info.day[:4], []).append(key)
    return by_year


def init_ids(cfg: Config, work: Path, tags: set[str], push: bool = True, years: list[str] | None = None,
             zip_source: str | None = None, ids_dir: Path | None = None) -> int:
    """First ID files from the zip, without touching any artifact: a day's
    rows go in only if ``tags`` has its legacy tag with the zip CSV's exact
    generation, i.e. the artifact was built from this very CSV. Every other
    day is left out, so the first ``sync`` compares its artifact directly."""
    session = make_session()
    catalogue = CatalogueZip(zip_source, session) if zip_source else \
        CatalogueZip.for_listing(cfg.csv_list_url, session, cfg.http_timeout)
    registry = Registry(cfg.oci_repo) if push else None
    id_files = ids.IdFiles(ids_dir or work / "ids", cfg.mission)
    for platform in cfg.platforms:
        for year, keys in sorted(keys_by_year(catalogue, platform, years=years).items()):
            built = [k for k in keys if stem(k) in tags]
            csv_dir = work / "csv" / platform / year
            try:
                _, table = read_year(catalogue, built, csv_dir, cfg.exclude)
            finally:
                shutil.rmtree(csv_dir, ignore_errors=True)
            ids.write(table, id_files.path(platform, year))
            log.info("%s %s: %d of %d days built from the zip's CSV, %d rows", platform, year, len(built),
                     len(keys), table.num_rows)
            if push:
                id_files.push(registry, platform, year)
    return 0


def ids_from_artifacts(cfg: Config, artifacts_dir: Path, ids_dir: Path, years: list[str] | None = None,
                       push: bool = False) -> int:
    """ID files from a local artifact tree (``cdsemeta-update pull``): what each
    day's newest artifact contains, whatever CSV it came from. For when the CSVs
    an artifact was built from are gone. Names the current rules exclude (CARD)
    are left out, so ``sync`` doesn't rewrite every day just to drop them; they
    leave an artifact whenever that day is rewritten for another reason, and
    ``merge`` filters them regardless."""
    registry = Registry(cfg.oci_repo) if push else None
    id_files = ids.IdFiles(ids_dir, cfg.mission)
    for platform in cfg.platforms:
        newest: dict[str, dict[str, Path]] = {}
        for f in sorted((artifacts_dir / platform).glob("*/*/*.parquet")):
            info = parse_tag(f.stem)
            if info is None or before_launch(platform, info.day) or (years and info.day[:4] not in years):
                continue
            newest.setdefault(info.day[:4], {})[info.day] = f
        for year, days in sorted(newest.items()):
            table = ids.table_from_artifacts(list(days.values()), cfg.exclude)
            ids.write(table, id_files.path(platform, year))
            log.info("%s %s: %d days, %d rows", platform, year, len(days), table.num_rows)
            if push:
                id_files.push(registry, platform, year)
    return 0


def sync(cfg: Config, work: Path, push: bool = True, years: list[str] | None = None,
         recent_days: int = 0, zip_source: str | None = None, artifacts_dir: Path | None = None,
         out_dir: Path | None = None, use_odata: bool = True, ids_dir: Path | None = None,
         dry_run: bool = False) -> int:
    """See the module docstring. Returns 1 if any platform-year had failures
    (its ID file is then not saved, so a rerun looks at the same days)."""
    session = make_session(pool_size=max(8, cfg.odata_workers * 2))
    catalogue = CatalogueZip(zip_source, session) if zip_source else \
        CatalogueZip.for_listing(cfg.csv_list_url, session, cfg.http_timeout)
    log.info("catalogue zip: %d CSVs (ETag %s)", len(catalogue.members), catalogue.etag or "-")
    registry = Registry(cfg.oci_repo) if (push or artifacts_dir is None or ids_dir is None) else None
    artifacts = Artifacts(local=artifacts_dir) if artifacts_dir else Artifacts(registry)
    odata = ODataClient(session, cfg.odata_url, cfg.odata_batch, cfg.odata_workers, cfg.odata_rps,
                        cfg.http_timeout) if use_odata else None
    id_files = ids.IdFiles(ids_dir or work / "ids", cfg.mission)
    since = f"{date.today() - timedelta(days=recent_days):%Y%m%d}" if recent_days else ""
    failed = 0

    for platform in cfg.platforms:
        for year, keys in sorted(keys_by_year(catalogue, platform, since, years).items()):
            csv_dir = work / "csv" / platform / year
            try:
                csvs, new = read_year(catalogue, keys, csv_dir, cfg.exclude)
                old_path = id_files.path(platform, year) if ids_dir else (
                    id_files.pull(registry, platform, year) if registry else None)
                old = ids.read(old_path)
                if recent_days:  # only these days were read: keep the rest of the year as it was
                    rest = old.filter(pc.invert(pc.is_in(old.column("day"), pa.array(sorted(csvs)))))
                    new = pa.concat_tables([rest, new])
                counts = ids.changes(old, new)
                todo = [csvs[d] for d in counts["day"] if d in csvs]
                gone = [d for d in counts["day"] if d not in csvs]
                if gone:
                    log.warning("%s %s: %d days have rows in the ID file but no CSV: %s", platform, year,
                                len(gone), ", ".join(gone[:5]))
                log.info("%s %s: %d CSVs, %d days to check (%d added, %d removed, %d modified)", platform, year,
                         len(keys), len(todo), counts.added.sum(), counts.removed.sum(), counts.modified.sum())
                if dry_run:
                    continue
                stats = apply_days(todo, platform, artifacts, odata, cfg, registry if push else None, out_dir,
                                   cfg.odata_workers) if todo else Counter()
                log.info("%s %s: %s", platform, year, dict(stats))
                if stats.get("failed"):
                    log.error("%s %s: %d days failed; not saving its ID file (rerun to retry)", platform, year,
                              stats["failed"])
                    failed += 1
                    continue
                ids.write(new, id_files.path(platform, year))
                if push:
                    id_files.push(registry, platform, year)
            finally:
                shutil.rmtree(csv_dir, ignore_errors=True)
    return 1 if failed else 0


def retag(registry: Registry, dry_run: bool = False, workers: int = 8) -> int:
    """Add the per-day tag to the newest legacy artifact of every day that
    doesn't have one yet."""
    tags = registry.tags()
    todo = [t for t in latest_per_day(sorted(tags)) if parse_tag(t).generated]
    log.info("%d tags, %d days without a per-day tag", len(tags), len(todo))
    if dry_run:
        for t in todo[:20]:
            print(f"{t} -> {parse_tag(t).day_tag}")
        if len(todo) > 20:
            print(f"... and {len(todo) - 20} more")
        return 0
    failed = 0

    def one(t: str) -> None:
        nonlocal failed
        try:
            registry.tag(t, parse_tag(t).day_tag)
        except Exception:  # noqa: BLE001 - report and carry on; a rerun picks it up
            failed += 1
            log.exception("could not tag %s", t)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, _ in enumerate(pool.map(one, todo), 1):
            if i % 500 == 0 or i == len(todo):
                log.info("%d/%d tagged", i, len(todo))
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log-level", default=None)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sync", help="update changed days from the catalogue zip, then the ID files")
    s.add_argument("--work", type=Path, default=Path("sync"))
    s.add_argument("--year", action="append", help="only these years (repeatable)")
    s.add_argument("--recent-days", type=int, default=0, help="only days sensed in the last N days (daily run)")
    s.add_argument("--zip", help="use this catalogue zip (path or URL) instead of CDSE's")
    s.add_argument("--artifacts", type=Path, help="local artifact tree instead of pulling from the registry")
    s.add_argument("--ids", type=Path, help="local ID files root instead of the registry's")
    s.add_argument("--out", type=Path, help="also write updated artifacts here")
    s.add_argument("--no-push", action="store_true")
    s.add_argument("--dry-run", action="store_true",
                   help="only log how many days would be checked, per platform and year; change nothing")
    s.add_argument("--no-odata", action="store_true",
                   help="never call OData: rows that would need it are built from the CSV (odata_found=false)")

    i = sub.add_parser("init-ids", help="first ID files from the zip, for days built from exactly that CSV")
    i.add_argument("--work", type=Path, default=Path("sync"))
    i.add_argument("--year", action="append")
    i.add_argument("--zip", help="use this catalogue zip (path or URL) instead of CDSE's")
    i.add_argument("--tags", type=Path, help="registry tags, one per line (`oras repo tags …`), instead of listing them")
    i.add_argument("--ids", type=Path, help="write the ID files under this root")
    i.add_argument("--no-push", action="store_true")

    a = sub.add_parser("ids-from-artifacts", help="ID files from a local artifact tree, when the CSVs are gone")
    a.add_argument("--artifacts", type=Path, required=True, help="e.g. local-work/cache")
    a.add_argument("--ids", type=Path, required=True, help="write the ID files under this root")
    a.add_argument("--year", action="append")
    a.add_argument("--push", action="store_true")

    r = sub.add_parser("retag", help="add per-day tags to legacy-tagged artifacts")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--workers", type=int, default=8)

    pl = sub.add_parser("pull", help="pull the newest artifact of every day into a local tree (skips cached days)")
    pl.add_argument("--cache", type=Path, required=True)
    pl.add_argument("--workers", type=int, default=8)

    args = ap.parse_args(argv)
    logging.basicConfig(level=(args.log_level or os.environ.get("LOG_LEVEL", "INFO")).upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = Config.from_env()

    if args.cmd == "retag":
        return retag(Registry(cfg.oci_repo), args.dry_run, args.workers)
    if args.cmd == "ids-from-artifacts":
        return ids_from_artifacts(cfg, args.artifacts, args.ids, args.year, args.push)
    if args.cmd == "init-ids":
        tags = ({t.strip() for t in args.tags.read_text().splitlines() if t.strip()} if args.tags
                else Registry(cfg.oci_repo).tags())
        return init_ids(cfg, args.work, tags, not args.no_push, args.year, args.zip, args.ids)
    if args.cmd == "pull":
        from .merge import pull_all
        registry = Registry(cfg.oci_repo)
        files = pull_all(registry, latest_per_day(sorted(registry.tags())), args.cache, args.workers)
        log.info("%d artifacts in %s", len(files), args.cache)
        return 0
    return sync(cfg, args.work, not args.no_push, args.year, args.recent_days, args.zip, args.artifacts,
                args.out, not args.no_odata, args.ids, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
