"""Main worker loop.

Work left = CSVs in the catalogue listing whose tag is not in the registry.
Nothing else is stored, so the worker can be stopped and restarted at will.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import signal
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

from . import __version__, csvfile, geoparquet
from .config import COLLECTIONS, Config
from .http import make_session
from .listing import CsvEntry, list_csvs
from .naming import parquet_relpath, parse_tag, tag_for
from .odata import Blocked, ODataClient
from .registry import Registry
from .transform import build_table

log = logging.getLogger("cdsemeta.worker")

BLOCKED_WAIT_MIN = 30 * 60
BLOCKED_WAIT_MAX = 6 * 3600


class Stop(BaseException):
    pass


def process(entry: CsvEntry, cfg: Config, session, odata: ODataClient, work: Path) -> tuple[Path, dict]:
    """Build the GeoParquet for one CSV at ``work/<platform>/<year>/<month>/``.

    Returns (path, annotations).
    """
    t0 = time.monotonic()
    csv_path = work / Path(entry.key).name
    csvfile.download(session, entry.url(cfg.csv_list_url), entry, csv_path, timeout=cfg.http_timeout)
    df = csvfile.read_csv(csv_path)
    info = parse_tag(tag_for(entry.key))
    products = odata.fetch_day(COLLECTIONS[cfg.mission], info.platform,
                               datetime.strptime(info.day, "%Y%m%d").date(), df["Id"].tolist())
    table, geoms, missing = build_table(df, products)
    out = work / parquet_relpath(entry.key)
    out.parent.mkdir(parents=True, exist_ok=True)
    geoparquet.write(table, geoms, out)
    geoparquet.check(out, len(df))
    ann = {
        "org.opencontainers.image.title": parquet_relpath(entry.key),
        "eu.copernicus.csv.key": entry.key,
        "eu.copernicus.csv.md5": entry.md5,
        "eu.copernicus.csv.last_modified": entry.last_modified,
        "cdsemeta.rows": str(len(df)),
        "cdsemeta.odata_found": str(len(df) - missing),
        "cdsemeta.odata_missing": str(missing),
        "cdsemeta.mission": cfg.mission,
        "cdsemeta.version": __version__,
    }
    log.info("%s: %d rows, %d not in OData, %.1fs", entry.key, len(df), missing, time.monotonic() - t0)
    return out, ann


def todo_list(entries: list[CsvEntry], done: set[str], order: str) -> list[CsvEntry]:
    todo = [e for e in entries if tag_for(e.key) not in done]
    # Sort by day, then platform, so the archive fills up chronologically.
    todo.sort(key=lambda e: (Path(e.key).name.split("_")[1:2], e.key), reverse=(order == "desc"))
    return todo


def run(cfg: Config, dry_run: bool = False, only: str | None = None, no_push: bool = False,
        out_dir: Path | None = None) -> int:
    session = make_session(pool_size=max(8, cfg.odata_workers * 2))
    odata = ODataClient(session, cfg.odata_url, cfg.odata_batch, cfg.odata_workers, cfg.odata_rps, cfg.http_timeout)
    registry = Registry(cfg.oci_repo)
    cfg.work_dir.mkdir(parents=True, exist_ok=True)
    log.info("mission %s (%s) -> %s", cfg.mission, ",".join(cfg.platforms), cfg.oci_repo)

    blocked_wait = BLOCKED_WAIT_MIN
    while True:
        entries = list_csvs(session, cfg.csv_list_url, cfg.platforms, timeout=cfg.http_timeout)
        if only:
            entries = [e for e in entries if e.key == only or tag_for(e.key) == only or Path(e.key).name == only]
            if not entries:
                log.error("no CSV matches %r", only)
                return 1
        done = set() if (no_push or only) else registry.tags()
        todo = todo_list(entries, done, cfg.order)
        log.info("%d CSVs listed, %d already in %s, %d to do", len(entries), len(entries) - len(todo),
                 cfg.oci_repo, len(todo))
        if dry_run:
            for e in todo[:20]:
                print(e.key)
            if len(todo) > 20:
                print(f"... and {len(todo) - 20} more")
            return 0

        if not todo and not (cfg.run_once or only):
            # Fully caught up. This worker's job (a one-time backfill) is
            # done; ongoing new-day catch-up is a separate, periodic process
            # (see .github/workflows/worker.yml). Polling forever here would
            # just repeat the same "nothing to do" check against CDSE
            # indefinitely for no purpose -- stop reaching out entirely
            # instead. Still interruptible (SIGTERM/SIGINT -> Stop); a
            # restart is what resumes checking, not a timer.
            log.info("nothing left to do; stopping instead of polling further")
            while True:
                time.sleep(86400)

        failed = 0
        blocked = False
        for i, entry in enumerate(todo, 1):
            work = Path(tempfile.mkdtemp(prefix="job-", dir=cfg.work_dir))
            try:
                log.info("[%d/%d] %s", i, len(todo), entry.key)
                out, ann = process(entry, cfg, session, odata, work)
                if out_dir is not None:
                    dest = out_dir / parquet_relpath(entry.key)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(out, dest)
                    log.info("wrote %s", dest)
                if not no_push:
                    registry.push(tag_for(entry.key), out, ann, root=work)
                    log.info("pushed %s:%s", cfg.oci_repo, tag_for(entry.key))
                blocked_wait = BLOCKED_WAIT_MIN
            except Stop:
                log.info("stopping; %s was not pushed and will be redone", entry.key)
                return 0
            except Blocked as e:
                # CDSE's WAF rejected us (see odata.Blocked). Observed to be
                # temporary -- it cleared within hours both times -- so back
                # off and retry rather than hammering it or giving up. The
                # CSV wasn't pushed, so the next pass redoes it.
                if cfg.run_once or only:
                    log.error("CDSE blocked this worker (%s) on %s; exiting", e, entry.key)
                    return 1
                log.error("CDSE blocked this worker (%s) on %s; waiting %ds before retrying",
                          e, entry.key, blocked_wait)
                time.sleep(blocked_wait)
                blocked_wait = min(blocked_wait * 2, BLOCKED_WAIT_MAX)
                blocked = True
                break
            except Exception:  # noqa: BLE001 - one bad file must not stop the worker
                # Not pushed, so it is still missing from the registry and the
                # next pass picks it up again.
                failed += 1
                log.exception("failed %s; will retry on the next pass", entry.key)
            finally:
                shutil.rmtree(work, ignore_errors=True)

        if blocked:
            continue
        if cfg.run_once or only:
            return 1 if failed else 0
        log.info("pass finished (%d failed); sleeping %ds before checking again", failed, cfg.poll_interval)
        time.sleep(cfg.poll_interval)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="list what is left to do and exit")
    ap.add_argument("--only", help="process a single CSV (key, file name or tag) and exit")
    ap.add_argument("--no-push", action="store_true", help="do not push to the registry")
    ap.add_argument("--out", type=Path, help="also copy the GeoParquet files into this directory")
    ap.add_argument("--log-level", default=None)
    args = ap.parse_args(argv)

    import os
    logging.basicConfig(level=(args.log_level or os.environ.get("LOG_LEVEL", "INFO")).upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    def on_signal(signum, _frame):
        raise Stop()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    try:
        return run(Config.from_env(), args.dry_run, args.only, args.no_push, args.out)
    except Stop:
        return 0


if __name__ == "__main__":
    sys.exit(main())
