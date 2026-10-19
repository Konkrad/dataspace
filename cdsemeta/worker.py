"""Main worker loop (backfill).

Work left = (platform, sensing day) CSVs in the catalogue listing that have
no artifact in the registry yet, under any tag: the per-day tag
(``S1A_20240101``) or a legacy one (``S1A_20240101_COPERNICUS_catalogue_*``,
whatever generation). Each is built from OData + the CSV and pushed to the
per-day tag. Days that exist but changed upstream are ``cdsemeta-update
sync``'s job, so a regeneration (new file names) is no new work here. The registry is the only state, so the
worker can be stopped and restarted at will.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import signal
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from . import csvfile, geoparquet
from .config import COLLECTIONS, Config, before_launch
from .http import make_session
from .listing import CsvEntry, list_csvs
from .naming import day_tag_for, parquet_relpath, parse_tag, stem, tag_for
from .odata import Blocked, ODataClient
from .registry import Registry
from .transform import build_table
from .update import annotations

log = logging.getLogger("cdsemeta.worker")

BLOCKED_WAIT_MIN = 2 * 60
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
    df = csvfile.read_csv(csv_path, cfg.exclude)
    info = parse_tag(tag_for(entry.key))
    products = odata.fetch_day(COLLECTIONS[cfg.mission], info.platform,
                               datetime.strptime(info.day, "%Y%m%d").date(), df["Id"].tolist())
    table, geoms, missing = build_table(df, products)
    out = work / parquet_relpath(entry.key)
    out.parent.mkdir(parents=True, exist_ok=True)
    geoparquet.write(table, geoms, out)
    geoparquet.check(out, len(df))
    ann = annotations(entry.key, len(df), missing, cfg.mission, entry.md5, entry.last_modified)
    log.info("%s: %d rows, %d not in OData, %.1fs", entry.key, len(df), missing, time.monotonic() - t0)
    return out, ann


def todo_list(entries: list[CsvEntry], tags: set[str], order: str) -> list[CsvEntry]:
    """Entries without an artifact (under any tag), in (day, platform) order."""
    have = set()
    for t in tags:
        info = parse_tag(t)
        if info is not None:
            have.add((info.platform, info.day))
    todo = []
    for e in entries:
        info = parse_tag(stem(e.key))
        if info is None:
            log.warning("skipping %s: not a catalogue CSV name", e.key)
            continue
        if (info.platform, info.day) not in have:
            todo.append(e)
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
        early = [e for e in entries if (info := parse_tag(stem(e.key))) and before_launch(info.platform, info.day)]
        if early:
            log.info("skipping %d CSVs dated before their satellite's launch", len(early))
            entries = [e for e in entries if e not in set(early)]
        if cfg.recent_days:
            since = f"{date.today() - timedelta(days=cfg.recent_days):%Y%m%d}"
            entries = [e for e in entries if (info := parse_tag(stem(e.key))) is None or info.day >= since]
        if only:
            entries = [e for e in entries if e.key == only or tag_for(e.key) == only or day_tag_for(e.key) == only
                       or Path(e.key).name == only]
            if not entries:
                log.error("no CSV matches %r", only)
                return 1
        tags = set() if (no_push or only) else registry.tags()
        todo = todo_list(entries, tags, cfg.order)
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
            tag = day_tag_for(entry.key)
            try:
                log.info("[%d/%d] %s", i, len(todo), entry.key)
                out, ann = process(entry, cfg, session, odata, work)
                if out_dir is not None:
                    dest = out_dir / parquet_relpath(entry.key)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(out, dest)
                    log.info("wrote %s", dest)
                if not no_push:
                    registry.push(tag, out, ann, root=work)
                    log.info("pushed %s:%s", cfg.oci_repo, tag)
                blocked_wait = BLOCKED_WAIT_MIN
            except Stop:
                log.info("stopping; %s was not pushed and will be redone", entry.key)
                return 0
            except Blocked as e:
                # CDSE's WAF rejected us (see odata.Blocked). Through WARP we
                # share an exit address with other users, and the block on
                # it flickers on and off within minutes (same address,
                # same request: 200, 403, 200, 200). So retry soon, doubling
                # the wait while it persists. The CSV wasn't pushed, so the
                # next pass redoes it.
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
