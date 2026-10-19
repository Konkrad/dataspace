"""ID files: what the per-day artifacts were built from, to find what changed.

One small Parquet per platform and year, ``<mission>_ids/<PLATFORM>/<year>.parquet``
(tag ``ids-<year>-<s1>-<s1a>``), with three string columns:

- ``Name``: the product name from the CSV (unique, like ``Id``, but smaller);
- ``ModificationDate``: as the CSV has it;
- ``day``: the sensing day, from the CSV's file name. Not derivable from the
  name in general (AUX orbit files carry their creation date first; S5P
  orbits that cross midnight carry the previous day), and needed to find
  the day file a removed product is in.

Diffing a new month's CSV rows against this gives the days to look at;
the exact per-product changes then come from comparing each of those days'
artifact with its CSV (``update.py``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import __version__
from .naming import parse_tag
from .registry import Registry, RegistryError
from .transform import parse_ts

log = logging.getLogger(__name__)

SCHEMA = pa.schema([("Name", pa.string()), ("ModificationDate", pa.string()), ("day", pa.string())])
MISSION_SHORT = {"sentinel-1": "s1", "sentinel-2": "s2", "sentinel-3": "s3", "sentinel-5p": "s5p"}


def tag(mission: str, platform: str, year: str) -> str:
    return f"ids-{year}-{MISSION_SHORT.get(mission, mission)}-{platform.lower()}"


def canon_ts(v) -> str | None:
    """A timestamp as ``YYYY-MM-DDTHH:MM:SS.mmm`` (UTC, ms): OData has µs, the CSV ms, and the
    CSV's own spelling can vary, so ID files always store this form."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        t = v if isinstance(v, datetime) else parse_ts(v)
    except ValueError:
        return str(v)
    if t is None:
        return None
    t = t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)
    return f"{t:%Y-%m-%dT%H:%M:%S}.{t.microsecond // 1000:03d}"


def table_for(df: pd.DataFrame, day: str) -> pa.Table:
    """ID rows for one CSV (as read by ``csvfile.read_csv``)."""
    names = [None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v) for v in df["Name"]]
    return pa.table({"Name": names, "ModificationDate": [canon_ts(v) for v in df["ModificationDate"]],
                     "day": [day] * len(df)}, schema=SCHEMA)


def table_from_artifacts(files: list[Path], exclude: tuple[str, ...] = ()) -> pa.Table:
    """ID rows for artifacts (``S1A_<day>_…parquet``): what they were built from,
    less names matching ``exclude``. Those rows may still be in the artifact; the
    CSV doesn't list them either, so they must not show up as removed."""
    parts = []
    pattern = "|".join(f"(?:{e})" for e in exclude)
    for f in files:
        t = pq.read_table(f, columns=["name", "modification_date"])
        if pattern:
            t = t.filter(pc.invert(pc.match_substring_regex(pc.fill_null(t.column("name"), ""), pattern)))
        day = parse_tag(f.stem).day
        parts.append(pa.table({"Name": t.column("name").cast(pa.string()),
                               "ModificationDate": [canon_ts(v) for v in t.column("modification_date").to_pylist()],
                               "day": [day] * t.num_rows}, schema=SCHEMA))
    return pa.concat_tables(parts) if parts else empty()


def empty() -> pa.Table:
    return SCHEMA.empty_table()


def read(path: Path | None) -> pa.Table:
    return pq.read_table(path, schema=SCHEMA) if path is not None and path.exists() else empty()


def write(table: pa.Table, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = table.sort_by([("day", "ascending")])
    tmp = path.with_suffix(".parquet.part")
    pq.write_table(table, tmp, compression="zstd", compression_level=9)
    tmp.replace(path)
    return path


def changes(old: pa.Table, new: pa.Table) -> pd.DataFrame:
    """Per-day counts of ``added`` / ``removed`` / ``modified`` names.

    A product that moved to another day counts as removed from the old day
    and added to the new one. Only days with at least one change appear.
    """
    con = duckdb.connect()
    con.register("o", old)
    con.register("n", new)
    return con.execute("""
        WITH j AS (SELECT o."Name" AS o_name, n."Name" AS n_name, o.day AS o_day, n.day AS n_day,
                          o."ModificationDate" AS o_mod, n."ModificationDate" AS n_mod
                   FROM o FULL OUTER JOIN n ON o."Name" = n."Name"),
        c AS (
            SELECT n_day AS day, 'added' AS change FROM j WHERE n_name IS NOT NULL
                AND (o_name IS NULL OR o_day <> n_day)
            UNION ALL
            SELECT o_day, 'removed' FROM j WHERE o_name IS NOT NULL
                AND (n_name IS NULL OR o_day <> n_day)
            UNION ALL
            SELECT n_day, 'modified' FROM j WHERE o_day = n_day AND o_mod IS DISTINCT FROM n_mod)
        SELECT day, count(*) FILTER (change = 'added') AS added,
               count(*) FILTER (change = 'removed') AS removed,
               count(*) FILTER (change = 'modified') AS modified
        FROM c GROUP BY day ORDER BY day
    """).df()


class IdFiles:
    """Local copies of a mission's ID files under ``root/<mission>_ids/``."""

    def __init__(self, root: Path, mission: str):
        self.root = Path(root)
        self.mission = mission
        self.dir = self.root / f"{mission}_ids"

    def path(self, platform: str, year: str) -> Path:
        return self.dir / platform / f"{year}.parquet"

    def pull(self, registry: Registry, platform: str, year: str) -> Path | None:
        """Pull this platform-year's file; None if there is none yet."""
        try:
            registry.pull(tag(self.mission, platform, year), self.root)
        except RegistryError as e:
            msg = str(e).lower()
            if any(m in msg for m in ("not found", "name unknown", "name_unknown", "manifest unknown")):
                return None
            raise
        p = self.path(platform, year)
        return p if p.exists() else None

    def push(self, registry: Registry, platform: str, year: str) -> None:
        p = self.path(platform, year)
        registry.push(tag(self.mission, platform, year), p, {
            "org.opencontainers.image.title": p.relative_to(self.root).as_posix(),
            "cdsemeta.rows": str(pq.ParquetFile(p).metadata.num_rows),
            "cdsemeta.platform": platform,
            "cdsemeta.year": year,
            "cdsemeta.mission": self.mission,
            "cdsemeta.version": __version__,
        }, root=self.root)
        log.info("pushed %s", tag(self.mission, platform, year))
