"""Mapping between catalogue CSV keys and OCI tags.

CSV keys (same layout for every Sentinel mission) look like ``S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20260901.csv``.
The trailing date is the catalogue generation date. CDSE regenerates the last
30 days daily and every file on the 2nd of each month, so the generation is
not part of the artifact tag: each (platform, sensing day) has one mutable tag,
``S1A_20240101`` (``day_tag``), overwritten when that day changes. The file
inside the artifact keeps the full CSV name, so it still records which
generation it was built from.

Older artifacts were tagged with the full file name
(``S1A_20240101_COPERNICUS_catalogue_20260901``, "legacy" tags); ``parse_tag``
reads both forms.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

TAG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$")
NAME_RE = re.compile(r"^(?P<platform>S\d[A-Z0-9]*)_(?P<day>\d{8})_COPERNICUS_catalogue_(?P<generated>\d{8})$")
DAY_TAG_RE = re.compile(r"^(?P<platform>S\d[A-Z0-9]*)_(?P<day>\d{8})$")


@dataclass(frozen=True, order=True)
class TagInfo:
    platform: str
    day: str
    generated: str  # "" for a per-day tag

    @property
    def day_tag(self) -> str:
        return day_tag(self.platform, self.day)


def stem(key: str) -> str:
    name = PurePosixPath(key).name
    return name[:-4] if name.lower().endswith(".csv") else name


def tag_for(key: str) -> str:
    """Legacy tag: the CSV file name without ``.csv`` (includes the generation)."""
    tag = stem(key)
    if not TAG_RE.match(tag):
        raise ValueError(f"cannot derive a valid OCI tag from {key!r}")
    return tag


def day_tag(platform: str, day: str) -> str:
    return f"{platform}_{day}"


def day_tag_for(key: str) -> str:
    """Per-day tag for a CSV key: ``S1A/2024/01/S1A_20240101_…_20260901.csv`` -> ``S1A_20240101``."""
    info = parse_tag(tag_for(key))
    if info is None:
        raise ValueError(f"not a catalogue CSV name: {key!r}")
    return info.day_tag


def parquet_name(key: str) -> str:
    return stem(key) + ".parquet"


def parquet_relpath(key: str) -> str:
    """Path of the GeoParquet inside the artifact; mirrors the CSV archive.

    ``S1A/2024/01/S1A_20240101_…csv`` -> ``S1A/2024/01/S1A_20240101_….parquet``
    """
    return str(PurePosixPath(key).with_name(parquet_name(key)))


def parse_tag(tag: str) -> TagInfo | None:
    """Parse a legacy tag or CSV stem, or a per-day tag (``generated == ""``)."""
    m = NAME_RE.match(tag)
    if m:
        return TagInfo(m["platform"], m["day"], m["generated"])
    m = DAY_TAG_RE.match(tag)
    if m:
        return TagInfo(m["platform"], m["day"], "")
    return None


def day_dir(platform: str, day: str) -> str:
    return f"{platform}/{day[:4]}/{day[4:6]}"


def relpath_for_tag(tag: str) -> str | None:
    """Inverse of ``tag_for`` + ``parquet_relpath`` for legacy tags.

    For a per-day tag the file name depends on the generation it was built
    from, so only the directory is known: use ``find_day_file``.
    """
    info = parse_tag(tag)
    if info is None or not info.generated:
        return None
    return f"{day_dir(info.platform, info.day)}/{tag}.parquet"


def find_day_file(root: Path, platform: str, day: str) -> Path | None:
    """The newest-generation GeoParquet for (platform, day) under ``root``
    (as laid out by ``oras pull``), or None."""
    files = sorted((root / day_dir(platform, day)).glob(f"{platform}_{day}_COPERNICUS_catalogue_*.parquet"))
    return files[-1] if files else None


def latest_per_day(tags: list[str]) -> list[str]:
    """Keep only the newest catalogue generation for each (platform, day).

    A per-day tag always wins over legacy tags of the same day: it is the
    one that gets updated.

    Returned in (day, platform) order -- not sorted tag-string order, which
    would group all of one satellite's days before the next (tags start with
    the platform code). merge() relies on (day, platform) order to
    concatenate same-day files from different satellites (e.g. S1A/S1B/S1C/
    S1D) in roughly chronological order without a separate sort.
    """
    best: dict[tuple[str, str], tuple[str, str]] = {}
    for t in tags:
        info = parse_tag(t)
        if info is None:
            continue
        k = (info.platform, info.day)
        rank = info.generated or "99999999"
        if k not in best or rank > best[k][0]:
            best[k] = (rank, t)
    return [tag for (platform, day), (_, tag) in sorted(best.items(), key=lambda kv: (kv[0][1], kv[0][0]))]
