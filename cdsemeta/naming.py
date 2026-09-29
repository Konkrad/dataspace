"""Mapping between catalogue CSV keys and OCI tags.

CSV keys (same layout for every Sentinel mission) look like ``S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20260901.csv``.
The tag is the file name without ``.csv``, which is already a valid OCI tag.
The trailing date is the catalogue generation date: recent days get
regenerated, which produces a new file name and therefore a new tag.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath

TAG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$")
NAME_RE = re.compile(r"^(?P<platform>S\d[A-Z0-9]*)_(?P<day>\d{8})_COPERNICUS_catalogue_(?P<generated>\d{8})$")


@dataclass(frozen=True, order=True)
class TagInfo:
    platform: str
    day: str
    generated: str


def stem(key: str) -> str:
    name = PurePosixPath(key).name
    return name[:-4] if name.lower().endswith(".csv") else name


def tag_for(key: str) -> str:
    tag = stem(key)
    if not TAG_RE.match(tag):
        raise ValueError(f"cannot derive a valid OCI tag from {key!r}")
    return tag


def parquet_name(key: str) -> str:
    return stem(key) + ".parquet"


def parquet_relpath(key: str) -> str:
    """Path of the GeoParquet inside the artifact; mirrors the CSV archive.

    ``S1A/2024/01/S1A_20240101_…csv`` -> ``S1A/2024/01/S1A_20240101_….parquet``
    """
    return str(PurePosixPath(key).with_name(parquet_name(key)))


def parse_tag(tag: str) -> TagInfo | None:
    m = NAME_RE.match(tag)
    if not m:
        return None
    return TagInfo(m["platform"], m["day"], m["generated"])


def relpath_for_tag(tag: str) -> str | None:
    """Inverse of ``tag_for`` + ``parquet_relpath`` for catalogue tags."""
    info = parse_tag(tag)
    if info is None:
        return None
    return f"{info.platform}/{info.day[:4]}/{info.day[4:6]}/{tag}.parquet"


def latest_per_day(tags: list[str]) -> list[str]:
    """Keep only the newest catalogue generation for each (platform, day).

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
        if k not in best or info.generated > best[k][0]:
            best[k] = (info.generated, t)
    return [tag for (platform, day), (_, tag) in sorted(best.items(), key=lambda kv: (kv[0][1], kv[0][0]))]
