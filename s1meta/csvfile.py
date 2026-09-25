"""Download and parse a catalogue CSV."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import requests

from .listing import CsvEntry


def download(session: requests.Session, url: str, entry: CsvEntry, dest: Path, timeout: int = 120) -> Path:
    md5 = hashlib.md5()
    tmp = dest.with_suffix(dest.suffix + ".part")
    with session.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
                md5.update(chunk)
    if entry.md5 and md5.hexdigest() != entry.md5:
        tmp.unlink(missing_ok=True)
        raise IOError(f"md5 mismatch for {entry.key}: got {md5.hexdigest()}, expected {entry.md5}")
    tmp.replace(dest)
    return dest


def read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, na_values=[""])
    if "Id" not in df.columns:
        raise ValueError(f"{path.name}: no 'Id' column (columns: {list(df.columns)})")
    df = df[df["Id"].notna()].drop_duplicates(subset="Id").reset_index(drop=True)
    return df
