"""Read single CSVs out of CDSE's catalogue zip without downloading all of it.

``CopernicusCatalogueCSV.zip`` (13 GB, every mission, rebuilt daily around
03:00 UTC) sits next to the CSVs in the Swift container. A zip keeps its
directory at the end, and the server supports Range requests, so reading the
directory costs ~4 MB and each CSV costs about its compressed size (S1:
1.9 GB, S2: 7.9 GB for every day).

All reads carry ``If-Match`` with the ETag seen at open: if CDSE rewrites
the zip mid-run, the next read fails with ``ZipChanged`` instead of mixing
two versions.
"""

from __future__ import annotations

import io
import shutil
import zipfile
from pathlib import Path, PurePosixPath

import requests

ZIP_NAME = "CopernicusCatalogueCSV.zip"


class ZipChanged(RuntimeError):
    """The zip was rewritten while we were reading it; start over."""


class HttpFile(io.RawIOBase):
    """Seekable, read-only view of a URL through Range requests."""

    def __init__(self, session: requests.Session, url: str, timeout: int = 120):
        self.session, self.url, self.timeout, self.pos = session, url, timeout, 0
        r = session.head(url, timeout=timeout)
        r.raise_for_status()
        self.size = int(r.headers["Content-Length"])
        self.etag = r.headers.get("ETag", "")

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = 0) -> int:
        self.pos = {0: offset, 1: self.pos + offset, 2: self.size + offset}[whence]
        return self.pos

    def readinto(self, b) -> int:
        n = min(len(b), self.size - self.pos)
        if n <= 0:
            return 0
        headers = {"Range": f"bytes={self.pos}-{self.pos + n - 1}"}
        if self.etag:
            headers["If-Match"] = self.etag
        r = self.session.get(self.url, headers=headers, timeout=self.timeout)
        if r.status_code == 412:
            raise ZipChanged(f"{self.url} changed since it was opened (ETag {self.etag})")
        r.raise_for_status()
        if r.status_code != 206:
            raise IOError(f"{self.url}: expected a partial response, got HTTP {r.status_code}")
        data = r.content
        b[:len(data)] = data
        self.pos += len(data)
        return len(data)


class CatalogueZip:
    """The catalogue zip (a URL, or a local file for tests and reruns).

    Member keys are given like the Swift listing's:
    ``S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20261001.csv``.
    """

    def __init__(self, source: str | Path, session: requests.Session | None = None, timeout: int = 120):
        if isinstance(source, Path) or not str(source).startswith(("http://", "https://")):
            raw = open(source, "rb")
            self.etag = ""
        else:
            http = HttpFile(session or requests.Session(), str(source), timeout)
            raw, self.etag = http, http.etag
        self.zip = zipfile.ZipFile(io.BufferedReader(raw, buffer_size=1 << 20))
        self.members: dict[str, zipfile.ZipInfo] = {}
        for info in self.zip.infolist():
            if info.filename.lower().endswith(".csv"):
                parts = PurePosixPath(info.filename).parts
                self.members["/".join(parts[1:])] = info  # drop the top folder (CatalogueVolume/)

    @classmethod
    def for_listing(cls, csv_list_url: str, session: requests.Session, timeout: int = 120) -> "CatalogueZip":
        return cls(csv_list_url.rstrip("/") + "/" + ZIP_NAME, session, timeout)

    def keys(self, platform: str) -> list[str]:
        return sorted(k for k in self.members if k.startswith(f"{platform}/"))

    def extract(self, key: str, dest: Path) -> Path:
        """Write one member to ``dest`` (checked against the zip's CRC)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        with self.zip.open(self.members[key]) as src, open(tmp, "wb") as out:
            shutil.copyfileobj(src, out, 1 << 20)
        tmp.replace(dest)
        return dest
