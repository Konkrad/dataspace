"""List the catalogue CSV files from the public Swift container.

``csv.dataspace.copernicus.eu`` is only an HTML view of this container; the
Swift JSON listing gives name, md5, size and modification time directly.
"""

from __future__ import annotations

from dataclasses import dataclass

import requests

PAGE_SIZE = 1000  # server-side maximum


@dataclass(frozen=True)
class CsvEntry:
    key: str  # e.g. S1A/2024/01/S1A_20240101_COPERNICUS_catalogue_20260901.csv
    md5: str
    size: int
    last_modified: str

    def url(self, base: str) -> str:
        return base.rstrip("/") + "/" + self.key


def list_csvs(session: requests.Session, base_url: str, platforms: tuple[str, ...], timeout: int = 120) -> list[CsvEntry]:
    base = base_url.rstrip("/") + "/"
    out: list[CsvEntry] = []
    for platform in platforms:
        marker = ""
        while True:
            params = {"format": "json", "prefix": f"{platform}/", "limit": PAGE_SIZE}
            if marker:
                params["marker"] = marker
            r = session.get(base, params=params, timeout=timeout)
            r.raise_for_status()
            page = r.json() if r.content else []
            if not page:
                break
            for o in page:
                if o["name"].lower().endswith(".csv"):
                    out.append(CsvEntry(o["name"], o.get("hash", ""), int(o.get("bytes", 0)), o.get("last_modified", "")))
            marker = page[-1]["name"]
    return out
