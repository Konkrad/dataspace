"""Batched lookups against the CDSE OData catalogue."""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable

import requests

log = logging.getLogger(__name__)

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
EXPAND = ("Attributes", "Assets", "Locations")


class RateLimiter:
    """Spaces calls so that at most ``rps`` start per second across threads."""

    def __init__(self, rps: float):
        self.interval = 1.0 / rps if rps > 0 else 0.0
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self) -> None:
        if not self.interval:
            return
        with self.lock:
            now = time.monotonic()
            at = max(now, self.next_at)
            self.next_at = at + self.interval
        if at > now:
            time.sleep(at - now)


class ODataClient:
    def __init__(self, session: requests.Session, url: str, batch: int = 100, workers: int = 4,
                 rps: float = 3.0, timeout: int = 120):
        self.session = session
        self.url = url
        self.batch = batch
        self.workers = workers
        self.timeout = timeout
        self.limiter = RateLimiter(rps)

    def _get(self, params: list[tuple[str, str]] | None, url: str | None = None) -> dict:
        self.limiter.wait()
        r = self.session.get(url or self.url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _query(self, ids: list[str]) -> list[dict]:
        params = [("$filter", " or ".join(f"Id eq {i}" for i in ids)), ("$top", str(len(ids)))]
        params += [("$expand", e) for e in EXPAND]
        data = self._get(params)
        products = list(data.get("value", []))
        next_link = data.get("@odata.nextLink")
        while next_link:
            data = self._get(None, next_link)
            products += data.get("value", [])
            next_link = data.get("@odata.nextLink")
        return products

    def _fetch_batch(self, ids: list[str]) -> list[dict]:
        try:
            return self._query(ids)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            # A 4xx other than 429 is about the request itself: split it up to
            # isolate the offending ID instead of failing the whole file.
            if status is None or not (400 <= status < 500) or status == 429:
                raise
            if len(ids) == 1:
                log.warning("OData rejected id %s (HTTP %s); treating as not found", ids[0], status)
                return []
            mid = len(ids) // 2
            return self._fetch_batch(ids[:mid]) + self._fetch_batch(ids[mid:])

    def fetch(self, ids: Iterable[str]) -> dict[str, dict]:
        """Return ``{id: product}`` for all IDs OData knows about."""
        valid = [i for i in ids if UUID_RE.match(i)]
        batches = [valid[i:i + self.batch] for i in range(0, len(valid), self.batch)]
        out: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for products in pool.map(self._fetch_batch, batches):
                for p in products:
                    out[p["Id"]] = p
        return out
