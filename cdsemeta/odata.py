"""Batched lookups against the CDSE OData catalogue."""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import requests

log = logging.getLogger(__name__)

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
EXPAND = ("Attributes", "Assets", "Locations")
PAGE = 1000
MAX_WINDOW = 10_000


def _iso(t: datetime) -> str:
    return f"{t:%Y-%m-%dT%H:%M:%S}.{t.microsecond // 1000:03d}Z"


class Blocked(Exception):
    """CDSE's WAF rejected the request outright -- not a per-ID or rate issue.

    Shaped like ``{"status": "error", "data": {"message": "...violation...
    reference ID: ..."}}``, distinct from OData's own error shape
    (``{"error": {"code": ..., "message": ...}}``), so this is specifically
    a security-policy block, not an ordinary not-found/bad-request response.
    No Retry-After, no rate-limit wording -- retrying (with or without
    backoff) won't clear this; it needs CDSE support to lift it.
    """


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
        if r.status_code == 403:
            try:
                body = r.json()
            except ValueError:
                body = None
            if isinstance(body, dict) and body.get("status") == "error":
                raise Blocked(body.get("data", {}).get("message", "request rejected (403)"))
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

    def _window(self, base_filter: str, start: datetime, end: datetime) -> list[dict]:
        """All products sensed in [start, end), splitting the window if needed.

        CDSE stops paging at $skip=10000, so a query can return at most
        10000 + PAGE items -- the rest is silently dropped (measured: a
        12,416-product day came back as exactly 11,000). The first page
        carries @odata.count, so oversized windows are halved at no extra
        cost before paging on.
        """
        f = f"{base_filter} and ContentDate/Start ge {_iso(start)} and ContentDate/Start lt {_iso(end)}"
        params = [("$filter", f), ("$top", str(PAGE)), ("$orderby", "Id asc"), ("$count", "true")]
        params += [("$expand", e) for e in EXPAND]
        data = self._get(params)
        count = data.get("@odata.count")
        if count is not None and count > MAX_WINDOW and end - start > timedelta(minutes=1):
            mid = start + (end - start) / 2
            return self._window(base_filter, start, mid) + self._window(base_filter, mid, end)
        products = list(data.get("value", []))
        next_link = data.get("@odata.nextLink")
        while next_link:
            data = self._get(None, next_link)
            products += data.get("value", [])
            next_link = data.get("@odata.nextLink")
        if count is not None and len(products) < count:
            log.warning("window %s..%s: got %d of %d products", _iso(start), _iso(end), len(products), count)
        return products

    def fetch_day(self, collection: str, platform: str, day: date, ids: Iterable[str]) -> dict[str, dict]:
        """Return ``{id: product}`` for ``ids`` (one CSV: one platform, one day).

        Asks for the whole sensing day in pages of 1000 (~10x fewer, much
        shorter requests than 100-ID batches -- the batches are what got us
        WAF-blocked), then looks up any ID the day query didn't return with
        the per-ID query, so a product is only reported missing if both
        methods agree it's not in OData.
        """
        wanted = list(ids)
        wanted_set = set(wanted)
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        base = f"Collection/Name eq '{collection}' and startswith(Name,'{platform}_')"
        found = {p["Id"]: p for p in self._window(base, start, start + timedelta(days=1)) if p["Id"] in wanted_set}
        missing = [i for i in wanted if i not in found]
        if missing:
            found.update(self.fetch(missing))
        return found

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
