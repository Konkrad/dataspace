"""Clear Cloudflare's cached copies of files just uploaded to R2.

The zone caches every file at the edge for a day ("dataspace: cache
everything"). Without a purge a rewritten month stays the old version there,
while HEAD requests already see the new size, so readers get HTTP 416 or mixed
versions. Needs ``CLOUDFLARE_API_TOKEN`` (Zone > Cache Purge > Purge on the
zone) and ``CLOUDFLARE_ZONE_ID``; without them it warns and does nothing.
"""

from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger("cdsemeta.cfcache")

API = "https://api.cloudflare.com/client/v4"
PER_CALL = 30  # Cloudflare's limit of URLs per purge request
DEFAULT_BASE_URL = "https://dataspace.konrad.online"


def configured() -> bool:
    return bool(os.environ.get("CLOUDFLARE_API_TOKEN") and os.environ.get("CLOUDFLARE_ZONE_ID"))


def purge(keys: list[str], base_url: str | None = None) -> None:
    """Purge the public URLs of these bucket keys (``a.parquet/2026-10.parquet``)."""
    if not keys:
        return
    if not configured():
        log.warning("CLOUDFLARE_API_TOKEN/CLOUDFLARE_ZONE_ID not set: %d uploaded files stay cached "
                    "in their old version for up to a day", len(keys))
        return
    base = (base_url or os.environ.get("PUBLIC_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
    urls = sorted({f"{base}/{k.lstrip('/')}" for k in keys})
    zone, token = os.environ["CLOUDFLARE_ZONE_ID"], os.environ["CLOUDFLARE_API_TOKEN"]
    for i in range(0, len(urls), PER_CALL):
        batch = urls[i:i + PER_CALL]
        r = requests.post(f"{API}/zones/{zone}/purge_cache", json={"files": batch},
                          headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if r.status_code != 200 or not r.json().get("success"):
            raise RuntimeError(f"Cloudflare purge failed (HTTP {r.status_code}): {r.text[:300]}")
    log.info("purged %d URLs from Cloudflare's cache", len(urls))
