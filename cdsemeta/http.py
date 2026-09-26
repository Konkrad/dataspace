"""Shared HTTP session with retries."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import __version__


def make_session(pool_size: int = 8) -> requests.Session:
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        status=8,
        backoff_factor=2,
        backoff_max=300,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers["User-Agent"] = f"cdsemeta/{__version__}"
    return s
