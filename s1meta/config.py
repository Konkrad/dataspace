"""Settings, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_bool(name: str, default: bool) -> bool:
    return _env(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    csv_list_url: str = "https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/"
    platforms: tuple[str, ...] = ("S1A", "S1B", "S1C", "S1D")
    odata_url: str = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    odata_batch: int = 100
    odata_workers: int = 4
    odata_rps: float = 3.0
    oci_repo: str = "ghcr.io/konkrad/s1-metadata"
    work_dir: Path = field(default_factory=lambda: Path("/tmp/s1meta"))
    poll_interval: int = 6 * 3600
    run_once: bool = False
    order: str = "asc"
    http_timeout: int = 120

    @classmethod
    def from_env(cls) -> "Config":
        d = cls()
        return cls(
            csv_list_url=_env("CSV_LIST_URL", d.csv_list_url),
            platforms=tuple(p.strip() for p in _env("PLATFORMS", ",".join(d.platforms)).split(",") if p.strip()),
            odata_url=_env("ODATA_URL", d.odata_url),
            odata_batch=int(_env("ODATA_BATCH", str(d.odata_batch))),
            odata_workers=int(_env("ODATA_WORKERS", str(d.odata_workers))),
            odata_rps=float(_env("ODATA_RPS", str(d.odata_rps))),
            oci_repo=_env("OCI_REPO", d.oci_repo),
            work_dir=Path(_env("WORK_DIR", str(d.work_dir))),
            poll_interval=int(_env("POLL_INTERVAL", str(d.poll_interval))),
            run_once=_env_bool("RUN_ONCE", d.run_once),
            order=_env("ORDER", d.order).lower(),
            http_timeout=int(_env("HTTP_TIMEOUT", str(d.http_timeout))),
        )
