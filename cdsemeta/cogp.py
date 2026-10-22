"""Reorder a GeoParquet file into a COGP layout for progressive map rendering.

Wraps the ``cogp`` CLI (https://github.com/Kanahiro/cloud-optimized-geoparquet),
vendored in the image like ``oras``. A COGP file is still ordinary GeoParquet
1.1, so this is meant to be the last write to a file: anything that adds or
changes columns afterwards without rerunning this would leave the `geo.lod`
metadata describing stale row-group boundaries.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)


def convert_to_cogp(path: Path, cogp_bin: str = "cogp") -> None:
    """Reorder ``path`` in place into a COGP layout. Skips with a warning if
    the binary is not found (vendored in the Docker image; not required locally
    -- the file is still valid GeoParquet, just without progressive loading)."""
    if not shutil.which(cogp_bin):
        log.warning("cogp not found, skipping COGP conversion for %s", path.name)
        return
    tmp = path.with_suffix(path.suffix + ".cogp.tmp")
    res = subprocess.run([cogp_bin, "convert", str(path), str(tmp)], capture_output=True, text=True)
    if res.returncode != 0:
        tmp.unlink(missing_ok=True)
        msg = res.stderr.strip() or res.stdout.strip()
        if "No space left on device" in msg:
            raise RuntimeError(f"cogp convert failed ({res.returncode}): {msg}")
        log.warning("cogp convert failed for %s (%s): %s -- uploading without COGP layout",
                    path.name, res.returncode, msg[:120])
        return
    tmp.replace(path)
