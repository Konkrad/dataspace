"""Reorder a GeoParquet file into a COGP layout for progressive map rendering.

Wraps the ``cogp`` CLI (https://github.com/Kanahiro/cloud-optimized-geoparquet),
vendored in the image like ``oras``. A COGP file is still ordinary GeoParquet
1.1, so this is meant to be the last write to a file: anything that adds or
changes columns afterwards without rerunning this would leave the `geo.lod`
metadata describing stale row-group boundaries.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def convert_to_cogp(path: Path, cogp_bin: str = "cogp") -> None:
    """Reorder ``path`` in place into a COGP layout."""
    tmp = path.with_suffix(path.suffix + ".cogp.tmp")
    res = subprocess.run([cogp_bin, "convert", str(path), str(tmp)], capture_output=True, text=True)
    if res.returncode != 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"cogp convert failed ({res.returncode}): {res.stderr.strip() or res.stdout.strip()}")
    tmp.replace(path)
