"""Thin wrapper around the ``oras`` CLI.

Authentication is left to ``oras login`` (done by the container entrypoint),
so no credentials pass through this module.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

ARTIFACT_TYPE = "application/vnd.cdsemeta.geoparquet.v1"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
# GHCR links a package to the repository named in this annotation.
SOURCE_REPO_URL = "https://github.com/konkrad/dataspace"
SOURCE_ANNOTATION = "org.opencontainers.image.source"


class RegistryError(RuntimeError):
    pass


class Registry:
    def __init__(self, repo: str, oras: str = "oras", extra_args: list[str] | None = None):
        self.repo = repo
        self.oras = oras
        # e.g. ["--plain-http"] for a local test registry
        self.extra_args = extra_args if extra_args is not None else os.environ.get("ORAS_EXTRA_ARGS", "").split()

    def _run(self, args: list[str], cwd: Path | None = None) -> str:
        cmd = [self.oras, *args, *self.extra_args]
        res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RegistryError(f"{' '.join(args[:2])} failed ({res.returncode}): {res.stderr.strip() or res.stdout.strip()}")
        return res.stdout

    def tags(self) -> set[str]:
        try:
            out = self._run(["repo", "tags", self.repo])
        except RegistryError as e:
            # A repository that has never been pushed to does not exist yet.
            msg = str(e).lower()
            if any(m in msg for m in ("not found", "name unknown", "name_unknown")):
                return set()
            raise
        return {line.strip() for line in out.splitlines() if line.strip()}

    def push(self, tag: str, file: Path, annotations: dict[str, str], root: Path | None = None) -> None:
        """Push ``file`` as the only layer of ``repo:tag``.

        Inside the artifact the file is stored at its path relative to
        ``root`` (default: just its name), and ``oras pull`` recreates that
        path.

        ``oras push`` uploads blobs first and the manifest last, so a tag only
        ever appears once the whole artifact is in the registry.
        """
        ann = {SOURCE_ANNOTATION: SOURCE_REPO_URL, **annotations}
        manifest_ann = {"$manifest": {k: str(v) for k, v in ann.items()}}
        root = root or file.parent
        rel = file.relative_to(root).as_posix()
        ann_file = root / f"{file.name}.annotations.json"
        ann_file.write_text(json.dumps(manifest_ann))
        self._run([
            "push", f"{self.repo}:{tag}",
            "--artifact-type", ARTIFACT_TYPE,
            "--annotation-file", str(ann_file),
            "--disable-path-validation",
            f"{rel}:{PARQUET_MEDIA_TYPE}",
        ], cwd=root)

    def pull(self, tag: str, dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        self._run(["pull", f"{self.repo}:{tag}", "-o", str(dest)])
