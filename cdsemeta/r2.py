"""Upload final merge/STAC outputs to Cloudflare R2 (S3-compatible).

Only the aggregate outputs (merge, STAC) go here. The worker's per-day
pushes stay on GHCR via ``registry.py`` -- unrelated code path, unchanged.

Unlike the OCI registry, R2 speaks the S3 API, so a "folder" of partition
files (see ``merge.py``'s per-month output) is directly readable by anything
that understands S3 prefixes -- DuckDB's ``read_parquet('s3://bucket/...')``,
``pyarrow.dataset``, etc. -- with no resolve-then-fetch dance required.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

log = logging.getLogger("cdsemeta.r2")

# R2 can expire multipart upload IDs under load. Raise the single-PUT threshold
# to 50 MB so most monthly files go as one request; use sequential 10 MB chunks
# for the rare larger files.
_TRANSFER_CFG = TransferConfig(
    multipart_threshold=500 << 20,   # 500 MB: nearly all monthly files fit in one PUT
    multipart_chunksize=50 << 20,    # 50 MB chunks for the rare larger files
    max_concurrency=1,
)


def client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("R2_ENDPOINT_URL") or
                     f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def upload(path: Path, key: str, bucket: str | None = None) -> None:
    bucket = bucket or os.environ["R2_BUCKET"]
    client().upload_file(str(path), bucket, key, Config=_TRANSFER_CFG)
    log.info("uploaded %s to s3://%s/%s", path, bucket, key)
