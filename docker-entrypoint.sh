#!/bin/sh
set -eu
# Log in to the registry once; oras keeps the credentials in ~/.docker/config.json.
if [ -n "${GHCR_TOKEN:-}" ]; then
    registry="${OCI_REPO:-${OCI_BASE:-ghcr.io/konkrad/dataspace}}"
    registry="${registry%%/*}"
    printf '%s' "$GHCR_TOKEN" | oras login "$registry" -u "${GHCR_USER:?GHCR_USER must be set}" --password-stdin >/dev/null
fi
exec "$@"
