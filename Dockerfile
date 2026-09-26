FROM python:3.12-slim

ARG ORAS_VERSION=1.2.3
# cogp (https://github.com/Kanahiro/cloud-optimized-geoparquet) reorders the
# merged GeoParquet into a progressive-rendering (COGP) layout for visual apps.
ARG COGP_VERSION=2.0.1
ARG TARGETARCH
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates tini; rm -rf /var/lib/apt/lists/*; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
      amd64) oras_sha=b4efc97a91f471f323f193ea4b4d63d8ff443ca3aab514151a30751330852827; \
             cogp_target=x86_64-unknown-linux-gnu; \
             cogp_sha=66e26f29fb1efb43038edd4e0cb816dffbf5fdb1a6bb55408c1218cf387af89e ;; \
      arm64) oras_sha=90e24e234dc6dffe73365533db66fd14449d2c9ae77381081596bf92f40f6b82; \
             cogp_target=aarch64-unknown-linux-gnu; \
             cogp_sha=15711597d71d4b67762d38e3e2adb29731c250396c975f1c6315200ee562b8bc ;; \
      *) echo "unsupported arch $arch"; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/oras.tgz "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_${arch}.tar.gz"; \
    echo "$oras_sha  /tmp/oras.tgz" | sha256sum -c -; \
    tar -xzf /tmp/oras.tgz -C /usr/local/bin oras; rm /tmp/oras.tgz; oras version; \
    curl -fsSL -o /usr/local/bin/cogp "https://github.com/Kanahiro/cloud-optimized-geoparquet/releases/download/v${COGP_VERSION}/cogp-v${COGP_VERSION}-${cogp_target}"; \
    echo "$cogp_sha  /usr/local/bin/cogp" | sha256sum -c -; \
    chmod +x /usr/local/bin/cogp; cogp --version

WORKDIR /app
COPY pyproject.toml ./
COPY cdsemeta ./cdsemeta
RUN pip install --no-cache-dir . && useradd -m -u 1000 worker && mkdir -p /work && chown worker /work
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

USER worker
ENV WORK_DIR=/work PYTHONUNBUFFERED=1
ENTRYPOINT ["tini", "--", "docker-entrypoint.sh"]
CMD ["python", "-m", "cdsemeta.worker"]
