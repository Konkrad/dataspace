FROM python:3.12-slim

ARG ORAS_VERSION=1.2.3
ARG TARGETARCH
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates tini; rm -rf /var/lib/apt/lists/*; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
      amd64) sha=b4efc97a91f471f323f193ea4b4d63d8ff443ca3aab514151a30751330852827 ;; \
      arm64) sha=90e24e234dc6dffe73365533db66fd14449d2c9ae77381081596bf92f40f6b82 ;; \
      *) echo "unsupported arch $arch"; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/oras.tgz "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_${arch}.tar.gz"; \
    echo "$sha  /tmp/oras.tgz" | sha256sum -c -; \
    tar -xzf /tmp/oras.tgz -C /usr/local/bin oras; rm /tmp/oras.tgz; oras version

WORKDIR /app
COPY pyproject.toml ./
COPY cdsemeta ./cdsemeta
RUN pip install --no-cache-dir . && useradd -m -u 1000 worker && mkdir -p /work && chown worker /work
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

USER worker
ENV WORK_DIR=/work PYTHONUNBUFFERED=1
ENTRYPOINT ["tini", "--", "docker-entrypoint.sh"]
CMD ["python", "-m", "cdsemeta.worker"]
