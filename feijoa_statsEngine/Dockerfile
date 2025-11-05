ARG BUILD_FROM=ghcr.io/home-assistant/amd64-addon-base:latest
ARG BUILD_ARCH=amd64

FROM ${BUILD_FROM}

ENV \
  LANG=C.UTF-8 \
  PYTHONUNBUFFERED=1 \
  PIP_NO_CACHE_DIR=1

# Install runtime dependencies (Alpine base).
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-virtualenv \
    curl \
    ca-certificates \
    mariadb-client

# supercronic (arch-aware download)
ARG SUPERCRONIC_VERSION=v0.2.29
RUN set -eo pipefail \
  && case "${BUILD_ARCH:-amd64}" in \
       "amd64") SUPERCRONIC_BIN="supercronic-linux-amd64";; \
       "aarch64") SUPERCRONIC_BIN="supercronic-linux-arm64";; \
       "armhf") SUPERCRONIC_BIN="supercronic-linux-arm";; \
       "armv7") SUPERCRONIC_BIN="supercronic-linux-arm";; \
       "i386") SUPERCRONIC_BIN="supercronic-linux-386";; \
       *) echo "Unsupported build arch: ${BUILD_ARCH}" >&2; exit 1;; \
     esac \
  && curl -fsSL \
       "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/${SUPERCRONIC_BIN}" \
       -o /usr/local/bin/supercronic \
  && chmod +x /usr/local/bin/supercronic

# Application & service files
COPY rootfs /

# Python virtual environment for app dependencies.
ENV VIRTUAL_ENV=/opt/feijoa-venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN python3 -m venv "${VIRTUAL_ENV}"

RUN if [ -f /usr/local/share/feijoa/requirements.txt ] && \
       [ -s /usr/local/share/feijoa/requirements.txt ]; then \
      pip install --no-cache-dir -r /usr/local/share/feijoa/requirements.txt; \
    fi

# Ensure service scripts are executable.
RUN chmod +x \
      /etc/cont-init.d/10-db-bootstrap.sh \
      /etc/services.d/importer/run \
      /usr/local/share/feijoa/import.sh \
      /usr/local/share/feijoa/s3_contributions_to_sql.py
