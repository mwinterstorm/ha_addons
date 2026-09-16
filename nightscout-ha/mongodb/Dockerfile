FROM mongo:8.0.32-noble@sha256:f279edf46e7280f382cff860efae3f23a84c648ba54fdea4ef57b12867a1efea
ARG BUILD_ARCH
ARG BUILD_VERSION=8.0.32-1
LABEL io.hass.type="app" io.hass.arch="${BUILD_ARCH}" io.hass.version="${BUILD_VERSION}"
RUN apt-get update && apt-get install -y --no-install-recommends jq \
    && rm -rf /var/lib/apt/lists/*
COPY rootfs/run.sh /usr/local/bin/ha-mongo
COPY rootfs/health.js /opt/ha-health.js
COPY rootfs/init-user.js /docker-entrypoint-initdb.d/10-nightscout.js
RUN chmod 0755 /usr/local/bin/ha-mongo
ENTRYPOINT ["/usr/local/bin/ha-mongo"]
CMD []
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 CMD ["mongosh", "--quiet", "--norc", "/opt/ha-health.js"]
