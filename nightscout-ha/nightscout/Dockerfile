# Multi-platform index digest; upstream application is used without patches.
FROM nightscout/cgm-remote-monitor:15.0.8@sha256:462266b2dac62f7fddc4656051039e53449f649d2a38851d6decd3a8b42e7301
ARG BUILD_ARCH
ARG BUILD_VERSION=15.0.8-1
LABEL io.hass.type="app" io.hass.arch="${BUILD_ARCH}" io.hass.version="${BUILD_VERSION}"
USER root
COPY rootfs/ /opt/ha/
WORKDIR /opt/app
ENTRYPOINT ["node", "/opt/ha/start.cjs"]
CMD []
HEALTHCHECK --interval=30s --timeout=10s --start-period=300s --retries=3 CMD ["node", "/opt/ha/health.cjs"]
