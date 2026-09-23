FROM ghcr.io/tobiasehlert/teslamateapi:1.25.0@sha256:6f7eea55c6475c2bc4639ee582dfec01d5cf618d30b0c7444020274a4786704e AS upstream

ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.20
FROM ${BUILD_FROM}

ARG BUILD_ARCH
ARG BUILD_VERSION=0.1.1
LABEL io.hass.type="addon" io.hass.arch="${BUILD_ARCH}" io.hass.version="${BUILD_VERSION}"

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /opt/app
COPY --from=upstream /opt/app/app /opt/app/app

COPY rootfs/run.sh /run.sh
RUN chmod 0755 /run.sh
ENTRYPOINT ["/run.sh"]
CMD []
