ARG BUILD_ARCH=amd64
FROM ghcr.io/tobiasehlert/teslamateapi:1.25.0@sha256:6f7eea55c6475c2bc4639ee582dfec01d5cf618d30b0c7444020274a4786704e AS upstream

FROM ghcr.io/home-assistant/${BUILD_ARCH}-base:3.20

ARG BUILD_ARCH
ARG BUILD_VERSION=0.1.2
LABEL io.hass.type="addon" io.hass.arch="${BUILD_ARCH}" io.hass.version="${BUILD_VERSION}"

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /opt/app
COPY --from=upstream /opt/app/app /opt/app/app

COPY rootfs/run.sh /run.sh
RUN chmod 0755 /run.sh
ENTRYPOINT ["/run.sh"]
CMD []
