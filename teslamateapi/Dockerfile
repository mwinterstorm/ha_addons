ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.20
FROM ${BUILD_FROM}

ARG BUILD_ARCH
ARG BUILD_VERSION=0.1.0
ARG UPSTREAM_VERSION=1.25.0
LABEL io.hass.type="addon" io.hass.arch="${BUILD_ARCH}" io.hass.version="${BUILD_VERSION}"

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S teslamateapi \
    && adduser -S -G teslamateapi teslamateapi

WORKDIR /opt/app
ADD https://github.com/tobiasehlert/teslamateapi/releases/download/v${UPSTREAM_VERSION}/teslamateapi_${UPSTREAM_VERSION}_linux_${BUILD_ARCH}.tar.gz /tmp/teslamateapi.tar.gz
RUN tar -xzf /tmp/teslamateapi.tar.gz -C /opt/app \
    && rm /tmp/teslamateapi.tar.gz \
    && chmod 0555 /opt/app/teslamateapi

COPY rootfs/ /
ENTRYPOINT ["/usr/bin/with-contenv", "bashio", "/run.sh"]
CMD []
