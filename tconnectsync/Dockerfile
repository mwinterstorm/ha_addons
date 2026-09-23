# Build upstream source for both platforms; the upstream release image is not multi-arch.
FROM python:3.11-slim-bookworm@sha256:528257d48c1da0dcecc2e725d1ae34498d60c965f1241e39cd6a85a8859bdf84
ARG BUILD_ARCH
ARG BUILD_VERSION=3.0.0-3
LABEL io.hass.type="app" io.hass.arch="${BUILD_ARCH}" io.hass.version="${BUILD_VERSION}"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ADD https://codeload.github.com/jwoglom/tconnectsync/tar.gz/refs/tags/v3.0.0 /tmp/upstream.tar.gz
RUN echo '96b39aac29150b320c41e9403b5fe798a375211a481bb8deadfa4f167c0c83bc  /tmp/upstream.tar.gz' | sha256sum -c - \
    && mkdir -p /opt/upstream \
    && tar -xzf /tmp/upstream.tar.gz --strip-components=1 -C /opt/upstream \
    && rm /tmp/upstream.tar.gz
COPY requirements.txt /opt/ha/requirements.txt
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r /opt/ha/requirements.txt
COPY patches/ /opt/ha/patches/
RUN python3 /opt/ha/patches/apply.py /opt/upstream/tconnectsync/nightscout.py
COPY rootfs/ /opt/ha/
ENTRYPOINT ["python3", "-u", "/opt/ha/run.py"]
CMD []
