# Sources and pins

Checked 16 September 2026:

- [Latest release API](https://api.github.com/repos/jwoglom/tconnectsync/releases/latest)
- [v3.0.0 release](https://github.com/jwoglom/tconnectsync/releases/tag/v3.0.0): required
  after the June 2026 Tandem Source API change; earlier versions no longer suffice.
- [Tagged source](https://github.com/jwoglom/tconnectsync/tree/v3.0.0)
- [Tagged configuration](https://github.com/jwoglom/tconnectsync/blob/v3.0.0/tconnectsync/secret.py)
- [Tagged CLI](https://github.com/jwoglom/tconnectsync/blob/v3.0.0/tconnectsync/__init__.py)
- [Tagged feature list](https://github.com/jwoglom/tconnectsync/blob/v3.0.0/tconnectsync/features.py)
- [Tagged dependency lock](https://github.com/jwoglom/tconnectsync/blob/v3.0.0/Pipfile.lock)
- [Tagged Dockerfile](https://github.com/jwoglom/tconnectsync/blob/v3.0.0/Dockerfile)
- [Upstream documentation](https://github.com/jwoglom/tconnectsync#readme)

Source archive SHA-256:
`96b39aac29150b320c41e9403b5fe798a375211a481bb8deadfa4f167c0c83bc`.

Python base `python:3.11-slim-bookworm` multi-platform digest:
`sha256:528257d48c1da0dcecc2e725d1ae34498d60c965f1241e39cd6a85a8859bdf84`.

The upstream `ghcr.io/jwoglom/tconnectsync/tconnectsync:v3.0.0` reference resolves
to a single image manifest, not a multi-platform index. We use the same upstream
Python baseline and its locked source dependencies to build for both HA architectures.
Version 3.0.0-2 patches only the shared date/history query code; see patches/ and MAINTAINING.md. Newer master documentation includes
retry settings absent from the pinned release; this wrapper does not advertise them.
