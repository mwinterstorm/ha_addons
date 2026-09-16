# Maintenance

The app builds unmodified upstream v3.0.0 source on a pinned Python 3.11 Debian
base. This supports native amd64/aarch64 without depending on the single-manifest
upstream release image. Native dependency wheels are required, so unsupported
platform/dependency combinations fail the build instead of compiling unexpectedly.

`requirements.txt` is generated from v3.0.0's `Pipfile.lock` default section,
omitting its local `tconnectsync` entry. All external dependencies retain upstream
versions, environment markers and SHA-256 hashes. Source is executed directly,
like upstream's Docker entrypoint; package-metadata `--version` can therefore show
UNKNOWN while the HA app version and source archive are pinned to 3.0.0.

For updates, check GitHub's latest stable release, review its API/auth changes,
then update the Dockerfile source tag/checksum, base tag/digest as needed, exported
requirements, config version and changelog. Review upstream CLI options and cache
paths. Keep master-only options out of a stable release wrapper. In v3.0.0,
CACHE_CREDENTIALS is mistakenly also used to read a cache path: this wrapper leaves
it unset and configures HOME instead, preserving the upstream default behavior.

Run from this folder:

```sh
python3 -m unittest discover -s tests -v
python3 -m py_compile rootfs/run.py
```

Run an add-on config linter (for example frenck/action-addon-linter@v2 with this
folder as its path), then build on each architecture with a running Docker engine:

```sh
docker build -t ha-tconnectsync:local .
docker run --rm --network none --entrypoint python3 ha-tconnectsync:local /opt/upstream/main.py --help
```

For a runtime installation test, install the local app in HA, use check_login,
then preview, then sync with deliberately selected features. Verify new records
and timestamps in Nightscout. Check restart, cache persistence and backup/restore.
Only run live sync against a destination you intend to modify. No automated test
in this folder connects to Tandem or writes Nightscout records.

HA supplies its default init process; the launcher uses exec so signals reach the
application directly. Default startup is application and boot is auto. No build.yaml
is needed because the Dockerfile names an explicit multi-platform base.

The upstream software is MIT licensed; its LICENSE.md is retained in the source
inside the image. This wrapper is MIT licensed as well.
