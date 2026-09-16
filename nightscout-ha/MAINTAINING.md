# Maintenance and validation

## Upgrade Nightscout

1. Read the official latest stable release and tagged README/Dockerfile. Do not
   follow `master`, `dev` or `latest` automatically.
2. Inspect the release image with `docker buildx imagetools inspect
   nightscout/cgm-remote-monitor:VERSION`. Confirm amd64 and arm64, working directory,
   launch command, Node UID, API_SECRET_FILE and driver compatibility.
3. Change Nightscout's Dockerfile tag/index digest and BUILD_VERSION default;
   increment `nightscout/config.yaml` version (`UPSTREAM-WRAPPER_REVISION`). Update
   changelog, source evidence and docs. No MongoDB rebuild is required.
4. Run static checks and the disposable smoke test below on both architectures.
   The GitHub workflow uses native x86 and ARM runners; ARM hosted runner availability
   can depend on repository/account plan. Do not substitute emulation for CPU checks.
5. Take a backup including both apps, install the new build, verify historical and
   fresh readings, clients, browser/WebSocket updates, and clean stop/start.
6. For local installs, copy updated source into `/addons`, refresh the app store,
   then use Update or the app's Rebuild action as offered. Repository installs need
   a commit and version bump before Supervisor can offer the update.

The digest pins application/base contents. MongoDB's small adapter installs `jq`
from Ubuntu during build, so the entire adapter image is not byte-for-byte
reproducible across package mirror changes. For distribution, build/test immutable
per-architecture images in CI and add a real registry `image:` template; this
repository intentionally does not refer to nonexistent published images.

## Upgrade MongoDB separately

Apply stable 8.0 patch upgrades deliberately: update tag/digest and wrapper version,
review MongoDB release notes, back up and test restore. Do not auto-advance the major
version. Major upgrades may require staged binary versions and featureCompatibilityVersion
changes. Database downgrade is not equivalent to switching an image tag: restore
a compatible pre-upgrade backup when required by MongoDB's documented process.
Keep enough disk space for backups and migrations.

## Tests

From this repository folder:

```sh
node --test tests/*.test.cjs
node --check nightscout/rootfs/start.cjs
node --check nightscout/rootfs/health.cjs
node --check mongodb/rootfs/init-user.js
node --check mongodb/rootfs/health.js
bash -n mongodb/rootfs/run.sh
shellcheck mongodb/rootfs/run.sh
python3 tests/smoke.py
```

The smoke test needs a running Linux-container Docker engine and suitable hardware.
It creates uniquely named disposable containers, a network and a database volume;
uses generated test secrets; checks authenticated upload/read, anonymous-read denial,
container-replacement persistence and password-change refusal; then removes its own
resources. It never uses the configured HA database or real glucose data. Pulling
images may consume several GB. Interrupted tests can leave uniquely prefixed
`ns-ha-test-` resources; inspect before removing them.

GitHub Actions runs configuration lint and smoke tests on amd64 and arm64. It does
not publish images or deploy anything. These tests complement, but do not replace,
HAOS verification: install, first boot, Watchdog behavior, host reboot, cold backup,
restore, uploader, HA integration, UI login, WebSockets and actual CPU compatibility.

## Licensing

The small adapter files in this repository are provided under MIT (LICENSE).
Nightscout remains upstream AGPL-3.0; MongoDB Server has its own SSPL terms and the
Docker image contains other separately licensed software. Follow their respective
licenses when distributing built images. Upstream source links and exact versions
are recorded in SOURCES.md; do not represent this as an official Nightscout or
Home Assistant-maintained app.
