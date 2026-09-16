# Upstream evidence — checked 16 September 2026

## Nightscout release and container

The live GitHub `releases/latest` API returned `v15.0.8`, published
2026-09-04T18:27:47Z, not a draft/prerelease. Search results still showed 15.0.7;
the live release record takes precedence.

- [Release 15.0.8](https://github.com/nightscout/cgm-remote-monitor/releases/tag/v15.0.8)
- [Live release API](https://api.github.com/repos/nightscout/cgm-remote-monitor/releases/latest)
- [Tagged Dockerfile](https://github.com/nightscout/cgm-remote-monitor/blob/v15.0.8/Dockerfile)
- [Tagged README](https://github.com/nightscout/cgm-remote-monitor/blob/v15.0.8/README.md)
- [Tagged package.json](https://github.com/nightscout/cgm-remote-monitor/blob/v15.0.8/package.json)
- [Tagged Compose example](https://github.com/nightscout/cgm-remote-monitor/blob/v15.0.8/docker-compose.yml)

The release adds native amd64/arm64 publishing and API_SECRET_FILE support.
Upstream uses Node 22 Alpine, builds frontend artifacts, prunes development
packages and runs as `node` (UID 1000). This wrapper inherits those built artifacts
rather than maintaining a fork or rebuilding npm dependencies on every HA install.

Live registry inspection confirmed `nightscout/cgm-remote-monitor:15.0.8` has Linux
amd64 and arm64 manifests. Pinned index:
`sha256:462266b2dac62f7fddc4656051039e53449f649d2a38851d6decd3a8b42e7301`.

## MongoDB requirements and choice

The tagged Nightscout README says MongoDB 4.4 or later, explicitly naming 5.0/6.0;
its Compose example still pins 5.0.32. These describe compatibility and an example,
not a recommendation to deploy an obsolete database branch. The tagged npm
manifest requests the MongoDB Node driver `^5.9.2`. Nightscout's
[15.0.7 release notes](https://github.com/nightscout/cgm-remote-monitor/releases/tag/v15.0.7)
explicitly describe MongoDB 8 and driver 5.x compatibility fixes.

This companion selects MongoDB **8.0.32** (8.0 supported stable branch), rather than
blindly reproducing Compose's 5.0 example. Compatibility is supported by the upstream
fixes; this particular HA wrapper and pairing still require the included runtime
smoke tests. Driver 5.x compatibility should not be confused with support for every
new MongoDB 8 feature.

- [MongoDB lifecycle policy](https://www.mongodb.com/legal/support-policy/lifecycles)
- [MongoDB driver compatibility](https://www.mongodb.com/docs/drivers/compatibility/?driver-language=javascript&javascript-driver-framework=nodejs)
- [MongoDB production notes](https://www.mongodb.com/docs/manual/administration/production-notes/)
- [MongoDB 8.0 standalone upgrade](https://www.mongodb.com/docs/v8.0/release-notes/8.0-upgrade-standalone/)
- [Official Mongo image implementation](https://github.com/docker-library/mongo/tree/master/8.0)

Live registry inspection confirmed `mongo:8.0.32-noble` with amd64 and arm64/v8.
Pinned index:
`sha256:f279edf46e7280f382cff860efae3f23a84c648ba54fdea4ef57b12867a1efea`.

MongoDB requires AVX-capable x86 hardware or ARMv8.2-A+. Aarch64 image availability
does not imply compatibility with every ARM HA device. Current production notes
also identify a kernel incompatibility covering Linux 6.19–7.0.13; check the actual
HAOS kernel against current vendor guidance before installation. We have not
inspected Mark's HA host or CPU.

## Home Assistant conventions

- [Configuration](https://developers.home-assistant.io/docs/apps/configuration/):
  config.yaml, explicit base images, build labels, persistent `/data`, cold backup.
- [Internal networking](https://developers.home-assistant.io/docs/apps/communication/):
  repository/slug hostnames and underscore-to-hyphen DNS conversion.
- [Local testing](https://developers.home-assistant.io/docs/apps/testing/):
  local `/addons` layout and Supervisor builds.
- [Repository layout](https://developers.home-assistant.io/docs/apps/repository/).
- [Ingress](https://developers.home-assistant.io/docs/apps/presentation/): proxy
  gateway support does not itself establish Nightscout compatibility with path prefixes.
- [App linter](https://github.com/frenck/action-addon-linter): config schema and
  non-community checks used locally; included as a CI action.

`startup: application`, `boot: auto`, and `init: true` are Supervisor defaults;
redundant declarations are omitted to satisfy the app linter. MongoDB overrides
startup to `services`. Supervisor's init reaps descendants. Neither app requests
Supervisor/Core API access, elevated privileges, protection-mode changes or host mounts.
No build.yaml is needed: explicit multi-platform FROM lines handle both architectures.
