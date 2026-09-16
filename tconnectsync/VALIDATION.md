# Validation — 16 September 2026

Passed:

- GitHub's live latest-release endpoint returned v3.0.0 (published 1 July 2026).
- Release source downloaded and SHA-256 verified; checksum is enforced during build.
- Python base-image index verified to include amd64 and arm64.
- All locked external dependencies resolved as hash-verified binary wheels for
  Linux CPython 3.11 on both amd64 and aarch64 (manylinux targets through glibc 2.36).
- Upstream dependencies installed into an isolated local Python 3.13 environment;
  pip check passed and upstream --help ran successfully. This is an import/CLI check,
  not execution in the container's Python 3.11 runtime.
- Actual upstream argument parser accepts all three launcher modes; importing its
  configuration confirms credential caching is enabled at the expected persistent
  HOME path. These checks do not log in or make API calls.
- Five wrapper tests pass, including feature selection, preview/login arguments,
  invalid options, safe validation messages, privilege drop and exec environment.
- Python syntax checks, YAML lint and Frenck's v2 add-on config linter pass.

Not verified:

- Docker build/run: local Docker daemon unavailable at the OrbStack socket.
- Home Assistant OS install, shutdown, host reboot and cache backup/restore.
- Mark's Tandem account region or compatibility, login, real pump data and live
  Nightscout writes. No credentials were requested or used.

The add-on remains experimental until these runtime checks are performed. No
existing Nightscout/MongoDB add-on files or remote repository were changed.

## 3.0.0-2 regression validation

All 10 tests pass (five existing wrapper tests plus five patched-source tests).
The new tests cover all four endpoints with +12:00, +13:00, +05:30, -04:00 and UTC;
empty history makes exactly one request with intact ISO timestamps; existing
records remain visible; 401/403/500 failures do not become empty history; malformed
responses fail closed; an unexpected source file prevents patch application.
The patch applies cleanly to the archived v3.0.0 source and the resulting module
compiles/imports. Home Assistant config lint passes with version 3.0.0-2.

The user's HAOS logs have now confirmed the previous version builds, logs into
Tandem EU, reads pump records and reuses cached authentication. Those logs exposed
the date-query bug fixed here. The updated image has not yet been built or run
on HAOS, and a successful end-to-end upload is not yet verified. The second log's
connection closure is consistent with that shared fallback path, but the exact
server-side cause of that closure was not independently confirmed.
