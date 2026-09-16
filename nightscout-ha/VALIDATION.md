# Validation report — 16 September 2026

## Passed locally

- Live GitHub latest release: v15.0.8, released 4 September 2026.
- Live Docker registry: exact pinned Nightscout and MongoDB tags/digests exist,
  with native Linux amd64 and arm64 image manifests.
- Both config.yaml files pass Frenck's app linter, including its custom default
  value checks. Executed the upstream Python linter with only the schema file
  location redirected to a temporary local path; checked both main and CI's v2.
- All repository YAML parses and passes yamllint with relaxed formatting and no
  line-length limit.
- MongoDB launcher passes bash syntax validation and ShellCheck.
- All launcher/health/initialization JavaScript passes Node syntax checking.
- Seven Node tests pass: escaped database credentials, private auth defaults,
  file-based secret mapping, invalid-secret rejection, external URI behavior,
  reserved environment protection, literal extra settings/duplicate rejection,
  startup retries, privilege drop, clean child environment, signal forwarding,
  bounded failure and omission of sensitive connection errors from wrapper logs.
  Several assertions share test cases; total test cases = 7.
- Docker smoke-test Python compiles. The MongoDB option-validation jq expression
  was checked locally with a valid sample.

## Not yet verified

The installed Docker client could inspect remote registries, but its local daemon
was unavailable (`~/.orbstack/run/docker.sock` did not exist). No container builds,
Docker runtime smoke test, HAOS installation, full backup/restore, UI/WebSocket
session or real uploader test was run. Native CPU compatibility on Mark's HA host
is unknown. The add-ons remain `stage: experimental` for this reason.

The repository includes a disposable integration test and a GitHub workflow for
both native architectures. They test authentication, upload/read, data survival
across database-container replacement and refusal of an accidental password change.
No claim is made that those integration tests have passed. See MAINTAINING.md.

No existing HA instance, old Nightscout installation, real database, synced project
source, remote Git repository or public deployment was changed.
