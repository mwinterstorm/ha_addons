# Validation — 2026-09-21

Nine integration/security tests pass on Node.js 22.23.2 (the initial seven also passed on 26.8.1) with real HTTP servers, the official MCP client and
a synthetic Nightscout upstream. Tests cover all eight tools, read-only annotations,
GET-only upstream requests, timestamp/unit handling, invalid readings, missing
records, bounded windows and truncation, secret-safe errors, Host/Origin rejection,
unauthenticated access, and unsupported methods/tools.

OAuth tests cover public discovery, DCR callback restrictions, persistent 0600
client storage, browser consent and CSRF, incorrect owner secret, scope/resource
restrictions, PKCE verification, code replay, redirect binding, refresh rotation,
refresh reuse revocation and token revocation. No real health data or credentials
were used. `npm audit --omit=dev` reported zero known dependency vulnerabilities.

Add-on YAML parses successfully and its option/schema keys match.

The container pins a multi-platform Node 22 Alpine manifest; npm dependencies
are exact and lockfile-controlled. Docker was not running on the development Mac. GitHub Actions successfully
built the amd64 image and ran its health/unauthenticated-request smoke test
in a container with no network access and a read-only root filesystem.
The ARM64 image build and HAOS runtime remain unverified. Live Nightscout, actual
Tailscale/Caddy routing, browser consent rendering, ChatGPT account eligibility and
end-to-end ChatGPT linking have not yet been verified. Keep the add-on experimental.

Run from this directory:

```sh
npm ci --ignore-scripts
npm test
npm audit --omit=dev
docker build -t nightscout-mcp:test .
```

After installation, complete DOCS.md's public metadata/auth checks and compare
all tools with the live Nightscout UI before relying on the data. Repeat after an
HA reboot; expect to sign in again because runtime tokens are intentionally revoked.

Sources checked: Nightscout tag 15.0.8 `lib/authorization/index.js` and
`lib/server/swagger.yaml`; MCP SDK 1.30.0 implementation; OpenAI OAuth and connection
docs; Home Assistant app configuration; Tailscale Funnel and community add-on docs.

Initial successful container validation: https://github.com/mwinterstorm/ha_addons/actions/runs/35544191912
