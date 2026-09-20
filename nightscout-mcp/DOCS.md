# Install Nightscout MCP and connect ChatGPT

This is a separate add-on in `mwinterstorm/ha_addons`. It reads your existing
Nightscout over HTTP; it does not need MongoDB credentials, a Home Assistant token,
or an OpenAI API key. It supports amd64 and aarch64.

## 1. Install the add-on

Once the `nightscout-mcp` folder has been merged into your repository's default
branch, refresh Home Assistant's app/add-on store. If needed, add
`https://github.com/mwinterstorm/ha_addons` under the store's **Repositories** menu.
Install **Nightscout MCP**.

For testing before publication, extract the supplied ZIP and copy its
`nightscout-mcp` folder into the HA **addons** Samba share (or `/addons` through
Terminal & SSH). Required location: `/addons/nightscout-mcp/config.yaml`.
Refresh the store and install it under **Local apps / Local add-ons**. Do not move
or reinstall Nightscout/MongoDB: that could create new add-on storage identities.

Copy the **Hostname** from the existing Nightscout add-on's information page.
The repository currently uses prefix `ba5d2509`; verify the displayed hostname
rather than assuming the default below is correct.

In Nightscout's Admin Tools, create a dedicated access token/subject with only
`readable` permission. Name it `nightscout-mcp`. Use that complete access token
as `nightscout_token`. Do not give this add-on the administrator API secret.
Nightscout 15.0.8 accepts these access tokens in the `api-secret` header; this
implementation does not put tokens in URLs or exchange them with ChatGPT.

## 2. Choose the HTTPS route

Use a dedicated HTTPS origin, such as `https://nightscout-mcp.YOUR-TAILNET.ts.net`.
All routes on that origin must reach port 8099: `/mcp`, `/authorize`, `/approve`,
`/token`, `/register`, `/revoke` and `/.well-known/*`. Forwarding only `/mcp`
will break OAuth. Preserve the public Host and Authorization headers. Do not
add a separate browser-only proxy login in front of the OAuth endpoints.

**Tailscale:** private Serve addresses are reachable only within your tailnet.
ChatGPT's cloud connection needs public HTTPS or OpenAI's Secure MCP Tunnel.
Funnel supplies public HTTPS, but the add-on's OAuth protects the data; anyone
on the internet can reach the login page.

The community Home Assistant Tailscale add-on's `share_homeassistant` option
publishes Home Assistant itself. Its `services` feature currently supports
Serve, not Funnel. Do not enable `share_homeassistant: funnel` expecting it to
publish this MCP add-on. See the [Tailscale add-on documentation](https://github.com/hassio-addons/app-tailscale/blob/main/tailscale/DOCS.md).

### Funnel through an existing always-on Tailscale machine

Use a dedicated Tailscale node/origin, with Caddy installed and running as a
persistent service. The node must reach `HOME_ASSISTANT_IP:8099` through your
trusted LAN or an existing Tailscale route. Set Caddy's site configuration to:

```caddyfile
http://127.0.0.1:8098 {
    bind 127.0.0.1
    reverse_proxy http://HOME_ASSISTANT_IP:8099 {
        header_up Host nightscout-mcp.YOUR-TAILNET.ts.net
    }
}
```

Replace both placeholders. This listener is local to the relay; its upstream is
the MCP add-on only. Use a trusted private path for the HTTP upstream, never the
public internet. Prefer a tailnet address where the HA host exposes this port.
Do not enable Caddy access logging for OAuth query strings or request bodies.

Enable MagicDNS, HTTPS certificates and Funnel for this node in Tailscale. After
configuring and starting the add-on below, run on the relay:

```sh
tailscale funnel --bg --https=443 http://127.0.0.1:8098
tailscale funnel status
```

Use the actual hostname Tailscale assigns, consistently in Caddy and `public_url`.
A pre-existing Serve/Funnel site on port 443 may conflict; use a dedicated node.
Funnel's HTTP proxy targets loopback, hence the local Caddy relay. Configure
Caddy and Tailscale to start on boot; verify again after restarting the relay.
To remove just this Funnel listener: `tailscale funnel --https=443 off`.
See [Funnel CLI](https://tailscale.com/docs/reference/tailscale-cli/funnel).

An existing public HTTPS reverse proxy on Home Assistant can instead point a
new dedicated hostname to this add-on's hostname on port 8099. Keep port 1337,
MongoDB and the Home Assistant UI out of this publication.

**Private alternative:** OpenAI documents Secure MCP Tunnel for eligible
accounts/workspaces. It requires a separate running tunnel client and Platform
permissions. It is not included in this add-on. OAuth's browser-facing URL still
needs to be reachable by the user; confirm tunnel/discovery routing before using
it. See [Secure MCP Tunnel](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels).

## 3. Configure and start

Generate a new random owner secret in your password manager (32 or more random
characters). It must differ from the Nightscout token. Save it there; enter it
only in HA's configuration and the add-on's HTTPS login form, never in chat.

```yaml
nightscout_url: "http://ba5d2509-nightscout:1337"
nightscout_token: "YOUR-DEDICATED-NIGHTSCOUT-READABLE-TOKEN"
auth_mode: oauth
public_url: "https://nightscout-mcp.YOUR-TAILNET.ts.net"
owner_password: "YOUR-NEW-RANDOM-OWNER-SECRET-AT-LEAST-32-CHARACTERS"
bearer_token: ""
allowed_hosts: []
allowed_origins: []
extra_redirect_uris: []
max_history_hours: 168
max_records: 1000
stale_after_minutes: 15
```

The uppercase strings are placeholders. Use the Nightscout hostname copied in
step 1. `public_url` is an origin, with no `/mcp` path. It is automatically added
to the Host allowlist. Save, start, and check Logs. Enable start on boot/watchdog
after initial checks. The `/health` endpoint only checks the process; it does
not prove Nightscout connectivity or data freshness.

Before connecting, verify at the public HTTPS origin:

- `GET /.well-known/oauth-authorization-server` returns metadata with `S256`.
- `GET /.well-known/oauth-protected-resource/mcp` identifies the exact `/mcp` URL.
- An unauthenticated request to `/mcp` returns **401** with a
  `WWW-Authenticate` header, never patient data.
- `GET /health` returns `{"status":"ok"}`.

## 4. Connect in ChatGPT

Current OpenAI instructions place **Developer mode** under **Settings → Security
and login**, then let you add an MCP connection using the plus button in
**ChatGPT Plugins**. Account/workspace policy may affect availability or labels.

1. Create a connection called **Nightscout (read-only)**.
2. Enter `https://YOUR-ACTUAL-HOSTNAME/mcp`.
3. Select **OAuth** and **dynamic client registration (DCR)** if asked. Leave
   manually supplied client ID/secret empty; this server advertises `/register`.
4. Connect/sign in. On your own HTTPS origin, review the read-only consent screen
   and enter the add-on's **owner_password**. It is not your ChatGPT password.
5. Check that exactly eight tools are listed, then add the connection to a new chat.
6. Ask: “Read my latest glucose. Include the measurement time, both units, and
   whether the reading is stale.” Compare the result with Nightscout directly.
7. Ask for six hours of glucose and recorded carbs/insulin. Check timestamps and
   truncation; missing records must not be treated as zero.

The built-in callback allowlist accepts ChatGPT's official stable callback and
its `/connector/oauth/<callback_id>` callbacks. Other clients require an exact
`extra_redirect_uris` entry. There are no wildcard arbitrary redirect hosts.
This server uses DCR, not CIMD. Its SDK does not advertise RFC 9207 issuer-response
support, so a new ChatGPT connection may use the callback-specific URL.

See [OpenAI connection instructions](https://developers.openai.com/plugins/deploy/connect-chatgpt)
and [OAuth requirements](https://developers.openai.com/plugins/build/auth).

## Tools and limits

| Tool | Result |
| --- | --- |
| `get_current_glucose` | Latest stored CGM reading, units, age and stale/valid flags |
| `get_glucose_history` | CGM records in a bounded window |
| `get_treatments` | Recorded treatment fields; no free-text notes |
| `get_carbs_history` | Treatment records with a numeric carbs field, in grams |
| `get_insulin_history` | Insulin or basal treatment records, not inferred delivery |
| `get_profile` | Latest saved profile document, not a computed active profile |
| `get_device_status` | Stored battery/reservoir and available uploader IOB/COB |
| `get_status` | Nightscout service status/version/time and display units |

History tools accept optional `start`/`end` ISO-8601 timestamps with an explicit
UTC offset or `Z`, plus `limit`. Default window: six hours. Default limit: 288.
The configured maximum window is seven days and maximum limit 1000. Larger
windows require explicit owner configuration (absolute maximum 31 days/5000).

Results are bounded, not automatically paginated. `truncated: true` means
records were omitted; request smaller windows. Carbs/insulin filters run after
the source-record limit, so a truncated result with no matches is not proof of
no events. Upstream retention/limits or upload gaps can also omit records.
Treatment fields retain source units; basal rates and durations are not summed
into bolus doses. IOB/COB are uploader estimates, never calculated here.

## Private testing with a bearer token

For a local MCP Inspector or other client that supports custom headers, use
`auth_mode: bearer`, a different random `bearer_token` of 32+ characters, and
`allowed_hosts: ["HOME_ASSISTANT_IP:8099"]`. Set `public_url: ""`. Supply
`Authorization: Bearer ...` using the client's protected header configuration.
Use HTTPS or an encrypted tailnet path for credentials. A LAN-only HTTP test
requires a trusted network. Add the Inspector's exact browser origin to
`allowed_origins` only if that particular client sends an Origin header.

Bearer mode disables the OAuth routes and is not the documented ChatGPT setup.
Do not use a token in the URL or select “No authentication” in ChatGPT.

## Troubleshooting and revocation

- **Startup fails:** check the URL/secret requirements. Values are deliberately
  absent from logs.
- **403 Host not allowed:** make the proxy preserve the public Host, or add the
  exact trusted backend Host (including port) to `allowed_hosts`.
- **401 from Nightscout:** confirm its dedicated token is complete, enabled and
  has `readable` permission. Never solve this by granting admin rights.
- **OAuth callback error:** verify metadata is public, resource is exactly the
  `/mcp` URL, and the client selected DCR. Check clocks on HA and the client.
- **Login expired/invalid:** restart the connection. Consent expires in five
  minutes and each form allows one password attempt. Private-browser cookie
  blocking can prevent login. Do not remove CSRF or HTTPS checks.
- **429:** single-owner global throttles were reached. Login allows ten attempts
  per ten minutes; wait for `Retry-After` rather than loosening the limits.
- **Reconnect after restarting:** access/refresh tokens live only in RAM and
  are revoked on every restart. Registered client credentials persist in
  `/data/oauth-clients.json`; choose reconnect/sign in again. If the client
  cannot recover, remove and recreate the ChatGPT connection.
- **Revoke all MCP access:** stop the add-on, change `owner_password`, restart.
  Also revoke the dedicated Nightscout token if it leaked. To clear exhausted
  registrations, stop the add-on, back up then remove only
  `/data/oauth-clients.json` inside this add-on, restart and recreate connections.
- **Backups:** protect HA backups; configuration and OAuth client registrations
  contain secrets. No glucose records are persisted by this add-on.

This integration reads personal health records into ChatGPT when its tools are
used. Review your ChatGPT account's data controls. It is not an alarm system,
pump controller, or a basis for dosing decisions.
