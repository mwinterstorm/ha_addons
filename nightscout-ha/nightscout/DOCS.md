# Nightscout configuration

For a local installation, first install Nightscout MongoDB, set its password and
start it. Set this app's `mongo_host` to `local-nightscout-mongodb`, and copy the
same password into `mongo_password`. Repository installs use the Hostname shown
on the MongoDB app's information page instead.

| Option | Meaning |
| --- | --- |
| `api_secret` | Required Nightscout secret, minimum 12 characters; use a distinct long random secret. |
| `mongo_host` | Companion's internal hostname. Port 27017, database/user `nightscout`. |
| `mongo_password` | Companion database password, minimum 16 characters. Automatically URI-encoded. |
| `mongo_uri` | Optional complete `mongodb://` or `mongodb+srv://` URI; overrides host/password. Include database name and authentication settings. |
| `display_units` | `mmol/L` (default) or `mg/dl`. |
| `auth_default_roles` | `status-only` allows API-secret login without anonymous glucose reads; `denied` requires tokens; `readable` deliberately enables public reads. |
| `enable` | Space-separated upstream plugins; default `careportal`. |
| `base_url` | Optional final site URL for generated links; not a bind address or ingress prefix. |
| `extra_env` | List of uppercase `name` / string `value` pairs for upstream settings. |

Example extra settings:

```yaml
extra_env:
  - name: CUSTOM_TITLE
    value: "Mark's Nightscout"
  - name: TIME_FORMAT
    value: "24"
```

Startup-owned database/auth/network variables, runtime injection settings and
`*_FILE` overrides are blocked. Other settings follow upstream semantics; an
upstream setting can still affect security or call external services. Do not paste
unreviewed configuration. Use your HA timezone; fallback is Pacific/Auckland.

Supervisor stores secrets in `/data/options.json` and backups. `password` schema
fields mask UI input; they do not encrypt stored values. The adapter writes the
API secret to `/run/nightscout/api-secret`, owner Node UID 1000 and mode 0600,
then starts upstream via `API_SECRET_FILE`. The application runs as non-root.
The database URI remains a process environment value. Keep backups encrypted and
restrict administrator access. Startup validation errors omit supplied secrets;
upstream application logs are not globally redacted, so review logs before sharing.

No database files are stored by this app. Its `/data` contains Supervisor options;
all Nightscout records/settings in MongoDB require the database's own backup.
Changing API secret here and restarting takes effect immediately: update uploaders
and API-secret-based clients too. This does not revoke separately issued access
tokens; manage those in Nightscout.

## Web UI and clients

Open `http://HOME_ASSISTANT_IP:1337` or use Open Web UI. Change the host-side port
under Network if 1337 is occupied; update clients accordingly. Keep the container
port unchanged. Use a VPN or HTTPS reverse proxy for remote clients; HA ingress
is intentionally not enabled. Nightscout browser assets, WebSockets and uploader
APIs need a consistent root URL, and native ingress path handling is unverified.
The adapter enables same-origin framing protection.

## Troubleshooting

- “Waiting for MongoDB”: check the companion is started, its Hostname is correct,
  and both password options match. After approximately four minutes startup exits;
  correct the cause and restart. A native Docker health check checks authenticated
  HTTP service after startup; enable the HA Watchdog switch if available.
- No glucose readings: verify uploader address, credentials and phone network
  reachability, then check timestamps. Installing this app does not collect G7
  readings directly and does not enable Dexcom Connect automatically.
- Browser redirect/login issues: confirm the reverse proxy preserves WebSockets
  and Host/forwarded protocol; test direct LAN access first.
- For an external database: use a named database and an appropriately scoped user.
  Configure TLS in the URI as required, and permit the HA host's source network.
  External database backups are NOT included in Home Assistant backups.
