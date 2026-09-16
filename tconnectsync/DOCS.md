# Configuration and operation

This add-on requires a supported Tandem pump/account (upstream targets t:slim X2
and Mobi), uploaded data in Tandem Source, and a reachable Nightscout instance.
It does not connect to the pump over Bluetooth and does not operate the pump.
It is not a general uploader for other pump brands or CamAPS.

| Option | Purpose |
| --- | --- |
| `tconnect_email` | Your Tandem Source account email. |
| `tconnect_password` | Tandem Source account password, masked in the HA options UI. |
| `tconnect_region` | `US` or `EU`; choose the backend your account actually uses. |
| `nightscout_url` | Nightscout's root URL, without credentials or query parameters. |
| `nightscout_api_secret` | The existing Nightscout API secret, at least 12 characters. |
| `timezone` | Pump timezone as an IANA name, default `Pacific/Auckland`. |
| `pump_serial_number` | Optional quoted numeric serial; blank selects most recently used pump. |
| `features` | Record categories to synchronize, listed below. |
| `poll_interval_seconds` | Fixed polling sleep, 300–3600 seconds; default 300. Requests/processing add time. |
| `mode` | `sync`, `preview`, or `check_login`. |

Example for a repository installation (replace placeholders):

```yaml
tconnect_email: "your-email@example.com"
tconnect_password: "YOUR-TANDEM-PASSWORD"
tconnect_region: EU
nightscout_url: "http://YOUR-REPOSITORY-PREFIX-nightscout:1337"
nightscout_api_secret: "YOUR-EXISTING-NIGHTSCOUT-API-SECRET"
timezone: Pacific/Auckland
pump_serial_number: ""
features:
  - BASAL
  - BOLUS
  - PUMP_EVENTS
poll_interval_seconds: 300
mode: sync
```

`EU` in this example is not an assertion about New Zealand accounts. Upstream has
only US/EU backends. Confirm which one holds your account. Do not repeatedly retry
passwords or regions in a restart loop. Additional interactive authentication or
account-specific challenges are not handled by this wrapper.

## Features

- **BASAL**, **BOLUS**, **PUMP_EVENTS**: enabled by default.
- **PROFILES**: opt-in. Writes pump-derived profiles to Nightscout, using upstream's
  default `add` mode. Review existing profiles and take a database backup first.
- **CGM**: opt-in. Do not enable alongside another uploader of the same CGM readings.
  Upstream warns these readings can lag by more than 30 minutes; retain your direct
  CGM feed for timely readings.
- **IOB**, **PUMP_EVENTS_BASAL_SUSPENSION**, **CGM_ALERTS**, **DEVICE_STATUS**: available
  upstream feature switches; behavior and record availability depend on pump data.

Run only one tconnectsync instance against the same Nightscout records. The program
uses Nightscout to determine previously uploaded data, but this is not a guarantee
against duplicates from a different uploader or manual entries.

## Modes

`sync` continuously fetches records and writes missing records to Nightscout.
`preview` runs the same continuous cycle with upstream `--pretend`; it still logs in,
reads remote services, and caches authentication, but is intended not to upload.
`check_login` is a one-off Tandem credential check that exits afterward. It does
not establish that Nightscout writes work. Switching mode requires Save and Restart.

There is no ingress, host port, UI or MongoDB connection. Traffic goes outbound to
Tandem over HTTPS and to the configured Nightscout URL. Internal app HTTP is suitable
within HA's app network; use HTTPS for a Nightscout instance across an untrusted
network. TLS verification remains enabled. No Supervisor API access is requested.

## Storage and secrets

HA stores options in `/data/options.json`. The launcher reads them as root, then
drops supplementary groups and changes to UID/GID 1000 before replacing itself
with upstream Python. It passes a minimal environment with no Supervisor tokens.
Secrets are not passed on the command line or printed by the wrapper; upstream
logs can contain account identifiers and health records. Review logs before sharing.

Authentication state lives in `/data/home/.config/tconnectsync/.creds_cache` and
any library state under `/data/home`; the home directory is private and files are
created under a 0077 umask. The cache persists across updates/restarts and is
included in this add-on's HA backup. Cold backup stops the service while copying
state. These backups contain credentials/tokens: protect them and the recovery key.
Nightscout records live in its MongoDB database and need that separate backup.

If changing Tandem password or account settings, cached tokens may remain valid
until they expire. To force a fresh login, stop the add-on and clear its credential
cache through an appropriate administrative recovery method, or reinstall after
saving your options. Uninstall removes this add-on's state, not Nightscout history.
No reset/delete action is exposed by this wrapper.

## Troubleshooting

- **Login fails:** verify account backend, password and access to Tandem Source.
  Stop repeated restart attempts to avoid account rate limiting. Keep HA Watchdog
  off while diagnosing credentials or when using `check_login` mode.
- **No new records:** check whether Tandem Source itself has recent pump uploads.
  More frequent polling cannot recover data not yet uploaded to Tandem.
- **Nightscout errors:** verify the actual add-on Hostname, port, secret and selected
  features. Your Git repository changes the DNS prefix from `local-` to a hash.
- **Wrong timestamps:** verify the pump timezone and clock, not just HA's timezone.
- **HTTP 404 from Tandem:** version 3.0.0 addresses the June 2026 API change; a new
  upstream change can break this unofficial integration again.

No automatic wrapper restart loop or synthetic health indicator is added. Watchdog
and startup are managed by HA. The pinned release's behavior, not newer settings
on upstream master, determines retry/error behavior. Read Logs after outages; do
not assume a running process proves records are arriving.
