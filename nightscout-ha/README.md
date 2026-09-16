# Nightscout for Home Assistant OS

Two local apps (formerly add-ons): **Nightscout 15.0.8** and **MongoDB 8.0.32**.
Uses the upstream Nightscout image unchanged, with a small HA options adapter.
Both images are pinned to multi-platform digests for amd64 and aarch64.

**Status: experimental.** Static validation and configuration tests pass. Container
builds, HAOS installation, backup/restore and real uploader operation have not
been verified on this Mac because its Docker daemon is not running. Automated
disposable Docker smoke tests are included; run them before relying on this setup.

## Why two apps?

Nightscout updates independently of MongoDB. Both still run under Supervisor on
HAOS; no Docker socket, privileged mode, host networking or separate VM is needed.
MongoDB runs during the `services` startup stage, Nightscout during `application`.
Nightscout waits up to approximately four minutes for an authenticated database
connection. HAOS reboots stop both; restarting Home Assistant Core alone does not.

MongoDB listens on the internal app network with authentication and no host port
mapping. Other apps can reach that network, so it is not an authentication boundary.
Its dedicated account can read/write only the `nightscout` database. Database files
live in the MongoDB app's `/data/mongodb`, covered by its cold HA backups. Updating or
restarting Nightscout leaves this storage alone. Uninstalling the database app can
delete it. A Nightscout-only backup does NOT contain glucose data.

## Install locally on HAOS

1. Check hardware: the MongoDB companion requires AMD64 with AVX, or ARM64 with
   ARMv8.2-A or later. Raspberry Pi 4 is unsuitable for this MongoDB image. For a
   Proxmox HAOS VM, expose a compatible CPU (often CPU type `host`). Check current
   MongoDB kernel compatibility in [SOURCES.md](SOURCES.md). Allow approximately
   2 GB of available RAM for these services initially, plus HA's own needs; monitor
   actual memory/disk use. This is a planning allowance, not a measured minimum.
2. Install/configure HA's **Samba share** app if needed. On your Mac, Finder → Go →
   Connect to Server → `smb://homeassistant.local`, then open the **addons** share.
   Alternatively use the HA **Terminal & SSH** app's `/addons` directory.
3. Copy the `nightscout` and `mongodb` folders from this repository directly into
   that share. Required layout on HA:

   ```text
   /addons/nightscout/config.yaml
   /addons/nightscout/Dockerfile
   /addons/nightscout/rootfs/...
   /addons/mongodb/config.yaml
   /addons/mongodb/Dockerfile
   /addons/mongodb/rootfs/...
   ```

   Do not put them under `/config`, and do not copy a ZIP without extracting it.
4. In HA, open **Settings → Apps → Install app** (older releases: **Add-ons →
   Add-on Store**). Use **⋮ → Check for updates** / refresh. Find both under
   **Local apps / Local add-ons**. Install **Nightscout MongoDB** first. This pulls
   its base image and builds the small adapter; allow several minutes and internet
   access to Docker Hub and Ubuntu package repositories.
5. In MongoDB's Configuration, set `password` to a new random password of at least
   16 characters (32+ recommended). Leave `cache_size_gb: 0.5`. Save and Start.
   Wait for database initialization to finish; check Logs. Keep this password.
6. Install **Nightscout**. Set:

   ```yaml
   api_secret: "PASTE-A-DIFFERENT-RANDOM-SECRET-AT-LEAST-12-CHARACTERS"
   mongo_host: local-nightscout-mongodb
   mongo_password: "PASTE-THE-MONGODB-PASSWORD-FROM-STEP-5"
   mongo_uri: ""
   display_units: mmol/L
   auth_default_roles: status-only
   enable: careportal
   base_url: ""
   extra_env: []
   ```

   The uppercase example strings are placeholders, not usable default secrets.
   Save, Start, check Logs, then **Open Web UI**. The address is
   `http://HOME_ASSISTANT_IP:1337`. Authenticate using the API secret. Leave Start
   on boot enabled for both; enable Watchdog where offered after initial testing.
7. Configure xDrip+ to upload to that Nightscout address with your API secret.
   Use its Nightscout upload settings and the URL/auth format required by your
   xDrip+ version. Confirm a newly uploaded reading and timestamp in Nightscout.
   Existing Android data is not automatically imported by installing this app.
8. Add HA's **Nightscout** integration under Settings → Devices & services → Add
   integration. Use `http://HOME_ASSISTANT_IP:1337` and the API secret. Verify the
   integration and your Mac client can read new data before retiring the old setup.

Port 1337 is direct HTTP, not HA-authenticated ingress. Use it only on a trusted
LAN or through a correctly configured VPN; use an HTTPS reverse proxy for access
outside that network. HA Cloud's remote UI does not automatically publish this
port. Set `base_url` to your final HTTPS URL if needed for generated links. Do not
forward port 1337 directly onto the internet. MongoDB has no exposed LAN port.

## Git repository installation (optional)

Publish the contents of this folder as the root of your own Git repository. Add
its real URL to `repository.yaml` as `url`, and optionally update the app `url`
fields to your own documentation. Then HA's app store → ⋮ → Repositories → add
that Git URL. No `image:` field is set: Supervisor builds locally from Dockerfiles.
No repository has been published for you.

Repository installs have a different DNS prefix. Copy MongoDB's **Hostname** from
its app information page into Nightscout's `mongo_host`; it will resemble
`a1b2c3d4-nightscout-mongodb`. `local-nightscout-mongodb` applies only to a local
installation. Moving from local to repository installation creates a different
app identity and storage; migrate the database explicitly first.

## More instructions

- [Nightscout options and access](nightscout/DOCS.md)
- [Database, backups, restoration and migration](mongodb/DOCS.md)
- [Upstream evidence and version decisions](SOURCES.md)
- [Maintainer upgrade and validation guide](MAINTAINING.md)
- [What was actually validated](VALIDATION.md)
