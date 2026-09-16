# Home Assistant Add-ons

Personal add-ons for Home Assistant OS.

## List of Add-ons

### Nightscout

Runs the upstream Nightscout glucose-monitoring server in Home Assistant OS. Requires MongoDB, available through the companion add-on below.

**Quick setup**

1. Install **Nightscout MongoDB**, set a strong password of at least 16 characters, and start it.
2. Copy the **Hostname** shown on the MongoDB add-on’s information page.
3. Install **Nightscout** and configure:
   - `mongo_host`: the hostname copied above.
   - `mongo_password`: the password set in MongoDB.
   - `api_secret`: a separate strong secret of at least 12 characters.
4. Save, start Nightscout, and select **Open Web UI**.
5. Configure your uploader to use `http://HOME_ASSISTANT_IP:1337` and your API secret.

Use a trusted local network, VPN, or HTTPS reverse proxy for access. Do not expose port 1337 directly to the internet.

[Full configuration instructions](nightscout/DOCS.md)

### Nightscout MongoDB

A MongoDB database companion for Nightscout. Stores glucose readings and other Nightscout data persistently, with authentication and no exposed LAN port.

Database backups are included when this add-on is selected in a Home Assistant backup. MongoDB briefly stops during backup to keep the database copy consistent.

Supports amd64 with AVX and aarch64 with ARMv8.2-A or later. **Raspberry Pi 4 is not supported by this MongoDB image.**

[Setup, backups and maintenance](mongodb/DOCS.md)

Both Nightscout add-ons are currently experimental; validation on Home Assistant OS is pending.

### Feijoa Stats Engine

A personal add-on for my company, Feijoa. Built for our internal systems and unlikely to be useful to anyone else.