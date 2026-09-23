# TeslaMate API Home Assistant add-on

This add-on runs [TeslaMateApi](https://github.com/tobiasehlert/teslamateapi), the REST API for a self-hosted TeslaMate installation.

## Requirements

TeslaMate and its PostgreSQL database must already be running and reachable from Home Assistant. The add-on also needs to reach the MQTT broker used by TeslaMate unless MQTT is disabled. Enter hostnames or IP addresses that are reachable from the add-on container; Docker Compose service names such as `database` and `mosquitto` usually do not resolve in Home Assistant OS.

## Setup

1. Install TeslaMateApi from this add-on repository.
2. Set the database host, port, database name, username, and password to match TeslaMate.
3. Set `encryption_key` to the same encryption key TeslaMate uses. The add-on will stop with a clear error if this is blank or left at the placeholder from version 0.1.0.
4. Set the MQTT host and credentials to match the broker TeslaMate uses, or enable `disable_mqtt` if you do not want MQTT integration.
5. Start the add-on and open its API on port 8080. The `api/healthz` endpoint can be used for a basic check.

Commands are disabled by default. To enable command endpoints, set `enable_commands` and configure an API token of at least 32 characters. Then enable only the command groups you need. Keep access to this HTTP API restricted to a trusted network or place it behind an HTTPS reverse proxy with authentication.

The API reads vehicle data from TeslaMate. Command endpoints can trigger actions on your vehicle when enabled. Review the upstream documentation before exposing them: [TeslaMateApi README](https://github.com/tobiasehlert/teslamateapi).
