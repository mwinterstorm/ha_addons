#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
if ! jq -e '(.password | type == "string" and length >= 16 and (test("[\r\n\u0000]") | not)) and (.cache_size_gb | type == "number" and . >= 0.256 and . <= 16)' /data/options.json >/dev/null; then
  echo 'Set a password of at least 16 characters and cache_size_gb between 0.256 and 16.' >&2
  exit 1
fi
export NS_DB_PASSWORD
NS_DB_PASSWORD="$(jq -r '.password' /data/options.json)"
export NS_PASSWORD_HASH
NS_PASSWORD_HASH="$(printf '%s' "$NS_DB_PASSWORD" | sha256sum | cut -d ' ' -f 1)"
if [[ -f /data/mongodb/.ha-password-hash ]]; then
  if [[ "$(cat /data/mongodb/.ha-password-hash)" != "$NS_PASSWORD_HASH" ]]; then
    echo 'Password differs from initialized database. Restore the original option; see DOCS.md for rotation.' >&2
    exit 1
  fi
elif [[ -f /data/mongodb/WiredTiger ]]; then
  echo 'Database exists without completed companion initialization. Restore a backup; do not reuse raw database files.' >&2
  exit 1
fi
chgrp mongodb /data
chmod 0710 /data
mkdir -p /data/mongodb /data/db /data/configdb
# The official image declares VOLUME /data/db. Keep real records outside it so
# anonymous child volumes cannot hide them from Supervisor backups of /data.
find /data/mongodb ! -user mongodb -exec chown mongodb:mongodb {} +
chmod 0700 /data/mongodb
if [[ ! -s /data/.root-password ]]; then
  head -c 48 /dev/urandom | base64 > /data/.root-password
fi
chmod 0600 /data/.root-password
export MONGO_INITDB_ROOT_USERNAME=ha_admin
export MONGO_INITDB_ROOT_PASSWORD
MONGO_INITDB_ROOT_PASSWORD="$(cat /data/.root-password)"
export MONGO_INITDB_DATABASE=nightscout
cache="$(jq -r '.cache_size_gb' /data/options.json)"
exec /usr/local/bin/docker-entrypoint.sh mongod --auth --bind_ip_all --port 27017 \
  --dbpath /data/mongodb --wiredTigerCacheSizeGB "$cache"
