#!/usr/bin/with-contenv bashio

set -euo pipefail

APP_SCRIPT="${APP_SCRIPT:-/usr/local/share/feijoa/s3_contributions_to_sql.py}"

if [ ! -f "${APP_SCRIPT}" ]; then
  bashio::log.error "Importer script not found at ${APP_SCRIPT}."
  bashio::log.error "Add your implementation to ${APP_SCRIPT} or adjust APP_SCRIPT."
  exit 1
fi

bashio::log.info "Running Feijoa importer against MariaDB host ${DB_HOST}:${DB_PORT}"
exec python3 "${APP_SCRIPT}"
