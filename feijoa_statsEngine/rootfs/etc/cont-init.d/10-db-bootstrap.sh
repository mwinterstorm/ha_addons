#!/usr/bin/with-contenv bash

set -euo pipefail

APP_DIR="/usr/local/share/feijoa"
SCHEMA_DIR="${APP_DIR}/schema"
SENTINEL="/data/feijoa_schema_bootstrapped"

db_host="$(bashio::config 'mariadb_host')"
db_port="$(bashio::config 'mariadb_port')"
db_user="$(bashio::config 'mariadb_user')"
db_password="$(bashio::config 'mariadb_password')"
db_name="$(bashio::config 'mariadb_database')"

if bashio::config.is_empty 'mariadb_host' \
  || bashio::config.is_empty 'mariadb_user' \
  || bashio::config.is_empty 'mariadb_password' \
  || bashio::config.is_empty 'mariadb_database'; then
  bashio::log.warning "Database credentials are incomplete; skipping bootstrap."
  exit 0
fi

if ! command -v mariadb &> /dev/null; then
  bashio::log.error "MariaDB client is not installed. Cannot proceed with bootstrap."
  exit 1
fi

bashio::log.info "Bootstrap requested. Target host=${db_host}, port=${db_port}, user=${db_user}, database=${db_name}."
bashio::log.info "Waiting for MariaDB at ${db_host}:${db_port} (timeout 600s)..."
if ! bashio::net.wait_for "${db_host}" "${db_port}" 600; then
  bashio::log.warning "MariaDB is not reachable; skipping bootstrap."
  exit 0
fi
bashio::log.info "MariaDB reachable; continuing bootstrap."

if [ -f "${SENTINEL}" ]; then
  bashio::log.info "Database schema already bootstrapped; skipping."
  exit 0
fi

if [ ! -d "${SCHEMA_DIR}" ]; then
  bashio::log.warning "Schema directory ${SCHEMA_DIR} not found; skipping bootstrap."
  exit 0
fi

SQL_FILES=()
while IFS= read -r -d '' file; do
  SQL_FILES+=("$file")
done < <(find "${SCHEMA_DIR}" -maxdepth 1 -type f -name '*.sql' -print0 | sort -z)
bashio::log.info "Found ${#SQL_FILES[@]} schema file(s) in ${SCHEMA_DIR}."

if [ "${#SQL_FILES[@]}" -eq 0 ]; then
  bashio::log.warning "No SQL files found in ${SCHEMA_DIR}; nothing to bootstrap."
  exit 0
fi

export MYSQL_PWD="${db_password}"
mysql_cmd() {
  mariadb \
    -h "${db_host}" \
    -P "${db_port}" \
    -u "${db_user}" \
    "$@"
}

bashio::log.info "Ensuring database ${db_name} exists (creating if needed)..."
if ! mysql_cmd -e "CREATE DATABASE IF NOT EXISTS \`${db_name}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"; then
  bashio::log.error "Failed to ensure database ${db_name} exists."
  exit 1
fi

for sql_file in "${SQL_FILES[@]}"; do
  bashio::log.info "Applying schema from $(basename "${sql_file}")..."
  if ! mysql_cmd "${db_name}" < "${sql_file}"; then
    bashio::log.error "Failed executing ${sql_file} against ${db_name}."
    exit 1
  fi
done

touch "${SENTINEL}"
bashio::log.info "Database bootstrap completed. Sentinel written to ${SENTINEL}."
