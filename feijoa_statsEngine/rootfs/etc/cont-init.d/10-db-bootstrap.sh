#!/usr/bin/with-contenv bash

set -euo pipefail

APP_DIR="/usr/local/share/feijoa"
SCHEMA_DIR="${APP_DIR}/schema"
SENTINEL="/config/feijoa_schema_bootstrapped"
OPTIONS_FILE="/data/options.json"

log_info() {
  echo "[INFO] $*"
}

log_warn() {
  echo "[WARN] $*" >&2
}

log_error() {
  echo "[ERROR] $*" >&2
}

json_get() {
  local jq_filter=$1
  jq -r "${jq_filter} // empty" "${OPTIONS_FILE}"
}

wait_for_tcp() {
  local host=$1
  local port=$2
  local timeout=${3:-600}
  local interval=5
  local elapsed=0

  while [ "${elapsed}" -lt "${timeout}" ]; do
    if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep "${interval}"
    elapsed=$((elapsed + interval))
  done

  return 1
}

sql_escape_literal() {
  printf "%s" "$1" | sed "s/'/''/g"
}

if ! command -v mariadb >/dev/null 2>&1; then
  log_error "MariaDB client is not installed. Cannot proceed with bootstrap."
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  log_error "jq is required to read add-on options but is not installed."
  exit 1
fi

if [ ! -f "${OPTIONS_FILE}" ]; then
  log_warn "Options file ${OPTIONS_FILE} not found; skipping bootstrap."
  exit 0
fi

db_host="$(json_get '.mariadb_host')"
db_port="$(json_get '.mariadb_port')"
db_user="$(json_get '.mariadb_user')"
db_password="$(json_get '.mariadb_password')"
db_name="$(json_get '.mariadb_database')"

if [ -z "${db_port}" ]; then
  db_port="3306"
fi

if [ -z "${db_host}" ] || [ -z "${db_user}" ] || [ -z "${db_password}" ] || [ -z "${db_name}" ]; then
  log_warn "Database credentials are incomplete; skipping bootstrap."
  exit 0
fi

log_info "Bootstrap requested. Target host=${db_host}, port=${db_port}, user=${db_user}, database=${db_name}."
log_info "Waiting for MariaDB at ${db_host}:${db_port} (timeout 600s)..."
if ! wait_for_tcp "${db_host}" "${db_port}" 600; then
  log_warn "MariaDB is not reachable; skipping bootstrap."
  exit 0
fi
log_info "MariaDB reachable; continuing bootstrap."

if [ -f "${SENTINEL}" ]; then
  log_info "Database schema already bootstrapped; skipping."
  exit 0
fi

if [ ! -d "${SCHEMA_DIR}" ]; then
  log_warn "Schema directory ${SCHEMA_DIR} not found; skipping bootstrap."
  exit 0
fi

SQL_FILES=()
while IFS= read -r -d '' file; do
  SQL_FILES+=("$file")
done < <(find "${SCHEMA_DIR}" -maxdepth 1 -type f -name '*.sql' -print0 | sort -z)
log_info "Found ${#SQL_FILES[@]} schema file(s) in ${SCHEMA_DIR}."

if [ "${#SQL_FILES[@]}" -eq 0 ]; then
  log_warn "No SQL files found in ${SCHEMA_DIR}; nothing to bootstrap."
  exit 0
fi

export MYSQL_PWD="${db_password}"
mysql_cmd() {
  mariadb \
    -h "${db_host}" \
    -P "${db_port}" \
    -u "${db_user}" \
    --default-character-set=utf8mb4 \
    "$@"
}

escaped_db_name="$(sql_escape_literal "${db_name}")"
if mysql_cmd -N -s -r -e "SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='${escaped_db_name}' LIMIT 1;" >/dev/null; then
  log_info "Database ${db_name} already exists."
else
  log_info "Database ${db_name} not found; attempting to create it..."
  if ! mysql_cmd <<SQL
CREATE DATABASE IF NOT EXISTS \`${escaped_db_name}\`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
SQL
  then
    log_error "Failed to create database ${db_name}. Ensure the MariaDB user ${db_user} has CREATE privileges or create the database manually."
    exit 1
  fi
  log_info "Database ${db_name} created."
fi

for sql_file in "${SQL_FILES[@]}"; do
  log_info "Applying schema from $(basename "${sql_file}")..."
  if ! mysql_cmd "${db_name}" < "${sql_file}"; then
    log_error "Failed executing ${sql_file} against ${db_name}."
    exit 1
  fi
done

touch "${SENTINEL}"
log_info "Database bootstrap completed. Sentinel written to ${SENTINEL}."
