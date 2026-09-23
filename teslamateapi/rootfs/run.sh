#!/usr/bin/with-contenv bashio
set -euo pipefail

export DATABASE_HOST="$(bashio::config 'database_host')"
export DATABASE_PORT="$(bashio::config 'database_port')"
export DATABASE_USER="$(bashio::config 'database_user')"
export DATABASE_PASS="$(bashio::config 'database_password')"
export DATABASE_NAME="$(bashio::config 'database_name')"
export DATABASE_SSL="$(bashio::config 'database_ssl')"
export DATABASE_TIMEOUT="$(bashio::config 'database_timeout')"
export ENCRYPTION_KEY="$(bashio::config 'encryption_key')"
export MQTT_HOST="$(bashio::config 'mqtt_host')"
export MQTT_PORT="$(bashio::config 'mqtt_port')"
export MQTT_USERNAME="$(bashio::config 'mqtt_username')"
export MQTT_PASSWORD="$(bashio::config 'mqtt_password')"
export MQTT_TLS="$(bashio::config 'mqtt_tls')"
export MQTT_NAMESPACE="$(bashio::config 'mqtt_namespace')"
export DISABLE_MQTT="$(bashio::config 'disable_mqtt')"
export TZ="$(bashio::config 'timezone')"
export API_TOKEN="$(bashio::config 'api_token')"
export ENABLE_COMMANDS="$(bashio::config 'enable_commands')"
export COMMANDS_ALL="$(bashio::config 'commands_all')"
export COMMANDS_WAKE="$(bashio::config 'commands_wake')"
export COMMANDS_CHARGING="$(bashio::config 'commands_charging')"
export COMMANDS_CLIMATE="$(bashio::config 'commands_climate')"
export COMMANDS_DOORS="$(bashio::config 'commands_doors')"
export COMMANDS_LOGGING="$(bashio::config 'commands_logging')"

if [[ -z "${ENCRYPTION_KEY}" || "${ENCRYPTION_KEY}" == "MySuperSecretEncryptionKey" ]]; then
  bashio::log.fatal 'Set encryption_key to the key used by TeslaMate.'
  exit 1
fi

if [[ "${ENABLE_COMMANDS}" == "true" && ${#API_TOKEN} -lt 32 ]]; then
  bashio::log.fatal 'API token must contain at least 32 characters when commands are enabled.'
  exit 1
fi

exec /opt/app/app
