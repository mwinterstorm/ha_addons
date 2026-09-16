'use strict';
// Pure configuration translation: no shell interpolation or secret logging.
function environment(o) {
  const secret = o.api_secret;
  if (typeof secret !== 'string' || secret.trim() !== secret || secret.length < 12 || /[\r\n\0]/.test(secret))
    throw new Error('api_secret must have at least 12 characters and no surrounding whitespace or line breaks');
  let uri = o.mongo_uri || '';
  if (!uri) {
    if (!/^[a-zA-Z0-9.-]+$/.test(o.mongo_host || '')) throw new Error('Invalid mongo_host');
    if (typeof o.mongo_password !== 'string' || o.mongo_password.length < 16 || /[\r\n\0]/.test(o.mongo_password))
      throw new Error('mongo_password must have at least 16 characters');
    uri = `mongodb://nightscout:${encodeURIComponent(o.mongo_password)}@${o.mongo_host}:27017/nightscout?authSource=nightscout`;
  }
  if (!/^mongodb(?:\+srv)?:\/\/[^\s]+$/.test(uri)) throw new Error('Invalid mongo_uri');
  if (!['mg/dl', 'mmol/L'].includes(o.display_units)) throw new Error('Invalid display_units');
  if (!['status-only', 'denied', 'readable'].includes(o.auth_default_roles)) throw new Error('Invalid auth_default_roles');
  if (o.base_url && !/^https?:\/\/[^\s]+$/.test(o.base_url)) throw new Error('Invalid base_url');
  const env = {
    NODE_ENV: 'production', PORT: '1337', NIGHTSCOUT_HOSTNAME: '0.0.0.0',
    INSECURE_USE_HTTP: 'true', MONGO_CONNECTION: uri,
    DISPLAY_UNITS: o.display_units, AUTH_DEFAULT_ROLES: o.auth_default_roles,
    ENABLE: o.enable || '', BASE_URL: o.base_url || '',
    API_SECRET_FILE: '/run/nightscout/api-secret',
    ALLOW_UNRESTRICTED_FRAME_EMBEDDING: 'false',
  };
  const reserved = new Set([...Object.keys(env), 'API_SECRET', 'HOSTNAME', 'MONGODB_URI',
    'STORAGE_URI', 'MONGO', 'MONGOLAB_URI', 'NODE_OPTIONS', 'NODE_PATH', 'PATH',
    'HOME', 'LD_PRELOAD', 'LD_LIBRARY_PATH', 'SSL_KEY', 'SSL_CERT']);
  for (const item of o.extra_env || []) {
    if (!/^[A-Z][A-Z0-9_]*$/.test(item.name) || reserved.has(item.name) || item.name.endsWith('_FILE'))
      throw new Error('extra_env contains a reserved or invalid name');
    if (typeof item.value !== 'string' || item.value.includes('\0')) throw new Error('Invalid extra_env value');
    env[item.name] = item.value;
    reserved.add(item.name);
  }
  return env;
}
module.exports = { environment };
