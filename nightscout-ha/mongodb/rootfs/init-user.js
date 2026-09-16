// Executed by the official image only while initializing an empty database.
const ns = db.getSiblingDB('nightscout');
ns.createUser({ user: 'nightscout', pwd: process.env.NS_DB_PASSWORD,
  roles: [{ role: 'readWrite', db: 'nightscout' }] });
require('fs').writeFileSync('/data/mongodb/.ha-password-hash', process.env.NS_PASSWORD_HASH, { mode: 0o600 });
