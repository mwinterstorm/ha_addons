try {
  // Docker health commands do not inherit variables exported by the entrypoint.
  const options = JSON.parse(require('fs').readFileSync('/data/options.json', 'utf8'));
  const ns = db.getSiblingDB('nightscout');
  const auth = ns.auth('nightscout', options.password);
  if (auth !== 1 && auth?.ok !== 1) quit(1);
  if (ns.runCommand({ ping: 1 }).ok !== 1) quit(1);
} catch { quit(1); }
