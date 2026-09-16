# 3.0.0-2

Fix Nightscout history lookups for treatments, device status, glucose and activity.
URL-encode ISO timestamps, remove the incompatible space-date fallback, and keep
HTTP/invalid-response failures distinct from empty history. Add a 30-second read
timeout. Apply the small patch at build time with an upstream source checksum guard.

# 3.0.0-1

Initial experimental Home Assistant wrapper for upstream tconnectsync v3.0.0.
Pinned source and dependency hashes; persistent credential cache; non-root runtime;
US/EU selection; pump timezone; sync, preview and login-check modes.
CGM and profile syncing are opt-in.
