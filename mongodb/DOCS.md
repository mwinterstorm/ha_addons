# MongoDB companion

Set `password` to a random string of at least 16 characters, preferably 32+.
`cache_size_gb` defaults to 0.5 (WiredTiger cache only, not a total memory limit).
Start this app before Nightscout. On a local install its hostname is
`local-nightscout-mongodb`; use the actual Hostname from the app information page
for a Git repository installation. MongoDB port 27017 is internal only.

On an empty `/data/mongodb`, the official image creates a generated administrator
account `ha_admin` and the adapter creates `nightscout` with readWrite permissions
only on the `nightscout` database. The admin password is stored at
`/data/.root-password` (root-only). Nightscout receives only the scoped user's
password. MongoDB runs as the official image's unprivileged `mongodb` user.

This image requires AVX on amd64 and ARMv8.2-A or later on aarch64. Raspberry Pi 4
cannot run it. An `Illegal instruction` startup failure usually means incompatible
CPU features or VM masking. Do not downgrade to an obsolete MongoDB to work around
hardware limits: point Nightscout at a separately hosted supported database.
Consult MongoDB's current production notes for kernel restrictions as well.

## Persistence and backups

`/data/mongodb` holds database and journal files. The upstream image declares
anonymous volumes at `/data/db` and `/data/configdb`; these are unused. A separate
path avoids hiding real records from Supervisor backups of `/data`.
Supervisor mounts persistent `/data` without a `map` entry. Updates and restarts
preserve it; uninstalling the app can remove it. Keep free space monitored.
MongoDB's own log goes to the app log; no scheduled data pruning is enabled.

The app specifies `backup: cold`, so Supervisor stops MongoDB before copying its
files and restarts it afterward. This makes a consistent standalone database
backup and temporarily interrupts Nightscout. For a predictable manual backup:

1. Stop Nightscout (pause uploaders if they do not queue/retry).
2. Create an HA backup including **both** Nightscout and Nightscout MongoDB.
3. Download/secure that backup and its encryption recovery information.
4. Confirm MongoDB has restarted, then start Nightscout and verify fresh data.

Restore both apps from the same backup on compatible hardware. Start MongoDB
first, verify logs, then Nightscout. Check historical readings and a newly uploaded
reading. Keep the original backup until restore verification completes. Scheduled
cold database backups can interrupt active clients; configure an appropriate time.
A Nightscout-only backup omits the database; an external MongoDB needs external backups.

## Existing installations and database upgrades

This is not an in-place upgrade of marciogranzotto's app: its storage and identity
are different. Keep the old app and backup until the replacement is verified.
Do not copy old MongoDB data files into this image. Do not start MongoDB 8 against
a MongoDB 4.x data directory.

For migration, take a backup of the old app and identify its actual MongoDB version.
Use a MongoDB-supported logical dump/restore or staged major-version upgrade path
for that version. Stop writes during the final transfer; restore the Nightscout
database into the freshly initialized destination, preserving this companion's
users. Do not restore old `admin` users or overwrite the companion marker files.
Use compatible MongoDB Database Tools, verify collection counts/history/profiles,
and then redirect uploaders. The exact commands depend on the old server version
and how its data can be accessed; this repository deliberately performs no automatic
migration or destructive reset. MongoDB's official upgrade guide is linked in
`SOURCES.md` at the repository root.

## Password changes and interrupted initialization

Changing the HA `password` option alone does not change an existing MongoDB user.
The wrapper detects this and refuses startup instead of silently breaking clients.
Restore the original option to recover. Do not delete the data directory.

For an intentional rotation, take a backup and stop Nightscout. An administrator
must use an authenticated MongoDB shell to update the `nightscout` user's password,
update `/data/mongodb/.ha-password-hash` to the SHA-256 of the exact new password (no
newline), then change this app's option and Nightscout's `mongo_password`, and
restart both. This is an advanced maintenance operation requiring container/admin
access; normal add-on operation requires neither host access nor protection-mode
changes. Retain the backup until authenticated read/write has been checked.
The generated administrator password is not rotated by changing the app option.

If first initialization fails after creating storage but before creating the user,
the next start refuses to adopt the partial database. For a confirmed empty,
never-used installation only, uninstall/reinstall this companion to initialize
again. For any installation containing data, restore a known-good backup or
recover with MongoDB administration tools; never erase it as a troubleshooting step.
