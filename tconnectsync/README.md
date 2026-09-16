# tconnectsync for Home Assistant

A Home Assistant OS add-on for [upstream tconnectsync](https://github.com/jwoglom/tconnectsync),
pinned to **v3.0.0**, with the **3.0.0-2** Nightscout date-query compatibility fix. It copies records from a Tandem Source account into Nightscout.
This is a background service with no web interface and no extra database.

Copy this entire `tconnectsync` folder alongside `nightscout` and `mongodb` in your
existing add-ons repository, commit/push, and refresh the Home Assistant app store.
Keep your existing repository.yaml. For a local installation, put this folder
under `/addons/tconnectsync` instead.

## Quick setup

1. Ensure Nightscout is running and your Tandem Source account already shows pump
   uploads. This add-on reads the cloud service; it cannot make the pump upload.
2. Install **tconnectsync** and enter your Tandem email/password and account region.
   Upstream supports `US` or `EU`; select the backend you use, not your current
   physical location. New Zealand account compatibility has not been verified.
3. Set `nightscout_url` to `http://NIGHTSCOUT_ADDON_HOSTNAME:1337`, using the Hostname
   shown on Nightscout's information page. The default `local-nightscout` is only
   correct for local installation. Enter Nightscout's API secret separately.
4. Set `timezone` to the pump's timezone, for example `Pacific/Auckland`. Optionally
   supply its numeric serial number; otherwise upstream chooses the most recently
   used pump.
5. To test credentials first, set `mode: check_login`, save and start. Read Logs;
   the add-on stops when this one-off check completes. Keep Watchdog off for this mode.
6. Set `mode: sync`, save and start to upload continuously. The default selected
   features are BASAL, BOLUS and PUMP_EVENTS. Check a new record in Nightscout and
   confirm its timestamp and source. Avoid concurrent uploaders of the same pump data.

CGM and PROFILES are deliberately opt-in. Keep your existing xDrip+/Dexcom glucose
uploader. Tandem-derived CGM data is delayed, and enabling PROFILES allows upstream
to add pump-derived Nightscout profiles. See [DOCS.md](DOCS.md) before enabling either.

**Experimental:** configuration and dependency checks have been performed, but the
container and live Tandem-to-Nightscout sync have not been tested. See
[VALIDATION.md](VALIDATION.md) for the exact verification boundary.

## Updating from 3.0.0-1

Stop tconnectsync. Replace this folder in your existing repository with the entire
updated folder, including `patches`, Dockerfile and config.yaml. Commit/push, then
refresh the HA app store and update tconnectsync to **3.0.0-2**. If necessary use
its Rebuild action after refreshing. Restart Nightscout once if the earlier request
left it stopped, then start tconnectsync with `mode: sync`.

Do not uninstall either add-on or clear MongoDB. Existing options and cached login
state should remain. DEVICE_STATUS can be re-enabled after installing this patch;
leave your other intended feature choices unchanged. Check Logs for completed
processing and verify new records in Nightscout. Full live sync still needs checking
on your host; tests reproduce the request bug without using your account.
