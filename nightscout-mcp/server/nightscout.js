const paths = new Set(['entries/sgv.json', 'treatments.json', 'profile.json', 'devicestatus.json', 'status.json']);
export class UpstreamError extends Error {}
export const pick = (obj, keys) => Object.fromEntries(keys.filter(k => obj?.[k] !== undefined).map(k => [k, obj[k]]));
export class Nightscout {
  constructor(config, fetcher = fetch) { this.config = config; this.fetcher = fetcher; }
  async get(path, query = {}) {
    if (!paths.has(path)) throw new UpstreamError('Unsupported Nightscout resource');
    const url = new URL(`${this.config.nightscout_url}/api/v1/${path}`);
    for (const [key, value] of Object.entries(query)) url.searchParams.set(key, String(value));
    try {
      const response = await this.fetcher(url, {
        method: 'GET', redirect: 'error', signal: AbortSignal.timeout(10000),
        headers: { accept: 'application/json', 'api-secret': this.config.nightscout_token },
      });
      if (!response.ok) {
        await response.body?.cancel();
        throw new UpstreamError(`Nightscout returned HTTP ${response.status}`);
      }
      if (!response.headers.get('content-type')?.includes('application/json')) {
        await response.body?.cancel();
        throw new UpstreamError('Nightscout did not return JSON');
      }
      let size = 0;
      const chunks = [];
      for await (const chunk of response.body) {
        size += chunk.length;
        if (size > 4 * 1024 * 1024) throw new UpstreamError('Nightscout response too large; request a smaller window');
        chunks.push(chunk);
      }
      const data = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      if (path === 'status.json' ? (!data || Array.isArray(data) || typeof data !== 'object') : (!Array.isArray(data) || data.some(x => !x || typeof x !== 'object' || Array.isArray(x))))
        throw new UpstreamError('Nightscout returned an unexpected data shape');
      return data;
    } catch (e) {
      if (e instanceof UpstreamError) throw e;
      // Never echo URLs, upstream bodies, credentials, or fetch error details.
      throw new UpstreamError('Nightscout unavailable, timed out, redirected, or returned invalid JSON');
    }
  }
}
export function windowFor(args, config, now = Date.now()) {
  const end = args.end ? Date.parse(args.end) : now;
  const start = args.start ? Date.parse(args.start) : end - 6 * 3600000;
  if (!Number.isFinite(start) || !Number.isFinite(end) || start >= end || end - start > config.max_history_hours * 3600000 || end > now + 60000)
    throw new UpstreamError(`Use a past UTC/offset time window of at most ${config.max_history_hours} hours`);
  return { start: new Date(start).toISOString(), end: new Date(end).toISOString(), startMs: start, endMs: end };
}
export function glucose(row, config, now = Date.now()) {
  const timestamp = Number.isFinite(row.date) ? row.date : Date.parse(row.dateString);
  const valid = Number.isFinite(row.sgv) && row.sgv >= 20 && row.sgv <= 1000 && Number.isFinite(timestamp) && timestamp <= now + 60000;
  return {
    ...pick(row, ['_id', 'direction', 'trend']),
    recorded_at: Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : null,
    glucose_mg_dl: valid ? row.sgv : null,
    glucose_mmol_l: valid ? Math.round(row.sgv / 18.0182 * 10) / 10 : null,
    age_minutes: Number.isFinite(timestamp) ? Math.round((now - timestamp) / 6000) / 10 : null,
    stale: !Number.isFinite(timestamp) || now - timestamp > config.stale_after_minutes * 60000,
    valid,
  };
}
export const treatment = row => pick(row, ['_id', 'created_at', 'timestamp', 'eventType', 'carbs', 'insulin', 'protein', 'fat', 'notes', 'enteredBy', 'duration', 'absolute', 'percent', 'rate', 'glucose', 'glucoseType', 'units']);
export function deviceStatus(row) {
  const out = pick(row, ['_id', 'created_at', 'date']);
  if (row.uploader) out.uploader = pick(row.uploader, ['battery', 'batteryVoltage']);
  if (row.pump) out.pump = { ...pick(row.pump, ['clock', 'reservoir']), battery: pick(row.pump.battery, ['percent', 'voltage']) };
  if (row.openaps?.iob) out.openaps_iob = row.openaps.iob;
  if (row.loop) out.loop = pick(row.loop, ['timestamp', 'iob', 'cob']);
  return out;
}
export function profile(row) {
  const out = pick(row, ['_id', 'defaultProfile', 'startDate', 'mills', 'units', 'created_at']);
  out.store = Object.fromEntries(Object.entries(row.store || {}).map(([name, value]) => [name, pick(value, ['dia', 'carbratio', 'carbs_hr', 'delay', 'sens', 'timezone', 'basal', 'target_low', 'target_high', 'units'])]));
  return out;
}
