/********************************
 * Strava OAuth + sync helpers
 ********************************/

const STRAVA_BACKFILL_PER_PAGE_ = 200;
const STRAVA_BACKFILL_MAX_RUNTIME_MS_ = 4 * 60 * 1000;
const STRAVA_BACKFILL_TRIGGER_FN_ = 'stravaBackfillWorker_';

function apiGetStravaConnectUrl(args) {
  const auth = requireSessionFromArgs_(args || {});
  return { url: stravaAuthUrl_(auth.user.userId) };
}

function apiDisconnectStrava(args) {
  const auth = requireSessionFromArgs_(args || {});
  setStravaConnected_(auth.user.userId, false);
  return { ok: true };
}

function apiGetStravaStatus(args) {
  const auth = requireSessionFromArgs_(args || {});
  const status = getStravaConnectionStatus_(auth.user.userId);
  if (status.connected) installStravaTriggers_();
  return status;
}

function apiResumeStravaBackfill(args) {
  const auth = requireSessionFromArgs_(args || {});
  const userId = resolveTargetUserId_(auth.user, args || {});
  installStravaTriggers_();
  setBackfillState_(userId, {
    backfillDone: false,
    backfillLastRunAt: new Date(),
    updatedAt: new Date()
  });
  installStravaBackfillTrigger_();
  stravaBackfillWorker_();
  return getStravaConnectionStatus_(auth.user.userId);
}

function apiRestartStravaBackfill(args) {
  const auth = requireSessionFromArgs_(args || {});
  const userId = resolveTargetUserId_(auth.user, args || {});
   installStravaTriggers_();
  restartStravaBackfill_(userId);
  installStravaBackfillTrigger_();
  stravaBackfillWorker_();
  return getStravaConnectionStatus_(auth.user.userId);
}

function resolveTargetUserId_(user, args) {
  const reqUserId = String((args || {}).userId || '').trim();
  if (user.role === 'admin' && reqUserId) return reqUserId;
  return user.userId;
}

function isStravaOAuthCallback_(e) {
  return String((e.parameter || {}).route || '') === 'stravaCallback';
}

function handleStravaOAuthCallback_(e) {
  try {
    const statePayload = parseSignedState_((e.parameter || {}).state || '');
    const userId = statePayload.userId;
    const code = String((e.parameter || {}).code || '');
    if (!userId || !code) throw new Error('Missing state/code');

    const tokenResp = exchangeStravaCode_(code);
    upsertStravaAccount_(userId, tokenResp);
    restartStravaBackfill_(userId);
    installStravaTriggers_();
    installStravaBackfillTrigger_();
    stravaBackfillWorker_();

     return HtmlService.createHtmlOutput('<html><body style="font-family:sans-serif;padding:16px;">Strava connected. Historical import started and will continue in background.</body></html>');
  } catch (err) {
    const message = String((err && err.message) || err || 'Unknown error');
    const hint = message.indexOf('script.external_request') >= 0
      ? '<br/><br/>Fix: open the Apps Script project as the owner and re-authorize the web app so external requests are allowed.'
      : '';
    return HtmlService.createHtmlOutput('<html><body style="font-family:sans-serif;padding:16px;">Strava connect failed: ' + escHtml_(message) + hint + '</body></html>');
  }
}

function stravaAuthUrl_(userId) {
  const props = PropertiesService.getScriptProperties();
  const clientId = props.getProperty('STRAVA_CLIENT_ID');
  const base = props.getProperty('WEBAPP_BASE_URL');
  if (!clientId || !base) throw new Error('Missing STRAVA_CLIENT_ID or WEBAPP_BASE_URL script properties');

  const state = signState_({ userId, ts: Date.now() });
  const redirectUri = base + '?route=stravaCallback';
  const params = {
    client_id: clientId,
    redirect_uri: redirectUri,
    response_type: 'code',
    approval_prompt: 'auto',
    scope: 'activity:read_all',
    state
  };
  return 'https://www.strava.com/oauth/authorize?' + toQuery_(params);
}

function exchangeStravaCode_(code) {
  const props = PropertiesService.getScriptProperties();
  const payload = {
    client_id: props.getProperty('STRAVA_CLIENT_ID'),
    client_secret: props.getProperty('STRAVA_CLIENT_SECRET'),
    code,
    grant_type: 'authorization_code'
  };
  const resp = UrlFetchApp.fetch('https://www.strava.com/oauth/token', { method: 'post', payload, muteHttpExceptions: true });
  const json = JSON.parse(resp.getContentText() || '{}');
  if (resp.getResponseCode() >= 300) throw new Error('Strava token exchange failed: ' + resp.getContentText());
  return json;
}

function stravaEnsureAccessToken_(userId) {
  const acct = getStravaAccountByUserId_(userId);
  if (!acct || !isTruthy_(acct.connected)) throw new Error('Strava not connected');

  const nowSec = Math.floor(Date.now() / 1000);
  const exp = Number(acct.expiresAt || 0);
  if (exp > nowSec + 120) return acct.accessToken;

  const props = PropertiesService.getScriptProperties();
  const payload = {
    client_id: props.getProperty('STRAVA_CLIENT_ID'),
    client_secret: props.getProperty('STRAVA_CLIENT_SECRET'),
    grant_type: 'refresh_token',
    refresh_token: acct.refreshToken
  };
  const resp = UrlFetchApp.fetch('https://www.strava.com/oauth/token', { method: 'post', payload, muteHttpExceptions: true });
  if (resp.getResponseCode() >= 300) throw new Error('Strava refresh failed: ' + resp.getContentText());
  const json = JSON.parse(resp.getContentText() || '{}');
  upsertStravaAccount_(userId, json);
  return json.access_token;
}

function stravaApiGet_(accessToken, path, params) {
  const result = stravaApiGetResponse_(accessToken, path, params);
  if (result.responseCode >= 300) throw new Error('Strava API error ' + result.responseCode + ': ' + result.raw);
  return result.json;
}

function stravaApiGetResponse_(accessToken, path, params) {
  const url = 'https://www.strava.com/api/v3' + path + (params ? ('?' + toQuery_(params)) : '');
  const resp = UrlFetchApp.fetch(url, {
    method: 'get',
    muteHttpExceptions: true,
    headers: { Authorization: 'Bearer ' + accessToken }
  });
 const raw = resp.getContentText() || '{}';
  let json = {};
  try {
    json = JSON.parse(raw);
  } catch (e) {
    json = {};
  }
  return {
    json,
    raw,
    responseCode: resp.getResponseCode(),
    headers: resp.getAllHeaders()
  };
}

function stravaGetActivity_(userId, activityId) {
  const token = stravaEnsureAccessToken_(userId);
  return stravaApiGet_(token, '/activities/' + activityId, null);
}

function stravaListActivitiesResponse_(userId, page, perPage, after, before) {
  const token = stravaEnsureAccessToken_(userId);
  const params = { page: Number(page || 1), per_page: Number(perPage || 200) };
  if (after) params.after = Number(after);
  if (before) params.before = Number(before);
  return stravaApiGetResponse_(token, '/athlete/activities', params);
}

function stravaListActivities_(userId, page, perPage, after, before) {
  const result = stravaListActivitiesResponse_(userId, page, perPage, after, before);
  if (result.responseCode >= 300) throw new Error('Strava API error ' + result.responseCode + ': ' + result.raw);
  return result.json;
}

function syncCatchUp_() {
   stravaCatchupSync_();
}

function stravaCatchupSync_() {
  initTables_();
  const accounts = listStravaAccounts_().filter(a => isTruthy_(a.connected));
  accounts.forEach(a => syncUserActivities_(a.userId, { incremental: true, maxPages: 3 }));
}

function syncUserActivities_(userId, opts) {
  opts = opts || {};
  const acct = getStravaAccountByUserId_(userId);
  if (!acct || !isTruthy_(acct.connected)) return;

    let page = 1;
  const maxPages = Number(opts.maxPages || 10);
  let after = opts.incremental ? Number(acct.lastSyncAfter || 0) : 0;
  if (opts.fullBackfill) {
    page = 1;
    after = 0;
  }

  let newest = Number(acct.lastSyncAfter || 0);

  for (let i = 0; i < maxPages; i++) {
    const rows = stravaListActivities_(userId, page, STRAVA_BACKFILL_PER_PAGE_, after, null);
    if (!rows || !rows.length) break;
    upsertActivitiesBatchFromStrava_(userId, Number(acct.stravaAthleteId || 0), rows, false);
    upsertLogsBatchFromStravaActivities_(userId, rows);
    newest = Math.max(newest, newestEpochFromActivities_(rows));
    page += 1;
  }

   if (newest > Number(acct.lastSyncAfter || 0)) {
    setBackfillState_(userId, { lastSyncAfter: newest, updatedAt: new Date() });
  }
}

function stravaBackfillWorker_() {
  initTables_();
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return;

  try {
    const startedAt = Date.now();
    let reqCount = 0;
    const accounts = listStravaAccounts_().filter(a => isTruthy_(a.connected) && !isTruthy_(a.backfillDone));
    if (!accounts.length) {
      removeStravaBackfillTrigger_();
      return;
    }

    for (let i = 0; i < accounts.length; i++) {
      const acct = accounts[i];
      let mode = String(acct.backfillMode || 'beforeCursor');
      let page = Math.max(1, Number(acct.backfillPage || 1));
      let beforeEpoch = Number(acct.backfillBeforeEpoch || 0);
      let newestSeen = Number(acct.lastSyncAfter || 0);

      while ((Date.now() - startedAt) <= (STRAVA_BACKFILL_MAX_RUNTIME_MS_ - 10000) && reqCount < 10) {
        const reqPage = mode === 'page' ? page : 1;
        const response = stravaListActivitiesResponse_(acct.userId, reqPage, STRAVA_BACKFILL_PER_PAGE_, null, beforeEpoch || null);
        reqCount++;
        logStravaRateHeaders_(acct.userId, response.headers, response.responseCode);

        if (response.responseCode === 429) {
          Utilities.sleep(1500);
          setBackfillState_(acct.userId, {
            backfillLastRunAt: new Date(),
            updatedAt: new Date()
          });
          break;
        }
        if (response.responseCode >= 300) {
          throw new Error('Backfill failed for user ' + acct.userId + ': ' + response.responseCode + ' ' + response.raw);
        }

        const rows = response.json || [];
        if (!rows.length) {
          setBackfillState_(acct.userId, {
            backfillDone: true,
            backfillLastRunAt: new Date(),
            updatedAt: new Date(),
            lastSyncAfter: newestSeen
          });
          break;
        }

        upsertActivitiesBatchFromStrava_(acct.userId, Number(acct.stravaAthleteId || 0), rows, false);
        upsertLogsBatchFromStravaActivities_(acct.userId, rows);
        newestSeen = Math.max(newestSeen, newestEpochFromActivities_(rows));

        const stateUpdate = {
          backfillLastRunAt: new Date(),
          updatedAt: new Date(),
          lastSyncAfter: newestSeen
        };
        if (mode === 'page') {
          page += 1;
          stateUpdate.backfillPage = page;
        } else {
          mode = 'beforeCursor';
          beforeEpoch = oldestEpochFromActivities_(rows);
          stateUpdate.backfillMode = mode;
          stateUpdate.backfillBeforeEpoch = beforeEpoch;
        }
        setBackfillState_(acct.userId, stateUpdate);
      }

      if ((Date.now() - startedAt) > (STRAVA_BACKFILL_MAX_RUNTIME_MS_ - 10000) || reqCount >= 10) break;
    }

    const remaining = listStravaAccounts_().some(a => isTruthy_(a.connected) && !isTruthy_(a.backfillDone));
    if (!remaining) removeStravaBackfillTrigger_();
  } finally {
    lock.releaseLock();
  }
}

function newestEpochFromActivities_(activities) {
  let newest = 0;
  (activities || []).forEach(a => {
    const ts = toEpochSec_(a.start_date || a.start_date_local);
    if (ts > newest) newest = ts;
  });
  return newest;
}

function oldestEpochFromActivities_(activities) {
  let oldest = 0;
  (activities || []).forEach(a => {
    const ts = toEpochSec_(a.start_date || a.start_date_local);
    if (!oldest || (ts > 0 && ts < oldest)) oldest = ts;
  });
  return oldest > 0 ? Math.max(1, oldest - 1) : 0;
}

function logStravaRateHeaders_(userId, headers, code) {
  const all = headers || {};
  const shortLimit = all['X-RateLimit-Limit'] || all['x-ratelimit-limit'] || '';
  const shortUsage = all['X-RateLimit-Usage'] || all['x-ratelimit-usage'] || '';
  console.log('Strava rate user=' + userId + ' code=' + code + ' limit=' + shortLimit + ' usage=' + shortUsage);
}

function installStravaBackfillTrigger_() {
  const triggers = ScriptApp.getProjectTriggers().filter(t => t.getHandlerFunction() === STRAVA_BACKFILL_TRIGGER_FN_);
  if (!triggers.length) {
    ScriptApp.newTrigger(STRAVA_BACKFILL_TRIGGER_FN_).timeBased().everyMinutes(1).create();
  }
}

function removeStravaBackfillTrigger_() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === STRAVA_BACKFILL_TRIGGER_FN_)
    .forEach(t => ScriptApp.deleteTrigger(t));
}

function restartStravaBackfill_(userId) {
  const nowEpoch = Math.floor(Date.now() / 1000);
  setBackfillState_(userId, {
    backfillMode: 'beforeCursor',
    backfillPage: 1,
    backfillBeforeEpoch: nowEpoch,
    backfillDone: false,
    backfillLastRunAt: '',
    updatedAt: new Date()
  });
}

function upsertActivityFromStrava_(userId, athleteId, activity, deletedFlag) {
  upsertActivitiesBatchFromStrava_(userId, athleteId, [activity], deletedFlag);
}

function upsertActivitiesBatchFromStrava_(userId, athleteId, activities, deletedFlag) {
  initTables_();
  const sh = getSheet_('Activities');
  const data = sh.getDataRange().getValues();
  if (!data.length) return;
  const h = headerMap_(data[0]);

  const matrix = data.map(row => {
    const out = row.slice(0, ACTIVITY_HEADERS_.length);
    while (out.length < ACTIVITY_HEADERS_.length) out.push('');
    return out;
  });
  const idToIndex = {};
  for (let r = 1; r < matrix.length; r++) {
    const id = String(matrix[r][h.stravaActivityId] || '').trim();
    if (id) idToIndex[id] = r;
  }

  (activities || []).forEach(activity => {
    const id = String(activity.id || activity.object_id || '');
    if (!id) return;
    const rowObj = activityRowFromStrava_(userId, athleteId, activity, deletedFlag);
    const vals = ACTIVITY_HEADERS_.map(k => rowObj[k] == null ? '' : rowObj[k]);
    const idx = idToIndex[id];
    if (idx != null) matrix[idx] = vals;
    else {
      idToIndex[id] = matrix.length;
      matrix.push(vals);
    }
  });

  sh.getRange(1, 1, matrix.length, ACTIVITY_HEADERS_.length).setValues(matrix);
}

function activityRowFromStrava_(userId, athleteId, activity, deletedFlag) {
  return {
    stravaActivityId: String(activity.id || activity.object_id || ''),
    userId,
    athleteId: athleteId || Number((activity.athlete || {}).id || 0),
    startDate: activity.start_date || activity.start_date_local || '',
    type: activity.type || '',
    name: activity.name || '',
    distanceM: Number(activity.distance || 0),
    movingTimeS: Number(activity.moving_time || 0),
    elapsedTimeS: Number(activity.elapsed_time || 0),
    totalElevationGainM: Number(activity.total_elevation_gain || 0),
    averageHeartrate: Number(activity.average_heartrate || 0),
    maxHeartrate: Number(activity.max_heartrate || 0),
    averageSpeed: Number(activity.average_speed || 0),
    summaryPolyline: ((activity.map || {}).summary_polyline || ''),
    rawJson: JSON.stringify(activity).slice(0, 45000),
    deleted: !!deletedFlag,
    updatedAt: new Date()
  };
}

  function upsertLogsBatchFromStravaActivities_(userId, activities) {
  const matches = (activities || []).map(a => ({
    activity: a,
    match: matchPlanForActivity_(a)
  }));

  matches.forEach(item => {
    const a = item.activity;
    const m = item.match;
    const km = Number(a.distance || 0) / 1000;
    const min = Math.round(Number(a.moving_time || 0) / 60);
    upsertLog_({
      PlanID: m.planId,
      Status: 'DONE',
      ActualKm: km,
      ActualMin: min,
      CompletedAt: new Date(a.start_date || a.start_date_local || new Date()),
      LogNotes: 'Imported from Strava: ' + String(a.name || 'Activity'),
      UserId: userId,
      Source: 'strava',
      StravaActivityId: String(a.id || ''),
      SportType: String(a.type || ''),
      ImportedAt: new Date(),
      PlanMatchConfidence: m.confidence,
      PlanMatchReason: m.reason
    });
  });
}

function upsertLogFromStravaActivity_(userId, activity) {
  upsertLogsBatchFromStravaActivities_(userId, [activity]);
}

function matchPlanForActivity_(activity) {
  const settings = getSettings_();
  const tz = settings.Timezone || Session.getScriptTimeZone();
  const dateISO = toIsoDate_(activity.start_date_local || activity.start_date, tz);
  if (!dateISO) return { planId: 'UNPLANNED', confidence: 0, reason: 'no_date' };

  const candidates = readPlanBetween_(dateISO, dateISO, tz);
  if (!candidates.length) return { planId: 'UNPLANNED', confidence: 0, reason: 'no_plan_on_date' };

  const type = String(activity.type || '').toLowerCase();
  const bySport = candidates.filter(c => String(c.Sport || '').toLowerCase().includes(type) || type.includes(String(c.Sport || '').toLowerCase()));
  if (bySport.length) return { planId: bySport[0].PlanID, confidence: 0.9, reason: 'same_day_sport' };
  return { planId: candidates[0].PlanID, confidence: 0.5, reason: 'same_day_nearest' };
}

function getStravaConnectionStatus_(userId) {
  const acct = getStravaAccountByUserId_(userId);
  if (!acct) return { connected: false };
  return {
    connected: isTruthy_(acct.connected),
    athleteId: acct.stravaAthleteId || '',
    expiresAt: acct.expiresAt || '',
   scope: acct.scope || '',
    backfillMode: acct.backfillMode || 'beforeCursor',
    backfillPage: Number(acct.backfillPage || 1),
    backfillBeforeEpoch: Number(acct.backfillBeforeEpoch || 0),
    backfillDone: isTruthy_(acct.backfillDone),
    backfillLastRunAt: acct.backfillLastRunAt || '',
    lastSyncAfter: Number(acct.lastSyncAfter || 0)
  };
}

function upsertStravaAccount_(userId, tokenResp) {
  initTables_();
  const athleteId = Number(((tokenResp.athlete || {}).id) || tokenResp.athlete_id || 0);
  const existing = getStravaAccountByUserId_(userId) || {};
  const rowObj = {
    userId,
    stravaAthleteId: athleteId || Number(existing.stravaAthleteId || 0),
    accessToken: tokenResp.access_token,
    refreshToken: tokenResp.refresh_token,
    expiresAt: Number(tokenResp.expires_at || 0),
    scope: tokenResp.scope || existing.scope || '',
    connected: true,
   lastSyncAfter: Number(existing.lastSyncAfter || 0),
    createdAt: existing.createdAt || new Date(),
    updatedAt: new Date(),
    backfillMode: existing.backfillMode || 'beforeCursor',
    backfillPage: Number(existing.backfillPage || 1),
    backfillBeforeEpoch: Number(existing.backfillBeforeEpoch || 0),
    backfillDone: isTruthy_(existing.backfillDone),
    backfillLastRunAt: existing.backfillLastRunAt || ''
  };

  const sh = getSheet_('StravaAccounts');
  const data = sh.getDataRange().getValues();
  const h = headerMap_(data[0]);
  let found = -1;
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.userId] || '') === String(userId)) { found = r + 1; break; }
  }
  const vals = STRAVA_ACCOUNT_HEADERS_.map(k => rowObj[k] == null ? '' : rowObj[k]);
  if (found < 0) sh.appendRow(vals);
  else sh.getRange(found, 1, 1, vals.length).setValues([vals]);
}

function setStravaConnected_(userId, connected) {
  const sh = getSheet_('StravaAccounts');
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return;
  const h = headerMap_(data[0]);
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.userId] || '') === String(userId)) {
      sh.getRange(r + 1, h.connected + 1).setValue(!!connected);
      sh.getRange(r + 1, h.updatedAt + 1).setValue(new Date());
      return;
    }
  }
}

function setBackfillState_(userId, state) {
  const sh = getSheet_('StravaAccounts');
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return;
  const h = headerMap_(data[0]);
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.userId] || '') === String(userId)) {
      Object.keys(state || {}).forEach(key => writeIfHeader_(sh, r + 1, h, key, state[key]));
      return;
    }
  }
}

function getStravaAccountByUserId_(userId) {
  return listStravaAccounts_().find(a => String(a.userId) === String(userId)) || null;
}

function getStravaAccountByAthleteId_(athleteId) {
  return listStravaAccounts_().find(a => String(a.stravaAthleteId) === String(athleteId)) || null;
}

function listStravaAccounts_() {
  const data = getSheet_('StravaAccounts').getDataRange().getValues();
  if (data.length < 2) return [];
  const h = headerMap_(data[0]);
  return data.slice(1).filter(r => String(r[h.userId] || '').trim()).map(r => rowToObj_(h, r));
}

function signState_(payload) {
  const body = Utilities.base64EncodeWebSafe(JSON.stringify(payload));
  const secret = String(PropertiesService.getScriptProperties().getProperty('AUTH_SALT') || 'change-me');
  const sigBytes = Utilities.computeHmacSha256Signature(body, secret);
  const sig = Utilities.base64EncodeWebSafe(sigBytes);
  return body + '.' + sig;
}

function parseSignedState_(state) {
  const parts = String(state || '').split('.');
  const body = parts[0];
  const sig = parts[1];
  if (!body || !sig) throw new Error('Invalid OAuth state');
  const secret = String(PropertiesService.getScriptProperties().getProperty('AUTH_SALT') || 'change-me');
  const expected = Utilities.base64EncodeWebSafe(Utilities.computeHmacSha256Signature(body, secret));
  if (expected !== sig) throw new Error('State signature mismatch');
  const payload = JSON.parse(Utilities.newBlob(Utilities.base64DecodeWebSafe(body)).getDataAsString());
  if (Date.now() - Number(payload.ts || 0) > 15 * 60 * 1000) throw new Error('State expired');
  return payload;
}

function toQuery_(obj) {
  return Object.keys(obj).filter(k => obj[k] !== '' && obj[k] != null).map(k => encodeURIComponent(k) + '=' + encodeURIComponent(String(obj[k]))).join('&');
}

function toEpochSec_(iso) {
  if (!iso) return 0;
  const d = new Date(iso);
  return Math.floor(d.getTime() / 1000);
}

function escHtml_(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
}

function installStravaTriggers_() {
  const fnNames = ScriptApp.getProjectTriggers().map(t => t.getHandlerFunction());
  if (!fnNames.includes('processWebhookQueue_')) {
    ScriptApp.newTrigger('processWebhookQueue_').timeBased().everyMinutes(5).create();
  }
 if (!fnNames.includes('stravaCatchupSync_')) {
    ScriptApp.newTrigger('stravaCatchupSync_').timeBased().everyHours(1).create();
  }
installStravaBackfillTrigger_();
}

function setupInstructions_() {
  return [
    'Setup steps:',
    '1) In Script Properties set STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_VERIFY_TOKEN, WEBAPP_BASE_URL, AUTH_SALT.',
    '2) Deploy web app and set WEBAPP_BASE_URL to that deployment URL.',
    '3) Configure Strava app callback URL: WEBAPP_BASE_URL?route=stravaCallback.',
    '4) Create Strava webhook subscription pointing to WEBAPP_BASE_URL (same endpoint).',
    '5) Run initTables_() and installStravaTriggers_() once as admin.'
  ].join('\n');
}
