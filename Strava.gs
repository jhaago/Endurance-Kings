/********************************
 * Strava OAuth + sync helpers
 ********************************/

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
  return getStravaConnectionStatus_(auth.user.userId);
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
    syncUserActivities_(userId, { fullBackfill: true, maxPages: 3 });

    return HtmlService.createHtmlOutput('<html><body style="font-family:sans-serif;padding:16px;">Strava connected. You can close this tab and return to the app.</body></html>');
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
  const url = 'https://www.strava.com/api/v3' + path + (params ? ('?' + toQuery_(params)) : '');
  const resp = UrlFetchApp.fetch(url, {
    method: 'get',
    muteHttpExceptions: true,
    headers: { Authorization: 'Bearer ' + accessToken }
  });
  if (resp.getResponseCode() >= 300) throw new Error('Strava API error ' + resp.getResponseCode() + ': ' + resp.getContentText());
  return JSON.parse(resp.getContentText() || '{}');
}

function stravaGetActivity_(userId, activityId) {
  const token = stravaEnsureAccessToken_(userId);
  return stravaApiGet_(token, '/activities/' + activityId, null);
}

function stravaListActivities_(userId, page, perPage, after, before) {
  const token = stravaEnsureAccessToken_(userId);
  const params = { page: Number(page || 1), per_page: Number(perPage || 200) };
  if (after) params.after = Number(after);
  if (before) params.before = Number(before);
  return stravaApiGet_(token, '/athlete/activities', params);
}

function syncCatchUp_() {
  initTables_();
  const accounts = listStravaAccounts_().filter(a => isTruthy_(a.connected));
  accounts.forEach(a => syncUserActivities_(a.userId, { incremental: true, maxPages: 3 }));
}

function syncUserActivities_(userId, opts) {
  opts = opts || {};
  const acct = getStravaAccountByUserId_(userId);
  if (!acct || !isTruthy_(acct.connected)) return;

  let page = Number(acct.backfillPage || 1);
  const maxPages = Number(opts.maxPages || 10);
  let after = opts.incremental ? Number(acct.lastSyncAfter || 0) : 0;
  if (opts.fullBackfill) {
    page = 1;
    after = 0;
  }

  let processed = 0;
  let newest = Number(acct.lastSyncAfter || 0);

  for (let i = 0; i < maxPages; i++) {
    const rows = stravaListActivities_(userId, page, 200, after, null);
    if (!rows || !rows.length) {
      setBackfillState_(userId, page, true, newest);
      break;
    }
    rows.forEach(a => {
      upsertActivityFromStrava_(userId, Number(acct.stravaAthleteId || 0), a, false);
      upsertLogFromStravaActivity_(userId, a);
      const st = toEpochSec_(a.start_date || a.start_date_local);
      if (st > newest) newest = st;
      processed++;
    });
    page += 1;
  }

  if (processed > 0) setBackfillState_(userId, page, false, newest);
}

function upsertActivityFromStrava_(userId, athleteId, activity, deletedFlag) {
  initTables_();
  const sh = getSheet_('Activities');
  const data = sh.getDataRange().getValues();
  const h = headerMap_(data[0]);
  const id = String(activity.id || activity.object_id || '');
  if (!id) return;

  const rowObj = {
    stravaActivityId: id,
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

  let found = -1;
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.stravaActivityId] || '') === id) { found = r + 1; break; }
  }

  const vals = ACTIVITY_HEADERS_.map(k => rowObj[k] == null ? '' : rowObj[k]);
  if (found < 0) sh.appendRow(vals);
  else sh.getRange(found, 1, 1, vals.length).setValues([vals]);
}

function upsertLogFromStravaActivity_(userId, activity) {
  const match = matchPlanForActivity_(activity);
  const km = Number(activity.distance || 0) / 1000;
  const min = Math.round(Number(activity.moving_time || 0) / 60);
  upsertLog_({
    PlanID: match.planId,
    Status: 'DONE',
    ActualKm: km,
    ActualMin: min,
    CompletedAt: new Date(activity.start_date || activity.start_date_local || new Date()),
    LogNotes: 'Imported from Strava: ' + String(activity.name || 'Activity'),
    UserId: userId,
    Source: 'strava',
    StravaActivityId: String(activity.id || ''),
    SportType: String(activity.type || ''),
    ImportedAt: new Date(),
    PlanMatchConfidence: match.confidence,
    PlanMatchReason: match.reason
  });
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
    scope: acct.scope || ''
  };
}

function upsertStravaAccount_(userId, tokenResp) {
  initTables_();
  const athleteId = Number(((tokenResp.athlete || {}).id) || tokenResp.athlete_id || 0);
  const rowObj = {
    userId,
    stravaAthleteId: athleteId,
    accessToken: tokenResp.access_token,
    refreshToken: tokenResp.refresh_token,
    expiresAt: Number(tokenResp.expires_at || 0),
    scope: tokenResp.scope || '',
    connected: true,
    lastSyncAfter: 0,
    createdAt: new Date(),
    updatedAt: new Date(),
    backfillPage: 1,
    backfillDone: false
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

function setBackfillState_(userId, page, done, lastSyncAfter) {
  const sh = getSheet_('StravaAccounts');
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return;
  const h = headerMap_(data[0]);
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.userId] || '') === String(userId)) {
      writeIfHeader_(sh, r + 1, h, 'backfillPage', page);
      writeIfHeader_(sh, r + 1, h, 'backfillDone', !!done);
      writeIfHeader_(sh, r + 1, h, 'lastSyncAfter', Number(lastSyncAfter || 0));
      writeIfHeader_(sh, r + 1, h, 'updatedAt', new Date());
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
  const [body, sig] = String(state || '').split('.');
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
  if (!fnNames.includes('syncCatchUp_')) {
    ScriptApp.newTrigger('syncCatchUp_').timeBased().everyHours(1).create();
  }
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
