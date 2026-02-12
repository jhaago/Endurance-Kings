/********************************
 * Auth + table initialization
 ********************************/

const USER_HEADERS_ = ['userId', 'email', 'displayName', 'passwordHash', 'role', 'createdAt', 'lastLoginAt', 'isActive'];
const SESSION_HEADERS_ = ['sessionToken', 'userId', 'expiresAt', 'createdAt', 'lastSeenAt'];
const STRAVA_ACCOUNT_HEADERS_ = ['userId', 'stravaAthleteId', 'accessToken', 'refreshToken', 'expiresAt', 'scope', 'connected', 'lastSyncAfter', 'createdAt', 'updatedAt', 'backfillMode', 'backfillPage', 'backfillBeforeEpoch', 'backfillDone', 'backfillLastRunAt'];
const ACTIVITY_HEADERS_ = ['stravaActivityId', 'userId', 'athleteId', 'startDate', 'type', 'name', 'distanceM', 'movingTimeS', 'elapsedTimeS', 'totalElevationGainM', 'averageHeartrate', 'maxHeartrate', 'averageSpeed', 'summaryPolyline', 'rawJson', 'deleted', 'updatedAt'];
const WEBHOOK_QUEUE_HEADERS_ = ['queuedAt', 'subscriptionId', 'objectType', 'aspectType', 'objectId', 'ownerId', 'eventTime', 'updatesJson', 'processed', 'processedAt', 'error'];
const LOG_HEADERS_ = ['PlanID', 'Status', 'ActualKm', 'ActualMin', 'CompletedAt', 'LogNotes', 'UserId', 'Source', 'StravaActivityId', 'SportType', 'ImportedAt', 'PlanMatchConfidence', 'PlanMatchReason'];

function initTables_() {
  const ss = SpreadsheetApp.getActive();
  ensureSheetHeaders_(ss, 'Users', USER_HEADERS_);
  ensureSheetHeaders_(ss, 'Sessions', SESSION_HEADERS_);
  ensureSheetHeaders_(ss, 'StravaAccounts', STRAVA_ACCOUNT_HEADERS_);
  ensureSheetHeaders_(ss, 'Activities', ACTIVITY_HEADERS_);
  ensureSheetHeaders_(ss, 'WebhookQueue', WEBHOOK_QUEUE_HEADERS_);
  ensureSheetHeaders_(ss, 'Log', LOG_HEADERS_);
  ensurePlanHeaders_();
}

function ensureSheetHeaders_(ss, name, expectedHeaders) {
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    sh.getRange(1, 1, 1, expectedHeaders.length).setValues([expectedHeaders]);
    return sh;
  }
  if (sh.getLastRow() < 1) {
    sh.getRange(1, 1, 1, expectedHeaders.length).setValues([expectedHeaders]);
    return sh;
  }
  const current = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(String);
  expectedHeaders.forEach(h => {
    if (!current.includes(h)) {
      sh.getRange(1, sh.getLastColumn() + 1).setValue(h);
      current.push(h);
    }
  });
  return sh;
}

function apiInitTables() {
  initTables_();
  return { ok: true };
}

function apiGetAuthState(args) {
  args = args || {};
  initTables_();
  const users = listUsers_();
  if (!users.length) return { needsSetup: true, authenticated: false };

  const token = String(args.sessionToken || '');
  if (!token) return { needsSetup: false, authenticated: false };
  const session = getValidSession_(token);
  if (!session) return { needsSetup: false, authenticated: false };

  const user = getUserById_(session.userId);
  if (!user || !isTruthy_(user.isActive)) return { needsSetup: false, authenticated: false };

  touchSession_(token);
  return {
    needsSetup: false,
    authenticated: true,
    user: publicUser_(user),
    strava: getStravaConnectionStatus_(user.userId)
  };
}

function apiCreateAdmin(payload) {
  payload = payload || {};
  initTables_();
  if (listUsers_().length) throw new Error('Admin already exists');

  const email = normalizeEmail_(payload.email);
  const displayName = String(payload.displayName || '').trim() || 'Admin';
  const password = String(payload.password || '');
  if (!email || !password) throw new Error('Email and password required');

  const user = {
    userId: Utilities.getUuid(),
    email,
    displayName,
    passwordHash: hashPassword_(password),
    role: 'admin',
    createdAt: new Date(),
    lastLoginAt: new Date(),
    isActive: true
  };
  appendRowByHeaders_(getSheet_('Users'), USER_HEADERS_, user);

  const sessionToken = createSession_(user.userId);
  return { sessionToken, user: publicUser_(user), needsSetup: false, authenticated: true };
}

function apiLogin(payload) {
  payload = payload || {};
  initTables_();
  const email = normalizeEmail_(payload.email);
  const password = String(payload.password || '');
  if (!email || !password) throw new Error('Email and password required');

  const user = getUserByEmail_(email);
  if (!user || !isTruthy_(user.isActive)) throw new Error('Invalid login');
  if (user.passwordHash !== hashPassword_(password)) throw new Error('Invalid login');

  updateUserLastLogin_(user.userId);
  const sessionToken = createSession_(user.userId);
  return {
    sessionToken,
    user: publicUser_(getUserById_(user.userId)),
    strava: getStravaConnectionStatus_(user.userId),
    authenticated: true,
    needsSetup: false
  };
}


function apiResetPassword(payload) {
  payload = payload || {};
  initTables_();
  const email = normalizeEmail_(payload.email);
  const newPassword = String(payload.newPassword || '');
  if (!email || !newPassword) throw new Error('Email and new password required');

  const sh = getSheet_('Users');
  const data = sh.getDataRange().getValues();
  const h = headerMap_(data[0]);
  for (let r = 1; r < data.length; r++) {
    if (normalizeEmail_(data[r][h.email]) !== email) continue;
    sh.getRange(r + 1, h.passwordHash + 1).setValue(hashPassword_(newPassword));
    sh.getRange(r + 1, h.lastLoginAt + 1).setValue(new Date());
    return { ok: true };
  }
  throw new Error('User not found');
}

function apiLogout(payload) {
  payload = payload || {};
  const token = String(payload.sessionToken || '');
  if (token) deleteSession_(token);
  return { ok: true };
}

function requireSessionFromArgs_(args) {
  args = args || {};
  const token = String(args.sessionToken || '');
  if (!token) throw new Error('Authentication required');
  const session = getValidSession_(token);
  if (!session) throw new Error('Session expired. Please login again.');
  const user = getUserById_(session.userId);
  if (!user || !isTruthy_(user.isActive)) throw new Error('User inactive');
  touchSession_(token);
  return { token, session, user };
}

function listUsers_() {
  const data = getSheet_('Users').getDataRange().getValues();
  if (data.length < 2) return [];
  const h = headerMap_(data[0]);
  return data.slice(1).filter(r => String(r[h.userId] || '').trim()).map(r => rowToObj_(h, r));
}

function getUserByEmail_(email) {
  email = normalizeEmail_(email);
  return listUsers_().find(u => normalizeEmail_(u.email) === email) || null;
}

function getUserById_(userId) {
  return listUsers_().find(u => String(u.userId) === String(userId)) || null;
}

function updateUserLastLogin_(userId) {
  const sh = getSheet_('Users');
  const data = sh.getDataRange().getValues();
  const h = headerMap_(data[0]);
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.userId]) === String(userId)) {
      sh.getRange(r + 1, h.lastLoginAt + 1).setValue(new Date());
      return;
    }
  }
}

function createSession_(userId) {
  const token = Utilities.getUuid().replace(/-/g, '') + Utilities.getUuid().replace(/-/g, '');
  const now = new Date();
  const expiresAt = new Date(now.getTime() + (7 * 24 * 60 * 60 * 1000));
  appendRowByHeaders_(getSheet_('Sessions'), SESSION_HEADERS_, {
    sessionToken: token,
    userId,
    expiresAt,
    createdAt: now,
    lastSeenAt: now
  });
  return token;
}

function getValidSession_(token) {
  const sh = getSheet_('Sessions');
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return null;
  const h = headerMap_(data[0]);
  const nowMs = Date.now();
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.sessionToken] || '') !== token) continue;
    const exp = new Date(data[r][h.expiresAt]).getTime();
    if (!exp || exp < nowMs) return null;
    return rowToObj_(h, data[r]);
  }
  return null;
}

function touchSession_(token) {
  const sh = getSheet_('Sessions');
  const data = sh.getDataRange().getValues();
  const h = headerMap_(data[0]);
  for (let r = 1; r < data.length; r++) {
    if (String(data[r][h.sessionToken] || '') === token) {
      sh.getRange(r + 1, h.lastSeenAt + 1).setValue(new Date());
      return;
    }
  }
}

function deleteSession_(token) {
  const sh = getSheet_('Sessions');
  const data = sh.getDataRange().getValues();
  const h = headerMap_(data[0]);
  for (let r = data.length - 1; r >= 1; r--) {
    if (String(data[r][h.sessionToken] || '') === token) sh.deleteRow(r + 1);
  }
}

function hashPassword_(plain) {
  const salt = String(PropertiesService.getScriptProperties().getProperty('AUTH_SALT') || 'change-me');
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, salt + '|' + plain);
  return bytes.map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');
}

function normalizeEmail_(s) { return String(s || '').trim().toLowerCase(); }
function publicUser_(u) { return { userId: u.userId, email: u.email, displayName: u.displayName, role: u.role }; }
function isTruthy_(v) { return String(v).toLowerCase() === 'true' || v === true || v === 1 || String(v) === '1'; }

function getSheet_(name) {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(name);
  if (!sh) throw new Error('Missing sheet: ' + name);
  return sh;
}

function rowToObj_(headers, row) {
  const o = {};
  Object.keys(headers).forEach(k => o[k] = row[headers[k]]);
  return o;
}

function appendRowByHeaders_(sh, headers, obj) {
  sh.appendRow(headers.map(h => (obj[h] == null ? '' : obj[h])));
}

function valueForLogHeader_(h, logObj) {
  const map = {
    PlanID: logObj.PlanID || '',
    Status: logObj.Status || 'DONE',
    ActualKm: logObj.ActualKm || 0,
    ActualMin: logObj.ActualMin || 0,
    CompletedAt: logObj.CompletedAt || new Date(),
    LogNotes: logObj.LogNotes || '',
    UserId: logObj.UserId || '',
    Source: logObj.Source || 'manual',
    StravaActivityId: logObj.StravaActivityId || '',
    SportType: logObj.SportType || '',
    ImportedAt: logObj.ImportedAt || '',
    PlanMatchConfidence: logObj.PlanMatchConfidence || '',
    PlanMatchReason: logObj.PlanMatchReason || ''
  };
  return map[h] == null ? '' : map[h];
}

function writeIfHeader_(sh, rowNum, headers, key, value) {
  if (headers[key] == null) return;
  sh.getRange(rowNum, headers[key] + 1).setValue(value);
}
