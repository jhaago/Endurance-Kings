/*******************************
 * Training Planner (Bound)
 * Tabs in Spreadsheet:
 *  - Plan: PlanID, Date, Slot, Sport, Title, MetricMode, PlannedKm, PlannedMin, RPE, Notes
 *  - Log:  PlanID, Status, ActualKm, ActualMin, CompletedAt, LogNotes
 *  - Settings (A:B): RollingDays, WeekStartsOn, Timezone, PartialAllowancePerWeek
 *******************************/

function doGet(e) {
  e = e || { parameter: {} };
  if (isStravaWebhookChallenge_(e)) {
    return handleStravaWebhookChallenge_(e);
  }
  if (isStravaOAuthCallback_(e)) {
    return handleStravaOAuthCallback_(e);
  }

  const tpl = HtmlService.createTemplateFromFile('index');
  tpl.appConfig = getSettings_();
  return tpl.evaluate()
    .setTitle('Training Planner')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

/** -------------------------
 * Public API (client calls)
 * ------------------------*/
function apiBootstrap(args) {
  args = args || {};
  const auth = requireSessionFromArgs_(args);
  const settings = getSettings_();
  const tz = settings.Timezone || Session.getScriptTimeZone();

  const rollingDays = Number(settings.RollingDays || 7);
  const today = isoToday_(tz);
  const dateFrom = args.dateFrom || today;
  const dateTo = args.dateTo || addDaysIso_(dateFrom, rollingDays);

  const planRows = readPlanBetween_(dateFrom, dateTo, tz);
  ensurePlanIds_(planRows);

  const planIds = planRows.map(r => r.PlanID).filter(Boolean);
const logMap = readLogsByPlanId_(planIds, auth.user.userId);

  const items = planRows.map(p => ({ ...p, Log: logMap[p.PlanID] || null }));

  return {
    settings,
    dateFrom,
    dateTo,
   items,
    user: publicUser_(auth.user),
    strava: getStravaConnectionStatus_(auth.user.userId)
  };
}

function apiSetDone(args) {
  args = args || {};
  const auth = requireSessionFromArgs_(args);
  const planId = String(args.planId || '').trim();
  if (!planId) throw new Error('planId required');
  const plan = readPlanById_(planId);
  if (!plan) throw new Error('PlanID not found: ' + planId);

  const now = new Date();
  const update = {
    PlanID: planId,
    Status: 'DONE',
    ActualKm: coerceNumber_(plan.PlannedKm),
    ActualMin: coerceNumber_(plan.PlannedMin),
    CompletedAt: now,
    LogNotes: '',
    UserId: auth.user.userId,
    Source: 'manual'
  };

  upsertLog_(update);
  return readLogsByPlanId_([planId], auth.user.userId)[planId];
}

function apiUpdateLog(payload) {
  payload = payload || {};
  const auth = requireSessionFromArgs_(payload);
  const planId = String(payload.planId || '').trim();
  if (!planId) throw new Error('planId required');

  const status = String(payload.status || 'DONE').toUpperCase();
  if (!['DONE', 'PARTIAL', 'SKIPPED', 'DELETED'].includes(status)) {
    throw new Error('Invalid status: ' + status);
  }

  const update = {
    PlanID: planId,
    Status: status,
    ActualKm: coerceNumber_(payload.actualKm),
    ActualMin: coerceNumber_(payload.actualMin),
    CompletedAt: new Date(),
    LogNotes: String(payload.notes || ''),
    UserId: auth.user.userId,
    Source: 'manual'
  };

  upsertLog_(update);
  return readLogsByPlanId_([planId], auth.user.userId)[planId];
}

function apiComputeWeek(args) {
  args = args || {};
  const settings = getSettings_();
  const tz = settings.Timezone || Session.getScriptTimeZone();
  const weekStartsOn = 'MON';

  const anchor = String(args.anchorDate || isoToday_(tz)); // any date in week
  const { weekStart, weekEnd } = weekRange_(anchor, weekStartsOn, tz);

  const planRows = readPlanBetween_(weekStart, weekEnd, tz);
  ensurePlanIds_(planRows);

  const planIds = planRows.map(r => r.PlanID).filter(Boolean);
  const auth = requireSessionFromArgs_(args);
  const logMap = readLogsByPlanId_(planIds, auth.user.userId);

  const items = planRows.map(p => ({ ...p, Log: logMap[p.PlanID] || null }));
  const totals = computeTotals_(items);

  return { weekStart, weekEnd, items, totals, settings };
}


function apiListPlan(args) {
  args = args || {};
  const auth = requireSessionFromArgs_(args);
  const settings = getSettings_();
  const tz = settings.Timezone || Session.getScriptTimeZone();

  const planRows = readPlanBetween_('2000-01-01', '2100-12-31', tz);
  ensurePlanIds_(planRows);

  const planIds = planRows.map(r => r.PlanID).filter(Boolean);
  const logMap = readLogsByPlanId_(planIds, auth.user.userId);
  const items = planRows.map(p => ({ ...p, Log: logMap[p.PlanID] || null }));

  return { items };
}


function apiListActivities(args) {
  args = args || {};
  const auth = requireSessionFromArgs_(args);
  const limit = Math.max(1, Math.min(500, Number(args.limit || 200)));
  const sh = getSheet_('Activities');
  const data = sh.getDataRange().getValues();
  if (data.length < 2) return { items: [] };
  const h = headerMap_(data[0]);

  const items = data.slice(1)
    .filter(r => String(r[h.userId] || '') === String(auth.user.userId))
    .filter(r => String(r[h.deleted] || '').toLowerCase() !== 'true')
    .map(r => rowToObj_(h, r))
    .sort((a, b) => String(b.startDate || '').localeCompare(String(a.startDate || '')))
    .slice(0, limit);

  return { items };
}

function apiComputeStats(args) {
  args = args || {};
  const settings = getSettings_();
  const tz = settings.Timezone || Session.getScriptTimeZone();
  const weekStartsOn = 'MON';
  const partialAllowance = Number(settings.PartialAllowancePerWeek || 1);

  const dateFrom = String(args.dateFrom || addDaysIso_(isoToday_(tz), -30));
  const dateTo = String(args.dateTo || isoToday_(tz));
const granularity = String(args.granularity || 'week').toLowerCase() === 'month' ? 'month' : 'week';

  const planRows = readPlanBetween_(dateFrom, dateTo, tz);
  ensurePlanIds_(planRows);
  const planIds = planRows.map(r => r.PlanID).filter(Boolean);
  const auth = requireSessionFromArgs_(args);
  const logMap = readLogsByPlanId_(planIds, auth.user.userId);

  const items = planRows.map(p => ({ ...p, Log: logMap[p.PlanID] || null }));
  const daily = groupByDate_(items);

  // Streak #2: “Did something”
  const didSomething = Object.keys(daily).sort().map(dateISO => {
    const dayItems = daily[dateISO];
    const ok = dayItems.some(it => {
      const log = it.Log;
      if (!log) return false;
      if (!['DONE', 'PARTIAL'].includes(log.Status)) return false;
      const ak = coerceNumber_(log.ActualKm);
      const am = coerceNumber_(log.ActualMin);
      return (ak > 0) || (am > 0);
    });
    return { dateISO, ok };
  });

  const streakDidSomething = computeStreakFrom_(didSomething);

  // Streak #1: “All planned completed” with 1 partial/week
  // We compute per-week partial counts, then mark days “perfect” if all items DONE/PARTIAL, none SKIPPED, and partial count within allowance.
  const byWeekKey = groupByWeek_(items, weekStartsOn, tz); // key: weekStart
  const weekPartialCounts = {};
  Object.keys(byWeekKey).forEach(weekStart => {
    const weekItems = byWeekKey[weekStart];
    const partials = weekItems.filter(it => (it.Log && it.Log.Status === 'PARTIAL')).length;
    weekPartialCounts[weekStart] = partials;
  });

  const allPlanned = Object.keys(daily).sort().map(dateISO => {
    const dayItems = daily[dateISO];
    const anyPlanned = dayItems.length > 0;
    if (!anyPlanned) return { dateISO, ok: false };

    const wk = weekRange_(dateISO, weekStartsOn, tz).weekStart;
    const partials = weekPartialCounts[wk] || 0;

    const ok = dayItems.every(it => {
      const log = it.Log;
      if (!log) return false;
      if (log.Status === 'SKIPPED') return false;
      return (log.Status === 'DONE' || log.Status === 'PARTIAL');
    }) && (partials <= partialAllowance);

    return { dateISO, ok };
  });

  const streakAllPlanned = computeStreakFrom_(allPlanned);

// Distance summaries
  const yearStart = `${dateTo.slice(0, 4)}-01-01`;
  const monthStart = `${dateTo.slice(0, 7)}-01`;
  const kmRows = readPlanBetween_(yearStart, dateTo, tz);
  ensurePlanIds_(kmRows);
  const kmPlanIds = kmRows.map(r => r.PlanID).filter(Boolean);
  const kmLogMap = readLogsByPlanId_(kmPlanIds, auth.user.userId);

  let yearDoneKm = 0;
  let monthDoneKm = 0;
  kmRows.forEach(p => {
    const log = kmLogMap[p.PlanID];
    if (!log) return;
    if (!['DONE', 'PARTIAL'].includes(log.Status)) return;
    const km = coerceNumber_(log.ActualKm);
    yearDoneKm += km;
    if (p.Date >= monthStart) monthDoneKm += km;
  });

const distanceTrend = computeDistanceTrend_(items, weekStartsOn, tz, granularity);

  return {
    settings,
    dateFrom,
    dateTo,
    granularity,
    streaks: {
      allPlannedCompleted: streakAllPlanned,
      didSomething: streakDidSomething
       },
    kmSummary: {
      monthDoneKm,
      yearDoneKm
      },
    distanceTrend
  };
}

function computeDistanceTrend_(items, weekStartsOn, tz, granularity) {
  const buckets = {};
  (items || []).forEach(it => {
    const log = it.Log;
    if (!log || !['DONE', 'PARTIAL'].includes(log.Status)) return;
    const km = coerceNumber_(log.ActualKm);
    if (!km) return;
    const key = granularity === 'month'
      ? String(it.Date || '').slice(0, 7)
      : weekRange_(it.Date, weekStartsOn, tz).weekStart;
    buckets[key] = (buckets[key] || 0) + km;
  });
  return Object.keys(buckets).sort().map(k => ({ label: k, km: buckets[k] }));
}

/** -------------------------
 * Core sheet access helpers
 * ------------------------*/
function getSettings_() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('Settings');
  const defaults = {
    RollingDays: 7,
    WeekStartsOn: 'MON',
    Timezone: Session.getScriptTimeZone(),
    PartialAllowancePerWeek: 1
  };
  if (!sh) return defaults;

  const values = sh.getDataRange().getValues();
  const out = { ...defaults };
  for (let i = 0; i < values.length; i++) {
    const k = String(values[i][0] || '').trim();
    const v = values[i][1];
    if (!k) continue;
    out[k] = (v === '' || v == null) ? out[k] : v;
  }
  // normalize
  out.RollingDays = Number(out.RollingDays || 7);
  out.PartialAllowancePerWeek = Number(out.PartialAllowancePerWeek || 1);
  out.WeekStartsOn = String(out.WeekStartsOn || 'MON').toUpperCase();
  out.Timezone = String(out.Timezone || Session.getScriptTimeZone());
  return out;
}

function readPlanBetween_(dateFromISO, dateToISO, tz) {
  const ss = SpreadsheetApp.getActive();
  const sh = ensurePlanHeaders_();

  const data = sh.getDataRange().getValues();
  if (data.length < 2) return [];

  const headers = headerMap_(data[0]);
  const out = [];

  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const dateVal = row[headers.Date];
    const dateISO = toIsoDate_(dateVal, tz);
    if (!dateISO) continue;

    if (dateISO < dateFromISO) continue;
    if (dateISO > dateToISO) continue;

  const sport = headers.Sport != null ? String(row[headers.Sport] || '').trim() : String(row[headers.SportType] || '').trim();
    const title = headers.Title != null ? String(row[headers.Title] || '').trim() : String(row[headers.WorkoutType] || '').trim();
    const metricModeRaw = headers.MetricMode != null ? String(row[headers.MetricMode] || '').trim().toUpperCase() : '';
    const plannedKm = coerceNumber_(row[headers.PlannedKm]);
    const plannedMin = coerceNumber_(row[headers.PlannedMin]);
    const metricMode = metricModeRaw || ((plannedKm && plannedMin) ? 'BOTH' : (plannedKm ? 'KM' : 'MIN'));

    out.push({
      _row: r + 1,
      PlanID: String(row[headers.PlanID] || '').trim(),
      Date: dateISO,
     Slot: (headers.Slot != null ? String(row[headers.Slot] || '').trim().toUpperCase() : 'AM') || 'AM',
      Sport: sport,
      Title: title,
      MetricMode: metricMode,
      PlannedKm: plannedKm,
      PlannedMin: plannedMin,
      RPE: headers.RPE != null ? coerceNumber_(row[headers.RPE]) : 0,
      Notes: String((headers.Notes != null ? row[headers.Notes] : '') || ''),
      PlanName: headers.PlanName != null ? String(row[headers.PlanName] || '') : '',
      WorkoutType: headers.WorkoutType != null ? String(row[headers.WorkoutType] || '') : title,
      SportType: headers.SportType != null ? String(row[headers.SportType] || '') : sport,
      DayName: headers.DayName != null ? String(row[headers.DayName] || '') : '',
      Week: headers.Week != null ? row[headers.Week] : '',
      UserId: headers.UserId != null ? String(row[headers.UserId] || '') : ''
    });
  }
  // sort by date then slot then sport
  out.sort((a, b) => (a.Date.localeCompare(b.Date) || a.Slot.localeCompare(b.Slot) || a.Sport.localeCompare(b.Sport)));
  return out;
}

function readPlanById_(planId) {
  const ss = SpreadsheetApp.getActive();
   const sh = ensurePlanHeaders_();

  const data = sh.getDataRange().getValues();
  if (data.length < 2) return null;

  const headers = headerMap_(data[0]);
  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const id = String(row[headers.PlanID] || '').trim();
    if (id === planId) {
      const settings = getSettings_();
      const tz = settings.Timezone || Session.getScriptTimeZone();
      const sport = headers.Sport != null ? String(row[headers.Sport] || '').trim() : String(row[headers.SportType] || '').trim();
      const title = headers.Title != null ? String(row[headers.Title] || '').trim() : String(row[headers.WorkoutType] || '').trim();
      const plannedKm = coerceNumber_(row[headers.PlannedKm]);
      const plannedMin = coerceNumber_(row[headers.PlannedMin]);
      const metricModeRaw = headers.MetricMode != null ? String(row[headers.MetricMode] || 'BOTH').trim().toUpperCase() : '';
      const metricMode = metricModeRaw || ((plannedKm && plannedMin) ? 'BOTH' : (plannedKm ? 'KM' : 'MIN'));
      return {
        _row: r + 1,
        PlanID: id,
        Date: toIsoDate_(row[headers.Date], tz),
        Slot: (headers.Slot != null ? String(row[headers.Slot] || '').trim().toUpperCase() : 'AM') || 'AM',
        Sport: sport,
        Title: title,
        MetricMode: metricMode,
        PlannedKm: plannedKm,
        PlannedMin: plannedMin,
        RPE: headers.RPE != null ? coerceNumber_(row[headers.RPE]) : 0,
        Notes: String((headers.Notes != null ? row[headers.Notes] : '') || ''),
        PlanName: headers.PlanName != null ? String(row[headers.PlanName] || '') : '',
        WorkoutType: headers.WorkoutType != null ? String(row[headers.WorkoutType] || '') : title,
        SportType: headers.SportType != null ? String(row[headers.SportType] || '') : sport
      };
    }
  }
  return null;
}

function ensurePlanIds_(planRows) {
  // Assign UUIDs to any blank PlanID and write back to sheet.
  const missing = planRows.filter(r => !r.PlanID);
  if (!missing.length) return;

  const ss = SpreadsheetApp.getActive();
   const sh = ensurePlanHeaders_();
  const header = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const headers = headerMap_(header);

  const updates = [];
  missing.forEach(r => {
    const id = Utilities.getUuid();
    r.PlanID = id;
    updates.push({ row: r._row, col: headers.PlanID + 1, value: id });
  });

  // batch update
  updates.forEach(u => sh.getRange(u.row, u.col).setValue(u.value));
}

function readLogsByPlanId_(planIds, userId) {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('Log');
  if (!sh) return {};

  const data = sh.getDataRange().getValues();
  if (data.length < 2) return {};

  const headers = headerMap_(data[0]);
  const idSet = {};
  planIds.forEach(id => { idSet[id] = true; });

  const out = {};
  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const id = String(row[headers.PlanID] || '').trim();
    if (!id || !idSet[id]) continue;
    if (userId && headers.UserId != null) {
      const rowUser = String(row[headers.UserId] || '').trim();
      if (rowUser && rowUser !== userId) continue;
    }

    out[id] = {
      _row: r + 1,
      PlanID: id,
      Status: String(row[headers.Status] || 'PLANNED').toUpperCase(),
      ActualKm: coerceNumber_(row[headers.ActualKm]),
      ActualMin: coerceNumber_(row[headers.ActualMin]),
      CompletedAt: row[headers.CompletedAt] ? new Date(row[headers.CompletedAt]) : null,
      LogNotes: String(row[headers.LogNotes] || ''),
      UserId: headers.UserId != null ? String(row[headers.UserId] || '') : '',
      Source: headers.Source != null ? String(row[headers.Source] || '') : ''
    };
  }
  return out;
}

function upsertLog_(logObj) {
  initTables_();
  const lock = LockService.getDocumentLock();
  lock.waitLock(5000);
  try {
    const ss = SpreadsheetApp.getActive();
    let sh = ss.getSheetByName('Log');
    if (!sh) {
      sh = ss.insertSheet('Log');
      sh.appendRow(LOG_HEADERS_);
    }

    const data = sh.getDataRange().getValues();
    const headers = headerMap_(data[0]);

    const stravaId = String(logObj.StravaActivityId || '').trim();
    // Find existing row by StravaActivityId (for imports) or PlanID+UserId
    let foundRow = null;
    for (let r = 1; r < data.length; r++) {
      const row = data[r];
      const rowStravaId = headers.StravaActivityId != null ? String(row[headers.StravaActivityId] || '').trim() : '';
      const id = String(row[headers.PlanID] || '').trim();
      const rowUser = headers.UserId != null ? String(row[headers.UserId] || '').trim() : '';
      if (stravaId && rowStravaId && rowStravaId === stravaId) { foundRow = r + 1; break; }
      if (id === String(logObj.PlanID || '').trim() && (!logObj.UserId || !rowUser || rowUser === logObj.UserId)) { foundRow = r + 1; break; }
    }

    if (!foundRow) {
       const rowValues = LOG_HEADERS_.map(h => valueForLogHeader_(h, logObj));
      sh.appendRow(rowValues);
      return;
    }

    // Update in place
    sh.getRange(foundRow, headers.Status + 1).setValue(logObj.Status);
    sh.getRange(foundRow, headers.ActualKm + 1).setValue(logObj.ActualKm);
    sh.getRange(foundRow, headers.ActualMin + 1).setValue(logObj.ActualMin);
    sh.getRange(foundRow, headers.CompletedAt + 1).setValue(logObj.CompletedAt);
    sh.getRange(foundRow, headers.LogNotes + 1).setValue(logObj.LogNotes);
writeIfHeader_(sh, foundRow, headers, 'UserId', logObj.UserId || '');
    writeIfHeader_(sh, foundRow, headers, 'Source', logObj.Source || 'manual');
    writeIfHeader_(sh, foundRow, headers, 'StravaActivityId', logObj.StravaActivityId || '');
    writeIfHeader_(sh, foundRow, headers, 'SportType', logObj.SportType || '');
    writeIfHeader_(sh, foundRow, headers, 'ImportedAt', logObj.ImportedAt || '');
    writeIfHeader_(sh, foundRow, headers, 'PlanMatchConfidence', logObj.PlanMatchConfidence || '');
    writeIfHeader_(sh, foundRow, headers, 'PlanMatchReason', logObj.PlanMatchReason || '');

  } finally {
    lock.releaseLock();
  }
}

/** -------------------------
 * Computation helpers
 * ------------------------*/
function computeTotals_(items) {
  const totals = {
    plannedKm: 0, plannedMin: 0,
    doneKm: 0, doneMin: 0,
    bySport: {} // sport -> { plannedKm, plannedMin, doneKm, doneMin }
  };

  items.forEach(it => {
    const sport = it.Sport || 'Other';
    if (!totals.bySport[sport]) totals.bySport[sport] = { plannedKm: 0, plannedMin: 0, doneKm: 0, doneMin: 0 };

    totals.plannedKm += coerceNumber_(it.PlannedKm);
    totals.plannedMin += coerceNumber_(it.PlannedMin);
    totals.bySport[sport].plannedKm += coerceNumber_(it.PlannedKm);
    totals.bySport[sport].plannedMin += coerceNumber_(it.PlannedMin);

    const log = it.Log;
    if (log && (log.Status === 'DONE' || log.Status === 'PARTIAL')) {
      totals.doneKm += coerceNumber_(log.ActualKm);
      totals.doneMin += coerceNumber_(log.ActualMin);
      totals.bySport[sport].doneKm += coerceNumber_(log.ActualKm);
      totals.bySport[sport].doneMin += coerceNumber_(log.ActualMin);
    }
  });

  return totals;
}

function groupByDate_(items) {
  const out = {};
  items.forEach(it => {
    const d = it.Date;
    if (!out[d]) out[d] = [];
    out[d].push(it);
  });
  return out;
}

function groupByWeek_(items, weekStartsOn, tz) {
  const out = {};
  items.forEach(it => {
    const wk = weekRange_(it.Date, weekStartsOn, tz).weekStart;
    if (!out[wk]) out[wk] = [];
    out[wk].push(it);
  });
  return out;
}

function computeStreakFrom_(arr) {
  // arr = [{dateISO, ok}] sorted ascending. return current streak ending at latest date in range (or ending at today if included)
  let streak = 0;
  for (let i = arr.length - 1; i >= 0; i--) {
    if (arr[i].ok) streak++;
    else break;
  }
  return streak;
}

/** -------------------------
 * Date + parsing helpers
 * ------------------------*/
function isoToday_(tz) {
  return Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');
}

function addDaysIso_(dateISO, days) {
  const d = new Date(dateISO + 'T00:00:00');
  d.setDate(d.getDate() + Number(days));
  return Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd');
}

function toIsoDate_(value, tz) {
  if (!value) return '';
  if (Object.prototype.toString.call(value) === '[object Date]') {
    return Utilities.formatDate(value, tz, 'yyyy-MM-dd');
  }
  const s = String(value).trim();
  // Allow already-ISO
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  // Last-resort parse
  const d = new Date(s);
  if (isNaN(d.getTime())) return '';
  return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
}

function weekRange_(anchorISO, weekStartsOn, tz) {
  const d = new Date(anchorISO + 'T00:00:00');
  // JS: 0=Sun..6=Sat. We want Monday=1 default.
  const startDow = (weekStartsOn === 'SUN') ? 0 : 1;
  const dow = d.getDay();
  const diff = (dow - startDow + 7) % 7;
  const start = new Date(d);
  start.setDate(d.getDate() - diff);
  const end = new Date(start);
  end.setDate(start.getDate() + 6);
  const weekStart = Utilities.formatDate(start, tz, 'yyyy-MM-dd');
  const weekEnd = Utilities.formatDate(end, tz, 'yyyy-MM-dd');
  return { weekStart, weekEnd };
}

function headerMap_(headerRow) {
  const map = {};
  headerRow.forEach((h, i) => {
    const key = String(h || '').trim();
    if (key) map[key] = i;
  });
  return map;
}

function coerceNumber_(v) {
  if (v === '' || v == null) return 0;
  const n = Number(v);
  return isNaN(n) ? 0 : n;
}
