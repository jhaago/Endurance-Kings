/********************************
 * Plan import + generator
 ********************************/

const PLAN_ALLOWED_SPORTS_ = ['RUN', 'CYCLE', 'BIKE', 'SWIM', 'GYM', 'WALK', 'HIKE', 'ROW', 'WORKOUT', 'OTHER'];
const PLAN_CANON_HEADERS_ = ['PlanID', 'PlanName', 'Date', 'SportType', 'WorkoutType', 'PlannedKm', 'PlannedMin', 'Notes', 'Week', 'DayName', 'RPE', 'UserId', 'ExternalRowId'];
const PLAN_LEGACY_HEADERS_ = ['Slot', 'Sport', 'Title', 'MetricMode'];

function ensurePlanHeaders_() {
  const ss = SpreadsheetApp.getActive();
  let sh = ss.getSheetByName('Plan');
  const allHeaders = ['PlanID', ...PLAN_CANON_HEADERS_.filter(h => h !== 'PlanID'), ...PLAN_LEGACY_HEADERS_];
  if (!sh) {
    sh = ss.insertSheet('Plan');
    sh.getRange(1, 1, 1, allHeaders.length).setValues([allHeaders]);
    return sh;
  }
  if (sh.getLastRow() < 1) {
    sh.getRange(1, 1, 1, allHeaders.length).setValues([allHeaders]);
    return sh;
  }
  const row = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(v => String(v || '').trim());
  allHeaders.forEach(h => {
    if (!row.includes(h)) {
      sh.getRange(1, sh.getLastColumn() + 1).setValue(h);
      row.push(h);
    }
  });
  return sh;
}

function planImportPreview(payload) {
  payload = payload || {};
  const auth = requireSessionFromArgs_(payload);
  ensurePlanHeaders_();

  const headers = (payload.headers || []).map(h => String(h || '').trim());
  const rows = payload.rows || [];
  const mapping = inferPlanHeaderMap_(headers);

  const normalizedRows = [];
  const issues = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i] || [];
    const rowObj = normalizeImportedPlanRow_(mapping, headers, r, i + 2, auth.user.userId);
    normalizedRows.push(rowObj.row);
    issues.push({ rowNumber: i + 2, warnings: rowObj.warnings, errors: rowObj.errors });
  }

  return {
    mappedHeaders: mapping,
    normalizedRows,
    issues,
    sample: normalizedRows.slice(0, 20),
    criticalErrorCount: issues.reduce((a, it) => a + it.errors.length, 0)
  };
}

function planImportCommit(payload) {
  payload = payload || {};
  const auth = requireSessionFromArgs_(payload);
  const preview = planImportPreview(payload);
  if (preview.criticalErrorCount > 0) {
    throw new Error('Import blocked: fix critical row errors first.');
  }

  const sh = ensurePlanHeaders_();
  const lock = LockService.getDocumentLock();
  lock.waitLock(10000);
  try {
    const data = sh.getDataRange().getValues();
    const headers = headerMap_(data[0]);
    const existing = indexExistingPlanRows_(data, headers);

    let inserted = 0;
    let updated = 0;

    preview.normalizedRows.forEach(row => {
      const key = planRowKey_(row);
      const existingRow = findExistingPlanRow_(existing, row, key);
      const rowValues = buildPlanSheetRow_(headers, row);
      if (existingRow) {
        sh.getRange(existingRow, 1, 1, data[0].length).setValues([rowValues]);
        updated++;
      } else {
        sh.appendRow(rowValues);
        inserted++;
      }
    });

    return { inserted, updated, total: preview.normalizedRows.length, issues: preview.issues.slice(0, 40) };
  } finally {
    lock.releaseLock();
  }
}

function planGenerateCommit(params) {
  params = params || {};
  const auth = requireSessionFromArgs_(params);
  const settings = getSettings_();
  const tz = settings.Timezone || Session.getScriptTimeZone();
  const planName = String(params.planName || 'Generated Plan').trim();
  const targetDistanceKm = Number(params.targetDistanceKm || 10);
  const planLengthWeeks = Math.max(4, Number(params.planLengthWeeks || 12));
  const trainingDays = Math.min(7, Math.max(2, Number(params.trainingDaysPerWeek || 4)));
  const longRunDay = String(params.longRunDay || 'SUN').toUpperCase();
  const includeIntervals = params.includeIntervals == null ? trainingDays >= 4 : !!params.includeIntervals;
  const includeTempo = params.includeTempo == null ? trainingDays >= 3 : !!params.includeTempo;
  const startDateISO = String(params.startDate || nextWeekdayIso_(1)); // Monday default

  const planId = 'PLAN-' + Utilities.formatDate(new Date(), 'UTC', 'yyyyMMddHHmmss');
  const start = new Date(startDateISO + 'T00:00:00');
  const dayNames = ['SUN','MON','TUE','WED','THU','FRI','SAT'];

  const taperWeeks = targetDistanceKm >= 21 ? 2 : 1;
  const baseWeeklyKm = Math.max(12, targetDistanceKm * 1.2);
  let currentWeeklyKm = baseWeeklyKm;

  const rows = [];
  for (let w = 1; w <= planLengthWeeks; w++) {
    const isDeload = (w % 4 === 0) && (w !== planLengthWeeks);
    const isTaper = w > (planLengthWeeks - taperWeeks);

    if (w === 1) currentWeeklyKm = baseWeeklyKm;
    else if (isDeload) currentWeeklyKm = Math.max(10, currentWeeklyKm * 0.75);
    else if (isTaper) currentWeeklyKm = currentWeeklyKm * 0.8;
    else currentWeeklyKm = currentWeeklyKm * 1.07;

    const longRunKm = Math.min(targetDistanceKm * 0.85, Math.max(targetDistanceKm * 0.35, currentWeeklyKm * 0.35));

    const weekDates = [];
    for (let d = 0; d < 7; d++) {
      const dt = new Date(start);
      dt.setDate(start.getDate() + ((w - 1) * 7) + d);
      weekDates.push(dt);
    }

    const longIdx = dayNames.indexOf(longRunDay);
    const trainingIdxs = pickTrainingDayIndexes_(trainingDays, longIdx);

    let qualityAdded = 0;
    trainingIdxs.forEach((idx, pos) => {
      const dt = weekDates[idx];
      const dayName = dayNames[dt.getDay()];
      let workoutType = 'Easy';
      let km = currentWeeklyKm / trainingDays;
      let notes = '';

      if (idx === longIdx) {
        workoutType = 'Long Run';
        km = longRunKm;
        notes = 'Steady effort, conversational pace.';
      } else if (trainingDays <= 3) {
        if (includeTempo && qualityAdded < 1 && pos === 0) {
          workoutType = 'Tempo';
          km = currentWeeklyKm * 0.28;
          qualityAdded++;
          notes = 'Controlled threshold effort.';
        }
      } else {
        if (includeIntervals && qualityAdded < 1 && pos === 1) {
          workoutType = 'Intervals';
          km = currentWeeklyKm * 0.22;
          qualityAdded++;
          notes = 'Quality reps; full warmup/cooldown.';
        } else if (includeTempo && qualityAdded < 2 && pos === 0) {
          workoutType = 'Tempo';
          km = currentWeeklyKm * 0.24;
          qualityAdded++;
          notes = 'Sustained moderate-hard effort.';
        }
      }

      if (trainingDays >= 6 && workoutType === 'Easy') {
        notes = 'Recovery-focused easy run.';
      }

      rows.push(enrichPlanRow_({
        PlanID: planId,
        PlanName: planName,
        Date: Utilities.formatDate(dt, tz, 'yyyy-MM-dd'),
        DayName: dayName,
        SportType: 'Run',
        WorkoutType: workoutType,
        PlannedKm: Math.round(Math.max(2, km) * 10) / 10,
        PlannedMin: '',
        Notes: notes,
        Week: w,
        RPE: workoutType === 'Long Run' ? 4 : (workoutType === 'Easy' ? 3 : 6),
        UserId: auth.user.userId,
        ExternalRowId: planId + '-' + w + '-' + dayName
      }));
    });
  }

  const sh = ensurePlanHeaders_();
  const lock = LockService.getDocumentLock();
  lock.waitLock(10000);
  try {
    const data = sh.getDataRange().getValues();
    const headers = headerMap_(data[0]);
    const existing = indexExistingPlanRows_(data, headers);

    let inserted = 0;
    let updated = 0;
    rows.forEach(row => {
      const key = planRowKey_(row);
      const existingRow = findExistingPlanRow_(existing, row, key);
      const rowValues = buildPlanSheetRow_(headers, row);
      if (existingRow) {
        sh.getRange(existingRow, 1, 1, data[0].length).setValues([rowValues]);
        updated++;
      } else {
        sh.appendRow(rowValues);
        inserted++;
      }
    });
    return { planId, inserted, updated, total: rows.length };
  } finally {
    lock.releaseLock();
  }
}

function inferPlanHeaderMap_(headers) {
  const synonymMap = {
    PlanID: ['planid', 'plan_id'],
    PlanName: ['planname', 'plan_name', 'plan'],
    Date: ['date', 'day', 'session_date'],
    SportType: ['sport', 'type', 'discipline'],
    WorkoutType: ['workout', 'session', 'category', 'intensity'],
    PlannedKm: ['km', 'distance', 'distance_km', 'planned_km'],
    PlannedMin: ['min', 'mins', 'minutes', 'duration', 'duration_min'],
    Notes: ['notes', 'description', 'details'],
    Week: ['week'],
    DayName: ['dayname', 'day_name'],
    RPE: ['rpe'],
    UserId: ['userid', 'user_id', 'user'],
    ExternalRowId: ['externalrowid', 'external_row_id', 'externalid', 'source_row_id']
  };

  const normalizedHeaders = headers.map(h => String(h || '').trim().toLowerCase());
  const out = {};
  Object.keys(synonymMap).forEach(canon => {
    const candidates = [canon.toLowerCase()].concat(synonymMap[canon]);
    const idx = normalizedHeaders.findIndex(h => candidates.includes(h));
    if (idx >= 0) out[canon] = headers[idx];
  });
  return out;
}

function normalizeImportedPlanRow_(mapping, headers, rowArr, rowNumber, defaultUserId) {
  const src = {};
  headers.forEach((h, i) => src[h] = rowArr[i]);
  const warnings = [];
  const errors = [];

  const rawDate = getMappedValue_(src, mapping, 'Date');
  const dateIso = toIsoDate_(rawDate, Session.getScriptTimeZone());
  if (!dateIso) errors.push('Invalid date');

  const sport = String(getMappedValue_(src, mapping, 'SportType') || 'Run').trim();
  if (!PLAN_ALLOWED_SPORTS_.includes(sport.toUpperCase())) warnings.push('Unknown sport type: ' + sport);

  const plannedKm = numOrBlank_(getMappedValue_(src, mapping, 'PlannedKm'));
  const plannedMin = numOrBlank_(getMappedValue_(src, mapping, 'PlannedMin'));
  const notes = String(getMappedValue_(src, mapping, 'Notes') || '').trim();
  if (plannedKm === '' && plannedMin === '' && notes.length < 5) warnings.push('Neither PlannedKm nor PlannedMin present');

  let planId = String(getMappedValue_(src, mapping, 'PlanID') || '').trim();
  if (!planId) {
    planId = 'PLAN-' + Utilities.formatDate(new Date(), 'UTC', 'yyyyMMddHHmmss') + '-' + rowNumber;
    warnings.push('Auto-generated PlanID');
  }

  const row = enrichPlanRow_({
    PlanID: planId,
    PlanName: String(getMappedValue_(src, mapping, 'PlanName') || 'Imported Plan').trim(),
    Date: dateIso,
    SportType: sport || 'Run',
    WorkoutType: String(getMappedValue_(src, mapping, 'WorkoutType') || 'Easy').trim(),
    PlannedKm: plannedKm,
    PlannedMin: plannedMin,
    Notes: notes,
    Week: getMappedValue_(src, mapping, 'Week') || '',
    DayName: String(getMappedValue_(src, mapping, 'DayName') || '').trim(),
    RPE: numOrBlank_(getMappedValue_(src, mapping, 'RPE')),
    UserId: String(getMappedValue_(src, mapping, 'UserId') || defaultUserId || '').trim(),
    ExternalRowId: String(getMappedValue_(src, mapping, 'ExternalRowId') || '').trim()
  });

  return { row, warnings, errors };
}

function enrichPlanRow_(row) {
  const metricMode = row.PlannedKm !== '' && row.PlannedMin !== '' ? 'BOTH' : (row.PlannedKm !== '' ? 'KM' : 'MIN');
  const dayName = row.DayName || dayNameFromIso_(row.Date);
  return {
    PlanID: row.PlanID || '',
    PlanName: row.PlanName || '',
    Date: row.Date || '',
    SportType: row.SportType || 'Run',
    WorkoutType: row.WorkoutType || 'Easy',
    PlannedKm: row.PlannedKm === '' ? '' : Number(row.PlannedKm),
    PlannedMin: row.PlannedMin === '' ? '' : Number(row.PlannedMin),
    Notes: row.Notes || '',
    Week: row.Week || '',
    DayName: dayName,
    RPE: row.RPE === '' ? '' : Number(row.RPE),
    UserId: row.UserId || '',
    ExternalRowId: row.ExternalRowId || '',
    Slot: 'AM',
    Sport: row.SportType || 'Run',
    Title: row.WorkoutType || 'Session',
    MetricMode: metricMode
  };
}

function indexExistingPlanRows_(data, headers) {
  const byExternalRowId = {};
  const byComposite = {};
  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const ext = headers.ExternalRowId != null ? String(row[headers.ExternalRowId] || '').trim() : '';
    const planId = String(row[headers.PlanID] || '').trim();
    const dateIso = toIsoDate_(row[headers.Date], Session.getScriptTimeZone());
    const wt = headers.WorkoutType != null
      ? String(row[headers.WorkoutType] || '').trim()
      : String(row[headers.Title] || '').trim();

    if (ext) byExternalRowId[ext] = r + 1;
    if (planId && dateIso && wt) byComposite[(planId + '|' + dateIso + '|' + wt).toLowerCase()] = r + 1;
  }
  return { byExternalRowId, byComposite };
}

function findExistingPlanRow_(existing, row, compositeKey) {
  if (row.ExternalRowId && existing.byExternalRowId[row.ExternalRowId]) return existing.byExternalRowId[row.ExternalRowId];
  if (compositeKey && existing.byComposite[compositeKey]) return existing.byComposite[compositeKey];
  return null;
}

function planRowKey_(row) {
  return (String(row.PlanID || '') + '|' + String(row.Date || '') + '|' + String(row.WorkoutType || '')).toLowerCase();
}

function buildPlanSheetRow_(headers, rowObj) {
  const out = new Array(Object.keys(headers).length).fill('');
  Object.keys(headers).forEach(k => {
    out[headers[k]] = rowObj[k] == null ? '' : rowObj[k];
  });
  return out;
}

function getMappedValue_(src, mapping, canonical) {
  const mappedHeader = mapping[canonical];
  if (!mappedHeader) return '';
  return src[mappedHeader];
}

function numOrBlank_(v) {
  if (v === '' || v == null) return '';
  const n = Number(v);
  return isNaN(n) ? '' : n;
}

function dayNameFromIso_(iso) {
  if (!iso) return '';
  const d = new Date(iso + 'T00:00:00');
  return ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][d.getDay()];
}

function nextWeekdayIso_(targetDow) {
  const now = new Date();
  const d = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  let diff = (targetDow - d.getDay() + 7) % 7;
  if (diff === 0) diff = 7;
  d.setDate(d.getDate() + diff);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

function pickTrainingDayIndexes_(daysPerWeek, longDayIdx) {
  const preferred = [1, 2, 3, 4, 5, 6, 0]; // Mon..Sun
  const out = [longDayIdx];
  for (let i = 0; i < preferred.length && out.length < daysPerWeek; i++) {
    const idx = preferred[i];
    if (out.includes(idx)) continue;
    out.push(idx);
  }
  return out.sort((a, b) => a - b);
}
