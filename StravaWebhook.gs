/********************************
 * Strava webhook endpoint + queue processor
 ********************************/

function doPost(e) {
  return handleStravaWebhookPost_(e);
}

function isStravaWebhookChallenge_(e) {
  const p = (e && e.parameter) || {};
  return String(p['hub.mode'] || '') === 'subscribe' || !!p['hub.challenge'];
}

function handleStravaWebhookChallenge_(e) {
  const p = (e && e.parameter) || {};
  const verifyToken = String(PropertiesService.getScriptProperties().getProperty('STRAVA_VERIFY_TOKEN') || '');
  if (String(p['hub.verify_token'] || '') !== verifyToken) {
    return ContentService.createTextOutput(JSON.stringify({ error: 'invalid verify token' }))
      .setMimeType(ContentService.MimeType.JSON);
  }
  return ContentService.createTextOutput(JSON.stringify({ 'hub.challenge': p['hub.challenge'] || '' }))
    .setMimeType(ContentService.MimeType.JSON);
}

function handleStravaWebhookPost_(e) {
  initTables_();
  const start = Date.now();
  try {
    const payload = JSON.parse((e && e.postData && e.postData.contents) || '{}');
    enqueueWebhookEvent_(payload);
  } catch (err) {
    enqueueWebhookEvent_({ error: String(err) });
  }
  const elapsedMs = Date.now() - start;
  return ContentService.createTextOutput(JSON.stringify({ ok: true, elapsedMs }))
    .setMimeType(ContentService.MimeType.JSON);
}

function enqueueWebhookEvent_(evt) {
  const sh = getSheet_('WebhookQueue');
  appendRowByHeaders_(sh, WEBHOOK_QUEUE_HEADERS_, {
    queuedAt: new Date(),
    subscriptionId: evt.subscription_id || '',
    objectType: evt.object_type || '',
    aspectType: evt.aspect_type || '',
    objectId: evt.object_id || '',
    ownerId: evt.owner_id || '',
    eventTime: evt.event_time || '',
    updatesJson: JSON.stringify(evt.updates || {}),
    processed: false,
    processedAt: '',
    error: ''
  });
}

function processWebhookQueue_() {
  initTables_();
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return;
  try {
    const sh = getSheet_('WebhookQueue');
    const data = sh.getDataRange().getValues();
    if (data.length < 2) return;
    const h = headerMap_(data[0]);

    let handled = 0;
    for (let r = 1; r < data.length && handled < 20; r++) {
      const row = data[r];
      if (isTruthy_(row[h.processed])) continue;

      try {
        processWebhookEventRow_(row, h);
        sh.getRange(r + 1, h.processed + 1).setValue(true);
        sh.getRange(r + 1, h.processedAt + 1).setValue(new Date());
        sh.getRange(r + 1, h.error + 1).setValue('');
      } catch (err) {
        sh.getRange(r + 1, h.error + 1).setValue(String(err));
        sh.getRange(r + 1, h.processed + 1).setValue(true);
        sh.getRange(r + 1, h.processedAt + 1).setValue(new Date());
      }
      handled++;
    }
  } finally {
    lock.releaseLock();
  }
}

function processWebhookEventRow_(row, h) {
  const objectType = String(row[h.objectType] || '');
  const aspectType = String(row[h.aspectType] || '');
  const objectId = String(row[h.objectId] || '');
  const ownerId = String(row[h.ownerId] || '');
  const updates = JSON.parse(String(row[h.updatesJson] || '{}'));

  const acct = getStravaAccountByAthleteId_(ownerId);
  if (!acct) throw new Error('No Strava account for ownerId=' + ownerId);
  const userId = acct.userId;

  if (objectType === 'activity' && (aspectType === 'create' || aspectType === 'update')) {
    const activity = stravaGetActivity_(userId, objectId);
    upsertActivityFromStrava_(userId, Number(ownerId || 0), activity, false);
    upsertLogFromStravaActivity_(userId, activity);
    return;
  }

  if (objectType === 'activity' && aspectType === 'delete') {
    upsertActivityFromStrava_(userId, Number(ownerId || 0), { id: objectId }, true);
    upsertLogForDeletedStravaActivity_(userId, objectId);
    return;
  }

  if (objectType === 'athlete' && aspectType === 'update' && String(updates.authorized || '') === 'false') {
    setStravaConnected_(userId, false);
  }
}

function upsertLogForDeletedStravaActivity_(userId, stravaActivityId) {
  upsertLog_({
    PlanID: 'UNPLANNED',
    Status: 'DELETED',
    ActualKm: 0,
    ActualMin: 0,
    CompletedAt: new Date(),
    LogNotes: 'Strava activity deleted',
    UserId: userId,
    Source: 'strava',
    StravaActivityId: String(stravaActivityId),
    SportType: '',
    ImportedAt: new Date(),
    PlanMatchConfidence: 0,
    PlanMatchReason: 'deleted_by_webhook'
  });
}
