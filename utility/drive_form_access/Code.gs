/** SpotLake: verified Form email -> expiring Drive reader permission.
 * School-owned standalone Apps Script; enable the advanced Drive v3 service.
 * No web app, email delivery, AWS token, or public endpoint is required.
 */
const CONFIG = Object.freeze({
  formId: '1RXKSggYFJrODMqGolwecbqCNX4a8iwBafQ7nfHM_WeI',
  folderId: '1u3MU7-IR34-aNonCbJ3L1xLoU0e2ZQWW',
  owner: 'spotlake@hanyang.ac.kr',
  administrators: ['spotlake@hanyang.ac.kr', 'ddpslab@hanyang.ac.kr'],
  consentTitle: 'SpotLake Dataset — Terms of Use',
  consentAnswer: 'Agree',
});

function nextMonthUtc_(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1));
}
function monthStartUtc_(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}
function properties_() { return PropertiesService.getScriptProperties(); }
function withLock_(fn) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(10000)) throw new Error('Access worker busy; reconciliation will retry.');
  try { return fn(); } finally { lock.releaseLock(); }
}
function form_() {
  if (Session.getEffectiveUser().getEmail().toLowerCase() !== CONFIG.owner) {
    throw new Error('Run and install triggers as the school owner.');
  }
  const form = FormApp.openById(CONFIG.formId);
  if (!form.collectsEmail() || form.hasLimitOneResponsePerUser()) {
    throw new Error('Enable verified email collection and disable the one-response limit.');
  }
  // FormApp cannot distinguish Verified from Responder input. Keep the setting
  // verified in the Form UI; never use the separate free-text email question.
  const consent = form.getItems(FormApp.ItemType.CHECKBOX)
    .filter(item => item.getTitle() === CONFIG.consentTitle);
  if (consent.length !== 1 || !consent[0].asCheckboxItem().isRequired()) {
    throw new Error('Expected exactly one required Terms of Use checkbox.');
  }
  return form;
}
function permissions_(id) {
  let result = [], pageToken;
  do {
    const page = Drive.Permissions.list(id, {
      fields: 'nextPageToken,permissions(id,type,role,emailAddress,expirationTime,permissionDetails)',
      pageSize: 100, pageToken,
    });
    result = result.concat(page.permissions || []);
    pageToken = page.nextPageToken;
  } while (pageToken);
  return result;
}
function children_(id) {
  let result = [], pageToken;
  do {
    const page = Drive.Files.list({
      q: "'" + id + "' in parents and trashed = false",
      fields: 'nextPageToken,files(id,name,mimeType,owners(emailAddress))',
      pageSize: 1000, pageToken,
    });
    result = result.concat(page.files || []);
    pageToken = page.nextPageToken;
  } while (pageToken);
  return result;
}
function tree_() {
  const root = Drive.Files.get(CONFIG.folderId, {
    fields: 'id,name,mimeType,owners(emailAddress),trashed',
  });
  if (root.trashed || root.mimeType !== 'application/vnd.google-apps.folder') {
    throw new Error('Dataset root is not an active folder.');
  }
  const nodes = [root];
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    if (!(node.owners || []).some(o => o.emailAddress.toLowerCase() === CONFIG.owner)) {
      throw new Error('Dataset contains an item not owned by the school account.');
    }
    if (node.mimeType === 'application/vnd.google-apps.shortcut') {
      throw new Error('Audit shortcut targets separately before activation.');
    }
    if (node.mimeType === 'application/vnd.google-apps.folder') nodes.push(...children_(node.id));
    if (nodes.length > 250) throw new Error('Dataset audit exceeds the configured safety bound.');
  }
  return nodes;
}
function administrator_(p) {
  return p.type === 'user' && CONFIG.administrators.includes((p.emailAddress || '').toLowerCase());
}
function assertRestricted_() {
  if (permissions_(CONFIG.folderId).some(p => p.type === 'anyone' || p.type === 'domain')) {
    throw new Error('Dataset root still permits general access.');
  }
}

/** Read-only check; does not read respondent answers, grant, or revoke access. */
function preflight() {
  form_();
  const nodes = tree_();
  let general = 0;
  nodes.forEach(node => permissions_(node.id).forEach(p => {
    if (p.type === 'anyone' || p.type === 'domain') { general++; return; }
    if (!administrator_(p)) throw new Error('Unexpected existing collaborator; review before activation.');
  }));
  const result = {items: nodes.length, generalAccessEntries: general, ready: true};
  console.log(JSON.stringify(result));
  return result;
}

/** Validate this script's Drive API authorization using an empty temporary folder. */
function validateTemporaryExpiry() {
  form_();
  let folder;
  try {
    folder = Drive.Files.create({name: 'SpotLake temporary access validation',
      mimeType: 'application/vnd.google-apps.folder'}, null,
      {fields: 'id', ignoreDefaultVisibility: true});
    const expiration = nextMonthUtc_(new Date()).toISOString();
    const permission = Drive.Permissions.create({type: 'user', role: 'reader',
      emailAddress: 'ddpslab@hanyang.ac.kr', expirationTime: expiration}, folder.id,
      {sendNotificationEmail: false, fields: 'id'});
    const actual = Drive.Permissions.get(folder.id, permission.id,
      {fields: 'role,expirationTime'});
    if (actual.role !== 'reader' || new Date(actual.expirationTime).toISOString() !== expiration) {
      throw new Error('Native permission expiration validation failed.');
    }
    console.log('Temporary reader permission and native expiration verified.');
  } finally {
    if (folder) Drive.Files.remove(folder.id);
  }
}

/** One-time cutover. Only run after reviewing preflight and Verified in Forms UI. */
function activate() {
  return withLock_(() => {
    const props = properties_();
    if (props.getProperty('activatedAt')) throw new Error('Already activated; use reconcileAccess.');
    preflight(); // Complete audit before changing any permission.
    const nodes = tree_();
    const started = props.getProperty('cutoverStartedAt') || new Date().toISOString();
    props.setProperty('cutoverStartedAt', started);
    // Install while disabled so an installation failure cannot grant early access.
    installTriggers_();
    // Parent first: inherited public access disappears before child checks.
    nodes.forEach(node => permissions_(node.id).forEach(p => {
      if (p.type === 'anyone' || p.type === 'domain') Drive.Permissions.remove(node.id, p.id);
    }));
    nodes.forEach(node => {
      if (permissions_(node.id).some(p => p.type === 'anyone' || p.type === 'domain')) {
        throw new Error('General access remained after cutover; activation stopped.');
      }
    });
    const form = form_();
    if (props.getProperty('previousConfirmationMessage') === null) {
      props.setProperty('previousConfirmationMessage', form.getConfirmationMessage());
    }
    form.setConfirmationMessage(
      'Thank you for submitting your request. Access is granted to the Google account ' +
      'whose email was collected by this form. Please allow up to 5 minutes, then open:\n' +
      'https://drive.google.com/drive/folders/' + CONFIG.folderId + '\n\n' +
      'Use that same Google account to open the folder. Access expires at 00:00 UTC ' +
      'on the first day of the next month. Submit this form again each month to renew ' +
      'access. If access is still unavailable after 5 minutes, retry or contact spotlake@hanyang.ac.kr.\n\n' +
      'For data covering other time periods or use beyond academic and non-commercial research, ' +
      'please contact spotlake@hanyang.ac.kr. Such use requires prior written permission.'
    );
    props.setProperty('activatedAt', started);
    props.setProperty('enabled', 'true');
    console.log('Monthly account access activated. Earlier responses will not be imported.');
  });
}
function installTriggers_() {
  const triggers = ScriptApp.getProjectTriggers();
  if (!triggers.some(t => t.getHandlerFunction() === 'onDatasetRequest')) {
    ScriptApp.newTrigger('onDatasetRequest').forForm(CONFIG.formId).onFormSubmit().create();
  }
  if (!triggers.some(t => t.getHandlerFunction() === 'reconcileAccess')) {
    ScriptApp.newTrigger('reconcileAccess').timeBased().everyMinutes(5).create();
  }
}

function accepted_(response, now, activatedAt) {
  if (!activatedAt || !Number.isFinite(new Date(activatedAt).getTime())) {
    throw new Error('Missing activation cutoff.');
  }
  const submitted = response.getTimestamp();
  if (!submitted || submitted < new Date(activatedAt) || submitted > now ||
      nextMonthUtc_(submitted) <= now) return false;
  const consent = response.getItemResponses().filter(
    r => r.getItem().getTitle() === CONFIG.consentTitle);
  return consent.length === 1 && Array.isArray(consent[0].getResponse()) &&
    consent[0].getResponse().includes(CONFIG.consentAnswer);
}
function grantKey_(email) {
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, email);
  return 'grant:' + Utilities.base64EncodeWebSafe(digest);
}
function processResponse_(response, now) {
  const props = properties_();
  if (!accepted_(response, now, props.getProperty('activatedAt'))) return;
  const responseId = response.getId();
  if (!responseId) throw new Error('Missing persisted response ID.');
  const doneKey = 'done:' + responseId;
  if (props.getProperty(doneKey)) return;
  const email = (response.getRespondentEmail() || '').trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw new Error('Missing collected account email.');
  const expiration = nextMonthUtc_(response.getTimestamp()).toISOString();
  if (!CONFIG.administrators.includes(email)) {
    const existing = permissions_(CONFIG.folderId).filter(
      p => p.type === 'user' && (p.emailAddress || '').toLowerCase() === email);
    if (existing.length > 1 || existing.some(p => p.role !== 'reader')) {
      throw new Error('Refusing to change an existing non-reader or ambiguous permission.');
    }
    const key = grantKey_(email);
    if (existing.length && !props.getProperty(key)) {
      throw new Error('Existing reader is not managed by this automation.');
    }
    const record = {email, expiration, permissionId: existing[0] ? existing[0].id : null};
    // Persist intent before API mutation. A lost response is recovered by listing the user.
    props.setProperty(key, JSON.stringify(record));
    let permission;
    if (existing.length) {
      if (existing[0].expirationTime && new Date(existing[0].expirationTime) > new Date(expiration)) {
        throw new Error('Unexpected later expiration; manual review required.');
      }
      permission = Drive.Permissions.update({expirationTime: expiration}, CONFIG.folderId,
        existing[0].id, {fields: 'id,role,expirationTime'});
    } else {
      permission = Drive.Permissions.create({type: 'user', role: 'reader', emailAddress: email,
        expirationTime: expiration}, CONFIG.folderId,
        {sendNotificationEmail: false, fields: 'id,role,expirationTime'});
    }
    const verified = Drive.Permissions.get(CONFIG.folderId, permission.id,
      {fields: 'id,type,role,emailAddress,expirationTime'});
    if (verified.role !== 'reader' || (verified.emailAddress || '').toLowerCase() !== email ||
        new Date(verified.expirationTime).getTime() !== new Date(expiration).getTime()) {
      throw new Error('Reader grant verification failed.');
    }
    record.permissionId = permission.id;
    props.setProperty(key, JSON.stringify(record));
  }
  props.setProperty(doneKey, expiration);
}

function onDatasetRequest(event) {
  if (!event || !event.response || !event.source || event.source.getId() !== CONFIG.formId) {
    throw new Error('Expected an installed Google Form submission trigger.');
  }
  return withLock_(() => {
    if (properties_().getProperty('enabled') !== 'true') return;
    form_();
    assertRestricted_();
    processResponse_(event.response, new Date());
  });
}

/** Retry missed/failed submissions; old submissions never renew next month's access. */
function reconcileAccess() {
  return withLock_(() => {
    const props = properties_();
    if (props.getProperty('enabled') !== 'true') return;
    const form = form_(), now = new Date();
    assertRestricted_();
    const since = new Date(Math.max(new Date(props.getProperty('activatedAt')).getTime(),
      monthStartUtc_(now).getTime()));
    const responses = form.getResponses(since);
    let failures = 0;
    const deadline = Date.now() + 240000;
    for (const response of responses) {
      if (Date.now() > deadline) { failures++; break; }
      try { processResponse_(response, new Date()); } catch (_) { failures++; }
    }
    cleanupExpired_(new Date());
    props.setProperty('lastReconciledAt', new Date().toISOString());
    if (failures) throw new Error(failures + ' request(s) pending; inspect configuration or retry.');
  });
}
function cleanupExpired_(now) {
  const props = properties_(), all = props.getProperties();
  let current;
  Object.keys(all).forEach(key => {
    if (key.startsWith('done:') && new Date(all[key]) <= now) props.deleteProperty(key);
    if (!key.startsWith('grant:')) return;
    const record = JSON.parse(all[key]);
    if (new Date(record.expiration) > now) return;
    if (!current) current = permissions_(CONFIG.folderId);
    const p = current.find(p => p.type === 'user' &&
      (p.emailAddress || '').toLowerCase() === record.email);
    if (p) {
      // Never remove lab, manually elevated, or renewed access.
      if (administrator_(p) || p.role !== 'reader' || !p.expirationTime ||
          new Date(p.expirationTime) > now) throw new Error('Expired grant changed; manual review required.');
      Drive.Permissions.remove(CONFIG.folderId, p.id);
    }
    props.deleteProperty(key);
  });
}

/** Disable automation without reopening anonymous sharing. Native expirations remain. */
function pauseAccess() { properties_().setProperty('enabled', 'false'); }

function inspectStatus() {
  const p = properties_();
  console.log(JSON.stringify({enabled: p.getProperty('enabled') === 'true',
    activatedAt: p.getProperty('activatedAt'), lastReconciledAt: p.getProperty('lastReconciledAt'),
    triggers: ScriptApp.getProjectTriggers().map(t => t.getHandlerFunction()),
    generalAccess: permissions_(CONFIG.folderId).filter(
      permission => permission.type === 'anyone' || permission.type === 'domain').length}));
}
