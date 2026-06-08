const crypto = require('crypto');
const pool = require('../database/connection');
const env = require('../config/env');
const repo = require('./cluster-node.repository');
const applyService = require('./sync-apply.service');
const { toMysqlDateTime, nowMysql } = require('../utils/mysql-date');
const logger = require('../utils/logger');
const { normalizeUrl, getNodeBaseUrl, resolveNodeBaseUrl, getNodeSyncTarget } = require('../utils/sync-targets');
const payloadService = require('./sync-payload.service');
const clearAllLock = require('./clear-all-lock.service');

const normalizeBaseUrl = getNodeBaseUrl;

const parseMetadata = (value) => {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_error) { return {}; }
};

const asInt = (value, fallback = null) => {
  if (value === undefined || value === null || value === '') return fallback;
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : fallback;
};

const normalizeRole = (value) => (['HOST', 'STANDBY', 'UNKNOWN'].includes(value) ? value : 'UNKNOWN');
const normalizeStatus = (value) => (['ONLINE', 'OFFLINE', 'UNKNOWN'].includes(value) ? value : 'UNKNOWN');

const applyBootstrapNodes = async (nodes = []) => {
  const self = await repo.getSelfNode();
  for (const node of nodes) {
    if (!node?.node_uuid && !node?.tailscale_ip) continue;
    const isSelf = Boolean(self && (
      (node.node_uuid && node.node_uuid === self.node_uuid) ||
      (node.tailscale_ip && node.tailscale_ip === self.tailscale_ip)
    ));
    await repo.upsertClusterNode({
      node_uuid: node.node_uuid || null,
      node_name: node.node_name,
      tailscale_ip: node.tailscale_ip,
      public_url: isSelf && self?.public_url ? self.public_url : normalizeUrl(node.public_url),
      port: asInt(node.port, env.port),
      role: normalizeRole(node.role),
      status: normalizeStatus(node.status),
      is_self: isSelf ? 1 : 0,
      power_score: asInt(node.power_score, 5),
      metadata: parseMetadata(node.metadata),
      last_heartbeat_at: toMysqlDateTime(node.last_heartbeat_at),
      last_healthcheck_at: toMysqlDateTime(node.last_healthcheck_at),
      healthcheck_error: node.healthcheck_error || null
    }, { skipSyncEvent: true, reason: 'bootstrap' });
  }
  await repo.enforceSingleSelf();
};

const sanitizeResponseBody = (data) => {
  if (!data || typeof data !== 'object') return data;
  const clone = { ...data };
  delete clone.SESSION_SECRET;
  delete clone.sessionSecret;
  delete clone.secret;
  return clone;
};

const requestJson = async (url, options = {}, timeoutMs = 10000) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      headers: { 'Content-Type': 'application/json', 'X-Cluster-Secret': env.sessionSecret, ...(options.headers || {}) },
      signal: controller.signal
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const message = data.error || data.message || `HTTP ${response.status}`;
      const error = new Error(message);
      error.status = response.status;
      error.body = sanitizeResponseBody(data);
      throw error;
    }
    return data;
  } finally { clearTimeout(timer); }
};

const listEvents = async ({ since = null, limit = 500 } = {}) => {
  const safeLimit = Math.min(Math.max(Number(limit) || 500, 1), 1000);
  const params = [];
  let where = '';
  if (since) {
    const sinceMysql = toMysqlDateTime(since);
    if (!sinceMysql) {
      const error = new Error('Parâmetro since inválido para /api/sync/events. Use ISO 8601 ou YYYY-MM-DD HH:mm:ss.');
      error.statusCode = 400;
      throw error;
    }
    where = 'WHERE created_at > ?';
    params.push(sinceMysql);
  }
  const [rows] = await pool.execute(`SELECT * FROM sync_events ${where} ORDER BY created_at ASC, id ASC LIMIT ${safeLimit}`, params);
  return rows.map((row) => ({ ...row, payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload }));
};

const updateCursor = async (remoteNodeUuid, lastSeen, error = null, options = {}) => {
  const lastSeenMysql = toMysqlDateTime(lastSeen);
  const syncAt = nowMysql();
  try {
    await pool.execute(
      `INSERT INTO sync_node_cursors (remote_node_uuid, last_seen_event_created_at, last_sync_at, last_error)
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         last_seen_event_created_at=COALESCE(VALUES(last_seen_event_created_at), last_seen_event_created_at),
         last_sync_at=VALUES(last_sync_at),
         last_error=VALUES(last_error)`,
      [remoteNodeUuid, lastSeenMysql, syncAt, error]
    );
    if (!error && lastSeen) {
      logger.debug(`[sync] cursor atualizado para ${options.nodeName || remoteNodeUuid} em ${lastSeenMysql}`);
    }
  } catch (cursorError) {
    logger.error(`[sync] erro ao atualizar cursor do node ${options.nodeName || remoteNodeUuid}`);
    logger.error(`[sync] valor recebido last_seen_event_created_at: ${lastSeen || null}`);
    logger.error(`[sync] valor convertido: ${lastSeenMysql}`);
    throw cursorError;
  }
};

const getCursor = async (remoteNodeUuid) => {
  const [[row]] = await pool.execute('SELECT * FROM sync_node_cursors WHERE remote_node_uuid=? LIMIT 1', [remoteNodeUuid]);
  return row || null;
};

const pullFromNode = async ({ node_uuid, base_url, limit = 500 }) => {
  const node = node_uuid ? await repo.findByNodeUuid(node_uuid) : null;
  const remoteUuid = node_uuid || node?.node_uuid;
  const cursor = remoteUuid ? await getCursor(remoteUuid) : null;
  const since = cursor?.last_seen_event_created_at || null;
  const baseUrl = String(base_url || normalizeBaseUrl(node)).replace(/\/$/, '');
  const sinceMysql = toMysqlDateTime(since);
  const data = await requestJson(`${baseUrl}/api/sync/events?limit=${limit}${sinceMysql ? `&since=${encodeURIComponent(sinceMysql)}` : ''}`);
  const events = data.events || [];
  const summary = await applyService.applySyncEvents(events);
  const successfulStatuses = new Set(['APPLIED', 'SKIPPED_ALREADY_APPLIED', 'SKIPPED_OLDER_VERSION']);
  const statusByUuid = new Map((summary.results || []).map((result) => [result.event_uuid, String(result.status || 'UNKNOWN')]));
  let lastSeen = since;
  for (const event of events) {
    if (!successfulStatuses.has(statusByUuid.get(event.event_uuid))) break;
    lastSeen = event.created_at;
  }
  if (remoteUuid && lastSeen !== since) await updateCursor(remoteUuid, lastSeen, null, { nodeName: node?.node_name });
  else if (remoteUuid && (summary.failed || summary.deferred)) await updateCursor(remoteUuid, null, `pull pendente: failed=${summary.failed || 0} deferred=${summary.deferred || 0}`, { nodeName: node?.node_name });
  return { ok: true, pulled: events.length, ...summary };
};

const getPendingEventsForNode = async (nodeUuid, limit = 500) => {
  const safeLimit = Math.min(Math.max(Number(limit) || 500, 1), 1000);
  const [rows] = await pool.execute(
    `SELECT se.* FROM sync_event_deliveries d JOIN sync_events se ON se.event_uuid=d.event_uuid
      WHERE d.target_node_uuid=? AND d.status='PENDING' ORDER BY se.created_at ASC, se.id ASC LIMIT ${safeLimit}`,
    [nodeUuid]
  );
  return rows.map((row) => ({ ...row, payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload }));
};

const payloadBytes = (events) => Buffer.byteLength(JSON.stringify({ events }), 'utf8');
const formatKb = (bytes) => `${Math.ceil(bytes / 1024)}KB`;

const countPendingEventsForNode = async (nodeUuid) => {
  const [[row]] = await pool.execute(
    "SELECT COUNT(*) AS total FROM sync_event_deliveries WHERE target_node_uuid=? AND status='PENDING'",
    [nodeUuid]
  );
  return Number(row?.total || 0);
};

const markDeliveriesFailed = async (nodeUuid, events, error = null) => {
  for (const event of events) {
    await pool.execute(
      `UPDATE sync_event_deliveries
          SET attempts=attempts+1,
              status=IF(attempts + 1 >= 5, 'FAILED', 'PENDING'),
              last_error=?
        WHERE event_uuid=? AND target_node_uuid=?`,
      [String(error || 'erro ao enviar lote').slice(0, 1000), event.event_uuid, nodeUuid]
    );
  }
};

const takeBatchWithinPayloadLimit = (events, maxEvents, maxPayloadBytes) => {
  let batch = events.slice(0, maxEvents);
  while (batch.length > 1 && payloadBytes(batch) > maxPayloadBytes) {
    batch = batch.slice(0, Math.max(1, Math.floor(batch.length / 2)));
  }
  const bytes = payloadBytes(batch);
  if (batch.length === 1 && bytes > maxPayloadBytes) {
    const event = batch[0];
    logger.warn(`[sync] aviso: evento único excede limite configurado entity=${event.entity_type} key=${event.entity_key} payload=${formatKb(bytes)}`);
  }
  return { batch, bytes };
};

const SENT_RESULT_STATUSES = new Set(['APPLIED', 'SKIPPED_ALREADY_APPLIED']);

const normalizeResultStatus = (result) => String(result?.status || 'UNKNOWN').toUpperCase();

const markDeliverySent = async (nodeUuid, eventUuid) => {
  await pool.execute(
    `UPDATE sync_event_deliveries SET status='SENT', last_error=NULL, sent_at=NOW() WHERE event_uuid=? AND target_node_uuid=?`,
    [eventUuid, nodeUuid]
  );
};

const markDeliveryErrored = async (nodeUuid, eventUuid, status, error = null) => {
  if (String(status || '').toUpperCase() === 'DEFERRED_MISSING_DEPENDENCY') {
    await pool.execute(
      `UPDATE sync_event_deliveries SET status='PENDING', last_error=? WHERE event_uuid=? AND target_node_uuid=?`,
      [String(error || status || 'dependência ausente no node remoto').slice(0, 1000), eventUuid, nodeUuid]
    );
    return;
  }
  await pool.execute(
    `UPDATE sync_event_deliveries
        SET attempts=attempts+1,
            status=IF(attempts + 1 >= 5, 'FAILED', 'PENDING'),
            last_error=?
      WHERE event_uuid=? AND target_node_uuid=?`,
    [String(error || status || 'erro ao aplicar no node remoto').slice(0, 1000), eventUuid, nodeUuid]
  );
};

const markDeliveriesFromResults = async (nodeUuid, batch, results) => {
  if (!Array.isArray(results)) return { markedSent: 0, markedFailed: 0, missingResults: batch.length };
  const byEventUuid = new Map(results.filter((result) => result?.event_uuid).map((result) => [result.event_uuid, result]));
  let markedSent = 0;
  let markedFailed = 0;
  let missingResults = 0;
  for (const event of batch) {
    const result = byEventUuid.get(event.event_uuid);
    if (!result) {
      missingResults += 1;
      continue;
    }
    const status = normalizeResultStatus(result);
    if (SENT_RESULT_STATUSES.has(status)) {
      await markDeliverySent(nodeUuid, event.event_uuid);
      markedSent += 1;
    } else {
      await markDeliveryErrored(nodeUuid, event.event_uuid, status, result.message || result.error || null);
      markedFailed += 1;
    }
  }
  return { markedSent, markedFailed, missingResults };
};

const pushToNode = async ({ node_uuid, base_url, limit = env.syncBatchSize, maxBatches = env.syncMaxBatchesPerCycle } = {}) => {
  const node = node_uuid ? await repo.findByNodeUuid(node_uuid) : null;
  const self = await repo.getSelfNode();
  const remoteUuid = node_uuid || node?.node_uuid;
  if (!remoteUuid) throw new Error('node_uuid obrigatório');
  const resolved = base_url
    ? { baseUrl: String(base_url).replace(/\/+$/, ''), targetUrl: `${String(base_url).replace(/\/+$/, '')}/api/sync/apply`, matchedSelfUrl: false }
    : resolveNodeBaseUrl(node, self);
  const baseUrl = resolved.baseUrl;
  if (!baseUrl) throw new Error(`URL de sync não calculada para ${node?.node_name || remoteUuid}`);
  const nodeName = node?.node_name || remoteUuid;
  const batchSize = Math.max(1, Math.min(Number(limit) || env.syncBatchSize, env.syncBatchSize));
  const batchLimit = Math.max(1, Number(maxBatches) || env.syncMaxBatchesPerCycle);
  const destination = resolved.targetUrl;
  let pendingCount = await countPendingEventsForNode(remoteUuid);
  logger.debug(`[sync-target] self=${self?.node_name || '-'}/${self?.tailscale_ip || '-'} remote=${nodeName}/${node?.tailscale_ip || '-'} isSelf=${resolved.isSelf ? 'true' : 'false'} target=${baseUrl}`);
  logger.debug(`[sync] node remoto ${nodeName}: public_url=${node?.public_url || '-'} tailscale_ip=${node?.tailscale_ip || '-'} port=${node?.port || 3000} target=${destination}`);
  logger.debug(`[sync] target ${nodeName} = ${destination}`);
  if (resolved.matchedSelfUrl) logger.warn(`[sync] AVISO: destino calculado para ${nodeName} parece ser o próprio servidor; usando fallback: ${baseUrl}`);
  if (!pendingCount) {
    logger.debug(`[sync] nenhum evento pendente para ${nodeName}`);
    return { ok: true, target_url: destination, sent: 0, applied: 0, skipped: 0, failed: 0, received: 0, batches: 0, pending: 0 };
  }

  let sent = 0;
  let attempted = 0;
  let applied = 0;
  let skipped = 0;
  let failed = 0;
  let received = 0;
  const errors = [];
  let batches = 0;

  while (batches < batchLimit && pendingCount > 0) {
    const events = await getPendingEventsForNode(remoteUuid, batchSize);
    if (!events.length) break;
    const { batch, bytes } = takeBatchWithinPayloadLimit(events, batchSize, env.syncMaxPayloadBytes);
    if (!batch.length) break;
    logger.debug(`[sync] enviando ${batch.length} eventos para ${nodeName} payload=${formatKb(bytes)}`);
    try {
      const data = await requestJson(destination, { method: 'POST', body: JSON.stringify({ events: batch }) });
      batches += 1;
      attempted += batch.length;
      received += Number(data.received || 0);
      applied += Number(data.applied || 0);
      skipped += Number(data.skipped || 0);
      failed += Number(data.failed || 0) + Number(data.deferred || 0);
      if (Array.isArray(data.errors)) errors.push(...data.errors);
      const deliverySummary = await markDeliveriesFromResults(remoteUuid, batch, data.results);
      sent += deliverySummary.markedSent;
      if (deliverySummary.missingResults > 0) {
        logger.warn('[sync] resposta do node não informou results; deliveries mantidas como PENDING');
      }
      if (Number(data.received || 0) > 0 && Number(data.applied || 0) === 0 && Number(data.skipped || 0) === 0) {
        logger.warn(`[sync] ${nodeName}: received=${data.received} mas applied=0 e skipped=0; verifique o receptor`);
      }
      if (Number(data.received || 0) || Number(data.failed || 0) || Number(data.deferred || 0)) logger.info(`[sync] ${nodeName}: received=${Number(data.received || 0)} applied=${Number(data.applied || 0)} skipped=${Number(data.skipped || 0)} failed=${Number(data.failed || 0)} deferred=${Number(data.deferred || 0)}`);
      if (Number(data.failed || 0) > 0 && data.errors?.length) logger.warn(`[sync] erros reportados por ${nodeName}:`, data.errors);
    } catch (error) {
      if (error.status === 413) {
        logger.error(`[sync] payload recusado pelo node ${nodeName}. Reduzindo batch size.`);
        logger.error(`[sync] erro 413: ${error.message}`);
      } else if (error.status >= 500) {
        logger.error(`[sync] erro ${error.status} ao enviar lote para ${nodeName}: ${error.message}`);
        if (error.body && Object.keys(error.body).length) logger.error('[sync] resposta do node:', error.body);
      }
      await markDeliveriesFailed(remoteUuid, batch, error.message);
      throw error;
    }
    pendingCount = await countPendingEventsForNode(remoteUuid);
  }

  if (pendingCount > 0) logger.info(`[sync] ainda existem eventos pendentes para ${nodeName}, continuar no próximo ciclo`);
  return { ok: true, target_url: destination, sent, attempted, received, applied, skipped, failed, errors, batches, pending: pendingCount };
};


const BOOTSTRAP_ENTITIES = ['cluster_nodes', 'data_points', 'historical_imports', 'measurements', 'historical_measurements', 'alerts', 'chart_generation_jobs', 'chart_cache'];
const entityTypeForTable = {
  cluster_nodes: 'cluster_node',
  data_points: 'data_point',
  measurements: 'measurement',
  alerts: 'alert',
  historical_imports: 'historical_import',
  historical_measurements: 'historical_measurement',
  chart_generation_jobs: 'chart_generation_job',
  chart_cache: 'chart_cache'
};
const payloadGetterForTable = {
  cluster_nodes: payloadService.getClusterNodePayloadById,
  data_points: payloadService.getDataPointPayloadById,
  measurements: payloadService.getMeasurementPayloadById,
  alerts: payloadService.getAlertPayloadById,
  historical_imports: payloadService.getHistoricalImportPayloadById,
  historical_measurements: payloadService.getHistoricalMeasurementPayloadById,
  chart_generation_jobs: payloadService.getChartGenerationJobPayloadById,
  chart_cache: payloadService.getChartCachePayloadById
};

const getBootstrapManifest = async ({ requester_name = '-', requester_ip = '-' } = {}) => {
  const self = await repo.getSelfNode().catch(() => null);
  const counts = {};
  for (const table of BOOTSTRAP_ENTITIES) {
    const [[row]] = await pool.execute(`SELECT COUNT(*) AS total FROM ${table}`);
    counts[table] = Number(row?.total || 0);
  }
  const serverTime = nowMysql();
  logger.info(`[bootstrap-server] manifest local=${self?.node_name || env.serverName} requester=${requester_name || '-'} historical_imports=${counts.historical_imports || 0} historical_measurements=${counts.historical_measurements || 0} data_points=${counts.data_points || 0}`);
  return { ok: true, counts, generated_at: new Date().toISOString(), server_time: serverTime, exporter: { node_uuid: self?.node_uuid || null, node_name: self?.node_name || env.serverName, tailscale_ip: self?.tailscale_ip || null }, requester: { node_name: requester_name || null, ip: requester_ip || null } };
};

const exportBootstrapEntity = async ({ entity_type, offset = 0, limit = 500, requester_name = '-', requester_ip = '-' } = {}) => {
  const table = String(entity_type || '');
  if (!BOOTSTRAP_ENTITIES.includes(table)) {
    const error = new Error('entity_type inválido para bootstrap/export');
    error.statusCode = 400;
    throw error;
  }
  const safeLimit = Math.min(Math.max(Number(limit) || 500, 1), 1000);
  const safeOffset = Math.max(Number(offset) || 0, 0);
  const [[totalRow]] = await pool.execute(`SELECT COUNT(*) AS total FROM ${table}`);
  const total = Number(totalRow?.total || 0);
  const self = await repo.getSelfNode().catch(() => null);
  logger.info(`[bootstrap-server] local=${self?.node_name || env.serverName} exporting_to=${requester_name || '-'} requester_ip=${requester_ip || '-'} entity=${table} offset=${safeOffset} limit=${safeLimit} total=${total}`);
  const [rows] = await pool.execute(`SELECT id FROM ${table} ORDER BY id ASC LIMIT ${safeLimit} OFFSET ${safeOffset}`);
  const getter = payloadGetterForTable[table];
  const items = [];
  for (const row of rows) {
    const payload = await getter(row.id, pool);
    if (payload) items.push(payload);
  }
  return { ok: true, entity_type: table, offset: safeOffset, limit: safeLimit, count: items.length, items };
};

const updateBootstrapRun = async (id, patch = {}) => {
  const fields = [];
  const params = [];
  for (const [key, value] of Object.entries(patch)) {
    fields.push(`${key}=?`);
    params.push(value);
  }
  if (!fields.length) return;
  params.push(id);
  await pool.execute(`UPDATE bootstrap_runs SET ${fields.join(', ')} WHERE id=?`, params);
};

const applyBootstrapItems = async (table, items, sourceNodeUuid) => {
  const events = items.map((payload) => ({
    event_uuid: crypto.randomUUID(),
    source_node_uuid: sourceNodeUuid || crypto.randomUUID(),
    origin_node_uuid: sourceNodeUuid || null,
    source_mode: 'BOOTSTRAP',
    entity_type: entityTypeForTable[table],
    entity_key: payload.uuid || payload.node_uuid,
    operation: 'UPSERT',
    payload,
    created_at: nowMysql()
  }));
  return applyService.applySyncEvents(events, { sourceMode: 'BOOTSTRAP' });
};

const runFullBootstrap = async (runId, hostUrl) => {
  let processed = 0;
  try {
    const local = await repo.getSelfNode().catch(() => null);
    logger.info(`[bootstrap-client] local=${local?.node_name || env.serverName} iniciado host=${hostUrl}`);
    const requester = `requester_name=${encodeURIComponent(local?.node_name || env.serverName)}&requester_ip=${encodeURIComponent(local?.tailscale_ip || '')}`;
    const manifest = await requestJson(`${hostUrl}/api/sync/bootstrap/manifest?${requester}`);
    const exporter = manifest.exporter || {};
    logger.info(`[bootstrap-client] local=${local?.node_name || env.serverName} receiving_from=${exporter.node_name || '-'} host_url=${hostUrl} manifest historical_imports=${manifest.counts?.historical_imports || 0} historical_measurements=${manifest.counts?.historical_measurements || 0} data_points=${manifest.counts?.data_points || 0}`);
    const total = BOOTSTRAP_ENTITIES.reduce((sum, table) => sum + Number(manifest.counts?.[table] || 0), 0);
    await updateBootstrapRun(runId, { total_items: total, started_at: nowMysql(), progress_percent: 0 });
    const identity = await requestJson(`${hostUrl}/api/cluster/self-identity`).catch(() => ({}));
    for (const table of BOOTSTRAP_ENTITIES) {
      await updateBootstrapRun(runId, { current_entity: table });
      const entityTotal = Number(manifest.counts?.[table] || 0);
      for (let offset = 0; offset < entityTotal; offset += 500) {
        const exported = await requestJson(`${hostUrl}/api/sync/bootstrap/export?entity_type=${encodeURIComponent(table)}&offset=${offset}&limit=500&${requester}`);
        await applyBootstrapItems(table, exported.items || [], identity.node_uuid || crypto.randomUUID());
        processed += exported.items?.length || 0;
        const percent = total ? Math.min(100, (processed / total) * 100) : 100;
        await updateBootstrapRun(runId, { processed_items: processed, progress_percent: percent });
        if (table === 'historical_measurements' || exported.items?.length) logger.info(`[bootstrap-client] local=${local?.node_name || env.serverName} receiving_from=${exporter.node_name || '-'} host_url=${hostUrl} entity=${table} progress=${Math.min(offset + (exported.items?.length || 0), entityTotal)}/${entityTotal}`);
      }
    }
    let since = manifest.server_time;
    let lastSeen = since;
    while (true) {
      const data = await requestJson(`${hostUrl}/api/sync/events?limit=500&since=${encodeURIComponent(since)}`);
      const events = data.events || [];
      if (!events.length) break;
      await applyService.applySyncEvents(events);
      lastSeen = toMysqlDateTime(events.at(-1).created_at);
      since = lastSeen;
      if (events.length < 500) break;
    }
    if (identity.node_uuid) await updateCursor(identity.node_uuid, lastSeen, null, { nodeName: identity.node_name });
    await updateBootstrapRun(runId, { status: 'DONE', current_entity: null, processed_items: processed, progress_percent: 100, finished_at: nowMysql(), error_message: null });
    logger.info('[bootstrap-client] concluído');
  } catch (error) {
    logger.error(`[bootstrap-client] falhou: ${error.message}`);
    await updateBootstrapRun(runId, { status: 'FAILED', error_message: error.message, finished_at: nowMysql() }).catch(() => {});
  }
};

const startFullBootstrap = async ({ host_url, manual_confirm = false, ignore_clear_lock = false } = {}) => {
  const baseUrl = normalizeUrl(host_url)?.replace(/\/+$/, '');
  if (!baseUrl) throw new Error('host_url obrigatório');
  const lock = await clearAllLock.getLock();
  if (lock.exists && !manual_confirm && !ignore_clear_lock) {
    const error = new Error('Bootstrap bloqueado por .storage/clear-all.lock. Inicie manualmente pelo painel para confirmar.');
    error.statusCode = 409;
    throw error;
  }
  if (lock.exists && manual_confirm) await clearAllLock.removeLock();
  const [result] = await pool.execute(
    `INSERT INTO bootstrap_runs (host_url, status, started_at) VALUES (?, 'RUNNING', ?)`,
    [baseUrl, nowMysql()]
  );
  setImmediate(() => runFullBootstrap(result.insertId, baseUrl));
  return { ok: true, message: 'Bootstrap iniciado.', bootstrap_run_id: result.insertId };
};

const getFullBootstrapStatus = async () => {
  const [rows] = await pool.execute('SELECT * FROM bootstrap_runs ORDER BY id DESC LIMIT 10');
  return { ok: true, runs: rows, current: rows[0] || null };
};

const getFingerprint = async () => {
  await pool.execute('SET SESSION group_concat_max_len = 10485760');
  const result = {};
  const fingerprintQueries = {
    cluster_nodes: `SELECT COUNT(*) AS count, MAX(updated_at) AS latest_at,
      SHA2(COALESCE(GROUP_CONCAT(CONCAT_WS('|', node_uuid, node_name, tailscale_ip, public_url, port, role, power_score, structural_version, COALESCE(metadata,'')) ORDER BY node_uuid SEPARATOR '#'),''), 256) AS checksum
      FROM cluster_nodes`,
    data_points: `SELECT COUNT(*) AS count, MAX(updated_at) AS latest_at,
      SHA2(COALESCE(GROUP_CONCAT(CONCAT_WS('|', uuid, COALESCE(source_key,''), name, type, latitude, longitude, city_region, status, normal_level, warning_level, critical_level, measurement_unit) ORDER BY uuid SEPARATOR '#'),''), 256) AS checksum
      FROM data_points`,
    measurements: `SELECT COUNT(*) AS count, MAX(m.created_at) AS latest_at,
      SHA2(COALESCE(GROUP_CONCAT(CONCAT_WS('|', m.uuid, dp.uuid, m.measurement_type, m.value, m.unit, m.measured_at, m.source, COALESCE(m.observation,'')) ORDER BY m.uuid SEPARATOR '#'),''), 256) AS checksum
      FROM measurements m JOIN data_points dp ON dp.id=m.data_point_id`,
    alerts: `SELECT COUNT(*) AS count, MAX(a.updated_at) AS latest_at,
      SHA2(COALESCE(GROUP_CONCAT(CONCAT_WS('|', a.uuid, dp.uuid, COALESCE(m.uuid,''), a.alert_type, a.severity, a.current_value, a.unit, a.message, a.status, a.detected_at, COALESCE(a.resolved_at,'')) ORDER BY a.uuid SEPARATOR '#'),''), 256) AS checksum
      FROM alerts a JOIN data_points dp ON dp.id=a.data_point_id LEFT JOIN measurements m ON m.id=a.measurement_id`,
    historical_imports: `SELECT COUNT(*) AS count, MAX(hi.updated_at) AS latest_at,
      SHA2(COALESCE(GROUP_CONCAT(CONCAT_WS('|', hi.uuid, COALESCE(dp.uuid,''), hi.original_filename, COALESCE(hi.sensor_name,''), hi.status, hi.total_rows, hi.imported_rows, hi.failed_rows, hi.raw_unit, hi.converted_unit, COALESCE(hi.error_message,''), COALESCE(hi.completed_at,'')) ORDER BY hi.uuid SEPARATOR '#'),''), 256) AS checksum
      FROM historical_imports hi LEFT JOIN data_points dp ON dp.id=hi.data_point_id`,
    historical_measurements: `SELECT COUNT(*) AS count, MAX(hm.created_at) AS latest_at,
      SHA2(COALESCE(GROUP_CONCAT(CONCAT_WS('|', hm.uuid, dp.uuid, COALESCE(hi.uuid,''), hm.measured_at, hm.raw_value, hm.raw_unit, hm.value, hm.unit, COALESCE(hm.max_value,''), COALESCE(hm.min_value,''), hm.source) ORDER BY hm.uuid SEPARATOR '#'),''), 256) AS checksum
      FROM historical_measurements hm JOIN data_points dp ON dp.id=hm.data_point_id LEFT JOIN historical_imports hi ON hi.id=hm.import_id`,
    chart_cache: `SELECT COUNT(*) AS count, MAX(cc.updated_at) AS latest_at,
      SHA2(COALESCE(GROUP_CONCAT(CONCAT_WS('|', cc.uuid, dp.uuid, cc.chart_type, cc.status, COALESCE(cc.generated_by_node_name,''), cc.total_points, COALESCE(cc.date_start,''), COALESCE(cc.date_end,''), COALESCE(cc.generated_at,'')) ORDER BY cc.uuid SEPARATOR '#'),''), 256) AS checksum
      FROM chart_cache cc JOIN data_points dp ON dp.id=cc.data_point_id`
  };
  for (const table of BOOTSTRAP_ENTITIES) {
    const [[row]] = await pool.execute(fingerprintQueries[table]);
    result[table] = { count: Number(row?.count || 0), latest_at: row?.latest_at || null, checksum: row?.checksum || null };
  }
  return { ok: true, generated_at: new Date().toISOString(), tables: result };
};

const fullBootstrap = async ({ host_url }) => {
  const baseUrl = String(host_url || '').replace(/\/$/, '');
  if (!baseUrl) throw new Error('host_url obrigatório');
  const bootstrap = await requestJson(`${baseUrl}/api/cluster/bootstrap`);
  await applyBootstrapNodes(bootstrap.nodes || []);
  const identity = bootstrap.self || bootstrap.host || await requestJson(`${baseUrl}/api/cluster/self-identity`);
  let since = null;
  let pulled = 0;
  while (true) {
    const data = await requestJson(`${baseUrl}/api/sync/events?limit=500${since ? `&since=${encodeURIComponent(since)}` : ''}`);
    const events = data.events || [];
    if (!events.length) break;
    await applyService.applySyncEvents(events);
    pulled += events.length;
    since = toMysqlDateTime(events.at(-1).created_at);
    if (events.length < 500) break;
  }
  if (identity.node_uuid) await updateCursor(identity.node_uuid, since, null, { nodeName: identity.node_name });
  return { ok: true, host: identity, bootstrapped_nodes: bootstrap.nodes?.length || 0, pulled };
};


const compareFingerprint = async ({ node_uuid } = {}) => {
  const local = await getFingerprint();
  if (!node_uuid) return { ok: true, local, comparisons: [] };
  const node = await repo.findByNodeUuid(node_uuid);
  if (!node) throw new Error('node_uuid remoto não encontrado');
  const self = await repo.getSelfNode();
  const resolved = resolveNodeBaseUrl(node, self);
  if (!resolved.baseUrl) throw new Error('URL remota não configurada');
  const remote = await requestJson(`${resolved.baseUrl}/api/sync/fingerprint`);
  const comparisons = BOOTSTRAP_ENTITIES.map((table) => {
    const l = local.tables?.[table] || {};
    const r = remote.tables?.[table] || {};
    return { table, status: l.count === r.count && l.checksum === r.checksum ? 'OK' : 'DIVERGENT', local: l, remote: r };
  });
  return { ok: true, node_uuid, node_name: node.node_name, target_url: `${resolved.baseUrl}/api/sync/fingerprint`, comparisons };
};

const getStatus = async () => {
  const self = await repo.getSelfNode();
  const nodes = await repo.getExternalNodes();
  const result = [];
  for (const node of nodes) {
    const [[cursor]] = await pool.execute('SELECT * FROM sync_node_cursors WHERE remote_node_uuid=? LIMIT 1', [node.node_uuid]);
    const [[pending]] = await pool.execute("SELECT COUNT(*) AS total FROM sync_event_deliveries WHERE target_node_uuid=? AND status='PENDING'", [node.node_uuid]);
    const resolved = resolveNodeBaseUrl(node, self);
    result.push({ node_uuid: node.node_uuid, node_name: node.node_name, target_url: resolved.targetUrl || getNodeSyncTarget(node), status: node.status, last_sync_at: cursor?.last_sync_at || null, pending_events: Number(pending?.total || 0), last_error: cursor?.last_error || null, last_pull_count: 0, last_push_count: 0, last_remote_applied: 0, last_remote_failed: 0 });
  }
  return { ok: true, self, nodes: result };
};

module.exports = { listEvents, pullFromNode, pushToNode, fullBootstrap, startFullBootstrap, getFullBootstrapStatus, getBootstrapManifest, exportBootstrapEntity, getFingerprint, compareFingerprint, getStatus, updateCursor, getCursor, normalizeBaseUrl, getNodeBaseUrl, getNodeSyncTarget };
