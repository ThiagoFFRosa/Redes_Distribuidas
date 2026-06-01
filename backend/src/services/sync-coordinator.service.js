const pool = require('../database/connection');
const env = require('../config/env');
const repo = require('./cluster-node.repository');
const applyService = require('./sync-apply.service');
const { toMysqlDateTime, nowMysql } = require('../utils/mysql-date');

const normalizeBaseUrl = (node) => {
  const raw = node?.base_url || node?.public_url || (node?.tailscale_ip ? `http://${node.tailscale_ip}:${node.port || env.port}` : '');
  return String(raw || '').replace(/\/$/, '');
};


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

const normalizeUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^https?:\/\//i.test(raw)) return raw;
  if (/^[\w.-]+\.(ngrok\.dev|ngrok-free\.app|dev)(:\d+)?(\/.*)?$/i.test(raw)) return `https://${raw}`;
  if (/^(localhost|\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-f:]+\])(:\d+)?(\/.*)?$/i.test(raw)) return `http://${raw}`;
  return `https://${raw}`;
};

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
      public_url: normalizeUrl(node.public_url),
      port: asInt(node.port, env.port),
      role: normalizeRole(node.role),
      status: normalizeStatus(node.status),
      is_self: isSelf ? 1 : 0,
      power_score: asInt(node.power_score, 5),
      metadata: parseMetadata(node.metadata),
      last_heartbeat_at: toMysqlDateTime(node.last_heartbeat_at),
      last_healthcheck_at: toMysqlDateTime(node.last_healthcheck_at),
      healthcheck_error: node.healthcheck_error || null
    }, { skipSyncEvent: true });
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
      console.log(`[sync] cursor atualizado para ${options.nodeName || remoteNodeUuid} em ${lastSeenMysql}`);
    }
  } catch (cursorError) {
    console.error(`[sync] erro ao atualizar cursor do node ${options.nodeName || remoteNodeUuid}`);
    console.error(`[sync] valor recebido last_seen_event_created_at: ${lastSeen || null}`);
    console.error(`[sync] valor convertido: ${lastSeenMysql}`);
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
  const summary = await applyService.applySyncEvents(data.events || []);
  const lastSeen = (data.events || []).at(-1)?.created_at || since;
  if (remoteUuid) await updateCursor(remoteUuid, lastSeen, null, { nodeName: node?.node_name });
  return { ok: true, pulled: data.events?.length || 0, ...summary };
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

const markDeliveriesSent = async (nodeUuid, events) => {
  for (const event of events) {
    await pool.execute(
      `UPDATE sync_event_deliveries SET status='SENT', last_error=NULL, sent_at=NOW() WHERE event_uuid=? AND target_node_uuid=?`,
      [event.event_uuid, nodeUuid]
    );
  }
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
    console.warn(`[sync] aviso: evento único excede limite configurado entity=${event.entity_type} key=${event.entity_key} payload=${formatKb(bytes)}`);
  }
  return { batch, bytes };
};

const pushToNode = async ({ node_uuid, base_url, limit = env.syncBatchSize, maxBatches = env.syncMaxBatchesPerCycle } = {}) => {
  const node = node_uuid ? await repo.findByNodeUuid(node_uuid) : null;
  const remoteUuid = node_uuid || node?.node_uuid;
  if (!remoteUuid) throw new Error('node_uuid obrigatório');
  const baseUrl = String(base_url || normalizeBaseUrl(node)).replace(/\/$/, '');
  const nodeName = node?.node_name || remoteUuid;
  const batchSize = Math.max(1, Math.min(Number(limit) || env.syncBatchSize, env.syncBatchSize));
  const batchLimit = Math.max(1, Number(maxBatches) || env.syncMaxBatchesPerCycle);
  const destination = `${baseUrl}/api/sync/apply`;
  let pendingCount = await countPendingEventsForNode(remoteUuid);
  console.log(`[sync] preparando envio para ${nodeName}`);
  console.log(`[sync] eventos pendentes: ${pendingCount}`);
  console.log(`[sync] destino: ${destination}`);
  if (!pendingCount) return { ok: true, sent: 0, applied: 0, skipped: 0, failed: 0, batches: 0, pending: 0 };

  let sent = 0;
  let applied = 0;
  let skipped = 0;
  let failed = 0;
  const errors = [];
  let batches = 0;

  while (batches < batchLimit && pendingCount > 0) {
    const events = await getPendingEventsForNode(remoteUuid, batchSize);
    if (!events.length) break;
    const { batch, bytes } = takeBatchWithinPayloadLimit(events, batchSize, env.syncMaxPayloadBytes);
    if (!batch.length) break;
    console.log(`[sync] lote: ${batch.length} eventos`);
    console.log(`[sync] payload estimado: ${formatKb(bytes)}`);
    console.log(`[sync] enviando ${batch.length} eventos para ${nodeName} payload=${formatKb(bytes)}`);
    try {
      const data = await requestJson(destination, { method: 'POST', body: JSON.stringify({ events: batch }) });
      await markDeliveriesSent(remoteUuid, batch);
      batches += 1;
      sent += batch.length;
      applied += Number(data.applied || 0);
      skipped += Number(data.skipped || 0);
      failed += Number(data.failed || 0);
      if (Array.isArray(data.errors)) errors.push(...data.errors);
      console.log(`[sync] lote enviado com sucesso para ${nodeName}`);
    } catch (error) {
      if (error.status === 413) {
        console.error(`[sync] payload recusado pelo node ${nodeName}. Reduzindo batch size.`);
        console.error(`[sync] erro 413: ${error.message}`);
      } else if (error.status >= 500) {
        console.error(`[sync] erro ${error.status} ao enviar lote para ${nodeName}: ${error.message}`);
        if (error.body && Object.keys(error.body).length) console.error('[sync] resposta do node:', error.body);
      }
      await markDeliveriesFailed(remoteUuid, batch, error.message);
      throw error;
    }
    pendingCount = await countPendingEventsForNode(remoteUuid);
  }

  if (pendingCount > 0) console.log(`[sync] ainda existem eventos pendentes para ${nodeName}, continuar no próximo ciclo`);
  return { ok: true, sent, applied, skipped, failed, errors, batches, pending: pendingCount };
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

const getStatus = async () => {
  const self = await repo.getSelfNode();
  const nodes = await repo.getExternalNodes();
  const result = [];
  for (const node of nodes) {
    const [[cursor]] = await pool.execute('SELECT * FROM sync_node_cursors WHERE remote_node_uuid=? LIMIT 1', [node.node_uuid]);
    const [[pending]] = await pool.execute("SELECT COUNT(*) AS total FROM sync_event_deliveries WHERE target_node_uuid=? AND status='PENDING'", [node.node_uuid]);
    result.push({ node_uuid: node.node_uuid, node_name: node.node_name, status: node.status, last_sync_at: cursor?.last_sync_at || null, pending_events: Number(pending?.total || 0), last_error: cursor?.last_error || null });
  }
  return { ok: true, self, nodes: result };
};

module.exports = { listEvents, pullFromNode, pushToNode, fullBootstrap, getStatus, updateCursor, getCursor, normalizeBaseUrl };
