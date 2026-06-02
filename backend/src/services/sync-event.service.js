const crypto = require('crypto');
const pool = require('../database/connection');
const { shouldSkipSyncEvent } = require('./sync-context.service');
const { nowMysql } = require('../utils/mysql-date');
const env = require('../config/env');
const logger = require('../utils/logger');

const normalizeNumberField = (key, value) => {
  if ((key === 'power_score' || key === 'port') && value !== null && value !== undefined && value !== '') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return value;
};

const canonicalizePayload = (value, key = null) => {
  if (value === undefined) return null;
  const normalized = normalizeNumberField(key, value);
  if (Array.isArray(normalized)) return normalized.map((item) => canonicalizePayload(item));
  if (normalized && typeof normalized === 'object') {
    return Object.keys(normalized).sort().reduce((acc, childKey) => {
      acc[childKey] = canonicalizePayload(normalized[childKey], childKey);
      return acc;
    }, {});
  }
  return normalized;
};

const hashPayload = (payload) => crypto.createHash('sha256').update(JSON.stringify(canonicalizePayload(payload ?? {}))).digest('hex');
const newUuid = () => crypto.randomUUID();
const toJson = (payload) => JSON.stringify(payload ?? {});
const payloadSizeBytes = (payload) => Buffer.byteLength(toJson(payload), 'utf8');
const formatKb = (bytes) => `${(bytes / 1024).toFixed(bytes >= 10240 ? 0 : 1)}KB`;

const getSelfIdentity = async (connection = pool) => {
  const [rows] = await connection.execute('SELECT * FROM cluster_nodes WHERE is_self = 1 LIMIT 1');
  return rows[0] || null;
};

const CLUSTER_NODE_SKIP_REASONS = new Set(['healthcheck', 'heartbeat', 'remote-sync', 'bootstrap', 'startup-health']);

const dedupeWindowSeconds = () => Math.max(1, Math.ceil(Number(env.syncDedupeWindowMs || 2000) / 1000));

const createSyncEvent = async ({ entityType, entityKey, operation = 'UPSERT', payload }, connection = pool, options = {}) => {
  if (!entityType || !entityKey) return null;
  const reason = options.reason || 'unspecified';
  if (entityType === 'cluster_node') {
    logger.debug(`[sync-event] tentativa cluster_node key=${String(entityKey)} reason=${reason}`);
    if (CLUSTER_NODE_SKIP_REASONS.has(reason)) {
      logger.debug(`[sync-event] cluster_node ignorado reason=${reason}`);
      return null;
    }
  }
  if (options.skipSyncEvent || shouldSkipSyncEvent()) return null;

  const self = await getSelfIdentity(connection);
  if (!self?.node_uuid) {
    logger.warn('[sync] self node_uuid não configurado; evento não criado.');
    return null;
  }

  const eventUuid = newUuid();
  const createdAt = nowMysql();
  const payloadHash = hashPayload(payload);
  const windowSeconds = dedupeWindowSeconds();
  const [[duplicate]] = await connection.execute(
    `SELECT id FROM sync_events
      WHERE entity_type = ? AND entity_key = ? AND operation = ? AND payload_hash = ?
        AND created_at >= DATE_SUB(NOW(), INTERVAL ${windowSeconds} SECOND)
      LIMIT 1`,
    [entityType, String(entityKey), operation, payloadHash]
  );
  logger.debug(`[sync-event] payload_hash=${payloadHash}`);
  logger.debug(`[sync-event] dedupe_window_hit=${Boolean(duplicate)}`);
  if (duplicate) return null;

  const sizeBytes = payloadSizeBytes(payload);
  const logPrefix = sizeBytes > 100 * 1024 ? '[sync-event] aviso: payload grande' : '[sync-event] criado';
  logger.info(`${logPrefix} entity=${entityType} key=${String(entityKey)} size=${formatKb(sizeBytes)}`);
  await connection.execute(
    `INSERT INTO sync_events (event_uuid, source_node_uuid, entity_type, entity_key, operation, payload, payload_hash, version, created_at, applied_locally_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [eventUuid, self.node_uuid, entityType, String(entityKey), operation, toJson(payload), payloadHash, Number(payload?.structural_version || 1), createdAt, createdAt]
  );
  await connection.execute(
    `INSERT IGNORE INTO sync_applied_events (event_uuid, source_node_uuid, entity_type, entity_key, payload_hash, applied_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [eventUuid, self.node_uuid, entityType, String(entityKey), payloadHash, createdAt]
  );

  const [targets] = await connection.execute(
    `SELECT node_uuid FROM cluster_nodes
      WHERE is_self = 0 AND node_uuid IS NOT NULL AND node_uuid <> ?`,
    [self.node_uuid]
  );
  for (const target of targets) {
    await connection.execute(
      `INSERT IGNORE INTO sync_event_deliveries (event_uuid, target_node_uuid, status)
       VALUES (?, ?, 'PENDING')`,
      [eventUuid, target.node_uuid]
    );
  }

  return { event_uuid: eventUuid, source_node_uuid: self.node_uuid, entity_type: entityType, entity_key: String(entityKey), operation, payload, payload_hash: payloadHash, created_at: createdAt };
};

const createEntitySyncEvent = async (entityType, payload, operation = 'UPSERT', connection = pool, options = {}) => (
  createSyncEvent({ entityType, entityKey: payload?.uuid || payload?.node_uuid || payload?.entity_key, operation, payload }, connection, options)
);

const backfillExistingSyncEvents = async () => {
  const self = await getSelfIdentity(pool);
  if (!self?.node_uuid) return { ok: true, skipped: true };
  const payloadService = require('./sync-payload.service');
  const entities = [
    ['cluster_node', 'cluster_nodes', 'node_uuid', payloadService.getClusterNodePayloadById],
    ['data_point', 'data_points', 'uuid', payloadService.getDataPointPayloadById],
    ['measurement', 'measurements', 'uuid', payloadService.getMeasurementPayloadById],
    ['alert', 'alerts', 'uuid', payloadService.getAlertPayloadById],
    ['historical_import', 'historical_imports', 'uuid', payloadService.getHistoricalImportPayloadById],
    ['historical_measurement', 'historical_measurements', 'uuid', payloadService.getHistoricalMeasurementPayloadById],
    ['chart_generation_job', 'chart_generation_jobs', 'uuid', payloadService.getChartGenerationJobPayloadById],
    ['chart_cache', 'chart_cache', 'uuid', payloadService.getChartCachePayloadById]
  ];
  let created = 0;
  for (const [entityType, tableName, keyColumn, getPayload] of entities) {
    const [rows] = await pool.execute(`SELECT id, ${keyColumn} AS entity_key FROM ${tableName} WHERE ${keyColumn} IS NOT NULL`);
    for (const row of rows) {
      const [[exists]] = await pool.execute('SELECT id FROM sync_events WHERE entity_type=? AND entity_key=? LIMIT 1', [entityType, row.entity_key]);
      if (exists) continue;
      const payload = await getPayload(row.id, pool);
      if (!payload) continue;
      const event = await createEntitySyncEvent(entityType, payload, 'UPSERT', pool, { reason: 'backfill' });
      if (event) created += 1;
    }
  }
  return { ok: true, created };
};

module.exports = { createSyncEvent, createEntitySyncEvent, backfillExistingSyncEvents, hashPayload, canonicalizePayload };
