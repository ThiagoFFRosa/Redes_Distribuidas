const crypto = require('crypto');
const pool = require('../database/connection');
const { shouldSkipSyncEvent } = require('./sync-context.service');
const { nowMysql } = require('../utils/mysql-date');

const normalizePayload = (value) => {
  if (Array.isArray(value)) return value.map(normalizePayload);
  if (value && typeof value === 'object') {
    return Object.keys(value).sort().reduce((acc, key) => {
      acc[key] = normalizePayload(value[key]);
      return acc;
    }, {});
  }
  return value;
};

const hashPayload = (payload) => crypto.createHash('sha256').update(JSON.stringify(normalizePayload(payload ?? {}))).digest('hex');
const newUuid = () => crypto.randomUUID();
const toJson = (payload) => JSON.stringify(payload ?? {});
const payloadSizeBytes = (payload) => Buffer.byteLength(toJson(payload), 'utf8');
const formatKb = (bytes) => `${(bytes / 1024).toFixed(bytes >= 10240 ? 0 : 1)}KB`;

const getSelfIdentity = async (connection = pool) => {
  const [rows] = await connection.execute('SELECT * FROM cluster_nodes WHERE is_self = 1 LIMIT 1');
  return rows[0] || null;
};

const createSyncEvent = async ({ entityType, entityKey, operation = 'UPSERT', payload }, connection = pool, options = {}) => {
  if (options.skipSyncEvent || shouldSkipSyncEvent()) return null;
  if (!entityType || !entityKey) return null;

  const self = await getSelfIdentity(connection);
  if (!self?.node_uuid) {
    console.warn('[sync] self node_uuid não configurado; evento não criado.');
    return null;
  }

  const eventUuid = newUuid();
  const createdAt = nowMysql();
  const payloadHash = hashPayload(payload);
  const sizeBytes = payloadSizeBytes(payload);
  const logPrefix = sizeBytes > 100 * 1024 ? '[sync-event] aviso: payload grande' : '[sync-event] criado';
  console.log(`${logPrefix} entity=${entityType} key=${String(entityKey)} size=${formatKb(sizeBytes)}`);
  await connection.execute(
    `INSERT INTO sync_events (event_uuid, source_node_uuid, entity_type, entity_key, operation, payload, payload_hash, version, created_at, applied_locally_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
    [eventUuid, self.node_uuid, entityType, String(entityKey), operation, toJson(payload), payloadHash, createdAt, createdAt]
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
    ['chart_cache', 'chart_cache', 'uuid', payloadService.getChartCachePayloadById]
  ];
  let created = 0;
  for (const [entityType, tableName, keyColumn, payloadBuilder] of entities) {
    const [rows] = await pool.execute(`SELECT id, ${keyColumn} AS entity_key FROM ${tableName} ORDER BY id ASC`);
    for (const row of rows) {
      const [[existing]] = await pool.execute('SELECT id FROM sync_events WHERE entity_type=? AND entity_key=? LIMIT 1', [entityType, row.entity_key]);
      if (existing) continue;
      const payload = await payloadBuilder(row.id);
      if (!payload) continue;
      await createSyncEvent({ entityType, entityKey: row.entity_key, operation: 'UPSERT', payload });
      created += 1;
    }
  }
  return { ok: true, created };
};

module.exports = { createSyncEvent, createEntitySyncEvent, hashPayload, normalizePayload, getSelfIdentity, newUuid, backfillExistingSyncEvents };
