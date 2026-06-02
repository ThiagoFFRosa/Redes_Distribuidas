const pool = require('../database/connection');
const { nowMysql } = require('../utils/mysql-date');

const VALID_SOURCE_MODES = new Set(['REMOTE_SYNC', 'BOOTSTRAP']);

const normalizeEntityKey = (entityKey) => (entityKey == null ? null : String(entityKey));

const registryEntityKeyForEvent = (event = {}) => normalizeEntityKey(
  event.entity_key || event.payload?.uuid || event.payload?.node_uuid || event.payload?.entity_key
);

const upsertSyncedEntity = async ({ entityType, entityKey, payloadHash, sourceNodeUuid, sourceMode }, connection = pool) => {
  const normalizedKey = normalizeEntityKey(entityKey);
  const normalizedMode = VALID_SOURCE_MODES.has(sourceMode) ? sourceMode : 'REMOTE_SYNC';
  if (!entityType || !normalizedKey || !payloadHash) return null;
  const timestamp = nowMysql();
  await connection.execute(
    `INSERT INTO synced_entity_registry
       (entity_type, entity_key, payload_hash, source_node_uuid, source_mode, first_seen_at, last_seen_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       payload_hash=VALUES(payload_hash),
       source_node_uuid=VALUES(source_node_uuid),
       source_mode=VALUES(source_mode),
       last_seen_at=VALUES(last_seen_at)`,
    [entityType, normalizedKey, payloadHash, sourceNodeUuid || null, normalizedMode, timestamp, timestamp]
  );
  return { entity_type: entityType, entity_key: normalizedKey, payload_hash: payloadHash, source_node_uuid: sourceNodeUuid || null, source_mode: normalizedMode };
};

const hasSyncedEntity = async (entityType, entityKey, connection = pool) => {
  const normalizedKey = normalizeEntityKey(entityKey);
  if (!entityType || !normalizedKey) return false;
  const [[row]] = await connection.execute(
    'SELECT id FROM synced_entity_registry WHERE entity_type=? AND entity_key=? LIMIT 1',
    [entityType, normalizedKey]
  );
  return Boolean(row);
};

module.exports = { upsertSyncedEntity, hasSyncedEntity, registryEntityKeyForEvent };
