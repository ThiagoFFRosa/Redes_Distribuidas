const columnExists = async (db, tableName, columnName) => {
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS total FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [tableName, columnName]
  );
  return Number(rows[0]?.total || 0) > 0;
};

const indexExists = async (db, tableName, indexName) => {
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS total FROM INFORMATION_SCHEMA.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`,
    [tableName, indexName]
  );
  return Number(rows[0]?.total || 0) > 0;
};

module.exports = {
  id: '014_remote_sync_origin_registry',
  up: async (db) => {
    if (!(await columnExists(db, 'sync_events', 'origin_node_uuid'))) {
      await db.execute('ALTER TABLE sync_events ADD COLUMN origin_node_uuid CHAR(36) NULL AFTER source_node_uuid');
      await db.execute('UPDATE sync_events SET origin_node_uuid = source_node_uuid WHERE origin_node_uuid IS NULL OR origin_node_uuid = ""');
    }
    if (!(await indexExists(db, 'sync_events', 'idx_sync_events_origin_node_uuid'))) {
      await db.execute('ALTER TABLE sync_events ADD INDEX idx_sync_events_origin_node_uuid (origin_node_uuid)');
    }

    if (!(await columnExists(db, 'sync_applied_events', 'origin_node_uuid'))) {
      await db.execute('ALTER TABLE sync_applied_events ADD COLUMN origin_node_uuid CHAR(36) NULL AFTER source_node_uuid');
      await db.execute('UPDATE sync_applied_events SET origin_node_uuid = source_node_uuid WHERE origin_node_uuid IS NULL OR origin_node_uuid = ""');
    }
    if (!(await indexExists(db, 'sync_applied_events', 'idx_sync_applied_origin_node_uuid'))) {
      await db.execute('ALTER TABLE sync_applied_events ADD INDEX idx_sync_applied_origin_node_uuid (origin_node_uuid)');
    }

    await db.execute(`
      CREATE TABLE IF NOT EXISTS synced_entity_registry (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        entity_type VARCHAR(80) NOT NULL,
        entity_key VARCHAR(180) NOT NULL,
        payload_hash VARCHAR(100) NOT NULL,
        source_node_uuid CHAR(36) NULL,
        source_mode ENUM('REMOTE_SYNC','BOOTSTRAP') NOT NULL,
        first_seen_at DATETIME NOT NULL,
        last_seen_at DATETIME NOT NULL,
        UNIQUE KEY uq_synced_entity_registry_entity (entity_type, entity_key),
        INDEX idx_synced_entity_registry_source (source_node_uuid, source_mode),
        INDEX idx_synced_entity_registry_seen_at (last_seen_at)
      )
    `);
  },
  down: async (db) => {
    await db.execute('DROP TABLE IF EXISTS synced_entity_registry');
    if (await columnExists(db, 'sync_applied_events', 'origin_node_uuid')) {
      await db.execute('ALTER TABLE sync_applied_events DROP COLUMN origin_node_uuid');
    }
    if (await columnExists(db, 'sync_events', 'origin_node_uuid')) {
      await db.execute('ALTER TABLE sync_events DROP COLUMN origin_node_uuid');
    }
  }
};
