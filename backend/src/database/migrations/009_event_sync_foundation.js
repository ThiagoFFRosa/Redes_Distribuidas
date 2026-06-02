const crypto = require('crypto');

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

const addUuidColumn = async (db, tableName) => {
  if (!(await columnExists(db, tableName, 'uuid'))) {
    await db.execute(`ALTER TABLE ${tableName} ADD COLUMN uuid CHAR(36) NULL`);
  }
  await db.execute(`UPDATE ${tableName} SET uuid = UUID() WHERE uuid IS NULL OR uuid = ''`);
  await db.execute(`ALTER TABLE ${tableName} MODIFY COLUMN uuid CHAR(36) NOT NULL`);
  if (!(await indexExists(db, tableName, `uq_${tableName}_uuid`))) {
    await db.execute(`ALTER TABLE ${tableName} ADD UNIQUE KEY uq_${tableName}_uuid (uuid)`);
  }
};

module.exports = {
  id: '009_event_sync_foundation',
  up: async (db) => {
    if (!(await columnExists(db, 'cluster_nodes', 'node_uuid'))) {
      await db.execute('ALTER TABLE cluster_nodes ADD COLUMN node_uuid CHAR(36) NULL');
    }
    await db.execute('UPDATE cluster_nodes SET node_uuid = UUID() WHERE node_uuid IS NULL OR node_uuid = ""');
    await db.execute('ALTER TABLE cluster_nodes MODIFY COLUMN node_uuid CHAR(36) NOT NULL');
    if (!(await indexExists(db, 'cluster_nodes', 'uq_cluster_nodes_node_uuid'))) {
      await db.execute('ALTER TABLE cluster_nodes ADD UNIQUE KEY uq_cluster_nodes_node_uuid (node_uuid)');
    }

    await addUuidColumn(db, 'data_points');
    await addUuidColumn(db, 'measurements');
    await addUuidColumn(db, 'alerts');
    await addUuidColumn(db, 'historical_imports');
    await addUuidColumn(db, 'historical_measurements');
    await addUuidColumn(db, 'chart_cache');

    await db.execute(`
      CREATE TABLE IF NOT EXISTS sync_events (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        event_uuid CHAR(36) NOT NULL,
        source_node_uuid CHAR(36) NOT NULL,
        origin_node_uuid CHAR(36) NULL,
        entity_type VARCHAR(80) NOT NULL,
        entity_key VARCHAR(180) NOT NULL,
        operation ENUM('UPSERT','DELETE','SOFT_DELETE','RESOLVE') NOT NULL DEFAULT 'UPSERT',
        payload JSON NOT NULL,
        payload_hash VARCHAR(100) NOT NULL,
        version BIGINT NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL,
        applied_locally_at DATETIME NULL,
        UNIQUE KEY uq_sync_event_uuid (event_uuid),
        INDEX idx_sync_events_source_node_uuid (source_node_uuid),
        INDEX idx_sync_events_origin_node_uuid (origin_node_uuid),
        INDEX idx_sync_events_entity (entity_type, entity_key),
        INDEX idx_sync_events_created_at (created_at)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS sync_applied_events (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        event_uuid CHAR(36) NOT NULL,
        source_node_uuid CHAR(36) NOT NULL,
        origin_node_uuid CHAR(36) NULL,
        entity_type VARCHAR(80) NOT NULL,
        entity_key VARCHAR(180) NOT NULL,
        payload_hash VARCHAR(100) NOT NULL,
        applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_sync_applied_event_uuid (event_uuid)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS sync_node_cursors (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        remote_node_uuid CHAR(36) NOT NULL,
        last_seen_event_created_at DATETIME NULL,
        last_sync_at DATETIME NULL,
        last_error TEXT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_sync_node_cursors_remote_uuid (remote_node_uuid)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS sync_event_deliveries (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        event_uuid CHAR(36) NOT NULL,
        target_node_uuid CHAR(36) NOT NULL,
        status ENUM('PENDING','SENT','FAILED') NOT NULL DEFAULT 'PENDING',
        attempts INT NOT NULL DEFAULT 0,
        last_error TEXT NULL,
        sent_at DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_event_target (event_uuid, target_node_uuid),
        INDEX idx_sync_deliveries_target_status (target_node_uuid, status)
      )
    `);
  },
  down: async (db) => {
    await db.execute('DROP TABLE IF EXISTS sync_event_deliveries');
    await db.execute('DROP TABLE IF EXISTS sync_node_cursors');
    await db.execute('DROP TABLE IF EXISTS sync_applied_events');
    await db.execute('DROP TABLE IF EXISTS sync_events');
  }
};
