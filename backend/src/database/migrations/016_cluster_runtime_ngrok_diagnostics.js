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

const addColumn = async (db, tableName, columnName, definition) => {
  if (!(await columnExists(db, tableName, columnName))) {
    await db.execute(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }
};

const addIndex = async (db, tableName, indexName, definition) => {
  if (!(await indexExists(db, tableName, indexName))) {
    await db.execute(`ALTER TABLE ${tableName} ADD ${definition}`);
  }
};

module.exports = {
  id: '016_cluster_runtime_ngrok_diagnostics',
  up: async (db) => {
    await addColumn(db, 'cluster_nodes', 'ngrok_enabled_currently', 'TINYINT(1) NOT NULL DEFAULT 0 AFTER public_url');
    await addColumn(db, 'cluster_nodes', 'ngrok_status', "ENUM('ONLINE','OFFLINE','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN' AFTER ngrok_enabled_currently");
    await addColumn(db, 'cluster_nodes', 'ngrok_last_seen_at', 'DATETIME NULL AFTER ngrok_status');

    await db.execute(`
      CREATE TABLE IF NOT EXISTS cluster_runtime_state (
        state_key VARCHAR(100) PRIMARY KEY,
        state_value TEXT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);

    await db.execute("ALTER TABLE chart_generation_jobs MODIFY COLUMN status ENUM('PENDING','PROCESSING','DONE','FAILED','CANCELLED') NOT NULL DEFAULT 'PENDING'");
    await addIndex(db, 'chart_cache', 'idx_chart_cache_point_type_status', 'INDEX idx_chart_cache_point_type_status (data_point_uuid, chart_type, status, generated_at)');
  }
};
