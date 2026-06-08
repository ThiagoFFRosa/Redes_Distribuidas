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
  id: '017_point_records_audit_soft_delete',
  up: async (db) => {
    await addColumn(db, 'measurements', 'updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP');
    await addColumn(db, 'measurements', 'deleted_at', 'DATETIME NULL');
    await addColumn(db, 'measurements', 'deleted_by_node_uuid', 'VARCHAR(36) NULL');
    await addIndex(db, 'measurements', 'idx_measurements_deleted_at', 'INDEX idx_measurements_deleted_at (deleted_at)');

    await addColumn(db, 'historical_measurements', 'updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP');
    await addColumn(db, 'historical_measurements', 'corrected_at', 'DATETIME NULL');
    await addColumn(db, 'historical_measurements', 'corrected_by_node_uuid', 'VARCHAR(36) NULL');
    await addColumn(db, 'historical_measurements', 'correction_reason', 'TEXT NULL');
    await addColumn(db, 'historical_measurements', 'original_value', 'DECIMAL(12,3) NULL');
    await addColumn(db, 'historical_measurements', 'original_measured_at', 'DATETIME NULL');
    await addColumn(db, 'historical_measurements', 'deleted_at', 'DATETIME NULL');
    await addColumn(db, 'historical_measurements', 'deleted_by_node_uuid', 'VARCHAR(36) NULL');
    await addIndex(db, 'historical_measurements', 'idx_historical_deleted_at', 'INDEX idx_historical_deleted_at (deleted_at)');
  }
};
