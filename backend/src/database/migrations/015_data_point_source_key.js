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
  id: '015_data_point_source_key',
  up: async (db) => {
    if (!(await columnExists(db, 'data_points', 'source_key'))) {
      await db.execute('ALTER TABLE data_points ADD COLUMN source_key VARCHAR(255) NULL AFTER uuid');
    }
    if (!(await indexExists(db, 'data_points', 'idx_data_points_source_key'))) {
      await db.execute('ALTER TABLE data_points ADD INDEX idx_data_points_source_key (source_key)');
    }
    if (!(await indexExists(db, 'data_points', 'idx_data_points_natural_key'))) {
      await db.execute('ALTER TABLE data_points ADD INDEX idx_data_points_natural_key (name, city_region, type)');
    }
  }
};
