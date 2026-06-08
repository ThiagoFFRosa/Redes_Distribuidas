const columnExists = async (db, tableName, columnName) => {
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS total FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [tableName, columnName]
  );
  return Number(rows[0]?.total || 0) > 0;
};

const addColumn = async (db, tableName, columnName, definition) => {
  if (!(await columnExists(db, tableName, columnName))) {
    await db.execute(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }
};

module.exports = {
  id: '020_chart_cache_payload_hash',
  up: async (db) => {
    await addColumn(db, 'chart_cache', 'payload_hash', 'CHAR(64) NULL AFTER payload_json');
    await db.execute(`
      UPDATE chart_cache
         SET payload_hash = SHA2(CONCAT(
           COALESCE(CAST(payload_json AS CHAR), COALESCE(CAST(payload AS CHAR), '')),
           '|',
           COALESCE(CAST(summary_json AS CHAR), COALESCE(CAST(summary AS CHAR), '')),
           '|',
           COALESCE(CAST(seasonal_analysis_json AS CHAR), ''),
           '|',
           COALESCE(CAST(forecast_json AS CHAR), '')
         ), 256)
       WHERE payload_hash IS NULL OR payload_hash = ''
    `);
  }
};
