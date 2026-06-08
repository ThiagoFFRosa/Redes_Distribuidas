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
  id: '018_chart_cache_analytics_payloads',
  up: async (db) => {
    await addColumn(db, 'chart_cache', 'summary_json', 'JSON NULL AFTER summary');
    await addColumn(db, 'chart_cache', 'seasonal_analysis_json', 'JSON NULL AFTER summary_json');
    await addColumn(db, 'chart_cache', 'forecast_json', 'JSON NULL AFTER seasonal_analysis_json');
    await addColumn(db, 'chart_cache', 'payload_json', 'JSON NULL AFTER forecast_json');
    await db.execute(`UPDATE chart_cache SET summary_json = COALESCE(summary_json, summary), payload_json = COALESCE(payload_json, payload) WHERE summary_json IS NULL OR payload_json IS NULL`);
  }
};
