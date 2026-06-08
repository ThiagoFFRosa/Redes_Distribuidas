const indexExists = async (db, tableName, indexName) => {
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS total
       FROM INFORMATION_SCHEMA.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND INDEX_NAME = ?`,
    [tableName, indexName]
  );
  return Number(rows[0]?.total || 0) > 0;
};

const addIndex = async (db, tableName, indexName, definition) => {
  if (!(await indexExists(db, tableName, indexName))) {
    await db.execute(`ALTER TABLE ${tableName} ADD ${definition}`);
  }
};

const cleanupDuplicateCurrentCaches = async (db) => {
  await db.execute(`
    DELETE older
      FROM chart_cache older
      JOIN chart_cache newer
        ON newer.data_point_uuid = older.data_point_uuid
       AND newer.chart_type = older.chart_type
       AND (
            COALESCE(newer.generated_at, '1970-01-01 00:00:00') > COALESCE(older.generated_at, '1970-01-01 00:00:00')
         OR (
              COALESCE(newer.generated_at, '1970-01-01 00:00:00') = COALESCE(older.generated_at, '1970-01-01 00:00:00')
          AND newer.id > older.id
            )
       )
     WHERE older.data_point_uuid IS NOT NULL
  `);
};

module.exports = {
  id: '019_optimize_chart_cache_indexes',
  up: async (db) => {
    await db.execute(`
      UPDATE chart_cache cc
      JOIN data_points dp ON dp.id = cc.data_point_id
         SET cc.data_point_uuid = dp.uuid
       WHERE cc.data_point_uuid IS NULL OR cc.data_point_uuid = ''
    `);

    await cleanupDuplicateCurrentCaches(db);

    await addIndex(
      db,
      'chart_cache',
      'idx_chart_cache_point_type_generated',
      'INDEX idx_chart_cache_point_type_generated (data_point_uuid, chart_type, generated_at, id)'
    );
    await addIndex(db, 'chart_cache', 'idx_chart_cache_data_point_uuid', 'INDEX idx_chart_cache_data_point_uuid (data_point_uuid)');
    await addIndex(db, 'chart_cache', 'idx_chart_cache_uuid', 'INDEX idx_chart_cache_uuid (uuid)');
    await addIndex(
      db,
      'chart_cache',
      'uq_chart_cache_point_uuid_type',
      'UNIQUE KEY uq_chart_cache_point_uuid_type (data_point_uuid, chart_type)'
    );
  }
};
