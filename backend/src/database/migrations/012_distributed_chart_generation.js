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
  id: '012_distributed_chart_generation',
  up: async (db) => {
    await addColumn(db, 'chart_generation_jobs', 'uuid', 'CHAR(36) NULL');
    await db.execute('UPDATE chart_generation_jobs SET uuid = UUID() WHERE uuid IS NULL OR uuid = ""');
    await db.execute('ALTER TABLE chart_generation_jobs MODIFY COLUMN uuid CHAR(36) NOT NULL');
    await addIndex(db, 'chart_generation_jobs', 'uq_chart_generation_jobs_uuid', 'UNIQUE KEY uq_chart_generation_jobs_uuid (uuid)');

    await addColumn(db, 'chart_generation_jobs', 'chart_type', "ENUM('HISTORICAL_RIVER_LEVEL') NOT NULL DEFAULT 'HISTORICAL_RIVER_LEVEL' AFTER data_point_id");
    await addColumn(db, 'chart_generation_jobs', 'data_point_uuid', 'CHAR(36) NULL AFTER chart_type');
    await addColumn(db, 'chart_generation_jobs', 'requested_by_node_uuid', 'CHAR(36) NULL AFTER requested_by_node_id');
    await addColumn(db, 'chart_generation_jobs', 'assigned_to_node_uuid', 'CHAR(36) NULL AFTER assigned_node_id');
    await db.execute("ALTER TABLE chart_generation_jobs MODIFY COLUMN status ENUM('PENDING', 'RUNNING', 'PROCESSING', 'DONE', 'FAILED') NOT NULL DEFAULT 'PENDING'");
    await db.execute("UPDATE chart_generation_jobs SET status='PROCESSING' WHERE status='RUNNING'");
    await db.execute("ALTER TABLE chart_generation_jobs MODIFY COLUMN status ENUM('PENDING', 'PROCESSING', 'DONE', 'FAILED') NOT NULL DEFAULT 'PENDING'");
    await db.execute(`UPDATE chart_generation_jobs cj JOIN data_points dp ON dp.id = cj.data_point_id
      SET cj.data_point_uuid = dp.uuid WHERE cj.data_point_uuid IS NULL OR cj.data_point_uuid = ''`);
    await db.execute(`UPDATE chart_generation_jobs cj JOIN cluster_nodes cn ON cn.id = cj.requested_by_node_id
      SET cj.requested_by_node_uuid = cn.node_uuid WHERE cj.requested_by_node_uuid IS NULL OR cj.requested_by_node_uuid = ''`);
    await db.execute(`UPDATE chart_generation_jobs cj JOIN cluster_nodes cn ON cn.id = cj.assigned_node_id
      SET cj.assigned_to_node_uuid = cn.node_uuid WHERE cj.assigned_to_node_uuid IS NULL OR cj.assigned_to_node_uuid = ''`);
    await addIndex(db, 'chart_generation_jobs', 'idx_chart_jobs_assigned_status', 'INDEX idx_chart_jobs_assigned_status (assigned_to_node_uuid, status, updated_at)');
    await addIndex(db, 'chart_generation_jobs', 'idx_chart_jobs_point_type_status', 'INDEX idx_chart_jobs_point_type_status (data_point_uuid, chart_type, status, created_at)');

    await addColumn(db, 'chart_cache', 'data_point_uuid', 'CHAR(36) NULL AFTER data_point_id');
    await addColumn(db, 'chart_cache', 'generated_by_node_uuid', 'CHAR(36) NULL AFTER generated_by_node_id');
    await addColumn(db, 'chart_cache', 'source_job_uuid', 'CHAR(36) NULL AFTER generated_by_node_name');
    await db.execute(`UPDATE chart_cache cc JOIN data_points dp ON dp.id = cc.data_point_id
      SET cc.data_point_uuid = dp.uuid WHERE cc.data_point_uuid IS NULL OR cc.data_point_uuid = ''`);
    await db.execute(`UPDATE chart_cache cc JOIN cluster_nodes cn ON cn.id = cc.generated_by_node_id
      SET cc.generated_by_node_uuid = cn.node_uuid WHERE cc.generated_by_node_uuid IS NULL OR cc.generated_by_node_uuid = ''`);
    await addIndex(db, 'chart_cache', 'idx_chart_cache_point_uuid_type', 'INDEX idx_chart_cache_point_uuid_type (data_point_uuid, chart_type)');
    await addIndex(db, 'chart_cache', 'idx_chart_cache_source_job_uuid', 'INDEX idx_chart_cache_source_job_uuid (source_job_uuid)');
  }
};
