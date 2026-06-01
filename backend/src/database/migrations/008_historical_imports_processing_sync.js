const columnExists = async (db, tableName, columnName) => {
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS total
       FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND COLUMN_NAME = ?`,
    [tableName, columnName]
  );
  return Number(rows[0]?.total || 0) > 0;
};

const addColumnIfMissing = async (db, tableName, columnName, ddl) => {
  if (!(await columnExists(db, tableName, columnName))) {
    await db.execute(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${ddl}`);
  }
};

module.exports = {
  id: '008_historical_imports_processing_sync',
  up: async (db) => {
    await addColumnIfMissing(db, 'cluster_nodes', 'power_score', 'TINYINT NOT NULL DEFAULT 5');
    await db.execute('UPDATE cluster_nodes SET power_score = 5 WHERE power_score IS NULL');
    await db.execute('ALTER TABLE cluster_nodes MODIFY COLUMN power_score TINYINT NOT NULL DEFAULT 5');

    await db.execute('ALTER TABLE data_points MODIFY COLUMN latitude DECIMAL(10,7) NULL');
    await db.execute('ALTER TABLE data_points MODIFY COLUMN longitude DECIMAL(10,7) NULL');

    await db.execute(`
      CREATE TABLE IF NOT EXISTS historical_imports (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data_point_id INT NULL,
        original_filename VARCHAR(255) NOT NULL,
        sensor_name VARCHAR(180) NULL,
        status ENUM('UPLOADED', 'IMPORTING', 'IMPORTED', 'PROCESSING', 'PROCESSED', 'FAILED') NOT NULL DEFAULT 'UPLOADED',
        total_rows INT NOT NULL DEFAULT 0,
        imported_rows INT NOT NULL DEFAULT 0,
        failed_rows INT NOT NULL DEFAULT 0,
        raw_unit VARCHAR(30) NOT NULL DEFAULT 'cm',
        converted_unit VARCHAR(30) NOT NULL DEFAULT 'm',
        error_message TEXT NULL,
        uploaded_by_user_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        completed_at DATETIME NULL,
        CONSTRAINT fk_historical_imports_data_point FOREIGN KEY (data_point_id) REFERENCES data_points(id) ON DELETE SET NULL,
        INDEX idx_historical_imports_data_point_id (data_point_id),
        INDEX idx_historical_imports_status (status)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS historical_measurements (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        data_point_id INT NOT NULL,
        import_id INT NULL,
        measured_at DATE NOT NULL,
        raw_value DECIMAL(12,3) NULL,
        raw_unit VARCHAR(30) NOT NULL DEFAULT 'cm',
        value DECIMAL(12,3) NOT NULL,
        unit VARCHAR(30) NOT NULL DEFAULT 'm',
        max_value DECIMAL(12,3) NULL,
        min_value DECIMAL(12,3) NULL,
        source ENUM('CSV_IMPORT', 'MANUAL', 'SENSOR') NOT NULL DEFAULT 'CSV_IMPORT',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_historical_measurements_data_point FOREIGN KEY (data_point_id) REFERENCES data_points(id) ON DELETE CASCADE,
        CONSTRAINT fk_historical_measurements_import FOREIGN KEY (import_id) REFERENCES historical_imports(id) ON DELETE SET NULL,
        UNIQUE KEY uq_historical_point_date (data_point_id, measured_at),
        INDEX idx_historical_data_point_id (data_point_id),
        INDEX idx_historical_measured_at (measured_at),
        INDEX idx_historical_import_id (import_id)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS chart_generation_jobs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data_point_id INT NOT NULL,
        import_id INT NULL,
        status ENUM('PENDING', 'RUNNING', 'DONE', 'FAILED') NOT NULL DEFAULT 'PENDING',
        requested_by_node_id INT NULL,
        assigned_node_id INT NULL,
        assigned_node_name VARCHAR(100) NULL,
        progress_percent INT NOT NULL DEFAULT 0,
        estimated_seconds INT NULL,
        error_message TEXT NULL,
        started_at DATETIME NULL,
        finished_at DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_chart_jobs_data_point FOREIGN KEY (data_point_id) REFERENCES data_points(id) ON DELETE CASCADE,
        CONSTRAINT fk_chart_jobs_import FOREIGN KEY (import_id) REFERENCES historical_imports(id) ON DELETE SET NULL,
        INDEX idx_chart_jobs_status_created (status, created_at),
        INDEX idx_chart_jobs_data_point_id (data_point_id)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS chart_cache (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data_point_id INT NOT NULL,
        chart_type ENUM('HISTORICAL_RIVER_LEVEL') NOT NULL DEFAULT 'HISTORICAL_RIVER_LEVEL',
        status ENUM('READY', 'STALE', 'GENERATING', 'FAILED') NOT NULL DEFAULT 'GENERATING',
        generated_by_node_id INT NULL,
        generated_by_node_name VARCHAR(100) NULL,
        total_points INT NOT NULL DEFAULT 0,
        date_start DATE NULL,
        date_end DATE NULL,
        payload JSON NULL,
        summary JSON NULL,
        error_message TEXT NULL,
        generated_at DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_chart_cache_data_point FOREIGN KEY (data_point_id) REFERENCES data_points(id) ON DELETE CASCADE,
        UNIQUE KEY uq_chart_cache_point_type (data_point_id, chart_type)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS sync_outbox (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        entity_type VARCHAR(80) NOT NULL,
        entity_id VARCHAR(120) NOT NULL,
        operation ENUM('UPSERT', 'DELETE') NOT NULL DEFAULT 'UPSERT',
        payload JSON NOT NULL,
        status ENUM('PENDING', 'SENT', 'FAILED') NOT NULL DEFAULT 'PENDING',
        target_node_id INT NULL,
        attempts INT NOT NULL DEFAULT 0,
        last_error TEXT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sent_at DATETIME NULL,
        INDEX idx_sync_outbox_status (status),
        INDEX idx_sync_outbox_target_node (target_node_id)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS sync_inbox (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_node_id INT NULL,
        entity_type VARCHAR(80) NOT NULL,
        entity_id VARCHAR(120) NOT NULL,
        operation ENUM('UPSERT', 'DELETE') NOT NULL DEFAULT 'UPSERT',
        payload_hash VARCHAR(100) NOT NULL,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_sync_inbox_event (entity_type, entity_id, payload_hash)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS sync_outbox_deliveries (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        outbox_id BIGINT NOT NULL,
        target_node_id INT NOT NULL,
        status ENUM('PENDING','SENT','FAILED') NOT NULL DEFAULT 'PENDING',
        attempts INT NOT NULL DEFAULT 0,
        last_error TEXT NULL,
        sent_at DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_sync_outbox_deliveries_outbox FOREIGN KEY (outbox_id) REFERENCES sync_outbox(id) ON DELETE CASCADE,
        UNIQUE KEY uq_sync_outbox_delivery_target (outbox_id, target_node_id),
        INDEX idx_sync_outbox_deliveries_status (status),
        INDEX idx_sync_outbox_deliveries_target (target_node_id)
      )
    `);
  }
};
