module.exports = {
  id: '006_create_monitoring_tables',
  up: async (db) => {
    await db.execute(`
      CREATE TABLE IF NOT EXISTS data_points (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(180) NOT NULL,
        type ENUM('RIVER_LEVEL') NOT NULL DEFAULT 'RIVER_LEVEL',
        latitude DECIMAL(10,7) NOT NULL,
        longitude DECIMAL(10,7) NOT NULL,
        city_region VARCHAR(180) NULL,
        description TEXT NULL,
        status ENUM('ACTIVE', 'INACTIVE') NOT NULL DEFAULT 'ACTIVE',
        created_by_user_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_data_points_type (type),
        INDEX idx_data_points_status (status),
        INDEX idx_data_points_location (latitude, longitude)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS measurements (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data_point_id INT NOT NULL,
        measurement_type ENUM('RIVER_LEVEL') NOT NULL DEFAULT 'RIVER_LEVEL',
        value DECIMAL(10,3) NOT NULL,
        unit VARCHAR(30) NOT NULL DEFAULT 'm',
        measured_at DATETIME NOT NULL,
        source ENUM('MANUAL', 'SENSOR', 'IMPORT') NOT NULL DEFAULT 'MANUAL',
        observation TEXT NULL,
        created_by_user_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_measurements_data_point FOREIGN KEY (data_point_id) REFERENCES data_points(id) ON DELETE CASCADE,
        INDEX idx_measurements_data_point_id (data_point_id),
        INDEX idx_measurements_measured_at (measured_at),
        INDEX idx_measurements_type (measurement_type)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS alerts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data_point_id INT NOT NULL,
        measurement_id INT NULL,
        alert_type ENUM('RIVER_LEVEL_HIGH', 'RIVER_LEVEL_CRITICAL') NOT NULL,
        severity ENUM('NORMAL', 'ATTENTION', 'CRITICAL') NOT NULL,
        current_value DECIMAL(10,3) NOT NULL,
        unit VARCHAR(30) NOT NULL DEFAULT 'm',
        message VARCHAR(255) NOT NULL,
        status ENUM('ACTIVE', 'RESOLVED') NOT NULL DEFAULT 'ACTIVE',
        detected_at DATETIME NOT NULL,
        resolved_at DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_alerts_data_point FOREIGN KEY (data_point_id) REFERENCES data_points(id) ON DELETE CASCADE,
        CONSTRAINT fk_alerts_measurement FOREIGN KEY (measurement_id) REFERENCES measurements(id) ON DELETE SET NULL,
        INDEX idx_alerts_data_point_id (data_point_id),
        INDEX idx_alerts_severity (severity),
        INDEX idx_alerts_status (status),
        INDEX idx_alerts_detected_at (detected_at)
      )
    `);

    await db.execute(`
      CREATE TABLE IF NOT EXISTS event_queue_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        event_type VARCHAR(80) NOT NULL,
        status ENUM('RECEIVED', 'VALIDATING', 'PERSISTED', 'REPLICATED', 'PROCESSED', 'FAILED') NOT NULL DEFAULT 'RECEIVED',
        payload JSON NULL,
        message VARCHAR(255) NULL,
        related_measurement_id INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed_at DATETIME NULL,
        INDEX idx_event_queue_logs_status (status),
        INDEX idx_event_queue_logs_created_at (created_at)
      )
    `);
  },
  down: async (db) => {
    await db.execute('DROP TABLE IF EXISTS event_queue_logs');
    await db.execute('DROP TABLE IF EXISTS alerts');
    await db.execute('DROP TABLE IF EXISTS measurements');
    await db.execute('DROP TABLE IF EXISTS data_points');
  }
};
