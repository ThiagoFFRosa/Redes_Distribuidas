module.exports = {
  id: '011_bootstrap_runs',
  up: async (db) => {
    await db.execute(`
      CREATE TABLE IF NOT EXISTS bootstrap_runs (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        host_url VARCHAR(500) NOT NULL,
        status ENUM('RUNNING','DONE','FAILED') NOT NULL DEFAULT 'RUNNING',
        current_entity VARCHAR(80) NULL,
        total_items BIGINT NOT NULL DEFAULT 0,
        processed_items BIGINT NOT NULL DEFAULT 0,
        progress_percent DECIMAL(5,2) NOT NULL DEFAULT 0,
        error_message TEXT NULL,
        started_at DATETIME NULL,
        finished_at DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
  },
  down: async (db) => { await db.execute('DROP TABLE IF EXISTS bootstrap_runs'); }
};
