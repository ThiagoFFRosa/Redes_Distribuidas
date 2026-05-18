module.exports = {
  up: async (db) => {
    await db.execute(`
      CREATE TABLE IF NOT EXISTS cluster_join_requests (
        id INT AUTO_INCREMENT PRIMARY KEY,
        node_name VARCHAR(100) NOT NULL,
        tailscale_ip VARCHAR(100) NOT NULL,
        public_url VARCHAR(255) NULL,
        requested_role ENUM('HOST', 'STANDBY', 'UNKNOWN') NOT NULL DEFAULT 'STANDBY',
        status ENUM('PENDING', 'APPROVED', 'REJECTED') NOT NULL DEFAULT 'PENDING',
        request_token_hash VARCHAR(255) NULL,
        secret_fingerprint VARCHAR(100) NULL,
        requester_metadata JSON NULL,
        approved_node_id INT NULL,
        approved_at DATETIME NULL,
        rejected_at DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_join_requests_status (status),
        INDEX idx_join_requests_tailscale_ip (tailscale_ip)
      )
    `);
  },
  down: async (db) => {
    await db.execute('DROP TABLE IF EXISTS cluster_join_requests');
  }
};
