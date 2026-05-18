module.exports = {
  id: '002_create_cluster_nodes_table',
  up: async (connection) => {
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS cluster_nodes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        node_name VARCHAR(100) NOT NULL UNIQUE,
        tailscale_ip VARCHAR(100) NULL,
        public_url VARCHAR(255) NULL,
        role ENUM('HOST', 'STANDBY', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
        status ENUM('ONLINE', 'OFFLINE', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
        last_heartbeat_at DATETIME NULL,
        metadata JSON NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
  }
};
