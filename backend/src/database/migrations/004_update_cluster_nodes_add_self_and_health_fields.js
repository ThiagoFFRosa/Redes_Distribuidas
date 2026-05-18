async function columnExists(connection, tableName, columnName) {
  const [rows] = await connection.query(
    `
    SELECT COUNT(*) AS count
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?
    `,
    [tableName, columnName]
  );

  return rows[0].count > 0;
}

module.exports = {
  id: '004_update_cluster_nodes_add_self_and_health_fields',
  up: async (connection) => {
    const ensureColumn = async (columnName, ddl) => {
      if (!(await columnExists(connection, 'cluster_nodes', columnName))) {
        await connection.query(`ALTER TABLE cluster_nodes ADD COLUMN ${ddl}`);
      }
    };

    await ensureColumn('is_self', 'is_self TINYINT(1) NOT NULL DEFAULT 0');
    await ensureColumn('last_healthcheck_at', 'last_healthcheck_at DATETIME NULL');
    await ensureColumn('healthcheck_error', 'healthcheck_error TEXT NULL');
    await ensureColumn('metadata', 'metadata JSON NULL');
    await ensureColumn('public_url', 'public_url VARCHAR(255) NULL');
    await ensureColumn('last_heartbeat_at', 'last_heartbeat_at DATETIME NULL');
    await ensureColumn('role', "role ENUM('HOST', 'STANDBY', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN'");
    await ensureColumn('status', "status ENUM('ONLINE', 'OFFLINE', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN'");

    if (await columnExists(connection, 'cluster_nodes', 'tailscale_ip')) {
      await connection.query('UPDATE cluster_nodes SET tailscale_ip = node_name WHERE tailscale_ip IS NULL OR tailscale_ip = ""');
      await connection.query('ALTER TABLE cluster_nodes MODIFY COLUMN tailscale_ip VARCHAR(100) NOT NULL');

      const [duplicateRows] = await connection.query(`
        SELECT tailscale_ip, COUNT(*) AS qty
        FROM cluster_nodes
        GROUP BY tailscale_ip
        HAVING COUNT(*) > 1
      `);

      if (duplicateRows.length > 0) {
        console.warn('[migrate] Encontrados tailscale_ip duplicados. Limpando registros mantendo menor id por tailscale_ip.');
        await connection.query(`
          DELETE c1
          FROM cluster_nodes c1
          JOIN cluster_nodes c2
            ON c1.tailscale_ip = c2.tailscale_ip
           AND c1.id > c2.id
        `);
      }

      const [indexes] = await connection.query('SHOW INDEX FROM cluster_nodes');
      const hasUniqueTailscaleIp = indexes.some((index) => index.Column_name === 'tailscale_ip' && index.Non_unique === 0);
      if (!hasUniqueTailscaleIp) {
        await connection.query('ALTER TABLE cluster_nodes ADD CONSTRAINT uq_cluster_nodes_tailscale_ip UNIQUE (tailscale_ip)');
      }
    }
  }
};
