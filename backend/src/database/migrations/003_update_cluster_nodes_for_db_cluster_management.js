module.exports = {
  id: '003_update_cluster_nodes_for_db_cluster_management',
  up: async (connection) => {
    const [columns] = await connection.execute('SHOW COLUMNS FROM cluster_nodes');
    const columnMap = new Map(columns.map((column) => [column.Field, column]));

    const hasColumn = (name) => columnMap.has(name);

    if (!hasColumn('tailscale_ip')) {
      await connection.execute("ALTER TABLE cluster_nodes ADD COLUMN tailscale_ip VARCHAR(100) NOT NULL");
    } else {
      await connection.execute("ALTER TABLE cluster_nodes MODIFY COLUMN tailscale_ip VARCHAR(100) NOT NULL");
    }

    if (!hasColumn('is_self')) {
      await connection.execute("ALTER TABLE cluster_nodes ADD COLUMN is_self TINYINT(1) NOT NULL DEFAULT 0");
    }

    if (!hasColumn('last_healthcheck_at')) {
      await connection.execute('ALTER TABLE cluster_nodes ADD COLUMN last_healthcheck_at DATETIME NULL');
    }

    if (!hasColumn('healthcheck_error')) {
      await connection.execute('ALTER TABLE cluster_nodes ADD COLUMN healthcheck_error TEXT NULL');
    }

    if (hasColumn('node_name')) {
      await connection.execute('ALTER TABLE cluster_nodes MODIFY COLUMN node_name VARCHAR(100) NOT NULL');
    }

    await connection.execute('UPDATE cluster_nodes SET tailscale_ip = node_name WHERE tailscale_ip IS NULL OR tailscale_ip = ""');

    const [indexes] = await connection.execute('SHOW INDEX FROM cluster_nodes');
    const hasTailScaleUnique = indexes.some((index) => index.Key_name === 'uq_cluster_nodes_tailscale_ip');
    if (!hasTailScaleUnique) {
      await connection.execute('ALTER TABLE cluster_nodes ADD CONSTRAINT uq_cluster_nodes_tailscale_ip UNIQUE (tailscale_ip)');
    }

    const hasNodeNameUnique = indexes.some((index) => index.Key_name !== 'PRIMARY' && index.Column_name === 'node_name' && index.Non_unique === 0);
    if (hasNodeNameUnique) {
      await connection.execute('ALTER TABLE cluster_nodes DROP INDEX node_name');
    }
  }
};
