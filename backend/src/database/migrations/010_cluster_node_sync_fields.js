const columnExists = async (db, tableName, columnName) => {
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS total FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [tableName, columnName]
  );
  return Number(rows[0]?.total || 0) > 0;
};

module.exports = {
  id: '010_cluster_node_sync_fields',
  up: async (db) => {
    if (!(await columnExists(db, 'cluster_nodes', 'port'))) {
      await db.execute('ALTER TABLE cluster_nodes ADD COLUMN port INT NULL AFTER public_url');
    }


    if (!(await columnExists(db, 'cluster_nodes', 'structural_version'))) {
      await db.execute('ALTER TABLE cluster_nodes ADD COLUMN structural_version BIGINT NOT NULL DEFAULT 1 AFTER power_score');
    }

    await db.execute('UPDATE cluster_nodes SET structural_version = 1 WHERE structural_version IS NULL OR structural_version < 1');

    if (!(await columnExists(db, 'cluster_join_requests', 'node_uuid'))) {
      await db.execute('ALTER TABLE cluster_join_requests ADD COLUMN node_uuid CHAR(36) NULL AFTER id');
    }

    if (!(await columnExists(db, 'cluster_join_requests', 'port'))) {
      await db.execute('ALTER TABLE cluster_join_requests ADD COLUMN port INT NULL AFTER public_url');
    }

    if (!(await columnExists(db, 'cluster_join_requests', 'power_score'))) {
      await db.execute('ALTER TABLE cluster_join_requests ADD COLUMN power_score TINYINT NOT NULL DEFAULT 5 AFTER requested_role');
    }

    await db.execute('UPDATE cluster_join_requests SET power_score = 5 WHERE power_score IS NULL');
  },
  down: async () => {}
};
