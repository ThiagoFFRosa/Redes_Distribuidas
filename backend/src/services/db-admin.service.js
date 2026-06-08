const pool = require('../database/connection');
const env = require('../config/env');
const clearAllLock = require('./clear-all-lock.service');

const COUNT_TABLES = [
  'users',
  'cluster_nodes',
  'join_requests',
  'cluster_join_requests',
  'data_points',
  'measurements',
  'historical_imports',
  'historical_measurements',
  'alerts',
  'chart_generation_jobs',
  'chart_cache',
  'chart_cache_versions',
  'sync_events',
  'sync_event_deliveries',
  'sync_applied_events',
  'sync_node_cursors',
  'synced_entity_registry',
  'bootstrap_runs',
  'cluster_runtime_state'
];

const SYNC_TABLES = [
  'sync_event_deliveries',
  'sync_applied_events',
  'sync_node_cursors',
  'sync_events',
  'synced_entity_registry'
];

const DATA_TABLES = [
  'chart_cache_versions',
  'chart_cache',
  'chart_generation_jobs',
  'alerts',
  'measurements',
  'historical_measurements',
  'historical_imports',
  'data_points'
];

const CLUSTER_TABLES = [
  'bootstrap_runs',
  'cluster_join_requests',
  'join_requests',
  'cluster_runtime_state',
  'cluster_nodes'
];

const quoteIdentifier = (name) => `\`${String(name).replace(/`/g, '``')}\``;

const getSelectedDatabase = async (connection = pool) => {
  const [[row]] = await connection.execute('SELECT DATABASE() AS database_name');
  return row?.database_name || env.db.name;
};

const tableExists = async (connection, table, databaseName = env.db.name) => {
  const [[row]] = await connection.execute(
    `SELECT COUNT(*) AS total
       FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
    [databaseName, table]
  );
  return Number(row?.total || 0) > 0;
};

const getDbCounts = async (connection = pool) => {
  const database = await getSelectedDatabase(connection);
  const counts = {};
  for (const table of COUNT_TABLES) {
    if (!(await tableExists(connection, table, database))) {
      counts[table] = null;
      continue;
    }
    const [[row]] = await connection.execute(`SELECT COUNT(*) AS total FROM ${quoteIdentifier(table)}`);
    counts[table] = Number(row?.total || 0);
  }
  return {
    ok: true,
    database,
    host: env.db.host,
    port: env.db.port,
    user: env.db.user,
    configured_database: env.db.name,
    counts
  };
};

const buildClearTables = ({ dataOnly = false, syncOnly = false, keepUsers = false } = {}) => {
  if (syncOnly) return SYNC_TABLES;
  const tables = dataOnly ? [...DATA_TABLES] : [...SYNC_TABLES, ...DATA_TABLES, ...CLUSTER_TABLES];
  if (!dataOnly && !keepUsers) tables.push('users');
  return tables;
};

const truncateOrDeleteTable = async (connection, table, { keepSelf = false } = {}) => {
  if (table === 'cluster_nodes' && keepSelf) {
    await connection.execute('DELETE FROM cluster_nodes WHERE COALESCE(is_self, 0) <> 1');
    return 'self preservado; demais registros removidos';
  }
  await connection.execute(`TRUNCATE TABLE ${quoteIdentifier(table)}`);
  return 'truncado';
};

const clearAll = async ({ yes = false, keepUsers = false, keepSelf = false, dataOnly = false, syncOnly = false } = {}) => {
  if (!yes) throw new Error('Confirme a limpeza com --yes.');
  if (dataOnly && syncOnly) throw new Error('Use apenas uma das flags --data-only ou --sync-only.');

  const connection = await pool.getConnection();
  const results = [];
  let database = env.db.name;
  try {
    database = await getSelectedDatabase(connection);
    await connection.execute('SET FOREIGN_KEY_CHECKS = 0');
    for (const table of buildClearTables({ dataOnly, syncOnly, keepUsers })) {
      if (!(await tableExists(connection, table, database))) {
        results.push({ table, action: 'ignored', message: 'não existe' });
        continue;
      }
      const message = await truncateOrDeleteTable(connection, table, { keepSelf });
      results.push({ table, action: 'cleared', message });
    }
    await connection.execute('SET FOREIGN_KEY_CHECKS = 1');
    const lock = await clearAllLock.createLock({ database, host: env.db.host, keep_users: keepUsers, keep_self: keepSelf, data_only: dataOnly, sync_only: syncOnly });
    return { ok: true, database, host: env.db.host, port: env.db.port, user: env.db.user, results, lock_path: clearAllLock.lockPath, lock };
  } finally {
    try { await connection.execute('SET FOREIGN_KEY_CHECKS = 1'); } catch (_error) {}
    connection.release();
  }
};

module.exports = { COUNT_TABLES, getDbCounts, clearAll };
