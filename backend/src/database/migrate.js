const pool = require('./connection');

const migrations = [
  require('./migrations/001_create_users_table'),
  require('./migrations/002_create_cluster_nodes_table'),
  require('./migrations/003_update_cluster_nodes_for_db_cluster_management'),
  require('./migrations/004_update_cluster_nodes_add_self_and_health_fields'),
  { id: '005_create_cluster_join_requests_table', ...require('./migrations/005_create_cluster_join_requests_table') },
  require('./migrations/006_create_monitoring_tables'),
  require('./migrations/007_add_thresholds_to_data_points'),
  require('./migrations/008_historical_imports_processing_sync'),
  require('./migrations/009_event_sync_foundation')
];

const ensureMigrationsTable = async (connection) => {
  await connection.execute(`
    CREATE TABLE IF NOT EXISTS migrations (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255) NOT NULL UNIQUE,
      executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
};

const formatParams = (params) => {
  if (!Array.isArray(params)) {
    return '[]';
  }

  try {
    return JSON.stringify(params);
  } catch (_error) {
    return '[unserializable params]';
  }
};

const buildDebugConnection = (connection, migrationId) => {
  const runWithDebug = async (method, sql, params = []) => {
    const startedAt = Date.now();

    try {
      return await connection[method](sql, params);
    } catch (error) {
      const durationMs = Date.now() - startedAt;
      const mysqlSql = error && error.sql ? error.sql : sql;
      console.error(`[migrate] erro em ${migrationId} após ${durationMs}ms`);
      console.error(`[migrate] método: connection.${method}`);
      console.error('[migrate] SQL com falha:');
      console.error(mysqlSql);
      console.error(`[migrate] parâmetros: ${formatParams(params)}`);

      throw error;
    }
  };

  return {
    ...connection,
    query: (sql, params) => runWithDebug('query', sql, params),
    execute: (sql, params) => runWithDebug('execute', sql, params)
  };
};

const run = async () => {
  const connection = await pool.getConnection();

  try {
    await ensureMigrationsTable(connection);
    const [rows] = await connection.execute('SELECT name FROM migrations');
    const executed = new Set(rows.map((row) => row.name));

    for (const migration of migrations) {
      if (executed.has(migration.id)) {
        console.log(`[migrate] ${migration.id} já executada, pulando.`);
        continue;
      }

      console.log(`[migrate] executando ${migration.id}...`);
      const debugConnection = buildDebugConnection(connection, migration.id);

      try {
        await migration.up(debugConnection);
      } catch (error) {
        console.error(`[migrate] ${migration.id} falhou.`);
        throw error;
      }

      await connection.execute('INSERT INTO migrations (name) VALUES (?)', [migration.id]);
      console.log(`[migrate] ${migration.id} executada com sucesso.`);
    }

    console.log('[migrate] finalizado.');
  } catch (error) {
    console.error('[migrate] falha ao executar migrations:', error.message);
    process.exitCode = 1;
  } finally {
    connection.release();
    await pool.end();
  }
};

run();
