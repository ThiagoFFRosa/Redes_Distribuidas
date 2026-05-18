const pool = require('./connection');

const migrations = [
  require('./migrations/001_create_users_table'),
  require('./migrations/002_create_cluster_nodes_table'),
  require('./migrations/003_update_cluster_nodes_for_db_cluster_management'),
  require('./migrations/004_update_cluster_nodes_add_self_and_health_fields')
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
      await migration.up(connection);
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
