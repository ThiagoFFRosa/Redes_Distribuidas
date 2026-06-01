const pool = require('./connection');

const run = async () => {
  const connection = await pool.getConnection();

  try {
    console.log('ATENÇÃO: limpando banco de dados de desenvolvimento.');
    await connection.execute('SET FOREIGN_KEY_CHECKS = 0');
    for (const table of [
      'event_queue_logs',
      'alerts',
      'measurements',
      'data_points',
      'cluster_join_requests',
      'cluster_nodes',
      'users',
      'migrations'
    ]) {
      await connection.execute(`DROP TABLE IF EXISTS ${table}`);
    }
    await connection.execute('SET FOREIGN_KEY_CHECKS = 1');
    console.log('[clear] limpeza concluída.');
  } catch (error) {
    console.error('[clear] falha ao limpar banco:', error.message);
    process.exitCode = 1;
  } finally {
    try { await connection.execute('SET FOREIGN_KEY_CHECKS = 1'); } catch (_error) {}
    connection.release();
    await pool.end();
  }
};

run();
