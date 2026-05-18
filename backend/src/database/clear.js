const pool = require('./connection');

const run = async () => {
  const connection = await pool.getConnection();

  try {
    console.log('ATENÇÃO: limpando banco de dados de desenvolvimento.');
    await connection.execute('DROP TABLE IF EXISTS users');
    await connection.execute('DROP TABLE IF EXISTS cluster_nodes');
    await connection.execute('DROP TABLE IF EXISTS migrations');
    console.log('[clear] limpeza concluída.');
  } catch (error) {
    console.error('[clear] falha ao limpar banco:', error.message);
    process.exitCode = 1;
  } finally {
    connection.release();
    await pool.end();
  }
};

run();
