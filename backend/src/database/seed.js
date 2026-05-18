const pool = require('./connection');
const adminSeed = require('./seeds/001_admin_user');

const run = async () => {
  const connection = await pool.getConnection();

  try {
    await connection.execute('SELECT 1 FROM users LIMIT 1');
  } catch (error) {
    console.error('[seed] tabela users não encontrada. Rode primeiro: npm run migrate');
    connection.release();
    await pool.end();
    process.exit(1);
  }

  try {
    console.log(`[seed] executando ${adminSeed.id}...`);
    await adminSeed.run(connection);
    console.log('[seed] finalizado.');
  } catch (error) {
    console.error('[seed] falha ao executar seeds:', error.message);
    process.exitCode = 1;
  } finally {
    connection.release();
    await pool.end();
  }
};

run();
