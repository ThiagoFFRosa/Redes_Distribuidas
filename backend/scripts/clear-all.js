const pool = require('../src/database/connection');
const { clearAll } = require('../src/services/db-admin.service');

const hasFlag = (name) => process.argv.includes(name);

const run = async () => {
  const options = {
    yes: hasFlag('--yes'),
    keepUsers: hasFlag('--keep-users'),
    keepSelf: hasFlag('--keep-self'),
    dataOnly: hasFlag('--data-only'),
    syncOnly: hasFlag('--sync-only')
  };
  const result = await clearAll(options);
  console.log(`[clear-all] database=${result.database} host=${result.host} port=${result.port} user=${result.user}`);
  for (const item of result.results) {
    const status = item.action === 'ignored' ? 'ignorado' : item.message;
    console.log(`[clear-all] ${item.table} ${status}`);
  }
  console.log(`[clear-all] lock criado em ${result.lock_path}`);
  console.log('[clear-all] concluído');
};

run().catch((error) => {
  console.error(`[clear-all] falha: ${error.message}`);
  process.exitCode = 1;
}).finally(async () => {
  await pool.end().catch(() => {});
});
