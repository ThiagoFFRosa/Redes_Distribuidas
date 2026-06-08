const env = require('../src/config/env');
const pool = require('../src/database/connection');
const { getDbCounts, COUNT_TABLES } = require('../src/services/db-admin.service');

const parseArgs = (argv) => argv.reduce((acc, arg, index) => {
  if (arg === '--remote-url') acc.remoteUrl = argv[index + 1];
  if (arg.startsWith('--remote-url=')) acc.remoteUrl = arg.slice('--remote-url='.length);
  return acc;
}, {});

const printCounts = (payload, prefix = '[db:counts]') => {
  console.log(`${prefix} database=${payload.database} selected_database=${payload.database} DB_HOST=${payload.host} DB_PORT=${payload.port} DB_NAME=${payload.configured_database || payload.database} DB_USER=${payload.user}`);
  for (const table of COUNT_TABLES) {
    const value = payload.counts?.[table];
    console.log(`${table}: ${value === null || value === undefined ? 'N/A' : value}`);
  }
};

const requestRemoteCounts = async (remoteUrl) => {
  const baseUrl = String(remoteUrl || '').trim().replace(/\/+$/, '');
  if (!baseUrl) throw new Error('--remote-url vazio.');
  const response = await fetch(`${baseUrl}/api/admin/db-counts`, {
    headers: { 'X-Cluster-Secret': env.clusterKey || env.sessionSecret || '' }
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.message || data.error || `HTTP ${response.status}`);
  return data;
};

const run = async () => {
  const args = parseArgs(process.argv.slice(2));
  if (args.remoteUrl) {
    const remote = await requestRemoteCounts(args.remoteUrl);
    printCounts(remote, `[db:counts:remote] url=${args.remoteUrl}`);
    return;
  }
  const local = await getDbCounts();
  printCounts(local);
};

run().catch((error) => {
  console.error(`[db:counts] falha: ${error.message}`);
  process.exitCode = 1;
}).finally(async () => {
  await pool.end().catch(() => {});
});
