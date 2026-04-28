const path = require('path');
const dotenv = require('dotenv');

dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const parseBoolean = (value, fallback = false) => {
  if (value === undefined) return fallback;
  return String(value).toLowerCase() === 'true';
};

const parseNumber = (value, fallback) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};


module.exports = {
  port: parseNumber(process.env.PORT, 3000),
  serverName: process.env.SERVER_NAME || 'server_a',
  serverUrl: process.env.SERVER_URL || 'http://127.0.0.1:3000',
  clusterKey: (process.env.CLUSTER_KEY || '').trim(),
  clusterNodesFile: (process.env.CLUSTER_NODES_FILE || 'cluster-nodes.json').trim(),
  initialRole: (process.env.INITIAL_ROLE || 'STANDBY').toUpperCase() === 'HOST' ? 'HOST' : 'STANDBY',
  adminUser: process.env.ADMIN_USER || 'admin',
  adminPassword: process.env.ADMIN_PASSWORD || 'admin123',
  sessionSecret: process.env.SESSION_SECRET || 'dev-secret',
  heartbeatIntervalMs: parseNumber(process.env.HEARTBEAT_INTERVAL_MS, 3000),
  heartbeatTimeoutMs: parseNumber(process.env.HEARTBEAT_TIMEOUT_MS, 9000),
  switchFallbackDelayMs: parseNumber(process.env.SWITCH_FALLBACK_DELAY_MS, 15000),
  enableNgrok: parseBoolean(process.env.ENABLE_NGROK, true),
  ngrokAuthtoken: process.env.NGROK_AUTHTOKEN || '',
  ngrokRegion: process.env.NGROK_REGION || 'sa',
  ngrokDomain: (process.env.NGROK_DOMAIN || '').trim()
};
