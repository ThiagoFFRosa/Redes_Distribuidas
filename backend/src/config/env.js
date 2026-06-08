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

const parsePositiveInteger = (value, fallback) => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
};


module.exports = {
  port: parseNumber(process.env.PORT, 3000),
  bindHost: process.env.BIND_HOST || '0.0.0.0',
  serverName: process.env.SERVER_NAME || 'server_a',
  serverUrl: process.env.SERVER_URL || 'http://127.0.0.1:3000',
  logLevel: process.env.LOG_LEVEL || 'info',
  clusterKey: (process.env.CLUSTER_KEY || '').trim(),
  clusterNodesFile: (process.env.CLUSTER_NODES_FILE || 'cluster-nodes.json').trim(),
  initialRole: (process.env.INITIAL_ROLE || 'STANDBY').toUpperCase() === 'HOST' ? 'HOST' : 'STANDBY',
  sessionSecret: process.env.SESSION_SECRET || 'dev-secret',
  jwtSecret: process.env.JWT_SECRET || 'dev_secret_change_me',
  db: {
    host: process.env.DB_HOST || 'localhost',
    port: parseNumber(process.env.DB_PORT, 3306),
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    name: process.env.DB_NAME || 'redes_distribuidas',
    connectionLimit: parseNumber(process.env.DB_CONNECTION_LIMIT, 10)
  },
  heartbeatIntervalMs: parseNumber(process.env.HEARTBEAT_INTERVAL_MS, 3000),
  heartbeatTimeoutMs: parseNumber(process.env.HEARTBEAT_TIMEOUT_MS, 9000),
  switchFallbackDelayMs: parseNumber(process.env.SWITCH_FALLBACK_DELAY_MS, 15000),
  enableNgrok: parseBoolean(process.env.ENABLE_NGROK, true),
  ngrokAuthtoken: process.env.NGROK_AUTHTOKEN || '',
  ngrokRegion: process.env.NGROK_REGION || 'sa',
  ngrokDomain: (process.env.NGROK_DOMAIN || '').trim(),
  publicUrlCheckIntervalMs: parseNumber(process.env.PUBLIC_URL_CHECK_INTERVAL_MS, 30000),
  publicUrlCheckTimeoutMs: parseNumber(process.env.PUBLIC_URL_CHECK_TIMEOUT_MS, 3000),
  ngrokCheckIntervalMs: parsePositiveInteger(process.env.NGROK_CHECK_INTERVAL_MS, 10000),
  ngrokTakeoverGraceMs: parsePositiveInteger(process.env.NGROK_TAKEOVER_GRACE_MS, 10000),
  syncBatchSize: parsePositiveInteger(process.env.SYNC_BATCH_SIZE, 100),
  syncMaxPayloadBytes: parsePositiveInteger(process.env.SYNC_MAX_PAYLOAD_BYTES, 512000),
  syncMaxBatchesPerCycle: parsePositiveInteger(process.env.SYNC_MAX_BATCHES_PER_CYCLE, 5),
  syncDedupeWindowMs: parsePositiveInteger(process.env.SYNC_DEDUPE_WINDOW_MS, 2000),
  chartJobTimeoutSeconds: parsePositiveInteger(process.env.CHART_JOB_TIMEOUT_SECONDS, 60),
  autoBootstrapOnJoin: parseBoolean(process.env.AUTO_BOOTSTRAP_ON_JOIN, false),
  autoJoinHostOnStartup: parseBoolean(process.env.AUTO_JOIN_HOST_ON_STARTUP, false),
  autoJoinHostUrl: (process.env.AUTO_JOIN_HOST_URL || '').trim()
};
