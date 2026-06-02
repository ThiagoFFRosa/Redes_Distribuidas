const repo = require('./cluster-node.repository');
const coordinator = require('./sync-coordinator.service');
const logger = require('../utils/logger');
const { resolveNodeBaseUrl } = require('../utils/sync-targets');

let timer = null;
let running = false;
const RETRY_COOLDOWN_MS = 30000;

const canRetryNode = async (node) => {
  const cursor = node.node_uuid ? await coordinator.getCursor(node.node_uuid) : null;
  if (!cursor?.last_error || !cursor.last_sync_at) return true;
  const lastAttempt = new Date(cursor.last_sync_at).getTime();
  if (Number.isNaN(lastAttempt)) return true;
  return Date.now() - lastAttempt >= RETRY_COOLDOWN_MS;
};

const runCycle = async () => {
  if (running) return { ok: true, skipped: true };
  running = true;
  logger.debug('[sync] iniciando ciclo');
  try {
    const self = await repo.getSelfNode();
    if (!self?.node_uuid) return { ok: true, message: 'self não configurado', nodes: [] };
    const nodes = (await repo.getExternalNodes()).filter((node) => node.node_uuid && node.status !== 'OFFLINE' && (node.public_url || node.tailscale_ip));
    if (!nodes.length) {
      logger.debug('[sync] nenhum nó externo elegível para sincronizar');
      return { ok: true, nodes: [] };
    }
    const results = [];
    for (const node of nodes) {
      const resolved = resolveNodeBaseUrl(node, self);
      const base_url = resolved.baseUrl;
      const target_url = resolved.targetUrl;
      try {
        if (!base_url) throw new Error('URL de destino não configurada');
        if (resolved.matchedSelfUrl) logger.warn(`[sync] AVISO: destino calculado para ${node.node_name} parece ser o próprio servidor; usando fallback: ${base_url}`);
        logger.debug(`[sync] node remoto ${node.node_name}: public_url=${node.public_url || '-'} tailscale_ip=${node.tailscale_ip || '-'} port=${node.port || 3000} target=${target_url}`);
        logger.debug(`[sync] target ${node.node_name} = ${target_url}`);
        if (!(await canRetryNode(node))) {
          results.push({ node_uuid: node.node_uuid, node_name: node.node_name, target_url, skipped: true, reason: 'retry_cooldown' });
          continue;
        }
        logger.debug(`[sync] puxando eventos de ${node.node_name}`);
        const pulled = await coordinator.pullFromNode({ node_uuid: node.node_uuid, base_url });
        if (pulled.applied || pulled.failed || pulled.deferred) logger.info(`[sync] pull ${node.node_name}: received=${pulled.received || pulled.pulled || 0} applied=${pulled.applied || 0} skipped=${pulled.skipped || 0} failed=${pulled.failed || 0}`);
        const pushed = await coordinator.pushToNode({ node_uuid: node.node_uuid, base_url });
        await coordinator.updateCursor(node.node_uuid, null, null, { nodeName: node.node_name });
        results.push({
          node_uuid: node.node_uuid,
          node_name: node.node_name,
          target_url,
          pulled: pulled.pulled || 0,
          sent: pushed.sent || 0,
          attempted: pushed.attempted || pushed.sent || 0,
          applied_by_remote: pushed.applied || 0,
          skipped_by_remote: pushed.skipped || 0,
          failed: pushed.failed || 0,
          pending: pushed.pending || 0,
          errors: pushed.errors || []
        });
      } catch (error) {
        logger.error(`[sync] erro ao sincronizar com ${node.node_name}: ${error.message}`);
        await coordinator.updateCursor(node.node_uuid, null, error.message, { nodeName: node.node_name }).catch(() => {});
        results.push({ node_uuid: node.node_uuid, node_name: node.node_name, target_url, error: error.message });
      }
    }
    return { ok: true, nodes: results };
  } finally {
    running = false;
  }
};

const start = (intervalMs = 30000) => {
  if (timer) return;
  timer = setInterval(() => runCycle().catch((error) => logger.error('[sync] ciclo falhou:', error.message)), intervalMs);
  timer.unref?.();
};

module.exports = { start, runCycle };
