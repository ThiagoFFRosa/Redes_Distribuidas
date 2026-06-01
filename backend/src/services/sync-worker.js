const repo = require('./cluster-node.repository');
const coordinator = require('./sync-coordinator.service');

let timer = null;
let running = false;

const runCycle = async () => {
  if (running) return { ok: true, skipped: true };
  running = true;
  console.log('[sync] iniciando ciclo');
  try {
    const self = await repo.getSelfNode();
    if (!self?.node_uuid) return { ok: true, message: 'self não configurado' };
    const nodes = (await repo.getExternalNodes()).filter((node) => node.node_uuid && node.status === 'ONLINE' && (node.public_url || node.tailscale_ip));
    if (!nodes.length) {
      console.log('[sync] nenhum nó externo online para sincronizar');
      return { ok: true, nodes: 0 };
    }
    const results = [];
    for (const node of nodes) {
      const base_url = coordinator.normalizeBaseUrl(node);
      try {
        console.log(`[sync] puxando eventos de ${node.node_name}`);
        const pulled = await coordinator.pullFromNode({ node_uuid: node.node_uuid, base_url });
        console.log(`[sync] aplicados ${pulled.applied || 0} eventos de ${node.node_name}`);
        console.log(`[sync] enviando eventos para ${node.node_name}`);
        const pushed = await coordinator.pushToNode({ node_uuid: node.node_uuid, base_url });
        console.log(`[sync] enviando ${pushed.sent || 0} eventos para ${node.node_name}`);
        await coordinator.updateCursor(node.node_uuid, null, null);
        results.push({ node_uuid: node.node_uuid, pulled, pushed });
      } catch (error) {
        console.error(`[sync] erro ao sincronizar com ${node.node_name}: ${error.message}`);
        await coordinator.updateCursor(node.node_uuid, null, error.message).catch(() => {});
        results.push({ node_uuid: node.node_uuid, error: error.message });
      }
    }
    return { ok: true, results };
  } finally {
    running = false;
  }
};

const start = (intervalMs = 30000) => {
  if (timer) return;
  timer = setInterval(() => runCycle().catch((error) => console.error('[sync] ciclo falhou:', error.message)), intervalMs);
  timer.unref?.();
};

module.exports = { start, runCycle };
