const env = require('../config/env');
const repo = require('./cluster-node.repository');
const ngrokCoordinator = require('./ngrok-coordinator.service');
const healthService = require('./cluster-health.service');

class ClusterStartupService {
  async initialize() {
    console.log('[cluster-db] carregando configuração local do banco...');
    const selfNode = await repo.getSelfNode();

    if (!selfNode) {
      console.log('[cluster] Servidor local ainda não configurado no banco. Acesse o painel e configure este servidor.');
      return { startedNgrok: false, selfNode: null };
    }

    console.log(`[cluster-db] servidor local: ${selfNode.node_name} / ${selfNode.tailscale_ip} / ${selfNode.role}`);
    await repo.updateStatus(selfNode.id, 'ONLINE', null, { skipSyncEvent: true, reason: 'startup-health' });

    console.log('[cluster-db] consultando hosts externos no banco...');
    const onlineHosts = await repo.getOnlineHosts();
    const externalHosts = onlineHosts.filter((n) => !n.is_self);

    let activeExternalHost = null;
    for (const node of externalHosts) {
      const checked = await healthService.checkNode(node);
      if (checked.status === 'ONLINE') { activeExternalHost = checked; break; }
    }

    if (activeExternalHost) {
      console.log(`[cluster-db] HOST externo online encontrado: ${activeExternalHost.node_name}`);
      console.log('[ngrok] não iniciado neste servidor');
      return { startedNgrok: false, selfNode };
    }

    console.log('[cluster-db] nenhum HOST externo online encontrado');
    if (selfNode.role === 'HOST') {
      console.log('[cluster-db] servidor local configurado como HOST');
      console.log('[ngrok] iniciando túnel...');
      const status = await ngrokCoordinator.performCheckCycle();
      return { startedNgrok: Boolean(status?.ngrok_online && status?.owner_node_uuid === selfNode.node_uuid), selfNode };
    }

    if (selfNode.role === 'UNKNOWN') {
      console.log('[cluster-db] role local UNKNOWN; ngrok não será iniciado');
      console.log('[cluster-db] Função do servidor local está UNKNOWN. Configure como HOST para iniciar ngrok.');
    } else {
      console.log('[cluster-db] servidor local em STANDBY; ngrok não será iniciado');
    }
    return { startedNgrok: false, selfNode };
  }
}

module.exports = new ClusterStartupService();
