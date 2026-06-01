const env = require('../config/env');
const repo = require('./cluster-node.repository');

const HEALTH_PATH = '/health';

const trimStr = (v) => (typeof v === 'string' ? v.trim() : '');

class ClusterHealthService {
  buildHealthUrl(node) {
    if (trimStr(node.public_url)) return `${trimStr(node.public_url).replace(/\/$/, '')}${HEALTH_PATH}`;
    return `http://${trimStr(node.tailscale_ip)}:${node.port || env.port}${HEALTH_PATH}`;
  }

  async checkNode(node) {
    const now = new Date();
    if (node.is_self) {
      return repo.updateNodeHealthStatus(node.id, { status: 'ONLINE', last_healthcheck_at: now, last_heartbeat_at: now, healthcheck_error: null }, { skipSyncEvent: true, reason: 'healthcheck' });
    }

    try {
      const response = await fetch(this.buildHealthUrl(node));
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return repo.updateNodeHealthStatus(node.id, { status: 'ONLINE', last_healthcheck_at: now, last_heartbeat_at: now, healthcheck_error: null }, { skipSyncEvent: true, reason: 'healthcheck' });
    } catch (error) {
      return repo.updateNodeHealthStatus(node.id, { status: 'OFFLINE', last_healthcheck_at: now, healthcheck_error: String(error.message || 'erro').slice(0, 255) }, { skipSyncEvent: true, reason: 'healthcheck' });
    }
  }

  async checkAllNodes() {
    const nodes = await repo.getAllNodes();
    const results = await Promise.all(nodes.map(async (node) => {
      const updated = await this.checkNode(node);
      return { id: node.id, status: updated.status, node: updated };
    }));
    return {
      ok: true,
      checked: results.length,
      online: results.filter((r) => r.status === 'ONLINE').length,
      offline: results.filter((r) => r.status === 'OFFLINE').length,
      results
    };
  }
}

module.exports = new ClusterHealthService();
