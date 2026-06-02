const env = require('../config/env');
const clusterNodeRepo = require('./cluster-node.repository');
const clusterHealthService = require('./cluster-health.service');
const logger = require('../utils/logger');

const isSchemaNotMigratedError = (error) => ['ER_BAD_FIELD_ERROR', 'ER_NO_SUCH_TABLE'].includes(error?.code);

class HeartbeatService {
  constructor() { this.timer = null; this.running = false; }

  async tick() {
    if (this.running) return;
    this.running = true;
    try {
      const selfNode = await clusterNodeRepo.getSelfNode();
      if (!selfNode) {
        logger.debug('[cluster-health] Servidor atual ainda não configurado no banco.');
      } else {
        await clusterNodeRepo.updateStatus(selfNode.id, 'ONLINE', null, { skipSyncEvent: true, reason: 'heartbeat' });
        await clusterHealthService.checkAllNodes();
      }
    } catch (error) {
      if (isSchemaNotMigratedError(error)) logger.error('[heartbeat] banco ainda não está migrado. Rode npm run migrate.');
      else logger.error('[heartbeat] erro no ciclo:', error.message);
    } finally { this.running = false; }
  }

  start() { if (this.timer) return; this.tick(); this.timer = setInterval(() => this.tick(), env.heartbeatIntervalMs); }
  stop() { if (this.timer) { clearInterval(this.timer); this.timer = null; } }
}

module.exports = new HeartbeatService();
