const env = require('../config/env');
const clusterService = require('./cluster.service');

class HeartbeatService {
  constructor() {
    this.timer = null;
    this.running = false;
  }

  async tick() {
    if (this.running) return;
    this.running = true;

    try {
      await clusterService.refreshPeers();
      const activeHost = clusterService.findActiveHost();

      if (!activeHost) {
        await clusterService.electHostIfNeeded();
      } else {
        const local = clusterService.getLocalState();
        if (activeHost.serverUrl !== local.serverUrl && local.role === 'HOST') {
          await clusterService.makeLocalStandby();
        }
      }
    } catch (error) {
      console.error('[heartbeat] erro no ciclo:', error.message);
    } finally {
      this.running = false;
    }
  }

  start() {
    if (this.timer) return;
    this.tick();
    this.timer = setInterval(() => this.tick(), env.heartbeatIntervalMs);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

module.exports = new HeartbeatService();
