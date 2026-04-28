const env = require('../config/env');
const ngrokService = require('./ngrok.service');

const ROLE_HOST = 'HOST';
const ROLE_STANDBY = 'STANDBY';

const state = {
  serverName: env.serverName,
  serverUrl: env.serverUrl,
  role: env.initialRole,
  publicUrl: null,
  peers: env.peers.map((peerUrl) => ({
    serverName: peerUrl,
    serverUrl: peerUrl,
    online: false,
    role: ROLE_STANDBY,
    publicUrl: null,
    lastSeen: null
  }))
};

const withTimeout = async (url, options = {}, timeoutMs = env.heartbeatIntervalMs) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {})
      },
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return await response.json();
  } finally {
    clearTimeout(timer);
  }
};

class ClusterService {
  getLocalState() {
    return {
      serverName: state.serverName,
      serverUrl: state.serverUrl,
      role: state.role,
      publicUrl: state.publicUrl,
      peers: state.peers
    };
  }

  getKnownServers() {
    const local = {
      serverName: state.serverName,
      serverUrl: state.serverUrl,
      online: true,
      role: state.role,
      publicUrl: state.publicUrl,
      isHostingPublicFrontend: state.role === ROLE_HOST && Boolean(state.publicUrl),
      lastSeen: new Date().toISOString()
    };

    return [local, ...state.peers.map((peer) => ({
      ...peer,
      isHostingPublicFrontend: peer.role === ROLE_HOST && Boolean(peer.publicUrl)
    }))];
  }

  updatePeer(peerUrl, data) {
    const peer = state.peers.find((p) => p.serverUrl === peerUrl);
    if (!peer) return;

    peer.serverName = data.serverName || peer.serverName;
    peer.serverUrl = data.serverUrl || peer.serverUrl;
    peer.role = data.role || ROLE_STANDBY;
    peer.publicUrl = data.publicUrl || null;
    peer.online = true;
    peer.lastSeen = data.time || new Date().toISOString();
  }

  markPeerOffline(peerUrl) {
    const peer = state.peers.find((p) => p.serverUrl === peerUrl);
    if (!peer) return;

    const last = peer.lastSeen ? new Date(peer.lastSeen).getTime() : 0;
    if (!last || Date.now() - last >= env.heartbeatTimeoutMs) {
      peer.online = false;
      peer.role = ROLE_STANDBY;
      peer.publicUrl = null;
    }
  }

  async refreshPeers() {
    await Promise.all(
      env.peers.map(async (peerUrl) => {
        try {
          const health = await withTimeout(`${peerUrl}/internal/health`, {}, env.heartbeatIntervalMs);
          this.updatePeer(peerUrl, health);
        } catch (_error) {
          this.markPeerOffline(peerUrl);
        }
      })
    );
  }

  findActiveHost() {
    if (state.role === ROLE_HOST) {
      return {
        serverName: state.serverName,
        serverUrl: state.serverUrl,
        role: state.role,
        publicUrl: state.publicUrl,
        online: true
      };
    }

    return state.peers.find((peer) => peer.online && peer.role === ROLE_HOST) || null;
  }

  async setRole(role) {
    if (role === ROLE_HOST) {
      state.role = ROLE_HOST;
      state.publicUrl = await ngrokService.startTunnel(env.port);
    } else {
      state.role = ROLE_STANDBY;
      await ngrokService.stopTunnel();
      state.publicUrl = null;
    }

    return this.getLocalState();
  }

  async makeLocalHost() {
    await this.setRole(ROLE_HOST);
    return this.getLocalState();
  }

  async makeLocalStandby() {
    await this.setRole(ROLE_STANDBY);
    return this.getLocalState();
  }

  async tellPeer(peerUrl, path) {
    return withTimeout(`${peerUrl}${path}`, { method: 'POST' }, env.heartbeatIntervalMs);
  }

  async switchHost(targetUrl) {
    const allServers = this.getKnownServers().filter((s) => s.serverUrl === state.serverUrl || s.online);
    const target = allServers.find((server) => server.serverUrl === targetUrl);

    if (!target) {
      throw new Error('Servidor alvo offline ou inexistente.');
    }

    const standByTargets = allServers.filter((server) => server.serverUrl !== targetUrl);
    await Promise.all(
      standByTargets.map(async (server) => {
        if (server.serverUrl === state.serverUrl) {
          await this.makeLocalStandby();
          return;
        }

        try {
          await this.tellPeer(server.serverUrl, '/internal/become-standby');
        } catch (error) {
          console.error(`[cluster] falha ao rebaixar ${server.serverUrl}:`, error.message);
        }
      })
    );

    if (targetUrl === state.serverUrl) {
      await this.makeLocalHost();
    } else {
      await this.tellPeer(targetUrl, '/internal/become-host');
      await this.makeLocalStandby();
    }

    await this.refreshPeers();
    return this.getKnownServers();
  }

  async electHostIfNeeded() {
    await this.refreshPeers();

    const existingHost = this.findActiveHost();
    if (existingHost) {
      if (existingHost.serverUrl !== state.serverUrl && state.role === ROLE_HOST) {
        await this.makeLocalStandby();
      }
      return existingHost;
    }

    const candidates = [
      { serverName: state.serverName, serverUrl: state.serverUrl, online: true },
      ...state.peers.filter((peer) => peer.online).map((peer) => ({
        serverName: peer.serverName,
        serverUrl: peer.serverUrl,
        online: true
      }))
    ].sort((a, b) => a.serverName.localeCompare(b.serverName));

    const winner = candidates[0];
    if (!winner) return null;

    if (winner.serverUrl === state.serverUrl) {
      await this.makeLocalHost();
      return this.findActiveHost();
    }

    try {
      await this.tellPeer(winner.serverUrl, '/internal/become-host');
      await this.makeLocalStandby();
      await this.refreshPeers();
      return this.findActiveHost();
    } catch (error) {
      console.error('[cluster] falha ao promover vencedor da eleição:', error.message);
      return null;
    }
  }
}

module.exports = new ClusterService();
