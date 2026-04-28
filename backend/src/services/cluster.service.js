const env = require('../config/env');
const ngrokService = require('./ngrok.service');
const clusterNodesService = require('./cluster-nodes.service');

const ROLE_HOST = 'HOST';
const ROLE_STANDBY = 'STANDBY';

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

const state = {
  serverName: env.serverName,
  serverUrl: env.serverUrl,
  role: env.initialRole,
  publicUrl: null,
  nodes: [],
  peerRuntime: {}
};

class ClusterService {
  constructor() {
    this.loadNodes();
  }

  getInternalHeaders() {
    return {
      'x-cluster-key': env.clusterKey
    };
  }

  loadNodes() {
    const loadedNodes = clusterNodesService.loadOrCreateNodes();

    if (env.peers.length > 0) {
      const fallbackPeers = env.peers.map((peerUrl) => ({
        serverName: peerUrl,
        serverUrl: peerUrl,
        addedAt: new Date().toISOString(),
        lastSeen: null
      }));

      state.nodes = clusterNodesService.mergeNodes(loadedNodes, fallbackPeers);
      clusterNodesService.saveNodes(state.nodes);
    } else {
      state.nodes = loadedNodes;
    }
  }

  saveNodes() {
    state.nodes = clusterNodesService.saveNodes(state.nodes);
  }

  getNodes() {
    return state.nodes.map((node) => ({ ...node }));
  }

  getKnownServers() {
    const localNode = state.nodes.find((node) => node.serverUrl === state.serverUrl);
    const local = {
      serverName: state.serverName,
      serverUrl: state.serverUrl,
      addedAt: localNode?.addedAt || new Date().toISOString(),
      lastSeen: localNode?.lastSeen || new Date().toISOString(),
      online: true,
      role: state.role,
      publicUrl: state.publicUrl,
      isHostingPublicFrontend: state.role === ROLE_HOST && Boolean(state.publicUrl)
    };

    const peers = state.nodes
      .filter((node) => node.serverUrl !== state.serverUrl)
      .map((node) => {
        const runtime = state.peerRuntime[node.serverUrl] || {};
        return {
          serverName: node.serverName,
          serverUrl: node.serverUrl,
          addedAt: node.addedAt,
          lastSeen: runtime.lastSeen || node.lastSeen || null,
          online: runtime.online || false,
          role: runtime.role || ROLE_STANDBY,
          publicUrl: runtime.publicUrl || null,
          isHostingPublicFrontend: runtime.role === ROLE_HOST && Boolean(runtime.publicUrl)
        };
      });

    return [local, ...peers];
  }

  getLocalState() {
    return {
      serverName: state.serverName,
      serverUrl: state.serverUrl,
      role: state.role,
      publicUrl: state.publicUrl,
      peers: this.getKnownServers().filter((server) => server.serverUrl !== state.serverUrl)
    };
  }

  upsertNode(node) {
    const merged = clusterNodesService.mergeNodes(state.nodes, [node]);
    state.nodes = merged;
    this.saveNodes();
    return this.getNodes().find((item) => item.serverUrl === node.serverUrl || item.serverName === node.serverName) || null;
  }

  mergeAndReplaceNodes(nodes) {
    state.nodes = clusterNodesService.mergeNodes(state.nodes, nodes || []);
    this.saveNodes();
    return this.getNodes();
  }

  async pingNode(node) {
    try {
      const handshake = await withTimeout(
        `${node.serverUrl}/internal/handshake`,
        { headers: this.getInternalHeaders() },
        env.heartbeatIntervalMs
      );

      const lastSeen = new Date().toISOString();
      this.upsertNode({
        serverName: handshake.serverName,
        serverUrl: handshake.serverUrl,
        addedAt: node.addedAt,
        lastSeen
      });

      return {
        serverName: handshake.serverName,
        serverUrl: handshake.serverUrl,
        addedAt: node.addedAt,
        lastSeen,
        online: true,
        role: handshake.role || ROLE_STANDBY,
        publicUrl: handshake.publicUrl || null,
        isHostingPublicFrontend: handshake.role === ROLE_HOST && Boolean(handshake.publicUrl)
      };
    } catch (_error) {
      const last = node.lastSeen ? new Date(node.lastSeen).getTime() : 0;
      const stillOnline = Boolean(last && Date.now() - last < env.heartbeatTimeoutMs);

      return {
        serverName: node.serverName,
        serverUrl: node.serverUrl,
        addedAt: node.addedAt,
        lastSeen: node.lastSeen || null,
        online: stillOnline,
        role: ROLE_STANDBY,
        publicUrl: null,
        isHostingPublicFrontend: false
      };
    }
  }

  async refreshPeers() {
    const peers = state.nodes.filter((node) => node.serverUrl !== state.serverUrl);
    const peerStates = await Promise.all(peers.map((node) => this.pingNode(node)));

    const nextRuntime = {};
    for (const peer of peerStates) {
      nextRuntime[peer.serverUrl] = {
        online: peer.online,
        role: peer.role,
        publicUrl: peer.publicUrl,
        lastSeen: peer.lastSeen
      };
    }

    state.peerRuntime = nextRuntime;
    return peerStates;
  }

  async getKnownServersWithStatus() {
    await this.refreshPeers();
    return this.getKnownServers();
  }

  findActiveHost(servers = []) {
    const known = servers.length ? servers : this.getKnownServers();
    return known.find((server) => server.online && server.role === ROLE_HOST) || null;
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

  async tellPeer(peerUrl, path, body = undefined) {
    return withTimeout(
      `${peerUrl}${path}`,
      {
        method: 'POST',
        headers: this.getInternalHeaders(),
        body: body ? JSON.stringify(body) : undefined
      },
      env.heartbeatIntervalMs
    );
  }

  async switchHost(targetUrl) {
    const allServers = (await this.getKnownServersWithStatus()).filter((server) => server.serverUrl === state.serverUrl || server.online);
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

    return this.getKnownServersWithStatus();
  }

  pickElectionWinner(servers) {
    const online = servers
      .filter((server) => server.online)
      .map((server) => ({ serverName: server.serverName, serverUrl: server.serverUrl }))
      .sort((a, b) => a.serverName.localeCompare(b.serverName));

    return online[0] || null;
  }

  async electHostIfNeeded() {
    const firstSnapshot = await this.getKnownServersWithStatus();
    const firstHost = this.findActiveHost(firstSnapshot);

    if (firstHost) {
      if (firstHost.serverUrl !== state.serverUrl && state.role === ROLE_HOST) {
        await this.makeLocalStandby();
      }
      return firstHost;
    }

    const winnerFirstPass = this.pickElectionWinner(firstSnapshot);
    if (!winnerFirstPass) return null;

    const secondSnapshot = await this.getKnownServersWithStatus();
    const secondHost = this.findActiveHost(secondSnapshot);
    if (secondHost) {
      if (secondHost.serverUrl !== state.serverUrl && state.role === ROLE_HOST) {
        await this.makeLocalStandby();
      }
      return secondHost;
    }

    const winnerSecondPass = this.pickElectionWinner(secondSnapshot);
    if (!winnerSecondPass || winnerSecondPass.serverUrl !== winnerFirstPass.serverUrl) {
      return null;
    }

    if (winnerSecondPass.serverUrl === state.serverUrl) {
      await this.makeLocalHost();
      return this.findActiveHost(await this.getKnownServersWithStatus());
    }

    if (state.role === ROLE_HOST) {
      await this.makeLocalStandby();
    }

    return winnerSecondPass;
  }
}

module.exports = new ClusterService();
