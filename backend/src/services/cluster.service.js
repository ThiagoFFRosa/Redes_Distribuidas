const env = require('../config/env');
const ngrokService = require('./ngrok.service');
const clusterNodesService = require('./cluster-nodes.service');

const ROLE_HOST = 'HOST';
const ROLE_STANDBY = 'STANDBY';
const HOST_TAKEOVER_ERROR = 'Falha ao assumir HOST: domínio ngrok indisponível após 3 tentativas';
const NO_PUBLIC_HOST_ERROR = 'Nenhum servidor conseguiu assumir o domínio público';
const SWITCH_WAIT_BEFORE_HOST_MS = 2000;

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

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(data.error || data.message || `HTTP ${response.status}`);
      error.payload = data;
      throw error;
    }

    return data;
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
    if (!env.enableNgrok) {
      state.role = ROLE_HOST;
      state.publicUrl = null;
      return {
        ok: true,
        serverName: state.serverName,
        role: state.role,
        publicUrl: state.publicUrl
      };
    }

    try {
      const publicUrl = await ngrokService.startTunnelWithRetry(env.port);
      state.role = ROLE_HOST;
      state.publicUrl = publicUrl;
      return {
        ok: true,
        serverName: state.serverName,
        role: state.role,
        publicUrl: state.publicUrl
      };
    } catch (error) {
      state.role = ROLE_STANDBY;
      state.publicUrl = null;
      console.error(`[cluster] ${HOST_TAKEOVER_ERROR}`);
      return {
        ok: false,
        serverName: state.serverName,
        role: state.role,
        publicUrl: null,
        error: error?.message || 'Falha ao iniciar ngrok após 3 tentativas'
      };
    }
  }

  async makeLocalStandby() {
    state.role = ROLE_STANDBY;
    await ngrokService.stopTunnel();
    state.publicUrl = null;
    return {
      ok: true,
      serverName: state.serverName,
      role: state.role,
      publicUrl: state.publicUrl
    };
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

    await new Promise((resolve) => setTimeout(resolve, SWITCH_WAIT_BEFORE_HOST_MS));

    let targetResult = null;
    if (targetUrl === state.serverUrl) {
      targetResult = await this.makeLocalHost();
    } else {
      try {
        targetResult = await this.tellPeer(targetUrl, '/internal/become-host');
      } catch (error) {
        targetResult = {
          ok: false,
          error: error?.message || 'Falha ao iniciar ngrok após 3 tentativas'
        };
      }
      await this.makeLocalStandby();
    }

    if (!targetResult?.ok) {
      console.error(`[cluster] falha ao promover ${targetUrl} para HOST: ${targetResult?.error || 'erro desconhecido'}`);
      const fallbackHost = await this.electHostIfNeeded(new Set([targetUrl]));
      const status = await this.getKnownServersWithStatus();
      return {
        servers: status,
        switched: false,
        message: fallbackHost ? HOST_TAKEOVER_ERROR : NO_PUBLIC_HOST_ERROR
      };
    }

    return {
      servers: await this.getKnownServersWithStatus(),
      switched: true
    };
  }

  pickElectionWinner(servers, excludedUrls = new Set()) {
    const online = servers
      .filter((server) => server.online && !excludedUrls.has(server.serverUrl))
      .map((server) => ({ serverName: server.serverName, serverUrl: server.serverUrl }))
      .sort((a, b) => a.serverName.localeCompare(b.serverName));

    return online[0] || null;
  }

  async electHostIfNeeded(excludedUrls = new Set()) {
    const firstSnapshot = await this.getKnownServersWithStatus();
    const firstHost = this.findActiveHost(firstSnapshot);

    if (firstHost) {
      if (firstHost.serverUrl !== state.serverUrl && state.role === ROLE_HOST) {
        await this.makeLocalStandby();
      }
      return firstHost;
    }

    const winnerFirstPass = this.pickElectionWinner(firstSnapshot, excludedUrls);
    if (!winnerFirstPass) return null;

    const secondSnapshot = await this.getKnownServersWithStatus();
    const secondHost = this.findActiveHost(secondSnapshot);
    if (secondHost) {
      if (secondHost.serverUrl !== state.serverUrl && state.role === ROLE_HOST) {
        await this.makeLocalStandby();
      }
      return secondHost;
    }

    const winnerSecondPass = this.pickElectionWinner(secondSnapshot, excludedUrls);
    if (!winnerSecondPass || winnerSecondPass.serverUrl !== winnerFirstPass.serverUrl) {
      return null;
    }

    if (winnerSecondPass.serverUrl === state.serverUrl) {
      const localHostResult = await this.makeLocalHost();
      if (localHostResult.ok) {
        return this.findActiveHost(await this.getKnownServersWithStatus());
      }

      const nextExcluded = new Set(excludedUrls);
      nextExcluded.add(state.serverUrl);
      return this.electHostIfNeeded(nextExcluded);
    }

    if (state.role === ROLE_HOST) {
      await this.makeLocalStandby();
    }

    try {
      const remoteResult = await this.tellPeer(winnerSecondPass.serverUrl, '/internal/become-host');
      if (remoteResult?.ok) {
        return winnerSecondPass;
      }
    } catch (error) {
      console.error(`[cluster] eleição falhou ao promover ${winnerSecondPass.serverUrl}:`, error.message);
    }

    const nextExcluded = new Set(excludedUrls);
    nextExcluded.add(winnerSecondPass.serverUrl);
    return this.electHostIfNeeded(nextExcluded);
  }
}

module.exports = new ClusterService();
