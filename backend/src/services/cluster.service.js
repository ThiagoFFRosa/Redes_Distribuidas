const env = require('../config/env');
const ngrokService = require('./ngrok.service');
const clusterNodesService = require('./cluster-nodes.service');

const ROLE_HOST = 'HOST';
const ROLE_STANDBY = 'STANDBY';
const HOST_TAKEOVER_ERROR = 'Falha ao assumir HOST: domínio ngrok indisponível após 3 tentativas';
const NO_PUBLIC_HOST_ERROR = 'Nenhum servidor conseguiu assumir o domínio público';
const SWITCH_WAIT_BEFORE_HOST_MS = 2000;
const CLUSTER_MODE_NORMAL = 'NORMAL';
const CLUSTER_MODE_SWITCHING = 'SWITCHING';

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
  peerRuntime: {},
  clusterMode: CLUSTER_MODE_NORMAL,
  switchTarget: null
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
    state.nodes = clusterNodesService.loadNodes();
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
      clusterMode: state.clusterMode,
      switchTarget: state.switchTarget,
      peers: this.getKnownServers().filter((server) => server.serverUrl !== state.serverUrl)
    };
  }

  setClusterMode(clusterMode, switchTarget = null) {
    state.clusterMode = clusterMode;
    state.switchTarget = clusterMode === CLUSTER_MODE_SWITCHING ? switchTarget : null;
  }

  isSwitching() {
    return state.clusterMode === CLUSTER_MODE_SWITCHING;
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

  cleanupNodes() {
    state.nodes = clusterNodesService.cleanupNodes(state.nodes);
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
    if (this.isSwitching() && state.switchTarget !== state.serverUrl) {
      console.warn('[cluster] Fallback bloqueado durante SWITCHING');
      return {
        ok: false,
        serverName: state.serverName,
        role: state.role,
        publicUrl: state.publicUrl,
        error: 'Fallback bloqueado durante SWITCHING'
      };
    }

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

  async broadcastClusterMode(clusterMode, switchTarget = null) {
    const peers = state.nodes.filter((node) => node.serverUrl !== state.serverUrl);
    await Promise.all(
      peers.map(async (peer) => {
        try {
          await this.tellPeer(peer.serverUrl, '/internal/switch-mode', { clusterMode, switchTarget });
        } catch (error) {
          console.error(`[cluster] falha ao propagar modo ${clusterMode} para ${peer.serverUrl}:`, error.message);
        }
      })
    );
  }

  async switchHost(targetUrl) {
    const switchStartedAt = Date.now();
    const allServers = (await this.getKnownServersWithStatus()).filter((server) => server.serverUrl === state.serverUrl || server.online);
    const target = allServers.find((server) => server.serverUrl === targetUrl);

    if (!target) {
      throw new Error('Servidor alvo offline ou inexistente.');
    }

    console.log(`[cluster] Troca manual iniciada: target=${target.serverName || targetUrl}`);
    this.setClusterMode(CLUSTER_MODE_SWITCHING, targetUrl);
    await this.broadcastClusterMode(CLUSTER_MODE_SWITCHING, targetUrl);
    try {

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
      console.log('[cluster] Host antigo aguardando target concluir tentativas');

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
        const elapsedMs = Date.now() - switchStartedAt;
        const remainingDelayMs = Math.max(0, env.switchFallbackDelayMs - elapsedMs);
        if (remainingDelayMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, remainingDelayMs));
        }
        console.log('[cluster] Target falhou, host antigo reassumindo após delay');
        this.setClusterMode(CLUSTER_MODE_NORMAL);
        await this.broadcastClusterMode(CLUSTER_MODE_NORMAL);

        const fallbackHost = await this.makeLocalHost();
        const status = await this.getKnownServersWithStatus();
        return {
          servers: status,
          switched: false,
          message: fallbackHost?.ok ? HOST_TAKEOVER_ERROR : NO_PUBLIC_HOST_ERROR
        };
      }

      console.log('[cluster] Target assumiu com sucesso, fallback cancelado');
      this.setClusterMode(CLUSTER_MODE_NORMAL);
      await this.broadcastClusterMode(CLUSTER_MODE_NORMAL);

      return {
        servers: await this.getKnownServersWithStatus(),
        switched: true
      };
    } catch (error) {
      this.setClusterMode(CLUSTER_MODE_NORMAL);
      await this.broadcastClusterMode(CLUSTER_MODE_NORMAL);
      throw error;
    }
  }

  pickElectionWinner(servers, excludedUrls = new Set()) {
    const online = servers
      .filter((server) => server.online && !excludedUrls.has(server.serverUrl))
      .map((server) => ({ serverName: server.serverName, serverUrl: server.serverUrl }))
      .sort((a, b) => a.serverName.localeCompare(b.serverName));

    return online[0] || null;
  }

  async electHostIfNeeded(excludedUrls = new Set()) {
    if (this.isSwitching()) {
      console.log('[cluster] Fallback bloqueado durante SWITCHING');
      return null;
    }

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
