const env = require('../config/env');
const ngrokService = require('./ngrok.service');
const clusterNodesService = require('./cluster-nodes.service');

const ROLE_HOST = 'HOST';
const ROLE_STANDBY = 'STANDBY';
const HOST_TAKEOVER_ERROR = 'Falha ao assumir HOST: domínio ngrok indisponível após 3 tentativas';
const NO_PUBLIC_HOST_ERROR = 'Nenhum servidor conseguiu assumir o domínio público';
const SWITCH_WAIT_BEFORE_HOST_MS = 2000;
const SWITCH_PROMOTE_TIMEOUT_MS = 20000;
const SWITCH_DOMAIN_RELEASE_GRACE_MS = 1200;
const CLUSTER_MODE_NORMAL = 'NORMAL';
const CLUSTER_MODE_SWITCHING = 'SWITCHING';
const CLUSTER_MODE_SWITCH_TARGET = 'SWITCH_TARGET';

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
  switchTarget: null,
  switchOldHost: null,
  lastDomainBusyAt: 0,
  localPromotionRequested: false,
  autoFallbackBlocked: false,
  allowManualPromotion: false
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

  setClusterMode(clusterMode, switchTarget = null, oldHostUrl = null) {
    state.clusterMode = clusterMode;
    state.switchTarget = clusterMode === CLUSTER_MODE_SWITCHING ? switchTarget : null;
    state.switchOldHost = clusterMode === CLUSTER_MODE_SWITCHING ? oldHostUrl : null;

    if (clusterMode === CLUSTER_MODE_SWITCHING) {
      if (switchTarget === state.serverUrl) {
        state.autoFallbackBlocked = false;
        state.allowManualPromotion = true;
        console.log(`[cluster] SWITCHING recebido: target=${switchTarget}, self=${state.serverUrl}, modo=${CLUSTER_MODE_SWITCH_TARGET}`);
      } else {
        state.autoFallbackBlocked = true;
        state.allowManualPromotion = false;
        console.log(`[cluster] SWITCHING recebido: target=${switchTarget}, self=${state.serverUrl}, modo=${CLUSTER_MODE_SWITCHING}`);
      }
      return;
    }

    state.autoFallbackBlocked = false;
    state.allowManualPromotion = false;
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
    return known.find((server) => server.online && server.role === ROLE_HOST && Boolean(server.publicUrl)) || null;
  }


  async checkPublicUrlAvailable() {
    if (!env.ngrokDomain) return true;

    const publicHealthUrl = `https://${env.ngrokDomain}/internal/public-health`;
    const timeoutMs = env.publicUrlCheckTimeoutMs;

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      let response;
      try {
        response = await fetch(publicHealthUrl, {
          method: 'GET',
          signal: controller.signal,
          redirect: 'manual'
        });
      } finally {
        clearTimeout(timer);
      }

      if (!response || response.status !== 200) {
        console.log('[cluster] Domínio ngrok responde página de erro/offline. Considerando domínio livre.');
        return true;
      }

      const contentType = (response.headers.get('content-type') || '').toLowerCase();
      if (!contentType.includes('application/json')) {
        console.log('[cluster] Domínio ngrok responde página de erro/offline. Considerando domínio livre.');
        return true;
      }

      let data;
      try {
        data = await response.json();
      } catch (_error) {
        console.log('[cluster] Domínio ngrok responde página de erro/offline. Considerando domínio livre.');
        return true;
      }

      if (data?.ok === true && data?.app === 'cluster-mvp') {
        console.log('[cluster] Domínio público já está servindo o cluster. Considerando em uso.');
        return false;
      }

      console.log('[cluster] Domínio ngrok não pertence ao cluster-mvp. Considerando domínio livre.');
      return true;
    } catch (_error) {
      console.log('[cluster] Domínio ngrok responde página de erro/offline. Considerando domínio livre.');
      return true;
    }
  }

  async checkActivePublicHost(publicUrl) {
    if (!publicUrl) return false;

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), env.publicUrlCheckTimeoutMs);
      let response;
      try {
        response = await fetch(publicUrl, { method: 'HEAD', signal: controller.signal, redirect: 'manual' });
      } finally {
        clearTimeout(timer);
      }
      return Boolean(response);
    } catch (_error) {
      return false;
    }
  }

  async findValidActiveHost(servers = []) {
    const known = servers.length ? servers : this.getKnownServers();
    const local = this.getLocalState();

    for (const server of known) {
      if (server.role !== ROLE_HOST || !server.publicUrl) continue;

      if (server.online) return server;
      const canValidatePublic = server.serverUrl !== local.serverUrl;
      if (canValidatePublic && await this.checkActivePublicHost(server.publicUrl)) {
        return { ...server, online: true };
      }
    }

    return null;
  }

  requestLocalPromotion() {
    state.localPromotionRequested = true;
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

  async makeLocalHost(options = {}) {
    state.localPromotionRequested = Boolean(state.localPromotionRequested || state.role === ROLE_HOST);
    if (!options.manualPromotion && !state.localPromotionRequested && state.role !== ROLE_HOST) {
      return { ok: false, serverName: state.serverName, role: state.role, publicUrl: state.publicUrl, error: 'Promoção local não autorizada neste ciclo' };
    }

    if (!options?.manualPromotion && this.isSwitching() && state.switchTarget !== state.serverUrl) {
      console.warn('[cluster] Fallback bloqueado durante SWITCHING');
      state.localPromotionRequested = false;
      state.allowManualPromotion = false;
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
      state.localPromotionRequested = false;
      state.allowManualPromotion = false;
      return {
        ok: true,
        serverName: state.serverName,
        role: state.role,
        publicUrl: state.publicUrl
      };
    }

    try {
      if (options.manualPromotion) {
        console.log('[cluster] promoção manual recebida, ignorando fallback automático');
        console.log('[cluster] aguardando domínio liberar...');
        await new Promise((resolve) => setTimeout(resolve, SWITCH_DOMAIN_RELEASE_GRACE_MS));
      }

      const urlAvailable = await this.checkPublicUrlAvailable();
      if (!urlAvailable && !options.manualPromotion) {
        state.role = ROLE_STANDBY;
        state.publicUrl = null;
        state.lastDomainBusyAt = Date.now();
        return { ok: false, serverName: state.serverName, role: state.role, publicUrl: null, error: 'Domínio público em uso. Promoção cancelada.' };
      }

      if (!urlAvailable && options.manualPromotion) {
        console.log('[cluster] domínio ainda ocupado durante promoção manual, tentando iniciar ngrok com retry');
      } else {
        console.log('[cluster] Domínio público livre');
      }
      const publicUrl = await ngrokService.startTunnelWithRetry(env.port);
      state.role = ROLE_HOST;
      state.publicUrl = publicUrl;
      state.localPromotionRequested = false;
      state.allowManualPromotion = false;
      if (options.manualPromotion) {
        console.log('[cluster] target iniciou ngrok com sucesso');
      }
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
      state.localPromotionRequested = false;
      state.allowManualPromotion = false;
      return {
        ok: false,
        serverName: state.serverName,
        role: state.role,
        publicUrl: null,
        error: error?.message || 'Falha ao iniciar ngrok após 3 tentativas'
      };
    }
  }


  async promoteToHostManually(payload = {}) {
    const targetUrl = payload.targetUrl;
    if (!targetUrl || targetUrl !== state.serverUrl) {
      return { ok: false, error: `Target inválido para promoção manual: recebido=${targetUrl || 'vazio'} self=${state.serverUrl}` };
    }

    state.allowManualPromotion = true;
    return this.makeLocalHost({ manualPromotion: true });
  }

  async makeLocalStandby() {
    state.localPromotionRequested = false;
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

  async tellPeer(peerUrl, path, body = undefined, timeoutMs = env.heartbeatIntervalMs) {
    return withTimeout(
      `${peerUrl}${path}`,
      {
        method: 'POST',
        headers: this.getInternalHeaders(),
        body: body ? JSON.stringify(body) : undefined
      },
      timeoutMs
    );
  }

  async broadcastClusterMode(clusterMode, switchTarget = null, oldHostUrl = null) {
    const peers = state.nodes.filter((node) => node.serverUrl !== state.serverUrl);
    await Promise.all(
      peers.map(async (peer) => {
        try {
          await this.tellPeer(peer.serverUrl, '/internal/switch-mode', { clusterMode, switchTarget, oldHostUrl });
        } catch (error) {
          console.error(`[cluster] falha ao propagar modo ${clusterMode} para ${peer.serverUrl}: ${error.name || 'Error'} ${error.message}`);
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
    this.setClusterMode(CLUSTER_MODE_SWITCHING, targetUrl, state.serverUrl);
    state.autoFallbackBlocked = true;
    await this.broadcastClusterMode(CLUSTER_MODE_SWITCHING, targetUrl, state.serverUrl);
    try {
      await this.makeLocalStandby();

      const standByTargets = allServers.filter((server) => server.serverUrl !== targetUrl);
      await Promise.all(
        standByTargets.map(async (server) => {
          if (server.serverUrl === state.serverUrl) {
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
      targetResult = await this.promoteToHostManually({ targetUrl, oldHostUrl: state.serverUrl });
    } else {
      try {
        targetResult = await this.tellPeer(targetUrl, '/internal/promote', { reason: 'manual-switch', oldHostUrl: state.serverUrl, targetUrl }, SWITCH_PROMOTE_TIMEOUT_MS);
      } catch (error) {
        targetResult = {
          ok: false,
          error: error?.message || 'Falha ao iniciar ngrok após 3 tentativas'
        };
      }
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

        this.requestLocalPromotion();
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
    if (this.isSwitching() || state.autoFallbackBlocked) {
      console.log('[cluster] Fallback bloqueado durante SWITCHING');
      return null;
    }

    const firstSnapshot = await this.getKnownServersWithStatus();
    const firstHost = await this.findValidActiveHost(firstSnapshot);

    if (firstHost) {
      if (firstHost.serverUrl !== state.serverUrl && state.role === ROLE_HOST) {
        await this.makeLocalStandby();
      }
      return firstHost;
    }

    const domainCooldownActive = state.lastDomainBusyAt && (Date.now() - state.lastDomainBusyAt) < env.publicUrlCheckIntervalMs;
    if (domainCooldownActive) {
      return null;
    }

    const publicUrlAvailable = await this.checkPublicUrlAvailable();
    if (!publicUrlAvailable) {
      state.lastDomainBusyAt = Date.now();
      console.log('[cluster] Domínio público já está em uso. Mantendo STANDBY.');
      return null;
    }

    const winnerFirstPass = this.pickElectionWinner(firstSnapshot, excludedUrls);
    if (!winnerFirstPass) return null;

    const secondSnapshot = await this.getKnownServersWithStatus();
    const secondHost = await this.findValidActiveHost(secondSnapshot);
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
      this.requestLocalPromotion();
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
