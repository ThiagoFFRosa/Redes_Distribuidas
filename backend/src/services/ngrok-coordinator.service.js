const env = require('../config/env');
const ngrokService = require('./ngrok.service');
const repo = require('./cluster-node.repository');
const healthService = require('./cluster-health.service');
const logger = require('../utils/logger');
const { getTailscaleBaseUrl, normalizeUrl } = require('../utils/sync-targets');

const ONLINE = 'ONLINE';
const OFFLINE = 'OFFLINE';
const UNKNOWN = 'UNKNOWN';
const NGROK_OFFLINE_PATTERNS = ['ERR_NGROK_3200', 'endpoint fuzzylogic.ngrok.dev is offline', 'is offline'];
let timer = null;
let running = false;
let takeoverRunning = false;
let lastStatus = { ok: true, ngrok_online: false, owner_node_uuid: null, owner_node_name: null, public_url: null, last_checked_at: null, reason: null };

const normalizeBaseUrl = (url) => String(url || '').trim().replace(/\/+$/, '');
const isJsonHealthOnline = (data) => Boolean(data && (data.ok === true || data.status === ONLINE || data.status === 'OK'));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fetchWithTimeout = async (url, timeoutMs, options = {}) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
};

const checkNgrokHealth = async (publicUrl) => {
  const baseUrl = normalizeBaseUrl(publicUrl);
  if (!baseUrl) return { status: OFFLINE, online: false, reason: 'missing_public_url' };
  try {
    const response = await fetchWithTimeout(`${baseUrl}/health`, Number(env.publicUrlCheckTimeoutMs || 3000));
    const text = await response.text();
    const lower = text.toLowerCase();
    if ([502, 503, 504].includes(response.status)) return { status: OFFLINE, online: false, reason: `HTTP_${response.status}` };
    if (NGROK_OFFLINE_PATTERNS.some((pattern) => lower.includes(pattern.toLowerCase()))) {
      return { status: OFFLINE, online: false, reason: 'ERR_NGROK_3200' };
    }
    let data = null;
    try { data = JSON.parse(text); } catch (_error) {
      return { status: OFFLINE, online: false, reason: 'non_json_health_response' };
    }
    if (response.ok && isJsonHealthOnline(data)) return { status: ONLINE, online: true, reason: 'health_ok', payload: data };
    return { status: OFFLINE, online: false, reason: `invalid_health_json_${response.status}` };
  } catch (error) {
    return { status: OFFLINE, online: false, reason: error.name === 'AbortError' ? 'timeout' : 'network_error', error: error.message };
  }
};

const sortNgrokCandidates = (nodes = []) => nodes.slice().sort((a, b) => {
  if (a.role === 'HOST' && b.role !== 'HOST') return -1;
  if (b.role === 'HOST' && a.role !== 'HOST') return 1;
  const powerDiff = Number(b.power_score ?? 5) - Number(a.power_score ?? 5);
  if (powerDiff !== 0) return powerDiff;
  return String(a.node_name || a.node_uuid || '').localeCompare(String(b.node_name || b.node_uuid || ''));
});

class NgrokCoordinatorService {
  checkNgrokHealth(publicUrl) { return checkNgrokHealth(publicUrl); }

  async getStatus() {
    const states = await repo.getRuntimeStates([
      'ngrok_owner_node_uuid',
      'ngrok_owner_node_name',
      'ngrok_public_url',
      'ngrok_status',
      'desired_ngrok_owner_node_uuid',
      'ngrok_takeover_requested_at',
      'ngrok_last_check_at'
    ]);
    const ownerUuid = states.ngrok_owner_node_uuid?.state_value || lastStatus.owner_node_uuid || null;
    const owner = ownerUuid ? await repo.findByNodeUuid(ownerUuid) : null;
    const runtimePublicUrl = states.ngrok_public_url?.state_value || null;
    return {
      ok: true,
      ngrok_online: states.ngrok_status ? states.ngrok_status.state_value === ONLINE : Boolean(lastStatus.ngrok_online),
      ngrok_status: states.ngrok_status?.state_value || (lastStatus.ngrok_online ? ONLINE : OFFLINE),
      owner_node_uuid: owner?.node_uuid || ownerUuid || null,
      owner_node_name: owner?.node_name || states.ngrok_owner_node_name?.state_value || lastStatus.owner_node_name || null,
      public_url: runtimePublicUrl || owner?.public_url || lastStatus.public_url || null,
      desired_ngrok_owner_node_uuid: states.desired_ngrok_owner_node_uuid?.state_value || null,
      takeover_requested_at: states.ngrok_takeover_requested_at?.state_value || null,
      last_checked_at: states.ngrok_last_check_at?.state_value || lastStatus.last_checked_at || null,
      reason: lastStatus.reason || null
    };
  }

  async setDesiredOwner(target, requestedBy, reason = 'manual_takeover') {
    await repo.setRuntimeState('desired_ngrok_owner_node_uuid', target.node_uuid);
    await repo.setRuntimeState('ngrok_takeover_requested_at', new Date().toISOString());
    logger.info(`[ngrok-takeover] requested target=${target.node_name} requested_by=${requestedBy?.node_name || requestedBy?.node_uuid || '-'}`);
    return { reason };
  }

  getNodeControlUrl(node) {
    return getTailscaleBaseUrl(node, node?.port || env.port) || normalizeUrl(node?.public_url);
  }

  async requestCurrentOwnerRelease(status, requestedBy, desiredOwner) {
    if (!status.owner_node_uuid || status.owner_node_uuid === desiredOwner.node_uuid) return true;
    const owner = await repo.findByNodeUuid(status.owner_node_uuid);
    const ownerUrl = this.getNodeControlUrl(owner) || status.public_url;
    if (!ownerUrl) return false;
    logger.info(`[ngrok-takeover] asking current owner ${owner?.node_name || status.owner_node_name || status.owner_node_uuid} to release`);
    try {
      const response = await fetchWithTimeout(`${normalizeBaseUrl(ownerUrl)}/api/cluster/ngrok/release`, Number(env.publicUrlCheckTimeoutMs || 3000), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Cluster-Secret': env.clusterKey || env.sessionSecret || '' },
        body: JSON.stringify({
          requested_by_node_uuid: requestedBy.node_uuid,
          requested_by_node_name: requestedBy.node_name,
          desired_owner_node_uuid: desiredOwner.node_uuid,
          reason: 'manual_takeover'
        })
      });
      if (!response.ok) throw new Error(`HTTP_${response.status}`);
      const data = await response.json().catch(() => ({}));
      return data.ok !== false;
    } catch (error) {
      logger.warn(`[ngrok-takeover] release remoto falhou/indisponível: ${error.message}`);
      return false;
    }
  }

  async broadcastClaim(owner, publicUrl, status = ONLINE) {
    const nodes = await repo.getExternalNodes();
    await Promise.all(nodes.map(async (node) => {
      const baseUrl = this.getNodeControlUrl(node);
      if (!baseUrl) return;
      try {
        await fetchWithTimeout(`${normalizeBaseUrl(baseUrl)}/api/cluster/ngrok/claim`, Number(env.publicUrlCheckTimeoutMs || 3000), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Cluster-Secret': env.clusterKey || env.sessionSecret || '' },
          body: JSON.stringify({ owner_node_uuid: owner.node_uuid, owner_node_name: owner.node_name, public_url: publicUrl, status })
        });
      } catch (error) {
        logger.warn(`[ngrok-claim] falha ao avisar ${node.node_name}: ${error.message}`);
      }
    }));
  }

  async claimLocal(publicUrl = null, options = {}) {
    const self = await repo.getSelfNode();
    if (!self) throw new Error('Servidor self não configurado.');
    const url = publicUrl || ngrokService.getPublicUrl() || (env.ngrokDomain ? `https://${env.ngrokDomain}` : null);
    await repo.markNgrokOwner(self.node_uuid, url, ONLINE, { reason: options.reason || 'ngrok-claim' });
    await repo.setRuntimeState('desired_ngrok_owner_node_uuid', '');
    await repo.setRuntimeState('ngrok_owner_node_name', self.node_name);
    await repo.setRuntimeState('ngrok_public_url', url || '');
    lastStatus = { ok: true, ngrok_online: true, owner_node_uuid: self.node_uuid, owner_node_name: self.node_name, public_url: url, last_checked_at: new Date().toISOString(), reason: options.reason || 'claim' };
    logger.info(`[ngrok] tunnel active owner=${self.node_name} url=${url}`);
    if (!options.skipBroadcast) await this.broadcastClaim(self, url, ONLINE);
    return lastStatus;
  }

  async releaseLocal(payload = {}) {
    const self = await repo.getSelfNode();
    if (!self?.node_uuid) throw new Error('Servidor self não configurado.');
    const status = await this.getStatus();
    if (status.owner_node_uuid && status.owner_node_uuid !== self.node_uuid && !ngrokService.getPublicUrl()) {
      return { ok: false, message: 'Este node não é o dono atual da ngrok.' };
    }
    logger.info(`[ngrok-release] stopping ngrok owner=${self.node_name}`);
    await ngrokService.stopTunnel();
    await repo.markNgrokOwner(self.node_uuid, null, OFFLINE, { skipSyncEvent: true, reason: 'ngrok-release' });
    await repo.setRuntimeState('ngrok_owner_node_uuid', '');
    await repo.setRuntimeState('ngrok_owner_node_name', '');
    await repo.setRuntimeState('ngrok_public_url', '');
    await repo.setRuntimeState('ngrok_status', OFFLINE);
    if (payload.desired_owner_node_uuid) {
      await repo.setRuntimeState('desired_ngrok_owner_node_uuid', payload.desired_owner_node_uuid);
      await repo.setRuntimeState('ngrok_takeover_requested_at', new Date().toISOString());
    }
    lastStatus = { ok: true, ngrok_online: false, owner_node_uuid: null, owner_node_name: null, public_url: null, last_checked_at: new Date().toISOString(), reason: payload.reason || 'release' };
    logger.info('[ngrok-release] released');
    return { ok: true, message: 'Ngrok liberada.', released_by_node_uuid: self.node_uuid };
  }

  async electWinner() {
    const nodes = await repo.getAllNodes();
    const checked = [];
    for (const node of nodes) {
      if (node.is_self) checked.push({ ...node, status: ONLINE });
      else checked.push(await healthService.checkNode(node).catch(() => ({ ...node, status: OFFLINE })));
    }
    const onlineNodes = checked.filter((node) => node.status === ONLINE);
    return sortNgrokCandidates(onlineNodes)[0] || null;
  }

  async startLocalTunnel(reason = 'manual_takeover') {
    if (takeoverRunning) return { ok: true, message: 'Takeover da ngrok já está em andamento.' };
    takeoverRunning = true;
    try {
      const self = await repo.getSelfNode();
      logger.info(`[ngrok] ${self?.node_name || 'self'} starting tunnel`);
      const url = await ngrokService.startTunnelWithRetry(env.port);
      const claimed = await this.claimLocal(url, { reason });
      return { ok: true, message: 'Ngrok assumida por esta máquina.', status: claimed };
    } finally {
      takeoverRunning = false;
    }
  }

  async assume(payload = {}) {
    const self = await repo.getSelfNode();
    if (!self?.node_uuid) throw new Error('Servidor self não configurado.');
    const targetUuid = String(payload.target_node_uuid || self.node_uuid).trim();
    const target = await repo.findByNodeUuid(targetUuid);
    if (!target) throw new Error('Nó alvo não encontrado.');
    if (target.status === OFFLINE) throw new Error('Nó alvo está OFFLINE.');

    const status = await this.getStatus();
    if (status.ngrok_online && status.owner_node_uuid === target.node_uuid) {
      return { ok: true, already_owner: true, message: 'Este node já está com a ngrok ativa.', target_node_uuid: target.node_uuid, target_node_name: target.node_name, status };
    }

    await this.setDesiredOwner(target, self, 'manual_takeover');
    const released = payload.skip_release ? true : await this.requestCurrentOwnerRelease(status, self, target);
    if (!released) {
      await sleep(Number(env.ngrokTakeoverGraceMs || 10000));
      const checkUrl = status.public_url || (env.ngrokDomain ? `https://${env.ngrokDomain}` : null);
      const checked = await checkNgrokHealth(checkUrl);
      if (checked.online) throw new Error('Dono atual ainda responde; takeover cancelado para evitar dois túneis ativos.');
    }

    if (target.node_uuid === self.node_uuid) {
      return this.startLocalTunnel('manual_takeover');
    }

    const targetUrl = this.getNodeControlUrl(target);
    if (!targetUrl) throw new Error('Nó alvo não tem URL local/Tailscale para receber a solicitação.');
    logger.info(`[ngrok-takeover] desired owner ${target.node_name} waiting to claim`);
    const response = await fetchWithTimeout(`${normalizeBaseUrl(targetUrl)}/api/cluster/ngrok/assume`, Number(env.publicUrlCheckTimeoutMs || 3000) + Number(env.ngrokTakeoverGraceMs || 10000), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Cluster-Secret': env.clusterKey || env.sessionSecret || '' },
      body: JSON.stringify({ target_node_uuid: target.node_uuid, skip_release: true })
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.message || `Falha ao solicitar takeover remoto (${response.status}).`);
    return { ok: true, message: 'Solicitação para assumir ngrok enviada.', target_node_uuid: target.node_uuid, target_node_name: target.node_name, remote: data };
  }

  async performCheckCycle() {
    if (running) return lastStatus;
    running = true;
    try {
      const self = await repo.getSelfNode();
      if (!self) {
        logger.info('[ngrok] ignorado: servidor local ainda não configurado');
        lastStatus = { ok: true, ngrok_online: false, owner_node_uuid: null, owner_node_name: null, public_url: null, last_checked_at: new Date().toISOString(), reason: 'self_not_configured' };
        return lastStatus;
      }
      const nodes = await repo.getAllNodes();
      const status = await this.getStatus();
      const candidates = nodes.filter((node) => node.public_url && (node.ngrok_enabled_currently || /ngrok/i.test(node.public_url) || node.node_uuid === status.owner_node_uuid));
      let onlineOwner = null;
      let offlineReason = 'no_public_endpoint';
      for (const node of candidates) {
        const checked = await checkNgrokHealth(node.public_url);
        if (checked.online) { onlineOwner = node; break; }
        offlineReason = checked.reason;
      }
      if (onlineOwner) {
        await repo.markNgrokOwner(onlineOwner.node_uuid, onlineOwner.public_url, ONLINE, { skipSyncEvent: true, reason: 'ngrok-check' });
        await repo.setRuntimeState('ngrok_owner_node_name', onlineOwner.node_name);
        await repo.setRuntimeState('ngrok_public_url', onlineOwner.public_url || '');
        lastStatus = { ok: true, ngrok_online: true, owner_node_uuid: onlineOwner.node_uuid, owner_node_name: onlineOwner.node_name, public_url: onlineOwner.public_url, last_checked_at: new Date().toISOString(), reason: ONLINE };
        logger.info(`[ngrok-check] status=ONLINE owner=${onlineOwner.node_name}`);
        if (!onlineOwner.is_self && ngrokService.getPublicUrl()) await ngrokService.stopTunnel();
        return lastStatus;
      }

      logger.info(`[ngrok-check] status=OFFLINE reason=${offlineReason}`);
      await repo.setRuntimeState('ngrok_status', OFFLINE);
      await repo.setRuntimeState('ngrok_last_check_at', new Date().toISOString());
      const desired = await repo.getRuntimeState('desired_ngrok_owner_node_uuid');
      let winner = desired?.state_value ? nodes.find((node) => node.node_uuid === desired.state_value) : null;
      if (!winner) winner = await this.electWinner();
      if (!winner) {
        logger.info('[ngrok-election] nenhum candidato elegível; aguardando próximo ciclo');
        lastStatus = { ok: true, ngrok_online: false, owner_node_uuid: null, owner_node_name: null, public_url: null, last_checked_at: new Date().toISOString(), reason: offlineReason };
        return lastStatus;
      }
      logger.info(`[ngrok-election] winner=${winner.node_name} reason=${winner.role === 'HOST' ? 'HOST_PRIORITY' : 'POWER_SCORE'}`);
      if (winner?.node_uuid && self?.node_uuid && winner.node_uuid === self.node_uuid) {
        await this.startLocalTunnel(desired?.state_value === self.node_uuid ? 'manual_takeover' : 'auto_election');
      }
      lastStatus = { ok: true, ngrok_online: false, owner_node_uuid: winner?.node_uuid || null, owner_node_name: winner?.node_name || null, public_url: winner?.public_url || null, last_checked_at: new Date().toISOString(), reason: offlineReason };
      return lastStatus;
    } finally {
      running = false;
    }
  }

  async start() {
    if (timer) return;
    const self = await repo.getSelfNode();
    if (!self) {
      logger.info('[ngrok] ignorado: servidor local ainda não configurado');
      return;
    }
    timer = setInterval(() => this.performCheckCycle().catch((error) => logger.error(`[ngrok-check] erro: ${error.message}`)), Number(env.ngrokCheckIntervalMs || 10000));
    setTimeout(() => this.performCheckCycle().catch((error) => logger.error(`[ngrok-check] erro: ${error.message}`)), 2500);
  }
}

module.exports = new NgrokCoordinatorService();
module.exports.checkNgrokHealth = checkNgrokHealth;
module.exports.sortNgrokCandidates = sortNgrokCandidates;
module.exports.ONLINE = ONLINE;
module.exports.OFFLINE = OFFLINE;
module.exports.UNKNOWN = UNKNOWN;
