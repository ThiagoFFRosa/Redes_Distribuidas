const env = require('../config/env');
const ngrokService = require('./ngrok.service');
const repo = require('./cluster-node.repository');
const healthService = require('./cluster-health.service');
const logger = require('../utils/logger');

const ONLINE = 'ONLINE';
const OFFLINE = 'OFFLINE';
const UNKNOWN = 'UNKNOWN';
const NGROK_OFFLINE_PATTERNS = ['ERR_NGROK_3200', 'endpoint fuzzylogic.ngrok.dev is offline', 'is offline'];
let timer = null;
let running = false;
let lastStatus = { ok: true, ngrok_online: false, owner_node_uuid: null, owner_node_name: null, public_url: null, last_checked_at: null, reason: null };

const normalizeBaseUrl = (url) => String(url || '').trim().replace(/\/$/, '');
const isJsonHealthOnline = (data) => Boolean(data && (data.ok === true || data.status === ONLINE || data.status === 'OK'));

const fetchWithTimeout = async (url, timeoutMs) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
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
    const states = await repo.getRuntimeStates(['ngrok_owner_node_uuid', 'ngrok_status', 'ngrok_last_check_at']);
    const ownerUuid = states.ngrok_owner_node_uuid?.state_value || lastStatus.owner_node_uuid || null;
    const owner = ownerUuid ? await repo.findByNodeUuid(ownerUuid) : null;
    return {
      ok: true,
      ngrok_online: states.ngrok_status ? states.ngrok_status.state_value === ONLINE : Boolean(lastStatus.ngrok_online),
      owner_node_uuid: owner?.node_uuid || ownerUuid || null,
      owner_node_name: owner?.node_name || lastStatus.owner_node_name || null,
      public_url: owner?.public_url || lastStatus.public_url || null,
      last_checked_at: states.ngrok_last_check_at?.state_value || lastStatus.last_checked_at || null,
      reason: lastStatus.reason || null
    };
  }

  async claimLocal(publicUrl = null, options = {}) {
    const self = await repo.getSelfNode();
    if (!self) throw new Error('Servidor self não configurado.');
    const url = publicUrl || ngrokService.getPublicUrl() || (env.ngrokDomain ? `https://${env.ngrokDomain}` : null);
    await repo.markNgrokOwner(self.node_uuid, url, ONLINE, { reason: options.reason || 'ngrok-claim' });
    await repo.setRuntimeState('desired_ngrok_owner_node_uuid', '');
    lastStatus = { ok: true, ngrok_online: true, owner_node_uuid: self.node_uuid, owner_node_name: self.node_name, public_url: url, last_checked_at: new Date().toISOString(), reason: options.reason || 'claim' };
    logger.info(`[ngrok] iniciado owner=${self.node_name} url=${url}`);
    return lastStatus;
  }

  async releaseLocal(payload = {}) {
    const self = await repo.getSelfNode();
    await ngrokService.stopTunnel();
    if (self?.node_uuid) await repo.markNgrokOwner(self.node_uuid, null, OFFLINE, { skipSyncEvent: true, reason: 'ngrok-release' });
    if (payload.requested_by_node_uuid) {
      await repo.setRuntimeState('desired_ngrok_owner_node_uuid', payload.requested_by_node_uuid);
      await repo.setRuntimeState('ngrok_takeover_requested_at', new Date().toISOString());
    }
    logger.info(`[ngrok] release requested_by=${payload.requested_by_node_name || payload.requested_by_node_uuid || '-'} reason=${payload.reason || '-'}`);
    return { ok: true, released: true };
  }

  async electWinner() {
    const nodes = await repo.getAllNodes();
    const checked = [];
    for (const node of nodes) {
      if (node.is_self) checked.push({ ...node, status: ONLINE });
      else checked.push(await healthService.checkNode(node).catch(() => ({ ...node, status: OFFLINE })));
    }
    const online = checked.filter((node) => node.status === ONLINE);
    return sortNgrokCandidates(online)[0] || null;
  }

  async performCheckCycle() {
    if (running) return lastStatus;
    running = true;
    try {
      const nodes = await repo.getAllNodes();
      const candidates = nodes.filter((node) => node.public_url && (node.ngrok_enabled_currently || /ngrok/i.test(node.public_url)));
      let onlineOwner = null;
      let offlineReason = 'no_public_endpoint';
      for (const node of candidates) {
        const checked = await checkNgrokHealth(node.public_url);
        if (checked.online) { onlineOwner = node; break; }
        offlineReason = checked.reason;
      }
      if (onlineOwner) {
        await repo.markNgrokOwner(onlineOwner.node_uuid, onlineOwner.public_url, ONLINE, { skipSyncEvent: true, reason: 'ngrok-check' });
        lastStatus = { ok: true, ngrok_online: true, owner_node_uuid: onlineOwner.node_uuid, owner_node_name: onlineOwner.node_name, public_url: onlineOwner.public_url, last_checked_at: new Date().toISOString(), reason: 'ONLINE' };
        logger.info(`[ngrok-check] status=ONLINE owner=${onlineOwner.node_name}`);
        if (!onlineOwner.is_self && ngrokService.getPublicUrl()) await ngrokService.stopTunnel();
        return lastStatus;
      }

      logger.info(`[ngrok-check] status=OFFLINE reason=${offlineReason}`);
      await repo.setRuntimeState('ngrok_status', OFFLINE);
      await repo.setRuntimeState('ngrok_last_check_at', new Date().toISOString());
      const desired = await repo.getRuntimeState('desired_ngrok_owner_node_uuid');
      const self = await repo.getSelfNode();
      let winner = desired?.state_value ? nodes.find((node) => node.node_uuid === desired.state_value) : null;
      const requestedAt = desired?.state_value ? await repo.getRuntimeState('ngrok_takeover_requested_at') : null;
      if (winner && requestedAt?.state_value) {
        const age = Date.now() - new Date(requestedAt.state_value).getTime();
        if (age > Number(env.ngrokTakeoverGraceMs || 10000)) winner = null;
      }
      if (!winner) winner = await this.electWinner();
      logger.info(`[ngrok-election] winner=${winner?.node_name || '-'} reason=${winner?.role === 'HOST' ? 'HOST_PRIORITY' : 'POWER_SCORE'}`);
      if (winner?.node_uuid && self?.node_uuid && winner.node_uuid === self.node_uuid) {
        const url = await ngrokService.startTunnelWithRetry(env.port);
        await this.claimLocal(url, { reason: desired?.state_value === self.node_uuid ? 'manual_takeover' : 'auto_election' });
      }
      lastStatus = { ok: true, ngrok_online: false, owner_node_uuid: winner?.node_uuid || null, owner_node_name: winner?.node_name || null, public_url: winner?.public_url || null, last_checked_at: new Date().toISOString(), reason: offlineReason };
      return lastStatus;
    } finally {
      running = false;
    }
  }

  async assumeLocal() {
    const self = await repo.getSelfNode();
    if (!self?.node_uuid) throw new Error('Servidor self não configurado.');
    const status = await this.getStatus();
    if (status.ngrok_online && status.owner_node_uuid === self.node_uuid) {
      return { ok: true, already_owner: true, message: 'Esta máquina já está com a ngrok ativa.', status };
    }
    await repo.setRuntimeState('desired_ngrok_owner_node_uuid', self.node_uuid);
    await repo.setRuntimeState('ngrok_takeover_requested_at', new Date().toISOString());
    const currentUrl = status.public_url || (env.ngrokDomain ? `https://${env.ngrokDomain}` : null);
    if (currentUrl && status.owner_node_uuid !== self.node_uuid) {
      await fetch(`${normalizeBaseUrl(currentUrl)}/api/cluster/ngrok/release`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Cluster-Secret': env.clusterKey || env.sessionSecret || '' },
        body: JSON.stringify({ requested_by_node_uuid: self.node_uuid, requested_by_node_name: self.node_name, reason: 'manual_takeover' })
      }).catch((error) => logger.warn(`[ngrok] release remoto falhou/indisponível: ${error.message}`));
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(1000, Number(env.ngrokTakeoverGraceMs || 10000))));
    const url = await ngrokService.startTunnelWithRetry(env.port);
    const claimed = await this.claimLocal(url, { reason: 'manual_takeover' });
    return { ok: true, message: 'Ngrok assumida por esta máquina.', status: claimed };
  }

  start() {
    if (timer) return;
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
