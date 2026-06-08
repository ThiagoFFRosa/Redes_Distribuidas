const crypto = require('crypto');
const express = require('express');
const repo = require('../services/cluster-node.repository');
const healthService = require('../services/cluster-health.service');
const { requireAuth } = require('../services/auth.service');
const env = require('../config/env');
const logger = require('../utils/logger');
const { getNodeSyncTarget, getTailscaleBaseUrl } = require('../utils/sync-targets');
const syncCoordinator = require('../services/sync-coordinator.service');
const ngrokCoordinator = require('../services/ngrok-coordinator.service');
const { detectTailscaleIp } = require('../utils/network-addresses');
const clearAllLock = require('../services/clear-all-lock.service');

const router = express.Router();
const ROLES = ['HOST', 'STANDBY', 'UNKNOWN'];
const STATUSES = ['ONLINE', 'OFFLINE', 'UNKNOWN'];
const JOIN_STATUSES = ['PENDING', 'APPROVED', 'REJECTED'];

const isSchemaNotMigratedError = (error) => ['ER_BAD_FIELD_ERROR', 'ER_NO_SUCH_TABLE'].includes(error?.code);
const handleSchemaNotMigrated = (error, res) => {
  if (!isSchemaNotMigratedError(error)) return false;
  return res.status(500).json({ ok: false, error: 'Banco de dados não migrado. Rode npm run migrate.' });
};

const normalizePowerScore = (value) => {
  if (value === undefined || value === null || value === '') return 5;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 10) return null;
  return parsed;
};

const normalizePort = (value) => {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) return null;
  return parsed;
};

const parseMetadata = (value) => {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_error) { return {}; }
};

const normalize = (body = {}) => {
  const power_score = normalizePowerScore(body.power_score);
  return {
    node_name: String(body.node_name || '').trim(),
    tailscale_ip: String(body.tailscale_ip || '').trim(),
    public_url: normalizeUrl(body.public_url),
    port: normalizePort(body.port),
    role: ROLES.includes(body.role) ? body.role : 'UNKNOWN',
    status: STATUSES.includes(body.status) ? body.status : 'UNKNOWN',
    power_score
  };
};

const normalizeRequestedRole = (value) => (ROLES.includes(value) ? value : 'STANDBY');


const normalizeUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^https?:\/\//i.test(raw)) return raw;
  if (/^[\w.-]+\.(ngrok\.dev|ngrok-free\.app|dev)(:\d+)?(\/.*)?$/i.test(raw)) return `https://${raw}`;
  if (/^(localhost|\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-f:]+\])(:\d+)?(\/.*)?$/i.test(raw)) return `http://${raw}`;
  return `https://${raw}`;
};

const resolveRequestedRoleForJoin = (self) => {
  const role = ROLES.includes(self?.role) ? self.role : 'STANDBY';
  return role === 'HOST' ? 'STANDBY' : role;
};

const normalizeNodePublicUrl = (node = {}) => {
  const url = normalizeUrl(node.public_url);
  if (!url) return null;
  return /ngrok/i.test(url) || Number(node.ngrok_enabled_currently || 0) === 1 ? url : null;
};

const getNodeLocalUrl = (node = {}) => getTailscaleBaseUrl(node, normalizePort(node.port) || env.port);

const serializeClusterNode = (node = {}) => ({
  id: node.id,
  node_uuid: node.node_uuid || null,
  node_name: node.node_name || '',
  tailscale_ip: node.tailscale_ip || '',
  local_url: getNodeLocalUrl(node),
  public_url: normalizeNodePublicUrl(node),
  port: normalizePort(node.port) || env.port,
  role: ROLES.includes(node.role) ? node.role : 'UNKNOWN',
  status: STATUSES.includes(node.status) ? node.status : 'UNKNOWN',
  is_self: Number(node.is_self || 0),
  power_score: normalizePowerScore(node.power_score) ?? 5,
  last_heartbeat_at: node.last_heartbeat_at || null,
  last_healthcheck_at: node.last_healthcheck_at || null,
  healthcheck_error: node.healthcheck_error || null,
  metadata: parseMetadata(node.metadata),
  created_at: node.created_at || null,
  updated_at: node.updated_at || null,
  structural_version: Number(node.structural_version || 1),
  ngrok_enabled_currently: Number(node.ngrok_enabled_currently || 0),
  ngrok_status: node.ngrok_status || 'UNKNOWN',
  ngrok_last_seen_at: node.ngrok_last_seen_at || null,
  is_ngrok_owner: Number(node.ngrok_enabled_currently || 0) === 1,
  has_public_endpoint: Number(node.ngrok_enabled_currently || 0) === 1,
  sync_target_url: getNodeSyncTarget(node)
});

const getSelfBootstrapSuggestions = () => {
  const tailscaleIp = detectTailscaleIp();
  const port = env.port;
  return {
    node_name: env.serverName || 'Minipc',
    tailscale_ip: tailscaleIp || '',
    port,
    public_url: tailscaleIp ? `http://${tailscaleIp}:${port}` : '',
    role: 'STANDBY',
    power_score: 5
  };
};

const handleGetSelf = async (_req, res) => {
  try {
    const node = await repo.getSelfNode();
    res.json({
      configured: Boolean(node),
      node: node ? serializeClusterNode(node) : null,
      suggestions: node ? null : getSelfBootstrapSuggestions()
    });
  } catch (error) {
    if (handleSchemaNotMigrated(error, res)) return;
    throw error;
  }
};

const saveSelfNode = async (req, res) => {
  const payload = normalize(req.body);
  if (payload.power_score === null) return res.status(400).json({ message: 'power_score deve ser um número inteiro entre 0 e 10.' });
  if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' });
  const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip);
  const selfNode = await repo.getSelfNode();
  if (existingIp && (!selfNode || existingIp.id !== selfNode.id)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' });
  const now = new Date();
  payload.port = payload.port || env.port;
  await repo.clearSelfFlag();
  let node;
  if (selfNode) node = await repo.updateNodeStructuralData(selfNode.id, { ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null }, { reason: 'self-config' });
  else node = await repo.createNode({ ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null, metadata: null, port: payload.port || env.port }, { reason: 'self-config' });
  res.json({ configured: true, node: serializeClusterNode(node) });
};


const allowInitialSelfStatus = async (req, res, next) => {
  try {
    const selfNode = await repo.getSelfNode();
    if (!selfNode) return handleGetSelf(req, res);
    return requireAuth(req, res, next);
  } catch (error) {
    return next(error);
  }
};

const allowInitialSelfConfig = async (req, res, next) => {
  try {
    const selfNode = await repo.getSelfNode();
    if (!selfNode) return saveSelfNode(req, res);
    return requireAuth(req, res, next);
  } catch (error) {
    return next(error);
  }
};

const applyBootstrapNodes = async (nodes = []) => {
  const self = await repo.getSelfNode();
  for (const rawNode of nodes) {
    if (!rawNode?.node_uuid && !rawNode?.tailscale_ip) continue;
    const sameAsSelf = Boolean(self && (
      (rawNode.node_uuid && rawNode.node_uuid === self.node_uuid) ||
      (rawNode.tailscale_ip && rawNode.tailscale_ip === self.tailscale_ip)
    ));
    await repo.upsertClusterNode({
      node_uuid: rawNode.node_uuid || null,
      node_name: rawNode.node_name,
      tailscale_ip: rawNode.tailscale_ip,
      public_url: sameAsSelf && self?.public_url ? self.public_url : normalizeNodePublicUrl(rawNode),
      port: normalizePort(rawNode.port) || env.port,
      role: ROLES.includes(rawNode.role) ? rawNode.role : 'UNKNOWN',
      status: STATUSES.includes(rawNode.status) ? rawNode.status : 'UNKNOWN',
      is_self: sameAsSelf ? 1 : 0,
      power_score: normalizePowerScore(rawNode.power_score) ?? 5,
      metadata: parseMetadata(rawNode.metadata),
      structural_version: Number(rawNode.structural_version || 1),
      ngrok_enabled_currently: Number(rawNode.ngrok_enabled_currently || 0),
      ngrok_status: rawNode.ngrok_status || 'UNKNOWN',
      ngrok_last_seen_at: rawNode.ngrok_last_seen_at || null,
      last_heartbeat_at: rawNode.last_heartbeat_at || null,
      last_healthcheck_at: rawNode.last_healthcheck_at || null,
      healthcheck_error: rawNode.healthcheck_error || null
    }, { skipSyncEvent: true, reason: 'bootstrap' });
  }
  await repo.enforceSingleSelf();
};


const requireClusterSecret = (req, res, next) => {
  const received = req.header('x-cluster-secret') || req.header('x-cluster-key');
  const accepted = [env.clusterKey, env.sessionSecret].filter(Boolean);
  if (!accepted.length) return res.status(500).json({ ok: false, message: 'CLUSTER_KEY/SESSION_SECRET não configurado.' });
  if (!received || !accepted.includes(received)) return res.status(401).json({ ok: false, message: 'Não autorizado.' });
  return next();
};

const requireAuthOrClusterSecret = (req, res, next) => {
  const received = req.header('x-cluster-secret') || req.header('x-cluster-key');
  const accepted = [env.clusterKey, env.sessionSecret].filter(Boolean);
  if (received && accepted.includes(received)) return next();
  return requireAuth(req, res, next);
};

const structuralFingerprint = (node = {}) => {
  const payload = {
    node_uuid: node.node_uuid || null,
    node_name: node.node_name || null,
    role: node.role || null,
    tailscale_ip: node.tailscale_ip || null,
    public_url: normalizeNodePublicUrl(node),
    port: normalizePort(node.port) || env.port,
    power_score: normalizePowerScore(node.power_score) ?? 5,
    structural_version: Number(node.structural_version || 1)
  };
  return { ...payload, checksum: crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex') };
};

const saveRemoteNodeList = async (nodes = []) => {
  await applyBootstrapNodes(nodes);
  return repo.getAllNodes();
};

router.post('/link-request', requireClusterSecret, async (req, res) => {
  const { request_id, handshake_key, node, known_nodes } = req.body || {};
  if (!handshake_key) return res.status(400).json({ ok: false, message: 'handshake_key obrigatório.' });
  if (!node?.node_uuid && !node?.tailscale_ip) return res.status(400).json({ ok: false, message: 'node.node_uuid ou node.tailscale_ip obrigatório.' });
  logger.info(`[cluster-link] request received from ${node.node_name || node.node_uuid || '-'}`);
  const saved = await repo.upsertClusterNode({
    node_uuid: node.node_uuid || null,
    node_name: String(node.node_name || node.node_uuid || '').trim(),
    tailscale_ip: String(node.tailscale_ip || '').trim(),
    public_url: normalizeNodePublicUrl(node),
    port: normalizePort(node.port) || env.port,
    role: ROLES.includes(node.role) ? node.role : 'STANDBY',
    status: STATUSES.includes(node.status) ? node.status : 'UNKNOWN',
    is_self: 0,
    power_score: normalizePowerScore(node.power_score) ?? 5,
    metadata: parseMetadata(node.metadata),
    structural_version: Number(node.structural_version || 1),
    ngrok_enabled_currently: Number(node.ngrok_enabled_currently || 0),
    ngrok_status: node.ngrok_status || 'UNKNOWN',
    ngrok_last_seen_at: node.ngrok_last_seen_at || null
  }, { reason: 'cluster-link' });
  if (Array.isArray(known_nodes) && known_nodes.length) await saveRemoteNodeList(known_nodes);
  const self = await repo.getSelfNode();
  const allNodes = await repo.getAllNodes();
  logger.info(`[cluster-link] approved; saved remote node ${saved.node_name}`);
  logger.info(`[cluster-link] response includes self ${self?.node_name || '-'}`);
  return res.json({
    ok: true,
    status: 'APPROVED',
    request_id: request_id || null,
    handshake_key,
    accepted_by: self ? serializeClusterNode(self) : null,
    known_nodes: allNodes.map(serializeClusterNode)
  });
});

router.get('/nodes/fingerprint', requireClusterSecret, async (_req, res) => {
  const nodes = await repo.getAllNodes();
  res.json({ ok: true, nodes: nodes.map((node) => ({ ...structuralFingerprint(node), updated_at: node.updated_at || null })) });
});

router.post('/nodes/reconcile', requireClusterSecret, async (req, res) => {
  const incoming = Array.isArray(req.body?.nodes) ? req.body.nodes : [];
  const before = await repo.getAllNodes();
  await saveRemoteNodeList(incoming);
  const after = await repo.getAllNodes();
  res.json({ ok: true, received: incoming.length, before_count: before.length, after_count: after.length, nodes: after.map(serializeClusterNode) });
});

router.post('/ngrok/release', requireClusterSecret, async (req, res) => {
  try { res.json(await ngrokCoordinator.releaseLocal(req.body || {})); }
  catch (error) { res.status(500).json({ ok: false, message: error.message }); }
});

router.post('/ngrok/claim', requireClusterSecret, async (req, res) => {
  const { owner_node_uuid, owner_node_name, public_url, status } = req.body || {};
  if (!owner_node_uuid) return res.status(400).json({ ok: false, message: 'owner_node_uuid obrigatório.' });
  const node = await repo.markNgrokOwner(owner_node_uuid, normalizeUrl(public_url), status || 'ONLINE', { reason: 'ngrok-claim' });
  logger.info(`[ngrok-claim] owner updated ${owner_node_name || node?.node_name || owner_node_uuid}`);
  res.json({ ok: true, node: node ? serializeClusterNode(node) : null });
});

router.get('/ngrok/status', requireAuthOrClusterSecret, async (_req, res) => {
  res.json(await ngrokCoordinator.getStatus());
});

router.post('/join-request', async (req, res) => {
  try {
    const requesterIp = req.ip || req.socket?.remoteAddress || 'desconhecido';
    const { node_uuid, node_name, tailscale_ip, public_url, port, requested_role, power_score, session_secret, metadata } = req.body || {};

    logger.info(`[join-request] solicitação recebida do servidor ${String(node_name || '').trim() || 'desconhecido'} / ${String(tailscale_ip || '').trim() || 'desconhecido'}`);
    logger.info(`[join-request] HTTP remote address: ${requesterIp}`);

    if (!env.sessionSecret) return res.status(500).json({ ok: false, message: 'SESSION_SECRET não configurado no host.' });

    if (session_secret !== env.sessionSecret) {
      logger.warn('[join-request] secret inválido');
      return res.status(403).json({ ok: false, message: 'Secret inválido.' });
    }
    if (!String(node_name || '').trim() || !String(tailscale_ip || '').trim()) return res.status(400).json({ ok: false, message: 'node_name e tailscale_ip são obrigatórios.' });

    const secretFingerprint = crypto.createHash('sha256').update(session_secret).digest('hex').slice(0, 16);
    const requestTokenHash = crypto.createHash('sha256').update(`${node_name}|${tailscale_ip}|${Date.now()}`).digest('hex');
    const payload = {
      node_uuid: String(node_uuid || '').trim() || null,
      node_name: String(node_name).trim(),
      tailscale_ip: String(tailscale_ip).trim(),
      public_url: normalizeUrl(public_url),
      port: normalizePort(port),
      requested_role: normalizeRequestedRole(requested_role),
      power_score: normalizePowerScore(power_score) ?? 5,
      request_token_hash: requestTokenHash,
      secret_fingerprint: secretFingerprint,
      requester_metadata: metadata || null
    };

    const existingNode = await repo.findByTailscaleIp(payload.tailscale_ip);
    if (existingNode) {
      await repo.upsertClusterNode({
        node_uuid: payload.node_uuid || existingNode.node_uuid,
        node_name: payload.node_name,
        tailscale_ip: payload.tailscale_ip,
        public_url: payload.public_url,
        port: payload.port || env.port,
        role: payload.requested_role === 'HOST' ? 'STANDBY' : payload.requested_role,
        status: existingNode.status || 'UNKNOWN',
        is_self: existingNode.is_self,
        power_score: payload.power_score,
        metadata: payload.requester_metadata
      }, { reason: 'join-approve' });
      return res.json({ ok: true, already_registered: true, message: 'Servidor já cadastrado no cluster.' });
    }

    const pending = await repo.findPendingJoinRequestByIp(payload.tailscale_ip);
    const savedRequest = pending
      ? await repo.updateJoinRequest(pending.id, payload)
      : await repo.createJoinRequest(payload);

    logger.info(`[join-request] solicitação registrada como PENDING id=${savedRequest.id}`);
    return res.json({ ok: true, status: 'PENDING', message: 'Solicitação enviada. Aguardando aprovação no host.' });
  } catch (error) {
    if (handleSchemaNotMigrated(error, res)) return;
    throw error;
  }
});


router.get('/self-identity', async (_req, res) => {
  const self = await repo.getSelfNode();
  if (!self) return res.status(404).json({ ok: false, message: 'Servidor self não configurado.' });
  res.json({ ok: true, ...serializeClusterNode(self) });
});

router.get('/bootstrap', async (req, res) => {
  if (!env.sessionSecret) return res.status(500).json({ ok: false, message: 'SESSION_SECRET não configurado no host.' });
  const secret = req.header('x-cluster-secret');
  if (!secret || secret !== env.sessionSecret) return res.status(403).json({ ok: false, message: 'Secret inválido.' });
  const self = await repo.getSelfNode();
  const nodes = await repo.getAllNodes();
  const serializedSelf = self ? serializeClusterNode(self) : null;
  return res.json({
    ok: true,
    self: serializedSelf,
    host: serializedSelf,
    nodes: nodes.map(serializeClusterNode),
    generated_at: new Date().toISOString()
  });
});

router.get('/self', allowInitialSelfStatus, handleGetSelf);
router.post('/self', allowInitialSelfConfig, saveSelfNode);

router.post('/ngrok/assume', requireAuthOrClusterSecret, async (req, res) => {
  try { res.json(await ngrokCoordinator.assume(req.body || {})); }
  catch (error) { res.status(503).json({ ok: false, message: error.message }); }
});

router.use(requireAuth);


router.get('/nodes', async (_req, res) => res.json({ nodes: (await repo.getAllNodes()).map(serializeClusterNode) }));
router.post('/nodes', async (req, res) => { const payload = normalize(req.body); if (payload.power_score === null) return res.status(400).json({ message: 'power_score deve ser um número inteiro entre 0 e 10.' }); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); if (await repo.findByTailscaleIp(payload.tailscale_ip)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); payload.port = payload.port || env.port; const node = await repo.createNode({ ...payload, is_self: 0, status: 'UNKNOWN', last_heartbeat_at: null, last_healthcheck_at: null, healthcheck_error: null, metadata: null }, { reason: 'manual-edit' }); res.status(201).json({ node: serializeClusterNode(node) }); });
router.put('/nodes/:id', async (req, res) => { const id = Number(req.params.id); const current = await repo.findById(id); if (!current) return res.status(404).json({ message: 'Nó não encontrado.' }); const payload = normalize(req.body); if (payload.power_score === null) return res.status(400).json({ message: 'power_score deve ser um número inteiro entre 0 e 10.' }); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip); if (existingIp && existingIp.id !== id) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); payload.port = payload.port || current.port || env.port; const node = await repo.updateNodeStructuralData(id, { ...payload, is_self: current.is_self }, { reason: 'manual-edit' }); res.json({ node: serializeClusterNode(node) }); });
router.post('/nodes/:id/fix-url-tailscale', async (req, res) => {
  const id = Number(req.params.id);
  const node = await repo.findById(id);
  if (!node) return res.status(404).json({ message: 'Nó não encontrado.' });
  if (!node.tailscale_ip) return res.status(400).json({ message: 'tailscale_ip não configurado.' });
  const port = normalizePort(node.port) || env.port;
  const publicUrl = `http://${String(node.tailscale_ip).trim()}:${port}`;
  const updated = await repo.updateNodeStructuralData(id, { public_url: publicUrl, port }, { reason: 'manual-edit' });
  res.json({ ok: true, node: serializeClusterNode(updated) });
});
router.delete('/nodes/:id', async (req, res) => { const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' }); if (node.is_self) return res.status(400).json({ message: 'Não é permitido remover o servidor atual.' }); await repo.deleteNode(id); res.json({ ok: true }); });
router.post('/nodes/:id/healthcheck', async (req, res) => { const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' }); const updated = await healthService.checkNode(node); const ok = updated.status === 'ONLINE'; res.status(ok ? 200 : 503).json({ ok, status: updated.status, node: updated, error: ok ? null : updated.healthcheck_error }); });
router.post('/healthcheck-all', async (_req, res) => res.json(await healthService.checkAllNodes()));

router.get('/join-requests', async (req, res) => {
  const status = String(req.query.status || '').toUpperCase();
  const filterStatus = JOIN_STATUSES.includes(status) ? status : null;
  const requests = await repo.listJoinRequests(filterStatus);
  const message = `[join-requests] listando solicitações status=${filterStatus || 'ALL'} total=${requests.length}`;
  if (requests.length === 0) logger.debug(message); else logger.info(message);
  res.json({ ok: true, data: requests });
});

router.post('/join-requests/:id/approve', async (req, res) => {
  const id = Number(req.params.id);
  const request = await repo.findJoinRequestById(id);
  if (!request) return res.status(404).json({ ok: false, message: 'Solicitação não encontrada.' });
  if (request.status === 'APPROVED') return res.json({ ok: true, message: 'Solicitação já aprovada.' });
  if (request.status === 'REJECTED') return res.status(409).json({ ok: false, message: 'Solicitação já rejeitada.' });

  const approvedRole = request.requested_role === 'HOST' ? 'STANDBY' : normalizeRequestedRole(request.requested_role);
  const powerScore = normalizePowerScore(request.power_score) ?? 5;
  let node = await repo.upsertClusterNode({
    node_uuid: request.node_uuid || null,
    node_name: request.node_name,
    tailscale_ip: request.tailscale_ip,
    public_url: normalizeUrl(request.public_url),
    port: normalizePort(request.port) || env.port,
    role: approvedRole,
    status: 'UNKNOWN',
    is_self: 0,
    power_score: powerScore,
    last_heartbeat_at: null,
    last_healthcheck_at: null,
    healthcheck_error: null,
    metadata: parseMetadata(request.requester_metadata)
  }, { reason: 'join-approve' });
  logger.info('[join-request] servidor salvo em cluster_nodes');
  await repo.approveJoinRequest(id, node.id);
  logger.info('[join-request] solicitação aprovada');
  res.json({ ok: true, node: serializeClusterNode(node) });
});

router.post('/join-requests/:id/reject', async (req, res) => {
  const id = Number(req.params.id); const request = await repo.findJoinRequestById(id);
  if (!request) return res.status(404).json({ ok: false, message: 'Solicitação não encontrada.' });
  await repo.rejectJoinRequest(id);
  res.json({ ok: true });
});

router.post('/request-join-host', async (req, res) => {
  const { host_url } = req.body || {};
  if (!env.sessionSecret) return res.status(500).json({ ok: false, message: 'SESSION_SECRET não configurado localmente.' });

  const self = await repo.getSelfNode();
  if (!self) return res.status(400).json({ ok: false, message: 'Configure este servidor antes de solicitar entrada em um cluster.' });

  const normalizedHostUrl = normalizeUrl(host_url);
  if (!normalizedHostUrl) return res.status(400).json({ ok: false, message: 'host_url inválida.' });

  logger.info(`[request-join-host] self node carregado: ${self.node_name} / ${self.tailscale_ip}`);
  const baseUrl = normalizedHostUrl.replace(/\/$/, '');
  const linkUrl = `${baseUrl}/api/cluster/link-request`;
  const legacyJoinUrl = `${baseUrl}/api/cluster/join-request`;
  logger.info(`[request-join-host] enviando solicitação para ${normalizedHostUrl}`);

  const payload = {
    node_name: String(self.node_name).trim(),
    tailscale_ip: String(self.tailscale_ip).trim(),
    node_uuid: self.node_uuid,
    public_url: normalizeNodePublicUrl(self),
    port: normalizePort(self.port) || env.port,
    requested_role: resolveRequestedRoleForJoin(self),
    power_score: normalizePowerScore(self.power_score) ?? 5,
    session_secret: env.sessionSecret,
    metadata: {
      ...parseMetadata(self.metadata),
      port: normalizePort(self.port) || env.port,
      source: 'self-node-db',
      local_node_id: self.id
    }
  };

  try {
    const handshakeKey = crypto.randomUUID();
    const knownNodes = (await repo.getAllNodes()).map(serializeClusterNode);
    const linkResponse = await fetch(linkUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Cluster-Secret': env.sessionSecret || env.clusterKey },
      body: JSON.stringify({ request_id: crypto.randomUUID(), handshake_key: handshakeKey, node: serializeClusterNode(self), known_nodes: knownNodes })
    }).catch(() => null);

    if (linkResponse?.ok) {
      const data = await linkResponse.json().catch(() => ({}));
      if (data.status === 'APPROVED' && data.handshake_key === handshakeKey) {
        const nodesToSave = [data.accepted_by, ...(data.known_nodes || [])].filter(Boolean);
        await saveRemoteNodeList(nodesToSave);
        const lock = await clearAllLock.getLock();
        if (lock.exists || !env.autoBootstrapOnJoin) {
          logger.info(`[request-join-host] bootstrap automático bloqueado auto=${env.autoBootstrapOnJoin} clear_all_lock=${lock.exists}`);
          return res.json({ ...data, message: 'Máquinas vinculadas nos dois lados. Bootstrap automático bloqueado; use o painel para iniciar sincronização inicial.', bootstrap: { ok: true, skipped: true, reason: lock.exists ? 'clear_all_lock' : 'AUTO_BOOTSTRAP_ON_JOIN=false' } });
        }
        const bootstrapRun = await syncCoordinator.startFullBootstrap({ host_url: normalizedHostUrl }).catch((error) => ({ ok: false, error: error.message }));
        return res.json({ ...data, message: 'Máquinas vinculadas nos dois lados.', bootstrap: bootstrapRun });
      }
    }

    const response = await fetch(legacyJoinUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) return res.status(response.status).json(data);

    logger.info(`[request-join-host] resposta do host: ${data.status || (data.already_registered ? 'ALREADY_REGISTERED' : 'UNKNOWN')}`);

    if (data.accepted_by || data.already_registered || data.status === 'APPROVED') {
      const nodesToSave = [data.accepted_by, ...(data.known_nodes || [])].filter(Boolean);
      if (nodesToSave.length) await saveRemoteNodeList(nodesToSave);
      const lock = await clearAllLock.getLock();
      if (lock.exists || !env.autoBootstrapOnJoin) {
        logger.info(`[request-join-host] bootstrap automático bloqueado auto=${env.autoBootstrapOnJoin} clear_all_lock=${lock.exists}`);
        return res.json({ ...data, bootstrap: { ok: true, skipped: true, reason: lock.exists ? 'clear_all_lock' : 'AUTO_BOOTSTRAP_ON_JOIN=false' } });
      }
      const bootstrapRun = await syncCoordinator.startFullBootstrap({ host_url: normalizedHostUrl });
      return res.json({ ...data, bootstrap: bootstrapRun });
    }

    return res.json(data);
  } catch (error) {
    logger.error(`[request-join-host] erro ao conectar no host: ${error.message}`);
    return res.status(502).json({ ok: false, message: 'Falha ao conectar no host informado.' });
  }
});

module.exports = router;
