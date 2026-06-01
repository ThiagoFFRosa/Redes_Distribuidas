const crypto = require('crypto');
const express = require('express');
const repo = require('../services/cluster-node.repository');
const healthService = require('../services/cluster-health.service');
const { requireAuth } = require('../services/auth.service');
const env = require('../config/env');

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
  const explicitUrl = normalizeUrl(node.public_url);
  if (explicitUrl) return explicitUrl;
  const port = normalizePort(node.port) || env.port;
  return node.tailscale_ip ? `http://${String(node.tailscale_ip).trim()}:${port}` : null;
};

const serializeClusterNode = (node = {}) => ({
  id: node.id,
  node_uuid: node.node_uuid || null,
  node_name: node.node_name || '',
  tailscale_ip: node.tailscale_ip || '',
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
  updated_at: node.updated_at || null
});

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
      public_url: normalizeNodePublicUrl(rawNode),
      port: normalizePort(rawNode.port) || env.port,
      role: ROLES.includes(rawNode.role) ? rawNode.role : 'UNKNOWN',
      status: STATUSES.includes(rawNode.status) ? rawNode.status : 'UNKNOWN',
      is_self: sameAsSelf ? 1 : 0,
      power_score: normalizePowerScore(rawNode.power_score) ?? 5,
      metadata: parseMetadata(rawNode.metadata),
      last_heartbeat_at: rawNode.last_heartbeat_at || null,
      last_healthcheck_at: rawNode.last_healthcheck_at || null,
      healthcheck_error: rawNode.healthcheck_error || null
    }, { skipSyncEvent: true });
  }
  await repo.enforceSingleSelf();
};

router.post('/join-request', async (req, res) => {
  try {
    const requesterIp = req.ip || req.socket?.remoteAddress || 'desconhecido';
    const { node_uuid, node_name, tailscale_ip, public_url, port, requested_role, power_score, session_secret, metadata } = req.body || {};

    console.log(`[join-request] solicitação recebida do servidor ${String(node_name || '').trim() || 'desconhecido'} / ${String(tailscale_ip || '').trim() || 'desconhecido'}`);
    console.log(`[join-request] HTTP remote address: ${requesterIp}`);

    if (!env.sessionSecret) return res.status(500).json({ ok: false, message: 'SESSION_SECRET não configurado no host.' });

    if (session_secret !== env.sessionSecret) {
      console.warn('[join-request] secret inválido');
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
      });
      return res.json({ ok: true, already_registered: true, message: 'Servidor já cadastrado no cluster.' });
    }

    const pending = await repo.findPendingJoinRequestByIp(payload.tailscale_ip);
    const savedRequest = pending
      ? await repo.updateJoinRequest(pending.id, payload)
      : await repo.createJoinRequest(payload);

    console.log(`[join-request] solicitação registrada como PENDING id=${savedRequest.id}`);
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

router.use(requireAuth);

router.get('/self', async (_req, res) => {
  try {
    const node = await repo.getSelfNode();
    res.json({ configured: Boolean(node), node: node ? serializeClusterNode(node) : null });
  } catch (error) {
    if (handleSchemaNotMigrated(error, res)) return;
    throw error;
  }
});
router.post('/self', async (req, res) => { const payload = normalize(req.body); if (payload.power_score === null) return res.status(400).json({ message: 'power_score deve ser um número inteiro entre 0 e 10.' }); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip); const selfNode = await repo.getSelfNode(); if (existingIp && (!selfNode || existingIp.id !== selfNode.id)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); const now = new Date(); payload.port = payload.port || env.port; await repo.clearSelfFlag(); let node; if (selfNode) node = await repo.updateNode(selfNode.id, { ...selfNode, ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null }); else node = await repo.createNode({ ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null, metadata: null, port: payload.port || env.port }); res.json({ configured: true, node: serializeClusterNode(node) }); });

router.get('/nodes', async (_req, res) => res.json({ nodes: (await repo.getAllNodes()).map(serializeClusterNode) }));
router.post('/nodes', async (req, res) => { const payload = normalize(req.body); if (payload.power_score === null) return res.status(400).json({ message: 'power_score deve ser um número inteiro entre 0 e 10.' }); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); if (await repo.findByTailscaleIp(payload.tailscale_ip)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); payload.port = payload.port || env.port; const node = await repo.createNode({ ...payload, is_self: 0, status: 'UNKNOWN', last_heartbeat_at: null, last_healthcheck_at: null, healthcheck_error: null, metadata: null }); res.status(201).json({ node: serializeClusterNode(node) }); });
router.put('/nodes/:id', async (req, res) => { const id = Number(req.params.id); const current = await repo.findById(id); if (!current) return res.status(404).json({ message: 'Nó não encontrado.' }); const payload = normalize(req.body); if (payload.power_score === null) return res.status(400).json({ message: 'power_score deve ser um número inteiro entre 0 e 10.' }); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip); if (existingIp && existingIp.id !== id) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); payload.port = payload.port || current.port || env.port; const node = await repo.updateNode(id, { ...current, ...payload, is_self: current.is_self }); res.json({ node: serializeClusterNode(node) }); });
router.delete('/nodes/:id', async (req, res) => { const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' }); if (node.is_self) return res.status(400).json({ message: 'Não é permitido remover o servidor atual.' }); await repo.deleteNode(id); res.json({ ok: true }); });
router.post('/nodes/:id/healthcheck', async (req, res) => { const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' }); const updated = await healthService.checkNode(node); const ok = updated.status === 'ONLINE'; res.status(ok ? 200 : 503).json({ ok, status: updated.status, node: updated, error: ok ? null : updated.healthcheck_error }); });
router.post('/healthcheck-all', async (_req, res) => res.json(await healthService.checkAllNodes()));

router.get('/join-requests', async (req, res) => {
  const status = String(req.query.status || '').toUpperCase();
  const filterStatus = JOIN_STATUSES.includes(status) ? status : null;
  const requests = await repo.listJoinRequests(filterStatus);
  console.log(`[join-requests] listando solicitações status=${filterStatus || 'ALL'} total=${requests.length}`);
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
  });
  console.log('[join-request] servidor salvo em cluster_nodes');
  await repo.approveJoinRequest(id, node.id);
  console.log('[join-request] solicitação aprovada');
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

  console.log(`[request-join-host] self node carregado: ${self.node_name} / ${self.tailscale_ip}`);
  const url = `${normalizedHostUrl.replace(/\/$/, '')}/api/cluster/join-request`;
  console.log(`[request-join-host] enviando solicitação para ${normalizedHostUrl}`);

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
    const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) return res.status(response.status).json(data);

    console.log(`[request-join-host] resposta do host: ${data.status || (data.already_registered ? 'ALREADY_REGISTERED' : 'UNKNOWN')}`);

    if (data.already_registered || data.status === 'APPROVED') {
      const bootstrapResp = await fetch(`${normalizedHostUrl.replace(/\/$/, '')}/api/cluster/bootstrap`, { headers: { 'X-Cluster-Secret': env.sessionSecret } });
      if (bootstrapResp.ok) {
        const bootstrapData = await bootstrapResp.json();
        await applyBootstrapNodes(bootstrapData.nodes || []);
      }
    }

    return res.json(data);
  } catch (error) {
    console.error(`[request-join-host] erro ao conectar no host: ${error.message}`);
    return res.status(502).json({ ok: false, message: 'Falha ao conectar no host informado.' });
  }
});

module.exports = router;
