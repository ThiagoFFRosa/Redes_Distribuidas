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

const normalize = (body = {}) => ({
  node_name: String(body.node_name || '').trim(),
  tailscale_ip: String(body.tailscale_ip || '').trim(),
  public_url: String(body.public_url || '').trim() || null,
  role: ROLES.includes(body.role) ? body.role : 'UNKNOWN',
  status: STATUSES.includes(body.status) ? body.status : 'UNKNOWN'
});

const normalizeRequestedRole = (value) => (ROLES.includes(value) ? value : 'STANDBY');


const normalizeUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^https?:\/\//i.test(raw)) return raw;
  if (/^[\w.-]+\.(ngrok\.dev|ngrok-free\.app|dev)(:\d+)?(\/.*)?$/i.test(raw)) return `https://${raw}`;
  if (/^(localhost|\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-f:]+\])(:\d+)?(\/.*)?$/i.test(raw)) return `http://${raw}`;
  return `https://${raw}`;
};

const resolveRequestedRoleForJoin = () => 'STANDBY';

router.post('/join-request', async (req, res) => {
  try {
    const requesterIp = req.ip || req.socket?.remoteAddress || 'desconhecido';
    const { node_name, tailscale_ip, public_url, requested_role, session_secret, metadata } = req.body || {};

    console.log(`[join-request] solicitação recebida do servidor ${String(node_name || '').trim() || 'desconhecido'} / ${String(tailscale_ip || '').trim() || 'desconhecido'}`);
    console.log(`[join-request] HTTP remote address: ${requesterIp}`);

    if (!env.sessionSecret) return res.status(500).json({ ok: false, message: 'SESSION_SECRET não configurado no host.' });

        if (session_secret !== env.sessionSecret) {
      console.warn('[join-request] secret inválido');
      return res.status(403).json({ ok: false, message: 'Secret inválido.' });
    }
    if (!String(node_name || '').trim() || !String(tailscale_ip || '').trim()) return res.status(400).json({ ok: false, message: 'node_name e tailscale_ip são obrigatórios.' });

    const existingNode = await repo.findByTailscaleIp(String(tailscale_ip).trim());
    if (existingNode) return res.json({ ok: true, already_registered: true, message: 'Servidor já cadastrado no cluster.' });

    const secretFingerprint = crypto.createHash('sha256').update(session_secret).digest('hex').slice(0, 16);
    const requestTokenHash = crypto.createHash('sha256').update(`${node_name}|${tailscale_ip}|${Date.now()}`).digest('hex');
    const payload = {
      node_name: String(node_name).trim(),
      tailscale_ip: String(tailscale_ip).trim(),
      public_url: String(public_url || '').trim() || null,
      requested_role: normalizeRequestedRole(requested_role),
      request_token_hash: requestTokenHash,
      secret_fingerprint: secretFingerprint,
      requester_metadata: metadata ? JSON.stringify(metadata) : null
    };

    const pending = await repo.findPendingJoinRequestByIp(payload.tailscale_ip);
    if (pending) await repo.updateJoinRequest(pending.id, payload);
    else await repo.createJoinRequest(payload);

    console.log('[join-request] solicitação registrada como PENDING');
    return res.json({ ok: true, status: 'PENDING', message: 'Solicitação enviada. Aguardando aprovação no host.' });
  } catch (error) {
    if (handleSchemaNotMigrated(error, res)) return;
    throw error;
  }
});

router.get('/bootstrap', async (req, res) => {
  if (!env.sessionSecret) return res.status(500).json({ ok: false, message: 'SESSION_SECRET não configurado no host.' });
  const secret = req.header('x-cluster-secret');
  if (!secret || secret !== env.sessionSecret) return res.status(403).json({ ok: false, message: 'Secret inválido.' });
  const self = await repo.getSelfNode();
  const nodes = await repo.getAllNodes();
  return res.json({ ok: true, host: self, nodes, generated_at: new Date().toISOString() });
});

router.use(requireAuth);

router.get('/self', async (_req, res) => {
  try {
    const node = await repo.getSelfNode();
    res.json({ configured: Boolean(node), node: node || null });
  } catch (error) {
    if (handleSchemaNotMigrated(error, res)) return;
    throw error;
  }
});
router.post('/self', async (req, res) => { const payload = normalize(req.body); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip); const selfNode = await repo.getSelfNode(); if (existingIp && (!selfNode || existingIp.id !== selfNode.id)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); const now = new Date(); await repo.clearSelfFlag(); let node; if (selfNode) node = await repo.updateNode(selfNode.id, { ...selfNode, ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null }); else node = await repo.createNode({ ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null, metadata: null }); res.json({ configured: true, node }); });

router.get('/nodes', async (_req, res) => res.json({ nodes: await repo.getAllNodes() }));
router.post('/nodes', async (req, res) => { const payload = normalize(req.body); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); if (await repo.findByTailscaleIp(payload.tailscale_ip)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); const node = await repo.createNode({ ...payload, is_self: 0, status: 'UNKNOWN', last_heartbeat_at: null, last_healthcheck_at: null, healthcheck_error: null, metadata: null }); res.status(201).json({ node }); });
router.put('/nodes/:id', async (req, res) => { const id = Number(req.params.id); const current = await repo.findById(id); if (!current) return res.status(404).json({ message: 'Nó não encontrado.' }); const payload = normalize(req.body); if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' }); const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip); if (existingIp && existingIp.id !== id) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' }); const node = await repo.updateNode(id, { ...current, ...payload, is_self: current.is_self }); res.json({ node }); });
router.delete('/nodes/:id', async (req, res) => { const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' }); if (node.is_self) return res.status(400).json({ message: 'Não é permitido remover o servidor atual.' }); await repo.deleteNode(id); res.json({ ok: true }); });
router.post('/nodes/:id/healthcheck', async (req, res) => { const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' }); const updated = await healthService.checkNode(node); const ok = updated.status === 'ONLINE'; res.status(ok ? 200 : 503).json({ ok, status: updated.status, node: updated, error: ok ? null : updated.healthcheck_error }); });
router.post('/healthcheck-all', async (_req, res) => res.json(await healthService.checkAllNodes()));

router.get('/join-requests', async (req, res) => {
  const status = String(req.query.status || '').toUpperCase();
  const filterStatus = JOIN_STATUSES.includes(status) ? status : null;
  const requests = await repo.listJoinRequests(filterStatus);
  res.json({ ok: true, requests });
});

router.post('/join-requests/:id/approve', async (req, res) => {
  const id = Number(req.params.id);
  const request = await repo.findJoinRequestById(id);
  if (!request) return res.status(404).json({ ok: false, message: 'Solicitação não encontrada.' });
  if (request.status === 'APPROVED') return res.json({ ok: true, message: 'Solicitação já aprovada.' });
  if (request.status === 'REJECTED') return res.status(409).json({ ok: false, message: 'Solicitação já rejeitada.' });

  let node = await repo.findByTailscaleIp(request.tailscale_ip);
  if (!node) {
    node = await repo.createNode({ node_name: request.node_name, tailscale_ip: request.tailscale_ip, public_url: request.public_url, role: request.requested_role === 'HOST' ? 'STANDBY' : normalizeRequestedRole(request.requested_role), status: 'UNKNOWN', is_self: 0, last_heartbeat_at: null, last_healthcheck_at: null, healthcheck_error: null, metadata: request.requester_metadata || null });
    console.log('[join-request] servidor criado em cluster_nodes');
  } else {
    node = await repo.updateNode(node.id, { ...node, node_name: request.node_name, tailscale_ip: request.tailscale_ip, public_url: request.public_url, role: request.requested_role === 'HOST' ? 'STANDBY' : normalizeRequestedRole(request.requested_role), status: 'UNKNOWN', is_self: 0 });
  }
  await repo.approveJoinRequest(id, node.id);
  console.log('[join-request] solicitação aprovada');
  res.json({ ok: true, node });
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
    public_url: normalizeUrl(self.public_url),
    requested_role: resolveRequestedRoleForJoin(),
    session_secret: env.sessionSecret,
    metadata: {
      port: process.env.PORT || null,
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
        for (const node of (bootstrapData.nodes || [])) {
          if (!node?.tailscale_ip || node.is_self || node.tailscale_ip === self.tailscale_ip) continue;
          await repo.upsertNodeByTailscaleIp({
            node_name: node.node_name,
            tailscale_ip: node.tailscale_ip,
            public_url: normalizeUrl(node.public_url),
            role: node.role || 'UNKNOWN',
            status: node.status || 'UNKNOWN',
            is_self: 0,
            last_heartbeat_at: node.last_heartbeat_at || null,
            last_healthcheck_at: node.last_healthcheck_at || null,
            healthcheck_error: node.healthcheck_error || null,
            metadata: node.metadata || null
          });
        }
      }
    }

    return res.json(data);
  } catch (error) {
    console.error(`[request-join-host] erro ao conectar no host: ${error.message}`);
    return res.status(502).json({ ok: false, message: 'Falha ao conectar no host informado.' });
  }
});

module.exports = router;
