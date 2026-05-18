const express = require('express');
const repo = require('../services/cluster-node.repository');
const healthService = require('../services/cluster-health.service');

const router = express.Router();
const ROLES = ['HOST', 'STANDBY', 'UNKNOWN'];
const STATUSES = ['ONLINE', 'OFFLINE', 'UNKNOWN'];

const normalize = (body = {}) => ({
  node_name: String(body.node_name || '').trim(),
  tailscale_ip: String(body.tailscale_ip || '').trim(),
  public_url: String(body.public_url || '').trim() || null,
  role: ROLES.includes(body.role) ? body.role : 'UNKNOWN',
  status: STATUSES.includes(body.status) ? body.status : 'UNKNOWN'
});

router.get('/self', async (_req, res) => {
  const node = await repo.getSelfNode();
  res.json({ configured: Boolean(node), node: node || null });
});

router.post('/self', async (req, res) => {
  const payload = normalize(req.body);
  if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' });

  const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip);
  const selfNode = await repo.getSelfNode();
  if (existingIp && (!selfNode || existingIp.id !== selfNode.id)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' });

  const now = new Date();
  await repo.clearSelfFlag();
  let node;
  if (selfNode) {
    node = await repo.updateNode(selfNode.id, { ...selfNode, ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null });
  } else {
    node = await repo.createNode({ ...payload, is_self: 1, status: 'ONLINE', last_heartbeat_at: now, last_healthcheck_at: now, healthcheck_error: null, metadata: null });
  }
  res.json({ configured: true, node });
});

router.get('/nodes', async (_req, res) => res.json({ nodes: await repo.getAllNodes() }));
router.post('/nodes', async (req, res) => {
  const payload = normalize(req.body);
  if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' });
  if (await repo.findByTailscaleIp(payload.tailscale_ip)) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' });
  const node = await repo.createNode({ ...payload, is_self: 0, status: 'UNKNOWN', last_heartbeat_at: null, last_healthcheck_at: null, healthcheck_error: null, metadata: null });
  res.status(201).json({ node });
});

router.put('/nodes/:id', async (req, res) => {
  const id = Number(req.params.id); const current = await repo.findById(id); if (!current) return res.status(404).json({ message: 'Nó não encontrado.' });
  const payload = normalize(req.body);
  if (!payload.node_name || !payload.tailscale_ip) return res.status(400).json({ message: 'node_name e tailscale_ip são obrigatórios.' });
  const existingIp = await repo.findByTailscaleIp(payload.tailscale_ip);
  if (existingIp && existingIp.id !== id) return res.status(409).json({ message: 'tailscale_ip já cadastrado.' });
  const node = await repo.updateNode(id, { ...current, ...payload, is_self: current.is_self });
  res.json({ node });
});

router.delete('/nodes/:id', async (req, res) => {
  const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' });
  if (node.is_self) return res.status(400).json({ message: 'Não é permitido remover o servidor atual.' });
  await repo.deleteNode(id); res.json({ ok: true });
});

router.post('/nodes/:id/healthcheck', async (req, res) => {
  const id = Number(req.params.id); const node = await repo.findById(id); if (!node) return res.status(404).json({ message: 'Nó não encontrado.' });
  const updated = await healthService.checkNode(node);
  const ok = updated.status === 'ONLINE';
  res.status(ok ? 200 : 503).json({ ok, status: updated.status, node: updated, error: ok ? null : updated.healthcheck_error });
});

router.post('/healthcheck-all', async (_req, res) => res.json(await healthService.checkAllNodes()));

module.exports = router;
