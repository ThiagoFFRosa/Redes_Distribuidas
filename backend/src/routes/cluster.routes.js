const express = require('express');
const env = require('../config/env');
const clusterService = require('../services/cluster.service');
const clusterNodesService = require('../services/cluster-nodes.service');

const router = express.Router();

const requireClusterKey = (req, res, next) => {
  if (!env.clusterKey) {
    return res.status(500).json({ message: 'CLUSTER_KEY não configurada.' });
  }

  const key = req.header('x-cluster-key');
  if (!key || key !== env.clusterKey) {
    return res.status(401).json({ message: 'Não autorizado.' });
  }

  return next();
};


router.get('/public-health', (req, res) => {
  const local = clusterService.getLocalState();
  res.json({
    ok: true,
    serverName: local.serverName,
    role: local.role,
    app: 'cluster-mvp'
  });
});

router.use(requireClusterKey);

router.get('/health', (req, res) => {
  const local = clusterService.getLocalState();
  res.json({
    ok: true,
    serverName: local.serverName,
    serverUrl: local.serverUrl,
    role: local.role,
    publicUrl: local.publicUrl,
    time: new Date().toISOString()
  });
});

router.get('/handshake', (req, res) => {
  const local = clusterService.getLocalState();

  res.json({
    ok: true,
    serverName: local.serverName,
    serverUrl: local.serverUrl,
    role: local.role,
    publicUrl: local.publicUrl,
    clusterMode: local.clusterMode,
    switchTarget: local.switchTarget,
    clusterKeyAccepted: true
  });
});

router.post('/switch-mode', (req, res) => {
  const { clusterMode, switchTarget, oldHostUrl } = req.body || {};
  if (clusterMode !== 'NORMAL' && clusterMode !== 'SWITCHING') {
    return res.status(400).json({ ok: false, message: 'clusterMode inválido.' });
  }

  clusterService.setClusterMode(clusterMode, switchTarget || null, oldHostUrl || null);
  return res.json({
    ok: true,
    clusterMode,
    switchTarget: switchTarget || null,
    oldHostUrl: oldHostUrl || null
  });
});

router.get('/nodes', (req, res) => {
  res.json({
    ok: true,
    nodes: clusterService.getNodes()
  });
});

router.post('/nodes/add', (req, res) => {
  const { serverName, serverUrl } = req.body || {};

  if (!serverName || !serverUrl) {
    return res.status(400).json({ message: 'serverName e serverUrl são obrigatórios.' });
  }

  if (!clusterNodesService.isValidClusterUrl(serverUrl)) {
    return res.status(400).json({ message: clusterNodesService.getInvalidClusterUrlMessage() });
  }

  const addedNode = clusterService.upsertNode({
    serverName,
    serverUrl,
    addedAt: new Date().toISOString(),
    lastSeen: null
  });

  return res.json({
    ok: true,
    node: addedNode,
    nodes: clusterService.getNodes()
  });
});

router.post('/nodes/replace', (req, res) => {
  const { nodes } = req.body || {};

  if (!Array.isArray(nodes)) {
    return res.status(400).json({ message: 'nodes deve ser um array.' });
  }

  const merged = clusterService.mergeAndReplaceNodes(nodes);

  setImmediate(() => {
    clusterService.refreshPeers().catch((error) => {
      console.error('[cluster] falha no refresh após replace:', error.message);
    });
  });

  return res.json({
    ok: true,
    nodes: merged
  });
});

router.post('/become-host', async (req, res) => {
  const data = await clusterService.makeLocalHost();
  if (data.ok) {
    return res.json(data);
  }

  return res.status(503).json({
    ok: false,
    serverName: data.serverName,
    role: 'STANDBY',
    publicUrl: null,
    error: 'Falha ao iniciar ngrok após 3 tentativas'
  });
});

router.post('/promote', async (req, res) => {
  const data = await clusterService.promoteToHostManually(req.body || {});
  if (data.ok) {
    return res.json(data);
  }

  return res.status(503).json({
    ok: false,
    serverName: data.serverName,
    role: 'STANDBY',
    publicUrl: null,
    error: data.error || 'Falha ao iniciar ngrok após 3 tentativas'
  });
});

router.post('/become-standby', async (req, res) => {
  const data = await clusterService.makeLocalStandby();
  res.json(data);
});

router.get('/state', (req, res) => {
  res.json(clusterService.getLocalState());
});

module.exports = router;
