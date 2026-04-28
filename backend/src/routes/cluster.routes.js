const express = require('express');
const env = require('../config/env');
const clusterService = require('../services/cluster.service');

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
    clusterKeyAccepted: true
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
  res.json(data);
});

router.post('/become-standby', async (req, res) => {
  const data = await clusterService.makeLocalStandby();
  res.json(data);
});

router.get('/state', (req, res) => {
  res.json(clusterService.getLocalState());
});

module.exports = router;
