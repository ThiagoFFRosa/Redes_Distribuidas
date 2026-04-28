const express = require('express');
const clusterService = require('../services/cluster.service');

const router = express.Router();

const formatServersResponse = () => {
  const local = clusterService.getLocalState();
  const servers = clusterService.getKnownServers();

  return {
    currentServer: {
      serverName: local.serverName,
      serverUrl: local.serverUrl,
      role: local.role,
      publicUrl: local.publicUrl
    },
    servers
  };
};

router.get('/', async (req, res) => {
  await clusterService.refreshPeers();
  res.json(formatServersResponse());
});

router.post('/switch-host', async (req, res) => {
  const { targetUrl } = req.body;
  if (!targetUrl) {
    return res.status(400).json({ message: 'targetUrl é obrigatório.' });
  }

  try {
    await clusterService.switchHost(targetUrl);
    return res.json(formatServersResponse());
  } catch (error) {
    return res.status(400).json({ message: error.message });
  }
});

router.post('/become-host', async (req, res) => {
  const local = clusterService.getLocalState();
  await clusterService.switchHost(local.serverUrl);
  res.json(formatServersResponse());
});

router.post('/become-standby', async (req, res) => {
  await clusterService.makeLocalStandby();
  await clusterService.electHostIfNeeded();
  res.json(formatServersResponse());
});

module.exports = router;
