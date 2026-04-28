const express = require('express');
const clusterService = require('../services/cluster.service');

const router = express.Router();

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
