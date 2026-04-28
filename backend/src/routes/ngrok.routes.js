const express = require('express');
const ngrokService = require('../services/ngrok.service');
const clusterService = require('../services/cluster.service');

const router = express.Router();

router.get('/status', (req, res) => {
  const local = clusterService.getLocalState();
  res.json({
    role: local.role,
    publicUrl: ngrokService.getPublicUrl(),
    isHostingPublicFrontend: local.role === 'HOST' && Boolean(local.publicUrl)
  });
});

module.exports = router;
