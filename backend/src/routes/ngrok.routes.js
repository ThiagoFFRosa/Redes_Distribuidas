const express = require('express');
const ngrokService = require('../services/ngrok.service');
const clusterService = require('../services/cluster.service');
const ngrokCoordinator = require('../services/ngrok-coordinator.service');

const router = express.Router();

router.get('/status', async (_req, res) => {
  const local = clusterService.getLocalState();
  const status = await ngrokCoordinator.getStatus().catch(() => null);
  res.json({
    role: local.role,
    publicUrl: ngrokService.getPublicUrl() || status?.public_url || null,
    isHostingPublicFrontend: Boolean(status?.ngrok_online && status?.owner_node_uuid),
    cluster: status
  });
});

module.exports = router;
