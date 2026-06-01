const express = require('express');
const env = require('../config/env');
const syncService = require('../services/sync.service');

const router = express.Router();

const requireClusterSecret = (req, res, next) => {
  const secret = req.header('x-cluster-secret');
  if (!env.sessionSecret || secret !== env.sessionSecret) return res.status(403).json({ ok: false, message: 'Secret inválido.' });
  next();
};

router.post('/apply', requireClusterSecret, async (req, res, next) => {
  try {
    const result = await syncService.applyIncomingEvents(req.body?.events || [], req.body?.source_node_id || null);
    res.json(result);
  } catch (error) { next(error); }
});

module.exports = router;
