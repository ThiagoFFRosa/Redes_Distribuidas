const express = require('express');
const env = require('../config/env');
const coordinator = require('../services/sync-coordinator.service');
const applyService = require('../services/sync-apply.service');
const syncWorker = require('../services/sync-worker');
const { requireAuth } = require('../services/auth.service');

const router = express.Router();

const requireClusterSecret = (req, res, next) => {
  const secret = req.header('x-cluster-secret');
  if (!env.sessionSecret || secret !== env.sessionSecret) return res.status(403).json({ ok: false, message: 'Secret inválido.' });
  next();
};

router.get('/events', requireClusterSecret, async (req, res, next) => {
  try {
    const events = await coordinator.listEvents({ since: req.query.since || null, limit: req.query.limit || 500 });
    res.json({ ok: true, events, server_time: new Date().toISOString() });
  } catch (error) {
    if (error.statusCode) return res.status(error.statusCode).json({ ok: false, message: error.message });
    return next(error);
  }
});

router.post('/apply', requireClusterSecret, async (req, res, next) => {
  try { res.json(await applyService.applySyncEvents(req.body?.events || [])); } catch (error) { next(error); }
});

router.post('/pull-from-node', requireClusterSecret, async (req, res, next) => {
  try { res.json(await coordinator.pullFromNode(req.body || {})); } catch (error) { next(error); }
});

router.post('/push-to-node', requireClusterSecret, async (req, res, next) => {
  try { res.json(await coordinator.pushToNode(req.body || {})); } catch (error) { next(error); }
});

router.post('/full-bootstrap', requireClusterSecret, async (req, res, next) => {
  try { res.json(await coordinator.fullBootstrap(req.body || {})); } catch (error) { next(error); }
});

router.get('/status', requireAuth, async (_req, res, next) => {
  try { res.json(await coordinator.getStatus()); } catch (error) { next(error); }
});

router.post('/run-now', requireAuth, async (_req, res, next) => {
  try { res.json(await syncWorker.runCycle()); } catch (error) { next(error); }
});

module.exports = router;
