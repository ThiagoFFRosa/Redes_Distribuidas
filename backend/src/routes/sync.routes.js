const express = require('express');
const env = require('../config/env');
const coordinator = require('../services/sync-coordinator.service');
const applyService = require('../services/sync-apply.service');
const syncWorker = require('../services/sync-worker');
const syncEventService = require('../services/sync-event.service');
const { requireAuth } = require('../services/auth.service');

const router = express.Router();

const requireClusterSecret = (req, res, next) => {
  const secret = req.header('x-cluster-secret');
  if (!env.sessionSecret || secret !== env.sessionSecret) return res.status(403).json({ ok: false, message: 'Secret inválido.' });
  next();
};

const requireClusterSecretOrAuth = (req, res, next) => {
  const secret = req.header('x-cluster-secret');
  if (env.sessionSecret && secret === env.sessionSecret) return next();
  return requireAuth(req, res, next);
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
  try {
    const events = req.body?.events;
    if (!Array.isArray(events)) return res.status(400).json({ ok: false, error: 'events precisa ser array.' });
    if (events.length > env.syncBatchSize) {
      return res.status(413).json({ ok: false, error: 'Lote de eventos muito grande.', limit: env.syncBatchSize });
    }
    return res.json(await applyService.applySyncEvents(events));
  } catch (error) { return next(error); }
});

router.post('/pull-from-node', requireClusterSecret, async (req, res, next) => {
  try { res.json(await coordinator.pullFromNode(req.body || {})); } catch (error) { next(error); }
});

router.post('/push-to-node', requireClusterSecret, async (req, res, next) => {
  try { res.json(await coordinator.pushToNode(req.body || {})); } catch (error) { next(error); }
});

router.get('/bootstrap/manifest', requireClusterSecret, async (_req, res, next) => {
  try { res.json(await coordinator.getBootstrapManifest()); } catch (error) { next(error); }
});

router.get('/bootstrap/export', requireClusterSecret, async (req, res, next) => {
  try { res.json(await coordinator.exportBootstrapEntity(req.query || {})); } catch (error) { if (error.statusCode) return res.status(error.statusCode).json({ ok: false, message: error.message }); next(error); }
});

router.get('/fingerprint', requireClusterSecretOrAuth, async (_req, res, next) => {
  try { res.json(await coordinator.getFingerprint()); } catch (error) { next(error); }
});

router.post('/full-bootstrap', requireAuth, async (req, res, next) => {
  try { res.json(await coordinator.startFullBootstrap(req.body || {})); } catch (error) { next(error); }
});

router.get('/full-bootstrap/status', requireAuth, async (_req, res, next) => {
  try { res.json(await coordinator.getFullBootstrapStatus()); } catch (error) { next(error); }
});

router.get('/compare', requireAuth, async (req, res, next) => {
  try { res.json(await coordinator.compareFingerprint(req.query || {})); } catch (error) { next(error); }
});

router.get('/status', requireAuth, async (_req, res, next) => {
  try { res.json(await coordinator.getStatus()); } catch (error) { next(error); }
});

router.post('/run-now', requireAuth, async (_req, res, next) => {
  try { res.json(await syncWorker.runCycle()); } catch (error) { next(error); }
});

router.post('/backfill', requireAuth, async (req, res, next) => {
  try {
    const dryRun = req.body?.dry_run !== false;
    res.json(await syncEventService.backfillExistingSyncEvents({ dryRun }));
  } catch (error) { next(error); }
});

module.exports = router;
