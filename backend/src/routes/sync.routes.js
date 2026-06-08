const express = require('express');
const env = require('../config/env');
const coordinator = require('../services/sync-coordinator.service');
const applyService = require('../services/sync-apply.service');
const syncWorker = require('../services/sync-worker');
const syncEventService = require('../services/sync-event.service');
const { requireAuth } = require('../services/auth.service');
const pool = require('../database/connection');

const router = express.Router();

const requireClusterSecret = (req, res, next) => {
  const secret = req.header('x-cluster-secret') || req.header('x-cluster-key');
  const accepted = [env.sessionSecret, env.clusterKey].filter(Boolean);
  if (!accepted.length || !accepted.includes(secret)) return res.status(403).json({ ok: false, message: 'Secret inválido.' });
  next();
};

const requireClusterSecretOrAuth = (req, res, next) => {
  const secret = req.header('x-cluster-secret') || req.header('x-cluster-key');
  if ([env.sessionSecret, env.clusterKey].filter(Boolean).includes(secret)) return next();
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


router.get('/diagnostics', requireAuth, async (_req, res, next) => {
  try {
    const [[orphanChartCache]] = await pool.execute(`SELECT COUNT(*) AS total FROM chart_cache cc LEFT JOIN data_points dp ON dp.uuid=cc.data_point_uuid WHERE dp.id IS NULL`);
    const [[orphanHistoricalMeasurements]] = await pool.execute(`SELECT COUNT(*) AS total FROM historical_measurements hm LEFT JOIN data_points dp ON dp.id=hm.data_point_id WHERE dp.id IS NULL`);
    const [duplicateDataPoints] = await pool.execute(`
      SELECT COALESCE(source_key, CONCAT(LOWER(TRIM(name)), '|', COALESCE(LOWER(TRIM(city_region)), ''), '|', type)) AS duplicate_key,
             COUNT(*) AS total, GROUP_CONCAT(uuid ORDER BY id SEPARATOR ',') AS uuids
        FROM data_points
       GROUP BY duplicate_key
      HAVING COUNT(*) > 1
      ORDER BY total DESC, duplicate_key ASC
      LIMIT 50`);
    const [pendingDeferredEvents] = await pool.execute(`
      SELECT event_uuid, target_node_uuid, last_error, status, attempts, updated_at
        FROM sync_event_deliveries
       WHERE status IN ('PENDING','FAILED')
         AND (last_error LIKE '%missing%' OR last_error LIKE '%dependency%' OR last_error LIKE '%uuid%')
       ORDER BY updated_at DESC
       LIMIT 50`).catch(() => [[]]);
    const [deliveryErrors] = await pool.execute(`
      SELECT target_node_uuid, status, last_error, attempts, updated_at
        FROM sync_event_deliveries
       WHERE status='FAILED'
       ORDER BY updated_at DESC
       LIMIT 50`).catch(() => [[]]);
    const [missingChartDependencies] = await pool.execute(`
      SELECT cc.uuid, cc.data_point_uuid, cc.source_job_uuid
        FROM chart_cache cc LEFT JOIN data_points dp ON dp.uuid=cc.data_point_uuid
       WHERE dp.id IS NULL
       ORDER BY cc.updated_at DESC
       LIMIT 50`);
    res.json({
      ok: true,
      missing_dependencies: { chart_cache: missingChartDependencies },
      duplicate_data_points: duplicateDataPoints,
      pending_deferred_events: pendingDeferredEvents,
      orphan_chart_cache: { count: Number(orphanChartCache.total || 0) },
      orphan_historical_measurements: { count: Number(orphanHistoricalMeasurements.total || 0) },
      sync_delivery_errors: deliveryErrors
    });
  } catch (error) { next(error); }
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
