const express = require('express');
const repository = require('../repositories/measurement.repository');
const measurementService = require('../services/measurement.service');
const { requireAuth } = require('../services/auth.service');

const router = express.Router();

router.get('/', async (req, res, next) => {
  try { res.json({ ok: true, data: await repository.findAll(req.query) }); } catch (error) { next(error); }
});

router.get('/latest', async (req, res, next) => {
  try { res.json({ ok: true, data: await repository.findLatest(req.query.limit || 10) }); } catch (error) { next(error); }
});

router.get('/chart/river-level', async (req, res, next) => {
  try {
    const rows = await repository.chartRiverLevel(req.query.limit || 8);
    res.json({ ok: true, labels: rows.map((row) => row.label), values: rows.map((row) => row.value) });
  } catch (error) { next(error); }
});

router.post('/', requireAuth, async (req, res, next) => {
  try {
    const userId = req.user?.id || req.session?.user?.id || null;
    const data = await measurementService.createMeasurement(req.body || {}, userId);
    res.status(201).json({ ok: true, ...data });
  } catch (error) {
    if (error.status) return res.status(error.status).json({ ok: false, message: error.message });
    next(error);
  }
});

module.exports = router;
