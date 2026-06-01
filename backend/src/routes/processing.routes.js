const express = require('express');
const env = require('../config/env');
const chartService = require('../services/historical-chart.service');

const router = express.Router();

router.post('/chart-jobs/accept', async (req, res, next) => {
  try {
    const secret = req.header('x-cluster-secret');
    if (!env.sessionSecret || secret !== env.sessionSecret) return res.status(403).json({ ok: false, message: 'Secret inválido.' });
    const { data_point_id, import_id } = req.body || {};
    if (!data_point_id) return res.status(400).json({ ok: false, message: 'data_point_id é obrigatório.' });
    const result = await chartService.regenerateChart(data_point_id, import_id || null);
    res.status(202).json(result);
  } catch (error) { next(error); }
});

module.exports = router;
