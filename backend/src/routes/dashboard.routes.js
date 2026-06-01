const express = require('express');
const dashboardService = require('../services/dashboard.service');

const router = express.Router();

router.get('/summary', async (_req, res, next) => {
  try { res.json(await dashboardService.getSummary()); } catch (error) { next(error); }
});

module.exports = router;
