const express = require('express');
const { runInmetEndpointTests } = require('../services/inmet-test.service');

const router = express.Router();

const getTodayDate = () => new Date().toISOString().slice(0, 10);

router.get('/test', async (req, res, next) => {
  const stationCode = (req.query.station || 'A769').toString().trim() || 'A769';
  const date = (req.query.date || getTodayDate()).toString().trim() || getTodayDate();

  try {
    const payload = await runInmetEndpointTests({ stationCode, date });
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
