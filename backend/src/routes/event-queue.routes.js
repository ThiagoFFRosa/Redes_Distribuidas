const express = require('express');
const repository = require('../repositories/event-queue.repository');
const { requireAuth } = require('../services/auth.service');

const router = express.Router();

router.get('/logs', async (req, res, next) => {
  try { res.json({ ok: true, data: await repository.findLatest(req.query.limit || 30) }); } catch (error) { next(error); }
});

router.post('/logs', requireAuth, async (req, res, next) => {
  try {
    const id = await repository.create(req.body || {});
    res.status(201).json({ ok: true, id });
  } catch (error) { next(error); }
});

module.exports = router;
