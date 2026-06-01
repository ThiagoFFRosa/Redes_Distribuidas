const express = require('express');
const repository = require('../repositories/alert.repository');
const { requireAuth } = require('../services/auth.service');

const router = express.Router();

router.get('/', async (req, res, next) => {
  try { res.json({ ok: true, data: await repository.findAll(req.query) }); } catch (error) { next(error); }
});

router.post('/:id/resolve', requireAuth, async (req, res, next) => {
  try { res.json({ ok: true, data: await repository.resolve(req.params.id) }); } catch (error) { next(error); }
});

module.exports = router;
