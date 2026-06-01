const express = require('express');
const repository = require('../repositories/data-point.repository');
const { requireAuth } = require('../services/auth.service');

const router = express.Router();
const allowedTypes = new Set(['RIVER_LEVEL']);
const allowedStatuses = new Set(['ACTIVE', 'INACTIVE']);

const validatePayload = (body) => {
  const payload = {
    name: (body.name || '').trim(),
    type: body.type || 'RIVER_LEVEL',
    latitude: Number(body.latitude),
    longitude: Number(body.longitude),
    city_region: body.city_region || null,
    description: body.description || null,
    status: body.status || 'ACTIVE'
  };
  if (!payload.name) return { error: 'name é obrigatório.' };
  if (!Number.isFinite(payload.latitude)) return { error: 'latitude obrigatória e numérica.' };
  if (!Number.isFinite(payload.longitude)) return { error: 'longitude obrigatória e numérica.' };
  if (!allowedTypes.has(payload.type)) return { error: 'type inválido.' };
  if (!allowedStatuses.has(payload.status)) return { error: 'status inválido.' };
  return { payload };
};

router.get('/', async (req, res, next) => {
  try { res.json({ ok: true, data: await repository.findAll(req.query) }); } catch (error) { next(error); }
});

router.get('/:id', async (req, res, next) => {
  try {
    const point = await repository.findById(req.params.id);
    if (!point) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    res.json({ ok: true, data: point });
  } catch (error) { next(error); }
});

router.post('/', requireAuth, async (req, res, next) => {
  try {
    const { payload, error } = validatePayload(req.body || {});
    if (error) return res.status(400).json({ ok: false, message: error });
    payload.created_by_user_id = req.user?.id || req.session?.user?.id || null;
    res.status(201).json({ ok: true, data: await repository.create(payload) });
  } catch (error) { next(error); }
});

router.put('/:id', requireAuth, async (req, res, next) => {
  try {
    const exists = await repository.findById(req.params.id);
    if (!exists) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    const { payload, error } = validatePayload({ ...exists, ...(req.body || {}) });
    if (error) return res.status(400).json({ ok: false, message: error });
    res.json({ ok: true, data: await repository.update(req.params.id, payload) });
  } catch (error) { next(error); }
});

router.delete('/:id', requireAuth, async (req, res, next) => {
  try {
    const point = await repository.setStatus(req.params.id, 'INACTIVE');
    if (!point) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    res.json({ ok: true, data: point });
  } catch (error) { next(error); }
});

module.exports = router;
