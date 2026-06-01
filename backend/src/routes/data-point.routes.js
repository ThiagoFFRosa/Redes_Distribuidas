const express = require('express');
const repository = require('../repositories/data-point.repository');
const { requireAuth } = require('../services/auth.service');

const router = express.Router();
const allowedTypes = new Set(['RIVER_LEVEL']);
const allowedStatuses = new Set(['ACTIVE', 'INACTIVE']);
const historicalChartService = require('../services/historical-chart.service');

const optionalNumber = (value) => {
  if (value === undefined || value === null || value === '') return { value: null };
  const numberValue = Number(value);
  if (!Number.isFinite(numberValue) || numberValue < 0) return { error: 'Os níveis devem ser valores numéricos positivos.' };
  return { value: numberValue };
};

const validatePayload = (body) => {
  const normal = optionalNumber(body.normal_level);
  const warning = optionalNumber(body.warning_level);
  const critical = optionalNumber(body.critical_level);

  if (normal.error) return { error: normal.error };
  if (warning.error) return { error: warning.error };
  if (critical.error) return { error: critical.error };
  if (warning.value !== null && critical.value !== null && critical.value <= warning.value) {
    return { error: 'O nível crítico deve ser maior que o nível de risco.' };
  }
  if (normal.value !== null && warning.value !== null && warning.value <= normal.value) {
    return { error: 'O nível de risco deve ser maior que o nível normal.' };
  }

  const latitudeMissing = body.latitude === undefined || body.latitude === null || body.latitude === '';
  const longitudeMissing = body.longitude === undefined || body.longitude === null || body.longitude === '';
  if (latitudeMissing || longitudeMissing) return { error: 'Latitude e longitude são obrigatórias.' };

  const payload = {
    name: (body.name || '').trim(),
    type: body.type || 'RIVER_LEVEL',
    latitude: Number(body.latitude),
    longitude: Number(body.longitude),
    city_region: body.city_region || null,
    description: body.description || null,
    status: body.status || 'ACTIVE',
    normal_level: normal.value,
    warning_level: warning.value,
    critical_level: critical.value,
    measurement_unit: (body.measurement_unit || 'm').trim() || 'm'
  };
  if (!payload.name) return { error: 'name é obrigatório.' };
  if (!Number.isFinite(payload.latitude) || !Number.isFinite(payload.longitude)) return { error: 'Latitude e longitude são obrigatórias.' };
  if (!allowedTypes.has(payload.type)) return { error: 'type inválido.' };
  if (!allowedStatuses.has(payload.status)) return { error: 'status inválido.' };
  return { payload };
};

router.get('/', async (req, res, next) => {
  try { res.json({ ok: true, data: await repository.findAll(req.query) }); } catch (error) { next(error); }
});


router.get('/:id/historical-chart', async (req, res, next) => {
  try {
    const result = await historicalChartService.getHistoricalChart(req.params.id);
    if (!result) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    res.json(result);
  } catch (error) { next(error); }
});

router.post('/:id/historical-chart/regenerate', requireAuth, async (req, res, next) => {
  try {
    const result = await historicalChartService.regenerateChart(req.params.id);
    if (!result) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    res.status(202).json(result);
  } catch (error) { next(error); }
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

router.post('/:id/reactivate', requireAuth, async (req, res, next) => {
  try {
    const point = await repository.setStatus(req.params.id, 'ACTIVE');
    if (!point) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    res.json({ ok: true, message: 'Ponto reativado com sucesso.', data: point });
  } catch (error) { next(error); }
});

router.delete('/:id', requireAuth, async (req, res, next) => {
  try {
    const point = await repository.setStatus(req.params.id, 'INACTIVE');
    if (!point) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    res.json({ ok: true, message: 'Ponto desativado com sucesso.', data: point });
  } catch (error) { next(error); }
});

module.exports = router;
