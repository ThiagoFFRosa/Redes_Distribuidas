const express = require('express');
const dataPointRepository = require('../repositories/data-point.repository');
const measurementRepository = require('../repositories/measurement.repository');

const router = express.Router();

const calculateRiskStatus = (point) => {
  if (point.status === 'INACTIVE') return 'INACTIVE';
  const latest = point.latest_measurement;
  if (!latest) return 'UNKNOWN';
  if (point.warning_level == null || point.critical_level == null) return 'UNKNOWN';
  const value = Number(latest.value);
  if (!Number.isFinite(value)) return 'UNKNOWN';
  if (point.critical_level != null && value >= Number(point.critical_level)) return 'CRITICAL';
  if (point.warning_level != null && value >= Number(point.warning_level)) return 'ATTENTION';
  return 'NORMAL';
};

const withRiskStatus = (point) => ({
  ...point,
  risk_status: calculateRiskStatus(point)
});

router.get('/monitoring-points', async (_req, res, next) => {
  try {
    const points = await dataPointRepository.findAllWithLatestMeasurement();
    res.json({ ok: true, data: points.map(withRiskStatus) });
  } catch (error) { next(error); }
});

router.get('/monitoring-points/:id/measurements', async (req, res, next) => {
  try {
    const point = await dataPointRepository.findById(req.params.id);
    if (!point) return res.status(404).json({ ok: false, message: 'Ponto não encontrado.' });
    const measurements = await measurementRepository.findLatestByDataPointAsc(req.params.id, req.query.limit || 12);
    res.json({ ok: true, point, measurements: measurements.map((measurement) => ({
      id: measurement.id,
      value: measurement.value,
      unit: measurement.unit,
      measured_at: measurement.measured_at
    })) });
  } catch (error) { next(error); }
});

module.exports = router;
module.exports.calculateRiskStatus = calculateRiskStatus;
