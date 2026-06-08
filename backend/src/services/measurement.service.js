const dataPointRepository = require('../repositories/data-point.repository');
const measurementRepository = require('../repositories/measurement.repository');
const alertRepository = require('../repositories/alert.repository');
const eventQueueRepository = require('../repositories/event-queue.repository');
const historicalChartService = require('./historical-chart.service');

const normalizeDateTime = (value) => {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 19).replace('T', ' ');
};

const createMeasurement = async (payload, userId = null) => {
  const dataPointId = Number(payload.data_point_id);
  const value = Number(payload.value);
  const measuredAt = normalizeDateTime(payload.measured_at);

  if (!Number.isInteger(dataPointId) || dataPointId <= 0) {
    const error = new Error('data_point_id é obrigatório.'); error.status = 400; throw error;
  }
  if (!Number.isFinite(value)) {
    const error = new Error('value deve ser numérico.'); error.status = 400; throw error;
  }
  if (!measuredAt) {
    const error = new Error('measured_at inválido.'); error.status = 400; throw error;
  }

  const point = await dataPointRepository.findById(dataPointId);
  if (!point) { const error = new Error('Ponto de dados não encontrado.'); error.status = 404; throw error; }

  await eventQueueRepository.create({
    event_type: 'MEASUREMENT_RECEIVED', status: 'RECEIVED', payload: { data_point_id: dataPointId, value }, message: 'medição recebida para processamento'
  });
  await eventQueueRepository.create({
    event_type: 'MEASUREMENT_VALIDATING', status: 'VALIDATING', payload: { data_point_id: dataPointId, value }, message: 'validando ponto e leitura'
  });

  const measurement = await measurementRepository.create({
    data_point_id: dataPointId,
    measurement_type: payload.measurement_type || 'RIVER_LEVEL',
    value,
    unit: point.measurement_unit || payload.unit || 'm',
    measured_at: measuredAt,
    source: payload.source || 'MANUAL',
    observation: payload.observation || null,
    created_by_user_id: userId
  });

  await historicalChartService.markChartCacheStale(point.uuid).catch((error) => {
    console.warn(`[historical-chart] falha ao marcar cache stale para data_point_uuid=${point.uuid}: ${error.message}`);
  });

  await eventQueueRepository.create({
    event_type: 'MEASUREMENT_PERSISTED', status: 'PERSISTED', payload: { measurement_id: measurement.id, data_point_id: dataPointId, value }, message: 'medição manual persistida', related_measurement_id: measurement.id, processed_at: measuredAt
  });

  let alert = null;
  let warningLevel = point.warning_level;
  let criticalLevel = point.critical_level;
  const hasConfiguredThresholds = warningLevel !== null && warningLevel !== undefined && criticalLevel !== null && criticalLevel !== undefined;

  if (!hasConfiguredThresholds) {
    console.warn('[alerts] ponto sem thresholds configurados, usando fallback genérico.');
    warningLevel = warningLevel ?? 3.5;
    criticalLevel = criticalLevel ?? 5.0;
  }

  const alertUnit = point.measurement_unit || payload.unit || 'm';

  if (criticalLevel !== null && criticalLevel !== undefined && value >= Number(criticalLevel)) {
    const alertId = await alertRepository.create({ data_point_id: dataPointId, measurement_id: measurement.id, alert_type: 'RIVER_LEVEL_CRITICAL', severity: 'CRITICAL', current_value: value, unit: alertUnit, message: 'Nível crítico detectado', detected_at: measuredAt });
    alert = { id: alertId, severity: 'CRITICAL', message: 'Nível crítico detectado' };
  } else if (warningLevel !== null && warningLevel !== undefined && value >= Number(warningLevel)) {
    const alertId = await alertRepository.create({ data_point_id: dataPointId, measurement_id: measurement.id, alert_type: 'RIVER_LEVEL_HIGH', severity: 'ATTENTION', current_value: value, unit: alertUnit, message: 'Nível acima do limite de risco', detected_at: measuredAt });
    alert = { id: alertId, severity: 'ATTENTION', message: 'Nível acima do limite de risco' };
  }

  await eventQueueRepository.create({
    event_type: alert ? 'ALERT_CREATED' : 'MEASUREMENT_PROCESSED', status: 'PROCESSED', payload: { measurement_id: measurement.id, alert }, message: alert ? 'alerta operacional gerado' : 'medição processada sem alerta', related_measurement_id: measurement.id, processed_at: measuredAt
  });

  return { measurement, alert };
};

module.exports = { createMeasurement };
