const crypto = require('crypto');
const pool = require('../database/connection');
const syncEventService = require('../services/sync-event.service');
const syncPayloadService = require('../services/sync-payload.service');

const toNullableNumber = (value) => (value == null ? null : Number(value));

const parse = (row) => row && ({
  ...row,
  current_value: Number(row.current_value),
  warning_level: toNullableNumber(row.warning_level),
  critical_level: toNullableNumber(row.critical_level),
  measurement_unit: row.measurement_unit || row.unit || 'm'
});

const findAll = async ({ status, severity, limit = 100 } = {}) => {
  const where = [];
  const params = [];
  if (status) { where.push('a.status = ?'); params.push(status); }
  if (severity) { where.push('a.severity = ?'); params.push(severity); }
  const safeLimit = Math.min(Math.max(Number(limit) || 100, 1), 200);
  const [rows] = await pool.execute(
    `SELECT a.*, dp.name AS data_point_name, dp.city_region, dp.warning_level, dp.critical_level, dp.measurement_unit
     FROM alerts a
     JOIN data_points dp ON dp.id = a.data_point_id
     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
     ORDER BY a.detected_at DESC, a.id DESC
     LIMIT ${safeLimit}`,
    params
  );
  return rows.map(parse);
};

const create = async (payload) => {
  const [result] = await pool.execute(
    `INSERT INTO alerts (uuid, data_point_id, measurement_id, alert_type, severity, current_value, unit, message, status, detected_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)`,
    [payload.uuid || crypto.randomUUID(), payload.data_point_id, payload.measurement_id || null, payload.alert_type, payload.severity, payload.current_value, payload.unit || 'm', payload.message, payload.detected_at]
  );
  const syncPayload = await syncPayloadService.getAlertPayloadById(result.insertId);
  await syncEventService.createEntitySyncEvent('alert', syncPayload);
  return result.insertId;
};

const resolve = async (id) => {
  await pool.execute("UPDATE alerts SET status = 'RESOLVED', resolved_at = NOW() WHERE id = ?", [id]);
  const [rows] = await pool.execute('SELECT * FROM alerts WHERE id = ? LIMIT 1', [id]);
  const alert = parse(rows[0]);
  if (alert) {
    const syncPayload = await syncPayloadService.getAlertPayloadById(id);
    await syncEventService.createEntitySyncEvent('alert', syncPayload, 'RESOLVE');
  }
  return alert;
};

module.exports = { findAll, create, resolve };
