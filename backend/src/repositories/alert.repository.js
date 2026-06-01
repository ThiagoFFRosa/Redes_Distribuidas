const pool = require('../database/connection');

const parse = (row) => row && ({ ...row, current_value: Number(row.current_value) });

const findAll = async ({ status, severity, limit = 100 } = {}) => {
  const where = [];
  const params = [];
  if (status) { where.push('a.status = ?'); params.push(status); }
  if (severity) { where.push('a.severity = ?'); params.push(severity); }
  const safeLimit = Math.min(Math.max(Number(limit) || 100, 1), 200);
  const [rows] = await pool.execute(
    `SELECT a.*, dp.name AS data_point_name, dp.city_region
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
    `INSERT INTO alerts (data_point_id, measurement_id, alert_type, severity, current_value, unit, message, status, detected_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)`,
    [payload.data_point_id, payload.measurement_id || null, payload.alert_type, payload.severity, payload.current_value, payload.unit || 'm', payload.message, payload.detected_at]
  );
  return result.insertId;
};

const resolve = async (id) => {
  await pool.execute("UPDATE alerts SET status = 'RESOLVED', resolved_at = NOW() WHERE id = ?", [id]);
  const [rows] = await pool.execute('SELECT * FROM alerts WHERE id = ? LIMIT 1', [id]);
  return parse(rows[0]);
};

module.exports = { findAll, create, resolve };
