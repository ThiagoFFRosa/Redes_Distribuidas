const pool = require('../database/connection');

const toNullableNumber = (value) => (value == null ? null : Number(value));

const toApi = (row) => row && ({
  ...row,
  latitude: Number(row.latitude),
  longitude: Number(row.longitude),
  normal_level: toNullableNumber(row.normal_level),
  warning_level: toNullableNumber(row.warning_level),
  critical_level: toNullableNumber(row.critical_level),
  measurement_unit: row.measurement_unit || 'm'
});

const findAll = async ({ status, type } = {}) => {
  const where = [];
  const params = [];
  if (status) { where.push('status = ?'); params.push(status); }
  if (type) { where.push('type = ?'); params.push(type); }
  const [rows] = await pool.execute(
    `SELECT * FROM data_points ${where.length ? `WHERE ${where.join(' AND ')}` : ''} ORDER BY name ASC`,
    params
  );
  return rows.map(toApi);
};

const findById = async (id) => {
  const [rows] = await pool.execute('SELECT * FROM data_points WHERE id = ? LIMIT 1', [id]);
  return toApi(rows[0]);
};

const create = async (payload) => {
  const [result] = await pool.execute(
    `INSERT INTO data_points (name, type, latitude, longitude, city_region, description, status, normal_level, warning_level, critical_level, measurement_unit, created_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.name,
      payload.type || 'RIVER_LEVEL',
      payload.latitude,
      payload.longitude,
      payload.city_region || null,
      payload.description || null,
      payload.status || 'ACTIVE',
      payload.normal_level ?? null,
      payload.warning_level ?? null,
      payload.critical_level ?? null,
      payload.measurement_unit || 'm',
      payload.created_by_user_id || null
    ]
  );
  return findById(result.insertId);
};

const update = async (id, payload) => {
  await pool.execute(
    `UPDATE data_points
        SET name = ?, type = ?, latitude = ?, longitude = ?, city_region = ?, description = ?, status = ?,
            normal_level = ?, warning_level = ?, critical_level = ?, measurement_unit = ?
      WHERE id = ?`,
    [
      payload.name,
      payload.type || 'RIVER_LEVEL',
      payload.latitude,
      payload.longitude,
      payload.city_region || null,
      payload.description || null,
      payload.status || 'ACTIVE',
      payload.normal_level ?? null,
      payload.warning_level ?? null,
      payload.critical_level ?? null,
      payload.measurement_unit || 'm',
      id
    ]
  );
  return findById(id);
};

const setStatus = async (id, status) => {
  await pool.execute('UPDATE data_points SET status = ? WHERE id = ?', [status, id]);
  return findById(id);
};

const countActive = async () => {
  const [rows] = await pool.execute("SELECT COUNT(*) total FROM data_points WHERE status = 'ACTIVE'");
  return Number(rows[0]?.total || 0);
};

module.exports = { findAll, findById, create, update, setStatus, countActive };
