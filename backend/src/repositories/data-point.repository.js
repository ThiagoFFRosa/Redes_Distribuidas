const crypto = require('crypto');
const pool = require('../database/connection');
const syncEventService = require('../services/sync-event.service');
const syncPayloadService = require('../services/sync-payload.service');

const toNullableNumber = (value) => (value == null ? null : Number(value));

const toApi = (row) => row && ({
  ...row,
  latitude: row.latitude == null ? null : Number(row.latitude),
  longitude: row.longitude == null ? null : Number(row.longitude),
  normal_level: toNullableNumber(row.normal_level),
  warning_level: toNullableNumber(row.warning_level),
  critical_level: toNullableNumber(row.critical_level),
  measurement_unit: row.measurement_unit || 'm'
});

const toApiWithLatestMeasurement = (row) => {
  if (!row) return null;
  const point = toApi(row);
  point.latest_measurement = row.latest_measurement_id ? {
    id: row.latest_measurement_id,
    value: Number(row.latest_measurement_value),
    unit: row.latest_measurement_unit || point.measurement_unit || 'm',
    measured_at: row.latest_measurement_measured_at
  } : null;
  return point;
};

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


const findAllWithLatestMeasurement = async () => {
  const [rows] = await pool.execute(
    `SELECT dp.*,
            m.id AS latest_measurement_id,
            m.value AS latest_measurement_value,
            m.unit AS latest_measurement_unit,
            m.measured_at AS latest_measurement_measured_at
       FROM data_points dp
       LEFT JOIN measurements m
         ON m.id = (
           SELECT m2.id
             FROM measurements m2
            WHERE m2.data_point_id = dp.id
            ORDER BY m2.measured_at DESC, m2.id DESC
            LIMIT 1
         )
      ORDER BY dp.name ASC`
  );
  return rows.map(toApiWithLatestMeasurement);
};

const create = async (payload) => {
  const [result] = await pool.execute(
    `INSERT INTO data_points (uuid, name, type, latitude, longitude, city_region, description, status, normal_level, warning_level, critical_level, measurement_unit, created_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      payload.uuid || crypto.randomUUID(),
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
  const point = await findById(result.insertId);
  const syncPayload = await syncPayloadService.getDataPointPayloadById(point.id);
  await syncEventService.createEntitySyncEvent('data_point', syncPayload);
  return point;
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
  const point = await findById(id);
  if (point) {
    const syncPayload = await syncPayloadService.getDataPointPayloadById(id);
    await syncEventService.createEntitySyncEvent('data_point', syncPayload);
  }
  return point;
};

const setStatus = async (id, status) => {
  await pool.execute('UPDATE data_points SET status = ? WHERE id = ?', [status, id]);
  const point = await findById(id);
  if (point) {
    const syncPayload = await syncPayloadService.getDataPointPayloadById(id);
    await syncEventService.createEntitySyncEvent('data_point', syncPayload, status === 'INACTIVE' ? 'SOFT_DELETE' : 'UPSERT');
  }
  return point;
};

const countActive = async () => {
  const [rows] = await pool.execute("SELECT COUNT(*) total FROM data_points WHERE status = 'ACTIVE'");
  return Number(rows[0]?.total || 0);
};

module.exports = { findAll, findById, findAllWithLatestMeasurement, create, update, setStatus, countActive };
