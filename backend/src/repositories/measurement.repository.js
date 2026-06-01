const crypto = require('crypto');
const pool = require('../database/connection');
const syncEventService = require('../services/sync-event.service');
const syncPayloadService = require('../services/sync-payload.service');

const parse = (row) => row && ({ ...row, value: Number(row.value), latitude: row.latitude == null ? null : Number(row.latitude), longitude: row.longitude == null ? null : Number(row.longitude) });

const findAll = async ({ data_point_id, limit = 50, from, to } = {}) => {
  const where = [];
  const params = [];
  if (data_point_id) { where.push('m.data_point_id = ?'); params.push(data_point_id); }
  if (from) { where.push('m.measured_at >= ?'); params.push(from); }
  if (to) { where.push('m.measured_at <= ?'); params.push(to); }
  const safeLimit = Math.min(Math.max(Number(limit) || 50, 1), 200);
  const [rows] = await pool.execute(
    `SELECT m.*, dp.name AS data_point_name, dp.city_region, dp.latitude, dp.longitude
     FROM measurements m
     JOIN data_points dp ON dp.id = m.data_point_id
     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
     ORDER BY m.measured_at DESC, m.id DESC
     LIMIT ${safeLimit}`,
    params
  );
  return rows.map(parse);
};

const findLatest = async (limit = 10) => findAll({ limit });

const findLatestByDataPointAsc = async (dataPointId, limit = 12) => {
  const safeLimit = Math.min(Math.max(Number(limit) || 12, 1), 100);
  const [rows] = await pool.execute(
    `SELECT recent.*
       FROM (
         SELECT id, data_point_id, measurement_type, value, unit, measured_at, source, observation, created_at
           FROM measurements
          WHERE data_point_id = ?
          ORDER BY measured_at DESC, id DESC
          LIMIT ${safeLimit}
       ) recent
      ORDER BY recent.measured_at ASC, recent.id ASC`,
    [dataPointId]
  );
  return rows.map(parse);
};


const create = async (payload) => {
  const [result] = await pool.execute(
    `INSERT INTO measurements (uuid, data_point_id, measurement_type, value, unit, measured_at, source, observation, created_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [payload.uuid || crypto.randomUUID(), payload.data_point_id, payload.measurement_type || 'RIVER_LEVEL', payload.value, payload.unit || 'm', payload.measured_at, payload.source || 'MANUAL', payload.observation || null, payload.created_by_user_id || null]
  );
  const [rows] = await pool.execute(`SELECT m.*, dp.name AS data_point_name, dp.city_region, dp.latitude, dp.longitude
     FROM measurements m
     JOIN data_points dp ON dp.id = m.data_point_id
     WHERE m.id = ? LIMIT 1`, [result.insertId]);
  const measurement = parse(rows[0]);
  const syncPayload = await syncPayloadService.getMeasurementPayloadById(measurement.id);
  await syncEventService.createEntitySyncEvent('measurement', syncPayload);
  return measurement;
};

const chartRiverLevel = async (limit = 8) => {
  const safeLimit = Math.min(Math.max(Number(limit) || 8, 1), 50);
  const [rows] = await pool.execute(
    `SELECT measured_at, ROUND(AVG(value), 3) AS value
     FROM measurements
     WHERE measurement_type = 'RIVER_LEVEL'
     GROUP BY measured_at
     ORDER BY measured_at DESC
     LIMIT ${safeLimit}`
  );
  return rows.reverse().map((row) => ({ label: new Date(row.measured_at).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }), value: Number(row.value) }));
};

const latestOne = async () => {
  const [rows] = await pool.execute('SELECT * FROM measurements ORDER BY measured_at DESC, id DESC LIMIT 1');
  return parse(rows[0]);
};

module.exports = { findAll, findLatest, findLatestByDataPointAsc, create, chartRiverLevel, latestOne };
