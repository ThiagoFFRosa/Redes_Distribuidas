const pool = require('../database/connection');

const parsePayload = (payload) => {
  if (!payload) return null;
  if (typeof payload === 'object') return payload;
  try { return JSON.parse(payload); } catch (_error) { return payload; }
};

const findLatest = async (limit = 30) => {
  const safeLimit = Math.min(Math.max(Number(limit) || 30, 1), 100);
  const [rows] = await pool.execute(`SELECT * FROM event_queue_logs ORDER BY created_at DESC, id DESC LIMIT ${safeLimit}`);
  return rows.map((row) => ({ ...row, payload: parsePayload(row.payload) }));
};

const create = async ({ event_type, status = 'RECEIVED', payload = null, message = null, related_measurement_id = null, processed_at = null }) => {
  const jsonPayload = payload == null ? null : JSON.stringify(payload);
  const [result] = await pool.execute(
    `INSERT INTO event_queue_logs (event_type, status, payload, message, related_measurement_id, processed_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [event_type, status, jsonPayload, message, related_measurement_id, processed_at]
  );
  return result.insertId;
};

module.exports = { findLatest, create };
