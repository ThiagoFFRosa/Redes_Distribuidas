const crypto = require('crypto');
const pool = require('../database/connection');

const hashPayload = (payload) => crypto.createHash('sha256').update(JSON.stringify(payload ?? {})).digest('hex');

const createOutboxEvent = async (entityType, entityId, payload, operation = 'UPSERT', connection = pool) => {
  const [result] = await connection.execute(
    `INSERT INTO sync_outbox (entity_type, entity_id, operation, payload, status)
     VALUES (?, ?, ?, ?, 'PENDING')`,
    [entityType, String(entityId), operation, JSON.stringify(payload ?? {})]
  );
  return result.insertId;
};

const createOutboxEvents = async (events, connection = pool) => {
  for (const event of events) {
    await createOutboxEvent(event.entity_type, event.entity_id, event.payload, event.operation || 'UPSERT', connection);
  }
};

const upsertDataPoint = async (payload, connection) => {
  await connection.execute(
    `INSERT INTO data_points (id, name, type, latitude, longitude, city_region, description, status, normal_level, warning_level, critical_level, measurement_unit, created_by_user_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE name=VALUES(name), type=VALUES(type), latitude=VALUES(latitude), longitude=VALUES(longitude), city_region=VALUES(city_region),
       description=VALUES(description), status=VALUES(status), normal_level=VALUES(normal_level), warning_level=VALUES(warning_level), critical_level=VALUES(critical_level), measurement_unit=VALUES(measurement_unit)`,
    [payload.id, payload.name, payload.type || 'RIVER_LEVEL', payload.latitude ?? null, payload.longitude ?? null, payload.city_region ?? null, payload.description ?? null, payload.status || 'ACTIVE', payload.normal_level ?? null, payload.warning_level ?? null, payload.critical_level ?? null, payload.measurement_unit || 'm', payload.created_by_user_id ?? null]
  );
};

const upsertHistoricalImport = async (payload, connection) => {
  await connection.execute(
    `INSERT INTO historical_imports (id, data_point_id, original_filename, sensor_name, status, total_rows, imported_rows, failed_rows, raw_unit, converted_unit, error_message, uploaded_by_user_id, completed_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE data_point_id=VALUES(data_point_id), original_filename=VALUES(original_filename), sensor_name=VALUES(sensor_name), status=VALUES(status), total_rows=VALUES(total_rows), imported_rows=VALUES(imported_rows), failed_rows=VALUES(failed_rows), error_message=VALUES(error_message), completed_at=VALUES(completed_at)`,
    [payload.id, payload.data_point_id ?? null, payload.original_filename, payload.sensor_name ?? null, payload.status || 'IMPORTED', payload.total_rows || 0, payload.imported_rows || 0, payload.failed_rows || 0, payload.raw_unit || 'cm', payload.converted_unit || 'm', payload.error_message ?? null, payload.uploaded_by_user_id ?? null, payload.completed_at ?? null]
  );
};

const upsertHistoricalMeasurement = async (payload, connection) => {
  await connection.execute(
    `INSERT INTO historical_measurements (data_point_id, import_id, measured_at, raw_value, raw_unit, value, unit, max_value, min_value, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE import_id=VALUES(import_id), raw_value=VALUES(raw_value), raw_unit=VALUES(raw_unit), value=VALUES(value), unit=VALUES(unit), max_value=VALUES(max_value), min_value=VALUES(min_value), source=VALUES(source)`,
    [payload.data_point_id, payload.import_id ?? null, payload.measured_at, payload.raw_value ?? null, payload.raw_unit || 'cm', payload.value, payload.unit || 'm', payload.max_value ?? null, payload.min_value ?? null, payload.source || 'CSV_IMPORT']
  );
};

const upsertChartJob = async (payload, connection) => {
  await connection.execute(
    `INSERT INTO chart_generation_jobs (id, data_point_id, import_id, status, requested_by_node_id, assigned_node_id, assigned_node_name, progress_percent, estimated_seconds, error_message, started_at, finished_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE status=VALUES(status), progress_percent=VALUES(progress_percent), estimated_seconds=VALUES(estimated_seconds), error_message=VALUES(error_message), assigned_node_id=VALUES(assigned_node_id), assigned_node_name=VALUES(assigned_node_name)`,
    [payload.id, payload.data_point_id, payload.import_id ?? null, payload.status || 'PENDING', payload.requested_by_node_id ?? null, payload.assigned_node_id ?? null, payload.assigned_node_name ?? null, payload.progress_percent || 0, payload.estimated_seconds ?? null, payload.error_message ?? null, payload.started_at ?? null, payload.finished_at ?? null]
  );
};

const applyEvent = async (event, connection) => {
  if (event.operation === 'DELETE') return;
  if (event.entity_type === 'data_point') return upsertDataPoint(event.payload, connection);
  if (event.entity_type === 'historical_import') return upsertHistoricalImport(event.payload, connection);
  if (event.entity_type === 'historical_measurement') return upsertHistoricalMeasurement(event.payload, connection);
  if (event.entity_type === 'chart_generation_job') return upsertChartJob(event.payload, connection);
};

const applyIncomingEvents = async (events, sourceNodeId = null) => {
  const connection = await pool.getConnection();
  const summary = { ok: true, applied: 0, skipped: 0, failed: 0 };
  try {
    for (const event of events || []) {
      const payloadHash = event.payload_hash || hashPayload(event.payload);
      try {
        const [existing] = await connection.execute('SELECT id FROM sync_inbox WHERE entity_type=? AND entity_id=? AND payload_hash=? LIMIT 1', [event.entity_type, String(event.entity_id), payloadHash]);
        if (existing.length) { summary.skipped += 1; continue; }
        await connection.beginTransaction();
        await applyEvent(event, connection);
        await connection.execute(
          `INSERT INTO sync_inbox (source_node_id, entity_type, entity_id, operation, payload_hash) VALUES (?, ?, ?, ?, ?)`,
          [sourceNodeId, event.entity_type, String(event.entity_id), event.operation || 'UPSERT', payloadHash]
        );
        await connection.commit();
        summary.applied += 1;
      } catch (error) {
        await connection.rollback().catch(() => {});
        summary.failed += 1;
        console.error('[sync] falha ao aplicar evento:', error.message);
      }
    }
  } finally { connection.release(); }
  return summary;
};

module.exports = { createOutboxEvent, createOutboxEvents, applyIncomingEvents, hashPayload };
