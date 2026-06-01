const pool = require('../database/connection');
const { hashPayload } = require('./sync-event.service');
const { runWithoutSyncEvents } = require('./sync-context.service');
const { toMysqlDateTime, nowMysql } = require('../utils/mysql-date');

const json = (value) => {
  if (typeof value === 'string') {
    try { return JSON.stringify(JSON.parse(value)); } catch (_error) { return value; }
  }
  return JSON.stringify(value ?? null);
};
const asDate = toMysqlDateTime;
const asDateOnly = (value) => {
  const mysqlDateTime = toMysqlDateTime(value);
  return mysqlDateTime ? mysqlDateTime.slice(0, 10) : null;
};
const eventTime = (event) => asDate(event.created_at) || nowMysql();

const findIdByUuid = async (connection, table, uuid) => {
  if (!uuid) return null;
  const [[row]] = await connection.execute(`SELECT id FROM ${table} WHERE uuid=? LIMIT 1`, [uuid]);
  return row?.id || null;
};

const shouldSkipOlder = async (connection, table, uuid, incomingAt) => {
  if (!uuid || !incomingAt) return false;
  const [[row]] = await connection.execute(`SELECT updated_at FROM ${table} WHERE uuid=? LIMIT 1`, [uuid]);
  return row?.updated_at && new Date(row.updated_at).getTime() > new Date(incomingAt).getTime();
};

const asInt = (value, fallback = null) => {
  if (value === undefined || value === null || value === '') return fallback;
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : fallback;
};

const upsertClusterNode = async (event, c) => {
  const p = event.payload || {};
  if (!p.node_uuid && !p.tailscale_ip) throw new Error('cluster_node sem node_uuid/tailscale_ip');

  const [[self]] = await c.execute('SELECT id, node_uuid, tailscale_ip FROM cluster_nodes WHERE is_self=1 LIMIT 1');
  const [[existingByUuid]] = p.node_uuid
    ? await c.execute('SELECT * FROM cluster_nodes WHERE node_uuid=? LIMIT 1', [p.node_uuid])
    : [[]];
  const [[existingByIp]] = !existingByUuid && p.tailscale_ip
    ? await c.execute('SELECT * FROM cluster_nodes WHERE tailscale_ip=? LIMIT 1', [p.tailscale_ip])
    : [[]];
  const existing = existingByUuid || existingByIp || null;
  const isSelf = Boolean(self && (
    (p.node_uuid && p.node_uuid === self.node_uuid) ||
    (p.tailscale_ip && p.tailscale_ip === self.tailscale_ip) ||
    (existing && existing.id === self.id)
  ));

  const values = {
    node_uuid: p.node_uuid || existing?.node_uuid,
    node_name: p.node_name || existing?.node_name,
    tailscale_ip: p.tailscale_ip || existing?.tailscale_ip,
    public_url: p.public_url ?? existing?.public_url ?? null,
    port: asInt(p.port, existing?.port ?? null),
    role: p.role || existing?.role || 'UNKNOWN',
    status: p.status || existing?.status || 'UNKNOWN',
    is_self: isSelf ? 1 : 0,
    last_heartbeat_at: asDate(p.last_heartbeat_at) || existing?.last_heartbeat_at || null,
    last_healthcheck_at: asDate(p.last_healthcheck_at) || existing?.last_healthcheck_at || null,
    healthcheck_error: p.healthcheck_error ?? existing?.healthcheck_error ?? null,
    metadata: json(p.metadata ?? existing?.metadata ?? null),
    power_score: asInt(p.power_score, existing?.power_score ?? 5)
  };

  if (existing) {
    await c.execute(
      `UPDATE cluster_nodes SET node_uuid=?, node_name=?, tailscale_ip=?, public_url=?, port=?, role=?, status=?, is_self=?,
       last_heartbeat_at=?, last_healthcheck_at=?, healthcheck_error=?, metadata=?, power_score=? WHERE id=?`,
      [values.node_uuid, values.node_name, values.tailscale_ip, values.public_url, values.port, values.role, values.status, values.is_self,
        values.last_heartbeat_at, values.last_healthcheck_at, values.healthcheck_error, values.metadata, values.power_score, existing.id]
    );
  } else {
    await c.execute(
      `INSERT INTO cluster_nodes (node_uuid, node_name, tailscale_ip, public_url, port, role, status, is_self, last_heartbeat_at, last_healthcheck_at, healthcheck_error, metadata, power_score)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [values.node_uuid, values.node_name, values.tailscale_ip, values.public_url, values.port, values.role, values.status, values.is_self,
        values.last_heartbeat_at, values.last_healthcheck_at, values.healthcheck_error, values.metadata, values.power_score]
    );
  }

  if (isSelf) {
    await c.execute('UPDATE cluster_nodes SET is_self = CASE WHEN node_uuid = ? OR tailscale_ip = ? THEN 1 ELSE 0 END', [values.node_uuid, values.tailscale_ip]);
  } else if (self?.id) {
    await c.execute('UPDATE cluster_nodes SET is_self = CASE WHEN id = ? THEN 1 ELSE 0 END', [self.id]);
  }

  return 'applied';
};

const upsertDataPoint = async (event, c) => {
  const p = event.payload || {};
  if (!p.uuid) throw new Error('data_point sem uuid');
  if (await shouldSkipOlder(c, 'data_points', p.uuid, eventTime(event))) return 'skipped_older';
  await c.execute(
    `INSERT INTO data_points (uuid, name, type, latitude, longitude, city_region, description, status, normal_level, warning_level, critical_level, measurement_unit)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE name=VALUES(name), type=VALUES(type), latitude=VALUES(latitude), longitude=VALUES(longitude), city_region=VALUES(city_region),
       description=VALUES(description), status=VALUES(status), normal_level=VALUES(normal_level), warning_level=VALUES(warning_level), critical_level=VALUES(critical_level), measurement_unit=VALUES(measurement_unit)`,
    [p.uuid, p.name, p.type || 'RIVER_LEVEL', p.latitude ?? null, p.longitude ?? null, p.city_region || null, p.description || null, p.status || 'ACTIVE', p.normal_level ?? null, p.warning_level ?? null, p.critical_level ?? null, p.measurement_unit || 'm']
  );
  return 'applied';
};

const upsertMeasurement = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  if (!p.uuid || !dataPointId) throw new Error('measurement sem uuid/data_point local');
  await c.execute(
    `INSERT INTO measurements (uuid, data_point_id, measurement_type, value, unit, measured_at, source, observation)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE data_point_id=VALUES(data_point_id), measurement_type=VALUES(measurement_type), value=VALUES(value), unit=VALUES(unit), measured_at=VALUES(measured_at), source=VALUES(source), observation=VALUES(observation)`,
    [p.uuid, dataPointId, p.measurement_type || 'RIVER_LEVEL', p.value, p.unit || 'm', asDate(p.measured_at), p.source || 'MANUAL', p.observation || null]
  );
};

const upsertAlert = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  const measurementId = await findIdByUuid(c, 'measurements', p.measurement_uuid);
  if (!p.uuid || !dataPointId) throw new Error('alert sem uuid/data_point local');
  if (await shouldSkipOlder(c, 'alerts', p.uuid, eventTime(event))) return 'skipped_older';
  await c.execute(
    `INSERT INTO alerts (uuid, data_point_id, measurement_id, alert_type, severity, current_value, unit, message, status, detected_at, resolved_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE data_point_id=VALUES(data_point_id), measurement_id=VALUES(measurement_id), alert_type=VALUES(alert_type), severity=VALUES(severity), current_value=VALUES(current_value), unit=VALUES(unit), message=VALUES(message), status=VALUES(status), resolved_at=VALUES(resolved_at)`,
    [p.uuid, dataPointId, measurementId, p.alert_type, p.severity, p.current_value, p.unit || 'm', p.message, p.status || 'ACTIVE', asDate(p.detected_at), asDate(p.resolved_at)]
  );
  return 'applied';
};

const upsertHistoricalImport = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  if (!p.uuid) throw new Error('historical_import sem uuid');
  if (await shouldSkipOlder(c, 'historical_imports', p.uuid, eventTime(event))) return 'skipped_older';
  await c.execute(
    `INSERT INTO historical_imports (uuid, data_point_id, original_filename, sensor_name, status, total_rows, imported_rows, failed_rows, raw_unit, converted_unit, error_message, completed_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE data_point_id=VALUES(data_point_id), original_filename=VALUES(original_filename), sensor_name=VALUES(sensor_name), status=VALUES(status), total_rows=VALUES(total_rows), imported_rows=VALUES(imported_rows), failed_rows=VALUES(failed_rows), raw_unit=VALUES(raw_unit), converted_unit=VALUES(converted_unit), error_message=VALUES(error_message), completed_at=VALUES(completed_at)`,
    [p.uuid, dataPointId, p.original_filename || 'sync-import.csv', p.sensor_name || null, p.status || 'IMPORTED', p.total_rows || 0, p.imported_rows || 0, p.failed_rows || 0, p.raw_unit || 'cm', p.converted_unit || 'm', p.error_message || null, asDate(p.completed_at)]
  );
  return 'applied';
};

const upsertHistoricalMeasurement = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  const importId = await findIdByUuid(c, 'historical_imports', p.import_uuid);
  if (!p.uuid || !dataPointId) throw new Error('historical_measurement sem uuid/data_point local');
  await c.execute(
    `INSERT INTO historical_measurements (uuid, data_point_id, import_id, measured_at, raw_value, raw_unit, value, unit, max_value, min_value, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE uuid=VALUES(uuid), import_id=VALUES(import_id), raw_value=VALUES(raw_value), raw_unit=VALUES(raw_unit), value=VALUES(value), unit=VALUES(unit), max_value=VALUES(max_value), min_value=VALUES(min_value), source=VALUES(source)`,
    [p.uuid, dataPointId, importId, asDateOnly(p.measured_at), p.raw_value ?? null, p.raw_unit || 'cm', p.value, p.unit || 'm', p.max_value ?? null, p.min_value ?? null, p.source || 'CSV_IMPORT']
  );
};

const upsertChartCache = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  if (!p.uuid || !dataPointId) throw new Error('chart_cache sem uuid/data_point local');
  if (await shouldSkipOlder(c, 'chart_cache', p.uuid, eventTime(event))) return 'skipped_older';
  await c.execute(
    `INSERT INTO chart_cache (uuid, data_point_id, chart_type, status, generated_by_node_name, total_points, date_start, date_end, payload, summary, error_message, generated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE data_point_id=VALUES(data_point_id), chart_type=VALUES(chart_type), status=VALUES(status), generated_by_node_name=VALUES(generated_by_node_name), total_points=VALUES(total_points), date_start=VALUES(date_start), date_end=VALUES(date_end), payload=VALUES(payload), summary=VALUES(summary), error_message=VALUES(error_message), generated_at=VALUES(generated_at)`,
    [p.uuid, dataPointId, p.chart_type || 'HISTORICAL_RIVER_LEVEL', p.status || 'READY', p.generated_by_node_name || null, p.total_points || 0, p.date_start || null, p.date_end || null, json(p.payload), json(p.summary), p.error_message || null, asDate(p.generated_at)]
  );
  return 'applied';
};

const handlers = {
  cluster_node: upsertClusterNode,
  data_point: upsertDataPoint,
  measurement: upsertMeasurement,
  alert: upsertAlert,
  historical_import: upsertHistoricalImport,
  historical_measurement: upsertHistoricalMeasurement,
  chart_cache: upsertChartCache
};

const applySyncEvent = async (event, connection = pool) => runWithoutSyncEvents(async () => {
  if (!event?.event_uuid) throw new Error('event_uuid obrigatório');
  const [[existing]] = await connection.execute('SELECT id FROM sync_applied_events WHERE event_uuid=? LIMIT 1', [event.event_uuid]);
  if (existing) return { status: 'skipped', reason: 'duplicate' };
  const handler = handlers[event.entity_type];
  if (!handler) return { status: 'skipped', reason: 'unsupported_entity' };
  const result = await handler(event, connection);
  const payloadHash = event.payload_hash || hashPayload(event.payload);
  await connection.execute(
    `INSERT IGNORE INTO sync_applied_events (event_uuid, source_node_uuid, entity_type, entity_key, payload_hash, applied_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [event.event_uuid, event.source_node_uuid, event.entity_type, event.entity_key || event.payload?.uuid || event.payload?.node_uuid, payloadHash, nowMysql()]
  );
  return result === 'skipped_older' ? { status: 'skipped', reason: 'older_than_local' } : { status: 'applied' };
});

const applySyncEvents = async (events = []) => {
  const connection = await pool.getConnection();
  const summary = { ok: true, applied: 0, skipped: 0, failed: 0, errors: [] };
  try {
    for (const event of events) {
      try {
        await connection.beginTransaction();
        const result = await applySyncEvent(event, connection);
        await connection.commit();
        if (result.status === 'applied') summary.applied += 1;
        else summary.skipped += 1;
      } catch (error) {
        await connection.rollback().catch(() => {});
        summary.failed += 1;
        summary.errors.push({ event_uuid: event?.event_uuid || null, message: error.message });
        console.error('[sync] falha ao aplicar evento:', error.message);
      }
    }
  } finally {
    connection.release();
  }
  return summary;
};

module.exports = { applySyncEvent, applySyncEvents };
