const pool = require('../database/connection');
const { hashPayload } = require('./sync-event.service');
const { runWithoutSyncEvents } = require('./sync-context.service');
const { toMysqlDateTime, nowMysql } = require('../utils/mysql-date');
const logger = require('../utils/logger');
const registryService = require('./synced-entity-registry.service');

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

const normalizeNaturalPart = (value) => String(value || '').trim().toLowerCase();

const findEquivalentDataPoint = async (connection, payload) => {
  if (!payload) return null;
  if (payload.source_key) {
    const [[bySourceKey]] = await connection.execute('SELECT * FROM data_points WHERE source_key=? LIMIT 1', [payload.source_key]);
    if (bySourceKey) return { ...bySourceKey, match: 'source_key' };
  }
  if (payload.name) {
    const [[byNaturalKey]] = await connection.execute(
      `SELECT * FROM data_points
        WHERE LOWER(TRIM(name))=?
          AND COALESCE(LOWER(TRIM(city_region)), '')=?
          AND type=?
        ORDER BY id ASC LIMIT 1`,
      [normalizeNaturalPart(payload.name), normalizeNaturalPart(payload.city_region), payload.type || 'RIVER_LEVEL']
    );
    if (byNaturalKey) return { ...byNaturalKey, match: 'natural_key' };
  }
  return null;
};

const reassignDataPointUuidForBootstrap = async (connection, existing, payload) => {
  logger.warn(`[sync-apply] data_point uuid conflict bootstrap: match=${existing.match || '-'} local_uuid=${existing.uuid} host_uuid=${payload.uuid}; preservando UUID do HOST`);
  await connection.execute('UPDATE data_points SET uuid=?, source_key=COALESCE(source_key, ?) WHERE id=?', [payload.uuid, payload.source_key || null, existing.id]);
};

const findChartCacheUuidMismatch = async (connection, payload) => {
  if (payload.source_job_uuid) {
    const [[jobPoint]] = await connection.execute(
      `SELECT dp.uuid, dp.name, dp.city_region, dp.source_key
         FROM chart_generation_jobs cj JOIN data_points dp ON dp.id=cj.data_point_id
        WHERE cj.uuid=? LIMIT 1`,
      [payload.source_job_uuid]
    );
    if (jobPoint && jobPoint.uuid !== payload.data_point_uuid) return { ...jobPoint, match: 'source_job_uuid' };
  }
  return null;
};

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

  const [[self]] = await c.execute('SELECT id, node_uuid, tailscale_ip, public_url, port, role, power_score FROM cluster_nodes WHERE is_self=1 LIMIT 1');
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
    public_url: isSelf ? (existing?.public_url ?? p.public_url ?? null) : (p.public_url ?? existing?.public_url ?? null),
    port: asInt(p.port, existing?.port ?? null),
    role: p.role || existing?.role || 'UNKNOWN',
    status: existing?.status || 'UNKNOWN',
    is_self: isSelf ? 1 : 0,
    last_heartbeat_at: existing?.last_heartbeat_at || null,
    last_healthcheck_at: existing?.last_healthcheck_at || null,
    healthcheck_error: existing?.healthcheck_error ?? null,
    metadata: json(p.metadata ?? existing?.metadata ?? null),
    power_score: asInt(p.power_score, existing?.power_score ?? 5),
    structural_version: asInt(p.structural_version, existing?.structural_version ?? 1)
  };

  if (existing) {
    await c.execute(
      `UPDATE cluster_nodes SET node_uuid=?, node_name=?, tailscale_ip=?, public_url=?, port=?, role=?, status=?, is_self=?,
       last_heartbeat_at=?, last_healthcheck_at=?, healthcheck_error=?, metadata=?, power_score=?, structural_version=? WHERE id=?`,
      [values.node_uuid, values.node_name, values.tailscale_ip, values.public_url, values.port, values.role, values.status, values.is_self,
        values.last_heartbeat_at, values.last_healthcheck_at, values.healthcheck_error, values.metadata, values.power_score, values.structural_version, existing.id]
    );
  } else {
    await c.execute(
      `INSERT INTO cluster_nodes (node_uuid, node_name, tailscale_ip, public_url, port, role, status, is_self, last_heartbeat_at, last_healthcheck_at, healthcheck_error, metadata, power_score, structural_version)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [values.node_uuid, values.node_name, values.tailscale_ip, values.public_url, values.port, values.role, values.status, values.is_self,
        values.last_heartbeat_at, values.last_healthcheck_at, values.healthcheck_error, values.metadata, values.power_score, values.structural_version]
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
  const [[existingByUuid]] = await c.execute('SELECT id FROM data_points WHERE uuid=? LIMIT 1', [p.uuid]);
  let reassignedBootstrapUuid = false;
  if (!existingByUuid) {
    const equivalent = await findEquivalentDataPoint(c, p);
    if (equivalent && equivalent.uuid !== p.uuid) {
      if (event.source_mode === 'BOOTSTRAP') {
        await reassignDataPointUuidForBootstrap(c, equivalent, p);
        reassignedBootstrapUuid = true;
      } else {
        logger.error(`[sync-apply] data_point uuid conflict: incoming_uuid=${p.uuid} local_equivalent_uuid=${equivalent.uuid} match=${equivalent.match || '-'} source_key=${p.source_key || '-'}`);
        throw new Error(`data_point uuid conflict: incoming=${p.uuid} local=${equivalent.uuid}`);
      }
    }
  }
  if (!reassignedBootstrapUuid && await shouldSkipOlder(c, 'data_points', p.uuid, eventTime(event))) return 'skipped_older';
  await c.execute(
    `INSERT INTO data_points (uuid, source_key, name, type, latitude, longitude, city_region, location_status, location_error, description, status, normal_level, warning_level, critical_level, measurement_unit)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE source_key=COALESCE(VALUES(source_key), source_key), name=VALUES(name), type=VALUES(type), latitude=VALUES(latitude), longitude=VALUES(longitude), city_region=VALUES(city_region),
       location_status=VALUES(location_status), location_error=VALUES(location_error), description=VALUES(description), status=VALUES(status), normal_level=VALUES(normal_level), warning_level=VALUES(warning_level), critical_level=VALUES(critical_level), measurement_unit=VALUES(measurement_unit)`,
    [p.uuid, p.source_key || null, p.name, p.type || 'RIVER_LEVEL', p.latitude ?? null, p.longitude ?? null, p.city_region || null, p.location_status || ((p.latitude == null || p.longitude == null) ? 'NEEDS_REVIEW' : 'VALID'), p.location_error || null, p.description || null, p.status || 'ACTIVE', p.normal_level ?? null, p.warning_level ?? null, p.critical_level ?? null, p.measurement_unit || 'm']
  );
  logger.info(`[sync-apply] data_point APPLIED uuid=${p.uuid} source_key=${p.source_key || '-'} name=${p.name || '-'}`);
  return 'applied';
};

const upsertMeasurement = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  if (!p.uuid) throw new Error('measurement sem uuid');
  if (!dataPointId) return { status: 'deferred', reason: 'missing_data_point', message: `missing data_point_uuid=${p.data_point_uuid || '-'}` };
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
  if (!p.uuid) throw new Error('alert sem uuid');
  if (!dataPointId || (p.measurement_uuid && !measurementId)) return { status: 'deferred', reason: 'missing_dependency', message: `missing data_point_uuid=${p.data_point_uuid || '-'} measurement_uuid=${p.measurement_uuid || '-'}` };
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
  if (!p.uuid) throw new Error('historical_measurement sem uuid');
  if (!dataPointId || (p.import_uuid && !importId)) return { status: 'deferred', reason: 'missing_dependency', message: `missing data_point_uuid=${p.data_point_uuid || '-'} import_uuid=${p.import_uuid || '-'}` };
  await c.execute(
    `INSERT INTO historical_measurements (uuid, data_point_id, import_id, measured_at, raw_value, raw_unit, value, unit, max_value, min_value, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE uuid=VALUES(uuid), import_id=VALUES(import_id), raw_value=VALUES(raw_value), raw_unit=VALUES(raw_unit), value=VALUES(value), unit=VALUES(unit), max_value=VALUES(max_value), min_value=VALUES(min_value), source=VALUES(source)`,
    [p.uuid, dataPointId, importId, asDateOnly(p.measured_at), p.raw_value ?? null, p.raw_unit || 'cm', p.value, p.unit || 'm', p.max_value ?? null, p.min_value ?? null, p.source || 'CSV_IMPORT']
  );
};


const upsertChartGenerationJob = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  if (!p.uuid) throw new Error('chart_generation_job sem uuid');
  if (!dataPointId) return { status: 'deferred', reason: 'missing_data_point', message: `missing data_point_uuid=${p.data_point_uuid || '-'}` };
  const requestedRows = p.requested_by_node_uuid
    ? await c.execute('SELECT id FROM cluster_nodes WHERE node_uuid=? LIMIT 1', [p.requested_by_node_uuid])
    : [[]];
  const assignedRows = p.assigned_to_node_uuid
    ? await c.execute('SELECT id, node_name FROM cluster_nodes WHERE node_uuid=? LIMIT 1', [p.assigned_to_node_uuid])
    : [[]];
  const requestedNode = requestedRows[0][0] || null;
  const assignedNode = assignedRows[0][0] || null;
  if ((p.requested_by_node_uuid && !requestedNode) || (p.assigned_to_node_uuid && !assignedNode)) {
    return { status: 'deferred', reason: 'missing_cluster_node', message: `missing cluster_node requested=${p.requested_by_node_uuid || '-'} assigned=${p.assigned_to_node_uuid || '-'}` };
  }
  if (await shouldSkipOlder(c, 'chart_generation_jobs', p.uuid, eventTime(event))) return 'skipped_older';
  await c.execute(
    `INSERT INTO chart_generation_jobs
      (uuid, data_point_id, chart_type, data_point_uuid, status, requested_by_node_id, requested_by_node_uuid,
       assigned_node_id, assigned_to_node_uuid, assigned_node_name, progress_percent, estimated_seconds,
       error_message, started_at, finished_at, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, NOW()), COALESCE(?, NOW()))
     ON DUPLICATE KEY UPDATE data_point_id=VALUES(data_point_id), chart_type=VALUES(chart_type), data_point_uuid=VALUES(data_point_uuid),
       status=VALUES(status), requested_by_node_id=VALUES(requested_by_node_id), requested_by_node_uuid=VALUES(requested_by_node_uuid),
       assigned_node_id=VALUES(assigned_node_id), assigned_to_node_uuid=VALUES(assigned_to_node_uuid), assigned_node_name=VALUES(assigned_node_name),
       progress_percent=VALUES(progress_percent), estimated_seconds=VALUES(estimated_seconds), error_message=VALUES(error_message),
       started_at=VALUES(started_at), finished_at=VALUES(finished_at), updated_at=VALUES(updated_at)`,
    [p.uuid, dataPointId, p.chart_type || 'HISTORICAL_RIVER_LEVEL', p.data_point_uuid, p.status || 'PENDING',
      requestedNode?.id || null, p.requested_by_node_uuid || null, assignedNode?.id || null, p.assigned_to_node_uuid || null,
      p.assigned_to_node_name || assignedNode?.node_name || null, Number(p.progress_percent || 0), p.estimated_seconds ?? null,
      p.error_message || null, asDate(p.started_at), asDate(p.finished_at), asDate(p.created_at), asDate(p.updated_at)]
  );
  return 'applied';
};

const upsertChartCache = async (event, c) => {
  const p = event.payload || {};
  const dataPointId = await findIdByUuid(c, 'data_points', p.data_point_uuid);
  if (!p.uuid) throw new Error('chart_cache sem uuid');
  if (!dataPointId) {
    const mismatch = await findChartCacheUuidMismatch(c, p);
    if (mismatch) {
      logger.error(`[sync-apply] chart_cache dependency uuid mismatch: cache data_point_uuid=${p.data_point_uuid || '-'}, local equivalent uuid=${mismatch.uuid} match=${mismatch.match}`);
      return { status: 'deferred', reason: 'data_point_uuid_mismatch', message: `chart_cache dependency uuid mismatch cache=${p.data_point_uuid || '-'} local=${mismatch.uuid}` };
    }
    logger.warn(`[sync-apply] chart_cache DEFERRED_MISSING_DEPENDENCY uuid=${p.uuid} data_point_uuid=${p.data_point_uuid || '-'}`);
    return { status: 'deferred', reason: 'missing_data_point', message: `missing data_point_uuid=${p.data_point_uuid || '-'}` };
  }
  if (await shouldSkipOlder(c, 'chart_cache', p.uuid, eventTime(event))) return 'skipped_older';
  await c.execute(
    `INSERT INTO chart_cache (uuid, data_point_id, data_point_uuid, chart_type, status, generated_by_node_id, generated_by_node_uuid, generated_by_node_name, source_job_uuid, total_points, date_start, date_end, payload, summary, error_message, generated_at)
     VALUES (?, ?, ?, ?, ?, (SELECT id FROM cluster_nodes WHERE node_uuid=? LIMIT 1), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE uuid=VALUES(uuid), data_point_id=VALUES(data_point_id), data_point_uuid=VALUES(data_point_uuid), chart_type=VALUES(chart_type), status=VALUES(status),
       generated_by_node_id=VALUES(generated_by_node_id), generated_by_node_uuid=VALUES(generated_by_node_uuid), generated_by_node_name=VALUES(generated_by_node_name),
       source_job_uuid=VALUES(source_job_uuid), total_points=VALUES(total_points), date_start=VALUES(date_start), date_end=VALUES(date_end), payload=VALUES(payload),
       summary=VALUES(summary), error_message=VALUES(error_message), generated_at=VALUES(generated_at)`,
    [p.uuid, dataPointId, p.data_point_uuid, p.chart_type || 'HISTORICAL_RIVER_LEVEL', p.status || 'READY', p.generated_by_node_uuid || null, p.generated_by_node_uuid || null,
      p.generated_by_node_name || null, p.source_job_uuid || null, p.total_points || 0, p.date_start || null, p.date_end || null, json(p.payload), json(p.summary), p.error_message || null, asDate(p.generated_at)]
  );
  console.log(`[sync-apply] chart_cache APPLIED uuid=${p.uuid} data_point_uuid=${p.data_point_uuid || '-'} points=${p.total_points || 0}`);
  return 'applied';
};

const handlers = {
  cluster_node: upsertClusterNode,
  data_point: upsertDataPoint,
  measurement: upsertMeasurement,
  alert: upsertAlert,
  historical_import: upsertHistoricalImport,
  historical_measurement: upsertHistoricalMeasurement,
  chart_generation_job: upsertChartGenerationJob,
  chart_cache: upsertChartCache
};

const resultStatusFor = (result) => {
  if (result === 'applied' || result?.status === 'applied') return 'APPLIED';
  if (result === 'skipped_older' || (result?.status === 'skipped' && result.reason === 'older_than_local')) return 'SKIPPED_OLDER_VERSION';
  if (result?.status === 'deferred') return 'DEFERRED_MISSING_DEPENDENCY';
  if (result?.status === 'skipped' && result.reason === 'duplicate') return 'SKIPPED_ALREADY_APPLIED';
  if (result?.status === 'skipped') return 'FAILED';
  return 'APPLIED';
};

const applySyncEvent = async (event, connection = pool, options = {}) => runWithoutSyncEvents(async () => {
  if (!event?.event_uuid) throw new Error('event_uuid obrigatório');
  const [[existing]] = await connection.execute('SELECT id FROM sync_applied_events WHERE event_uuid=? LIMIT 1', [event.event_uuid]);
  if (existing) return { status: 'skipped', reason: 'duplicate' };
  const handler = handlers[event.entity_type];
  if (!handler) throw new Error(`entity_type não suportado: ${event.entity_type}`);
  const result = await handler(event, connection);
  if (result && result.status === 'deferred') return result;
  const payloadHash = event.payload_hash || hashPayload(event.payload);
  const entityKey = registryService.registryEntityKeyForEvent(event);
  const originNodeUuid = event.origin_node_uuid || event.source_node_uuid || null;
  const appliedAt = nowMysql();
  await connection.execute(
    `INSERT IGNORE INTO sync_applied_events (event_uuid, source_node_uuid, origin_node_uuid, entity_type, entity_key, payload_hash, applied_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [event.event_uuid, event.source_node_uuid || originNodeUuid, originNodeUuid, event.entity_type, entityKey, payloadHash, appliedAt]
  );
  if (result !== 'skipped_older') {
    await registryService.upsertSyncedEntity({
      entityType: event.entity_type,
      entityKey,
      payloadHash,
      sourceNodeUuid: originNodeUuid,
      sourceMode: options.sourceMode || event.source_mode || 'REMOTE_SYNC'
    }, connection);
  }
  return result === 'skipped_older' ? { status: 'skipped', reason: 'older_than_local' } : { status: 'applied' };
});

const applySyncEvents = async (events = [], options = {}) => {
  const order = ['cluster_node', 'data_point', 'historical_import', 'measurement', 'historical_measurement', 'alert', 'chart_generation_job', 'chart_cache'];
  const safeEvents = (Array.isArray(events) ? events : []).slice().sort((a, b) => {
    const ai = order.indexOf(a?.entity_type);
    const bi = order.indexOf(b?.entity_type);
    return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
  });
  const connection = await pool.getConnection();
  const summary = { ok: true, received: safeEvents.length, applied: 0, skipped: 0, failed: 0, deferred: 0, results: [], errors: [] };
  try {
    for (const event of safeEvents) {
      try {
        await connection.beginTransaction();
        const result = await applySyncEvent(event, connection, options);
        await connection.commit();
        const status = resultStatusFor(result);
        if (status === 'APPLIED') summary.applied += 1;
        else if (status === 'DEFERRED_MISSING_DEPENDENCY') summary.deferred += 1;
        else summary.skipped += 1;
        summary.results.push({
          event_uuid: event?.event_uuid || null,
          entity_type: event?.entity_type || null,
          entity_key: event?.entity_key || event?.payload?.uuid || event?.payload?.node_uuid || null,
          status,
          reason: result?.reason || null,
          message: result?.message || null
        });
      } catch (error) {
        await connection.rollback().catch(() => {});
        summary.failed += 1;
        const result = { event_uuid: event?.event_uuid || null, entity_type: event?.entity_type || null, entity_key: event?.entity_key || event?.payload?.uuid || event?.payload?.node_uuid || null, status: 'FAILED', message: error.message };
        summary.results.push(result);
        summary.errors.push(result);
        if (event?.entity_type === 'data_point') logger.info(`[sync-apply] data_point FAILED uuid=${event?.payload?.uuid || event?.entity_key || '-'} reason=${error.message}`);
        logger.error('[sync] falha ao aplicar evento:', error.message);
      }
    }
  } finally {
    connection.release();
  }
  return summary;
};

module.exports = { applySyncEvent, applySyncEvents };
