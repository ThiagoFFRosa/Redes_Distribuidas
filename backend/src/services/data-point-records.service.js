const pool = require('../database/connection');
const { toMysqlDateTime, nowMysql } = require('../utils/mysql-date');
const syncPayloadService = require('./sync-payload.service');
const syncEventService = require('./sync-event.service');
const historicalChartService = require('./historical-chart.service');

const allowedSources = new Set(['all', 'site', 'csv']);
const allowedUnits = new Set(['m', 'cm', 'metro', 'metros', 'centimetro', 'centimetros', 'centímetros']);

const toIso = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
};

const numberOrNull = (value) => (value === null || value === undefined ? null : Number(value));

const normalizeSiteRecord = (row) => row && ({
  record_type: 'SITE',
  uuid: row.uuid,
  date: toIso(row.measured_at),
  value: numberOrNull(row.value),
  unit: row.unit || 'm',
  source_label: row.source === 'MANUAL' ? 'Cadastro manual' : (row.source || 'Cadastro manual'),
  observation: row.observation || null,
  created_at: toIso(row.created_at),
  updated_at: toIso(row.updated_at),
  deleted_at: toIso(row.deleted_at),
  deleted_by_node_uuid: row.deleted_by_node_uuid || null
});

const normalizeCsvRecord = (row) => row && ({
  record_type: 'CSV',
  uuid: row.uuid,
  date: toIso(row.measured_at),
  value: numberOrNull(row.value),
  unit: row.unit || 'm',
  source_label: row.import_filename ? `Importação CSV: ${row.import_filename}` : 'Importação CSV',
  import_uuid: row.import_uuid || null,
  correction_reason: row.correction_reason || null,
  corrected_at: toIso(row.corrected_at),
  corrected_by_node_uuid: row.corrected_by_node_uuid || null,
  original_value: numberOrNull(row.original_value),
  original_measured_at: toIso(row.original_measured_at),
  created_at: toIso(row.created_at),
  updated_at: toIso(row.updated_at),
  deleted_at: toIso(row.deleted_at),
  deleted_by_node_uuid: row.deleted_by_node_uuid || null
});

const getPoint = async (pointId, connection = pool) => {
  const [[point]] = await connection.execute('SELECT id, uuid, name, measurement_unit FROM data_points WHERE id=? LIMIT 1', [pointId]);
  return point || null;
};

const deletedClause = (includeDeleted, alias) => includeDeleted ? '1=1' : `${alias}.deleted_at IS NULL`;

const buildDateFilters = (query, alias, params) => {
  const filters = [];
  const from = toMysqlDateTime(query.from || query.date_from || query.start_date);
  const to = toMysqlDateTime(query.to || query.date_to || query.end_date);
  if (from) { filters.push(`${alias}.measured_at >= ?`); params.push(from); }
  if (to) { filters.push(`${alias}.measured_at <= ?`); params.push(to); }
  return filters;
};

const listRecords = async (pointId, query = {}) => {
  const point = await getPoint(pointId);
  if (!point) return null;
  const source = allowedSources.has(String(query.source || 'all').toLowerCase()) ? String(query.source || 'all').toLowerCase() : 'all';
  const includeDeleted = String(query.include_deleted || '').toLowerCase() === 'true';
  const order = String(query.order || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';
  const page = Math.max(Number.parseInt(query.page, 10) || 1, 1);
  const limit = Math.min(Math.max(Number.parseInt(query.limit, 10) || 50, 1), 200);
  const offset = (page - 1) * limit;
  const search = String(query.search || '').trim();
  let siteRows = [];
  let csvRows = [];

  if (source === 'all' || source === 'site') {
    const params = [point.uuid];
    const filters = [`dp.uuid = ?`, deletedClause(includeDeleted, 'm'), ...buildDateFilters(query, 'm', params)];
    if (search) { filters.push('(m.observation LIKE ? OR m.unit LIKE ? OR m.source LIKE ?)'); params.push(`%${search}%`, `%${search}%`, `%${search}%`); }
    const [rows] = await pool.execute(
      `SELECT m.* FROM measurements m JOIN data_points dp ON dp.id=m.data_point_id WHERE ${filters.join(' AND ')}`,
      params
    );
    siteRows = rows.map(normalizeSiteRecord);
  }

  if (source === 'all' || source === 'csv') {
    const params = [point.uuid];
    const filters = [`dp.uuid = ?`, deletedClause(includeDeleted, 'hm'), ...buildDateFilters(query, 'hm', params)];
    if (search) { filters.push('(hm.correction_reason LIKE ? OR hm.unit LIKE ? OR hi.original_filename LIKE ?)'); params.push(`%${search}%`, `%${search}%`, `%${search}%`); }
    const [rows] = await pool.execute(
      `SELECT hm.*, hi.uuid AS import_uuid, hi.original_filename AS import_filename
         FROM historical_measurements hm
         JOIN data_points dp ON dp.id=hm.data_point_id
         LEFT JOIN historical_imports hi ON hi.id=hm.import_id
        WHERE ${filters.join(' AND ')}`,
      params
    );
    csvRows = rows.map(normalizeCsvRecord);
  }

  const records = [...siteRows, ...csvRows].sort((a, b) => {
    const diff = new Date(a.date || 0).getTime() - new Date(b.date || 0).getTime();
    return order === 'asc' ? diff : -diff;
  });
  return {
    ok: true,
    data_point: { id: point.id, uuid: point.uuid, name: point.name },
    pagination: { page, limit, total: records.length },
    records: records.slice(offset, offset + limit)
  };
};

const validationError = (message, status = 400) => {
  const error = new Error(message);
  error.status = status;
  return error;
};

const validateRecordPayload = (body = {}, { requireReason = false } = {}) => {
  const measuredAt = toMysqlDateTime(body.measured_at || body.date);
  const hasValue = body.value !== undefined && body.value !== null && String(body.value).trim() !== '';
  const value = hasValue ? Number(body.value) : undefined;
  const unit = String(body.unit || 'm').trim() || 'm';
  if (!measuredAt) throw validationError('Data/hora inválida.');
  if (!Number.isFinite(value)) throw validationError('value deve ser um número finito.');
  if (!allowedUnits.has(unit.toLowerCase())) throw validationError('Unidade incompatível. Use m ou cm.');
  const correctionReason = String(body.correction_reason || body.reason || '').trim();
  if (requireReason && correctionReason.length > 1000) throw validationError('Motivo da correção muito longo.');
  return { measuredAt, value, unit, observation: body.observation ?? body.notes ?? null, correctionReason: correctionReason || null };
};

const getSelfNodeUuid = async (connection) => {
  const [[self]] = await connection.execute('SELECT node_uuid FROM cluster_nodes WHERE is_self=1 LIMIT 1');
  return self?.node_uuid || null;
};

const createRecordSyncEvent = async (entityType, id, operation, connection) => {
  const payload = entityType === 'measurement'
    ? await syncPayloadService.getMeasurementPayloadById(id, connection)
    : await syncPayloadService.getHistoricalMeasurementPayloadById(id, connection);
  if (payload) await syncEventService.createEntitySyncEvent(entityType, payload, operation, connection, { reason: 'point-record-local-change' });
};

const fetchSiteRecord = async (connection, pointId, recordUuid) => {
  const [[row]] = await connection.execute(
    `SELECT m.*, dp.uuid AS data_point_uuid FROM measurements m JOIN data_points dp ON dp.id=m.data_point_id WHERE dp.id=? AND m.uuid=? LIMIT 1`,
    [pointId, recordUuid]
  );
  return row || null;
};

const fetchCsvRecord = async (connection, pointId, recordUuid) => {
  const [[row]] = await connection.execute(
    `SELECT hm.*, dp.uuid AS data_point_uuid, hi.uuid AS import_uuid, hi.original_filename AS import_filename
       FROM historical_measurements hm JOIN data_points dp ON dp.id=hm.data_point_id LEFT JOIN historical_imports hi ON hi.id=hm.import_id
      WHERE dp.id=? AND hm.uuid=? LIMIT 1`,
    [pointId, recordUuid]
  );
  return row || null;
};

const withTransaction = async (work) => {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    const result = await work(connection);
    await connection.commit();
    return result;
  } catch (error) {
    await connection.rollback().catch(() => {});
    throw error;
  } finally {
    connection.release();
  }
};

const updateSiteRecord = async (pointId, recordUuid, body = {}) => withTransaction(async (connection) => {
  const row = await fetchSiteRecord(connection, pointId, recordUuid);
  if (!row) throw validationError('Medição do site não encontrada para este ponto.', 404);
  if (row.deleted_at) throw validationError('Registro excluído. Restaure antes de editar.', 409);
  const payload = validateRecordPayload(body);
  await connection.execute(
    `UPDATE measurements SET measured_at=?, value=?, unit=?, observation=?, updated_at=NOW() WHERE id=?`,
    [payload.measuredAt, payload.value, payload.unit, payload.observation, row.id]
  );
  await historicalChartService.markChartCacheStale(row.data_point_uuid, connection);
  await createRecordSyncEvent('measurement', row.id, 'UPSERT', connection);
  const updated = await fetchSiteRecord(connection, pointId, recordUuid);
  return normalizeSiteRecord(updated);
});

const deleteSiteRecord = async (pointId, recordUuid) => withTransaction(async (connection) => {
  const row = await fetchSiteRecord(connection, pointId, recordUuid);
  if (!row) throw validationError('Medição do site não encontrada para este ponto.', 404);
  const selfNodeUuid = await getSelfNodeUuid(connection);
  await connection.execute('UPDATE measurements SET deleted_at=COALESCE(deleted_at, ?), deleted_by_node_uuid=COALESCE(deleted_by_node_uuid, ?), updated_at=NOW() WHERE id=?', [nowMysql(), selfNodeUuid, row.id]);
  await historicalChartService.markChartCacheStale(row.data_point_uuid, connection);
  await createRecordSyncEvent('measurement', row.id, 'DELETE', connection);
  const updated = await fetchSiteRecord(connection, pointId, recordUuid);
  return normalizeSiteRecord(updated);
});

const restoreSiteRecord = async (pointId, recordUuid) => withTransaction(async (connection) => {
  const row = await fetchSiteRecord(connection, pointId, recordUuid);
  if (!row) throw validationError('Medição do site não encontrada para este ponto.', 404);
  await connection.execute('UPDATE measurements SET deleted_at=NULL, deleted_by_node_uuid=NULL, updated_at=NOW() WHERE id=?', [row.id]);
  await historicalChartService.markChartCacheStale(row.data_point_uuid, connection);
  await createRecordSyncEvent('measurement', row.id, 'UPSERT', connection);
  const updated = await fetchSiteRecord(connection, pointId, recordUuid);
  return normalizeSiteRecord(updated);
});

const updateCsvRecord = async (pointId, recordUuid, body = {}) => withTransaction(async (connection) => {
  const row = await fetchCsvRecord(connection, pointId, recordUuid);
  if (!row) throw validationError('Registro CSV não encontrado para este ponto.', 404);
  if (row.deleted_at) throw validationError('Registro excluído. Restaure antes de editar.', 409);
  const payload = validateRecordPayload(body, { requireReason: true });
  const selfNodeUuid = await getSelfNodeUuid(connection);
  await connection.execute(
    `UPDATE historical_measurements
        SET measured_at=?, value=?, unit=?, correction_reason=?,
            original_value=COALESCE(original_value, ?), original_measured_at=COALESCE(original_measured_at, ?),
            corrected_at=?, corrected_by_node_uuid=?, updated_at=NOW()
      WHERE id=?`,
    [payload.measuredAt.slice(0, 10), payload.value, payload.unit, payload.correctionReason, row.value, row.measured_at, nowMysql(), selfNodeUuid, row.id]
  );
  await historicalChartService.markChartCacheStale(row.data_point_uuid, connection);
  await createRecordSyncEvent('historical_measurement', row.id, 'UPSERT', connection);
  const updated = await fetchCsvRecord(connection, pointId, recordUuid);
  return normalizeCsvRecord(updated);
});

const deleteCsvRecord = async (pointId, recordUuid) => withTransaction(async (connection) => {
  const row = await fetchCsvRecord(connection, pointId, recordUuid);
  if (!row) throw validationError('Registro CSV não encontrado para este ponto.', 404);
  const selfNodeUuid = await getSelfNodeUuid(connection);
  await connection.execute('UPDATE historical_measurements SET deleted_at=COALESCE(deleted_at, ?), deleted_by_node_uuid=COALESCE(deleted_by_node_uuid, ?), updated_at=NOW() WHERE id=?', [nowMysql(), selfNodeUuid, row.id]);
  await historicalChartService.markChartCacheStale(row.data_point_uuid, connection);
  await createRecordSyncEvent('historical_measurement', row.id, 'DELETE', connection);
  const updated = await fetchCsvRecord(connection, pointId, recordUuid);
  return normalizeCsvRecord(updated);
});

const restoreCsvRecord = async (pointId, recordUuid) => withTransaction(async (connection) => {
  const row = await fetchCsvRecord(connection, pointId, recordUuid);
  if (!row) throw validationError('Registro CSV não encontrado para este ponto.', 404);
  await connection.execute('UPDATE historical_measurements SET deleted_at=NULL, deleted_by_node_uuid=NULL, updated_at=NOW() WHERE id=?', [row.id]);
  await historicalChartService.markChartCacheStale(row.data_point_uuid, connection);
  await createRecordSyncEvent('historical_measurement', row.id, 'UPSERT', connection);
  const updated = await fetchCsvRecord(connection, pointId, recordUuid);
  return normalizeCsvRecord(updated);
});

module.exports = { listRecords, updateSiteRecord, deleteSiteRecord, restoreSiteRecord, updateCsvRecord, deleteCsvRecord, restoreCsvRecord };
