const pool = require('../database/connection');
const { toMysqlDateTime } = require('../utils/mysql-date');

const parseJson = (value) => {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_error) { return null; }
};
const dateValue = toMysqlDateTime;
const dateOnly = (value) => {
  const mysqlDateTime = toMysqlDateTime(value);
  return mysqlDateTime ? mysqlDateTime.slice(0, 10) : null;
};
const num = (value) => (value == null ? null : Number(value));
const MAX_CHART_SYNC_POINTS = 1000;

const downsampleSeries = (items, maxItems = MAX_CHART_SYNC_POINTS) => {
  if (!Array.isArray(items) || items.length <= maxItems) return items;
  const step = (items.length - 1) / (maxItems - 1);
  return Array.from({ length: maxItems }, (_value, index) => items[Math.round(index * step)]);
};

const reduceChartPayload = (payload) => {
  if (!payload || typeof payload !== 'object') return payload;
  const labels = Array.isArray(payload.labels) ? payload.labels : [];
  const values = Array.isArray(payload.values) ? payload.values : [];
  const maxLength = Math.max(labels.length, values.length);
  if (maxLength <= MAX_CHART_SYNC_POINTS) return payload;
  return {
    ...payload,
    labels: downsampleSeries(labels),
    values: downsampleSeries(values),
    downsampled: true,
    full_count: payload.full_count || maxLength,
    sync_limited_to: MAX_CHART_SYNC_POINTS
  };
};

const dataPointPayload = (row) => row && ({
  uuid: row.uuid, name: row.name, type: row.type, latitude: num(row.latitude), longitude: num(row.longitude),
  city_region: row.city_region, description: row.description, status: row.status,
  normal_level: num(row.normal_level), warning_level: num(row.warning_level), critical_level: num(row.critical_level),
  measurement_unit: row.measurement_unit || 'm', created_at: dateValue(row.created_at), updated_at: dateValue(row.updated_at)
});

const clusterNodePayload = (row) => row && ({
  id: row.id,
  node_uuid: row.node_uuid,
  node_name: row.node_name,
  tailscale_ip: row.tailscale_ip,
  public_url: row.public_url,
  port: row.port == null ? null : Number(row.port),
  role: row.role,
  is_self: 0,
  metadata: parseJson(row.metadata) ?? {},
  power_score: Number(row.power_score ?? 5),
  structural_version: Number(row.structural_version ?? 1)
});

const getDataPointPayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute('SELECT * FROM data_points WHERE id=? LIMIT 1', [id]);
  return dataPointPayload(row);
};

const getClusterNodePayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute('SELECT * FROM cluster_nodes WHERE id=? LIMIT 1', [id]);
  return clusterNodePayload(row);
};

const getMeasurementPayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute(
    `SELECT m.*, dp.uuid AS data_point_uuid FROM measurements m JOIN data_points dp ON dp.id=m.data_point_id WHERE m.id=? LIMIT 1`, [id]
  );
  return row && {
    uuid: row.uuid, data_point_uuid: row.data_point_uuid, measurement_type: row.measurement_type,
    value: num(row.value), unit: row.unit || 'm', measured_at: dateValue(row.measured_at), source: row.source,
    observation: row.observation, created_at: dateValue(row.created_at)
  };
};

const getAlertPayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute(
    `SELECT a.*, dp.uuid AS data_point_uuid, m.uuid AS measurement_uuid
       FROM alerts a JOIN data_points dp ON dp.id=a.data_point_id LEFT JOIN measurements m ON m.id=a.measurement_id
      WHERE a.id=? LIMIT 1`, [id]
  );
  return row && {
    uuid: row.uuid, data_point_uuid: row.data_point_uuid, measurement_uuid: row.measurement_uuid,
    alert_type: row.alert_type, severity: row.severity, current_value: num(row.current_value), unit: row.unit || 'm',
    message: row.message, status: row.status, detected_at: dateValue(row.detected_at), resolved_at: dateValue(row.resolved_at),
    created_at: dateValue(row.created_at), updated_at: dateValue(row.updated_at)
  };
};

const getHistoricalImportPayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute(
    `SELECT hi.*, dp.uuid AS data_point_uuid FROM historical_imports hi LEFT JOIN data_points dp ON dp.id=hi.data_point_id WHERE hi.id=? LIMIT 1`, [id]
  );
  return row && {
    uuid: row.uuid, data_point_uuid: row.data_point_uuid, original_filename: row.original_filename, sensor_name: row.sensor_name,
    status: row.status, total_rows: Number(row.total_rows || 0), imported_rows: Number(row.imported_rows || 0), failed_rows: Number(row.failed_rows || 0),
    raw_unit: row.raw_unit || 'cm', converted_unit: row.converted_unit || 'm', error_message: row.error_message,
    created_at: dateValue(row.created_at), updated_at: dateValue(row.updated_at), completed_at: dateValue(row.completed_at)
  };
};

const getHistoricalMeasurementPayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute(
    `SELECT hm.*, dp.uuid AS data_point_uuid, hi.uuid AS import_uuid
       FROM historical_measurements hm JOIN data_points dp ON dp.id=hm.data_point_id LEFT JOIN historical_imports hi ON hi.id=hm.import_id
      WHERE hm.id=? LIMIT 1`, [id]
  );
  return row && {
    uuid: row.uuid, data_point_uuid: row.data_point_uuid, import_uuid: row.import_uuid, measured_at: dateOnly(row.measured_at),
    raw_value: num(row.raw_value), raw_unit: row.raw_unit || 'cm', value: num(row.value), unit: row.unit || 'm',
    max_value: num(row.max_value), min_value: num(row.min_value), source: row.source || 'CSV_IMPORT', created_at: dateValue(row.created_at)
  };
};

const getChartGenerationJobPayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute(
    `SELECT cj.*, dp.uuid AS joined_data_point_uuid, requested.node_uuid AS joined_requested_by_node_uuid,
            assigned.node_uuid AS joined_assigned_to_node_uuid, assigned.node_name AS assigned_to_node_name
       FROM chart_generation_jobs cj
       JOIN data_points dp ON dp.id=cj.data_point_id
       LEFT JOIN cluster_nodes requested ON requested.id=cj.requested_by_node_id
       LEFT JOIN cluster_nodes assigned ON assigned.id=cj.assigned_node_id
      WHERE cj.id=? LIMIT 1`, [id]
  );
  return row && {
    uuid: row.uuid,
    data_point_uuid: row.data_point_uuid || row.joined_data_point_uuid,
    chart_type: row.chart_type || 'HISTORICAL_RIVER_LEVEL',
    requested_by_node_uuid: row.requested_by_node_uuid || row.joined_requested_by_node_uuid,
    assigned_to_node_uuid: row.assigned_to_node_uuid || row.joined_assigned_to_node_uuid,
    assigned_to_node_name: row.assigned_to_node_name || row.assigned_node_name,
    status: row.status === 'RUNNING' ? 'PROCESSING' : row.status,
    progress_percent: Number(row.progress_percent || 0),
    estimated_seconds: row.estimated_seconds == null ? null : Number(row.estimated_seconds),
    error_message: row.error_message,
    started_at: dateValue(row.started_at),
    finished_at: dateValue(row.finished_at),
    created_at: dateValue(row.created_at),
    updated_at: dateValue(row.updated_at)
  };
};

const getChartCachePayloadById = async (id, connection = pool) => {
  const [[row]] = await connection.execute(
    `SELECT cc.*, dp.uuid AS joined_data_point_uuid, cn.node_uuid AS joined_generated_by_node_uuid
       FROM chart_cache cc
       JOIN data_points dp ON dp.id=cc.data_point_id
       LEFT JOIN cluster_nodes cn ON cn.id=cc.generated_by_node_id
      WHERE cc.id=? LIMIT 1`, [id]
  );
  return row && {
    uuid: row.uuid,
    data_point_uuid: row.data_point_uuid || row.joined_data_point_uuid,
    chart_type: row.chart_type,
    status: row.status,
    generated_by_node_uuid: row.generated_by_node_uuid || row.joined_generated_by_node_uuid,
    generated_by_node_name: row.generated_by_node_name,
    source_job_uuid: row.source_job_uuid,
    total_points: Number(row.total_points || 0),
    date_start: dateOnly(row.date_start),
    date_end: dateOnly(row.date_end),
    payload: reduceChartPayload(parseJson(row.payload)),
    summary: parseJson(row.summary),
    error_message: row.error_message,
    generated_at: dateValue(row.generated_at),
    created_at: dateValue(row.created_at),
    updated_at: dateValue(row.updated_at)
  };
};

module.exports = {
  dataPointPayload, clusterNodePayload, getDataPointPayloadById, getClusterNodePayloadById,
  getMeasurementPayloadById, getAlertPayloadById, getHistoricalImportPayloadById,
  getHistoricalMeasurementPayloadById, getChartGenerationJobPayloadById, getChartCachePayloadById
};
