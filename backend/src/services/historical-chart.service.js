const crypto = require('crypto');
const pool = require('../database/connection');
const syncEventService = require('./sync-event.service');
const syncPayloadService = require('./sync-payload.service');
const dataPointRepository = require('../repositories/data-point.repository');
const { selectBestProcessingNode } = require('./processing-node-selector.service');
const env = require('../config/env');

const CHART_TYPE = 'HISTORICAL_RIVER_LEVEL';
const MAX_POINTS = 1000;
const ACTIVE_JOB_REUSE_MINUTES = 2;

const parseJson = (value) => {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_error) { return null; }
};

const toNumberOrNull = (value) => (value == null ? null : Number(value));
const dateOnly = (value) => {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value).slice(0, 10);
};

const average = (items) => {
  if (!items.length) return null;
  return items.reduce((sum, item) => sum + Number(item.value || 0), 0) / items.length;
};

const calculateTrend = (rows) => {
  if (rows.length < 14) return 'UNKNOWN';
  const latest7 = rows.slice(-7);
  const previous7 = rows.slice(-14, -7);
  const diff = average(latest7) - average(previous7);
  if (diff > 0.10) return 'RISING';
  if (diff < -0.10) return 'FALLING';
  return 'STABLE';
};

const normalizeStatus = (status) => (status === 'RUNNING' ? 'PROCESSING' : status);

const createJobSyncEvent = async (jobId, connection = pool) => {
  const payload = await syncPayloadService.getChartGenerationJobPayloadById(jobId, connection);
  if (payload) await syncEventService.createEntitySyncEvent('chart_generation_job', payload, 'UPSERT', connection);
};

const createCacheSyncEvent = async (cacheId, connection = pool) => {
  const payload = await syncPayloadService.getChartCachePayloadById(cacheId, connection);
  if (payload) await syncEventService.createEntitySyncEvent('chart_cache', payload, 'UPSERT', connection);
};

const getLatestActiveJob = async (dataPointUuid) => {
  const [rows] = await pool.execute(
    `SELECT cj.*, cn.node_name AS assigned_to_node_name
       FROM chart_generation_jobs cj
       LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid
      WHERE cj.data_point_uuid = ? AND cj.chart_type = ? AND cj.status IN ('PENDING', 'PROCESSING')
      ORDER BY FIELD(cj.status, 'PROCESSING', 'PENDING'), cj.created_at DESC
      LIMIT 1`,
    [dataPointUuid, CHART_TYPE]
  );
  return rows[0] || null;
};

const getLatestJob = async (dataPointUuid) => {
  const [rows] = await pool.execute(
    `SELECT cj.*, cn.node_name AS assigned_to_node_name
       FROM chart_generation_jobs cj
       LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid
      WHERE cj.data_point_uuid = ? AND cj.chart_type = ?
      ORDER BY cj.created_at DESC
      LIMIT 1`,
    [dataPointUuid, CHART_TYPE]
  );
  return rows[0] || null;
};

const findReusableJob = async (dataPointUuid) => {
  const [rows] = await pool.execute(
    `SELECT cj.*, cn.node_name AS assigned_to_node_name
       FROM chart_generation_jobs cj
       LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid
      WHERE cj.data_point_uuid = ? AND cj.chart_type = ? AND cj.status IN ('PENDING', 'PROCESSING')
        AND cj.created_at >= DATE_SUB(NOW(), INTERVAL ${ACTIVE_JOB_REUSE_MINUTES} MINUTE)
      ORDER BY cj.created_at DESC
      LIMIT 1`,
    [dataPointUuid, CHART_TYPE]
  );
  return rows[0] || null;
};

const normalizeChart = (row) => row && ({
  available: row.status === 'READY',
  uuid: row.uuid,
  status: row.status,
  data: parseJson(row.payload),
  payload: parseJson(row.payload),
  summary: parseJson(row.summary),
  generated_at: row.generated_at,
  total_points: Number(row.total_points || 0),
  date_start: row.date_start,
  date_end: row.date_end,
  generated_by_node_uuid: row.generated_by_node_uuid,
  generated_by_node_name: row.generated_by_node_name,
  source_job_uuid: row.source_job_uuid,
  error_message: row.error_message
});

const normalizeJob = (job) => job && ({
  id: job.id,
  uuid: job.uuid,
  status: normalizeStatus(job.status),
  progress_percent: Number(job.progress_percent || 0),
  estimated_seconds: job.estimated_seconds,
  assigned_to_node_uuid: job.assigned_to_node_uuid,
  assigned_to: job.assigned_to_node_name || job.assigned_node_name || job.assigned_to_node_uuid || null,
  assigned_node_name: job.assigned_to_node_name || job.assigned_node_name || null,
  requested_by_node_uuid: job.requested_by_node_uuid,
  error_message: job.error_message
});

const chooseAssignee = async () => {
  const selection = await selectBestProcessingNode();
  const best = selection?.bestNode || selection?.selfNode || null;
  return { selection, assignedNode: best };
};

const createOrReuseJob = async (dataPoint, importId = null, options = {}) => {
  const reusable = await findReusableJob(dataPoint.uuid);
  if (reusable) return { job: reusable, reused: true };

  const { selection, assignedNode } = await chooseAssignee();
  const selfNode = selection?.selfNode || null;
  const [[countRow]] = await pool.execute('SELECT COUNT(*) AS total FROM historical_measurements WHERE data_point_id=?', [dataPoint.id]);
  const estimatedSeconds = Math.max(5, Math.ceil(Number(countRow.total || 0) / 5000));
  const jobUuid = crypto.randomUUID();
  const [result] = await pool.execute(
    `INSERT INTO chart_generation_jobs
      (uuid, data_point_id, chart_type, data_point_uuid, import_id, status, requested_by_node_id, requested_by_node_uuid,
       assigned_node_id, assigned_to_node_uuid, assigned_node_name, progress_percent, estimated_seconds)
     VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, ?, 0, ?)`,
    [jobUuid, dataPoint.id, CHART_TYPE, dataPoint.uuid, importId, selfNode?.id || null, selfNode?.node_uuid || null,
      assignedNode?.id || selfNode?.id || null, assignedNode?.node_uuid || selfNode?.node_uuid || null, assignedNode?.node_name || selfNode?.node_name || 'local', estimatedSeconds]
  );
  await pool.execute(
    `INSERT INTO chart_cache (uuid, data_point_id, data_point_uuid, chart_type, status)
     VALUES (?, ?, ?, ?, 'GENERATING')
     ON DUPLICATE KEY UPDATE status = IF(status='READY', 'STALE', 'GENERATING'), error_message=NULL`,
    [crypto.randomUUID(), dataPoint.id, dataPoint.uuid, CHART_TYPE]
  );
  await createJobSyncEvent(result.insertId);
  const [[job]] = await pool.execute(
    `SELECT cj.*, cn.node_name AS assigned_to_node_name FROM chart_generation_jobs cj
      LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid WHERE cj.id=?`,
    [result.insertId]
  );
  if (!options.silent) {
    console.log('[chart-worker] job atribuído a', job.assigned_to_node_name || job.assigned_node_name || job.assigned_to_node_uuid || 'local', `job_uuid=${job.uuid}`);
  }
  return { job, reused: false };
};

const getHistoricalChart = async (dataPointId) => {
  const dataPoint = await dataPointRepository.findById(dataPointId);
  if (!dataPoint) return null;
  const [cacheRows] = await pool.execute(
    `SELECT * FROM chart_cache WHERE data_point_uuid = ? AND chart_type = ? ORDER BY generated_at DESC, updated_at DESC LIMIT 1`,
    [dataPoint.uuid, CHART_TYPE]
  );
  let job = await getLatestActiveJob(dataPoint.uuid);
  let chart = normalizeChart(cacheRows[0]);
  const latestJob = job || await getLatestJob(dataPoint.uuid);
  const waitingForRemoteCache = latestJob?.status === 'DONE' && (!chart || ['GENERATING', 'STALE'].includes(chart.status));

  if (!job && !waitingForRemoteCache && (!chart || chart.status === 'FAILED' || chart.status === 'GENERATING')) {
    const queued = await createOrReuseJob(dataPoint, null, { silent: true });
    job = queued.job;
  } else if (!job && waitingForRemoteCache) {
    job = latestJob;
  }

  const jobStatus = normalizeStatus(job?.status);
  const ready = chart?.status === 'READY' && (!job || jobStatus === 'DONE');
  const status = ready ? 'READY' : (waitingForRemoteCache ? 'PROCESSING' : (jobStatus || (chart?.status === 'FAILED' ? 'FAILED' : 'NO_DATA')));
  let message = null;
  if (chart?.status === 'READY' && job) message = 'Um novo gráfico está sendo gerado. Exibindo a última versão disponível.';
  if (waitingForRemoteCache) message = 'Gráfico concluído no node responsável. Aguardando sincronização do cache.';
  if (!chart && jobStatus === 'PROCESSING') message = 'Gráfico sendo gerado.';
  if (!chart && jobStatus === 'PENDING') message = 'Aguardando processamento distribuído.';
  if (!chart && !job) message = 'Nenhum gráfico histórico disponível para este ponto.';

  return {
    ok: true,
    status,
    data_point: dataPoint,
    chart: chart?.status === 'READY' || chart?.status === 'STALE' ? chart : (chart || null),
    cache: chart ? { ...chart, available: chart.status === 'READY' || chart.status === 'STALE' } : { available: false },
    job: normalizeJob(job),
    message
  };
};

const regenerateChart = async (dataPointId, importId = null) => {
  const dataPoint = await dataPointRepository.findById(dataPointId);
  if (!dataPoint) return null;
  const { job, reused } = await createOrReuseJob(dataPoint, importId);
  return { ok: true, status: normalizeStatus(job.status), data_point: dataPoint, job: normalizeJob(job), message: reused ? 'Geração já estava em andamento.' : 'Geração de gráfico enfileirada.' };
};

const generateChartForJob = async (job, selfNode) => {
  const dataPointId = job.data_point_id;
  const dataPointUuid = job.data_point_uuid;
  const selfName = selfNode?.node_name || 'local';
  const selfUuid = selfNode?.node_uuid || null;
  await pool.execute(
    `UPDATE chart_generation_jobs SET status='PROCESSING', assigned_node_id=?, assigned_to_node_uuid=?, assigned_node_name=?,
      progress_percent=10, started_at=COALESCE(started_at, NOW()), error_message=NULL WHERE id=?`,
    [selfNode?.id || null, selfUuid, selfName, job.id]
  );
  await createJobSyncEvent(job.id);
  await pool.execute(
    `INSERT INTO chart_cache (uuid, data_point_id, data_point_uuid, chart_type, status, generated_by_node_id, generated_by_node_uuid, generated_by_node_name, source_job_uuid)
     VALUES (?, ?, ?, ?, 'GENERATING', ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE status=IF(status='READY','STALE','GENERATING'), generated_by_node_id=VALUES(generated_by_node_id),
       generated_by_node_uuid=VALUES(generated_by_node_uuid), generated_by_node_name=VALUES(generated_by_node_name), source_job_uuid=VALUES(source_job_uuid), error_message=NULL`,
    [crypto.randomUUID(), dataPointId, dataPointUuid, CHART_TYPE, selfNode?.id || null, selfUuid, selfName, job.uuid]
  );

  const [rows] = await pool.execute(
    `SELECT measured_at, value FROM historical_measurements WHERE data_point_id=? ORDER BY measured_at ASC`,
    [dataPointId]
  );
  await pool.execute('UPDATE chart_generation_jobs SET progress_percent=45 WHERE id=?', [job.id]);
  const total = rows.length;
  const sampled = total <= MAX_POINTS
    ? rows
    : Array.from({ length: MAX_POINTS }, (_value, index) => rows[Math.round(index * (total - 1) / (MAX_POINTS - 1))]);
  const labels = sampled.map((row) => dateOnly(row.measured_at));
  const values = sampled.map((row) => Number(row.value));
  const numericValues = rows.map((row) => Number(row.value)).filter(Number.isFinite);
  const latest = rows[rows.length - 1] || null;
  const summary = {
    total_measurements: total,
    sampled_points: sampled.length,
    min_value: numericValues.length ? Math.min(...numericValues) : null,
    max_value: numericValues.length ? Math.max(...numericValues) : null,
    average_value: numericValues.length ? numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length : null,
    latest_value: latest ? toNumberOrNull(latest.value) : null,
    latest_date: latest ? dateOnly(latest.measured_at) : null,
    date_start: rows[0] ? dateOnly(rows[0].measured_at) : null,
    date_end: latest ? dateOnly(latest.measured_at) : null,
    trend: calculateTrend(rows)
  };
  const payload = { labels, values, unit: 'm', full_count: total, sampled_count: sampled.length };
  await pool.execute('UPDATE chart_generation_jobs SET progress_percent=80 WHERE id=?', [job.id]);
  await pool.execute(
    `INSERT INTO chart_cache (uuid, data_point_id, data_point_uuid, chart_type, status, generated_by_node_id, generated_by_node_uuid,
      generated_by_node_name, source_job_uuid, total_points, date_start, date_end, payload, summary, generated_at)
     VALUES (?, ?, ?, ?, 'READY', ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE status='READY', generated_by_node_id=VALUES(generated_by_node_id), generated_by_node_uuid=VALUES(generated_by_node_uuid),
       generated_by_node_name=VALUES(generated_by_node_name), source_job_uuid=VALUES(source_job_uuid), total_points=VALUES(total_points), date_start=VALUES(date_start),
       date_end=VALUES(date_end), payload=VALUES(payload), summary=VALUES(summary), error_message=NULL, generated_at=NOW()`,
    [crypto.randomUUID(), dataPointId, dataPointUuid, CHART_TYPE, selfNode?.id || null, selfUuid, selfName, job.uuid, total, summary.date_start, summary.date_end, JSON.stringify(payload), JSON.stringify(summary)]
  );
  await pool.execute("UPDATE chart_generation_jobs SET status='DONE', progress_percent=100, finished_at=NOW() WHERE id=?", [job.id]);
  await createJobSyncEvent(job.id);
  const [[cacheRow]] = await pool.execute('SELECT id FROM chart_cache WHERE data_point_uuid=? AND chart_type=? LIMIT 1', [dataPointUuid, CHART_TYPE]);
  if (cacheRow) await createCacheSyncEvent(cacheRow.id);
  console.log(`[chart-worker] concluído job_uuid=${job.uuid} points=${total}`);
  return { payload, summary };
};

const markJobFailed = async (job, error) => {
  await pool.execute("UPDATE chart_generation_jobs SET status='FAILED', error_message=?, finished_at=NOW() WHERE id=?", [error.message, job.id]).catch(() => {});
  await pool.execute("UPDATE chart_cache SET status=IF(status='READY','READY','FAILED'), error_message=? WHERE data_point_uuid=? AND chart_type=?", [error.message, job.data_point_uuid, CHART_TYPE]).catch(() => {});
  await createJobSyncEvent(job.id).catch(() => {});
};

module.exports = { getHistoricalChart, regenerateChart, generateChartForJob, markJobFailed, createJobSyncEvent, CHART_TYPE, toNumberOrNull, normalizeStatus, CHART_JOB_TIMEOUT_SECONDS: env.chartJobTimeoutSeconds };
