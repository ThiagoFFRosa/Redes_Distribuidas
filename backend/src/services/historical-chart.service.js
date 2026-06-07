const crypto = require('crypto');
const pool = require('../database/connection');
const syncEventService = require('./sync-event.service');
const syncPayloadService = require('./sync-payload.service');
const dataPointRepository = require('../repositories/data-point.repository');
const clusterNodeRepository = require('./cluster-node.repository');
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

const emptySummary = () => ({
  points_count: 0,
  total_measurements: 0,
  sampled_points: 0,
  min: null,
  max: null,
  avg: null,
  min_value: null,
  max_value: null,
  average_value: null,
  latest_value: null,
  latest_date: null,
  date_start: null,
  date_end: null,
  trend: 'UNKNOWN'
});

const normalizeChartPayload = (payload, unit = 'm') => {
  if (!payload) return null;
  if (Array.isArray(payload.labels) && Array.isArray(payload.datasets)) return payload;
  if (Array.isArray(payload.labels) && Array.isArray(payload.values)) {
    return {
      ...payload,
      datasets: [{ label: `Cota histórica (${payload.unit || unit})`, data: payload.values }]
    };
  }
  return payload;
};

const isValidChartPayload = (payload) => Boolean(
  payload
  && Array.isArray(payload.labels)
  && Array.isArray(payload.datasets)
  && payload.datasets.length > 0
  && Array.isArray(payload.datasets[0].data)
);

const pointsCountFromSummary = (summary, row) => Number(
  summary?.points_count
  ?? summary?.sampled_points
  ?? summary?.total_measurements
  ?? row?.total_points
  ?? 0
);

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
       LEFT JOIN chart_cache cc ON cc.data_point_uuid = cj.data_point_uuid AND cc.chart_type = cj.chart_type AND cc.status = 'READY'
      WHERE cj.data_point_uuid = ? AND cj.chart_type = ?
        AND cj.created_at >= DATE_SUB(NOW(), INTERVAL ${ACTIVE_JOB_REUSE_MINUTES} MINUTE)
        AND (cj.status IN ('PENDING', 'PROCESSING') OR (cj.status = 'DONE' AND cc.id IS NULL))
      ORDER BY FIELD(cj.status, 'PROCESSING', 'PENDING', 'DONE'), cj.created_at DESC
      LIMIT 1`,
    [dataPointUuid, CHART_TYPE]
  );
  return rows[0] || null;
};

const normalizeChart = (row) => {
  if (!row) return null;
  const rawSummary = parseJson(row.summary) || {};
  const summary = {
    ...rawSummary,
    points_count: pointsCountFromSummary(rawSummary, row),
    min: rawSummary.min ?? rawSummary.min_value ?? null,
    max: rawSummary.max ?? rawSummary.max_value ?? null,
    avg: rawSummary.avg ?? rawSummary.average_value ?? null,
    min_value: rawSummary.min_value ?? rawSummary.min ?? null,
    max_value: rawSummary.max_value ?? rawSummary.max ?? null,
    average_value: rawSummary.average_value ?? rawSummary.avg ?? null
  };
  const payload = normalizeChartPayload(parseJson(row.payload));
  return {
    available: row.status === 'READY',
    uuid: row.uuid,
    status: row.status,
    data: payload,
    payload,
    summary,
    generated_at: row.generated_at,
    points_count: summary.points_count,
    total_points: Number(row.total_points || summary.points_count || 0),
    date_start: row.date_start || summary.date_start || null,
    date_end: row.date_end || summary.date_end || null,
    generated_by_node_uuid: row.generated_by_node_uuid,
    generated_by_node_name: row.generated_by_node_name,
    source_job_uuid: row.source_job_uuid,
    error_message: row.error_message
  };
};

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
  error_message: job.error_message,
  finished_at: job.finished_at
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
  const [[countRow]] = await pool.execute(
    `SELECT COUNT(*) AS total
       FROM historical_measurements hm JOIN data_points dp ON dp.id=hm.data_point_id
      WHERE dp.uuid=?`,
    [dataPoint.uuid]
  );
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
  await createJobSyncEvent(result.insertId);
  const [[job]] = await pool.execute(
    `SELECT cj.*, cn.node_name AS assigned_to_node_name FROM chart_generation_jobs cj
      LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid WHERE cj.id=?`,
    [result.insertId]
  );
  if (!options.silent) {
    console.log('[chart-worker] job atribuído a', job.assigned_to_node_name || job.assigned_node_name || job.assigned_to_node_uuid || 'local', `job_uuid=${job.uuid}`, `data_point_uuid=${dataPoint.uuid}`);
  }
  return { job, reused: false };
};

const getHistoricalChart = async (dataPointId) => {
  const dataPoint = await dataPointRepository.findById(dataPointId);
  if (!dataPoint) return null;
  console.log(`[historical-chart] point_id=${dataPoint.id} data_point_uuid=${dataPoint.uuid}`);

  const [cacheRows] = await pool.execute(
    `SELECT * FROM chart_cache
      WHERE data_point_uuid = ? AND chart_type = ?
      ORDER BY generated_at DESC, id DESC
      LIMIT 1`,
    [dataPoint.uuid, CHART_TYPE]
  );
  let chart = normalizeChart(cacheRows[0]);
  const [mismatchCacheRows] = chart ? [[]] : await pool.execute(
    `SELECT uuid, data_point_uuid, source_job_uuid, chart_type, generated_at
       FROM chart_cache
      WHERE data_point_id = ? AND chart_type = ? AND data_point_uuid <> ?
      ORDER BY generated_at DESC, id DESC
      LIMIT 1`,
    [dataPoint.id, CHART_TYPE, dataPoint.uuid]
  );
  const mismatchCache = mismatchCacheRows?.[0] || null;
  const chartPoints = Number(chart?.points_count || chart?.payload?.datasets?.[0]?.data?.length || 0);
  console.log(`[historical-chart] cache encontrado=${Boolean(chart)}${chart ? ` cache_uuid=${chart.uuid || '-'} points=${chartPoints}` : ` para data_point_uuid=${dataPoint.uuid}`}`);

  const latestJobForResponse = await getLatestJob(dataPoint.uuid);
  const selfNode = await clusterNodeRepository.getSelfNode();
  if (latestJobForResponse) {
    console.log(`[historical-chart] job status=${normalizeStatus(latestJobForResponse.status)} assigned_to=${latestJobForResponse.assigned_to_node_name || latestJobForResponse.assigned_node_name || latestJobForResponse.assigned_to_node_uuid || '-'} self=${selfNode?.node_name || selfNode?.node_uuid || '-'}`);
  }

  if (mismatchCache) {
    const message = `Cache de gráfico encontrado para o mesmo ponto local, mas com data_point_uuid divergente (${mismatchCache.data_point_uuid} != ${dataPoint.uuid}). Rode npm run sync:diagnose-duplicates e corrija com sync:merge-data-points em ambiente dev.`;
    console.error(`[historical-chart] UUID_MISMATCH point_id=${dataPoint.id} data_point_uuid=${dataPoint.uuid} cache_uuid=${mismatchCache.uuid} cache_data_point_uuid=${mismatchCache.data_point_uuid}`);
    return {
      ok: true,
      status: 'UUID_MISMATCH',
      data_point: dataPoint,
      chart: null,
      cache: { available: false, ...mismatchCache },
      job: normalizeJob(latestJobForResponse),
      message
    };
  }

  if (chart?.status === 'READY') {
    let status = 'READY';
    let message = 'Gráfico pronto.';
    if (chartPoints === 0) {
      status = 'NO_DATA';
      message = 'Este ponto ainda não possui dados históricos/medições.';
    } else if (!isValidChartPayload(chart.payload) || chart.payload.datasets[0].data.length === 0) {
      status = 'FAILED';
      message = 'Cache do gráfico possui payload inválido. Solicite a atualização do gráfico.';
      chart = { ...chart, available: false, error_message: message };
    }
    const cache = { ...chart, available: status === 'READY' || status === 'NO_DATA' };
    console.log(`[historical-chart] retornando status=${status}`);
    return {
      ok: true,
      status,
      data_point: dataPoint,
      chart: cache.available ? chart : null,
      cache,
      job: normalizeJob(latestJobForResponse),
      message
    };
  }

  if (chart?.status === 'FAILED') {
    const message = chart.error_message || 'Falha ao gerar cache do gráfico.';
    console.log('[historical-chart] retornando status=FAILED');
    return {
      ok: true,
      status: 'FAILED',
      data_point: dataPoint,
      chart: null,
      cache: { ...chart, available: false },
      job: normalizeJob(latestJobForResponse),
      message
    };
  }

  let job = await getLatestActiveJob(dataPoint.uuid);
  const latestJob = job || latestJobForResponse;

  if (!job && !latestJob && !chart) {
    const queued = await createOrReuseJob(dataPoint, null, { silent: true });
    job = queued.job;
  } else if (!job) {
    job = latestJob;
  }

  const jobStatus = normalizeStatus(job?.status);
  const assignedName = job?.assigned_to_node_name || job?.assigned_node_name || job?.assigned_to_node_uuid || null;
  const assignedToSelf = Boolean((job?.assigned_to_node_uuid && selfNode?.node_uuid && job.assigned_to_node_uuid === selfNode.node_uuid)
    || (job?.assigned_node_id && selfNode?.id && Number(job.assigned_node_id) === Number(selfNode.id)));
  let status = 'NO_DATA';
  let message = 'Este ponto ainda não possui dados históricos/medições.';

  if (jobStatus === 'FAILED') {
    status = 'FAILED';
    message = job?.error_message || 'Falha ao gerar gráfico histórico.';
  } else if (jobStatus === 'PENDING' || jobStatus === 'PROCESSING') {
    status = 'PROCESSING';
    message = jobStatus === 'PENDING' ? 'Aguardando processamento distribuído.' : 'Gráfico sendo gerado.';
  } else if (jobStatus === 'DONE') {
    if (assignedToSelf) {
      status = 'CACHE_MISSING_LOCAL';
      message = 'Job local concluído, mas chart_cache não foi encontrado.';
    } else {
      status = 'WAITING_CACHE_SYNC';
      message = `Gráfico gerado${assignedName ? ` em ${assignedName}` : ' no node responsável'}. Aguardando cache chegar neste servidor.`;
    }
  }

  console.log(`[historical-chart] retornando status=${status}`);
  const cache = chart ? { ...chart, available: false } : { available: false };
  return {
    ok: true,
    status,
    data_point: dataPoint,
    chart: null,
    cache,
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
  const dataPointUuid = job.data_point_uuid;
  const [[jobDataPoint]] = await pool.execute('SELECT id, uuid FROM data_points WHERE uuid=? LIMIT 1', [dataPointUuid]);
  if (!jobDataPoint) throw new Error(`data_point_uuid do job não existe localmente: ${dataPointUuid}`);
  const dataPointId = jobDataPoint.id;
  if (Number(job.data_point_id) !== Number(dataPointId)) {
    console.error(`[chart-worker] job data_point_id mismatch job_uuid=${job.uuid} job_data_point_id=${job.data_point_id} uuid=${dataPointUuid} local_data_point_id=${dataPointId}`);
  }
  console.log(`[chart-worker] processando job_uuid=${job.uuid} data_point_uuid=${dataPointUuid}`);
  const selfName = selfNode?.node_name || 'local';
  const selfUuid = selfNode?.node_uuid || null;
  await pool.execute(
    `UPDATE chart_generation_jobs SET status='PROCESSING', assigned_node_id=?, assigned_to_node_uuid=?, assigned_node_name=?,
      progress_percent=10, started_at=COALESCE(started_at, NOW()), error_message=NULL WHERE id=?`,
    [selfNode?.id || null, selfUuid, selfName, job.id]
  );
  await createJobSyncEvent(job.id);

  const [rows] = await pool.execute(
    `SELECT hm.measured_at, hm.value
       FROM historical_measurements hm JOIN data_points dp ON dp.id=hm.data_point_id
      WHERE dp.uuid=? ORDER BY hm.measured_at ASC`,
    [dataPointUuid]
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
  const summary = total === 0 ? emptySummary() : {
    points_count: sampled.length,
    total_measurements: total,
    sampled_points: sampled.length,
    min: numericValues.length ? Math.min(...numericValues) : null,
    max: numericValues.length ? Math.max(...numericValues) : null,
    avg: numericValues.length ? numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length : null,
    min_value: numericValues.length ? Math.min(...numericValues) : null,
    max_value: numericValues.length ? Math.max(...numericValues) : null,
    average_value: numericValues.length ? numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length : null,
    latest_value: latest ? toNumberOrNull(latest.value) : null,
    latest_date: latest ? dateOnly(latest.measured_at) : null,
    date_start: rows[0] ? dateOnly(rows[0].measured_at) : null,
    date_end: latest ? dateOnly(latest.measured_at) : null,
    trend: calculateTrend(rows)
  };
  const payload = total === 0
    ? { labels: [], datasets: [{ label: 'Cota histórica (m)', data: [] }], values: [], unit: 'm', full_count: 0, sampled_count: 0 }
    : { labels, datasets: [{ label: 'Cota histórica (m)', data: values }], values, unit: 'm', full_count: total, sampled_count: sampled.length };
  await pool.execute('UPDATE chart_generation_jobs SET progress_percent=80 WHERE id=?', [job.id]);

  let cacheId = null;
  let cacheUuid = null;
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    const [cacheResult] = await connection.execute(
      `INSERT INTO chart_cache (uuid, data_point_id, data_point_uuid, chart_type, status, generated_by_node_id, generated_by_node_uuid,
        generated_by_node_name, source_job_uuid, total_points, date_start, date_end, payload, summary, generated_at)
       VALUES (?, ?, ?, ?, 'READY', ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE status='READY', generated_by_node_id=VALUES(generated_by_node_id), generated_by_node_uuid=VALUES(generated_by_node_uuid),
         generated_by_node_name=VALUES(generated_by_node_name), source_job_uuid=VALUES(source_job_uuid), total_points=VALUES(total_points), date_start=VALUES(date_start),
         date_end=VALUES(date_end), payload=VALUES(payload), summary=VALUES(summary), error_message=NULL, generated_at=NOW()`,
      [crypto.randomUUID(), dataPointId, dataPointUuid, CHART_TYPE, selfNode?.id || null, selfUuid, selfName, job.uuid, total, summary.date_start, summary.date_end, JSON.stringify(payload), JSON.stringify(summary)]
    );
    if (cacheResult.insertId) {
      cacheId = cacheResult.insertId;
    }
    const [[cacheRow]] = await connection.execute('SELECT id, uuid, source_job_uuid FROM chart_cache WHERE data_point_uuid=? AND chart_type=? ORDER BY generated_at DESC, id DESC LIMIT 1', [dataPointUuid, CHART_TYPE]);
    cacheId = cacheId || cacheRow?.id || null;
    cacheUuid = cacheRow?.uuid || null;
    if (!cacheId) throw new Error('chart_cache não foi salvo');
    await connection.execute("UPDATE chart_generation_jobs SET status='DONE', progress_percent=100, finished_at=NOW(), error_message=NULL WHERE id=?", [job.id]);
    await createCacheSyncEvent(cacheId, connection);
    await createJobSyncEvent(job.id, connection);
    await connection.commit();
  } catch (error) {
    await connection.rollback().catch(() => {});
    await pool.execute("UPDATE chart_generation_jobs SET status='FAILED', error_message=?, finished_at=NOW() WHERE id=?", [error.message, job.id]).catch(() => {});
    await createJobSyncEvent(job.id).catch(() => {});
    throw error;
  } finally {
    connection.release();
  }

  console.log(`[chart-worker] job DONE uuid=${job.uuid}`);
  console.log(`[chart-worker] chart_cache salvo uuid=${cacheUuid || '-'} data_point_uuid=${dataPointUuid} points=${total} source_job_uuid=${job.uuid}`);
  return { payload, summary };
};

const markJobFailed = async (job, error) => {
  await pool.execute("UPDATE chart_generation_jobs SET status='FAILED', error_message=?, finished_at=NOW() WHERE id=?", [error.message, job.id]).catch(() => {});
  await pool.execute("UPDATE chart_cache SET status=IF(status='READY','READY','FAILED'), error_message=? WHERE data_point_uuid=? AND chart_type=?", [error.message, job.data_point_uuid, CHART_TYPE]).catch(() => {});
  await createJobSyncEvent(job.id).catch(() => {});
};

module.exports = { getHistoricalChart, regenerateChart, generateChartForJob, markJobFailed, createJobSyncEvent, CHART_TYPE, toNumberOrNull, normalizeStatus, isValidChartPayload, CHART_JOB_TIMEOUT_SECONDS: env.chartJobTimeoutSeconds };
