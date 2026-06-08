const crypto = require('crypto');
const pool = require('../database/connection');
const syncEventService = require('./sync-event.service');
const syncPayloadService = require('./sync-payload.service');
const dataPointRepository = require('../repositories/data-point.repository');
const clusterNodeRepository = require('./cluster-node.repository');
const { selectBestProcessingNode } = require('./processing-node-selector.service');
const { getPointTimeSeries } = require('./point-time-series.service');
const env = require('../config/env');

const CHART_TYPE = 'HISTORICAL_RIVER_LEVEL';
const MAX_POINTS = 1500;
const FORECAST_MAX_HOURS = 48;
const SEASONAL_WINDOW_DAYS = 30;
const ACTIVE_JOB_REUSE_MINUTES = 2;

const parseJson = (value) => {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_error) { return null; }
};

const toNumberOrNull = (value) => (value == null ? null : Number(value));
const dateOnly = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isNaN(date.getTime())) return date.toISOString().slice(0, 10);
  return String(value).slice(0, 10);
};

const average = (items) => {
  if (!items.length) return null;
  return items.reduce((sum, item) => sum + Number(item.value || 0), 0) / items.length;
};


const valuesOnly = (rows) => rows.map((row) => Number(row.value)).filter(Number.isFinite);
const median = (values) => { const sorted = values.filter(Number.isFinite).sort((a, b) => a - b); if (!sorted.length) return null; const mid = Math.floor(sorted.length / 2); return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2; };
const stdDev = (values) => { const finite = values.filter(Number.isFinite); if (finite.length < 2) return null; const mean = finite.reduce((sum, value) => sum + value, 0) / finite.length; return Math.sqrt(finite.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (finite.length - 1)); };
const percentileRank = (values, currentValue) => { const finite = values.filter(Number.isFinite).sort((a, b) => a - b); if (!finite.length || !Number.isFinite(currentValue)) return null; return (finite.filter((value) => value <= currentValue).length / finite.length) * 100; };
const dayOfYear = (value) => { const date = value instanceof Date ? value : new Date(value); if (Number.isNaN(date.getTime())) return null; const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 0)); return Math.floor((date - start) / 86400000); };
const dayDistance = (a, b) => { if (a == null || b == null) return 999; const diff = Math.abs(a - b); return Math.min(diff, 366 - diff); };
const linearRegression = (points) => { if (points.length < 2) return null; const n = points.length; const sumX = points.reduce((sum, point) => sum + point.x, 0); const sumY = points.reduce((sum, point) => sum + point.y, 0); const sumXY = points.reduce((sum, point) => sum + point.x * point.y, 0); const sumX2 = points.reduce((sum, point) => sum + point.x * point.x, 0); const denominator = n * sumX2 - sumX * sumX; if (!denominator) return null; return { slope: (n * sumXY - sumX * sumY) / denominator, intercept: (sumY - ((n * sumXY - sumX * sumY) / denominator) * sumX) / n }; };
const classifySeasonalStatus = (percentile) => { if (percentile == null) return 'INSUFFICIENT_DATA'; if (percentile < 10) return 'MUITO_ABAIXO_DO_NORMAL'; if (percentile < 20) return 'ABAIXO_DO_NORMAL'; if (percentile <= 80) return 'DENTRO_DO_NORMAL'; if (percentile <= 90) return 'ACIMA_DO_NORMAL'; return 'MUITO_ACIMA_DO_NORMAL'; };
const riskProjection = (predictedValue, dataPoint) => { if (!Number.isFinite(predictedValue)) return 'INDEFINIDO'; const critical = toNumberOrNull(dataPoint?.critical_level); const warning = toNumberOrNull(dataPoint?.warning_level); if (critical != null && predictedValue >= critical) return 'RISCO_CRITICO'; if (warning != null && predictedValue >= warning) return 'ATENCAO'; return 'SEM_RISCO'; };
const movingAverageAt = (rows, index, windowSize = 7) => { const window = rows.slice(Math.max(0, index - windowSize + 1), index + 1).map((row) => Number(row.value)).filter(Number.isFinite); return window.length ? window.reduce((sum, value) => sum + value, 0) / window.length : null; };
const buildSeasonalAnalysis = (rows) => { const latest = rows[rows.length - 1]; if (!latest) return { available: false, status: 'INSUFFICIENT_DATA', message: 'Dados insuficientes para análise sazonal.' }; const latestDate = new Date(latest.date); const latestDoy = dayOfYear(latestDate); const recentStart = new Date(latestDate.getTime() - (SEASONAL_WINDOW_DAYS - 1) * 86400000); const recentValues = valuesOnly(rows.filter((row) => new Date(row.date) >= recentStart && new Date(row.date) <= latestDate)); const historicalValues = valuesOnly(rows.filter((row) => { const date = new Date(row.date); return date.getUTCFullYear() < latestDate.getUTCFullYear() && dayDistance(dayOfYear(date), latestDoy) <= Math.floor(SEASONAL_WINDOW_DAYS / 2); })); if (historicalValues.length < 5 || !recentValues.length) return { available: false, status: 'INSUFFICIENT_DATA', message: 'Dados insuficientes para análise sazonal.', sample_size: historicalValues.length }; const historicalMean = historicalValues.reduce((sum, value) => sum + value, 0) / historicalValues.length; const deviation = stdDev(historicalValues) || 0; const currentValue = Number(latest.value); const percentile = percentileRank(historicalValues, currentValue); const status = classifySeasonalStatus(percentile); return { available: true, status, reference_days: SEASONAL_WINDOW_DAYS, sample_size: historicalValues.length, historical_mean: historicalMean, historical_median: median(historicalValues), historical_std_dev: deviation, historical_range_min: historicalMean - deviation, historical_range_max: historicalMean + deviation, historical_min: Math.min(...historicalValues), historical_max: Math.max(...historicalValues), difference_from_mean: currentValue - historicalMean, percentile, recent_amplitude: Math.max(...recentValues) - Math.min(...recentValues), message: `Comparado ao histórico desta época do ano, o ponto está em condição ${status.toLowerCase().replaceAll('_', ' ')}.` }; };
const buildForecast = (rows, dataPoint, unit = 'm') => { if (rows.length < 4) return { available: false, status: 'INSUFFICIENT_DATA', message: 'Dados insuficientes para previsão.' }; const latest = rows[rows.length - 1]; const latestTime = new Date(latest.date).getTime(); const recent = rows.filter((row) => latestTime - new Date(row.date).getTime() <= 7 * 86400000).slice(-24); if (recent.length < 4) return { available: false, status: 'INSUFFICIENT_DATA', message: 'Dados insuficientes para previsão.' }; const firstTime = new Date(recent[0].date).getTime(); const spanHours = (latestTime - firstTime) / 3600000; if (spanHours <= 0) return { available: false, status: 'INSUFFICIENT_DATA', message: 'Dados insuficientes para previsão.' }; const regression = linearRegression(recent.map((row) => ({ x: (new Date(row.date).getTime() - firstTime) / 3600000, y: Number(row.value) }))); if (!regression) return { available: false, status: 'INSUFFICIENT_DATA', message: 'Dados insuficientes para previsão.' }; const horizonHours = Math.min(FORECAST_MAX_HOURS, spanHours >= 24 ? 48 : 24); const currentX = (latestTime - firstTime) / 3600000; const currentValue = Number(latest.value); const predictedValue = regression.intercept + regression.slope * (currentX + horizonHours); const predictedChange = predictedValue - currentValue; const trend = Math.abs(predictedChange) < 0.03 ? 'STABLE' : predictedChange > 0 ? 'RISING' : 'FALLING'; return { available: true, horizon_hours: horizonHours, trend, predicted_change: predictedChange, predicted_value: predictedValue, confidence: recent.length >= 12 && spanHours >= 48 ? 'ALTA' : recent.length >= 6 ? 'MEDIA' : 'BAIXA', risk_projection: riskProjection(predictedValue, dataPoint), method: 'Regressão linear leve com janela recente', note: 'Previsão simples baseada em tendência recente; não é modelo hidrológico oficial.', points: [{ date: dateOnly(latest.date), value: currentValue, unit, source: 'OBSERVED_FORECAST_ANCHOR' }, { date: new Date(latestTime + horizonHours * 3600000).toISOString().slice(0, 10), value: predictedValue, unit, source: 'FORECAST' }] }; };

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
      datasets: [{ label: `Cota histórica / medições (${payload.unit || unit})`, data: payload.values }]
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


const markChartCacheStale = async (dataPointUuid, connection = pool) => {
  if (!dataPointUuid) return;
  const [result] = await connection.execute(
    `UPDATE chart_cache
        SET status='STALE', error_message=NULL
      WHERE data_point_uuid=? AND chart_type=? AND status='READY'`,
    [dataPointUuid, CHART_TYPE]
  );
  if (result.affectedRows) {
    const [rows] = await connection.execute('SELECT id FROM chart_cache WHERE data_point_uuid=? AND chart_type=?', [dataPointUuid, CHART_TYPE]);
    for (const row of rows) await createCacheSyncEvent(row.id, connection).catch(() => {});
  }
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


const isSortMemoryError = (error) => error?.code === 'ER_OUT_OF_SORTMEMORY' || error?.errno === 1038;

const getLatestChartCache = async (dataPointUuid, chartType = CHART_TYPE, connection = pool) => {
  try {
    const [ids] = await connection.execute(
      `SELECT id
         FROM chart_cache
        WHERE data_point_uuid = ? AND chart_type = ?
        LIMIT 1`,
      [dataPointUuid, chartType]
    );
    if (!ids.length) return null;

    const [rows] = await connection.execute(
      `SELECT id, uuid, data_point_id, data_point_uuid, chart_type, status, source_job_uuid,
              generated_by_node_id, generated_by_node_uuid, generated_by_node_name,
              total_points, date_start, date_end,
              payload, payload_json, summary, summary_json, seasonal_analysis_json, forecast_json,
              error_message, generated_at, created_at, updated_at
         FROM chart_cache
        WHERE id = ?
        LIMIT 1`,
      [ids[0].id]
    );
    return rows[0] || null;
  } catch (error) {
    if (isSortMemoryError(error)) {
      console.error(`[historical-chart] falha controlada ao consultar chart_cache por índice data_point_uuid=${dataPointUuid} chart_type=${chartType}: ${error.message}`);
    }
    throw error;
  }
};

const findReusableJob = async (dataPointUuid) => {
  const [rows] = await pool.execute(
    `SELECT cj.*, cn.node_name AS assigned_to_node_name
       FROM chart_generation_jobs cj
       LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid
       LEFT JOIN chart_cache cc ON cc.data_point_uuid = cj.data_point_uuid AND cc.chart_type = cj.chart_type
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
  const rawSummary = parseJson(row.summary_json) || parseJson(row.summary) || {};
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
  const payload = normalizeChartPayload(parseJson(row.payload_json) || parseJson(row.payload));
  const seasonalAnalysis = parseJson(row.seasonal_analysis_json) || payload?.seasonal_analysis || null;
  const forecast = parseJson(row.forecast_json) || payload?.forecast || null;
  return {
    available: row.status === 'READY',
    uuid: row.uuid,
    status: row.status,
    data: payload,
    payload,
    summary,
    seasonal_analysis: seasonalAnalysis,
    forecast,
    generated_at: row.generated_at,
    points_count: summary.points_count,
    total_points: Number(row.total_points || summary.points_count || 0),
    date_start: row.date_start || summary.date_start || null,
    date_end: row.date_end || summary.date_end || null,
    generated_by_node_uuid: row.generated_by_node_uuid,
    generated_by_node_name: row.generated_by_node_name,
    source_job_uuid: row.source_job_uuid,
    error_message: row.error_message,
    stale: row.status === 'STALE'
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
  const self = selection?.selfNode || null;
  if (!self) return { selection, assignedNode: best };
  const assignedNode = best && Number(best.power_score ?? 5) > Number(self.power_score ?? 5) ? best : self;
  return { selection, assignedNode };
};

const createOrReuseJob = async (dataPoint, importId = null, options = {}) => {
  const reusable = await findReusableJob(dataPoint.uuid);
  if (reusable) return { job: reusable, reused: true };

  const { selection, assignedNode } = await chooseAssignee();
  const selfNode = selection?.selfNode || null;
  const timeSeries = await getPointTimeSeries(dataPoint.uuid);
  const estimatedSeconds = Math.max(5, Math.ceil(Number(timeSeries.length || 0) / 5000));
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

  let cacheRow = null;
  try {
    cacheRow = await getLatestChartCache(dataPoint.uuid, CHART_TYPE);
  } catch (error) {
    if (isSortMemoryError(error)) {
      return {
        ok: false,
        status: 'FAILED',
        data_point: dataPoint,
        chart: null,
        cache: { available: false },
        job: normalizeJob(await getLatestJob(dataPoint.uuid).catch(() => null)),
        message: 'Falha ao consultar cache do gráfico. Índice/ordenação precisa ser otimizado.',
        error: 'Falha ao consultar cache do gráfico. Índice/ordenação precisa ser otimizado.'
      };
    }
    throw error;
  }
  let chart = normalizeChart(cacheRow);
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
      message = 'Este ponto ainda não possui dados históricos ou medições cadastradas.';
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


  if (chart?.status === 'STALE') {
    const message = 'Existem medições novas. Atualize o gráfico.';
    console.log('[historical-chart] retornando status=STALE');
    return {
      ok: true,
      status: 'STALE',
      data_point: dataPoint,
      chart,
      cache: { ...chart, available: true, stale: true },
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
  let message = 'Este ponto ainda não possui dados históricos ou medições cadastradas.';

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
  const [[jobDataPoint]] = await pool.execute('SELECT id, uuid, measurement_unit, normal_level, warning_level, critical_level FROM data_points WHERE uuid=? LIMIT 1', [dataPointUuid]);
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

  const rows = await getPointTimeSeries(dataPointUuid);
  await pool.execute('UPDATE chart_generation_jobs SET progress_percent=45 WHERE id=?', [job.id]);
  const total = rows.length;
  const sampled = total <= MAX_POINTS
    ? rows
    : Array.from({ length: MAX_POINTS }, (_value, index) => rows[Math.round(index * (total - 1) / (MAX_POINTS - 1))]);
  const unit = rows[0]?.unit || jobDataPoint.measurement_unit || 'm';
  const labels = sampled.map((row) => dateOnly(row.date));
  const values = sampled.map((row) => Number(row.value));
  const points = sampled.map((row) => ({ date: dateOnly(row.date), value: Number(row.value), unit: row.unit || unit, source: row.source }));
  const sampledIndexes = sampled.map((sample) => rows.findIndex((row) => row.date === sample.date));
  const seasonalAnalysis = buildSeasonalAnalysis(rows);
  const forecast = buildForecast(rows, jobDataPoint, unit);
  const seasonalMeanData = sampled.map(() => seasonalAnalysis.available ? seasonalAnalysis.historical_mean : null);
  const seasonalMinData = sampled.map(() => seasonalAnalysis.available ? seasonalAnalysis.historical_range_min : null);
  const seasonalMaxData = sampled.map(() => seasonalAnalysis.available ? seasonalAnalysis.historical_range_max : null);
  const trendData = sampledIndexes.map((rowIndex, sampleIndex) => movingAverageAt(rows, rowIndex >= 0 ? rowIndex : sampleIndex, 7));
  const forecastLabels = forecast.available ? forecast.points.map((point) => point.date) : [];
  const allLabels = [...labels, ...forecastLabels.slice(1)];
  const forecastData = forecast.available ? [...Array(Math.max(labels.length - 1, 0)).fill(null), ...forecast.points.map((point) => Number(point.value))] : allLabels.map(() => null);
  const chartPoints = forecast.available ? [...points, forecast.points[1]] : points;
  const numericValues = rows.map((row) => Number(row.value)).filter(Number.isFinite);
  const latest = rows[rows.length - 1] || null;
  const averageValue = numericValues.length ? numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length : null;
  const summary = total === 0 ? emptySummary() : {
    points_count: total,
    total_measurements: total,
    sampled_points: sampled.length,
    min: numericValues.length ? Math.min(...numericValues) : null,
    max: numericValues.length ? Math.max(...numericValues) : null,
    avg: averageValue,
    min_value: numericValues.length ? Math.min(...numericValues) : null,
    max_value: numericValues.length ? Math.max(...numericValues) : null,
    average_value: averageValue,
    latest_value: latest ? toNumberOrNull(latest.value) : null,
    latest_date: latest ? dateOnly(latest.date) : null,
    date_start: rows[0] ? dateOnly(rows[0].date) : null,
    date_end: latest ? dateOnly(latest.date) : null,
    trend: calculateTrend(rows),
    seasonal_status: seasonalAnalysis.status,
    forecast_trend: forecast.trend || 'UNKNOWN',
    risk_projection: forecast.risk_projection || 'INDEFINIDO'
  };
  const referenceLines = [
    jobDataPoint.normal_level != null ? { label: `Nível normal (${unit})`, value: Number(jobDataPoint.normal_level), color: '#64748b' } : null,
    jobDataPoint.warning_level != null ? { label: `Nível de atenção (${unit})`, value: Number(jobDataPoint.warning_level), color: '#f59e0b' } : null,
    jobDataPoint.critical_level != null ? { label: `Nível crítico (${unit})`, value: Number(jobDataPoint.critical_level), color: '#dc2626' } : null
  ].filter(Boolean);
  const payload = total === 0
    ? { labels: [], datasets: [{ label: `Nível observado (${unit})`, data: [] }], values: [], points: [], unit, full_count: 0, sampled_count: 0, seasonal_analysis: seasonalAnalysis, forecast, reference_lines: [] }
    : { labels: allLabels, datasets: [
      { label: `Nível observado (${unit})`, data: [...values, null], borderColor: '#0d9488', backgroundColor: 'rgba(13,148,136,0.10)', tension: 0.25, pointRadius: 0, fill: false },
      { label: `Faixa típica superior (${unit})`, data: [...seasonalMaxData, seasonalAnalysis.available ? seasonalAnalysis.historical_range_max : null], borderColor: 'rgba(59,130,246,0.05)', backgroundColor: 'rgba(59,130,246,0.12)', pointRadius: 0, fill: '+1' },
      { label: `Faixa típica inferior (${unit})`, data: [...seasonalMinData, seasonalAnalysis.available ? seasonalAnalysis.historical_range_min : null], borderColor: 'rgba(59,130,246,0.05)', backgroundColor: 'rgba(59,130,246,0.12)', pointRadius: 0, fill: false },
      { label: `Média histórica da época (${unit})`, data: [...seasonalMeanData, seasonalAnalysis.available ? seasonalAnalysis.historical_mean : null], borderColor: '#2563eb', borderDash: [6, 4], pointRadius: 0, fill: false },
      { label: 'Tendência recente', data: [...trendData, null], borderColor: '#f97316', borderDash: [4, 4], pointRadius: 0, fill: false },
      { label: `Previsão (${unit})`, data: forecastData, borderColor: '#7c3aed', borderDash: [8, 5], backgroundColor: 'rgba(124,58,237,0.10)', pointRadius: 2, fill: false }
    ], values, points: chartPoints, unit, full_count: total, sampled_count: sampled.length, downsampled: total > MAX_POINTS, seasonal_analysis: seasonalAnalysis, forecast, reference_lines: referenceLines };
  await pool.execute('UPDATE chart_generation_jobs SET progress_percent=80 WHERE id=?', [job.id]);

  let cacheId = null;
  let cacheUuid = null;
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    const [cacheResult] = await connection.execute(
      `INSERT INTO chart_cache (uuid, data_point_id, data_point_uuid, chart_type, status, generated_by_node_id, generated_by_node_uuid,
        generated_by_node_name, source_job_uuid, total_points, date_start, date_end, payload, summary, summary_json, seasonal_analysis_json, forecast_json, payload_json, generated_at)
       VALUES (?, ?, ?, ?, 'READY', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE status='READY', generated_by_node_id=VALUES(generated_by_node_id), generated_by_node_uuid=VALUES(generated_by_node_uuid),
         generated_by_node_name=VALUES(generated_by_node_name), source_job_uuid=VALUES(source_job_uuid), total_points=VALUES(total_points), date_start=VALUES(date_start),
         date_end=VALUES(date_end), payload=VALUES(payload), summary=VALUES(summary), summary_json=VALUES(summary_json), seasonal_analysis_json=VALUES(seasonal_analysis_json), forecast_json=VALUES(forecast_json), payload_json=VALUES(payload_json), error_message=NULL, generated_at=NOW()`,
      [crypto.randomUUID(), dataPointId, dataPointUuid, CHART_TYPE, selfNode?.id || null, selfUuid, selfName, job.uuid, total, summary.date_start, summary.date_end, JSON.stringify(payload), JSON.stringify(summary), JSON.stringify(summary), JSON.stringify(seasonalAnalysis), JSON.stringify(forecast), JSON.stringify(payload)]
    );
    if (cacheResult.insertId) {
      cacheId = cacheResult.insertId;
    }
    const [[cacheRow]] = await connection.execute('SELECT id, uuid, source_job_uuid FROM chart_cache WHERE data_point_uuid=? AND chart_type=? LIMIT 1', [dataPointUuid, CHART_TYPE]);
    cacheId = cacheId || cacheRow?.id || null;
    cacheUuid = cacheRow?.uuid || null;
    if (!cacheId || !cacheUuid) throw new Error('chart_cache não foi salvo com uuid válido');
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
  if (!cacheUuid) throw new Error('chart_cache salvo sem uuid');
  console.log(`[chart-worker] chart_cache saved uuid=${cacheUuid} data_point_uuid=${dataPointUuid} points=${total} source_job_uuid=${job.uuid}`);
  return { payload, summary };
};

const markJobFailed = async (job, error) => {
  await pool.execute("UPDATE chart_generation_jobs SET status='FAILED', error_message=?, finished_at=NOW() WHERE id=?", [error.message, job.id]).catch(() => {});
  await pool.execute("UPDATE chart_cache SET status=IF(status='READY','READY','FAILED'), error_message=? WHERE data_point_uuid=? AND chart_type=?", [error.message, job.data_point_uuid, CHART_TYPE]).catch(() => {});
  await createJobSyncEvent(job.id).catch(() => {});
};

module.exports = { getHistoricalChart, regenerateChart, generateChartForJob, markJobFailed, createJobSyncEvent, createCacheSyncEvent, markChartCacheStale, getLatestChartCache, getPointTimeSeries, CHART_TYPE, toNumberOrNull, normalizeStatus, isValidChartPayload, CHART_JOB_TIMEOUT_SECONDS: env.chartJobTimeoutSeconds };
