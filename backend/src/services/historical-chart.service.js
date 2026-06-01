const pool = require('../database/connection');
const dataPointRepository = require('../repositories/data-point.repository');

const CHART_TYPE = 'HISTORICAL_RIVER_LEVEL';
const MAX_POINTS = 1000;

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

const getLatestJob = async (dataPointId) => {
  const [rows] = await pool.execute(
    `SELECT * FROM chart_generation_jobs
      WHERE data_point_id = ? AND status IN ('PENDING', 'RUNNING')
      ORDER BY FIELD(status, 'RUNNING', 'PENDING'), created_at DESC
      LIMIT 1`,
    [dataPointId]
  );
  return rows[0] || null;
};

const normalizeChart = (row) => row && ({
  status: row.status,
  payload: parseJson(row.payload),
  summary: parseJson(row.summary),
  generated_at: row.generated_at,
  total_points: Number(row.total_points || 0),
  date_start: row.date_start,
  date_end: row.date_end,
  generated_by_node_name: row.generated_by_node_name,
  error_message: row.error_message
});

const getHistoricalChart = async (dataPointId) => {
  const dataPoint = await dataPointRepository.findById(dataPointId);
  if (!dataPoint) return null;
  const [cacheRows] = await pool.execute(
    `SELECT * FROM chart_cache WHERE data_point_id = ? AND chart_type = ? LIMIT 1`,
    [dataPointId, CHART_TYPE]
  );
  const job = await getLatestJob(dataPointId);
  const chart = normalizeChart(cacheRows[0]);
  let message = null;
  if (chart?.status === 'READY' && job) message = 'Um novo gráfico está sendo gerado. Exibindo a última versão disponível.';
  if (!chart && job?.status === 'RUNNING') message = 'Gráfico sendo gerado.';
  if (!chart && job?.status === 'PENDING') message = 'Aguardando processamento.';
  if (!chart && !job) message = 'Nenhum gráfico histórico disponível para este ponto.';
  return {
    ok: true,
    data_point: dataPoint,
    chart: chart?.status === 'READY' ? chart : (chart || null),
    job: job ? {
      id: job.id,
      status: job.status,
      progress_percent: Number(job.progress_percent || 0),
      estimated_seconds: job.estimated_seconds,
      assigned_node_name: job.assigned_node_name
    } : null,
    message
  };
};

const regenerateChart = async (dataPointId, importId = null) => {
  const dataPoint = await dataPointRepository.findById(dataPointId);
  if (!dataPoint) return null;
  const [[countRow]] = await pool.execute('SELECT COUNT(*) AS total FROM historical_measurements WHERE data_point_id=?', [dataPointId]);
  const estimatedSeconds = Math.max(5, Math.ceil(Number(countRow.total || 0) / 5000));
  const [result] = await pool.execute(
    `INSERT INTO chart_generation_jobs (data_point_id, import_id, status, progress_percent, estimated_seconds)
     VALUES (?, ?, 'PENDING', 0, ?)`,
    [dataPointId, importId, estimatedSeconds]
  );
  await pool.execute(
    `INSERT INTO chart_cache (data_point_id, chart_type, status)
     VALUES (?, ?, 'GENERATING')
     ON DUPLICATE KEY UPDATE status = IF(status='READY', 'STALE', 'GENERATING'), error_message=NULL`,
    [dataPointId, CHART_TYPE]
  );
  const [[job]] = await pool.execute('SELECT * FROM chart_generation_jobs WHERE id=?', [result.insertId]);
  return { ok: true, data_point: dataPoint, job, message: 'Geração de gráfico enfileirada.' };
};

const generateChartForJob = async (job, selfNode) => {
  const dataPointId = job.data_point_id;
  await pool.execute(
    `UPDATE chart_generation_jobs SET status='RUNNING', assigned_node_id=?, assigned_node_name=?, progress_percent=10, started_at=NOW(), error_message=NULL WHERE id=?`,
    [selfNode?.id || null, selfNode?.node_name || 'local', job.id]
  );
  await pool.execute(
    `INSERT INTO chart_cache (data_point_id, chart_type, status, generated_by_node_id, generated_by_node_name)
     VALUES (?, ?, 'GENERATING', ?, ?)
     ON DUPLICATE KEY UPDATE status=IF(status='READY','STALE','GENERATING'), generated_by_node_id=VALUES(generated_by_node_id), generated_by_node_name=VALUES(generated_by_node_name), error_message=NULL`,
    [dataPointId, CHART_TYPE, selfNode?.id || null, selfNode?.node_name || 'local']
  );

  const [rows] = await pool.execute(
    `SELECT measured_at, value FROM historical_measurements WHERE data_point_id=? ORDER BY measured_at ASC`,
    [dataPointId]
  );
  await pool.execute('UPDATE chart_generation_jobs SET progress_percent=45 WHERE id=?', [job.id]);
  const total = rows.length;
  const step = Math.max(1, Math.ceil(total / MAX_POINTS));
  const sampled = rows.filter((_row, index) => index % step === 0 || index === total - 1);
  const labels = sampled.map((row) => dateOnly(row.measured_at));
  const values = sampled.map((row) => Number(row.value));
  const numericValues = rows.map((row) => Number(row.value)).filter(Number.isFinite);
  const latest = rows[rows.length - 1] || null;
  const summary = {
    total_measurements: total,
    date_start: rows[0] ? dateOnly(rows[0].measured_at) : null,
    date_end: latest ? dateOnly(latest.measured_at) : null,
    latest_value: latest ? Number(latest.value) : null,
    min_value: numericValues.length ? Math.min(...numericValues) : null,
    max_value: numericValues.length ? Math.max(...numericValues) : null,
    average_value: numericValues.length ? Number((numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length).toFixed(3)) : null,
    trend: calculateTrend(rows)
  };
  const payload = { labels, values, unit: 'm', downsampled: step > 1, full_count: total, step };
  await pool.execute('UPDATE chart_generation_jobs SET progress_percent=80 WHERE id=?', [job.id]);
  await pool.execute(
    `INSERT INTO chart_cache (data_point_id, chart_type, status, generated_by_node_id, generated_by_node_name, total_points, date_start, date_end, payload, summary, generated_at)
     VALUES (?, ?, 'READY', ?, ?, ?, ?, ?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE status='READY', generated_by_node_id=VALUES(generated_by_node_id), generated_by_node_name=VALUES(generated_by_node_name), total_points=VALUES(total_points), date_start=VALUES(date_start), date_end=VALUES(date_end), payload=VALUES(payload), summary=VALUES(summary), error_message=NULL, generated_at=NOW()`,
    [dataPointId, CHART_TYPE, selfNode?.id || null, selfNode?.node_name || 'local', total, summary.date_start, summary.date_end, JSON.stringify(payload), JSON.stringify(summary)]
  );
  await pool.execute("UPDATE chart_generation_jobs SET status='DONE', progress_percent=100, finished_at=NOW() WHERE id=?", [job.id]);
  return { payload, summary };
};

module.exports = { getHistoricalChart, regenerateChart, generateChartForJob, CHART_TYPE, toNumberOrNull };
