const fs = require('fs');
const path = require('path');
const os = require('os');
const readline = require('readline');
const pool = require('../database/connection');
const dataPointRepository = require('../repositories/data-point.repository');
const syncService = require('./sync.service');
const chartService = require('./historical-chart.service');

const BATCH_SIZE = 500;

const parseOptionalNumber = (value) => {
  if (value === undefined || value === null || String(value).trim() === '') return null;
  const parsed = Number(String(value).replace(',', '.'));
  return Number.isFinite(parsed) ? parsed : null;
};

const filenameToSensorName = (filename) => {
  const base = path.basename(filename, path.extname(filename)).replace(/_?Cota$/i, '').replace(/[_-]+/g, ' ').trim();
  return base.toLowerCase().replace(/(^|\s)\S/g, (letter) => letter.toUpperCase()) || 'Sensor importado';
};

const parseCsvLine = (line) => {
  const values = [];
  let current = '';
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      if (quoted && line[i + 1] === '"') { current += '"'; i += 1; }
      else quoted = !quoted;
    } else if (char === ',' && !quoted) {
      values.push(current.trim()); current = '';
    } else current += char;
  }
  values.push(current.trim());
  return values;
};

const normalizeDate = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  if (match) return match[1];
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const createUploadFile = async (req) => {
  const contentType = req.headers['content-type'] || '';
  const match = contentType.match(/boundary=(?:(?:"([^"]+)")|([^;]+))/i);
  if (!match) throw new Error('Envie multipart/form-data com arquivo CSV.');
  const boundary = `--${match[1] || match[2]}`;
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const body = Buffer.concat(chunks);
  const binary = body.toString('binary');
  const parts = binary.split(boundary).filter((part) => part && part !== '--\r\n' && part !== '--');
  const fields = {};
  let file = null;
  for (const part of parts) {
    const trimmed = part.replace(/^\r\n/, '').replace(/\r\n--$/, '');
    const sep = trimmed.indexOf('\r\n\r\n');
    if (sep === -1) continue;
    const header = trimmed.slice(0, sep);
    let content = trimmed.slice(sep + 4);
    if (content.endsWith('\r\n')) content = content.slice(0, -2);
    const name = header.match(/name="([^"]+)"/)?.[1];
    const filename = header.match(/filename="([^"]*)"/)?.[1];
    if (!name) continue;
    if (filename !== undefined) {
      const tmpPath = path.join(os.tmpdir(), `historical-${Date.now()}-${Math.random().toString(16).slice(2)}.csv`);
      fs.writeFileSync(tmpPath, Buffer.from(content, 'binary'));
      file = { fieldname: name, originalname: path.basename(filename), path: tmpPath, size: Buffer.byteLength(content, 'binary') };
    } else {
      fields[name] = Buffer.from(content, 'binary').toString('utf8').trim();
    }
  }
  if (!file) throw new Error('Arquivo CSV é obrigatório.');
  return { fields, file };
};

const ensureDataPoint = async (fields, file, userId) => {
  const id = parseOptionalNumber(fields.data_point_id);
  if (id) {
    const existing = await dataPointRepository.findById(id);
    if (!existing) throw new Error('Ponto de dados informado não foi encontrado.');
    return existing;
  }
  const name = String(fields.sensor_name || '').trim() || filenameToSensorName(file.originalname);
  const payload = {
    name,
    type: 'RIVER_LEVEL',
    latitude: parseOptionalNumber(fields.latitude),
    longitude: parseOptionalNumber(fields.longitude),
    city_region: String(fields.city_region || '').trim() || null,
    description: 'Criado automaticamente por importação histórica CSV.',
    status: 'ACTIVE',
    normal_level: null,
    warning_level: parseOptionalNumber(fields.warning_level),
    critical_level: parseOptionalNumber(fields.critical_level),
    measurement_unit: 'm',
    created_by_user_id: userId || null
  };
  return dataPointRepository.create(payload);
};

const flushRows = async (rows) => {
  if (!rows.length) return;
  const placeholders = rows.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
  const params = [];
  rows.forEach((row) => params.push(row.data_point_id, row.import_id, row.measured_at, row.raw_value, row.raw_unit, row.value, row.unit, row.max_value, row.min_value));
  await pool.execute(
    `INSERT INTO historical_measurements (data_point_id, import_id, measured_at, raw_value, raw_unit, value, unit, max_value, min_value)
     VALUES ${placeholders}
     ON DUPLICATE KEY UPDATE import_id=VALUES(import_id), raw_value=VALUES(raw_value), raw_unit=VALUES(raw_unit), value=VALUES(value), unit=VALUES(unit), max_value=VALUES(max_value), min_value=VALUES(min_value), source='CSV_IMPORT'`,
    params
  );
};

const importHistoricalCsv = async (req, userId) => {
  const { fields, file } = await createUploadFile(req);
  if (!file.originalname.toLowerCase().endsWith('.csv')) throw new Error('Apenas arquivos .csv são aceitos nesta etapa.');
  const dataPoint = await ensureDataPoint(fields, file, userId);
  const sensorName = String(fields.sensor_name || '').trim() || dataPoint.name || filenameToSensorName(file.originalname);
  const [importResult] = await pool.execute(
    `INSERT INTO historical_imports (data_point_id, original_filename, sensor_name, status, uploaded_by_user_id)
     VALUES (?, ?, ?, 'IMPORTING', ?)`,
    [dataPoint.id, file.originalname, sensorName, userId || null]
  );
  const importId = importResult.insertId;
  let totalRows = 0, importedRows = 0, failedRows = 0;
  const batch = [];
  try {
    const rl = readline.createInterface({ input: fs.createReadStream(file.path), crlfDelay: Infinity });
    let headers = null;
    let indexes = null;
    for await (const line of rl) {
      if (!line.trim()) continue;
      if (!headers) {
        headers = parseCsvLine(line).map((h) => h.replace(/^\uFEFF/, '').trim());
        indexes = { datetime: headers.indexOf('datetime'), cota: headers.indexOf('Cota'), max: headers.indexOf('max'), min: headers.indexOf('min') };
        if (indexes.datetime === -1 || indexes.cota === -1) throw new Error('CSV inválido: colunas datetime e Cota são obrigatórias.');
        continue;
      }
      totalRows += 1;
      const values = parseCsvLine(line);
      const measuredAt = normalizeDate(values[indexes.datetime]);
      const rawValue = parseOptionalNumber(values[indexes.cota]);
      if (!measuredAt || rawValue === null) { failedRows += 1; continue; }
      batch.push({
        data_point_id: dataPoint.id,
        import_id: importId,
        measured_at: measuredAt,
        raw_value: rawValue,
        raw_unit: 'cm',
        value: Number((rawValue / 100).toFixed(3)),
        unit: 'm',
        max_value: indexes.max >= 0 && parseOptionalNumber(values[indexes.max]) !== null ? Number((parseOptionalNumber(values[indexes.max]) / 100).toFixed(3)) : null,
        min_value: indexes.min >= 0 && parseOptionalNumber(values[indexes.min]) !== null ? Number((parseOptionalNumber(values[indexes.min]) / 100).toFixed(3)) : null
      });
      importedRows += 1;
      if (batch.length >= BATCH_SIZE) { await flushRows(batch.splice(0)); }
    }
    await flushRows(batch.splice(0));
    await pool.execute(
      `UPDATE historical_imports SET status='IMPORTED', total_rows=?, imported_rows=?, failed_rows=?, completed_at=NOW() WHERE id=?`,
      [totalRows, importedRows, failedRows, importId]
    );
    const chartQueued = await chartService.regenerateChart(dataPoint.id, importId);
    const [[historicalImport]] = await pool.execute('SELECT * FROM historical_imports WHERE id=?', [importId]);
    const [[job]] = await pool.execute('SELECT * FROM chart_generation_jobs WHERE id=?', [chartQueued.job.id]);
    const events = [
      { entity_type: 'data_point', entity_id: String(dataPoint.id), payload: dataPoint },
      { entity_type: 'historical_import', entity_id: String(importId), payload: historicalImport },
      { entity_type: 'chart_generation_job', entity_id: String(job.id), payload: job }
    ];
    const [measurements] = await pool.execute('SELECT data_point_id, import_id, measured_at, raw_value, raw_unit, value, unit, max_value, min_value, source FROM historical_measurements WHERE import_id=?', [importId]);
    measurements.forEach((m) => {
      const measuredAt = m.measured_at instanceof Date ? m.measured_at.toISOString().slice(0, 10) : String(m.measured_at).slice(0, 10);
      events.push({ entity_type: 'historical_measurement', entity_id: `${m.data_point_id}:${measuredAt}`, payload: { ...m, measured_at: measuredAt } });
    });
    await syncService.createOutboxEvents(events);
    return { ok: true, import: historicalImport, data_point: dataPoint, job };
  } catch (error) {
    await pool.execute("UPDATE historical_imports SET status='FAILED', total_rows=?, imported_rows=?, failed_rows=?, error_message=?, completed_at=NOW() WHERE id=?", [totalRows, importedRows, failedRows, error.message, importId]);
    throw error;
  } finally {
    fs.unlink(file.path, () => {});
  }
};

const listImports = async () => {
  const [rows] = await pool.execute(
    `SELECT hi.id, hi.original_filename AS filename, hi.sensor_name, hi.status, hi.total_rows, hi.imported_rows, hi.failed_rows, hi.created_at, hi.completed_at,
            dp.id AS data_point_id, dp.name AS data_point_name
       FROM historical_imports hi
       LEFT JOIN data_points dp ON dp.id = hi.data_point_id
      ORDER BY hi.created_at DESC, hi.id DESC`
  );
  return rows.map((row) => ({ ...row, data_point: row.data_point_id ? { id: row.data_point_id, name: row.data_point_name } : null }));
};

const getImport = async (id) => {
  const [rows] = await pool.execute(
    `SELECT hi.*, dp.name AS data_point_name FROM historical_imports hi LEFT JOIN data_points dp ON dp.id=hi.data_point_id WHERE hi.id=? LIMIT 1`,
    [id]
  );
  return rows[0] || null;
};

module.exports = { importHistoricalCsv, listImports, getImport, filenameToSensorName };
