const pool = require('../database/connection');

const SOURCE_PRIORITY = { CSV: 1, SITE: 2 };

const toDate = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const toIsoDate = (value) => {
  const date = toDate(value);
  return date ? date.toISOString() : null;
};

const normalizeUnit = (unit) => String(unit || '').trim().toLowerCase();

const convertValue = (value, fromUnit, toUnit) => {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return null;
  const sourceUnit = normalizeUnit(fromUnit || toUnit || 'm');
  const targetUnit = normalizeUnit(toUnit || fromUnit || 'm');
  if (!sourceUnit || !targetUnit || sourceUnit === targetUnit) return numericValue;
  if ((sourceUnit === 'cm' || sourceUnit === 'centimetro' || sourceUnit === 'centimetros' || sourceUnit === 'centímetros') && (targetUnit === 'm' || targetUnit === 'metro' || targetUnit === 'metros')) {
    return Number((numericValue / 100).toFixed(3));
  }
  if ((sourceUnit === 'm' || sourceUnit === 'metro' || sourceUnit === 'metros') && (targetUnit === 'cm' || targetUnit === 'centimetro' || targetUnit === 'centimetros' || targetUnit === 'centímetros')) {
    return Number((numericValue * 100).toFixed(3));
  }
  return numericValue;
};

const sortByDateAsc = (a, b) => toDate(a.date).getTime() - toDate(b.date).getTime();

const dedupeByExactTimestamp = (points) => {
  const byTimestamp = new Map();
  for (const point of points) {
    if (!point.date || point.value === null || point.value === undefined) continue;
    const existing = byTimestamp.get(point.date);
    if (!existing || SOURCE_PRIORITY[point.source] > SOURCE_PRIORITY[existing.source]) {
      byTimestamp.set(point.date, point);
    }
  }
  return Array.from(byTimestamp.values()).sort(sortByDateAsc);
};

const normalizeRows = (rows, targetUnit) => rows.map((row) => ({
  date: toIsoDate(row.date),
  value: convertValue(row.value, row.unit, targetUnit),
  unit: targetUnit || row.unit || 'm',
  source: row.source
}));

const getPointTimeSeries = async (dataPointUuid, connection = pool) => {
  if (!dataPointUuid) return [];
  const [[dataPoint]] = await connection.execute('SELECT uuid, measurement_unit FROM data_points WHERE uuid = ? LIMIT 1', [dataPointUuid]);
  if (!dataPoint) return [];
  const targetUnit = dataPoint.measurement_unit || 'm';

  const [historicalRows] = await connection.execute(
    `SELECT hm.measured_at AS date, hm.value, hm.unit, 'CSV' AS source
       FROM historical_measurements hm
       JOIN data_points dp ON dp.id = hm.data_point_id
      WHERE dp.uuid = ?`,
    [dataPointUuid]
  );

  const [siteRows] = await connection.execute(
    `SELECT COALESCE(m.measured_at, m.created_at) AS date, m.value, m.unit, 'SITE' AS source
       FROM measurements m
       JOIN data_points dp ON dp.id = m.data_point_id
      WHERE dp.uuid = ?`,
    [dataPointUuid]
  );

  return dedupeByExactTimestamp([
    ...normalizeRows(historicalRows, targetUnit),
    ...normalizeRows(siteRows, targetUnit)
  ]);
};

module.exports = { getPointTimeSeries, convertValue, dedupeByExactTimestamp };
