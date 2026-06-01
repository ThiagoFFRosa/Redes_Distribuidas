const MYSQL_DATETIME_RE = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;

function pad(n) {
  return String(n).padStart(2, '0');
}

function toMysqlDateTime(value) {
  if (!value) return null;

  if (typeof value === 'string' && MYSQL_DATETIME_RE.test(value)) {
    return value;
  }

  const date = value instanceof Date ? value : new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return [
    date.getUTCFullYear(),
    pad(date.getUTCMonth() + 1),
    pad(date.getUTCDate())
  ].join('-') + ' ' + [
    pad(date.getUTCHours()),
    pad(date.getUTCMinutes()),
    pad(date.getUTCSeconds())
  ].join(':');
}

function nowMysql() {
  return toMysqlDateTime(new Date());
}

module.exports = {
  toMysqlDateTime,
  nowMysql
};
