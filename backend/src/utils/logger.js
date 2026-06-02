const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 50 };

const normalizeLevel = (value) => {
  const level = String(value || 'info').trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(LEVELS, level) ? level : 'info';
};

const currentLevel = () => normalizeLevel(process.env.LOG_LEVEL);
const shouldLog = (level) => LEVELS[level] >= LEVELS[currentLevel()] && currentLevel() !== 'silent';

const logger = {
  debug: (...args) => { if (shouldLog('debug')) console.debug(...args); },
  info: (...args) => { if (shouldLog('info')) console.log(...args); },
  warn: (...args) => { if (shouldLog('warn')) console.warn(...args); },
  error: (...args) => { if (shouldLog('error')) console.error(...args); }
};

module.exports = logger;
