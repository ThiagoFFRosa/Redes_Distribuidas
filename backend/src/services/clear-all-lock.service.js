const fs = require('fs/promises');
const path = require('path');

const storageDir = path.resolve(__dirname, '../../.storage');
const lockPath = path.join(storageDir, 'clear-all.lock');

const ensureStorageDir = async () => {
  await fs.mkdir(storageDir, { recursive: true });
};

const createLock = async (metadata = {}) => {
  await ensureStorageDir();
  const payload = {
    created_at: new Date().toISOString(),
    reason: 'clear-all',
    ...metadata
  };
  await fs.writeFile(lockPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  return payload;
};

const getLock = async () => {
  try {
    const raw = await fs.readFile(lockPath, 'utf8');
    try { return { exists: true, path: lockPath, data: JSON.parse(raw) }; }
    catch (_error) { return { exists: true, path: lockPath, data: { raw } }; }
  } catch (error) {
    if (error.code === 'ENOENT') return { exists: false, path: lockPath, data: null };
    throw error;
  }
};

const removeLock = async () => {
  await fs.rm(lockPath, { force: true });
};

module.exports = { storageDir, lockPath, createLock, getLock, removeLock };
