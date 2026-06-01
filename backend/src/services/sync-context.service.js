let applyingRemoteEvent = false;

const runWithoutSyncEvents = async (fn) => {
  const previous = applyingRemoteEvent;
  applyingRemoteEvent = true;
  try {
    return await fn();
  } finally {
    applyingRemoteEvent = previous;
  }
};

const shouldSkipSyncEvent = () => applyingRemoteEvent;

module.exports = { runWithoutSyncEvents, shouldSkipSyncEvent };
