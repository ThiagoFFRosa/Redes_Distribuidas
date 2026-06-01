const clusterRepo = require('./cluster-node.repository');

const selectBestProcessingNode = async () => {
  const nodes = (await clusterRepo.getAllNodes()).filter((node) => node.status === 'ONLINE');
  if (!nodes.length) return null;
  const self = nodes.find((node) => node.is_self);
  nodes.sort((a, b) => {
    const powerDiff = Number(b.power_score ?? 5) - Number(a.power_score ?? 5);
    if (powerDiff !== 0) return powerDiff;
    if (a.role === 'HOST' && b.role !== 'HOST') return -1;
    if (b.role === 'HOST' && a.role !== 'HOST') return 1;
    if (self && a.id === self.id && b.id !== self.id) return -1;
    if (self && b.id === self.id && a.id !== self.id) return 1;
    return String(a.node_name || '').localeCompare(String(b.node_name || ''));
  });
  return { bestNode: nodes[0], selfNode: self || null, onlineNodes: nodes };
};

module.exports = { selectBestProcessingNode };
