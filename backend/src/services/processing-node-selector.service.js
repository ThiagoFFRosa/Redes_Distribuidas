const clusterRepo = require('./cluster-node.repository');

const selectBestProcessingNode = async () => {
  const nodes = (await clusterRepo.getAllNodes()).filter((node) => node.status === 'ONLINE');
  if (!nodes.length) return null;
  const self = nodes.find((node) => node.is_self);
  nodes.sort((a, b) => {
    const powerDiff = Number(b.power_score ?? 5) - Number(a.power_score ?? 5);
    if (powerDiff !== 0) return powerDiff;
    const nameDiff = String(a.node_name || '').localeCompare(String(b.node_name || ''));
    if (nameDiff !== 0) return nameDiff;
    return String(a.node_uuid || '').localeCompare(String(b.node_uuid || ''));
  });
  return { bestNode: nodes[0], selfNode: self || null, onlineNodes: nodes };
};

module.exports = { selectBestProcessingNode };
