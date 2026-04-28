const fs = require('fs');
const path = require('path');
const env = require('../config/env');

const resolveNodesFilePath = () => {
  if (path.isAbsolute(env.clusterNodesFile)) {
    return env.clusterNodesFile;
  }

  return path.resolve(__dirname, '../../', env.clusterNodesFile);
};

const nodesFilePath = resolveNodesFilePath();

const toNode = (rawNode) => ({
  serverName: String(rawNode.serverName || '').trim(),
  serverUrl: String(rawNode.serverUrl || '').trim(),
  addedAt: rawNode.addedAt || new Date().toISOString(),
  lastSeen: rawNode.lastSeen || null
});

const isValidNode = (node) => Boolean(node.serverName && node.serverUrl);

const dedupeNodes = (nodes) => {
  const byName = new Map();
  const byUrl = new Map();

  for (const rawNode of nodes) {
    const node = toNode(rawNode);
    if (!isValidNode(node)) continue;

    if (byName.has(node.serverName) || byUrl.has(node.serverUrl)) {
      continue;
    }

    byName.set(node.serverName, node);
    byUrl.set(node.serverUrl, node);
  }

  return Array.from(byName.values());
};

const ensureSelfNode = (nodes) => {
  const filtered = nodes.filter((node) => node.serverName !== env.serverName && node.serverUrl !== env.serverUrl);
  return dedupeNodes([
    {
      serverName: env.serverName,
      serverUrl: env.serverUrl,
      addedAt: new Date().toISOString(),
      lastSeen: null
    },
    ...filtered
  ]);
};

class ClusterNodesService {
  loadOrCreateNodes() {
    if (!fs.existsSync(nodesFilePath)) {
      const bootstrapped = ensureSelfNode([]);
      this.saveNodes(bootstrapped);
      return bootstrapped;
    }

    try {
      const raw = fs.readFileSync(nodesFilePath, 'utf-8');
      const parsed = JSON.parse(raw);
      const nodes = Array.isArray(parsed?.nodes) ? parsed.nodes : [];
      const merged = ensureSelfNode(dedupeNodes(nodes));
      this.saveNodes(merged);
      return merged;
    } catch (error) {
      console.error('[cluster-nodes] falha ao carregar arquivo, recriando:', error.message);
      const bootstrapped = ensureSelfNode([]);
      this.saveNodes(bootstrapped);
      return bootstrapped;
    }
  }

  saveNodes(nodes) {
    const merged = ensureSelfNode(dedupeNodes(nodes));
    const payload = { nodes: merged };
    fs.writeFileSync(nodesFilePath, JSON.stringify(payload, null, 2));
    return merged;
  }

  mergeNodes(existingNodes, incomingNodes) {
    return ensureSelfNode(dedupeNodes([...(existingNodes || []), ...(incomingNodes || [])]));
  }

  getNodesFilePath() {
    return nodesFilePath;
  }
}

module.exports = new ClusterNodesService();
