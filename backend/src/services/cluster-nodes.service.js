const fs = require('fs');
const path = require('path');
const env = require('../config/env');

const INVALID_CLUSTER_URL_MESSAGE =
  'URL inválida para cluster. Use o IP Tailscale, exemplo: http://100.x.x.x:3000';

const resolveNodesFilePath = () => {
  if (path.isAbsolute(env.clusterNodesFile)) {
    return env.clusterNodesFile;
  }

  return path.resolve(__dirname, '../../', env.clusterNodesFile);
};

const nodesFilePath = resolveNodesFilePath();

const toNode = (rawNode) => ({
  serverName: String(rawNode?.serverName || '').trim(),
  serverUrl: String(rawNode?.serverUrl || '').trim(),
  addedAt: rawNode?.addedAt || new Date().toISOString(),
  lastSeen: rawNode?.lastSeen || null
});

const isValidClusterUrl = (url) => {
  const value = String(url || '').trim();
  if (!value) return false;

  if (!(value.startsWith('http://') || value.startsWith('https://'))) {
    return false;
  }

  try {
    const parsed = new URL(value);
    const host = parsed.hostname.toLowerCase();
    if (host === 'localhost' || host === '127.0.0.1') {
      return false;
    }

    return true;
  } catch (_error) {
    return false;
  }
};

const isValidNode = (node) => Boolean(node.serverName && isValidClusterUrl(node.serverUrl));

const registerSelf = (nodes) => {
  if (!isValidClusterUrl(env.serverUrl)) {
    console.error('[cluster-nodes] SERVER_URL inválido para cluster. Use IP Tailscale.');
    return nodes.filter((node) => node.serverName !== env.serverName && node.serverUrl !== env.serverUrl);
  }

  const withoutSelf = nodes.filter((node) => node.serverName !== env.serverName && node.serverUrl !== env.serverUrl);

  return [
    {
      serverName: env.serverName,
      serverUrl: env.serverUrl,
      addedAt: new Date().toISOString(),
      lastSeen: null
    },
    ...withoutSelf
  ];
};

const dedupeNodes = (nodes) => {
  const byName = new Map();
  const byUrl = new Map();

  for (const rawNode of nodes) {
    const node = toNode(rawNode);
    if (!isValidNode(node)) {
      continue;
    }

    if (byName.has(node.serverName) || byUrl.has(node.serverUrl)) {
      continue;
    }

    byName.set(node.serverName, node);
    byUrl.set(node.serverUrl, node);
  }

  return Array.from(byName.values());
};

const sanitizeNodes = (nodes) => {
  const clean = dedupeNodes(nodes);
  return dedupeNodes(registerSelf(clean));
};

class ClusterNodesService {
  loadNodes() {
    let nodes = [];

    if (fs.existsSync(nodesFilePath)) {
      try {
        const raw = fs.readFileSync(nodesFilePath, 'utf-8');
        const parsed = JSON.parse(raw);
        nodes = Array.isArray(parsed?.nodes) ? parsed.nodes : [];
      } catch (error) {
        console.error('[cluster-nodes] falha ao carregar arquivo, recriando:', error.message);
      }
    }

    const sanitized = sanitizeNodes(nodes);
    this.saveNodes(sanitized);
    return sanitized;
  }

  saveNodes(nodes) {
    const sanitized = sanitizeNodes(nodes || []);
    const payload = { nodes: sanitized };
    fs.writeFileSync(nodesFilePath, JSON.stringify(payload, null, 2));
    return sanitized;
  }

  mergeNodes(existingNodes, incomingNodes) {
    return sanitizeNodes([...(existingNodes || []), ...(incomingNodes || [])]);
  }

  cleanupNodes(nodes) {
    return sanitizeNodes(nodes || []);
  }

  isValidClusterUrl(url) {
    return isValidClusterUrl(url);
  }

  getInvalidClusterUrlMessage() {
    return INVALID_CLUSTER_URL_MESSAGE;
  }

  getNodesFilePath() {
    return nodesFilePath;
  }
}

module.exports = new ClusterNodesService();
