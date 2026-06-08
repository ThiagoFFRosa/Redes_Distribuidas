const env = require('../config/env');

const normalizeUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const withoutTrailingSlash = raw.replace(/\/+$/, '');
  if (/^https?:\/\//i.test(withoutTrailingSlash)) return withoutTrailingSlash;
  if (/^[\w.-]+\.(ngrok\.dev|ngrok-free\.app|dev)(:\d+)?(\/.*)?$/i.test(withoutTrailingSlash)) return `https://${withoutTrailingSlash}`;
  if (/^(localhost|\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-f:]+\])(:\d+)?(\/.*)?$/i.test(withoutTrailingSlash)) return `http://${withoutTrailingSlash}`;
  return `https://${withoutTrailingSlash}`;
};

const stripApiPath = (value) => normalizeUrl(String(value || '').replace(/\/api\/sync\/apply\/?$/i, ''));

const getTailscaleBaseUrl = (node, defaultPort = 3000) => {
  if (!node?.tailscale_ip) return null;
  return `http://${node.tailscale_ip}:${node.port || defaultPort}`;
};

const getNodeBaseUrl = (node) => {
  const tailscaleUrl = getTailscaleBaseUrl(node, node?.port || 3000);
  if (tailscaleUrl) return tailscaleUrl;
  return normalizeUrl(node?.public_url);
};

const getSelfBaseUrls = (self) => {
  const urls = new Set();
  [process.env.NGROK_DOMAIN, env.ngrokDomain, self?.public_url].forEach((url) => {
    const normalized = normalizeUrl(url);
    if (normalized) urls.add(normalized);
  });
  const tailscaleUrl = getTailscaleBaseUrl(self, self?.port || env.port || 3000);
  if (tailscaleUrl) urls.add(tailscaleUrl);
  return [...urls];
};

const getSyncTargetUrl = (node) => {
  const baseUrl = getNodeBaseUrl(node);
  return baseUrl ? `${baseUrl}/api/sync/apply` : null;
};

const getNodeSyncTarget = (node) => {
  const baseUrl = getNodeBaseUrl(node);
  return baseUrl ? `${baseUrl}/api/sync/apply` : null;
};

const isSelfNode = (node, self = null) => Boolean(self && node && (
  (node.node_uuid && self.node_uuid && node.node_uuid === self.node_uuid) ||
  (node.tailscale_ip && self.tailscale_ip && node.tailscale_ip === self.tailscale_ip)
));

const resolveNodeBaseUrl = (node, self = null) => {
  let baseUrl = getNodeBaseUrl(node);
  const matchedSelfUrl = isSelfNode(node, self);
  if (matchedSelfUrl) {
    const fallback = getTailscaleBaseUrl(node, node?.port || 3000);
    if (fallback && stripApiPath(fallback) !== stripApiPath(baseUrl)) baseUrl = fallback;
  }
  return { baseUrl, targetUrl: baseUrl ? `${baseUrl}/api/sync/apply` : null, matchedSelfUrl, isSelf: matchedSelfUrl };
};

module.exports = { normalizeUrl, getNodeBaseUrl, getSyncTargetUrl, getSelfBaseUrls, getNodeSyncTarget, resolveNodeBaseUrl, getTailscaleBaseUrl, isSelfNode };
