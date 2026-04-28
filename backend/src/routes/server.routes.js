const express = require('express');
const env = require('../config/env');
const clusterService = require('../services/cluster.service');

const router = express.Router();

const withTimeout = async (url, options = {}, timeoutMs = env.heartbeatIntervalMs) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {})
      },
      signal: controller.signal
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.message || `HTTP ${response.status}`);
    }

    return data;
  } finally {
    clearTimeout(timer);
  }
};

const formatServersResponse = async () => {
  const local = clusterService.getLocalState();
  const servers = await clusterService.getKnownServersWithStatus();

  return {
    currentServer: {
      serverName: local.serverName,
      serverUrl: local.serverUrl,
      role: local.role,
      publicUrl: local.publicUrl
    },
    servers
  };
};

const callHandshake = async (serverUrl) => {
  return withTimeout(`${serverUrl}/internal/handshake`, {
    headers: {
      'x-cluster-key': env.clusterKey
    }
  });
};

router.get('/', async (req, res) => {
  res.json(await formatServersResponse());
});

router.post('/test-connection', async (req, res) => {
  const { serverUrl } = req.body || {};

  if (!serverUrl) {
    return res.status(400).json({ message: 'serverUrl é obrigatório.' });
  }

  try {
    const handshake = await callHandshake(serverUrl);
    return res.json({
      ok: true,
      serverName: handshake.serverName,
      serverUrl: handshake.serverUrl,
      role: handshake.role,
      publicUrl: handshake.publicUrl,
      clusterKeyAccepted: handshake.clusterKeyAccepted
    });
  } catch (_error) {
    return res.status(400).json({
      ok: false,
      message: 'Servidor não respondeu ou chave do cluster inválida'
    });
  }
});

router.post('/register', async (req, res) => {
  const { serverUrl } = req.body || {};

  if (!serverUrl) {
    return res.status(400).json({ message: 'serverUrl é obrigatório.' });
  }

  try {
    const handshake = await callHandshake(serverUrl);
    const newNode = clusterService.upsertNode({
      serverName: handshake.serverName,
      serverUrl: handshake.serverUrl,
      addedAt: new Date().toISOString(),
      lastSeen: null
    });

    const allNodes = clusterService.getNodes();

    await withTimeout(`${handshake.serverUrl}/internal/nodes/replace`, {
      method: 'POST',
      headers: { 'x-cluster-key': env.clusterKey },
      body: JSON.stringify({ nodes: allNodes })
    });

    const knownServers = await clusterService.getKnownServersWithStatus();
    await Promise.all(
      knownServers
        .filter((peer) => peer.online && peer.serverUrl !== handshake.serverUrl && peer.serverUrl !== env.serverUrl)
        .map(async (peer) => {
          try {
            await withTimeout(`${peer.serverUrl}/internal/nodes/add`, {
              method: 'POST',
              headers: { 'x-cluster-key': env.clusterKey },
              body: JSON.stringify({
                serverName: handshake.serverName,
                serverUrl: handshake.serverUrl
              })
            });
          } catch (error) {
            console.error(`[cluster] falha ao avisar ${peer.serverUrl}:`, error.message);
          }
        })
    );

    await clusterService.refreshPeers();

    return res.json({
      ok: true,
      message: 'Servidor cadastrado com sucesso',
      newNode,
      nodes: clusterService.getNodes()
    });
  } catch (_error) {
    return res.status(400).json({
      ok: false,
      message: 'Servidor não respondeu ou chave do cluster inválida'
    });
  }
});

router.post('/switch-host', async (req, res) => {
  const { targetUrl } = req.body;
  if (!targetUrl) {
    return res.status(400).json({ message: 'targetUrl é obrigatório.' });
  }

  try {
    const result = await clusterService.switchHost(targetUrl);
    const response = await formatServersResponse();
    return res.json({
      ...response,
      switchHost: result
    });
  } catch (error) {
    return res.status(400).json({ message: error.message });
  }
});

router.post('/become-host', async (req, res) => {
  const local = clusterService.getLocalState();
  await clusterService.switchHost(local.serverUrl);
  res.json(await formatServersResponse());
});

router.post('/become-standby', async (req, res) => {
  await clusterService.makeLocalStandby();
  await clusterService.electHostIfNeeded();
  res.json(await formatServersResponse());
});

module.exports = router;
