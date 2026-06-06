const path = require('path');
const net = require('net');
const express = require('express');
const session = require('express-session');

const env = require('./config/env');
const { requireAuth } = require('./services/auth.service');
const heartbeatService = require('./services/heartbeat.service');
const clusterStartupService = require('./services/cluster-startup.service');
const repo = require('./services/cluster-node.repository');

const authRoutes = require('./routes/auth.routes');
const serverRoutes = require('./routes/server.routes');
const clusterRoutes = require('./routes/cluster.routes');
const ngrokRoutes = require('./routes/ngrok.routes');
const inmetRoutes = require('./routes/inmet.routes');
const clusterDbRoutes = require('./routes/cluster-db.routes');
const dataPointRoutes = require('./routes/data-point.routes');
const measurementRoutes = require('./routes/measurement.routes');
const alertRoutes = require('./routes/alert.routes');
const eventQueueRoutes = require('./routes/event-queue.routes');
const dashboardRoutes = require('./routes/dashboard.routes');
const publicMonitoringRoutes = require('./routes/public-monitoring.routes');
const importRoutes = require('./routes/import.routes');
const syncRoutes = require('./routes/sync.routes');
const processingRoutes = require('./routes/processing.routes');
const chartWorker = require('./services/chart-worker.service');
const syncWorker = require('./services/sync-worker');

const app = express();
const publicPath = path.resolve(__dirname, '../../public');
app.use(express.json({ limit: '5mb' }));
app.use(express.urlencoded({ extended: true, limit: '5mb' }));
app.use((err, req, res, next) => {
  if (err && err.type === 'entity.too.large') {
    console.error('[server] payload muito grande:', {
      path: req.path,
      method: req.method,
      expected: err.expected,
      length: err.length,
      limit: err.limit
    });

    return res.status(413).json({
      ok: false,
      error: 'Payload muito grande. Envie os dados em lotes menores.'
    });
  }

  return next(err);
});
app.use(session({ secret: env.sessionSecret, resave: false, saveUninitialized: false, cookie: { httpOnly: true, sameSite: 'lax' } }));

app.get('/', (req, res) => res.sendFile(path.join(publicPath, 'index.html')));
app.get('/login', (req, res) => res.sendFile(path.join(publicPath, 'login.html')));
app.get('/dashboard', (req, res) => res.sendFile(path.join(publicPath, 'dashboard.html')));
app.get('/login.html', (req, res) => res.redirect('/login'));
app.get('/admin.html', (req, res) => res.redirect('/dashboard'));
app.use('/assets', express.static(path.join(publicPath, 'assets')));


app.get('/api/health', (_req, res) => {
  res.json({ ok: true, service: 'custom-newtab-api' });
});

app.get('/health', async (_req, res) => {
  const selfNode = await repo.getSelfNode();
  if (!selfNode) return res.json({ ok: true, status: 'ONLINE', configured: false, message: 'Servidor ainda não configurado no banco.' });
  return res.json({ ok: true, status: 'ONLINE', node: { name: selfNode.node_name, tailscale_ip: selfNode.tailscale_ip, role: selfNode.role }, timestamp: new Date().toISOString() });
});

app.use('/api/auth', authRoutes);
app.use('/internal', clusterRoutes);
app.use('/api/servers', serverRoutes);
app.use('/api/ngrok', requireAuth, ngrokRoutes);
app.use('/api/inmet', requireAuth, inmetRoutes);
app.use('/api/cluster', clusterDbRoutes);
app.use('/api/data-points', dataPointRoutes);
app.use('/api/measurements', measurementRoutes);
app.use('/api/alerts', alertRoutes);
app.use('/api/event-queue', eventQueueRoutes);
app.use('/api/dashboard', dashboardRoutes);
app.use('/api/public', publicMonitoringRoutes);
app.use('/api/imports', importRoutes);
app.use('/api/sync', syncRoutes);
app.use('/api/processing', processingRoutes);
app.use((error, req, res, next) => {
  console.error('[server] erro não tratado:', error);
  if (res.headersSent) return next(error);
  return res.status(500).json({ ok: false, error: 'Erro interno do servidor.' });
});

const listen = (port, host, callback) => {
  if (host) return app.listen(port, host, callback);
  return app.listen(port, callback);
};

const checkPortAvailable = (port, host = env.host) => new Promise((resolve) => {
  const tester = net.createServer()
    .once('error', (error) => {
      resolve({ available: false, error });
    })
    .once('listening', () => {
      tester.close(() => resolve({ available: true }));
    });

  if (host) tester.listen(port, host);
  else tester.listen(port);
});

const isAutoPortFallbackEnabled = () => {
  if (!env.autoPortFallback) return false;
  if (String(env.nodeEnv).toLowerCase() === 'production') {
    console.warn('[server] AUTO_PORT_FALLBACK=true ignorado em produção. Configure outra porta explicitamente.');
    return false;
  }
  return true;
};

const resolveStartupPort = async (preferredPort) => {
  const initialCheck = await checkPortAvailable(preferredPort);
  if (initialCheck.available) return { port: preferredPort, blocked: false };

  if (initialCheck.error?.code !== 'EADDRINUSE') return { port: preferredPort, blocked: false };

  if (!isAutoPortFallbackEnabled()) return { port: preferredPort, blocked: true };

  for (let candidatePort = preferredPort + 1; candidatePort <= preferredPort + 10; candidatePort += 1) {
    console.warn(`[server] porta ${candidatePort - 1} ocupada. Tentando ${candidatePort}...`);
    const candidateCheck = await checkPortAvailable(candidatePort);
    if (candidateCheck.available) return { port: candidatePort, blocked: false };
    if (candidateCheck.error?.code !== 'EADDRINUSE') return { port: candidatePort, blocked: false };
  }

  console.error(`[server] não foi possível encontrar porta livre entre ${preferredPort} e ${preferredPort + 10}.`);
  return { port: preferredPort, blocked: true };
};

const buildLocalPublicUrl = (selfNode, port) => {
  const hostIp = selfNode?.tailscale_ip || '127.0.0.1';
  return `http://${hostIp}:${port}`;
};

const updateSelfNodePortIfNeeded = async (port, preferredPort) => {
  if (port === preferredPort) return;

  const selfNode = await repo.getSelfNode();
  if (!selfNode) return;

  const publicUrl = buildLocalPublicUrl(selfNode, port);
  if (Number(selfNode.port) === Number(port) && selfNode.public_url === publicUrl) return;

  await repo.updateNodeStructuralData(selfNode.id, {
    port,
    public_url: publicUrl
  }, { reason: 'auto-port-fallback' });
  console.log(`[server] cluster_nodes atualizado para porta ${port}: ${publicUrl}`);
};

const printPortInUseHelp = (port) => {
  console.error(`[server] porta ${port} já está ocupada.`);
  console.error('[server] isso não é erro do ngrok; é conflito local de porta.');
  console.error('[server] provavelmente já existe outro backend rodando.');
  console.error('[server] encerre o processo antigo ou configure outra porta no .env.');
  console.error(`[server] Linux: sudo lsof -i :${port}`);
  console.error(`[server] Linux: sudo ss -ltnp | grep :${port}`);
  console.error('[server] Para matar no Linux: kill -9 PID');
  console.error('[server] Se estiver usando PM2: pm2 list; pm2 stop all; pm2 delete all');
  console.error(`[server] Windows: netstat -ano | findstr :${port}`);
  console.error('[server] Windows: taskkill /PID <PID> /F');
};

const start = async () => {
  const preferredPort = env.port;
  const startupPort = await resolveStartupPort(preferredPort);
  const { port } = startupPort;

  if (startupPort.blocked) {
    printPortInUseHelp(port);
    process.exit(1);
  }

  if (port !== preferredPort) env.port = port;

  await clusterStartupService.initialize({ port });
  heartbeatService.start();
  chartWorker.start();
  syncWorker.start();

  const server = listen(port, env.host, () => {
    (async () => {
      const selfNode = await repo.getSelfNode();
      const display = selfNode?.node_name || 'server';
      const hostIp = selfNode?.tailscale_ip || '127.0.0.1';
      const localHost = env.host || 'localhost';

      await updateSelfNodePortIfNeeded(port, preferredPort);
      console.log(`Custom NewTab API running on http://${localHost}:${port}`);
      console.log(`[${display}] rodando em http://${hostIp}:${port} (porta ${port})`);
    })().catch((error) => {
      console.error('[server] erro após iniciar servidor:', error);
      process.exit(1);
    });
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      printPortInUseHelp(port);
      process.exit(1);
    }

    console.error('[server] erro ao iniciar servidor:', err);
    process.exit(1);
  });
};

start().catch((error) => {
  console.error('[server] erro durante inicialização:', error);
  process.exit(1);
});
