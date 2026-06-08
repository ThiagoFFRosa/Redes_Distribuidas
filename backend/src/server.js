const path = require('path');
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
const adminRoutes = require('./routes/admin.routes');
const processingRoutes = require('./routes/processing.routes');
const chartWorker = require('./services/chart-worker.service');
const syncWorker = require('./services/sync-worker');
const ngrokCoordinator = require('./services/ngrok-coordinator.service');
const { getSuggestedAccessUrls } = require('./utils/network-addresses');

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
app.use('/api/admin', adminRoutes);
app.use('/api/processing', processingRoutes);
app.use((error, req, res, next) => {
  console.error('[server] erro não tratado:', error);
  if (res.headersSent) return next(error);
  return res.status(error.status || 500).json({ ok: false, message: error.message || 'Erro interno do servidor.', error: error.status ? undefined : 'Erro interno do servidor.' });
});

const logAccessUrls = (port, configured) => {
  const urls = getSuggestedAccessUrls(port);
  if (!configured) console.log('[server] painel de configuração disponível em:');
  console.log(`[server] acesso local: ${urls.localUrl}`);
  if (urls.tailscaleUrl) console.log(`[server] acesso Tailscale: ${urls.tailscaleUrl}`);
  else if (urls.lanUrls.length) console.log(`[server] IPs disponíveis: ${urls.lanUrls.join(', ')}`);
};

const handleListenError = (error) => {
  if (error.code !== 'EADDRINUSE') throw error;
  console.error(`[server] erro: porta ${env.port} já está em uso.`);
  console.error('[server] encerre o processo que está usando a porta ou configure outra PORT no .env.');
  console.error(`[server] Linux: sudo lsof -i :${env.port}`);
  console.error(`[server] Linux: sudo ss -ltnp | grep :${env.port}`);
  console.error(`[server] Windows: netstat -ano | findstr :${env.port}`);
  process.exitCode = 1;
};

const start = async () => {
  const startup = await clusterStartupService.initialize();
  heartbeatService.start();
  chartWorker.start();
  if (startup.clearAllLock?.exists) {
    console.log('[startup] sync automático bloqueado por .storage/clear-all.lock.');
  } else {
    syncWorker.start();
  }
  if (startup.selfNode) await ngrokCoordinator.start();
  else console.log('[ngrok] ignorado: servidor local ainda não configurado');

  const server = app.listen(env.port, env.bindHost, () => {
    console.log(`[server] escutando em ${env.bindHost}:${env.port}`);
    logAccessUrls(env.port, Boolean(startup.selfNode));
    if (env.autoJoinHostOnStartup && !startup.clearAllLock?.exists && env.autoJoinHostUrl) {
      setImmediate(async () => {
        try {
          await fetch(`http://127.0.0.1:${env.port}/api/cluster/request-join-host`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ host_url: env.autoJoinHostUrl })
          });
        } catch (error) {
          console.error(`[startup] AUTO_JOIN_HOST_ON_STARTUP falhou: ${error.message}`);
        }
      });
    } else if (env.autoJoinHostOnStartup && startup.clearAllLock?.exists) {
      console.log('[startup] AUTO_JOIN_HOST_ON_STARTUP bloqueado por .storage/clear-all.lock.');
    }
  });
  server.on('error', handleListenError);
};

start().catch((error) => {
  console.error('[server] falha ao iniciar:', error);
  process.exitCode = 1;
});
