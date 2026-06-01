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
const processingRoutes = require('./routes/processing.routes');
const chartWorker = require('./services/chart-worker.service');
const syncWorker = require('./services/sync-worker');
const syncEventService = require('./services/sync-event.service');

const app = express();
const publicPath = path.resolve(__dirname, '../../public');
app.use(express.json());
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
app.use((error, req, res, _next) => { console.error('[server] erro não tratado:', error); res.status(500).json({ message: 'Erro interno do servidor.' }); });

const start = async () => {
  await clusterStartupService.initialize();
  await syncEventService.backfillExistingSyncEvents().catch((error) => console.error('[sync] backfill inicial falhou:', error.message));
  heartbeatService.start();
  chartWorker.start();
  syncWorker.start();
  app.listen(env.port, async () => {
    console.log(`Custom NewTab API running on http://localhost:${env.port}`);
    const selfNode = await repo.getSelfNode();
    const display = selfNode?.node_name || 'server';
    const hostIp = selfNode?.tailscale_ip || '127.0.0.1';
    console.log(`[${display}] rodando em http://${hostIp}:${env.port} (porta ${env.port})`);
  });
};

start();
