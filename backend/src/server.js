const path = require('path');
const express = require('express');
const session = require('express-session');

const env = require('./config/env');
const { requireAuth } = require('./services/auth.service');
const clusterService = require('./services/cluster.service');
const heartbeatService = require('./services/heartbeat.service');

const authRoutes = require('./routes/auth.routes');
const serverRoutes = require('./routes/server.routes');
const clusterRoutes = require('./routes/cluster.routes');
const ngrokRoutes = require('./routes/ngrok.routes');
const inmetRoutes = require('./routes/inmet.routes');

const app = express();
const publicPath = path.resolve(__dirname, '../../public');

app.use(express.json());
app.use(
  session({
    secret: env.sessionSecret,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: 'lax'
    }
  })
);

app.get('/', (req, res) => {
  if (!req.session.user) {
    return res.redirect('/login.html');
  }

  return res.redirect('/admin.html');
});

app.get('/login.html', (req, res) => {
  res.sendFile(path.join(publicPath, 'login.html'));
});

app.get('/admin.html', requireAuth, (req, res) => {
  res.sendFile(path.join(publicPath, 'admin.html'));
});

app.use('/assets', express.static(path.join(publicPath, 'assets')));

app.use('/api/auth', authRoutes);
app.use('/internal', clusterRoutes);
app.use('/api/servers', requireAuth, serverRoutes);
app.use('/api/ngrok', requireAuth, ngrokRoutes);
app.use('/api/inmet', requireAuth, inmetRoutes);

app.use((error, req, res, _next) => {
  console.error('[server] erro não tratado:', error);
  res.status(500).json({ message: 'Erro interno do servidor.' });
});

const start = async () => {
  await clusterService.makeLocalStandby();

  console.log('[cluster] Consultando peers antes de iniciar ngrok...');
  await clusterService.refreshPeers();
  const known = clusterService.getKnownServers();
  const activeHost = await clusterService.findValidActiveHost(known);

  if (activeHost && activeHost.serverUrl !== env.serverUrl) {
    console.log(`[cluster] HOST ativo encontrado: ${activeHost.serverName}`);
    console.log('[cluster] HOST ativo encontrado. Iniciando como STANDBY.');
    console.log('[cluster] Ngrok não será iniciado neste servidor');
  } else {
    await clusterService.electHostIfNeeded();
  }

  heartbeatService.start();

  app.listen(env.port, () => {
    console.log(`[${env.serverName}] rodando em ${env.serverUrl} (porta ${env.port})`);
  });
};

start();
