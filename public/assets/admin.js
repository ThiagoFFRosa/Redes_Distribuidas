const serversBody = document.getElementById('serversBody');
const summary = document.getElementById('summary');
const statusMsg = document.getElementById('statusMsg');
const refreshBtn = document.getElementById('refreshBtn');
const cleanupNodesBtn = document.getElementById('cleanupNodesBtn');
const syncNowBtn = document.getElementById('syncNowBtn');
const logoutBtn = document.getElementById('logoutBtn');
const newServerUrlInput = document.getElementById('newServerUrl');
const testConnectionBtn = document.getElementById('testConnectionBtn');
const registerServerBtn = document.getElementById('registerServerBtn');
const registerMsg = document.getElementById('registerMsg');

const viewServers = document.getElementById('view-servers');
const viewInmet = document.getElementById('view-inmet');
const navLinks = document.querySelectorAll('.sidebar nav a');
const inmetStationInput = document.getElementById('inmetStation');
const inmetDateInput = document.getElementById('inmetDate');
const runInmetTestBtn = document.getElementById('runInmetTestBtn');
const inmetStatusMsg = document.getElementById('inmetStatusMsg');
const inmetResults = document.getElementById('inmetResults');

let polling = null;
let syncStatusByNodeName = new Map();

const showMessage = (message, isError = false) => {
  statusMsg.textContent = message;
  statusMsg.className = isError ? 'error' : 'success';
};
const showRegisterMessage = (message, isError = false) => {
  registerMsg.textContent = message;
  registerMsg.className = isError ? 'error' : 'success';
};
const showInmetMessage = (message, isError = false) => {
  inmetStatusMsg.textContent = message;
  inmetStatusMsg.className = isError ? 'error' : 'success';
};

const badgeClass = (server) => (!server.online ? 'badge offline' : server.role === 'HOST' ? 'badge host' : 'badge standby');
const formatLastSeen = (lastSeen) => (!lastSeen ? '-' : new Date(lastSeen).toLocaleString('pt-BR'));

const renderSummary = (data) => {
  const activeHost = data.servers.find((server) => server.online && server.role === 'HOST');
  const activePublicUrl = activeHost?.publicUrl || null;
  summary.innerHTML = `<article class="card"><h3>Servidor atual</h3><p>${data.currentServer.serverName}</p><small>${data.currentServer.serverUrl}</small></article>
    <article class="card"><h3>HOST ativo</h3><p>${activeHost ? activeHost.serverName : 'Nenhum HOST ativo'}</p><small>${activeHost ? activeHost.serverUrl : 'Failover em andamento'}</small></article>
    <article class="card"><h3>URL pública atual</h3><p>${activePublicUrl || 'Sem ngrok ativo'}</p></article>`;
};

const renderServers = (servers) => {
  serversBody.innerHTML = servers.map((server) => {
    const sync = syncStatusByNodeName.get(server.serverName) || {};
    const syncLabel = sync.last_error ? `Erro: ${sync.last_error}` : (Number(sync.pending_events || 0) > 0 ? `Pendente` : 'Sync OK');
    return `<tr><td>${server.serverName}</td><td>${server.serverUrl}</td><td><span class="${server.online ? 'badge online' : 'badge offline'}">${server.online ? 'ONLINE' : 'OFFLINE'}</span></td><td><span class="${badgeClass(server)}">${server.role}</span></td><td>${server.publicUrl || '-'}</td><td>${formatLastSeen(server.lastSeen)}</td><td>${server.isHostingPublicFrontend ? '<span class="badge host">SIM</span>' : 'NÃO'}</td><td>${sync.last_sync_at ? formatLastSeen(sync.last_sync_at) : '-'}<br><small>${syncLabel}</small></td><td>${sync.pending_events ?? '-'}</td><td><button class="switch-btn" data-url="${server.serverUrl}" ${!server.online ? 'disabled' : ''}>Tornar HOST</button></td></tr>`;
  }).join('');
  document.querySelectorAll('.switch-btn').forEach((button) => button.addEventListener('click', async () => switchHost(button.dataset.url)));
};

const loadServers = async (silent = false) => {
  try {
    const response = await fetch('/api/servers');
    if (response.status === 401) return (window.location.href = '/login.html');
    if (!response.ok) throw new Error('Falha ao buscar servidores.');
    const data = await response.json();
    await loadSyncStatus();
    renderSummary(data); renderServers(data.servers);
    if (!silent) showMessage('Lista atualizada com sucesso.');
  } catch (error) { showMessage(error.message || 'Erro ao atualizar lista.', true); }
};

const loadSyncStatus = async () => {
  try {
    const response = await fetch('/api/sync/status');
    if (!response.ok) return;
    const data = await response.json();
    syncStatusByNodeName = new Map((data.nodes || []).map((node) => [node.node_name, node]));
  } catch (_error) {
    syncStatusByNodeName = new Map();
  }
};

const cleanupInvalidNodes = async () => { cleanupNodesBtn.disabled = true; try { const r = await fetch('/api/servers/cleanup', { method: 'POST' }); const d = await r.json(); if (!r.ok || !d.ok) throw new Error(d.message || 'Falha ao limpar nós inválidos.'); renderSummary(d); renderServers(d.servers); showMessage(d.message || 'Nós inválidos removidos com sucesso.'); } catch (e) { showMessage(e.message, true);} finally {cleanupNodesBtn.disabled=false;} };

const switchHost = async (targetUrl) => { try { const r= await fetch('/api/servers/switch-host',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({targetUrl})}); const d=await r.json(); if(!r.ok) throw new Error(d.message||'Falha ao trocar HOST.'); renderSummary(d); renderServers(d.servers); showMessage(d.switchHost?.switched ? `HOST alterado para ${targetUrl}.` : (d.switchHost?.message || 'Falha ao assumir HOST.'), !d.switchHost?.switched); } catch(e){showMessage(e.message,true);} };

const renderInmetResults = (results) => {
  inmetResults.innerHTML = results.map((item) => `
    <article class="card inmet-result ${item.hasUsefulFields ? 'inmet-useful' : ''}">
      <p><strong>URL:</strong> ${item.url}</p>
      <p><strong>Status:</strong> ${item.status}</p>
      <p><strong>Content-Type:</strong> ${item.contentType}</p>
      <p><strong>Sucesso:</strong> ${item.success ? 'SIM' : 'NÃO'}</p>
      <p><strong>Campos encontrados:</strong> ${(item.fieldsFound || []).join(', ') || '-'}</p>
      ${item.hasUsefulFields ? '<p class="success"><strong>Possível endpoint útil encontrado</strong></p>' : ''}
      ${item.error ? `<p class="error"><strong>Erro:</strong> ${item.error}</p>` : ''}
      ${item.sample ? `<pre>${JSON.stringify(item.sample, null, 2)}</pre>` : ''}
    </article>`).join('');
};

const runInmetTests = async () => {
  const station = inmetStationInput.value.trim() || 'A769';
  const date = inmetDateInput.value || new Date().toISOString().slice(0, 10);
  runInmetTestBtn.disabled = true;
  showInmetMessage('Executando testes...');
  try {
    const response = await fetch(`/api/inmet/test?station=${encodeURIComponent(station)}&date=${encodeURIComponent(date)}`);
    if (response.status === 401) return (window.location.href = '/login.html');
    const data = await response.json();
    if (!response.ok) throw new Error(data.message || 'Falha ao executar testes INMET.');
    renderInmetResults(data.results || []);
    showInmetMessage(`Teste concluído em ${new Date(data.testedAt).toLocaleString('pt-BR')}.`);
  } catch (error) {
    showInmetMessage(error.message || 'Erro ao executar testes.', true);
    inmetResults.innerHTML = '';
  } finally { runInmetTestBtn.disabled = false; }
};

const showView = (view) => {
  const inmet = view === 'inmet';
  viewServers.classList.toggle('hidden', inmet);
  viewInmet.classList.toggle('hidden', !inmet);
  navLinks.forEach((l) => l.classList.toggle('active', l.dataset.view === view));
};

refreshBtn.addEventListener('click', () => loadServers());
cleanupNodesBtn.addEventListener('click', cleanupInvalidNodes);
syncNowBtn.addEventListener('click', async () => {
  syncNowBtn.disabled = true;
  showMessage('Sincronizando agora...');
  try {
    const response = await fetch('/api/sync/run-now', { method: 'POST' });
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.message || 'Falha ao executar sync manual.');
    await loadServers(true);
    showMessage('Sync manual concluído.');
  } catch (error) { showMessage(error.message || 'Erro no sync manual.', true); }
  finally { syncNowBtn.disabled = false; }
});
testConnectionBtn.addEventListener('click', async () => {
  const serverUrl = newServerUrlInput.value.trim(); if (!serverUrl) return showRegisterMessage('Informe a URL do servidor.', true);
  testConnectionBtn.disabled = true;
  try { const r = await fetch('/api/servers/test-connection',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({serverUrl})}); const d=await r.json(); if(!r.ok||!d.ok) throw new Error(d.message||'Servidor não respondeu'); showRegisterMessage(`Conexão válida: ${d.serverName} (${d.role}) - ${d.serverUrl}`);} catch(e){showRegisterMessage(e.message,true);} finally{testConnectionBtn.disabled=false;}
});
registerServerBtn.addEventListener('click', async () => {
  const serverUrl = newServerUrlInput.value.trim(); if (!serverUrl) return showRegisterMessage('Informe a URL do servidor.', true);
  registerServerBtn.disabled=true; try{const r=await fetch('/api/servers/register',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({serverUrl})}); const d=await r.json(); if(!r.ok||!d.ok) throw new Error(d.message||'Falha ao cadastrar servidor.'); showRegisterMessage(d.message||'Servidor cadastrado com sucesso.'); await loadServers(true);} catch(e){showRegisterMessage(e.message,true);} finally{registerServerBtn.disabled=false;}
});
logoutBtn.addEventListener('click', async () => { await fetch('/api/auth/logout',{method:'POST'}); window.location.href='/login.html'; });
runInmetTestBtn.addEventListener('click', runInmetTests);
navLinks.forEach((link) => link.addEventListener('click', (event) => { event.preventDefault(); showView(link.dataset.view); }));

inmetDateInput.value = new Date().toISOString().slice(0, 10);
showView('servers');
loadServers();
polling = setInterval(() => loadServers(true), 3000);
window.addEventListener('beforeunload', () => clearInterval(polling));
