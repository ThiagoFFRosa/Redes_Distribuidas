const serversBody = document.getElementById('serversBody');
const summary = document.getElementById('summary');
const statusMsg = document.getElementById('statusMsg');
const refreshBtn = document.getElementById('refreshBtn');
const logoutBtn = document.getElementById('logoutBtn');

let polling = null;

const showMessage = (message, isError = false) => {
  statusMsg.textContent = message;
  statusMsg.className = isError ? 'error' : 'success';
};

const badgeClass = (server) => {
  if (!server.online) return 'badge offline';
  if (server.role === 'HOST') return 'badge host';
  return 'badge standby';
};

const renderSummary = (data) => {
  const activeHost = data.servers.find((server) => server.online && server.role === 'HOST');
  const activePublicUrl = activeHost?.publicUrl || null;
  summary.innerHTML = `
    <article class="card">
      <h3>Servidor atual</h3>
      <p>${data.currentServer.serverName}</p>
      <small>${data.currentServer.serverUrl}</small>
    </article>
    <article class="card">
      <h3>HOST ativo</h3>
      <p>${activeHost ? activeHost.serverName : 'Nenhum HOST ativo'}</p>
      <small>${activeHost ? activeHost.serverUrl : 'Failover em andamento'}</small>
    </article>
    <article class="card">
      <h3>URL pública atual</h3>
      <p>${activePublicUrl || 'Sem ngrok ativo'}</p>
    </article>
  `;

  if (!activeHost) {
    showMessage('Alerta: nenhum HOST online no momento.', true);
    return;
  }

  if (!activePublicUrl) {
    showMessage('HOST ativo sem URL pública no momento (ngrok indisponível).', true);
  }
};

const renderServers = (servers) => {
  serversBody.innerHTML = servers
    .map((server) => {
      const disabled = !server.online ? 'disabled' : '';
      return `
        <tr>
          <td>${server.serverName}</td>
          <td>${server.serverUrl}</td>
          <td><span class="${server.online ? 'badge online' : 'badge offline'}">${server.online ? 'ONLINE' : 'OFFLINE'}</span></td>
          <td><span class="${badgeClass(server)}">${server.role}</span></td>
          <td>${server.publicUrl || '-'}</td>
          <td>${server.isHostingPublicFrontend ? '<span class="badge host">SIM</span>' : 'NÃO'}</td>
          <td><button class="switch-btn" data-url="${server.serverUrl}" ${disabled}>Tornar HOST</button></td>
        </tr>
      `;
    })
    .join('');

  document.querySelectorAll('.switch-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const targetUrl = button.dataset.url;
      await switchHost(targetUrl);
    });
  });
};

const loadServers = async (silent = false) => {
  try {
    const response = await fetch('/api/servers');

    if (response.status === 401) {
      window.location.href = '/login.html';
      return;
    }

    if (!response.ok) {
      throw new Error('Falha ao buscar servidores.');
    }

    const data = await response.json();
    renderSummary(data);
    renderServers(data.servers);
    if (!silent) showMessage('Lista atualizada com sucesso.');
  } catch (error) {
    showMessage(error.message || 'Erro ao atualizar lista.', true);
  }
};

const switchHost = async (targetUrl) => {
  try {
    const response = await fetch('/api/servers/switch-host', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetUrl })
    });

    const data = await response.json();
    if (!response.ok) throw new Error(data.message || 'Falha ao trocar HOST.');

    renderSummary(data);
    renderServers(data.servers);
    showMessage(`HOST alterado para ${targetUrl}.`);
  } catch (error) {
    showMessage(error.message || 'Erro ao trocar HOST.', true);
  }
};

refreshBtn.addEventListener('click', () => loadServers());

logoutBtn.addEventListener('click', async () => {
  await fetch('/api/auth/logout', { method: 'POST' });
  window.location.href = '/login.html';
});

loadServers();
polling = setInterval(() => loadServers(true), 3000);
window.addEventListener('beforeunload', () => clearInterval(polling));
