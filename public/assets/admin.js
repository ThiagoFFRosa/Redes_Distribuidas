const serversBody = document.getElementById('serversBody');
const summary = document.getElementById('summary');
const statusMsg = document.getElementById('statusMsg');
const refreshBtn = document.getElementById('refreshBtn');
const logoutBtn = document.getElementById('logoutBtn');
const newServerUrlInput = document.getElementById('newServerUrl');
const testConnectionBtn = document.getElementById('testConnectionBtn');
const registerServerBtn = document.getElementById('registerServerBtn');
const registerMsg = document.getElementById('registerMsg');

let polling = null;

const showMessage = (message, isError = false) => {
  statusMsg.textContent = message;
  statusMsg.className = isError ? 'error' : 'success';
};

const showRegisterMessage = (message, isError = false) => {
  registerMsg.textContent = message;
  registerMsg.className = isError ? 'error' : 'success';
};

const badgeClass = (server) => {
  if (!server.online) return 'badge offline';
  if (server.role === 'HOST') return 'badge host';
  return 'badge standby';
};

const formatLastSeen = (lastSeen) => {
  if (!lastSeen) return '-';
  return new Date(lastSeen).toLocaleString('pt-BR');
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
          <td>${formatLastSeen(server.lastSeen)}</td>
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
    showMessage('Tentando liberar domínio público...');
    const response = await fetch('/api/servers/switch-host', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetUrl })
    });

    const data = await response.json();
    if (!response.ok) throw new Error(data.message || 'Falha ao trocar HOST.');

    renderSummary(data);
    renderServers(data.servers);

    if (!data.switchHost?.switched) {
      showMessage(data.switchHost?.message || 'Falha ao assumir HOST. Tentando eleição automática.', true);
      await loadServers(true);
      return;
    }

    showMessage(`HOST alterado para ${targetUrl}.`);
  } catch (error) {
    showMessage(error.message || 'Erro ao trocar HOST.', true);
  }
};

const getServerUrlInput = () => newServerUrlInput.value.trim();

const testConnection = async () => {
  const serverUrl = getServerUrlInput();
  if (!serverUrl) {
    showRegisterMessage('Informe a URL do servidor.', true);
    return;
  }

  testConnectionBtn.disabled = true;
  try {
    const response = await fetch('/api/servers/test-connection', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ serverUrl })
    });

    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.message || 'Servidor não respondeu ou chave do cluster inválida');
    }

    showRegisterMessage(`Conexão válida: ${data.serverName} (${data.role}) - ${data.serverUrl}`);
  } catch (error) {
    showRegisterMessage(error.message || 'Falha ao testar conexão.', true);
  } finally {
    testConnectionBtn.disabled = false;
  }
};

const registerServer = async () => {
  const serverUrl = getServerUrlInput();
  if (!serverUrl) {
    showRegisterMessage('Informe a URL do servidor.', true);
    return;
  }

  registerServerBtn.disabled = true;
  try {
    const response = await fetch('/api/servers/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ serverUrl })
    });

    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.message || 'Falha ao cadastrar servidor.');
    }

    showRegisterMessage(data.message || 'Servidor cadastrado com sucesso.');
    await loadServers(true);
  } catch (error) {
    showRegisterMessage(error.message || 'Falha ao cadastrar servidor.', true);
  } finally {
    registerServerBtn.disabled = false;
  }
};

refreshBtn.addEventListener('click', () => loadServers());
testConnectionBtn.addEventListener('click', testConnection);
registerServerBtn.addEventListener('click', registerServer);

logoutBtn.addEventListener('click', async () => {
  await fetch('/api/auth/logout', { method: 'POST' });
  window.location.href = '/login.html';
});

loadServers();
polling = setInterval(() => loadServers(true), 3000);
window.addEventListener('beforeunload', () => clearInterval(polling));
