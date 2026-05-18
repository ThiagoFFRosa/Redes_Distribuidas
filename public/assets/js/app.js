/**
 * app.js
 * Lógica principal do Frontend Simulado.
 * Feito com JavaScript puro para facilitar a integração com a versão real (Node/Express).
 */

document.addEventListener('DOMContentLoaded', () => {

    /* ==========================================================================
       ESTADO DA APLICAÇÃO (Mock)
       ========================================================================== */
    
    const state = {
        pontos: [
            { id: 1, nome: 'Rio Paraíba do Sul - Centro', lat: -23.1868, lng: -45.8860, cidade: 'SJC - SP', tipo: 'nivel', status: 'ativo', nivel: 2.30 },
            { id: 2, nome: 'Rio Tietê - Ponto 42', lat: -23.518, lng: -46.732, cidade: 'São Paulo - SP', tipo: 'nivel', status: 'ativo', nivel: 3.10 },
            { id: 3, nome: 'Represa Guarapiranga', lat: -23.682, lng: -46.733, cidade: 'São Paulo - SP', tipo: 'nivel', status: 'ativo', nivel: 4.80 },
            { id: 4, nome: 'Rio Una - Ponte Nova', lat: -23.030, lng: -45.560, cidade: 'Taubaté - SP', tipo: 'nivel', status: 'ativo', nivel: 5.15 }, /* Nivel critico mock */
        ],
        servidores: [],
        historicoGrafico: [2.1, 2.15, 2.2, 2.3, 2.4, 2.5, 2.45, 2.3],
        eventosDashboard: [
            { id: 1, tipo: 'info', msg: 'server_a assumiu como HOST', time: '10 min atrás' },
            { id: 2, tipo: 'dado', msg: 'Nova medição: Rio Tietê (3.10m)', time: '5 min atrás' },
            { id: 3, tipo: 'alerta', msg: 'Rio Una em nível crítico (5.15m)', time: '2 min atrás' },
        ]
    };


    /* ==========================================================================
       NAVEGAÇÃO DO DASHBOARD (SPA Simples)
       ========================================================================== */
    const navLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('.section-content');
    const pageTitle = document.getElementById('page-title');

    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const targetId = link.getAttribute('data-target');
            
            // Atualizar classes dos links
            navLinks.forEach(l => l.classList.remove('active'));
            link.classList.add('active');
            
            // Atualizar o título
            pageTitle.textContent = link.textContent.trim();

            // Esconder todas as seções e mostrar a escolhida
            sections.forEach(sec => sec.classList.remove('block'));
            document.getElementById(`sec-${targetId}`).classList.add('block');

            // Fechar sidebar no mobile (se estiver aberta)
            sidebar.classList.add('-translate-x-full');
            sidebarOverlay.classList.add('hidden');

            // Refazer o resize do mapa do Leaflet caso precise (bug comum do leaflet em div oculta)
            if(targetId === 'pontos-dados' && map) {
                setTimeout(() => map.invalidateSize(), 150);
            }
        });
    });

    // MOBILE SIDEBAR
    const mobileMenuBtn = document.getElementById('mobile-menu-btn');
    const closeSidebarBtn = document.getElementById('close-sidebar-btn');
    const sidebar = document.getElementById('sidebar');
    const sidebarOverlay = document.getElementById('sidebar-overlay');

    if(mobileMenuBtn) {
        mobileMenuBtn.addEventListener('click', () => {
            sidebar.classList.remove('-translate-x-full');
            sidebarOverlay.classList.remove('hidden');
        });
    }

    if(closeSidebarBtn && sidebarOverlay) {
        const fecharMenu = () => {
            sidebar.classList.add('-translate-x-full');
            sidebarOverlay.classList.add('hidden');
        };
        closeSidebarBtn.addEventListener('click', fecharMenu);
        sidebarOverlay.addEventListener('click', fecharMenu);
    }


    /* ==========================================================================
       SETUP INICIAL DAS VIEWS
       ========================================================================== */
    
    // Inicializar ícones dinâmicos onde não foram renderizados nativamente (Lucide)
    const initCards = () => {
        document.getElementById('dash-pontos').textContent = state.pontos.length;
        
        let standbyCount = state.servidores.filter(s => s.role === 'STANDBY' && s.status === 'ONLINE').length;
        document.getElementById('dash-standby').textContent = standbyCount;
        
        let host = state.servidores.find(s => s.is_self) || state.servidores.find(s => s.role === 'HOST');
        if(host) document.getElementById('dash-host').textContent = host.node_name;
    }

    /* ==========================================================================
       GRÁFICO (Chart.js)
       ========================================================================== */
    let chart;
    const initChart = () => {
        const ctx = document.getElementById('chart-nivel-rio');
        if(!ctx) return;

        chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: ['16:00', '17:00','18:00','19:00','20:00','21:00','22:00','Agora'],
                datasets: [{
                    label: 'Nível Médio (m)',
                    data: state.historicoGrafico,
                    borderColor: '#0284c7', // primary
                    backgroundColor: 'rgba(2, 132, 199, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointBackgroundColor: '#0ea5e9',
                    pointRadius: 4,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: false, min: 1.5, ticks: { padding: 10 } },
                    x: { grid: { display: false } }
                }
            }
        });
    };

    /* ==========================================================================
       MAPA E CADASTRO DE PONTO (Leaflet)
       ========================================================================== */
    let map;
    let marker;
    let selectedLatLng = null;

    const initMap = () => {
        const mapContainer = document.getElementById('map-container');
        if(!mapContainer) return;

        // Focar no estado de SP para demonstração
        map = L.map('map-container').setView([-23.5505, -46.6333], 7);

        L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap &copy; CARTO'
        }).addTo(map);

        // Adicionar marcadores baseados no state
        state.pontos.forEach(p => {
            const m = L.circleMarker([p.lat, p.lng], {
                radius: 8,
                fillColor: p.nivel > 5 ? "#ef4444" : "#0284c7",
                color: "#fff",
                weight: 2,
                opacity: 1,
                fillOpacity: 0.8
            }).addTo(map);
            m.bindPopup(`<b>${p.nome}</b><br>Nível Atual: ${p.nivel}m`);
        });

        // Clique para adicionar novo
        map.on('click', function(e) {
            selectedLatLng = e.latlng;
            
            // Atualizar inputs
            document.getElementById('pt-lat').value = selectedLatLng.lat.toFixed(5);
            document.getElementById('pt-lng').value = selectedLatLng.lng.toFixed(5);

            // Mover/Criar pino de seleção
            if(marker) {
                marker.setLatLng(selectedLatLng);
            } else {
                marker = L.marker(selectedLatLng).addTo(map);
            }
        });

        renderTabelaPontos();
        // TODO(back-end): integrar GET /api/data-points para carregar pontos reais ao iniciar.
    };

    const renderTabelaPontos = () => {
        const tbody = document.getElementById('tbody-pontos');
        const select = document.getElementById('dado-ponto');
        
        if(!tbody || !select) return;
        
        tbody.innerHTML = '';
        select.innerHTML = '<option value="" disabled selected>Selecione um ponto...</option>';

        state.pontos.forEach(p => {
            // Add na tabela
            tbody.innerHTML += `
                <tr class="hover:bg-slate-50">
                    <td class="p-4">
                        <div class="font-medium text-dark">${p.nome}</div>
                        <div class="text-xs text-slate-500">ID: ${p.id}</div>
                    </td>
                    <td class="p-4 text-slate-600">${p.cidade}</td>
                    <td class="p-4 text-xs font-mono text-slate-500">${p.lat.toFixed(4)}, ${p.lng.toFixed(4)}</td>
                    <td class="p-4">
                        <span class="px-2 py-1 rounded bg-green-100 text-green-700 text-xs font-semibold text-center inline-block w-16">
                            ${p.status.toUpperCase()}
                        </span>
                    </td>
                </tr>
            `;

            // Add no select do form de inserir dados
            select.innerHTML += `<option value="${p.id}">${p.nome}</option>`;
        });
        
        document.getElementById('card-pontos-lista').classList.remove('hidden');
    }

    // Submit de Cadastro de Ponto
    const formCadastrar = document.getElementById('form-cadastrar-ponto');
    if(formCadastrar){
        formCadastrar.addEventListener('submit', (e) => {
            e.preventDefault();
            
            if(!selectedLatLng) {
                console.warn("Por favor, clique no mapa para selecionar a localização do ponto.");
                return;
            }

            const novoPonto = {
                id: state.pontos.length + 1,
                nome: document.getElementById('pt-nome').value,
                lat: selectedLatLng.lat,
                lng: selectedLatLng.lng,
                cidade: document.getElementById('pt-cidade').value,
                tipo: document.getElementById('pt-tipo').value,
                status: 'ativo',
                nivel: 0.0
            };

            // TODO(back-end): integrar POST /api/data-points e persistir em banco.
            // Simular DB
            state.pontos.push(novoPonto);
            
            // Resetar
            formCadastrar.reset();
            selectedLatLng = null;
            if(marker) map.removeLayer(marker);
            marker = null;
            
            document.getElementById('pt-lat').value = "No mapa";
            document.getElementById('pt-lng').value = "No mapa";

            // Atualizar UI
            renderTabelaPontos();
        // TODO(back-end): integrar GET /api/data-points para carregar pontos reais ao iniciar.
            initCards();

            // Adicionar ao mapa (simples marker verde pra indicar novo)
            L.circleMarker([novoPonto.lat, novoPonto.lng], {
                radius: 8, fillColor: "#22c55e", color: "#fff", weight: 2, opacity: 1, fillOpacity: 0.8
            }).addTo(map).bindPopup(`<b>${novoPonto.nome}</b><br>Ponto Novo cadatrado.`);

            console.info(`Ponto "${novoPonto.nome}" cadastrado com sucesso!`);
        });
    }

    /* ==========================================================================
       INSERÇÃO DE DADOS & "RABBITMQ SIMULADO"
       ========================================================================== */
    
    // Gerar Aleatório
    const btnAleatorio = document.getElementById('btn-dado-aleatorio');
    if(btnAleatorio){
        btnAleatorio.addEventListener('click', () => {
            const selects = document.getElementById('dado-ponto');
            if(selects.options.length > 1) {
                const randomOpt = 1 + Math.floor(Math.random() * (selects.options.length - 1));
                selects.selectedIndex = randomOpt;
            }

            // Gerar nível entre 1.00 e 6.00
            const val = (Math.random() * (6.0 - 1.0) + 1.0).toFixed(2);
            document.getElementById('dado-valor').value = val;
            
            const obs = ["Nível subindo", "Vazão normal", "Alerta temporal", "Choverá em breve"];
            document.getElementById('dado-obs').value = obs[Math.floor(Math.random() * obs.length)];
        });
    }

    const formInserir = document.getElementById('form-inserir-dado');
    if(formInserir) {
        formInserir.addEventListener('submit', (e) => {
            e.preventDefault();
            
            const pontoId = document.getElementById('dado-ponto').value;
            if(!pontoId) { console.warn("Selecione um ponto."); return; }

            const ponto = state.pontos.find(p => p.id == pontoId);
            const valor = parseFloat(document.getElementById('dado-valor').value);
            const horaStr = new Date().toLocaleTimeString();

            // 1. Atualizar state e gráfico (mock simples)
            ponto.nivel = valor;
            state.historicoGrafico.shift();
            state.historicoGrafico.push(valor);
            chart.update();
            document.getElementById('dash-last-reading').textContent = horaStr;

            // 2. Simular fila RabbitMq (Adicionar logs)
            adicionarLogFila(ponto, valor);

            // 3. Checar status de alerta
            checarStatusAlerta(ponto);

            formInserir.reset();
        });
    }

    const adicionarLogFila = (ponto, valor) => {
        const logContainer = document.getElementById('log-fila');
        if(!logContainer) return;
        
        const tempo = new Date().toLocaleTimeString();
        const refId = Math.random().toString(36).substring(2, 8).toUpperCase();

        // Fluxo mockado de eventos em tela simulando log real
        
        const eventoStr = `
            <div class="flex flex-col text-slate-300 pb-2 border-b border-slate-800/50 animate-flash">
                <span class="text-accent">>[${tempo}] [PUBLISH] Evento emitido: { p_id: ${ponto.id}, val: ${valor}m }</span>
                <span class="text-slate-500 pl-4">|- ROUTING_KEY: 'telemetria.agua.nivel' | msg_id: ${refId}</span>
                <span class="text-yellow-400 pl-4 mt-1">... Processando no servidor HOST (server_a)</span>
            </div>
        `;
        
        // Inserir no topo
        logContainer.insertAdjacentHTML('afterbegin', eventoStr);

        // Simular delay de processamento
        setTimeout(() => {
            logContainer.insertAdjacentHTML('afterbegin', `
                <div class="flex flex-col pb-2 border-b border-slate-800/50 animate-flash">
                    <span class="text-green-400">>[${new Date().toLocaleTimeString()}] [ACK] Dado persistido (ID: ${refId})</span>
                    <span class="text-slate-500 pl-4">|- Replicando para NÓS STANDBY...</span>
                </div>
            `);
        }, 800);

        setTimeout(() => {
            logContainer.insertAdjacentHTML('afterbegin', `
                 <div class="text-slate-500 pb-2 border-b border-slate-800/50">
                    >[${new Date().toLocaleTimeString()}] Replicação síncrona completa (server_b: OK, server_c: TIMEOUT)
                </div>
            `);
        }, 1500);
    }

    /* ==========================================================================
       DASHBOARD E ALERTAS
       ========================================================================== */


    const selfModal = document.getElementById('self-config-modal');
    const selfError = document.getElementById('self-config-error');
    const openSelfModal = () => { if (selfModal) selfModal.classList.remove('hidden'); if (selfModal) selfModal.classList.add('flex'); };
    const closeSelfModal = () => { if (selfModal) selfModal.classList.add('hidden'); if (selfModal) selfModal.classList.remove('flex'); };
    const addModal = document.getElementById('add-server-modal');
    const openAddModal = () => { if (addModal) { addModal.classList.remove('hidden'); addModal.classList.add('flex'); } };
    const closeAddModal = () => { if (addModal) { addModal.classList.add('hidden'); addModal.classList.remove('flex'); } };

    const fetchClusterNodes = async () => {
        const selfResp = await fetch('/api/cluster/self');
        if (selfResp.status === 401) { window.location.href = '/login'; return; }
        const selfData = await selfResp.json();
        if (!selfData.configured) {
            openSelfModal();
        } else {
            closeSelfModal();
        }
        const nodesResp = await fetch('/api/cluster/nodes');
        const nodesData = await nodesResp.json();
        state.servidores = nodesData.nodes || [];
    };

    const renderServidores = () => {
        const tbody = document.getElementById('tbody-servidores');
        if(!tbody) return;

        tbody.innerHTML = '';
        state.servidores.forEach(s => {
            const isHost = s.role === 'HOST';
            const isOnline = s.status === 'ONLINE';
            const roleBadge = `<span class="px-2.5 py-1 text-xs font-semibold rounded-md bg-slate-100 text-slate-700">${s.role}</span>`;
            const statusInd = `<div class="flex items-center gap-1.5 text-sm font-medium ${isOnline ? 'text-green-600':'text-red-600'}"><span class="w-2 h-2 rounded-full ${isOnline ? 'bg-green-500':'bg-red-500'}"></span>${s.status}</div>`;
            tbody.innerHTML += `
                <tr class="hover:bg-slate-50 transition-colors">
                    <td class="p-4 font-semibold text-dark">${s.node_name}${s.is_self ? ' (Este servidor)' : ''}</td>
                    <td class="p-4 text-sm font-mono text-slate-600">${s.tailscale_ip}</td>
                    <td class="p-4">${roleBadge}</td>
                    <td class="p-4">${statusInd}</td>
                    <td class="p-4 text-right flex justify-end gap-2">${s.is_self ? '<button class="px-2 py-1 border rounded" data-action="edit-self">Editar</button>' : ''}<button class="px-2 py-1 border rounded" onclick="fetch('/api/cluster/nodes/${s.id}/healthcheck',{method:'POST'}).then(()=>window.location.reload())">Testar conexão</button></td>
                </tr>`;
        });
    }

    const renderEventosDash = () => {
        const ul = document.getElementById('dashboard-events-list');
        if(!ul) return;
        
        ul.innerHTML = '';
        state.eventosDashboard.forEach((ev, idx) => {
            let icon = 'info';
            let color = 'text-primary bg-primary/10';
            
            if(ev.tipo === 'dado') { icon = 'activity'; color = 'text-green-600 bg-green-50'; }
            if(ev.tipo === 'alerta') { icon = 'siren'; color = 'text-red-500 bg-red-50'; }

            ul.innerHTML += `
                <li class="p-4 flex gap-4 hover:bg-white transition-colors duration-200">
                    <div class="w-8 h-8 rounded-full ${color} flex items-center justify-center mt-1 shrink-0">
                        <i data-lucide="${icon}" class="w-4 h-4"></i>
                    </div>
                    <div>
                        <p class="text-sm font-medium text-dark">${ev.msg}</p>
                        <p class="text-xs text-slate-500 mt-1">${ev.time}</p>
                    </div>
                </li>
            `;
        });
        
        lucide.createIcons(); // renderizar os icones que acabei de por
    };

    const checarStatusAlerta = (ponto) => {
        let severidade = 'normal';
        if(ponto.nivel >= 4.0 && ponto.nivel < 5.0) severidade = 'atencao';
        if(ponto.nivel >= 5.0) severidade = 'critico';

        if(severidade !== 'normal') {
            const h = new Date().toLocaleTimeString();
            const logMsg = `Alerta gerado: ${ponto.nome} em nível ${severidade.toUpperCase()}`;
            
            state.eventosDashboard.unshift({ id: Date.now(), tipo: 'alerta', msg: logMsg, time: 'Agora' });
            if(state.eventosDashboard.length > 5) state.eventosDashboard.pop(); // manter 5
            
            renderEventosDash();
            renderTabelaAlertas(); // re-render alerta se a pagina estiver limpa
        }
    }


    const renderTabelaAlertas = () => {
        const tbody = document.getElementById('tbody-alertas');
        if(!tbody) return;

        tbody.innerHTML = '';
        
        // Gerar alertas baseados nos pontos atuais
        const alertas = state.pontos.filter(p => p.nivel >= 4.0).map(p => {
            const isCrit = p.nivel >= 5.0;
            return {
                id: p.id,
                severidade: isCrit ? 'critico' : 'atencao',
                ponto: p.nome,
                nivel: `${p.nivel.toFixed(2)}m`,
                tipo: 'Nível Elevado',
                hora: '2 min atrás',
                status: 'Ativo'
            }
        });

        if(alertas.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" class="p-8 text-center text-slate-500">Nenhum evento de risco detectado. Todo o sistema está operando em normalidade.</td></tr>`;
            return;
        }

        alertas.forEach(a => {
            const isCrit = a.severidade === 'critico';
            
            const dotClass = isCrit 
                ? "bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.8)]" 
                : "bg-yellow-500 shadow-[0_0_8px_rgba(234,179,8,0.5)]";
            
            const badgeClass = isCrit
                ? "bg-red-50 text-red-700 border border-red-200"
                : "bg-yellow-50 text-yellow-700 border border-yellow-200";
            
            tbody.innerHTML += `
                <tr class="hover:bg-slate-50 transition-colors bg-white">
                    <td class="p-4 text-center">
                        <div class="inline-flex w-3 h-3 rounded-full ${dotClass}"></div>
                    </td>
                    <td class="p-4 font-semibold text-dark">${a.ponto}</td>
                    <td class="p-4 font-mono font-bold text-slate-600">${a.nivel}</td>
                    <td class="p-4 text-slate-600">${a.tipo}</td>
                    <td class="p-4 text-slate-500 text-sm whitespace-nowrap">${a.hora}</td>
                    <td class="p-4 text-right">
                        <span class="px-2.5 py-1 text-xs font-semibold rounded-md ${badgeClass}">
                            ${a.status.toUpperCase()}
                        </span>
                    </td>
                </tr>
            `;
        });
    }



    const renderJoinSelfData = (selfNode) => {
        const btn = document.getElementById('btn-request-join-host');
        const nameEl = document.getElementById('join-self-node-name');
        const ipEl = document.getElementById('join-self-node-ip');
        const urlEl = document.getElementById('join-self-public-url');
        const roleEl = document.getElementById('join-self-requested-role');
        const feedback = document.getElementById('join-host-feedback');
        if (!nameEl || !ipEl || !urlEl || !roleEl) return;
        if (!selfNode) {
            nameEl.textContent = '-'; ipEl.textContent = '-'; urlEl.textContent = '-'; roleEl.textContent = 'STANDBY';
            if (btn) btn.disabled = true;
            if (feedback) feedback.textContent = 'Configure este servidor antes de solicitar entrada em um host.';
            return;
        }
        nameEl.textContent = selfNode.node_name || '-';
        ipEl.textContent = selfNode.tailscale_ip || '-';
        urlEl.textContent = selfNode.public_url || '-';
        roleEl.textContent = 'STANDBY';
        if (btn) btn.disabled = false;
    };

    const loadJoinRequests = async () => {
        const resp = await fetch('/api/cluster/join-requests?status=PENDING');
        if (!resp.ok) return;
        const data = await resp.json();
        const tbody = document.getElementById('tbody-join-requests');
        if (!tbody) return;
        tbody.innerHTML = '';
        (data.requests || []).forEach((r) => {
            tbody.innerHTML += `<tr><td class="p-2">${r.node_name}</td><td class="p-2 font-mono">${r.tailscale_ip}</td><td class="p-2">${r.requested_role}</td><td class="p-2">${new Date(r.created_at).toLocaleString()}</td><td class="p-2 text-right"><button data-act="approve" data-id="${r.id}" class="px-2 py-1 bg-green-600 text-white rounded">Aprovar</button> <button data-act="reject" data-id="${r.id}" class="px-2 py-1 bg-red-600 text-white rounded">Rejeitar</button></td></tr>`;
        });
    };

    document.getElementById('self-config-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        selfError.textContent = '';
        const payload = { node_name: document.getElementById('self-node-name').value.trim(), tailscale_ip: document.getElementById('self-node-ip').value.trim(), public_url: document.getElementById('self-public-url').value.trim(), role: document.getElementById('self-role').value };
        if (!payload.node_name || !payload.tailscale_ip) { selfError.textContent = 'Nome e IP são obrigatórios.'; return; }
        const resp = await fetch('/api/cluster/self', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) { selfError.textContent = data.message || 'Erro ao salvar configuração.'; return; }
        closeSelfModal(); await fetchClusterNodes(); renderServidores(); initCards();
    });

    document.getElementById('tbody-join-requests')?.addEventListener('click', async (e) => {
        const btn = e.target.closest('button[data-id]'); if (!btn) return;
        const id = btn.getAttribute('data-id');
        const act = btn.getAttribute('data-act');
        await fetch(`/api/cluster/join-requests/${id}/${act}`, { method:'POST' });
        await fetchClusterNodes(); renderServidores(); await loadJoinRequests();
    });

    document.getElementById('form-request-join-host')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const feedback = document.getElementById('join-host-feedback');
        feedback.textContent = '';
        const payload = { host_url: document.getElementById('join-host-url').value.trim() };
        const resp = await fetch('/api/cluster/request-join-host', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
        const data = await resp.json().catch(() => ({}));
        feedback.textContent = resp.ok ? (data.message || 'Solicitação enviada. Aguarde aprovação no host.') : (data.message || 'Falha ao enviar solicitação.');
    });

    // --- Inits Globais ---
    fetch('/api/cluster/self').then((r) => r.ok ? r.json() : null).then((d) => renderJoinSelfData(d?.node || null)).catch(() => renderJoinSelfData(null));

    fetchClusterNodes().finally(async () => {
        renderServidores();
        initCards();
    });
    initChart();
    if(document.getElementById('map-container')) {
        setTimeout(initMap, 100); // Dar tempo pra div renderizar no css se inicializou visivel
    }
    renderEventosDash();
    renderTabelaAlertas();



    document.getElementById('refresh-health-btn')?.addEventListener('click', async () => {
        await fetch('/api/cluster/healthcheck-all', { method: 'POST' });
        await fetchClusterNodes();
        renderServidores();
        initCards();
    });

    document.getElementById('add-server-btn')?.addEventListener('click', openAddModal);
    document.getElementById('close-add-server-modal')?.addEventListener('click', closeAddModal);

    document.getElementById('add-server-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const err = document.getElementById('add-server-error');
        err.textContent = '';
        const payload = { node_name: document.getElementById('add-node-name').value.trim(), tailscale_ip: document.getElementById('add-node-ip').value.trim(), public_url: document.getElementById('add-public-url').value.trim(), role: document.getElementById('add-role').value, status: 'UNKNOWN' };
        const resp = await fetch('/api/cluster/nodes', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) { err.textContent = data.message || 'Erro ao adicionar servidor.'; return; }
        closeAddModal();
        await fetchClusterNodes();
        renderServidores();
        initCards();
    });

    document.getElementById('tbody-servidores')?.addEventListener('click', async (e) => {
      if (!e.target.closest('[data-action="edit-self"]')) return;
      const self = state.servidores.find((n) => n.is_self);
      if (!self) return;
      document.getElementById('self-node-name').value = self.node_name || '';
      document.getElementById('self-node-ip').value = self.tailscale_ip || '';
      document.getElementById('self-public-url').value = self.public_url || '';
      document.getElementById('self-role').value = self.role || 'UNKNOWN';
      openSelfModal();
    });

    // Reativar icons finais
    lucide.createIcons();
});
