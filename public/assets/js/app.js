/**
 * app.js
 * Integra o dashboard com as APIs persistidas em MySQL.
 */

document.addEventListener('DOMContentLoaded', () => {
    const state = {
        pontos: [],
        servidores: [],
        dashboard: null,
        eventosDashboard: [],
        alertas: [],
        fila: []
    };

    const apiFetch = async (url, options = {}) => {
        const response = await fetch(url, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...(options.headers || {})
            }
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok) {
            throw new Error(data.message || `Falha HTTP ${response.status}`);
        }
        return data;
    };

    const escapeHtml = (value = '') => String(value).replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[char]));
    const moneyNumber = (value) => Number(value || 0).toFixed(2);
    const hasValue = (value) => value !== null && value !== undefined && value !== '';
    const formatLevel = (value, unit = 'm') => hasValue(value) ? `${Number(value).toFixed(2)}${escapeHtml(unit || 'm')}` : 'Não configurado';
    const dateLabel = (value) => value ? new Date(value).toLocaleString('pt-BR') : '-';
    const setFeedback = (id, message, isError = false) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.textContent = message || '';
        el.className = `text-sm mt-3 ${isError ? 'text-red-600' : 'text-green-600'}`;
    };

    const navLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('.section-content');
    const pageTitle = document.getElementById('page-title');
    const mobileMenuBtn = document.getElementById('mobile-menu-btn');
    const closeSidebarBtn = document.getElementById('close-sidebar-btn');
    const sidebar = document.getElementById('sidebar');
    const sidebarOverlay = document.getElementById('sidebar-overlay');

    navLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const targetId = link.getAttribute('data-target');
            navLinks.forEach(l => l.classList.remove('active'));
            link.classList.add('active');
            pageTitle.textContent = link.textContent.trim();
            sections.forEach(sec => sec.classList.remove('block'));
            document.getElementById(`sec-${targetId}`).classList.add('block');
            sidebar.classList.add('-translate-x-full');
            sidebarOverlay.classList.add('hidden');
            if(targetId === 'pontos-dados' && map) setTimeout(() => map.invalidateSize(), 150);
            if(targetId === 'visao-geral') loadDashboard();
            if(targetId === 'pontos-dados') loadDataPoints();
            if(targetId === 'inserir-dados') { loadActiveDataPoints(); loadEventQueue(); }
            if(targetId === 'alertas') loadAlerts();
        });
    });

    if(mobileMenuBtn) mobileMenuBtn.addEventListener('click', () => { sidebar.classList.remove('-translate-x-full'); sidebarOverlay.classList.remove('hidden'); });
    if(closeSidebarBtn && sidebarOverlay) {
        const fecharMenu = () => { sidebar.classList.add('-translate-x-full'); sidebarOverlay.classList.add('hidden'); };
        closeSidebarBtn.addEventListener('click', fecharMenu);
        sidebarOverlay.addEventListener('click', fecharMenu);
    }

    const initCards = () => {
        const dash = state.dashboard?.summary;
        document.getElementById('dash-pontos').textContent = dash?.data_points_count ?? state.pontos.length ?? 0;
        document.getElementById('dash-standby').textContent = dash?.standby_count ?? state.servidores.filter(s => s.role === 'STANDBY').length;
        document.getElementById('dash-host').textContent = dash?.current_host || state.servidores.find(s => s.is_self)?.node_name || state.servidores.find(s => s.role === 'HOST')?.node_name || 'Não configurado';
        document.getElementById('dash-last-reading').textContent = dash?.last_measurement_label || 'Sem leituras';
    };

    let chart;
    const initChart = () => {
        const ctx = document.getElementById('chart-nivel-rio');
        if(!ctx) return;
        chart = new Chart(ctx, {
            type: 'line',
            data: { labels: [], datasets: [{ label: 'Nível Médio (m)', data: [], borderColor: '#0284c7', backgroundColor: 'rgba(2, 132, 199, 0.1)', borderWidth: 2, fill: true, tension: 0.4, pointBackgroundColor: '#0ea5e9', pointRadius: 4 }] },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: false, ticks: { padding: 10 } }, x: { grid: { display: false } } } }
        });
    };

    const updateChart = (chartData = { labels: [], values: [] }) => {
        if (!chart) return;
        chart.data.labels = chartData.labels || [];
        chart.data.datasets[0].data = chartData.values || [];
        chart.update();
    };

    let map;
    let marker;
    let selectedLatLng = null;
    let pointMarkers = [];
    let pointBeingEdited = null;
    let pendingPointAction = null;

    const editPointModal = document.getElementById('edit-point-modal');
    const editPointForm = document.getElementById('edit-point-form');
    const editPointError = document.getElementById('edit-point-error');
    const confirmPointModal = document.getElementById('confirm-point-modal');
    const confirmPointTitle = document.getElementById('confirm-point-title');
    const confirmPointMessage = document.getElementById('confirm-point-message');
    const confirmPointError = document.getElementById('confirm-point-error');


    const clearPointMarkers = () => {
        pointMarkers.forEach((m) => map?.removeLayer(m));
        pointMarkers = [];
    };

    const renderMapPoints = () => {
        if (!map) return;
        clearPointMarkers();
        state.pontos.forEach((p) => {
            const m = L.circleMarker([p.latitude, p.longitude], { radius: 8, fillColor: p.status === 'ACTIVE' ? '#0284c7' : '#94a3b8', color: '#fff', weight: 2, opacity: p.status === 'ACTIVE' ? 1 : 0.75, fillOpacity: p.status === 'ACTIVE' ? 0.85 : 0.45 }).addTo(map);
            m.bindPopup(`<b>${escapeHtml(p.name)}</b><br>${escapeHtml(p.city_region || '')}<br>Risco: ${formatLevel(p.warning_level, p.measurement_unit)}<br>Crítico: ${formatLevel(p.critical_level, p.measurement_unit)}<br>Status: ${escapeHtml(p.status)}`);
            pointMarkers.push(m);
        });
    };

    const initMap = () => {
        const mapContainer = document.getElementById('map-container');
        if(!mapContainer) return;
        map = L.map('map-container').setView([-23.5505, -46.6333], 7);
        L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', { attribution: '&copy; OpenStreetMap &copy; CARTO' }).addTo(map);
        map.on('click', function(e) {
            selectedLatLng = e.latlng;
            document.getElementById('pt-lat').value = selectedLatLng.lat.toFixed(7);
            document.getElementById('pt-lng').value = selectedLatLng.lng.toFixed(7);
            if(marker) marker.setLatLng(selectedLatLng); else marker = L.marker(selectedLatLng).addTo(map);
        });
        loadDataPoints();
    };

    const renderTabelaPontos = () => {
        const tbody = document.getElementById('tbody-pontos');
        if(!tbody) return;
        tbody.innerHTML = '';
        if (!state.pontos.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="p-8 text-center text-slate-500">Nenhum ponto cadastrado no banco.</td></tr>';
            return;
        }
        state.pontos.forEach(p => {
            const badge = p.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-slate-100 text-slate-600';
            tbody.innerHTML += `
                <tr class="hover:bg-slate-50">
                    <td class="p-4"><div class="font-medium text-dark">${escapeHtml(p.name)}</div><div class="text-xs text-slate-500">ID: ${p.id}</div></td>
                    <td class="p-4 text-slate-600">${escapeHtml(p.city_region || '-')}</td>
                    <td class="p-4 text-xs font-mono text-slate-500">${Number(p.latitude).toFixed(4)}, ${Number(p.longitude).toFixed(4)}</td>
                    <td class="p-4"><span class="px-2 py-1 rounded ${badge} text-xs font-semibold text-center inline-block w-20">${escapeHtml(p.status)}</span></td>
                    <td class="p-4 text-xs text-slate-600"><div>Risco: ${formatLevel(p.warning_level, p.measurement_unit)}</div><div>Crítico: ${formatLevel(p.critical_level, p.measurement_unit)}</div></td>
                    <td class="p-4 text-right"><div class="inline-flex flex-wrap justify-end gap-2"><button data-action="edit-point" data-id="${p.id}" class="px-2 py-1 border rounded text-primary hover:bg-sky-50">Editar</button>${p.status === 'ACTIVE' ? `<button data-action="deactivate-point" data-id="${p.id}" class="px-2 py-1 border rounded text-red-600 hover:bg-red-50">Desativar</button>` : `<button data-action="reactivate-point" data-id="${p.id}" class="px-2 py-1 border rounded text-green-700 hover:bg-green-50">Reativar</button>`}</div></td>
                </tr>`;
        });
    };

    const openEditPointModal = (point) => {
        pointBeingEdited = point;
        if (editPointError) editPointError.textContent = '';
        document.getElementById('edit-point-name').value = point.name || '';
        document.getElementById('edit-point-latitude').value = point.latitude ?? '';
        document.getElementById('edit-point-longitude').value = point.longitude ?? '';
        document.getElementById('edit-point-type').value = point.type || 'RIVER_LEVEL';
        document.getElementById('edit-point-city').value = point.city_region || '';
        document.getElementById('edit-point-description').value = point.description || '';
        document.getElementById('edit-point-status').value = point.status || 'ACTIVE';
        document.getElementById('edit-point-normal').value = point.normal_level ?? '';
        document.getElementById('edit-point-warning').value = point.warning_level ?? '';
        document.getElementById('edit-point-critical').value = point.critical_level ?? '';
        document.getElementById('edit-point-unit').value = point.measurement_unit || 'm';
        editPointModal?.classList.remove('hidden');
        editPointModal?.classList.add('flex');
    };

    const closeEditPointModal = () => {
        pointBeingEdited = null;
        editPointModal?.classList.add('hidden');
        editPointModal?.classList.remove('flex');
    };

    const parseOptionalLevel = (value) => value === '' ? null : Number(value);

    const validatePointFormPayload = (payload) => {
        if (!payload.name) return 'Nome do ponto é obrigatório.';
        if (!Number.isFinite(payload.latitude) || !Number.isFinite(payload.longitude)) return 'Latitude e longitude são obrigatórias.';
        const levels = [payload.normal_level, payload.warning_level, payload.critical_level];
        if (levels.some((value) => value !== null && (!Number.isFinite(value) || value < 0))) return 'Os níveis devem ser valores numéricos positivos.';
        if (payload.warning_level !== null && payload.critical_level !== null && payload.critical_level <= payload.warning_level) return 'O nível crítico deve ser maior que o nível de risco.';
        if (payload.normal_level !== null && payload.warning_level !== null && payload.warning_level <= payload.normal_level) return 'O nível de risco deve ser maior que o nível normal.';
        return '';
    };

    const openConfirmPointModal = (point, action) => {
        pendingPointAction = { point, action };
        if (confirmPointError) confirmPointError.textContent = '';
        const isReactivate = action === 'reactivate';
        if (confirmPointTitle) confirmPointTitle.textContent = isReactivate ? 'Reativar ponto' : 'Desativar ponto';
        if (confirmPointMessage) confirmPointMessage.textContent = isReactivate ? 'Deseja reativar este ponto?' : 'Deseja desativar este ponto?';
        confirmPointModal?.classList.remove('hidden');
        confirmPointModal?.classList.add('flex');
    };

    const closeConfirmPointModal = () => {
        pendingPointAction = null;
        confirmPointModal?.classList.add('hidden');
        confirmPointModal?.classList.remove('flex');
    };

    const renderSelectedPointThresholds = () => {
        const select = document.getElementById('dado-ponto');
        const card = document.getElementById('dado-limites-card');
        const unitInput = document.getElementById('dado-unidade');
        const suffix = document.getElementById('dado-valor-unit-suffix');
        if (!select || !card) return;
        const selectedPoint = state.pontos.find((p) => String(p.id) === String(select.value));
        if (!selectedPoint) { card.classList.add('hidden'); return; }
        const unit = selectedPoint.measurement_unit || 'm';
        if (unitInput) unitInput.value = unit === 'm' ? 'Metros (m)' : unit;
        if (suffix) suffix.textContent = unit;
        const hasThresholds = hasValue(selectedPoint.warning_level) && hasValue(selectedPoint.critical_level);
        card.classList.remove('hidden');
        card.innerHTML = hasThresholds
            ? `<h4 class="font-bold text-dark mb-2">Limites deste ponto</h4><div class="grid grid-cols-1 sm:grid-cols-3 gap-2"><span>Normal: <b>${formatLevel(selectedPoint.normal_level, unit)}</b></span><span>Risco: <b>${formatLevel(selectedPoint.warning_level, unit)}</b></span><span>Crítico: <b>${formatLevel(selectedPoint.critical_level, unit)}</b></span></div>`
            : '<h4 class="font-bold text-dark mb-2">Limites deste ponto</h4><p>Este ponto ainda não possui limites configurados. O sistema usará valores genéricos temporários.</p>';
    };

    const populateDataPointSelect = (points) => {
        const select = document.getElementById('dado-ponto');
        if (!select) return;
        select.innerHTML = '<option value="" disabled selected>Selecione um ponto...</option>';
        points.forEach((p) => { select.innerHTML += `<option value="${p.id}">${escapeHtml(p.name)} (${escapeHtml(p.city_region || 'sem região')})</option>`; });
        renderSelectedPointThresholds();
    };

    const loadDataPoints = async () => {
        try {
            const resp = await apiFetch('/api/data-points');
            state.pontos = resp.data || [];
            renderTabelaPontos();
            renderMapPoints();
            populateDataPointSelect(state.pontos.filter((p) => p.status === 'ACTIVE'));
            initCards();
        } catch (error) {
            setFeedback('pontos-feedback', error.message, true);
            const tbody = document.getElementById('tbody-pontos');
            if (tbody) tbody.innerHTML = `<tr><td colspan="6" class="p-8 text-center text-red-600">${escapeHtml(error.message)}</td></tr>`;
        }
    };

    const loadActiveDataPoints = async () => {
        try { const resp = await apiFetch('/api/data-points?status=ACTIVE'); state.pontos = resp.data || []; populateDataPointSelect(state.pontos); }
        catch (error) { setFeedback('inserir-feedback', error.message, true); }
    };

    document.getElementById('form-cadastrar-ponto')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        if(!selectedLatLng) { setFeedback('pontos-feedback', 'Clique no mapa para selecionar latitude e longitude.', true); return; }
        const normalLevel = document.getElementById('pt-normal').value;
        const warningLevel = document.getElementById('pt-risco').value;
        const criticalLevel = document.getElementById('pt-critico').value;
        const normalNumber = normalLevel === '' ? null : Number(normalLevel);
        const warningNumber = warningLevel === '' ? null : Number(warningLevel);
        const criticalNumber = criticalLevel === '' ? null : Number(criticalLevel);
        if ((normalNumber !== null && normalNumber < 0) || (warningNumber !== null && warningNumber < 0) || (criticalNumber !== null && criticalNumber < 0)) { setFeedback('pontos-feedback', 'Os níveis devem ser valores numéricos positivos.', true); return; }
        if (warningNumber !== null && criticalNumber !== null && criticalNumber <= warningNumber) { setFeedback('pontos-feedback', 'O nível crítico deve ser maior que o nível de risco.', true); return; }
        if (normalNumber !== null && warningNumber !== null && warningNumber <= normalNumber) { setFeedback('pontos-feedback', 'O nível de risco deve ser maior que o nível normal.', true); return; }
        const payload = { name: document.getElementById('pt-nome').value.trim(), type: 'RIVER_LEVEL', latitude: selectedLatLng.lat, longitude: selectedLatLng.lng, city_region: document.getElementById('pt-cidade').value.trim(), status: 'ACTIVE', normal_level: normalNumber, warning_level: warningNumber, critical_level: criticalNumber, measurement_unit: document.getElementById('pt-unidade').value.trim() || 'm' };
        try {
            await apiFetch('/api/data-points', { method: 'POST', body: JSON.stringify(payload) });
            e.target.reset(); selectedLatLng = null; if(marker) map.removeLayer(marker); marker = null;
            document.getElementById('pt-lat').value = ''; document.getElementById('pt-lng').value = ''; document.getElementById('pt-unidade').value = 'm';
            setFeedback('pontos-feedback', 'Ponto cadastrado com sucesso.');
            await loadDataPoints(); await loadDashboard();
        } catch (error) { setFeedback('pontos-feedback', error.message, true); }
    });

    document.getElementById('tbody-pontos')?.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-action]');
        if (!btn) return;
        const point = state.pontos.find((p) => String(p.id) === String(btn.dataset.id));
        if (!point) return;
        if (btn.dataset.action === 'edit-point') openEditPointModal(point);
        if (btn.dataset.action === 'deactivate-point') openConfirmPointModal(point, 'deactivate');
        if (btn.dataset.action === 'reactivate-point') openConfirmPointModal(point, 'reactivate');
    });

    document.getElementById('close-edit-point-modal')?.addEventListener('click', closeEditPointModal);
    document.getElementById('cancel-edit-point-modal')?.addEventListener('click', closeEditPointModal);

    editPointForm?.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (!pointBeingEdited) return;
        const payload = {
            name: document.getElementById('edit-point-name').value.trim(),
            type: document.getElementById('edit-point-type').value,
            latitude: Number(document.getElementById('edit-point-latitude').value),
            longitude: Number(document.getElementById('edit-point-longitude').value),
            city_region: document.getElementById('edit-point-city').value.trim() || null,
            description: document.getElementById('edit-point-description').value.trim() || null,
            status: document.getElementById('edit-point-status').value,
            normal_level: parseOptionalLevel(document.getElementById('edit-point-normal').value),
            warning_level: parseOptionalLevel(document.getElementById('edit-point-warning').value),
            critical_level: parseOptionalLevel(document.getElementById('edit-point-critical').value),
            measurement_unit: document.getElementById('edit-point-unit').value.trim() || 'm'
        };
        const error = validatePointFormPayload(payload);
        if (error) { if (editPointError) editPointError.textContent = error; return; }
        try {
            await apiFetch(`/api/data-points/${pointBeingEdited.id}`, { method: 'PUT', body: JSON.stringify(payload) });
            closeEditPointModal();
            setFeedback('pontos-feedback', 'Ponto atualizado com sucesso.');
            await loadDataPoints();
            await loadDashboard();
        } catch (err) { if (editPointError) editPointError.textContent = err.message; }
    });

    document.getElementById('cancel-confirm-point-modal')?.addEventListener('click', closeConfirmPointModal);
    document.getElementById('close-confirm-point-modal')?.addEventListener('click', closeConfirmPointModal);
    document.getElementById('confirm-point-action')?.addEventListener('click', async () => {
        if (!pendingPointAction) return;
        const { point, action } = pendingPointAction;
        try {
            if (action === 'reactivate') {
                await apiFetch(`/api/data-points/${point.id}/reactivate`, { method: 'POST' });
                setFeedback('pontos-feedback', 'Ponto reativado com sucesso.');
            } else {
                await apiFetch(`/api/data-points/${point.id}`, { method: 'DELETE' });
                setFeedback('pontos-feedback', 'Ponto desativado com sucesso.');
            }
            closeConfirmPointModal();
            await loadDataPoints();
            await loadDashboard();
        } catch (err) { if (confirmPointError) confirmPointError.textContent = err.message; }
    });

    const renderEventQueue = () => {
        const logContainer = document.getElementById('log-fila');
        if(!logContainer) return;
        if (!state.fila.length) {
            logContainer.innerHTML = '<div class="text-slate-400">>[SYSTEM] Nenhum evento registrado no banco.</div>';
            return;
        }
        logContainer.innerHTML = state.fila.map((log) => `
            <div class="flex flex-col text-slate-300 pb-2 border-b border-slate-800/50">
                <span class="text-accent">>[${dateLabel(log.created_at)}] [${escapeHtml(log.status)}] ${escapeHtml(log.event_type)}</span>
                <span class="text-slate-500 pl-4">|- ${escapeHtml(log.message || 'sem mensagem')}</span>
                ${log.related_measurement_id ? `<span class="text-slate-500 pl-4">|- measurement_id: ${log.related_measurement_id}</span>` : ''}
            </div>`).join('');
    };

    const loadEventQueue = async () => {
        try { const resp = await apiFetch('/api/event-queue/logs?limit=30'); state.fila = resp.data || []; renderEventQueue(); }
        catch (error) { const el = document.getElementById('log-fila'); if (el) el.innerHTML = `<div class="text-red-400">>${escapeHtml(error.message)}</div>`; }
    };

    document.getElementById('btn-dado-aleatorio')?.addEventListener('click', () => {
        const selects = document.getElementById('dado-ponto');
        if(selects.options.length > 1) { selects.selectedIndex = 1 + Math.floor(Math.random() * (selects.options.length - 1)); renderSelectedPointThresholds(); }
        document.getElementById('dado-valor').value = (Math.random() * 5 + 1).toFixed(2);
        const obs = ['Nível subindo', 'Vazão normal', 'Alerta temporal', 'Chuva prevista'];
        document.getElementById('dado-obs').value = obs[Math.floor(Math.random() * obs.length)];
    });

    document.getElementById('form-inserir-dado')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const selectedPoint = state.pontos.find((p) => String(p.id) === String(document.getElementById('dado-ponto').value));
        const payload = { data_point_id: Number(document.getElementById('dado-ponto').value), measurement_type: 'RIVER_LEVEL', value: Number(document.getElementById('dado-valor').value), unit: selectedPoint?.measurement_unit || 'm', measured_at: new Date().toISOString(), observation: document.getElementById('dado-obs').value.trim() || null };
        if (!payload.data_point_id) { setFeedback('inserir-feedback', 'Selecione um ponto.', true); return; }
        try {
            const resp = await apiFetch('/api/measurements', { method: 'POST', body: JSON.stringify(payload) });
            setFeedback('inserir-feedback', resp.alert ? `Medição salva e alerta gerado: ${resp.alert.severity}` : 'Medição salva sem alerta.');
            document.getElementById('dado-valor').value = ''; document.getElementById('dado-obs').value = '';
            await Promise.all([loadEventQueue(), loadAlerts(), loadDashboard()]);
        } catch (error) { setFeedback('inserir-feedback', error.message, true); }
    });

    const renderEventosDash = () => {
        const ul = document.getElementById('dashboard-events-list');
        if(!ul) return;
        if (!state.eventosDashboard.length) { ul.innerHTML = '<li class="p-4 text-sm text-slate-500">Nenhum evento no banco.</li>'; return; }
        ul.innerHTML = '';
        state.eventosDashboard.forEach((ev) => {
            const isAlert = ev.event_type === 'ALERT_CREATED';
            const isMeasurement = String(ev.event_type || '').startsWith('MEASUREMENT');
            const icon = isAlert ? 'siren' : (isMeasurement ? 'activity' : 'info');
            const color = isAlert ? 'text-red-500 bg-red-50' : (isMeasurement ? 'text-green-600 bg-green-50' : 'text-primary bg-primary/10');
            ul.innerHTML += `<li class="p-4 flex gap-4 hover:bg-white transition-colors duration-200"><div class="w-8 h-8 rounded-full ${color} flex items-center justify-center mt-1 shrink-0"><i data-lucide="${icon}" class="w-4 h-4"></i></div><div><p class="text-sm font-medium text-dark">${escapeHtml(ev.message || ev.event_type)}</p><p class="text-xs text-slate-500 mt-1">${dateLabel(ev.created_at)}</p></div></li>`;
        });
        lucide.createIcons();
    };

    const loadDashboard = async () => {
        try {
            const data = await apiFetch('/api/dashboard/summary');
            state.dashboard = data;
            state.eventosDashboard = data.latest_events || [];
            initCards(); updateChart(data.chart); renderEventosDash();
            const alertDot = document.querySelector('[data-target="alertas"] span.absolute');
            if (alertDot) alertDot.classList.toggle('hidden', !(data.latest_alerts || []).length);
        } catch (error) {
            const ul = document.getElementById('dashboard-events-list');
            if (ul) ul.innerHTML = `<li class="p-4 text-sm text-red-600">Erro ao carregar dashboard: ${escapeHtml(error.message)}</li>`;
        }
    };

    const renderTabelaAlertas = () => {
        const tbody = document.getElementById('tbody-alertas');
        if(!tbody) return;
        tbody.innerHTML = '';
        if(!state.alertas.length) { tbody.innerHTML = '<tr><td colspan="8" class="p-8 text-center text-slate-500">Nenhum alerta ativo no banco.</td></tr>'; return; }
        state.alertas.forEach((a) => {
            const isCrit = a.severity === 'CRITICAL';
            const dotClass = isCrit ? 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.8)]' : 'bg-yellow-500 shadow-[0_0_8px_rgba(234,179,8,0.5)]';
            const badgeClass = isCrit ? 'bg-red-50 text-red-700 border border-red-200' : 'bg-yellow-50 text-yellow-700 border border-yellow-200';
            tbody.innerHTML += `<tr class="hover:bg-slate-50 transition-colors bg-white"><td class="p-4 text-center"><div class="inline-flex w-3 h-3 rounded-full ${dotClass}"></div></td><td class="p-4 font-semibold text-dark"><div>${escapeHtml(a.data_point_name)}</div><div class="text-xs text-slate-500 font-normal">${escapeHtml(a.city_region || '')}</div></td><td class="p-4 font-mono font-bold text-slate-600">${moneyNumber(a.current_value)}${escapeHtml(a.unit)}</td><td class="p-4 text-xs text-slate-600"><div>Risco: ${formatLevel(a.warning_level, a.measurement_unit || a.unit)}</div><div>Crítico: ${formatLevel(a.critical_level, a.measurement_unit || a.unit)}</div></td><td class="p-4 text-slate-600">${escapeHtml(a.message)}</td><td class="p-4 text-slate-500 text-sm whitespace-nowrap">${dateLabel(a.detected_at)}</td><td class="p-4"><span class="px-2.5 py-1 text-xs font-semibold rounded-md ${badgeClass}">${escapeHtml(a.severity)}</span></td><td class="p-4 text-right"><button data-action="resolve-alert" data-id="${a.id}" class="px-3 py-1.5 rounded bg-green-600 hover:bg-green-700 text-white text-xs font-semibold">Resolver</button></td></tr>`;
        });
    };

    const loadAlerts = async () => {
        try { const resp = await apiFetch('/api/alerts?status=ACTIVE'); state.alertas = resp.data || []; renderTabelaAlertas(); }
        catch (error) { const tbody = document.getElementById('tbody-alertas'); if (tbody) tbody.innerHTML = `<tr><td colspan="8" class="p-8 text-center text-red-600">${escapeHtml(error.message)}</td></tr>`; }
    };

    document.getElementById('dado-ponto')?.addEventListener('change', renderSelectedPointThresholds);

    document.getElementById('tbody-alertas')?.addEventListener('click', async (e) => {
        const btn = e.target.closest('[data-action="resolve-alert"]');
        if (!btn) return;
        try { await apiFetch(`/api/alerts/${btn.dataset.id}/resolve`, { method: 'POST' }); await Promise.all([loadAlerts(), loadDashboard()]); }
        catch (error) { setFeedback('inserir-feedback', error.message, true); }
    });

    /* ========================================================================== 
       SERVIDORES / CLUSTER
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

    initChart();
    fetchClusterNodes().finally(async () => {
        renderServidores();
        initCards();
        await loadDashboard();
    });
    if(document.getElementById('map-container')) setTimeout(initMap, 100);
    loadActiveDataPoints();
    loadEventQueue();
    loadAlerts();

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