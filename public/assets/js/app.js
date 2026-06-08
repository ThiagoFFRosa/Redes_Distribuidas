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
        fila: [],
        historicalChart: null,
        historicalPayload: null,
        historicalZoom: null,
        historicalPollTimer: null,
        currentHistoricalPointId: null,
        pointRecords: { point: null, source: 'all', order: 'desc', page: 1, limit: 50, includeDeleted: false, total: 0, records: [] },
        recordBeingEdited: null,
        joinRequests: [],
        syncStatusByUuid: new Map(),
        syncStatusByName: new Map(),
        ngrokStatus: null
    };

    const authHeaders = () => {
        const token = localStorage.getItem('auth_token');
        return token ? { Authorization: `Bearer ${token}` } : {};
    };

    const apiFetch = async (url, options = {}) => {
        const response = await fetch(url, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...authHeaders(),
                ...(options.headers || {})
            }
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok) {
            const error = new Error(data.message || `Falha HTTP ${response.status}`);
            error.status = response.status;
            throw error;
        }
        return data;
    };

    const escapeHtml = (value = '') => String(value).replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[char]));
    const moneyNumber = (value) => Number(value || 0).toFixed(2);
    const hasValue = (value) => value !== null && value !== undefined && value !== '';
    const isValidLatitude = (value) => { const n = Number(value); return value !== null && value !== undefined && String(value).trim() !== '' && Number.isFinite(n) && n >= -90 && n <= 90; };
    const isValidLongitude = (value) => { const n = Number(value); return value !== null && value !== undefined && String(value).trim() !== '' && Number.isFinite(n) && n >= -180 && n <= 180; };
    const hasValidCoordinates = (point = {}) => isValidLatitude(point.latitude) && isValidLongitude(point.longitude);
    const parseOptionalCoordinate = (value) => value === null || value === undefined || String(value).trim() === '' ? null : Number(value);
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
            if(targetId === 'visao-geral') { loadDashboard(); loadJoinRequests(); }
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
        const statusEl = document.getElementById('map-location-warning');
        const validMapPoints = state.pontos.filter(hasValidCoordinates);
        state.pontos.forEach((p) => {
            if (!hasValidCoordinates(p) && window.location.hostname === 'localhost') console.warn('Ponto ignorado no mapa por coordenadas inválidas:', p);
        });
        validMapPoints.forEach((p) => {
            const lat = Number(p.latitude);
            const lng = Number(p.longitude);
            const m = L.circleMarker([lat, lng], { radius: 8, fillColor: p.status === 'ACTIVE' ? '#0284c7' : '#94a3b8', color: '#fff', weight: 2, opacity: p.status === 'ACTIVE' ? 1 : 0.75, fillOpacity: p.status === 'ACTIVE' ? 0.85 : 0.45 }).addTo(map);
            m.bindPopup(`<b>${escapeHtml(p.name)}</b><br>${escapeHtml(p.city_region || '')}<br>Risco: ${formatLevel(p.warning_level, p.measurement_unit)}<br>Crítico: ${formatLevel(p.critical_level, p.measurement_unit)}<br>Status: ${escapeHtml(p.status)}`);
            pointMarkers.push(m);
        });
        if (validMapPoints.length > 0) {
            if (statusEl) statusEl.textContent = '';
        } else {
            map.setView([-23.5505, -46.6333], 7);
            if (statusEl) statusEl.textContent = 'Nenhum ponto com coordenadas válidas para exibir no mapa.';
        }
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
            const validLocation = hasValidCoordinates(p);
            const badge = p.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-slate-100 text-slate-600';
            const reviewBadge = !validLocation || p.location_status === 'NEEDS_REVIEW' ? '<span class="ml-2 px-2 py-1 rounded bg-yellow-100 text-yellow-800 text-[11px] font-semibold">Corrigir localização</span>' : '';
            tbody.innerHTML += `
                <tr class="hover:bg-slate-50">
                    <td class="p-4"><div class="font-medium text-dark">${escapeHtml(p.name)}</div><div class="text-xs text-slate-500">ID: ${p.id}</div></td>
                    <td class="p-4 text-slate-600">${validLocation ? escapeHtml(p.city_region || '-') : `<span class="text-yellow-700 font-medium">Sem coordenadas — Corrigir</span><div class="text-xs text-slate-500">${escapeHtml(p.location_error || 'Localização pendente')}</div>`}</td>
                    <td class="p-4 text-xs font-mono text-slate-500">${validLocation ? `${Number(p.latitude).toFixed(4)}, ${Number(p.longitude).toFixed(4)}` : '-'}</td>
                    <td class="p-4"><span class="px-2 py-1 rounded ${badge} text-xs font-semibold text-center inline-block w-20">${escapeHtml(p.status)}</span>${reviewBadge}</td>
                    <td class="p-4 text-xs text-slate-600"><div>Risco: ${formatLevel(p.warning_level, p.measurement_unit)}</div><div>Crítico: ${formatLevel(p.critical_level, p.measurement_unit)}</div></td>
                    <td class="p-4 text-right"><div class="inline-flex flex-wrap justify-end gap-2"><button data-action="historical-point" data-id="${p.id}" class="px-2 py-1 border rounded text-secondary hover:bg-teal-50">Histórico</button><button data-action="data-records-point" data-id="${p.id}" class="px-2 py-1 border rounded text-indigo-600 hover:bg-indigo-50">Dados</button><button data-action="edit-point" data-id="${p.id}" class="px-2 py-1 border rounded text-primary hover:bg-sky-50">${validLocation ? 'Editar' : 'Corrigir localização'}</button>${p.status === 'ACTIVE' ? `<button data-action="deactivate-point" data-id="${p.id}" class="px-2 py-1 border rounded text-red-600 hover:bg-red-50">Desativar</button>` : `<button data-action="reactivate-point" data-id="${p.id}" class="px-2 py-1 border rounded text-green-700 hover:bg-green-50">Reativar</button>`}</div></td>
                </tr>`;
        });
    };



    const closePointRecordsModal = () => {
        document.getElementById('point-records-modal')?.classList.add('hidden');
        document.getElementById('point-records-modal')?.classList.remove('flex');
        state.pointRecords.point = null;
    };

    const closeEditRecordModal = () => {
        document.getElementById('edit-record-modal')?.classList.add('hidden');
        document.getElementById('edit-record-modal')?.classList.remove('flex');
        state.recordBeingEdited = null;
        const err = document.getElementById('edit-record-error');
        if (err) err.textContent = '';
    };

    const formatRecordDateInput = (value) => {
        if (!value) return '';
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return '';
        return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
    };

    const renderPointRecords = () => {
        const tbody = document.getElementById('tbody-point-records');
        const info = document.getElementById('point-records-page-info');
        if (!tbody) return;
        const { records, page, limit, total } = state.pointRecords;
        if (info) info.textContent = `Página ${page} • ${records.length} de ${total} registros`;
        if (!records.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="p-6 text-center text-slate-500">Nenhum registro encontrado.</td></tr>';
            return;
        }
        tbody.innerHTML = records.map((r) => {
            const sourceBadge = r.record_type === 'SITE' ? 'bg-sky-100 text-sky-700' : 'bg-amber-100 text-amber-700';
            const correctedBadge = r.corrected_at ? `<span title="Valor original: ${escapeHtml(r.original_value ?? '-')} • Data original: ${dateLabel(r.original_measured_at)} • Corrigido em: ${dateLabel(r.corrected_at)} • Node: ${escapeHtml(r.corrected_by_node_uuid || '-')}" class="ml-1 px-2 py-0.5 rounded bg-purple-100 text-purple-700 text-[11px] font-semibold">Corrigido</span>` : '';
            const deletedBadge = r.deleted_at ? `<span class="ml-1 px-2 py-0.5 rounded bg-red-100 text-red-700 text-[11px] font-semibold">Excluído</span>` : '';
            return `<tr class="hover:bg-slate-50">
                <td class="p-3 whitespace-nowrap text-slate-700">${dateLabel(r.date)}</td>
                <td class="p-3 font-mono font-semibold">${hasValue(r.value) ? Number(r.value).toFixed(3) : '-'}</td>
                <td class="p-3">${escapeHtml(r.unit || 'm')}</td>
                <td class="p-3"><span class="px-2 py-1 rounded ${sourceBadge} text-xs font-bold">${r.record_type}</span>${correctedBadge}${deletedBadge}</td>
                <td class="p-3 text-slate-600">${escapeHtml(r.source_label || '-')}</td>
                <td class="p-3 text-xs text-slate-500 whitespace-nowrap">${dateLabel(r.created_at)}</td>
                <td class="p-3 text-xs text-slate-500 whitespace-nowrap">${dateLabel(r.updated_at)}</td>
                <td class="p-3 text-right"><div class="inline-flex gap-2"><button data-action="edit-record" data-uuid="${r.uuid}" data-type="${r.record_type}" class="px-2 py-1 border rounded text-primary hover:bg-sky-50" ${r.deleted_at ? 'disabled title="Restaure antes de editar"' : ''}>Editar</button>${r.deleted_at ? `<button data-action="restore-record" data-uuid="${r.uuid}" data-type="${r.record_type}" class="px-2 py-1 border rounded text-green-700 hover:bg-green-50">Restaurar</button>` : `<button data-action="delete-record" data-uuid="${r.uuid}" data-type="${r.record_type}" class="px-2 py-1 border rounded text-red-600 hover:bg-red-50">Excluir</button>`}</div></td>
            </tr>`;
        }).join('');
    };

    const loadPointRecords = async () => {
        const cfg = state.pointRecords;
        if (!cfg.point) return;
        const qs = new URLSearchParams({ source: cfg.source, page: String(cfg.page), limit: String(cfg.limit), order: cfg.order });
        if (cfg.includeDeleted) qs.set('include_deleted', 'true');
        const from = document.getElementById('point-records-from')?.value;
        const to = document.getElementById('point-records-to')?.value;
        if (from) qs.set('from', from);
        if (to) qs.set('to', to);
        const data = await apiFetch(`/api/data-points/${cfg.point.id}/records?${qs}`);
        cfg.total = data.pagination?.total || 0;
        cfg.records = data.records || [];
        renderPointRecords();
    };

    const openPointRecordsModal = async (point) => {
        state.pointRecords = { point, source: 'all', order: 'desc', page: 1, limit: 50, includeDeleted: false, total: 0, records: [] };
        document.getElementById('point-records-title').textContent = `Dados do ponto: ${point.name}`;
        document.getElementById('point-records-source').value = 'all';
        document.getElementById('point-records-order').value = 'desc';
        document.getElementById('point-records-include-deleted').checked = false;
        document.getElementById('point-records-modal')?.classList.remove('hidden');
        document.getElementById('point-records-modal')?.classList.add('flex');
        try { await loadPointRecords(); } catch (error) { setFeedback('point-records-feedback', error.message, true); }
    };

    const openEditRecordModal = (record) => {
        state.recordBeingEdited = record;
        document.getElementById('edit-record-title').textContent = `Editar dado ${record.record_type}`;
        document.getElementById('edit-record-date').value = formatRecordDateInput(record.date);
        document.getElementById('edit-record-value').value = hasValue(record.value) ? record.value : '';
        document.getElementById('edit-record-unit').value = record.unit || 'm';
        document.getElementById('edit-record-reason').value = record.correction_reason || record.observation || '';
        document.getElementById('edit-record-csv-warning').classList.toggle('hidden', record.record_type !== 'CSV');
        document.getElementById('edit-record-modal')?.classList.remove('hidden');
        document.getElementById('edit-record-modal')?.classList.add('flex');
    };

    const trendLabel = (trend) => ({ RISING: 'Subindo', FALLING: 'Baixando', STABLE: 'Estável', UNKNOWN: 'Desconhecida' }[trend] || trend || '-');
    const seasonalLabel = (status) => ({ MUITO_ABAIXO_DO_NORMAL: 'Muito abaixo do normal', ABAIXO_DO_NORMAL: 'Abaixo do normal', DENTRO_DO_NORMAL: 'Dentro do normal', ACIMA_DO_NORMAL: 'Acima do normal', MUITO_ACIMA_DO_NORMAL: 'Muito acima do normal', INSUFFICIENT_DATA: 'Dados insuficientes' }[status] || status || 'Dados insuficientes');
    const riskLabel = (risk) => ({ SEM_RISCO: 'Sem risco', ATENCAO: 'Atenção', RISCO_CRITICO: 'Risco crítico', INDEFINIDO: 'Indefinido' }[risk] || risk || 'Indefinido');
    const formatMetric = (value, suffix = '') => hasValue(value) ? `${Number(value).toFixed(2)}${suffix}` : '-';

    const stopHistoricalPolling = () => {
        if (state.historicalPollTimer) clearTimeout(state.historicalPollTimer);
        state.historicalPollTimer = null;
    };

    const closeHistoricalModal = () => {
        stopHistoricalPolling();
        document.getElementById('historical-modal')?.classList.add('hidden');
        document.getElementById('historical-modal')?.classList.remove('flex');
    };

    const isValidChartPayload = (payload) => Boolean(
        payload
        && Array.isArray(payload.labels)
        && Array.isArray(payload.datasets)
        && payload.datasets.length > 0
        && Array.isArray(payload.datasets[0].data)
    );

    const normalizeChartPayload = (payload) => {
        if (!payload) return null;
        if (Array.isArray(payload.datasets)) return payload;
        if (Array.isArray(payload.labels) && Array.isArray(payload.values)) return {
            ...payload,
            datasets: [{ label: `Cota histórica (${payload.unit || 'm'})`, data: payload.values }]
        };
        return payload;
    };

    const setHistoricalCanvasVisible = (visible, message = '') => {
        const canvas = document.getElementById('historical-chart');
        const container = canvas?.parentElement;
        if (!canvas || !container) return;
        let empty = document.getElementById('historical-chart-empty');
        if (!empty) {
            empty = document.createElement('div');
            empty.id = 'historical-chart-empty';
            empty.className = 'hidden h-full min-h-[320px] rounded-lg bg-slate-50 border border-dashed border-slate-200 flex items-center justify-center p-6 text-slate-500 text-center';
            container.appendChild(empty);
        }
        if (visible) {
            empty.classList.add('hidden');
            canvas.classList.remove('hidden');
        } else {
            empty.textContent = message || 'Dados ainda não disponíveis.';
            empty.classList.remove('hidden');
            canvas.classList.add('hidden');
        }
    };

    const historicalReferenceLinePlugin = {
        id: 'historicalReferenceLines',
        afterDatasetsDraw(chart) {
            const lines = chart.options?.plugins?.historicalReferenceLines?.lines || [];
            const yScale = chart.scales?.y;
            if (!lines.length || !yScale) return;
            const { ctx, chartArea } = chart;
            ctx.save();
            lines.forEach((line) => {
                const y = yScale.getPixelForValue(line.value);
                if (Number.isNaN(y) || y < chartArea.top || y > chartArea.bottom) return;
                ctx.beginPath();
                ctx.setLineDash([6, 5]);
                ctx.strokeStyle = line.color || '#64748b';
                ctx.lineWidth = 1;
                ctx.moveTo(chartArea.left, y);
                ctx.lineTo(chartArea.right, y);
                ctx.stroke();
                ctx.setLineDash([]);
                ctx.fillStyle = line.color || '#64748b';
                ctx.font = '11px sans-serif';
                ctx.fillText(line.label, chartArea.left + 8, y - 4);
            });
            ctx.restore();
        }
    };
    if (window.Chart) { try { Chart.register(historicalReferenceLinePlugin); } catch (_error) {} }

    const sliceHistoricalPayload = (payload, zoom = null) => {
        const normalized = normalizeChartPayload(payload);
        if (!normalized?.labels?.length || !zoom) return normalized;
        const start = Math.max(0, zoom.start);
        const end = Math.min(normalized.labels.length - 1, zoom.end);
        return {
            ...normalized,
            labels: normalized.labels.slice(start, end + 1),
            datasets: (normalized.datasets || []).map((dataset) => ({ ...dataset, data: (dataset.data || []).slice(start, end + 1) })),
            points: (normalized.points || []).slice(start, Math.min(end + 1, normalized.points.length))
        };
    };

    const applyHistoricalZoom = (zoom) => {
        if (!state.historicalPayload) return;
        const labels = state.historicalPayload.labels || [];
        if (!labels.length) return;
        if (!zoom) {
            state.historicalZoom = null;
        } else {
            const size = Math.max(1, Math.round(zoom.end - zoom.start + 1));
            let start = Math.round(zoom.start);
            let end = Math.round(zoom.end);
            if (start < 0) { end += -start; start = 0; }
            if (end > labels.length - 1) { start -= end - (labels.length - 1); end = labels.length - 1; }
            start = Math.max(0, start);
            end = Math.min(labels.length - 1, Math.max(start, end));
            if (end - start + 1 < Math.min(size, labels.length)) end = Math.min(labels.length - 1, start + size - 1);
            state.historicalZoom = { start, end };
        }
        renderHistoricalChart(state.historicalPayload, state.historicalZoom);
    };

    const applyHistoricalRange = (days) => {
        const payload = state.historicalPayload;
        const labels = payload?.labels || [];
        if (!labels.length || days === 'all') return applyHistoricalZoom(null);
        const lastTime = new Date(labels[labels.length - 1]).getTime();
        const minTime = lastTime - Number(days) * 86400000;
        const start = labels.findIndex((label) => new Date(label).getTime() >= minTime);
        applyHistoricalZoom({ start: start >= 0 ? start : 0, end: labels.length - 1 });
    };

    const renderHistoricalChart = (payload, zoom = null) => {
        const canvas = document.getElementById('historical-chart');
        if (!canvas) return;
        if (state.historicalChart) state.historicalChart.destroy();
        const normalized = sliceHistoricalPayload(payload, zoom);
        const points = normalized?.points || [];
        let dragStart = null;
        const datasets = (normalized?.datasets || []).map((dataset) => ({
            ...dataset,
            borderColor: dataset.borderColor || '#0d9488',
            backgroundColor: dataset.backgroundColor || 'rgba(13,148,136,0.12)',
            fill: dataset.fill ?? false,
            tension: dataset.tension ?? 0.25,
            pointRadius: dataset.pointRadius ?? 0,
            spanGaps: true
        }));
        state.historicalChart = new Chart(canvas, {
            type: 'line',
            data: { labels: normalized?.labels || [], datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: true, labels: { usePointStyle: true } },
                    historicalReferenceLines: { lines: normalized?.reference_lines || [] },
                    tooltip: {
                        callbacks: {
                            label: (context) => {
                                const point = points[context.dataIndex] || {};
                                const value = context.parsed?.y ?? context.raw;
                                if (value == null) return `${context.dataset.label}: sem valor`;
                                const unit = point.unit || normalized?.unit || 'm';
                                const source = point.source === 'SITE' ? 'Site' : point.source === 'FORECAST' ? 'Previsão' : (point.source || 'CSV');
                                return `${context.dataset.label}: ${Number(value).toFixed(2)} ${unit}${source ? ` · ${source}` : ''}`;
                            },
                            afterBody: () => {
                                const seasonal = normalized?.seasonal_analysis;
                                return seasonal?.available ? [`Diferença p/ média sazonal: ${formatMetric(seasonal.difference_from_mean, normalized?.unit || 'm')}`] : [];
                            }
                        }
                    }
                },
                scales: { y: { title: { display: true, text: normalized?.unit || 'm' } }, x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } } }
            }
        });
        canvas.onmousedown = (event) => { dragStart = event.offsetX; };
        canvas.onmouseup = (event) => {
            if (dragStart == null || !state.historicalPayload?.labels?.length) return;
            const chart = state.historicalChart;
            const xScale = chart.scales.x;
            const from = xScale.getValueForPixel(Math.min(dragStart, event.offsetX));
            const to = xScale.getValueForPixel(Math.max(dragStart, event.offsetX));
            if (event.shiftKey && state.historicalZoom) {
                const delta = Math.round((dragStart - event.offsetX) / Math.max(1, xScale.width) * (state.historicalZoom.end - state.historicalZoom.start));
                return applyHistoricalZoom({ start: state.historicalZoom.start + delta, end: state.historicalZoom.end + delta });
            }
            dragStart = null;
            if (Math.abs(to - from) >= 2) {
                const baseStart = state.historicalZoom?.start || 0;
                applyHistoricalZoom({ start: baseStart + Math.floor(from), end: baseStart + Math.ceil(to) });
            }
        };
        canvas.onwheel = (event) => {
            event.preventDefault();
            const labels = state.historicalPayload?.labels || [];
            if (labels.length < 3) return;
            const current = state.historicalZoom || { start: 0, end: labels.length - 1 };
            const range = current.end - current.start + 1;
            const direction = event.deltaY > 0 ? 1 : -1;
            const nextRange = Math.max(3, Math.min(labels.length, Math.round(range * (direction > 0 ? 1.25 : 0.8))));
            const center = Math.round((current.start + current.end) / 2);
            applyHistoricalZoom({ start: center - Math.floor(nextRange / 2), end: center + Math.ceil(nextRange / 2) });
        };
    };

    const scheduleHistoricalPolling = (pointId, status) => {
        stopHistoricalPolling();
        if (!['PENDING', 'PROCESSING', 'WAITING_CACHE_SYNC'].includes(status)) return;
        state.historicalPollTimer = setTimeout(async () => {
            if (String(state.currentHistoricalPointId) !== String(pointId)) return;
            try { await loadHistoricalChart(pointId); }
            catch (error) { document.getElementById('historical-message').textContent = error.message; }
        }, 2500);
    };

    const renderHistoricalSummary = (summary = {}, status = 'PROCESSING', hasCache = false, seasonal = {}, forecast = {}) => {
        const waiting = ['PROCESSING', 'WAITING_CACHE_SYNC'].includes(status) && !hasCache;
        const period = waiting ? 'Dados ainda não disponíveis' : `${summary.date_start || '-'} a ${summary.date_end || '-'}`;
        const seasonalAvailable = seasonal?.available;
        const forecastAvailable = forecast?.available;
        document.getElementById('historical-summary').innerHTML = `
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Período</span><strong>${escapeHtml(period)}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Medições</span><strong>${waiting ? 'Aguardando cache' : (summary.points_count ?? summary.total_measurements ?? '-')}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Mínimo</span><strong>${waiting ? '-' : formatMetric(summary.min ?? summary.min_value, 'm')}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Máximo</span><strong>${waiting ? '-' : formatMetric(summary.max ?? summary.max_value, 'm')}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Média</span><strong>${waiting ? '-' : formatMetric(summary.avg ?? summary.average_value, 'm')}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Tendência</span><strong>${waiting ? '-' : escapeHtml(trendLabel(summary.trend))}</strong></div>
            <div class="rounded-xl bg-blue-50 p-3 lg:col-span-2"><span class="block text-blue-700">Comparação sazonal</span><strong>${waiting ? '-' : escapeHtml(seasonalLabel(seasonal?.status))}</strong><small class="block text-slate-500 mt-1">${seasonalAvailable ? `Percentil ${Number(seasonal.percentile).toFixed(0)}%` : 'Dados insuficientes para análise sazonal'}</small></div>
            <div class="rounded-xl bg-blue-50 p-3"><span class="block text-blue-700">Faixa histórica da época</span><strong>${seasonalAvailable ? `${formatMetric(seasonal.historical_range_min, 'm')} a ${formatMetric(seasonal.historical_range_max, 'm')}` : '-'}</strong></div>
            <div class="rounded-xl bg-blue-50 p-3"><span class="block text-blue-700">Desvio da média sazonal</span><strong>${seasonalAvailable ? formatMetric(seasonal.difference_from_mean, 'm') : '-'}</strong></div>
            <div class="rounded-xl bg-blue-50 p-3"><span class="block text-blue-700">Oscilação recente</span><strong>${seasonalAvailable ? formatMetric(seasonal.recent_amplitude, 'm') : '-'}</strong></div>
            <div class="rounded-xl bg-purple-50 p-3 lg:col-span-2"><span class="block text-purple-700">Previsão</span><strong>${forecastAvailable ? `${escapeHtml(trendLabel(forecast.trend))} · ${formatMetric(forecast.predicted_value, 'm')}` : 'Dados insuficientes para previsão'}</strong><small class="block text-slate-500 mt-1">${forecastAvailable ? `${forecast.horizon_hours}h · variação ${formatMetric(forecast.predicted_change, 'm')} · confiança ${escapeHtml(String(forecast.confidence || '').toLowerCase())}` : ''}</small></div>
            <div class="rounded-xl bg-amber-50 p-3"><span class="block text-amber-700">Risco projetado</span><strong>${forecastAvailable ? escapeHtml(riskLabel(forecast.risk_projection)) : '-'}</strong></div>`;
    };

    const loadHistoricalChart = async (pointId) => {
        const data = await apiFetch(`/api/data-points/${pointId}/historical-chart`);
        const point = data.data_point || state.pontos.find((p) => String(p.id) === String(pointId));
        const responseStatus = data.status || data.job?.status || 'PROCESSING';
        const cache = data.cache?.available ? data.cache : data.chart;
        const payload = normalizeChartPayload(cache?.data || cache?.payload);
        const summary = cache?.summary || {};
        const seasonal = cache?.seasonal_analysis || payload?.seasonal_analysis || {};
        const forecast = cache?.forecast || payload?.forecast || {};
        const hasRenderableCache = isValidChartPayload(payload);
        const status = hasRenderableCache && data.cache?.available ? (responseStatus === 'STALE' || cache?.stale ? 'STALE' : 'READY') : responseStatus;
        document.getElementById('historical-title').textContent = `Histórico do ponto: ${point?.name || pointId}`;
        document.getElementById('historical-message').textContent = status === 'READY' && responseStatus === 'WAITING_CACHE_SYNC'
            ? 'Cache local disponível. Gráfico pronto.'
            : status === 'STALE'
                ? (data.message || 'Existem medições novas. Atualize o gráfico.')
                : (data.message || '');
        renderHistoricalSummary(summary, status, hasRenderableCache, seasonal, forecast);
        state.historicalPayload = payload;
        state.historicalZoom = null;
        document.getElementById('historical-job').innerHTML = data.job ? `Node: <strong>${escapeHtml(data.job.assigned_to || data.job.assigned_node_name || '-')}</strong> · Status do job: <strong>${escapeHtml(data.job.status || '-')}</strong> · Status do cache: <strong>${escapeHtml(status)}</strong> · Progresso: <strong>${data.job.progress_percent || 0}%</strong> · Tempo estimado: <strong>${data.job.estimated_seconds || '-'}s</strong>` : `Status do cache: <strong>${escapeHtml(status)}</strong>`;

        if ((status === 'READY' || status === 'STALE') && hasRenderableCache) {
            setHistoricalCanvasVisible(true);
            renderHistoricalChart(payload);
        } else {
            if (state.historicalChart) { state.historicalChart.destroy(); state.historicalChart = null; }
            const emptyMessage = status === 'NO_DATA'
                ? 'Este ponto ainda não possui dados históricos ou medições cadastradas.'
                : status === 'FAILED'
                    ? (data.message || 'Falha ao gerar gráfico histórico.')
                    : status === 'CACHE_MISSING_LOCAL'
                        ? (data.message || 'Job local concluído, mas chart_cache não foi encontrado.')
                        : status === 'WAITING_CACHE_SYNC'
                            ? (data.message || 'Gráfico gerado no node responsável. Aguardando cache chegar neste servidor.')
                            : 'Processando / aguardando cache do gráfico.';
            setHistoricalCanvasVisible(false, emptyMessage);
        }
        scheduleHistoricalPolling(pointId, status);
    };

    const openHistoricalModal = async (point) => {
        stopHistoricalPolling();
        state.currentHistoricalPointId = point.id;
        document.getElementById('historical-modal')?.classList.remove('hidden');
        document.getElementById('historical-modal')?.classList.add('flex');
        document.getElementById('historical-title').textContent = `Histórico do ponto: ${point.name}`;
        document.getElementById('historical-message').textContent = 'Carregando gráfico histórico...';
        try { await loadHistoricalChart(point.id); }
        catch (error) { document.getElementById('historical-message').textContent = error.message; }
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
        if (payload.latitude !== null && !isValidLatitude(payload.latitude)) return 'Latitude inválida. Use um número entre -90 e 90.';
        if (payload.longitude !== null && !isValidLongitude(payload.longitude)) return 'Longitude inválida. Use um número entre -180 e 180.';
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
        const histSelect = document.getElementById('hist-data-point');
        if (histSelect) {
            histSelect.innerHTML = '<option value="">Criar ponto automaticamente</option>';
            points.forEach((p) => { histSelect.innerHTML += `<option value="${p.id}">${escapeHtml(p.name)} (${escapeHtml(p.city_region || 'sem região')})</option>`; });
        }
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
        if (btn.dataset.action === 'historical-point') openHistoricalModal(point);
        if (btn.dataset.action === 'data-records-point') openPointRecordsModal(point);
        if (btn.dataset.action === 'edit-point') openEditPointModal(point);
        if (btn.dataset.action === 'deactivate-point') openConfirmPointModal(point, 'deactivate');
        if (btn.dataset.action === 'reactivate-point') openConfirmPointModal(point, 'reactivate');
    });


    document.getElementById('close-point-records-modal')?.addEventListener('click', closePointRecordsModal);
    document.getElementById('point-records-source')?.addEventListener('change', async (e) => { state.pointRecords.source = e.target.value; state.pointRecords.page = 1; await loadPointRecords().catch((error) => setFeedback('point-records-feedback', error.message, true)); });
    document.getElementById('point-records-order')?.addEventListener('change', async (e) => { state.pointRecords.order = e.target.value; await loadPointRecords().catch((error) => setFeedback('point-records-feedback', error.message, true)); });
    document.getElementById('point-records-include-deleted')?.addEventListener('change', async (e) => { state.pointRecords.includeDeleted = e.target.checked; state.pointRecords.page = 1; await loadPointRecords().catch((error) => setFeedback('point-records-feedback', error.message, true)); });
    document.getElementById('apply-point-records-filters')?.addEventListener('click', async () => { state.pointRecords.page = 1; await loadPointRecords().catch((error) => setFeedback('point-records-feedback', error.message, true)); });
    document.getElementById('point-records-prev')?.addEventListener('click', async () => { if (state.pointRecords.page > 1) { state.pointRecords.page -= 1; await loadPointRecords(); } });
    document.getElementById('point-records-next')?.addEventListener('click', async () => { if (state.pointRecords.page * state.pointRecords.limit < state.pointRecords.total) { state.pointRecords.page += 1; await loadPointRecords(); } });
    document.getElementById('tbody-point-records')?.addEventListener('click', async (e) => {
        const btn = e.target.closest('[data-action]');
        if (!btn || !state.pointRecords.point) return;
        const record = state.pointRecords.records.find((item) => item.uuid === btn.dataset.uuid && item.record_type === btn.dataset.type);
        if (!record) return;
        const sourcePath = record.record_type === 'SITE' ? 'site' : 'csv';
        if (btn.dataset.action === 'edit-record') return openEditRecordModal(record);
        if (btn.dataset.action === 'delete-record') {
            if (!confirm('Tem certeza que deseja excluir este registro? A exclusão será replicada para as outras máquinas.')) return;
            await apiFetch(`/api/data-points/${state.pointRecords.point.id}/records/${sourcePath}/${record.uuid}`, { method: 'DELETE' });
        }
        if (btn.dataset.action === 'restore-record') {
            await apiFetch(`/api/data-points/${state.pointRecords.point.id}/records/${sourcePath}/${record.uuid}/restore`, { method: 'POST' });
        }
        setFeedback('point-records-feedback', 'Registro atualizado. O gráfico foi marcado como desatualizado.');
        await loadPointRecords();
    });
    document.getElementById('close-edit-record-modal')?.addEventListener('click', closeEditRecordModal);
    document.getElementById('cancel-edit-record-modal')?.addEventListener('click', closeEditRecordModal);
    document.getElementById('edit-record-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const record = state.recordBeingEdited;
        const point = state.pointRecords.point;
        if (!record || !point) return;
        const payload = {
            measured_at: document.getElementById('edit-record-date').value,
            value: Number(document.getElementById('edit-record-value').value),
            unit: document.getElementById('edit-record-unit').value.trim() || 'm',
            observation: document.getElementById('edit-record-reason').value.trim(),
            correction_reason: document.getElementById('edit-record-reason').value.trim()
        };
        const err = document.getElementById('edit-record-error');
        try {
            const sourcePath = record.record_type === 'SITE' ? 'site' : 'csv';
            await apiFetch(`/api/data-points/${point.id}/records/${sourcePath}/${record.uuid}`, { method: 'PUT', body: JSON.stringify(payload) });
            closeEditRecordModal();
            setFeedback('point-records-feedback', 'Registro salvo. Os dados foram alterados. Atualize o gráfico.');
            await loadPointRecords();
        } catch (error) { if (err) err.textContent = error.message; }
    });

    document.getElementById('close-edit-point-modal')?.addEventListener('click', closeEditPointModal);
    document.getElementById('cancel-edit-point-modal')?.addEventListener('click', closeEditPointModal);

    editPointForm?.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (!pointBeingEdited) return;
        const payload = {
            name: document.getElementById('edit-point-name').value.trim(),
            type: document.getElementById('edit-point-type').value,
            latitude: parseOptionalCoordinate(document.getElementById('edit-point-latitude').value),
            longitude: parseOptionalCoordinate(document.getElementById('edit-point-longitude').value),
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
    const applySelfSuggestions = (suggestions = {}) => {
        const setIfEmpty = (id, value) => {
            const el = document.getElementById(id);
            if (el && !el.value && value !== undefined && value !== null) el.value = value;
        };
        setIfEmpty('self-node-name', suggestions.node_name || 'Minipc');
        setIfEmpty('self-node-ip', suggestions.tailscale_ip || '');
        setIfEmpty('self-port', suggestions.port || 3000);
        setIfEmpty('self-public-url', suggestions.public_url || '');
        setIfEmpty('self-power-score', suggestions.power_score ?? 5);
        const roleEl = document.getElementById('self-role');
        if (roleEl && (!roleEl.value || roleEl.value === 'UNKNOWN')) roleEl.value = suggestions.role || 'STANDBY';
    };
    const openSelfModal = (suggestions = null) => { if (suggestions) applySelfSuggestions(suggestions); if (selfModal) selfModal.classList.remove('hidden'); if (selfModal) selfModal.classList.add('flex'); };
    const closeSelfModal = () => { if (selfModal) selfModal.classList.add('hidden'); if (selfModal) selfModal.classList.remove('flex'); };
    const addModal = document.getElementById('add-server-modal');
    const openAddModal = () => { if (addModal) { addModal.classList.remove('hidden'); addModal.classList.add('flex'); } };
    const closeAddModal = () => { if (addModal) { addModal.classList.add('hidden'); addModal.classList.remove('flex'); } };

    const fetchClusterNodes = async () => {
        try {
            const selfData = await apiFetch('/api/cluster/self');
            if (!selfData.configured) {
                openSelfModal(selfData.suggestions || {});
                state.servidores = [];
                state.syncStatusByUuid = new Map();
                state.syncStatusByName = new Map();
                return;
            }
            closeSelfModal();
            const [nodesData, syncData, ngrokData] = await Promise.all([
                apiFetch('/api/cluster/nodes'),
                apiFetch('/api/sync/status').catch(() => ({ nodes: [] })),
                apiFetch('/api/cluster/ngrok/status').catch(() => null)
            ]);
            state.servidores = nodesData.nodes || [];
            state.ngrokStatus = ngrokData;
            state.syncStatusByUuid = new Map((syncData.nodes || []).map((node) => [node.node_uuid, node]));
            state.syncStatusByName = new Map((syncData.nodes || []).map((node) => [node.node_name, node]));
        } catch (error) {
            if (error.status === 401) { window.location.href = '/login'; return; }
            throw error;
        }
    };

    const renderNgrokStatus = () => {
        const statusEl = document.getElementById('ngrok-status-value');
        if (!statusEl) return;
        const ngrok = state.ngrokStatus || {};
        document.getElementById('ngrok-status-value').textContent = ngrok.ngrok_status || (ngrok.ngrok_online ? 'ONLINE' : 'OFFLINE');
        document.getElementById('ngrok-owner-value').textContent = ngrok.owner_node_name || '-';
        document.getElementById('ngrok-url-value').textContent = ngrok.public_url || '-';
        document.getElementById('ngrok-last-check-value').textContent = ngrok.last_checked_at ? dateLabel(ngrok.last_checked_at) : '-';
    };

    const renderServidores = () => {
        const tbody = document.getElementById('tbody-servidores');
        if(!tbody) return;
        renderNgrokStatus();

        tbody.innerHTML = '';
        state.servidores.forEach(s => {
            const isOnline = s.status === 'ONLINE';
            const isNgrokOwner = s.node_uuid && state.ngrokStatus?.owner_node_uuid === s.node_uuid && state.ngrokStatus?.ngrok_online;
            const roleBadge = `<span class="px-2.5 py-1 text-xs font-semibold rounded-md bg-slate-100 text-slate-700">${escapeHtml(s.role)}</span>`;
            const statusInd = `<div class="flex items-center gap-1.5 text-sm font-medium ${isOnline ? 'text-green-600':'text-red-600'}"><span class="w-2 h-2 rounded-full ${isOnline ? 'bg-green-500':'bg-red-500'}"></span>${escapeHtml(s.status)}</div>`;
            const sync = state.syncStatusByUuid.get(s.node_uuid) || state.syncStatusByName.get(s.node_name) || {};
            const localUrl = s.local_url || (s.tailscale_ip ? `http://${s.tailscale_ip}:${s.port || 3000}` : null);
            const targetUrl = sync.target_url || (localUrl ? `${String(localUrl).replace(/\/$/, '')}/api/sync/apply` : (s.public_url ? `${String(s.public_url).replace(/\/$/, '')}/api/sync/apply` : '-'));
            const ngrokAction = isNgrokOwner
                ? '<button class="px-2 py-1 border rounded bg-green-50 text-green-700 cursor-default" disabled>Ngrok ativa</button>'
                : (isOnline ? `<button class="px-2 py-1 border rounded bg-indigo-600 text-white" data-action="assume-ngrok" data-node-uuid="${escapeHtml(s.node_uuid || '')}">Assumir ngrok</button>` : '<button class="px-2 py-1 border rounded text-slate-400 cursor-not-allowed" disabled>Offline</button>');
            tbody.innerHTML += `
                <tr class="hover:bg-slate-50 transition-colors">
                    <td class="p-4 font-semibold text-dark">${escapeHtml(s.node_name || '-')}${s.is_self ? ' (Este servidor)' : ''}${isNgrokOwner ? '<div><span class="inline-block mt-1 px-2 py-0.5 rounded bg-green-100 text-green-700 text-xs">Ngrok ativa</span></div>' : ''}</td>
                    <td class="p-4 text-sm font-mono text-slate-600">${escapeHtml(s.tailscale_ip || '-')}</td>
                    <td class="p-4">${roleBadge}</td>
                    <td class="p-4">${statusInd}</td>
                    <td class="p-4 text-xs font-mono text-slate-600 break-all">${escapeHtml(localUrl || '-')}</td>
                    <td class="p-4 text-xs font-mono text-slate-600 break-all">${escapeHtml(s.public_url || '-')}</td>
                    <td class="p-4 text-sm font-mono">${s.port || 3000}</td>
                    <td class="p-4 text-xs font-mono text-slate-600 max-w-xs break-all">${escapeHtml(targetUrl)}</td>
                    <td class="p-4 text-sm">${sync.pending_events ?? (s.is_self ? '-' : '0')}</td>
                    <td class="p-4"><span class="font-mono">${s.power_score ?? 5}/10</span></td>
                    <td class="p-4 text-right flex justify-end gap-2"><button class="px-2 py-1 border rounded" data-action="edit-self" ${s.is_self ? '' : 'disabled title="Edite no servidor local do node"'}>Editar</button>${ngrokAction}${s.is_self ? '' : '<button class="px-2 py-1 border rounded" data-action="fix-url-tailscale" data-node-id="'+s.id+'">Corrigir URL pelo IP Tailscale</button>'}<button class="px-2 py-1 border rounded" onclick="fetch('/api/cluster/nodes/${s.id}/healthcheck',{method:'POST'}).then(()=>window.location.reload())">Testar conexão</button></td>
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

    const renderJoinRequests = () => {
        const tbody = document.getElementById('tbody-join-requests');
        if (!tbody) return;
        if (!state.joinRequests.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="p-3 text-sm text-slate-500">Nenhuma solicitação pendente.</td></tr>';
            return;
        }
        tbody.innerHTML = state.joinRequests.map((request) => `
            <tr class="hover:bg-slate-50 transition-colors">
                <td class="p-2 font-semibold text-dark">${escapeHtml(request.node_name)}</td>
                <td class="p-2 font-mono text-slate-600">${escapeHtml(request.tailscale_ip)}</td>
                <td class="p-2">${escapeHtml(request.requested_role)}</td>
                <td class="p-2">${dateLabel(request.created_at)}</td>
                <td class="p-2 text-right whitespace-nowrap">
                    <button data-act="approve" data-id="${request.id}" class="px-2 py-1 bg-green-600 hover:bg-green-700 text-white rounded">Aprovar</button>
                    <button data-act="reject" data-id="${request.id}" class="px-2 py-1 bg-red-600 hover:bg-red-700 text-white rounded">Rejeitar</button>
                </td>
            </tr>`).join('');
    };

    const loadJoinRequests = async () => {
        try {
            const data = await apiFetch('/api/cluster/join-requests?status=PENDING');
            state.joinRequests = Array.isArray(data.data)
                ? data.data.filter((request) => request.status === 'PENDING')
                : [];
            renderJoinRequests();
            setFeedback('join-requests-feedback', state.joinRequests.length ? `${state.joinRequests.length} solicitação(ões) pendente(s).` : '', false);
        } catch (error) {
            console.error('[join-requests] erro ao carregar:', error);
            state.joinRequests = [];
            renderJoinRequests();
            setFeedback('join-requests-feedback', `Erro ao carregar solicitações: ${error.message}`, true);
        }
    };

    document.getElementById('self-config-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        selfError.textContent = '';
        const portValue = Number(document.getElementById('self-port')?.value || 3000);
        const payload = { node_name: document.getElementById('self-node-name').value.trim(), tailscale_ip: document.getElementById('self-node-ip').value.trim(), port: portValue, public_url: document.getElementById('self-public-url').value.trim(), role: document.getElementById('self-role').value || 'STANDBY', power_score: Number(document.getElementById('self-power-score')?.value || 5) };
        if (!payload.node_name || !payload.tailscale_ip) { selfError.textContent = 'Nome e IP são obrigatórios.'; return; }
        if (!Number.isInteger(payload.port) || payload.port < 1 || payload.port > 65535) { selfError.textContent = 'Porta deve ficar entre 1 e 65535.'; return; }
        if (!Number.isInteger(payload.power_score) || payload.power_score < 0 || payload.power_score > 10) { selfError.textContent = 'Ordem de potência deve ficar entre 0 e 10.'; return; }
        try {
            await apiFetch('/api/cluster/self', { method:'POST', body: JSON.stringify(payload)});
            closeSelfModal(); await fetchClusterNodes(); renderServidores(); initCards();
        } catch (error) {
            selfError.textContent = error.message || 'Erro ao salvar configuração.';
        }
    });

    document.getElementById('tbody-join-requests')?.addEventListener('click', async (e) => {
        const btn = e.target.closest('button[data-id]'); if (!btn) return;
        const id = btn.getAttribute('data-id');
        const act = btn.getAttribute('data-act');
        btn.disabled = true;
        setFeedback('join-requests-feedback', act === 'approve' ? 'Aprovando solicitação...' : 'Rejeitando solicitação...', false);
        try {
            await apiFetch(`/api/cluster/join-requests/${id}/${act}`, { method:'POST', body: JSON.stringify({}) });
            await fetchClusterNodes();
            renderServidores();
            initCards();
            await loadJoinRequests();
            setFeedback('join-requests-feedback', act === 'approve' ? 'Solicitação aprovada com sucesso.' : 'Solicitação rejeitada com sucesso.', false);
        } catch (error) {
            console.error('[join-requests] erro ao processar:', error);
            setFeedback('join-requests-feedback', `Erro ao processar solicitação: ${error.message}`, true);
        } finally {
            btn.disabled = false;
        }
    });

    document.getElementById('refresh-join-requests-btn')?.addEventListener('click', loadJoinRequests);

    document.getElementById('form-request-join-host')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const feedback = document.getElementById('join-host-feedback');
        feedback.textContent = '';
        const payload = { host_url: document.getElementById('join-host-url').value.trim() };
        try {
            const data = await apiFetch('/api/cluster/request-join-host', { method:'POST', body: JSON.stringify(payload)});
            await fetchClusterNodes();
            renderServidores();
            initCards();
            feedback.textContent = data.message || 'Solicitação enviada. Aguarde aprovação no host.';
            feedback.className = 'text-sm mt-3 text-green-600';
        } catch (error) {
            feedback.textContent = error.message || 'Falha ao enviar solicitação.';
            feedback.className = 'text-sm mt-3 text-red-600';
        }
    });

    // --- Inits Globais ---
    apiFetch('/api/cluster/self').then((d) => renderJoinSelfData(d?.node || null)).catch(() => renderJoinSelfData(null));

    initChart();
    fetchClusterNodes().finally(async () => {
        renderServidores();
        initCards();
        await loadDashboard();
        await loadJoinRequests();
    });
    if(document.getElementById('map-container')) setTimeout(initMap, 100);
    loadActiveDataPoints();
    loadEventQueue();
    loadAlerts();
    setInterval(loadJoinRequests, 15000);

    document.getElementById('refresh-health-btn')?.addEventListener('click', async () => {
        await apiFetch('/api/cluster/healthcheck-all', { method: 'POST', body: JSON.stringify({}) });
        await fetchClusterNodes();
        renderServidores();
        initCards();
    });


    document.getElementById('compare-db-btn')?.addEventListener('click', async (event) => {
        const btn = event.currentTarget;
        btn.disabled = true;
        try {
            const remote = state.servidores.find((node) => !node.is_self && node.node_uuid);
            const data = await apiFetch(remote ? `/api/sync/compare?node_uuid=${encodeURIComponent(remote.node_uuid)}` : '/api/sync/compare');
            const rows = (data.comparisons || []).map((item) => `${item.table}: ${item.status} (local=${item.local?.count ?? '-'}, remoto=${item.remote?.count ?? '-'})`).join(' | ');
            setFeedback('join-requests-feedback', rows ? `Comparação com ${data.node_name || 'local'}: ${rows}` : 'Fingerprint local calculado. Configure um nó remoto para comparar.');
        } catch (error) {
            setFeedback('join-requests-feedback', `Erro ao comparar bancos: ${error.message}`, true);
        } finally {
            btn.disabled = false;
        }
    });


    document.getElementById('bootstrap-initial-btn')?.addEventListener('click', async (event) => {
        const btn = event.currentTarget;
        const remote = state.servidores.find((node) => !node.is_self && (node.public_url || node.tailscale_ip));
        if (!remote) {
            setFeedback('join-requests-feedback', 'Nenhum nó remoto encontrado para bootstrap inicial.', true);
            return;
        }
        const hostUrl = remote.public_url || `http://${remote.tailscale_ip}:${remote.port || 3000}`;
        if (!window.confirm(`Iniciar sincronização inicial recebendo dados de ${remote.node_name || hostUrl}?`)) return;
        btn.disabled = true;
        btn.textContent = 'Iniciando bootstrap...';
        try {
            const data = await apiFetch('/api/sync/full-bootstrap', { method: 'POST', body: JSON.stringify({ host_url: hostUrl }) });
            setFeedback('join-requests-feedback', data.message || `Bootstrap inicial iniciado a partir de ${hostUrl}.`, false);
        } catch (error) {
            setFeedback('join-requests-feedback', `Erro ao iniciar bootstrap: ${error.message}`, true);
        } finally {
            btn.disabled = false;
            btn.textContent = 'Iniciar sincronização inicial';
        }
    });

    document.getElementById('sync-now-btn')?.addEventListener('click', async (event) => {
        const btn = event.currentTarget;
        btn.disabled = true;
        btn.textContent = 'Sincronizando...';
        try {
            const data = await apiFetch('/api/sync/run-now', { method: 'POST', body: JSON.stringify({}) });
            const totals = (data.nodes || []).reduce((acc, node) => {
                acc.sent += Number(node.sent || 0);
                acc.applied += Number(node.applied_by_remote || 0);
                acc.failed += Number(node.failed || 0);
                return acc;
            }, { sent: 0, applied: 0, failed: 0 });
            await fetchClusterNodes();
            renderServidores();
            setFeedback('join-requests-feedback', `Sync concluído: enviados=${totals.sent} applied=${totals.applied} failed=${totals.failed}`, totals.failed > 0);
        } catch (error) {
            setFeedback('join-requests-feedback', `Erro ao sincronizar: ${error.message}`, true);
        } finally {
            btn.disabled = false;
            btn.textContent = 'Sincronizar agora';
        }
    });

    document.getElementById('add-server-btn')?.addEventListener('click', openAddModal);
    document.getElementById('close-add-server-modal')?.addEventListener('click', closeAddModal);

    document.getElementById('add-server-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const err = document.getElementById('add-server-error');
        err.textContent = '';
        const payload = { node_name: document.getElementById('add-node-name').value.trim(), tailscale_ip: document.getElementById('add-node-ip').value.trim(), public_url: document.getElementById('add-public-url').value.trim(), role: document.getElementById('add-role').value, status: 'UNKNOWN', power_score: Number(document.getElementById('add-power-score')?.value || 5) };
        if (!Number.isInteger(payload.power_score) || payload.power_score < 0 || payload.power_score > 10) { err.textContent = 'Ordem de potência deve ficar entre 0 e 10.'; return; }
        try {
            await apiFetch('/api/cluster/nodes', { method:'POST', body: JSON.stringify(payload)});
            closeAddModal();
            await fetchClusterNodes();
            renderServidores();
            initCards();
        } catch (error) {
            err.textContent = error.message || 'Erro ao adicionar servidor.';
        }
    });

    document.getElementById('tbody-servidores')?.addEventListener('click', async (e) => {

      const assumeBtn = e.target.closest('[data-action="assume-ngrok"]');
      if (assumeBtn) {
        assumeBtn.disabled = true;
        try {
          const data = await apiFetch('/api/cluster/ngrok/assume', { method: 'POST', body: JSON.stringify({ target_node_uuid: assumeBtn.dataset.nodeUuid }) });
          setFeedback('join-requests-feedback', data.message || 'Solicitação enviada.', false);
          setTimeout(async () => { await fetchClusterNodes(); renderServidores(); }, 2500);
          await fetchClusterNodes();
          renderServidores();
        } catch (error) {
          setFeedback('join-requests-feedback', `Erro ao assumir ngrok: ${error.message}`, true);
        } finally {
          assumeBtn.disabled = false;
        }
        return;
      }

      const fixBtn = e.target.closest('[data-action="fix-url-tailscale"]');
      if (fixBtn) {
        await apiFetch(`/api/cluster/nodes/${fixBtn.dataset.nodeId}/fix-url-tailscale`, { method: 'POST', body: JSON.stringify({}) });
        await fetchClusterNodes();
        renderServidores();
        return;
      }
      if (!e.target.closest('[data-action="edit-self"]')) return;
      const self = state.servidores.find((n) => n.is_self);
      if (!self) return;
      document.getElementById('self-node-name').value = self.node_name || '';
      document.getElementById('self-node-ip').value = self.tailscale_ip || '';
      document.getElementById('self-public-url').value = self.public_url || '';
      document.getElementById('self-port').value = self.port || 3000;
      document.getElementById('self-role').value = self.role || 'STANDBY';
      document.getElementById('self-power-score').value = self.power_score ?? 5;
      openSelfModal();
    });



    document.getElementById('close-historical-modal')?.addEventListener('click', closeHistoricalModal);
    document.getElementById('historical-reset-zoom')?.addEventListener('click', () => applyHistoricalZoom(null));
    document.querySelectorAll('[data-history-range]').forEach((button) => button.addEventListener('click', () => applyHistoricalRange(button.dataset.historyRange)));
    document.getElementById('regenerate-historical-chart')?.addEventListener('click', async () => {
        if (!state.currentHistoricalPointId) return;
        stopHistoricalPolling();
        await apiFetch(`/api/data-points/${state.currentHistoricalPointId}/historical-chart/regenerate`, { method: 'POST', body: JSON.stringify({}) });
        await loadHistoricalChart(state.currentHistoricalPointId);
    });

    document.getElementById('form-import-historical')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const feedback = document.getElementById('hist-import-feedback');
        const button = document.getElementById('hist-import-btn');
        const formData = new FormData(e.target);
        if (!document.getElementById('hist-file')?.files?.length) { feedback.textContent = 'Selecione um CSV.'; feedback.className = 'text-sm mt-3 text-red-600'; return; }
        button.disabled = true; feedback.textContent = 'Enviando e importando...'; feedback.className = 'text-sm mt-3 text-slate-600';
        try {
            const response = await fetch('/api/imports/historical-csv', { method: 'POST', body: formData });
            const data = await response.json().catch(() => ({}));
            if (!response.ok || !data.ok) throw new Error(data.message || 'Falha ao importar CSV.');
            feedback.className = 'text-sm mt-3 text-green-600';
            feedback.innerHTML = `${data.warning ? `<span class="block text-yellow-700 font-semibold mb-1">${escapeHtml(data.warning)}</span>` : ''}Importação concluída: ${data.import.total_rows} linhas, ${data.import.imported_rows} importadas, ${data.import.failed_rows} falhas. <button type="button" data-import-historical-id="${data.data_point.id}" class="underline font-semibold">Ver gráfico histórico</button>`;
            await loadDataPoints(); await loadActiveDataPoints();
        } catch (error) { feedback.textContent = error.message; feedback.className = 'text-sm mt-3 text-red-600'; }
        finally { button.disabled = false; }
    });

    document.getElementById('hist-import-feedback')?.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-import-historical-id]');
        if (!btn) return;
        const point = state.pontos.find((p) => String(p.id) === String(btn.dataset.importHistoricalId)) || { id: btn.dataset.importHistoricalId, name: 'Ponto importado' };
        openHistoricalModal(point);
    });

    // Reativar icons finais
    lucide.createIcons();
});