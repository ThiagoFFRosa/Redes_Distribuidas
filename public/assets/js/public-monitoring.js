document.addEventListener('DOMContentLoaded', () => {
  const mapEl = document.getElementById('public-monitoring-map');
  if (!mapEl || typeof L === 'undefined') return;

  const state = { points: [], selectedPoint: null, chart: null, map: null, markers: [] };
  const colors = { NORMAL: '#16a34a', ATTENTION: '#f59e0b', CRITICAL: '#dc2626', INACTIVE: '#64748b', UNKNOWN: '#93c5fd' };
  const labels = { NORMAL: 'Normal', ATTENTION: 'Atenção', CRITICAL: 'Crítico', INACTIVE: 'Inativo', UNKNOWN: 'Sem dados' };
  const escapeHtml = (value = '') => String(value).replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[char]));
  const hasValue = (value) => value !== null && value !== undefined && value !== '';
  const formatLevel = (value, unit = 'm') => hasValue(value) ? `${Number(value).toFixed(2)} ${unit || 'm'}` : 'Não configurado';
  const formatDate = (value) => value ? new Date(value).toLocaleString('pt-BR') : '-';
  const latestValue = (point) => point.latest_measurement ? `${Number(point.latest_measurement.value).toFixed(2)} ${point.latest_measurement.unit || point.measurement_unit || 'm'}` : 'Sem medição';

  const makeIcon = (riskStatus) => L.divIcon({
    className: 'public-map-marker',
    html: `<span style="background:${colors[riskStatus] || colors.UNKNOWN}"></span>`,
    iconSize: [22, 22],
    iconAnchor: [11, 11]
  });

  const renderLegend = () => {
    const legend = L.control({ position: 'bottomright' });
    legend.onAdd = () => {
      const div = L.DomUtil.create('div', 'public-map-legend');
      div.innerHTML = Object.entries(labels).map(([status, label]) => `<div><span style="background:${colors[status]}"></span>${label}</div>`).join('');
      return div;
    };
    legend.addTo(state.map);
  };

  const renderSummaryCards = () => {
    const totalEl = document.getElementById('public-stat-points');
    const alertEl = document.getElementById('public-stat-alerts');
    const updatedEl = document.getElementById('public-stat-updated');
    if (totalEl) totalEl.textContent = state.points.length;
    if (alertEl) alertEl.textContent = state.points.filter((p) => ['ATTENTION', 'CRITICAL'].includes(p.risk_status)).length;
    if (updatedEl) {
      const latest = state.points.map((p) => p.latest_measurement?.measured_at).filter(Boolean).sort().pop();
      updatedEl.textContent = latest ? new Date(latest).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }) : 'Sem dados';
    }
  };

  const renderTable = () => {
    const tbody = document.getElementById('public-points-tbody');
    if (!tbody) return;
    if (!state.points.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="p-5 text-center text-slate-500">Nenhum ponto cadastrado no banco.</td></tr>';
      return;
    }
    tbody.innerHTML = state.points.map((point) => `
      <tr class="hover:bg-slate-50 cursor-pointer" data-point-id="${point.id}">
        <td class="p-4 font-semibold text-dark">${escapeHtml(point.name)}</td>
        <td class="p-4 text-slate-600">${escapeHtml(point.city_region || '-')}</td>
        <td class="p-4 font-mono text-slate-600">${escapeHtml(latestValue(point))}</td>
        <td class="p-4"><span class="inline-flex items-center gap-2 px-2.5 py-1 rounded-full text-xs font-semibold bg-slate-100 text-slate-700"><span class="w-2.5 h-2.5 rounded-full" style="background:${colors[point.risk_status] || colors.UNKNOWN}"></span>${labels[point.risk_status] || point.risk_status}</span></td>
        <td class="p-4 text-slate-500 whitespace-nowrap">${formatDate(point.latest_measurement?.measured_at)}</td>
      </tr>`).join('');
  };

  const renderPointDetails = (point) => {
    const card = document.getElementById('public-point-details');
    if (!card) return;
    card.classList.remove('hidden');
    card.innerHTML = `
      <div class="flex flex-col lg:flex-row gap-6">
        <div class="lg:w-1/3">
          <p class="text-xs uppercase tracking-wide text-slate-500 font-bold">Ponto selecionado</p>
          <h3 class="text-2xl font-bold text-dark mt-1">${escapeHtml(point.name)}</h3>
          <p class="text-slate-600 mt-1">${escapeHtml(point.city_region || 'Região não informada')}</p>
          <div class="mt-4 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-1 gap-3 text-sm">
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Status operacional</span><strong style="color:${colors[point.risk_status] || colors.UNKNOWN}">${labels[point.risk_status] || point.risk_status}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Última medição</span><strong>${escapeHtml(latestValue(point))}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Atualizado em</span><strong>${formatDate(point.latest_measurement?.measured_at)}</strong></div>
            <div class="rounded-xl bg-slate-50 p-3"><span class="block text-slate-500">Limites</span><strong>Risco: ${formatLevel(point.warning_level, point.measurement_unit)} · Crítico: ${formatLevel(point.critical_level, point.measurement_unit)}</strong></div>
          </div>
        </div>
        <div class="lg:w-2/3 min-h-[280px]">
          <div id="public-point-chart-empty" class="hidden h-full rounded-xl bg-slate-50 border border-dashed border-slate-200 flex items-center justify-center p-6 text-slate-500 text-center">Ainda não há medições registradas para este ponto.</div>
          <canvas id="public-point-chart" height="140"></canvas>
        </div>
      </div>`;
  };

  const loadMeasurements = async (point) => {
    renderPointDetails(point);
    const response = await fetch(`/api/public/monitoring-points/${point.id}/measurements?limit=12`);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.message || 'Falha ao carregar medições.');
    const empty = document.getElementById('public-point-chart-empty');
    const canvas = document.getElementById('public-point-chart');
    if (state.chart) state.chart.destroy();
    if (!payload.measurements?.length) {
      empty?.classList.remove('hidden');
      canvas?.classList.add('hidden');
      return;
    }
    empty?.classList.add('hidden');
    canvas?.classList.remove('hidden');
    state.chart = new Chart(canvas, {
      type: 'line',
      data: {
        labels: payload.measurements.map((m) => new Date(m.measured_at).toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })),
        datasets: [{ label: `Medição (${point.measurement_unit || 'm'})`, data: payload.measurements.map((m) => Number(m.value)), borderColor: colors[point.risk_status] || '#0284c7', backgroundColor: 'rgba(2,132,199,0.10)', fill: true, tension: 0.35, pointRadius: 4 }]
      },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true } }, scales: { y: { title: { display: true, text: point.measurement_unit || 'm' } } } }
    });
  };

  const selectPoint = async (point) => {
    state.selectedPoint = point;
    try { await loadMeasurements(point); } catch (error) {
      const card = document.getElementById('public-point-details');
      if (card) card.innerHTML += `<p class="text-sm text-red-600 mt-3">${escapeHtml(error.message)}</p>`;
    }
  };

  const renderMarkers = () => {
    state.markers.forEach((marker) => state.map.removeLayer(marker));
    state.markers = [];
    const bounds = [];
    state.points.forEach((point) => {
      if (!Number.isFinite(Number(point.latitude)) || !Number.isFinite(Number(point.longitude))) return;
      const marker = L.marker([point.latitude, point.longitude], { icon: makeIcon(point.risk_status) }).addTo(state.map);
      marker.bindPopup(`<strong>${escapeHtml(point.name)}</strong><br>${escapeHtml(labels[point.risk_status] || point.risk_status)}<br>${escapeHtml(latestValue(point))}`);
      marker.on('click', () => selectPoint(point));
      state.markers.push(marker);
      bounds.push([point.latitude, point.longitude]);
    });
    if (bounds.length) state.map.fitBounds(bounds, { padding: [35, 35], maxZoom: 13 });
    else state.map.setView([-23.0264, -45.5553], 11);
  };

  const loadPoints = async () => {
    const statusEl = document.getElementById('public-map-status');
    try {
      const response = await fetch('/api/public/monitoring-points');
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) throw new Error(payload.message || 'Falha ao carregar pontos.');
      state.points = payload.data || [];
      renderMarkers();
      renderTable();
      renderSummaryCards();
      if (statusEl) statusEl.textContent = state.points.length ? 'Pontos carregados do banco.' : 'Nenhum ponto cadastrado no banco.';
    } catch (error) {
      if (statusEl) statusEl.textContent = error.message;
      renderTable();
    }
  };

  state.map = L.map('public-monitoring-map', { scrollWheelZoom: false }).setView([-23.0264, -45.5553], 11);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', { attribution: '&copy; OpenStreetMap &copy; CARTO' }).addTo(state.map);
  renderLegend();
  document.getElementById('public-points-tbody')?.addEventListener('click', (event) => {
    const row = event.target.closest('[data-point-id]');
    const point = state.points.find((item) => String(item.id) === row?.dataset.pointId);
    if (point) selectPoint(point);
  });
  loadPoints();
});
