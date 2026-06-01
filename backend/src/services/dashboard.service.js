const dataPointRepository = require('../repositories/data-point.repository');
const measurementRepository = require('../repositories/measurement.repository');
const alertRepository = require('../repositories/alert.repository');
const eventQueueRepository = require('../repositories/event-queue.repository');
const clusterNodeRepository = require('./cluster-node.repository');

const relativeLabel = (dateValue) => {
  if (!dateValue) return 'Sem leituras';
  const diffMs = Date.now() - new Date(dateValue).getTime();
  if (diffMs < 60_000) return 'Agora';
  const minutes = Math.round(diffMs / 60_000);
  if (minutes < 60) return `${minutes} min atrás`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} h atrás`;
  return new Date(dateValue).toLocaleDateString('pt-BR');
};

const getSummary = async () => {
  const nodes = await clusterNodeRepository.getAllNodes();
  const host = nodes.find((n) => n.role === 'HOST' && n.status === 'ONLINE') || nodes.find((n) => n.is_self) || nodes.find((n) => n.role === 'HOST');
  const standbyCount = nodes.filter((n) => n.role === 'STANDBY').length;
  const dataPointsCount = await dataPointRepository.countActive();
  const latestMeasurement = await measurementRepository.latestOne();
  const chartRows = await measurementRepository.chartRiverLevel(8);
  const latestEvents = await eventQueueRepository.findLatest(5);
  const latestAlerts = await alertRepository.findAll({ status: 'ACTIVE', limit: 5 });

  return {
    ok: true,
    summary: {
      current_host: host?.node_name || 'Não configurado',
      standby_count: standbyCount,
      data_points_count: dataPointsCount,
      last_measurement_label: relativeLabel(latestMeasurement?.measured_at),
      system_status: 'Sistema Operacional'
    },
    chart: { labels: chartRows.map((row) => row.label), values: chartRows.map((row) => row.value) },
    latest_events: latestEvents,
    latest_alerts: latestAlerts
  };
};

module.exports = { getSummary };
