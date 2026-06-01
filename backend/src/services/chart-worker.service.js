const pool = require('../database/connection');
const { selectBestProcessingNode } = require('./processing-node-selector.service');
const chartService = require('./historical-chart.service');

let timer = null;
let running = false;

const processChartJobs = async () => {
  if (running) return;
  running = true;
  try {
    const [[job]] = await pool.execute("SELECT * FROM chart_generation_jobs WHERE status='PENDING' ORDER BY created_at ASC, id ASC LIMIT 1");
    if (!job) return;
    const selection = await selectBestProcessingNode();
    const best = selection?.bestNode;
    const self = selection?.selfNode;
    if (best && self && best.id !== self.id && selection.onlineNodes.length > 1) {
      console.log('[chart-worker] job deve ser processado por outro nó, aguardando distribuição.', { job_id: job.id, assigned_to: best.node_name });
      return;
    }
    console.log('[chart-worker] processando job local', { job_id: job.id, data_point_id: job.data_point_id, node: self?.node_name || 'local' });
    try {
      await chartService.generateChartForJob(job, self || best || null);
    } catch (error) {
      await pool.execute("UPDATE chart_generation_jobs SET status='FAILED', error_message=?, finished_at=NOW() WHERE id=?", [error.message, job.id]).catch(() => {});
      await pool.execute("UPDATE chart_cache SET status=IF(status='READY','READY','FAILED'), error_message=? WHERE data_point_id=? AND chart_type='HISTORICAL_RIVER_LEVEL'", [error.message, job.data_point_id]).catch(() => {});
      throw error;
    }
  } catch (error) {
    console.error('[chart-worker] falha:', error.message);
  } finally {
    running = false;
  }
};

const start = () => {
  if (timer) return;
  timer = setInterval(processChartJobs, 10000);
  setTimeout(processChartJobs, 1500);
};

module.exports = { start, processChartJobs };
