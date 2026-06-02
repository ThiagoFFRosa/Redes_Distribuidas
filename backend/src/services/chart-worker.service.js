const pool = require('../database/connection');
const env = require('../config/env');
const { selectBestProcessingNode } = require('./processing-node-selector.service');
const chartService = require('./historical-chart.service');

let timer = null;
let running = false;
const remoteLogAt = new Map();
const LOG_THROTTLE_MS = 30000;

const shouldLogRemoteWait = (jobUuid) => {
  const now = Date.now();
  const previous = remoteLogAt.get(jobUuid) || 0;
  if (now - previous < LOG_THROTTLE_MS) return false;
  remoteLogAt.set(jobUuid, now);
  return true;
};

const reassignTimedOutJobs = async (selection) => {
  const self = selection?.selfNode || null;
  const onlineNodes = selection?.onlineNodes || [];
  if (!self || !onlineNodes.length) return;
  const timeoutSeconds = Number(env.chartJobTimeoutSeconds || 60);
  const [jobs] = await pool.execute(
    `SELECT cj.*, cn.status AS assigned_node_status, cn.node_name AS assigned_node_name
       FROM chart_generation_jobs cj
       LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid
      WHERE cj.status IN ('PENDING', 'PROCESSING')
        AND (cj.assigned_to_node_uuid IS NULL OR cn.status <> 'ONLINE' OR cj.updated_at < DATE_SUB(NOW(), INTERVAL ? SECOND))
      ORDER BY cj.updated_at ASC, cj.created_at ASC
      LIMIT 5`,
    [timeoutSeconds]
  );
  for (const job of jobs) {
    const best = onlineNodes[0] || self;
    if (!best?.node_uuid || best.node_uuid === job.assigned_to_node_uuid) continue;
    await pool.execute(
      `UPDATE chart_generation_jobs SET assigned_node_id=?, assigned_to_node_uuid=?, assigned_node_name=?, status='PENDING', progress_percent=0,
        error_message=NULL WHERE id=?`,
      [best.id, best.node_uuid, best.node_name, job.id]
    );
    await chartService.createJobSyncEvent(job.id).catch(() => {});
    console.log(`[chart-worker] job_uuid=${job.uuid} atribuído a ${job.assigned_node_name || job.assigned_to_node_uuid || 'outro node'}, mas timeout excedido. Reatribuindo para ${best.node_name || best.node_uuid}.`);
  }
};

const processChartJobs = async () => {
  if (running) return;
  running = true;
  try {
    const selection = await selectBestProcessingNode();
    const self = selection?.selfNode || null;
    if (!self?.node_uuid) return;
    await reassignTimedOutJobs(selection);

    const [[job]] = await pool.execute(
      `SELECT cj.*, cn.node_name AS assigned_to_node_name
         FROM chart_generation_jobs cj
         LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid
        WHERE cj.status IN ('PENDING', 'PROCESSING')
          AND cj.assigned_to_node_uuid = ?
        ORDER BY FIELD(cj.status, 'PENDING', 'PROCESSING'), cj.created_at ASC, cj.id ASC
        LIMIT 1`,
      [self.node_uuid]
    );
    if (!job) {
      const [[remoteJob]] = await pool.execute(
        `SELECT cj.uuid, cj.assigned_to_node_uuid, cn.node_name AS assigned_to_node_name
           FROM chart_generation_jobs cj
           LEFT JOIN cluster_nodes cn ON cn.node_uuid = cj.assigned_to_node_uuid
          WHERE cj.status IN ('PENDING', 'PROCESSING')
          ORDER BY cj.created_at ASC LIMIT 1`
      );
      if (remoteJob && shouldLogRemoteWait(remoteJob.uuid)) {
        console.log(`[chart-worker] job atribuído a outro node ${remoteJob.assigned_to_node_name || remoteJob.assigned_to_node_uuid || '-'}, aguardando chart_cache. job_uuid=${remoteJob.uuid}`);
      }
      return;
    }

    console.log(`[chart-worker] processando job_uuid=${job.uuid} data_point_uuid=${job.data_point_uuid}`);
    try {
      await chartService.generateChartForJob(job, self);
    } catch (error) {
      await chartService.markJobFailed(job, error);
      console.error(`[chart-worker] falhou job_uuid=${job.uuid} error=${error.message}`);
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
