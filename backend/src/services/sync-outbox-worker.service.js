const pool = require('../database/connection');
const clusterRepo = require('./cluster-node.repository');
const env = require('../config/env');
const { hashPayload } = require('./sync.service');

let timer = null;
let running = false;
let loggedNoNodes = false;

const nodeBaseUrl = (node) => {
  if (node.public_url) return String(node.public_url).replace(/\/$/, '');
  if (node.tailscale_ip) return `http://${node.tailscale_ip}:${node.port || env.port}`;
  return null;
};

const syncOutboxWorker = async () => {
  if (running) return;
  running = true;
  try {
    const nodes = (await clusterRepo.getAllNodes()).filter((node) => !node.is_self && node.status === 'ONLINE');
    if (!nodes.length) {
      if (!loggedNoNodes) console.log('[sync] nenhum nó externo online para replicar.');
      loggedNoNodes = true;
      return;
    }
    loggedNoNodes = false;
    const [events] = await pool.execute("SELECT * FROM sync_outbox WHERE status IN ('PENDING','FAILED') ORDER BY created_at ASC, id ASC LIMIT 100");
    if (!events.length) return;
    for (const node of nodes) {
      for (const event of events) {
        await pool.execute(
          `INSERT IGNORE INTO sync_outbox_deliveries (outbox_id, target_node_id, status) VALUES (?, ?, 'PENDING')`,
          [event.id, node.id]
        );
      }
    }
    const [deliveries] = await pool.execute(
      `SELECT d.*, o.entity_type, o.entity_id, o.operation, o.payload
         FROM sync_outbox_deliveries d
         JOIN sync_outbox o ON o.id=d.outbox_id
        WHERE d.status IN ('PENDING','FAILED')
        ORDER BY d.created_at ASC, d.id ASC
        LIMIT 100`
    );
    for (const delivery of deliveries) {
      const node = nodes.find((item) => item.id === delivery.target_node_id);
      const baseUrl = nodeBaseUrl(node || {});
      if (!node || !baseUrl) continue;
      const payload = typeof delivery.payload === 'object' ? delivery.payload : JSON.parse(delivery.payload);
      const body = { events: [{ entity_type: delivery.entity_type, entity_id: delivery.entity_id, operation: delivery.operation, payload, payload_hash: hashPayload(payload) }] };
      try {
        const response = await fetch(`${baseUrl}/api/sync/apply`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Cluster-Secret': env.sessionSecret }, body: JSON.stringify(body) });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        await pool.execute("UPDATE sync_outbox_deliveries SET status='SENT', sent_at=NOW(), last_error=NULL WHERE id=?", [delivery.id]);
      } catch (error) {
        await pool.execute("UPDATE sync_outbox_deliveries SET status='FAILED', attempts=attempts+1, last_error=? WHERE id=?", [error.message, delivery.id]);
      }
    }
    await pool.execute(
      `UPDATE sync_outbox o
          SET status = CASE
            WHEN EXISTS (SELECT 1 FROM sync_outbox_deliveries d WHERE d.outbox_id=o.id AND d.status='FAILED') THEN 'FAILED'
            WHEN EXISTS (SELECT 1 FROM sync_outbox_deliveries d WHERE d.outbox_id=o.id AND d.status!='SENT') THEN 'PENDING'
            WHEN EXISTS (SELECT 1 FROM sync_outbox_deliveries d WHERE d.outbox_id=o.id) THEN 'SENT'
            ELSE o.status END,
              sent_at = CASE WHEN NOT EXISTS (SELECT 1 FROM sync_outbox_deliveries d WHERE d.outbox_id=o.id AND d.status!='SENT') AND EXISTS (SELECT 1 FROM sync_outbox_deliveries d WHERE d.outbox_id=o.id) THEN NOW() ELSE sent_at END
        WHERE o.id IN (${events.map(() => '?').join(',')})`,
      events.map((event) => event.id)
    );
  } catch (error) {
    console.error('[sync] falha no worker:', error.message);
  } finally { running = false; }
};

const start = () => {
  if (timer) return;
  timer = setInterval(syncOutboxWorker, 30000);
  setTimeout(syncOutboxWorker, 3000);
};

module.exports = { start, syncOutboxWorker };
