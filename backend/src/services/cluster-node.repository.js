const crypto = require('crypto');
const db = require('../database/connection');
const syncEventService = require('./sync-event.service');
const syncPayloadService = require('./sync-payload.service');

const mapNode = (row) => ({ ...row, is_self: Number(row.is_self), power_score: Number(row.power_score ?? 5) });

class ClusterNodeRepository {
  async getSelfNode() {
    const [rows] = await db.execute('SELECT * FROM cluster_nodes WHERE is_self = 1 LIMIT 1');
    return rows[0] ? mapNode(rows[0]) : null;
  }

  async getAllNodes() {
    const [rows] = await db.execute(`SELECT * FROM cluster_nodes
      ORDER BY is_self DESC, CASE role WHEN 'HOST' THEN 0 WHEN 'STANDBY' THEN 1 ELSE 2 END, node_name ASC`);
    return rows.map(mapNode);
  }


  async getExternalNodes() {
    const [rows] = await db.execute('SELECT * FROM cluster_nodes WHERE is_self = 0 ORDER BY node_name ASC');
    return rows.map(mapNode);
  }

  async getKnownHosts() {
    const [rows] = await db.execute("SELECT * FROM cluster_nodes WHERE role = 'HOST' ORDER BY is_self DESC, node_name ASC");
    return rows.map(mapNode);
  }

  async getOnlineHosts() {
    const [rows] = await db.execute("SELECT * FROM cluster_nodes WHERE role = 'HOST' AND status = 'ONLINE' ORDER BY is_self DESC, node_name ASC");
    return rows.map(mapNode);
  }

  async findById(id) {
    const [rows] = await db.execute('SELECT * FROM cluster_nodes WHERE id = ? LIMIT 1', [id]);
    return rows[0] ? mapNode(rows[0]) : null;
  }

  async findByTailscaleIp(ip) {
    const [rows] = await db.execute('SELECT * FROM cluster_nodes WHERE tailscale_ip = ? LIMIT 1', [ip]);
    return rows[0] ? mapNode(rows[0]) : null;
  }

  async findByNodeUuid(nodeUuid) {
    const [rows] = await db.execute('SELECT * FROM cluster_nodes WHERE node_uuid = ? LIMIT 1', [nodeUuid]);
    return rows[0] ? mapNode(rows[0]) : null;
  }


  async upsertNodeByTailscaleIp(data) {
    const existing = await this.findByTailscaleIp(data.tailscale_ip);
    if (existing) return this.updateNode(existing.id, { ...existing, ...data, is_self: Number(data.is_self ?? existing.is_self) });
    return this.createNode({
      node_uuid: data.node_uuid,
      node_name: data.node_name,
      tailscale_ip: data.tailscale_ip,
      public_url: data.public_url || null,
      role: data.role || 'UNKNOWN',
      status: data.status || 'UNKNOWN',
      is_self: Number(data.is_self || 0),
      last_heartbeat_at: data.last_heartbeat_at || null,
      last_healthcheck_at: data.last_healthcheck_at || null,
      healthcheck_error: data.healthcheck_error || null,
      metadata: data.metadata || null,
      power_score: data.power_score ?? 5
    });
  }

  async updateStatus(id, status, error = null) {
    const current = await this.findById(id);
    if (!current) return null;
    const now = new Date();
    return this.updateNode(id, {
      ...current,
      status,
      last_healthcheck_at: now,
      last_heartbeat_at: status === 'ONLINE' ? now : current.last_heartbeat_at,
      healthcheck_error: error
    });
  }

  async setSelfNode(data) {
    await this.clearSelfFlag();
    const now = new Date();
    const payload = {
      ...data,
      is_self: 1,
      status: data.status || 'ONLINE',
      last_healthcheck_at: now,
      last_heartbeat_at: now,
      healthcheck_error: null,
      power_score: data.power_score ?? 5
    };
    return this.upsertNodeByTailscaleIp(payload);
  }

  async clearSelfFlag() { await db.execute('UPDATE cluster_nodes SET is_self = 0 WHERE is_self = 1'); }

  async createNode(payload) {
    const [result] = await db.execute(`INSERT INTO cluster_nodes
      (node_uuid, node_name, tailscale_ip, public_url, role, status, is_self, last_heartbeat_at, last_healthcheck_at, healthcheck_error, metadata, power_score)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [payload.node_uuid || crypto.randomUUID(), payload.node_name, payload.tailscale_ip, payload.public_url, payload.role, payload.status, payload.is_self, payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, payload.metadata, payload.power_score ?? 5]);
    const node = await this.findById(result.insertId);
    const syncPayload = await syncPayloadService.getClusterNodePayloadById(node.id);
    await syncEventService.createEntitySyncEvent('cluster_node', syncPayload);
    return node;
  }

  async updateNode(id, payload) {
    await db.execute(`UPDATE cluster_nodes SET node_uuid=?, node_name=?, tailscale_ip=?, public_url=?, role=?, status=?, is_self=?,
      last_heartbeat_at=?, last_healthcheck_at=?, healthcheck_error=?, metadata=?, power_score=? WHERE id=?`,
    [payload.node_uuid || crypto.randomUUID(), payload.node_name, payload.tailscale_ip, payload.public_url, payload.role, payload.status, payload.is_self, payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, payload.metadata, payload.power_score ?? 5, id]);
    const node = await this.findById(id);
    if (node) {
      const syncPayload = await syncPayloadService.getClusterNodePayloadById(id);
      await syncEventService.createEntitySyncEvent('cluster_node', syncPayload);
    }
    return node;
  }

  async deleteNode(id) { await db.execute('DELETE FROM cluster_nodes WHERE id=?', [id]); }

  async findPendingJoinRequestByIp(ip) {
    const [rows] = await db.execute('SELECT * FROM cluster_join_requests WHERE tailscale_ip = ? AND status = "PENDING" LIMIT 1', [ip]);
    return rows[0] || null;
  }

  async createJoinRequest(payload) {
    const [result] = await db.execute(`INSERT INTO cluster_join_requests
      (node_name, tailscale_ip, public_url, requested_role, status, request_token_hash, secret_fingerprint, requester_metadata)
      VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?)`,
    [payload.node_name, payload.tailscale_ip, payload.public_url, payload.requested_role, payload.request_token_hash, payload.secret_fingerprint, payload.requester_metadata]);
    return this.findJoinRequestById(result.insertId);
  }

  async updateJoinRequest(id, payload) {
    await db.execute(`UPDATE cluster_join_requests SET node_name=?, tailscale_ip=?, public_url=?, requested_role=?,
      request_token_hash=?, secret_fingerprint=?, requester_metadata=? WHERE id=?`,
    [payload.node_name, payload.tailscale_ip, payload.public_url, payload.requested_role, payload.request_token_hash, payload.secret_fingerprint, payload.requester_metadata, id]);
    return this.findJoinRequestById(id);
  }

  async listJoinRequests(status) {
    if (status) {
      const [rows] = await db.execute('SELECT * FROM cluster_join_requests WHERE status = ? ORDER BY created_at DESC', [status]);
      return rows;
    }
    const [rows] = await db.execute('SELECT * FROM cluster_join_requests ORDER BY created_at DESC');
    return rows;
  }

  async findJoinRequestById(id) {
    const [rows] = await db.execute('SELECT * FROM cluster_join_requests WHERE id = ? LIMIT 1', [id]);
    return rows[0] || null;
  }

  async approveJoinRequest(id, approvedNodeId) {
    await db.execute(`UPDATE cluster_join_requests
      SET status='APPROVED', approved_node_id=?, approved_at=NOW(), rejected_at=NULL
      WHERE id=?`, [approvedNodeId, id]);
    return this.findJoinRequestById(id);
  }

  async rejectJoinRequest(id) {
    await db.execute("UPDATE cluster_join_requests SET status='REJECTED', rejected_at=NOW() WHERE id=?", [id]);
    return this.findJoinRequestById(id);
  }
}

module.exports = new ClusterNodeRepository();
