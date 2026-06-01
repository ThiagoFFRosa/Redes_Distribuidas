const crypto = require('crypto');
const db = require('../database/connection');
const syncEventService = require('./sync-event.service');
const syncPayloadService = require('./sync-payload.service');

const asNumber = (value, fallback = null) => {
  if (value === undefined || value === null || value === '') return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const mapNode = (row) => ({
  ...row,
  is_self: Number(row.is_self),
  port: asNumber(row.port, null),
  power_score: asNumber(row.power_score, 5)
});

const toJsonValue = (value) => {
  if (value === undefined) return null;
  if (value === null) return null;
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
};

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

  async findByDistributedIdentity(data) {
    if (data?.node_uuid) {
      const byUuid = await this.findByNodeUuid(data.node_uuid);
      if (byUuid) return byUuid;
    }
    if (data?.tailscale_ip) return this.findByTailscaleIp(data.tailscale_ip);
    return null;
  }

  async enforceSingleSelf(selfId = null) {
    if (selfId) {
      await db.execute('UPDATE cluster_nodes SET is_self = CASE WHEN id = ? THEN 1 ELSE 0 END', [selfId]);
      return;
    }
    const self = await this.getSelfNode();
    if (self) await this.enforceSingleSelf(self.id);
  }

  async upsertNodeByTailscaleIp(data, options = {}) {
    return this.upsertClusterNode(data, options);
  }

  async upsertClusterNode(data, options = {}) {
    const self = await this.getSelfNode();
    const existing = await this.findByDistributedIdentity(data);
    const matchesLocalSelf = Boolean(self && (
      (data.node_uuid && self.node_uuid === data.node_uuid) ||
      (data.tailscale_ip && self.tailscale_ip === data.tailscale_ip) ||
      (existing && existing.id === self.id)
    ));
    const isSelf = (matchesLocalSelf || (!self && Number(data.is_self) === 1)) ? 1 : 0;
    const payload = {
      ...(existing || {}),
      ...data,
      node_uuid: data.node_uuid || existing?.node_uuid || crypto.randomUUID(),
      node_name: data.node_name || existing?.node_name,
      tailscale_ip: data.tailscale_ip || existing?.tailscale_ip,
      public_url: data.public_url ?? existing?.public_url ?? null,
      port: asNumber(data.port, existing?.port ?? null),
      role: data.role || existing?.role || 'UNKNOWN',
      status: data.status || existing?.status || 'UNKNOWN',
      is_self: isSelf,
      last_heartbeat_at: data.last_heartbeat_at ?? existing?.last_heartbeat_at ?? null,
      last_healthcheck_at: data.last_healthcheck_at ?? existing?.last_healthcheck_at ?? null,
      healthcheck_error: data.healthcheck_error ?? existing?.healthcheck_error ?? null,
      metadata: toJsonValue(data.metadata ?? existing?.metadata ?? null),
      power_score: asNumber(data.power_score, existing?.power_score ?? 5)
    };

    const node = existing
      ? await this.updateNode(existing.id, payload, options)
      : await this.createNode(payload, options);

    if (node?.is_self) await this.enforceSingleSelf(node.id);
    else await this.enforceSingleSelf();
    return node;
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
    const now = new Date();
    const payload = {
      ...data,
      is_self: 1,
      status: data.status || 'ONLINE',
      last_healthcheck_at: now,
      last_heartbeat_at: now,
      healthcheck_error: null,
      power_score: asNumber(data.power_score, 5)
    };
    const node = await this.upsertClusterNode(payload);
    await this.enforceSingleSelf(node.id);
    return node;
  }

  async clearSelfFlag() { await db.execute('UPDATE cluster_nodes SET is_self = 0 WHERE is_self = 1'); }

  async createNode(payload, options = {}) {
    const [result] = await db.execute(`INSERT INTO cluster_nodes
      (node_uuid, node_name, tailscale_ip, public_url, port, role, status, is_self, last_heartbeat_at, last_healthcheck_at, healthcheck_error, metadata, power_score)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [payload.node_uuid || crypto.randomUUID(), payload.node_name, payload.tailscale_ip, payload.public_url, payload.port ?? null, payload.role, payload.status, Number(payload.is_self || 0), payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, toJsonValue(payload.metadata), payload.power_score ?? 5]);
    const node = await this.findById(result.insertId);
    if (node && !options.skipSyncEvent) {
      const syncPayload = await syncPayloadService.getClusterNodePayloadById(node.id);
      await syncEventService.createEntitySyncEvent('cluster_node', syncPayload);
    }
    return node;
  }

  async updateNode(id, payload, options = {}) {
    await db.execute(`UPDATE cluster_nodes SET node_uuid=?, node_name=?, tailscale_ip=?, public_url=?, port=?, role=?, status=?, is_self=?,
      last_heartbeat_at=?, last_healthcheck_at=?, healthcheck_error=?, metadata=?, power_score=? WHERE id=?`,
    [payload.node_uuid || crypto.randomUUID(), payload.node_name, payload.tailscale_ip, payload.public_url, payload.port ?? null, payload.role, payload.status, Number(payload.is_self || 0), payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, toJsonValue(payload.metadata), payload.power_score ?? 5, id]);
    const node = await this.findById(id);
    if (node && !options.skipSyncEvent) {
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
      (node_uuid, node_name, tailscale_ip, public_url, port, requested_role, power_score, status, request_token_hash, secret_fingerprint, requester_metadata)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)`,
    [payload.node_uuid || null, payload.node_name, payload.tailscale_ip, payload.public_url, payload.port ?? null, payload.requested_role, payload.power_score ?? 5, payload.request_token_hash, payload.secret_fingerprint, toJsonValue(payload.requester_metadata)]);
    return this.findJoinRequestById(result.insertId);
  }

  async updateJoinRequest(id, payload) {
    await db.execute(`UPDATE cluster_join_requests SET node_uuid=?, node_name=?, tailscale_ip=?, public_url=?, port=?, requested_role=?, power_score=?,
      request_token_hash=?, secret_fingerprint=?, requester_metadata=? WHERE id=?`,
    [payload.node_uuid || null, payload.node_name, payload.tailscale_ip, payload.public_url, payload.port ?? null, payload.requested_role, payload.power_score ?? 5, payload.request_token_hash, payload.secret_fingerprint, toJsonValue(payload.requester_metadata), id]);
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
