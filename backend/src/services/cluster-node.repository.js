const crypto = require('crypto');
const db = require('../database/connection');
const syncEventService = require('./sync-event.service');
const syncPayloadService = require('./sync-payload.service');
const { toMysqlDateTime } = require('../utils/mysql-date');
const logger = require('../utils/logger');

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


const normalizeMetadata = (value) => {
  if (value === undefined || value === null || value === '') return {};
  let parsed = value;
  if (typeof value === 'string') {
    try { parsed = JSON.parse(value); } catch (_error) { return value; }
  }
  if (Array.isArray(parsed)) return parsed.map(normalizeMetadata);
  if (parsed && typeof parsed === 'object') {
    return Object.keys(parsed).sort().reduce((acc, key) => {
      acc[key] = normalizeMetadata(parsed[key]);
      return acc;
    }, {});
  }
  return parsed;
};

const pickClusterNodeStructuralFields = (node = {}) => ({
  node_uuid: node.node_uuid || null,
  node_name: node.node_name || null,
  tailscale_ip: node.tailscale_ip || null,
  public_url: node.public_url || null,
  port: node.port == null ? null : Number(node.port),
  role: node.role || null,
  power_score: node.power_score == null ? 5 : Number(node.power_score),
  metadata: normalizeMetadata(node.metadata)
});

const hasStructuralChange = (before, after) => JSON.stringify(pickClusterNodeStructuralFields(before)) !== JSON.stringify(pickClusterNodeStructuralFields(after));

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
      public_url: matchesLocalSelf ? (existing?.public_url ?? data.public_url ?? null) : (data.public_url ?? existing?.public_url ?? null),
      port: asNumber(data.port, existing?.port ?? null),
      role: data.role || existing?.role || 'UNKNOWN',
      status: data.status || existing?.status || 'UNKNOWN',
      is_self: isSelf,
      last_heartbeat_at: toMysqlDateTime(data.last_heartbeat_at) ?? existing?.last_heartbeat_at ?? null,
      last_healthcheck_at: toMysqlDateTime(data.last_healthcheck_at) ?? existing?.last_healthcheck_at ?? null,
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

  async updateStatus(id, status, error = null, options = {}) {
    const current = await this.findById(id);
    if (!current) return null;
    const now = new Date();
    return this.updateNodeHealthStatus(id, {
      status,
      last_healthcheck_at: now,
      last_heartbeat_at: status === 'ONLINE' ? now : current.last_heartbeat_at,
      healthcheck_error: error
    }, { ...options, skipSyncEvent: true });
  }

  async setSelfNode(data, options = {}) {
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
    const node = await this.upsertClusterNode(payload, options);
    await this.enforceSingleSelf(node.id);
    return node;
  }

  async clearSelfFlag() { await db.execute('UPDATE cluster_nodes SET is_self = 0 WHERE is_self = 1'); }

  async maybeCreateClusterNodeSyncEvent(before, after, options = {}) {
    if (!after) return null;
    if (options.skipSyncEvent) return null;
    if (before && !hasStructuralChange(before, after)) return null;
    const syncPayload = await syncPayloadService.getClusterNodePayloadById(after.id);
    if (!syncPayload) return null;
    return syncEventService.createEntitySyncEvent('cluster_node', syncPayload, 'UPSERT', db, { reason: options.reason || 'manual-edit' });
  }

  async createNode(payload, options = {}) {
    const [result] = await db.execute(`INSERT INTO cluster_nodes
      (node_uuid, node_name, tailscale_ip, public_url, port, role, status, is_self, last_heartbeat_at, last_healthcheck_at, healthcheck_error, metadata, power_score)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [payload.node_uuid || crypto.randomUUID(), payload.node_name, payload.tailscale_ip, payload.public_url, payload.port ?? null, payload.role, payload.status, Number(payload.is_self || 0), payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, toJsonValue(payload.metadata), payload.power_score ?? 5]);
    const node = await this.findById(result.insertId);
    await this.maybeCreateClusterNodeSyncEvent(null, node, options);
    return node;
  }

  async updateNode(id, payload, options = {}) {
    const before = await this.findById(id);
    if (!before) return null;
    await db.execute(`UPDATE cluster_nodes SET node_uuid=?, node_name=?, tailscale_ip=?, public_url=?, port=?, role=?, status=?, is_self=?,
      last_heartbeat_at=?, last_healthcheck_at=?, healthcheck_error=?, metadata=?, power_score=? WHERE id=?`,
    [payload.node_uuid || before.node_uuid || crypto.randomUUID(), payload.node_name, payload.tailscale_ip, payload.public_url, payload.port ?? null, payload.role, payload.status, Number(payload.is_self || 0), payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, toJsonValue(payload.metadata), payload.power_score ?? 5, id]);
    const node = await this.findById(id);
    await this.maybeCreateClusterNodeSyncEvent(before, node, options);
    return node;
  }

  async updateNodeStructuralData(id, data, options = {}) {
    const current = await this.findById(id);
    if (!current) return null;
    return this.updateNode(id, { ...current, ...data }, options);
  }

  async updateNodeHealthStatus(id, data, options = {}) {
    const current = await this.findById(id);
    if (!current) return null;
    const payload = {
      status: data.status ?? current.status,
      last_heartbeat_at: Object.prototype.hasOwnProperty.call(data, 'last_heartbeat_at') ? data.last_heartbeat_at : current.last_heartbeat_at,
      last_healthcheck_at: Object.prototype.hasOwnProperty.call(data, 'last_healthcheck_at') ? data.last_healthcheck_at : current.last_healthcheck_at,
      healthcheck_error: Object.prototype.hasOwnProperty.call(data, 'healthcheck_error') ? data.healthcheck_error : current.healthcheck_error
    };
    await db.execute(`UPDATE cluster_nodes
      SET status=?, last_heartbeat_at=?, last_healthcheck_at=?, healthcheck_error=?
      WHERE id=?`,
    [payload.status, payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, id]);
    const node = await this.findById(id);
    const reason = options.reason || 'healthcheck';
    if (reason === 'heartbeat') logger.debug('[heartbeat] self marcado ONLINE sem gerar sync_event');
    else logger.debug(`[${reason}] status atualizado para ${node.node_name} sem gerar sync_event`);
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
