const db = require('../database/connection');

const mapNode = (row) => ({ ...row, is_self: Number(row.is_self) });

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

  async findById(id) {
    const [rows] = await db.execute('SELECT * FROM cluster_nodes WHERE id = ? LIMIT 1', [id]);
    return rows[0] ? mapNode(rows[0]) : null;
  }

  async findByTailscaleIp(ip) {
    const [rows] = await db.execute('SELECT * FROM cluster_nodes WHERE tailscale_ip = ? LIMIT 1', [ip]);
    return rows[0] ? mapNode(rows[0]) : null;
  }

  async clearSelfFlag() {
    await db.execute('UPDATE cluster_nodes SET is_self = 0 WHERE is_self = 1');
  }

  async createNode(payload) {
    const [result] = await db.execute(`INSERT INTO cluster_nodes
      (node_name, tailscale_ip, public_url, role, status, is_self, last_heartbeat_at, last_healthcheck_at, healthcheck_error, metadata)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [payload.node_name, payload.tailscale_ip, payload.public_url, payload.role, payload.status, payload.is_self, payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, payload.metadata]);
    return this.findById(result.insertId);
  }

  async updateNode(id, payload) {
    await db.execute(`UPDATE cluster_nodes SET node_name=?, tailscale_ip=?, public_url=?, role=?, status=?, is_self=?,
      last_heartbeat_at=?, last_healthcheck_at=?, healthcheck_error=?, metadata=? WHERE id=?`,
    [payload.node_name, payload.tailscale_ip, payload.public_url, payload.role, payload.status, payload.is_self, payload.last_heartbeat_at, payload.last_healthcheck_at, payload.healthcheck_error, payload.metadata, id]);
    return this.findById(id);
  }

  async deleteNode(id) { await db.execute('DELETE FROM cluster_nodes WHERE id=?', [id]); }
}

module.exports = new ClusterNodeRepository();
