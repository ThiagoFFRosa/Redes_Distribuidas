const points = [
  ['Rio Paraíba do Sul - Centro', -23.1868, -45.8866, 'SJC - SP', 'Ponto central de monitoramento do Rio Paraíba do Sul.'],
  ['Rio Tietê - Ponto 42', -23.518, -46.732, 'São Paulo - SP', 'Ponto urbano de acompanhamento do nível do Rio Tietê.'],
  ['Represa Guarapiranga', -23.682, -46.733, 'São Paulo - SP', 'Monitoramento de variação de nível da represa.'],
  ['Rio Una - Ponte Nova', -23.03, -45.56, 'Taubaté - SP', 'Ponto de risco próximo à Ponte Nova.']
];

const nowMinusHours = (hours) => new Date(Date.now() - hours * 60 * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ');

module.exports = {
  id: '002_monitoring_demo',
  run: async (connection) => {
    await connection.execute('SELECT 1 FROM data_points LIMIT 1');
    const pointIds = new Map();

    for (const [name, latitude, longitude, cityRegion, description] of points) {
      const [existing] = await connection.execute('SELECT id FROM data_points WHERE name = ? LIMIT 1', [name]);
      if (existing.length) {
        pointIds.set(name, existing[0].id);
        continue;
      }
      const [result] = await connection.execute(
        `INSERT INTO data_points (name, type, latitude, longitude, city_region, description, status)
         VALUES (?, 'RIVER_LEVEL', ?, ?, ?, ?, 'ACTIVE')`,
        [name, latitude, longitude, cityRegion, description]
      );
      pointIds.set(name, result.insertId);
      console.log(`[seed] ponto criado: ${name}`);
    }

    const measurements = [
      ['Rio Paraíba do Sul - Centro', 2.15, 8], ['Rio Paraíba do Sul - Centro', 2.3, 4],
      ['Rio Tietê - Ponto 42', 3.1, 6], ['Rio Tietê - Ponto 42', 3.25, 2],
      ['Represa Guarapiranga', 3.8, 3], ['Rio Una - Ponte Nova', 5.15, 1]
    ];

    for (const [name, value, hoursAgo] of measurements) {
      const pointId = pointIds.get(name);
      const measuredAt = nowMinusHours(hoursAgo);
      const [existing] = await connection.execute(
        'SELECT id FROM measurements WHERE data_point_id = ? AND value = ? AND measured_at = ? LIMIT 1',
        [pointId, value, measuredAt]
      );
      if (existing.length) continue;
      await connection.execute(
        `INSERT INTO measurements (data_point_id, measurement_type, value, unit, measured_at, source, observation)
         VALUES (?, 'RIVER_LEVEL', ?, 'm', ?, 'MANUAL', ?)`,
        [pointId, value, measuredAt, 'Leitura inicial de demonstração']
      );
    }

    const alertSeeds = [
      ['Represa Guarapiranga', 'RIVER_LEVEL_HIGH', 'ATTENTION', 3.8, 'Nível acima do normal'],
      ['Rio Una - Ponte Nova', 'RIVER_LEVEL_CRITICAL', 'CRITICAL', 5.15, 'Nível crítico detectado']
    ];

    for (const [name, alertType, severity, value, message] of alertSeeds) {
      const pointId = pointIds.get(name);
      const [existing] = await connection.execute(
        'SELECT id FROM alerts WHERE data_point_id = ? AND severity = ? AND status = ? LIMIT 1',
        [pointId, severity, 'ACTIVE']
      );
      if (existing.length) continue;
      await connection.execute(
        `INSERT INTO alerts (data_point_id, alert_type, severity, current_value, unit, message, status, detected_at)
         VALUES (?, ?, ?, ?, 'm', ?, 'ACTIVE', ?)`,
        [pointId, alertType, severity, value, message, nowMinusHours(severity === 'CRITICAL' ? 1 : 3)]
      );
    }

    const logs = [
      ['SYSTEM', 'PROCESSED', 'sistema iniciado'],
      ['MEASUREMENT_CREATED', 'PERSISTED', 'medição manual persistida'],
      ['ALERT_CREATED', 'PROCESSED', 'alerta operacional gerado']
    ];
    for (const [eventType, status, message] of logs) {
      const [existing] = await connection.execute('SELECT id FROM event_queue_logs WHERE event_type = ? AND message = ? LIMIT 1', [eventType, message]);
      if (existing.length) continue;
      await connection.execute(
        `INSERT INTO event_queue_logs (event_type, status, payload, message, processed_at)
         VALUES (?, ?, JSON_OBJECT('seed', true), ?, NOW())`,
        [eventType, status, message]
      );
    }

    console.log('[seed] dados de monitoramento prontos.');
  }
};
