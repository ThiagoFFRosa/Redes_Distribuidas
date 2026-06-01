const points = [
  {
    name: 'Rio Paraíba do Sul - Centro', latitude: -23.1868, longitude: -45.8866, cityRegion: 'SJC - SP', description: 'Ponto central de monitoramento do Rio Paraíba do Sul.',
    normalLevel: 2.0, warningLevel: 3.5, criticalLevel: 5.0, measurementUnit: 'm'
  },
  {
    name: 'Rio Tietê - Ponto 42', latitude: -23.518, longitude: -46.732, cityRegion: 'São Paulo - SP', description: 'Ponto urbano de acompanhamento do nível do Rio Tietê.',
    normalLevel: 1.8, warningLevel: 3.0, criticalLevel: 4.5, measurementUnit: 'm'
  },
  {
    name: 'Represa Guarapiranga', latitude: -23.682, longitude: -46.733, cityRegion: 'São Paulo - SP', description: 'Monitoramento de variação de nível da represa.',
    normalLevel: 3.0, warningLevel: 4.0, criticalLevel: 4.8, measurementUnit: 'm'
  },
  {
    name: 'Rio Una - Ponte Nova', latitude: -23.03, longitude: -45.56, cityRegion: 'Taubaté - SP', description: 'Ponto de risco próximo à Ponte Nova.',
    normalLevel: 2.2, warningLevel: 3.2, criticalLevel: 5.0, measurementUnit: 'm'
  }
];

const nowMinusHours = (hours) => new Date(Date.now() - hours * 60 * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ');

module.exports = {
  id: '002_monitoring_demo',
  run: async (connection) => {
    await connection.execute('SELECT 1 FROM data_points LIMIT 1');
    const pointIds = new Map();

    for (const point of points) {
      const [existing] = await connection.execute('SELECT id FROM data_points WHERE name = ? LIMIT 1', [point.name]);
      if (existing.length) {
        pointIds.set(point.name, existing[0].id);
        await connection.execute(
          `UPDATE data_points
              SET normal_level = COALESCE(normal_level, ?),
                  warning_level = COALESCE(warning_level, ?),
                  critical_level = COALESCE(critical_level, ?),
                  measurement_unit = COALESCE(NULLIF(measurement_unit, ''), ?)
            WHERE id = ?`,
          [point.normalLevel, point.warningLevel, point.criticalLevel, point.measurementUnit, existing[0].id]
        );
        continue;
      }
      const [result] = await connection.execute(
        `INSERT INTO data_points (name, type, latitude, longitude, city_region, description, status, normal_level, warning_level, critical_level, measurement_unit)
         VALUES (?, 'RIVER_LEVEL', ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?)`,
        [point.name, point.latitude, point.longitude, point.cityRegion, point.description, point.normalLevel, point.warningLevel, point.criticalLevel, point.measurementUnit]
      );
      pointIds.set(point.name, result.insertId);
      console.log(`[seed] ponto criado: ${point.name}`);
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
      ['Rio Tietê - Ponto 42', 'RIVER_LEVEL_HIGH', 'ATTENTION', 3.25, 'Nível acima do limite de risco'],
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
        [pointId, alertType, severity, value, message, nowMinusHours(severity === 'CRITICAL' ? 1 : 2)]
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
