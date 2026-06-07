const pool = require('../src/database/connection');

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, ...rest] = arg.replace(/^--/, '').split('=');
  return [key, rest.join('=') || true];
}));

const main = async () => {
  const fromUuid = args.from;
  const toUuid = args.to;
  if (!fromUuid || !toUuid || fromUuid === toUuid) {
    throw new Error('Uso: npm run sync:merge-data-points -- --from=<uuid_errado> --to=<uuid_correto>');
  }
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    const [[fromPoint]] = await connection.execute('SELECT * FROM data_points WHERE uuid=? FOR UPDATE', [fromUuid]);
    const [[toPoint]] = await connection.execute('SELECT * FROM data_points WHERE uuid=? FOR UPDATE', [toUuid]);
    if (!fromPoint) throw new Error(`data_point origem não encontrado: ${fromUuid}`);
    if (!toPoint) throw new Error(`data_point destino não encontrado: ${toUuid}`);

    await connection.execute(`
      DELETE hm FROM historical_measurements hm
      JOIN historical_measurements existing
        ON existing.data_point_id=? AND existing.measured_at=hm.measured_at
      WHERE hm.data_point_id=?
    `, [toPoint.id, fromPoint.id]);
    const [hm] = await connection.execute('UPDATE historical_measurements SET data_point_id=? WHERE data_point_id=?', [toPoint.id, fromPoint.id]);

    const [jobs] = await connection.execute('UPDATE chart_generation_jobs SET data_point_id=?, data_point_uuid=? WHERE data_point_id=? OR data_point_uuid=?', [toPoint.id, toUuid, fromPoint.id, fromUuid]);

    await connection.execute('UPDATE chart_cache SET data_point_uuid=? WHERE data_point_uuid=?', [toUuid, fromUuid]);
    await connection.execute(`
      DELETE cc FROM chart_cache cc
      JOIN chart_cache existing
        ON existing.data_point_id=? AND existing.chart_type=cc.chart_type
      WHERE cc.data_point_id=?
    `, [toPoint.id, fromPoint.id]);
    const [cache] = await connection.execute('UPDATE chart_cache SET data_point_id=?, data_point_uuid=? WHERE data_point_id=? OR data_point_uuid=?', [toPoint.id, toUuid, fromPoint.id, fromUuid]);

    const [measurements] = await connection.execute('UPDATE measurements SET data_point_id=? WHERE data_point_id=?', [toPoint.id, fromPoint.id]);
    const [alerts] = await connection.execute('UPDATE alerts SET data_point_id=? WHERE data_point_id=?', [toPoint.id, fromPoint.id]);
    const [imports] = await connection.execute('UPDATE historical_imports SET data_point_id=? WHERE data_point_id=?', [toPoint.id, fromPoint.id]);
    await connection.execute('UPDATE data_points SET status=\'INACTIVE\' WHERE id=?', [fromPoint.id]);
    await connection.commit();
    console.log(JSON.stringify({ ok: true, from: fromUuid, to: toUuid, updated: {
      historical_measurements: hm.affectedRows,
      chart_generation_jobs: jobs.affectedRows,
      chart_cache: cache.affectedRows,
      measurements: measurements.affectedRows,
      alerts: alerts.affectedRows,
      historical_imports: imports.affectedRows,
      deactivated_data_point_id: fromPoint.id
    } }, null, 2));
  } catch (error) {
    await connection.rollback().catch(() => {});
    throw error;
  } finally {
    connection.release();
  }
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
}).finally(() => pool.end());
