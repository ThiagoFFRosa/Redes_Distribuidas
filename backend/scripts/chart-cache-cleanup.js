const pool = require('../src/database/connection');

const cleanup = async () => {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const [duplicates] = await connection.execute(`
      SELECT data_point_uuid, chart_type, COUNT(*) AS total
        FROM chart_cache
       WHERE data_point_uuid IS NOT NULL
       GROUP BY data_point_uuid, chart_type
      HAVING COUNT(*) > 1
    `);

    const [result] = await connection.execute(`
      DELETE older
        FROM chart_cache older
        JOIN chart_cache newer
          ON newer.data_point_uuid = older.data_point_uuid
         AND newer.chart_type = older.chart_type
         AND (
              COALESCE(newer.generated_at, '1970-01-01 00:00:00') > COALESCE(older.generated_at, '1970-01-01 00:00:00')
           OR (
                COALESCE(newer.generated_at, '1970-01-01 00:00:00') = COALESCE(older.generated_at, '1970-01-01 00:00:00')
            AND newer.id > older.id
              )
         )
       WHERE older.data_point_uuid IS NOT NULL
    `);

    await connection.commit();
    console.log(`[chart-cache:cleanup] grupos duplicados encontrados: ${duplicates.length}`);
    console.log(`[chart-cache:cleanup] caches antigos removidos: ${result.affectedRows || 0}`);
  } catch (error) {
    await connection.rollback().catch(() => {});
    console.error('[chart-cache:cleanup] falha:', error.message);
    process.exitCode = 1;
  } finally {
    connection.release();
    await pool.end();
  }
};

cleanup();
