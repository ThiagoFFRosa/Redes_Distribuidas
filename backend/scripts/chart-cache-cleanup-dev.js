const pool = require('../src/database/connection');

const cleanupDev = async () => {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const [orphanCachesResult] = await connection.execute(`
      DELETE cc
        FROM chart_cache cc
        LEFT JOIN data_points dp ON dp.uuid = cc.data_point_uuid
       WHERE dp.uuid IS NULL
    `);

    const [orphanJobsResult] = await connection.execute(`
      DELETE cj
        FROM chart_generation_jobs cj
        LEFT JOIN data_points dp ON dp.uuid = cj.data_point_uuid
       WHERE dp.uuid IS NULL
    `);

    const [duplicateGroups] = await connection.execute(`
      SELECT data_point_uuid, chart_type, COUNT(*) AS total
        FROM chart_cache
       WHERE data_point_uuid IS NOT NULL
       GROUP BY data_point_uuid, chart_type
      HAVING COUNT(*) > 1
    `);

    const [duplicateRowsResult] = await connection.execute(`
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
    console.log(`[chart-cache:cleanup-dev] chart_cache órfãos removidos: ${orphanCachesResult.affectedRows || 0}`);
    console.log(`[chart-cache:cleanup-dev] chart_generation_jobs órfãos removidos: ${orphanJobsResult.affectedRows || 0}`);
    console.log(`[chart-cache:cleanup-dev] grupos duplicados encontrados: ${duplicateGroups.length}`);
    console.log(`[chart-cache:cleanup-dev] caches duplicados antigos removidos: ${duplicateRowsResult.affectedRows || 0}`);
  } catch (error) {
    await connection.rollback().catch(() => {});
    console.error('[chart-cache:cleanup-dev] falha:', error.message);
    process.exitCode = 1;
  } finally {
    connection.release();
    await pool.end();
  }
};

cleanupDev();
