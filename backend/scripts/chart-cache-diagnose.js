const pool = require('../src/database/connection');

const printRows = (title, rows) => {
  console.log(`\n[chart-cache:diagnose] ${title}: ${rows.length}`);
  if (rows.length) console.table(rows);
};

const diagnose = async () => {
  try {
    const [orphanCaches] = await pool.execute(`
      SELECT cc.uuid, cc.data_point_uuid, cc.chart_type, cc.generated_at
        FROM chart_cache cc
        LEFT JOIN data_points dp ON dp.uuid = cc.data_point_uuid
       WHERE dp.uuid IS NULL
       ORDER BY cc.generated_at DESC, cc.id DESC
    `);

    const [duplicateCaches] = await pool.execute(`
      SELECT data_point_uuid, chart_type, COUNT(*) total
        FROM chart_cache
       GROUP BY data_point_uuid, chart_type
      HAVING COUNT(*) > 1
       ORDER BY total DESC, data_point_uuid, chart_type
    `);

    const [orphanJobs] = await pool.execute(`
      SELECT cj.uuid, cj.data_point_uuid, cj.chart_type, cj.status, cj.created_at
        FROM chart_generation_jobs cj
        LEFT JOIN data_points dp ON dp.uuid = cj.data_point_uuid
       WHERE dp.uuid IS NULL
       ORDER BY cj.created_at DESC, cj.id DESC
    `);

    printRows('chart_cache órfão sem data_point_uuid existente', orphanCaches);
    printRows('chart_cache duplicado por data_point_uuid + chart_type', duplicateCaches);
    printRows('chart_generation_jobs órfão sem data_point_uuid existente', orphanJobs);
  } catch (error) {
    console.error('[chart-cache:diagnose] falha:', error.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
};

diagnose();
