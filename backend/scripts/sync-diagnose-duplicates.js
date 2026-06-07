const pool = require('../src/database/connection');

const printRows = (title, rows) => {
  console.log(`\n=== ${title} (${rows.length}) ===`);
  if (!rows.length) return console.log('OK');
  console.table(rows);
};

const main = async () => {
  const [points] = await pool.execute(`
    SELECT id, uuid, source_key, name, city_region, status, created_at, updated_at
    FROM data_points
    ORDER BY name, id
  `);
  printRows('data_points', points);

  const [duplicateNaturalKeys] = await pool.execute(`
    SELECT LOWER(TRIM(name)) normalized_name, COALESCE(city_region, '') city_region, type,
           COUNT(*) total, GROUP_CONCAT(uuid ORDER BY id SEPARATOR ', ') uuids
    FROM data_points
    GROUP BY LOWER(TRIM(name)), COALESCE(city_region, ''), type
    HAVING COUNT(*) > 1
  `);
  printRows('data_points duplicados por nome/cidade/tipo', duplicateNaturalKeys);

  const [duplicateSourceKeys] = await pool.execute(`
    SELECT source_key, COUNT(*) total, GROUP_CONCAT(uuid ORDER BY id SEPARATOR ', ') uuids
    FROM data_points
    WHERE source_key IS NOT NULL AND source_key <> ''
    GROUP BY source_key
    HAVING COUNT(*) > 1
  `);
  printRows('data_points duplicados por source_key', duplicateSourceKeys);

  const [jobs] = await pool.execute(`
    SELECT uuid, data_point_uuid, status, assigned_to_node_uuid, created_at, updated_at
    FROM chart_generation_jobs
    ORDER BY id DESC
    LIMIT 10
  `);
  printRows('últimos chart_generation_jobs', jobs);

  const [cache] = await pool.execute(`
    SELECT uuid, data_point_uuid, source_job_uuid, chart_type, generated_at
    FROM chart_cache
    ORDER BY id DESC
    LIMIT 10
  `);
  printRows('últimos chart_cache', cache);

  const [historicalGroups] = await pool.execute(`
    SELECT dp.uuid AS data_point_uuid, hi.uuid AS import_uuid, COUNT(*) total
    FROM historical_measurements hm
    JOIN data_points dp ON dp.id = hm.data_point_id
    LEFT JOIN historical_imports hi ON hi.id = hm.import_id
    GROUP BY dp.uuid, hi.uuid
    ORDER BY total DESC
    LIMIT 50
  `);
  printRows('historical_measurements por data_point_uuid/import_uuid', historicalGroups);

  const [orphanHistorical] = await pool.execute(`
    SELECT hm.data_point_id, COUNT(*) total
    FROM historical_measurements hm
    LEFT JOIN data_points dp ON dp.id = hm.data_point_id
    WHERE dp.id IS NULL
    GROUP BY hm.data_point_id
  `);
  printRows('historical_measurements apontando para data_point inexistente', orphanHistorical);

  const [orphanCache] = await pool.execute(`
    SELECT cc.data_point_uuid, COUNT(*) total
    FROM chart_cache cc
    LEFT JOIN data_points dp ON dp.uuid = cc.data_point_uuid
    WHERE cc.data_point_uuid IS NOT NULL AND dp.uuid IS NULL
    GROUP BY cc.data_point_uuid
  `);
  printRows('chart_cache apontando para data_point_uuid inexistente', orphanCache);

  const [orphanJobs] = await pool.execute(`
    SELECT cj.data_point_uuid, COUNT(*) total
    FROM chart_generation_jobs cj
    LEFT JOIN data_points dp ON dp.uuid = cj.data_point_uuid
    WHERE cj.data_point_uuid IS NOT NULL AND dp.uuid IS NULL
    GROUP BY cj.data_point_uuid
  `);
  printRows('chart_generation_jobs apontando para data_point_uuid inexistente', orphanJobs);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
}).finally(() => pool.end());
