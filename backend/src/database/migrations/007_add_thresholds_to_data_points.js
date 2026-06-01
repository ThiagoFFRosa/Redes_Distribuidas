const columnExists = async (db, tableName, columnName) => {
  const [rows] = await db.execute(
    `SELECT COUNT(*) AS total
       FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND COLUMN_NAME = ?`,
    [tableName, columnName]
  );

  return Number(rows[0]?.total || 0) > 0;
};

const addColumnIfMissing = async (db, columnName, definition) => {
  if (await columnExists(db, 'data_points', columnName)) {
    console.log(`[migrate] data_points.${columnName} já existe, pulando.`);
    return;
  }

  await db.execute(`ALTER TABLE data_points ADD COLUMN ${columnName} ${definition}`);
};

module.exports = {
  id: '007_add_thresholds_to_data_points',
  up: async (db) => {
    await addColumnIfMissing(db, 'normal_level', 'DECIMAL(10,3) NULL AFTER status');
    await addColumnIfMissing(db, 'warning_level', 'DECIMAL(10,3) NULL AFTER normal_level');
    await addColumnIfMissing(db, 'critical_level', 'DECIMAL(10,3) NULL AFTER warning_level');
    await addColumnIfMissing(db, 'measurement_unit', "VARCHAR(30) NOT NULL DEFAULT 'm' AFTER critical_level");
  },
  down: async (db) => {
    for (const columnName of ['measurement_unit', 'critical_level', 'warning_level', 'normal_level']) {
      if (await columnExists(db, 'data_points', columnName)) {
        await db.execute(`ALTER TABLE data_points DROP COLUMN ${columnName}`);
      }
    }
  }
};
