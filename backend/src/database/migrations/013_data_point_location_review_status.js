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

const addColumnIfMissing = async (db, tableName, columnName, ddl) => {
  if (!(await columnExists(db, tableName, columnName))) {
    await db.execute(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${ddl}`);
  }
};

module.exports = {
  id: '013_data_point_location_review_status',
  up: async (db) => {
    await db.execute('ALTER TABLE data_points MODIFY COLUMN latitude DECIMAL(10,7) NULL');
    await db.execute('ALTER TABLE data_points MODIFY COLUMN longitude DECIMAL(10,7) NULL');
    await addColumnIfMissing(db, 'data_points', 'location_status', "ENUM('VALID','NEEDS_REVIEW') NOT NULL DEFAULT 'VALID'");
    await addColumnIfMissing(db, 'data_points', 'location_error', 'VARCHAR(255) NULL');
    await db.execute(
      `UPDATE data_points
          SET location_status = CASE
                WHEN latitude IS NULL OR longitude IS NULL OR latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180 THEN 'NEEDS_REVIEW'
                ELSE 'VALID'
              END,
              location_error = CASE
                WHEN latitude IS NULL OR longitude IS NULL THEN COALESCE(location_error, 'Coordenadas ausentes')
                WHEN latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180 THEN COALESCE(location_error, 'Coordenadas inválidas')
                ELSE NULL
              END`
    );
  },
  down: async (db) => {
    if (await columnExists(db, 'data_points', 'location_error')) await db.execute('ALTER TABLE data_points DROP COLUMN location_error');
    if (await columnExists(db, 'data_points', 'location_status')) await db.execute('ALTER TABLE data_points DROP COLUMN location_status');
  }
};
