const bcrypt = require('bcryptjs');

module.exports = {
  id: '001_admin_user',
  run: async (connection) => {
    const [existing] = await connection.execute('SELECT id FROM users WHERE email = ? LIMIT 1', ['admin@local.test']);

    if (existing.length > 0) {
      console.log('Admin já existe, ignorando seed.');
      return;
    }

    const passwordHash = await bcrypt.hash('admin123', 10);

    await connection.execute(
      `INSERT INTO users (name, email, password_hash, role, is_active)
       VALUES (?, ?, ?, ?, ?)`,
      ['Administrador', 'admin@local.test', passwordHash, 'admin', 1]
    );

    console.log('Admin criado com sucesso.');
  }
};
