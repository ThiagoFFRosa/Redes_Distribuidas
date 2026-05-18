const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const pool = require('../database/connection');
const env = require('../config/env');

const authenticateWithEmail = async (email, password) => {
  const [rows] = await pool.execute(
    'SELECT id, name, email, password_hash, role, is_active FROM users WHERE email = ? LIMIT 1',
    [email]
  );

  const user = rows[0];
  if (!user || user.is_active !== 1) {
    return null;
  }

  const isValid = await bcrypt.compare(password, user.password_hash);
  if (!isValid) {
    return null;
  }

  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role
  };
};

const signToken = (user) => {
  return jwt.sign(user, env.jwtSecret, { expiresIn: '1d' });
};

const requireAuth = (req, res, next) => {
  if (req.session && req.session.user) {
    return next();
  }

  const authHeader = req.headers.authorization || '';
  const [scheme, token] = authHeader.split(' ');

  if (scheme === 'Bearer' && token) {
    try {
      req.user = jwt.verify(token, env.jwtSecret);
      return next();
    } catch (_error) {
      // segue para resposta de não autenticado
    }
  }

  if (req.path.endsWith('.html')) {
    return res.redirect('/login.html');
  }

  return res.status(401).json({ message: 'Não autenticado.' });
};

module.exports = {
  authenticateWithEmail,
  signToken,
  requireAuth
};
