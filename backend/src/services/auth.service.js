const env = require('../config/env');

const validateLogin = (username, password) => {
  return username === env.adminUser && password === env.adminPassword;
};

const requireAuth = (req, res, next) => {
  if (req.session && req.session.user) {
    return next();
  }

  if (req.path.endsWith('.html')) {
    return res.redirect('/login.html');
  }

  return res.status(401).json({ message: 'Não autenticado.' });
};

module.exports = {
  validateLogin,
  requireAuth
};
