const express = require('express');
const { validateLogin } = require('../services/auth.service');

const router = express.Router();

router.post('/login', (req, res) => {
  const { username, password } = req.body;

  if (!validateLogin(username, password)) {
    return res.status(401).json({ message: 'Usuário ou senha inválidos.' });
  }

  req.session.user = { username };
  return res.json({ ok: true, user: req.session.user });
});

router.post('/logout', (req, res) => {
  req.session.destroy(() => {
    res.clearCookie('connect.sid');
    res.json({ ok: true });
  });
});

router.get('/me', (req, res) => {
  if (!req.session.user) {
    return res.status(401).json({ message: 'Não autenticado.' });
  }

  return res.json({ ok: true, user: req.session.user });
});

module.exports = router;
