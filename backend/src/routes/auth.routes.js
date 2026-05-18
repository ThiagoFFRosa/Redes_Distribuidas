const express = require('express');
const { authenticateWithEmail, signToken } = require('../services/auth.service');

const router = express.Router();

router.post('/login', async (req, res, next) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(401).json({ message: 'Credenciais inválidas.' });
    }

    const user = await authenticateWithEmail(email, password);

    if (!user) {
      return res.status(401).json({ message: 'Credenciais inválidas.' });
    }

    const token = signToken(user);
    req.session.user = user;

    return res.json({ ok: true, user, token });
  } catch (error) {
    return next(error);
  }
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
