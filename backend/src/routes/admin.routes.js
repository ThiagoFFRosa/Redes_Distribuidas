const express = require('express');
const env = require('../config/env');
const { requireAuth } = require('../services/auth.service');
const dbAdmin = require('../services/db-admin.service');

const router = express.Router();

const hasClusterSecret = (req) => {
  const secret = req.header('x-cluster-secret') || req.header('x-cluster-key');
  return Boolean(secret && [env.sessionSecret, env.clusterKey].filter(Boolean).includes(secret));
};

const requireAuthOrClusterSecret = (req, res, next) => {
  if (hasClusterSecret(req)) return next();
  return requireAuth(req, res, next);
};

router.get('/db-counts', requireAuthOrClusterSecret, async (_req, res, next) => {
  try { res.json(await dbAdmin.getDbCounts()); } catch (error) { next(error); }
});

router.post('/dev/clear-all', requireAuthOrClusterSecret, async (req, res, next) => {
  try {
    if (process.env.NODE_ENV !== 'development') {
      return res.status(403).json({ ok: false, message: 'Endpoint disponível somente com NODE_ENV=development.' });
    }
    res.json(await dbAdmin.clearAll({
      yes: req.body?.yes === true,
      keepUsers: req.body?.keep_users === true,
      keepSelf: req.body?.keep_self === true,
      dataOnly: req.body?.data_only === true,
      syncOnly: req.body?.sync_only === true
    }));
  } catch (error) { next(error); }
});

module.exports = router;
