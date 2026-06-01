const express = require('express');
const { requireAuth } = require('../services/auth.service');
const importService = require('../services/historical-import.service');

const router = express.Router();
router.use(requireAuth);

router.post('/historical-csv', async (req, res, next) => {
  try {
    const result = await importService.importHistoricalCsv(req, req.user?.id || req.session?.user?.id || null);
    res.status(201).json(result);
  } catch (error) {
    if (error.message?.includes('CSV') || error.message?.includes('arquivo') || error.message?.includes('multipart') || error.message?.includes('Ponto')) {
      return res.status(400).json({ ok: false, message: error.message });
    }
    next(error);
  }
});

router.get('/', async (_req, res, next) => {
  try { res.json({ ok: true, imports: await importService.listImports() }); }
  catch (error) { next(error); }
});

router.get('/:id', async (req, res, next) => {
  try {
    const item = await importService.getImport(req.params.id);
    if (!item) return res.status(404).json({ ok: false, message: 'Importação não encontrada.' });
    res.json({ ok: true, import: item });
  } catch (error) { next(error); }
});

module.exports = router;
