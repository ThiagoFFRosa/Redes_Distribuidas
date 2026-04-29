#!/usr/bin/env node

const { runInmetEndpointTests } = require('../backend/src/services/inmet-test.service');

const STATION_ID = 'A769';
const DATE = '2026-04-28';

async function main() {
  console.log('Teste de endpoints INMET');
  console.log(`Estação: ${STATION_ID}`);
  console.log(`Data de referência: ${DATE}`);

  const result = await runInmetEndpointTests({ stationCode: STATION_ID, date: DATE });

  result.results.forEach((item) => {
    console.log('\n============================================================');
    console.log(`Endpoint: ${item.url}`);
    console.log(`Status HTTP: ${item.status}`);
    console.log(`Content-Type: ${item.contentType}`);
    if (item.error) console.log(`Erro: ${item.error}`);
    if (item.sample) {
      console.log('Prévia da resposta (até 3 itens/linhas):');
      console.log(JSON.stringify(item.sample, null, 2));
    }
    console.log(`Contém CHUVA, DT_MEDICAO, HR_MEDICAO e CD_ESTACAO? ${item.hasUsefulFields ? 'SIM' : 'NÃO'}`);
  });

  const matches = result.results.filter((r) => r.hasUsefulFields);
  console.log('\n========================== RESUMO ==========================');
  if (!matches.length) {
    console.log('Nenhum endpoint retornou todos os campos obrigatórios.');
    return;
  }

  console.log('Endpoint(s) que retornaram os 4 campos obrigatórios:');
  matches.forEach((item) => console.log(`- ${item.url}`));
}

main().catch((error) => {
  console.error('Falha inesperada no script:', error);
  process.exitCode = 1;
});
