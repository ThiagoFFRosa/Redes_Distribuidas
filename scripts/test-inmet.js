#!/usr/bin/env node

/**
 * Testa endpoints públicos do INMET para descobrir qual rota
 * retorna dados horários com os campos desejados.
 */

const STATION_ID = 'A769';
const DATE = '2026-04-28';
const REQUIRED_FIELDS = ['CHUVA', 'DT_MEDICAO', 'HR_MEDICAO', 'CD_ESTACAO'];

const endpoints = [
  `https://apitempo.inmet.gov.br/estacao/${STATION_ID}`,
  `https://apitempo.inmet.gov.br/estacao/dados/${STATION_ID}`,
  `https://apitempo.inmet.gov.br/estacao/dados/${DATE}/${STATION_ID}`,
  `https://apitempo.inmet.gov.br/estacoes/dados/${DATE}`,
  `https://apitempo.inmet.gov.br/estacoes/T/${DATE}`,
  `https://apitempo.inmet.gov.br/estacoes/${STATION_ID}/${DATE}`,
  `https://apitempo.inmet.gov.br/estacao/${DATE}/${STATION_ID}`,
];

function isJsonContentType(contentType) {
  return contentType && contentType.toLowerCase().includes('application/json');
}

function findRequiredFields(data) {
  const seen = new Set();

  function visit(value, depth = 0) {
    if (depth > 5 || value == null) return;

    if (Array.isArray(value)) {
      for (const item of value.slice(0, 50)) {
        visit(item, depth + 1);
      }
      return;
    }

    if (typeof value === 'object') {
      for (const [key, nested] of Object.entries(value)) {
        if (REQUIRED_FIELDS.includes(key)) {
          seen.add(key);
        }
        visit(nested, depth + 1);
      }
    }
  }

  visit(data);
  return REQUIRED_FIELDS.every((field) => seen.has(field));
}

function previewData(data) {
  if (Array.isArray(data)) {
    return data.slice(0, 3);
  }

  if (data && typeof data === 'object') {
    return Object.entries(data).slice(0, 3).reduce((acc, [key, value]) => {
      acc[key] = value;
      return acc;
    }, {});
  }

  return data;
}

async function testEndpoint(url) {
  console.log('\n============================================================');
  console.log(`Endpoint: ${url}`);

  try {
    const response = await fetch(url);
    const contentType = response.headers.get('content-type') || '(não informado)';

    console.log(`Status HTTP: ${response.status} ${response.statusText}`);
    console.log(`Content-Type: ${contentType}`);

    if (!isJsonContentType(contentType)) {
      const text = await response.text();
      console.log('Resposta não JSON (primeiros 300 chars):');
      console.log((text || '').slice(0, 300));
      return { url, ok: false, hasRequiredFields: false, reason: 'non-json' };
    }

    const data = await response.json();

    console.log('Prévia da resposta (até 3 itens/linhas):');
    console.log(JSON.stringify(previewData(data), null, 2));

    const hasRequiredFields = findRequiredFields(data);
    console.log(`Contém CHUVA, DT_MEDICAO, HR_MEDICAO e CD_ESTACAO? ${hasRequiredFields ? 'SIM' : 'NÃO'}`);

    return { url, ok: response.ok, hasRequiredFields, status: response.status };
  } catch (error) {
    console.log(`Erro ao consultar endpoint: ${error.message}`);
    return { url, ok: false, hasRequiredFields: false, reason: error.message };
  }
}

async function main() {
  console.log('Teste de endpoints INMET');
  console.log(`Estação: ${STATION_ID}`);
  console.log(`Data de referência: ${DATE}`);

  const results = [];

  for (const endpoint of endpoints) {
    const result = await testEndpoint(endpoint);
    results.push(result);
  }

  const matches = results.filter((r) => r.hasRequiredFields);

  console.log('\n========================== RESUMO ==========================');
  if (matches.length === 0) {
    console.log('Nenhum endpoint retornou todos os campos obrigatórios.');
  } else {
    console.log('Endpoint(s) que retornaram os 4 campos obrigatórios:');
    for (const item of matches) {
      console.log(`- ${item.url}`);
    }
  }
}

main().catch((error) => {
  console.error('Falha inesperada no script:', error);
  process.exitCode = 1;
});
