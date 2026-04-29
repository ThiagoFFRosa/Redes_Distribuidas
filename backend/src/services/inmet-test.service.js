const REQUIRED_FIELDS = ['CHUVA', 'DT_MEDICAO', 'HR_MEDICAO', 'CD_ESTACAO'];

const buildEndpoints = ({ stationCode, date }) => [
  `https://apitempo.inmet.gov.br/estacao/${stationCode}`,
  `https://apitempo.inmet.gov.br/estacao/dados/${stationCode}`,
  `https://apitempo.inmet.gov.br/estacao/dados/${date}/${stationCode}`,
  `https://apitempo.inmet.gov.br/estacoes/dados/${date}`,
  `https://apitempo.inmet.gov.br/estacoes/T/${date}`,
  `https://apitempo.inmet.gov.br/estacoes/${stationCode}/${date}`,
  `https://apitempo.inmet.gov.br/estacao/${date}/${stationCode}`
];

const isJsonContentType = (contentType) => Boolean(contentType && contentType.toLowerCase().includes('application/json'));

const previewData = (data) => {
  if (Array.isArray(data)) return data.slice(0, 3);
  if (data && typeof data === 'object') {
    return Object.entries(data)
      .slice(0, 3)
      .reduce((acc, [key, value]) => {
        acc[key] = value;
        return acc;
      }, {});
  }

  return data;
};

const detectUsefulFields = (data) => {
  const seen = new Set();

  const visit = (value, depth = 0) => {
    if (depth > 5 || value == null) return;

    if (Array.isArray(value)) {
      value.slice(0, 50).forEach((item) => visit(item, depth + 1));
      return;
    }

    if (typeof value === 'object') {
      Object.entries(value).forEach(([key, nested]) => {
        if (REQUIRED_FIELDS.includes(key)) seen.add(key);
        visit(nested, depth + 1);
      });
    }
  };

  visit(data);

  const fieldsFound = REQUIRED_FIELDS.filter((field) => seen.has(field));
  return {
    hasUsefulFields: REQUIRED_FIELDS.every((field) => seen.has(field)),
    fieldsFound
  };
};

const safeFetch = async (url) => {
  try {
    const response = await fetch(url);
    const contentType = response.headers.get('content-type') || '(não informado)';

    if (!isJsonContentType(contentType)) {
      const text = await response.text();
      return {
        url,
        status: response.status,
        contentType,
        success: false,
        hasJson: false,
        hasUsefulFields: false,
        fieldsFound: [],
        error: text ? text.slice(0, 200) : 'Resposta não JSON'
      };
    }

    const data = await response.json();
    const { hasUsefulFields, fieldsFound } = detectUsefulFields(data);

    return {
      url,
      status: response.status,
      contentType,
      success: response.ok,
      hasJson: true,
      hasUsefulFields,
      fieldsFound,
      sample: previewData(data)
    };
  } catch (error) {
    return {
      url,
      status: 0,
      contentType: '(erro)',
      success: false,
      hasJson: false,
      hasUsefulFields: false,
      fieldsFound: [],
      error: error.message
    };
  }
};

const runInmetEndpointTests = async ({ stationCode, date }) => {
  const endpoints = buildEndpoints({ stationCode, date });
  const results = [];

  for (const url of endpoints) {
    results.push(await safeFetch(url));
  }

  return {
    ok: true,
    testedAt: new Date().toISOString(),
    stationCode,
    date,
    results
  };
};

module.exports = {
  runInmetEndpointTests,
  detectUsefulFields,
  safeFetch
};
