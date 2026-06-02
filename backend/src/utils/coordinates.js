const isBlank = (value) => value === null || value === undefined || String(value).trim() === '';

const toCoordinateNumber = (value) => {
  if (isBlank(value)) return null;
  const normalized = typeof value === 'string' ? value.replace(',', '.') : value;
  const number = Number(normalized);
  return Number.isFinite(number) ? number : NaN;
};

const isValidLatitude = (value) => {
  const n = toCoordinateNumber(value);
  return Number.isFinite(n) && n >= -90 && n <= 90;
};

const isValidLongitude = (value) => {
  const n = toCoordinateNumber(value);
  return Number.isFinite(n) && n >= -180 && n <= 180;
};

const hasValidCoordinates = (point = {}) => isValidLatitude(point.latitude) && isValidLongitude(point.longitude);

const normalizeLocationForStorage = ({ latitude, longitude, invalidError = 'Coordenadas inválidas.', missingError = 'Coordenadas ausentes.' } = {}) => {
  const latitudeBlank = isBlank(latitude);
  const longitudeBlank = isBlank(longitude);
  const parsedLatitude = toCoordinateNumber(latitude);
  const parsedLongitude = toCoordinateNumber(longitude);

  if (latitudeBlank || longitudeBlank) {
    return {
      latitude: null,
      longitude: null,
      location_status: 'NEEDS_REVIEW',
      location_error: missingError
    };
  }

  if (!isValidLatitude(latitude) || !isValidLongitude(longitude)) {
    return {
      latitude: null,
      longitude: null,
      location_status: 'NEEDS_REVIEW',
      location_error: invalidError
    };
  }

  return {
    latitude: parsedLatitude,
    longitude: parsedLongitude,
    location_status: 'VALID',
    location_error: null
  };
};

module.exports = { isValidLatitude, isValidLongitude, hasValidCoordinates, normalizeLocationForStorage, toCoordinateNumber, isBlank };
