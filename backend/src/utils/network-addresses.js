const os = require('os');

const isIpv4 = (address) => /^\d{1,3}(?:\.\d{1,3}){3}$/.test(String(address || ''));
const isTailscaleIpv4 = (address) => /^100\./.test(String(address || ''));

const getExternalIpv4Addresses = () => {
  const interfaces = os.networkInterfaces();
  return Object.values(interfaces)
    .flat()
    .filter(Boolean)
    .filter((entry) => entry.family === 'IPv4' && !entry.internal && isIpv4(entry.address))
    .map((entry) => entry.address);
};

const detectTailscaleIp = () => getExternalIpv4Addresses().find(isTailscaleIpv4) || null;

const getSuggestedAccessUrls = (port) => {
  const externalIps = getExternalIpv4Addresses();
  const tailscaleIp = externalIps.find(isTailscaleIpv4) || null;
  return {
    localUrl: `http://127.0.0.1:${port}`,
    tailscaleIp,
    tailscaleUrl: tailscaleIp ? `http://${tailscaleIp}:${port}` : null,
    lanUrls: externalIps
      .filter((ip) => ip !== tailscaleIp)
      .map((ip) => `http://${ip}:${port}`)
  };
};

module.exports = {
  detectTailscaleIp,
  getExternalIpv4Addresses,
  getSuggestedAccessUrls,
  isTailscaleIpv4
};
