const ngrok = require('ngrok');
const env = require('../config/env');

class NgrokService {
  constructor() {
    this.publicUrl = null;
    this.running = false;
  }

  async startTunnel(port) {
    if (!env.enableNgrok) {
      this.publicUrl = null;
      this.running = false;
      return null;
    }

    if (this.running && this.publicUrl) {
      return this.publicUrl;
    }

    try {
      const options = {
        addr: port,
        proto: 'http',
        authtoken: env.ngrokAuthtoken || undefined
      };

      if (env.ngrokDomain) {
        options.domain = env.ngrokDomain;
      } else {
        options.region = env.ngrokRegion;
      }

      const url = await ngrok.connect(options);

      this.publicUrl = url;
      this.running = true;
      return this.publicUrl;
    } catch (error) {
      const message = error?.message || 'erro desconhecido';
      if (env.ngrokDomain && /domain|already in use|in use|ERR_NGROK_/i.test(message)) {
        console.warn(`[ngrok] domínio fixo indisponível (${env.ngrokDomain}): ${message}`);
      } else {
        console.error('[ngrok] erro ao iniciar túnel:', message);
      }

      this.publicUrl = null;
      this.running = false;
      return null;
    }
  }

  async stopTunnel() {
    if (!env.enableNgrok) {
      this.publicUrl = null;
      this.running = false;
      return;
    }

    if (!this.running) {
      this.publicUrl = null;
      return;
    }

    try {
      await ngrok.disconnect();
      await ngrok.kill();
    } catch (error) {
      console.error('[ngrok] erro ao parar túnel:', error.message);
    } finally {
      this.publicUrl = null;
      this.running = false;
    }
  }

  getPublicUrl() {
    return this.publicUrl;
  }
}

module.exports = new NgrokService();
