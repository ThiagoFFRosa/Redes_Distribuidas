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
      if (env.ngrokAuthtoken) {
        await ngrok.authtoken(env.ngrokAuthtoken);
      }

      const url = await ngrok.connect({
        addr: port,
        region: env.ngrokRegion,
        proto: 'http'
      });

      this.publicUrl = url;
      this.running = true;
      return this.publicUrl;
    } catch (error) {
      console.error('[ngrok] erro ao iniciar túnel:', error.message);
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
