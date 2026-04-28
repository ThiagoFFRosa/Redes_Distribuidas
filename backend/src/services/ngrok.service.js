const ngrok = require('ngrok');
const env = require('../config/env');

class NgrokService {
  constructor() {
    this.publicUrl = null;
    this.running = false;

    console.log(
      `[ngrok] startup config: ENABLE_NGROK=${env.enableNgrok}, NGROK_DOMAIN=${env.ngrokDomain || '(vazio)'}`
    );
    console.log(
      `[ngrok] modo esperado: ${env.ngrokDomain ? 'domínio fixo (NGROK_DOMAIN)' : 'URL aleatória'}`
    );
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

    const baseOptions = {
      addr: port,
      proto: 'http',
      authtoken: env.ngrokAuthtoken || undefined
    };

    if (!env.ngrokDomain) {
      try {
        const options = { ...baseOptions, region: env.ngrokRegion };
        console.log('[ngrok] iniciando em modo URL aleatória.');
        const url = await ngrok.connect(options);
        this.publicUrl = url;
        this.running = true;
        return this.publicUrl;
      } catch (error) {
        const message = error?.message || 'erro desconhecido';
        console.error('[ngrok] erro ao iniciar túnel:', message);
        this.publicUrl = null;
        this.running = false;
        return null;
      }
    }

    const expectedDomain = env.ngrokDomain;
    const domainModes = ['domain', 'hostname'];

    for (const field of domainModes) {
      try {
        const options = { ...baseOptions, [field]: expectedDomain };
        console.log(`[ngrok] tentando domínio fixo com opção "${field}": ${expectedDomain}`);

        const url = await ngrok.connect(options);
        if (!String(url).includes(expectedDomain)) {
          console.error('[ngrok] NGROK_DOMAIN configurado, mas ngrok retornou URL aleatória');
          console.error(`[ngrok] URL retornada: ${url}`);
          await ngrok.disconnect(url);
          await ngrok.kill();
          continue;
        }

        console.log(`[ngrok] túnel ativo com domínio fixo usando "${field}": ${url}`);
        this.publicUrl = url;
        this.running = true;
        return this.publicUrl;
      } catch (error) {
        const message = error?.message || 'erro desconhecido';
        console.warn(`[ngrok] falha ao iniciar com "${field}": ${message}`);
      }
    }

    try {
      await ngrok.kill();
    } catch (_error) {
      // ignora erro de limpeza final
    }

    console.error('[ngrok] não foi possível subir túnel com NGROK_DOMAIN configurado.');
    this.publicUrl = null;
    this.running = false;
    return null;
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
