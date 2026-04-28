# MVP Cluster HOST/STANDBY com Tailscale + ngrok

Este projeto implementa um MVP em **Node.js + Express + HTML/CSS/JS puro** para rodar em **2+ servidores**, com:

- comunicação interna entre servidores via **HTTP em IPs privados Tailscale**;
- apenas **1 HOST ativo** por vez;
- apenas o HOST mantendo túnel **ngrok** para expor painel/admin na internet;
- failover automático sem banco de dados (estado em memória).

## 1) Instalação de dependências

```bash
cd backend
npm install
```

## 2) Configurar `.env` no Server A

```bash
cd backend
cp .env.example .env
```

Exemplo Server A:

```env
PORT=3000
SERVER_NAME=server_a
SERVER_URL=http://100.64.0.10:3000
PEERS=http://100.64.0.11:3000

INITIAL_ROLE=HOST

ADMIN_USER=admin
ADMIN_PASSWORD=admin123
SESSION_SECRET=dev-secret

HEARTBEAT_INTERVAL_MS=3000
HEARTBEAT_TIMEOUT_MS=9000

ENABLE_NGROK=true
NGROK_AUTHTOKEN=SEU_TOKEN_AQUI
NGROK_REGION=sa
```

## 3) Configurar `.env` no Server B

Exemplo Server B:

```env
PORT=3000
SERVER_NAME=server_b
SERVER_URL=http://100.64.0.11:3000
PEERS=http://100.64.0.10:3000

INITIAL_ROLE=STANDBY

ADMIN_USER=admin
ADMIN_PASSWORD=admin123
SESSION_SECRET=dev-secret

HEARTBEAT_INTERVAL_MS=3000
HEARTBEAT_TIMEOUT_MS=9000

ENABLE_NGROK=true
NGROK_AUTHTOKEN=SEU_TOKEN_AQUI
NGROK_REGION=sa
```

## 4) Rodar com `npm start`

Em cada servidor:

```bash
cd backend
npm start
```

## 5) Como usar Tailscale para `SERVER_URL` e `PEERS`

1. Instale e conecte o Tailscale nas máquinas.
2. Pegue o IP privado (`100.x.x.x`) de cada nó.
3. Preencha `SERVER_URL` com o IP/porta local do próprio servidor.
4. Preencha `PEERS` com os outros nós separados por vírgula.

> A comunicação interna usa somente esses endereços privados, nunca URL do ngrok.

## 6) Configurar `NGROK_AUTHTOKEN`

1. Crie conta no ngrok.
2. Copie seu authtoken.
3. Cole em `NGROK_AUTHTOKEN` no `.env`.
4. Se quiser desabilitar túneis, use `ENABLE_NGROK=false`.

## 7) Acessar o painel

- Localmente/Tailscale: `http://SEU_IP:PORT/`
- Login: `/login.html`
- Painel: `/admin.html`

Credenciais vêm de `ADMIN_USER` e `ADMIN_PASSWORD`.

## 8) Testar troca manual de HOST

1. Faça login no painel.
2. Na tabela de servidores, clique em **Tornar HOST** no servidor alvo.
3. Resultado esperado:
   - alvo vira `HOST`;
   - demais viram `STANDBY`;
   - apenas o HOST fica com `publicUrl` ngrok.

## 9) Testar failover automático

1. Suba A(HOST) e B(STANDBY).
2. Derrube A.
3. Aguarde `HEARTBEAT_TIMEOUT_MS`.
4. B detecta ausência de HOST, executa eleição e assume HOST.
5. Suba A novamente: A deve permanecer STANDBY se B já for HOST.

## 10) Limitações do MVP

- Estado somente em memória (reinício perde histórico).
- Eleição simplificada por menor `SERVER_NAME` entre nós online.
- Sem criptografia adicional entre nós (assume rede privada Tailscale).
- Sem proteção avançada para rotas internas além da rede privada.
- Sem banco de dados e sem fila de eventos distribuída.

---

## Estrutura

```text
/backend
  package.json
  .env.example
  /src
    server.js
    /config/env.js
    /routes
      auth.routes.js
      server.routes.js
      cluster.routes.js
      ngrok.routes.js
    /services
      auth.service.js
      cluster.service.js
      heartbeat.service.js
      ngrok.service.js
/public
  login.html
  admin.html
  /assets
    style.css
    login.js
    admin.js
```
