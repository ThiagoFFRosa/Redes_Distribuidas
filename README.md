# MVP Cluster HOST/STANDBY com Tailscale + ngrok

Este projeto implementa um MVP em **Node.js + Express + HTML/CSS/JS puro** para rodar em **2+ servidores**, com:

- comunicação interna entre servidores via **HTTP em IPs privados Tailscale**;
- apenas **1 HOST ativo** por vez;
- apenas o HOST mantendo túnel **ngrok** para expor painel/admin na internet;
- failover automático sem banco de dados (estado em arquivo JSON local + memória).

## 1) Instalação de dependências

```bash
cd backend
npm install
```

## 2) Configurar `.env`

```bash
cd backend
cp .env.example .env
```

Exemplo:

```env
PORT=3000
SERVER_NAME=server_a
SERVER_URL=http://100.64.0.10:3000
CLUSTER_KEY=uma-chave-secreta-do-cluster
CLUSTER_NODES_FILE=cluster-nodes.json

# fallback legado opcional
PEERS=

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

## 3) Cadastro dinâmico de servidores

- A lista de nós fica em `backend/cluster-nodes.json`.
- Ao subir, se o arquivo não existir, ele é criado com o próprio servidor.
- O painel Admin permite:
  - **Testar conexão** (`POST /api/servers/test-connection`);
  - **Cadastrar servidor** (`POST /api/servers/register`).
- Toda rota interna do cluster exige header `x-cluster-key` com o mesmo `CLUSTER_KEY`.

## 4) Rodar com `npm start`

Em cada servidor:

```bash
cd backend
npm start
```

## 5) Como usar Tailscale

1. Instale e conecte o Tailscale nas máquinas.
2. Pegue o IP privado (`100.x.x.x`) de cada nó.
3. Preencha `SERVER_URL` com o IP/porta local do próprio servidor.
4. No painel, cadastre os outros nós pela URL Tailscale.

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
