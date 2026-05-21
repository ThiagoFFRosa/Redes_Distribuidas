# Custom New Tab Dashboard — Desenvolvimento

## Desenvolvimento (frontend + backend juntos)

1. Instale as dependências do frontend:
   ```bash
   npm install
   ```
2. Instale as dependências do backend:
   ```bash
   npm --prefix ../backend install
   ```
3. Rode frontend e backend juntos:
   ```bash
   npm run dev
   ```

## URLs

- Frontend: http://localhost:4177
- Backend: http://localhost:4178
- Healthcheck: http://localhost:4178/api/health
- Config: http://localhost:4178/api/config

## Comandos separados

- Frontend (dev):
  ```bash
  npm run dev:frontend
  ```
- Backend (dev):
  ```bash
  npm run dev:backend
  ```
- Frontend (preview):
  ```bash
  npm run start:frontend
  ```
- Backend (start):
  ```bash
  npm run start:backend
  ```
