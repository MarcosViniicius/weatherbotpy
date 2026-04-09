# WeatherBet v3

Bot de trading para mercados de temperatura no Polymarket.

## Requisitos
- Python 3.11+
- Docker + Docker Compose (opcional, recomendado)

## Estrutura essencial
- `main.py`: entrada principal
- `.env` / `.env.example`: configuração geral
- `risk.toml`: parâmetros de risco
- `dashboard.html`: UI web
- `data/`: estado e histórico

## Configuração rápida
1. Copie o arquivo de ambiente:
```bash
cp .env.example .env
```

2. Edite o `.env` com no mínimo:
- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`
- `BOT_MODE=simulation`
- `DASHBOARD_AUTH_ENABLED=true`
- `DASHBOARD_USERNAME`
- `DASHBOARD_PASSWORD`

3. Ajuste risco em `risk.toml` (ou via Telegram, abaixo).

## Rodar com Docker (recomendado)
```bash
docker compose up -d --build
docker compose logs -f weatherbot
```

Dashboard:
- `http://localhost:8877`

Parar:
```bash
docker compose down
```

## Rodar local (sem Docker)
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
# source venv/bin/activate

pip install -r requirements.txt
python main.py
```

## Comandos Telegram (principais)
- `/start` ou `/menu`: abre painel inline no chat
- `/status`: status geral
- `/scan`: força varredura
- `/startbot` e `/stopbot`: inicia/pausa scheduler
- `/simulate` e `/production`: troca de modo
- `/risk`: mostra risco atual
- `/setrisk <key> <value>`: altera `risk.toml` direto

Exemplos:
```text
/setrisk scan_interval 600
/setrisk min_edge 0.08
/setrisk max_bet 3.5
```

## Arquivos de risco (TOML)
Fonte principal: `risk.toml` na raiz.

Chaves:
- `balance`
- `max_bet`
- `min_edge`
- `max_price`
- `min_volume`
- `min_hours`
- `max_hours`
- `kelly_fraction`
- `max_slippage`
- `scan_interval`
- `calibration_min`

## Troubleshooting rápido
- Comando não responde no Telegram:
  - confira se está falando com o bot correto (token do `.env`)
  - confira `TELEGRAM_CHAT_ID`
- Dashboard sem login:
  - valide `DASHBOARD_AUTH_ENABLED=true`
  - reinicie o container
- Mudou config e não refletiu:
  - rode `docker compose up -d --build`

## Aviso
Use `simulation` primeiro. Trading em produção envolve risco real.
