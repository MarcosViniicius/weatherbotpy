# 🌤 WeatherBet — Polymarket Weather Trading Bot

Automated weather market trading bot for Polymarket. Finds mispriced temperature outcomes using real forecast data from multiple sources across 20 cities worldwide.

No SDK. No black box. Pure Python.

---

## Versions

### `bot_v1.py` — Base Bot
The foundation. Scans 6 US cities, fetches forecasts from NWS using airport station coordinates, finds matching temperature buckets on Polymarket, and enters trades when the market price is below the entry threshold.

No math, no complexity. Just the core logic — good for understanding how the system works.

### `weatherbet.py` — Full Bot (current)
Everything in v1, plus:
- **20 cities** across 4 continents (US, Europe, Asia, South America, Oceania)
- **3 forecast sources** — ECMWF (global), HRRR/GFS (US, hourly), METAR (real-time observations)
- **Expected Value** — skips trades where the math doesn't work
- **Kelly Criterion** — sizes positions based on edge strength
- **Stop-loss + trailing stop** — 20% stop, moves to breakeven at +20%
- **Slippage filter** — skips markets with spread > $0.03
- **Self-calibration** — learns forecast accuracy per city over time
- **Full data storage** — every forecast snapshot, trade, and resolution saved to JSON

---

## How It Works

Polymarket runs markets like "Will the highest temperature in Chicago be between 46–47°F on March 7?" These markets are often mispriced — the forecast says 78% likely but the market is trading at 8 cents.

The bot:
1. Fetches forecasts from ECMWF and HRRR via Open-Meteo (free, no key required)
2. Gets real-time observations from METAR airport stations
3. Finds the matching temperature bucket on Polymarket
4. Calculates Expected Value — only enters if the math is positive
5. Sizes the position using fractional Kelly Criterion
6. Monitors stops every 10 minutes, full scan every hour
7. Auto-resolves markets by querying Polymarket API directly

---

## Why Airport Coordinates Matter

Most bots use city center coordinates. That's wrong.

Every Polymarket weather market resolves on a specific airport station. NYC resolves on LaGuardia (KLGA), Dallas on Love Field (KDAL) — not DFW. The difference between city center and airport can be 3–8°F. On markets with 1–2°F buckets, that's the difference between the right trade and a guaranteed loss.

| City | Station | Airport |
|------|---------|---------|
| NYC | KLGA | LaGuardia |
| Chicago | KORD | O'Hare |
| Miami | KMIA | Miami Intl |
| Dallas | KDAL | Love Field |
| Seattle | KSEA | Sea-Tac |
| Atlanta | KATL | Hartsfield |
| London | EGLC | London City |
| Tokyo | RJTT | Haneda |
| ... | ... | ... |

---

## 🚀 Quick Start (Docker)

**Fastest way to get running:**

```bash
# 1. Copy configuration
cp .env.example .env

# 2. Edit with your credentials
nano .env
# Fill in: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

# 3. Start bot
docker-compose up -d

# 4. Access dashboard
open http://localhost:8877
```

→ **Full guide**: See [SETUP.md](SETUP.md)

---

## Installation (Local/Development)

```bash
git clone https://github.com/alteregoeth-ai/weatherbot
cd weatherbot
pip install -r requirements.txt
```

Create `.env` file:
```bash
cp .env.example .env
nano .env
```

Fill in required variables:
```env
TELEGRAM_TOKEN=your-token-from-botfather
TELEGRAM_CHAT_ID=your-chat-id-here
BOT_MODE=simulation
DASHBOARD_PORT=8877
```

---

## Usage

**With Docker (Recommended for production):**
```bash
docker-compose up -d           # Start in background
docker-compose logs -f         # View live logs
docker-compose down            # Stop container
```

**Locally (For testing):**
```bash
python main.py                 # Start bot
# Dashboard: http://localhost:8877
```

---

## Recent Improvements

✅ **Real Edge Tracking** — Measures actual edge after spread + slippage costs
✅ **Single-Instance Protection** — Prevents duplicate bot instances
✅ **HTTP Basic Authentication** — Secure dashboard access
✅ **Remote Dashboard Access** — Access from VPS via DASHBOARD_PUBLIC_URL
✅ **Multi-Stage Docker Build** — Minimal ~192MB image (vs 900MB+)
✅ **Resilience** — Increased API timeouts for international connections
✅ **Clean Simulation Mode** — `/clear` command for testing

---

## 📊 Dashboard Features

- **Real-time metrics**: Balance, P&L, total trades
- **Trade history**: All executed trades with timestamps
- **Open positions**: Current bets and their status
- **Calibration stats**: Model accuracy by location
- **Edge tracking**: Theoretical vs actual edge comparison
- **Authentication**: Optional HTTP Basic Auth with localStorage persistence

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [SETUP.md](SETUP.md) | Quick start guide |
| [DEPLOY_AND_OPS.md](DEPLOY_AND_OPS.md) | Complete deployment & operations |
| [QUICK_REFERENCE.md](QUICK_REFERENCE.md) | Docker commands cheatsheet |
| [DOCKER_GUIDE.md](DOCKER_GUIDE.md) | Detailed Docker guide |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Debugging & fixing issues |
| [.env.example](.env.example) | Configuration template |

---

## Pre-Deployment Checklist

Run this before first deployment:
```bash
chmod +x check-deployment.sh
./check-deployment.sh
```

---

## Deploy to VPS

Automated setup script:
```bash
chmod +x deploy-vps.sh
scp deploy-vps.sh root@YOUR_VPS_IP:/tmp/
ssh root@YOUR_VPS_IP
sudo bash /tmp/deploy-vps.sh
```

Then deploy your code and run Docker:
```bash
cd /opt/weatherbot
docker-compose up -d
```

→ **Full guide**: See [DEPLOY_AND_OPS.md](DEPLOY_AND_OPS.md)

---

## Data Storage

All data is saved to `data/markets/` — one JSON file per market. Each file contains:
- Hourly forecast snapshots (ECMWF, HRRR, METAR)
- Market price history
- Position details (entry, stop, PnL)
- Final resolution outcome

This data is used for self-calibration — the bot learns forecast accuracy per city over time and adjusts position sizing accordingly.

---

## APIs Used

| API | Auth | Purpose |
|-----|------|---------|
| Open-Meteo | None | ECMWF + HRRR forecasts |
| Aviation Weather (METAR) | None | Real-time station observations |
| Polymarket Gamma | None | Market data |
| Visual Crossing | Free key | Historical temps for resolution |

---

## Disclaimer

This is not financial advice. Prediction markets carry real risk. Run the simulation thoroughly before committing real capital.
