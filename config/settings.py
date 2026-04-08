"""
config/settings.py — Centralised configuration loader.
Reads from .env file and falls back to safe defaults.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_float(key: str, default: float = 0.0) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def _env_int(key: str, default: int = 0) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


# ── Paths ────────────────────────────────────────────────
PROJECT_ROOT = _PROJECT_ROOT
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
MARKETS_DIR = DATA_DIR / "markets"
MARKETS_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"
CALIBRATION_FILE = DATA_DIR / "calibration.json"
MODE_FILE = PROJECT_ROOT / "config" / "mode.json"

# ── Telegram ─────────────────────────────────────────────
TELEGRAM_TOKEN = _env("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = _env("TELEGRAM_CHAT_ID")

# ── Polymarket ───────────────────────────────────────────
POLYMARKET_HOST = "https://clob.polymarket.com"
POLYMARKET_CHAIN_ID = 137  # Polygon
POLYMARKET_PRIVATE_KEY = _env("POLYMARKET_PRIVATE_KEY")
POLYMARKET_FUNDER = _env("POLYMARKET_FUNDER")
POLYMARKET_SIGNATURE_TYPE = _env_int("POLYMARKET_SIGNATURE_TYPE", 0)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# ── Weather APIs ─────────────────────────────────────────
VC_KEY = _env("VC_KEY")

# ── Risk Parameters ──────────────────────────────────────
BALANCE = _env_float("BALANCE", 20.0)
MAX_BET = _env_float("MAX_BET", 2.0)
MIN_EV = _env_float("MIN_EV", 0.08)
MIN_EDGE = _env_float("MIN_EDGE", 0.08)  # Minimum edge (p - price) to enter
MAX_PRICE = _env_float("MAX_PRICE", 0.60)
MIN_VOLUME = _env_float("MIN_VOLUME", 200)
MIN_HOURS = _env_float("MIN_HOURS", 2.0)
MAX_HOURS = _env_float("MAX_HOURS", 72.0)
KELLY_FRACTION = _env_float("KELLY_FRACTION", 0.25)
MAX_SLIPPAGE = _env_float("MAX_SLIPPAGE", 0.02)
SCAN_INTERVAL = _env_int("SCAN_INTERVAL", 900)
CALIBRATION_MIN = _env_int("CALIBRATION_MIN", 50)

# ── Derived Constants ────────────────────────────────────
SIGMA_F = 2.0  # Default forecast sigma for Fahrenheit cities
SIGMA_C = 1.2  # Default forecast sigma for Celsius cities
MONITOR_INTERVAL = 600  # Check positions every 10 min

MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


def validate_production_credentials() -> list[str]:
    """Returns a list of missing credentials required for production mode."""
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if not POLYMARKET_PRIVATE_KEY:
        missing.append("POLYMARKET_PRIVATE_KEY")
    return missing
