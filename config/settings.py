"""
config/settings.py — Centralised configuration loader.
Reads from .env file and falls back to safe defaults.
Risk parameters are primarily loaded from risk.toml at project root.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

logger = logging.getLogger("weatherbet.settings")

_legacy_divergence_warned = False


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
RISK_CONFIG_FILE = PROJECT_ROOT / "risk.toml"
LEGACY_RISK_CONFIG_FILE = PROJECT_ROOT / "config" / "risk.toml"

# ── Telegram ─────────────────────────────────────────────
TELEGRAM_TOKEN = _env("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = _env("TELEGRAM_CHAT_ID")

# ── Polymarket ───────────────────────────────────────────
POLYMARKET_HOST = "https://clob.polymarket.com"
POLYMARKET_CHAIN_ID = 137  # Polygon
POLYMARKET_PRIVATE_KEY = _env("POLYMARKET_PRIVATE_KEY")
POLYMARKET_FUNDER = _env("POLYMARKET_FUNDER")
POLYMARKET_SIGNATURE_TYPE = _env_int("POLYMARKET_SIGNATURE_TYPE", 0)
POLYMARKET_TIMEOUT = _env_int("POLYMARKET_TIMEOUT", 15)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# ── Weather APIs ─────────────────────────────────────────
VC_KEY = _env("VC_KEY")
WEATHER_TIMEOUT = _env_int("WEATHER_TIMEOUT", 15)

# ── Dashboard Configuration ──────────────────────────────
DASHBOARD_PORT = _env_int("DASHBOARD_PORT", 8877)
DASHBOARD_AUTH_ENABLED = _env("DASHBOARD_AUTH_ENABLED", "false").lower() in ("true", "1", "yes", "on")
DASHBOARD_USERNAME = _env("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = _env("DASHBOARD_PASSWORD", "changeme")
DASHBOARD_PUBLIC_URL = _env("DASHBOARD_PUBLIC_URL", "")

# ── Logging Configuration ────────────────────────────────
LOG_LEVEL = _env("LOG_LEVEL", "INFO").upper()

# ── Risk Parameters ──────────────────────────────────────
_DEFAULT_RISK_CONFIG = {
    "balance": 20.0,
    "max_bet": 2.0,
    "min_ev": 0.08,
    "min_edge": 0.07,
    "max_price": 0.60,
    "min_volume": 200,
    "min_hours": 2.0,
    "max_hours": 72.0,
    "kelly_fraction": 0.25,
    "max_slippage": 0.02,
    "scan_interval": 900,
    "calibration_min": 50,
}

_RISK_TYPE = {
    "balance": float,
    "max_bet": float,
    "min_ev": float,
    "min_edge": float,
    "max_price": float,
    "min_volume": int,
    "min_hours": float,
    "max_hours": float,
    "kelly_fraction": float,
    "max_slippage": float,
    "scan_interval": int,
    "calibration_min": int,
}


def _write_risk_toml(risk: dict) -> None:
    lines = [
        "# WeatherBet risk configuration",
        "# Updated via Telegram /setrisk or manual edit",
        "",
        "[account]",
        f"balance = {float(risk.get('balance', _DEFAULT_RISK_CONFIG['balance']))}",
        f"max_bet = {float(risk.get('max_bet', _DEFAULT_RISK_CONFIG['max_bet']))}",
        "",
        "[risk]",
        f"min_ev = {float(risk.get('min_ev', _DEFAULT_RISK_CONFIG['min_ev']))}",
        f"min_edge = {float(risk.get('min_edge', _DEFAULT_RISK_CONFIG['min_edge']))}",
        f"max_price = {float(risk.get('max_price', _DEFAULT_RISK_CONFIG['max_price']))}",
        f"kelly_fraction = {float(risk.get('kelly_fraction', _DEFAULT_RISK_CONFIG['kelly_fraction']))}",
        "",
        "[market_filters]",
        f"min_volume = {int(risk.get('min_volume', _DEFAULT_RISK_CONFIG['min_volume']))}",
        f"min_hours = {float(risk.get('min_hours', _DEFAULT_RISK_CONFIG['min_hours']))}",
        f"max_hours = {float(risk.get('max_hours', _DEFAULT_RISK_CONFIG['max_hours']))}",
        f"max_slippage = {float(risk.get('max_slippage', _DEFAULT_RISK_CONFIG['max_slippage']))}",
        "",
        "[execution]",
        f"scan_interval = {int(risk.get('scan_interval', _DEFAULT_RISK_CONFIG['scan_interval']))}",
        "",
        "[model]",
        f"calibration_min = {int(risk.get('calibration_min', _DEFAULT_RISK_CONFIG['calibration_min']))}",
    ]
    RISK_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    RISK_CONFIG_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Keep legacy path synchronized to reduce operator confusion.
    try:
        LEGACY_RISK_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        LEGACY_RISK_CONFIG_FILE.write_text(RISK_CONFIG_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    except Exception as e:
        logger.warning("[RISK] Could not sync legacy risk file: %s", e)


def _migrate_legacy_risk_file_if_needed() -> None:
    """If only legacy config/risk.toml exists, copy it to root risk.toml."""
    if RISK_CONFIG_FILE.exists():
        return
    if not LEGACY_RISK_CONFIG_FILE.exists():
        return
    try:
        RISK_CONFIG_FILE.write_text(LEGACY_RISK_CONFIG_FILE.read_text(encoding="utf-8"), encoding="utf-8")
        logger.info("[RISK] Migrated legacy config/risk.toml to root risk.toml")
    except Exception:
        # Ignore migration failures; loader will fallback to defaults
        logger.exception("[RISK] Failed migrating legacy risk TOML")


def _warn_if_root_legacy_diverge() -> None:
    global _legacy_divergence_warned
    if not RISK_CONFIG_FILE.exists() or not LEGACY_RISK_CONFIG_FILE.exists():
        return
    try:
        root_txt = RISK_CONFIG_FILE.read_text(encoding="utf-8").strip()
        legacy_txt = LEGACY_RISK_CONFIG_FILE.read_text(encoding="utf-8").strip()
        if root_txt != legacy_txt:
            if not _legacy_divergence_warned:
                logger.warning(
                    "[RISK] Found divergent TOMLs. Root file '%s' is canonical. Syncing legacy '%s'.",
                    RISK_CONFIG_FILE,
                    LEGACY_RISK_CONFIG_FILE,
                )
                _legacy_divergence_warned = True
            LEGACY_RISK_CONFIG_FILE.write_text(root_txt + "\n", encoding="utf-8")
    except Exception as e:
        logger.warning("[RISK] Could not compare root/legacy TOML files: %s", e)


def _load_risk_toml() -> dict:
    _migrate_legacy_risk_file_if_needed()
    _warn_if_root_legacy_diverge()

    if not RISK_CONFIG_FILE.exists():
        _write_risk_toml(_DEFAULT_RISK_CONFIG)
        return dict(_DEFAULT_RISK_CONFIG)

    try:
        with RISK_CONFIG_FILE.open("rb") as f:
            raw = tomllib.load(f)
        if not isinstance(raw, dict):
            logger.warning("[RISK] Invalid TOML root type in %s. Using defaults.", RISK_CONFIG_FILE)
            return dict(_DEFAULT_RISK_CONFIG)

        # Accept both legacy flat [risk] and newer sectioned TOML schema.
        source_values = {}

        # 1) Top-level flat keys
        for key in _DEFAULT_RISK_CONFIG:
            if key in raw:
                source_values[key] = raw.get(key)

        # 2) Legacy [risk] section keys
        risk_section = raw.get("risk")
        if isinstance(risk_section, dict):
            for key in _DEFAULT_RISK_CONFIG:
                if key in risk_section:
                    source_values[key] = risk_section.get(key)

        # 3) Sectioned schema keys
        section_key_map = {
            "account": ["balance", "max_bet"],
            "risk": ["min_ev", "min_edge", "max_price", "kelly_fraction"],
            "market_filters": ["min_volume", "min_hours", "max_hours", "max_slippage"],
            "execution": ["scan_interval"],
            "model": ["calibration_min"],
        }
        for section_name, keys in section_key_map.items():
            section = raw.get(section_name)
            if not isinstance(section, dict):
                continue
            for key in keys:
                if key in section:
                    source_values[key] = section.get(key)

        merged = dict(_DEFAULT_RISK_CONFIG)
        for k, default in _DEFAULT_RISK_CONFIG.items():
            if k in source_values and source_values[k] is not None:
                caster = _RISK_TYPE[k]
                try:
                    merged[k] = caster(source_values[k])
                except (TypeError, ValueError):
                    logger.warning(
                        "[RISK] Invalid value for key '%s' in %s: %r. Keeping default=%r",
                        k,
                        RISK_CONFIG_FILE,
                        source_values[k],
                        default,
                    )
        return merged
    except Exception:
        logger.exception("[RISK] Failed to load risk TOML from %s. Using defaults.", RISK_CONFIG_FILE)
        return dict(_DEFAULT_RISK_CONFIG)


_risk_cfg = _load_risk_toml()

_RISK_GLOBAL_MAPPING = {
    "balance": "BALANCE",
    "max_bet": "MAX_BET",
    "min_ev": "MIN_EV",
    "min_edge": "MIN_EDGE",
    "max_price": "MAX_PRICE",
    "min_volume": "MIN_VOLUME",
    "min_hours": "MIN_HOURS",
    "max_hours": "MAX_HOURS",
    "kelly_fraction": "KELLY_FRACTION",
    "max_slippage": "MAX_SLIPPAGE",
    "scan_interval": "SCAN_INTERVAL",
    "calibration_min": "CALIBRATION_MIN",
}


def _risk_float(toml_key: str, default: float) -> float:
    val = _risk_cfg.get(toml_key)
    if val is not None:
        try:
            return float(val)
        except (TypeError, ValueError):
            pass
    return default


def _risk_int(toml_key: str, default: int) -> int:
    val = _risk_cfg.get(toml_key)
    if val is not None:
        try:
            return int(val)
        except (TypeError, ValueError):
            pass
    return default


BALANCE = _risk_float("balance", 20.0)
MAX_BET = _risk_float("max_bet", 2.0)
MIN_EV = _risk_float("min_ev", 0.08)
MIN_EDGE = _risk_float("min_edge", 0.08)  # Minimum edge (p - price) to enter
MAX_PRICE = _risk_float("max_price", 0.60)
MIN_VOLUME = _risk_int("min_volume", 200)
MIN_HOURS = _risk_float("min_hours", 2.0)
MAX_HOURS = _risk_float("max_hours", 72.0)
KELLY_FRACTION = _risk_float("kelly_fraction", 0.25)
MAX_SLIPPAGE = _risk_float("max_slippage", 0.02)
SCAN_INTERVAL = _risk_int("scan_interval", 900)
CALIBRATION_MIN = _risk_int("calibration_min", 50)

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
    if not POLYMARKET_PRIVATE_KEY or str(POLYMARKET_PRIVATE_KEY).strip().lower().startswith("your-"):
        missing.append("POLYMARKET_PRIVATE_KEY")
    return missing


def reload_risk_config() -> dict:
    """Reload risk.toml and apply values to live module globals."""
    global _risk_cfg
    _risk_cfg = _load_risk_toml()
    for key, var_name in _RISK_GLOBAL_MAPPING.items():
        value = _risk_cfg.get(key, _DEFAULT_RISK_CONFIG[key])
        try:
            value = _RISK_TYPE[key](value)
        except (TypeError, ValueError):
            value = _DEFAULT_RISK_CONFIG[key]
        globals()[var_name] = value
    return get_risk_config()


def get_risk_config() -> dict:
    """Current risk configuration (reloaded from TOML)."""
    # Keep all layers in sync with edits made directly in risk.toml.
    _latest = _load_risk_toml()
    for key, var_name in _RISK_GLOBAL_MAPPING.items():
        value = _latest.get(key, _DEFAULT_RISK_CONFIG[key])
        try:
            value = _RISK_TYPE[key](value)
        except (TypeError, ValueError):
            value = _DEFAULT_RISK_CONFIG[key]
        globals()[var_name] = value

    return {
        "balance": BALANCE,
        "max_bet": MAX_BET,
        "min_ev": MIN_EV,
        "min_edge": MIN_EDGE,
        "max_price": MAX_PRICE,
        "min_volume": MIN_VOLUME,
        "min_hours": MIN_HOURS,
        "max_hours": MAX_HOURS,
        "kelly_fraction": KELLY_FRACTION,
        "max_slippage": MAX_SLIPPAGE,
        "scan_interval": SCAN_INTERVAL,
        "calibration_min": CALIBRATION_MIN,
    }


def update_risk_config(key: str, value: str) -> tuple[bool, str]:
    """
    Update a risk key in risk.toml and current runtime globals.
    Returns (ok, message).
    """
    key = key.strip().lower()
    if key not in _DEFAULT_RISK_CONFIG:
        allowed = ", ".join(_DEFAULT_RISK_CONFIG.keys())
        return False, f"Invalid key '{key}'. Allowed: {allowed}"

    caster = _RISK_TYPE[key]
    try:
        parsed = caster(value)
    except (TypeError, ValueError):
        typ = "integer" if caster is int else "number"
        return False, f"Invalid value '{value}' for {key}. Expected {typ}."

    if isinstance(parsed, (int, float)) and parsed < 0:
        return False, f"Value for {key} must be >= 0."

    cfg = _load_risk_toml()
    cfg[key] = parsed
    _write_risk_toml(cfg)

    # Apply live in current process from canonical TOML content.
    reload_risk_config()
    return True, f"Updated {key}={parsed} in {RISK_CONFIG_FILE}"
