"""
core/state.py — Persistent state management.
Handles balance/trade counters (state.json) and per-market data files.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from config.settings import (
    STATE_FILE, MARKETS_DIR, BALANCE,
)
from config.locations import LOCATIONS

logger = logging.getLogger("weatherbet.state")


# ═══════════════════════════════════════════════════════════
# GLOBAL STATE (balance, win/loss counters)
# ═══════════════════════════════════════════════════════════

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error("[STATE] Corrupt state file: %s", e)
    return {
        "balance":          BALANCE,
        "starting_balance": BALANCE,
        "total_trades":     0,
        "wins":             0,
        "losses":           0,
        "peak_balance":     BALANCE,
    }


def save_state(state: dict):
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ═══════════════════════════════════════════════════════════
# PER-MARKET DATA (one JSON file per city+date)
# ═══════════════════════════════════════════════════════════

def market_path(city_slug: str, date_str: str) -> Path:
    return MARKETS_DIR / f"{city_slug}_{date_str}.json"


def load_market(city_slug: str, date_str: str) -> dict | None:
    p = market_path(city_slug, date_str)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def save_market(market: dict):
    p = market_path(market["city"], market["date"])
    p.write_text(
        json.dumps(market, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def load_all_markets() -> list[dict]:
    markets = []
    for f in MARKETS_DIR.glob("*.json"):
        try:
            markets.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    return markets


def new_market(city_slug: str, date_str: str, event: dict, hours: float) -> dict:
    loc = LOCATIONS[city_slug]
    return {
        "city":               city_slug,
        "city_name":          loc["name"],
        "date":               date_str,
        "unit":               loc["unit"],
        "station":            loc["station"],
        "event_end_date":     event.get("endDate", ""),
        "hours_at_discovery": round(hours, 1),
        "status":             "open",
        "position":           None,
        "actual_temp":        None,
        "resolved_outcome":   None,
        "pnl":                None,
        "forecast_snapshots": [],
        "market_snapshots":   [],
        "all_outcomes":       [],
        "created_at":         datetime.now(timezone.utc).isoformat(),
    }


# ═══════════════════════════════════════════════════════════
# CLEAR SIMULATION DATA
# ═══════════════════════════════════════════════════════════

def clear_simulation_data():
    """Clear all simulation data: markets, state, and predictions log."""
    try:
        # Delete all market cache files
        for f in MARKETS_DIR.glob("*.json"):
            try:
                f.unlink()
                logger.info("[CLEAR] Deleted market file: %s", f.name)
            except Exception as e:
                logger.error("[CLEAR] Failed to delete %s: %s", f.name, e)
        
        # Reset state to initial values
        initial_state = {
            "balance":          BALANCE,
            "starting_balance": BALANCE,
            "total_trades":     0,
            "wins":             0,
            "losses":           0,
            "peak_balance":     BALANCE,
        }
        save_state(initial_state)
        logger.info("[CLEAR] State reset to initial values")
        
        # Clear predictions log
        pred_log = STATE_FILE.parent / "predictions_log.json"
        if pred_log.exists():
            pred_log.write_text("[]", encoding="utf-8")
            logger.info("[CLEAR] Predictions log cleared")
        
        return True, "✅ Simulation data cleared successfully"
    except Exception as e:
        logger.error("[CLEAR] Failed to clear simulation data: %s", e)
        return False, f"❌ Error clearing data: {e}"
