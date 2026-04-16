"""
core/state.py — Persistent state management.
Handles balance/trade counters (state.json) and per-market data files.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from config import settings
from config.locations import LOCATIONS

logger = logging.getLogger("weatherbet.state")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_market_record(market: dict) -> tuple[dict, bool]:
    """
    Keep top-level market bookkeeping aligned with the nested position payload.

    Older files can drift into states such as:
    - market.status == "open" while position.status == "closed"
    - market.pnl missing even though position.pnl exists
    - resolved outcome missing for resolved positions
    """
    changed = False
    pos = market.get("position")
    status = str(market.get("status") or "open")

    if pos:
        pos_status = str(pos.get("status") or "open")
        pos_pnl = pos.get("pnl")
        close_reason = str(pos.get("close_reason") or "")
        is_resolved = close_reason == "resolved" or status == "resolved"

        if pos_status == "open" and status != "open":
            market["status"] = "open"
            changed = True

        if pos_status == "closed":
            desired_status = "resolved" if is_resolved else "closed"
            if status != desired_status:
                market["status"] = desired_status
                changed = True

            if pos_pnl is not None and market.get("pnl") != pos_pnl:
                market["pnl"] = pos_pnl
                changed = True

            if desired_status == "resolved":
                inferred_outcome = market.get("resolved_outcome")
                if not inferred_outcome and pos_pnl is not None:
                    inferred_outcome = "win" if _safe_float(pos_pnl) > 0 else "loss"
                if inferred_outcome and market.get("resolved_outcome") != inferred_outcome:
                    market["resolved_outcome"] = inferred_outcome
                    changed = True

    return market, changed


def _has_open_positions() -> bool:
    for f in settings.MARKETS_DIR.glob("*.json"):
        try:
            m = json.loads(f.read_text(encoding="utf-8"))
            m, changed = _normalize_market_record(m)
            if changed:
                save_market(m)
            pos = m.get("position")
            if pos and pos.get("status") == "open":
                return True
        except Exception:
            continue
    return False


def _sync_state_balance_if_idle(state: dict) -> tuple[dict, bool]:
    """Rebase state balance to risk.toml balance when no positions are open."""
    from services.mode_manager import get_mode
    if get_mode() == "production":
        return state, False

    desired = float(settings.BALANCE)
    if _has_open_positions():
        return state, False

    old_start = float(state.get("starting_balance", desired) or desired)
    old_balance = float(state.get("balance", desired) or desired)
    pnl = old_balance - old_start
    new_balance = round(max(0.0, desired + pnl), 2)

    changed = False
    if old_start != desired:
        state["starting_balance"] = desired
        changed = True
    if old_balance != new_balance:
        state["balance"] = new_balance
        changed = True

    peak = float(state.get("peak_balance", new_balance) or new_balance)
    new_peak = max(peak, new_balance, desired)
    if peak != new_peak:
        state["peak_balance"] = new_peak
        changed = True

    return state, changed


def _sync_state_balance_from_wallet(state: dict) -> tuple[dict, bool]:
    """In production mode, sync state balance from wallet API when available."""
    from services.mode_manager import get_mode
    if get_mode() != "production":
        return state, False

    try:
        from connectors.polymarket_trade import get_wallet_balance
        wallet_balance = get_wallet_balance()
    except Exception as e:
        logger.warning("[STATE] Wallet balance sync failed: %s", e)
        return state, False

    if wallet_balance is None:
        return state, False

    wallet_balance = round(max(0.0, float(wallet_balance)), 2)
    current_balance = float(state.get("balance", wallet_balance) or wallet_balance)
    changed = False

    if abs(current_balance - wallet_balance) >= 0.01:
        state["balance"] = wallet_balance
        changed = True

    if not state.get("starting_balance"):
        state["starting_balance"] = wallet_balance
        changed = True

    peak = float(state.get("peak_balance", wallet_balance) or wallet_balance)
    if wallet_balance > peak:
        state["peak_balance"] = wallet_balance
        changed = True

    state["balance_source"] = "wallet_api"
    if changed:
        state["balance_updated_at"] = datetime.now(timezone.utc).isoformat()
    return state, changed


def _sync_state_counters_from_markets(state: dict) -> tuple[dict, bool]:
    """
    Rebuild summary counters from market files so dashboard/Telegram metrics remain
    correct even if state.json drifts or some older market files were persisted
    with stale top-level status fields.
    """
    entry_count = 0
    wins = 0
    losses = 0

    for market in load_all_markets():
        pos = market.get("position")
        if not pos:
            continue

        entry_count += 1
        if pos.get("status") != "closed":
            continue

        is_resolved = market.get("status") == "resolved" or pos.get("close_reason") == "resolved"
        if not is_resolved:
            continue

        outcome = market.get("resolved_outcome")
        if outcome not in ("win", "loss"):
            pnl_value = pos.get("pnl", market.get("pnl"))
            if pnl_value is None:
                continue
            outcome = "win" if _safe_float(pnl_value) > 0 else "loss"

        if outcome == "win":
            wins += 1
        else:
            losses += 1

    changed = False
    if state.get("total_trades") != entry_count:
        state["total_trades"] = entry_count
        changed = True
    if state.get("wins") != wins:
        state["wins"] = wins
        changed = True
    if state.get("losses") != losses:
        state["losses"] = losses
        changed = True

    peak = _safe_float(state.get("peak_balance"), _safe_float(state.get("balance"), settings.BALANCE))
    current_balance = _safe_float(state.get("balance"), settings.BALANCE)
    starting_balance = _safe_float(state.get("starting_balance"), settings.BALANCE)
    new_peak = max(peak, current_balance, starting_balance)
    if peak != new_peak:
        state["peak_balance"] = new_peak
        changed = True

    return state, changed


# ═══════════════════════════════════════════════════════════
# GLOBAL STATE (balance, win/loss counters)
# ═══════════════════════════════════════════════════════════

def load_state() -> dict:
    settings.reload_risk_config()
    if settings.STATE_FILE.exists():
        try:
            state = json.loads(settings.STATE_FILE.read_text(encoding="utf-8"))
            state, wallet_sync_changed = _sync_state_balance_from_wallet(state)
            state, changed = _sync_state_balance_if_idle(state)
            state, counters_changed = _sync_state_counters_from_markets(state)
            if changed or wallet_sync_changed or counters_changed:
                save_state(state)
            return state
        except Exception as e:
            logger.error("[STATE] Corrupt state file: %s", e)
    state = {
        "balance":          settings.BALANCE,
        "starting_balance": settings.BALANCE,
        "total_trades":     0,
        "wins":             0,
        "losses":           0,
        "peak_balance":     settings.BALANCE,
    }
    state, _ = _sync_state_balance_from_wallet(state)
    state, _ = _sync_state_counters_from_markets(state)
    return state


def save_state(state: dict):
    settings.STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ═══════════════════════════════════════════════════════════
# PER-MARKET DATA (one JSON file per city+date)
# ═══════════════════════════════════════════════════════════

def market_path(city_slug: str, date_str: str) -> Path:
    return settings.MARKETS_DIR / f"{city_slug}_{date_str}.json"


def load_market(city_slug: str, date_str: str) -> dict | None:
    p = market_path(city_slug, date_str)
    if p.exists():
        try:
            market = json.loads(p.read_text(encoding="utf-8"))
            market, changed = _normalize_market_record(market)
            if changed:
                save_market(market)
            return market
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
    for f in settings.MARKETS_DIR.glob("*.json"):
        try:
            market = json.loads(f.read_text(encoding="utf-8"))
            market, changed = _normalize_market_record(market)
            if changed:
                save_market(market)
            markets.append(market)
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
        settings.reload_risk_config()
        # Delete all market cache files
        for f in settings.MARKETS_DIR.glob("*.json"):
            try:
                f.unlink()
                logger.info("[CLEAR] Deleted market file: %s", f.name)
            except Exception as e:
                logger.error("[CLEAR] Failed to delete %s: %s", f.name, e)
        
        # Reset state to initial values
        initial_state = {
            "balance":          settings.BALANCE,
            "starting_balance": settings.BALANCE,
            "total_trades":     0,
            "wins":             0,
            "losses":           0,
            "peak_balance":     settings.BALANCE,
        }
        save_state(initial_state)
        logger.info("[CLEAR] State reset to initial values")
        
        # Clear predictions log
        pred_log = settings.STATE_FILE.parent / "predictions_log.json"
        if pred_log.exists():
            pred_log.write_text("[]", encoding="utf-8")
            logger.info("[CLEAR] Predictions log cleared")
        
        return True, "✅ Simulation data cleared successfully"
    except Exception as e:
        logger.error("[CLEAR] Failed to clear simulation data: %s", e)
        return False, f"❌ Error clearing data: {e}"
