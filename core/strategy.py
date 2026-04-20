"""
core/strategy.py — Central trading strategy.
scan_and_update()    : Full cycle — forecasts, market discovery, position entry/exit.
monitor_positions()  : Quick stop/take-profit check between full scans.

In simulation mode   : updates virtual balance in state.json.
In production mode   : additionally calls connectors.polymarket_trade to place real orders.
"""

import json
import time
import logging
from datetime import datetime, timezone, timedelta

from config import settings
from config.locations import LOCATIONS
from connectors import polymarket_read as pm_read
from connectors import polymarket_trade as pm_trade
from core.math_utils import (
    adaptive_bet_size, bucket_prob, calc_edge, calc_ev_after_costs, calc_kelly, in_bucket,
    confidence_by_time, forecast_disagreement_sigma, late_market_multiplier,
    portfolio_concentration_multiplier,
)
from core.calibration import get_sigma, run_calibration, load_cal, log_prediction, record_outcome
from core.state import (
    load_state, save_state, load_market, save_market,
    load_all_markets, new_market,
)

logger = logging.getLogger("weatherbet.strategy")
# Adjacent buckets are allowed but receive an 8% confidence haircut to keep
# the primary forecast bucket prioritized while still recovering near-boundary opportunities.
ADJACENT_BUCKET_CONFIDENCE_PENALTY = 0.92
EXACT_BUCKET_CONFIDENCE_PENALTY = 0.88

# Will be set by the scheduler to push Telegram notifications
_notify_func = None


def set_notify(func):
    """Register the async notification callback (called from scheduler)."""
    global _notify_func
    _notify_func = func


def _notify(msg: str):
    """Best-effort notification push."""
    if _notify_func:
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_notify_func(msg))
            else:
                loop.run_until_complete(_notify_func(msg))
        except Exception:
            pass
    logger.info(msg)


def _is_production() -> bool:
    from services.mode_manager import get_mode
    return get_mode() == "production"


def _shrink_probability(prob: float, confidence: float) -> float:
    """
    Confidence should pull probabilities toward 50/50, not toward zero.
    The previous multiplicative adjustment overstated NO trades on low-confidence buckets.
    """
    p = max(0.0, min(1.0, float(prob)))
    conf = max(0.0, min(1.0, float(confidence)))
    return round(0.5 + (p - 0.5) * conf, 4)


def _rollout_thresholds() -> dict:
    """
    Effective risk thresholds by rollout stage:
      0=baseline, 1=A(min_volume), 2=B(slippage/spread), 3=C(max_price), 4=D(min_edge)
    """
    stage = max(0, min(int(settings.RELAX_STAGE), 4))
    cfg = {
        "stage": stage,
        "min_volume": int(settings.MIN_VOLUME),
        "max_slippage": float(settings.MAX_SLIPPAGE),
        "max_price": float(settings.MAX_PRICE),
        "min_edge": float(settings.MIN_EDGE),
        "max_relative_spread": 0.15,
    }
    # NOTE: relax_stage only LOOSENS thresholds, never tightens them
    if stage >= 1:
        cfg["min_volume"] = min(cfg["min_volume"], 200)   # floor at 200 (not below config)
    if stage >= 2:
        cfg["max_slippage"] = max(cfg["max_slippage"], 0.03)
        cfg["max_relative_spread"] = max(cfg["max_relative_spread"], 0.20)
    if stage >= 3:
        cfg["max_price"] = max(cfg["max_price"], 0.65)
    if stage >= 4:
        cfg["min_edge"] = min(cfg["min_edge"], 0.06)   # floor at 6%, not 4%
    return cfg


def _new_cycle_stats(thresholds: dict) -> dict:
    return {
        "rollout_stage": thresholds["stage"],
        "expected_events": 0,
        "events_found": 0,
        "markets_read": 0,
        "markets_valid": 0,
        "signals_generated": 0,
        "net_ev_sum": 0.0,
        "real_spread_sum": 0.0,
        "signals_by_city": {},
        "discard_reasons": {
            "event_not_found": 0,
            "hours": 0,
            "spread_relative": 0,
            "volume": 0,
            "ev": 0,
            "price": 0,
            "slippage": 0,
        },
    }


def _log_cycle_metrics(stats: dict):
    signals = int(stats["signals_generated"])
    avg_ev = round(stats["net_ev_sum"] / signals, 4) if signals else 0.0
    avg_spread = round(stats["real_spread_sum"] / signals, 4) if signals else 0.0
    concentration = 0.0
    top_city = None
    if signals and stats["signals_by_city"]:
        top_city = max(stats["signals_by_city"], key=stats["signals_by_city"].get)
        concentration = stats["signals_by_city"][top_city] / signals
    discards_json = json.dumps(stats["discard_reasons"], sort_keys=True)

    logger.info(
        "[SCAN_METRICS] stage=%s events=%s/%s markets=%s valid=%s signals=%s avg_net_ev=%+.4f avg_spread=%.4f discards=%s",
        stats["rollout_stage"],
        stats["events_found"],
        stats["expected_events"],
        stats["markets_read"],
        stats["markets_valid"],
        signals,
        avg_ev,
        avg_spread,
        discards_json,
    )
    if top_city:
        logger.info("[SCAN_METRICS] signal_concentration top_city=%s ratio=%.2f", top_city, concentration)
    if signals and avg_ev < 0:
        logger.warning("[ROLLBACK_GUARD] Negative average net EV detected in this cycle: %+.4f", avg_ev)
    if signals and avg_spread > settings.MAX_SLIPPAGE:
        logger.warning(
            "[ROLLBACK_GUARD] Average spread above configured max_slippage: spread=%.4f max=%.4f",
            avg_spread,
            settings.MAX_SLIPPAGE,
        )
    if concentration > 0.7:
        logger.warning(
            "[ROLLBACK_GUARD] Signal concentration high on %s: %.0f%%",
            top_city,
            concentration * 100,
        )


def _set_signal_ev_fields(signal: dict, ev_value: float) -> None:
    """Keep legacy EV keys synchronized while transitioning to net_ev naming."""
    signal["net_ev"] = ev_value
    signal["ev_after_costs"] = ev_value
    signal["ev"] = ev_value


def _get_position_side(pos: dict) -> str:
    side = str(pos.get("side") or "YES").upper()
    return "NO" if side == "NO" else "YES"


def _extract_token_ids(market_detail: dict | None) -> tuple[str | None, str | None]:
    """Return (yes_token_id, no_token_id) from Gamma market detail."""
    if not market_detail:
        return None, None

    raw = market_detail.get("clobTokenIds")
    token_ids = raw
    if isinstance(raw, str):
        try:
            token_ids = json.loads(raw)
        except Exception:
            token_ids = [raw]

    if isinstance(token_ids, list):
        yes_token = token_ids[0] if len(token_ids) > 0 else None
        no_token = token_ids[1] if len(token_ids) > 1 else None
        return yes_token, no_token

    return None, None


def _quotes_for_side_from_outcome(outcome: dict, side: str) -> tuple[float, float, float]:
    """Return bid, ask and midpoint for the requested binary side."""
    yes_bid = float(outcome.get("bid", outcome.get("price", 0.0)) or 0.0)
    yes_ask = float(outcome.get("ask", outcome.get("price", 0.0)) or 0.0)
    yes_mid = float(outcome.get("price", (yes_bid + yes_ask) / 2.0) or 0.0)

    if side == "NO":
        no_bid = max(0.0, round(1.0 - yes_ask, 4))
        no_ask = min(0.9999, round(1.0 - yes_bid, 4))
        no_mid = min(0.9999, max(0.0, round(1.0 - yes_mid, 4)))
        return no_bid, no_ask, no_mid

    return yes_bid, yes_ask, yes_mid


def _position_current_price_from_outcomes(pos: dict, outcomes: list[dict]) -> float | None:
    side = _get_position_side(pos)
    for outcome in outcomes:
        if outcome["market_id"] != pos.get("market_id"):
            continue
        bid, _, mid = _quotes_for_side_from_outcome(outcome, side)
        return bid if bid > 0 else mid
    return None


def _close_reason_label(close_reason: str) -> tuple[str, str]:
    mapping = {
        "take_profit": ("TAKE", "💰"),
        "forecast_shift_close": ("FCAST", "🔄"),
        "forecast_changed": ("FCAST", "🔄"),
        "stop_loss": ("STOP", "🛑"),
        "trailing_stop": ("TRAILING", "🔒"),
    }
    return mapping.get(close_reason, ("CLOSE", "🔄"))


def _pending_exit_reason(pos: dict) -> str:
    return str(pos.get("pending_close_reason") or pos.get("close_reason") or "close")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _take_profit_target(entry_price: float, hours_left: float) -> float | None:
    """
    Dynamic take-profit schedule.
    Short-dated trades still need a profit-taking path; otherwise they can
    carry large unrealized gains all the way to resolution.
    """
    if entry_price <= 0:
        return None
    if hours_left < 6:
        mult = 1.10
    elif hours_left < 24:
        mult = 1.25
    elif hours_left < 48:
        mult = 1.60
    else:
        mult = 2.00
    return round(entry_price * mult, 4)


def _default_stop_price(entry_price: float, hours_left: float) -> float:
    """
    Dynamic stop schedule shared across scan/monitor paths.
    """
    if hours_left > 48:
        stop_pct = 0.65
    elif hours_left > 24:
        stop_pct = 0.70
    elif hours_left > 12:
        stop_pct = 0.75
    else:
        stop_pct = 0.80

    abs_stop = entry_price - max(entry_price * (1 - stop_pct), 0.03)
    return round(max(entry_price * stop_pct, abs_stop), 4)


def _avg_price_from_fills(fills: list[dict]) -> float | None:
    total_size = sum(float(fill.get("size", 0.0) or 0.0) for fill in fills)
    if total_size <= 0:
        return None
    total_notional = sum(
        float(fill.get("size", 0.0) or 0.0) * float(fill.get("price", 0.0) or 0.0)
        for fill in fills
    )
    return round(total_notional / total_size, 6)


def _merge_new_fills(existing: list[dict], new_fills: list[dict]) -> list[dict]:
    known_ids = {str(fill.get("id") or "") for fill in existing}
    merged = list(existing)
    for fill in new_fills:
        fill_id = str(fill.get("id") or "")
        dedupe_key = (
            fill_id,
            round(float(fill.get("size", 0.0) or 0.0), 4),
            round(float(fill.get("price", 0.0) or 0.0), 6),
            str(fill.get("timestamp") or ""),
        )
        if fill_id and fill_id in known_ids:
            continue
        if not fill_id and any(
            (
                str(old.get("id") or ""),
                round(float(old.get("size", 0.0) or 0.0), 4),
                round(float(old.get("price", 0.0) or 0.0), 6),
                str(old.get("timestamp") or ""),
            ) == dedupe_key
            for old in merged
        ):
            continue
        merged.append(fill)
        if fill_id:
            known_ids.add(fill_id)
    return merged


def _sync_entry_execution_from_fills(pos: dict, trades: list[dict]) -> tuple[float, float]:
    fills = _merge_new_fills(pos.get("entry_fills", []), trades)
    pos["entry_fills"] = fills
    filled_shares = round(sum(float(fill.get("size", 0.0) or 0.0) for fill in fills), 4)
    filled_cost = round(
        sum(float(fill.get("size", 0.0) or 0.0) * float(fill.get("price", 0.0) or 0.0) for fill in fills),
        4,
    )
    avg_entry = _avg_price_from_fills(fills)
    pos["filled_shares"] = filled_shares
    pos["filled_cost"] = filled_cost
    pos["avg_entry_price"] = avg_entry or pos.get("avg_entry_price") or pos.get("entry_price")
    pos["shares"] = filled_shares
    pos["cost"] = filled_cost
    if avg_entry is not None:
        pos["entry_price"] = round(avg_entry, 4)
    return filled_shares, filled_cost


def _sync_exit_execution_from_fills(pos: dict, trades: list[dict]) -> tuple[float, float]:
    fills = _merge_new_fills(pos.get("exit_fills", []), trades)
    pos["exit_fills"] = fills
    filled_shares = round(sum(float(fill.get("size", 0.0) or 0.0) for fill in fills), 4)
    filled_value = round(
        sum(float(fill.get("size", 0.0) or 0.0) * float(fill.get("price", 0.0) or 0.0) for fill in fills),
        4,
    )
    avg_exit = _avg_price_from_fills(fills)
    pos["exit_filled_shares"] = filled_shares
    pos["exit_filled_value"] = filled_value
    if avg_exit is not None:
        pos["avg_exit_price"] = avg_exit
    return filled_shares, filled_value


def _materialize_entry_position(pos: dict, balance: float) -> float:
    reserved_cash = float(pos.get("reserved_cash", pos.get("requested_cost", pos.get("cost", 0.0))) or 0.0)
    filled_cost = float(pos.get("filled_cost", pos.get("cost", 0.0)) or 0.0)
    refund = max(0.0, round(reserved_cash - filled_cost, 2))
    if refund > 0:
        balance += refund
    pos["reserved_cash"] = filled_cost
    pos["shares"] = round(float(pos.get("filled_shares", pos.get("shares", 0.0)) or 0.0), 4)
    pos["cost"] = round(filled_cost, 4)
    pos["order_status"] = "filled"
    return balance


def _apply_partial_exit(pos: dict, filled_shares: float, filled_value: float) -> tuple[float, float]:
    if filled_shares <= 0:
        return 0.0, 0.0
    total_shares = float(pos.get("filled_shares", pos.get("shares", 0.0)) or pos.get("shares", 0.0) or 0.0)
    total_cost = float(pos.get("filled_cost", pos.get("cost", 0.0)) or pos.get("cost", 0.0) or 0.0)
    if total_shares <= 0:
        return 0.0, 0.0
    avg_entry = total_cost / total_shares
    realized_cost = round(avg_entry * filled_shares, 4)
    realized_pnl = round(filled_value - realized_cost, 2)
    remaining_shares = max(0.0, round(total_shares - filled_shares, 4))
    remaining_cost = max(0.0, round(total_cost - realized_cost, 4))
    pos["shares"] = remaining_shares
    pos["filled_shares"] = remaining_shares
    pos["cost"] = remaining_cost
    pos["filled_cost"] = remaining_cost
    pos["realized_exit_value"] = round(float(pos.get("realized_exit_value", 0.0) or 0.0) + filled_value, 4)
    pos["realized_pnl"] = round(float(pos.get("realized_pnl", 0.0) or 0.0) + realized_pnl, 2)
    return realized_cost, realized_pnl


def _estimate_entry_budget(balance: float, current_open: int, ask: float, kelly_adjusted: float, volume: float) -> float:
    concentration = portfolio_concentration_multiplier(current_open)
    visible_depth_cap = max(0.0, min(volume * ask * 0.01, settings.MAX_BET * 3))
    return adaptive_bet_size(
        kelly=kelly_adjusted,
        balance=balance,
        max_fraction_of_balance=0.12,
        max_fraction_of_depth=visible_depth_cap,
        concentration_multiplier=concentration,
    )


def _finalize_position_close(
    pos: dict,
    exit_price: float,
    close_reason: str,
    closed_at: str,
) -> float:
    shares = float(pos.get("shares", 0.0) or 0.0)
    pnl = round((exit_price - pos["entry_price"]) * shares, 2)
    pnl += round(float(pos.get("realized_pnl", 0.0) or 0.0), 2)
    pos["exit_price"] = round(exit_price, 4)
    pos["pnl"] = pnl
    pos["close_reason"] = close_reason
    pos["closed_at"] = closed_at
    pos["status"] = "closed"
    pos["exit_order_status"] = "filled"
    pos.pop("pending_close_reason", None)
    pos.pop("pending_close_requested_at", None)
    pos.pop("exit_target_price", None)
    pos.pop("exit_order_price", None)
    return pnl


def _queue_position_close(
    pos: dict,
    market: dict,
    current_price: float,
    close_reason: str,
    ts: str,
) -> tuple[bool, bool]:
    """
    Queue a close request.
    Returns (closed_now, pending_exit_started).
    """
    city_name = market.get("city_name", market.get("city", "?"))
    date = market.get("date", "")

    if pos.get("exit_order_status") == "pending":
        return False, True

    if not _is_production():
        _finalize_position_close(pos, current_price, close_reason, ts)
        return True, False

    sell_resp = _place_sell(pos, current_price, city_name, date)
    if not sell_resp:
        return False, False

    pos["exit_order_status"] = "pending"
    pos["pending_close_reason"] = close_reason
    pos["pending_close_requested_at"] = ts
    pos["exit_target_price"] = round(current_price, 4)
    pos["exit_order_price"] = round(float(sell_resp.get("price", current_price)), 4)
    pos["sell_order_id"] = sell_resp.get("order_id")
    return False, True


def _resolve_position_market_price(pos: dict, market: dict) -> float | None:
    current_price = None
    try:
        market_id = pos.get("market_id")
        if market_id:
            mdata = pm_read.get_market_detail(market_id)
            if mdata:
                quote_market = {
                    "bid": float(mdata.get("bestBid", 0.0) or 0.0),
                    "ask": float(mdata.get("bestAsk", 0.0) or 0.0),
                    "price": float(json.loads(mdata.get("outcomePrices", "[0.5,0.5]"))[0]),
                }
                best_bid, _, quote_mid = _quotes_for_side_from_outcome(quote_market, _get_position_side(pos))
                current_price = best_bid if best_bid > 0 else quote_mid
    except Exception:
        current_price = None

    if current_price is None:
        current_price = _position_current_price_from_outcomes(pos, market.get("all_outcomes", []))
    return current_price


def request_manual_close(identifier: str, date_str: str | None = None) -> tuple[bool, str]:
    """
    Manually request a position close by market_id or by city/date.
    Returns (success, user_message).
    """
    markets = load_all_markets()
    state = load_state()
    target = None
    identifier_norm = str(identifier or "").strip().lower()
    date_norm = str(date_str or "").strip()

    for market in markets:
        pos = market.get("position")
        if not pos or pos.get("status") != "open":
            continue

        city_slug = str(market.get("city", "")).strip().lower()
        city_name = str(market.get("city_name", "")).strip().lower()
        market_id = str(pos.get("market_id", "")).strip().lower()

        if date_norm:
            if market.get("date") == date_norm and identifier_norm in {city_slug, city_name}:
                target = market
                break
        elif identifier_norm and identifier_norm == market_id:
            target = market
            break

    if target is None:
        if date_norm:
            return False, f"No open position found for {identifier} {date_norm}."
        return False, f"No open position found for identifier {identifier}."

    pos = target["position"]
    if pos.get("exit_order_status") in ("pending", "partial"):
        return False, f"Exit already in progress for {target['city_name']} {target['date']}."

    current_price = _resolve_position_market_price(pos, target)
    if current_price is None or current_price <= 0:
        return False, f"Could not determine an exit price for {target['city_name']} {target['date']}."

    ts = _utc_now_iso()
    closed_now, pending_exit = _queue_position_close(pos, target, float(current_price), "manual_close", ts)
    save_market(target)

    if closed_now:
        proceeds = round(float(pos.get("shares", 0.0) or 0.0) * float(pos.get("exit_price", current_price) or current_price), 2)
        state["balance"] = round(float(state.get("balance", 0.0) or 0.0) + proceeds, 2)
        state["peak_balance"] = max(float(state.get("peak_balance", state["balance"]) or state["balance"]), float(state["balance"]))
        save_state(state)
        pnl = float(pos.get("pnl", 0.0) or 0.0)
        return True, (
            f"Closed {target['city_name']} {target['date']} at ${float(pos.get('exit_price', current_price)):.3f}. "
            f"PnL: {pnl:+.2f}"
        )

    if pending_exit:
        order_id = str(pos.get("sell_order_id", "") or "")
        exit_price = float(pos.get("exit_order_price", current_price) or current_price)
        suffix = f" Order {order_id}." if order_id else "."
        return True, (
            f"Exit queued for {target['city_name']} {target['date']} at about ${exit_price:.3f}.{suffix}"
        )

    return False, f"Could not queue exit for {target['city_name']} {target['date']}."


# ═══════════════════════════════════════════════════════════
# FULL SCAN
# ═══════════════════════════════════════════════════════════

def scan_and_update() -> tuple[int, int, int]:
    """
    Main cycle: update forecasts, open/close positions.
    Returns (new_positions, closed, resolved).
    """
    from core.forecasts import take_forecast_snapshot
    settings.reload_risk_config()

    now = datetime.now(timezone.utc)
    state = load_state()
    balance = state["balance"]
    new_pos = 0
    closed = 0
    resolved_count = 0
    production = _is_production()
    thresholds = _rollout_thresholds()
    cycle_stats = _new_cycle_stats(thresholds)
    balance, _ = _check_pending_fills(load_all_markets(), balance)

    # ── Max open positions guard ──────────────────────────
    MAX_OPEN_POSITIONS = 5  # Never allocate more than 5 simultaneous bets
    all_markets_snapshot = load_all_markets()
    current_open = sum(
        1 for m in all_markets_snapshot
        if m.get("position") and m["position"].get("status") == "open"
    )

    # ── Per-city state: which cities already have open positions?
    cities_with_open_pos: set[str] = set()
    # Cooldown: city+date pairs that lost recently (24h lockout)
    _24h_ago = (now - timedelta(hours=24)).isoformat()
    cooldown_keys: set[str] = set()
    for _m in all_markets_snapshot:
        _p = _m.get("position")
        if _p:
            if _p.get("status") == "open":
                cities_with_open_pos.add(_m["city"])
            elif (
                _p.get("status") == "closed"
                and _p.get("close_reason") in ("stop_loss", "forecast_shift_close")
                and _p.get("closed_at", "") >= _24h_ago
            ):
                cooldown_keys.add(f"{_m['city']}_{_m['date']}")

    # ── Stale ghost market cleanup ──────────────────────────
    # Markets >24h past their resolution date with open positions
    # are ghost trades: force-close them to unlock city slots
    for _m in all_markets_snapshot:
        if _m.get("status") == "resolved":
            continue
        _end = _m.get("event_end_date", "")
        if not _end:
            continue
        _overdue_hours = -pm_read.hours_to_resolution(_end)  # negative = past
        if _overdue_hours > 24:
            _p = _m.get("position")
            if _p and _p.get("status") == "open":
                logger.warning(
                    "[CLEANUP] Ghost market %s %s overdue %.0fh — force-closing",
                    _m["city"], _m["date"], _overdue_hours
                )
                _p["status"] = "closed"
                _p["close_reason"] = "ghost_cleanup"
                _p["pnl"] = 0.0
                _p["closed_at"] = now.isoformat()
                _m["status"] = "resolved"
                save_market(_m)  # already imported at module level

    # ── Drawdown kill-switch ─────────────────────────────
    # Pause new entries if we've lost >50% from starting balance
    starting_bal = state.get("starting_balance", settings.BALANCE) or settings.BALANCE
    drawdown_pct = (balance - starting_bal) / starting_bal if starting_bal > 0 else 0
    drawdown_kill = drawdown_pct <= -0.50  # Lost 50% or more
    if drawdown_kill:
        logger.warning(
            "[DRAWDOWN] Balance $%.2f is %.0f%% below start $%.2f — suspending new entries",
            balance, abs(drawdown_pct) * 100, starting_bal
        )
        _notify(
            f"⚠️ [DRAWDOWN GUARD] Balance ${balance:.2f} is {abs(drawdown_pct):.0%} below "
            f"starting ${starting_bal:.2f}. New entries SUSPENDED. \n"
            f"Use /setrisk balance <amount> to reset when ready."
        )

    for city_slug, loc in LOCATIONS.items():
        unit = loc["unit"]
        unit_sym = "F" if unit == "F" else "C"

        # Liquidity window: skip cities where local time is 00h-07h
        # Markets have very low volume and wide spreads during these hours
        try:
            from config.locations import TIMEZONES
            import zoneinfo
            city_tz = zoneinfo.ZoneInfo(TIMEZONES.get(city_slug, "UTC"))
            local_hour = now.astimezone(city_tz).hour
            if 0 <= local_hour < 7:
                logger.debug("[SKIP] city=%s local_hour=%d (liquidity window)", city_slug, local_hour)
                continue
        except Exception:
            pass  # If tz lookup fails, proceed normally

        try:
            dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]
            snapshots = take_forecast_snapshot(city_slug, dates)
            time.sleep(0.3)
        except Exception as e:
            logger.warning("[SCAN] %s skipped: %s", loc["name"], e)
            continue

        for i, date in enumerate(dates):
            cycle_stats["expected_events"] += 1
            dt = datetime.strptime(date, "%Y-%m-%d")
            event = pm_read.get_event(
                city_slug,
                settings.MONTHS[dt.month - 1],
                dt.day,
                dt.year,
            )
            if not event:
                cycle_stats["discard_reasons"]["event_not_found"] += 1
                logger.info("[DISCARD] reason=event_not_found city=%s date=%s", city_slug, date)
                continue
            cycle_stats["events_found"] += 1

            end_date = event.get("endDate", "")
            hours = pm_read.hours_to_resolution(end_date) if end_date else 0
            horizon = f"D+{i}"

            # Load or create market record
            mkt = load_market(city_slug, date)
            if mkt is None:
                if hours < settings.MIN_HOURS or hours > settings.MAX_HOURS:
                    cycle_stats["discard_reasons"]["hours"] += 1
                    logger.info("[DISCARD] reason=hours city=%s date=%s hours=%.1f range=[%.1f,%.1f]", city_slug, date, hours, settings.MIN_HOURS, settings.MAX_HOURS)
                    continue
                mkt = new_market(city_slug, date, event, hours)

            if mkt["status"] == "resolved":
                continue

            # ── Parse all outcomes ───────────────────────
            outcomes = []
            for market in event.get("markets", []):
                cycle_stats["markets_read"] += 1
                question = market.get("question", "")
                mid = str(market.get("id", ""))
                volume = float(market.get("volume", 0))
                rng = pm_read.parse_temp_range(question)
                if not rng:
                    continue
                try:
                    prices = json.loads(market.get("outcomePrices", "[0.5,0.5]"))
                    # Gamma outcomePrices is [YES, NO], not [bid, ask].
                    yes_price = float(prices[0])
                    best_bid_raw = market.get("bestBid")
                    best_ask_raw = market.get("bestAsk")
                    bid = float(best_bid_raw) if best_bid_raw is not None else yes_price
                    ask = float(best_ask_raw) if best_ask_raw is not None else yes_price
                    if ask < bid:
                        ask = bid
                except Exception:
                    continue
                outcomes.append({
                    "question":  question,
                    "market_id": mid,
                    "range":     rng,
                    "bid":       round(bid, 4),
                    "ask":       round(ask, 4),
                    "price":     round(yes_price, 4),
                    "spread":    round(ask - bid, 4),
                    "volume":    round(volume, 0),
                })
                cycle_stats["markets_valid"] += 1

            outcomes.sort(key=lambda x: x["range"][0])
            mkt["all_outcomes"] = outcomes

            # ── Forecast snapshot ────────────────────────
            snap = snapshots.get(date, {})
            forecast_snap = {
                "ts":          snap.get("ts"),
                "horizon":     horizon,
                "hours_left":  round(hours, 1),
                "ecmwf":       snap.get("ecmwf"),
                "hrrr":        snap.get("hrrr"),
                "metar":       snap.get("metar"),
                "best":        snap.get("best"),
                "best_source": snap.get("best_source"),
            }
            mkt["forecast_snapshots"].append(forecast_snap)

            # ── Market price snapshot ────────────────────
            top = max(outcomes, key=lambda x: x["price"]) if outcomes else None
            market_snap = {
                "ts":         snap.get("ts"),
                "top_bucket": f"{top['range'][0]}-{top['range'][1]}{unit_sym}" if top else None,
                "top_price":  top["price"] if top else None,
            }
            mkt["market_snapshots"].append(market_snap)

            forecast_temp = snap.get("best")
            best_source = snap.get("best_source")
            all_forecasts = snap.get("all_forecasts", [])

            # ── ADAPTIVE STOP-LOSS / TRAILING STOP ────────
            if mkt.get("position") and mkt["position"].get("status") == "open":
                pos = mkt["position"]
                current_price = None if pos.get("exit_order_status") == "pending" else _position_current_price_from_outcomes(pos, outcomes)

                if current_price is not None:
                    entry = pos["entry_price"]
                    # Adaptive stop: percentage-based but with an absolute floor.
                    # For cheap tokens, a tight % stop triggers on noise — use absolute $0.03 min drop.
                    if hours > 48:
                        stop_pct = 0.65   # Far out: loose — forecasts will shift
                    elif hours > 24:
                        stop_pct = 0.70
                    elif hours > 12:
                        stop_pct = 0.75
                    else:
                        stop_pct = 0.80   # Near resolution: tighter
                    
                    # Absolute floor: never stop unless we've lost at least $0.03 per share
                    abs_stop = entry - max(entry * (1 - stop_pct), 0.03)
                    default_stop = max(entry * stop_pct, abs_stop)
                    stop = pos.get("stop_price", default_stop)

                    # Trailing: if up 20%+, move stop to breakeven
                    if current_price >= entry * 1.20 and stop < entry:
                        pos["stop_price"] = entry
                        pos["trailing_activated"] = True

                    if current_price <= stop:
                        close_reason = "stop_loss" if current_price < entry else "trailing_stop"
                        closed_now, pending_exit = _queue_position_close(
                            pos, mkt, current_price, close_reason, snap.get("ts") or now.isoformat()
                        )
                        if closed_now:
                            pnl = pos.get("pnl", 0.0)
                            balance += pos["cost"] + pnl
                            closed += 1
                            reason, emoji = _close_reason_label(close_reason)
                            _notify(
                                f"{emoji} [{reason}] {loc['name']} {date} | "
                                f"{_get_position_side(pos)} entry ${entry:.3f} exit ${current_price:.3f} | "
                                f"PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
                            )
                        elif pending_exit:
                            reason, emoji = _close_reason_label(close_reason)
                            _notify(
                                f"{emoji} [EXIT PENDING] {loc['name']} {date} | "
                                f"{_get_position_side(pos)} @ ${current_price:.3f} | waiting for SELL fill"
                            )

                    if current_price <= stop and False:
                        pnl = round((current_price - entry) * pos["shares"], 2)
                        balance += pos["cost"] + pnl
                        pos["closed_at"] = snap.get("ts")
                        pos["close_reason"] = "stop_loss" if current_price < entry else "trailing_stop"
                        pos["exit_price"] = current_price
                        pos["pnl"] = pnl
                        pos["status"] = "closed"
                        closed += 1
                        reason = "STOP" if current_price < entry else "TRAILING"
                        msg = f"🛑 [{reason}] {loc['name']} {date} | entry ${entry:.3f} exit ${current_price:.3f} | PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
                        _notify(msg)

            # ── CLOSE on forecast shift ──────────────────
            if (
                mkt.get("position")
                and mkt["position"].get("status") == "open"
                and forecast_temp is not None
            ):
                pos = mkt["position"]
                if pos.get("exit_order_status") == "pending":
                    save_market(mkt)
                    time.sleep(0.1)
                    continue
                old_low = pos["bucket_low"]
                old_high = pos["bucket_high"]
                buffer = 2.0 if unit == "F" else 1.0
                mid_bucket = (old_low + old_high) / 2 if old_low != -999 and old_high != 999 else forecast_temp
                forecast_far = abs(forecast_temp - mid_bucket) > (abs(mid_bucket - old_low) + buffer)

                if not in_bucket(forecast_temp, old_low, old_high) and forecast_far:
                    current_price = _position_current_price_from_outcomes(pos, outcomes)
                    if current_price is not None:
                        closed_now, pending_exit = _queue_position_close(
                            pos, mkt, current_price, "forecast_shift_close", snap.get("ts") or now.isoformat()
                        )
                        if closed_now:
                            pnl = pos.get("pnl", 0.0)
                            balance += pos["cost"] + pnl
                            closed += 1
                            _notify(
                                f"🔄 [CLOSE] {loc['name']} {date} — forecast shifted | "
                                f"{_get_position_side(pos)} PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
                            )
                        elif pending_exit:
                            _notify(
                                f"🔄 [EXIT PENDING] {loc['name']} {date} — forecast shifted, "
                                f"waiting for {_get_position_side(pos)} exit fill at ${current_price:.3f}"
                            )

                    current_price = None
                    for o in outcomes:
                        if o["market_id"] == pos["market_id"]:
                            current_price = o["price"]
                            break
                    if current_price is not None and False:
                        pnl = round((current_price - pos["entry_price"]) * pos["shares"], 2)
                        balance += pos["cost"] + pnl
                        pos["closed_at"] = snap.get("ts")
                        pos["close_reason"] = "forecast_changed"
                        pos["exit_price"] = current_price
                        pos["pnl"] = pnl
                        pos["status"] = "closed"
                        closed += 1
                        msg = f"🔄 [CLOSE] {loc['name']} {date} — forecast shifted | PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
                        _notify(msg)

            # ── OPEN POSITION (v3.1 — improved) ──────────
            if not mkt.get("position") and forecast_temp is not None and hours >= settings.MIN_HOURS:
                # Hard cap: don't open if already at max positions
                if (current_open + new_pos) >= MAX_OPEN_POSITIONS:
                    logger.debug("[SKIP] Max open positions reached (%d)", MAX_OPEN_POSITIONS)
                    continue
                # Drawdown kill-switch: suspend new entries during severe drawdown
                if drawdown_kill:
                    continue
                # Per-city cap: only 1 open position per city at a time
                if city_slug in cities_with_open_pos:
                    logger.debug("[SKIP] city=%s already has an open position", city_slug)
                    continue
                # Cooldown: skip city+date that suffered a recent stop-loss
                cooldown_key = f"{city_slug}_{date}"
                if cooldown_key in cooldown_keys:
                    logger.info("[SKIP] city=%s date=%s in 24h loss cooldown", city_slug, date)
                    continue
                # 1. Dynamic sigma: base + forecast disagreement + horizon scaling
                base_sigma = get_sigma(city_slug, best_source or "ecmwf")
                sigma = forecast_disagreement_sigma(all_forecasts, base_sigma, hours)

                # 2. Time-based confidence
                conf = confidence_by_time(hours)

                best_signal = None
                primary_bucket_index = None
                for idx, o in enumerate(outcomes):
                    t_low, t_high = o["range"]
                    if in_bucket(forecast_temp, t_low, t_high):
                        primary_bucket_index = idx
                        break

                for idx, o in enumerate(outcomes):
                    t_low, t_high = o["range"]
                    volume = o["volume"]
                    p_yes_raw = bucket_prob(forecast_temp, t_low, t_high, sigma)
                    bucket_distance = abs(idx - primary_bucket_index) if primary_bucket_index is not None else idx
                    conf_adj = conf
                    if bucket_distance == 1:
                        conf_adj *= ADJACENT_BUCKET_CONFIDENCE_PENALTY
                    if t_low == t_high:
                        conf_adj *= EXACT_BUCKET_CONFIDENCE_PENALTY
                    p_yes = _shrink_probability(p_yes_raw, conf_adj)

                    for side in ("YES", "NO"):
                        bid, ask, price_mid = _quotes_for_side_from_outcome(o, side)
                        spread = max(0.0, round(ask - bid, 4))

                        if price_mid <= 0:
                            cycle_stats["discard_reasons"]["price"] += 1
                            continue
                        spread_ratio = spread / price_mid
                        if spread_ratio > thresholds["max_relative_spread"]:
                            cycle_stats["discard_reasons"]["spread_relative"] += 1
                            continue
                        if volume < thresholds["min_volume"]:
                            cycle_stats["discard_reasons"]["volume"] += 1
                            continue
                        if ask >= thresholds["max_price"] or ask < settings.MIN_PRICE:
                            cycle_stats["discard_reasons"]["price"] += 1
                            continue

                        p = p_yes if side == "YES" else round(1.0 - p_yes, 4)
                        edge = calc_edge(p, ask)
                        ev_after_costs = calc_ev_after_costs(p, ask, spread)
                        if ev_after_costs < thresholds["min_edge"]:
                            cycle_stats["discard_reasons"]["ev"] += 1
                            continue

                        kelly = calc_kelly(p, ask)
                        lm_mult = late_market_multiplier(hours)
                        kelly_adjusted = min(kelly * lm_mult, 0.25)
                        size = _estimate_entry_budget(
                            balance=balance,
                            current_open=current_open + new_pos,
                            ask=ask,
                            kelly_adjusted=kelly_adjusted,
                            volume=volume,
                        )
                        if size < 0.50:
                            continue

                        bucket_priority = "primary" if bucket_distance == 0 else "adjacent" if bucket_distance == 1 else "global"
                        question = o["question"] if side == "YES" else f"NO | {o['question']}"
                        candidate = {
                            "market_id":     o["market_id"],
                            "question":      question,
                            "bucket_low":    t_low,
                            "bucket_high":   t_high,
                            "entry_price":   ask,
                            "bid_at_entry":  bid,
                            "spread":        spread,
                            "shares":        round(size / ask, 2),
                            "cost":          size,
                            "requested_shares": round(size / ask, 2),
                            "requested_cost": round(size, 2),
                            "reserved_cash": round(size, 2),
                            "filled_shares": 0.0,
                            "filled_cost": 0.0,
                            "entry_fills": [],
                            "exit_fills": [],
                            "realized_pnl": 0.0,
                            "realized_exit_value": 0.0,
                            "p":             round(p, 4),
                            "p_raw":         round(p_yes_raw if side == "YES" else (1.0 - p_yes_raw), 4),
                            "confidence":    round(conf_adj, 2),
                            "adjusted_confidence": round(conf_adj, 2),
                            "edge":          round(edge, 4),
                            "kelly":         round(kelly_adjusted, 4),
                            "kelly_raw":     round(kelly, 4),
                            "lm_mult":       lm_mult,
                            "bucket_priority": bucket_priority,
                            "forecast_temp": forecast_temp,
                            "forecast_src":  best_source,
                            "volume":        volume,
                            "sigma":         round(sigma, 2),
                            "sigma_base":    round(base_sigma, 2),
                            "hours_left":    round(hours, 1),
                            "opened_at":     snap.get("ts"),
                            "status":        "open",
                            "pnl":           None,
                            "exit_price":    None,
                            "close_reason":  None,
                            "closed_at":     None,
                            "forecast_at_entry": forecast_temp,
                            "side":          side,
                        }
                        _set_signal_ev_fields(candidate, round(ev_after_costs, 4))

                        if not best_signal or candidate["net_ev"] > best_signal["net_ev"]:
                            best_signal = candidate

                if best_signal:
                    # Fetch real ask from Polymarket for accurate entry
                    skip = False
                    try:
                        mdata = pm_read.get_market_detail(best_signal["market_id"])
                        if mdata:
                            quote_market = {
                                "bid": float(mdata.get("bestBid", best_signal["bid_at_entry"])),
                                "ask": float(mdata.get("bestAsk", best_signal["entry_price"])),
                                "price": float(json.loads(mdata.get("outcomePrices", "[0.5,0.5]"))[0]),
                            }
                            real_bid, real_ask, _ = _quotes_for_side_from_outcome(quote_market, _get_position_side(best_signal))
                            real_spread = round(real_ask - real_bid, 4)
                            if real_ask >= thresholds["max_price"]:
                                cycle_stats["discard_reasons"]["price"] += 1
                                logger.info("[DISCARD] reason=price city=%s date=%s market=%s ask=%.4f max=%.4f", city_slug, date, best_signal["market_id"], real_ask, thresholds["max_price"])
                                skip = True
                            elif real_spread > thresholds["max_slippage"]:
                                cycle_stats["discard_reasons"]["slippage"] += 1
                                logger.info("[DISCARD] reason=slippage city=%s date=%s market=%s spread=%.4f max=%.4f", city_slug, date, best_signal["market_id"], real_spread, thresholds["max_slippage"])
                                skip = True
                            else:
                                best_signal["entry_price"] = real_ask
                                best_signal["bid_at_entry"] = real_bid
                                best_signal["spread"] = real_spread
                                # Recalculate with real execution data (keep legacy `ev` key for compatibility)
                                best_signal["edge"] = round(calc_edge(best_signal["p"], real_ask), 4)
                                real_ev = round(calc_ev_after_costs(best_signal["p"], real_ask, real_spread), 4)
                                _set_signal_ev_fields(best_signal, real_ev)
                                real_kelly = calc_kelly(best_signal["p"], real_ask)
                                real_kelly_adjusted = min(real_kelly * best_signal.get("lm_mult", 1.0), 0.25)
                                real_size = _estimate_entry_budget(
                                    balance=balance,
                                    current_open=current_open + new_pos,
                                    ask=real_ask,
                                    kelly_adjusted=real_kelly_adjusted,
                                    volume=best_signal.get("volume", 0.0),
                                )
                                if real_size < 0.50:
                                    skip = True
                                else:
                                    best_signal["kelly_raw"] = round(real_kelly, 4)
                                    best_signal["kelly"] = round(real_kelly_adjusted, 4)
                                    best_signal["cost"] = round(real_size, 2)
                                    best_signal["requested_cost"] = best_signal["cost"]
                                    best_signal["reserved_cash"] = best_signal["cost"]
                                    best_signal["shares"] = round(best_signal["cost"] / real_ask, 2)
                                    best_signal["requested_shares"] = best_signal["shares"]
                    except Exception as e:
                        logger.warning("[SCAN] Could not fetch real ask: %s", e)

                    if not skip:
                        # ── Execute trade ────────────────
                        if production:
                            try:
                                # Find the token_id from market data
                                mdata = pm_read.get_market_detail(best_signal["market_id"])
                                token_id = None
                                if mdata:
                                    yes_token_id, no_token_id = _extract_token_ids(mdata)
                                    token_id = yes_token_id if _get_position_side(best_signal) == "YES" else no_token_id

                                if token_id:
                                    book_estimate = pm_trade.estimate_limit_price_from_book(
                                        token_id=token_id,
                                        side="BUY",
                                        size=best_signal["requested_shares"],
                                    )
                                    if book_estimate:
                                        book_price = float(book_estimate.get("price", best_signal["entry_price"]))
                                        book_avg = float(book_estimate.get("avg_price", book_price))
                                        visible_shares = float(book_estimate.get("filled_size", best_signal["requested_shares"]))
                                        conservative_shares = min(best_signal["requested_shares"], max(0.0, visible_shares * 0.75))
                                        if conservative_shares * max(book_price, 0.01) < 0.50:
                                            skip = True
                                        else:
                                            best_signal["shares"] = round(conservative_shares, 2)
                                            best_signal["requested_shares"] = best_signal["shares"]
                                            best_signal["cost"] = round(best_signal["shares"] * book_avg, 2)
                                            best_signal["requested_cost"] = best_signal["cost"]
                                            best_signal["reserved_cash"] = best_signal["cost"]
                                            best_signal["entry_price"] = round(book_price, 4)
                                            best_signal["expected_fill_price"] = round(book_avg, 4)
                                            best_signal["book_coverage"] = float(book_estimate.get("coverage", 0.0))
                                    resp = None if skip else pm_trade.place_limit_order(
                                        token_id=token_id,
                                        price=best_signal["entry_price"],
                                        size=best_signal["shares"],
                                        side="BUY",
                                    )
                                    if skip:
                                        pass
                                    elif resp:
                                        order_id = resp.get("orderID") or resp.get("id")
                                        best_signal["clob_order_id"] = order_id
                                        best_signal["token_id"] = token_id
                                        best_signal["order_status"] = "pending"
                                        best_signal["order_placed_at"] = now.isoformat()
                                        logger.info(
                                            "[TRADE] BUY placed order_id=%s token=%s shares=%.2f @ $%.3f",
                                            order_id, token_id, best_signal["shares"], best_signal["entry_price"]
                                        )
                                    else:
                                        logger.error("[TRADE] Order returned None — skipping")
                                        skip = True
                                else:
                                    logger.error("[TRADE] Could not resolve token_id for %s", best_signal["market_id"])
                                    skip = True
                            except Exception as e:
                                logger.error("[TRADE] Order failed: %s", e)
                                _notify(f"❌ Order failed for {loc['name']} {date}: {e}")
                                skip = True

                        if not skip:
                            if not production:
                                best_signal["order_status"] = "filled"
                                best_signal["filled_shares"] = best_signal["shares"]
                                best_signal["filled_cost"] = best_signal["cost"]
                                best_signal["avg_entry_price"] = best_signal["entry_price"]
                            balance -= best_signal["cost"]
                            mkt["position"] = best_signal
                            state["total_trades"] += 1
                            new_pos += 1
                            cities_with_open_pos.add(city_slug)  # prevent same-city re-entry this cycle
                            cycle_stats["signals_generated"] += 1
                            cycle_stats["net_ev_sum"] += best_signal.get("net_ev", 0.0)
                            cycle_stats["real_spread_sum"] += best_signal.get("spread", 0.0)
                            cycle_stats["signals_by_city"][city_slug] = cycle_stats["signals_by_city"].get(city_slug, 0) + 1
                            bucket_label = f"{best_signal['bucket_low']}-{best_signal['bucket_high']}{unit_sym}"
                            mode_tag = "🔴 PROD" if production else "🟡 SIM"
                            msg = (
                                f"📈 [{mode_tag}] BUY {loc['name']} {horizon} {date}\n"
                                f"   {bucket_label} @ ${best_signal['entry_price']:.3f}\n"
                                f"   Edge {best_signal['edge']:+.2%} | Net EV {best_signal['net_ev']:+.4f} | ${best_signal['cost']:.2f}\n"
                                f"   σ={best_signal['sigma']:.1f} | conf={best_signal['confidence']:.0%} | {best_signal['forecast_src'].upper()}"
                            )
                            _notify(msg)

                            # Log prediction for calibration curve (with execution costs)
                            log_prediction(
                                city=city_slug, date=date,
                                p=best_signal["p"], edge=best_signal["edge"],
                                price=best_signal["entry_price"],
                                source=best_signal["forecast_src"] or "",
                                sigma=best_signal["sigma"],
                                confidence=best_signal["confidence"],
                                spread=best_signal.get("spread", 0.0),
                                ev_after_costs=best_signal.get("net_ev", 0.0),
                            )

            # Market closed by time
            if hours < 0.5 and mkt["status"] == "open":
                mkt["status"] = "closed"

            save_market(mkt)
            time.sleep(0.1)

    # ── AUTO-RESOLUTION ──────────────────────────────────
    for mkt in load_all_markets():
        if mkt["status"] == "resolved":
            continue
        pos = mkt.get("position")
        if not pos or pos.get("status") != "open":
            continue
        if pos.get("order_status") in ("pending", "partial") or pos.get("exit_order_status") in ("pending", "partial"):
            continue
        market_id = pos.get("market_id")
        if not market_id:
            continue

        yes_won = pm_read.check_market_resolved(market_id)
        if yes_won is None:
            continue

        position_won = yes_won if _get_position_side(pos) == "YES" else not yes_won
        price = float(pos.get("entry_price", 0.0) or 0.0)
        size = float(pos.get("cost", 0.0) or 0.0)
        shares = float(pos.get("shares", 0.0) or 0.0)
        pnl = round(shares * (1 - price), 2) if position_won else round(-size, 2)

        try:
            from core.forecasts import get_actual_temp
            actual_temp = get_actual_temp(mkt["city"], mkt["date"] )
            if actual_temp is not None:
                mkt["actual_temp"] = actual_temp
        except Exception:
            pass

        balance += size + pnl
        pos["exit_price"] = 1.0 if position_won else 0.0
        pos["pnl"] = pnl
        pos["close_reason"] = "resolved"
        pos["closed_at"] = now.isoformat()
        pos["status"] = "closed"
        mkt["pnl"] = pnl
        mkt["status"] = "resolved"
        mkt["resolved_outcome"] = "win" if position_won else "loss"

        record_outcome(mkt["city"], mkt["date"], position_won)

        if position_won:
            state["wins"] += 1
        else:
            state["losses"] += 1

        emoji = "✅" if position_won else "❌"
        result = "WIN" if position_won else "LOSS"
        msg = f"{emoji} [{result}] {mkt['city_name']} {mkt['date']} | {_get_position_side(pos)} PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
        _notify(msg)
        resolved_count += 1
        save_market(mkt)
        time.sleep(0.3)
    state["balance"] = round(balance, 2)
    state["peak_balance"] = max(state.get("peak_balance", balance), balance)
    save_state(state)

    # Calibration
    all_mkts = load_all_markets()
    resolved_total = len([m for m in all_mkts if m["status"] == "resolved"])
    if resolved_total >= settings.CALIBRATION_MIN:
        run_calibration(all_mkts)

    _log_cycle_metrics(cycle_stats)
    return new_pos, closed, resolved_count


# ═══════════════════════════════════════════════════════════
# PRODUCTION HELPERS
# ═══════════════════════════════════════════════════════════

_CLOB_RECONCILED = False  # Run once per process startup


def _reconcile_clob_on_startup():
    """
    On first run in production, compare internal open positions vs actual CLOB.
    Positions whose GTC order is no longer open and not filled → ghost_cleanup.
    """
    global _CLOB_RECONCILED
    if _CLOB_RECONCILED or not _is_production():
        _CLOB_RECONCILED = True
        return
    _CLOB_RECONCILED = True

    try:
        open_clob_ids = {
            str(o.get("id") or o.get("orderID", ""))
            for o in pm_trade.get_open_orders()
        }
        markets = load_all_markets()
        cleaned = 0
        for mkt in markets:
            pos = mkt.get("position")
            if not pos or pos.get("status") != "open":
                continue
            order_id = pos.get("clob_order_id")
            if not order_id:
                continue
            if pos.get("order_status") in ("filled", None):
                continue
            if order_id not in open_clob_ids:
                detail = pm_trade.get_order_status_detail(order_id)
                fills = pm_trade.get_trades(order_id=order_id, token_id=pos.get("token_id"))
                filled_shares, _ = _sync_entry_execution_from_fills(pos, fills)
                fill_status = detail.get("status")
                if fill_status == "matched" and filled_shares > 0:
                    pos["order_status"] = "filled"
                    logger.info("[RECONCILE] order %s confirmed filled", order_id)
                else:
                    pos["status"] = "closed"
                    pos["close_reason"] = "ghost_cleanup"
                    pos["pnl"] = 0.0
                    pos["closed_at"] = datetime.now(timezone.utc).isoformat()
                    mkt["status"] = "resolved"
                    logger.warning("[RECONCILE] order %s vanished — cleaned ghost", order_id)
                    cleaned += 1
                save_market(mkt)
        if cleaned:
            _notify(f"🔧 [RECONCILE] {cleaned} ghost order(s) cleaned on startup")
    except Exception as e:
        logger.error("[RECONCILE] Failed: %s", e)


def _place_sell(pos: dict, current_price: float, city_name: str, date: str) -> dict | None:
    """
    Place a SELL limit order in production to close a position.
    In simulation: no-op, returns a synthetic filled response.
    Returns metadata for the pending exit, or None on hard failure.
    """
    if not _is_production():
        return {"order_id": "simulated", "price": round(current_price, 4)}

    token_id = pos.get("token_id")
    shares = float(pos.get("shares", 0.0) or 0.0)

    if not token_id:
        logger.warning("[SELL] No token_id stored for %s %s — cannot SELL", city_name, date)
        _notify(
            f"⚠️ [PROD] No token_id for {city_name} {date} — "
            f"position marked closed internally but NO SELL ORDER was placed. "
            f"Please close manually on Polymarket!"
        )
        return None

    if shares < 0.01:
        logger.info("[SELL] Shares %.4f too small — skipping SELL", shares)
        return {"order_id": "tiny_position", "price": round(current_price, 4)}

    book_estimate = pm_trade.estimate_limit_price_from_book(
        token_id=token_id,
        side="SELL",
        size=shares,
    )
    if book_estimate and float(book_estimate.get("filled_size", 0.0) or 0.0) > 0:
        sell_price = max(0.01, round(float(book_estimate.get("price", current_price) or current_price), 4))
        pos["exit_book_coverage"] = float(book_estimate.get("coverage", 0.0) or 0.0)
        pos["expected_exit_price"] = round(float(book_estimate.get("avg_price", sell_price) or sell_price), 4)
    else:
        sell_price = max(0.01, round(current_price - 0.01, 4))

    try:
        resp = pm_trade.place_limit_order(
            token_id=token_id,
            price=sell_price,
            size=shares,
            side="SELL",
        )
        if resp:
            sell_id = resp.get("orderID") or resp.get("id")
            pos["sell_order_id"] = sell_id
            pos["exit_requested_shares"] = shares
            logger.info("[SELL] Order placed id=%s @ $%.3f × %.2f shares", sell_id, sell_price, shares)
            return {"order_id": sell_id, "price": sell_price}
        else:
            logger.error("[SELL] Order returned None for %s %s", city_name, date)
            _notify(f"⚠️ [SELL FAILED] {city_name} {date} — SELL returned None. Close manually on Polymarket!")
            return None
    except Exception as e:
        logger.error("[SELL] Order failed for %s %s: %s", city_name, date, e)
        _notify(f"⚠️ [SELL FAILED] {city_name} {date}: {e}\nClose manually on Polymarket!")
        return None


def _check_pending_fills(markets: list, balance: float) -> tuple[float, int]:
    """
    Reconcile pending entry and exit orders.
    Returns (updated_balance, closed_count).
    """
    if not _is_production():
        for mkt in markets:
            pos = mkt.get("position")
            if not pos:
                continue
            if pos.get("order_status") == "pending":
                pos["order_status"] = "filled"
                pos["filled_shares"] = pos.get("shares", 0.0)
                pos["filled_cost"] = pos.get("cost", 0.0)
                save_market(mkt)
            if pos.get("exit_order_status") == "pending":
                close_reason = _pending_exit_reason(pos)
                exit_price = float(pos.get("exit_target_price", pos.get("entry_price", 0.0)))
                proceeds = round(float(pos.get("shares", 0.0) or 0.0) * exit_price, 2)
                pnl = _finalize_position_close(pos, exit_price, close_reason, _utc_now_iso())
                balance += proceeds
                save_market(mkt)
        return balance, 0

    fill_timeout_seconds = 30 * 60
    closed_count = 0

    for mkt in markets:
        pos = mkt.get("position")
        if not pos:
            continue

        if pos.get("order_status") in ("pending", "partial"):
            order_id = pos.get("clob_order_id")
            placed_at_str = pos.get("order_placed_at", "")

            if not order_id:
                pos["order_status"] = "filled"
                save_market(mkt)
            else:
                try:
                    placed_at = datetime.fromisoformat(placed_at_str.replace("Z", "+00:00"))
                    age_seconds = (datetime.now(timezone.utc) - placed_at).total_seconds()
                except Exception:
                    age_seconds = 0

                detail = pm_trade.get_order_status_detail(order_id)
                entry_trades = pm_trade.get_trades(order_id=order_id, token_id=pos.get("token_id"))
                filled_shares, filled_cost = _sync_entry_execution_from_fills(pos, entry_trades)
                fill_status = detail.get("status")
                matched_size = max(float(detail.get("matched_size", 0.0) or 0.0), filled_shares)
                if matched_size > filled_shares:
                    avg_price = float(detail.get("avg_price", pos.get("entry_price", 0.0)) or pos.get("entry_price", 0.0))
                    synthetic_fill = {
                        "id": f"detail-{order_id}-{matched_size}",
                        "order_id": order_id,
                        "token_id": pos.get("token_id", ""),
                        "side": "BUY",
                        "size": round(matched_size - filled_shares, 4),
                        "price": round(avg_price, 6),
                        "timestamp": _utc_now_iso(),
                    }
                    filled_shares, filled_cost = _sync_entry_execution_from_fills(pos, [synthetic_fill])

                if fill_status == "matched" and filled_shares > 0:
                    balance = _materialize_entry_position(pos, balance)
                    logger.info("[FILL] Entry order %s confirmed filled %.2f shares", order_id, filled_shares)
                    save_market(mkt)
                elif fill_status == "partial" and filled_shares > 0:
                    pos["order_status"] = "partial"
                    save_market(mkt)
                elif fill_status == "cancelled":
                    reserved_cash = float(pos.get("reserved_cash", pos.get("requested_cost", pos.get("cost", 0.0))) or 0.0)
                    if filled_shares > 0:
                        balance = _materialize_entry_position(pos, balance)
                        pos["order_status"] = "filled"
                        _notify(
                            f"[ENTRY PARTIAL] {mkt['city_name']} {mkt['date']} "
                            f"kept {filled_shares:.2f} shares, refunded ${max(0.0, reserved_cash - filled_cost):.2f}"
                        )
                    else:
                        refund = round(reserved_cash, 2)
                        balance += refund
                        pos["status"] = "closed"
                        pos["order_status"] = "cancelled"
                        pos["close_reason"] = "order_cancelled"
                        pos["pnl"] = 0.0
                        pos["closed_at"] = _utc_now_iso()
                        mkt["status"] = "resolved"
                        _notify(f"[ORDER CANCELLED] {mkt['city_name']} {mkt['date']} refunded ${refund:.2f}")
                        closed_count += 1
                    save_market(mkt)
                elif age_seconds > fill_timeout_seconds:
                    pm_trade.cancel_order(order_id)
                    reserved_cash = float(pos.get("reserved_cash", pos.get("requested_cost", pos.get("cost", 0.0))) or 0.0)
                    if filled_shares > 0:
                        balance = _materialize_entry_position(pos, balance)
                        pos["order_status"] = "filled"
                        _notify(
                            f"[ENTRY TIMEOUT] {mkt['city_name']} {mkt['date']} "
                            f"kept {filled_shares:.2f} shares after timeout, refunded ${max(0.0, reserved_cash - filled_cost):.2f}"
                        )
                    else:
                        refund = round(reserved_cash, 2)
                        balance += refund
                        pos["status"] = "closed"
                        pos["order_status"] = "timeout_cancelled"
                        pos["close_reason"] = "fill_timeout"
                        pos["pnl"] = 0.0
                        pos["closed_at"] = _utc_now_iso()
                        mkt["status"] = "resolved"
                        _notify(
                            f"[FILL TIMEOUT] {mkt['city_name']} {mkt['date']} "
                            f"BUY order unfilled after 30min, refunded ${refund:.2f}"
                        )
                        closed_count += 1
                    save_market(mkt)

        if pos.get("status") != "open" or pos.get("exit_order_status") not in ("pending", "partial"):
            continue

        sell_order_id = pos.get("sell_order_id")
        requested_at_str = pos.get("pending_close_requested_at", "")
        try:
            requested_at = datetime.fromisoformat(requested_at_str.replace("Z", "+00:00"))
            age_seconds = (datetime.now(timezone.utc) - requested_at).total_seconds()
        except Exception:
            age_seconds = 0

        if not sell_order_id:
            pos["exit_order_status"] = "failed"
            save_market(mkt)
            continue

        detail = pm_trade.get_order_status_detail(sell_order_id)
        exit_trades = pm_trade.get_trades(order_id=sell_order_id, token_id=pos.get("token_id"))
        filled_shares, filled_value = _sync_exit_execution_from_fills(pos, exit_trades)
        fill_status = detail.get("status")
        matched_size = max(float(detail.get("matched_size", 0.0) or 0.0), filled_shares)
        if matched_size > filled_shares:
            avg_price = float(detail.get("avg_price", pos.get("exit_order_price", pos.get("entry_price", 0.0))) or pos.get("exit_order_price", pos.get("entry_price", 0.0)))
            synthetic_fill = {
                "id": f"detail-{sell_order_id}-{matched_size}",
                "order_id": sell_order_id,
                "token_id": pos.get("token_id", ""),
                "side": "SELL",
                "size": round(matched_size - filled_shares, 4),
                "price": round(avg_price, 6),
                "timestamp": _utc_now_iso(),
            }
            filled_shares, filled_value = _sync_exit_execution_from_fills(pos, [synthetic_fill])

        if fill_status == "matched" and filled_shares > 0:
            close_reason = _pending_exit_reason(pos)
            exit_price = round(filled_value / filled_shares, 4)
            original_shares = float(pos.get("shares", 0.0) or 0.0)
            proceeds = round(filled_value, 2)
            _apply_partial_exit(pos, filled_shares, filled_value)
            pnl = _finalize_position_close(pos, exit_price, close_reason, _utc_now_iso())
            balance += proceeds
            logger.info("[SELL FILL] Order %s confirmed filled %.2f/%.2f shares", sell_order_id, filled_shares, original_shares)
            save_market(mkt)
            closed_count += 1
            continue

        if fill_status == "partial" and filled_shares > 0:
            pos["exit_order_status"] = "partial"
            save_market(mkt)
            continue

        if fill_status == "cancelled":
            if filled_shares > 0:
                proceeds = round(filled_value, 2)
                _, realized_pnl = _apply_partial_exit(pos, filled_shares, filled_value)
                balance += proceeds
                pos["exit_order_status"] = "cancelled"
                pos.pop("sell_order_id", None)
                pos["exit_fills"] = []
                pos["exit_filled_shares"] = 0.0
                pos["exit_filled_value"] = 0.0
                _notify(
                    f"[EXIT PARTIAL] {mkt['city_name']} {mkt['date']} "
                    f"realized {'+' if realized_pnl >= 0 else ''}{realized_pnl:.2f}, position remains open."
                )
            else:
                pos["exit_order_status"] = "cancelled"
                pos.pop("sell_order_id", None)
                _notify(f"[EXIT CANCELLED] {mkt['city_name']} {mkt['date']} exit order cancelled, position remains open.")
            save_market(mkt)
            continue

        if age_seconds > fill_timeout_seconds:
            pm_trade.cancel_order(sell_order_id)
            if filled_shares > 0:
                proceeds = round(filled_value, 2)
                _, realized_pnl = _apply_partial_exit(pos, filled_shares, filled_value)
                balance += proceeds
                pos["exit_order_status"] = "timeout_cancelled"
                pos.pop("sell_order_id", None)
                pos["exit_fills"] = []
                pos["exit_filled_shares"] = 0.0
                pos["exit_filled_value"] = 0.0
                _notify(
                    f"[EXIT TIMEOUT] {mkt['city_name']} {mkt['date']} "
                    f"kept partial exit only, realized {'+' if realized_pnl >= 0 else ''}{realized_pnl:.2f}."
                )
            else:
                pos["exit_order_status"] = "timeout_cancelled"
                pos.pop("sell_order_id", None)
                _notify(
                    f"[EXIT TIMEOUT] {mkt['city_name']} {mkt['date']} "
                    "SELL order unfilled after 30min, position remains open."
                )
            save_market(mkt)

    return balance, closed_count


def monitor_positions() -> int:
    """
    Quick stop/take-profit check on open positions.
    Positions are only closed after a confirmed exit fill.
    """
    settings.reload_risk_config()
    _reconcile_clob_on_startup()

    markets = load_all_markets()
    open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
    if not open_pos:
        return 0

    state = load_state()
    balance = state["balance"]
    closed = 0

    balance, reconciled_closed = _check_pending_fills(open_pos, balance)
    closed += reconciled_closed
    if reconciled_closed:
        markets = load_all_markets()
        open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]

    for mkt in open_pos:
        pos = mkt["position"]
        if pos.get("order_status") in ("pending", "partial") or pos.get("exit_order_status") in ("pending", "partial"):
            continue

        mid = pos["market_id"]
        side = _get_position_side(pos)
        current_price = None
        try:
            mdata = pm_read.get_market_detail(mid)
            if mdata:
                quote_market = {
                    "bid": float(mdata.get("bestBid", pos.get("bid_at_entry", pos["entry_price"]))),
                    "ask": float(mdata.get("bestAsk", pos.get("entry_price", 0.0))),
                    "price": float(json.loads(mdata.get("outcomePrices", "[0.5,0.5]"))[0]),
                }
                best_bid, _, quote_mid = _quotes_for_side_from_outcome(quote_market, side)
                current_price = best_bid if best_bid > 0 else quote_mid
        except Exception:
            pass

        if current_price is None:
            current_price = _position_current_price_from_outcomes(pos, mkt.get("all_outcomes", []))
        if current_price is None:
            continue

        entry = pos["entry_price"]
        city_name = LOCATIONS.get(mkt["city"], {}).get("name", mkt["city"])
        end_date = mkt.get("event_end_date", "")
        hours_left = pm_read.hours_to_resolution(end_date) if end_date else 999.0
        stop = float(
            pos.get("stop_price", _default_stop_price(entry, hours_left))
            or _default_stop_price(entry, hours_left)
        )

        take_profit = _take_profit_target(entry, hours_left)

        forecast_shift = False
        if pos.get("forecast_at_entry"):
            for snap_record in mkt.get("forecast_snapshots", []):
                if snap_record.get("best") and abs(snap_record["best"] - pos["forecast_at_entry"]) > 2.0:
                    forecast_shift = True
                    break

        if current_price >= entry * 1.20 and stop < entry:
            pos["stop_price"] = entry
            pos["trailing_activated"] = True
            _notify(f"[TRAILING] {city_name} {mkt['date']} stop moved to breakeven ${entry:.3f}")

        take_triggered = take_profit is not None and current_price >= take_profit
        stop_triggered = current_price <= stop
        forecast_close = forecast_shift and hours_left < 6

        if not (take_triggered or stop_triggered or forecast_close):
            continue

        if take_triggered:
            close_reason = "take_profit"
        elif forecast_close:
            close_reason = "forecast_shift_close"
        elif current_price < entry:
            close_reason = "stop_loss"
        else:
            close_reason = "trailing_stop"

        closed_now, pending_exit = _queue_position_close(
            pos, mkt, current_price, close_reason, datetime.now(timezone.utc).isoformat()
        )

        if closed_now:
            pnl = pos.get("pnl", 0.0)
            balance += pos["cost"] + pnl
            closed += 1
            reason, emoji = _close_reason_label(close_reason)
            _notify(
                f"{emoji} [{reason}] {city_name} {mkt['date']}\n"
                f"   {side} ${entry:.3f} -> ${current_price:.3f} | {hours_left:.0f}h left\n"
                f"   PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
            )
            save_market(mkt)
        elif pending_exit:
            reason, emoji = _close_reason_label(close_reason)
            _notify(
                f"{emoji} [EXIT PENDING] {city_name} {mkt['date']}\n"
                f"   {side} exit queued near ${current_price:.3f} | {hours_left:.0f}h left"
            )
            save_market(mkt)

    if closed:
        state["balance"] = round(balance, 2)
        save_state(state)

    return closed
