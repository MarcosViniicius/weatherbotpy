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
    bucket_prob, calc_edge, calc_ev_after_costs, calc_kelly, bet_size, in_bucket,
    confidence_by_time, forecast_disagreement_sigma, late_market_multiplier,
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
                current_price = None
                for o in outcomes:
                    if o["market_id"] == pos["market_id"]:
                        current_price = o.get("bid", o["price"])
                        break

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
                old_low = pos["bucket_low"]
                old_high = pos["bucket_high"]
                buffer = 2.0 if unit == "F" else 1.0
                mid_bucket = (old_low + old_high) / 2 if old_low != -999 and old_high != 999 else forecast_temp
                forecast_far = abs(forecast_temp - mid_bucket) > (abs(mid_bucket - old_low) + buffer)

                if not in_bucket(forecast_temp, old_low, old_high) and forecast_far:
                    current_price = None
                    for o in outcomes:
                        if o["market_id"] == pos["market_id"]:
                            current_price = o["price"]
                            break
                    if current_price is not None:
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

                # Find the bucket matching our forecast
                primary_bucket_index = None
                for idx, o in enumerate(outcomes):
                    t_low, t_high = o["range"]
                    if in_bucket(forecast_temp, t_low, t_high):
                        primary_bucket_index = idx
                        break

                if primary_bucket_index is not None:
                    candidate_indices = [primary_bucket_index]
                    adjacent_indices = []
                    if primary_bucket_index > 0:
                        adjacent_indices.append(primary_bucket_index - 1)
                    if primary_bucket_index < len(outcomes) - 1:
                        adjacent_indices.append(primary_bucket_index + 1)
                    adjacent_indices.sort(
                        key=lambda bucket_idx: abs(((outcomes[bucket_idx]["range"][0] + outcomes[bucket_idx]["range"][1]) / 2.0) - forecast_temp)
                    )
                    candidate_indices.extend(adjacent_indices)

                    for idx in candidate_indices:
                        o = outcomes[idx]
                        t_low, t_high = o["range"]
                        volume = o["volume"]
                        bid = o.get("bid", o["price"])
                        ask = o.get("ask", o["price"])
                        spread = o.get("spread", 0.0)
                        price_mid = o.get("price", (bid + ask) / 2.0)

                        if price_mid <= 0:
                            cycle_stats["discard_reasons"]["price"] += 1
                            logger.info("[DISCARD] reason=price city=%s date=%s market=%s detail=invalid_mid", city_slug, date, o["market_id"])
                            continue
                        spread_ratio = spread / price_mid
                        if spread_ratio > thresholds["max_relative_spread"]:
                            cycle_stats["discard_reasons"]["spread_relative"] += 1
                            logger.info("[DISCARD] reason=spread_relative city=%s date=%s market=%s spread_ratio=%.4f max=%.4f", city_slug, date, o["market_id"], spread_ratio, thresholds["max_relative_spread"])
                            continue
                        if volume < thresholds["min_volume"]:
                            cycle_stats["discard_reasons"]["volume"] += 1
                            logger.info("[DISCARD] reason=volume city=%s date=%s market=%s volume=%.0f min=%d", city_slug, date, o["market_id"], volume, thresholds["min_volume"])
                            continue
                        if ask >= thresholds["max_price"]:
                            cycle_stats["discard_reasons"]["price"] += 1
                            logger.info("[DISCARD] reason=price city=%s date=%s market=%s ask=%.4f max=%.4f", city_slug, date, o["market_id"], ask, thresholds["max_price"])
                            continue
                        # MIN_PRICE guard: penny tokens have unacceptable spread/liquidity risk
                        if ask < settings.MIN_PRICE:
                            cycle_stats["discard_reasons"]["price"] += 1
                            logger.info("[DISCARD] reason=min_price city=%s date=%s market=%s ask=%.4f min=%.4f", city_slug, date, o["market_id"], ask, settings.MIN_PRICE)
                            continue

                        # Raw probability from model
                        p_raw = bucket_prob(forecast_temp, t_low, t_high, sigma)
                        is_adjacent = idx != primary_bucket_index
                        conf_adj = conf * (ADJACENT_BUCKET_CONFIDENCE_PENALTY if is_adjacent else 1.0)
                        p = max(0.0, min(1.0, p_raw * conf_adj))

                        edge = calc_edge(p, ask)
                        ev_after_costs = calc_ev_after_costs(p, ask, spread)
                        if ev_after_costs < thresholds["min_edge"]:
                            cycle_stats["discard_reasons"]["ev"] += 1
                            logger.info("[DISCARD] reason=ev city=%s date=%s market=%s net_ev=%+.4f min=%+.4f", city_slug, date, o["market_id"], ev_after_costs, thresholds["min_edge"])
                            continue

                        kelly = calc_kelly(p, ask)
                        lm_mult = late_market_multiplier(hours)
                        kelly_adjusted = min(kelly * lm_mult, 0.25)
                        size = bet_size(kelly_adjusted, balance)
                        if size < 0.50:
                            continue

                        best_signal = {
                            "market_id":     o["market_id"],
                            "question":      o["question"],
                            "bucket_low":    t_low,
                            "bucket_high":   t_high,
                            "entry_price":   ask,
                            "bid_at_entry":  bid,
                            "spread":        spread,
                            "shares":        round(size / ask, 2),
                            "cost":          size,
                            "p":             round(p, 4),
                            "p_raw":         round(p_raw, 4),
                            "confidence":    round(conf_adj, 2),
                            "adjusted_confidence": round(conf_adj, 2),
                            "edge":          round(edge, 4),
                            "kelly":         round(kelly_adjusted, 4),
                            "kelly_raw":     round(kelly, 4),
                            "lm_mult":       lm_mult,
                            "bucket_priority": "adjacent" if is_adjacent else "primary",
                            "forecast_temp": forecast_temp,
                            "forecast_src":  best_source,
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
                        }
                        _set_signal_ev_fields(best_signal, round(ev_after_costs, 4))
                        break

                if best_signal:
                    # Fetch real ask from Polymarket for accurate entry
                    skip = False
                    try:
                        mdata = pm_read.get_market_detail(best_signal["market_id"])
                        if mdata:
                            real_ask = float(mdata.get("bestAsk", best_signal["entry_price"]))
                            real_bid = float(mdata.get("bestBid", best_signal["bid_at_entry"]))
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
                                best_signal["shares"] = round(best_signal["cost"] / real_ask, 2)
                                # Recalculate with real execution data (keep legacy `ev` key for compatibility)
                                best_signal["edge"] = round(calc_edge(best_signal["p"], real_ask), 4)
                                real_ev = round(calc_ev_after_costs(best_signal["p"], real_ask, real_spread), 4)
                                _set_signal_ev_fields(best_signal, real_ev)
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
                                    token_id = mdata.get("clobTokenIds")
                                    if isinstance(token_id, str):
                                        token_ids = json.loads(token_id)
                                        token_id = token_ids[0] if token_ids else None
                                    elif isinstance(token_id, list):
                                        token_id = token_id[0] if token_id else None

                                if token_id:
                                    resp = pm_trade.place_limit_order(
                                        token_id=token_id,
                                        price=best_signal["entry_price"],
                                        size=best_signal["shares"],
                                        side="BUY",
                                    )
                                    if resp:
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
        market_id = pos.get("market_id")
        if not market_id:
            continue

        won = pm_read.check_market_resolved(market_id)
        if won is None:
            continue

        price = pos["entry_price"]
        size = pos["cost"]
        shares = pos["shares"]
        pnl = round(shares * (1 - price), 2) if won else round(-size, 2)

        # Fetch actual temperature for calibration (non-blocking failure OK)
        try:
            from core.forecasts import get_actual_temp
            actual_temp = get_actual_temp(mkt["city"], mkt["date"])
            if actual_temp is not None:
                mkt["actual_temp"] = actual_temp
        except Exception:
            pass

        balance += size + pnl
        pos["exit_price"] = 1.0 if won else 0.0
        pos["pnl"] = pnl
        pos["close_reason"] = "resolved"
        pos["closed_at"] = now.isoformat()
        pos["status"] = "closed"
        mkt["pnl"] = pnl
        mkt["status"] = "resolved"
        mkt["resolved_outcome"] = "win" if won else "loss"

        # Track for calibration curve
        record_outcome(mkt["city"], mkt["date"], won)

        if won:
            state["wins"] += 1
        else:
            state["losses"] += 1

        emoji = "✅" if won else "❌"
        result = "WIN" if won else "LOSS"
        msg = f"{emoji} [{result}] {mkt['city_name']} {mkt['date']} | PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
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
                fill_status = pm_trade.get_order_status(order_id)
                if fill_status == "matched":
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


def _place_sell(pos: dict, current_price: float, city_name: str, date: str) -> bool:
    """
    Place a SELL limit order in production to close a position.
    In simulation: no-op, returns True.
    Returns True if successful (or simulation), False on hard failure.
    """
    if not _is_production():
        return True

    token_id = pos.get("token_id")
    shares = pos.get("shares", 0.0)

    if not token_id:
        logger.warning("[SELL] No token_id stored for %s %s — cannot SELL", city_name, date)
        _notify(
            f"⚠️ [PROD] No token_id for {city_name} {date} — "
            f"position marked closed internally but NO SELL ORDER was placed. "
            f"Please close manually on Polymarket!"
        )
        return False

    if shares < 0.01:
        logger.info("[SELL] Shares %.4f too small — skipping SELL", shares)
        return True

    # Sell slightly below bestBid to get immediate fill
    sell_price = max(0.01, round(current_price - 0.01, 2))

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
            logger.info("[SELL] Order placed id=%s @ $%.3f × %.2f shares", sell_id, sell_price, shares)
            return True
        else:
            logger.error("[SELL] Order returned None for %s %s", city_name, date)
            _notify(f"⚠️ [SELL FAILED] {city_name} {date} — SELL returned None. Close manually on Polymarket!")
            return False
    except Exception as e:
        logger.error("[SELL] Order failed for %s %s: %s", city_name, date, e)
        _notify(f"⚠️ [SELL FAILED] {city_name} {date}: {e}\nClose manually on Polymarket!")
        return False


def _check_pending_fills(markets: list, balance: float) -> tuple[float, int]:
    """
    For production positions with order_status='pending':
      - Check CLOB fill status
      - If filled → mark as 'filled'
      - If cancelled externally → refund and close
      - If open after 30 min → cancel & refund
    In simulation mode: instantly marks all pending as 'filled'.
    Returns (updated_balance, n_cancelled).
    """
    if not _is_production():
        for mkt in markets:
            pos = mkt.get("position")
            if pos and pos.get("order_status") == "pending":
                pos["order_status"] = "filled"
                save_market(mkt)
        return balance, 0

    FILL_TIMEOUT_SECONDS = 30 * 60  # 30 minutes
    cancelled = 0

    for mkt in markets:
        pos = mkt.get("position")
        if not pos or pos.get("order_status") != "pending":
            continue

        order_id = pos.get("clob_order_id")
        placed_at_str = pos.get("order_placed_at", "")

        if not order_id:
            pos["order_status"] = "filled"
            save_market(mkt)
            continue

        try:
            placed_at = datetime.fromisoformat(placed_at_str.replace("Z", "+00:00"))
            age_seconds = (datetime.now(timezone.utc) - placed_at).total_seconds()
        except Exception:
            age_seconds = 0

        fill_status = pm_trade.get_order_status(order_id)

        if fill_status == "matched":
            pos["order_status"] = "filled"
            logger.info("[FILL] Order %s confirmed filled", order_id)
            save_market(mkt)

        elif fill_status == "cancelled":
            refund = pos.get("cost", 0)
            balance += refund
            pos["status"] = "closed"
            pos["order_status"] = "cancelled"
            pos["close_reason"] = "order_cancelled"
            pos["pnl"] = 0.0
            pos["closed_at"] = datetime.now(timezone.utc).isoformat()
            mkt["status"] = "resolved"
            logger.warning("[FILL] Order %s cancelled externally — refunded $%.2f", order_id, refund)
            _notify(f"⚠️ [ORDER CANCELLED] {mkt['city_name']} {mkt['date']} — refunded ${refund:.2f}")
            save_market(mkt)
            cancelled += 1

        elif age_seconds > FILL_TIMEOUT_SECONDS:
            pm_trade.cancel_order(order_id)
            refund = pos.get("cost", 0)
            balance += refund
            pos["status"] = "closed"
            pos["order_status"] = "timeout_cancelled"
            pos["close_reason"] = "fill_timeout"
            pos["pnl"] = 0.0
            pos["closed_at"] = datetime.now(timezone.utc).isoformat()
            mkt["status"] = "resolved"
            logger.warning("[FILL] Order %s timed out (%.0fs) — cancelled, refunded $%.2f", order_id, age_seconds, refund)
            _notify(
                f"⏱️ [FILL TIMEOUT] {mkt['city_name']} {mkt['date']} — "
                f"BUY order unfilled after 30min, cancelled. Refunded ${refund:.2f}"
            )
            save_market(mkt)
            cancelled += 1

    return balance, cancelled


# ═══════════════════════════════════════════════════════════
# QUICK POSITION MONITOR
# ═══════════════════════════════════════════════════════════

def monitor_positions() -> int:
    """
    Quick stop/take-profit check on open positions (runs between full scans).
    Production: places real SELL orders; verifies GTC BUY fill status.
    Simulation: purely internal state updates.
    """
    settings.reload_risk_config()

    # Run once-per-startup CLOB reconciliation
    _reconcile_clob_on_startup()

    markets = load_all_markets()
    open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
    if not open_pos:
        return 0

    state = load_state()
    balance = state["balance"]
    closed = 0

    # ── GTC fill verification / pending cleanup ────────────
    balance, fill_cancelled = _check_pending_fills(open_pos, balance)
    closed += fill_cancelled
    if fill_cancelled:
        # Reload after cleanup to avoid processing stale data
        markets = load_all_markets()
        open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]

    for mkt in open_pos:
        pos = mkt["position"]

        # Skip positions still awaiting fill confirmation
        if pos.get("order_status") == "pending":
            continue

        mid = pos["market_id"]

        # Fetch real bestBid from Polymarket
        current_price = None
        try:
            mdata = pm_read.get_market_detail(mid)
            if mdata:
                best_bid = mdata.get("bestBid")
                if best_bid is not None:
                    current_price = float(best_bid)
        except Exception:
            pass

        if current_price is None:
            for o in mkt.get("all_outcomes", []):
                if o["market_id"] == mid:
                    current_price = o.get("bid", o["price"])
                    break
        if current_price is None:
            continue

        entry = pos["entry_price"]
        stop = pos.get("stop_price", entry * 0.80)
        city_name = LOCATIONS.get(mkt["city"], {}).get("name", mkt["city"])
        end_date = mkt.get("event_end_date", "")
        hours_left = pm_read.hours_to_resolution(end_date) if end_date else 999.0

        # Take-profit: close when price appreciated enough above entry
        entry_price = pos["entry_price"]
        take_profit = None
        if hours_left >= 24:
            mult = 1.60 if hours_left < 48 else 2.00
            take_profit = entry_price * mult

        # Forecast shift closes (only when very near resolution)
        forecast_shift = False
        if pos.get("forecast_at_entry"):
            for snap_record in mkt.get("forecast_snapshots", []):
                if snap_record.get("best") and abs(snap_record["best"] - pos["forecast_at_entry"]) > 2.0:
                    forecast_shift = True
                    break

        # Trailing stop: move stop to breakeven after +20%
        if current_price >= entry * 1.20 and stop < entry:
            pos["stop_price"] = entry
            pos["trailing_activated"] = True
            _notify(f"🔒 [TRAILING] {city_name} {mkt['date']} — stop moved to breakeven ${entry:.3f}")

        take_triggered = take_profit is not None and current_price >= take_profit
        stop_triggered = current_price <= stop
        forecast_close = forecast_shift and hours_left < 6

        if take_triggered or stop_triggered or forecast_close:
            # ── SELL in production (no-op in simulation) ──
            sell_ok = _place_sell(pos, current_price, city_name, mkt["date"])

            pnl = round((current_price - entry) * pos["shares"], 2)
            balance += pos["cost"] + pnl
            pos["closed_at"] = datetime.now(timezone.utc).isoformat()

            if take_triggered:
                pos["close_reason"] = "take_profit"
                reason, emoji = "TAKE", "💰"
            elif forecast_close:
                pos["close_reason"] = "forecast_shift_close"
                reason, emoji = "FCAST", "🔄"
            elif current_price < entry:
                pos["close_reason"] = "stop_loss"
                reason, emoji = "STOP", "🛑"
            else:
                pos["close_reason"] = "trailing_stop"
                reason, emoji = "TRAILING", "🔒"

            pos["exit_price"] = current_price
            pos["pnl"] = pnl
            pos["status"] = "closed"
            closed += 1

            sell_warn = "" if sell_ok else "\n⚠️ SELL order failed — close manually on Polymarket!"
            _notify(
                f"{emoji} [{reason}] {city_name} {mkt['date']}\n"
                f"   ${entry:.3f} → ${current_price:.3f} | {hours_left:.0f}h left\n"
                f"   PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}{sell_warn}"
            )
            save_market(mkt)

    if closed:
        state["balance"] = round(balance, 2)
        save_state(state)

    return closed

