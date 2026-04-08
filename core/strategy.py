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
    bucket_prob, calc_ev, calc_edge, calc_kelly, bet_size, in_bucket,
    confidence_by_time, forecast_disagreement_sigma, late_market_multiplier,
)
from core.calibration import get_sigma, run_calibration, load_cal, log_prediction, record_outcome
from core.state import (
    load_state, save_state, load_market, save_market,
    load_all_markets, new_market,
)

logger = logging.getLogger("weatherbet.strategy")

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


# ═══════════════════════════════════════════════════════════
# FULL SCAN
# ═══════════════════════════════════════════════════════════

def scan_and_update() -> tuple[int, int, int]:
    """
    Main cycle: update forecasts, open/close positions.
    Returns (new_positions, closed, resolved).
    """
    from core.forecasts import take_forecast_snapshot

    now = datetime.now(timezone.utc)
    state = load_state()
    balance = state["balance"]
    new_pos = 0
    closed = 0
    resolved_count = 0
    production = _is_production()

    for city_slug, loc in LOCATIONS.items():
        unit = loc["unit"]
        unit_sym = "F" if unit == "F" else "C"

        try:
            dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]
            snapshots = take_forecast_snapshot(city_slug, dates)
            time.sleep(0.3)
        except Exception as e:
            logger.warning("[SCAN] %s skipped: %s", loc["name"], e)
            continue

        for i, date in enumerate(dates):
            dt = datetime.strptime(date, "%Y-%m-%d")
            event = pm_read.get_event(
                city_slug,
                settings.MONTHS[dt.month - 1],
                dt.day,
                dt.year,
            )
            if not event:
                continue

            end_date = event.get("endDate", "")
            hours = pm_read.hours_to_resolution(end_date) if end_date else 0
            horizon = f"D+{i}"

            # Load or create market record
            mkt = load_market(city_slug, date)
            if mkt is None:
                if hours < settings.MIN_HOURS or hours > settings.MAX_HOURS:
                    continue
                mkt = new_market(city_slug, date, event, hours)

            if mkt["status"] == "resolved":
                continue

            # ── Parse all outcomes ───────────────────────
            outcomes = []
            for market in event.get("markets", []):
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
                    # Adaptive stop: tighter as we approach resolution
                    # Far out (>48h): wider stop (70%) to avoid noise
                    # Close (<12h): tighter stop (85%) because info is reliable
                    if hours > 48:
                        stop_pct = 0.70
                    elif hours > 24:
                        stop_pct = 0.75
                    elif hours > 12:
                        stop_pct = 0.80
                    else:
                        stop_pct = 0.85
                    default_stop = entry * stop_pct
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
                # 1. Dynamic sigma: base + forecast disagreement
                base_sigma = get_sigma(city_slug, best_source or "ecmwf")
                sigma = forecast_disagreement_sigma(all_forecasts, base_sigma)

                # 2. Time-based confidence
                conf = confidence_by_time(hours)

                best_signal = None

                # Find the bucket matching our forecast
                matched_bucket = None
                for o in outcomes:
                    t_low, t_high = o["range"]
                    if in_bucket(forecast_temp, t_low, t_high):
                        matched_bucket = o
                        break

                if matched_bucket:
                    o = matched_bucket
                    t_low, t_high = o["range"]
                    volume = o["volume"]
                    bid = o.get("bid", o["price"])
                    ask = o.get("ask", o["price"])
                    spread = o.get("spread", 0)

                    if volume >= settings.MIN_VOLUME:
                        # Raw probability from model
                        p_raw = bucket_prob(forecast_temp, t_low, t_high, sigma)
                        # Adjusted probability with time confidence
                        p = p_raw * conf

                        # Edge = p - price (correct for binary markets)
                        edge = calc_edge(p, ask)
                        ev = calc_ev(p, ask)

                        # Filter: need minimum edge AND positive EV
                        if edge >= settings.MIN_EDGE and ev >= settings.MIN_EV:
                            kelly = calc_kelly(p, ask)

                            # Late market aggressiveness: boost Kelly 6-18h before event
                            lm_mult = late_market_multiplier(hours)
                            kelly_adjusted = min(kelly * lm_mult, 0.25)

                            size = bet_size(kelly_adjusted, balance)
                            if size >= 0.50:
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
                                    "confidence":    round(conf, 2),
                                    "edge":          round(edge, 4),
                                    "ev":            round(ev, 4),
                                    "kelly":         round(kelly_adjusted, 4),
                                    "kelly_raw":     round(kelly, 4),
                                    "lm_mult":       lm_mult,
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
                                }

                if best_signal:
                    # Fetch real ask from Polymarket for accurate entry
                    skip = False
                    try:
                        mdata = pm_read.get_market_detail(best_signal["market_id"])
                        if mdata:
                            real_ask = float(mdata.get("bestAsk", best_signal["entry_price"]))
                            real_bid = float(mdata.get("bestBid", best_signal["bid_at_entry"]))
                            real_spread = round(real_ask - real_bid, 4)
                            if real_spread > settings.MAX_SLIPPAGE or real_ask >= settings.MAX_PRICE:
                                skip = True
                            else:
                                best_signal["entry_price"] = real_ask
                                best_signal["bid_at_entry"] = real_bid
                                best_signal["spread"] = real_spread
                                best_signal["shares"] = round(best_signal["cost"] / real_ask, 2)
                                # Recalculate with real price
                                best_signal["edge"] = round(calc_edge(best_signal["p"], real_ask), 4)
                                best_signal["ev"] = round(calc_ev(best_signal["p"], real_ask), 4)
                    except Exception as e:
                        logger.warning("[SCAN] Could not fetch real ask: %s", e)

                    if not skip and best_signal["entry_price"] < settings.MAX_PRICE:
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
                                        best_signal["clob_order_id"] = resp.get("orderID") or resp.get("id")
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
                            bucket_label = f"{best_signal['bucket_low']}-{best_signal['bucket_high']}{unit_sym}"
                            mode_tag = "🔴 PROD" if production else "🟡 SIM"
                            msg = (
                                f"📈 [{mode_tag}] BUY {loc['name']} {horizon} {date}\n"
                                f"   {bucket_label} @ ${best_signal['entry_price']:.3f}\n"
                                f"   Edge {best_signal['edge']:+.2%} | EV {best_signal['ev']:+.4f} | ${best_signal['cost']:.2f}\n"
                                f"   σ={best_signal['sigma']:.1f} | conf={best_signal['confidence']:.0%} | {best_signal['forecast_src'].upper()}"
                            )
                            _notify(msg)

                            # Log prediction for calibration curve
                            log_prediction(
                                city=city_slug, date=date,
                                p=best_signal["p"], edge=best_signal["edge"],
                                price=best_signal["entry_price"],
                                source=best_signal["forecast_src"] or "",
                                sigma=best_signal["sigma"],
                                confidence=best_signal["confidence"],
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

    return new_pos, closed, resolved_count


# ═══════════════════════════════════════════════════════════
# QUICK POSITION MONITOR
# ═══════════════════════════════════════════════════════════

def monitor_positions() -> int:
    """Quick stop/take-profit check on open positions (runs between full scans)."""
    markets = load_all_markets()
    open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
    if not open_pos:
        return 0

    state = load_state()
    balance = state["balance"]
    closed = 0

    for mkt in open_pos:
        pos = mkt["position"]
        mid = pos["market_id"]

        # Fetch real bestBid
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

        # Take-profit thresholds
        if hours_left < 24:
            take_profit = None
        elif hours_left < 48:
            take_profit = 0.85
        else:
            take_profit = 0.75

        # Trailing
        if current_price >= entry * 1.20 and stop < entry:
            pos["stop_price"] = entry
            pos["trailing_activated"] = True
            _notify(f"🔒 [TRAILING] {city_name} {mkt['date']} — stop → breakeven ${entry:.3f}")

        take_triggered = take_profit is not None and current_price >= take_profit
        stop_triggered = current_price <= stop

        if take_triggered or stop_triggered:
            pnl = round((current_price - entry) * pos["shares"], 2)
            balance += pos["cost"] + pnl
            pos["closed_at"] = datetime.now(timezone.utc).isoformat()
            if take_triggered:
                pos["close_reason"] = "take_profit"
                reason = "TAKE"
                emoji = "💰"
            elif current_price < entry:
                pos["close_reason"] = "stop_loss"
                reason = "STOP"
                emoji = "🛑"
            else:
                pos["close_reason"] = "trailing_stop"
                reason = "TRAILING"
                emoji = "🔒"
            pos["exit_price"] = current_price
            pos["pnl"] = pnl
            pos["status"] = "closed"
            closed += 1

            msg = (
                f"{emoji} [{reason}] {city_name} {mkt['date']}\n"
                f"   entry ${entry:.3f} → exit ${current_price:.3f} | {hours_left:.0f}h left\n"
                f"   PnL: {'+' if pnl >= 0 else ''}{pnl:.2f}"
            )
            _notify(msg)
            save_market(mkt)

    if closed:
        state["balance"] = round(balance, 2)
        save_state(state)

    return closed
