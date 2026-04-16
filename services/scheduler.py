"""
services/scheduler.py — Async scheduler that runs scan and monitor loops.
Designed to live alongside the Telegram bot in the same asyncio event loop.
"""

import asyncio
import logging
from datetime import datetime, timezone
from copy import deepcopy

from config import settings
from core.strategy import scan_and_update, monitor_positions, set_notify

logger = logging.getLogger("weatherbet.scheduler")

_running = False
_scan_task: asyncio.Task | None = None
_notifications_enabled = False
_notify_interval = 600  # 10 min default
_last_notify_time = ""  # ISO timestamp of last notification
BASE_SCAN_BACKOFF_SECONDS = 60
MAX_SCAN_BACKOFF_SECONDS = 900
_scan_activity = {
    "last_scan_at": "",
    "last_status": "idle",
    "new": 0,
    "closed": 0,
    "resolved": 0,
    "last_error": "",
    "consecutive_failures": 0,
    "next_retry_in_sec": 0,
    "last_monitor_closed": 0,
}


def _position_side(pos: dict) -> str:
    return "NO" if str(pos.get("side") or "YES").upper() == "NO" else "YES"


def _quote_for_side(outcome: dict, side: str) -> tuple[float, float, float]:
    yes_bid = float(outcome.get("bid", outcome.get("price", 0.0)) or 0.0)
    yes_ask = float(outcome.get("ask", outcome.get("price", 0.0)) or 0.0)
    yes_mid = float(outcome.get("price", (yes_bid + yes_ask) / 2.0) or 0.0)
    if side == "NO":
        return max(0.0, 1.0 - yes_ask), min(0.9999, 1.0 - yes_bid), max(0.0, min(0.9999, 1.0 - yes_mid))
    return yes_bid, yes_ask, yes_mid


def _execution_metrics(pos: dict) -> tuple[float, float]:
    requested_shares = float(pos.get("requested_shares", pos.get("shares", 0.0)) or 0.0)
    filled_shares = float(pos.get("filled_shares", pos.get("shares", 0.0)) or 0.0)
    expected_fill = float(pos.get("expected_fill_price", pos.get("entry_price", 0.0)) or 0.0)
    avg_fill = float(pos.get("avg_entry_price", pos.get("entry_price", 0.0)) or 0.0)
    fill_rate = (filled_shares / requested_shares) if requested_shares > 0 else 1.0
    slippage_bps = ((avg_fill - expected_fill) / expected_fill * 10000) if expected_fill > 0 else 0.0
    return fill_rate, slippage_bps


def set_notifications(enabled: bool, interval: int = 600):
    """Toggle periodic notifications. Called from /notifications handler."""
    global _notifications_enabled, _notify_interval
    _notifications_enabled = enabled
    _notify_interval = interval


def get_notifications_status() -> tuple[bool, int]:
    return _notifications_enabled, _notify_interval


def get_scan_activity() -> dict:
    """Expose recent scheduler scan activity for dashboard/API."""
    return deepcopy(_scan_activity)


async def _send_positions_update(notify_func):
    """Build and send a compact dashboard-style update."""
    from core.state import load_state, load_all_markets
    from connectors.polymarket_read import hours_to_resolution
    from services.mode_manager import get_mode

    state = load_state()
    markets = load_all_markets()
    mode = get_mode()
    mode_tag = "PROD" if mode == "production" else "SIM"
    now_str = datetime.now().strftime("%H:%M")

    open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
    recently_closed = [
        m for m in markets
        if m.get("position")
        and m["position"].get("status") == "closed"
        and m["position"].get("closed_at", "") >= _last_notify_time
    ]

    bal = state["balance"]
    wins = state.get("wins", 0)
    losses = state.get("losses", 0)
    total_trades = state.get("total_trades", 0)
    start = state.get("starting_balance", settings.BALANCE)
    ret_pct = (bal - start) / start * 100 if start > 0 else 0
    ret_sign = "+" if ret_pct >= 0 else ""

    lines = [
        f"━━━ WeatherBet [{mode_tag}] {now_str} ━━━",
        f"💰 ${bal:.2f} ({ret_sign}{ret_pct:.1f}%) | W:{wins} L:{losses} T:{total_trades}",
    ]

    # Recently closed positions
    if recently_closed:
        lines.append("")
        for m in recently_closed:
            pos = m["position"]
            reason = pos.get("close_reason", "?")
            pnl = pos.get("pnl", 0)
            pnl_sign = "+" if pnl >= 0 else ""
            side = _position_side(pos)
            emoji = {"stop_loss": "🛑", "trailing_stop": "🔒", "take_profit": "💰", "resolved": "✅" if pnl >= 0 else "❌", "forecast_shift_close": "🔄"}.get(reason, "📌")
            lines.append(f"  {emoji} {m['city_name']} {m['date']} | {side} | {reason} | {pnl_sign}{pnl:.2f}")

    # Open positions
    if open_pos:
        lines.append(f"\n📂 Open ({len(open_pos)}):")
        total_unrealized = 0.0

        for m in sorted(open_pos, key=lambda x: x["city_name"]):
            pos = m["position"]
            side = _position_side(pos)
            unit_sym = "F" if m["unit"] == "F" else "C"
            bl = pos["bucket_low"]
            bh = pos["bucket_high"]
            if bl == bh:
                bucket = f"{bl}{unit_sym}"
            else:
                bucket = f"{bl}-{bh}{unit_sym}"
            entry = pos["entry_price"]
            src = (pos.get("forecast_src") or "?").upper()[:4]

            # Current price
            current_price = entry
            for o in m.get("all_outcomes", []):
                if o["market_id"] == pos["market_id"]:
                    best_bid, _, mid = _quote_for_side(o, side)
                    current_price = best_bid if best_bid > 0 else mid
                    break

            unrealized = round((current_price - entry) * pos["shares"], 2)
            total_unrealized += unrealized
            pnl_sign = "+" if unrealized >= 0 else ""
            fill_rate, slippage_bps = _execution_metrics(pos)

            # Hours left
            end_date = m.get("event_end_date", "")
            hrs = hours_to_resolution(end_date) if end_date else 0
            hrs_str = f"{hrs:.0f}h" if hrs < 100 else "—"
            state_tag = "EXIT_PARTIAL" if pos.get("exit_order_status") == "partial" else "ENTRY_PARTIAL" if pos.get("order_status") == "partial" else "EXIT_PENDING" if pos.get("exit_order_status") == "pending" else "ENTRY_PENDING" if pos.get("order_status") == "pending" else "OPEN"

            # Arrow direction
            arrow = "📈" if current_price > entry else "📉" if current_price < entry else "➡️"

            lines.append(
                f"  {arrow} {m['city_name']} {m['date']} | {side} {bucket} | "
                f"${entry:.3f}→${current_price:.3f} | {pnl_sign}{unrealized:.2f} | {hrs_str} | {state_tag} | {src}"
            )
            lines.append(
                f"     fill {fill_rate * 100:.0f}% | slip {slippage_bps:+.1f}bps | expEV {pos.get('requested_cost', pos.get('cost', 0.0)) * pos.get('net_ev', pos.get('ev', 0.0)):+.2f}"
            )

        u_sign = "+" if total_unrealized >= 0 else ""
        lines.append(f"  ── Unrealized: {u_sign}{total_unrealized:.2f}")
    else:
        lines.append("\n📭 No open positions")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    await notify_func("\n".join(lines))


async def _scan_loop(notify_func):
    """Main scan loop: full scan every SCAN_INTERVAL, monitor every MONITOR_INTERVAL."""
    global _running, _last_notify_time, _scan_activity
    _running = True
    set_notify(notify_func)

    last_full_scan = 0.0
    last_notification = 0.0
    consecutive_scan_failures = 0
    next_scan_allowed_at = 0.0

    await notify_func("🟢 WeatherBet scheduler started")

    while _running:
        settings.reload_risk_config()
        now_ts = asyncio.get_event_loop().time()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if now_ts < next_scan_allowed_at:
            await asyncio.sleep(min(settings.MONITOR_INTERVAL, max(1.0, next_scan_allowed_at - now_ts)))
            continue

        if now_ts - last_full_scan >= settings.SCAN_INTERVAL:
            logger.info("[%s] Full scan...", now_str)
            try:
                loop = asyncio.get_event_loop()
                new_pos, closed, resolved = await loop.run_in_executor(
                    None, scan_and_update
                )

                from core.state import load_state
                state = load_state()
                # Only notify when something actually happened — avoid scan noise
                if new_pos or closed or resolved:
                    summary = (
                        f"📊 Scan — {now_str}\n"
                        f"Balance: ${state['balance']:,.2f}\n"
                        f"New: {new_pos} | Closed: {closed} | Resolved: {resolved}"
                    )
                    await notify_func(summary)
                consecutive_scan_failures = 0
                next_scan_allowed_at = 0.0
                last_full_scan = asyncio.get_event_loop().time()
                _scan_activity.update({
                    "last_scan_at": datetime.now(timezone.utc).isoformat(),
                    "last_status": "ok",
                    "new": int(new_pos),
                    "closed": int(closed),
                    "resolved": int(resolved),
                    "last_error": "",
                    "consecutive_failures": 0,
                    "next_retry_in_sec": 0,
                })

            except Exception as e:
                consecutive_scan_failures += 1
                exponent = min(max(0, consecutive_scan_failures - 1), 10)
                backoff = min(
                    BASE_SCAN_BACKOFF_SECONDS * (2 ** exponent),
                    MAX_SCAN_BACKOFF_SECONDS,
                )
                next_scan_allowed_at = asyncio.get_event_loop().time() + backoff
                logger.error("[SCAN] Error: %s", e)
                await notify_func(
                    f"🚨 Scan error: {e}\n"
                    f"Failure #{consecutive_scan_failures} — retrying in {int(backoff)}s."
                )
                _scan_activity.update({
                    "last_status": "error",
                    "last_error": str(e),
                    "consecutive_failures": int(consecutive_scan_failures),
                    "next_retry_in_sec": int(backoff),
                })
                continue
        else:
            # Quick position monitor
            logger.info("[%s] Monitoring positions...", now_str)
            try:
                loop = asyncio.get_event_loop()
                stopped = await loop.run_in_executor(None, monitor_positions)
                _scan_activity["last_monitor_closed"] = int(stopped)
                if stopped:
                    from core.state import load_state
                    state = load_state()
                    await notify_func(f"📊 Monitor: {stopped} position(s) closed | Balance: ${state['balance']:,.2f}")
            except Exception as e:
                logger.error("[MONITOR] Error: %s", e)

        # Periodic notifications
        if _notifications_enabled and (now_ts - last_notification >= _notify_interval):
            try:
                await _send_positions_update(notify_func)
                last_notification = now_ts
                _last_notify_time = datetime.now().isoformat()
            except Exception as e:
                logger.error("[NOTIFY] Periodic update failed: %s", e)

        # Sleep until next event: whichever comes first — next scan or next monitor
        sleep_secs = min(settings.MONITOR_INTERVAL, settings.SCAN_INTERVAL)
        await asyncio.sleep(sleep_secs)


def start_scheduler(notify_func) -> asyncio.Task:
    """Start the scheduler as an asyncio task. Returns the task handle."""
    global _scan_task
    if _scan_task and not _scan_task.done():
        logger.info("[SCHEDULER] Already running — reusing existing task")
        return _scan_task
    _scan_task = asyncio.create_task(_scan_loop(notify_func))
    return _scan_task


def stop_scheduler():
    """Signal the scheduler to stop."""
    global _running, _scan_task
    _running = False
    if _scan_task and not _scan_task.done():
        _scan_task.cancel()
    logger.info("[SCHEDULER] Stopped")


def is_running() -> bool:
    return _running


async def force_scan(notify_func) -> str:
    """Force an immediate full scan (called via /scan command)."""
    global _scan_activity
    try:
        settings.reload_risk_config()
        set_notify(notify_func)
        loop = asyncio.get_event_loop()
        new_pos, closed, resolved = await loop.run_in_executor(None, scan_and_update)
        from core.state import load_state
        state = load_state()
        _scan_activity.update({
            "last_scan_at": datetime.now(timezone.utc).isoformat(),
            "last_status": "ok",
            "new": int(new_pos),
            "closed": int(closed),
            "resolved": int(resolved),
            "last_error": "",
            "consecutive_failures": 0,
            "next_retry_in_sec": 0,
        })
        return (
            f"📊 Manual scan complete\n"
            f"Balance: ${state['balance']:,.2f}\n"
            f"New: {new_pos} | Closed: {closed} | Resolved: {resolved}"
        )
    except Exception as e:
        _scan_activity.update({
            "last_status": "error",
            "last_error": str(e),
        })
        return f"🚨 Scan failed: {e}"
