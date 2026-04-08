"""
telegram_bot/handlers.py — All Telegram command handlers.
Each handler is wrapped in try/except to prevent silent crashes.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from telegram import Update
from telegram.ext import ContextTypes

from config.settings import TELEGRAM_CHAT_ID
from config.locations import LOCATIONS
from core.state import load_state, load_all_markets
from connectors import polymarket_trade as pm_trade
from connectors import polymarket_read as pm_read
from services import mode_manager
from services.scheduler import force_scan, is_running
from telegram_bot import formatters as fmt
from telegram_bot.notifications import notify

logger = logging.getLogger("weatherbet.handlers")


def _authorized(update: Update) -> bool:
    """Check if the sender is the authorised user."""
    if not TELEGRAM_CHAT_ID:
        return True  # no restriction if chat ID not configured
    return str(update.effective_chat.id) == str(TELEGRAM_CHAT_ID)


async def _deny(update: Update):
    await update.message.reply_text("⛔ Unauthorized.")


async def _safe_reply(update: Update, text: str):
    """Try MarkdownV2 first; fall back to plain text if parsing fails."""
    try:
        await update.message.reply_text(text, parse_mode="MarkdownV2")
    except Exception:
        # Strip markdown formatting and send as plain text
        import re
        plain = text.replace("\\.", ".").replace("\\-", "-").replace("\\+", "+")
        plain = plain.replace("\\|", "|").replace("\\(", "(").replace("\\)", ")")
        plain = plain.replace("\\!", "!").replace("\\=", "=").replace("\\#", "#")
        plain = re.sub(r"(?<!\w)\*([^*]+)\*(?!\w)", r"\1", plain)  # bold
        plain = re.sub(r"`([^`]+)`", r"\1", plain)  # code
        await update.message.reply_text(plain)

# ═══════════════════════════════════════════════════════════
# INFORMATIONAL COMMANDS
# ═══════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start — welcome message."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode = mode_manager.get_mode()
        mode_emoji = "🔴 Production" if mode == "production" else "🟡 Simulation"

        text = (
            "🌤 *WeatherBet v3*\n\n"
            f"Mode: {fmt.escape_md(mode_emoji)}\n"
            f"Scheduler: {'🟢 Running' if is_running() else '🔴 Stopped'}\n\n"
            "*Commands:*\n"
            "/status — Balance, PnL, open positions\n"
            "/positions — Detailed open positions\n"
            "/markets — Active weather markets\n"
            "/orders — Open CLOB orders \\(production\\)\n"
            "/scan — Force immediate scan\n"
            "/notifications — Toggle periodic updates\n"
            "/report — Full report\n"
            "/daily — Today's summary\n"
            "/weekly — Last 7 days\n"
            "/calibration — Forecast calibration metrics\n"
            "/mode — Current mode\n"
            "/simulate — Switch to simulation\n"
            "/production — Switch to production\n"
            "/confirm \\<code\\> — Confirm production\n"
            "/clear — Clear simulation data \\(simulation only\\)\n"
            "/help — This message"
        )
        await _safe_reply(update, text)
    except Exception as e:
        logger.error("[/start] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help."""
    return await cmd_start(update, context)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status — balance, PnL, positions summary."""
    try:
        if not _authorized(update):
            return await _deny(update)

        state = load_state()
        markets = load_all_markets()
        open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
        mode = mode_manager.get_mode()

        text = fmt.format_status(state, open_pos, mode)
        await _safe_reply(update, text)
    except Exception as e:
        logger.error("[/status] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /positions — detailed view."""
    try:
        if not _authorized(update):
            return await _deny(update)

        markets = load_all_markets()
        text = fmt.format_positions(markets)
        await _safe_reply(update, text)
    except Exception as e:
        logger.error("[/positions] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_markets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /markets — list active weather markets on Polymarket."""
    try:
        if not _authorized(update):
            return await _deny(update)

        await update.message.reply_text("🔍 Scanning markets...")

        from config.settings import MONTHS
        now = datetime.now(timezone.utc)
        events_info = []

        for city_slug, loc in list(LOCATIONS.items())[:10]:  # limit to 10 to avoid timeout
            for i in range(3):
                dt = now + timedelta(days=i)
                date_str = dt.strftime("%Y-%m-%d")
                event = pm_read.get_event(city_slug, MONTHS[dt.month - 1], dt.day, dt.year)
                if not event:
                    continue

                markets_list = event.get("markets", [])
                if not markets_list:
                    continue

                # Find top bucket
                top_price = 0
                top_bucket = "?"
                unit_sym = "F" if loc["unit"] == "F" else "C"
                for mkt in markets_list:
                    try:
                        prices = json.loads(mkt.get("outcomePrices", "[0.5,0.5]"))
                        p = float(prices[0])
                        if p > top_price:
                            top_price = p
                            rng = pm_read.parse_temp_range(mkt.get("question", ""))
                            if rng:
                                top_bucket = f"{rng[0]}-{rng[1]}{unit_sym}"
                    except Exception:
                        pass

                events_info.append({
                    "city_name": loc["name"],
                    "date": date_str,
                    "bucket_count": len(markets_list),
                    "top_bucket": top_bucket,
                    "top_price": top_price,
                })

        text = fmt.format_markets_list(events_info)
        await _safe_reply(update, text)
    except Exception as e:
        logger.error("[/markets] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /orders — show open CLOB orders (production only)."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode = mode_manager.get_mode()
        if mode != "production":
            await update.message.reply_text("📭 Orders only available in production mode. Use /production to activate.")
            return

        orders = pm_trade.get_open_orders()
        if not orders:
            await update.message.reply_text("📭 No open orders on the CLOB.")
            return

        lines = [f"📋 *Open Orders* ({len(orders)})\n"]
        for order in orders[:15]:  # cap
            lines.append(
                f"• ID: `{fmt.escape_md(str(order.get('id', '?')))}`\n"
                f"  Price: ${fmt.escape_md(str(order.get('price', '?')))} "
                f"Side: {fmt.escape_md(str(order.get('side', '?')))}"
            )
        await _safe_reply(update, "\n".join(lines))
    except Exception as e:
        logger.error("[/orders] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /report — full performance breakdown."""
    try:
        if not _authorized(update):
            return await _deny(update)

        markets = load_all_markets()
        text = fmt.format_report(markets)
        await _safe_reply(update, text)
    except Exception as e:
        logger.error("[/report] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /daily — today's summary."""
    try:
        if not _authorized(update):
            return await _deny(update)

        markets = load_all_markets()
        text = fmt.format_daily_report(markets)
        await _safe_reply(update, text)
    except Exception as e:
        logger.error("[/daily] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /weekly — last 7 days."""
    try:
        if not _authorized(update):
            return await _deny(update)

        markets = load_all_markets()
        text = fmt.format_weekly_report(markets)
        await _safe_reply(update, text)
    except Exception as e:
        logger.error("[/weekly] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_calibration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /calibration — show Brier score and accuracy curve."""
    try:
        if not _authorized(update):
            return await _deny(update)

        from core.calibration import compute_calibration_report
        rep = compute_calibration_report()
        
        if rep["total"] == 0:
            await _safe_reply(update, "⚖️ *Calibration*\nNo resolved predictions yet.")
            return

        lines = [
            "⚖️ *Forecast Calibration*",
            f"Total Predictions: {rep['total']}",
            f"Brier Score: {rep['brier_score']:.4f} \\(closer to 0 is better\\)",
            f"Directional Accuracy: {rep['accuracy']:.1%} \\(>50% win rate when p>0\\.5\\)",
            "",
            "📊 *Curve* \\(Predicted vs Actual\\):"
        ]

        # Escape special chars manually because _safe_reply will try parsing it as MarkdownV2
        curve = rep.get("calibration_curve", {})
        for bucket, stats in curve.items():
            pred = stats['predicted_avg'] * 100
            actual = stats['actual_win_rate'] * 100
            n = stats['n']
            lines.append(f"`{bucket.replace('.', '\\.')}`: {pred:.1f}% -> {actual:.1f}% \\(n\\={n}\\)")

        await _safe_reply(update, "\n".join(lines))
    except Exception as e:
        logger.error("[/calibration] %s", e)
        await update.message.reply_text(f"Error: {e}")


# ═══════════════════════════════════════════════════════════
# ACTION COMMANDS
# ═══════════════════════════════════════════════════════════

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /scan — force an immediate scan."""
    try:
        if not _authorized(update):
            return await _deny(update)

        await update.message.reply_text("🔄 Starting manual scan...")
        result = await force_scan(notify)
        await update.message.reply_text(result)
    except Exception as e:
        logger.error("[/scan] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /notifications — toggle periodic position updates."""
    try:
        if not _authorized(update):
            return await _deny(update)

        from services.scheduler import set_notifications, get_notifications_status

        enabled, interval = get_notifications_status()
        args = context.args

        # /notifications off
        if args and args[0].lower() in ("off", "0", "stop", "disable"):
            set_notifications(False)
            await update.message.reply_text("🔕 Periodic notifications DISABLED")
            return

        # /notifications on [interval_minutes]
        if args and args[0].lower() in ("on", "1", "start", "enable"):
            minutes = 10
            if len(args) > 1:
                try:
                    minutes = max(1, int(args[1]))
                except ValueError:
                    pass
            set_notifications(True, minutes * 60)
            await update.message.reply_text(
                f"🔔 Periodic notifications ENABLED\n"
                f"Interval: every {minutes} min\n"
                f"You'll receive position updates automatically.\n\n"
                f"Use /notifications off to disable."
            )
            return

        # Toggle if no args
        if enabled:
            set_notifications(False)
            await update.message.reply_text("🔕 Periodic notifications DISABLED")
        else:
            minutes = 10
            set_notifications(True, minutes * 60)
            await update.message.reply_text(
                f"🔔 Periodic notifications ENABLED\n"
                f"Interval: every {minutes} min\n\n"
                f"Commands:\n"
                f"/notifications off — Disable\n"
                f"/notifications on 5 — Every 5 min\n"
                f"/notifications on 30 — Every 30 min"
            )

    except Exception as e:
        logger.error("[/notifications] %s", e)
        await update.message.reply_text(f"Error: {e}")

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /cancel <order_id> — cancel a CLOB order."""
    try:
        if not _authorized(update):
            return await _deny(update)

        if mode_manager.get_mode() != "production":
            await update.message.reply_text("❌ Only available in production mode.")
            return

        args = context.args
        if not args:
            await update.message.reply_text("Usage: /cancel <order_id>")
            return

        order_id = args[0]
        result = pm_trade.cancel_order(order_id)
        if result:
            await update.message.reply_text(f"✅ Order `{order_id}` cancelled.")
        else:
            await update.message.reply_text(f"❌ Failed to cancel order `{order_id}`.")
    except Exception as e:
        logger.error("[/cancel] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_cancelall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /cancelall — cancel all open CLOB orders."""
    try:
        if not _authorized(update):
            return await _deny(update)

        if mode_manager.get_mode() != "production":
            await update.message.reply_text("❌ Only available in production mode.")
            return

        result = pm_trade.cancel_all_orders()
        if result:
            await update.message.reply_text("✅ All orders cancelled.")
        else:
            await update.message.reply_text("❌ Failed to cancel orders.")
    except Exception as e:
        logger.error("[/cancelall] %s", e)
        await update.message.reply_text(f"Error: {e}")


# ═══════════════════════════════════════════════════════════
# MODE MANAGEMENT
# ═══════════════════════════════════════════════════════════

async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /mode — show current mode."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode = mode_manager.get_mode()
        emoji = "🔴 Production" if mode == "production" else "🟡 Simulation"
        await update.message.reply_text(f"Current mode: {emoji}")
    except Exception as e:
        logger.error("[/mode] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_simulate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /simulate — switch to simulation mode."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode_manager.set_mode("simulation")
        await _safe_reply(update, "🟡 Switched to *simulation mode*\\. No real orders will be placed\\.")
    except Exception as e:
        logger.error("[/simulate] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_production(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /production — request production mode activation."""
    try:
        if not _authorized(update):
            return await _deny(update)

        success, message = mode_manager.request_production()
        if success:
            await _safe_reply(update, message)
        else:
            await update.message.reply_text(message)
    except Exception as e:
        logger.error("[/production] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /confirm <code> — confirm production mode."""
    try:
        if not _authorized(update):
            return await _deny(update)

        args = context.args
        if not args:
            await update.message.reply_text("Usage: /confirm <6-digit-code>")
            return

        code = args[0]
        success, message = mode_manager.confirm_production(code)
        if success:
            await _safe_reply(update, message)
        else:
            await update.message.reply_text(message)
    except Exception as e:
        logger.error("[/confirm] %s", e)
        await update.message.reply_text(f"Error: {e}")


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /clear — clear all simulation data (simulation mode only)."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode = mode_manager.get_mode()
        if mode != "simulation":
            await update.message.reply_text("⚠️ *Unsafe in production mode\\!* Use /simulate first\\.", parse_mode="MarkdownV2")
            return

        from core.state import clear_simulation_data
        success, message = clear_simulation_data()
        if success:
            await _safe_reply(update, message)
        else:
            await update.message.reply_text(message)

    except Exception as e:
        logger.error("[/clear] %s", e)
        await update.message.reply_text(f"Error: {e}")
