"""
telegram_bot/handlers.py — All Telegram command handlers.
Each handler is wrapped in try/except to prevent silent crashes.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from telegram.ext import ContextTypes

from config.settings import TELEGRAM_CHAT_ID, get_risk_config, update_risk_config, RISK_CONFIG_FILE
from config.locations import LOCATIONS
from core.state import load_state, load_all_markets
from connectors import polymarket_trade as pm_trade
from connectors import polymarket_read as pm_read
from services import mode_manager
from services.scheduler import force_scan, is_running, start_scheduler, stop_scheduler
from telegram_bot import formatters as fmt
from telegram_bot.notifications import notify

logger = logging.getLogger("weatherbet.handlers")

_RISK_KEYS_ORDER = [
    "balance",
    "max_bet",
    "min_ev",
    "min_edge",
    "max_price",
    "min_volume",
    "min_hours",
    "max_hours",
    "kelly_fraction",
    "max_slippage",
    "scan_interval",
    "calibration_min",
]

_RISK_INT_KEYS = {"min_volume", "scan_interval", "calibration_min"}


def _risk_display_value(key: str, value) -> str:
    if key in _RISK_INT_KEYS:
        return str(int(value))
    return f"{float(value):.4f}".rstrip("0").rstrip(".")


def _risk_edit_prompt_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("❌ Cancel Edit", callback_data="wb:risk:cancel_edit"),
                InlineKeyboardButton("↩️ Back to Risk Keys", callback_data="wb:risk"),
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="wb:refresh")],
        ]
    )


def _risk_menu_inline() -> InlineKeyboardMarkup:
    risk = get_risk_config()
    rows = []
    for key in _RISK_KEYS_ORDER:
        rows.append(
            [
                InlineKeyboardButton(
                    f"{key}: { _risk_display_value(key, risk.get(key, 0)) }",
                    callback_data=f"wb:risk:key:{key}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="wb:risk"),
            InlineKeyboardButton("🏠 Main Menu", callback_data="wb:refresh"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _notifications_menu_inline(enabled: bool, interval: int) -> InlineKeyboardMarkup:
    label = "Disable" if enabled else "Enable"
    toggle = "wb:notif:off" if enabled else "wb:notif:on"
    minutes = max(1, int(interval // 60))
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"{label} Notifications", callback_data=toggle)],
            [
                InlineKeyboardButton("5m", callback_data="wb:notif:int:300"),
                InlineKeyboardButton("10m", callback_data="wb:notif:int:600"),
                InlineKeyboardButton("30m", callback_data="wb:notif:int:1800"),
            ],
            [
                InlineKeyboardButton("60m", callback_data="wb:notif:int:3600"),
                InlineKeyboardButton(f"Current: {minutes}m", callback_data="wb:notif:menu"),
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="wb:refresh")],
        ]
    )


def _mode_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🟡 Set Simulation", callback_data="wb:mode:set:simulation"),
                InlineKeyboardButton("🔴 Request Production", callback_data="wb:mode:reqprod"),
            ],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="wb:refresh")],
        ]
    )


def _mode_confirm_inline(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Confirm Production", callback_data=f"wb:mode:confirm:{code}")],
            [InlineKeyboardButton("🟡 Keep Simulation", callback_data="wb:mode:set:simulation")],
            [InlineKeyboardButton("🏠 Main Menu", callback_data="wb:refresh")],
        ]
    )


def _main_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 Status", callback_data="wb:status"),
                InlineKeyboardButton("📂 Positions", callback_data="wb:positions"),
            ],
            [
                InlineKeyboardButton("🔎 Markets", callback_data="wb:markets"),
                InlineKeyboardButton("📋 Orders", callback_data="wb:orders"),
            ],
            [
                InlineKeyboardButton("🔄 Scan Now", callback_data="wb:scan"),
                InlineKeyboardButton("🔔 Notifications", callback_data="wb:notif"),
            ],
            [
                InlineKeyboardButton("▶️ Start Bot", callback_data="wb:start"),
                InlineKeyboardButton("⏸️ Stop Bot", callback_data="wb:stop"),
            ],
            [
                InlineKeyboardButton("⚙️ Mode", callback_data="wb:mode"),
                InlineKeyboardButton("⚖️ Calibration", callback_data="wb:calib"),
            ],
            [InlineKeyboardButton("🛡 Risk Config", callback_data="wb:risk")],
            [
                InlineKeyboardButton("📈 Report", callback_data="wb:report"),
                InlineKeyboardButton("🗓 Daily", callback_data="wb:daily"),
                InlineKeyboardButton("📆 Weekly", callback_data="wb:weekly"),
            ],
            [
                InlineKeyboardButton("🟡 Simulation", callback_data="wb:simulate"),
                InlineKeyboardButton("🔴 Production", callback_data="wb:production"),
            ],
            [
                InlineKeyboardButton("♻️ Refresh", callback_data="wb:refresh"),
                InlineKeyboardButton("❓ Help", callback_data="wb:help"),
            ],
        ]
    )


def _route_menu_action(action: str):
    return {
        "wb:status": cmd_status,
        "wb:positions": cmd_positions,
        "wb:markets": cmd_markets,
        "wb:orders": cmd_orders,
        "wb:scan": cmd_scan,
        "wb:risk": cmd_risk,
        "wb:start": cmd_iniciar,
        "wb:stop": cmd_parar,
        "wb:mode": cmd_mode,
        "wb:simulate": cmd_simulate,
        "wb:production": cmd_production,
        "wb:report": cmd_report,
        "wb:daily": cmd_daily,
        "wb:weekly": cmd_weekly,
        "wb:calib": cmd_calibration,
        "wb:notif": cmd_notifications,
        "wb:help": cmd_help,
    }.get(action)


async def _handle_notifications_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str) -> bool:
    from services.scheduler import set_notifications, get_notifications_status

    if action == "wb:notif":
        action = "wb:notif:menu"

    if action == "wb:notif:menu":
        enabled, interval = get_notifications_status()
        status = "ENABLED" if enabled else "DISABLED"
        minutes = max(1, int(interval // 60))
        await _message(update).reply_text(
            f"🔔 Notifications: {status}\nCurrent interval: {minutes} min",
            reply_markup=_notifications_menu_inline(enabled, interval),
        )
        return True

    if action == "wb:notif:on":
        enabled, interval = get_notifications_status()
        set_notifications(True, interval)
        await _message(update).reply_text(
            "🔔 Periodic notifications ENABLED",
            reply_markup=_notifications_menu_inline(True, interval),
        )
        return True

    if action == "wb:notif:off":
        _, interval = get_notifications_status()
        set_notifications(False, interval)
        await _message(update).reply_text(
            "🔕 Periodic notifications DISABLED",
            reply_markup=_notifications_menu_inline(False, interval),
        )
        return True

    if action.startswith("wb:notif:int:"):
        try:
            seconds = max(60, int(action.split(":")[-1]))
        except ValueError:
            await _message(update).reply_text("Invalid interval option.")
            return True

        set_notifications(True, seconds)
        minutes = seconds // 60
        await _message(update).reply_text(
            f"🔔 Interval updated to every {minutes} min (enabled)",
            reply_markup=_notifications_menu_inline(True, seconds),
        )
        return True

    return False


async def _handle_mode_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str) -> bool:
    if action == "wb:mode":
        action = "wb:mode:menu"

    if action == "wb:mode:menu":
        mode = mode_manager.get_mode()
        emoji = "🔴 Production" if mode == "production" else "🟡 Simulation"
        await _message(update).reply_text(
            f"Current mode: {emoji}\nChoose below:",
            reply_markup=_mode_menu_inline(),
        )
        return True

    if action == "wb:mode:set:simulation":
        mode_manager.set_mode("simulation")
        await _safe_reply(update, "🟡 Switched to *simulation mode*\. No real orders will be placed\.")
        await _message(update).reply_text("Mode menu:", reply_markup=_mode_menu_inline())
        return True

    if action == "wb:mode:reqprod":
        success, message = mode_manager.request_production()
        if not success:
            await _message(update).reply_text(message)
            await _message(update).reply_text("Mode menu:", reply_markup=_mode_menu_inline())
            return True

        code = message.split("Confirmation code: `")[-1].split("`")[0]
        context.user_data["pending_prod_code"] = code
        await _safe_reply(update, message)
        await _message(update).reply_text(
            "Confirm via button within 2 minutes:",
            reply_markup=_mode_confirm_inline(code),
        )
        return True

    if action.startswith("wb:mode:confirm:"):
        code = action.split(":")[-1].strip()
        success, message = mode_manager.confirm_production(code)
        if success:
            await _safe_reply(update, message)
        else:
            await _message(update).reply_text(message)
        await _message(update).reply_text("Mode menu:", reply_markup=_mode_menu_inline())
        return True

    return False


async def _handle_risk_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str) -> bool:
    if action == "wb:risk:cancel_edit":
        context.user_data.pop("pending_risk_key", None)
        await _message(update).reply_text("Risk edit canceled.", reply_markup=_risk_menu_inline())
        return True

    if action == "wb:risk":
        context.user_data.pop("pending_risk_key", None)
        risk = get_risk_config()
        lines = [
            "🛡 Risk Configuration \(tap a key to edit\)",
            f"Source: `{fmt.escape_md(str(RISK_CONFIG_FILE))}`",
            "",
        ]
        for k in _RISK_KEYS_ORDER:
            v = risk.get(k)
            lines.append(f"• `{fmt.escape_md(k)}` = `{fmt.escape_md(_risk_display_value(k, v))}`")

        await _safe_reply(update, "\n".join(lines))
        await _message(update).reply_text("Choose a risk key:", reply_markup=_risk_menu_inline())
        return True

    if action.startswith("wb:risk:key:"):
        key = action.split(":")[-1]
        risk = get_risk_config()
        if key not in risk:
            await _message(update).reply_text("Unknown risk key.")
            return True

        context.user_data["pending_risk_key"] = key
        current = _risk_display_value(key, risk[key])
        typ = "integer" if key in _RISK_INT_KEYS else "number"
        await _safe_reply(
            update,
            f"🛠 Editing `{fmt.escape_md(key)}`\nCurrent: `{fmt.escape_md(current)}`\nType expected: `{typ}`\n\nType the new value in chat\. Example: `{fmt.escape_md(current)}`",
        )
        await _message(update).reply_text("Waiting for your typed value...", reply_markup=_risk_edit_prompt_keyboard())
        return True

    return False


def _message(update: Update):
    msg = update.effective_message
    if msg is None and update.callback_query is not None:
        msg = update.callback_query.message
    if msg is None:
        raise RuntimeError("No message context available")
    return msg


def _menu_summary() -> str:
    try:
        state = load_state()
        markets = load_all_markets()
        open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
        mode = mode_manager.get_mode()
        sched = "Running" if is_running() else "Stopped"
        return (
            "🌤 *WeatherBet Control Panel*\n\n"
            f"Mode: *{fmt.escape_md(mode.upper())}*\n"
            f"Scheduler: *{fmt.escape_md(sched)}*\n"
            f"Balance: `${state.get('balance', 0):.2f}`\n"
            f"Open Positions: *{len(open_pos)}*\n"
            f"Total Trades: *{state.get('total_trades', 0)}*\n\n"
            "Tap a button below\\."
        )
    except Exception:
        return "🌤 *WeatherBet Control Panel*\n\nTap a button below\\."


def _authorized(update: Update) -> bool:
    """Check if sender/chat is authorized.

    Supports TELEGRAM_CHAT_ID as:
    - single id: "123456"
    - multiple ids: "123456,789012"

    Accepts either matching chat id or user id for flexibility.
    """
    if not TELEGRAM_CHAT_ID:
        return True  # no restriction if not configured

    allowed = {x.strip() for x in str(TELEGRAM_CHAT_ID).split(",") if x.strip()}
    if not allowed:
        return True

    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    user_id = str(update.effective_user.id) if update.effective_user else ""

    return chat_id in allowed or user_id in allowed


async def _deny(update: Update):
    chat_id = str(update.effective_chat.id) if update.effective_chat else "unknown"
    user_id = str(update.effective_user.id) if update.effective_user else "unknown"
    if update.callback_query is not None:
        try:
            await update.callback_query.answer("Unauthorized", show_alert=True)
        except Exception:
            pass
    await _message(update).reply_text(
        "⛔ Unauthorized.\n"
        f"chat_id={chat_id}\n"
        f"user_id={user_id}\n"
        "Add one of these IDs to TELEGRAM_CHAT_ID in .env."
    )


async def _safe_reply(update: Update, text: str):
    """Try MarkdownV2 first; fall back to plain text if parsing fails."""
    try:
        await _message(update).reply_text(text, parse_mode="MarkdownV2")
    except Exception:
        # Strip markdown formatting and send as plain text
        import re
        plain = text.replace("\\.", ".").replace("\\-", "-").replace("\\+", "+")
        plain = plain.replace("\\|", "|").replace("\\(", "(").replace("\\)", ")")
        plain = plain.replace("\\!", "!").replace("\\=", "=").replace("\\#", "#")
        plain = re.sub(r"(?<!\w)\*([^*]+)\*(?!\w)", r"\1", plain)  # bold
        plain = re.sub(r"`([^`]+)`", r"\1", plain)  # code
        await _message(update).reply_text(plain)

# ═══════════════════════════════════════════════════════════
# INFORMATIONAL COMMANDS
# ═══════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start — welcome message."""
    try:
        if not _authorized(update):
            return await _deny(update)

        # Clear legacy reply keyboard if present, then show inline panel
        await _message(update).reply_text("Refreshing control panel...", reply_markup=ReplyKeyboardRemove())
        await _safe_reply(update, _menu_summary())
        await _message(update).reply_text("Choose an action:", reply_markup=_main_menu_inline())
    except Exception as e:
        logger.error("[/start] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help."""
    return await cmd_start(update, context)


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show inline control panel in chat."""
    try:
        if not _authorized(update):
            return await _deny(update)
        await _message(update).reply_text("Refreshing control panel...", reply_markup=ReplyKeyboardRemove())
        await _safe_reply(update, _menu_summary())
        await _message(update).reply_text("Choose an action:", reply_markup=_main_menu_inline())
    except Exception as e:
        logger.error("[/menu] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_hidemenu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Explain inline menu behavior."""
    try:
        if not _authorized(update):
            return await _deny(update)
        await _message(update).reply_text("Inline menu is attached to messages in chat. Use /menu to open a fresh panel.")
    except Exception as e:
        logger.error("[/hidemenu] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current risk config loaded from TOML."""
    try:
        if not _authorized(update):
            return await _deny(update)

        risk = get_risk_config()
        lines = [
            "🛡 *Risk Configuration*",
            f"Source: `{fmt.escape_md(str(RISK_CONFIG_FILE))}`",
            "",
        ]
        for k, v in risk.items():
            lines.append(f"• `{fmt.escape_md(k)}` = `{fmt.escape_md(str(v))}`")

        lines.extend([
            "",
            "Usage:",
            "`/setrisk scan_interval 600`",
            "`/setrisk min_edge 0.08`",
            "`/setrisk max_bet 3.5`",
        ])
        await _safe_reply(update, "\n".join(lines))
    except Exception as e:
        logger.error("[/risk] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_setrisk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Update a risk setting directly in risk.toml."""
    try:
        if not _authorized(update):
            return await _deny(update)

        args = context.args
        if len(args) < 2:
            await _message(update).reply_text("Usage: /setrisk <key> <value>\nExample: /setrisk min_edge 0.08")
            return

        key = args[0]
        value = " ".join(args[1:]).strip()
        ok, msg = update_risk_config(key, value)
        if not ok:
            await _message(update).reply_text(f"❌ {msg}")
            return

        await _message(update).reply_text(
            f"✅ {msg}\n"
            f"Changes were written directly to TOML and applied to runtime."
        )
    except Exception as e:
        logger.error("[/setrisk] %s", e)
        await _message(update).reply_text(f"Error: {e}")


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
        await _message(update).reply_text(f"Error: {e}")


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
        await _message(update).reply_text(f"Error: {e}")


async def cmd_markets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /markets — list active weather markets on Polymarket."""
    try:
        if not _authorized(update):
            return await _deny(update)

        await _message(update).reply_text("🔍 Scanning markets...")

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
        await _message(update).reply_text(f"Error: {e}")


async def cmd_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /orders — show open CLOB orders (production only)."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode = mode_manager.get_mode()
        if mode != "production":
            await _message(update).reply_text("📭 Orders only available in production mode. Use /production to activate.")
            return

        orders = pm_trade.get_open_orders()
        if not orders:
            await _message(update).reply_text("📭 No open orders on the CLOB.")
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
        await _message(update).reply_text(f"Error: {e}")


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
        await _message(update).reply_text(f"Error: {e}")


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
        await _message(update).reply_text(f"Error: {e}")


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
        await _message(update).reply_text(f"Error: {e}")


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
        await _message(update).reply_text(f"Error: {e}")


# ═══════════════════════════════════════════════════════════
# ACTION COMMANDS
# ═══════════════════════════════════════════════════════════

async def cmd_iniciar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /iniciar — start scheduler (without restarting Telegram bot)."""
    try:
        if not _authorized(update):
            return await _deny(update)

        if is_running():
            await _message(update).reply_text("🟢 Bot is already running.")
            return

        start_scheduler(notify)
        await _message(update).reply_text("🟢 Bot started. Scheduler is active again.")
    except Exception as e:
        logger.error("[/iniciar] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_parar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /parar — stop scheduler (keeps Telegram bot online)."""
    try:
        if not _authorized(update):
            return await _deny(update)

        if not is_running():
            await _message(update).reply_text("🟡 Bot is already stopped.")
            return

        stop_scheduler()
        await _message(update).reply_text("⏸️ Bot paused. Scheduler stopped, Telegram stays online.")
    except Exception as e:
        logger.error("[/parar] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks from control panel."""
    try:
        if not _authorized(update):
            return await _deny(update)

        query = update.callback_query
        if query is None:
            return

        await query.answer()

        action = query.data or ""
        logger.info("[menu_callback] action=%s chat_id=%s user_id=%s", action, str(update.effective_chat.id) if update.effective_chat else "?", str(update.effective_user.id) if update.effective_user else "?")

        if await _handle_notifications_callback(update, context, action):
            return

        if await _handle_mode_callback(update, context, action):
            return

        if await _handle_risk_callback(update, context, action):
            return

        if action == "wb:refresh":
            await _safe_reply(update, _menu_summary())
            await _message(update).reply_text("Choose an action:", reply_markup=_main_menu_inline())
            return

        handler = _route_menu_action(action)
        if handler is None:
            await query.answer("Unknown action", show_alert=False)
            return

        await handler(update, context)
    except Exception as e:
        logger.exception("[menu_callback] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_menu_text_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Backward compatibility for old reply-keyboard buttons that send plain text."""
    try:
        if not _authorized(update):
            return await _deny(update)

        text = (update.message.text or "").strip() if update.message else ""
        normalized = text.lower().replace("\ufe0f", "").strip()

        pending_risk_key = context.user_data.get("pending_risk_key")
        if pending_risk_key and text and not text.startswith("/"):
            ok, msg = update_risk_config(pending_risk_key, text)
            if ok:
                context.user_data.pop("pending_risk_key", None)
                await _message(update).reply_text(f"✅ {pending_risk_key} updated to {text}")
                await _message(update).reply_text("Choose a risk key:", reply_markup=_risk_menu_inline())
            else:
                await _message(update).reply_text(
                    f"❌ {msg}\nType another value or tap Cancel Edit.",
                    reply_markup=_risk_edit_prompt_keyboard(),
                )
            return

        action = None
        if "status" in normalized:
            action = "wb:status"
        elif "position" in normalized:
            action = "wb:positions"
        elif "market" in normalized:
            action = "wb:markets"
        elif "order" in normalized:
            action = "wb:orders"
        elif "scan" in normalized:
            action = "wb:scan"
        elif "notification" in normalized:
            action = "wb:notif"
        elif "start bot" in normalized or normalized.startswith("start"):
            action = "wb:start"
        elif "stop bot" in normalized or normalized.startswith("stop") or "pause" in normalized:
            action = "wb:stop"
        elif normalized.endswith("mode") or normalized == "mode":
            action = "wb:mode"
        elif "calibration" in normalized:
            action = "wb:calib"
        elif "risk" in normalized:
            action = "wb:risk"
        elif "report" in normalized:
            action = "wb:report"
        elif "daily" in normalized:
            action = "wb:daily"
        elif "weekly" in normalized:
            action = "wb:weekly"
        elif "simulation" in normalized:
            action = "wb:simulate"
        elif "production" in normalized:
            action = "wb:production"
        elif "refresh" in normalized or "atualizar" in normalized:
            action = "wb:refresh"
        elif "help" in normalized or "ajuda" in normalized:
            action = "wb:help"

        logger.info("[menu_text_fallback] text=%s normalized=%s action=%s", text, normalized, action)
        if not action:
            # Give a small hint instead of silent ignore
            if normalized in {"menu", "/menu", "start", "/start"}:
                await cmd_start(update, context)
            return

        if action == "wb:refresh":
            await _message(update).reply_text("Refreshing control panel...", reply_markup=ReplyKeyboardRemove())
            await _safe_reply(update, _menu_summary())
            await _message(update).reply_text("Choose an action:", reply_markup=_main_menu_inline())
            return

        handler = _route_menu_action(action)
        if handler:
            await handler(update, context)
    except Exception as e:
        logger.exception("[menu_text_fallback] %s", e)
        await _message(update).reply_text(f"Error: {e}")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /scan — force an immediate scan."""
    try:
        if not _authorized(update):
            return await _deny(update)

        await _message(update).reply_text("🔄 Starting manual scan...")
        result = await force_scan(notify)
        await _message(update).reply_text(result)
    except Exception as e:
        logger.error("[/scan] %s", e)
        await _message(update).reply_text(f"Error: {e}")


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
            await _message(update).reply_text("🔕 Periodic notifications DISABLED")
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
            await _message(update).reply_text(
                f"🔔 Periodic notifications ENABLED\n"
                f"Interval: every {minutes} min\n"
                f"You'll receive position updates automatically.\n\n"
                f"Use /notifications off to disable."
            )
            return

        # Toggle if no args
        if enabled:
            set_notifications(False)
            await _message(update).reply_text("🔕 Periodic notifications DISABLED")
        else:
            minutes = 10
            set_notifications(True, minutes * 60)
            await _message(update).reply_text(
                f"🔔 Periodic notifications ENABLED\n"
                f"Interval: every {minutes} min\n\n"
                f"Commands:\n"
                f"/notifications off — Disable\n"
                f"/notifications on 5 — Every 5 min\n"
                f"/notifications on 30 — Every 30 min"
            )

    except Exception as e:
        logger.error("[/notifications] %s", e)
        await _message(update).reply_text(f"Error: {e}")

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /cancel <order_id> — cancel a CLOB order."""
    try:
        if not _authorized(update):
            return await _deny(update)

        if mode_manager.get_mode() != "production":
            await _message(update).reply_text("❌ Only available in production mode.")
            return

        args = context.args
        if not args:
            await _message(update).reply_text("Usage: /cancel <order_id>")
            return

        order_id = args[0]
        result = pm_trade.cancel_order(order_id)
        if result:
            await _message(update).reply_text(f"✅ Order `{order_id}` cancelled.")
        else:
            await _message(update).reply_text(f"❌ Failed to cancel order `{order_id}`.")
    except Exception as e:
        logger.error("[/cancel] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_cancelall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /cancelall — cancel all open CLOB orders."""
    try:
        if not _authorized(update):
            return await _deny(update)

        if mode_manager.get_mode() != "production":
            await _message(update).reply_text("❌ Only available in production mode.")
            return

        result = pm_trade.cancel_all_orders()
        if result:
            await _message(update).reply_text("✅ All orders cancelled.")
        else:
            await _message(update).reply_text("❌ Failed to cancel orders.")
    except Exception as e:
        logger.error("[/cancelall] %s", e)
        await _message(update).reply_text(f"Error: {e}")


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
        await _message(update).reply_text(f"Current mode: {emoji}")
    except Exception as e:
        logger.error("[/mode] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_simulate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /simulate — switch to simulation mode."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode_manager.set_mode("simulation")
        await _safe_reply(update, "🟡 Switched to *simulation mode*\\. No real orders will be placed\\.")
    except Exception as e:
        logger.error("[/simulate] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_production(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /production — request production mode activation."""
    try:
        if not _authorized(update):
            return await _deny(update)

        success, message = mode_manager.request_production()
        if success:
            await _safe_reply(update, message)
        else:
            await _message(update).reply_text(message)
    except Exception as e:
        logger.error("[/production] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /confirm <code> — confirm production mode."""
    try:
        if not _authorized(update):
            return await _deny(update)

        args = context.args
        if not args:
            await _message(update).reply_text("Usage: /confirm <6-digit-code>")
            return

        code = args[0]
        success, message = mode_manager.confirm_production(code)
        if success:
            await _safe_reply(update, message)
        else:
            await _message(update).reply_text(message)
    except Exception as e:
        logger.error("[/confirm] %s", e)
        await _message(update).reply_text(f"Error: {e}")


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /clear — clear all simulation data (simulation mode only)."""
    try:
        if not _authorized(update):
            return await _deny(update)

        mode = mode_manager.get_mode()
        if mode != "simulation":
            await _message(update).reply_text("⚠️ *Unsafe in production mode\\!* Use /simulate first\\.", parse_mode="MarkdownV2")
            return

        from core.state import clear_simulation_data
        success, message = clear_simulation_data()
        if success:
            await _safe_reply(update, message)
        else:
            await _message(update).reply_text(message)

    except Exception as e:
        logger.error("[/clear] %s", e)
        await _message(update).reply_text(f"Error: {e}")
