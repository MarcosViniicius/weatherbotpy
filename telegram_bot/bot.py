"""
telegram_bot/bot.py — Telegram Application setup.
Registers all handlers and runs the polling loop.
"""

import logging
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

from config.settings import TELEGRAM_TOKEN
from telegram_bot import handlers
from telegram_bot.notifications import set_bot

logger = logging.getLogger("weatherbet.bot")


def build_application() -> Application | None:
    """Build the Telegram Application with all handlers registered."""
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN.startswith("your-"):
        logger.warning(
            "[BOT] TELEGRAM_TOKEN not configured. "
            "Bot will run in headless mode (no Telegram)."
        )
        return None

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Register command handlers
    app.add_handler(CommandHandler("start",      handlers.cmd_start))
    app.add_handler(CommandHandler("help",       handlers.cmd_help))
    app.add_handler(CommandHandler("menu",       handlers.cmd_menu))
    app.add_handler(CommandHandler("hidemenu",   handlers.cmd_hidemenu))
    app.add_handler(CommandHandler("status",     handlers.cmd_status))
    app.add_handler(CommandHandler("positions",  handlers.cmd_positions))
    app.add_handler(CommandHandler("markets",    handlers.cmd_markets))
    app.add_handler(CommandHandler("orders",     handlers.cmd_orders))
    app.add_handler(CommandHandler("startbot",   handlers.cmd_iniciar))
    app.add_handler(CommandHandler("stopbot",    handlers.cmd_parar))
    app.add_handler(CommandHandler("iniciar",    handlers.cmd_iniciar))
    app.add_handler(CommandHandler("parar",      handlers.cmd_parar))
    app.add_handler(CommandHandler("resume",     handlers.cmd_iniciar))
    app.add_handler(CommandHandler("pause",      handlers.cmd_parar))
    app.add_handler(CommandHandler("scan",       handlers.cmd_scan))
    app.add_handler(CommandHandler("risk",       handlers.cmd_risk))
    app.add_handler(CommandHandler("setrisk",    handlers.cmd_setrisk))
    app.add_handler(CommandHandler("report",     handlers.cmd_report))
    app.add_handler(CommandHandler("daily",      handlers.cmd_daily))
    app.add_handler(CommandHandler("weekly",     handlers.cmd_weekly))
    app.add_handler(CommandHandler("mode",       handlers.cmd_mode))
    app.add_handler(CommandHandler("simulate",   handlers.cmd_simulate))
    app.add_handler(CommandHandler("production", handlers.cmd_production))
    app.add_handler(CommandHandler("confirm",    handlers.cmd_confirm))
    app.add_handler(CommandHandler("clear",      handlers.cmd_clear))
    app.add_handler(CommandHandler("cancel",     handlers.cmd_cancel))
    app.add_handler(CommandHandler("cancelall",  handlers.cmd_cancelall))
    app.add_handler(CommandHandler("notifications", handlers.cmd_notifications))
    app.add_handler(CommandHandler("calibration", handlers.cmd_calibration))

    # Inline menu callbacks (catch all; handler validates action prefix)
    app.add_handler(CallbackQueryHandler(handlers.cmd_menu_callback))

    # Legacy reply-keyboard text fallback
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.cmd_menu_text_fallback))

    logger.info("[BOT] Application built with %d handlers", 29)
    return app
