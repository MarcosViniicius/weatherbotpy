#!/usr/bin/env python3
"""
main.py — WeatherBet v3 entrypoint.
Starts the Telegram bot and the scan scheduler in a single asyncio event loop.
If TELEGRAM_TOKEN is not configured, runs in headless mode (scheduler only).
"""

import sys
import asyncio
import logging
import os
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import settings
from config.locations import LOCATIONS
from services import mode_manager
from services.scheduler import start_scheduler, stop_scheduler
from services.web_dashboard import start_dashboard, stop_dashboard, DASHBOARD_PORT
from telegram_bot.bot import build_application
from telegram_bot.notifications import set_bot, notify
from core.calibration import load_cal

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("weatherbet")


# ── Single Instance Lock ──────────────────────────────────
def _check_single_instance():
    """Ensure only one bot instance is running. Remove stale lockfile on restart."""
    lock_file = Path("/tmp/weatherbot.lock")
    
    if lock_file.exists():
        try:
            pid = int(lock_file.read_text().strip())
            # Check if process still exists
            if os.path.exists(f"/proc/{pid}"):
                logger.error(
                    "[LOCK] Another bot instance is already running (PID %d). "
                    "Run: pkill -f 'python main.py'", pid
                )
                sys.exit(1)
            else:
                # Stale lock file — remove it
                lock_file.unlink()
                logger.info("[LOCK] Removed stale lock file (process %d not found)", pid)
        except Exception:
            pass
    
    # Create new lock file
    lock_file.write_text(str(os.getpid()))
    logger.info("[LOCK] Instance lock acquired (PID %d)", os.getpid())


def _print_banner():
    mode = mode_manager.get_mode()
    mode_label = "🔴 PRODUCTION" if mode == "production" else "🟡 SIMULATION"
    telegram_ok = "✅" if settings.TELEGRAM_TOKEN and not settings.TELEGRAM_TOKEN.startswith("your-") else "❌"
    clob_ok = "✅" if settings.POLYMARKET_PRIVATE_KEY and not settings.POLYMARKET_PRIVATE_KEY.startswith("your-") else "❌"

    print(f"""
╔══════════════════════════════════════════════╗
║          🌤  WeatherBet v3                   ║
╠══════════════════════════════════════════════╣
║  Mode:      {mode_label:<33}║
║  Cities:    {len(LOCATIONS):<33}║
║  Balance:   ${settings.BALANCE:<32}║
║  Max bet:   ${settings.MAX_BET:<32}║
║  Scan:      {settings.SCAN_INTERVAL // 60} min{' ' * 28}║
║  Telegram:  {telegram_ok:<33}║
║  CLOB:      {clob_ok:<33}║
║  Data:      {str(settings.DATA_DIR):<33}║
║  Dashboard: http://localhost:{DASHBOARD_PORT:<22}║
╚══════════════════════════════════════════════╝
""")


async def _run_headless():
    """Run without Telegram — scheduler only."""
    logger.info("Running in HEADLESS mode (no Telegram token)")
    _print_banner()

    async def log_notify(msg):
        logger.info("[NOTIFY] %s", msg)

    task = start_scheduler(log_notify)
    try:
        await task
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        stop_scheduler()


async def _post_init(app):
    """Called after the Application is initialised — start the scheduler."""
    set_bot(app.bot)
    start_scheduler(notify)
    logger.info("[BOT] Scheduler started alongside Telegram polling")


async def _post_shutdown(app):
    """Called on graceful shutdown."""
    stop_scheduler()
    stop_dashboard()
    
    # Clean up lock file
    lock_file = Path("/tmp/weatherbot.lock")
    if lock_file.exists():
        try:
            lock_file.unlink()
        except Exception:
            pass
    
    logger.info("[BOT] Scheduler stopped")


def main():
    # Check single instance
    _check_single_instance()
    
    # Load calibration data
    load_cal()

    # Start web dashboard
    start_dashboard(DASHBOARD_PORT)

    _print_banner()

    app = build_application()

    if app is None:
        # No Telegram — headless mode
        try:
            asyncio.run(_run_headless())
        except KeyboardInterrupt:
            stop_scheduler()
            print("\n  Stopped. Bye!")
    else:
        # Full mode: Telegram + Scheduler
        app.post_init = _post_init
        app.post_shutdown = _post_shutdown

        logger.info("[MAIN] Starting Telegram polling + scheduler...")
        
        # Add error handler for Telegram conflicts
        async def telegram_error_handler(update, context):
            """Handle errors from Telegram updates."""
            try:
                raise context.error
            except Exception as e:
                error_msg = str(e)
                if "Conflict" in error_msg and "getUpdates" in error_msg:
                    logger.warning(
                        "[BOT] Telegram getUpdates conflict detected. "
                        "This is expected if you have multiple instances. "
                        "Ignoring and continuing..."
                    )
                else:
                    logger.error("[BOT] Telegram error: %s", e)
        
        if hasattr(app, 'add_error_handler'):
            app.add_error_handler(telegram_error_handler)
        
        try:
            app.run_polling(
                drop_pending_updates=True,
                allowed_updates=["message"],
            )
        except KeyboardInterrupt:
            logger.info("[MAIN] Keyboard interrupt received")
        finally:
            # Clean up
            _post_shutdown(app)


if __name__ == "__main__":
    main()
