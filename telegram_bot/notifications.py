"""
telegram_bot/notifications.py — Push notifications to the configured Telegram chat.
Used by strategy.py and scheduler.py to send trade alerts, errors, etc.
"""

import logging
from config.settings import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger("weatherbet.notifications")

_bot_instance = None


def set_bot(bot):
    """Register the telegram.Bot instance (called during bot startup)."""
    global _bot_instance
    _bot_instance = bot


async def notify(message: str):
    """Send a text notification to the configured chat."""
    if not _bot_instance or not TELEGRAM_CHAT_ID:
        logger.info("[NOTIFY-LOCAL] %s", message)
        return

    try:
        await _bot_instance.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=None,  # plain text for auto notifications
        )
    except Exception as e:
        logger.error("[NOTIFY] Failed to send: %s", e)


async def notify_md(message: str):
    """Send a MarkdownV2-formatted notification."""
    if not _bot_instance or not TELEGRAM_CHAT_ID:
        logger.info("[NOTIFY-LOCAL] %s", message)
        return

    try:
        await _bot_instance.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode="MarkdownV2",
        )
    except Exception as e:
        # Fallback to plain text
        try:
            await _bot_instance.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
            )
        except Exception:
            logger.error("[NOTIFY] Failed completely: %s", e)


async def notify_error(error: str):
    """Send a critical error notification."""
    msg = f"🚨 *CRITICAL ERROR*\n\n{error}"
    await notify(msg)
