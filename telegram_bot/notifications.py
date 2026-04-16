"""
telegram_bot/notifications.py — Push notifications to the configured Telegram chat.
Used by strategy.py and scheduler.py to send trade alerts, errors, etc.
"""

import logging
from config.settings import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger("weatherbet.notifications")

_bot_instance = None


def _target_chat_ids() -> list[str]:
    raw = str(TELEGRAM_CHAT_ID or "").strip()
    if not raw:
        return []
    return [chat_id.strip() for chat_id in raw.split(",") if chat_id.strip()]


def set_bot(bot):
    """Register the telegram.Bot instance (called during bot startup)."""
    global _bot_instance
    _bot_instance = bot


async def notify(message: str):
    """Send a text notification to the configured chat."""
    chat_ids = _target_chat_ids()
    if not _bot_instance or not chat_ids:
        logger.info("[NOTIFY-LOCAL] %s", message)
        return

    for chat_id in chat_ids:
        try:
            await _bot_instance.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=None,  # plain text for auto notifications
            )
        except Exception as e:
            logger.error("[NOTIFY] Failed to send to %s: %s", chat_id, e)


async def notify_md(message: str):
    """Send a MarkdownV2-formatted notification."""
    chat_ids = _target_chat_ids()
    if not _bot_instance or not chat_ids:
        logger.info("[NOTIFY-LOCAL] %s", message)
        return

    for chat_id in chat_ids:
        try:
            await _bot_instance.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode="MarkdownV2",
            )
        except Exception as e:
            # Fallback to plain text
            try:
                await _bot_instance.send_message(
                    chat_id=chat_id,
                    text=message,
                )
            except Exception:
                logger.error("[NOTIFY] Failed completely for %s: %s", chat_id, e)


async def notify_error(error: str):
    """Send a critical error notification."""
    msg = f"🚨 *CRITICAL ERROR*\n\n{error}"
    await notify(msg)
