"""
services/mode_manager.py — Bot mode management (simulation / production).
Supports .env, config/mode.json, and runtime switching via Telegram.
Production mode requires a one-time confirmation code.
"""

import json
import random
import string
import logging
from datetime import datetime, timezone
from config.settings import MODE_FILE, validate_production_credentials

logger = logging.getLogger("weatherbet.mode")

_pending_code: str | None = None
_pending_expiry: float = 0


def get_mode() -> str:
    """
    Current operating mode. Priority:
    1. Environment variable BOT_MODE (if set)
    2. config/mode.json
    3. Default: 'simulation'
    """
    import os
    env_mode = os.environ.get("BOT_MODE", "").strip().lower()
    if env_mode in ("simulation", "production"):
        return env_mode

    if MODE_FILE.exists():
        try:
            data = json.loads(MODE_FILE.read_text(encoding="utf-8"))
            mode = data.get("mode", "simulation").strip().lower()
            if mode in ("simulation", "production"):
                return mode
        except Exception:
            pass

    return "simulation"


def set_mode(mode: str) -> bool:
    """Persist mode to config/mode.json. Returns True on success."""
    mode = mode.strip().lower()
    if mode not in ("simulation", "production"):
        return False

    data = {
        "mode": mode,
        "changed_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        MODE_FILE.parent.mkdir(parents=True, exist_ok=True)
        MODE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info("[MODE] Changed to: %s", mode)
        return True
    except Exception as e:
        logger.error("[MODE] Failed to save: %s", e)
        return False


def request_production() -> tuple[bool, str]:
    """
    Initiate production mode activation.
    Returns (success, message).
    If success, the message contains the 6-digit confirmation code.
    """
    global _pending_code, _pending_expiry

    missing = validate_production_credentials()
    if missing:
        return False, f"❌ Missing credentials: {', '.join(missing)}\nConfigure them in .env before switching to production."

    code = "".join(random.choices(string.digits, k=6))
    _pending_code = code
    _pending_expiry = datetime.now(timezone.utc).timestamp() + 120  # 2 min expiry

    return True, (
        f"⚠️ *Production Mode Activation*\n\n"
        f"This will execute REAL trades with REAL money\\.\n"
        f"Confirmation code: `{code}`\n\n"
        f"Send `/confirm {code}` within 2 minutes to activate\\.\n"
        f"Send `/simulate` to cancel\\."
    )


def confirm_production(code: str) -> tuple[bool, str]:
    """
    Confirm production mode with the generated code.
    Returns (success, message).
    """
    global _pending_code, _pending_expiry

    if _pending_code is None:
        return False, "❌ No pending production request. Use `/production` first."

    now = datetime.now(timezone.utc).timestamp()
    if now > _pending_expiry:
        _pending_code = None
        return False, "❌ Confirmation code expired. Use `/production` again."

    if code.strip() != _pending_code:
        return False, "❌ Invalid code. Try again or use `/production` for a new code."

    _pending_code = None
    _pending_expiry = 0

    if set_mode("production"):
        return True, "🔴 *Production mode ACTIVATED*\nReal orders will be placed on Polymarket\\."
    else:
        return False, "❌ Failed to save mode. Check file permissions."
