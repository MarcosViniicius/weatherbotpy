"""
connectors/polymarket_read.py — Read-only Polymarket Gamma API connector.
No authentication required. Used in both simulation and production modes.
"""

import re
import json
import logging
import requests
from connectors.resilience import retry_with_backoff, gamma_cb

logger = logging.getLogger("weatherbet.polymarket_read")

GAMMA_BASE = "https://gamma-api.polymarket.com"


# ═══════════════════════════════════════════════════════════
# EVENT / MARKET QUERIES
# ═══════════════════════════════════════════════════════════

@retry_with_backoff(max_retries=3, base_delay=1.0)
def get_event(city_slug: str, month: str, day: int, year: int) -> dict | None:
    """Fetch a weather event from Polymarket by its URL slug."""
    if not gamma_cb.can_execute():
        logger.warning("[GAMMA] Circuit open — skipping get_event")
        return None

    slug = f"highest-temperature-in-{city_slug}-on-{month}-{day}-{year}"
    try:
        r = requests.get(f"{GAMMA_BASE}/events?slug={slug}", timeout=(15, 20))
        r.raise_for_status()
        data = r.json()
        gamma_cb.record_success()
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
    except requests.RequestException as e:
        gamma_cb.record_failure()
        raise
    return None


@retry_with_backoff(max_retries=2, base_delay=0.5)
def get_market_price(market_id: str) -> float | None:
    """Get the current YES price for a specific market."""
    if not gamma_cb.can_execute():
        return None
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=(10, 15))
        r.raise_for_status()
        data = r.json()
        gamma_cb.record_success()
        prices = json.loads(data.get("outcomePrices", "[0.5,0.5]"))
        return float(prices[0])
    except requests.RequestException:
        gamma_cb.record_failure()
        raise
    except Exception:
        return None


@retry_with_backoff(max_retries=2, base_delay=0.5)
def get_market_detail(market_id: str) -> dict | None:
    """Get full market detail (bestBid, bestAsk, closed status, etc.)."""
    if not gamma_cb.can_execute():
        return None
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=(10, 15))
        r.raise_for_status()
        gamma_cb.record_success()
        return r.json()
    except requests.RequestException:
        gamma_cb.record_failure()
        raise


def check_market_resolved(market_id: str) -> bool | None:
    """
    Check if a market has closed and who won.
    Returns: None (still open), True (YES won), False (NO won).
    """
    try:
        data = get_market_detail(market_id)
        if not data:
            return None
        closed = data.get("closed", False)
        if not closed:
            return None
        prices = json.loads(data.get("outcomePrices", "[0.5,0.5]"))
        yes_price = float(prices[0])
        if yes_price >= 0.95:
            return True
        elif yes_price <= 0.05:
            return False
        return None
    except Exception as e:
        logger.error("[RESOLVE] %s: %s", market_id, e)
        return None


# ═══════════════════════════════════════════════════════════
# PARSING HELPERS
# ═══════════════════════════════════════════════════════════

def parse_temp_range(question: str) -> tuple[float, float] | None:
    """Extract temperature range from a market question string."""
    if not question:
        return None
    num = r'(-?\d+(?:\.\d+)?)'
    if re.search(r'or below', question, re.IGNORECASE):
        m = re.search(num + r'[°]?[FC] or below', question, re.IGNORECASE)
        if m:
            return (-999.0, float(m.group(1)))
    if re.search(r'or higher', question, re.IGNORECASE):
        m = re.search(num + r'[°]?[FC] or higher', question, re.IGNORECASE)
        if m:
            return (float(m.group(1)), 999.0)
    m = re.search(r'between ' + num + r'-' + num + r'[°]?[FC]', question, re.IGNORECASE)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    m = re.search(r'be ' + num + r'[°]?[FC] on', question, re.IGNORECASE)
    if m:
        v = float(m.group(1))
        return (v, v)
    return None


def hours_to_resolution(end_date_str: str) -> float:
    """Hours remaining until a market resolves."""
    from datetime import datetime, timezone
    try:
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return max(0.0, (end - datetime.now(timezone.utc)).total_seconds() / 3600)
    except Exception:
        return 999.0
