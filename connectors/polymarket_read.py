"""
connectors/polymarket_read.py — Read-only Polymarket Gamma API connector.
No authentication required. Used in both simulation and production modes.
"""

import re
import json
import logging
import requests
from config import settings
from config.locations import LOCATIONS
from connectors.resilience import retry_with_backoff, gamma_cb, get_http_session

logger = logging.getLogger("weatherbet.polymarket_read")

GAMMA_BASE = "https://gamma-api.polymarket.com"
_gamma_session = get_http_session("gamma")


def _slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return re.sub(r"-{2,}", "-", value).strip("-")


def _event_slug_candidates(city_slug: str, month: str, day: int, year: int) -> list[str]:
    city_name = LOCATIONS.get(city_slug, {}).get("name", city_slug)
    city_variants = {_slugify(city_slug), _slugify(city_name)}
    day_variants = {str(day), f"{day:02d}"}
    candidates: list[str] = []
    templates = [
        "highest-temperature-in-{city}-on-{month}-{day}-{year}",
        "what-will-be-the-highest-temperature-in-{city}-on-{month}-{day}-{year}",
        "temperature-in-{city}-on-{month}-{day}-{year}",
    ]
    for city in city_variants:
        if not city:
            continue
        for d in day_variants:
            for tpl in templates:
                slug = tpl.format(city=city, month=month, day=d, year=year)
                if slug not in candidates:
                    candidates.append(slug)
    return candidates


def _event_matches_city_and_date(event: dict, city_slug: str, month: str, day: int, year: int) -> bool:
    city_name = _slugify(LOCATIONS.get(city_slug, {}).get("name", city_slug))
    slug = _slugify(str(event.get("slug", "")))
    title = _slugify(str(event.get("title", "")))
    end_date = str(event.get("endDate", ""))
    date_token = f"-{month}-{day}-{year}"
    date_token_padded = f"-{month}-{day:02d}-{year}"

    has_city = (
        _slugify(city_slug) in slug
        or _slugify(city_slug) in title
        or city_name in slug
        or city_name in title
    )
    has_date = (
        date_token in slug
        or date_token_padded in slug
        or end_date.startswith(f"{year:04d}-")
    )
    return has_city and has_date


# ═══════════════════════════════════════════════════════════
# EVENT / MARKET QUERIES
# ═══════════════════════════════════════════════════════════

@retry_with_backoff(max_retries=3, base_delay=1.0)
def get_event(city_slug: str, month: str, day: int, year: int) -> dict | None:
    """Fetch a weather event from Polymarket by its URL slug."""
    if not gamma_cb.can_execute():
        logger.warning("[GAMMA] Circuit open — skipping get_event")
        return None

    date_label = f"{year:04d}-{month}-{day:02d}"
    candidates = _event_slug_candidates(city_slug, month, day, year)
    for slug in candidates:
        try:
            r = _gamma_session.get(
                f"{GAMMA_BASE}/events",
                params={"slug": slug},
                timeout=(settings.POLYMARKET_TIMEOUT, settings.POLYMARKET_TIMEOUT + 5),
            )
            r.raise_for_status()
            data = r.json()
            gamma_cb.record_success()
            if data and isinstance(data, list):
                for event in data:
                    if _event_matches_city_and_date(event, city_slug, month, day, year):
                        return event
        except requests.RequestException as e:
            logger.debug("[GAMMA] Slug lookup failed slug=%s city=%s date=%s error=%s", slug, city_slug, date_label, e)
            gamma_cb.record_failure()
            raise

    # Fallback query in case slug format changed upstream.
    try:
        r = _gamma_session.get(
            f"{GAMMA_BASE}/events",
            params={"active": "true", "limit": 250},
            timeout=(settings.POLYMARKET_TIMEOUT, settings.POLYMARKET_TIMEOUT + 5),
        )
        r.raise_for_status()
        data = r.json()
        gamma_cb.record_success()
        if isinstance(data, list):
            sample_slugs = [str(e.get("slug")) for e in data[:5]]
            for event in data:
                if _event_matches_city_and_date(event, city_slug, month, day, year):
                    logger.info(
                        "[GAMMA] Event resolved via fallback city=%s date=%s slug=%s",
                        city_slug, date_label, event.get("slug"),
                    )
                    return event
            logger.warning(
                "[GAMMA] Event not found city=%s date=%s tried_slugs=%s fallback_count=%d sample_slugs=%s",
                city_slug, date_label, candidates[:5], len(data), sample_slugs,
            )
        else:
            logger.warning(
                "[GAMMA] Event lookup returned non-list payload city=%s date=%s payload_type=%s",
                city_slug, date_label, type(data).__name__,
            )
    except requests.RequestException:
        gamma_cb.record_failure()
        raise
    return None


@retry_with_backoff(max_retries=2, base_delay=0.5)
def get_market_price(market_id: str) -> float | None:
    """Get the current YES price for a specific market."""
    if not gamma_cb.can_execute():
        return None
    try:
        r = _gamma_session.get(
            f"{GAMMA_BASE}/markets/{market_id}",
            timeout=(settings.POLYMARKET_TIMEOUT, settings.POLYMARKET_TIMEOUT + 5),
        )
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
        r = _gamma_session.get(
            f"{GAMMA_BASE}/markets/{market_id}",
            timeout=(settings.POLYMARKET_TIMEOUT, settings.POLYMARKET_TIMEOUT + 5),
        )
        r.raise_for_status()
        gamma_cb.record_success()
        return r.json()
    except requests.RequestException:
        gamma_cb.record_failure()
        raise


def polymarket_market_url(payload: dict | None) -> str:
    """
    Best-effort Polymarket permalink from Gamma payloads.
    Prefers event URLs because they are the most stable public route.
    """
    if not isinstance(payload, dict):
        return ""

    for key in ("eventSlug", "event_slug", "event_slug_id"):
        slug = str(payload.get(key, "") or "").strip()
        if slug:
            return f"https://polymarket.com/event/{slug}"

    event = payload.get("event")
    if isinstance(event, dict):
        for key in ("slug", "eventSlug", "event_slug"):
            slug = str(event.get(key, "") or "").strip()
            if slug:
                return f"https://polymarket.com/event/{slug}"

    for key in ("slug", "marketSlug", "market_slug"):
        slug = str(payload.get(key, "") or "").strip()
        if slug:
            return f"https://polymarket.com/market/{slug}"

    return ""


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
        for key in ("winner", "outcomeWinner", "winningOutcome", "result"):
            raw = data.get(key)
            if raw is None:
                continue
            value = str(raw).strip().lower()
            if value in ("yes", "1", "true"):
                return True
            if value in ("no", "0", "false"):
                return False

        prices = json.loads(data.get("outcomePrices", "[0.5,0.5]"))
        yes_price = float(prices[0])
        if yes_price >= 0.999:
            return True
        elif yes_price <= 0.001:
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
