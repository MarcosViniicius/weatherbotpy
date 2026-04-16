"""
Polymarket CLOB trading connector.
Uses py-clob-client for authenticated order placement.
Only called in production mode.
"""

import inspect
import logging

from config import settings
from connectors.resilience import clob_cb, retry_with_backoff

logger = logging.getLogger("weatherbet.polymarket_trade")

_client = None


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_client():
    """Lazy-initialise the CLOB client. Returns None if credentials are missing."""
    global _client
    if _client is not None:
        return _client

    pk = str(settings.POLYMARKET_PRIVATE_KEY or "").strip()
    if not pk or pk.lower().startswith("your-"):
        logger.error("[CLOB] No valid private key configured - cannot trade")
        return None

    try:
        from py_clob_client.client import ClobClient

        _client = ClobClient(
            settings.POLYMARKET_HOST,
            key=pk,
            chain_id=settings.POLYMARKET_CHAIN_ID,
            signature_type=settings.POLYMARKET_SIGNATURE_TYPE,
            funder=settings.POLYMARKET_FUNDER or None,
        )
        _client.set_api_creds(_client.create_or_derive_api_creds())
        logger.info("[CLOB] Client initialised successfully")
        return _client
    except Exception as e:
        logger.error("[CLOB] Failed to initialise client: %s", e)
        _client = None
        return None


def _extract_numeric_balance(payload, depth: int = 0) -> float | None:
    """Best-effort extraction of a wallet balance from nested API payloads."""
    if depth > 4:
        return None
    if payload is None:
        return None
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, str):
        try:
            return float(payload)
        except ValueError:
            return None
    if isinstance(payload, dict):
        for key in (
            "balance",
            "available",
            "total",
            "amount",
            "usdc",
            "usdc_balance",
            "collateral",
            "value",
            "asset_balance",
            "numericBalance",
            "numeric_balance",
        ):
            if key in payload:
                parsed = _extract_numeric_balance(payload.get(key), depth + 1)
                if parsed is not None:
                    return parsed
        for value in payload.values():
            parsed = _extract_numeric_balance(value, depth + 1)
            if parsed is not None:
                return parsed
    if isinstance(payload, (list, tuple)):
        for item in payload:
            parsed = _extract_numeric_balance(item, depth + 1)
            if parsed is not None:
                return parsed
    return None


def _has_required_positional_params(method) -> bool:
    """Return True if method requires positional args without defaults."""
    try:
        sig = inspect.signature(method)
    except Exception:
        return False
    for p in sig.parameters.values():
        if (
            p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            and p.default is inspect.Parameter.empty
        ):
            return True
    return False


@retry_with_backoff(max_retries=2, base_delay=1.0)
def get_wallet_balance() -> float | None:
    """
    Fetch wallet collateral balance from CLOB API.
    Returns USDC-equivalent balance or None on failure.
    """
    client = _get_client()
    if client is None:
        return None

    calls = []
    if hasattr(client, "get_balance_allowance"):
        calls.append(("get_balance_allowance", client.get_balance_allowance))
    if hasattr(client, "get_collateral"):
        calls.append(("get_collateral", client.get_collateral))
    if hasattr(client, "get_usdc_balance"):
        calls.append(("get_usdc_balance", client.get_usdc_balance))
    if hasattr(client, "get_balance"):
        calls.append(("get_balance", client.get_balance))

    for name, method in calls:
        if _has_required_positional_params(method):
            continue
        try:
            payload = method()
            value = _extract_numeric_balance(payload)
            if value is not None:
                clob_cb.record_success()
                return max(0.0, round(float(value), 2))
        except Exception as e:
            clob_cb.record_failure()
            logger.warning("[CLOB] %s failed while reading wallet balance: %s", name, e)

    logger.warning("[CLOB] Could not read wallet balance from client API")
    return None


@retry_with_backoff(max_retries=2, base_delay=1.0)
def place_limit_order(
    token_id: str,
    price: float,
    size: float,
    side: str = "BUY",
) -> dict | None:
    """
    Place a GTC limit order on the Polymarket CLOB.

    Args:
        token_id: The outcome token ID.
        price: Price per share (0.01 - 0.99).
        size: Number of shares.
        side: "BUY" or "SELL".

    Returns:
        API response dict or None on failure.
    """
    client = _get_client()
    if client is None:
        return None

    if not clob_cb.can_execute():
        logger.warning("[CLOB] Circuit open - skipping order")
        return None

    if price <= 0 or price >= 1:
        logger.error("[CLOB] Invalid price: %.4f", price)
        return None
    if size < 0.1:
        logger.error("[CLOB] Size too small: %.2f", size)
        return None

    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        order_side = BUY if side.upper() == "BUY" else SELL
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=order_side,
        )
        signed = client.create_order(order_args)
        resp = client.post_order(signed, OrderType.GTC)
        clob_cb.record_success()
        logger.info("[CLOB] Order placed: %s %s shares @ $%.3f -> %s", side, size, price, resp)
        return resp
    except Exception as e:
        clob_cb.record_failure()
        logger.error("[CLOB] Order failed: %s", e)
        raise


@retry_with_backoff(max_retries=2, base_delay=1.0)
def place_market_order(
    token_id: str,
    amount: float,
    side: str = "BUY",
) -> dict | None:
    """
    Place a FOK market order (buy by dollar amount).

    Args:
        token_id: The outcome token ID.
        amount: Dollar amount to spend.
        side: "BUY" or "SELL".
    """
    client = _get_client()
    if client is None:
        return None

    if not clob_cb.can_execute():
        logger.warning("[CLOB] Circuit open - skipping market order")
        return None

    if amount < 0.50:
        logger.error("[CLOB] Amount too small: $%.2f", amount)
        return None

    try:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        order_side = BUY if side.upper() == "BUY" else SELL
        mo = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=order_side,
            order_type=OrderType.FOK,
        )
        signed = client.create_market_order(mo)
        resp = client.post_order(signed, OrderType.FOK)
        clob_cb.record_success()
        logger.info("[CLOB] Market order: %s $%.2f -> %s", side, amount, resp)
        return resp
    except Exception as e:
        clob_cb.record_failure()
        logger.error("[CLOB] Market order failed: %s", e)
        raise


def cancel_order(order_id: str) -> dict | None:
    """Cancel a specific open order."""
    client = _get_client()
    if client is None:
        return None
    try:
        resp = client.cancel(order_id)
        logger.info("[CLOB] Cancelled order %s", order_id)
        return resp
    except Exception as e:
        logger.error("[CLOB] Cancel failed for %s: %s", order_id, e)
        return None


def cancel_all_orders() -> dict | None:
    """Cancel all open orders."""
    client = _get_client()
    if client is None:
        return None
    try:
        resp = client.cancel_all()
        logger.info("[CLOB] Cancelled all orders")
        return resp
    except Exception as e:
        logger.error("[CLOB] Cancel all failed: %s", e)
        return None


def get_open_orders() -> list:
    """Get all open orders."""
    client = _get_client()
    if client is None:
        return []
    try:
        from py_clob_client.clob_types import OpenOrderParams

        orders = client.get_orders(OpenOrderParams())
        return orders if orders else []
    except Exception as e:
        logger.error("[CLOB] get_open_orders failed: %s", e)
        return []


def get_order_status(order_id: str) -> str | None:
    """
    Check the fill status of a specific CLOB order.

    Returns:
        "open"      - order exists and is unfilled
        "partial"   - order has some matched size but remains working
        "matched"   - order fully filled
        "cancelled" - order was cancelled
        None        - could not determine
    """
    client = _get_client()
    if client is None:
        return None
    try:
        detail = get_order_status_detail(order_id)
        return detail.get("status")
    except Exception as e:
        logger.warning("[CLOB] get_order_status(%s) failed: %s", order_id, e)
        return None


def get_order_status_detail(order_id: str) -> dict:
    """Best-effort status detail for a specific order."""
    client = _get_client()
    if client is None:
        return {"status": None, "matched_size": 0.0, "original_size": 0.0, "remaining_size": 0.0, "avg_price": None}
    try:
        if hasattr(client, "get_order"):
            order = client.get_order(order_id)
            if order:
                raw_status = str(order.get("status", "")).lower()
                original_size = _safe_float(order.get("original_size", order.get("size", order.get("initial_size", 0.0))))
                matched_size = _safe_float(order.get("size_matched", order.get("matched_size", order.get("filled_size", 0.0))))
                remaining_size = _safe_float(order.get("size_remaining", order.get("remaining_size", max(0.0, original_size - matched_size))))
                avg_price_raw = order.get("avg_price")
                avg_price = _safe_float(avg_price_raw, None) if avg_price_raw is not None else None

                if raw_status in ("matched", "filled"):
                    status = "matched"
                elif raw_status in ("cancelled", "canceled"):
                    status = "cancelled"
                elif matched_size > 0 and remaining_size > 0:
                    status = "partial"
                elif raw_status:
                    status = "open"
                else:
                    status = "unknown"

                return {
                    "status": status,
                    "matched_size": round(matched_size, 4),
                    "original_size": round(original_size, 4),
                    "remaining_size": round(remaining_size, 4),
                    "avg_price": avg_price,
                }

        open_orders = get_open_orders()
        open_map = {str(o.get("id") or o.get("orderID", "")): o for o in open_orders}
        open_order = open_map.get(str(order_id))
        if open_order:
            original_size = _safe_float(open_order.get("original_size", open_order.get("size", open_order.get("initial_size", 0.0))))
            matched_size = _safe_float(open_order.get("size_matched", open_order.get("matched_size", open_order.get("filled_size", 0.0))))
            remaining_size = _safe_float(open_order.get("size_remaining", open_order.get("remaining_size", max(0.0, original_size - matched_size))))
            status = "partial" if matched_size > 0 and remaining_size > 0 else "open"
            return {
                "status": status,
                "matched_size": round(matched_size, 4),
                "original_size": round(original_size, 4),
                "remaining_size": round(remaining_size, 4),
                "avg_price": None,
            }

        return {"status": "unknown", "matched_size": 0.0, "original_size": 0.0, "remaining_size": 0.0, "avg_price": None}
    except Exception as e:
        logger.warning("[CLOB] get_order_status_detail(%s) failed: %s", order_id, e)
        return {"status": None, "matched_size": 0.0, "original_size": 0.0, "remaining_size": 0.0, "avg_price": None}


def _normalize_trade(trade: dict) -> dict | None:
    if not isinstance(trade, dict):
        return None

    order_id = str(
        trade.get("order_id")
        or trade.get("orderID")
        or trade.get("maker_order_id")
        or trade.get("taker_order_id")
        or ""
    )
    token_id = str(trade.get("asset_id") or trade.get("token_id") or trade.get("tokenID") or "")
    side = str(trade.get("side") or trade.get("taker_side") or "").upper()
    size = _safe_float(trade.get("size", trade.get("matched_size", trade.get("amount", 0.0))))
    price = _safe_float(trade.get("price", trade.get("matched_price", trade.get("rate", 0.0))))
    trade_id = str(trade.get("id") or trade.get("tradeID") or trade.get("match_id") or "")
    ts = trade.get("created_at") or trade.get("timestamp") or trade.get("time") or trade.get("matched_at")
    if size <= 0 or price <= 0:
        return None
    return {
        "id": trade_id,
        "order_id": order_id,
        "token_id": token_id,
        "side": side,
        "size": round(size, 4),
        "price": round(price, 6),
        "timestamp": ts,
        "raw": trade,
    }


def get_trades(order_id: str | None = None, token_id: str | None = None) -> list:
    """Get recent trade history, optionally filtered and normalized."""
    client = _get_client()
    if client is None:
        return []
    try:
        trades = client.get_trades()
        if not trades:
            return []
        normalized = []
        for trade in trades:
            parsed = _normalize_trade(trade)
            if not parsed:
                continue
            if order_id and parsed["order_id"] != str(order_id):
                continue
            if token_id and parsed["token_id"] != str(token_id):
                continue
            normalized.append(parsed)
        return normalized
    except Exception as e:
        logger.error("[CLOB] get_trades failed: %s", e)
        return []


def get_order_book(token_id: str) -> dict | None:
    """Fetch order book summary for a token when the SDK exposes it."""
    client = _get_client()
    if client is None:
        return None
    try:
        if not hasattr(client, "get_order_book"):
            return None
        book = client.get_order_book(token_id)
        if not book:
            return None
        if isinstance(book, dict):
            return book
        payload = {}
        for attr in ("market", "bids", "asks", "asset_id", "token_id"):
            if hasattr(book, attr):
                payload[attr] = getattr(book, attr)
        return payload or None
    except Exception as e:
        logger.warning("[CLOB] get_order_book(%s) failed: %s", token_id, e)
        return None


def estimate_limit_price_from_book(token_id: str, side: str, size: float) -> dict | None:
    """
    Estimate executable price for a target size using visible book levels.

    Returns:
      price: worst visible level needed to fill `size`
      avg_price: average visible execution price
      filled_size: visible shares up to requested size
      coverage: visible fraction of requested size
      source: book | fallback_price
    """
    if size <= 0:
        return None

    client = _get_client()
    if client is None:
        return None

    book = get_order_book(token_id)
    if book:
        book_side = "asks" if str(side).upper() == "BUY" else "bids"
        levels = book.get(book_side)
        if isinstance(levels, list):
            remaining = float(size)
            filled = 0.0
            notional = 0.0
            worst = None
            for level in levels:
                if isinstance(level, dict):
                    price = _safe_float(level.get("price"))
                    level_size = _safe_float(level.get("size", level.get("quantity", level.get("amount", 0.0))))
                elif isinstance(level, (list, tuple)) and len(level) >= 2:
                    price = _safe_float(level[0])
                    level_size = _safe_float(level[1])
                else:
                    continue
                if price <= 0 or level_size <= 0:
                    continue
                take = min(level_size, remaining)
                notional += take * price
                filled += take
                remaining -= take
                worst = price
                if remaining <= 1e-9:
                    break
            if filled > 0:
                return {
                    "price": round(float(worst), 6),
                    "avg_price": round(notional / filled, 6),
                    "filled_size": round(filled, 4),
                    "coverage": round(min(1.0, filled / float(size)), 4),
                    "source": "book",
                }

    try:
        if hasattr(client, "get_price"):
            ref_price = _safe_float(client.get_price(token_id, side=str(side).upper()))
            if ref_price > 0:
                return {
                    "price": round(ref_price, 6),
                    "avg_price": round(ref_price, 6),
                    "filled_size": round(float(size), 4),
                    "coverage": 0.0,
                    "source": "fallback_price",
                }
    except Exception as e:
        logger.warning("[CLOB] get_price fallback failed for %s: %s", token_id, e)

    return None
