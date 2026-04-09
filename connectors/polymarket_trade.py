"""
connectors/polymarket_trade.py — Polymarket CLOB trading connector.
Uses py-clob-client for authenticated order placement.
Only called in production mode.
"""

import logging
import inspect
from config import settings
from connectors.resilience import retry_with_backoff, clob_cb

logger = logging.getLogger("weatherbet.polymarket_trade")

_client = None


def _get_client():
    """Lazy-initialise the CLOB client. Returns None if credentials are missing."""
    global _client
    if _client is not None:
        return _client

    pk = str(settings.POLYMARKET_PRIVATE_KEY or "").strip()
    if not pk or pk.lower().startswith("your-"):
        logger.error("[CLOB] No valid private key configured — cannot trade")
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
            "balance", "available", "total", "amount", "usdc", "usdc_balance",
            "collateral", "value", "asset_balance", "numericBalance", "numeric_balance",
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


# ═══════════════════════════════════════════════════════════
# ORDER PLACEMENT
# ═══════════════════════════════════════════════════════════

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
        price: Price per share (0.01 – 0.99).
        size: Number of shares.
        side: "BUY" or "SELL".

    Returns:
        API response dict or None on failure.
    """
    client = _get_client()
    if client is None:
        return None

    if not clob_cb.can_execute():
        logger.warning("[CLOB] Circuit open — skipping order")
        return None

    # Validate parameters
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
        logger.info("[CLOB] Order placed: %s %s shares @ $%.3f → %s", side, size, price, resp)
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
        logger.warning("[CLOB] Circuit open — skipping market order")
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
        logger.info("[CLOB] Market order: %s $%.2f → %s", side, amount, resp)
        return resp
    except Exception as e:
        clob_cb.record_failure()
        logger.error("[CLOB] Market order failed: %s", e)
        raise


# ═══════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ═══════════════════════════════════════════════════════════

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


def get_trades() -> list:
    """Get recent trade history."""
    client = _get_client()
    if client is None:
        return []
    try:
        trades = client.get_trades()
        return trades if trades else []
    except Exception as e:
        logger.error("[CLOB] get_trades failed: %s", e)
        return []
