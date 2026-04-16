"""
services/web_dashboard.py — Lightweight HTTP server for the live dashboard.
Serves the dashboard HTML and a JSON API endpoint with bot data.
Runs alongside the Telegram bot in the same process.
"""

import json
import asyncio
import logging
import base64
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlsplit
from pathlib import Path
from threading import Thread
from datetime import datetime, timezone

from config import settings
import socket

logger = logging.getLogger("weatherbet.dashboard")

_server: ThreadingHTTPServer | None = None
_thread: Thread | None = None
DASHBOARD_PORT = settings.DASHBOARD_PORT


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _execution_metrics(pos: dict) -> dict:
    expected_entry = _safe_float(pos.get("expected_fill_price", pos.get("entry_price")))
    avg_entry = _safe_float(pos.get("avg_entry_price", pos.get("entry_price")))
    expected_exit = _safe_float(pos.get("expected_exit_price", pos.get("exit_price")))
    avg_exit = _safe_float(pos.get("avg_exit_price", pos.get("exit_price")))
    requested_shares = _safe_float(pos.get("requested_shares", pos.get("shares")))
    filled_shares = _safe_float(pos.get("filled_shares", pos.get("shares")))
    exit_requested_shares = _safe_float(pos.get("exit_requested_shares", requested_shares))
    exit_filled_shares = _safe_float(pos.get("exit_filled_shares", 0.0))
    expected_cost = _safe_float(pos.get("requested_cost", pos.get("cost")))
    realized_pnl = _safe_float(pos.get("realized_pnl", pos.get("pnl")))

    entry_slippage_bps = 0.0
    if expected_entry > 0:
        entry_slippage_bps = ((avg_entry - expected_entry) / expected_entry) * 10000

    exit_slippage_bps = 0.0
    if expected_exit > 0 and avg_exit > 0:
        exit_slippage_bps = ((expected_exit - avg_exit) / expected_exit) * 10000

    fill_rate = filled_shares / requested_shares if requested_shares > 0 else 1.0
    exit_fill_rate = exit_filled_shares / exit_requested_shares if exit_requested_shares > 0 else 0.0
    expected_ev_dollars = expected_cost * _safe_float(pos.get("net_ev", pos.get("ev")))
    realized_ev_pct = realized_pnl / expected_cost if expected_cost > 0 else 0.0

    return {
        "expected_entry_price": round(expected_entry, 4),
        "avg_entry_price": round(avg_entry, 4),
        "expected_exit_price": round(expected_exit, 4) if expected_exit > 0 else None,
        "avg_exit_price": round(avg_exit, 4) if avg_exit > 0 else None,
        "entry_slippage_bps": round(entry_slippage_bps, 1),
        "exit_slippage_bps": round(exit_slippage_bps, 1),
        "fill_rate": round(fill_rate, 4),
        "exit_fill_rate": round(exit_fill_rate, 4),
        "expected_ev_dollars": round(expected_ev_dollars, 2),
        "realized_ev_pct": round(realized_ev_pct, 4),
        "realized_pnl": round(realized_pnl, 2),
    }


def _trade_outcome(market: dict, pos: dict) -> str | None:
    if pos.get("status") != "closed":
        return None
    if market.get("status") == "resolved" or pos.get("close_reason") == "resolved":
        outcome = market.get("resolved_outcome")
        if outcome in ("win", "loss"):
            return outcome
        pnl_value = pos.get("pnl", market.get("pnl"))
        if pnl_value is None:
            return None
        return "win" if _safe_float(pnl_value) > 0 else "loss"
    return None


# ═══════════════════════════════════════════════════════════
# AUTHENTICATION
# ═══════════════════════════════════════════════════════════

def _is_auth_enabled() -> bool:
    """Check if authentication is enabled."""
    return settings.DASHBOARD_AUTH_ENABLED


def _check_auth(auth_header: str | None) -> bool:
    """
    Verify Basic Auth credentials.
    Returns True if valid or auth disabled, False if invalid.
    """
    if not _is_auth_enabled():
        return True
    
    if not auth_header or not auth_header.startswith("Basic "):
        return False
    
    try:
        # Extract and decode credentials
        encoded = auth_header[6:]  # Skip "Basic "
        decoded = base64.b64decode(encoded).decode('utf-8')
        username, password = decoded.split(':', 1)
        
        # Check against settings
        expected_user = settings.DASHBOARD_USERNAME
        expected_pass = settings.DASHBOARD_PASSWORD
        
        return username == expected_user and password == expected_pass
    except Exception:
        return False


def _build_api_data() -> dict:
    """Build the JSON payload that the dashboard fetches."""
    settings.reload_risk_config()
    from core.state import load_state, load_all_markets
    from connectors.polymarket_read import hours_to_resolution
    from services.mode_manager import get_mode
    from services.scheduler import get_scan_activity
    from core.calibration import compute_calibration_report

    state = load_state()
    markets = load_all_markets()
    mode = get_mode()

    def quote_for_side(outcome: dict, side: str) -> tuple[float, float]:
        bid = float(outcome.get("bid", outcome.get("price", 0.0)) or 0.0)
        ask = float(outcome.get("ask", outcome.get("price", 0.0)) or 0.0)
        if side == "NO":
            return max(0.0, 1.0 - ask), min(0.9999, 1.0 - bid)
        return bid, ask

    # Open positions
    positions = {}
    performance = {
        "entry_count": 0,
        "closed_count": 0,
        "resolved_count": 0,
        "wins": 0,
        "losses": 0,
        "open_cost_basis": 0.0,
        "open_market_value": 0.0,
        "unrealized_pnl": 0.0,
        "realized_pnl": 0.0,
        "cash_balance": _safe_float(state.get("balance"), 0.0),
        "equity_balance": 0.0,
        "total_pnl": 0.0,
        "return_pct": 0.0,
    }
    execution_rollup = {
        "entries": 0,
        "exits": 0,
        "avg_entry_slippage_bps": 0.0,
        "avg_exit_slippage_bps": 0.0,
        "avg_fill_rate": 0.0,
        "avg_exit_fill_rate": 0.0,
        "expected_ev_dollars": 0.0,
        "realized_pnl": 0.0,
    }
    for m in markets:
        pos = m.get("position")
        if not pos:
            continue

        performance["entry_count"] += 1

        exec_metrics = _execution_metrics(pos)
        execution_rollup["entries"] += 1
        execution_rollup["avg_entry_slippage_bps"] += exec_metrics["entry_slippage_bps"]
        execution_rollup["avg_fill_rate"] += exec_metrics["fill_rate"]
        execution_rollup["expected_ev_dollars"] += exec_metrics["expected_ev_dollars"]
        execution_rollup["realized_pnl"] += exec_metrics["realized_pnl"]
        if exec_metrics["exit_fill_rate"] > 0:
            execution_rollup["exits"] += 1
            execution_rollup["avg_exit_slippage_bps"] += exec_metrics["exit_slippage_bps"]
            execution_rollup["avg_exit_fill_rate"] += exec_metrics["exit_fill_rate"]

        if pos.get("status") != "open":
            performance["closed_count"] += 1
            performance["realized_pnl"] += _safe_float(pos.get("pnl"))
            outcome = _trade_outcome(m, pos)
            if outcome == "win":
                performance["wins"] += 1
                performance["resolved_count"] += 1
            elif outcome == "loss":
                performance["losses"] += 1
                performance["resolved_count"] += 1
            continue

        mid = pos.get("market_id", m["city"] + "_" + m["date"])
        unit_sym = "F" if m.get("unit") == "F" else "C"
        bl = pos.get("bucket_low", 0)
        bh = pos.get("bucket_high", 0)
        side = str(pos.get("side") or "YES").upper()

        current_price = pos["entry_price"]
        for o in m.get("all_outcomes", []):
            if o["market_id"] == pos["market_id"]:
                current_price, _ = quote_for_side(o, side)
                break

        unrealized = round((current_price - pos["entry_price"]) * pos["shares"], 2)
        market_value = round(current_price * pos["shares"], 2)
        performance["open_cost_basis"] += _safe_float(pos.get("cost"))
        performance["open_market_value"] += market_value
        performance["unrealized_pnl"] += unrealized

        end_date = m.get("event_end_date", "")
        hrs = hours_to_resolution(end_date) if end_date else 0

        question = pos.get("question", f"{m.get('city_name', '')} {m['date']} — {bl}-{bh}{unit_sym}")

        spread = pos.get("spread", 0)
        ev_after_costs = pos.get("ev_after_costs", pos.get("ev", 0))
        
        positions[mid] = {
            "question": question,
            "location": m.get("city_name", m["city"]),
            "date": m["date"],
            "status": pos.get("status", "open"),
            "side": side,
            "entry_price": pos["entry_price"],
            "current_price": current_price,
            "cost": pos["cost"],
            "shares": pos["shares"],
            "pnl": unrealized,
            "market_value": market_value,
            "ev": pos.get("ev", 0),
            "ev_after_costs": ev_after_costs,
            "edge": pos.get("edge", 0),
            "spread": spread,
            "kelly_pct": pos.get("kelly", 0),
            "our_prob": pos.get("p", 0),
            "forecast_temp": pos.get("forecast_temp"),
            "forecast_src": pos.get("forecast_src", ""),
            "hours_left": round(hrs, 1),
            "sigma": pos.get("sigma", 0),
            "confidence": pos.get("confidence", 1),
            "opened_at": pos.get("opened_at", ""),
            **exec_metrics,
        }

    # Trade history (all entries + closed)
    trades = []
    for m in sorted(markets, key=lambda x: x.get("created_at", ""), reverse=True):
        pos = m.get("position")
        if not pos:
            continue
        exec_metrics = _execution_metrics(pos)

        unit_sym = "F" if m.get("unit") == "F" else "C"
        bl = pos.get("bucket_low", 0)
        bh = pos.get("bucket_high", 0)
        question = pos.get("question", f"{m.get('city_name', '')} {m['date']} — {bl}-{bh}{unit_sym}")

        # Entry trade
        trades.append({
            "type": "entry",
            "question": question,
            "location": m.get("city_name", m["city"]),
            "date": m["date"],
            "side": str(pos.get("side") or "YES").upper(),
            "entry_price": pos["entry_price"],
            "cost": pos["cost"],
            "ev": pos.get("ev", 0),
            "edge": pos.get("edge", 0),
            "kelly_pct": pos.get("kelly", 0),
            "our_prob": pos.get("p", 0),
            "opened_at": pos.get("opened_at", ""),
            **exec_metrics,
        })

        # Exit trade (if closed)
        if pos.get("status") == "closed" and pos.get("pnl") is not None:
            trades.append({
                "type": "exit",
                "question": question,
                "location": m.get("city_name", m["city"]),
                "date": m["date"],
                "side": str(pos.get("side") or "YES").upper(),
                "exit_price": pos.get("exit_price", 0),
                "pnl": pos["pnl"],
                "close_reason": pos.get("close_reason", ""),
                "closed_at": pos.get("closed_at", ""),
                "outcome": _trade_outcome(m, pos),
                "ev": pos.get("ev", 0),
                "edge": pos.get("edge", 0),
                "kelly_pct": pos.get("kelly", 0),
                **exec_metrics,
            })

    # Sort trades by time (newest first in JSON, dashboard reverses)
    trades.sort(key=lambda t: t.get("opened_at") or t.get("closed_at") or "")

    if execution_rollup["entries"] > 0:
        execution_rollup["avg_entry_slippage_bps"] = round(
            execution_rollup["avg_entry_slippage_bps"] / execution_rollup["entries"], 1
        )
        execution_rollup["avg_fill_rate"] = round(
            execution_rollup["avg_fill_rate"] / execution_rollup["entries"], 4
        )
    if execution_rollup["exits"] > 0:
        execution_rollup["avg_exit_slippage_bps"] = round(
            execution_rollup["avg_exit_slippage_bps"] / execution_rollup["exits"], 1
        )
        execution_rollup["avg_exit_fill_rate"] = round(
            execution_rollup["avg_exit_fill_rate"] / execution_rollup["exits"], 4
        )
    execution_rollup["expected_ev_dollars"] = round(execution_rollup["expected_ev_dollars"], 2)
    execution_rollup["realized_pnl"] = round(execution_rollup["realized_pnl"], 2)
    performance["open_cost_basis"] = round(performance["open_cost_basis"], 2)
    performance["open_market_value"] = round(performance["open_market_value"], 2)
    performance["unrealized_pnl"] = round(performance["unrealized_pnl"], 2)
    performance["realized_pnl"] = round(performance["realized_pnl"], 2)
    performance["equity_balance"] = round(
        performance["cash_balance"] + performance["open_market_value"], 2
    )
    starting_balance = _safe_float(state.get("starting_balance"), settings.BALANCE)
    performance["total_pnl"] = round(performance["equity_balance"] - starting_balance, 2)
    performance["return_pct"] = round(
        (performance["total_pnl"] / starting_balance * 100) if starting_balance > 0 else 0.0,
        2,
    )

    return {
        "balance": state["balance"],
        "starting_balance": starting_balance,
        "total_trades": performance["entry_count"],
        "wins": performance["wins"],
        "losses": performance["losses"],
        "peak_balance": state.get("peak_balance", state["balance"]),
        "positions": positions,
        "trades": trades,
        "performance": performance,
        "calibration": compute_calibration_report(),
        "execution": execution_rollup,
        "mode": mode,
        "scan_activity": get_scan_activity(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


class DashboardHandler(SimpleHTTPRequestHandler):
    """Custom handler that serves the dashboard and API."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(settings.PROJECT_ROOT), **kwargs)

    def _send_auth_required(self):
        """Send 401 Unauthorized response."""
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="WeatherBot Dashboard"')
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Authentication required")

    def do_GET(self):
        request_path = urlsplit(self.path).path

        # Login endpoint (no auth required)
        if request_path.startswith("/api/login"):
            try:
                auth_header = self.headers.get("Authorization")
                if not _check_auth(auth_header):
                    return self._send_auth_required()
                
                # Return success
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok"}).encode())
            except Exception as e:
                logger.error("[DASH] Login error: %s", e)
                self.send_response(500)
                self.end_headers()
            return

        # API endpoint
        if request_path.startswith("/api/data"):
            try:
                # Check authentication
                auth_header = self.headers.get("Authorization")
                if not _check_auth(auth_header):
                    return self._send_auth_required()
                
                data = _build_api_data()
                payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(payload)
            except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
                # Client disconnected before the full response was sent
                pass
            except Exception as e:
                logger.error("[DASH] API error: %s", e)
                try:
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": str(e)}).encode())
                except:
                    pass
            return

        # Protect dashboard routes and any HTML file route.
        # Using urlsplit(path).path avoids bypass with query strings such as /dashboard.html?v=1
        if request_path in ("/", "/dashboard", "/dashboard.html", "/index.html") or request_path.endswith(".html"):
            # Check authentication for dashboard page
            auth_header = self.headers.get("Authorization")
            if not _check_auth(auth_header):
                return self._send_auth_required()

            if request_path in ("/", "/dashboard", "/index.html"):
                self.path = "/dashboard.html"

        # Serve static files normally
        super().do_GET()

    def log_message(self, format, *args):
        # Suppress noisy access logs
        pass


def start_dashboard(port: int = DASHBOARD_PORT):
    """Start the dashboard HTTP server in a background thread."""
    global _server, _thread

    try:
        ThreadingHTTPServer.allow_reuse_address = True
        _server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
        _thread = Thread(target=_server.serve_forever, daemon=True)
        _thread.start()
        
        # Determine public URL
        public_url = settings.DASHBOARD_PUBLIC_URL.strip() if settings.DASHBOARD_PUBLIC_URL else ''
        if public_url:
            # User specified a public URL/IP for VPS access
            logger.info("[DASH] Dashboard running at http://%s:%d (local: http://localhost:%d)", public_url, port, port)
        else:
            # Local only
            logger.info("[DASH] Dashboard running at http://localhost:%d", port)
    except OSError as e:
        logger.warning("[DASH] Could not start dashboard: %s", e)


def stop_dashboard():
    global _server
    if _server:
        _server.shutdown()
        logger.info("[DASH] Dashboard stopped")
