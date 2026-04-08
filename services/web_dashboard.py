"""
services/web_dashboard.py — Lightweight HTTP server for the live dashboard.
Serves the dashboard HTML and a JSON API endpoint with bot data.
Runs alongside the Telegram bot in the same process.
"""

import json
import asyncio
import logging
import base64
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlsplit
from pathlib import Path
from threading import Thread
from datetime import datetime, timezone

from config import settings
import socket

logger = logging.getLogger("weatherbet.dashboard")

_server: HTTPServer | None = None
_thread: Thread | None = None
DASHBOARD_PORT = settings.DASHBOARD_PORT


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
    from core.state import load_state, load_all_markets
    from connectors.polymarket_read import hours_to_resolution
    from services.mode_manager import get_mode
    from core.calibration import compute_calibration_report

    state = load_state()
    markets = load_all_markets()
    mode = get_mode()

    # Open positions
    positions = {}
    for m in markets:
        pos = m.get("position")
        if not pos or pos.get("status") != "open":
            continue

        mid = pos.get("market_id", m["city"] + "_" + m["date"])
        unit_sym = "F" if m.get("unit") == "F" else "C"
        bl = pos.get("bucket_low", 0)
        bh = pos.get("bucket_high", 0)

        current_price = pos["entry_price"]
        for o in m.get("all_outcomes", []):
            if o["market_id"] == pos["market_id"]:
                current_price = o.get("bid", o["price"])
                break

        unrealized = round((current_price - pos["entry_price"]) * pos["shares"], 2)

        end_date = m.get("event_end_date", "")
        hrs = hours_to_resolution(end_date) if end_date else 0

        question = pos.get("question", f"{m.get('city_name', '')} {m['date']} — {bl}-{bh}{unit_sym}")

        spread = pos.get("spread", 0)
        ev_after_costs = pos.get("ev_after_costs", pos.get("ev", 0))
        
        positions[mid] = {
            "question": question,
            "location": m.get("city_name", m["city"]),
            "date": m["date"],
            "entry_price": pos["entry_price"],
            "current_price": current_price,
            "cost": pos["cost"],
            "shares": pos["shares"],
            "pnl": unrealized,
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
        }

    # Trade history (all entries + closed)
    trades = []
    for m in sorted(markets, key=lambda x: x.get("created_at", ""), reverse=True):
        pos = m.get("position")
        if not pos:
            continue

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
            "entry_price": pos["entry_price"],
            "cost": pos["cost"],
            "ev": pos.get("ev", 0),
            "edge": pos.get("edge", 0),
            "kelly_pct": pos.get("kelly", 0),
            "our_prob": pos.get("p", 0),
            "opened_at": pos.get("opened_at", ""),
        })

        # Exit trade (if closed)
        if pos.get("status") == "closed" and pos.get("pnl") is not None:
            trades.append({
                "type": "exit",
                "question": question,
                "location": m.get("city_name", m["city"]),
                "date": m["date"],
                "exit_price": pos.get("exit_price", 0),
                "pnl": pos["pnl"],
                "close_reason": pos.get("close_reason", ""),
                "closed_at": pos.get("closed_at", ""),
                "ev": pos.get("ev", 0),
                "edge": pos.get("edge", 0),
                "kelly_pct": pos.get("kelly", 0),
            })

    # Sort trades by time (newest first in JSON, dashboard reverses)
    trades.sort(key=lambda t: t.get("opened_at") or t.get("closed_at") or "")

    return {
        "balance": state["balance"],
        "starting_balance": state.get("starting_balance", settings.BALANCE),
        "total_trades": state.get("total_trades", 0),
        "wins": state.get("wins", 0),
        "losses": state.get("losses", 0),
        "peak_balance": state.get("peak_balance", state["balance"]),
        "positions": positions,
        "trades": trades,
        "calibration": compute_calibration_report(),
        "mode": mode,
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
        _server = HTTPServer(("0.0.0.0", port), DashboardHandler)
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
