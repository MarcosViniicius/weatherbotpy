from core.state import _normalize_market_record
from services.web_dashboard import _trade_outcome


def test_normalize_market_record_closes_non_resolved_market():
    market = {
        "status": "open",
        "pnl": None,
        "resolved_outcome": None,
        "position": {
            "status": "closed",
            "close_reason": "stop_loss",
            "pnl": -0.4,
        },
    }

    normalized, changed = _normalize_market_record(market)

    assert changed is True
    assert normalized["status"] == "closed"
    assert normalized["pnl"] == -0.4
    assert normalized["resolved_outcome"] is None


def test_normalize_market_record_marks_resolved_outcome():
    market = {
        "status": "open",
        "pnl": None,
        "resolved_outcome": None,
        "position": {
            "status": "closed",
            "close_reason": "resolved",
            "pnl": 0.75,
        },
    }

    normalized, changed = _normalize_market_record(market)

    assert changed is True
    assert normalized["status"] == "resolved"
    assert normalized["pnl"] == 0.75
    assert normalized["resolved_outcome"] == "win"


def test_trade_outcome_only_counts_resolved_trades():
    unresolved_market = {"status": "closed", "resolved_outcome": None}
    resolved_market = {"status": "resolved", "resolved_outcome": "loss"}
    pos = {"status": "closed", "close_reason": "forecast_shift_close", "pnl": -0.1}
    resolved_pos = {"status": "closed", "close_reason": "resolved", "pnl": -0.7}

    assert _trade_outcome(unresolved_market, pos) is None
    assert _trade_outcome(resolved_market, resolved_pos) == "loss"
