from services.web_dashboard import _execution_metrics


def test_execution_metrics_tracks_entry_and_exit_slippage():
    pos = {
        "entry_price": 0.41,
        "expected_fill_price": 0.40,
        "avg_entry_price": 0.41,
        "requested_shares": 10.0,
        "filled_shares": 8.0,
        "expected_exit_price": 0.62,
        "avg_exit_price": 0.60,
        "exit_requested_shares": 8.0,
        "exit_filled_shares": 4.0,
        "requested_cost": 4.0,
        "net_ev": 0.12,
        "realized_pnl": 0.75,
    }

    metrics = _execution_metrics(pos)

    assert metrics["fill_rate"] == 0.8
    assert metrics["exit_fill_rate"] == 0.5
    assert metrics["expected_ev_dollars"] == 0.48
    assert metrics["realized_pnl"] == 0.75
    assert metrics["entry_slippage_bps"] == 250.0
    assert metrics["exit_slippage_bps"] == 322.6


def test_execution_metrics_defaults_to_live_position_fields():
    pos = {
        "entry_price": 0.33,
        "shares": 6.0,
        "cost": 1.98,
        "ev": 0.08,
        "pnl": 0.22,
    }

    metrics = _execution_metrics(pos)

    assert metrics["fill_rate"] == 1.0
    assert metrics["exit_fill_rate"] == 0.0
    assert metrics["entry_slippage_bps"] == 0.0
    assert metrics["expected_ev_dollars"] == 0.16
