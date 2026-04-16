from core.math_utils import adaptive_bet_size, portfolio_concentration_multiplier


def test_adaptive_bet_size_respects_depth_and_concentration():
    size = adaptive_bet_size(
        kelly=0.2,
        balance=100.0,
        max_fraction_of_balance=0.12,
        max_fraction_of_depth=3.0,
        concentration_multiplier=0.76,
    )

    assert size == 1.52


def test_portfolio_concentration_multiplier_has_floor():
    assert portfolio_concentration_multiplier(0) == 1.0
    assert portfolio_concentration_multiplier(3) == 0.64
    assert portfolio_concentration_multiplier(8) == 0.4
