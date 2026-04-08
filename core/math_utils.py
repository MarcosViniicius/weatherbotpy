"""
core/math_utils.py — Pure math functions for probability, EV, Kelly sizing.
No side effects, no I/O.

v3.1 — Improved:
  - EV uses correct binary market formula: edge = p - price
  - Kelly uses correct binary payout structure
  - Time-based confidence adjustment
  - Gaussian mixture for bucket probability (fat tails)
"""

import math
from config.settings import KELLY_FRACTION, MAX_BET


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def bucket_prob(forecast: float, t_low: float, t_high: float, sigma: float = 2.0) -> float:
    """
    Probability that the actual temperature falls in [t_low, t_high].
    Uses a Gaussian mixture model (90% main + 10% fat tail with 2.5x sigma)
    to account for forecast busts and extreme events.
    """
    # Main component (90% weight)
    p_main = _single_normal_prob(forecast, t_low, t_high, sigma)
    # Fat tail component (10% weight, wider distribution)
    p_tail = _single_normal_prob(forecast, t_low, t_high, sigma * 2.5)

    return 0.90 * p_main + 0.10 * p_tail


def _single_normal_prob(forecast: float, t_low: float, t_high: float, sigma: float) -> float:
    """CDF-based probability for a single normal component."""
    if sigma <= 0:
        sigma = 0.5

    if t_low == -999:  # "X or below"
        return norm_cdf((t_high - forecast) / sigma)
    if t_high == 999:  # "X or above"
        return 1.0 - norm_cdf((t_low - forecast) / sigma)

    # Regular bucket — integrate the normal over [t_low, t_high]
    if t_low == t_high:
        # Single-degree bucket: integrate from t_low - 0.5 to t_low + 0.5
        z_low = (t_low - 0.5 - forecast) / sigma
        z_high = (t_low + 0.5 - forecast) / sigma
    else:
        z_low = (t_low - forecast) / sigma
        z_high = (t_high - forecast) / sigma

    return max(0.0, norm_cdf(z_high) - norm_cdf(z_low))


def calc_edge(p: float, price: float) -> float:
    """
    True edge in a binary market.
    edge = p - price
    Positive means we have an informational advantage.
    """
    if price <= 0 or price >= 1:
        return 0.0
    return round(p - price, 4)


def calc_ev(p: float, price: float) -> float:
    """
    Expected value per dollar risked in a binary market.
    EV = p × (1 - price) - (1 - p) × price
       = p - price  (simplified)
    This is equivalent to edge but kept for backwards compatibility.
    """
    if price <= 0 or price >= 1:
        return 0.0
    return round(p * (1.0 - price) - (1.0 - p) * price, 4)


def calc_kelly(p: float, price: float) -> float:
    """
    Fractional Kelly criterion for binary markets.
    Full Kelly: f* = (p/price - 1) × price / (1 - price)
              = (p - price) / (1 - price)
    We use KELLY_FRACTION (25%) of full Kelly for safety.
    """
    if price <= 0 or price >= 1 or p <= price:
        return 0.0
    # Full Kelly for binary outcome: (p - price) / (1 - price)
    f = (p - price) / (1.0 - price)
    return round(min(max(0.0, f) * KELLY_FRACTION, 1.0), 4)


def bet_size(kelly: float, balance: float) -> float:
    """Dollar amount to bet, capped by MAX_BET."""
    raw = kelly * balance
    return round(min(raw, MAX_BET), 2)


def in_bucket(forecast: float, t_low: float, t_high: float) -> bool:
    """Check if a forecast temperature falls within a bucket range."""
    if t_low == t_high:
        return round(float(forecast)) == round(t_low)
    return t_low <= float(forecast) <= t_high


def confidence_by_time(hours: float) -> float:
    """
    Confidence multiplier based on hours until event resolution.
    Forecasts get more reliable as we approach the event.

    Returns a value between 0.70 and 1.0:
      < 6h  → 1.00 (very reliable)
      6-12h → 0.97
      12-24h → 0.93
      24-48h → 0.85
      48-72h → 0.78
      > 72h  → 0.70
    """
    if hours <= 6:
        return 1.00
    elif hours <= 12:
        return 0.97
    elif hours <= 24:
        return 0.93
    elif hours <= 48:
        return 0.85
    elif hours <= 72:
        return 0.78
    else:
        return 0.70


def forecast_disagreement_sigma(forecasts: list[float], base_sigma: float) -> float:
    """
    Adjust sigma based on disagreement between forecast models.
    Higher disagreement → higher uncertainty → wider distribution.

    sigma_adjusted = base_sigma + std_dev(forecasts)
    """
    if len(forecasts) < 2:
        return base_sigma

    mean = sum(forecasts) / len(forecasts)
    variance = sum((f - mean) ** 2 for f in forecasts) / len(forecasts)
    std_dev = math.sqrt(variance)

    return base_sigma + std_dev


def late_market_multiplier(hours: float) -> float:
    """
    Aggressiveness multiplier for late-market inefficiency.
    Markets 6-18h before resolution often have the most edge
    because forecasts are recent but market hasn't fully adjusted.

    Returns Kelly multiplier (applied on top of KELLY_FRACTION):
      6-18h  → 1.5x (more aggressive)
      18-24h → 1.2x
      others → 1.0x (normal)
    """
    if 6 <= hours <= 18:
        return 1.5
    elif 18 < hours <= 24:
        return 1.2
    else:
        return 1.0
