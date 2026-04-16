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
from config import settings


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

    v3.2: Overconfidence guard — probability for a single 1-2°F/C bucket
    cannot realistically exceed 60% even with good sigma. Cap at 65% to prevent
    edge inflation from degenerate cases (e.g. sigma very small).
    """
    # Main component (90% weight)
    p_main = _single_normal_prob(forecast, t_low, t_high, sigma)
    # Fat tail component (10% weight, wider distribution)
    p_tail = _single_normal_prob(forecast, t_low, t_high, sigma * 2.5)

    p = 0.90 * p_main + 0.10 * p_tail

    # Sanity cap: no single 1-degree bucket should ever have >65% probability
    # (implies sigma would need to be near-zero, which means model is lying)
    bucket_width = abs(t_high - t_low) if t_low != t_high and t_low != -999 and t_high != 999 else 10.0
    if bucket_width <= 2.0:
        p = min(p, 0.65)

    return p


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


def calc_ev_after_costs(p: float, entry_price: float, spread: float, slippage_frac: float = 0.005) -> float:
    """
    TRUE expected value after real execution costs.
    entry_price: actual ask we pay
    spread: bid-ask spread observed
    slippage_frac: additional slippage (default 0.5% for Polymarket)
    
    Real cost: (spread/2 + slippage)
    """
    if entry_price <= 0 or entry_price >= 1:
        return 0.0
    cost = (spread / 2.0) + slippage_frac
    effective_price = min(entry_price + cost, 0.99)
    return round(p * (1.0 - effective_price) - (1.0 - p) * effective_price, 4)


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
    return round(min(max(0.0, f) * settings.KELLY_FRACTION, 1.0), 4)


def bet_size(kelly: float, balance: float) -> float:
    """Dollar amount to bet, capped by MAX_BET."""
    raw = kelly * balance
    return round(min(raw, settings.MAX_BET), 2)


def in_bucket(forecast: float, t_low: float, t_high: float) -> bool:
    """Check if a forecast temperature falls within a bucket range."""
    if t_low == t_high:
        return round(float(forecast)) == round(t_low)
    return t_low <= float(forecast) <= t_high


def confidence_by_time(hours: float) -> float:
    """
    Confidence multiplier based on hours until event resolution.
    REDUCED from previous to avoid overconfidence bias.
    Returns value between 0.65 and 0.85:
      < 6h   → 0.85
      6-12h  → 0.82
      12-24h → 0.78
      24-48h → 0.72
      48-72h → 0.68
      > 72h  → 0.65
    """
    if hours <= 6:
        return 0.85
    elif hours <= 12:
        return 0.82
    elif hours <= 24:
        return 0.78
    elif hours <= 48:
        return 0.72
    elif hours <= 72:
        return 0.68
    else:
        return 0.65


def forecast_disagreement_sigma(forecasts: list[float], base_sigma: float, hours: float = 24.0) -> float:
    """
    Adjust sigma based on:
    1. Disagreement between forecast models (std dev of ensemble)
    2. Forecast horizon (farther out = more uncertain)

    sigma_adjusted = base_sigma * horizon_factor + std_dev(forecasts)
    """
    # Horizon scaling: uncertainty grows with time
    # <12h: 0.8x (METAR/obs dominates, very reliable)
    # 12-24h: 1.0x (baseline)
    # 24-48h: 1.3x (meaningful degradation)
    # >48h: 1.6x (significant uncertainty, near climatology)
    if hours < 12:
        horizon_factor = 0.8
    elif hours <= 24:
        horizon_factor = 1.0
    elif hours <= 48:
        horizon_factor = 1.3
    else:
        horizon_factor = 1.6

    scaled_sigma = base_sigma * horizon_factor

    if len(forecasts) < 2:
        return scaled_sigma

    mean = sum(forecasts) / len(forecasts)
    variance = sum((f - mean) ** 2 for f in forecasts) / len(forecasts)
    std_dev = math.sqrt(variance)

    return scaled_sigma + std_dev


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
