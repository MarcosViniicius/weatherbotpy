"""
core/calibration.py — Self-calibration of forecast accuracy.
Learns per-city sigma from resolved markets over time.

v3.1 — Added:
  - Calibration curve tracking (predicted p vs actual outcome)
  - Brier score computation
  - Per-bucket reliability analysis
"""

import json
import math
import logging
from datetime import datetime, timezone
from config import settings
from config.locations import LOCATIONS

logger = logging.getLogger("weatherbet.calibration")

_cal: dict = {}

# File to track predictions for calibration curve
PREDICTIONS_FILE = settings.DATA_DIR / "predictions_log.json"


def load_cal() -> dict:
    global _cal
    if settings.CALIBRATION_FILE.exists():
        try:
            _cal = json.loads(settings.CALIBRATION_FILE.read_text(encoding="utf-8"))
        except Exception:
            _cal = {}
    else:
        _cal = {}
    return _cal


def get_sigma(city_slug: str, source: str = "ecmwf") -> float:
    """Get calibrated sigma for a city+source. Falls back to city-type default."""
    key = f"{city_slug}_{source}"
    if key in _cal:
        return _cal[key]["sigma"]
    if "+" in source:
        parts = [part for part in source.split("+") if part]
        part_sigmas = [get_sigma(city_slug, part) for part in parts if f"{city_slug}_{part}" in _cal]
        if part_sigmas:
            return round(sum(part_sigmas) / len(part_sigmas), 3)

    # City-specific base sigmas: tropical/equatorial cities have much lower variance
    # than mid-latitude cities. Overrides global SIGMA_F/SIGMA_C defaults.
    CITY_SIGMA_OVERRIDES = {
        # Tropical/equatorial — very stable temperatures, ECMWF very accurate
        "singapore":    1.2,
        "lucknow":      1.5,
        "sao-paulo":    1.5,
        "miami":        2.5,  # Fahrenheit but tropical
        # Patagonia/Southern Hemisphere — high variability
        "buenos-aires": 3.0,
        "wellington":   3.5,
    }
    if city_slug in CITY_SIGMA_OVERRIDES:
        return CITY_SIGMA_OVERRIDES[city_slug]

    return settings.SIGMA_F if LOCATIONS[city_slug]["unit"] == "F" else settings.SIGMA_C


def log_prediction(city: str, date: str, p: float, edge: float, price: float,
                    source: str, sigma: float, confidence: float, spread: float = 0.0, ev_after_costs: float = 0.0):
    """Log a prediction for later calibration analysis (with execution cost tracking)."""
    try:
        if PREDICTIONS_FILE.exists():
            preds = json.loads(PREDICTIONS_FILE.read_text(encoding="utf-8"))
        else:
            preds = []

        preds.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "city": city,
            "date": date,
            "p": round(p, 4),
            "edge": round(edge, 4),
            "price": round(price, 4),
            "source": source,
            "sigma": round(sigma, 2),
            "confidence": round(confidence, 2),
            "spread": round(spread, 4),
            "ev_after_costs": round(ev_after_costs, 4),
            "outcome": None,  # filled when resolved
        })

        # Keep last 500 predictions
        if len(preds) > 500:
            preds = preds[-500:]

        PREDICTIONS_FILE.write_text(json.dumps(preds, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[CAL] Failed to log prediction: %s", e)


def record_outcome(city: str, date: str, won: bool):
    """Record the outcome of a resolved prediction."""
    try:
        if not PREDICTIONS_FILE.exists():
            return
        preds = json.loads(PREDICTIONS_FILE.read_text(encoding="utf-8"))
        updated = False
        for pred in reversed(preds):
            if pred["city"] == city and pred["date"] == date and pred["outcome"] is None:
                pred["outcome"] = 1 if won else 0
                pred["resolved_at"] = datetime.now(timezone.utc).isoformat()
                updated = True
                break
        if updated:
            PREDICTIONS_FILE.write_text(json.dumps(preds, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[CAL] Failed to record outcome: %s", e)


def compute_calibration_report() -> dict:
    """
    Compute calibration metrics from logged predictions.
    Returns: brier_score, hit_rate, calibration_curve, total_predictions.
    """
    try:
        if not PREDICTIONS_FILE.exists():
            return {"total": 0}
        preds = json.loads(PREDICTIONS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"total": 0}

    resolved = [p for p in preds if p.get("outcome") is not None]
    if not resolved:
        return {"total": 0}

    # Brier score: mean squared error of probability predictions
    brier_sum = sum((p["p"] - p["outcome"]) ** 2 for p in resolved)
    brier_score = brier_sum / len(resolved)

    # Long-only hit rate: every logged prediction is an executed YES/NO trade.
    # We do not count p < 0.5 as "correct" by directional symmetry because that
    # masks real losing trades in a long-only book.
    hit_rate = sum(1 for p in resolved if p["outcome"] == 1) / len(resolved)

    # Calibration curve: group predictions into bins
    bins = {}
    for p in resolved:
        # Round p to nearest 0.1 for binning
        bin_key = round(p["p"] * 10) / 10
        if bin_key not in bins:
            bins[bin_key] = {"predicted": [], "actual": []}
        bins[bin_key]["predicted"].append(p["p"])
        bins[bin_key]["actual"].append(p["outcome"])

    curve = {}
    for bin_key in sorted(bins):
        predicted_avg = sum(bins[bin_key]["predicted"]) / len(bins[bin_key]["predicted"])
        actual_avg = sum(bins[bin_key]["actual"]) / len(bins[bin_key]["actual"])
        n = len(bins[bin_key]["predicted"])
        curve[f"{bin_key:.1f}"] = {
            "predicted_avg": round(predicted_avg, 3),
            "actual_win_rate": round(actual_avg, 3),
            "n": n,
            "gap": round(abs(predicted_avg - actual_avg), 3),
        }

    return {
        "total": len(resolved),
        "brier_score": round(brier_score, 4),
        "hit_rate": round(hit_rate, 3),
        "calibration_curve": curve,
    }


def run_calibration(markets: list[dict]) -> dict:
    """Recalculate sigma from resolved markets and persist."""
    resolved = [
        m for m in markets
        if m.get("resolved") or (m.get("status") == "resolved" and m.get("actual_temp") is not None)
    ]
    cal = load_cal()
    updated = []

    sources = sorted({
        s.get("best_source")
        for m in resolved
        for s in m.get("forecast_snapshots", [])
        if s.get("best_source")
    } | {"ecmwf", "hrrr", "metar"})

    for source in sources:
        for city in set(m["city"] for m in resolved):
            group = [m for m in resolved if m["city"] == city]
            errors = []
            for m in group:
                snap = next(
                    (s for s in reversed(m.get("forecast_snapshots", []))
                     if s.get("best_source") == source),
                    None,
                )
                if snap and snap.get("best") is not None and m.get("actual_temp") is not None:
                    errors.append(abs(snap["best"] - m["actual_temp"]))
            if len(errors) < settings.CALIBRATION_MIN:
                continue
            mae = sum(errors) / len(errors)
            key = f"{city}_{source}"
            old = cal.get(key, {}).get(
                "sigma",
                settings.SIGMA_F if LOCATIONS.get(city, {}).get("unit") == "F" else settings.SIGMA_C,
            )
            new = round(mae, 3)
            cal[key] = {
                "sigma": new,
                "n": len(errors),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if abs(new - old) > 0.05:
                city_name = LOCATIONS.get(city, {}).get("name", city)
                updated.append(f"{city_name} {source}: {old:.2f}->{new:.2f}")

    settings.CALIBRATION_FILE.write_text(json.dumps(cal, indent=2), encoding="utf-8")
    if updated:
        logger.info("[CAL] %s", ", ".join(updated))
    return cal
