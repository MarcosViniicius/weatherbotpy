"""
core/forecasts.py — Weather forecast fetchers (ECMWF, HRRR, METAR, Visual Crossing).
All HTTP calls use retry_with_backoff and circuit breakers.
"""

import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from config.locations import LOCATIONS, TIMEZONES
from config import settings
from config.settings import VC_KEY
from connectors.resilience import retry_with_backoff, openmeteo_cb, metar_cb, get_http_session

logger = logging.getLogger("weatherbet.forecasts")

# ═══════════════════════════════════════════════════════════
# IN-MEMORY FORECAST CACHE
# ═══════════════════════════════════════════════════════════
_CACHE_TTL = 300  # 5 minutes in seconds
_forecast_cache: dict[str, tuple[float, dict]] = {}  # key -> (timestamp, data)


def _cache_get(key: str) -> dict | None:
    """Return cached value if within TTL, else None."""
    entry = _forecast_cache.get(key)
    if entry and (time.monotonic() - entry[0]) < _CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: str, value: dict) -> None:
    _forecast_cache[key] = (time.monotonic(), value)


def clear_forecast_cache():
    """Force clear (used in tests or after config changes)."""
    _forecast_cache.clear()

_weather_session = get_http_session("weather")
_metar_session = get_http_session("metar")
_vc_session = get_http_session("visual_crossing")


# ═══════════════════════════════════════════════════════════
# ECMWF (global, all cities)
# ═══════════════════════════════════════════════════════════

@retry_with_backoff(max_retries=3, base_delay=1.0)
def get_ecmwf(city_slug: str, dates: list[str]) -> dict[str, float]:
    """ECMWF via Open-Meteo with bias correction."""
    if not openmeteo_cb.can_execute():
        return {}

    loc = LOCATIONS[city_slug]
    unit = loc["unit"]
    temp_unit = "fahrenheit" if unit == "F" else "celsius"
    tz = TIMEZONES.get(city_slug, "UTC")
    result = {}

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc['lat']}&longitude={loc['lon']}"
        f"&daily=temperature_2m_max&temperature_unit={temp_unit}"
        f"&forecast_days=7&timezone={tz}"
        f"&models=ecmwf_ifs025&bias_correction=true"
    )

    try:
        response = _weather_session.get(
            url,
            timeout=(settings.WEATHER_TIMEOUT, settings.WEATHER_TIMEOUT + 5),
        )
        if response.status_code != 200:
            openmeteo_cb.record_failure()
            return {}
        
        try:
            data = response.json()
        except Exception:
            openmeteo_cb.record_failure()
            return {}

        if "error" not in data:
            for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
                if date in dates and temp is not None:
                    result[date] = round(temp, 1) if unit == "C" else round(temp)
        openmeteo_cb.record_success()
    except Exception as e:
        openmeteo_cb.record_failure()
        logger.warning("[ECMWF] Failed to fetch: %s", e)
        return {}

    return result


# ═══════════════════════════════════════════════════════════
# HRRR / GFS (US cities only, up to 48h)
# ═══════════════════════════════════════════════════════════

@retry_with_backoff(max_retries=3, base_delay=1.0)
def get_hrrr(city_slug: str, dates: list[str]) -> dict[str, float]:
    """HRRR+GFS seamless via Open-Meteo. US cities only."""
    loc = LOCATIONS[city_slug]
    if loc["region"] != "us":
        return {}

    if not openmeteo_cb.can_execute():
        return {}

    tz = TIMEZONES.get(city_slug, "UTC")
    result = {}

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc['lat']}&longitude={loc['lon']}"
        f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
        f"&forecast_days=3&timezone={tz}"
        f"&models=gfs_seamless"
    )

    try:
        response = _weather_session.get(
            url,
            timeout=(settings.WEATHER_TIMEOUT, settings.WEATHER_TIMEOUT + 5),
        )
        if response.status_code != 200:
            openmeteo_cb.record_failure()
            return {}

        try:
            data = response.json()
        except Exception:
            openmeteo_cb.record_failure()
            return {}

        if "error" not in data:
            for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
                if date in dates and temp is not None:
                    result[date] = round(temp)
        openmeteo_cb.record_success()
    except Exception as e:
        openmeteo_cb.record_failure()
        logger.warning("[HRRR] Failed to fetch: %s", e)
        return {}

    return result


# ═══════════════════════════════════════════════════════════
# METAR (real-time observation, D+0 only)
# ═══════════════════════════════════════════════════════════

@retry_with_backoff(max_retries=2, base_delay=0.5)
def get_metar(city_slug: str) -> float | None:
    """Current observed temperature from METAR airport station."""
    if not metar_cb.can_execute():
        return None

    loc = LOCATIONS[city_slug]
    station = loc["station"]
    unit = loc["unit"]

    try:
        url = f"https://aviationweather.gov/api/data/metar?ids={station}&format=json"
        response = _metar_session.get(
            url,
            timeout=(settings.WEATHER_TIMEOUT, settings.WEATHER_TIMEOUT + 5),
        )
        if response.status_code != 200:
            metar_cb.record_failure()
            return None
            
        try:
            data = response.json()
        except Exception:
            # Not a JSON response (maybe empty or HTML error)
            metar_cb.record_failure()
            return None

        if data and isinstance(data, list):
            temp_c = data[0].get("temp")
            if temp_c is not None:
                metar_cb.record_success()
                if unit == "F":
                    return round(float(temp_c) * 9 / 5 + 32)
                return round(float(temp_c), 1)
    except Exception as e:
        metar_cb.record_failure()
        logger.warning("[METAR] Failed to fetch %s: %s", station, e)
        return None

    return None


# ═══════════════════════════════════════════════════════════
# ACTUAL TEMPERATURE (post-resolution via Visual Crossing)
# ═══════════════════════════════════════════════════════════

@retry_with_backoff(max_retries=2, base_delay=1.0)
def get_actual_temp(city_slug: str, date_str: str) -> float | None:
    """Fetch actual max temperature for a past date."""
    if not VC_KEY:
        return None

    loc = LOCATIONS[city_slug]
    station = loc["station"]
    unit = loc["unit"]
    vc_unit = "us" if unit == "F" else "metric"

    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
        f"/{station}/{date_str}/{date_str}"
        f"?unitGroup={vc_unit}&key={VC_KEY}&include=days&elements=tempmax"
    )

    try:
        data = _vc_session.get(
            url,
            timeout=(settings.WEATHER_TIMEOUT, settings.WEATHER_TIMEOUT + 5),
        ).json()
        days = data.get("days", [])
        if days and days[0].get("tempmax") is not None:
            return round(float(days[0]["tempmax"]), 1)
    except Exception as e:
        logger.error("[VC] %s %s: %s", city_slug, date_str, e)
        raise

    return None


# ═══════════════════════════════════════════════════════════
# COMPOSITE SNAPSHOT
# ═══════════════════════════════════════════════════════════

def take_forecast_snapshot(city_slug: str, dates: list[str]) -> dict:
    """
    Fetch forecasts from all sources and return a snapshot per date.
    v3.2: In-memory cache (5 min TTL), METAR primary for D+0,
    ensemble weighted average for best estimate.
    """
    now_str = datetime.now(timezone.utc).isoformat()

    # Cache key includes city + first date of window
    cache_key = f"{city_slug}_{dates[0] if dates else 'none'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        logger.debug("[CACHE] Hit for %s", cache_key)
        return cached

    try:
        ecmwf = get_ecmwf(city_slug, dates)
    except Exception:
        ecmwf = {}

    try:
        hrrr = get_hrrr(city_slug, dates)
    except Exception:
        hrrr = {}

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    two_days = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")
    loc = LOCATIONS[city_slug]

    # METAR: fetch once for today (real observation)
    metar_temp = None
    try:
        metar_temp = get_metar(city_slug)
    except Exception:
        pass

    snapshots = {}
    for date in dates:
        snap = {
            "ts":    now_str,
            "ecmwf": ecmwf.get(date),
            "hrrr":  hrrr.get(date) if date <= two_days else None,
            "metar": metar_temp if date == today else None,
        }

        # ── Ensemble weighted average ──────────────────────────────
        # Weights based on proven skill at each horizon:
        #   D+0: METAR=0.7 HRRR=0.2 ECMWF=0.1  (obs > NWP)
        #   D+1: HRRR=0.6  ECMWF=0.4            (short-range NWP)
        #   D+2: HRRR=0.5  ECMWF=0.5            (equal skill at 2 days)
        #   D+3: ECMWF=1.0                       (HRRR unreliable >48h)
        all_forecasts = []
        weighted_sum = 0.0
        weight_total = 0.0

        if date == today:
            if snap["metar"] is not None:
                all_forecasts.append(snap["metar"])
                weighted_sum += snap["metar"] * 0.70
                weight_total += 0.70
            if snap["hrrr"] is not None:
                all_forecasts.append(snap["hrrr"])
                weighted_sum += snap["hrrr"] * 0.20
                weight_total += 0.20
            if snap["ecmwf"] is not None:
                all_forecasts.append(snap["ecmwf"])
                weighted_sum += snap["ecmwf"] * 0.10
                weight_total += 0.10
        elif date == tomorrow:
            if snap["hrrr"] is not None:
                all_forecasts.append(snap["hrrr"])
                weighted_sum += snap["hrrr"] * 0.60
                weight_total += 0.60
            if snap["ecmwf"] is not None:
                all_forecasts.append(snap["ecmwf"])
                weighted_sum += snap["ecmwf"] * 0.40
                weight_total += 0.40
        elif date <= two_days:
            if snap["hrrr"] is not None:
                all_forecasts.append(snap["hrrr"])
                weighted_sum += snap["hrrr"] * 0.50
                weight_total += 0.50
            if snap["ecmwf"] is not None:
                all_forecasts.append(snap["ecmwf"])
                weighted_sum += snap["ecmwf"] * 0.50
                weight_total += 0.50
        else:  # D+3 and beyond: ECMWF only
            if snap["ecmwf"] is not None:
                all_forecasts.append(snap["ecmwf"])
                weighted_sum += snap["ecmwf"] * 1.00
                weight_total += 1.00

        snap["all_forecasts"] = all_forecasts

        # Best estimate: weighted ensemble average when multiple sources available
        if weight_total > 0:
            snap["best"] = round(weighted_sum / weight_total, 1)
            # Determine primary source label for logging
            if date == today and snap["metar"] is not None:
                snap["best_source"] = "metar"
            elif snap["hrrr"] is not None and date <= two_days:
                snap["best_source"] = "hrrr+ecmwf" if snap["ecmwf"] is not None else "hrrr"
            else:
                snap["best_source"] = "ecmwf"
        else:
            snap["best"] = None
            snap["best_source"] = None

        snapshots[date] = snap

    _cache_set(cache_key, snapshots)
    return snapshots

