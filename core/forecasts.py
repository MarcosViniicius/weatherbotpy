"""
core/forecasts.py — Weather forecast fetchers (ECMWF, HRRR, METAR, Visual Crossing).
All HTTP calls use retry_with_backoff and circuit breakers.
"""

import logging
import requests
from datetime import datetime, timezone, timedelta
from config.locations import LOCATIONS, TIMEZONES
from config import settings
from config.settings import VC_KEY
from connectors.resilience import retry_with_backoff, openmeteo_cb, metar_cb, get_http_session

logger = logging.getLogger("weatherbet.forecasts")

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
    """Fetch forecasts from all sources and return a snapshot per date."""
    now_str = datetime.now(timezone.utc).isoformat()

    try:
        ecmwf = get_ecmwf(city_slug, dates)
    except Exception:
        ecmwf = {}

    try:
        hrrr = get_hrrr(city_slug, dates)
    except Exception:
        hrrr = {}

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    two_days = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")
    loc = LOCATIONS[city_slug]

    snapshots = {}
    for date in dates:
        metar_temp = None
        if date == today:
            try:
                metar_temp = get_metar(city_slug)
            except Exception:
                pass

        snap = {
            "ts":    now_str,
            "ecmwf": ecmwf.get(date),
            "hrrr":  hrrr.get(date) if date <= two_days else None,
            "metar": metar_temp,
        }

        # Collect all available forecasts for disagreement analysis
        all_forecasts = []
        if snap["ecmwf"] is not None:
            all_forecasts.append(snap["ecmwf"])
        if snap["hrrr"] is not None:
            all_forecasts.append(snap["hrrr"])
        if snap["metar"] is not None:
            all_forecasts.append(snap["metar"])
        snap["all_forecasts"] = all_forecasts

        # Best forecast: HRRR for US D+0/D+1, otherwise ECMWF
        if loc["region"] == "us" and snap["hrrr"] is not None:
            snap["best"] = snap["hrrr"]
            snap["best_source"] = "hrrr"
        elif snap["ecmwf"] is not None:
            snap["best"] = snap["ecmwf"]
            snap["best_source"] = "ecmwf"
        else:
            snap["best"] = None
            snap["best_source"] = None

        snapshots[date] = snap

    return snapshots

