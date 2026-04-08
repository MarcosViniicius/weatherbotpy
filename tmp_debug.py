import sys, json
sys.path.insert(0,'.')
from datetime import datetime, timezone, timedelta
from config import settings
from config.locations import LOCATIONS
from connectors import polymarket_read as pm_read
from core.forecasts import take_forecast_snapshot
from core.calibration import get_sigma
from core.math_utils import bucket_prob, calc_ev, calc_edge, confidence_by_time, forecast_disagreement_sigma, calc_kelly, late_market_multiplier, bet_size, in_bucket

city_slug = list(LOCATIONS.keys())[0]
loc = LOCATIONS[city_slug]
dates = [(datetime.now(timezone.utc) + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]
snapshots = take_forecast_snapshot(city_slug, dates)

with open('tmp_out.txt', 'w', encoding='utf-8') as f:
    for i, date in enumerate(dates):
        dt = datetime.strptime(date, "%Y-%m-%d")
        event = pm_read.get_event(city_slug, settings.MONTHS[dt.month - 1], dt.day, dt.year)
        if not event: continue
        
        end_date = event.get("endDate", "")
        hours = pm_read.hours_to_resolution(end_date) if end_date else 0
        snap = snapshots.get(date, {})
        forecast_temp = snap.get("best")
        all_forecasts = snap.get("all_forecasts", [])
        
        f.write(f"\n[{date}] Forecast: {forecast_temp} (Sources: {all_forecasts}) | Hours left: {hours:.1f}\n")
        
        outcomes = []
        for market in event.get("markets", []):
            question = market.get("question", "")
            volume = float(market.get("volume", 0))
            rng = pm_read.parse_temp_range(question)
            if not rng: continue
            try:
                prices = json.loads(market.get("outcomePrices", "[0.5,0.5]"))
                bid, ask = float(prices[0]), float(prices[1]) if len(prices)>1 else float(prices[0])
            except: continue
            outcomes.append({"range": rng, "ask": ask, "volume": volume})
        
        matched = None
        for o in outcomes:
            if forecast_temp is not None and in_bucket(forecast_temp, o["range"][0], o["range"][1]):
                matched = o
                break
                
        if not matched:
            f.write("  [-] No matched bucket.\n")
            continue
            
        f.write(f"  [+] Matched bucket: {matched['range'][0]}-{matched['range'][1]} | Ask: ${matched['ask']} | Vol: ${matched['volume']}\n")
        
        if matched['volume'] < settings.MIN_VOLUME:
            f.write(f"  [-] Blocked: Volume ${matched['volume']} < Minimum ${settings.MIN_VOLUME}\n")
            continue
            
        base_sigma = get_sigma(city_slug, snap.get("best_source", "ecmwf"))
        sigma = forecast_disagreement_sigma(all_forecasts, base_sigma)
        conf = confidence_by_time(hours)
        
        p_raw = bucket_prob(forecast_temp, matched['range'][0], matched['range'][1], sigma)
        p = p_raw * conf
        edge = calc_edge(p, matched['ask'])
        ev = calc_ev(p, matched['ask'])
        
        f.write(f"  > p_raw: {p_raw:.4f} | conf: {conf:.2f} | p: {p:.4f}\n")
        f.write(f"  > edge: {edge:+.4f} (MIN: {settings.MIN_EDGE}) | ev: {ev:+.4f} (MIN: {settings.MIN_EV})\n")
        
        if edge < settings.MIN_EDGE:
            f.write(f"  [-] Blocked: Edge too low.\n")
        if ev < settings.MIN_EV:
            f.write(f"  [-] Blocked: EV too low.\n")
            
        if edge >= settings.MIN_EDGE and ev >= settings.MIN_EV:
            f.write(f"  [+] Signals PASSED.\n")
            kelly = calc_kelly(p, matched['ask'])
            lm_mult = late_market_multiplier(hours)
            k_adj = min(kelly * lm_mult, 0.25)
            size = bet_size(k_adj, 20.0)
            f.write(f"  > Kelly: {kelly:.4f} | Size: ${size:.2f}\n")
            if size < 0.50:
                f.write("  [-] Blocked: Size < $0.50\n")

print("Done")
