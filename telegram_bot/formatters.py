"""
Format data into Telegram-friendly messages.
Uses MarkdownV2 for rich formatting.
"""

from datetime import datetime, timedelta, timezone


def escape_md(text) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!"
    result = []
    for ch in str(text):
        if ch in special:
            result.append(f"\\{ch}")
        else:
            result.append(ch)
    return "".join(result)


def _fmt_price(val: float) -> str:
    return escape_md(f"{val:.3f}")


def _fmt_pct(val: float) -> str:
    return escape_md(f"{val:.1f}")


def _fmt_money(val: float) -> str:
    return escape_md(f"{val:,.2f}")


def _fmt_shares(val: float) -> str:
    return escape_md(f"{val:.1f}")


def _fmt_bps(val: float) -> str:
    return escape_md(f"{val:.1f}")


def _position_side(pos: dict) -> str:
    return "NO" if str(pos.get("side") or "YES").upper() == "NO" else "YES"


def _quote_for_side(outcome: dict, side: str) -> tuple[float, float, float]:
    yes_bid = float(outcome.get("bid", outcome.get("price", 0.0)) or 0.0)
    yes_ask = float(outcome.get("ask", outcome.get("price", 0.0)) or 0.0)
    yes_mid = float(outcome.get("price", (yes_bid + yes_ask) / 2.0) or 0.0)
    if side == "NO":
        return (
            max(0.0, 1.0 - yes_ask),
            min(0.9999, 1.0 - yes_bid),
            max(0.0, min(0.9999, 1.0 - yes_mid)),
        )
    return yes_bid, yes_ask, yes_mid


def _market_position_snapshot(market: dict) -> dict:
    pos = market["position"]
    side = _position_side(pos)
    current_price = float(pos.get("entry_price", 0.0) or 0.0)
    for outcome in market.get("all_outcomes", []):
        if outcome.get("market_id") == pos.get("market_id"):
            bid, _ask, mid = _quote_for_side(outcome, side)
            current_price = bid if bid > 0 else mid
            break
    shares = float(pos.get("shares", 0.0) or 0.0)
    entry_price = float(pos.get("entry_price", 0.0) or 0.0)
    return {
        "side": side,
        "current_price": current_price,
        "unrealized": round((current_price - entry_price) * shares, 2),
        "entry_pending": pos.get("order_status") == "pending",
        "exit_pending": pos.get("exit_order_status") == "pending",
        "entry_partial": pos.get("order_status") == "partial",
        "exit_partial": pos.get("exit_order_status") == "partial",
    }


def _execution_metrics(pos: dict) -> dict:
    requested_shares = float(pos.get("requested_shares", pos.get("shares", 0.0)) or 0.0)
    filled_shares = float(pos.get("filled_shares", pos.get("shares", 0.0)) or 0.0)
    expected_fill = float(pos.get("expected_fill_price", pos.get("entry_price", 0.0)) or 0.0)
    avg_fill = float(pos.get("avg_entry_price", pos.get("entry_price", 0.0)) or 0.0)
    requested_cost = float(pos.get("requested_cost", pos.get("cost", 0.0)) or 0.0)
    net_ev = float(pos.get("net_ev", pos.get("ev", 0.0)) or 0.0)
    fill_rate = filled_shares / requested_shares if requested_shares > 0 else 1.0
    slippage_bps = ((avg_fill - expected_fill) / expected_fill * 10000) if expected_fill > 0 else 0.0
    return {
        "fill_rate": fill_rate,
        "slippage_bps": slippage_bps,
        "expected_ev_dollars": requested_cost * net_ev,
        "realized_pnl": float(pos.get("realized_pnl", pos.get("pnl", 0.0)) or 0.0),
    }


def _money_line(label: str, value: float) -> str:
    sign = "\\+" if value >= 0 else ""
    return f"{label}: {sign}{escape_md(f'{value:.2f}')}"


def _unit_symbol(market: dict) -> str:
    return "F" if market.get("unit") == "F" else "C"


def _bucket_label(pos: dict, market: dict) -> str:
    return f"{pos['bucket_low']}-{pos['bucket_high']}{_unit_symbol(market)}"


def _position_state_label(snap: dict) -> str:
    if snap["exit_partial"]:
        return "exit partial"
    if snap["entry_partial"]:
        return "entry partial"
    if snap["exit_pending"]:
        return "exit pending"
    if snap["entry_pending"]:
        return "entry pending"
    return "live"


def _position_health(market: dict, pos: dict, snap: dict) -> str:
    if snap["exit_pending"] or snap["exit_partial"]:
        return "exit in progress"
    if snap["entry_pending"] or snap["entry_partial"]:
        return "entry still filling"

    unrealized = snap["unrealized"]
    current_forecast = None
    snapshots = market.get("forecast_snapshots") or []
    if snapshots:
        current_forecast = snapshots[-1].get("best")
    entry_forecast = pos.get("forecast_at_entry", pos.get("forecast_temp"))

    if current_forecast is not None and entry_forecast is not None:
        drift = float(current_forecast) - float(entry_forecast)
        if abs(drift) >= 1.5:
            return "thesis moving favorably" if unrealized >= 0 else "forecast moved against entry"
        if abs(drift) >= 0.5:
            return "forecast changed, watch closely"

    if unrealized >= 0.5:
        return "position in profit"
    if unrealized <= -0.5:
        return "position under pressure"
    return "position stable"


def _close_reason_label(reason: str | None) -> str:
    mapping = {
        "forecast_shift_close": "forecast shifted",
        "stop_loss": "stop loss",
        "trailing_stop": "trailing stop",
        "manual_close": "manual close",
        "resolution": "market resolved",
    }
    if not reason:
        return "closed"
    return mapping.get(reason, reason.replace("_", " "))


def _closed_trade_kind(market: dict) -> str:
    if market.get("status") == "resolved":
        outcome = market.get("resolved_outcome")
        if outcome == "win":
            return "resolved win"
        if outcome == "loss":
            return "resolved loss"
        return "resolved"
    return _close_reason_label((market.get("position") or {}).get("close_reason"))


def _sort_by_pnl(markets: list[dict], reverse: bool = True) -> list[dict]:
    return sorted(markets, key=lambda m: float(m.get("pnl", 0.0) or 0.0), reverse=reverse)


def _fmt_signed_money_raw(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}"


def _escape_md_url(url: str) -> str:
    return str(url or "").replace("\\", "\\\\").replace(")", "\\)")


def _market_type_label(pos: dict, market: dict) -> str:
    question = str(pos.get("question") or "").strip()
    if question:
        return question
    side = _position_side(pos)
    return f"{side} on {_bucket_label(pos, market)}"


def _market_permalink(pos: dict, market: dict) -> str:
    return str(
        market.get("event_url")
        or pos.get("market_url")
        or market.get("market_url")
        or ""
    ).strip()


def _iso_day(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).date().isoformat()
    except Exception:
        return raw[:10]


def _hours_left(market: dict) -> float | None:
    end_date = str(market.get("event_end_date") or "").strip()
    if not end_date:
        return None
    try:
        end = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        return max(0.0, (end - datetime.now(timezone.utc)).total_seconds() / 3600.0)
    except Exception:
        return None


def _forecast_shift_active(pos: dict, market: dict) -> bool:
    entry_forecast = pos.get("forecast_at_entry")
    if entry_forecast in (None, ""):
        return False
    for snap_record in market.get("forecast_snapshots", []):
        best = snap_record.get("best")
        if best in (None, ""):
            continue
        try:
            if abs(float(best) - float(entry_forecast)) > 2.0:
                return True
        except Exception:
            continue
    return False


def _take_profit_target(entry_price: float, hours_left: float | None) -> float | None:
    if entry_price <= 0 or hours_left is None:
        return None
    if hours_left < 6:
        mult = 1.10
    elif hours_left < 24:
        mult = 1.25
    elif hours_left < 48:
        mult = 1.60
    else:
        mult = 2.00
    return round(entry_price * mult, 4)


def _default_stop_price(entry_price: float, hours_left: float | None) -> float:
    if entry_price <= 0:
        return 0.0
    if hours_left is None:
        hours_left = 24.0
    if hours_left > 48:
        stop_pct = 0.65
    elif hours_left > 24:
        stop_pct = 0.70
    elif hours_left > 12:
        stop_pct = 0.75
    else:
        stop_pct = 0.80
    abs_stop = entry_price - max(entry_price * (1 - stop_pct), 0.03)
    return round(max(entry_price * stop_pct, abs_stop), 4)


def _exit_transparency(pos: dict, market: dict, snap: dict) -> list[str]:
    entry = float(pos.get("entry_price", 0.0) or 0.0)
    current_price = float(snap.get("current_price", entry) or entry)
    hours_left = _hours_left(market)
    stop_price = float(pos.get("stop_price", _default_stop_price(entry, hours_left)) or _default_stop_price(entry, hours_left))
    take_profit = _take_profit_target(entry, hours_left)

    forecast_shift = _forecast_shift_active(pos, market)
    forecast_close_gate = forecast_shift and hours_left is not None and hours_left < 6
    take_ready = take_profit is not None and current_price >= take_profit
    stop_ready = current_price <= stop_price

    exit_status = str(pos.get("exit_order_status") or "idle")
    if exit_status in ("pending", "partial"):
        summary = f"SELL already queued ({exit_status})"
    elif take_ready:
        summary = "Take-profit trigger reached"
    elif stop_ready:
        summary = "Stop/trailing trigger reached"
    elif forecast_close_gate:
        summary = "Forecast-shift exit gate reached"
    else:
        summary = "No exit trigger yet"

    lines = [
        f"Exit status: {escape_md(summary)}",
        f"Open PnL only: {escape_md('yes' if exit_status not in ('pending', 'partial') else 'partially locked or pending')}",
        f"Stop: ${_fmt_price(stop_price)}" + (" | Trailing: active" if pos.get("trailing_activated") else " | Trailing: inactive"),
    ]

    if take_profit is not None:
        lines.append(f"Take-profit target: ${_fmt_price(take_profit)}")

    if hours_left is not None:
        lines.append(
            f"Forecast-shift exit: {'armed' if forecast_shift else 'not armed'} | activates only under 6h left | now {escape_md(f'{hours_left:.1f}')}h"
        )
    else:
        lines.append(f"Forecast-shift exit: {'armed' if forecast_shift else 'not armed'}")

    return lines


def format_status(state: dict, open_positions: list[dict], mode: str) -> str:
    cash = float(state.get("balance", 0.0) or 0.0)
    start = float(state.get("starting_balance", 0.0) or 0.0)
    wins = int(state.get("wins", 0) or 0)
    losses = int(state.get("losses", 0) or 0)
    total_resolved = wins + losses

    unrealized = 0.0
    pending_entries = 0
    pending_exits = 0
    partial_entries = 0
    partial_exits = 0
    for market in open_positions:
        snap = _market_position_snapshot(market)
        unrealized += snap["unrealized"]
        pending_entries += int(snap["entry_pending"])
        pending_exits += int(snap["exit_pending"])
        partial_entries += int(snap["entry_partial"])
        partial_exits += int(snap["exit_partial"])

    equity = cash + unrealized
    total_return_pct = ((equity - start) / start * 100.0) if start > 0 else 0.0
    mode_label = "PRODUCTION" if mode == "production" else "SIMULATION"

    lines = [
        "*WeatherBet Status*",
        "",
        f"Mode: {escape_md(mode_label)}",
        f"Equity: ${_fmt_money(equity)}",
        f"Cash: ${_fmt_money(cash)} | Unrealized: {'\\+' if unrealized >= 0 else ''}{escape_md(f'{unrealized:.2f}')}",
        f"Since start: {'\\+' if total_return_pct >= 0 else ''}{_fmt_pct(total_return_pct)}% on ${_fmt_money(start)}",
    ]

    if total_resolved > 0:
        win_rate = wins / total_resolved * 100.0
        lines.append(f"Resolved trades: {total_resolved} | W: {wins} | L: {losses} | WR: {_fmt_pct(win_rate)}%")
    else:
        lines.append("Resolved trades: none yet")

    lines.append(
        f"Open positions: {len(open_positions)} | Entry pending: {pending_entries} | Exit pending: {pending_exits}"
    )
    if partial_entries or partial_exits:
        lines.append(f"Partial fills: entry {partial_entries} | exit {partial_exits}")

    if open_positions:
        ranked = []
        for market in open_positions:
            snap = _market_position_snapshot(market)
            ranked.append((snap["unrealized"], market, snap))
        ranked.sort(key=lambda item: item[0], reverse=True)

        lines.extend(["", "*Open exposure*"])
        for _pnl, market, snap in ranked[:3]:
            pos = market["position"]
            current_price = snap["current_price"]
            current_forecast = None
            snapshots = market.get("forecast_snapshots") or []
            if snapshots:
                current_forecast = snapshots[-1].get("best")
            unrealized_text = escape_md(_fmt_signed_money_raw(float(snap["unrealized"])))
            forecast_text = (
                f"{escape_md(str(pos.get('forecast_at_entry', pos.get('forecast_temp', '?'))))} -> "
                f"{escape_md(str(current_forecast))}"
                if current_forecast is not None
                else escape_md(str(pos.get("forecast_at_entry", pos.get("forecast_temp", "?"))))
            )
            lines.append(
                f"- {escape_md(market['city_name'])} {escape_md(market['date'])} | {escape_md(snap['side'])} "
                f"{escape_md(_bucket_label(pos, market))}"
            )
            lines.append(
                f"  Entry ${_fmt_price(float(pos.get('entry_price', 0.0)))} -> now ${_fmt_price(current_price)} | "
                f"PnL {unrealized_text}"
            )
            lines.append(
                f"  Forecast: {forecast_text} | State: {escape_md(_position_health(market, pos, snap))}"
            )

    return "\n".join(lines)


def format_positions(markets: list[dict], state: dict | None = None) -> str:
    open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
    if not open_pos:
        return "*Posicoes abertas*\nNenhuma posicao aberta agora\\."

    total_cost = 0.0
    total_unrealized = 0.0
    daily_realized = 0.0
    cash_balance = float(state.get("balance", 0.0) or 0.0) if state is not None else 0.0
    today = datetime.now(timezone.utc).date().isoformat()

    for market in markets:
        pos = market.get("position")
        if not pos:
            continue
        if pos.get("status") == "closed" and _iso_day(pos.get("closed_at") or market.get("closed_at")) == today:
            daily_realized += float(pos.get("pnl", market.get("pnl", 0.0)) or 0.0)

    ranked = []
    for market in open_pos:
        snap = _market_position_snapshot(market)
        ranked.append((snap["unrealized"], market, snap))
    ranked.sort(key=lambda item: item[0], reverse=True)

    lines = ["*Posicoes abertas*", ""]
    for idx, item in enumerate(ranked, start=1):
        pnl, market, snap = item
        pos = market["position"]
        total_cost += float(pos.get("cost", 0.0) or 0.0)
        total_unrealized += pnl

        exec_metrics = _execution_metrics(pos)
        current_forecast = None
        snapshots = market.get("forecast_snapshots") or []
        if snapshots:
            current_forecast = snapshots[-1].get("best")

        cost = float(pos.get("cost", 0.0) or 0.0)
        market_value = float(snap.get("current_price", 0.0) or 0.0) * float(pos.get("shares", 0.0) or 0.0)
        pnl_pct = (pnl / cost * 100.0) if cost > 0 else 0.0
        status_label = "Saida pendente" if str(pos.get("exit_order_status") or "") in ("pending", "partial") else "Aberto"
        event_url = _market_permalink(pos, market)
        trade_label = _market_type_label(pos, market)
        hours_left = _hours_left(market)
        take_target = _take_profit_target(float(pos.get("entry_price", 0.0) or 0.0), hours_left or 0.0) if hours_left is not None else None
        stop_target = _default_stop_price(float(pos.get("entry_price", 0.0) or 0.0), hours_left)
        forecast_entry = pos.get("forecast_at_entry", pos.get("forecast_temp", "?"))
        forecast_now = current_forecast if current_forecast is not None else forecast_entry
        exit_summary = _exit_transparency(pos, market, snap)[0].replace("Exit status: ", "")
        edge_value = escape_md(f"{float(pos.get('edge', 0.0) or 0.0):+.2%}")
        kelly_value = escape_md(f"{float(pos.get('kelly', 0.0) or 0.0):.2%}")
        confidence_value = escape_md(f"{float(pos.get('confidence', 0.0) or 0.0):.0%}")
        fill_value = escape_md(f"{exec_metrics['fill_rate'] * 100:.0f}")
        ev_value = escape_md(f"{exec_metrics['expected_ev_dollars']:+.2f}")

        lines.append(f"{idx}\\. {escape_md(market['city_name'])} \\- {escape_md(trade_label)}")
        lines.append(f"├ {escape_md(snap['side'])} | {escape_md(_bucket_label(pos, market))} | {escape_md(str(pos.get('forecast_src', '?')).upper())}")
        lines.append(
            "├ Med/Atual: "
            + escape_md(f"{float(pos.get('entry_price', 0.0) or 0.0) * 100:.1f}")
            + "c -> "
            + escape_md(f"{float(snap.get('current_price', 0.0) or 0.0) * 100:.1f}")
            + "c"
        )
        lines.append(f"├ Custo/Valor: ${_fmt_money(cost)} -> ${_fmt_money(market_value)}")
        lines.append(f"├ PnL: {escape_md(_fmt_signed_money_raw(float(pnl)))} \\({escape_md(f'{pnl_pct:+.1f}%')}\\)")
        lines.append(f"├ Forecast: {escape_md(str(forecast_entry))} -> {escape_md(str(forecast_now))}")
        lines.append(f"├ Status: {escape_md(status_label)} | Exit: {escape_md(exit_summary)}")
        lines.append(f"├ TP/SL: {(('$' + _fmt_price(take_target)) if take_target is not None else 'off')} / ${_fmt_price(stop_target)}")
        lines.append(f"├ Edge/Kelly/Conf: {edge_value} | {kelly_value} | {confidence_value}")
        lines.append(f"├ Fill/Slip/EV: {fill_value}% | {_fmt_bps(exec_metrics['slippage_bps'])}bps | {ev_value}")
        lines.append(f"├ Leitura: {escape_md(_position_health(market, pos, snap))}")
        lines.append(f"└ Sell: `/close {escape_md(str(pos.get('market_id', '?')))}`" + (f" • [View]({_escape_md_url(event_url)})" if event_url else ""))
        lines.append("")

    lines.extend(
        [
            f"PnL diario: {escape_md(_fmt_signed_money_raw(daily_realized))}",
            f"Saldo disponivel: ${_fmt_money(cash_balance)}" if state is not None else "Saldo disponivel: n/d",
            f"Volume total: ${_fmt_money(total_cost)}",
            f"PnL aberto: {escape_md(_fmt_signed_money_raw(total_unrealized))}",
        ]
    )
    return "\n".join(lines).strip()


def format_report(markets: list[dict]) -> str:
    closed = [m for m in markets if m.get("pnl") is not None and (m.get("status") in {"closed", "resolved"} or (m.get("position") or {}).get("status") == "closed")]
    if not closed:
        return "*Trading Report*\nNo closed trades yet\\."

    resolved = [m for m in closed if m.get("status") == "resolved"]
    early_closed = [m for m in closed if m.get("status") != "resolved"]
    pnl_total = sum(float(m.get("pnl", 0.0) or 0.0) for m in closed)
    wins = [m for m in resolved if m.get("resolved_outcome") == "win"]
    losses = [m for m in resolved if m.get("resolved_outcome") == "loss"]
    positive_closed = [m for m in closed if float(m.get("pnl", 0.0) or 0.0) > 0]
    negative_closed = [m for m in closed if float(m.get("pnl", 0.0) or 0.0) < 0]

    lines = [
        "*Trading Report*",
        "",
        f"Closed trades: {len(closed)} | Resolved: {len(resolved)} | Closed early: {len(early_closed)}",
        f"Net PnL: {'\\+' if pnl_total >= 0 else ''}{escape_md(f'{pnl_total:.2f}')}",
        f"Positive closes: {len(positive_closed)} | Negative closes: {len(negative_closed)}",
    ]

    if resolved:
        win_rate = len(wins) / len(resolved) * 100.0
        lines.append(f"Resolved win rate: {_fmt_pct(win_rate)}%")

    best = _sort_by_pnl(closed, reverse=True)[:3]
    worst = _sort_by_pnl(closed, reverse=False)[:3]

    if best:
        lines.extend(["", "*Best closes*"])
        for market in best:
            pnl_text = escape_md(_fmt_signed_money_raw(float(market.get("pnl", 0.0) or 0.0)))
            lines.append(
                f"- {escape_md(market['city_name'])} {escape_md(market['date'])} | "
                f"{_closed_trade_kind(market)} | {pnl_text}"
            )

    if worst:
        lines.extend(["", "*Worst closes*"])
        for market in worst:
            pnl_text = escape_md(_fmt_signed_money_raw(float(market.get("pnl", 0.0) or 0.0)))
            lines.append(
                f"- {escape_md(market['city_name'])} {escape_md(market['date'])} | "
                f"{_closed_trade_kind(market)} | {pnl_text}"
            )

    by_city = {}
    for market in closed:
        city_name = market.get("city_name", market.get("city", "?"))
        group = by_city.setdefault(city_name, {"count": 0, "pnl": 0.0})
        group["count"] += 1
        group["pnl"] += float(market.get("pnl", 0.0) or 0.0)

    lines.extend(["", "*By city*"])
    for city_name, stats in sorted(by_city.items(), key=lambda item: item[1]["pnl"], reverse=True):
        pnl = stats["pnl"]
        lines.append(
            f"- {escape_md(city_name)} | trades: {stats['count']} | pnl: {escape_md(_fmt_signed_money_raw(pnl))}"
        )

    return "\n".join(lines)


def format_daily_report(markets: list[dict]) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_markets = [m for m in markets if m.get("date") == today]
    if not today_markets:
        return f"*Daily Report*\\nNo activity for {escape_md(today)}\\."

    opened = [m for m in today_markets if m.get("position")]
    open_now = [m for m in today_markets if m.get("position") and m["position"].get("status") == "open"]
    closed_today = [m for m in today_markets if m.get("pnl") is not None and (m.get("status") in {"closed", "resolved"} or (m.get("position") or {}).get("status") == "closed")]
    resolved_today = [m for m in today_markets if m.get("status") == "resolved" and m.get("pnl") is not None]

    realized_pnl = sum(float(m.get("pnl", 0.0) or 0.0) for m in closed_today)
    unrealized = 0.0
    for market in open_now:
        unrealized += _market_position_snapshot(market)["unrealized"]

    lines = [
        f"*Daily Report* | {escape_md(today)}",
        "",
        f"Tracked markets: {len(today_markets)} | Trades opened: {len(opened)}",
        f"Open now: {len(open_now)} | Closed today: {len(closed_today)} | Resolved today: {len(resolved_today)}",
        f"Realized PnL: {'\\+' if realized_pnl >= 0 else ''}{escape_md(f'{realized_pnl:.2f}')}",
        f"Open PnL: {'\\+' if unrealized >= 0 else ''}{escape_md(f'{unrealized:.2f}')}",
    ]

    if closed_today:
        best = _sort_by_pnl(closed_today, reverse=True)[0]
        worst = _sort_by_pnl(closed_today, reverse=False)[0]
        best_pnl_text = escape_md(_fmt_signed_money_raw(float(best.get("pnl", 0.0) or 0.0)))
        worst_pnl_text = escape_md(_fmt_signed_money_raw(float(worst.get("pnl", 0.0) or 0.0)))
        lines.extend(
            [
                "",
                "*Daily extremes*",
                f"Best: {escape_md(best['city_name'])} | {best_pnl_text} | {escape_md(_closed_trade_kind(best))}",
                f"Worst: {escape_md(worst['city_name'])} | {worst_pnl_text} | {escape_md(_closed_trade_kind(worst))}",
            ]
        )

    active_cities = sorted({m.get("city_name", m.get("city", "?")) for m in today_markets})
    if active_cities:
        lines.extend(["", f"Cities active: {escape_md(', '.join(active_cities[:6]))}"])

    return "\n".join(lines)


def format_weekly_report(markets: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    recent = [m for m in markets if m.get("date", "") >= start_date]
    if not recent:
        return "*Weekly Report*\nNo activity in the last 7 days\\."

    closed = [m for m in recent if m.get("pnl") is not None and (m.get("status") in {"closed", "resolved"} or (m.get("position") or {}).get("status") == "closed")]
    resolved = [m for m in closed if m.get("status") == "resolved"]
    open_now = [m for m in recent if m.get("position") and m["position"].get("status") == "open"]

    pnl_total = sum(float(m.get("pnl", 0.0) or 0.0) for m in closed)
    unrealized = sum(_market_position_snapshot(m)["unrealized"] for m in open_now)
    wins = len([m for m in resolved if m.get("resolved_outcome") == "win"])
    losses = len([m for m in resolved if m.get("resolved_outcome") == "loss"])

    lines = [
        "*Weekly Report*",
        "",
        f"Window start: {escape_md(start_date)}",
        f"Closed trades: {len(closed)} | Resolved: {len(resolved)} | Still open: {len(open_now)}",
        f"Realized PnL: {'\\+' if pnl_total >= 0 else ''}{escape_md(f'{pnl_total:.2f}')}",
        f"Open PnL: {'\\+' if unrealized >= 0 else ''}{escape_md(f'{unrealized:.2f}')}",
    ]

    if resolved:
        win_rate = (wins / len(resolved) * 100.0) if resolved else 0.0
        lines.append(f"Resolved W/L: {wins}/{losses} | WR: {_fmt_pct(win_rate)}%")

    if closed:
        best = _sort_by_pnl(closed, reverse=True)[0]
        worst = _sort_by_pnl(closed, reverse=False)[0]
        best_pnl_text = escape_md(_fmt_signed_money_raw(float(best.get("pnl", 0.0) or 0.0)))
        worst_pnl_text = escape_md(_fmt_signed_money_raw(float(worst.get("pnl", 0.0) or 0.0)))
        lines.extend(
            [
                "",
                "*Weekly extremes*",
                f"Best: {escape_md(best['city_name'])} {escape_md(best['date'])} | {best_pnl_text}",
                f"Worst: {escape_md(worst['city_name'])} {escape_md(worst['date'])} | {worst_pnl_text}",
            ]
        )

    return "\n".join(lines)


def format_markets_list(events_info: list[dict]) -> str:
    if not events_info:
        return "*Active Weather Markets*\nNo active weather markets found\\."

    lines = ["*Active Weather Markets*", ""]
    ranked = sorted(events_info, key=lambda info: float(info.get("top_price", 0.0) or 0.0), reverse=True)
    for info in ranked[:20]:
        top_price = float(info.get("top_price", 0.0) or 0.0)
        conviction = "high concentration" if top_price >= 0.65 else "balanced board" if top_price < 0.4 else "moderate concentration"
        lines.append(
            f"- {escape_md(info['city_name'])} | {escape_md(info['date'])}"
        )
        lines.append(
            f"  Buckets: {info['bucket_count']} | Leading bucket: {escape_md(str(info['top_bucket']))} @ ${_fmt_price(top_price)}"
        )
        lines.append(f"  Read: {escape_md(conviction)}")

    return "\n".join(lines)



