"""
telegram_bot/formatters.py — Format data into Telegram-friendly messages.
Uses MarkdownV2 for rich formatting.
"""

from datetime import datetime, timezone


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


def _fmt_price(val):
    return escape_md(f"{val:.3f}")


def _fmt_pct(val):
    return escape_md(f"{val:.1f}")


def _fmt_money(val):
    return escape_md(f"{val:,.2f}")


def _fmt_shares(val):
    return escape_md(f"{val:.1f}")


def _fmt_bps(val):
    return escape_md(f"{val:.1f}")


def _position_side(pos: dict) -> str:
    return "NO" if str(pos.get("side") or "YES").upper() == "NO" else "YES"


def _quote_for_side(outcome: dict, side: str) -> tuple[float, float, float]:
    yes_bid = float(outcome.get("bid", outcome.get("price", 0.0)) or 0.0)
    yes_ask = float(outcome.get("ask", outcome.get("price", 0.0)) or 0.0)
    yes_mid = float(outcome.get("price", (yes_bid + yes_ask) / 2.0) or 0.0)
    if side == "NO":
        return max(0.0, 1.0 - yes_ask), min(0.9999, 1.0 - yes_bid), max(0.0, min(0.9999, 1.0 - yes_mid))
    return yes_bid, yes_ask, yes_mid


def _market_position_snapshot(market: dict) -> dict:
    pos = market["position"]
    side = _position_side(pos)
    current_price = pos["entry_price"]
    for outcome in market.get("all_outcomes", []):
        if outcome["market_id"] == pos["market_id"]:
            bid, ask, mid = _quote_for_side(outcome, side)
            current_price = bid if bid > 0 else mid
            break
    return {
        "side": side,
        "current_price": current_price,
        "unrealized": round((current_price - pos["entry_price"]) * pos["shares"], 2),
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
    expected_ev = float(pos.get("requested_cost", pos.get("cost", 0.0)) or 0.0) * float(pos.get("net_ev", pos.get("ev", 0.0)) or 0.0)
    realized_pnl = float(pos.get("realized_pnl", pos.get("pnl", 0.0)) or 0.0)
    fill_rate = filled_shares / requested_shares if requested_shares > 0 else 1.0
    slippage_bps = ((avg_fill - expected_fill) / expected_fill * 10000) if expected_fill > 0 else 0.0
    return {
        "fill_rate": fill_rate,
        "slippage_bps": slippage_bps,
        "expected_ev_dollars": expected_ev,
        "realized_pnl": realized_pnl,
    }


def format_status(state: dict, open_positions: list[dict], mode: str) -> str:
    """Format bot status message."""
    bal = state["balance"]
    start = state["starting_balance"]
    ret_pct = (bal - start) / start * 100 if start > 0 else 0
    wins = state.get("wins", 0)
    losses = state.get("losses", 0)
    total = wins + losses
    ret_sign = "\\+" if ret_pct >= 0 else ""

    mode_emoji = "🔴 PRODUCTION" if mode == "production" else "🟡 SIMULATION"

    lines = [
        "📊 *WeatherBet Status*",
        "",
        f"Mode: {escape_md(mode_emoji)}",
        f"Balance: `${bal:,.2f}` \\(start ${_fmt_money(start)}, {ret_sign}{_fmt_pct(ret_pct)}%\\)",
    ]

    if total > 0:
        wr = wins / total * 100
        wr_str = escape_md(f"{wr:.0f}")
        lines.append(f"Trades: {total} \\| W: {wins} \\| L: {losses} \\| WR: {wr_str}%")
    else:
        lines.append("No resolved trades yet")

    pending_entries = sum(1 for m in open_positions if m["position"].get("order_status") == "pending")
    pending_exits = sum(1 for m in open_positions if m["position"].get("exit_order_status") == "pending")
    partial_entries = sum(1 for m in open_positions if m["position"].get("order_status") == "partial")
    partial_exits = sum(1 for m in open_positions if m["position"].get("exit_order_status") == "partial")
    lines.append(f"Open positions: {len(open_positions)} \\| Entry pending: {pending_entries} \\| Exit pending: {pending_exits}")
    lines.append(f"Partial fills: entry {partial_entries} \\| exit {partial_exits}")

    if open_positions:
        lines.append("")
        total_unrealized = 0.0
        for m in open_positions:
            pos = m["position"]
            unit_sym = "F" if m["unit"] == "F" else "C"
            label = f"{pos['bucket_low']}\\-{pos['bucket_high']}{unit_sym}"

            snap = _market_position_snapshot(m)
            current_price = snap["current_price"]
            unrealized = snap["unrealized"]
            total_unrealized += unrealized
            pnl_sign = "\\+" if unrealized >= 0 else ""
            src = pos.get("forecast_src", "?").upper()
            side = snap["side"]
            exec_metrics = _execution_metrics(pos)
            entry_str = _fmt_price(pos["entry_price"])
            curr_str = _fmt_price(current_price)
            pnl_str = escape_md(f"{unrealized:.2f}")
            state_tag = "EXIT\\_PARTIAL" if snap["exit_partial"] else "ENTRY\\_PARTIAL" if snap["entry_partial"] else "EXIT\\_PENDING" if snap["exit_pending"] else "ENTRY\\_PENDING" if snap["entry_pending"] else "OPEN"

            lines.append(
                f"• {escape_md(m['city_name'])} {escape_md(m['date'])} \\| "
                f"{escape_md(side)} {label} \\| "
                f"${entry_str} → ${curr_str} \\| "
                f"PnL: {pnl_sign}{pnl_str} \\| {escape_md(state_tag)} \\| {src}"
            )
            lines.append(
                "  Fill: "
                + escape_md(f"{exec_metrics['fill_rate'] * 100:.0f}")
                + "% \\| Slip: "
                + _fmt_bps(exec_metrics["slippage_bps"])
                + "bps \\| ExpEV: "
                + escape_md(f"{exec_metrics['expected_ev_dollars']:+.2f}")
            )

        u_sign = "\\+" if total_unrealized >= 0 else ""
        lines.append(f"\nUnrealized PnL: {u_sign}{escape_md(f'{total_unrealized:.2f}')}")

    return "\n".join(lines)


def format_positions(markets: list[dict]) -> str:
    """Format detailed open positions."""
    open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]

    if not open_pos:
        return "📭 No open positions"

    lines = [f"📂 *Open Positions* \\({len(open_pos)}\\)\n"]
    for m in open_pos:
        pos = m["position"]
        unit_sym = "F" if m["unit"] == "F" else "C"
        label = f"{pos['bucket_low']}\\-{pos['bucket_high']}{unit_sym}"

        snap = _market_position_snapshot(m)
        current_price = snap["current_price"]
        unrealized = snap["unrealized"]
        pnl_sign = "\\+" if unrealized >= 0 else ""
        src = pos.get("forecast_src", "?").upper()
        side = snap["side"]

        entry_str = _fmt_price(pos["entry_price"])
        curr_str = _fmt_price(current_price)
        shares_str = _fmt_shares(pos["shares"])
        cost_str = _fmt_money(pos["cost"])
        pnl_str = escape_md(f"{unrealized:.2f}")
        ev_str = escape_md(f"{pos.get('net_ev', pos['ev']):+.3f}")
        kelly_str = escape_md(f"{pos['kelly']:.2%}")
        exec_metrics = _execution_metrics(pos)
        order_state = "exit partial" if snap["exit_partial"] else "entry partial" if snap["entry_partial"] else "exit pending" if snap["exit_pending"] else "entry pending" if snap["entry_pending"] else "live"

        lines.append(f"*{escape_md(m['city_name'])}* — {escape_md(m['date'])}")
        lines.append(f"  Side: {escape_md(side)} \\| Bucket: {label}")
        lines.append(f"  Entry: ${entry_str} → Now: ${curr_str}")
        lines.append(f"  Shares: {shares_str} \\| Cost: ${cost_str}")
        lines.append(f"  PnL: {pnl_sign}{pnl_str} \\| Source: {src} \\| {escape_md(order_state)}")
        lines.append(f"  Net EV: {ev_str} \\| Kelly: {kelly_str}")
        fill_pct = escape_md(f"{exec_metrics['fill_rate'] * 100:.0f}")
        slip_bps = _fmt_bps(exec_metrics["slippage_bps"])
        exp_ev = escape_md(f"{exec_metrics['expected_ev_dollars']:+.2f}")
        lines.append(f"  Fill: {fill_pct}% \\| Slip: {slip_bps}bps \\| ExpEV: {exp_ev}")
        lines.append("")

    return "\n".join(lines)


def format_report(markets: list[dict]) -> str:
    """Format full report of resolved markets."""
    resolved = [m for m in markets if m["status"] == "resolved" and m.get("pnl") is not None]

    if not resolved:
        return "📭 No resolved markets yet"

    total_pnl = sum(m["pnl"] for m in resolved)
    wins = [m for m in resolved if m["resolved_outcome"] == "win"]
    losses = [m for m in resolved if m["resolved_outcome"] == "loss"]
    wr = len(wins) / len(resolved) * 100 if resolved else 0

    lines = [
        "📊 *WeatherBet — Full Report*\n",
        f"Total resolved: {len(resolved)}",
        f"Wins: {len(wins)} \\| Losses: {len(losses)}",
        f"Win rate: {escape_md(f'{wr:.0f}')}%",
        f"Total PnL: {'\\+' if total_pnl >= 0 else ''}{escape_md(f'{total_pnl:.2f}')}\n",
        "*By city:*",
    ]

    from config.locations import LOCATIONS
    for city in sorted(set(m["city"] for m in resolved)):
        group = [m for m in resolved if m["city"] == city]
        w = len([m for m in group if m["resolved_outcome"] == "win"])
        pnl = sum(m["pnl"] for m in group)
        name = LOCATIONS.get(city, {}).get("name", city)
        pnl_sign = "\\+" if pnl >= 0 else ""
        wr_city = f"{w/len(group):.0%}"
        lines.append(f"  {escape_md(name)}: {w}/{len(group)} \\({escape_md(wr_city)}\\) PnL: {pnl_sign}{escape_md(f'{pnl:.2f}')}")

    return "\n".join(lines)


def format_daily_report(markets: list[dict]) -> str:
    """Format a daily summary (today's activity)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_markets = [m for m in markets if m.get("date") == today]

    if not today_markets:
        return f"📭 No activity for {escape_md(today)}"

    opened = [m for m in today_markets if m.get("position")]
    resolved = [m for m in today_markets if m["status"] == "resolved"]
    total_pnl = sum(m.get("pnl", 0) for m in resolved if m.get("pnl") is not None)

    lines = [
        f"📅 *Daily Report — {escape_md(today)}*\n",
        f"Markets tracked: {len(today_markets)}",
        f"Positions opened: {len(opened)}",
        f"Resolved: {len(resolved)}",
    ]
    if resolved:
        pnl_sign = "\\+" if total_pnl >= 0 else ""
        lines.append(f"PnL: {pnl_sign}{escape_md(f'{total_pnl:.2f}')}")

    return "\n".join(lines)


def format_weekly_report(markets: list[dict]) -> str:
    """Format a weekly summary (last 7 days)."""
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    recent = [m for m in markets if m.get("date", "") >= week_ago]
    resolved = [m for m in recent if m["status"] == "resolved" and m.get("pnl") is not None]

    if not resolved:
        return "📭 No resolved trades in the last 7 days"

    total_pnl = sum(m["pnl"] for m in resolved)
    wins = len([m for m in resolved if m["resolved_outcome"] == "win"])
    wr_str = escape_md(f"{wins/len(resolved):.0%}")

    lines = [
        "📆 *Weekly Report*\n",
        f"Resolved: {len(resolved)}",
        f"Wins: {wins} \\| Losses: {len(resolved) - wins}",
        f"Win rate: {wr_str}",
        f"Total PnL: {'\\+' if total_pnl >= 0 else ''}{escape_md(f'{total_pnl:.2f}')}",
    ]

    return "\n".join(lines)


def format_markets_list(events_info: list[dict]) -> str:
    """Format active weather markets."""
    if not events_info:
        return "📭 No active weather markets found"

    lines = ["🌡️ *Active Weather Markets*\n"]
    for info in events_info[:20]:
        top_price_str = _fmt_price(info["top_price"])
        lines.append(
            f"• {escape_md(info['city_name'])} — {escape_md(info['date'])}\n"
            f"  Buckets: {info['bucket_count']} \\| Top: {escape_md(info['top_bucket'])} @ ${top_price_str}"
        )
    return "\n".join(lines)
