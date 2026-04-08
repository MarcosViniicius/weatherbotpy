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

    lines.append(f"Open positions: {len(open_positions)}")

    if open_positions:
        lines.append("")
        total_unrealized = 0.0
        for m in open_positions:
            pos = m["position"]
            unit_sym = "F" if m["unit"] == "F" else "C"
            label = f"{pos['bucket_low']}\\-{pos['bucket_high']}{unit_sym}"

            current_price = pos["entry_price"]
            for o in m.get("all_outcomes", []):
                if o["market_id"] == pos["market_id"]:
                    current_price = o["price"]
                    break

            unrealized = round((current_price - pos["entry_price"]) * pos["shares"], 2)
            total_unrealized += unrealized
            pnl_sign = "\\+" if unrealized >= 0 else ""
            src = pos.get("forecast_src", "?").upper()
            entry_str = _fmt_price(pos["entry_price"])
            curr_str = _fmt_price(current_price)
            pnl_str = escape_md(f"{unrealized:.2f}")

            lines.append(
                f"• {escape_md(m['city_name'])} {escape_md(m['date'])} \\| "
                f"{label} \\| "
                f"${entry_str} → ${curr_str} \\| "
                f"PnL: {pnl_sign}{pnl_str} \\| {src}"
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

        current_price = pos["entry_price"]
        for o in m.get("all_outcomes", []):
            if o["market_id"] == pos["market_id"]:
                current_price = o["price"]
                break

        unrealized = round((current_price - pos["entry_price"]) * pos["shares"], 2)
        pnl_sign = "\\+" if unrealized >= 0 else ""
        src = pos.get("forecast_src", "?").upper()

        entry_str = _fmt_price(pos["entry_price"])
        curr_str = _fmt_price(current_price)
        shares_str = _fmt_shares(pos["shares"])
        cost_str = _fmt_money(pos["cost"])
        pnl_str = escape_md(f"{unrealized:.2f}")
        ev_str = escape_md(f"{pos['ev']:+.2f}")
        kelly_str = escape_md(f"{pos['kelly']:.2%}")

        lines.append(f"*{escape_md(m['city_name'])}* — {escape_md(m['date'])}")
        lines.append(f"  Bucket: {label}")
        lines.append(f"  Entry: ${entry_str} → Now: ${curr_str}")
        lines.append(f"  Shares: {shares_str} \\| Cost: ${cost_str}")
        lines.append(f"  PnL: {pnl_sign}{pnl_str} \\| Source: {src}")
        lines.append(f"  EV: {ev_str} \\| Kelly: {kelly_str}")
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
