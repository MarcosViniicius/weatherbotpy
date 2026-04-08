import json
from pathlib import Path
from statistics import mean, pstdev
from datetime import datetime

root = Path(r'd:/asoftware-development/weatherbot')
market_dir = root / 'data' / 'markets'
state = json.loads((root / 'data' / 'state.json').read_text(encoding='utf-8'))
pred_log = json.loads((root / 'data' / 'predictions_log.json').read_text(encoding='utf-8'))

rows = []
for fp in market_dir.glob('*.json'):
    m = json.loads(fp.read_text(encoding='utf-8'))
    pos = m.get('position')
    if not pos:
        continue
    rows.append({
        'file': fp.name,
        'city': m.get('city'),
        'date': m.get('date'),
        'status': pos.get('status'),
        'opened_at': pos.get('opened_at'),
        'closed_at': pos.get('closed_at'),
        'entry_price': float(pos.get('entry_price') or 0),
        'exit_price': (None if pos.get('exit_price') is None else float(pos.get('exit_price'))),
        'shares': float(pos.get('shares') or 0),
        'cost': float(pos.get('cost') or 0),
        'pnl': (None if pos.get('pnl') is None else float(pos.get('pnl'))),
        'edge': float(pos.get('edge') or 0),
        'ev': float(pos.get('ev') or 0),
        'p': float(pos.get('p') or 0),
        'confidence': float(pos.get('confidence') or 0),
        'sigma': float(pos.get('sigma') or 0),
        'hours_left': float(pos.get('hours_left') or 0),
        'close_reason': pos.get('close_reason') or '',
        'market_id': str(pos.get('market_id') or ''),
    })

open_rows = [r for r in rows if r['status'] == 'open']
closed_rows = [r for r in rows if r['status'] == 'closed' and r['pnl'] is not None]
resolved_rows = [r for r in closed_rows if r['close_reason'] == 'resolved']

# mark-to-market for open positions from latest bid in each market file
open_mtm = 0.0
for r in open_rows:
    m = json.loads((market_dir / r['file']).read_text(encoding='utf-8'))
    cp = r['entry_price']
    for o in m.get('all_outcomes', []):
        if str(o.get('market_id')) == r['market_id']:
            cp = float(o.get('bid', o.get('price', cp)))
            break
    open_mtm += (cp - r['entry_price']) * r['shares']

balance = float(state.get('balance', 0))
start = float(state.get('starting_balance', 0))
equity = balance + open_mtm
roi_total = ((equity / start) - 1) if start else None

realized_pnl = sum(r['pnl'] for r in closed_rows)
win_rows = [r for r in closed_rows if r['pnl'] > 0]
loss_rows = [r for r in closed_rows if r['pnl'] < 0]
win_rate = (len(win_rows) / len(closed_rows)) if closed_rows else None
avg_win = mean([r['pnl'] for r in win_rows]) if win_rows else None
avg_loss = mean([r['pnl'] for r in loss_rows]) if loss_rows else None

# equity curve from closed trades only (conservative realized curve)
curve = [start]
for r in sorted(closed_rows, key=lambda x: x['closed_at'] or ''):
    curve.append(curve[-1] + r['pnl'])
peak = curve[0] if curve else 0
max_dd = 0.0
for v in curve:
    if v > peak:
        peak = v
    if peak > 0:
        dd = (peak - v) / peak
        if dd > max_dd:
            max_dd = dd

# Sharpe-like on closed trade returns only
trade_rets = []
for r in closed_rows:
    c = r['cost']
    if c > 0:
        trade_rets.append(r['pnl'] / c)
sharpe_like = None
if len(trade_rets) >= 2:
    sd = pstdev(trade_rets)
    if sd > 0:
        sharpe_like = mean(trade_rets) / sd

# Kelly overbet check: implied full-kelly from p and price
# f* = (p-price)/(1-price) when p>price else 0 ; compare to actual fraction of balance used at entry
kelly_checks = []
for r in rows:
    p = r['p']
    price = r['entry_price']
    f_full = ((p - price) / (1 - price)) if (price < 1 and p > price) else 0.0
    bal_est = start if not r['opened_at'] else None
    # fallback: use start because per-trade pre-balance is not stored in position
    frac_used = (r['cost'] / start) if start else 0.0
    kelly_checks.append((f_full, frac_used))

overbet_count = sum(1 for f_full, frac_used in kelly_checks if f_full > 0 and frac_used > f_full)

# bins by time-to-event
bins = {'<=12h': [], '12-24h': [], '24-48h': [], '>48h': []}
for r in rows:
    h = r['hours_left']
    if h <= 12: bins['<=12h'].append(r)
    elif h <= 24: bins['12-24h'].append(r)
    elif h <= 48: bins['24-48h'].append(r)
    else: bins['>48h'].append(r)

print(f'ROWS={len(rows)}')
print(f'OPEN={len(open_rows)}')
print(f'CLOSED={len(closed_rows)}')
print(f'RESOLVED={len(resolved_rows)}')
print(f'PRED_LOG={len(pred_log)}')
print(f'STATE_BALANCE={balance:.4f}')
print(f'OPEN_MTM={open_mtm:.4f}')
print(f'EQUITY_MTM={equity:.4f}')
print(f'ROI_TOTAL_PCT={(roi_total*100):.2f}' if roi_total is not None else 'ROI_TOTAL_PCT=NA')
print(f'REALIZED_PNL={realized_pnl:.4f}')
print(f'WIN_RATE_PCT={(win_rate*100):.2f}' if win_rate is not None else 'WIN_RATE_PCT=NA')
print(f'AVG_WIN={avg_win:.4f}' if avg_win is not None else 'AVG_WIN=NA')
print(f'AVG_LOSS={avg_loss:.4f}' if avg_loss is not None else 'AVG_LOSS=NA')
print(f'MAX_DD_PCT={max_dd*100:.2f}')
print(f'SHARPE_LIKE={sharpe_like:.4f}' if sharpe_like is not None else 'SHARPE_LIKE=NA')
print(f'AVG_EDGE={mean([r["edge"] for r in rows]):.4f}' if rows else 'AVG_EDGE=NA')
print(f'AVG_EV={mean([r["ev"] for r in rows]):.4f}' if rows else 'AVG_EV=NA')
print(f'AVG_P={mean([r["p"] for r in rows]):.4f}' if rows else 'AVG_P=NA')
print(f'AVG_CONF={mean([r["confidence"] for r in rows]):.4f}' if rows else 'AVG_CONF=NA')
print(f'AVG_SIGMA={mean([r["sigma"] for r in rows]):.4f}' if rows else 'AVG_SIGMA=NA')
print(f'OVERBET_COUNT={overbet_count}')

print('---TIME_BINS---')
for k, vals in bins.items():
    if not vals:
        print(f'{k}|n=0')
        continue
    print(f'{k}|n={len(vals)}|avg_edge={mean([x["edge"] for x in vals]):.4f}|avg_ev={mean([x["ev"] for x in vals]):.4f}|avg_p={mean([x["p"] for x in vals]):.4f}')

print('---OPEN_TRADES---')
for r in sorted(open_rows, key=lambda x: x['opened_at'] or ''):
    print(f"{r['city']}|{r['date']}|p={r['p']:.4f}|edge={r['edge']:.4f}|ev={r['ev']:.4f}|entry={r['entry_price']:.4f}|cost={r['cost']:.2f}|hours={r['hours_left']:.1f}|sigma={r['sigma']:.2f}")

print('---CLOSED_TRADES---')
for r in sorted(closed_rows, key=lambda x: x['opened_at'] or ''):
    print(f"{r['city']}|{r['date']}|pnl={r['pnl']:.4f}|reason={r['close_reason']}|p={r['p']:.4f}|edge={r['edge']:.4f}|entry={r['entry_price']:.4f}|exit={(r['exit_price'] if r['exit_price'] is not None else 'NA')}|cost={r['cost']:.2f}")
