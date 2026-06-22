#!/usr/bin/env python3
"""
Jarvis — MNQ Signal Engine + Conversational Telegram Bot + Web UI
"""

import os, json, time, requests, threading
import yfinance as yf
import pandas as pd
import websocket
import anthropic
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request


# ── Config ────────────────────────────────────────────────────────
POLYGON_KEY    = os.environ["POLYGON_API_KEY"]
GOOGLE_SHEETS_URL = os.environ.get("GOOGLE_SHEETS_URL", "")  # Apps Script web app URL
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")

SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=representation"
}
SB_REST = f"{SUPABASE_URL}/rest/v1"

STARTING_BALANCE = 50000
PTS_TO_USD       = 2.0   # $2/pt per MNQ contract

# ── Prop Firm Eval Tracker ────────────────────────────────────────
EVAL_START    = 50000
EVAL_TARGET   = 53000   # +$3k = funded
EVAL_FLOOR    = 48000   # -$2k = blown
_eval_alerted = {"last": None}  # prevent repeat alerts

# ── Eval checkpoints — persisted to Supabase ─────────────────────
def load_eval_checkpoints():
    """Load eval checkpoints from Supabase signals_log (survives deploys)."""
    try:
        r = requests.get(
            f"{SB_REST}/signals_log?select=*&notes=like.EVAL_CHECKPOINT%25&order=id.asc",
            headers=SB_HEADERS, timeout=5
        )
        rows = r.json()
        if not isinstance(rows, list):
            return []
        checkpoints = []
        for row in rows:
            try:
                data = json.loads(row["notes"].replace("EVAL_CHECKPOINT:", ""))
                checkpoints.append(data)
            except:
                pass
        return checkpoints
    except:
        return []

def save_eval_checkpoint(cp):
    """Persist an eval checkpoint to Supabase."""
    try:
        sb_insert("signals_log", {
            "side": "EVAL", "entry": 0, "sl": 0, "tp1": 0, "tp2": 0, "tp3": 0,
            "contracts": 0, "session": "EVAL", "strength": 0, "taken": False, "skipped": False,
            "notes": f"EVAL_CHECKPOINT:{json.dumps(cp)}"
        })
    except:
        pass

def reset_eval_checkpoints():
    """Nuclear reset — wipe all eval checkpoints from DB. Use when data is corrupt."""
    try:
        requests.delete(
            f"{SB_REST}/signals_log?notes=like.EVAL_CHECKPOINT%25",
            headers=SB_HEADERS, timeout=5
        )
        print("[EVAL] Checkpoints reset")
    except:
        pass

_eval_checkpoints = load_eval_checkpoints()
print(f"[EVAL] Loaded {len(_eval_checkpoints)} checkpoints from DB")

# ── Last SL level — prevent re-entry at blown level ───────────────
_last_sl = {"price": None, "side": None, "time": 0}   # cleared after 2hrs
# ── Last signal time — hard dedup even if price barely moves ──────
_last_signal_time = {"t": 0}

# ── Trade Cooldown ────────────────────────────────────────────────
# Win: 30min  |  Loss: 60min
# Reads from DB so it survives Railway deploys/restarts.
_cooldown = {"until": 0, "reason": ""}

def get_cooldown_remaining():
    """Check DB for last closed trade — return seconds remaining in cooldown."""
    # First check in-memory (fast path)
    remaining = _cooldown["until"] - time.time()
    if remaining > 0:
        return remaining, _cooldown["reason"]
    # Fallback: check DB for last CLOSED Jarvis trade specifically
    try:
        trades = sb_select("trades", extra="&source=eq.JARVIS&result=in.(WIN,LOSS)&order=id.desc&limit=1")
        closed = [t for t in trades if t.get("closed_at")]
        if not closed:
            return 0, ""
        last = closed[0]
        closed_at = datetime.fromisoformat(last["closed_at"].replace("Z",""))
        elapsed   = (datetime.utcnow() - closed_at).total_seconds()
        mins      = 30 if last["result"] == "WIN" else 60
        remaining = (mins * 60) - elapsed
        if remaining > 0:
            reason = f"{last['result']} — {mins}min cooldown"
            _cooldown["until"]  = time.time() + remaining
            _cooldown["reason"] = reason
            return remaining, reason
    except:
        pass
    return 0, ""

def get_scale(contracts):
    """
    Scale-out breakdown: [tp1_contracts, tp2_contracts, tp3_contracts]
    TP3 is ALWAYS 1 contract. Extra size loads into TP1/TP2.
    3  → 1/1/1
    4  → 2/1/1
    5  → 3/1/1
    6  → 3/2/1
    7  → 4/2/1
    8  → 5/2/1
    """
    c = int(contracts)
    if c <= 3:  return [1, 1, 1]
    if c == 4:  return [2, 1, 1]
    if c == 5:  return [3, 1, 1]
    if c == 6:  return [3, 2, 1]
    if c == 7:  return [4, 2, 1]
    return [c - 3, 2, 1]  # 8+ keeps TP2=2, TP3=1

def calc_scaled_pnl(entry, side, tp1, tp2, tp3, sl, contracts, tp1_hit, tp2_hit, tp3_hit, sl_hit):
    """Correct P&L with partial closes at each TP."""
    c1, c2, c3 = get_scale(contracts)
    sign = 1 if side == "BUY" else -1
    pnl = 0
    closed = 0
    if tp1_hit:
        pnl   += c1 * sign * (tp1 - entry) * PTS_TO_USD
        closed += c1
    if tp2_hit:
        pnl   += c2 * sign * (tp2 - entry) * PTS_TO_USD
        closed += c2
    if tp3_hit:
        pnl   += c3 * sign * (tp3 - entry) * PTS_TO_USD
        closed += c3
    if sl_hit:
        remaining = contracts - closed
        pnl += remaining * sign * (sl - entry) * PTS_TO_USD
    return round(pnl, 2)

def calc_unrealized_pnl(entry, side, price, contracts, tp1_hit, tp2_hit):
    """Live unrealized P&L on REMAINING contracts."""
    c1, c2, _ = get_scale(contracts)
    closed = (c1 if tp1_hit else 0) + (c2 if tp2_hit else 0)
    remaining = contracts - closed
    pts = (price - entry) if side == "BUY" else (entry - price)
    locked = 0
    # already closed contracts' locked P&L isn't tracked here — just show remaining
    return round(pts * remaining * PTS_TO_USD, 2), remaining

app = Flask(__name__)

# ── Supabase ──────────────────────────────────────────────────────
def sb_select(table, filters=None, extra=""):
    url = f"{SB_REST}/{table}?select=*{extra}"
    if filters:
        for k, v in filters.items():
            url += f"&{k}=eq.{v}"
    r = requests.get(url, headers=SB_HEADERS, timeout=5)
    result = r.json()
    return result if isinstance(result, list) else []

def sb_insert(table, data):
    r = requests.post(f"{SB_REST}/{table}", headers=SB_HEADERS, json=data, timeout=5)
    result = r.json()
    return result[0] if isinstance(result, list) and result else None

def sb_update(table, row_id, data):
    requests.patch(f"{SB_REST}/{table}?id=eq.{row_id}", headers=SB_HEADERS, json=data, timeout=5)

# ── Telegram ──────────────────────────────────────────────────────
_tg_offset = {"offset": 0}
_pending_signal = {"signal": None}  # signal waiting for /take or /skip

def tg_send(msg, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": parse_mode},
            timeout=5
        )
    except:
        pass

def tg_get_updates():
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": _tg_offset["offset"], "allowed_updates": ["message"]},
            timeout=8
        )
        return r.json().get("result", [])
    except:
        return []

# ── Command registry — add entries here, /help auto-updates ──────
COMMANDS = {
    "📡 Market": [
        ("/price",    "Live MNQM6 price, session, pre-trend direction + strength"),
        ("/analysis", "Full read: trends, key levels, HTF context, AI outlook + setup status"),
        ("/status",   "Eval balance, W/L record, open trade P&L, cooldown, session"),
    ],
    "📊 Active Trade": [
        ("/progress", "Live P&L, distance to each TP/SL, locked profit, bar chart"),
        ("/skip",     "Block the current pending signal (use before it fires)"),
    ],
    "📋 Performance": [
        ("/recap",    "AI recap — BUY vs SELL breakdown, TP3 rate, what's working"),
        ("/trades",   "Last 15 trades — date, side, result, P&L"),
        ("/daily",    "Today's trades + P&L + eval status, auto-pushes to Sheets"),
        ("/weekly",   "This week's full stats + session breakdown, auto-pushes to Sheets"),
        ("/sheet",    "Last 10 trades as copy-paste spreadsheet rows"),
    ],
    "📥 Training Data": [
        ("/gm",       "Log Goldmine callout: /gm SELL 5 entry tp1 tp2 tp3 sl"),
    ],
    "⚙️ Admin": [
        ("/reseteval","Nuclear reset eval checkpoints — only if data is corrupt"),
    ],
}

def help_text():
    lines = "<b>Jarvis Commands v3.0</b>\n\n"
    for section, cmds in COMMANDS.items():
        lines += f"{section}\n"
        for cmd, desc in cmds:
            lines += f"  {cmd} — {desc}\n"
        lines += "\n"
    lines += "💬 Natural language works too — \"analysis\" \"recap\" \"where at\" \"results\"\n\n"
    lines += "Signals fire automatically. 🤖"
    return lines

def handle_tg_command(text):
    original = text.strip()
    text = original.lower()
    cmd  = text.split()[0] if text else ""

    # ── /reseteval ─────────────────────────────────────────────────
    if cmd == "/reseteval":
        reset_eval_checkpoints()
        _eval_checkpoints.clear()
        _eval_alerted["last"] = None
        ev = get_eval_status()
        tg_send(f"🔄 <b>Eval reset</b>\n\nCheckpoints cleared. Starting fresh.\n"
                f"Current P&L: ${ev['total_pnl']:+,.2f}  →  Eval balance: ${ev['eval_balance']:,.2f}")

    # ── /skip ──────────────────────────────────────────────────────
    elif cmd in ("/skip", "skip", "❌", "nah"):
        if _pending_signal["signal"]:
            log_signal(_pending_signal["signal"], skipped=True)
            _pending_signal["signal"] = None
            tg_send("⏭ Skipped. Watching for the next setup.")
        else:
            tg_send("No pending signal to skip.")

    # ── /price ─────────────────────────────────────────────────────
    elif cmd in ("/price", "price"):
        price = get_live_price()
        session_name, session_ok = get_session()
        candles = get_15min_candles()
        pre_trend = get_pretend(candles)
        strength  = get_trend_strength(candles)
        age = round(time.time() - _ws_price["updated"], 1) if _ws_price["updated"] else "?"
        tg_send(
            f"<b>MNQM6</b>  <code>{price:,.2f}</code>  ({age}s ago)\n"
            f"{'🟢' if session_ok else '🔴'} {session_name}  "
            f"{'📉' if pre_trend=='DOWN' else '📈'} Pre-trend {pre_trend or '?'} ({strength:.0f}pts)"
        )

    # ── /status ────────────────────────────────────────────────────
    elif cmd in ("/status", "status", "s"):
        price = get_live_price()
        stats = get_stats()
        session_name, session_ok = get_session()
        candles   = get_15min_candles()
        pre_trend = get_pretend(candles)
        _, status_msg = check_for_signal()

        open_str = ""
        for t in stats["open_trades"]:
            side      = t.get("side")
            entry     = t.get("entry", 0)
            contracts = t.get("contracts", 5)
            tp1_hit   = t.get("tp1_hit", False)
            tp2_hit   = t.get("tp2_hit", False)
            if price and entry:
                tp1 = t.get("tp1", 0)
                tp2 = t.get("tp2", 0)
                unreal, remaining = calc_unrealized_pnl(entry, side, price, contracts, tp1_hit, tp2_hit)
                c1, c2, _  = get_scale(contracts)
                locked     = (c1*(tp1-entry if side=="BUY" else entry-tp1)*PTS_TO_USD if tp1_hit else 0) + (c2*(tp2-entry if side=="BUY" else entry-tp2)*PTS_TO_USD if tp2_hit else 0)
                total      = round(locked + unreal, 2)
                be_str     = "  SL@BE ✅" if tp2_hit else ""
                open_str   = (f"\n\n📊 <b>Open #{t['id']}</b>: {side} {contracts}MNQ @ {entry}{be_str}\n"
                              f"   Unrealized: ${unreal:+,.2f}  |  Total P&L: <b>${total:+,.2f}</b>  ({remaining} MNQ left)")

        # In live mode — pull real balance from Topstep
        if TRADING_MODE == "live" and TOPSTEP_ACCOUNT_ID:
            bal = ts_get_balance()
            if bal:
                real_bal   = float(bal.get("balance") or 0)
                daily_pnl  = float(bal.get("daily_pnl") or 0)
                open_pnl   = float(bal.get("open_pnl") or 0)
                can_trade  = bal.get("can_trade", True)
                acct_name  = bal.get("name", TOPSTEP_ACCOUNT_ID)
                to_target  = max(0, 53000 - real_bal)
                dd_buffer  = real_bal - 48000
                eval_pct   = max(0, min(100, round((real_bal - 50000 + 2000) / 5000 * 100)))
                eval_bar   = "█" * round(eval_pct/10) + "░" * (10 - round(eval_pct/10))
                trade_flag = "🟢 Can trade" if can_trade else "🔴 Trading disabled"
                eval_str   = (
                    f"\n\n🏦 <b>Topstep — {acct_name}</b>\n"
                    f"Balance: <code>${real_bal:,.2f}</code>  (today: {'+'if daily_pnl>=0 else ''}${daily_pnl:,.2f})\n"
                    f"[{eval_bar}] {eval_pct}%\n"
                    f"To target: ${to_target:,.0f}  |  DD buffer: ${dd_buffer:,.0f}\n"
                    f"{trade_flag}  |  Daily stop: −${DAILY_LOSS_LIMIT:,.0f}"
                )
            else:
                eval_str = "\n\n⚠️ Couldn't fetch Topstep balance"
        else:
            ev = get_eval_status()
            eval_pct  = max(0, min(100, round((ev["eval_pnl"] + 2000) / 5000 * 100)))
            eval_bar  = "█" * round(eval_pct/10) + "░" * (10 - round(eval_pct/10))
            dd_pct    = round(ev["drawdown_used"] / 2000 * 100) if ev["drawdown_used"] > 0 else 0
            eval_str  = (
                f"\n\n📋 <b>Paper Eval #{ev['eval_num']}</b>  (Funded: {ev['funded']}  Blown: {ev['blown']})\n"
                f"Balance: <code>${ev['eval_balance']:,.2f}</code>  ({ev['eval_pnl']:+,.2f} this eval)\n"
                f"[{eval_bar}] {eval_pct}%\n"
                f"To target: ${ev['to_target']:,.0f}  |  DD buffer: ${ev['to_floor']:,.0f} left  ({dd_pct}% used)"
            )

        tg_send(
            f"<b>Jarvis Status</b>\n\n"
            f"📈 Record: {stats['jarvis_wins']}W / {stats['jarvis_losses']}L  ({stats['jarvis_wr']}% WR)"
            f"{open_str}"
            f"{eval_str}\n\n"
            f"🕐 {session_name}  |  Pre-trend: {pre_trend or '?'}\n"
            f"📡 Price: {price:,.2f}\n"
            f"💬 {status_msg}"
        )

    # ── /progress ──────────────────────────────────────────────────
    elif any(w in text for w in ["/progress", "progress", "prog", "where at", "update", "p&l", "pnl"]):
        open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
        if not open_trades:
            tg_send("No open trade. Watching for next setup.")
            return
        t         = open_trades[0]
        price     = get_live_price()
        side      = t["side"];  entry = t["entry"];  sl = t["sl"]
        tp1       = t["tp1"];   tp2   = t["tp2"];    tp3 = t["tp3"]
        contracts = t.get("contracts", 5)
        tp1_hit   = t.get("tp1_hit", False)
        tp2_hit   = t.get("tp2_hit", False)
        c1, c2, c3 = get_scale(contracts)

        pts        = (price - entry) if side == "BUY" else (entry - price)
        unreal, remaining = calc_unrealized_pnl(entry, side, price, contracts, tp1_hit, tp2_hit)
        locked     = (c1*50*PTS_TO_USD if tp1_hit else 0) + (c2*75*PTS_TO_USD if tp2_hit else 0)
        total      = round(locked + unreal, 2)

        total_range   = abs(tp3 - sl)
        progress_pts  = pts + 40
        pct           = max(0, min(100, round(progress_pts / total_range * 100)))
        bar           = "█" * round(pct/10) + "░" * (10 - round(pct/10))
        status        = "🟢 IN PROFIT" if pts > 0 else "⚪ BREAKEVEN" if pts == 0 else "🔴 DRAWDOWN"

        tp1_str = "✅ hit" if tp1_hit else f"<code>{tp1}</code> ({abs(price-tp1):.0f}pts)"
        tp2_str = "✅ hit — SL@BE" if tp2_hit else f"<code>{tp2}</code> ({abs(price-tp2):.0f}pts)"
        tp3_str = f"<code>{tp3}</code> ({abs(price-tp3):.0f}pts)"

        tg_send(
            f"📊 <b>Trade #{t['id']} — {status}</b>\n\n"
            f"{side} {contracts}MNQ  |  Entry <code>{entry}</code>\n"
            f"Price: <code>{price:,.2f}</code>  ({remaining} MNQ still open)\n\n"
            f"[{bar}] {pct}%\n"
            f"SL{'(BE)' if tp2_hit else ''} ←——————→ TP3\n\n"
            f"Unrealized: ${unreal:+,.2f}"
            + (f"  |  Locked: ${locked:+.0f}" if locked else "") +
            f"\n<b>Total P&L: ${total:+,.2f}</b>\n\n"
            f"TP1 [{c1} MNQ]: {tp1_str}\n"
            f"TP2 [{c2} MNQ]: {tp2_str}\n"
            f"TP3 [{c3} MNQ]: {tp3_str}\n"
            f"SL:            <code>{sl}</code>  ({abs(price-sl):.0f}pts away)"
        )

    # ── /daily ─────────────────────────────────────────────────────
    elif any(w in text for w in ["/daily", "daily", "today", "day recap"]):
        send_daily_summary()

    # ── /weekly ────────────────────────────────────────────────────
    elif any(w in text for w in ["/weekly", "weekly", "week recap", "this week"]):
        send_weekly_summary()

    # ── /sheet ─────────────────────────────────────────────────────
    elif any(w in text for w in ["/sheet", "sheet", "spreadsheet", "copy paste", "export"]):
        send_sheet_export()

    # ── /recap ─────────────────────────────────────────────────────
    elif any(w in text for w in ["/recap", "recap", "how we doing", "performance", "summary"]):
        send_smart_recap()

    # ── /trades ────────────────────────────────────────────────────
    elif any(w in text for w in ["/trades", "trades", "history", "last week", "results", "trade history"]):
        send_trade_history()

    # ── /gm — log a Goldmine callout to training data ──────────────
    elif cmd == "/gm":
        # Format: /gm SELL 5 entry sl tp1 tp2 tp3
        # or just paste their callout text and we parse it
        parts = original.split()
        try:
            # /gm SELL 5 30185 30095 30070 30020 SL:30225
            # flexible parse — find side, contracts, numbers
            side_raw = next((p for p in parts if p.upper() in ("BUY","SELL","LONG","SHORT")), None)
            if not side_raw:
                raise ValueError("no side")
            side = "BUY" if side_raw.upper() in ("BUY","LONG") else "SELL"
            numbers = [float(p.replace("SL:","").replace("sl:","")) for p in parts if p.replace(".","").replace("SL:","").replace("sl:","").isdigit() or (p.replace(".","").lstrip("-").isdigit())]
            contracts_raw = next((int(p) for p in parts[1:] if p.isdigit() and int(p) <= 20), 5)
            # numbers should be entry, tp1, tp2, tp3, sl — or entry, sl, tp1, tp2, tp3
            # pick 5 largest/smallest depending on side
            nums = sorted(set(numbers))
            if side == "SELL":
                # entry is highest, tps go down, sl is above entry
                sl_val  = max(nums)
                tp_vals = sorted([n for n in nums if n < sl_val])
                entry_v = tp_vals[-1] if len(tp_vals) >= 2 else sl_val - 40
                tps     = sorted([n for n in tp_vals if n < entry_v])
            else:
                sl_val  = min(nums)
                tp_vals = sorted([n for n in nums if n > sl_val])
                entry_v = tp_vals[0] if tp_vals else sl_val + 40
                tps     = sorted([n for n in tp_vals if n > entry_v])

            tp1 = tps[0] if len(tps) > 0 else None
            tp2 = tps[1] if len(tps) > 1 else None
            tp3 = tps[2] if len(tps) > 2 else None

            trade = {
                "trade_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "side": side, "entry": entry_v, "sl": sl_val,
                "tp1": tp1,   "tp2": tp2,       "tp3": tp3,
                "contracts": contracts_raw,
                "trader": "GOLDMINE", "source": "TRAINING",
                "result": "OPEN",
                "notes": "Added via /gm Telegram command"
            }
            logged = sb_insert("trades", trade)
            if logged:
                tg_send(
                    f"✅ <b>Goldmine trade logged #{logged['id']}</b>\n\n"
                    f"{side} {contracts_raw}MNQ  |  Entry: {entry_v}\n"
                    f"SL: {sl_val}  |  TP1: {tp1}  TP2: {tp2}  TP3: {tp3}\n\n"
                    f"Source: TRAINING  |  Jarvis will resolve it when price hits levels."
                )
            else:
                tg_send("⚠️ DB error. Check the format: /gm SELL 5 30185 30095 30070 30020 SL:30225")
        except Exception as e:
            tg_send(
                f"Couldn't parse that. Use:\n"
                f"<code>/gm SELL 5 30185 30095 30070 30020 30225</code>\n"
                f"(side, contracts, entry, tp1, tp2, tp3, sl)"
            )

    # ── /analysis ─────────────────────────────────────────────────
    elif any(w in text for w in ["/analysis", "analysis", "analyze", "read", "market", "outlook", "what you seeing", "what do you see", "where we going", "where going"]):
        send_market_analysis()

    # ── /contractsearch — find correct MNQ contract ID ────────────
    elif cmd == "/contractsearch":
        hdrs = ts_headers()
        if not hdrs:
            tg_send("Auth failed.")
            return
        try:
            # Try search first
            r = requests.post(f"{PROJECTX_BASE}/api/Contract/search",
                headers=hdrs, json={"searchText": "MNQ", "live": True}, timeout=10)
            tg_send(f"Search ({r.status_code}):\n<code>{r.text[:600]}</code>")
            # Also try available contracts
            r2 = requests.post(f"{PROJECTX_BASE}/api/Contract/available",
                headers=hdrs, json={"live": True}, timeout=10)
            tg_send(f"Available ({r2.status_code}):\n<code>{r2.text[:600]}</code>")
        except Exception as e:
            tg_send(f"Error: {e}")

    # ── /tstest — raw auth + account debug ────────────────────────
    elif cmd == "/tstest":
        tg_send(f"Testing ProjectX auth...\nUsername: <code>{TOPSTEP_USERNAME}</code>\nKey: <code>...{TOPSTEP_API_KEY[-6:] if TOPSTEP_API_KEY else 'NOT SET'}</code>")
        # Force fresh login, bypass cache
        _ts_token["jwt"] = None
        _ts_token["expires"] = 0
        try:
            r = requests.post(
                f"{PROJECTX_BASE}/api/Auth/loginKey",
                headers={"Content-Type": "application/json", "accept": "text/plain"},
                json={"userName": TOPSTEP_USERNAME, "apiKey": TOPSTEP_API_KEY},
                timeout=10
            )
            tg_send(f"Auth response ({r.status_code}):\n<code>{r.text[:500]}</code>")
            if r.status_code == 200:
                data = r.json()
                token = data.get("token") or data.get("accessToken")
                if token:
                    _ts_token["jwt"] = token
                    _ts_token["expires"] = time.time() + 86400
                    # Try account search
                    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                    r2 = requests.post(f"{PROJECTX_BASE}/api/Account/search",
                                       headers=hdrs, json={"onlyActiveAccounts": False}, timeout=10)
                    tg_send(f"Account search ({r2.status_code}):\n<code>{r2.text[:500]}</code>")
        except Exception as e:
            tg_send(f"Error: {e}")

    # ── /accounts ─────────────────────────────────────────────────
    elif cmd == "/accounts":
        handle_accounts_command()

    # ── /balance ──────────────────────────────────────────────────
    elif cmd in ("/balance", "balance"):
        handle_balance_command()

    # ── /setaccount <id> ──────────────────────────────────────────
    elif cmd == "/setaccount":
        parts = original.split()
        if len(parts) < 2:
            tg_send("Usage: /setaccount <account_id>\nRun /accounts to see all IDs.")
        else:
            new_id = parts[1].strip()
            _save_account_id(new_id)
            tg_send(f"✅ Active account set to <code>{new_id}</code> — saved permanently.\nSwitch anytime with /setaccount.")

    # ── /yes — approve pending confirmation signal ─────────────────
    elif cmd in ("/yes", "yes", "y", "take it", "take"):
        sig = _pending_confirmation.get("signal")
        if sig and time.time() < _pending_confirmation.get("expires", 0):
            _pending_confirmation["signal"]  = None
            _pending_confirmation["expires"] = 0
            tg_send("✅ Confirmed — entering trade now.")
            if TRADING_MODE == "live":
                ts_enter_trade(sig)
            else:
                auto_enter_trade(sig)
        else:
            tg_send("No pending signal to approve (or it expired).")

    # ── /no — reject pending confirmation signal ───────────────────
    elif cmd in ("/no", "no", "n", "skip it", "pass"):
        if _pending_confirmation.get("signal"):
            _pending_confirmation["signal"]  = None
            _pending_confirmation["expires"] = 0
            log_signal(_pending_confirmation.get("signal") or {}, skipped=True)
            tg_send("⏭ Signal rejected. Watching for next setup.")
        else:
            tg_send("No pending signal.")

    # ── /help ──────────────────────────────────────────────────────
    elif any(w in text for w in ["/help", "help", "?", "commands"]):
        tg_send(help_text())

    # ── natural language ───────────────────────────────────────────
    elif any(w in text for w in ["entry", "entries", "explain", "setup", "levels", "signal", "why"]):
        candles      = get_15min_candles()
        session_name, _ = get_session()
        pre_trend    = get_pretend(candles)
        strength     = get_trend_strength(candles)
        price        = get_live_price()
        sig          = _pending_signal["signal"]
        if sig:
            c1, c2, c3 = get_scale(sig["contracts"])
            tg_send(
                f"<b>Current setup:</b>\n\n"
                f"Price: <code>{price:,.2f}</code>  |  {session_name}  |  Trend: {pre_trend}\n"
                f"Pre-trend drop: {strength:.0f}pts over 90min\n\n"
                f"<b>{sig['side']}</b> — {'buying the dip in an uptrend' if sig['side']=='BUY' else 'fading the push in a downtrend'}\n"
                f"Entry @ <code>{sig['entry']}</code>  |  R:R = 1:3.1 to TP3\n"
                f"Scale: {c1}/{c2}/{c3} MNQ at TP1/TP2/TP3\n\n"
                f"{session_name} historically {64 if session_name=='Overnight' else 52}% WR on this setup"
            )
        else:
            tg_send(
                f"<b>Watching for:</b>\n"
                f"• London or Overnight session ✅\n"
                f"• Pre-trend DOWN (price dips 15+ pts over 90min)\n"
                f"• Bigger trend aligned\n"
                f"• Pre-trend drop is real (not just noise)\n\n"
                f"Right now: {session_name}  |  Pre-trend {pre_trend or '?'}  ({strength:.0f}pts)\n"
                f"Price: <code>{price:,.2f}</code>"
            )

    else:
        tg_send(f"Didn't catch that.\n{help_text()}")

def telegram_poll_loop():
    """Long-poll Telegram for incoming messages and handle commands."""
    if not TELEGRAM_TOKEN:
        print("[TG] No token — skipping")
        return

    print("[TG] Starting poll loop...")

    # Drain existing updates so we don't replay old messages on startup
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": -1}, timeout=8
        ).json()
        updates = r.get("result", [])
        if updates:
            _tg_offset["offset"] = updates[-1]["update_id"] + 1
        print(f"[TG] Baseline offset: {_tg_offset['offset']}")
    except Exception as e:
        print(f"[TG] Baseline error: {e}")

    mode_str = (
        "🟢 <b>LIVE MODE</b> — real orders on Topstep" if TRADING_MODE == "live"
        else "📋 Paper mode — simulated trades only"
    )
    confirm_str = "  |  ✋ Confirm mode ON" if CONFIRM_MODE else ""
    _load_account_id()
    tg_send(
        "🤖 <b>Jarvis online v3.1</b>\n\n"
        f"{mode_str}{confirm_str}\n"
        f"Watching MNQM6 — {datetime.now().strftime('%H:%M EST')}\n"
        "Sessions: Overnight ✅  London ✅  NY Fridays only ✅\n"
        "SELLs: A+++ only (NY Friday, triple TF aligned, 30pt+ strength)\n"
        "Type /help for all commands."
    )
    print("[TG] Startup message sent")

    while True:
        try:
            updates = tg_get_updates()
            for upd in updates:
                _tg_offset["offset"] = upd["update_id"] + 1
                msg  = upd.get("message", {})
                text = msg.get("text", "")
                chat_id = str(msg.get("chat", {}).get("id", ""))
                print(f"[TG] Received: '{text}' from {chat_id}")
                if chat_id and TELEGRAM_CHAT and chat_id != str(TELEGRAM_CHAT):
                    print(f"[TG] Ignoring — not authorized chat")
                    continue
                if text:
                    handle_tg_command(text)
        except Exception as e:
            print(f"[TG] Poll error: {e}")
        time.sleep(2)

# ── Live Price (Polygon WebSocket) ────────────────────────────────
_ws_price = {"price": None, "updated": 0}
_ws_connected = {"status": False}

def on_ws_message(ws, message):
    try:
        events = json.loads(message)
        for ev in events:
            evt    = ev.get("ev")
            status = ev.get("status", "")
            if evt == "status" and status == "connected":
                ws.send(json.dumps({"action": "auth", "params": POLYGON_KEY}))
            elif evt == "status" and status == "auth_success":
                ws.send(json.dumps({"action": "subscribe", "params": "T.MNQM6"}))
                _ws_connected["status"] = True
            elif evt == "status" and "error" in status.lower():
                _ws_connected["status"] = False
            elif evt == "T":
                p = ev.get("p")
                if p:
                    _ws_price["price"] = float(p)
                    _ws_price["updated"] = time.time()
    except:
        pass

def on_ws_error(ws, error):
    _ws_connected["status"] = False

def on_ws_close(ws, *args):
    _ws_connected["status"] = False
    print("[WS] Connection closed — loop will reconnect")

_ws_instance = {"ws": None}

def start_ws():
    """
    Single persistent WebSocket to Polygon futures.
    ping_interval=20 keeps Railway from killing the TCP connection.
    On disconnect: wait 20s before reconnecting so Polygon releases
    the old connection (plan allows 1 connection — immediate reconnect
    hits max_connections and fails silently).
    """
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://socket.polygon.io/futures",
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close
            )
            _ws_instance["ws"] = ws
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[WS] Crashed: {e}")
        _ws_connected["status"] = False
        print("[WS] Disconnected — waiting 20s for Polygon to release connection...")
        time.sleep(20)
        print("[WS] Reconnecting...")

def is_market_open():
    """MNQ futures: Sun 6pm ET → Fri 5pm ET, daily break 5-6pm ET."""
    now = datetime.now()
    dow = now.weekday()   # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    h   = now.hour
    # Saturday — always closed
    if dow == 5: return False
    # Sunday — closed before 6pm ET
    if dow == 6 and h < 18: return False
    # Friday — closed at/after 5pm ET
    if dow == 4 and h >= 17: return False
    # Daily maintenance break 5-6pm ET every day
    if h == 17: return False
    return True

def _rest_price():
    """Pull latest MNQ front-month price from Polygon REST snapshot."""
    try:
        url = f"https://api.polygon.io/futures/v1/snapshot?product_code=MNQ&apiKey={POLYGON_KEY}"
        data = requests.get(url, timeout=5).json()
        # Prefer active front month — pick the non-spread result with the highest volume / most recent trade
        candidates = []
        for r in data.get("results", []):
            ticker = r.get("details", {}).get("ticker", "")
            # Skip spreads (contain a dash)
            if "-" in ticker:
                continue
            p = r.get("last_trade", {}).get("price")
            if p:
                candidates.append((ticker, float(p)))
        if not candidates:
            return None
        # Prefer MNQM6 explicitly, otherwise take first outright
        for ticker, price in candidates:
            if ticker == "MNQM6":
                return price
        return candidates[0][1]
    except:
        return None

def get_live_price():
    """
    Return live MNQ price.
    1. WebSocket tick (real-time, Polygon)
    2. yfinance 1m bar (~15s delay, always live)
    3. Last known price
    Polygon REST is excluded — it does not update live during session.
    """
    ws_age = time.time() - _ws_price["updated"] if _ws_price["updated"] else 9999
    if _ws_price["price"] and ws_age < 15:
        return _ws_price["price"]
    # WS stale — use yfinance
    p = _yf_price()
    if p:
        _ws_price["price"]   = p
        _ws_price["updated"] = time.time()
        return p
    return _ws_price["price"]

def is_price_fresh():
    """True if we have a price updated in the last 60 seconds."""
    if not _ws_price["updated"]:
        return False
    return time.time() - _ws_price["updated"] < 60

# ── 15-min candles (yfinance NQ=F) ───────────────────────────────
_candle_cache = {"candles": [], "updated": 0}

def get_15min_candles():
    if time.time() - _candle_cache["updated"] < 60:
        return _candle_cache["candles"]
    try:
        df = yf.Ticker("NQ=F").history(interval="15m", period="5d")
        if len(df) >= 6:
            candles = []
            for ts, row in df.iterrows():
                candles.append({
                    "t": int(ts.timestamp() * 1000),
                    "o": row["Open"], "h": row["High"],
                    "l": row["Low"],  "c": row["Close"]
                })
            _candle_cache["candles"] = candles
            _candle_cache["updated"] = time.time()
            return candles
    except:
        pass
    return _candle_cache["candles"]

# ── Daily + Overnight levels (HTF context) ───────────────────────
_daily_cache = {"data": None, "updated": 0}

def get_daily_levels():
    """Fetch prev day's high/low/close + overnight range. Cached 15min."""
    if time.time() - _daily_cache["updated"] < 900:
        return _daily_cache["data"]
    try:
        df = yf.Ticker("NQ=F").history(interval="1d", period="5d")
        if len(df) < 2:
            return None
        rows = list(df.iterrows())
        prev = rows[-2][1]  # yesterday
        today_row = rows[-1][1]
        # Overnight range: today's low-to-high so far
        overnight_high = round(float(today_row["High"]), 2)
        overnight_low  = round(float(today_row["Low"]),  2)
        levels = {
            "prev_high":       round(float(prev["High"]),  2),
            "prev_low":        round(float(prev["Low"]),   2),
            "prev_close":      round(float(prev["Close"]), 2),
            "overnight_high":  overnight_high,
            "overnight_low":   overnight_low,
            "overnight_range": round(overnight_high - overnight_low, 1),
        }
        _daily_cache["data"]    = levels
        _daily_cache["updated"] = time.time()
        return levels
    except:
        return None

# ── Session Detection ─────────────────────────────────────────────
def get_session():
    h   = datetime.now().hour
    dow = datetime.now().weekday()  # 0=Mon, 4=Fri
    if 17 <= h or h < 3:
        return "Overnight", True
    elif 3 <= h < 9:
        return "London", True
    elif 9 <= h < 16:
        if dow == 4:                # Friday only — weekly close momentum
            return "NY", True
        return "NY", False
    return "Other", False

# ── Pre-trend (last 6 × 15min candles) ───────────────────────────
def get_pretend(candles):
    if len(candles) < 6:
        return None
    closes = [c["c"] for c in candles[-6:]]
    return "DOWN" if closes[-1] < closes[0] else "UP"

# ── Trend strength (how far did it drop over 6 candles) ──────────
def get_trend_strength(candles):
    if len(candles) < 6:
        return 0
    return abs(candles[-6]["c"] - candles[-1]["c"])

# ── Contract Sizing (mirrors Goldmine patterns from 66 trades) ────
# Data: 52% = 5MNQ, 33% = 3MNQ, 8% = 6MNQ, 2% = 7-8MNQ
# Bigger size when: stronger pre-trend drop, overnight session
def get_contracts(session_name, candles):
    strength = get_trend_strength(candles)
    if session_name == "Overnight":
        if strength >= 50: return 6
        return 5
    elif session_name == "London":
        if strength >= 40: return 5
        return 3
    elif session_name == "NY":
        return 3                       # NY always small — choppier
    return 3

# ── Adaptive Logic ────────────────────────────────────────────────
def get_jarvis_form():
    """
    Look at last 10 Jarvis trades to assess current form.
    Returns: (win_rate, streak, should_reduce_size)
    """
    trades = sb_select("trades", extra="&order=id.desc&limit=10")
    jarvis = [t for t in trades if t.get("source") == "JARVIS" and t.get("result") in ("WIN","LOSS")]
    if len(jarvis) < 3:
        return None, 0, False

    wins   = sum(1 for t in jarvis if t["result"] == "WIN")
    wr     = round(wins / len(jarvis) * 100)

    # Current streak
    streak = 0
    last_result = jarvis[0]["result"]
    for t in jarvis:
        if t["result"] == last_result:
            streak += 1
        else:
            break
    streak = streak if last_result == "WIN" else -streak

    # Reduce size if on 3+ loss streak or WR < 30% over last 10
    reduce = streak <= -3 or wr < 30
    return wr, streak, reduce

# ── Recent SELL form — consecutive SELL losses ────────────────────
def get_recent_sell_form():
    """
    Returns consecutive SELL result streak (negative = losses).
    Used to block SELLs after 2+ consecutive SELL losses.
    """
    try:
        trades = sb_select("trades", extra="&source=eq.JARVIS&side=eq.SELL&result=in.(WIN,LOSS)&order=id.desc&limit=5")
        if not trades:
            return 0
        streak = 0
        last = trades[0]["result"]
        for t in trades:
            if t["result"] == last:
                streak += 1
            else:
                break
        return streak if last == "WIN" else -streak
    except:
        return 0

# ── Directional Bias ──────────────────────────────────────────────
def get_directional_bias():
    """
    Look at last 5 closed Jarvis + recent Goldmine trades.
    If 4+ same direction → that's the bias.
    Goldmine ran 13 straight BUYs — once trend is clear, stay with it.
    Returns: "BUY", "SELL", or None
    """
    try:
        trades = sb_select("trades", extra="&order=id.desc&limit=10")
        recent = [t for t in trades if t.get("source") in ("JARVIS","TRAINING")
                  and t.get("result") in ("WIN","LOSS","OPEN")][:5]
        if len(recent) < 3:
            return None
        sides = [t["side"] for t in recent]
        buys  = sides.count("BUY")
        sells = sides.count("SELL")
        if buys >= 4:   return "BUY"
        if sells >= 4:  return "SELL"
    except:
        pass
    return None

# ── Signal Engine ─────────────────────────────────────────────────
def check_for_signal():
    # Rule 1: No open Jarvis trades
    open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
    if open_trades:
        return None, f"Open trade #{open_trades[0]['id']} — watching it"

    # Rule 2: Good session
    session_name, session_ok = get_session()
    if not session_ok:
        # Be descriptive about why
        dow = datetime.now().weekday()
        h   = datetime.now().hour
        if dow <= 3 and 9 <= h < 17:
            return None, "NY session Mon-Thu — sitting out. London/Overnight only."
        return None, f"Waiting for next active session"

    candles = get_15min_candles()
    if len(candles) < 6:
        return None, "Not enough candle data"

    pre_trend = get_pretend(candles)
    strength  = get_trend_strength(candles)

    price = get_live_price()
    if not price:
        return None, "No live price"
    if not is_price_fresh():
        return None, f"Price stale ({round(time.time()-_ws_price['updated'])}s old) — not trading"

    # ── Bigger trend direction (20 candles ≈ 5hrs) ────────────────
    long_closes = [c["c"] for c in candles[-20:]] if len(candles) >= 20 else [c["c"] for c in candles]
    long_trend  = "DOWN" if long_closes[-1] < long_closes[0] else "UP"

    # ── Directional bias from recent trade history ─────────────────
    bias = get_directional_bias()

    # ── Mid-trend (8 candles ≈ 2hrs) ──────────────────────────────
    mid_closes = [c["c"] for c in candles[-8:]] if len(candles) >= 8 else [c["c"] for c in candles]
    mid_trend  = "DOWN" if mid_closes[-1] < mid_closes[0] else "UP"

    # ── Determine side from pre-trend + bigger trend ───────────────
    # Default lean: BUY. 59% WR vs 50% WR on SELLs — BUY is the edge.
    if pre_trend == "DOWN":
        side = "BUY" if long_trend == "UP" else "SELL"
    elif pre_trend == "UP":
        side = "SELL" if long_trend == "DOWN" else "BUY"
    else:
        return None, "No clear pre-trend"

    # ── SELL gate — A+++ setups only ──────────────────────────────
    # Historical data: Overnight SELLs 42% WR, London SELLs 40% WR — no edge.
    # NY (Friday) SELLs: 56% WR — the only session with sell edge.
    # BUYs: 59% WR in ALL sessions — primary edge, default direction.
    if side == "SELL":
        # Rule 1: No SELLs outside NY Friday — data shows no edge elsewhere
        if session_name != "NY":
            # Force to BUY if conditions allow, else skip
            if long_trend == "UP" and pre_trend == "DOWN":
                side = "BUY"  # dip buy — this is what we want
            elif long_trend == "UP" and pre_trend == "UP":
                side = "BUY"  # continuation buy
            else:
                return None, f"No SELLs in {session_name} session (40-42% WR historically). Waiting for BUY setup."

        # Rule 2: Must be fully trend-aligned — no counter-trend sells ever
        if not (long_trend == "DOWN"):
            return None, f"SELL blocked — 5hr trend is UP. Only buy dips in uptrends."

        # Rule 3: Mid-trend must also confirm DOWN — triple timeframe agreement required
        if mid_trend != "DOWN":
            return None, f"SELL blocked — 2hr trend is {mid_trend}. Need 5hr+2hr+pre-trend all DOWN for a sell."

        # Rule 4: Strength requirement is 3x higher for SELLs — need a real move
        if strength < 30:
            return None, f"SELL blocked — pre-trend only {strength:.0f}pts (need 30+ for sells, 10+ for buys)."

        # Rule 5: Block SELLs when BUY bias is active — don't fight the tape
        if bias == "BUY":
            return None, f"SELL blocked — active BUY bias from recent trades. Not shorting into a buy streak."

        # Rule 6: Check recent SELL form — 2+ consecutive sell losses → cool off
        sell_form = get_recent_sell_form()
        if sell_form <= -2:
            return None, f"SELL blocked — {abs(sell_form)} consecutive SELL losses. Sitting out shorts."

    # ── BUY filters (much lighter — BUYs are the primary weapon) ──
    # Minimum pre-trend strength — real move, not noise
    if strength < 10:
        return None, f"Pre-trend too weak ({strength:.0f}pts) — noise"

    # Bias override — counter-bias BUY needs modest extra strength
    if bias and bias != side and side == "BUY":
        if strength < 15:
            return None, f"BUY pre-trend weak ({strength:.0f}pts) + counter to {bias} bias — skipping"

    # ── Rule: Don't re-enter where we just got stopped out ────────
    if (_last_sl["price"] and _last_sl["side"] == side and
            time.time() - _last_sl["time"] < 7200 and
            abs(price - _last_sl["price"]) < 30):
        return None, f"Within 30pts of last SL @ {_last_sl['price']} — respect the level"

    aligned = (side == "BUY" and long_trend == "UP") or (side == "SELL" and long_trend == "DOWN")

    # ── Levels — based on actual Goldmine data ─────────────────────
    entry = round(price, 2)
    d = 1 if side == "BUY" else -1
    sl  = round(entry - d * 40,  2)
    tp1 = round(entry + d * 34,  2)
    tp2 = round(entry + d * 65,  2)
    tp3 = round(entry + d * 100, 2)

    # ── HTF structural context — adjust tp3 if it runs into key level ──
    levels   = get_daily_levels()
    htf_note = ""
    if levels:
        if side == "BUY":
            # Is TP3 above prev day's high? That's a strong resistance zone
            if tp3 > levels["prev_high"] and tp2 <= levels["prev_high"]:
                htf_note = f"⚠️ TP3 runs above prev high ({levels['prev_high']}) — structural resistance"
            elif tp1 > levels["overnight_high"]:
                htf_note = f"⚠️ TP1 above overnight high ({levels['overnight_high']}) — be cautious"
            elif entry > levels["prev_high"] - 30 and entry < levels["prev_high"]:
                htf_note = f"⚠️ Entry near prev day high resistance ({levels['prev_high']}) — tighter setup"
        else:  # SELL
            if tp3 < levels["prev_low"] and tp2 >= levels["prev_low"]:
                htf_note = f"⚠️ TP3 runs below prev low ({levels['prev_low']}) — structural support"
            elif tp1 < levels["overnight_low"]:
                htf_note = f"⚠️ TP1 below overnight low ({levels['overnight_low']}) — be cautious"
            elif entry < levels["prev_low"] + 30 and entry > levels["prev_low"]:
                htf_note = f"⚠️ Entry near prev day low support ({levels['prev_low']}) — tighter setup"

    contracts = get_contracts(session_name, candles)

    # Adapt size down on losing streak
    wr, streak, reduce = get_jarvis_form()
    if reduce:
        contracts = max(3, contracts - 2)

    signal = {
        "side":       side,
        "entry":      entry,
        "sl":         sl,
        "tp1":        tp1,
        "tp2":        tp2,
        "tp3":        tp3,
        "contracts":  contracts,
        "session":    session_name,
        "pre_trend":  pre_trend,
        "strength":   round(strength, 1),
        "aligned":    aligned,
        "long_trend": long_trend,
        "mid_trend":  mid_trend,
        "bias":       bias,
        "htf_note":   htf_note,
        "levels":     levels,
        "time":       datetime.now().strftime("%H:%M EST")
    }
    return signal, "SIGNAL"

# ── Build signal Telegram message ─────────────────────────────────
def format_signal_message(sig):
    # (unused by auto-trader but kept for reference)
    pass

# ── Stats ─────────────────────────────────────────────────────────
def get_stats():
    all_trades = sb_select("trades")
    jarvis   = [t for t in all_trades if t.get("source") == "JARVIS"]
    training = [t for t in all_trades if t.get("source") == "TRAINING"]
    resolved_t = [t for t in training if t.get("result") in ("WIN", "LOSS")]

    j_wins   = [t for t in jarvis if t.get("result") == "WIN"]
    j_losses = [t for t in jarvis if t.get("result") == "LOSS"]
    j_open   = [t for t in jarvis if t.get("result") == "OPEN"]
    total_pnl = sum(t.get("pnl_usd") or 0 for t in jarvis)
    balance   = STARTING_BALANCE + total_pnl

    t_wins = [t for t in resolved_t if t.get("result") == "WIN"]

    return {
        "balance":       round(balance, 2),
        "total_pnl":     round(total_pnl, 2),
        "jarvis_trades": len(jarvis),
        "jarvis_wins":   len(j_wins),
        "jarvis_losses": len(j_losses),
        "jarvis_open":   len(j_open),
        "jarvis_wr":     round(len(j_wins) / max(len(j_wins) + len(j_losses), 1) * 100),
        "training_total": len(training),
        "training_wr":   round(len(t_wins) / max(len(resolved_t), 1) * 100),
        "open_trades":   j_open
    }

# ── Eval Account ─────────────────────────────────────────────────
def get_eval_status():
    """Compute current eval standing from all-time Jarvis P&L."""
    all_trades = sb_select("trades", extra="&source=eq.JARVIS")
    total_pnl  = sum(t.get("pnl_usd") or 0 for t in all_trades if t.get("result") in ("WIN","LOSS"))

    # Offset P&L by checkpoints (each funded/blown event resets the eval)
    pnl_offset = _eval_checkpoints[-1]["total_pnl"] if _eval_checkpoints else 0
    eval_pnl   = total_pnl - pnl_offset
    eval_bal   = EVAL_START + eval_pnl

    funded = sum(1 for c in _eval_checkpoints if c["type"] == "funded")
    blown  = sum(1 for c in _eval_checkpoints if c["type"] == "blown")
    eval_num = funded + blown + 1  # which eval we're on

    return {
        "eval_balance":  round(eval_bal, 2),
        "eval_pnl":      round(eval_pnl, 2),
        "total_pnl":     round(total_pnl, 2),
        "funded":        funded,
        "blown":         blown,
        "eval_num":      eval_num,
        "to_target":     round(EVAL_TARGET - eval_bal, 2),
        "to_floor":      round(eval_bal - EVAL_FLOOR, 2),
        "drawdown_used": round(EVAL_START - min(eval_bal, EVAL_START), 2),
    }

def check_eval_thresholds():
    """Alert and reset eval when funded or blown."""
    ev = get_eval_status()
    bal = ev["eval_balance"]
    key = f"{ev['eval_num']}_{round(bal)}"

    if _eval_alerted["last"] == key:
        return  # already alerted this state

    if bal >= EVAL_TARGET:
        _eval_alerted["last"] = key
        cp = {"type": "funded", "total_pnl": ev["total_pnl"], "date": datetime.now().isoformat()}
        _eval_checkpoints.append(cp)
        save_eval_checkpoint(cp)
        ev2 = get_eval_status()
        tg_send(
            f"🏆 <b>EVAL FUNDED — #{ev['eval_num']}</b>\n\n"
            f"Account hit ${EVAL_TARGET:,.0f}  (+$3,000)\n"
            f"Total funded: {ev2['funded']}  |  Blown: {ev2['blown']}\n\n"
            f"Starting Eval #{ev2['eval_num']} — fresh $50k, let's go."
        )
    elif bal <= EVAL_FLOOR:
        _eval_alerted["last"] = key
        cp = {"type": "blown", "total_pnl": ev["total_pnl"], "date": datetime.now().isoformat()}
        _eval_checkpoints.append(cp)
        save_eval_checkpoint(cp)
        ev2 = get_eval_status()
        tg_send(
            f"💀 <b>EVAL BLOWN — #{ev['eval_num']}</b>\n\n"
            f"Hit max drawdown — account at ${bal:,.2f}\n"
            f"Total blown: {ev2['blown']}  |  Funded: {ev2['funded']}\n\n"
            f"Starting Eval #{ev2['eval_num']} — reset. Let's be smarter."
        )

# ── Flask Routes ──────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/status")
def api_status():
    price = get_live_price()
    session_name, session_ok = get_session()
    candles = get_15min_candles()
    pre_trend = get_pretend(candles)
    signal, status_msg = check_for_signal()
    stats = get_stats()
    return jsonify({
        "price":        price,
        "session":      session_name,
        "session_ok":   session_ok,
        "pre_trend":    pre_trend,
        "status_msg":   status_msg,
        "signal":       signal,
        "stats":        stats,
        "ws_connected": _ws_connected["status"],
        "price_age":    round(time.time() - _ws_price["updated"], 1) if _ws_price["updated"] else None,
        "time":         datetime.now().strftime("%H:%M:%S EST")
    })

@app.route("/api/take_signal", methods=["POST"])
def take_signal_api():
    data = request.json
    sig = data.get("signal")
    if not sig:
        return jsonify({"error": "no signal"}), 400
    _pending_signal["signal"] = sig
    handle_tg_command("/take")
    return jsonify({"ok": True})

@app.route("/api/skip_signal", methods=["POST"])
def skip_signal_api():
    _pending_signal["signal"] = None
    tg_send("⏭ Signal skipped via dashboard.")
    return jsonify({"ok": True})

@app.route("/api/trades")
def api_trades():
    trades = sb_select("trades", extra="&order=id.desc&limit=50")
    return jsonify(trades)

@app.route("/api/close_trade", methods=["POST"])
def close_trade():
    data = request.json
    trade_id = data.get("id")
    price = get_live_price()
    trade = sb_select("trades", {"id": trade_id})
    if not trade:
        return jsonify({"error": "not found"}), 404
    t = trade[0]
    pts = (price - t["entry"]) if t["side"] == "BUY" else (t["entry"] - price)
    pnl_usd = round(pts * PTS_TO_USD * (t.get("contracts") or 5), 2)
    sb_update("trades", trade_id, {
        "result":    "WIN" if pts > 0 else "LOSS",
        "pnl_pts":   round(pts, 2),
        "pnl_usd":   pnl_usd,
        "closed_at": datetime.now().isoformat(),
        "notes":     f"Manually closed @ {price}"
    })
    tg_send(f"🔒 Trade #{trade_id} manually closed @ {price}  |  ${pnl_usd:+,.2f}")
    return jsonify({"ok": True, "pnl_usd": pnl_usd})

# ── Background Monitor ────────────────────────────────────────────
def log_signal(signal, taken=False, skipped=False):
    """Persist every signal Jarvis sees to signals_log for memory/learning."""
    try:
        sb_insert("signals_log", {
            "side":       signal["side"],
            "entry":      signal["entry"],
            "sl":         signal["sl"],
            "tp1":        signal["tp1"],
            "tp2":        signal["tp2"],
            "tp3":        signal["tp3"],
            "contracts":  signal["contracts"],
            "session":    signal["session"],
            "strength":   signal["strength"],
            "taken":      taken,
            "skipped":    skipped,
            "notes":      f"pre_trend:{signal['pre_trend']} aligned:{signal.get('aligned')}"
        })
    except:
        pass

def build_entry_bio(signal):
    """
    Plain English narrative of WHY Jarvis took this trade.
    This goes into Supabase notes AND Google Sheets 'Why' column.
    Written so a human can read it a year later and understand.
    """
    side      = signal["side"]
    contracts = signal["contracts"]
    session   = signal["session"]
    pre_trend = signal.get("pre_trend", "?")
    strength  = signal.get("strength", 0)
    long_trend= signal.get("long_trend", "?")
    aligned   = signal.get("aligned", False)
    bias      = signal.get("bias")
    entry     = signal["entry"]
    sl        = signal["sl"]
    tp3       = signal["tp3"]
    htf_note  = signal.get("htf_note", "")

    # Setup description
    if side == "BUY" and pre_trend == "DOWN":
        setup = f"dip buy — price pulled back {strength:.0f}pts in the last 90min, buying the dip into an uptrend"
    elif side == "BUY" and pre_trend == "UP":
        setup = f"trend continuation buy — price climbing {strength:.0f}pts, riding the uptrend"
    elif side == "SELL" and pre_trend == "UP":
        setup = f"fade sell — price pushed up {strength:.0f}pts into a downtrend, fading the bounce"
    elif side == "SELL" and pre_trend == "DOWN":
        setup = f"trend continuation sell — price falling {strength:.0f}pts, riding the downtrend"
    else:
        setup = f"momentum trade — {side} on {strength:.0f}pt pre-trend move"

    align_str = "trend aligned" if aligned else "counter-trend (needed 20pt+ pre-trend to qualify)"
    bias_str  = f" Last 4-5 trades were {bias}s so directional bias confirmed." if bias == side else (
                f" Going against recent {bias} bias — needed extra strength to qualify." if bias else "")

    levels  = signal.get("levels") or {}
    htf_str = ""
    if levels:
        htf_str = (f" Prev day range: {levels.get('prev_low','?')}-{levels.get('prev_high','?')}."
                   f" Overnight: {levels.get('overnight_low','?')}-{levels.get('overnight_high','?')}.")

    warn_str = f" NOTE: {htf_note}." if htf_note else ""

    risk_pts = round(abs(entry - sl))
    max_pts  = round(abs(tp3 - entry))

    bio = (
        f"{side} {contracts} MNQ @ {entry} — {setup}. "
        f"{session} session, 5hr trend {long_trend}, {align_str}.{bias_str}"
        f" Risk: {risk_pts}pts SL @ {sl}. Max target: {max_pts}pts @ TP3 {tp3}."
        f"{htf_str}{warn_str}"
    )
    return bio

def build_close_bio(trade, result, tp1h, tp2h, tp3h, slh, pnl_usd, pts):
    """Plain English narrative of how the trade closed."""
    side  = trade.get("side")
    entry = trade.get("entry")
    sl    = trade.get("sl")
    tp1   = trade.get("tp1")
    tp2   = trade.get("tp2")
    tp3   = trade.get("tp3")
    contracts = trade.get("contracts", 5)

    if tp3h:
        close_story = f"Hit all 3 targets — ran the full {round(abs(tp3-entry))}pts to TP3 @ {tp3}. Clean trend continuation."
    elif tp2h and not slh:
        close_story = f"Hit TP1 + TP2, then reversed before TP3. SL moved to breakeven after TP2, closed there. Partial win."
    elif tp2h and slh:
        close_story = f"Hit TP1 + TP2 (SL at breakeven), remaining contract closed at entry. Locked partial profit."
    elif tp1h:
        close_story = f"Hit TP1 only, then reversed. Took partial profit but didn't reach TP2."
    elif slh:
        stop_pts = round(abs(entry - sl))
        close_story = f"Stopped out — SL hit @ {sl}, -{stop_pts}pts. Price moved against the setup without reaching TP1."
    else:
        close_story = f"Closed — {result}."

    return f"{close_story} P&L: ${pnl_usd:+.2f} ({contracts} MNQ)."

def auto_enter_trade(signal):
    """Auto-log trade to Supabase and alert Telegram — no manual /take needed."""
    bio = build_entry_bio(signal)
    trade = {
        "trade_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "side":       signal["side"],
        "entry":      signal["entry"],
        "sl":         signal["sl"],
        "tp1":        signal["tp1"],
        "tp2":        signal["tp2"],
        "tp3":        signal["tp3"],
        "contracts":  signal["contracts"],
        "trader":     "JARVIS",
        "source":     "JARVIS",
        "result":     "OPEN",
        "notes":      bio
    }
    logged = sb_insert("trades", trade)
    if not logged:
        return

    c1, c2, c3 = get_scale(signal["contracts"])
    risk_usd = round(40  * PTS_TO_USD * signal["contracts"])
    tp1_usd  = round(34  * PTS_TO_USD * c1)
    tp2_usd  = round(65  * PTS_TO_USD * c2)
    tp3_usd  = round(100 * PTS_TO_USD * c3)
    max_usd  = tp1_usd + tp2_usd + tp3_usd
    side_emoji = "🟢" if signal["side"] == "BUY" else "🔴"
    session_wr = 64 if signal["session"] == "Overnight" else 52

    wr, streak, reduce = get_jarvis_form()
    form_str = ""
    if streak >= 3:   form_str = f"🔥 {streak} win streak"
    elif streak <= -2: form_str = f"⚠️ {abs(streak)} losses — sized down"
    elif wr:           form_str = f"📈 {wr}% WR last 10"

    aligned_str = "✅ trend aligned" if signal.get("aligned") else "⚠️ counter-trend"

    bias = signal.get("bias")
    pre  = signal.get("pre_trend", "?")
    lt   = signal.get("long_trend", "?")
    if signal["side"] == "BUY":
        setup_str = "Dip buy — short pullback in uptrend" if pre == "DOWN" else "Continuation — trend still climbing"
    else:
        setup_str = "Fade sell — push into downtrend" if pre == "UP" else "Continuation — trend still falling"

    # Pull multi-timeframe context for the entry message
    candles = get_15min_candles()
    mid_closes  = [c["c"] for c in candles[-10:]] if len(candles) >= 10 else []
    short_closes = [c["c"] for c in candles[-3:]] if len(candles) >= 3 else []
    long_closes  = [c["c"] for c in candles[-20:]] if len(candles) >= 20 else []
    long_move  = round(long_closes[-1] - long_closes[0], 0) if len(long_closes) >= 2 else 0
    mid_move   = round(mid_closes[-1] - mid_closes[0], 0) if len(mid_closes) >= 2 else 0
    short_move = round(short_closes[-1] - short_closes[0], 0) if len(short_closes) >= 2 else 0
    bias_str   = f"  |  🔄 {bias} bias" if bias else ""

    ev = get_eval_status()

    # HTF levels for context in entry message
    levels   = signal.get("levels") or {}
    htf_note = signal.get("htf_note", "")
    htf_str  = ""
    if levels:
        htf_str = (f"\n<b>HTF:</b>\n"
                   f"  Prev day: {levels.get('prev_low','?')} – {levels.get('prev_high','?')}\n"
                   f"  Overnight: {levels.get('overnight_low','?')} – {levels.get('overnight_high','?')}")
    htf_warn = f"\n{htf_note}" if htf_note else ""

    tg_send(
        f"{side_emoji} <b>JARVIS ENTERED — #{logged['id']}</b>\n\n"
        f"<b>{signal['side']} {signal['contracts']} MNQ</b> @ <code>{signal['entry']}</code>\n\n"
        f"<b>Why:</b>\n"
        f"  5hr:   {lt} ({long_move:+.0f}pts)\n"
        f"  2hr:   {signal.get('mid_trend', ('DOWN' if mid_move < 0 else 'UP'))} ({mid_move:+.0f}pts)\n"
        f"  45min: {'DOWN' if short_move < 0 else 'UP'} ({short_move:+.0f}pts)\n"
        + (f"  ⚠️ A+++ SELL — all 3 TFs DOWN, strength {signal.get('strength',0):.0f}pts, NY only\n" if signal["side"]=="SELL" else "")
        + f"  → {setup_str}{bias_str}"
        f"{htf_str}"
        f"{htf_warn}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💀 Risk:  {signal['contracts']}MNQ × 40pts = <b>−${risk_usd}</b>\n"
        f"TP1: <code>{signal['tp1']}</code>  {c1}MNQ × 34pts = +${tp1_usd}\n"
        f"TP2: <code>{signal['tp2']}</code>  {c2}MNQ × 65pts = +${tp2_usd}  ← SL→BE here\n"
        f"TP3: <code>{signal['tp3']}</code>  {c3}MNQ × 100pts = +${tp3_usd}\n"
        f"Max: <b>+${max_usd}</b>  |  R:R = 1:{round(max_usd/max(risk_usd,1), 1)}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏦 Eval: ${ev['eval_balance']:,.2f}  |  ${ev['to_target']:,.0f} to pass  |  ${ev['to_floor']:,.0f} DD left\n"
        f"📊 {signal['session']} ({session_wr}% WR)  |  {aligned_str}"
        + (f"\n{form_str}" if form_str else "") +
        f"\n\n/progress for live updates 🤖"
    )
    return logged["id"]

def check_open_jarvis_trades():
    """Monitor open trades — resolve TP/SL hits, move SL to BE at TP2."""
    price = get_live_price()
    if not price:
        return

    open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
    for trade in open_trades:
        side      = trade.get("side")
        entry     = trade.get("entry")
        sl        = trade.get("sl")
        tp1       = trade.get("tp1")
        tp2       = trade.get("tp2")
        tp3       = trade.get("tp3")
        contracts = trade.get("contracts", 5)
        tp2_hit   = trade.get("tp2_hit", False)

        if not all([entry, sl]):
            continue

        c1, c2, c3 = get_scale(contracts)

        # Move SL to breakeven when TP2 is hit (only do this once)
        if not tp2_hit:
            tp2_crossed = (side == "BUY" and tp2 and price >= tp2) or \
                          (side == "SELL" and tp2 and price <= tp2)
            if tp2_crossed:
                # Move SL to entry (breakeven), mark TP1+TP2 hit
                sb_update("trades", trade["id"], {
                    "sl": entry,
                    "tp1_hit": True,
                    "tp2_hit": True
                })
                tg_send(
                    f"⚡ <b>TP2 HIT — SL → Breakeven</b>\n\n"
                    f"Trade #{trade['id']}  |  {side} {contracts}MNQ\n"
                    f"TP2 @ <code>{tp2}</code>  ✅\n"
                    f"SL moved to entry <code>{entry}</code> — <b>risk-free now</b>\n"
                    f"Targeting TP3 @ <code>{tp3}</code>  (+${round(100*PTS_TO_USD*c3)})"
                )
                sl = entry  # use updated SL for rest of checks
                trade["tp2_hit"] = True

        # Read already-hit TPs from DB — must carry through to final P&L
        tp1_already = trade.get("tp1_hit", False)
        tp2_already = trade.get("tp2_hit", False)

        # Check for final resolution
        result = tp1h = tp2h = tp3h = slh = None
        pts = 0

        if side == "BUY":
            if tp3 and price >= tp3:
                result="WIN"; tp1h=tp2h=tp3h=True; slh=False; pts=tp3-entry
            elif tp2_already and price <= sl:
                # TP2 was hit, SL moved to BE — guaranteed win
                result="WIN"; tp1h=True; tp2h=True; tp3h=False; slh=True; pts=0
            elif tp1_already and price <= sl:
                # TP1 was hit, then stopped — partial WIN (TP1 profit minus remaining loss)
                result="WIN"; tp1h=True; tp2h=False; tp3h=False; slh=True; pts=tp1-entry
            elif tp1 and price >= tp1 and not tp1_already:
                sb_update("trades", trade["id"], {"tp1_hit": True})
                tg_send(f"✅ <b>TP1 hit</b> — Trade #{trade['id']}  |  +{round(tp1-entry)}pts on {c1}MNQ  |  Holding {c2+c3} for TP2/TP3")
            elif price <= sl:
                result="LOSS"; tp1h=False; tp2h=False; tp3h=False; slh=True; pts=sl-entry
        else:
            if tp3 and price <= tp3:
                result="WIN"; tp1h=tp2h=tp3h=True; slh=False; pts=entry-tp3
            elif tp2_already and price >= sl:
                result="WIN"; tp1h=True; tp2h=True; tp3h=False; slh=True; pts=0
            elif tp1_already and price >= sl:
                # TP1 was hit, then stopped — partial WIN
                result="WIN"; tp1h=True; tp2h=False; tp3h=False; slh=True; pts=entry-tp1
            elif tp1 and price <= tp1 and not tp1_already:
                sb_update("trades", trade["id"], {"tp1_hit": True})
                tg_send(f"✅ <b>TP1 hit</b> — Trade #{trade['id']}  |  +{round(entry-tp1)}pts on {c1}MNQ  |  Holding {c2+c3} for TP2/TP3")
            elif price >= sl:
                result="LOSS"; tp1h=False; tp2h=False; tp3h=False; slh=True; pts=entry-sl

        if result:
            pnl_usd = calc_scaled_pnl(entry, side, tp1, tp2, tp3, sl, contracts,
                                       bool(tp1h), bool(tp2h), bool(tp3h), bool(slh))
            sb_update("trades", trade["id"], {
                "result": result, "tp1_hit": bool(tp1h), "tp2_hit": bool(tp2h),
                "tp3_hit": bool(tp3h), "sl_hit": bool(slh),
                "pnl_pts": round(pts, 2), "pnl_usd": pnl_usd,
                "closed_at": datetime.now().isoformat()
            })

            # Set cooldown — Win=30min, Loss=60min
            if result == "WIN":
                _cooldown["until"]  = time.time() + 1800
                _cooldown["reason"] = "WIN — 30min cooldown"
            else:
                _cooldown["until"]  = time.time() + 3600
                _cooldown["reason"] = "LOSS — 60min cooldown"
                # Remember where SL got hit so we don't re-enter there
                _last_sl["price"] = sl
                _last_sl["side"]  = side
                _last_sl["time"]  = time.time()

            ev = get_eval_status()
            if result == "WIN":
                tps = " → ".join([x for x, h in [("TP1", tp1h), ("TP2", tp2h), ("TP3", tp3h)] if h])
                msg = (
                    f"✅ <b>WIN — #{trade['id']}</b>\n\n"
                    f"{side} {contracts}MNQ  |  {tps}\n"
                    f"+{round(pts)}pts  |  <b>+${pnl_usd:,.2f}</b>\n\n"
                    f"🏦 Eval: ${ev['eval_balance']:,.2f}  (${ev['to_target']:,.0f} to target)\n"
                    f"Cooling down 30min. Back to watching. 👀"
                )
            else:
                msg = (
                    f"❌ <b>LOSS — #{trade['id']}</b>\n\n"
                    f"{side} {contracts}MNQ  |  SL hit @ <code>{sl}</code>\n"
                    f"{round(pts)}pts  |  <b>−${abs(pnl_usd):,.2f}</b>\n\n"
                    f"🏦 Eval: ${ev['eval_balance']:,.2f}  (${ev['to_floor']:,.0f} DD buffer left)\n"
                    f"Cooling down 60min. Back to watching."
                )
            close_bio = build_close_bio(trade, result, tp1h, tp2h, tp3h, slh, pnl_usd, pts)
            # Append close bio to the trade notes in DB
            existing_notes = trade.get("notes") or ""
            sb_update("trades", trade["id"], {"notes": existing_notes + "  ||  " + close_bio})

            tg_send(msg)
            push_trade_to_sheets(trade["id"], close_bio=close_bio)
            threading.Thread(target=send_post_trade_analysis,
                             args=(trade, result, pnl_usd, tp1h, tp2h, tp3h, slh, pts),
                             daemon=True).start()

def send_post_trade_analysis(trade, result, pnl_usd, tp1h, tp2h, tp3h, slh, pts):
    """
    Fire during cooldown after trade closes.
    Uses Claude to analyze what happened and what to watch for next.
    """
    time.sleep(5)  # small delay so it doesn't collide with the close message
    side      = trade.get("side")
    entry     = trade.get("entry")
    sl        = trade.get("sl")
    tp1       = trade.get("tp1")
    tp2       = trade.get("tp2")
    tp3       = trade.get("tp3")
    contracts = trade.get("contracts", 5)
    notes     = trade.get("notes","")
    session   = notes.split("Session:")[-1].split()[0] if "Session:" in notes else "?"
    strength  = notes.split("Strength:")[-1].split()[0] if "Strength:" in notes else "?"
    aligned   = "aligned" if "Aligned:True" in notes else "counter-trend"

    tp_reached = ("TP3" if tp3h else "TP2" if tp2h else "TP1" if tp1h else "none — SL hit")
    cooldown_mins = 30 if result == "WIN" else 60
    price_now = get_live_price() or entry

    ai_text = ""
    if ANTHROPIC_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
            msg = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=200,
                messages=[{"role":"user","content":
                    f"""You are Jarvis, MNQ trading algo. A trade just closed. Give a quick 2-3 sentence analysis like a sharp trading partner reviewing the tape. Be specific to the numbers, not generic.

Trade: {side} {contracts}MNQ @ {entry}
SL: {sl}  TP1: {tp1}  TP2: {tp2}  TP3: {tp3}
Result: {result} — reached {tp_reached} — P&L: ${pnl_usd:+.2f}
Session: {session}  Pre-trend strength: {strength}pts  Direction: {aligned}
Current price: {price_now}

Cover: what the trade did (did it move cleanly or chop?), one pattern worth noting, and what the cooldown period should focus on watching for."""
                }]
            )
            ai_text = msg.content[0].text
        except:
            pass

    if not ai_text:
        if result == "WIN":
            ai_text = (f"Reached {tp_reached} — {'clean move, trend held' if tp3h else 'partial, reversed before TP3'}. "
                       f"Cooldown: watch for next pullback to form cleanly.")
        else:
            ai_text = (f"SL hit — price moved {round(abs(price_now - entry))}pts against entry. "
                       f"Reassessing direction over the next hour before next trade.")

    candles      = get_15min_candles()
    long_closes  = [c["c"] for c in candles[-20:]] if len(candles) >= 20 else []
    long_trend   = ("DOWN" if long_closes[-1] < long_closes[0] else "UP") if len(long_closes) >= 2 else "?"
    long_move    = round(long_closes[-1] - long_closes[0], 0) if len(long_closes) >= 2 else 0

    tg_send(
        f"🔍 <b>Post-trade read</b>\n\n"
        f"{ai_text}\n\n"
        f"Bigger trend still: <b>{long_trend}</b> ({long_move:+.0f}pts 5hr)\n"
        f"Cooling down {cooldown_mins}min — next entry after {(datetime.now() + timedelta(minutes=cooldown_mins)).strftime('%H:%M')}"
    )

def send_market_analysis():
    """Full market read — what Jarvis sees right now, where it thinks price goes, what it needs for a setup."""
    price        = get_live_price()
    candles      = get_15min_candles()
    session_name, session_ok = get_session()
    pre_trend    = get_pretend(candles)
    strength     = get_trend_strength(candles)
    bias         = get_directional_bias()

    if not price or len(candles) < 6:
        tg_send("Not enough data yet — still warming up.")
        return

    # Bigger trend (20 candles = ~5hrs of 15min bars)
    long_closes = [c["c"] for c in candles[-20:]] if len(candles) >= 20 else [c["c"] for c in candles]
    long_trend  = "DOWN" if long_closes[-1] < long_closes[0] else "UP"
    long_move   = round(long_closes[-1] - long_closes[0], 1)

    # Medium trend (10 candles = ~2.5hrs)
    mid_closes  = [c["c"] for c in candles[-10:]] if len(candles) >= 10 else [c["c"] for c in candles]
    mid_trend   = "DOWN" if mid_closes[-1] < mid_closes[0] else "UP"
    mid_move    = round(mid_closes[-1] - mid_closes[0], 1)

    # Short (last 3 candles = ~45min)
    short_closes = [c["c"] for c in candles[-3:]]
    short_trend  = "DOWN" if short_closes[-1] < short_closes[0] else "UP"
    short_move   = round(short_closes[-1] - short_closes[0], 1)

    # Key levels — recent highs/lows
    recent_high = round(max(c["h"] for c in candles[-20:]), 1)
    recent_low  = round(min(c["l"] for c in candles[-20:]), 1)
    range_size  = round(recent_high - recent_low, 1)
    range_pct   = round((price - recent_low) / range_size * 100) if range_size > 0 else 50

    # Where price is in the range
    if range_pct >= 75:
        range_pos = f"near top of range ({range_pct}%)"
    elif range_pct <= 25:
        range_pos = f"near bottom of range ({range_pct}%)"
    else:
        range_pos = f"mid-range ({range_pct}%)"

    # Signal readiness
    open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
    in_trade = bool(open_trades)

    # What would trigger a setup
    if in_trade:
        setup_needed = f"Already in trade #{open_trades[0]['id']} — not looking for new entry"
    elif not session_ok:
        setup_needed = f"NY session — sitting out. Next window: Overnight starts ~5pm ET"
    else:
        if pre_trend == "DOWN" and long_trend == "UP":
            setup_needed = "✅ BUY setup conditions met — pre-trend dip in uptrend. Could fire now."
        elif pre_trend == "UP" and long_trend == "DOWN":
            setup_needed = "✅ SELL setup conditions met — push into downtrend. Could fire now."
        elif pre_trend == "DOWN" and long_trend == "DOWN":
            setup_needed = "✅ SELL continuation conditions met — trending down. Could fire now."
        elif pre_trend == "UP" and long_trend == "UP":
            setup_needed = "✅ BUY continuation conditions met — trending up. Could fire now."
        elif strength < 10:
            setup_needed = f"Pre-trend too weak ({strength:.0f}pts) — need at least 10pts of directional move on 15min"
        else:
            setup_needed = f"Pre-trend {pre_trend} but need a cleaner directional read"

    # Bias context
    bias_str = f"\n🔄 <b>Recent bias: {bias}</b> — last 4/5 trades same direction" if bias else ""

    # HTF daily levels
    levels   = get_daily_levels()
    htf_str  = ""
    htf_context = ""
    if levels:
        htf_str = (f"\n<b>Key levels:</b>\n"
                   f"  Prev day: {levels['prev_low']} – {levels['prev_high']}  (close: {levels['prev_close']})\n"
                   f"  Overnight: {levels['overnight_low']} – {levels['overnight_high']}  ({levels['overnight_range']}pts range)")
        htf_context = (f"Prev day high/low: {levels['prev_high']}/{levels['prev_low']} (close: {levels['prev_close']})\n"
                       f"Overnight range: {levels['overnight_low']}-{levels['overnight_high']}\n"
                       f"Price relative to: {'above prev high' if price > levels['prev_high'] else 'below prev low' if price < levels['prev_low'] else 'inside prev range'}")

    # Recent Jarvis trade history for Claude context
    recent_trades = sb_select("trades", extra="&source=eq.JARVIS&order=id.desc&limit=8")
    recent_closed = [t for t in recent_trades if t.get("result") in ("WIN","LOSS")]
    trade_history_str = ""
    if recent_closed:
        buy_wins  = sum(1 for t in recent_closed if t["side"]=="BUY"  and t["result"]=="WIN")
        buy_loss  = sum(1 for t in recent_closed if t["side"]=="BUY"  and t["result"]=="LOSS")
        sell_wins = sum(1 for t in recent_closed if t["side"]=="SELL" and t["result"]=="WIN")
        sell_loss = sum(1 for t in recent_closed if t["side"]=="SELL" and t["result"]=="LOSS")
        tp3_pct   = round(sum(1 for t in recent_closed if t.get("tp3_hit")) / max(len(recent_closed),1) * 100)
        trade_history_str = (f"Recent performance: BUY {buy_wins}W/{buy_loss}L  SELL {sell_wins}W/{sell_loss}L\n"
                             f"TP3 hit rate recent: {tp3_pct}%\n"
                             f"Last 3 results: {[t['result'] for t in recent_closed[:3]]}")

    # Build the AI analysis if we have the key
    ai_read = ""
    if ANTHROPIC_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
            candle_summary = f"Last 6 closes (15min): {[round(c['c'],1) for c in candles[-6:]]}"
            msg = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=250,
                messages=[{
                    "role": "user",
                    "content": f"""You are Jarvis, an MNQ futures algo. Give a SHORT market read in 2-3 sentences. Casual, direct, like a trader talking to another trader. Cover: what the price action looks like right now relative to key levels, where you think it's likely to go next (up/down/chop), and why. Reference actual numbers. No fluff, no disclaimers.

Current price: {price}
Session: {session_name} ({'active' if session_ok else 'sitting out'})
5hr trend: {long_trend} ({long_move:+.0f}pts)
2.5hr trend: {mid_trend} ({mid_move:+.0f}pts)
45min trend: {short_trend} ({short_move:+.0f}pts)
Range (20 candles): {recent_low} – {recent_high} ({range_size}pts), price at {range_pos}
{htf_context}
Recent trade bias: {bias or 'neutral'}
{trade_history_str}
{candle_summary}"""
                }]
            )
            ai_read = "\n\n🧠 <b>Read:</b>\n" + msg.content[0].text
        except:
            pass

    if not ai_read:
        # fallback without AI
        if long_trend == mid_trend == short_trend:
            direction = "strongly trending " + long_trend.lower()
            outlook   = "likely continues " + long_trend.lower()
        elif long_trend != short_trend:
            direction = f"bigger trend {long_trend} but short-term {short_trend}"
            outlook   = "could be a reversal setup forming" if strength > 15 else "chop — no clear edge"
        else:
            direction = f"trending {long_trend}"
            outlook   = "momentum with it"
        ai_read = f"\n\n💬 <b>Read:</b> {direction.capitalize()}. {outlook.capitalize()}."

    tg_send(
        f"📊 <b>Jarvis Analysis — {datetime.now().strftime('%H:%M EST')}</b>\n\n"
        f"Price: <code>{price:,.2f}</code>  |  {session_name}{'✅' if session_ok else '🔴'}\n\n"
        f"<b>Trends:</b>\n"
        f"  5hr:   {long_trend} ({long_move:+.0f}pts)\n"
        f"  2.5hr: {mid_trend} ({mid_move:+.0f}pts)\n"
        f"  45min: {short_trend} ({short_move:+.0f}pts)\n\n"
        f"<b>Range (20c):</b> {recent_low} – {recent_high}  ({range_size}pts)\n"
        f"Price at {range_pos}"
        f"{htf_str}"
        f"{bias_str}"
        f"{ai_read}\n\n"
        f"<b>Setup status:</b>\n{setup_needed}"
    )

# ── Google Sheets (Apps Script webhook) ───────────────────────────
def sheets_post(payload):
    """POST to Google Apps Script web app. No OAuth needed.
    Apps Script returns 302 redirect — must send as GET with params."""
    if not GOOGLE_SHEETS_URL:
        return
    try:
        # Apps Script web apps require a GET with the payload as a query param
        # OR a POST that follows the 302 redirect (which converts to GET)
        import urllib.parse
        data_str = urllib.parse.quote(json.dumps(payload))
        url = f"{GOOGLE_SHEETS_URL}?data={data_str}"
        r = requests.get(url, timeout=10)
        print(f"[SHEETS] Pushed {payload.get('action')} — {r.status_code}")
    except Exception as e:
        print(f"[SHEETS] Error: {e}")

def push_trade_to_sheets(trade_id, close_bio=""):
    """Push a completed trade to Google Sheets with full human-readable bio."""
    if not GOOGLE_SHEETS_URL:
        return
    try:
        trade = sb_select("trades", {"id": trade_id})
        if not trade: return
        t  = trade[0]
        ev = get_eval_status()
        dt = (t.get("trade_date") or "")
        tp_hit = ("TP3" if t.get("tp3_hit") else
                  "TP2" if t.get("tp2_hit") else
                  "TP1" if t.get("tp1_hit") else
                  "SL"  if t.get("sl_hit")  else "OPEN")

        # The notes field IS the entry bio now (plain English)
        entry_bio = t.get("notes") or ""
        # Strip old-format notes that have colons (legacy trades)
        if "Session:" in entry_bio or "AUTO-CANCELED" in entry_bio or "restored" in entry_bio:
            entry_bio = f"{t.get('side')} {t.get('contracts')} MNQ @ {t.get('entry')} — legacy trade"

        # Infer session from bio or notes
        session = "Overnight"
        for s in ["London", "NY", "Overnight"]:
            if s.lower() in entry_bio.lower():
                session = s
                break

        # Full story = entry bio + close bio
        full_story = entry_bio
        if close_bio:
            full_story = entry_bio + "  |  CLOSE: " + close_bio

        sheets_post({
            "action":       "log_trade",
            "date":         dt[:10],
            "time":         dt[11:16],
            "trade_id":     t.get("id"),
            "side":         t.get("side"),
            "contracts":    t.get("contracts"),
            "entry":        t.get("entry"),
            "sl":           t.get("sl"),
            "tp1":          t.get("tp1"),
            "tp2":          t.get("tp2"),
            "tp3":          t.get("tp3"),
            "tp_hit":       tp_hit,
            "result":       t.get("result"),
            "pnl_pts":      t.get("pnl_pts"),
            "pnl_usd":      t.get("pnl_usd"),
            "eval_balance": ev["eval_balance"],
            "to_pass":      ev["to_target"],
            "dd_left":      ev["to_floor"],
            "eval_num":     ev["eval_num"],
            "session":      session,
            "why":          full_story,
        })
    except Exception as e:
        print(f"[SHEETS] push_trade error: {e}")

def format_sheet_row(trade):
    """Return a single pipe-delimited row for copy-paste."""
    ev = get_eval_status()
    dt   = (trade.get("trade_date") or "")
    tp_hit = ("TP3" if trade.get("tp3_hit") else
              "TP2" if trade.get("tp2_hit") else
              "TP1" if trade.get("tp1_hit") else
              "SL"  if trade.get("sl_hit")  else "OPEN")
    notes = trade.get("notes","")
    session = notes.split("Session:")[-1].split()[0] if "Session:" in notes else "-"
    pnl = trade.get("pnl_usd") or 0
    return (f"{dt[:10]} | {dt[11:16]} | #{trade.get('id','')} | "
            f"{trade.get('side','')} | {trade.get('contracts','')}MNQ | "
            f"{trade.get('entry','')} | {trade.get('sl','')} | "
            f"{trade.get('tp1','')} | {trade.get('tp2','')} | {trade.get('tp3','')} | "
            f"{tp_hit} | {trade.get('result','')} | ${pnl:+.2f} | "
            f"${ev['eval_balance']:,.2f} | ${ev['to_target']:,.0f} | ${ev['to_floor']:,.0f} | "
            f"{session}")

def send_sheet_export():
    """Output last 10 trades as copy-paste spreadsheet rows."""
    trades = sb_select("trades", extra="&source=eq.JARVIS&order=id.desc&limit=10")
    done   = [t for t in trades if t.get("result") in ("WIN","LOSS")]
    if not done:
        tg_send("No completed trades yet.")
        return
    header = "Date | Time | ID | Dir | Size | Entry | SL | TP1 | TP2 | TP3 | TP Hit | Result | P&L | Eval Bal | To Pass | DD Left | Session"
    rows   = "\n".join(format_sheet_row(t) for t in done[:10])
    tg_send(f"📋 <b>Sheet Export</b>\n\n<code>{header}\n{rows}</code>")

def send_daily_summary():
    """Today's performance recap + sheet row."""
    today = datetime.now().strftime("%Y-%m-%d")
    trades = sb_select("trades", extra=f"&source=eq.JARVIS&order=id.asc")
    today_trades = [t for t in trades if (t.get("trade_date") or "")[:10] == today]
    done   = [t for t in today_trades if t.get("result") in ("WIN","LOSS")]
    open_t = [t for t in today_trades if t.get("result") == "OPEN"]

    ev = get_eval_status()

    if not done and not open_t:
        tg_send(f"📅 <b>Daily — {today}</b>\n\nNo trades today. Jarvis was watching but conditions weren't right.")
        return

    wins   = [t for t in done if t["result"] == "WIN"]
    losses = [t for t in done if t["result"] == "LOSS"]
    pnls   = [t.get("pnl_usd") or 0 for t in done]
    total_pnl = sum(pnls)
    wr  = round(len(wins) / max(len(done), 1) * 100)
    best  = max(pnls) if pnls else 0
    worst = min(pnls) if pnls else 0

    trade_lines = ""
    for t in today_trades:
        tp_hit = ("TP3" if t.get("tp3_hit") else "TP2" if t.get("tp2_hit") else "TP1" if t.get("tp1_hit") else "SL" if t.get("sl_hit") else "OPEN")
        emoji  = "✅" if t.get("result") == "WIN" else "❌" if t.get("result") == "LOSS" else "🟡"
        pnl    = t.get("pnl_usd") or 0
        notes  = t.get("notes","")
        sess   = notes.split("Session:")[-1].split()[0] if "Session:" in notes else ""
        trade_lines += f"  {emoji} #{t['id']} {t['side']} {t.get('contracts',5)}MNQ @ {t.get('entry',0)} → {tp_hit}  ${pnl:+.0f}  {sess}\n"

    eval_status = "✅ PASSED" if ev["eval_balance"] >= EVAL_TARGET else "❌ BLOWN" if ev["eval_balance"] <= EVAL_FLOOR else "🟡 Active"
    sheet_row = (f"{today} | {len(done)} | {len(wins)} | {len(losses)} | {wr}% | "
                 f"${total_pnl:+.2f} | ${best:+.2f} | ${worst:+.2f} | "
                 f"${ev['eval_balance']:,.2f} | ${ev['to_target']:,.0f} | ${ev['to_floor']:,.0f} | {eval_status}")

    tg_send(
        f"📅 <b>Daily Recap — {today}</b>\n\n"
        f"{trade_lines}\n"
        f"Trades: {len(done)}  |  {len(wins)}W/{len(losses)}L  |  {wr}% WR\n"
        f"P&L: <b>${total_pnl:+,.2f}</b>  |  Best: ${best:+.0f}  Worst: ${worst:+.0f}\n\n"
        f"🏦 Eval #{ev['eval_num']}: <b>${ev['eval_balance']:,.2f}</b>  ({eval_status})\n"
        f"To pass: ${ev['to_target']:,.0f}  |  DD buffer: ${ev['to_floor']:,.0f}\n\n"
        f"<b>Sheet row:</b>\n<code>{sheet_row}</code>"
    )

    sheets_post({"action":"log_daily","date":today,"trades":len(done),
        "wins":len(wins),"losses":len(losses),"win_rate":f"{wr}%",
        "total_pnl":total_pnl,"avg_win":round(sum(pnls)/max(len(wins),1),2) if wins else 0,
        "avg_loss":round(abs(sum(t.get("pnl_usd",0) or 0 for t in losses))/max(len(losses),1),2) if losses else 0,
        "best":best,"worst":worst,"eval_balance":ev["eval_balance"],
        "to_pass":ev["to_target"],"dd_left":ev["to_floor"],"eval_status":eval_status})

def send_weekly_summary():
    """This week's full stats + sheet rows."""
    now   = datetime.now()
    week_start = now - timedelta(days=now.weekday())  # Monday
    week_start_str = week_start.strftime("%Y-%m-%d")
    week_end_str   = now.strftime("%Y-%m-%d")

    trades = sb_select("trades", extra=f"&source=eq.JARVIS&order=id.asc")
    week_trades = [t for t in trades
                   if (t.get("trade_date") or "")[:10] >= week_start_str]
    done = [t for t in week_trades if t.get("result") in ("WIN","LOSS")]

    if not done:
        tg_send(f"📆 <b>Weekly — {week_start_str} to {week_end_str}</b>\n\nNo completed trades this week.")
        return

    wins   = [t for t in done if t["result"] == "WIN"]
    losses = [t for t in done if t["result"] == "LOSS"]
    pnls   = [t.get("pnl_usd") or 0 for t in done]
    win_pnls  = [t.get("pnl_usd") or 0 for t in wins]
    loss_pnls = [abs(t.get("pnl_usd") or 0) for t in losses]
    total_pnl = sum(pnls)
    wr  = round(len(wins) / max(len(done), 1) * 100)
    avg_win  = round(sum(win_pnls) / max(len(win_pnls), 1), 2)
    avg_loss = round(sum(loss_pnls) / max(len(loss_pnls), 1), 2)
    pf       = round(sum(win_pnls) / max(sum(loss_pnls), 0.01), 2)
    best     = max(pnls) if pnls else 0
    worst    = min(pnls) if pnls else 0

    # Max drawdown this week (running balance)
    running = EVAL_START
    peak    = EVAL_START
    max_dd  = 0
    for t in done:
        running += (t.get("pnl_usd") or 0)
        peak     = max(peak, running)
        max_dd   = max(max_dd, peak - running)

    ev = get_eval_status()
    eval_passed = ev["funded"]
    eval_blown  = ev["blown"]

    # Per-session breakdown
    sess_stats = {}
    for t in done:
        notes = t.get("notes","")
        s = notes.split("Session:")[-1].split()[0] if "Session:" in notes else "Other"
        if s not in sess_stats: sess_stats[s] = {"W":0,"L":0}
        if t["result"] == "WIN": sess_stats[s]["W"] += 1
        else: sess_stats[s]["L"] += 1
    sess_lines = "  ".join([f"{s}: {v['W']}W/{v['L']}L" for s, v in sess_stats.items()])

    sheet_row = (f"{week_start_str} | {week_end_str} | {len(done)} | {len(wins)} | {len(losses)} | "
                 f"{wr}% | ${total_pnl:+.2f} | ${avg_win:.2f} | ${avg_loss:.2f} | "
                 f"{pf} | ${max_dd:.2f} | ${best:+.2f} | ${worst:+.2f} | "
                 f"{eval_passed} | {eval_blown} | ${ev['eval_balance']:,.2f}")

    tg_send(
        f"📆 <b>Weekly Recap — {week_start_str} → {week_end_str}</b>\n\n"
        f"Trades: {len(done)}  |  {len(wins)}W/{len(losses)}L  |  <b>{wr}% WR</b>\n"
        f"P&L: <b>${total_pnl:+,.2f}</b>\n"
        f"Avg win: ${avg_win:+.2f}  |  Avg loss: -${avg_loss:.2f}\n"
        f"Profit factor: {pf}  |  Max DD: ${max_dd:.2f}\n"
        f"Best: ${best:+.0f}  |  Worst: ${worst:+.0f}\n\n"
        f"Sessions: {sess_lines}\n\n"
        f"🏦 Evals: {eval_passed} passed  |  {eval_blown} blown  |  Balance ${ev['eval_balance']:,.2f}\n\n"
        f"<b>Sheet row:</b>\n<code>{sheet_row}</code>"
    )

    sheets_post({"action":"log_weekly","week_start":week_start_str,"week_end":week_end_str,
        "trades":len(done),"wins":len(wins),"losses":len(losses),"win_rate":f"{wr}%",
        "total_pnl":total_pnl,"avg_win":avg_win,"avg_loss":avg_loss,
        "profit_factor":pf,"max_dd":max_dd,"best":best,"worst":worst,
        "evals_passed":eval_passed,"evals_blown":eval_blown,"ending_balance":ev["eval_balance"]})

def send_trade_history():
    """Last 7 days of Jarvis trades."""
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    trades = sb_select("trades", extra=f"&source=eq.JARVIS&order=id.desc&limit=30")
    recent = [t for t in trades if t.get("result") in ("WIN","LOSS","OPEN")]

    if not recent:
        tg_send("No trades in the last 7 days.")
        return

    lines = ""
    for t in recent[:15]:
        side      = t.get("side","?")
        entry     = t.get("entry","?")
        result    = t.get("result","?")
        pnl       = t.get("pnl_usd") or 0
        contracts = t.get("contracts", 5)
        session   = (t.get("notes") or "").split("Session:")[-1].split()[0] if "Session:" in (t.get("notes") or "") else "?"
        tp3h = "→TP3" if t.get("tp3_hit") else ("→TP2" if t.get("tp2_hit") else ("→TP1" if t.get("tp1_hit") else ""))
        emoji = "✅" if result == "WIN" else "❌" if result == "LOSS" else "🟡"
        date  = (t.get("trade_date") or "")[:10]
        lines += f"{emoji} {date}  {side} {contracts}MNQ @ {entry}  {tp3h}  ${pnl:+.0f}\n"

    wins   = sum(1 for t in recent if t.get("result") == "WIN")
    losses = sum(1 for t in recent if t.get("result") == "LOSS")
    total_pnl = sum(t.get("pnl_usd") or 0 for t in recent if t.get("result") in ("WIN","LOSS"))
    wr = round(wins / max(wins+losses, 1) * 100)

    tg_send(
        f"📅 <b>Recent Trades</b>\n\n"
        f"{lines}\n"
        f"━━━━━━━━━━\n"
        f"{wins}W / {losses}L  |  {wr}% WR  |  ${total_pnl:+,.2f}"
    )

def send_smart_recap():
    """AI-generated recap — what happened, what Jarvis is learning."""
    all_jarvis = sb_select("trades", extra="&source=eq.JARVIS&order=id.desc&limit=20")
    resolved   = [t for t in all_jarvis if t.get("result") in ("WIN","LOSS")]

    if not resolved:
        tg_send("No completed trades yet. Jarvis is still watching. First signal will fire when conditions align.")
        return

    wins   = [t for t in resolved if t["result"] == "WIN"]
    losses = [t for t in resolved if t["result"] == "LOSS"]
    total_pnl = sum(t.get("pnl_usd") or 0 for t in resolved)
    wr    = round(len(wins) / len(resolved) * 100)

    # Sessions breakdown
    sessions = {}
    for t in resolved:
        s = (t.get("notes") or "").split("Session:")[-1].split()[0] if "Session:" in (t.get("notes") or "") else "Unknown"
        if s not in sessions: sessions[s] = {"W":0,"L":0}
        sessions[s]["WIN" == t["result"] and "W" or "L"] += 1

    # TP breakdown
    tp3_wins = sum(1 for t in wins if t.get("tp3_hit"))
    tp2_wins = sum(1 for t in wins if t.get("tp2_hit") and not t.get("tp3_hit"))
    tp1_wins = sum(1 for t in wins if t.get("tp1_hit") and not t.get("tp2_hit"))

    # Recent streak
    streak = 0
    last = resolved[0]["result"]
    for t in resolved:
        if t["result"] == last: streak += 1
        else: break
    streak_str = f"{streak} {'win' if last == 'WIN' else 'loss'} streak"

    # Directional breakdown
    buy_trades  = [t for t in resolved if t["side"] == "BUY"]
    sell_trades = [t for t in resolved if t["side"] == "SELL"]
    buy_wr  = round(sum(1 for t in buy_trades  if t["result"]=="WIN") / max(len(buy_trades),1)  * 100)
    sell_wr = round(sum(1 for t in sell_trades if t["result"]=="WIN") / max(len(sell_trades),1) * 100)
    buy_pnl  = sum(t.get("pnl_usd") or 0 for t in buy_trades)
    sell_pnl = sum(t.get("pnl_usd") or 0 for t in sell_trades)

    # Build data summary for Claude
    trade_summary = f"""
Jarvis MNQ trading bot stats:
- Total trades: {len(resolved)} ({len(wins)}W / {len(losses)}L)
- Win rate: {wr}%
- Total P&L: ${total_pnl:+,.2f}
- Current streak: {streak_str}
- BUY trades:  {len(buy_trades)}  ({buy_wr}% WR)  ${buy_pnl:+.2f}
- SELL trades: {len(sell_trades)} ({sell_wr}% WR)  ${sell_pnl:+.2f}
- TP breakdown (wins): TP3={tp3_wins}, TP2={tp2_wins}, TP1={tp1_wins}
- Session breakdown: {dict(sessions)}
- Recent 8 trades (newest first): {[{"side":t["side"],"result":t["result"],"pnl":t.get("pnl_usd",0),"tp3":t.get("tp3_hit"),"session":(t.get("notes") or "").split("Session:")[-1].split()[0] if "Session:" in (t.get("notes") or "") else "?"} for t in resolved[:8]]}
"""

    # Use Claude to generate intelligent recap
    smart_text = ""
    if ANTHROPIC_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
            msg = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=300,
                messages=[{
                    "role": "user",
                    "content": f"""You are Jarvis, an MNQ algo. Give a SHORT technical recap in 3-4 sentences. Speak like a blunt trading desk analyst — no generic advice, no filler.

ONLY cover what the DATA actually shows:
- Which session (London/Overnight/NY) had the wins vs losses
- Whether BUY or SELL trades performed better
- Did trades reach TP2/TP3 or all dying at SL (suggests entries too late or wrong direction)
- One specific pattern you can actually see in the numbers

DO NOT say things like "audit the rules", "this screams bad logic", "time to pause", or generic trading clichés. Just read the data like a quant.

{trade_summary}"""
                }]
            )
            smart_text = msg.content[0].text
        except:
            pass

    if not smart_text:
        smart_text = (f"{'Solid run' if wr >= 55 else 'Rough patch' if wr < 40 else 'Holding steady'} — "
                      f"{wr}% WR over {len(resolved)} trades. "
                      f"{'TP3 hitting well, letting winners run.' if tp3_wins > len(wins)*0.4 else 'Need more trades riding to TP3.'} "
                      f"{'On a good streak right now.' if last=='WIN' and streak>=2 else 'Taking some heat lately, sizing is adjusted.' if last=='LOSS' and streak>=2 else ''}")

    tg_send(
        f"🧠 <b>Jarvis Recap</b>\n\n"
        f"<b>Overall:</b>  {len(resolved)} trades  |  {wr}% WR  |  ${total_pnl:+,.2f}\n"
        f"TP3: {tp3_wins}  TP2: {tp2_wins}  TP1: {tp1_wins}  ({streak_str})\n\n"
        f"<b>By direction:</b>\n"
        f"  BUY  {len(buy_trades)} trades  {buy_wr}% WR  ${buy_pnl:+.2f}\n"
        f"  SELL {len(sell_trades)} trades  {sell_wr}% WR  ${sell_pnl:+.2f}\n\n"
        f"<b>What I'm seeing:</b>\n"
        f"{smart_text}"
    )

def send_daily_recap():
    """Send end-of-day performance recap to Telegram."""
    today = datetime.now().strftime("%Y-%m-%d")
    all_trades = sb_select("trades", extra=f"&trade_date=gte.{today}&source=eq.JARVIS&order=id.asc")
    resolved   = [t for t in all_trades if t.get("result") in ("WIN", "LOSS")]

    if not resolved:
        tg_send(f"📋 <b>Daily Recap — {today}</b>\n\nNo trades taken today. Jarvis was watching but conditions weren't right.")
        return

    wins   = [t for t in resolved if t["result"] == "WIN"]
    losses = [t for t in resolved if t["result"] == "LOSS"]
    total_pnl = sum(t.get("pnl_usd") or 0 for t in resolved)
    wr    = round(len(wins) / len(resolved) * 100)

    # Build trade list
    trade_lines = ""
    for t in resolved:
        side = t.get("side","?")
        entry = t.get("entry","?")
        pnl = t.get("pnl_usd", 0) or 0
        tps = "TP3" if t.get("tp3_hit") else "TP2" if t.get("tp2_hit") else "TP1" if t.get("tp1_hit") else "SL"
        emoji = "✅" if t["result"] == "WIN" else "❌"
        trade_lines += f"{emoji} {side} @ {entry}  →  {tps}  ${pnl:+.0f}\n"

    # All-time stats
    all_jarvis = sb_select("trades", extra="&source=eq.JARVIS")
    all_resolved = [t for t in all_jarvis if t.get("result") in ("WIN","LOSS")]
    all_wins = sum(1 for t in all_resolved if t["result"] == "WIN")
    all_wr   = round(all_wins / max(len(all_resolved), 1) * 100)
    all_pnl  = sum(t.get("pnl_usd") or 0 for t in all_jarvis)

    tg_send(
        f"📋 <b>Daily Recap — {today}</b>\n\n"
        f"Trades: {len(resolved)}  |  {len(wins)}W / {len(losses)}L  |  {wr}% WR\n"
        f"Day P&L: <b>${total_pnl:+,.2f}</b>\n\n"
        f"{trade_lines}\n"
        f"━━━━━━━━━━\n"
        f"All-time: {len(all_resolved)} trades  |  {all_wr}% WR  |  ${all_pnl:+,.2f}\n"
        f"Account: ${STARTING_BALANCE + all_pnl:,.2f}"
    )

def send_progress_chime():
    """Periodic trade update — fires every ~20min while trade is open."""
    open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
    if not open_trades:
        return
    t = open_trades[0]
    price     = get_live_price()
    if not price: return
    side      = t["side"]
    entry     = t["entry"]
    sl        = t["sl"]
    tp1       = t["tp1"]
    tp2       = t["tp2"]
    tp3       = t["tp3"]
    contracts = t.get("contracts", 5)
    tp1_hit   = t.get("tp1_hit", False)
    tp2_hit   = t.get("tp2_hit", False)
    c1, c2, c3 = get_scale(contracts)

    pts = (price - entry) if side == "BUY" else (entry - price)
    unreal, remaining = calc_unrealized_pnl(entry, side, price, contracts, tp1_hit, tp2_hit)
    locked = (c1*(tp1-entry if side=="BUY" else entry-tp1)*PTS_TO_USD if tp1_hit else 0) + (c2*(tp2-entry if side=="BUY" else entry-tp2)*PTS_TO_USD if tp2_hit else 0)
    total  = round(locked + unreal, 2)

    dist_tp2 = round(abs(price - tp2), 1)
    dist_tp3 = round(abs(price - tp3), 1)
    dist_sl  = round(abs(price - sl), 1)

    emoji = "🟢" if pts > 0 else "🔴"
    next_target = f"TP3 {dist_tp3}pts away" if tp2_hit else f"TP2 {dist_tp2}pts away" if tp1_hit else f"TP1 {round(abs(price-tp1),1)}pts away"

    tg_send(
        f"{emoji} <b>Trade #{t['id']} update</b>\n\n"
        f"{side} {contracts}MNQ  |  <code>{price:,.2f}</code>\n"
        f"P&L: <b>${total:+,.2f}</b>  ({remaining} MNQ open)\n"
        f"Next: {next_target}  |  SL {dist_sl}pts away\n\n"
        f"/progress for full breakdown"
    )

def background_monitor():
    """Every 30s: auto-enter trades when signal fires, monitor open positions."""
    last_recap_day   = None
    last_weekly_day  = None
    last_chime_time  = 0
    market_close_done = {}   # date → True when 5pm close was handled

    while True:
        try:
            now_dt  = datetime.now()
            now_h   = now_dt.hour
            now_dow = now_dt.weekday()
            today   = now_dt.strftime("%Y-%m-%d")

            # ── Market close at 5pm ET — close any open position ──
            if now_h == 17 and now_dow <= 4 and not market_close_done.get(today):
                market_close_done[today] = True
                open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
                for t in open_trades:
                    price  = get_live_price() or t.get("entry", 0)
                    entry  = t.get("entry", 0)
                    side   = t.get("side")
                    tp1_h  = t.get("tp1_hit", False)
                    tp2_h  = t.get("tp2_hit", False)
                    contracts = t.get("contracts", 5)
                    c1, c2, c3 = get_scale(contracts)
                    # If SL was moved to BE after TP2, close remaining at entry
                    if tp2_h:
                        close_price = t.get("entry", price)  # BE close
                        remaining   = contracts - c1 - c2
                        locked_pnl  = calc_scaled_pnl(entry, side, t.get("tp1"), t.get("tp2"),
                                                       t.get("tp3"), close_price, contracts,
                                                       True, True, False, True)
                        pnl    = locked_pnl
                        result = "WIN"
                        pts    = 0
                    else:
                        pts    = (price - entry) if side == "BUY" else (entry - price)
                        pnl    = round(pts * PTS_TO_USD * contracts, 2)
                        result = "WIN" if pts > 0 else "LOSS"
                    sb_update("trades", t["id"], {
                        "result": result, "pnl_pts": round(pts, 2), "pnl_usd": pnl,
                        "sl_hit": True,
                        "tp1_hit": tp1_h, "tp2_hit": tp2_h,
                        "closed_at": now_dt.isoformat(),
                        "notes": (t.get("notes","") + " [closed at market close 5pm ET]")
                    })
                    ev = get_eval_status()
                    emoji = "✅" if result == "WIN" else "❌"
                    if TRADING_MODE == "live":
                        # Actually close the real position on Topstep
                        remaining_c = contracts - (c1 if tp1_h else 0) - (c2 if tp2_h else 0)
                        if remaining_c > 0:
                            ts_close_position(remaining_c, side)
                    tg_send(f"🔔 <b>Market close — Trade #{t['id']} {emoji}</b>\n"
                            f"{side} @ {entry}  |  Closed ${pnl:+,.2f}\n"
                            f"{'TP2 hit → closed remaining at breakeven ✅' if tp2_h else f'Price: {price}'}\n"
                            f"CME maintenance break. Back at 6pm ET.\n"
                            f"🏦 Eval: ${ev['eval_balance']:,.2f}")
                    mkt_close_bio = build_close_bio(t, result, tp1_h, tp2_h, False, True, pnl, pts)
                    push_trade_to_sheets(t["id"], close_bio=mkt_close_bio)
                    if result == "WIN":
                        _cooldown["until"]  = time.time() + 1800
                        _cooldown["reason"] = "WIN — 30min cooldown"
                    else:
                        _cooldown["until"]  = time.time() + 3600
                        _cooldown["reason"] = "LOSS — 60min cooldown"
                        _last_sl["price"] = entry
                        _last_sl["side"]  = side
                        _last_sl["time"]  = time.time()

            if TRADING_MODE == "live":
                check_live_trade()
            else:
                check_open_jarvis_trades()
            check_eval_thresholds()

            # ── Signal engine — only when market is open ───────────
            if not is_market_open():
                print(f"[SIGNAL] Market closed — skipping")
            else:
                signal, msg = check_for_signal()
                if signal:
                    cd_secs, cd_reason = get_cooldown_remaining()
                    if cd_secs > 0:
                        print(f"[SIGNAL] Cooldown {round(cd_secs/60)}min left ({cd_reason})")
                    # Hard dedup: don't fire same signal within 10 minutes
                    elif time.time() - _last_signal_time["t"] < 600:
                        print(f"[SIGNAL] Dedup — last signal was {round((time.time()-_last_signal_time['t'])/60, 1)}min ago")
                    else:
                        # ── Daily loss hard stop ───────────────────
                        dl_blocked, dl_loss = check_daily_loss_limit()
                        if dl_blocked:
                            print(f"[SIGNAL] Daily loss limit hit (${dl_loss:,.0f}) — sitting out")
                        elif CONFIRM_MODE:
                            # Confirmation mode — ask user before entering
                            log_signal(signal, taken=False)
                            send_confirmation_request(signal)
                            _last_signal_time["t"] = time.time()
                        else:
                            log_signal(signal, taken=True)
                            if TRADING_MODE == "live":
                                ts_enter_trade(signal)
                            else:
                                auto_enter_trade(signal)
                            _last_signal_time["t"] = time.time()
                else:
                    print(f"[SIGNAL] {msg}")

            # ── Periodic chime every 20min on open trade ───────────
            open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
            if open_trades and time.time() - last_chime_time > 1200:
                send_progress_chime()
                last_chime_time = time.time()

            # ── Daily recap at 4pm ET ──────────────────────────────
            if now_h == 16 and now_dt.minute < 1 and last_recap_day != today:
                send_daily_summary()
                last_recap_day = today

            # ── Weekly recap Friday after 5pm ET ──────────────────
            if now_dow == 4 and now_h == 17 and now_dt.minute < 2 and last_weekly_day != today:
                send_weekly_summary()
                last_weekly_day = today

        except Exception as e:
            print(f"[MONITOR] Error: {e}")
        time.sleep(30)

# ── Startup — runs on import (works with gunicorn AND python app.py) ──
def _yf_price():
    """Pull latest price from yfinance 1-min NQ=F bars. ~15s delay but always live."""
    try:
        df = yf.Ticker("NQ=F").history(interval="1m", period="1d")
        if len(df) > 0:
            return round(float(df["Close"].iloc[-1]), 2)
    except:
        pass
    return None

def price_heartbeat():
    """
    Every 10s: keep price fresh.
    WebSocket is primary (real-time ticks from Polygon).
    When WS is stale >15s → yfinance 1m bars (always live, ~15s delay).
    Polygon REST is skipped — it doesn't update live during session.
    """
    while True:
        try:
            ws_age = time.time() - _ws_price["updated"] if _ws_price["updated"] else 9999
            if ws_age > 15:
                p = _yf_price()
                if p:
                    _ws_price["price"]   = p
                    _ws_price["updated"] = time.time()
                    print(f"[PRICE] yfinance: {p} (WS was {round(ws_age)}s stale)")
        except Exception as e:
            print(f"[PRICE] Heartbeat error: {e}")
        time.sleep(10)

# ══════════════════════════════════════════════════════════════════
# TOPSTEP / PROJECTX LIVE TRADING LAYER
# ══════════════════════════════════════════════════════════════════
#
# TRADING_MODE controls whether Jarvis places real orders:
#   "paper"  — current behavior: logs to Supabase only, no real orders
#   "live"   — places real orders on Topstep via ProjectX API
#
# To go live: set env vars TOPSTEP_USERNAME + TOPSTEP_API_KEY + TOPSTEP_ACCOUNT_ID
# then set TRADING_MODE=live on Railway.
# DO NOT set TRADING_MODE=live until you have confirmed:
#   1. Daily loss limit is configured (DAILY_LOSS_LIMIT env var, default $500)
#   2. The account ID is correct (run /accounts command to list them)
#   3. You're running on a personal machine — Topstep bans VPS
#
TRADING_MODE       = os.environ.get("TRADING_MODE", "paper")   # "paper" | "live"
TOPSTEP_USERNAME   = os.environ.get("TOPSTEP_USERNAME", "")
TOPSTEP_API_KEY    = os.environ.get("TOPSTEP_API_KEY", "")
TOPSTEP_ACCOUNT_ID = os.environ.get("TOPSTEP_ACCOUNT_ID", "")  # fallback — overridden by Supabase

def _load_account_id():
    """Load active account ID from Supabase so /setaccount persists across restarts."""
    global TOPSTEP_ACCOUNT_ID
    try:
        rows = sb_select("signals_log", extra="&notes=like.ACTIVE_ACCOUNT%25&order=id.desc&limit=1")
        if rows:
            stored = rows[0]["notes"].replace("ACTIVE_ACCOUNT:", "").strip()
            if stored:
                TOPSTEP_ACCOUNT_ID = stored
                print(f"[TOPSTEP] Loaded account ID from DB: {stored}")
    except:
        pass

def _save_account_id(account_id):
    """Persist active account ID to Supabase."""
    global TOPSTEP_ACCOUNT_ID
    TOPSTEP_ACCOUNT_ID = account_id
    try:
        # Delete old entry then insert new
        requests.delete(f"{SB_REST}/signals_log?notes=like.ACTIVE_ACCOUNT%25", headers=SB_HEADERS, timeout=5)
        sb_insert("signals_log", {
            "side": "CONFIG", "entry": 0, "sl": 0, "tp1": 0, "tp2": 0, "tp3": 0,
            "contracts": 0, "session": "CONFIG", "strength": 0, "taken": False, "skipped": False,
            "notes": f"ACTIVE_ACCOUNT:{account_id}"
        })
    except:
        pass
DAILY_LOSS_LIMIT   = float(os.environ.get("DAILY_LOSS_LIMIT", "500"))  # $500/day hard stop

PROJECTX_BASE = "https://api.topstepx.com"
PROJECTX_SYMBOL = "CON.F.US.MNQ.U26"  # Micro NQ September 2026 (confirmed from ProjectX API)

# ── JWT Token cache ───────────────────────────────────────────────
_ts_token = {"jwt": None, "expires": 0}

def ts_login():
    """Authenticate with TopstepX API. JWT valid 24hrs — cached."""
    if _ts_token["jwt"] and time.time() < _ts_token["expires"] - 300:
        return _ts_token["jwt"]
    if not TOPSTEP_USERNAME or not TOPSTEP_API_KEY:
        print("[TOPSTEP] No credentials configured")
        return None
    try:
        r = requests.post(
            f"{PROJECTX_BASE}/api/Auth/loginKey",
            headers={"Content-Type": "application/json", "accept": "text/plain"},
            json={"userName": TOPSTEP_USERNAME, "apiKey": TOPSTEP_API_KEY},
            timeout=10
        )
        print(f"[TOPSTEP] Auth status={r.status_code} body={r.text[:300]}")
        data = r.json()
        token = data.get("token") or data.get("accessToken")
        if not token:
            print(f"[TOPSTEP] Login failed: {data}")
            return None
        _ts_token["jwt"]     = token
        _ts_token["expires"] = time.time() + 86400  # 24hrs
        print("[TOPSTEP] Authenticated ✓")
        return token
    except Exception as e:
        print(f"[TOPSTEP] Login error: {e}")
        return None

def ts_headers():
    token = ts_login()
    if not token:
        return None
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }

# ── Account listing ───────────────────────────────────────────────
def ts_get_accounts():
    """Return all accounts on this Topstep login."""
    hdrs = ts_headers()
    if not hdrs:
        return []
    try:
        # ProjectX uses POST for search endpoints
        r = requests.post(
            f"{PROJECTX_BASE}/api/Account/search",
            headers=hdrs,
            json={"onlyActiveAccounts": False},
            timeout=10
        )
        data = r.json()
        print(f"[TOPSTEP] Account search raw: {str(data)[:300]}")
        # Response may be list directly or wrapped in "accounts"
        if isinstance(data, list):
            return data
        return data.get("accounts") or data.get("data") or []
    except Exception as e:
        print(f"[TOPSTEP] Account search error: {e}")
        return []

def ts_get_balance(account_id=None):
    """Fetch real live account balance from Topstep."""
    acct = account_id or TOPSTEP_ACCOUNT_ID
    hdrs = ts_headers()
    if not hdrs:
        return None
    try:
        r = requests.post(
            f"{PROJECTX_BASE}/api/Account/search",
            headers=hdrs,
            json={"onlyActiveAccounts": False},
            timeout=10
        )
        data = r.json()
        accounts = data if isinstance(data, list) else (data.get("accounts") or data.get("data") or [])
        # If no account ID specified, return first active account
        for a in accounts:
            aid = str(a.get("id") or "")
            if not acct or aid == str(acct):
                return {
                    "id":           aid,
                    "name":         a.get("name") or "?",
                    "balance":      a.get("balance") or 0,
                    "daily_pnl":    a.get("dailyPnl") or 0,
                    "open_pnl":     a.get("openPnl") or 0,
                    "max_loss":     a.get("maxDailyLoss") or a.get("dailyLossLimit"),
                    "profit_target":a.get("profitTarget"),
                    "status":       "sim" if a.get("simulated") else "live",
                    "can_trade":    a.get("canTrade", True),
                }
        return None
    except Exception as e:
        print(f"[TOPSTEP] Balance error: {e}")
        return None

# ── Daily loss hard stop ──────────────────────────────────────────
def check_daily_loss_limit():
    """
    Pull today's realized P&L from Supabase trades.
    Returns (is_blocked: bool, loss_today: float).
    Live mode also checks Topstep balance for real daily P&L.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        trades_today = sb_select("trades", extra=f"&source=eq.JARVIS&result=in.(WIN,LOSS)&trade_date=gte.{today}")
        loss_today = sum(t.get("pnl_usd") or 0 for t in trades_today)
        if loss_today <= -DAILY_LOSS_LIMIT:
            return True, loss_today
        # In live mode, also check real balance from Topstep
        if TRADING_MODE == "live" and TOPSTEP_ACCOUNT_ID:
            bal = ts_get_balance()
            if bal and bal.get("daily_pnl") is not None:
                real_daily = float(bal["daily_pnl"])
                if real_daily <= -DAILY_LOSS_LIMIT:
                    return True, real_daily
    except:
        pass
    return False, 0

# ── Open position check ───────────────────────────────────────────
def ts_get_open_position(account_id=None):
    """Check if there's an open position on Topstep for this account."""
    acct = account_id or TOPSTEP_ACCOUNT_ID
    hdrs = ts_headers()
    if not hdrs or not acct:
        return None
    try:
        # Correct endpoint: /api/Position/searchOpen
        r = requests.post(
            f"{PROJECTX_BASE}/api/Position/searchOpen",
            headers=hdrs,
            json={"accountId": int(acct)},
            timeout=10
        )
        data = r.json()
        positions = data if isinstance(data, list) else (data.get("positions") or data.get("data") or [])
        for p in positions:
            cid = str(p.get("contractId") or "")
            if PROJECTX_SYMBOL in cid or "MNQ" in cid:
                return p
        return None
    except Exception as e:
        print(f"[TOPSTEP] Position check error: {e}")
        return None

# ── Place order ───────────────────────────────────────────────────
# Swagger-confirmed field names:
#   side: 0=Buy, 1=Sell  |  type: 1=Market, 2=Limit, 3=Stop, 4=StopLimit
ORDER_SIDE = {"Buy": 0, "Sell": 1}
ORDER_TYPE = {"Market": 1, "Limit": 2, "Stop": 3, "StopLimit": 4}

def ts_place_order(side, contracts, order_type="Market", price=None,
                   stop_price=None, account_id=None, custom_tag=None):
    """
    Place a single order on Topstep via ProjectX.
    Returns order_id or None.
    side: "Buy" | "Sell"
    order_type: "Market" | "Limit" | "Stop"
    """
    acct = account_id or TOPSTEP_ACCOUNT_ID
    hdrs = ts_headers()
    if not hdrs or not acct:
        return None
    payload = {
        "accountId":  int(acct),
        "contractId": PROJECTX_SYMBOL,
        "side":       ORDER_SIDE.get(side, 0),
        "type":       ORDER_TYPE.get(order_type, 1),
        "size":       contracts,
    }
    if price:       payload["limitPrice"] = price
    if stop_price:  payload["stopPrice"]  = stop_price
    if custom_tag:  payload["customTag"]  = str(custom_tag)

    try:
        r = requests.post(
            f"{PROJECTX_BASE}/api/Order/place",
            headers=hdrs, json=payload, timeout=10
        )
        data = r.json()
        print(f"[TOPSTEP] Place order response: {str(data)[:300]}")
        order_id = data.get("orderId") or data.get("id")
        if not order_id:
            err = data.get("errorMessage") or data.get("message") or str(data)[:200]
            tg_send(f"⚠️ <b>Order failed</b>\n<code>{err}</code>\nSymbol: {PROJECTX_SYMBOL}  Side: {side}  Size: {contracts}")
            print(f"[TOPSTEP] Place order failed: {data}")
        return order_id
    except Exception as e:
        tg_send(f"⚠️ <b>Order error</b>: {e}")
        print(f"[TOPSTEP] Place order error: {e}")
        return None

def ts_cancel_order(order_id, account_id=None):
    """Cancel an open order."""
    acct = account_id or TOPSTEP_ACCOUNT_ID
    hdrs = ts_headers()
    if not hdrs:
        return False
    try:
        r = requests.post(
            f"{PROJECTX_BASE}/api/Order/cancel",
            headers=hdrs,
            json={"accountId": int(acct), "orderId": order_id},
            timeout=10
        )
        return r.json().get("success", False)
    except:
        return False

def ts_close_position(contracts, side, account_id=None):
    """Partial close N contracts using the dedicated endpoint."""
    acct = account_id or TOPSTEP_ACCOUNT_ID
    hdrs = ts_headers()
    if not hdrs or not acct:
        return False
    try:
        r = requests.post(
            f"{PROJECTX_BASE}/api/Position/partialCloseContract",
            headers=hdrs,
            json={"accountId": int(acct), "contractId": PROJECTX_SYMBOL, "size": contracts},
            timeout=10
        )
        return r.json().get("success", False)
    except:
        return False

# ── Enter trade — live version ─────────────────────────────────────
def ts_enter_trade(signal):
    """
    Full entry sequence for live Topstep trading:
    1. Safety checks (daily loss, existing position, account health)
    2. Market entry order
    3. Hard stop-loss order (StopMarket)
    4. Log to Supabase, alert Telegram with real order IDs
    Scale-out (TP1→close c1, TP2→close c2, TP3→close c3) is handled
    by check_live_trade() which polls position and price.
    """
    if TRADING_MODE != "live":
        return None  # paper mode — use auto_enter_trade() instead

    # ── Safety gate 1: daily loss limit ───────────────────────────
    blocked, loss_today = check_daily_loss_limit()
    if blocked:
        msg = (f"🛑 <b>DAILY LOSS LIMIT HIT — TRADING HALTED</b>\n\n"
               f"Today's P&L: <b>−${abs(loss_today):,.2f}</b>  (limit: −${DAILY_LOSS_LIMIT:,.0f})\n"
               f"Jarvis is done for today. Reset at midnight UTC.")
        tg_send(msg)
        print(f"[LIVE] Daily loss limit ${DAILY_LOSS_LIMIT} hit. Blocking entry.")
        return None

    # ── Safety gate 2: no existing open position ───────────────────
    existing = ts_get_open_position()
    if existing:
        print(f"[LIVE] Already have open position — skipping entry")
        return None

    # ── Safety gate 3: account health ─────────────────────────────
    bal = ts_get_balance()
    if not bal:
        tg_send("⚠️ <b>Can't read Topstep balance</b> — skipping entry to be safe.")
        return None
    acct_balance = float(bal.get("balance") or 0)
    # Don't trade if we're within $200 of the floor
    if acct_balance < (EVAL_FLOOR + 200):
        tg_send(f"🛑 <b>Account too close to floor</b> — ${acct_balance:,.2f}. "
                f"Stopping to protect the account.")
        return None

    side       = signal["side"]
    contracts  = signal["contracts"]
    entry      = signal["entry"]
    sl         = signal["sl"]
    c1, c2, c3 = get_scale(contracts)
    ts_side    = "Buy" if side == "BUY" else "Sell"
    ts_sl_side = "Sell" if side == "BUY" else "Buy"

    print(f"[LIVE] Placing {ts_side} {contracts}MNQ — entry market, SL @ {sl}")

    # ── Step 1: Market entry ───────────────────────────────────────
    entry_order_id = ts_place_order(ts_side, contracts, "Market")
    if not entry_order_id:
        tg_send("⚠️ <b>Entry order FAILED</b> — check Topstep account manually.")
        return None
    time.sleep(1)  # brief wait for fill

    # ── Step 2: Hard stop-loss (full size, stop-market) ───────────
    sl_order_id = ts_place_order(
        ts_sl_side, contracts, "Stop",
        stop_price=sl
    )
    if not sl_order_id:
        # Entry is live but SL failed — emergency close
        ts_close_position(contracts, side)
        tg_send("🚨 <b>SL ORDER FAILED — emergency closed position.</b> Check account.")
        return None

    # ── Log to Supabase ───────────────────────────────────────────
    bio = build_entry_bio(signal)
    trade_row = {
        "trade_date":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        "side":          side,
        "entry":         entry,
        "sl":            sl,
        "tp1":           signal["tp1"],
        "tp2":           signal["tp2"],
        "tp3":           signal["tp3"],
        "contracts":     contracts,
        "trader":        "JARVIS",
        "source":        "JARVIS",
        "result":        "OPEN",
        "notes":         bio + f" | entry_order:{entry_order_id} sl_order:{sl_order_id}",
    }
    logged = sb_insert("trades", trade_row)
    trade_id = logged["id"] if logged else "?"

    # ── Telegram alert ────────────────────────────────────────────
    risk_usd = round(40 * PTS_TO_USD * contracts)
    tp1_usd  = round(34 * PTS_TO_USD * c1)
    tp2_usd  = round(65 * PTS_TO_USD * c2)
    tp3_usd  = round(100 * PTS_TO_USD * c3)
    tg_send(
        f"{'🟢' if side=='BUY' else '🔴'} <b>LIVE ORDER PLACED — #{trade_id}</b>\n\n"
        f"<b>{side} {contracts} MNQ</b>  (real money 💰)\n"
        f"Entry: Market  |  SL: <code>{sl}</code>\n"
        f"TP1 {c1}c @ <code>{signal['tp1']}</code>  +${tp1_usd}\n"
        f"TP2 {c2}c @ <code>{signal['tp2']}</code>  +${tp2_usd}  ← SL→BE\n"
        f"TP3 {c3}c @ <code>{signal['tp3']}</code>  +${tp3_usd}\n"
        f"Risk: −${risk_usd}  |  Max: +${tp1_usd+tp2_usd+tp3_usd}\n\n"
        f"🏦 Balance: ${acct_balance:,.2f}  |  entry#{entry_order_id}  sl#{sl_order_id}"
    )

    # Store order IDs in memory for check_live_trade to use
    _live_trade_state["trade_id"]       = trade_id
    _live_trade_state["sl_order_id"]    = sl_order_id
    _live_trade_state["contracts"]      = contracts
    _live_trade_state["c1"]             = c1
    _live_trade_state["c2"]             = c2
    _live_trade_state["c3"]             = c3
    _live_trade_state["side"]           = side
    _live_trade_state["entry"]          = entry
    _live_trade_state["sl"]             = sl
    _live_trade_state["tp1"]            = signal["tp1"]
    _live_trade_state["tp2"]            = signal["tp2"]
    _live_trade_state["tp3"]            = signal["tp3"]
    _live_trade_state["tp1_hit"]        = False
    _live_trade_state["tp2_hit"]        = False
    return trade_id

# ── Live trade monitor state ──────────────────────────────────────
_live_trade_state = {
    "trade_id": None, "sl_order_id": None, "contracts": 0,
    "c1": 0, "c2": 0, "c3": 0, "side": None,
    "entry": 0, "sl": 0, "tp1": 0, "tp2": 0, "tp3": 0,
    "tp1_hit": False, "tp2_hit": False
}

def check_live_trade():
    """
    Called every 10s when TRADING_MODE=live.
    Monitors price vs TP levels — places partial close orders at each TP.
    Handles: TP1 close c1 → TP2 close c2 + cancel SL + place SL at BE → TP3 close c3.
    Falls back to Supabase update + Telegram just like paper mode.
    """
    if TRADING_MODE != "live":
        return
    s = _live_trade_state
    if not s["trade_id"] or not s["side"]:
        return

    price = get_live_price()
    if not price:
        return

    side  = s["side"]
    entry = s["entry"]
    sl    = s["sl"]
    tp1   = s["tp1"]
    tp2   = s["tp2"]
    tp3   = s["tp3"]
    c1, c2, c3 = s["c1"], s["c2"], s["c3"]
    close_side = "Sell" if side == "BUY" else "Buy"

    hit_tp1 = (side=="BUY" and price >= tp1) or (side=="SELL" and price <= tp1)
    hit_tp2 = (side=="BUY" and price >= tp2) or (side=="SELL" and price <= tp2)
    hit_tp3 = (side=="BUY" and price >= tp3) or (side=="SELL" and price <= tp3)
    hit_sl  = (side=="BUY" and price <= sl)  or (side=="SELL" and price >= sl)

    # ── TP1 ──────────────────────────────────────────────────────
    if hit_tp1 and not s["tp1_hit"] and not hit_tp2:
        print(f"[LIVE] TP1 hit @ {price} — closing {c1} contracts")
        oid = ts_place_order(close_side, c1, "Market")
        if oid:
            s["tp1_hit"] = True
            sb_update("trades", s["trade_id"], {"tp1_hit": True})
            tg_send(f"✅ <b>TP1 hit</b> — closed {c1}MNQ @ <code>{tp1}</code>  +${round(34*PTS_TO_USD*c1)}")

    # ── TP2 — close c2 + cancel old SL + place new SL at entry (BE) ──
    if hit_tp2 and not s["tp2_hit"]:
        print(f"[LIVE] TP2 hit @ {price} — closing {c2} contracts, moving SL to BE")
        oid = ts_place_order(close_side, c2, "Market")
        if oid:
            # Cancel original SL order
            if s["sl_order_id"]:
                ts_cancel_order(s["sl_order_id"])
            # Place new SL at breakeven (for remaining c3 contracts)
            be_sl_id = ts_place_order(
                close_side, c3, "Stop", stop_price=entry
            )
            s["tp1_hit"] = True
            s["tp2_hit"] = True
            s["sl_order_id"] = be_sl_id
            s["sl"] = entry
            sb_update("trades", s["trade_id"], {
                "tp1_hit": True, "tp2_hit": True, "sl": entry
            })
            tg_send(
                f"⚡ <b>TP2 hit — SL at Breakeven</b>\n"
                f"Closed {c2}MNQ @ <code>{tp2}</code>  +${round(65*PTS_TO_USD*c2)}\n"
                f"SL → entry <code>{entry}</code>  |  {c3}MNQ targeting TP3 <code>{tp3}</code>"
            )

    # ── TP3 — close final c3 contracts ────────────────────────────
    if hit_tp3:
        print(f"[LIVE] TP3 hit @ {price} — closing final {c3} contracts")
        oid = ts_place_order(close_side, c3, "Market")
        if s["sl_order_id"]:
            ts_cancel_order(s["sl_order_id"])
        pnl = calc_scaled_pnl(entry, side, tp1, tp2, tp3, sl, s["contracts"],
                               True, True, True, False)
        _resolve_live_trade("WIN", True, True, True, False, pnl)
        return

    # ── SL hit ────────────────────────────────────────────────────
    if hit_sl and not hit_tp3:
        # Position was closed by the SL order on Topstep's side
        # Just resolve the Supabase record
        tp1h = s["tp1_hit"]; tp2h = s["tp2_hit"]
        if tp2h:
            result = "WIN"  # SL was at BE, so we're guaranteed +
        elif tp1h:
            result = "WIN"  # TP1 profit - remaining loss = still positive
        else:
            result = "LOSS"
        pnl = calc_scaled_pnl(entry, side, tp1, tp2, tp3, sl, s["contracts"],
                               tp1h, tp2h, False, True)
        _resolve_live_trade(result, tp1h, tp2h, False, True, pnl)

def _resolve_live_trade(result, tp1h, tp2h, tp3h, slh, pnl_usd):
    """Write final result to Supabase and send Telegram close message."""
    s = _live_trade_state
    if not s["trade_id"]:
        return

    pts = abs(s["tp3"] - s["entry"]) if tp3h else (
          abs(s["tp2"] - s["entry"]) if tp2h else (
          abs(s["tp1"] - s["entry"]) if tp1h else
          abs(s["sl"]  - s["entry"])))

    sb_update("trades", s["trade_id"], {
        "result":   result,
        "tp1_hit":  bool(tp1h),
        "tp2_hit":  bool(tp2h),
        "tp3_hit":  bool(tp3h),
        "sl_hit":   bool(slh),
        "pnl_usd":  pnl_usd,
        "pnl_pts":  round(pts, 2),
        "closed_at": datetime.now().isoformat()
    })

    # Set cooldown
    if result == "WIN":
        _cooldown["until"]  = time.time() + 1800
        _cooldown["reason"] = "WIN — 30min cooldown"
    else:
        _cooldown["until"]  = time.time() + 3600
        _cooldown["reason"] = "LOSS — 60min cooldown"
        _last_sl["price"] = s["sl"]
        _last_sl["side"]  = s["side"]
        _last_sl["time"]  = time.time()

    ev = get_eval_status()
    emoji = "✅" if result == "WIN" else "❌"
    tps = " → ".join([x for x, h in [("TP1", tp1h), ("TP2", tp2h), ("TP3", tp3h)] if h])
    tg_send(
        f"{emoji} <b>LIVE CLOSE — #{s['trade_id']}</b>  ({result})\n\n"
        f"{s['side']} {s['contracts']}MNQ  |  {tps or 'SL'}\n"
        f"P&L: <b>{'+'if pnl_usd>=0 else ''}${pnl_usd:,.2f}</b>\n\n"
        f"🏦 Topstep balance: ${ev['eval_balance']:,.2f}"
    )

    # Clear live state
    for k in _live_trade_state:
        _live_trade_state[k] = None if k in ("trade_id","sl_order_id","side") else (
                               False if k in ("tp1_hit","tp2_hit") else 0)

# ── Confirmation mode — Telegram approve before entry ────────────
# CONFIRM_MODE=true → Jarvis sends signal to Telegram, waits for /yes or /no
# CONFIRM_MODE=false (default) → fully autonomous, enters immediately
CONFIRM_MODE = os.environ.get("CONFIRM_MODE", "false").lower() == "true"
_pending_confirmation = {"signal": None, "expires": 0}

def send_confirmation_request(signal):
    """Send pending signal to Telegram, wait for /yes or /no (5min timeout)."""
    _pending_confirmation["signal"]  = signal
    _pending_confirmation["expires"] = time.time() + 300  # 5min to decide

    side  = signal["side"]
    c     = signal["contracts"]
    entry = signal["entry"]
    sl    = signal["sl"]
    tp3   = signal["tp3"]
    c1,c2,c3 = get_scale(c)
    risk  = round(40 * PTS_TO_USD * c)
    maxx  = round((34*PTS_TO_USD*c1) + (65*PTS_TO_USD*c2) + (100*PTS_TO_USD*c3))
    side_emoji = "🟢" if side=="BUY" else "🔴"

    tg_send(
        f"{side_emoji} <b>SIGNAL — WAITING FOR APPROVAL</b>\n\n"
        f"<b>{side} {c} MNQ</b> @ market (~<code>{entry}</code>)\n"
        f"SL: <code>{sl}</code>  |  TP3: <code>{tp3}</code>\n"
        f"Risk: −${risk}  |  Max: +${maxx}\n\n"
        f"Reply <b>/yes</b> to take  |  <b>/no</b> to skip\n"
        f"<i>Auto-expires in 5 minutes</i>"
    )

# ── /accounts command ─────────────────────────────────────────────
def handle_accounts_command():
    if not TOPSTEP_USERNAME:
        tg_send("⚠️ TOPSTEP_USERNAME not set. Add it to Railway env vars.")
        return
    accounts = ts_get_accounts()
    if not accounts:
        tg_send("No accounts found — check credentials.\nMake sure TOPSTEP_USERNAME matches your ProjectX dashboard username exactly.")
        return
    lines = [f"<b>Topstep Accounts ({len(accounts)})</b>\n"]
    for a in accounts:
        acct_id  = str(a.get("id") or "?")
        name     = a.get("name") or "?"
        bal      = float(a.get("balance") or 0)
        daily    = float(a.get("dailyPnl") or 0)
        sim_tag  = " [SIM]" if a.get("simulated") else ""
        can_trade = a.get("canTrade", True)
        active   = "  ◀ TRADING" if acct_id == str(TOPSTEP_ACCOUNT_ID) else ""
        flag     = "🟢" if can_trade else "🔴"
        daily_str = f"  today {'+'if daily>=0 else ''}${daily:,.0f}"
        lines.append(f"{flag} <code>{acct_id}</code>  {name}{sim_tag}\n  ${bal:,.2f}{daily_str}{active}\n")
    lines.append(f"Active: /setaccount &lt;id&gt;")
    lines.append(f"Mode: <b>{TRADING_MODE.upper()}</b>  |  Daily stop: −${DAILY_LOSS_LIMIT:,.0f}")
    tg_send("\n".join(lines))

# ── /balance command ──────────────────────────────────────────────
def handle_balance_command():
    accounts = ts_get_accounts()
    if not accounts:
        tg_send("⚠️ Can't reach Topstep — check TOPSTEP_USERNAME and TOPSTEP_API_KEY.")
        return
    lines = [f"🏦 <b>All Topstep Accounts</b>\n"]
    total_bal = 0
    for a in accounts:
        acct_id   = str(a.get("id") or "?")
        name      = a.get("name") or "?"
        bal       = float(a.get("balance") or 0)
        daily     = float(a.get("dailyPnl") or 0)
        open_pnl  = float(a.get("openPnl") or 0)
        can_trade = a.get("canTrade", True)
        sim_tag   = " [SIM]" if a.get("simulated") else ""
        flag      = "🟢" if can_trade else "🔴"
        active    = "  ◀ ACTIVE" if acct_id == str(TOPSTEP_ACCOUNT_ID) else ""
        total_bal += bal
        dl_ok = daily > -DAILY_LOSS_LIMIT
        lines.append(
            f"{flag} <b>{name}{sim_tag}</b>{active}\n"
            f"  Balance: <b>${bal:,.2f}</b>\n"
            f"  Today: {'+'if daily>=0 else ''}${daily:,.2f}  {'✅' if dl_ok else '⛔ LIMIT HIT'}\n"
            + (f"  Open P&L: {'+'if open_pnl>=0 else ''}${open_pnl:,.2f}\n" if open_pnl else "")
            + f"  ID: <code>{acct_id}</code>\n"
        )
    lines.append(f"Daily loss limit per account: −${DAILY_LOSS_LIMIT:,.0f}")
    tg_send("\n".join(lines))

# ── Wire /yes /no /accounts /balance /setaccount into commands ───
COMMANDS.setdefault("⚙️ Admin", []).extend([
    ("/accounts", "List all Topstep funded accounts + balances"),
    ("/balance",  "Live Topstep balance, today's P&L, daily limit status"),
    ("/setaccount","Switch active account: /setaccount <id>"),
])

def _start_threads():
    threading.Thread(target=start_ws,           daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    threading.Thread(target=telegram_poll_loop, daemon=True).start()
    threading.Thread(target=price_heartbeat,    daemon=True).start()

_start_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
