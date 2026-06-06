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

# optional Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials as GCredentials
    GSPREAD_OK = True
except ImportError:
    GSPREAD_OK = False

# ── Config ────────────────────────────────────────────────────────
POLYGON_KEY    = os.environ["POLYGON_API_KEY"]
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_CREDS    = os.environ.get("GOOGLE_CREDS_JSON", "")  # service account JSON as string
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
_eval_checkpoints = []   # list of {"type": "funded"|"blown", "total_pnl": float}
_eval_alerted     = {"last": None}  # prevent repeat alerts

# ── Trade Cooldown ────────────────────────────────────────────────
# After a trade closes, wait before next entry.
# Win: 30min cooldown  |  Loss: 60min cooldown
# Mirrors Goldmine's 1-2 trades/day pace, prevents spam entries.
_cooldown = {"until": 0, "reason": ""}

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
        ("/price",    "Live MNQM6 price + session + pre-trend"),
        ("/analysis", "Full market read — what Jarvis sees + what it needs for a setup"),
        ("/status",   "Full status: eval balance, record, open trade P&L"),
    ],
    "📊 Active Trade": [
        ("/progress", "Live progress: levels, P&L, eval balance, distance to TP/SL"),
        ("/skip",     "Block the next auto-signal"),
    ],
    "📋 Performance": [
        ("/recap",    "AI-written recap: technical analysis of what's working"),
        ("/trades",   "Last 7 days trade history"),
        ("/daily",    "Today's recap — trades, P&L, eval status + sheet row"),
        ("/weekly",   "This week's recap — full stats + sheet rows"),
        ("/sheet",    "Copy-paste spreadsheet block for all recent trades"),
    ],
    "📥 Training Data": [
        ("/gm",       "Log a Goldmine callout  e.g. /gm SELL 5 30185 30095 30070 30020 SL:30225"),
    ],
}

def help_text():
    lines = "<b>Jarvis Commands v2.1</b>\n\n"
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

    # ── /skip ──────────────────────────────────────────────────────
    if cmd in ("/skip", "skip", "❌", "nah"):
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
                unreal, remaining = calc_unrealized_pnl(entry, side, price, contracts, tp1_hit, tp2_hit)
                c1, c2, _  = get_scale(contracts)
                locked     = (c1*(tp1-entry if side=="BUY" else entry-tp1)*PTS_TO_USD if tp1_hit else 0) + (c2*(tp2-entry if side=="BUY" else entry-tp2)*PTS_TO_USD if tp2_hit else 0)
                total      = round(locked + unreal, 2)
                be_str     = "  SL@BE ✅" if tp2_hit else ""
                open_str   = (f"\n\n📊 <b>Open #{t['id']}</b>: {side} {contracts}MNQ @ {entry}{be_str}\n"
                              f"   Unrealized: ${unreal:+,.2f}  |  Total P&L: <b>${total:+,.2f}</b>  ({remaining} MNQ left)")

        ev = get_eval_status()
        # Eval progress bar
        eval_pct  = max(0, min(100, round((ev["eval_pnl"] + 2000) / 5000 * 100)))
        eval_bar  = "█" * round(eval_pct/10) + "░" * (10 - round(eval_pct/10))
        dd_pct    = round(ev["drawdown_used"] / 2000 * 100) if ev["drawdown_used"] > 0 else 0
        eval_str  = (
            f"\n\n🏦 <b>Eval #{ev['eval_num']}</b>  (Funded: {ev['funded']}  Blown: {ev['blown']})\n"
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

    tg_send(
        "🤖 <b>Jarvis online v2.1</b>\n\n"
        f"Watching MNQM6 — {datetime.now().strftime('%H:%M EST')}\n"
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
    time.sleep(5)
    start_ws()

def start_ws():
    ws = websocket.WebSocketApp(
        "wss://socket.polygon.io/futures",
        on_message=on_ws_message,
        on_error=on_ws_error,
        on_close=on_ws_close
    )
    ws.run_forever()

def get_live_price():
    if _ws_price["price"] and time.time() - _ws_price["updated"] < 30:
        return _ws_price["price"]
    try:
        url = f"https://api.polygon.io/futures/v1/snapshot?product_code=MNQ&apiKey={POLYGON_KEY}"
        data = requests.get(url, timeout=5).json()
        for r in data.get("results", []):
            if r.get("details", {}).get("ticker") == "MNQM6":
                p = r.get("last_trade", {}).get("price")
                if p:
                    return float(p)
    except:
        pass
    return _ws_price["price"]

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
        return None, f"NY session — sitting out"

    candles = get_15min_candles()
    if len(candles) < 6:
        return None, "Not enough candle data"

    pre_trend = get_pretend(candles)
    strength  = get_trend_strength(candles)

    price = get_live_price()
    if not price:
        return None, "No live price"

    # ── Bigger trend direction (20 candles) ───────────────────────
    long_closes = [c["c"] for c in candles[-20:]] if len(candles) >= 20 else [c["c"] for c in candles]
    long_trend  = "DOWN" if long_closes[-1] < long_closes[0] else "UP"

    # ── Directional bias from recent trade history ─────────────────
    bias = get_directional_bias()

    # ── Determine side from pre-trend + bigger trend ───────────────
    # pre-trend DOWN = dip/pullback → BUY the dip (if bigger trend UP)
    #                               → SELL continuation (if bigger trend DOWN)
    # pre-trend UP   = push/bounce  → SELL the push (if bigger trend DOWN)
    #                               → BUY continuation (if bigger trend UP)
    if pre_trend == "DOWN":
        side = "BUY" if long_trend == "UP" else "SELL"
    elif pre_trend == "UP":
        side = "SELL" if long_trend == "DOWN" else "BUY"
    else:
        return None, "No clear pre-trend"

    # Bias override: if recent 4/5 trades strongly lean one way, trust it
    if bias and bias != side:
        # Counter-bias signal — still take it but need stronger pre-trend
        if strength < 20:
            return None, f"Pre-trend weak ({strength:.0f}pts) + counter to {bias} bias — skipping"

    # Minimum pre-trend strength — needs to be a real move not noise
    if strength < 10:
        return None, f"Pre-trend too weak ({strength:.0f}pts) — noise"

    aligned = (side == "BUY" and long_trend == "UP") or (side == "SELL" and long_trend == "DOWN")

    # ── Levels — based on actual Goldmine data ─────────────────────
    entry = round(price, 2)
    d = 1 if side == "BUY" else -1
    sl  = round(entry - d * 40,  2)
    tp1 = round(entry + d * 34,  2)   # avg 33.7pts across 61 Goldmine trades
    tp2 = round(entry + d * 65,  2)   # avg 65.0pts
    tp3 = round(entry + d * 100, 2)   # avg 101.7pts

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
        "bias":       bias,
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
        _eval_checkpoints.append({"type": "funded", "total_pnl": ev["total_pnl"]})
        ev2 = get_eval_status()
        tg_send(
            f"🏆 <b>EVAL FUNDED — #{ev['eval_num']}</b>\n\n"
            f"Account hit ${EVAL_TARGET:,.0f}  (+$3,000)\n"
            f"Total funded: {ev2['funded']}  |  Blown: {ev2['blown']}\n\n"
            f"Starting Eval #{ev2['eval_num']} — fresh $50k, let's go."
        )
    elif bal <= EVAL_FLOOR:
        _eval_alerted["last"] = key
        _eval_checkpoints.append({"type": "blown", "total_pnl": ev["total_pnl"]})
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

def auto_enter_trade(signal):
    """Auto-log trade to Supabase and alert Telegram — no manual /take needed."""
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
        "notes":      f"Session:{signal['session']} Strength:{signal['strength']} Aligned:{signal['aligned']} AUTO"
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

    tg_send(
        f"{side_emoji} <b>JARVIS ENTERED — #{logged['id']}</b>\n\n"
        f"<b>{signal['side']} {signal['contracts']} MNQ</b> @ <code>{signal['entry']}</code>\n\n"
        f"<b>Why:</b>\n"
        f"  5hr:   {lt} ({long_move:+.0f}pts)\n"
        f"  2.5hr: {'DOWN' if mid_move < 0 else 'UP'} ({mid_move:+.0f}pts)\n"
        f"  45min: {'DOWN' if short_move < 0 else 'UP'} ({short_move:+.0f}pts)\n"
        f"  → {setup_str}{bias_str}\n\n"
        f"SL:  <code>{signal['sl']}</code>  (−${risk_usd})\n"
        f"TP1: <code>{signal['tp1']}</code>  {c1}MNQ → +${tp1_usd}\n"
        f"TP2: <code>{signal['tp2']}</code>  {c2}MNQ → +${tp2_usd}  ← SL→BE\n"
        f"TP3: <code>{signal['tp3']}</code>  {c3}MNQ → +${tp3_usd}\n"
        f"Max: <b>+${max_usd}</b>\n\n"
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

        # Check for final resolution
        result = tp1h = tp2h = tp3h = slh = None
        pts = 0

        if side == "BUY":
            if tp3 and price >= tp3:
                result="WIN"; tp1h=tp2h=tp3h=True; pts=tp3-entry
            elif trade.get("tp2_hit") and price <= sl:
                result="WIN"; tp1h=True; tp2h=True; tp3h=False; pts=sl-entry  # closed at BE = 0pts but WIN
            elif tp1 and price >= tp1 and not trade.get("tp1_hit"):
                sb_update("trades", trade["id"], {"tp1_hit": True})
                tg_send(f"✅ TP1 hit — Trade #{trade['id']}  |  +{round(tp1-entry)}pts  |  Holding for TP2/TP3")
            elif price <= sl:
                result="LOSS"; slh=True; pts=sl-entry
        else:
            if tp3 and price <= tp3:
                result="WIN"; tp1h=tp2h=tp3h=True; pts=entry-tp3
            elif trade.get("tp2_hit") and price >= sl:
                result="WIN"; tp1h=True; tp2h=True; tp3h=False; pts=entry-sl
            elif tp1 and price <= tp1 and not trade.get("tp1_hit"):
                sb_update("trades", trade["id"], {"tp1_hit": True})
                tg_send(f"✅ TP1 hit — Trade #{trade['id']}  |  +{round(entry-tp1)}pts  |  Holding for TP2/TP3")
            elif price >= sl:
                result="LOSS"; slh=True; pts=entry-sl

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
            tg_send(msg)
            push_trade_to_sheets(trade["id"])

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

    # Build the AI analysis if we have the key
    ai_read = ""
    if ANTHROPIC_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
            candle_summary = f"Last 6 closes (15min): {[round(c['c'],1) for c in candles[-6:]]}"
            msg = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=200,
                messages=[{
                    "role": "user",
                    "content": f"""You are Jarvis, an MNQ futures algo. Give a SHORT market read in 2-3 sentences. Casual, direct, like a trader talking to another trader. Cover: what the price action looks like right now, where you think it's likely to go next (up/down/chop), and why. No fluff, no disclaimers.

Current price: {price}
Session: {session_name} ({'active' if session_ok else 'sitting out'})
5hr trend: {long_trend} ({long_move:+.0f}pts)
2.5hr trend: {mid_trend} ({mid_move:+.0f}pts)
45min trend: {short_trend} ({short_move:+.0f}pts)
Range: {recent_low} – {recent_high} ({range_size}pts), price at {range_pos}
Recent trade bias: {bias or 'neutral'}
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
        f"<b>Range:</b> {recent_low} – {recent_high}  ({range_size}pts)\n"
        f"Price at {range_pos}"
        f"{bias_str}"
        f"{ai_read}\n\n"
        f"<b>Setup status:</b>\n{setup_needed}"
    )

# ── Google Sheets ─────────────────────────────────────────────────
def get_gsheet():
    """Return (worksheet_trades, worksheet_daily, worksheet_weekly, worksheet_eval) or None."""
    if not GSPREAD_OK or not GOOGLE_SHEET_ID or not GOOGLE_CREDS:
        return None
    try:
        creds_dict = json.loads(GOOGLE_CREDS)
        creds = GCredentials.from_service_account_info(
            creds_dict,
            scopes=["https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(GOOGLE_SHEET_ID)

        # Get or create tabs
        def get_or_create(name, headers):
            try:
                ws = sh.worksheet(name)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=name, rows=1000, cols=len(headers))
                ws.append_row(headers)
            return ws

        trades_ws = get_or_create("Trade Log", [
            "Date","Time","Trade ID","Direction","Size MNQ","Entry","Stop",
            "TP1","TP2","TP3","TP Hit","Result","Points","P&L","Balance After",
            "To Pass","To Fail","Eval #","Session","Notes"
        ])
        daily_ws = get_or_create("Daily Recaps", [
            "Date","Trades","Wins","Losses","Win Rate","Total P&L","Avg Win",
            "Avg Loss","Best Trade","Worst Trade","Balance EOD","To Pass","To Fail","Eval Status"
        ])
        weekly_ws = get_or_create("Weekly Recaps", [
            "Week Start","Week End","Trades","Wins","Losses","Win Rate","Total P&L",
            "Avg Win","Avg Loss","Profit Factor","Max DD","Best","Worst",
            "Eval Passed?","Eval Failed?","Ending Balance"
        ])
        eval_ws = get_or_create("Eval Simulator", [
            "Eval #","Start Date","End Date","Trades","Final Balance","Result",
            "Max Drawdown","Total P&L","Days"
        ])
        return trades_ws, daily_ws, weekly_ws, eval_ws
    except Exception as e:
        print(f"[SHEETS] Error: {e}")
        return None

def trade_to_sheet_row(trade):
    """Convert a trade dict to a spreadsheet row."""
    ev = get_eval_status()
    dt  = (trade.get("trade_date") or "")
    tp_hit = ("TP3" if trade.get("tp3_hit") else
              "TP2" if trade.get("tp2_hit") else
              "TP1" if trade.get("tp1_hit") else
              "SL"  if trade.get("sl_hit")  else "OPEN")
    notes = trade.get("notes","")
    session = notes.split("Session:")[-1].split()[0] if "Session:" in notes else ""
    return [
        dt[:10], dt[11:16],
        trade.get("id",""), trade.get("side",""), trade.get("contracts",""),
        trade.get("entry",""), trade.get("sl",""),
        trade.get("tp1",""), trade.get("tp2",""), trade.get("tp3",""),
        tp_hit, trade.get("result",""),
        trade.get("pnl_pts",""), trade.get("pnl_usd",""),
        ev["eval_balance"], ev["to_target"], ev["to_floor"],
        ev["eval_num"], session, notes[:80]
    ]

def push_trade_to_sheets(trade_id):
    """Auto-push a completed trade to Google Sheets if configured."""
    try:
        sheets = get_gsheet()
        if not sheets: return
        trades_ws = sheets[0]
        trade = sb_select("trades", {"id": trade_id})
        if not trade: return
        row = trade_to_sheet_row(trade[0])
        trades_ws.append_row(row)
        print(f"[SHEETS] Trade #{trade_id} pushed")
    except Exception as e:
        print(f"[SHEETS] Push error: {e}")

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

    # Also push to Google Sheets if connected
    try:
        sheets = get_gsheet()
        if sheets:
            sheets[1].append_row(sheet_row.split(" | "))
    except: pass

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

    try:
        sheets = get_gsheet()
        if sheets:
            sheets[2].append_row(sheet_row.split(" | "))
    except: pass

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

    # Build data summary for Claude
    trade_summary = f"""
Jarvis MNQ trading bot stats:
- Total trades: {len(resolved)} ({len(wins)}W / {len(losses)}L)
- Win rate: {wr}%
- Total P&L: ${total_pnl:+,.2f}
- Current streak: {streak_str}
- TP breakdown (wins): TP3={tp3_wins}, TP2={tp2_wins}, TP1={tp1_wins}
- Recent trades (newest first): {[{"side":t["side"],"result":t["result"],"pnl":t.get("pnl_usd",0),"tp3":t.get("tp3_hit"),"session":(t.get("notes") or "").split("Session:")[-1].split()[0] if "Session:" in (t.get("notes") or "") else "?"} for t in resolved[:8]]}
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
        f"<b>Numbers:</b>\n"
        f"{len(resolved)} trades  |  {wr}% WR  |  ${total_pnl:+,.2f}\n"
        f"TP3: {tp3_wins}  TP2: {tp2_wins}  TP1: {tp1_wins}  ({streak_str})\n\n"
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
    last_entry_price = None   # don't re-enter if price barely moved
    last_recap_day   = None
    last_chime_time  = 0

    while True:
        try:
            check_open_jarvis_trades()
            check_eval_thresholds()

            signal, msg = check_for_signal()
            if signal:
                price = signal["entry"]
                # Cooldown check — don't re-enter during cooldown period
                if time.time() < _cooldown["until"]:
                    mins_left = round((_cooldown["until"] - time.time()) / 60)
                    print(f"[SIGNAL] Blocked by cooldown ({_cooldown['reason']}, {mins_left}min left)")
                elif last_entry_price is None or abs(price - last_entry_price) >= 25:
                    log_signal(signal)
                    auto_enter_trade(signal)
                    last_entry_price = price

            # Periodic chime every 20min on open trade
            open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
            if open_trades and time.time() - last_chime_time > 1200:
                send_progress_chime()
                last_chime_time = time.time()

            # Daily recap at 4pm ET
            now_dt = datetime.now()
            if now_dt.hour == 16 and now_dt.minute < 1:
                today = now_dt.strftime("%Y-%m-%d")
                if last_recap_day != today:
                    send_daily_recap()
                    last_recap_day = today

        except Exception as e:
            pass
        time.sleep(30)

# ── Startup — runs on import (works with gunicorn AND python app.py) ──
def _start_threads():
    threading.Thread(target=start_ws,           daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    threading.Thread(target=telegram_poll_loop, daemon=True).start()

_start_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
