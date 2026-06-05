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
        ("/status",   "Full status: balance, record, open trade P&L"),
    ],
    "📊 Active Trade": [
        ("/progress", "Live progress: levels, P&L, distance to TP/SL"),
        ("/skip",     "Block the next auto-signal"),
    ],
    "📋 Performance": [
        ("/recap",    "AI-written recap: what happened + what I'm learning"),
        ("/trades",   "Last 7 days trade history"),
    ],
    "📥 Training Data": [
        ("/gm",       "Log a Goldmine callout  e.g. /gm SELL 5 30185 30095 30070 30020 SL:30225"),
    ],
}

def help_text():
    lines = "<b>Jarvis Commands</b>\n\n"
    for section, cmds in COMMANDS.items():
        lines += f"{section}\n"
        for cmd, desc in cmds:
            lines += f"  {cmd} — {desc}\n"
        lines += "\n"
    lines += "💬 Natural language works too — \"entry?\" \"explain\" \"how we doing\" \"where at\" \"results\" \"history\"\n\n"
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
                locked     = (c1*50*PTS_TO_USD if tp1_hit else 0) + (c2*75*PTS_TO_USD if tp2_hit else 0)
                total      = round(locked + unreal, 2)
                be_str     = "  SL@BE ✅" if tp2_hit else ""
                open_str   = (f"\n\n📊 <b>Open #{t['id']}</b>: {side} {contracts}MNQ @ {entry}{be_str}\n"
                              f"   Unrealized: ${unreal:+,.2f}  |  Total P&L: <b>${total:+,.2f}</b>  ({remaining} MNQ left)")

        tg_send(
            f"<b>Jarvis Status</b>\n\n"
            f"💰 Balance: <code>${stats['balance']:,.2f}</code>  ({stats['total_pnl']:+,.2f} all-time)\n"
            f"📈 Record: {stats['jarvis_wins']}W / {stats['jarvis_losses']}L  ({stats['jarvis_wr']}% WR)"
            f"{open_str}\n\n"
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
                f"• MEDIUM or HIGH conviction score\n\n"
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
        "🤖 <b>Jarvis online</b>\n\n"
        f"Watching MNQM6 — {datetime.now().strftime('%H:%M EST')}\n"
        "Commands: /price  /status  /take  /skip  /help"
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
    h = datetime.now().hour
    if 9 <= h < 16:
        return "NY", False          # 59% WR but choppy — skip
    elif 17 <= h or h < 3:
        return "Overnight", True    # 64% WR — best
    elif 3 <= h < 9:
        return "London", True       # 52% WR — ok
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

# ── Contract Sizing (mirrors Goldmine patterns) ───────────────────
def get_contracts(session_name, candles):
    strength = get_trend_strength(candles)
    if session_name == "Overnight":
        return 6 if strength >= 30 else 5
    elif session_name == "London":
        return 5
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

    # Rule 3: Pre-trend pullback (DOWN = dip = BUY setup, Goldmine pattern)
    candles = get_15min_candles()
    pre_trend = get_pretend(candles)
    if pre_trend != "DOWN":
        return None, f"Pre-trend {pre_trend} — waiting for pullback ({session_name})"

    if len(candles) < 3:
        return None, "Not enough candle data"

    # Rule 4: Determine direction from longer trend (20 candles)
    price = get_live_price()
    if not price:
        return None, "No live price"

    long_closes = [c["c"] for c in candles[-20:]] if len(candles) >= 20 else [c["c"] for c in candles]
    above_longer = price > long_closes[0]
    side = "BUY" if above_longer else "SELL"

    # Levels
    entry = round(price, 2)
    sl  = round(entry - 40, 2) if side == "BUY" else round(entry + 40, 2)
    tp1 = round(entry + 50, 2) if side == "BUY" else round(entry - 50, 2)
    tp2 = round(entry + 75, 2) if side == "BUY" else round(entry - 75, 2)
    tp3 = round(entry + 125, 2) if side == "BUY" else round(entry - 125, 2)
    contracts = get_contracts(session_name, candles)

    # Adapt size based on recent form
    wr, streak, reduce = get_jarvis_form()
    if reduce:
        contracts = max(1, contracts - 2)  # cut size on bad streak

    strength = get_trend_strength(candles)

    # Conviction scoring — based on what actually won in 66 Goldmine trades
    # Overnight 64% WR, strong pre-trend, clean directional alignment = HIGH
    score = 0
    if session_name == "Overnight": score += 3
    elif session_name == "London":  score += 1
    if strength >= 40:  score += 3
    elif strength >= 25: score += 2
    elif strength >= 15: score += 1
    # Trend alignment: long trend and short trend pointing same way
    if len(candles) >= 20:
        long_trend  = "DOWN" if long_closes[-1] < long_closes[0] else "UP"
        short_trend = pre_trend
        if (side == "BUY"  and long_trend == "UP"   and short_trend == "DOWN") or \
           (side == "SELL" and long_trend == "DOWN" and short_trend == "UP"):
            score += 2  # perfect setup: pullback against prevailing trend
    if   score >= 7: conviction = "HIGH"
    elif score >= 4: conviction = "MEDIUM"
    else:            conviction = "LOW"

    # Only auto-enter HIGH or MEDIUM — skip LOW conviction setups
    if conviction == "LOW":
        return None, f"LOW conviction ({session_name}, strength {strength:.0f}pts) — skipping"

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
        "conviction": conviction,
        "strength":   round(strength, 1),
        "time":       datetime.now().strftime("%H:%M EST")
    }
    return signal, "SIGNAL"

# ── Build signal Telegram message ─────────────────────────────────
def format_signal_message(sig):
    side_emoji = "🟢" if sig["side"] == "BUY" else "🔴"
    conviction_map = {"HIGH": "🔥 HIGH", "MEDIUM": "⚡ MEDIUM", "LOW": "💧 LOW"}
    conviction_str = conviction_map.get(sig["conviction"], sig["conviction"])

    session_wr = {"Overnight": 64, "London": 52}.get(sig["session"], 59)
    risk_usd   = round(40 * PTS_TO_USD * sig["contracts"])
    tp3_usd    = round(125 * PTS_TO_USD * sig["contracts"])

    # Explain the entry
    if sig["side"] == "BUY":
        entry_explanation = (
            f"Price pulled back {sig['strength']:.0f}pts over the last 90min "
            f"but the bigger trend is still UP — this is a dip entry, expecting a bounce."
        )
    else:
        entry_explanation = (
            f"Price pushed up {sig['strength']:.0f}pts over the last 90min "
            f"but the bigger trend is DOWN — fading the push, expecting a roll."
        )

    return (
        f"{side_emoji} <b>JARVIS SIGNAL — {sig['side']} MNQ</b>\n\n"
        f"<b>Entry:</b>  <code>{sig['entry']}</code>  ({sig['time']})\n"
        f"<b>SL:</b>     <code>{sig['sl']}</code>  (−{risk_usd}$ risk)\n"
        f"<b>TP1:</b>    <code>{sig['tp1']}</code>  (+50pts)\n"
        f"<b>TP2:</b>    <code>{sig['tp2']}</code>  (+75pts)\n"
        f"<b>TP3:</b>    <code>{sig['tp3']}</code>  (+${tp3_usd})\n\n"
        f"<b>Session:</b> {sig['session']} ({session_wr}% WR historically)\n"
        f"<b>Conviction:</b> {conviction_str}  |  <b>Size:</b> {sig['contracts']} MNQ\n\n"
        f"💡 {entry_explanation}\n\n"
        f"Reply <b>/take</b> to enter  |  <b>/skip</b> to pass"
    )

# ── Auto-resolve open trades ──────────────────────────────────────
def check_open_jarvis_trades():
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

        if not all([entry, sl]):
            continue

        result = tp1h = tp2h = tp3h = slh = None
        pts = 0

        if side == "BUY":
            if tp3 and price >= tp3:
                result="WIN"; tp1h=tp2h=tp3h=True; pts=tp3-entry
            elif tp2 and price >= tp2:
                result="WIN"; tp1h=tp2h=True; tp3h=False; pts=tp2-entry
            elif tp1 and price >= tp1:
                result="WIN"; tp1h=True; tp2h=tp3h=False; pts=tp1-entry
            elif price <= sl:
                result="LOSS"; slh=True; pts=sl-entry
        else:
            if tp3 and price <= tp3:
                result="WIN"; tp1h=tp2h=tp3h=True; pts=entry-tp3
            elif tp2 and price <= tp2:
                result="WIN"; tp1h=tp2h=True; tp3h=False; pts=entry-tp2
            elif tp1 and price <= tp1:
                result="WIN"; tp1h=True; tp2h=tp3h=False; pts=entry-tp1
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
            tps_hit = " → ".join([x for x, h in [("TP1", tp1h), ("TP2", tp2h), ("TP3", tp3h)] if h])
            if result == "WIN":
                msg = (
                    f"✅ <b>WIN — Jarvis #{trade['id']}</b>\n\n"
                    f"{trade['side']} {contracts}MNQ  |  {tps_hit}\n"
                    f"+{round(pts)}pts  |  <b>+${pnl_usd:,.2f}</b>\n\n"
                    f"Nice. Watching for the next setup."
                )
            else:
                msg = (
                    f"❌ <b>LOSS — Jarvis #{trade['id']}</b>\n\n"
                    f"{trade['side']} {contracts}MNQ  |  SL hit @ {sl}\n"
                    f"{round(pts)}pts  |  <b>-${abs(pnl_usd):,.2f}</b>\n\n"
                    f"It happens. Back to watching."
                )
            tg_send(msg)

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
            "conviction": signal["conviction"],
            "strength":   signal["strength"],
            "taken":      taken,
            "skipped":    skipped,
            "notes":      f"pre_trend:{signal['pre_trend']}"
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
        "notes":      f"Session:{signal['session']} Conviction:{signal['conviction']} AUTO"
    }
    logged = sb_insert("trades", trade)
    if not logged:
        return

    c1, c2, c3 = get_scale(signal["contracts"])
    risk_usd = round(40  * PTS_TO_USD * signal["contracts"])
    tp1_usd  = round(50  * PTS_TO_USD * c1)
    tp2_usd  = round(75  * PTS_TO_USD * c2)
    tp3_usd  = round(125 * PTS_TO_USD * c3)
    max_usd  = tp1_usd + tp2_usd + tp3_usd
    side_emoji = "🟢" if signal["side"] == "BUY" else "🔴"
    session_wr = 64 if signal["session"] == "Overnight" else 52 if signal["session"] == "London" else 59

    # Get form context for the alert
    wr, streak, reduce = get_jarvis_form()
    form_str = ""
    if streak >= 3:
        form_str = f"🔥 {streak} trade win streak"
    elif streak <= -2:
        form_str = f"⚠️ {abs(streak)} losses in a row — sized down to {signal['contracts']} MNQ"
    elif wr:
        form_str = f"📈 {wr}% WR last 10 trades"

    tg_send(
        f"{side_emoji} <b>JARVIS ENTERED — #{logged['id']}</b>\n\n"
        f"<b>{signal['side']} {signal['contracts']} MNQ</b> @ <code>{signal['entry']}</code>\n\n"
        f"SL:  <code>{signal['sl']}</code>  (−${risk_usd})  [{signal['contracts']} MNQ]\n"
        f"TP1: <code>{signal['tp1']}</code>  close {c1} MNQ → +${tp1_usd}\n"
        f"TP2: <code>{signal['tp2']}</code>  close {c2} MNQ → +${tp2_usd}  ← SL to BE\n"
        f"TP3: <code>{signal['tp3']}</code>  close {c3} MNQ → +${tp3_usd}\n"
        f"Max profit: <b>+${max_usd}</b>\n\n"
        f"📊 {signal['session']} | {session_wr}% WR hist | {signal['conviction']} conviction\n"
        f"💡 {'Dip entry — trend UP, bought pullback' if signal['side'] == 'BUY' else 'Fade — trend DOWN, sold push'}\n"
        + (f"{form_str}\n" if form_str else "") +
        f"\nSend /progress anytime for live update. 🤖"
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
                    f"Targeting TP3 @ <code>{tp3}</code>  (+${round(125*PTS_TO_USD*contracts)})"
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
            if result == "WIN":
                tps = " → ".join([x for x, h in [("TP1", tp1h), ("TP2", tp2h), ("TP3", tp3h)] if h])
                msg = (
                    f"✅ <b>WIN — #{trade['id']}</b>\n\n"
                    f"{side} {contracts}MNQ  |  {tps}\n"
                    f"+{round(pts)}pts  |  <b>+${pnl_usd:,.2f}</b>\n\n"
                    f"Back to watching. 👀"
                )
            else:
                msg = (
                    f"❌ <b>LOSS — #{trade['id']}</b>\n\n"
                    f"{side} {contracts}MNQ  |  SL hit @ <code>{sl}</code>\n"
                    f"{round(pts)}pts  |  <b>−${abs(pnl_usd):,.2f}</b>\n\n"
                    f"Part of the game. Back to watching."
                )
            tg_send(msg)

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
                    "content": f"""You are Jarvis, an MNQ futures trading bot. Give a SHORT intelligent recap (3-5 sentences max, casual tone like a smart trading partner). Cover: overall performance vibe, what's working or not, and one insight about what the data is showing. Be direct and honest. No fluff.

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
    locked = (c1*50*PTS_TO_USD if tp1_hit else 0) + (c2*75*PTS_TO_USD if tp2_hit else 0)
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
    last_signal_session = None   # track which session we last traded
    last_recap_day      = None
    last_chime_time     = 0

    while True:
        try:
            check_open_jarvis_trades()

            signal, msg = check_for_signal()
            if signal:
                session_key = signal["session"] + datetime.now().strftime("%Y-%m-%d")
                # MAX 1 trade per session per day (Overnight = 1, London = 1)
                if last_signal_session != session_key:
                    log_signal(signal)
                    auto_enter_trade(signal)
                    last_signal_session = session_key

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

# ── Startup ───────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=start_ws,           daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()
    threading.Thread(target=telegram_poll_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
