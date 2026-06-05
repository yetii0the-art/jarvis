#!/usr/bin/env python3
"""
Jarvis — MNQ Signal Engine + Conversational Telegram Bot + Web UI
"""

import os, json, time, requests, threading
import yfinance as yf
import pandas as pd
import websocket
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request

# ── Config ────────────────────────────────────────────────────────
POLYGON_KEY    = os.environ["POLYGON_API_KEY"]
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

def handle_tg_command(text):
    text = text.strip().lower()
    cmd = text.split()[0] if text else ""

    if cmd in ("/take", "take", "✅"):
        sig = _pending_signal["signal"]
        if not sig:
            tg_send("No active signal right now. I'll ping you when the next one fires.")
            return
        log_signal(sig, taken=True)
        trade = {
            "trade_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "side":       sig["side"],
            "entry":      sig["entry"],
            "sl":         sig["sl"],
            "tp1":        sig["tp1"],
            "tp2":        sig["tp2"],
            "tp3":        sig["tp3"],
            "contracts":  sig["contracts"],
            "trader":     "JARVIS",
            "source":     "JARVIS",
            "result":     "OPEN",
            "notes":      f"Session:{sig['session']} PreTrend:{sig['pre_trend']} Conviction:{sig.get('conviction','')}"
        }
        logged = sb_insert("trades", trade)
        if logged:
            _pending_signal["signal"] = None
            pnl_risk = round(40 * PTS_TO_USD * sig["contracts"])
            pnl_tp3  = round(125 * PTS_TO_USD * sig["contracts"])
            tg_send(
                f"✅ <b>TRADE OPEN — #{logged['id']}</b>\n\n"
                f"<b>{sig['side']} {sig['contracts']} MNQ</b>\n"
                f"Entry: <code>{sig['entry']}</code>\n"
                f"SL:    <code>{sig['sl']}</code>  (−${pnl_risk} if hit)\n"
                f"TP1:   <code>{sig['tp1']}</code>  (+${round(50*PTS_TO_USD*sig['contracts'])})\n"
                f"TP2:   <code>{sig['tp2']}</code>  (+${round(75*PTS_TO_USD*sig['contracts'])})\n"
                f"TP3:   <code>{sig['tp3']}</code>  (+${pnl_tp3})\n\n"
                f"I'm watching it live. Will alert when TP or SL hits."
            )
        else:
            tg_send("⚠️ DB error logging trade. Check Supabase.")

    elif cmd in ("/skip", "skip", "❌", "nah"):
        if _pending_signal["signal"]:
            log_signal(_pending_signal["signal"], skipped=True)
            _pending_signal["signal"] = None
            tg_send("⏭ Skipped. Logged it. Watching for the next setup.")
        else:
            tg_send("No pending signal to skip.")

    elif cmd in ("/price", "price", "p"):
        price = get_live_price()
        session_name, session_ok = get_session()
        candles = get_15min_candles()
        pre_trend = get_pretend(candles)
        trend_emoji = "📉" if pre_trend == "DOWN" else "📈"
        session_emoji = "🟢" if session_ok else "🔴"
        age = round(time.time() - _ws_price["updated"], 1) if _ws_price["updated"] else "?"
        tg_send(
            f"<b>MNQM6</b>  <code>{price:,.2f}</code>  ({age}s ago)\n"
            f"{session_emoji} {session_name}  {trend_emoji} Pre-trend {pre_trend or '?'}"
        )

    elif cmd in ("/status", "status", "s"):
        price = get_live_price()
        stats = get_stats()
        session_name, session_ok = get_session()
        candles = get_15min_candles()
        pre_trend = get_pretend(candles)
        _, status_msg = check_for_signal()

        open_str = ""
        for t in stats["open_trades"]:
            side = t.get("side")
            entry = t.get("entry", 0)
            contracts = t.get("contracts", 5)
            if price and entry:
                pts = (price - entry) if side == "BUY" else (entry - price)
                live_pnl = round(pts * PTS_TO_USD * contracts, 2)
                open_str = f"\n📊 Open #{t['id']}: {side} @ {entry}  Live: ${live_pnl:+,.2f}"

        tg_send(
            f"<b>Jarvis Status</b>\n\n"
            f"💰 Balance: <code>${stats['balance']:,.2f}</code>  ({stats['total_pnl']:+.2f})\n"
            f"📈 Record: {stats['jarvis_wins']}W / {stats['jarvis_losses']}L  ({stats['jarvis_wr']}% WR)\n"
            f"{open_str}\n\n"
            f"🕐 {session_name} session  |  Pre-trend: {pre_trend or '?'}\n"
            f"📡 Price: {price:,.2f}\n"
            f"💬 {status_msg}"
        )

    elif cmd in ("/progress", "progress", "prog", "p&l", "pnl", "where", "where at", "update"):
        open_trades = sb_select("trades", {"result": "OPEN", "source": "JARVIS"})
        if not open_trades:
            tg_send("No open trade right now. Watching for the next setup.")
            return
        t = open_trades[0]
        price     = get_live_price()
        side      = t["side"]
        entry     = t["entry"]
        sl        = t["sl"]
        tp1       = t["tp1"]
        tp2       = t["tp2"]
        tp3       = t["tp3"]
        contracts = t.get("contracts", 5)
        tp1_hit   = t.get("tp1_hit", False)
        tp2_hit   = t.get("tp2_hit", False)

        pts = (price - entry) if side == "BUY" else (entry - price)
        pnl_usd = round(pts * PTS_TO_USD * contracts, 2)

        # Distance to each level
        dist_sl  = abs(price - sl)
        dist_tp1 = abs(price - tp1)
        dist_tp2 = abs(price - tp2)
        dist_tp3 = abs(price - tp3)

        # Progress bar: SL → TP3 range
        total_range = abs(tp3 - sl)
        progress_pts = pts + 40  # shift so SL=0, entry=40
        pct = max(0, min(100, round(progress_pts / total_range * 100)))
        bar_filled = round(pct / 10)
        bar = "█" * bar_filled + "░" * (10 - bar_filled)

        # Status
        if pts > 0:
            status = "🟢 IN PROFIT"
        elif pts == 0:
            status = "⚪ BREAKEVEN"
        else:
            status = "🔴 IN DRAWDOWN"

        tp1_str = "✅" if tp1_hit else f"<code>{tp1}</code> ({dist_tp1:.0f}pts away)"
        tp2_str = "✅ (SL @ BE)" if tp2_hit else f"<code>{tp2}</code> ({dist_tp2:.0f}pts away)"
        tp3_str = f"<code>{tp3}</code> ({dist_tp3:.0f}pts away)"

        tg_send(
            f"📊 <b>Trade #{t['id']} — {status}</b>\n\n"
            f"{side} {contracts}MNQ  |  Entry: <code>{entry}</code>\n"
            f"Live price: <code>{price:,.2f}</code>\n\n"
            f"[{bar}] {pct}%\n"
            f"SL {'(BE)' if tp2_hit else ''} ←————————→ TP3\n\n"
            f"P&L: <b>{'+'if pts>=0 else ''}{round(pts)}pts  |  ${pnl_usd:+,.2f}</b>\n\n"
            f"TP1: {tp1_str}\n"
            f"TP2: {tp2_str}\n"
            f"TP3: {tp3_str}\n"
            f"SL:  <code>{sl}</code> ({dist_sl:.0f}pts away)"
        )

    elif cmd in ("/help", "help", "?"):
        tg_send(
            "<b>Jarvis Commands</b>\n\n"
            "/take  — enter the pending signal\n"
            "/skip  — pass on the signal\n"
            "/price — live MNQ price\n"
            "/status — full system status\n"
            "/help — this message\n\n"
            "I'll alert you automatically when a setup fires."
        )

    elif any(w in text for w in ["entry", "entries", "what", "explain", "why", "how", "setup", "levels", "signal"]):
        sig = _pending_signal["signal"]
        candles = get_15min_candles()
        session_name, _ = get_session()
        pre_trend = get_pretend(candles)
        strength = get_trend_strength(candles)
        price = get_live_price()
        if sig:
            tg_send(
                f"<b>Here's what Jarvis is seeing:</b>\n\n"
                f"📍 Current price: <code>{price:,.2f}</code>\n"
                f"📉 Pre-trend: price dropped {strength:.0f}pts over last 90min\n"
                f"📊 Bigger trend: {'UP (long bias)' if sig['side'] == 'BUY' else 'DOWN (short bias)'}\n\n"
                f"<b>The setup:</b> {sig['side']} — {'dip into an uptrend' if sig['side'] == 'BUY' else 'push into a downtrend'}\n"
                f"Entry @ <code>{sig['entry']}</code>  →  Risk 40pts, Target up to 125pts\n"
                f"R:R = 1:3.1 at TP3\n\n"
                f"Based on 66 Goldmine trades — {session_name} session historically {64 if session_name == 'Overnight' else 52 if session_name == 'London' else 59}% WR\n\n"
                f"/take to enter  |  /skip to pass"
            )
        else:
            tg_send(
                f"<b>Current conditions:</b>\n\n"
                f"📍 Price: <code>{price:,.2f}</code>\n"
                f"🕐 Session: {session_name}\n"
                f"📈 Pre-trend: {pre_trend or '?'} ({strength:.0f}pts move)\n\n"
                f"Waiting for: London/Overnight session + pre-trend DOWN\n"
                f"Will alert when a setup fires."
            )

    else:
        tg_send(
            f"Didn't catch that. Try /price, /status, or /help.\n"
            f"If there's a pending signal, say /take or /skip."
        )

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
    if session_name == "Overnight" and strength >= 30:
        conviction = "HIGH"
    elif session_name == "Overnight":
        conviction = "MEDIUM"
    elif session_name == "London" and strength >= 20:
        conviction = "MEDIUM"
    else:
        conviction = "LOW"

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
            pnl_usd = round(pts * PTS_TO_USD * contracts, 2)
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

    risk_usd = round(40 * PTS_TO_USD * signal["contracts"])
    tp3_usd  = round(125 * PTS_TO_USD * signal["contracts"])
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
        f"SL:  <code>{signal['sl']}</code>  (−${risk_usd})\n"
        f"TP1: <code>{signal['tp1']}</code>  (+${round(50*PTS_TO_USD*signal['contracts'])})\n"
        f"TP2: <code>{signal['tp2']}</code>  (+${round(75*PTS_TO_USD*signal['contracts'])})  ← SL moves to BE\n"
        f"TP3: <code>{signal['tp3']}</code>  (+${tp3_usd})\n\n"
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
            pnl_usd = round(pts * PTS_TO_USD * contracts, 2)
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

def background_monitor():
    """Every 30s: auto-enter trades when signal fires, monitor open positions."""
    last_signal_price = None
    last_signal_time  = 0

    while True:
        try:
            check_open_jarvis_trades()

            signal, msg = check_for_signal()
            if signal:
                price = signal["entry"]
                now   = time.time()
                price_moved  = last_signal_price is None or abs(price - last_signal_price) > 20
                time_elapsed = now - last_signal_time > 3600  # min 1hr between new trades

                if price_moved and time_elapsed:
                    log_signal(signal)
                    auto_enter_trade(signal)
                    last_signal_price = price
                    last_signal_time  = now

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
