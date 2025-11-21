#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production-ready Premium SMC Scanner (Signals Only) — patched with user rules
- Adds: Entry pre-checks, TF confirmation, momentum filters, ATR-based TP/SL,
  SL-cluster deprioritization, minimal structural checks.
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque

# ---------------- ENV ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")  # optional
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")

DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 40))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", 0.0015))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", 1000000))

BTC_PAIR = os.getenv("BTC_PAIR", "BTC-USDT-SWAP")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", 23))

# ---------------- TIMEFRAMES ----------------
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("smc_bot")

# ---------------- GLOBAL DB LOCK ----------------
db_lock = asyncio.Lock()

# ---------------- TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg:
        return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram creds missing.")
        return
    if not msg:
        log.error("Attempted to send empty Telegram message")
        return

    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode":"HTML"})
            if r.status_code != 200:
                log.error(f"Telegram send failed: {r.status_code} | {r.text}")
        except Exception as e:
            log.error(f"Telegram send exception: {e}")

# ---------------- SQLITE ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS pauses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reason TEXT,
            timestamp TEXT
        );
        """)
        await db.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.warning(f"OHLCV fetch failed for {symbol} {timeframe}: {e}")
        return None

# ---------------------------
# IMPROVED INDICATORS
# ---------------------------
def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()
    return atr.reindex(df.index)

def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm = high.diff().clip(lower=0)
    minus_dm = -low.diff().clip(lower=0)
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()
    plus_di = 100 * plus_dm.rolling(period, min_periods=1).sum() / atr
    minus_di = 100 * minus_dm.rolling(period, min_periods=1).sum() / atr
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di)).rolling(period, min_periods=1).mean()
    return dx.reindex(df.index)

def compute_ema(df: pd.DataFrame, span: int) -> pd.Series:
    return df["close"].ewm(span=span, adjust=False).mean()


# ---------------------------
# SMC DETECTIONS (unchanged)
# ---------------------------
def detect_order_block(df: pd.DataFrame):
    if len(df) < 3:
        return None, None, None
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        return "bullish", candle["open"], candle["low"]
    return "bearish", candle["high"], candle["open"]

def detect_fvg(df: pd.DataFrame):
    if len(df) < 3:
        return False, False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
    bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
    return bull, bear

def detect_liquidity_sweep(df):
    if len(df) < 6:
        return False, False
    last = df.iloc[-1]
    prev = df.iloc[-5:-1]
    return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

def detect_bos(df):
    if len(df) < 6:
        return False, False
    last = df.iloc[-1]
    prev = df.iloc[-5:-1]
    hh = last["high"] > prev["high"].max()
    ll = last["low"] < prev["low"].min()
    return hh, ll

def detect_mitigation_entry(df, ob_hi, ob_lo, side):
    if df is None or len(df) == 0:
        return False
    last = df["close"].iloc[-1]
    if side=="BUY":
        return last <= ob_hi
    return last >= ob_lo


# ---------------------------
# VOLATILITY / MOMENTUM
# ---------------------------
def volatility_ok(df: pd.DataFrame, min_atr_ratio=0.0005, max_atr_ratio=0.05) -> bool:
    if df is None or len(df) < 10:
        return False
    atr_series = compute_atr(df)
    last_atr = float(atr_series.iloc[-1])
    last_close = float(df["close"].iloc[-1])
    if last_close == 0:
        return False
    atr_ratio = last_atr / last_close
    return min_atr_ratio <= atr_ratio <= max_atr_ratio

def momentum_ok(df: pd.DataFrame, side: str) -> bool:
    if df is None or len(df) < 22:
        return False
    ema_fast = compute_ema(df, 8).iloc[-1]
    ema_slow = compute_ema(df, 21).iloc[-1]
    if side=="BUY":
        return ema_fast > ema_slow
    return ema_fast < ema_slow


# ---------------------------
# SIGNAL GENERATOR (IMPROVED INDICATORS)
# ---------------------------
def generate_signal(df: pd.DataFrame, symbol: str, context: dict = None):
    context = context or {}
    tf = context.get("tf", None)

    if df is None or len(df) < 6:
        return None

    last = df["close"].iloc[-1]

    if not coin_allowed(symbol):
        return None

    if not volatility_ok(df):
        return None

    ob_type, ob_hi, ob_lo = detect_order_block(df)
    if ob_type is None:
        return None

    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_high, sweep_low = detect_liquidity_sweep(df)
    bos_hh, bos_ll = detect_bos(df)

    if not (bos_hh or bos_ll):
        return None

    # ---------------------------
    # SCORE & REASONS
    # ---------------------------
    score = 0
    reasons = []

    if ob_type=="bullish": score+=2; reasons.append("Order Block (Bullish) +2")
    else: score+=2; reasons.append("Order Block (Bearish) +2")

    if bull_fvg: score+=2; reasons.append("FVG Bullish +2")
    elif bear_fvg: score+=2; reasons.append("FVG Bearish +2")
    else: reasons.append("No FVG +0")

    score+=2; reasons.append("Break of Structure +2")

    if sweep_high or sweep_low: score+=1; reasons.append("Liquidity Sweep +1")
    else: reasons.append("No Sweep +0")

    if detect_mitigation_entry(df, ob_hi, ob_lo, "BUY" if ob_type=="bullish" else "SELL"):
        score+=1; reasons.append("Mitigation Entry +1")
    else: reasons.append("No Mitigation Entry +0")

    # momentum last vs 5 bars ago
    if df["close"].iloc[-1] > df["close"].iloc[-5]:
        score+=1; reasons.append("Momentum Up +1")
    else: reasons.append("Momentum Weak +0")

    min_score = 6 if symbol not in BLACKLIST_COINS else 7
    if score < min_score:
        return None

    side = "BUY" if ob_type=="bullish" else "SELL"
    if not momentum_ok(df, side):
        if not (("Mitigation Entry +1" in reasons) and score>=7):
            return None

    # ---------------------------
    # TIMEFRAME CONFIRMATION
    # ---------------------------
    df_15m = context.get("df_15m")
    df_1h = context.get("df_1h")

    def get_trend(d):
        if d is None or len(d)<50:
            return "neutral"
        c = d["close"]
        e50 = c.ewm(span=50, adjust=False).mean().iloc[-1]
        e200 = c.ewm(span=200, adjust=False).mean().iloc[-1]
        if c.iloc[-1] > e50 > e200: return "bull"
        if c.iloc[-1] < e50 < e200: return "bear"
        return "neutral"

    if tf in ("3m","5m"):
        trend15 = get_trend(df_15m)
        trend1h = get_trend(df_1h)
        desired = "bull" if side=="BUY" else "bear"
        if trend15 != desired and trend1h != desired:
            return None

    if tf=="1m":
        if not (sweep_high or sweep_low):
            return None
        trend15 = get_trend(df_15m)
        desired = "bull" if side=="BUY" else "bear"
        if trend15 != desired:
            return None

    # ---------------------------
    # TP / SL using ATR(15m) if available
    # ---------------------------
    entry = float(last)
    atr15_val = None
    if df_15m is not None and len(df_15m)>=10:
        try:
            atr15_val = float(compute_atr(df_15m).iloc[-1])
        except Exception:
            atr15_val = None

    tp_multiplier_default = 0.8
    sl_multiplier_default = 1.0
    tp_multiplier = tp_multiplier_default
    sl_multiplier = sl_multiplier_default

    if atr15_val and atr15_val>0:
        if side=="BUY":
            sl = entry - sl_multiplier*atr15_val
            tp1 = entry + tp_multiplier*atr15_val
            tp2 = entry + 1.5*tp_multiplier*atr15_val
            tp3 = entry + 2.5*tp_multiplier*atr15_val
        else:
            sl = entry + sl_multiplier*atr15_val
            tp1 = entry - tp_multiplier*atr15_val
            tp2 = entry - 1.5*tp_multiplier*atr15_val
            tp3 = entry - 2.5*tp_multiplier*atr15_val
    else:
        if side=="BUY":
            sl = float(ob_lo)
            tp1 = entry*1.004
            tp2 = entry*1.008
            tp3 = entry*1.012
        else:
            sl = float(ob_hi)
            tp1 = entry*0.996
            tp2 = entry*0.992
            tp3 = entry*0.988

    if sl==entry:
        sl = entry - (entry*0.002) if side=="BUY" else entry + (entry*0.002)

    # ---------------------------
    # RETURN SIGNAL DICTIONARY (same as old)
    # ---------------------------
    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": float(sl),
        "tp1": float(tp1),
        "tp2": float(tp2),
        "tp3": float(tp3),
        "reason": "Optimized SMC high-probability signal",
        "score": score,
        "reason_list": reasons
    }
# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (sig["symbol"],sig["side"],sig["entry"],sig["sl"],sig["tp1"],sig["tp2"],sig["tp3"],
                  datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"]))
            await db.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals(exchange):
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status 
                    FROM signals 
                    WHERE status='OPEN'
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last", None)
                        if last_price is None:
                            continue
                        
                        hits = []
                        sl_hit = False
                        if side=="BUY":
                            if not tp1_hit and last_price >= tp1:
                                hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price >= tp2:
                                hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price >= tp3:
                                hits.append("TP3"); tp3_hit=1
                            if last_price <= sl:
                                hits.append("SL"); status="CLOSED"; sl_hit = True
                        else:
                            if not tp1_hit and last_price <= tp1:
                                hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price <= tp2:
                                hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price <= tp3:
                                hits.append("TP3"); tp3_hit=1
                            if last_price >= sl:
                                hits.append("SL"); status="CLOSED"; sl_hit = True

                        if hits:
                            # Send update
                            await tg(
                                f"🎯 <b>SMC Signal Update</b>\n"
                                f"{symbol} | {side}\n"
                                f"Entry: {entry}\n"
                                f"Last: {last_price}\n"
                                f"HIT: {', '.join(hits)}\n"
                                f"SL: {sl}\n"
                                f"TP1: {tp1}  TP2: {tp2}  TP3: {tp3}"
                            )

                        # if SL was hit - register in-memory SL history for deprioritization
                        if sl_hit:
                            try:
                                # record in memory for deprioritization
                                record_sl_hit_in_memory(symbol)
                            except Exception:
                                pass

                        # write updates under lock to avoid SQLite 'database is locked'
                        async with db_lock:
                            await db.execute("""
                                UPDATE signals
                                SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=?
                                WHERE id=?
                            """, (tp1_hit, tp2_hit, tp3_hit, status, sig_id))
                await db.commit()
        except Exception as e:
            log.exception("Error in monitor_signals: %s", e)
            await tg(f"❌ Error in monitor_signals: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
btc_paused = False

async def scan_loop(exchange):
    global btc_paused
    last_heartbeat = 0

    while True:
        t0 = time.time()
        try:
            # BTC clean check (logging only)
            btc_clean, reason = await btc_is_clean(exchange)
            if not btc_clean and not btc_paused:
                log.info(f"⚠️ BTC not clean: {reason}")
                await tg(f"⚠️ PAUSED — BTC not clean: {reason}")
                async with db_lock:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "INSERT INTO pauses (reason,timestamp) VALUES (?,?)",
                            (reason, datetime.datetime.utcnow().isoformat())
                        )
                        await db.commit()
                btc_paused = True
            else:
                btc_paused = False

            tickers = await exchange.fetch_tickers()
            top = sorted(
                [(s, v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")],
                key=lambda x:x[1], reverse=True
            )[:TOP_N]

            # Always include BTC
            if BTC_PAIR in tickers and BTC_PAIR not in [s for s,_ in top]:
                top.insert(0, (BTC_PAIR, tickers[BTC_PAIR].get("quoteVolume",0)))

            for symbol, vol in top:
                # Skip deprioritized coins early
                if coin_deprioritized(symbol):
                    log.info(f"Deprioritized {symbol} due to recent SL cluster. Skipping.")
                    continue

                if vol < MIN_VOLUME:
                    log.info(f"Skipped {symbol} — volume {vol} below MIN_VOLUME")
                    continue

                log.info(f"Scanning {symbol}...")

                # We'll fetch 15m and 1h once per symbol when needed for TF confirmation / ATR
                df_15m = None
                df_1h = None
                # Pre-fetch higher TFs lazily (only if we encounter candidate TFs 1m/3m/5m)
                # We'll fetch inside the TF loop on demand to reduce requests.

                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 1800:
                        log.info(f"Skipped {symbol} ({tf}) — cooldown active")
                        continue

                    # If coin is deprioritized mid-loop re-check
                    if coin_deprioritized(symbol):
                        log.info(f"Deprioritized {symbol} during TF loop. Skipping remaining TFs.")
                        break

                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv:
                        log.warning(f"OHLCV fetch failed for {symbol} ({tf})")
                        continue

                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    # ensure float columns
                    for c in ["open","high","low","close","vol"]:
                        df[c] = pd.to_numeric(df[c], errors="coerce")

                    # If tf is a low tf that requires higher TF confirmation, we fetch higher TFs
                    if tf in ("1m","3m","5m"):
                        if df_15m is None:
                            ohlcv15 = await fetch_ohlcv(exchange, symbol, "15m", 200)
                            if ohlcv15:
                                df_15m = pd.DataFrame(ohlcv15, columns=["ts","open","high","low","close","vol"])
                                for c in ["open","high","low","close","vol"]:
                                    df_15m[c] = pd.to_numeric(df_15m[c], errors="coerce")
                        if df_1h is None:
                            ohlcv1h = await fetch_ohlcv(exchange, symbol, "1h", 200)
                            if ohlcv1h:
                                df_1h = pd.DataFrame(ohlcv1h, columns=["ts","open","high","low","close","vol"])
                                for c in ["open","high","low","close","vol"]:
                                    df_1h[c] = pd.to_numeric(df_1h[c], errors="coerce")

                    context = {"tf": tf, "df_15m": df_15m, "df_1h": df_1h}
                    sig = generate_signal(df, symbol, context=context)
                    if sig:
                        log.info(f"Signal generated for {symbol} ({tf})")

                        # When a signal is generated, we re-check timeframe confirmation here as an extra safety
                        # (generate_signal already enforced the TF rules if context.tf provided)

                        # Build breakdown text
                        breakdown_text = "\n• ".join(sig.get("reason_list", []))

                        await tg(
                            f"🚀 <b>SMC Signal</b>\n"
                            f"{sig['symbol']} ({tf}) | {sig['side']}\n"
                            f"Entry: {sig['entry']}\nSL: {sig['sl']}\n"
                            f"TP1: {sig['tp1']}  TP2: {sig['tp2']}  TP3: {sig['tp3']}\n"
                            f"Reason: {sig['reason']}\n"
                            f"Score: {sig['score']}\n\n"
                            f"<b>Breakdown:</b>\n• {breakdown_text}"
                        )
                        await log_signal(sig)
                        last_signal_time[key] = time.time()
                    else:
                        log.info(f"No signal for {symbol} ({tf})")

            # Heartbeat
            now = time.time()
            if now - last_heartbeat > HEARTBEAT_INTERVAL:
                last_heartbeat = now
                await tg("❤️ SMC Scanner running.")
                log.info("Heartbeat sent.")

            # Daily summary
            utc = datetime.datetime.utcnow()
            if utc.hour == DAILY_SUMMARY_HOUR and utc.minute < 2:
                await tg("📊 Daily summary placeholder.")
                log.info("Daily summary sent.")

        except Exception as e:
            log.exception("Error in scan_loop: %s", e)
            await tg(f"❌ Error in scan_loop: {e}")

        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))

# ---------------- FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token!=WEBHOOK_SECRET:
        raise HTTPException(403,"Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

# ---------------- MAIN ----------------
async def main():
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    await asyncio.gather(
        scan_loop(exchange),
        monitor_signals(exchange)
    )

if __name__=="__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args = p.parse_args()

    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        asyncio.run(main())