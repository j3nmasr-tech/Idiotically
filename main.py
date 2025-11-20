#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production-ready Premium SMC Scanner (Signals Only)
----------------------------------------------------
- BTC clean filter
- Top N OKX USDT symbols
- Full SMC detection (OB, FVG, BOS, Liquidity Sweep, Mitigation Entry)
- Scoring system for high-probability signals
- Symbol+timeframe cooldown to avoid duplicates
- Telegram alerts including TP/SL hits (timeframe shown)
- SQLite logging
- Async, ENV-only configuration
- Heartbeat and daily summary
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn

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

# ---------------- INDICATORS ----------------
def compute_atr(df: pd.DataFrame, period:int=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()
    return atr.reindex(df.index)

def compute_adx(df: pd.DataFrame, period:int=14):
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
    dx = (100*(plus_di - minus_di).abs()/(plus_di + minus_di)).rolling(period, min_periods=1).mean()
    return dx.reindex(df.index)

# ---------------- SMC DETECTION ----------------
def detect_order_block(df: pd.DataFrame):
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        return "bullish", candle["open"], candle["low"]
    return "bearish", candle["high"], candle["open"]

def detect_fvg(df: pd.DataFrame):
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
    bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
    return bull, bear

def detect_liquidity_sweep(df):
    last = df.iloc[-1]
    prev = df.iloc[-5:-1]
    return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

def detect_bos(df):
    last = df.iloc[-1]
    prev = df.iloc[-5:-1]
    hh = last["high"] > prev["high"].max()
    ll = last["low"] < prev["low"].min()
    return hh, ll

def detect_mitigation_entry(df, ob_hi, ob_lo, side):
    last = df["close"].iloc[-1]
    if side=="BUY":
        return last <= ob_hi
    return last >= ob_lo

# ---------------- BTC CLEAN FILTER ----------------
async def btc_is_clean(exchange) -> (bool, str):
    ohlcv1h = await fetch_ohlcv(exchange, BTC_PAIR, "1h", 200)
    ohlcv15 = await fetch_ohlcv(exchange, BTC_PAIR, "15m", 200)
    if not ohlcv1h or not ohlcv15:
        return False, "BTC OHLCV fetch failed"

    df1h = pd.DataFrame(ohlcv1h, columns=["ts","open","high","low","close","vol"])
    df15 = pd.DataFrame(ohlcv15, columns=["ts","open","high","low","close","vol"])
    df1h["atr"] = compute_atr(df1h)
    df1h["adx"] = compute_adx(df1h)

    adx_ok = df1h["adx"].iloc[-1] > 20
    vol_rising = df1h["atr"].iloc[-1] > df1h["atr"].iloc[-5]

    hh1, ll1 = detect_bos(df1h)
    d1h = "up" if hh1 else "down" if ll1 else "neutral"
    hh15, ll15 = detect_bos(df15)
    d15 = "up" if hh15 else "down" if ll15 else "neutral"
    structure_ok = d1h == d15 and d1h != "neutral"

    # News check optional
    news_ok = True
    if NEWS_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(f"https://cryptopanic.com/api/v1/posts/?auth_token={NEWS_API_KEY}")
                for item in r.json().get("results", []):
                    if item.get("votes", {}).get("important",0) > 2:
                        news_ok = False
        except:
            news_ok = False

    is_clean = adx_ok and vol_rising and structure_ok and news_ok
    reason = ""
    if not adx_ok: reason += "ADX<20; "
    if not vol_rising: reason += "ATR not rising; "
    if not structure_ok: reason += "Structure mismatch; "
    if not news_ok: reason += "News upcoming; "
    return is_clean, reason.strip()

# ================================================
# OPTIMIZED PREMIUM SMC SIGNAL GENERATOR
# ================================================

def generate_signal(df: pd.DataFrame, symbol: str):

    # -------------------------------
    # 0. REJECT UNSTABLE / WICKY COINS
    # -------------------------------
    #if not coin_allowed(symbol):
        #return None

    last = df["close"].iloc[-1]

    # -------------------------------
    # 1. VOLATILITY FILTER (ATR trend)
    # -------------------------------
    if not volatility_ok(df):
        return None

    # -------------------------------
    # 2. SMC DETECTIONS
    # -------------------------------
    ob_type, ob_hi, ob_lo = detect_order_block(df)
    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_high, sweep_low = detect_liquidity_sweep(df)
    bos_hh, bos_ll = detect_bos(df)

    # -------------------------------
    # 3. STRUCTURE SANITY CHECK
    # -------------------------------
    struct = structure_ok(bos_hh, bos_ll, sweep_high, sweep_low)
    if struct is False:
        # Missing BOS = SL magnet (based on dataset)
        return None

    # -------------------------------
    # 4. RAW SCORE SYSTEM
    # -------------------------------
    score = 0
    reasons = []

    # --- Order Block ---
    if ob_type == "bullish":
        score += 2; reasons.append("Order Block (Bullish) +2")
    else:
        score += 2; reasons.append("Order Block (Bearish) +2")

    # --- FVG ---
    if bull_fvg:
        score += 2; reasons.append("FVG Bullish +2")
    elif bear_fvg:
        score += 2; reasons.append("FVG Bearish +2")
    else:
        reasons.append("No FVG +0")

    # --- BOS ---
    if bos_hh or bos_ll:
        score += 2; reasons.append("Break of Structure +2")
    else:
        reasons.append("No BOS +0")

    # --- Sweep ---
    if sweep_high or sweep_low:
        score += 1; reasons.append("Liquidity Sweep +1")
    else:
        reasons.append("No Sweep +0")

    # --- Mitigation ---
    if detect_mitigation_entry(df, ob_hi, ob_lo, "BUY" if ob_type=="bullish" else "SELL"):
        score += 1; reasons.append("Mitigation Entry +1")
    else:
        reasons.append("No Mitigation Entry +0")

    # --- Momentum ---
    # Momentum: last > past X bars
    if df["close"].iloc[-1] > df["close"].iloc[-5]:
        score += 1; reasons.append("Momentum Up +1")
    else:
        reasons.append("Momentum Weak +0")

    # -------------------------------
    # 5. FINAL SCORE THRESHOLD
    # -------------------------------
    min_score = 6 if symbol not in BLACKLIST_COINS else 7
    if score < min_score:
        return None

    # -------------------------------
    # 6. MOMENTUM ALIGNMENT FILTER
    # -------------------------------
    side = "BUY" if ob_type == "bullish" else "SELL"
    if not momentum_ok(df, side):
        return None

    # -------------------------------
    # 7. GENERATE TP/SL LEVELS
    # -------------------------------
    entry = float(last)
    if side == "BUY":
        sl = float(ob_lo)
        tp1 = entry * 1.004
        tp2 = entry * 1.008
        tp3 = entry * 1.012
    else:
        sl = float(ob_hi)
        tp1 = entry * 0.996
        tp2 = entry * 0.992
        tp3 = entry * 0.988

    # -------------------------------
    # 8. FINAL SIGNAL OBJECT
    # -------------------------------
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
                        if side=="BUY":
                            if not tp1_hit and last_price >= tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price >= tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price >= tp3: hits.append("TP3"); tp3_hit=1
                            if last_price <= sl: hits.append("SL"); status="CLOSED"
                        else:
                            if not tp1_hit and last_price <= tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price <= tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price <= tp3: hits.append("TP3"); tp3_hit=1
                            if last_price >= sl: hits.append("SL"); status="CLOSED"

                        if hits:
                            await tg(
                                f"🎯 <b>SMC Signal Update</b>\n"
                                f"{symbol} | {side}\n"
                                f"Entry: {entry}\n"
                                f"Last: {last_price}\n"
                                f"HIT: {', '.join(hits)}\n"
                                f"SL: {sl}\n"
                                f"TP1: {tp1}  TP2: {tp2}  TP3: {tp3}"
                            )
                        
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
                if vol < MIN_VOLUME:
                    log.info(f"Skipped {symbol} — volume {vol} below MIN_VOLUME")
                    continue

                log.info(f"Scanning {symbol}...")

                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 1800:
                        log.info(f"Skipped {symbol} ({tf}) — cooldown active")
                        continue

                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv:
                        log.warning(f"OHLCV fetch failed for {symbol} ({tf})")
                        continue

                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    sig = generate_signal(df, symbol)
                    if sig:
                        log.info(f"Signal generated for {symbol} ({tf})")

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