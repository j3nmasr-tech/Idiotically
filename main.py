#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Premium SMC Scanner (Signals Only) – OKX Top 100
----------------------------------------------------
✓ NO trading
✓ NO OKX API keys needed
✓ Public-market data only
✓ Telegram-only signal notifications
✓ Northflank-ready (environment variables only)

Required ENV variables:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
"""

import os
import time
import asyncio
import logging
import datetime
from typing import Dict, Any, Optional

import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np

from fastapi import FastAPI, Request, HTTPException
import uvicorn


###########################################################
# ---------------------- ENV CONFIG -----------------------
###########################################################

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

NEWS_API_KEY = os.getenv("NEWS_API_KEY")  # optional
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")

DB_PATH = "signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 100))

MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.0015"))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", "1000000"))

BTC_PAIR = os.getenv("BTC_PAIR", "BTC-USDT-SWAP")

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", 23))


###########################################################
# ---------------------- LOGGING --------------------------
###########################################################

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("smc_bot")


###########################################################
# ------------------- TELEGRAM UTILS ----------------------
###########################################################

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram creds missing — cannot send message.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        await client.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML"
        })


###########################################################
# --------------------- SQLITE INIT -----------------------
###########################################################

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
            reason TEXT
        );
        """)
        await db.commit()


###########################################################
# -------------------- FETCH OHLCV ------------------------
###########################################################

async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except:
        return None


###########################################################
# ----------------------- INDICATORS ----------------------
###########################################################

def compute_atr(df: pd.DataFrame, length=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr = np.maximum(high[1:], close[:-1]) - np.minimum(low[1:], close[:-1])
    atr = pd.Series(tr).rolling(length).mean()
    return np.concatenate([[np.nan], atr])


def compute_adx(df: pd.DataFrame, length=14):
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values

    plus_dm = np.maximum(high[1:] - high[:-1], 0)
    minus_dm = np.maximum(low[:-1] - low[1:], 0)

    tr = np.maximum.reduce([
        high[1:] - low[1:],
        np.abs(high[1:] - close[:-1]),
        np.abs(low[1:] - close[:-1])
    ])

    atr = pd.Series(tr).rolling(length).mean()

    plus_di = 100 * pd.Series(plus_dm).rolling(length).sum() / atr
    minus_di = 100 * pd.Series(minus_dm).rolling(length).sum() / atr
    dx = (100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)).rolling(length).mean()

    return np.concatenate([[np.nan] * 2, dx])


###########################################################
# ---------------------- SMC CORE --------------------------
###########################################################

def detect_order_block(df: pd.DataFrame):
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        return "bullish", candle["open"], candle["low"]
    else:
        return "bearish", candle["high"], candle["open"]


def detect_fvg(df: pd.DataFrame):
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]

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


###########################################################
# ------------------- BTC CLEAN FILTER --------------------
###########################################################

async def btc_is_clean(exchange) -> bool:
    ohlcv1h = await fetch_ohlcv(exchange, BTC_PAIR, "1h", 200)
    ohlcv15 = await fetch_ohlcv(exchange, BTC_PAIR, "15m", 200)

    if not ohlcv1h or not ohlcv15:
        return False

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

    # News feed optional
    if NEWS_API_KEY:
        async with httpx.AsyncClient() as client:
            try:
                r = await client.get(f"https://cryptopanic.com/api/v1/posts/?auth_token={NEWS_API_KEY}")
                items = r.json().get("results", [])
                for item in items:
                    if item.get("votes", {}).get("important", 0) > 2:
                        return False
            except:
                pass

    return adx_ok and vol_rising and structure_ok


###########################################################
# ------------------- SIGNAL CREATION ---------------------
###########################################################

def generate_signal(df: pd.DataFrame, symbol: str):
    ob_type, ob_hi, ob_lo = detect_order_block(df)
    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_high, sweep_low = detect_liquidity_sweep(df)
    bos_hh, bos_ll = detect_bos(df)

    last = df["close"].iloc[-1]

    long = (
        ob_type == "bullish" and bull_fvg and sweep_low and bos_hh and last > ob_hi
    )

    short = (
        ob_type == "bearish" and bear_fvg and sweep_high and bos_ll and last < ob_lo
    )

    if long:
        return {
            "symbol": symbol,
            "side": "BUY",
            "entry": float(last),
            "sl": float(ob_lo),
            "tp1": float(last * 1.004),
            "tp2": float(last * 1.008),
            "tp3": float(last * 1.012),
            "reason": "Premium SMC long"
        }

    if short:
        return {
            "symbol": symbol,
            "side": "SELL",
            "entry": float(last),
            "sl": float(ob_hi),
            "tp1": float(last * 0.996),
            "tp2": float(last * 0.992),
            "tp3": float(last * 0.988),
            "reason": "Premium SMC short"
        }

    return None


###########################################################
# ---------------------- LOG SIGNAL -----------------------
###########################################################

async def log_signal(sig):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], sig["sl"],
            sig["tp1"], sig["tp2"], sig["tp3"],
            datetime.datetime.utcnow().isoformat(),
            "OPEN", sig["reason"]
        ))
        await db.commit()


###########################################################
# ------------------------ MAIN LOOP ----------------------
###########################################################

async def scan_loop():
    exchange = ccxt.okx({"enableRateLimit": True})
    await init_db()

    last_heartbeat = 0

    while True:
        t0 = time.time()

        try:
            # BTC cleanliness filter
            if not await btc_is_clean(exchange):
                await tg("⚠️ PAUSED — BTC not clean for SMC conditions")
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            markets = await exchange.load_markets()
            tickers = await exchange.fetch_tickers()

            # top 100 by quote volume
            top = sorted(
                [(s, v.get("quoteVolume", 0)) for s, v in tickers.items() if s.endswith("USDT")],
                key=lambda x: x[1],
                reverse=True
            )[:TOP_N]

            for symbol, vol in top:
                if vol < MIN_VOLUME:
                    continue

                ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 200)
                if not ohlcv:
                    continue

                df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                sig = generate_signal(df, symbol)

                if sig:
                    await tg(
                        f"🚀 <b>SMC Signal</b>\n"
                        f"{sig['symbol']} | {sig['side']}\n"
                        f"Entry: {sig['entry']}\n"
                        f"SL: {sig['sl']}\n"
                        f"TP1: {sig['tp1']}  TP2: {sig['tp2']}  TP3: {sig['tp3']}\n"
                        f"{sig['reason']}"
                    )
                    await log_signal(sig)

            # Heartbeat
            now = time.time()
            if now - last_heartbeat > HEARTBEAT_INTERVAL:
                last_heartbeat = now
                await tg("❤️ SMC Scanner running smoothly.")

            # Daily summary placeholder
            utc = datetime.datetime.utcnow()
            if utc.hour == DAILY_SUMMARY_HOUR and utc.minute < 2:
                await tg("📊 Daily summary: Placeholder (expand if needed).")

        except Exception as e:
            log.exception("Error: %s", e)
            await tg(f"❌ Error: {e}")

        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))


###########################################################
# ---------------------- FASTAPI API ----------------------
###########################################################

app = FastAPI()

@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(403, "Invalid secret")

    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok": True}


###########################################################
# --------------------------- RUN -------------------------
###########################################################

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args = p.parse_args()

    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        asyncio.run(scan_loop())