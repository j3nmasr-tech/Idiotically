#!/usr/bin/env python3
"""
ROMEOPT ULTRA-SCANNER (Multi-Timeframe)
----------------------------------------
Pure early RomeOPT 6-step logic for 1m,3m,5m,15m
Top 20 BingX USDT pairs with Telegram alerts
Tracks signals in SQLite until TP/SL
"""

import os
import asyncio
import time
import json
from datetime import datetime
from typing import List, Tuple, Optional
import logging
import httpx
import aiosqlite
import pandas as pd
from dataclasses import dataclass, field

# -------------------- ENV --------------------
BINGX_API_KEY = os.getenv("BINGX_API_KEY")
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not BINGX_API_KEY or not BINGX_SECRET_KEY:
    raise ValueError("Missing BingX API key/secret.")
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("Missing Telegram token/chat id.")

# -------------------- CONFIG --------------------
BINGX_BASE = "https://open-api.bingx.com"
SCAN_INTERVAL = 5
TOP_N = 20
DB_PATH = "./romeopt_signals.db"
TIMEFRAMES = ["1m", "3m", "5m", "15m"]
CANDLE_LIMIT = 150

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# -------------------- Telegram --------------------
async def telegram_send(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
        except Exception as e:
            logging.error(f"Telegram send failed: {e}")

# -------------------- Signal Dataclass --------------------
@dataclass
class Signal:
    symbol: str
    side: str
    timeframe: str
    entry_zone: Tuple[float, float]
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    trigger_step: int
    rome_score: int
    sequence_verified: List[bool]
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: str = "LIVE"

# -------------------- Database --------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            timeframe TEXT,
            entry_low REAL,
            entry_high REAL,
            stoploss REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            trigger_step INTEGER,
            rome_score INTEGER,
            sequence_steps TEXT,
            created_at TEXT,
            status TEXT
        )
        """)
        await db.commit()

async def save_signal(sig: Signal):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO signals(symbol, side, timeframe, entry_low, entry_high, stoploss, tp1, tp2, tp3, trigger_step, rome_score, sequence_steps, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (sig.symbol, sig.side, sig.timeframe, sig.entry_zone[0], sig.entry_zone[1],
              sig.stop_loss, sig.tp1, sig.tp2, sig.tp3, sig.trigger_step,
              sig.rome_score, json.dumps(sig.sequence_verified), sig.created_at.isoformat(), sig.status))
        await db.commit()

# -------------------- BingX Market Data --------------------
async def fetch_json(path: str, params: dict = None):
    async with httpx.AsyncClient() as client:
        r = await client.get(BINGX_BASE + path, params=params, timeout=10)
        return r.json()

async def get_top_symbols() -> List[str]:
    r = await fetch_json("/openApi/spot/v1/ticker/24hr")
    data = r.get("data", [])
    df = pd.DataFrame(data)
    df = df[df["symbol"].str.contains("USDT")]
    df["vol"] = pd.to_numeric(df.get("quoteVolume", df.get("volume", 0)), errors="coerce")
    df = df.sort_values("vol", ascending=False).head(TOP_N)
    return df["symbol"].tolist()

async def get_ohlcv(symbol: str, timeframe: str) -> pd.DataFrame:
    r = await fetch_json("/openApi/spot/v1/market/klines", {
        "symbol": symbol,
        "interval": timeframe,
        "limit": CANDLE_LIMIT
    })
    data = r.get("data", [])
    if not data: return pd.DataFrame()
    df = pd.DataFrame(data, columns=["time","open","high","low","close","volume"])
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    return df

# -------------------- RomeOPT Logic --------------------
def detect_liquidity_sweep(df: pd.DataFrame) -> bool:
    last = df.iloc[-1]
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    return wick_up > (last["high"] - last["low"]) * 0.3 or wick_down > (last["high"] - last["low"]) * 0.3

def detect_displacement(df: pd.DataFrame) -> bool:
    body = abs(df.iloc[-1]["close"] - df.iloc[-1]["open"])
    rng = df["high"].iloc[-14:] - df["low"].iloc[-14:]
    atr = rng.mean()
    return body > atr * 0.6

def detect_zone_approach(df: pd.DataFrame) -> bool:
    return df.iloc[-1]["close"] < df["open"].iloc[-5] or df.iloc[-1]["close"] > df["close"].iloc[-5]

def detect_pd_alignment(df: pd.DataFrame) -> bool:
    mid = (df["high"].max() + df["low"].min()) / 2
    return abs(df.iloc[-1]["close"] - mid) < (mid * 0.03)

def detect_htf_relaxed() -> bool:
    return True

def detect_early_momentum(df: pd.DataFrame) -> bool:
    return abs(df.iloc[-1]["close"] - df.iloc[-2]["close"]) > df.iloc[-1]["close"] * 0.0015

# -------------------- Determine BUY/SELL --------------------
def determine_side(df: pd.DataFrame) -> str:
    last = df.iloc[-1]
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]

    sweep_dir = "BUY" if wick_down > wick_up else "SELL"
    mid = (df["high"].max() + df["low"].min()) / 2
    zone_dir = "BUY" if last["close"] < mid else "SELL"

    if sweep_dir == zone_dir:
        return sweep_dir
    else:
        return "BUY" if last["close"] > last["open"] else "SELL"

# -------------------- Build Signal --------------------
def build_signal(symbol: str, df: pd.DataFrame, timeframe: str) -> Optional[Signal]:
    steps = [
        detect_liquidity_sweep(df),
        detect_displacement(df),
        detect_zone_approach(df),
        detect_pd_alignment(df),
        detect_htf_relaxed(),
        detect_early_momentum(df),
    ]
    if not all(steps[:3]):
        return None

    side = determine_side(df)
    price = df.iloc[-1]["close"]

    entry_low = price * 0.999
    entry_high = price * 1.001
    sl = price * (0.996 if side == "BUY" else 1.004)
    tp1 = price * (1.003 if side == "BUY" else 0.997)
    tp2 = price * (1.006 if side == "BUY" else 0.994)
    tp3 = price * (1.010 if side == "BUY" else 0.990)

    return Signal(
        symbol=symbol,
        side=side,
        timeframe=timeframe,
        entry_zone=(entry_low, entry_high),
        stop_loss=sl,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        trigger_step=sum(steps),
        rome_score=sum(steps) + 5,
        sequence_verified=steps
    )

# -------------------- Main Scanner --------------------
async def scanner():
    await init_db()
    await telegram_send("🚀 ROMEOPT ULTRA-SCANNER STARTED\nTracking top 20 BingX USDT pairs on 1m,3m,5m,15m")

    while True:
        try:
            symbols = await get_top_symbols()
            logging.info(f"Scanning {len(symbols)} symbols...")

            for symbol in symbols:
                for tf in TIMEFRAMES:
                    df = await get_ohlcv(symbol, tf)
                    if df.empty:
                        continue
                    sig = build_signal(symbol, df, tf)
                    if sig:
                        await save_signal(sig)
                        msg = (
                            f"🏛 EARLY ROMEOPT SIGNAL\n\n"
                            f"Symbol: {sig.symbol}\n"
                            f"Side: {sig.side}\n"
                            f"TF: {sig.timeframe}\n"
                            f"Entry: {sig.entry_zone}\n"
                            f"SL: {sig.stop_loss}\n"
                            f"TP1: {sig.tp1}\nTP2: {sig.tp2}\nTP3: {sig.tp3}\n"
                            f"Rome Score: {sig.rome_score}\n"
                            f"Steps: {sig.sequence_verified}"
                        )
                        await telegram_send(msg)

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            logging.error(f"Scanner error: {e}")
            await asyncio.sleep(3)

# -------------------- Run --------------------
if __name__ == "__main__":
    asyncio.run(scanner())