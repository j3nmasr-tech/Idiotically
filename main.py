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
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
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
    url = BINGX_BASE + path
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error {e.response.status_code} for {url}: {e}")
            return {}
        except Exception as e:
            logging.error(f"Request failed for {url}: {e}")
            return {}

async def get_top_symbols() -> List[str]:
    """Get top volume symbols from BingX"""
    data = await fetch_json("/openApi/spot/v1/ticker/24hr")
    
    if not data or 'data' not in data:
        logging.error("Failed to fetch symbols data or invalid response structure")
        return []
    
    symbols_data = data['data']
    if not symbols_data:
        logging.warning("No symbols data returned from API")
        return []
    
    try:
        # Filter for USDT pairs and process volume
        usdt_pairs = []
        for symbol_data in symbols_data:
            symbol = symbol_data.get('symbol', '')
            if symbol.endswith('-USDT') or 'USDT' in symbol:
                # Handle different possible volume field names
                volume_str = symbol_data.get('quoteVolume') or symbol_data.get('volume') or '0'
                try:
                    volume = float(volume_str)
                    usdt_pairs.append((symbol, volume))
                except (ValueError, TypeError):
                    continue
        
        # Sort by volume and get top N
        usdt_pairs.sort(key=lambda x: x[1], reverse=True)
        top_symbols = [pair[0] for pair in usdt_pairs[:TOP_N]]
        
        logging.info(f"Found {len(top_symbols)} top symbols")
        return top_symbols
        
    except Exception as e:
        logging.error(f"Error processing symbols: {e}")
        return []

async def get_ohlcv(symbol: str, timeframe: str) -> pd.DataFrame:
    """Get OHLCV data from BingX"""
    params = {
        "symbol": symbol,
        "interval": timeframe,
        "limit": CANDLE_LIMIT
    }
    
    data = await fetch_json("/openApi/spot/v1/market/klines", params)
    
    if not data or 'data' not in data or not data['data']:
        logging.warning(f"No OHLCV data for {symbol} {timeframe}")
        return pd.DataFrame()
    
    try:
        candles = data['data']
        # BingX returns: [timestamp, open, high, low, close, volume, ...]
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        
        # Convert to numeric types
        numeric_columns = ["open", "high", "low", "close", "volume"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert timestamp to datetime if needed
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df = df.dropna()
        
        return df
        
    except Exception as e:
        logging.error(f"Error processing OHLCV for {symbol} {timeframe}: {e}")
        return pd.DataFrame()

# -------------------- RomeOPT Logic --------------------
def detect_liquidity_sweep(df: pd.DataFrame) -> bool:
    if len(df) < 1:
        return False
    last = df.iloc[-1]
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]
    total_range = last["high"] - last["low"]
    
    if total_range == 0:
        return False
        
    return wick_up > total_range * 0.3 or wick_down > total_range * 0.3

def detect_displacement(df: pd.DataFrame) -> bool:
    if len(df) < 14:
        return False
        
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    
    # Calculate ATR-like value from recent range
    recent_highs = df["high"].iloc[-14:]
    recent_lows = df["low"].iloc[-14:]
    ranges = recent_highs.values - recent_lows.values
    atr = np.mean(ranges) if len(ranges) > 0 else 0
    
    if atr == 0:
        return False
        
    return body > atr * 0.6

def detect_zone_approach(df: pd.DataFrame) -> bool:
    if len(df) < 6:
        return False
        
    current_close = df.iloc[-1]["close"]
    reference_open = df.iloc[-5]["open"]
    reference_close = df.iloc[-5]["close"]
    
    return current_close < reference_open or current_close > reference_close

def detect_pd_alignment(df: pd.DataFrame) -> bool:
    if len(df) < 1:
        return False
        
    mid = (df["high"].max() + df["low"].min()) / 2
    current_close = df.iloc[-1]["close"]
    
    return abs(current_close - mid) < (mid * 0.03)

def detect_htf_relaxed() -> bool:
    # Placeholder - you can implement higher timeframe analysis here
    return True

def detect_early_momentum(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False
        
    price_change = abs(df.iloc[-1]["close"] - df.iloc[-2]["close"])
    return price_change > df.iloc[-1]["close"] * 0.0015

# -------------------- Determine BUY/SELL --------------------
def determine_side(df: pd.DataFrame) -> str:
    if len(df) < 1:
        return "NEUTRAL"
        
    last = df.iloc[-1]
    wick_up = last["high"] - max(last["open"], last["close"])
    wick_down = min(last["open"], last["close"]) - last["low"]

    sweep_dir = "BUY" if wick_down > wick_up else "SELL"
    
    # Calculate mid price of recent range
    lookback = min(14, len(df))
    recent_high = df["high"].iloc[-lookback:].max()
    recent_low = df["low"].iloc[-lookback:].min()
    mid = (recent_high + recent_low) / 2
    
    zone_dir = "BUY" if last["close"] < mid else "SELL"

    if sweep_dir == zone_dir:
        return sweep_dir
    else:
        return "BUY" if last["close"] > last["open"] else "SELL"

# -------------------- Build Signal --------------------
def build_signal(symbol: str, df: pd.DataFrame, timeframe: str) -> Optional[Signal]:
    if df.empty or len(df) < 14:
        return None

    steps = [
        detect_liquidity_sweep(df),
        detect_displacement(df),
        detect_zone_approach(df),
        detect_pd_alignment(df),
        detect_htf_relaxed(),
        detect_early_momentum(df),
    ]
    
    # Require first 3 steps to be true
    if not all(steps[:3]):
        return None

    side = determine_side(df)
    if side == "NEUTRAL":
        return None
        
    price = df.iloc[-1]["close"]

    # Calculate entry zone and targets
    if side == "BUY":
        entry_low = price * 0.999
        entry_high = price * 1.001
        sl = price * 0.996
        tp1 = price * 1.003
        tp2 = price * 1.006
        tp3 = price * 1.010
    else:  # SELL
        entry_low = price * 0.999
        entry_high = price * 1.001
        sl = price * 1.004
        tp1 = price * 0.997
        tp2 = price * 0.994
        tp3 = price * 0.990

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
            if not symbols:
                logging.warning("No symbols to scan, retrying...")
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            logging.info(f"Scanning {len(symbols)} symbols...")
            tasks = []
            
            for symbol in symbols:
                for tf in TIMEFRAMES:
                    # Process each symbol/timeframe sequentially to avoid rate limits
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
                            f"Entry: ({sig.entry_zone[0]:.6f}, {sig.entry_zone[1]:.6f})\n"
                            f"SL: {sig.stop_loss:.6f}\n"
                            f"TP1: {sig.tp1:.6f}\nTP2: {sig.tp2:.6f}\nTP3: {sig.tp3:.6f}\n"
                            f"Rome Score: {sig.rome_score}\n"
                            f"Steps: {sum(sig.sequence_verified)}/6 verified"
                        )
                        await telegram_send(msg)
                        logging.info(f"Signal detected: {sig.symbol} {sig.side} {sig.timeframe}")

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            logging.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL)

# -------------------- Run --------------------
if __name__ == "__main__":
    # Import numpy for ATR calculation
    import numpy as np
    
    try:
        asyncio.run(scanner())
    except KeyboardInterrupt:
        logging.info("Scanner stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")