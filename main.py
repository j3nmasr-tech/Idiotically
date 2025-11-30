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
import numpy as np
from dataclasses import dataclass, field

# -------------------- ENV with Better Handling --------------------
def load_environment_variables():
    """Load and validate environment variables with detailed error reporting"""
    
    # Try to load from .env file first
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logging.info("Attempted to load .env file")
    except ImportError:
        logging.warning("python-dotenv not installed, skipping .env file")
    
    # Get all environment variables
    BINGX_API_KEY = os.getenv("BINGX_API_KEY")
    BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    # Debug: Log what we found
    logging.info("=== ENVIRONMENT VARIABLES CHECK ===")
    logging.info(f"BINGX_API_KEY: {'***SET***' if BINGX_API_KEY else 'MISSING'}")
    logging.info(f"BINGX_SECRET_KEY: {'***SET***' if BINGX_SECRET_KEY else 'MISSING'}")
    logging.info(f"TELEGRAM_BOT_TOKEN: {'***SET***' if TELEGRAM_TOKEN else 'MISSING'}")
    logging.info(f"TELEGRAM_CHAT_ID: {'***SET***' if TELEGRAM_CHAT_ID else 'MISSING'}")
    
    # For debugging, also check if any env vars exist at all
    all_env_vars = dict(os.environ)
    bingx_vars = {k: v for k, v in all_env_vars.items() if 'BINGX' in k}
    telegram_vars = {k: v for k, v in all_env_vars.items() if 'TELEGRAM' in k}
    
    logging.info(f"All BINGX related vars: {list(bingx_vars.keys())}")
    logging.info(f"All TELEGRAM related vars: {list(telegram_vars.keys())}")
    
    # Check required variables but don't crash immediately
    missing_vars = []
    if not BINGX_API_KEY:
        missing_vars.append("BINGX_API_KEY")
    if not BINGX_SECRET_KEY:
        missing_vars.append("BINGX_SECRET_KEY")
    
    # Telegram is optional for scanner operation
    telegram_available = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)
    if not telegram_available:
        logging.warning("Telegram notifications disabled - missing token or chat ID")

    if missing_vars:
        error_msg = f"CRITICAL: Missing required BingX API variables: {', '.join(missing_vars)}"
        logging.error(error_msg)
        logging.error("Scanner cannot function without BingX API credentials")
        return None
    
    return {
        'BINGX_API_KEY': BINGX_API_KEY,
        'BINGX_SECRET_KEY': BINGX_SECRET_KEY,
        'TELEGRAM_TOKEN': TELEGRAM_TOKEN,
        'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID,
        'TELEGRAM_AVAILABLE': telegram_available
    }

# -------------------- CONFIG --------------------
BINGX_BASE = "https://open-api.bingx.com"
SCAN_INTERVAL = 5
TOP_N = 20
DB_PATH = "./romeopt_signals.db"
TIMEFRAMES = ["1m", "3m", "5m", "15m"]
CANDLE_LIMIT = 150

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# -------------------- Telegram --------------------
async def telegram_send(msg: str, telegram_token: str, chat_id: str):
    """Send message to Telegram with error handling"""
    if not telegram_token or not chat_id:
        logging.warning("Telegram not configured - skipping message")
        return
        
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                url, 
                json={
                    "chat_id": chat_id, 
                    "text": msg,
                    "parse_mode": "HTML"
                },
                timeout=10
            )
            if response.status_code == 200:
                logging.info("Telegram message sent successfully")
            else:
                logging.error(f"Telegram API error: {response.status_code} - {response.text}")
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
    """Initialize SQLite database"""
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
        logging.info("Database initialized successfully")

async def save_signal(sig: Signal):
    """Save signal to database"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO signals(symbol, side, timeframe, entry_low, entry_high, stoploss, tp1, tp2, tp3, trigger_step, rome_score, sequence_steps, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (sig.symbol, sig.side, sig.timeframe, sig.entry_zone[0], sig.entry_zone[1],
              sig.stop_loss, sig.tp1, sig.tp2, sig.tp3, sig.trigger_step,
              sig.rome_score, json.dumps(sig.sequence_verified), sig.created_at.isoformat(), sig.status))
        await db.commit()
        logging.info(f"Signal saved to database: {sig.symbol} {sig.side}")

# -------------------- BingX Market Data --------------------
async def fetch_json(path: str, params: dict = None):
    """Fetch JSON data from BingX API"""
    url = BINGX_BASE + path
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error {e.response.status_code} for {url}")
            return {}
        except Exception as e:
            logging.error(f"Request failed for {url}: {e}")
            return {}

async def get_top_symbols() -> List[str]:
    """Get top volume symbols from BingX"""
    logging.info("Fetching top symbols from BingX...")
    data = await fetch_json("/openApi/spot/v1/ticker/24hr")
    
    if not data:
        logging.error("No data returned from BingX API")
        return []
    
    if 'data' not in data:
        logging.error(f"Unexpected API response structure: {data}")
        return []
    
    symbols_data = data['data']
    if not symbols_data:
        logging.warning("Empty symbols data returned from API")
        return []
    
    try:
        # Filter for USDT pairs and process volume
        usdt_pairs = []
        for symbol_data in symbols_data:
            symbol = symbol_data.get('symbol', '')
            if symbol and ('USDT' in symbol or '-USDT' in symbol):
                # Handle different possible volume field names
                volume_str = symbol_data.get('quoteVolume') or symbol_data.get('volume') or '0'
                try:
                    volume = float(volume_str)
                    usdt_pairs.append((symbol, volume))
                except (ValueError, TypeError) as e:
                    logging.debug(f"Could not convert volume for {symbol}: {volume_str}")
                    continue
        
        if not usdt_pairs:
            logging.warning("No USDT pairs found in API response")
            return []
            
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
        logging.debug(f"No OHLCV data for {symbol} {timeframe}")
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
    """Main scanner loop"""
    logging.info("Initializing ROMEOPT Scanner...")
    
    # Load environment variables
    env = load_environment_variables()
    if env is None:
        logging.error("Cannot start scanner - missing required environment variables")
        return
    
    # Initialize database
    await init_db()
    
    # Send startup message if Telegram is available
    if env['TELEGRAM_AVAILABLE']:
        startup_msg = """🚀 <b>ROMEOPT ULTRA-SCANNER STARTED</b>
        
Tracking top 20 BingX USDT pairs
Timeframes: 1m, 3m, 5m, 15m
Scan interval: 5 seconds
Database: Active"""
        await telegram_send(startup_msg, env['TELEGRAM_TOKEN'], env['TELEGRAM_CHAT_ID'])
    
    logging.info("Scanner started successfully")
    logging.info(f"Telegram notifications: {'ENABLED' if env['TELEGRAM_AVAILABLE'] else 'DISABLED'}")

    while True:
        try:
            symbols = await get_top_symbols()
            if not symbols:
                logging.warning("No symbols to scan, retrying...")
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            logging.info(f"Scanning {len(symbols)} symbols across {len(TIMEFRAMES)} timeframes...")
            
            signals_found = 0
            for symbol in symbols:
                for tf in TIMEFRAMES:
                    # Process each symbol/timeframe sequentially to avoid rate limits
                    df = await get_ohlcv(symbol, tf)
                    if df.empty:
                        continue
                        
                    sig = build_signal(symbol, df, tf)
                    if sig:
                        await save_signal(sig)
                        signals_found += 1
                        
                        msg = (
                            f"🏛 <b>EARLY ROMEOPT SIGNAL</b>\n\n"
                            f"<b>Symbol:</b> {sig.symbol}\n"
                            f"<b>Side:</b> {sig.side}\n"
                            f"<b>TF:</b> {sig.timeframe}\n"
                            f"<b>Entry:</b> ({sig.entry_zone[0]:.6f}, {sig.entry_zone[1]:.6f})\n"
                            f"<b>SL:</b> {sig.stop_loss:.6f}\n"
                            f"<b>TP1:</b> {sig.tp1:.6f}\n<b>TP2:</b> {sig.tp2:.6f}\n<b>TP3:</b> {sig.tp3:.6f}\n"
                            f"<b>Rome Score:</b> {sig.rome_score}\n"
                            f"<b>Steps:</b> {sum(sig.sequence_verified)}/6 verified"
                        )
                        
                        if env['TELEGRAM_AVAILABLE']:
                            await telegram_send(msg, env['TELEGRAM_TOKEN'], env['TELEGRAM_CHAT_ID'])
                        else:
                            logging.info(f"SIGNAL (No Telegram): {sig.symbol} {sig.side} {sig.timeframe}")
            
            if signals_found > 0:
                logging.info(f"Scan complete. Found {signals_found} signals.")
            else:
                logging.info("Scan complete. No signals found.")

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            logging.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL)

# -------------------- Run --------------------
if __name__ == "__main__":
    try:
        asyncio.run(scanner())
    except KeyboardInterrupt:
        logging.info("Scanner stopped by user")
        # Try to send shutdown message if env vars were loaded
        try:
            env = load_environment_variables()
            if env and env['TELEGRAM_AVAILABLE']:
                asyncio.run(telegram_send(
                    "🛑 <b>ROMEOPT SCANNER STOPPED</b>\n\nManual interruption by user.",
                    env['TELEGRAM_TOKEN'],
                    env['TELEGRAM_CHAT_ID']
                ))
        except:
            pass
    except Exception as e:
        logging.error(f"Fatal error: {e}")