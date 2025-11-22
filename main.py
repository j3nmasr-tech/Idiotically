#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OPTIMIZED BTC WINNER SCANNER
- 15m+ timeframes only
- Trend alignment filter
- Volume confirmation
- Reduced noise signals
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from collections import deque

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/btc_winners.db"

# OPTIMIZED SETTINGS FOR BTC ONLY
SCAN_INTERVAL = 120  # Less frequent, higher quality
BTC_SYMBOL = "BTC/USDT"
WINNING_TIMEFRAMES = ["15m", "30m", "1h"]  # Remove noisy low TFs
TREND_CONFIRMATION_TF = "4h"  # For trend alignment

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | BTC-WINNER | %(message)s")
log = logging.getLogger("btc_winner_scanner")
db_lock = asyncio.Lock()

# ---------------- TELEGRAM ----------------
async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    async with httpx.AsyncClient() as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
        )

# ---------------- OPTIMIZED DATABASE ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS btc_winners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
            timestamp TEXT, score INTEGER, trend_aligned INTEGER,
            volume_confirm INTEGER, timeframe TEXT,
            status TEXT DEFAULT 'OPEN'
        );
        """)
        await db.commit()

# ---------------- WINNER DETECTION CORE ----------------
def calculate_trend_strength(df_1h, df_4h):
    """Return trend strength: 0-100, >60 = strong trend"""
    if df_1h is None or df_4h is None:
        return 0
    
    # Multi-timeframe EMA alignment
    ema_1h_20 = df_1h['close'].ewm(span=20).mean().iloc[-1]
    ema_1h_50 = df_1h['close'].ewm(span=50).mean().iloc[-1]
    ema_4h_20 = df_4h['close'].ewm(span=20).mean().iloc[-1]
    ema_4h_50 = df_4h['close'].ewm(span=50).mean().iloc[-1]
    
    current_price = df_1h['close'].iloc[-1]
    
    # Score trend alignment (0-100 points)
    score = 0
    if current_price > ema_1h_20: score += 25
    if current_price > ema_1h_50: score += 25  
    if current_price > ema_4h_20: score += 25
    if current_price > ema_4h_50: score += 25
    
    return score

def volume_confirmation(current_df, higher_tf_df):
    """Check if volume supports the move"""
    if higher_tf_df is None or len(higher_tf_df) < 20:
        return False
    
    current_volume = current_df['vol'].iloc[-1]
    avg_volume = higher_tf_df['vol'].tail(20).mean()
    
    return current_volume > avg_volume * 1.2  # 20% above average

def detect_high_probability_setup(df, higher_tf_df, trend_strength):
    """Only trigger on high-probability patterns"""
    if trend_strength < 60:  # Must have strong trend alignment
        return None
    
    # Your existing SMC logic but filtered
    ob_type, ob_hi, ob_lo = detect_order_blocks(df)
    if ob_type is None:
        return None
    
    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_h, sweep_l = detect_sweep(df)
    bos_hh, bos_ll = detect_bos_mss(df)
    
    if not (bos_hh or bos_ll):
        return None
    
    # Volume confirmation required
    if not volume_confirmation(df, higher_tf_df):
        return None
    
    # Enhanced scoring for winners
    score = 0
    reasons = []
    
    # Trend alignment bonus
    if trend_strength >= 80:
        score += 3
        reasons.append("Strong Trend +3")
    else:
        score += 1
        reasons.append("Good Trend +1")
    
    # Core SMC components
    if ob_type == "bullish": 
        score += 2
        reasons.append("OB Bull +2")
    else: 
        score += 2
        reasons.append("OB Bear +2")
    
    if bull_fvg or bear_fvg: 
        score += 2
        reasons.append("FVG +2")
    
    score += 2
    reasons.append("BOS +2")
    
    if sweep_h or sweep_l: 
        score += 1
        reasons.append("Sweep +1")
    
    # Minimum score threshold for winners
    if score < 8:  # Increased from 5
        return None
    
    return {
        "side": "BUY" if ob_type == "bullish" else "SELL",
        "entry": float(df['close'].iloc[-1]),
        "score": score,
        "trend_strength": trend_strength,
        "reasons": reasons
    }

# ---------------- KEEP YOUR EXISTING SMC FUNCTIONS ----------------
def detect_order_blocks(df: pd.DataFrame):
    if len(df) < 3: return None, None, None
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        return "bullish", candle["open"], candle["low"]
    return "bearish", candle["high"], candle["open"]

def detect_fvg(df: pd.DataFrame):
    if len(df) < 3: return False, False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
    bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
    return bull, bear

def detect_sweep(df: pd.DataFrame):
    if len(df) < 6: return False, False
    last = df.iloc[-1]
    prev = df.iloc[-5:-1]
    return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

def detect_bos_mss(df: pd.DataFrame):
    return detect_sweep(df)

# ---------------- OPTIMIZED POSITION SIZING ----------------
def calculate_optimal_tp_sl(signal, atr_val, trend_strength):
    """Better TP/SL based on trend strength"""
    entry = signal["entry"]
    side = signal["side"]
    
    # Dynamic multipliers based on trend strength
    if trend_strength >= 80:
        tp_mult, sl_mult = 1.2, 0.8  # Let winners run in strong trends
    else:
        tp_mult, sl_mult = 0.8, 1.0  # Conservative in weaker trends
    
    if atr_val:
        if side == "BUY":
            sl = entry - sl_mult * atr_val
            tp1 = entry + tp_mult * atr_val
            tp2 = entry + tp_mult * 1.8 * atr_val  # Wider spacing
            tp3 = entry + tp_mult * 3.0 * atr_val
        else:
            sl = entry + sl_mult * atr_val
            tp1 = entry - tp_mult * atr_val
            tp2 = entry - tp_mult * 1.8 * atr_val
            tp3 = entry - tp_mult * 3.0 * atr_val
    else:
        # Fallback to percentage-based
        if side == "BUY":
            sl = entry * 0.994
            tp1 = entry * 1.006
            tp2 = entry * 1.012
            tp3 = entry * 1.020
        else:
            sl = entry * 1.006
            tp1 = entry * 0.994
            tp2 = entry * 0.988
            tp3 = entry * 0.980
    
    return sl, tp1, tp2, tp3

# ---------------- OPTIMIZED SCAN LOOP ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.warning(f"OHLCV failed {timeframe}: {e}")
        return None

async def optimized_btc_scan(exchange):
    """Scan only BTC on optimized timeframes"""
    log.info("Starting BTC winner scan...")
    
    # Fetch multi-timeframe data for context
    timeframe_data = {}
    for tf in WINNING_TIMEFRAMES + [TREND_CONFIRMATION_TF]:
        ohlcv = await fetch_ohlcv(exchange, BTC_SYMBOL, tf, 200)
        if ohlcv:
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
            for col in ["open", "high", "low", "close", "vol"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            timeframe_data[tf] = df
    
    if TREND_CONFIRMATION_TF not in timeframe_data:
        return  # Need trend data
    
    trend_strength = calculate_trend_strength(
        timeframe_data.get("1h"), 
        timeframe_data[TREND_CONFIRMATION_TF]
    )
    
    # Scan only winning timeframes
    for tf in WINNING_TIMEFRAMES:
        if tf not in timeframe_data:
            continue
            
        signal = detect_high_probability_setup(
            timeframe_data[tf],
            timeframe_data[TREND_CONFIRMATION_TF], 
            trend_strength
        )
        
        if signal:
            # Calculate ATR for better TP/SL
            atr_val = None
            if "1h" in timeframe_data:
                df_1h = timeframe_data["1h"]
                high, low, close = df_1h["high"], df_1h["low"], df_1h["close"]
                tr = pd.DataFrame({
                    "h-l": high - low,
                    "h-pc": (high - close.shift(1)).abs(),
                    "l-pc": (low - close.shift(1)).abs()
                }).max(axis=1)
                atr_val = float(tr.rolling(14).mean().iloc[-1])
            
            sl, tp1, tp2, tp3 = calculate_optimal_tp_sl(signal, atr_val, trend_strength)
            
            # Send high-confidence signal
            message = (
                f"🏆 BTC HIGH CONFIDENCE ({tf})\n"
                f"Direction: {signal['side']}\n"
                f"Entry: {signal['entry']:.1f}\n"
                f"SL: {sl:.1f} | TP: {tp1:.1f} / {tp2:.1f} / {tp3:.1f}\n"
                f"Score: {signal['score']}/10 | Trend: {trend_strength}%\n"
                f"Reasons: {', '.join(signal['reasons'])}"
            )
            
            await tg(message)
            
            # Log to database
            async with db_lock:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("""
                        INSERT INTO btc_winners 
                        (entry, sl, tp1, tp2, tp3, timestamp, score, trend_aligned, volume_confirm, timeframe)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (signal["entry"], sl, tp1, tp2, tp3, 
                          datetime.datetime.utcnow().isoformat(),
                          signal["score"], trend_strength, 1, tf))
                    await db.commit()

# ---------------- MAIN LOOP ----------------
async def main_loop():
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    
    while True:
        try:
            await optimized_btc_scan(exchange)
            await asyncio.sleep(SCAN_INTERVAL)
        except Exception as e:
            log.error(f"Main loop error: {e}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main_loop())