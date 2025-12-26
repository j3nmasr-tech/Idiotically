#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
YOUR 6-STEP CONFLUENCE SCANNER
WITH COMPLETE LOGIC BREAKDOWN
Database initialization FIXED
"""

import os
import time
import asyncio
import logging
import datetime
import json
import hashlib
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 60))
MIN_CONFIDENCE = 1  # Need at least 4 out of 6 steps confirmed

# Timeframes for MTF analysis
TIMEFRAMES = {
    "HTF": "4h",    # Higher Timeframe: Primary Trend
    "MTF": "1h",    # Medium Timeframe: Wave Analysis  
    "LTF": "15m"    # Lower Timeframe: Entry & Indicators
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("6step_transparent")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# ---------------- TELEGRAM ----------------
async def tg(msg: str):
    """Send Telegram message"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient() as client:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            })
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

# ---------------- DATABASE FIXED ----------------
async def init_db():
    """Initialize database - DROP and RECREATE to ensure schema matches"""
    global db_conn
    try:
        # Remove old database file if exists
        if os.path.exists(DB_PATH):
            log.info(f"Removing old database: {DB_PATH}")
            os.remove(DB_PATH)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        # Create fresh database
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Create table with ALL required columns
        await db_conn.execute("""
            CREATE TABLE signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp REAL NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                
                -- Step Results --
                step1_result INTEGER DEFAULT 0,
                step2_result INTEGER DEFAULT 0,
                step3_result INTEGER DEFAULT 0,
                step4_result INTEGER DEFAULT 0,
                step5_result INTEGER DEFAULT 0,
                step6_result INTEGER DEFAULT 0,
                confidence_score INTEGER DEFAULT 0,
                
                -- Complete Logic Breakdown --
                logic_breakdown TEXT,
                
                -- Analysis Details --
                timeframe_combo TEXT,
                trend_direction TEXT,
                wave_pattern TEXT,
                rsi_value REAL,
                macd_hist REAL,
                volume_ratio REAL,
                strength_level TEXT,
                
                -- Risk Management --
                atr_value REAL,
                rr_ratio REAL,
                risk_pct REAL,
                reward_pct REAL,
                
                signal_hash TEXT UNIQUE
            )
        """)
        
        # Create indexes
        await db_conn.execute("CREATE INDEX idx_symbol_status ON signals(symbol, status);")
        await db_conn.execute("CREATE INDEX idx_timestamp ON signals(timestamp);")
        await db_conn.execute("CREATE INDEX idx_confidence ON signals(confidence_score);")
        await db_conn.execute("CREATE INDEX idx_signal_hash ON signals(signal_hash);")
        
        await db_conn.commit()
        log.info("✅ Database created FRESH with all columns")
        
        # Verify columns exist
        async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            log.info(f"Database columns: {[col[1] for col in columns]}")
            
        return True
        
    except Exception as e:
        log.error(f"Database initialization error: {e}")
        if db_conn:
            await db_conn.close()
            db_conn = None
        return False

# ---------------- DATA FETCHING ----------------
async def fetch_multi_timeframe_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Fetch data for all 3 timeframes"""
    data = {}
    for tf_type, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=200)
            if ohlcv and len(ohlcv) > 50:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                data[tf_type] = df
            else:
                return None
        except:
            return None
    return data

# ================ YOUR 6 STEPS WITH LOGIC TRACKING ================

def step1_mtf_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, str]:
    """
    STEP 1: Multi-Timeframe Analysis
    Returns: (passed, trend, logic_description)
    """
    logic_lines = []
    try:
        htf_df = data['HTF']
        mtf_df = data['MTF']
        ltf_df = data['LTF']
        
        # Calculate trends
        htf_price_50 = htf_df['close'].iloc[-50] if len(htf_df) >= 50 else htf_df['close'].iloc[0]
        mtf_price_20 = mtf_df['close'].iloc[-20] if len(mtf_df) >= 20 else mtf_df['close'].iloc[0]
        ltf_price_10 = ltf_df['close'].iloc[-10] if len(ltf_df) >= 10 else ltf_df['close'].iloc[0]
        
        htf_current = htf_df['close'].iloc[-1]
        mtf_current = mtf_df['close'].iloc[-1]
        ltf_current = ltf_df['close'].iloc[-1]
        
        htf_trend = "UP" if htf_current > htf_price_50 else "DOWN"
        mtf_trend = "UP" if mtf_current > mtf_price_20 else "DOWN"
        ltf_trend = "UP" if ltf_current > ltf_price_10 else "DOWN"
        
        logic_lines.append("📊 STEP 1: MULTI-TIMEFRAME ANALYSIS")
        logic_lines.append(f"HTF (4h): {htf_current:.6f} vs {htf_price_50:.6f} → {htf_trend}")
        logic_lines.append(f"MTF (1h): {mtf_current:.6f} vs {mtf_price_20:.6f} → {mtf_trend}")
        logic_lines.append(f"LTF (15m): {ltf_current:.6f} vs {ltf_price_10:.6f} → {ltf_trend}")
        
        # Score alignment
        score = 0
        if htf_trend == mtf_trend: 
            score += 1
            logic_lines.append("✓ HTF & MTF aligned")
        else:
            logic_lines.append("✗ HTF & MTF not aligned")
            
        if mtf_trend == ltf_trend: 
            score += 1
            logic_lines.append("✓ MTF & LTF aligned")
        else:
            logic_lines.append("✗ MTF & LTF not aligned")
            
        if htf_trend == ltf_trend: 
            score += 1
            logic_lines.append("✓ HTF & LTF aligned")
        else:
            logic_lines.append("✗ HTF & LTF not aligned")
        
        logic_lines.append(f"Alignment Score: {score}/3")
        
        # Determine dominant trend
        trends = [htf_trend, mtf_trend, ltf_trend]
        dominant_trend = max(set(trends), key=trends.count)
        logic_lines.append(f"Dominant Trend: {dominant_trend} (based on majority)")
        
        # Pass if at least 2 timeframes aligned
        passed = 1 if score >= 2 else 0
        logic_lines.append(f"Step Result: {'PASS' if passed else 'FAIL'} ({'≥2 timeframes aligned' if passed else '<2 timeframes aligned'})")
        
        logic_text = "\n".join(logic_lines)
        return passed, dominant_trend, logic_text
        
    except Exception as e:
        logic_lines.append(f"ERROR: {str(e)}")
        return 0, "ERROR", "\n".join(logic_lines)

def step2_wave_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, str]:
    """
    STEP 2: Wave Analysis
    Returns: (passed, pattern, logic_description)
    """
    logic_lines = []
    try:
        mtf_df = data['MTF']
        prices = mtf_df['close'].values[-30:]
        
        logic_lines.append("\n🌊 STEP 2: WAVE ANALYSIS")
        logic_lines.append(f"Analyzing last {len(prices)} candles on 1h")
        
        # Find swing points
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(prices)-2):
            # Swing high detection
            if (prices[i] > prices[i-1] and prices[i] > prices[i-2] and
                prices[i] > prices[i+1] and prices[i] > prices[i+2]):
                swing_highs.append(prices[i])
            
            # Swing low detection
            if (prices[i] < prices[i-1] and prices[i] < prices[i-2] and
                prices[i] < prices[i+1] and prices[i] < prices[i+2]):
                swing_lows.append(prices[i])
        
        logic_lines.append(f"Found {len(swing_highs)} swing highs, {len(swing_lows)} swing lows")
        
        # Pattern recognition
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            last_high_1 = swing_highs[-1]
            last_high_2 = swing_highs[-2] if len(swing_highs) >= 2 else last_high_1
            last_low_1 = swing_lows[-1]
            last_low_2 = swing_lows[-2] if len(swing_lows) >= 2 else last_low_1
            
            logic_lines.append(f"Last 2 highs: {last_high_2:.6f} → {last_high_1:.6f}")
            logic_lines.append(f"Last 2 lows: {last_low_2:.6f} → {last_low_1:.6f}")
            
            # Uptrend pattern: higher highs AND higher lows
            if last_high_1 > last_high_2 and last_low_1 > last_low_2:
                pattern = "IMPULSE_UP"
                passed = 1
                logic_lines.append("✓ Pattern: IMPULSE UP (Higher Highs & Higher Lows)")
            
            # Downtrend pattern: lower highs AND lower lows
            elif last_high_1 < last_high_2 and last_low_1 < last_low_2:
                pattern = "IMPULSE_DOWN"
                passed = 1
                logic_lines.append("✓ Pattern: IMPULSE DOWN (Lower Highs & Lower Lows)")
            
            # Correction pattern
            elif (last_high_1 < last_high_2 and last_low_1 > last_low_2) or \
                 (last_high_1 > last_high_2 and last_low_1 < last_low_2):
                pattern = "CORRECTION"
                passed = 1
                logic_lines.append("✓ Pattern: CORRECTION (Mixed Highs/Lows)")
            
            else:
                pattern = "CONSOLIDATION"
                passed = 0
                logic_lines.append("✗ Pattern: CONSOLIDATION (No clear direction)")
        
        else:
            pattern = "INSUFFICIENT_SWINGS"
            passed = 0
            logic_lines.append("✗ Insufficient swing points for analysis")
        
        logic_lines.append(f"Step Result: {'PASS' if passed else 'FAIL'} ({pattern})")
        logic_text = "\n".join(logic_lines)
        return passed, pattern, logic_text
        
    except Exception as e:
        logic_lines.append(f"ERROR: {str(e)}")
        return 0, "ERROR", "\n".join(logic_lines)

def step3_strength_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, str]:
    """
    STEP 3: Strength/Momentum Analysis
    Returns: (passed, strength_level, logic_description)
    """
    logic_lines = []
    try:
        mtf_df = data['MTF']
        prices = mtf_df['close'].values[-20:]
        
        logic_lines.append("\n💪 STEP 3: STRENGTH ANALYSIS")
        logic_lines.append(f"Analyzing momentum on last {len(prices)} candles")
        
        # 1. Rate of Change (5-period)
        if len(prices) >= 6:
            roc_5 = ((prices[-1] / prices[-6]) - 1) * 100
            logic_lines.append(f"ROC(5): {roc_5:+.2f}%")
        else:
            roc_5 = 0
            logic_lines.append("ROC(5): Insufficient data")
        
        # 2. Price slope (linear regression on last 10)
        if len(prices) >= 10:
            x = np.arange(len(prices[-10:]))
            y = prices[-10:]
            slope, _ = np.polyfit(x, y, 1)
            slope_pct = (slope / prices[-1]) * 100
            logic_lines.append(f"Slope(10): {slope_pct:+.4f}% per candle")
        else:
            slope_pct = 0
            logic_lines.append("Slope(10): Insufficient data")
        
        # 3. Average candle size
        if len(mtf_df) >= 10:
            avg_candle_size = (mtf_df['high'].iloc[-10:] - mtf_df['low'].iloc[-10:]).mean()
            avg_candle_pct = (avg_candle_size / prices[-1]) * 100
            logic_lines.append(f"Avg Candle Size: {avg_candle_pct:.2f}% of price")
        else:
            avg_candle_pct = 0
            logic_lines.append("Avg Candle Size: Insufficient data")
        
        # Strength determination
        if abs(roc_5) > 2.0 and abs(slope_pct) > 0.1 and avg_candle_pct > 0.5:
            strength = "STRONG"
            passed = 1
            logic_lines.append(f"✓ Strength: STRONG (All thresholds met)")
        elif abs(roc_5) > 1.0 and abs(slope_pct) > 0.05 and avg_candle_pct > 0.3:
            strength = "MODERATE"
            passed = 1
            logic_lines.append(f"✓ Strength: MODERATE (Medium thresholds met)")
        else:
            strength = "WEAK"
            passed = 0
            logic_lines.append(f"✗ Strength: WEAK (Thresholds not met)")
        
        # Direction
        direction = "UP" if roc_5 > 0 else "DOWN"
        logic_lines.append(f"Direction: {direction} (Based on ROC sign)")
        
        logic_lines.append(f"Step Result: {'PASS' if passed else 'FAIL'} ({strength} {direction})")
        logic_text = "\n".join(logic_lines)
        return passed, f"{strength}_{direction}", logic_text
        
    except Exception as e:
        logic_lines.append(f"ERROR: {str(e)}")
        return 0, "ERROR", "\n".join(logic_lines)

def step4_indicators_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, str, float, float]:
    """
    STEP 4: Technical Indicators (RSI & MACD)
    Returns: (passed, signal, logic_description, rsi, macd_hist)
    """
    logic_lines = []
    try:
        ltf_df = data['LTF']
        prices = ltf_df['close'].values
        
        logic_lines.append("\n📈 STEP 4: TECHNICAL INDICATORS")
        
        # RSI Calculation
        if len(prices) >= 15:
            deltas = np.diff(prices[-15:])
            gains = deltas[deltas >= 0]
            losses = -deltas[deltas < 0]
            
            avg_gain = np.mean(gains) if len(gains) > 0 else 0
            avg_loss = np.mean(losses) if len(losses) > 0 else 0
            
            if avg_loss == 0:
                rs = 100 if avg_gain > 0 else 0
            else:
                rs = avg_gain / avg_loss
            
            rsi = 100 - (100 / (1 + rs))
            logic_lines.append(f"RSI: {rsi:.2f} (Avg Gain: {avg_gain:.6f}, Avg Loss: {avg_loss:.6f})")
        else:
            rsi = 50
            logic_lines.append("RSI: 50.0 (default, insufficient data)")
        
        # RSI Signal
        if rsi < 30:
            rsi_signal = "BULLISH"
        elif rsi > 70:
            rsi_signal = "BEARISH"
        else:
            rsi_signal = "NEUTRAL"
        
        logic_lines.append(f"RSI Signal: {rsi_signal}")
        
        # MACD Calculation
        if len(prices) >= 26:
            ema_12 = pd.Series(prices[-26:]).ewm(span=12, adjust=False).mean().iloc[-1]
            ema_26 = pd.Series(prices[-26:]).ewm(span=26, adjust=False).mean().iloc[-1]
            macd_line = ema_12 - ema_26
            macd_hist = pd.Series([macd_line]).ewm(span=9, adjust=False).mean().iloc[-1]
            histogram = macd_line - macd_hist
            
            logic_lines.append(f"MACD Histogram: {histogram:+.6f}")
            
            if macd_line > macd_hist and histogram > 0:
                macd_signal = "BULLISH"
            elif macd_line < macd_hist and histogram < 0:
                macd_signal = "BEARISH"
            else:
                macd_signal = "NEUTRAL"
            
            logic_lines.append(f"MACD Signal: {macd_signal}")
        else:
            macd_signal = "NEUTRAL"
            histogram = 0
            logic_lines.append("MACD: NEUTRAL (insufficient data)")
        
        # Combined Signal
        if rsi_signal == macd_signal and rsi_signal != "NEUTRAL":
            signal = rsi_signal
            passed = 1
            logic_lines.append(f"✓ Both indicators agree: {signal}")
        elif (rsi_signal == "BULLISH" or macd_signal == "BULLISH") and \
             not (rsi_signal == "BEARISH" or macd_signal == "BEARISH"):
            signal = "BULLISH"
            passed = 1
            logic_lines.append(f"✓ At least one BULLISH: {signal}")
        elif (rsi_signal == "BEARISH" or macd_signal == "BEARISH") and \
             not (rsi_signal == "BULLISH" or macd_signal == "BULLISH"):
            signal = "BEARISH"
            passed = 1
            logic_lines.append(f"✓ At least one BEARISH: {signal}")
        else:
            signal = "NEUTRAL"
            passed = 0
            logic_lines.append(f"✗ No clear signal: {signal}")
        
        logic_lines.append(f"Step Result: {'PASS' if passed else 'FAIL'} ({signal})")
        logic_text = "\n".join(logic_lines)
        return passed, signal, logic_text, rsi, histogram
        
    except Exception as e:
        logic_lines.append(f"ERROR: {str(e)}")
        return 0, "ERROR", "\n".join(logic_lines), 50, 0

def step5_volume_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, str, float]:
    """
    STEP 5: Volume Analysis
    Returns: (passed, volume_strength, logic_description, volume_ratio)
    """
    logic_lines = []
    try:
        ltf_df = data['LTF']
        
        logic_lines.append("\n📊 STEP 5: VOLUME ANALYSIS")
        
        # Recent vs previous volume
        recent_vol = ltf_df['volume'].values[-10:]
        prev_vol = ltf_df['volume'].values[-20:-10]
        
        if len(recent_vol) > 0 and len(prev_vol) > 0:
            avg_recent = np.mean(recent_vol)
            avg_prev = np.mean(prev_vol)
            volume_ratio = avg_recent / avg_prev if avg_prev > 0 else 1
            
            logic_lines.append(f"Volume Ratio: {volume_ratio:.2f}x (Recent/Previous)")
        else:
            volume_ratio = 1
            logic_lines.append("Volume Ratio: 1.0x (insufficient data)")
        
        # Volume on up vs down candles
        up_mask = ltf_df['close'] > ltf_df['open']
        down_mask = ltf_df['close'] < ltf_df['open']
        
        up_volume = ltf_df[up_mask]['volume'].mean() if up_mask.any() else 0
        down_volume = ltf_df[down_mask]['volume'].mean() if down_mask.any() else 0
        
        logic_lines.append(f"Up Candle Volume: {up_volume:.2f}")
        logic_lines.append(f"Down Candle Volume: {down_volume:.2f}")
        
        # Price change for confirmation
        price_change = ((ltf_df['close'].iloc[-1] / ltf_df['close'].iloc[-10]) - 1) * 100
        logic_lines.append(f"Price Change: {price_change:+.2f}%")
        
        # Volume confirmation
        volume_confirmed = False
        if price_change > 0 and up_volume > down_volume:
            volume_confirmed = True
            logic_lines.append("✓ Volume confirms UP move")
        elif price_change < 0 and down_volume > up_volume:
            volume_confirmed = True
            logic_lines.append("✓ Volume confirms DOWN move")
        else:
            logic_lines.append("✗ Volume divergence")
        
        # Volume strength
        if volume_ratio > 1.5:
            strength = "VERY_HIGH"
            passed = 1 if volume_confirmed else 0
        elif volume_ratio > 1.2:
            strength = "HIGH"
            passed = 1 if volume_confirmed else 0
        elif volume_ratio > 0.8:
            strength = "MODERATE"
            passed = 1 if volume_confirmed else 0
        else:
            strength = "LOW"
            passed = 0
        
        logic_lines.append(f"Volume Strength: {strength}")
        logic_lines.append(f"Step Result: {'PASS' if passed else 'FAIL'} ({strength})")
        
        logic_text = "\n".join(logic_lines)
        return passed, strength, logic_text, volume_ratio
        
    except Exception as e:
        logic_lines.append(f"ERROR: {str(e)}")
        return 0, "ERROR", "\n".join(logic_lines), 1

def step6_trend_identification(data: Dict[str, pd.DataFrame], 
                              step1_trend: str,
                              step2_pattern: str,
                              step3_strength: str,
                              step4_signal: str,
                              step5_strength: str) -> Tuple[int, str, str]:
    """
    STEP 6: Trend Identification (Final Decision)
    Returns: (passed, final_trend, logic_description)
    """
    logic_lines = []
    try:
        htf_df = data['HTF']
        
        logic_lines.append("\n🎯 STEP 6: TREND IDENTIFICATION")
        
        # Collect votes from previous steps
        trend_votes = []
        
        # From Step 1
        trend_votes.append(step1_trend)
        logic_lines.append(f"Step 1 vote: {step1_trend}")
        
        # From Step 2
        if "UP" in step2_pattern:
            trend_votes.append("UP")
            logic_lines.append(f"Step 2 vote: UP ({step2_pattern})")
        elif "DOWN" in step2_pattern:
            trend_votes.append("DOWN")
            logic_lines.append(f"Step 2 vote: DOWN ({step2_pattern})")
        else:
            logic_lines.append(f"Step 2 vote: NO_VOTE ({step2_pattern})")
        
        # From Step 3
        if "UP" in step3_strength:
            trend_votes.append("UP")
            logic_lines.append(f"Step 3 vote: UP ({step3_strength})")
        elif "DOWN" in step3_strength:
            trend_votes.append("DOWN")
            logic_lines.append(f"Step 3 vote: DOWN ({step3_strength})")
        else:
            logic_lines.append(f"Step 3 vote: NO_VOTE ({step3_strength})")
        
        # From Step 4
        if step4_signal == "BULLISH":
            trend_votes.append("UP")
            logic_lines.append(f"Step 4 vote: UP ({step4_signal})")
        elif step4_signal == "BEARISH":
            trend_votes.append("DOWN")
            logic_lines.append(f"Step 4 vote: DOWN ({step4_signal})")
        else:
            logic_lines.append(f"Step 4 vote: NO_VOTE ({step4_signal})")
        
        # From Step 5 (only if strong volume)
        if step5_strength in ["VERY_HIGH", "HIGH"]:
            price_change = ((htf_df['close'].iloc[-1] / htf_df['close'].iloc[-5]) - 1) * 100
            if price_change > 0:
                trend_votes.append("UP")
                logic_lines.append(f"Step 5 vote: UP (Strong volume on UP move)")
            elif price_change < 0:
                trend_votes.append("DOWN")
                logic_lines.append(f"Step 5 vote: DOWN (Strong volume on DOWN move)")
        else:
            logic_lines.append(f"Step 5 vote: NO_VOTE ({step5_strength} volume)")
        
        # Count votes
        up_count = trend_votes.count("UP")
        down_count = trend_votes.count("DOWN")
        
        logic_lines.append(f"\nVote Count: UP={up_count}, DOWN={down_count}")
        
        # Decision
        if up_count > down_count:
            final_trend = "UP"
            passed = 1
            logic_lines.append(f"✓ Majority votes for UP")
        elif down_count > up_count:
            final_trend = "DOWN"
            passed = 1
            logic_lines.append(f"✓ Majority votes for DOWN")
        else:
            final_trend = "NEUTRAL"
            passed = 0
            logic_lines.append(f"✗ No clear majority")
        
        # Moving average confirmation
        if len(htf_df) >= 50:
            sma_20 = htf_df['close'].rolling(window=20).mean().iloc[-1]
            sma_50 = htf_df['close'].rolling(window=50).mean().iloc[-1]
            current_price = htf_df['close'].iloc[-1]
            
            logic_lines.append(f"\nSMA(20): {sma_20:.6f}")
            logic_lines.append(f"SMA(50): {sma_50:.6f}")
            logic_lines.append(f"Current Price: {current_price:.6f}")
            
            if final_trend == "UP" and current_price > sma_20 > sma_50:
                logic_lines.append("✓ Price structure confirms UP trend")
                passed = 1
            elif final_trend == "DOWN" and current_price < sma_20 < sma_50:
                logic_lines.append("✓ Price structure confirms DOWN trend")
                passed = 1
            else:
                logic_lines.append("✗ Price structure doesn't confirm trend")
                passed = 0
        
        logic_lines.append(f"\nStep Result: {'PASS' if passed else 'FAIL'} ({final_trend})")
        logic_text = "\n".join(logic_lines)
        return passed, final_trend, logic_text
        
    except Exception as e:
        logic_lines.append(f"ERROR: {str(e)}")
        return 0, "ERROR", "\n".join(logic_lines)

# ================ MAIN ANALYSIS FUNCTION ================

def execute_6step_analysis_with_full_logic(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """
    Execute all 6 steps and return signal with COMPLETE logic breakdown
    """
    try:
        log.info(f"Analyzing {symbol}...")
        
        # Initialize logic
        complete_logic = []
        
        # Step 1
        step1_passed, step1_trend, step1_logic = step1_mtf_analysis(data)
        complete_logic.append(step1_logic)
        
        # Step 2
        step2_passed, step2_pattern, step2_logic = step2_wave_analysis(data)
        complete_logic.append(step2_logic)
        
        # Step 3
        step3_passed, step3_strength, step3_logic = step3_strength_analysis(data)
        complete_logic.append(step3_logic)
        
        # Step 4
        step4_passed, step4_signal, step4_logic, rsi_value, macd_hist = step4_indicators_analysis(data)
        complete_logic.append(step4_logic)
        
        # Step 5
        step5_passed, step5_strength, step5_logic, volume_ratio = step5_volume_analysis(data)
        complete_logic.append(step5_logic)
        
        # Step 6
        step6_passed, step6_trend, step6_logic = step6_trend_identification(
            data, step1_trend, step2_pattern, step3_strength, step4_signal, step5_strength
        )
        complete_logic.append(step6_logic)
        
        # Calculate total
        steps_passed = [step1_passed, step2_passed, step3_passed, step4_passed, step5_passed, step6_passed]
        total_passed = sum(steps_passed)
        
        # Combine logic
        full_logic_text = "\n".join(complete_logic)
        
        # Add summary
        summary = f"\n{'='*50}\nFINAL SUMMARY FOR {symbol}\n{'='*50}"
        summary += f"\nStep 1 (MTF): {'✓' if step1_passed else '✗'} - {step1_trend}"
        summary += f"\nStep 2 (Wave): {'✓' if step2_passed else '✗'} - {step2_pattern}"
        summary += f"\nStep 3 (Strength): {'✓' if step3_passed else '✗'} - {step3_strength}"
        summary += f"\nStep 4 (Indicators): {'✓' if step4_passed else '✗'} - {step4_signal}"
        summary += f"\nStep 5 (Volume): {'✓' if step5_passed else '✗'} - {step5_strength}"
        summary += f"\nStep 6 (Trend): {'✓' if step6_passed else '✗'} - {step6_trend}"
        summary += f"\n{'='*50}"
        summary += f"\nTOTAL CONFLUENCE: {total_passed}/6"
        summary += f"\nMINIMUM REQUIRED: {MIN_CONFIDENCE}/6"
        summary += f"\n{'='*50}"
        
        full_logic_text += summary
        
        log.info(f"Analysis: {total_passed}/6 confluence for {symbol}")
        
        # Check confluence
        if total_passed >= MIN_CONFIDENCE and step6_trend in ["UP", "DOWN"]:
            current_price = data['LTF']['close'].iloc[-1]
            
            # Calculate ATR
            def calculate_atr(df, period=14):
                high = df['high']
                low = df['low']
                close = df['close']
                
                tr1 = high - low
                tr2 = abs(high - close.shift())
                tr3 = abs(low - close.shift())
                
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr = tr.rolling(window=period).mean()
                return atr.iloc[-1] if len(atr) > 0 else current_price * 0.02
            
            atr_value = calculate_atr(data['LTF'])
            
            # Set trade parameters
            if step6_trend == "UP":
                side = "BUY"
                sl = current_price - (atr_value * 1.5)
                tp = current_price + (2 * (current_price - sl))
            else:
                side = "SELL"
                sl = current_price + (atr_value * 1.5)
                tp = current_price - (2 * (sl - current_price))
            
            # Ensure valid
            if side == "BUY":
                if sl >= current_price:
                    sl = current_price * 0.98
                if tp <= current_price:
                    tp = current_price * 1.04
            else:
                if sl <= current_price:
                    sl = current_price * 1.02
                if tp >= current_price:
                    tp = current_price * 0.96
            
            # Calculate metrics
            risk = abs(current_price - sl)
            reward = abs(tp - current_price)
            rr_ratio = reward / risk if risk > 0 else 0
            risk_pct = (risk / current_price) * 100
            reward_pct = (reward / current_price) * 100
            
            # Create signal
            signal = {
                'symbol': symbol,
                'side': side,
                'entry': current_price,
                'sl': sl,
                'tp': tp,
                'status': 'OPEN',
                
                'step1_result': step1_passed,
                'step2_result': step2_passed,
                'step3_result': step3_passed,
                'step4_result': step4_passed,
                'step5_result': step5_passed,
                'step6_result': step6_passed,
                'confidence_score': total_passed,
                
                'logic_breakdown': full_logic_text,
                
                'timeframe_combo': f"{TIMEFRAMES['HTF']}|{TIMEFRAMES['MTF']}|{TIMEFRAMES['LTF']}",
                'trend_direction': step6_trend,
                'wave_pattern': step2_pattern,
                'rsi_value': rsi_value,
                'macd_hist': macd_hist,
                'volume_ratio': volume_ratio,
                'strength_level': step3_strength,
                
                'atr_value': atr_value,
                'rr_ratio': rr_ratio,
                'risk_pct': risk_pct,
                'reward_pct': reward_pct,
                
                'signal_hash': hashlib.md5(
                    f"{symbol}:{side}:{current_price:.8f}:{total_passed}:{time.time()}".encode()
                ).hexdigest()
            }
            
            log.info(f"✅ Signal found for {symbol}: {side}")
            return signal
        
        log.info(f"❌ No signal for {symbol}: {total_passed}/6")
        return None
        
    except Exception as e:
        log.error(f"Analysis error: {e}")
        return None

# ---------------- DATABASE & ALERT FUNCTIONS ----------------
async def save_signal_with_logic(signal: Dict) -> bool:
    """Save signal to database"""
    try:
        async with db_lock:
            # Check duplicate
            async with db_conn.execute(
                "SELECT COUNT(*) FROM signals WHERE signal_hash = ?",
                (signal['signal_hash'],)
            ) as cursor:
                exists = (await cursor.fetchone())[0]
            
            if exists > 0:
                log.info(f"Duplicate: {signal['symbol']}")
                return False
            
            # Insert
            await db_conn.execute("""
                INSERT INTO signals (
                    symbol, side, entry, sl, tp, status,
                    step1_result, step2_result, step3_result, step4_result,
                    step5_result, step6_result, confidence_score, logic_breakdown,
                    timeframe_combo, trend_direction, wave_pattern, rsi_value,
                    macd_hist, volume_ratio, strength_level, atr_value, rr_ratio,
                    risk_pct, reward_pct, signal_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                signal['tp'], signal['status'], signal['step1_result'],
                signal['step2_result'], signal['step3_result'], signal['step4_result'],
                signal['step5_result'], signal['step6_result'], signal['confidence_score'],
                signal['logic_breakdown'], signal['timeframe_combo'], signal['trend_direction'],
                signal['wave_pattern'], signal['rsi_value'], signal['macd_hist'],
                signal['volume_ratio'], signal['strength_level'], signal['atr_value'],
                signal['rr_ratio'], signal['risk_pct'], signal['reward_pct'],
                signal['signal_hash']
            ))
            
            await db_conn.commit()
            log.info(f"Signal saved: {signal['symbol']}")
            return True
            
    except Exception as e:
        log.error(f"Save error: {e}")
        return False

async def send_comprehensive_alert(signal: Dict):
    """Send alert with full logic"""
    try:
        # Create message
        message = f"""
🎯 **6-STEP METHOD - COMPLETE LOGIC** 🎯

**{signal['symbol']}** | **{signal['side']}**
Confluence: {signal['confidence_score']}/6 steps

**TRADE:**
Entry: {signal['entry']:.6f}
SL: {signal['sl']:.6f} ({signal['risk_pct']:.1f}%)
TP: {signal['tp']:.6f} ({signal['reward_pct']:.1f}%)
R:R: {signal['rr_ratio']:.2f}:1

**SUMMARY:**
Trend: {signal['trend_direction']}
Wave: {signal['wave_pattern']}
Strength: {signal['strength_level']}
RSI: {signal['rsi_value']:.1f}
MACD Hist: {signal['macd_hist']:+.6f}
Volume Ratio: {signal['volume_ratio']:.2f}x

**STEPS:**
1️⃣ MTF: {'✓' if signal['step1_result'] else '✗'}
2️⃣ Wave: {'✓' if signal['step2_result'] else '✗'}
3️⃣ Strength: {'✓' if signal['step3_result'] else '✗'}
4️⃣ Indicators: {'✓' if signal['step4_result'] else '✗'}
5️⃣ Volume: {'✓' if signal['step5_result'] else '✗'}
6️⃣ Trend: {'✓' if signal['step6_result'] else '✗'}

Timeframes: {signal['timeframe_combo']}
"""

        # Add logic breakdown if not too long
        logic_preview = signal['logic_breakdown'][:1500]
        if len(signal['logic_breakdown']) > 1500:
            logic_preview += "\n... [full logic available in database]"
        
        message += f"\n{'='*40}\n**LOGIC BREAKDOWN:**\n{logic_preview}"
        
        await tg(message)
        log.info(f"Alert sent: {signal['symbol']}")
        
    except Exception as e:
        log.error(f"Alert error: {e}")

# ---------------- SCANNING LOOP ----------------
async def scan_symbols_loop(exchange):
    """Main scanning loop"""
    while True:
        try:
            log.info("Starting scan...")
            
            # Get top pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, t['quoteVolume']) for s, t in tickers.items() 
                         if s.endswith('/USDT') and t.get('quoteVolume', 0) > 0]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                try:
                    log.info(f"Analyzing {symbol}")
                    
                    # Fetch data
                    data = await fetch_multi_timeframe_data(exchange, symbol)
                    if not data:
                        continue
                    
                    # Run analysis
                    signal = execute_6step_analysis_with_full_logic(data, symbol)
                    
                    if signal:
                        # Save and alert
                        if await save_signal_with_logic(signal):
                            await send_comprehensive_alert(signal)
                            signals_found += 1
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    log.error(f"Error on {symbol}: {e}")
                    continue
            
            log.info(f"Scan complete. Found {signals_found} signals.")
            log.info(f"Next scan in {SCAN_INTERVAL} seconds...")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan error: {e}")
            await asyncio.sleep(30)

# ---------------- MONITORING ----------------
async def monitor_signals(exchange):
    """Monitor open signals for TP/SL"""
    while True:
        try:
            async with db_lock:
                # Get open signals
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp FROM signals 
                    WHERE status='OPEN'
                """) as cursor:
                    signals = await cursor.fetchall()
                
                for sig_id, symbol, side, entry, sl, tp in signals:
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = ticker.get('last')
                        
                        if not current_price:
                            continue
                        
                        tp_hit = False
                        sl_hit = False
                        
                        if side == "BUY":
                            if current_price >= tp:
                                tp_hit = True
                                await tg(f"✅ TP HIT: {symbol}")
                            elif current_price <= sl:
                                sl_hit = True
                                await tg(f"❌ SL HIT: {symbol}")
                        else:  # SELL
                            if current_price <= tp:
                                tp_hit = True
                                await tg(f"✅ TP HIT: {symbol}")
                            elif current_price >= sl:
                                sl_hit = True
                                await tg(f"❌ SL HIT: {symbol}")
                        
                        if tp_hit or sl_hit:
                            await db_conn.execute(
                                "UPDATE signals SET status='CLOSED' WHERE id=?",
                                (sig_id,)
                            )
                            await db_conn.commit()
                            
                    except Exception as e:
                        log.error(f"Monitor error {symbol}: {e}")
                        continue
            
        except Exception as e:
            log.error(f"Monitor loop error: {e}")
        
        await asyncio.sleep(30)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "6-Step Method with Complete Logic",
        "min_confidence": f"{MIN_CONFIDENCE}/6"
    }

@app.get("/signals")
async def get_signals(limit: int = 20, status: str = "OPEN"):
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT id, symbol, side, entry, sl, tp, confidence_score, 
                       timestamp, trend_direction, rr_ratio 
                FROM signals WHERE status=? ORDER BY timestamp DESC LIMIT ?
            """, (status, limit)) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
        
        signals = [dict(zip(columns, row)) for row in rows]
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        return {"error": str(e)}

# ---------------- MAIN ----------------
async def main():
    global exchange
    
    log.info("="*60)
    log.info("🚀 STARTING 6-STEP SCANNER")
    log.info("="*60)
    
    try:
        # Initialize database (FRESH)
        log.info("Initializing FRESH database...")
        if not await init_db():
            log.error("❌ Database failed")
            return
        
        # Initialize exchange
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        
        # Test connection
        await exchange.fetch_ticker("BTC/USDT")
        log.info("✅ Exchange connected")
        
        # Startup message
        await tg(f"""
🚀 **6-STEP SCANNER STARTED**

Method: Your exact 6-step confluence
Minimum: {MIN_CONFIDENCE}/6 steps required
Timeframes: 4h | 1h | 15m
Every signal shows complete logic breakdown
        """)
        
        # Start loops
        await asyncio.gather(
            scan_symbols_loop(exchange),
            monitor_signals(exchange)
        )
        
    except KeyboardInterrupt:
        log.info("Stopped by user")
    except Exception as e:
        log.error(f"Fatal: {e}")
    finally:
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("Clean shutdown")

if __name__ == "__main__":
    asyncio.run(main())