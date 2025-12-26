#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
YOUR 6-STEP CONFLUENCE SCANNER
1. Multi-Timeframe Analysis (MTF)
2. Wave Analysis
3. Strength/Momentum
4. Technical Indicators
5. Volume Analysis
6. Trend Identification

EXACTLY as you specified.
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
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 80))
MIN_CONFIDENCE = 1  # Need at least 4 out of 6 steps confirmed

# Timeframes for MTF analysis - your method requires 3 timeframes minimum
TIMEFRAMES = {
    "HTF": "4h",    # Higher Timeframe for Primary Trend
    "MTF": "1h",    # Medium Timeframe for Wave Analysis
    "LTF": "15m"    # Lower Timeframe for Entry
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("6step_scanner")
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

# ---------------- DATABASE ----------------
async def init_db():
    """Initialize database for your 6-step method"""
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Create table specifically for your 6-step method
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp REAL NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                
                -- Your 6 Steps Tracking --
                step1_mtf INTEGER DEFAULT 0,
                step2_wave INTEGER DEFAULT 0,
                step3_strength INTEGER DEFAULT 0,
                step4_indicators INTEGER DEFAULT 0,
                step5_volume INTEGER DEFAULT 0,
                step6_trend INTEGER DEFAULT 0,
                
                -- Additional Info --
                confidence_score INTEGER DEFAULT 0,
                timeframe_combo TEXT,
                wave_pattern TEXT,
                trend_direction TEXT,
                rsi_value REAL,
                macd_signal TEXT,
                volume_ratio REAL,
                
                -- Trade Management --
                atr_value REAL,
                rr_ratio REAL,
                risk_pct REAL,
                reward_pct REAL,
                
                signal_hash TEXT UNIQUE
            )
        """)
        
        # Create indexes
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON signals(symbol);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_confidence ON signals(confidence_score);")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_hash ON signals(signal_hash);")
        
        await db_conn.commit()
        log.info("✅ Database initialized for 6-step method")
        return True
    except Exception as e:
        log.error(f"Database initialization error: {e}")
        return False

# ---------------- OHLCV FETCH ----------------
async def fetch_multi_timeframe_data(exchange, symbol: str) -> Dict[str, pd.DataFrame]:
    """Fetch data for all 3 timeframes required by your method"""
    data = {}
    
    for tf_type, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=200)
            if ohlcv and len(ohlcv) > 50:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                data[tf_type] = df
                log.debug(f"✓ Fetched {tf} data for {symbol}")
            else:
                log.warning(f"Failed to fetch {tf} data for {symbol}")
                return None
        except Exception as e:
            log.warning(f"Error fetching {tf} for {symbol}: {e}")
            return None
    
    return data

# ================ YOUR 6-STEP ANALYSIS ================

def step1_mtf_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, Dict]:
    """
    STEP 1: Multi-Timeframe Analysis
    أراقب كل الفريمات - Watch all timeframes
    """
    try:
        htf_df = data['HTF']  # 4h
        mtf_df = data['MTF']  # 1h
        ltf_df = data['LTF']  # 15m
        
        # Check trend consistency across timeframes
        htf_trend = "UP" if htf_df['close'].iloc[-1] > htf_df['close'].iloc[-50] else "DOWN"
        mtf_trend = "UP" if mtf_df['close'].iloc[-1] > mtf_df['close'].iloc[-20] else "DOWN"
        ltf_trend = "UP" if ltf_df['close'].iloc[-1] > ltf_df['close'].iloc[-10] else "DOWN"
        
        # Score based on alignment (3 points max)
        score = 0
        if htf_trend == mtf_trend: score += 1
        if mtf_trend == ltf_trend: score += 1
        if htf_trend == ltf_trend: score += 1
        
        # Determine dominant trend
        trends = [htf_trend, mtf_trend, ltf_trend]
        dominant_trend = max(set(trends), key=trends.count)
        
        details = {
            "htf_trend": htf_trend,
            "mtf_trend": mtf_trend,
            "ltf_trend": ltf_trend,
            "dominant_trend": dominant_trend,
            "alignment": f"{score}/3 timeframes aligned"
        }
        
        # Must have at least 2 timeframes aligned to pass
        passed = 1 if score >= 2 else 0
        
        return passed, dominant_trend, details
    except Exception as e:
        log.error(f"Step 1 error: {e}")
        return 0, "NEUTRAL", {"error": str(e)}

def step2_wave_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, Dict]:
    """
    STEP 2: Wave Analysis
    المدى الموجي - Wave range analysis
    """
    try:
        mtf_df = data['MTF']  # Use 1h for wave analysis
        
        # Simple wave/price structure detection
        prices = mtf_df['close'].values[-50:]
        
        # Find swing highs and lows
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(prices)-2):
            # Swing high
            if prices[i] > prices[i-1] and prices[i] > prices[i-2] and \
               prices[i] > prices[i+1] and prices[i] > prices[i+2]:
                swing_highs.append(prices[i])
            
            # Swing low
            if prices[i] < prices[i-1] and prices[i] < prices[i-2] and \
               prices[i] < prices[i+1] and prices[i] < prices[i+2]:
                swing_lows.append(prices[i])
        
        # Analyze wave pattern
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            # Check for higher highs/lows (uptrend pattern)
            if swing_highs[-1] > swing_highs[-2] and swing_lows[-1] > swing_lows[-2]:
                pattern = "IMPULSE_UP"
                score = 1
            # Check for lower highs/lows (downtrend pattern)
            elif swing_highs[-1] < swing_highs[-2] and swing_lows[-1] < swing_lows[-2]:
                pattern = "IMPULSE_DOWN"
                score = 1
            # Correction pattern
            elif (swing_highs[-1] < swing_highs[-2] and swing_lows[-1] > swing_lows[-2]) or \
                 (swing_highs[-1] > swing_highs[-2] and swing_lows[-1] < swing_lows[-2]):
                pattern = "CORRECTION"
                score = 1
            else:
                pattern = "CONSOLIDATION"
                score = 0
        else:
            pattern = "NO_CLEAR_WAVE"
            score = 0
        
        details = {
            "pattern": pattern,
            "swing_highs_count": len(swing_highs),
            "swing_lows_count": len(swing_lows),
            "current_price": float(mtf_df['close'].iloc[-1])
        }
        
        return score, pattern, details
    except Exception as e:
        log.error(f"Step 2 error: {e}")
        return 0, "UNKNOWN", {"error": str(e)}

def step3_strength_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, Dict]:
    """
    STEP 3: Strength/Momentum Analysis
    القوة - Strength measurement
    """
    try:
        mtf_df = data['MTF']  # Use 1h for strength analysis
        
        # Calculate momentum using rate of change and price slope
        prices = mtf_df['close'].values[-20:]
        
        if len(prices) < 10:
            return 0, "WEAK", {"error": "Insufficient data"}
        
        # Rate of Change (5-period)
        roc_5 = ((prices[-1] / prices[-6]) - 1) * 100
        
        # Price slope (linear regression on last 10 periods)
        x = np.arange(len(prices[-10:]))
        y = prices[-10:]
        slope, _ = np.polyfit(x, y, 1)
        slope_pct = (slope / prices[-1]) * 100
        
        # Average candle size (strength of moves)
        avg_candle_size = (mtf_df['high'].iloc[-10:] - mtf_df['low'].iloc[-10:]).mean()
        avg_candle_pct = (avg_candle_size / prices[-1]) * 100
        
        # Determine strength
        if abs(roc_5) > 2.0 and abs(slope_pct) > 0.1 and avg_candle_pct > 0.5:
            strength = "STRONG"
            score = 1
        elif abs(roc_5) > 1.0 and abs(slope_pct) > 0.05 and avg_candle_pct > 0.3:
            strength = "MODERATE"
            score = 1
        else:
            strength = "WEAK"
            score = 0
        
        direction = "UP" if roc_5 > 0 else "DOWN"
        
        details = {
            "roc_5": float(roc_5),
            "slope_pct": float(slope_pct),
            "avg_candle_pct": float(avg_candle_pct),
            "direction": direction,
            "strength_level": strength
        }
        
        return score, strength, details
    except Exception as e:
        log.error(f"Step 3 error: {e}")
        return 0, "WEAK", {"error": str(e)}

def step4_indicators_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, Dict]:
    """
    STEP 4: Technical Indicators
    المؤشرات - RSI, MACD, etc.
    """
    try:
        ltf_df = data['LTF']  # Use 15m for indicator analysis
        
        # Calculate RSI
        def calculate_rsi(prices, period=14):
            deltas = np.diff(prices)
            seed = deltas[:period+1]
            up = seed[seed >= 0].sum()/period
            down = -seed[seed < 0].sum()/period
            rs = up/down if down != 0 else 0
            rsi = 100 - 100/(1 + rs)
            
            for i in range(period, len(prices)):
                delta = deltas[i-1]
                if delta > 0:
                    upval = delta
                    downval = 0
                else:
                    upval = 0
                    downval = -delta
                
                up = (up*(period-1) + upval)/period
                down = (down*(period-1) + downval)/period
                rs = up/down if down != 0 else 0
                rsi = np.append(rsi, 100 - 100/(1 + rs))
            
            return rsi
        
        prices = ltf_df['close'].values
        if len(prices) > 14:
            rsi_values = calculate_rsi(prices)
            current_rsi = rsi_values[-1] if len(rsi_values) > 0 else 50
        else:
            current_rsi = 50
        
        # Simple MACD calculation
        def calculate_macd(prices, fast=12, slow=26, signal=9):
            exp1 = pd.Series(prices).ewm(span=fast, adjust=False).mean()
            exp2 = pd.Series(prices).ewm(span=slow, adjust=False).mean()
            macd = exp1 - exp2
            signal_line = macd.ewm(span=signal, adjust=False).mean()
            histogram = macd - signal_line
            return macd.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]
        
        macd_val, signal_val, hist_val = calculate_macd(prices[-50:])
        
        # Determine signal from indicators
        rsi_signal = "BULLISH" if current_rsi < 30 else "BEARISH" if current_rsi > 70 else "NEUTRAL"
        macd_signal = "BULLISH" if macd_val > signal_val and hist_val > 0 else "BEARISH" if macd_val < signal_val and hist_val < 0 else "NEUTRAL"
        
        # Score if both indicators agree
        if rsi_signal == macd_signal and rsi_signal != "NEUTRAL":
            score = 1
            signal = rsi_signal
        elif (rsi_signal == "BULLISH" or macd_signal == "BULLISH") and not (rsi_signal == "BEARISH" or macd_signal == "BEARISH"):
            score = 1
            signal = "BULLISH"
        elif (rsi_signal == "BEARISH" or macd_signal == "BEARISH") and not (rsi_signal == "BULLISH" or macd_signal == "BULLISH"):
            score = 1
            signal = "BEARISH"
        else:
            score = 0
            signal = "NEUTRAL"
        
        details = {
            "rsi": float(current_rsi),
            "rsi_signal": rsi_signal,
            "macd": float(macd_val),
            "macd_signal": macd_signal,
            "histogram": float(hist_val),
            "final_signal": signal
        }
        
        return score, signal, details
    except Exception as e:
        log.error(f"Step 4 error: {e}")
        return 0, "NEUTRAL", {"error": str(e)}

def step5_volume_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, Dict]:
    """
    STEP 5: Volume Analysis
    Volume - Confirmation of moves
    """
    try:
        ltf_df = data['LTF']  # Use 15m for volume analysis
        
        # Calculate volume metrics
        recent_volume = ltf_df['volume'].values[-10:]
        prev_volume = ltf_df['volume'].values[-20:-10]
        
        if len(recent_volume) == 0 or len(prev_volume) == 0:
            return 0, "LOW", {"error": "Insufficient volume data"}
        
        avg_recent_volume = np.mean(recent_volume)
        avg_prev_volume = np.mean(prev_volume)
        
        volume_ratio = avg_recent_volume / avg_prev_volume if avg_prev_volume > 0 else 1
        
        # Check volume on up vs down candles
        up_candle_volume = ltf_df[ltf_df['close'] > ltf_df['open']]['volume'].mean()
        down_candle_volume = ltf_df[ltf_df['close'] < ltf_df['open']]['volume'].mean()
        
        # Volume confirmation score
        if volume_ratio > 1.5:
            volume_strength = "VERY_HIGH"
            score = 1
        elif volume_ratio > 1.2:
            volume_strength = "HIGH"
            score = 1
        elif volume_ratio > 0.8:
            volume_strength = "MODERATE"
            score = 1
        else:
            volume_strength = "LOW"
            score = 0
        
        # Check if volume supports the trend
        price_change = ((ltf_df['close'].iloc[-1] / ltf_df['close'].iloc[-10]) - 1) * 100
        volume_confirmation = "CONFIRMED" if (price_change > 0 and up_candle_volume > down_candle_volume) or \
                                             (price_change < 0 and down_candle_volume > up_candle_volume) else "DIVERGING"
        
        if volume_confirmation == "DIVERGING":
            score = 0
        
        details = {
            "volume_ratio": float(volume_ratio),
            "volume_strength": volume_strength,
            "up_volume": float(up_candle_volume) if not np.isnan(up_candle_volume) else 0,
            "down_volume": float(down_candle_volume) if not np.isnan(down_candle_volume) else 0,
            "confirmation": volume_confirmation,
            "price_change_pct": float(price_change)
        }
        
        return score, volume_strength, details
    except Exception as e:
        log.error(f"Step 5 error: {e}")
        return 0, "LOW", {"error": str(e)}

def step6_trend_identification(data: Dict[str, pd.DataFrame], wave_pattern: str, 
                              strength_signal: str, indicator_signal: str) -> Tuple[int, str, Dict]:
    """
    STEP 6: Trend Identification
    أحدد الاتجاه - Determine direction
    """
    try:
        htf_df = data['HTF']
        
        # Multiple trend confirmation methods
        trend_signals = []
        
        # 1. Simple moving average trend
        sma_50 = htf_df['close'].rolling(window=50).mean().iloc[-1]
        sma_20 = htf_df['close'].rolling(window=20).mean().iloc[-1]
        current_price = htf_df['close'].iloc[-1]
        
        if current_price > sma_20 > sma_50:
            trend_signals.append("UP")
        elif current_price < sma_20 < sma_50:
            trend_signals.append("DOWN")
        
        # 2. Higher highs/lows for uptrend, lower highs/lows for downtrend
        highs = htf_df['high'].values[-20:]
        lows = htf_df['low'].values[-20:]
        
        if len(highs) >= 5 and len(lows) >= 5:
            if highs[-1] > highs[-5] and lows[-1] > lows[-5]:
                trend_signals.append("UP")
            elif highs[-1] < highs[-5] and lows[-1] < lows[-5]:
                trend_signals.append("DOWN")
        
        # 3. Wave pattern confirmation
        if wave_pattern == "IMPULSE_UP":
            trend_signals.append("UP")
        elif wave_pattern == "IMPULSE_DOWN":
            trend_signals.append("DOWN")
        
        # 4. Strength confirmation
        if strength_signal == "STRONG":
            # Check if strong move is in trend direction
            if "UP" in trend_signals and len(trend_signals) > 0:
                trend_signals.append("UP")
            elif "DOWN" in trend_signals and len(trend_signals) > 0:
                trend_signals.append("DOWN")
        
        # 5. Indicator confirmation
        if indicator_signal == "BULLISH":
            trend_signals.append("UP")
        elif indicator_signal == "BEARISH":
            trend_signals.append("DOWN")
        
        # Determine final trend
        if trend_signals:
            up_count = trend_signals.count("UP")
            down_count = trend_signals.count("DOWN")
            
            if up_count > down_count:
                final_trend = "UP"
                score = 1
            elif down_count > up_count:
                final_trend = "DOWN"
                score = 1
            else:
                final_trend = "NEUTRAL"
                score = 0
        else:
            final_trend = "NEUTRAL"
            score = 0
        
        details = {
            "sma_trend": "UP" if current_price > sma_20 > sma_50 else "DOWN" if current_price < sma_20 < sma_50 else "NEUTRAL",
            "price_structure": "UP" if (highs[-1] > highs[-5] and lows[-1] > lows[-5]) else "DOWN" if (highs[-1] < highs[-5] and lows[-1] < lows[-5]) else "NEUTRAL",
            "wave_confirmation": wave_pattern,
            "signal_count": len(trend_signals),
            "up_count": up_count if 'up_count' in locals() else 0,
            "down_count": down_count if 'down_count' in locals() else 0,
            "final_trend": final_trend
        }
        
        return score, final_trend, details
    except Exception as e:
        log.error(f"Step 6 error: {e}")
        return 0, "NEUTRAL", {"error": str(e)}

def execute_6step_analysis(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """
    Execute ALL 6 steps of your method and generate signal if confluence exists
    """
    try:
        log.info(f"Analyzing {symbol} with 6-step method...")
        
        # Step 1: Multi-Timeframe Analysis
        step1_passed, step1_trend, step1_details = step1_mtf_analysis(data)
        
        # Step 2: Wave Analysis
        step2_passed, step2_pattern, step2_details = step2_wave_analysis(data)
        
        # Step 3: Strength Analysis
        step3_passed, step3_strength, step3_details = step3_strength_analysis(data)
        
        # Step 4: Indicators Analysis
        step4_passed, step4_signal, step4_details = step4_indicators_analysis(data)
        
        # Step 5: Volume Analysis
        step5_passed, step5_volume, step5_details = step5_volume_analysis(data)
        
        # Step 6: Trend Identification (uses outputs from previous steps)
        step6_passed, step6_trend, step6_details = step6_trend_identification(
            data, step2_pattern, step3_strength, step4_signal
        )
        
        # Calculate total confidence (how many steps passed)
        steps_passed = [
            step1_passed, step2_passed, step3_passed, 
            step4_passed, step5_passed, step6_passed
        ]
        
        total_passed = sum(steps_passed)
        
        log.info(f"6-Step Results for {symbol}:")
        log.info(f"  Step 1 (MTF): {'✓' if step1_passed else '✗'} - {step1_trend}")
        log.info(f"  Step 2 (Wave): {'✓' if step2_passed else '✗'} - {step2_pattern}")
        log.info(f"  Step 3 (Strength): {'✓' if step3_passed else '✗'} - {step3_strength}")
        log.info(f"  Step 4 (Indicators): {'✓' if step4_passed else '✗'} - {step4_signal}")
        log.info(f"  Step 5 (Volume): {'✓' if step5_passed else '✗'} - {step5_volume}")
        log.info(f"  Step 6 (Trend): {'✓' if step6_passed else '✗'} - {step6_trend}")
        log.info(f"  Total: {total_passed}/6 passed")
        
        # Check if we have minimum confluence
        if total_passed >= MIN_CONFIDENCE:
            # Determine trade direction based on trend
            if step6_trend == "UP":
                side = "BUY"
                current_price = data['LTF']['close'].iloc[-1]
                sl = current_price * 0.98  # 2% stop loss
                tp = current_price * 1.04  # 4% take profit
            elif step6_trend == "DOWN":
                side = "SELL"
                current_price = data['LTF']['close'].iloc[-1]
                sl = current_price * 1.02  # 2% stop loss
                tp = current_price * 0.96  # 4% take profit
            else:
                return None  # No clear trend
            
            # Calculate ATR for risk management
            def calculate_atr(df, period=14):
                high = df['high']
                low = df['low']
                close = df['close']
                
                tr1 = high - low
                tr2 = abs(high - close.shift())
                tr3 = abs(low - close.shift())
                
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr = tr.rolling(window=period).mean()
                return atr.iloc[-1] if len(atr) > 0 else 0
            
            atr_value = calculate_atr(data['LTF'])
            
            # Calculate R:R ratio
            risk = abs(current_price - sl)
            reward = abs(tp - current_price)
            rr_ratio = reward / risk if risk > 0 else 0
            
            # Create signal
            signal = {
                'symbol': symbol,
                'side': side,
                'entry': current_price,
                'sl': sl,
                'tp': tp,
                'status': 'OPEN',
                
                # Your 6 Steps
                'step1_mtf': step1_passed,
                'step2_wave': step2_passed,
                'step3_strength': step3_passed,
                'step4_indicators': step4_passed,
                'step5_volume': step5_passed,
                'step6_trend': step6_passed,
                
                # Details
                'confidence_score': total_passed,
                'timeframe_combo': f"{TIMEFRAMES['HTF']}|{TIMEFRAMES['MTF']}|{TIMEFRAMES['LTF']}",
                'wave_pattern': step2_pattern,
                'trend_direction': step6_trend,
                'rsi_value': step4_details.get('rsi', 50),
                'macd_signal': step4_details.get('final_signal', 'NEUTRAL'),
                'volume_ratio': step5_details.get('volume_ratio', 1),
                
                # Trade metrics
                'atr_value': atr_value,
                'rr_ratio': rr_ratio,
                'risk_pct': (risk / current_price) * 100,
                'reward_pct': (reward / current_price) * 100,
                
                # Signal hash for duplicate prevention
                'signal_hash': hashlib.md5(
                    f"{symbol}:{side}:{current_price:.6f}:{step6_trend}:{total_passed}".encode()
                ).hexdigest()
            }
            
            log.info(f"✓ Strong confluence found for {symbol}: {side} at {current_price:.6f}")
            return signal
        else:
            log.info(f"✗ Insufficient confluence for {symbol}: {total_passed}/6")
            return None
            
    except Exception as e:
        log.error(f"6-step analysis error for {symbol}: {e}")
        return None

# ---------------- SIGNAL MANAGEMENT ----------------
async def log_signal_to_db(signal: Dict):
    """Log signal to database"""
    try:
        async with db_lock:
            # Check for duplicate using hash
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals WHERE signal_hash=?
            """, (signal['signal_hash'],)) as cursor:
                count = (await cursor.fetchone())[0]
            
            if count > 0:
                log.info(f"⏭️ Duplicate signal found: {signal['symbol']}")
                return False
            
            # Insert new signal
            await db_conn.execute("""
                INSERT INTO signals (
                    symbol, side, entry, sl, tp, status,
                    step1_mtf, step2_wave, step3_strength, step4_indicators, 
                    step5_volume, step6_trend, confidence_score, timeframe_combo,
                    wave_pattern, trend_direction, rsi_value, macd_signal, volume_ratio,
                    atr_value, rr_ratio, risk_pct, reward_pct, signal_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'],
                signal['side'],
                signal['entry'],
                signal['sl'],
                signal['tp'],
                signal['status'],
                signal['step1_mtf'],
                signal['step2_wave'],
                signal['step3_strength'],
                signal['step4_indicators'],
                signal['step5_volume'],
                signal['step6_trend'],
                signal['confidence_score'],
                signal['timeframe_combo'],
                signal['wave_pattern'],
                signal['trend_direction'],
                signal['rsi_value'],
                signal['macd_signal'],
                signal['volume_ratio'],
                signal['atr_value'],
                signal['rr_ratio'],
                signal['risk_pct'],
                signal['reward_pct'],
                signal['signal_hash']
            ))
            
            await db_conn.commit()
            log.info(f"✅ Signal logged to DB: {signal['symbol']} {signal['side']}")
            return True
            
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

async def send_signal_alert(signal: Dict):
    """Send formatted alert to Telegram"""
    try:
        message = f"""
🎯 **YOUR 6-STEP METHOD SIGNAL** 🎯

{signal['symbol']} | {signal['side']}

**Entry:** {signal['entry']:.6f}
**SL:** {signal['sl']:.6f} ({signal['risk_pct']:.1f}%)
**TP:** {signal['tp']:.6f} ({signal['reward_pct']:.1f}%)
**R:R:** {signal['rr_ratio']:.2f}:1

**Confluence Score:** {signal['confidence_score']}/6
**Trend:** {signal['trend_direction']}
**Wave Pattern:** {signal['wave_pattern']}
**RSI:** {signal['rsi_value']:.1f}
**Volume Ratio:** {signal['volume_ratio']:.2f}

**Timeframes:** {signal['timeframe_combo']}
**ATR:** {signal['atr_value']:.6f}

**Steps Confirmed:**
1️⃣ MTF: {'✓' if signal['step1_mtf'] else '✗'}
2️⃣ Wave: {'✓' if signal['step2_wave'] else '✗'}
3️⃣ Strength: {'✓' if signal['step3_strength'] else '✗'}
4️⃣ Indicators: {'✓' if signal['step4_indicators'] else '✗'}
5️⃣ Volume: {'✓' if signal['step5_volume'] else '✗'}
6️⃣ Trend: {'✓' if signal['step6_trend'] else '✗'}

#6StepMethod #{signal['side']}
"""
        await tg(message)
        log.info(f"📢 Alert sent for {signal['symbol']}")
    except Exception as e:
        log.error(f"Alert error: {e}")

# ---------------- SCANNING LOOP ----------------
async def scan_symbols(exchange):
    """Main scanning loop using your 6-step method"""
    while True:
        try:
            log.info("=" * 50)
            log.info("Starting 6-step method scan...")
            
            # Get top USDT pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol in tickers:
                if symbol.endswith("/USDT"):
                    volume = tickers[symbol].get("quoteVolume", 0)
                    if volume > 0:
                        usdt_pairs.append((symbol, volume))
            
            # Take top N pairs
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                try:
                    log.info(f"Analyzing {symbol}...")
                    
                    # Fetch multi-timeframe data
                    data = await fetch_multi_timeframe_data(exchange, symbol)
                    
                    if not data:
                        log.warning(f"Could not fetch data for {symbol}")
                        continue
                    
                    # Execute your 6-step analysis
                    signal = execute_6step_analysis(data, symbol)
                    
                    if signal:
                        # Log to database
                        if await log_signal_to_db(signal):
                            # Send alert
                            await send_signal_alert(signal)
                            signals_found += 1
                    
                    # Small delay between symbols
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    log.error(f"Error analyzing {symbol}: {e}")
                    continue
            
            log.info(f"Scan complete. Found {signals_found} signals with confluence.")
            log.info(f"Next scan in {SCAN_INTERVAL} seconds...")
            log.info("=" * 50)
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
        
        # Wait for next scan
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SIGNAL MONITORING ----------------
async def monitor_open_signals(exchange):
    """Monitor open signals for TP/SL hits"""
    while True:
        try:
            async with db_lock:
                # Get open signals
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp FROM signals 
                    WHERE status='OPEN'
                """) as cursor:
                    signals = await cursor.fetchall()
                
                for sig in signals:
                    sig_id, symbol, side, entry, sl, tp = sig
                    
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
                                await tg(f"✅ TP HIT: {symbol}\nEntry: {entry:.6f}\nTP: {tp:.6f}\nCurrent: {current_price:.6f}")
                            elif current_price <= sl:
                                sl_hit = True
                                await tg(f"❌ SL HIT: {symbol}\nEntry: {entry:.6f}\nSL: {sl:.6f}\nCurrent: {current_price:.6f}")
                        else:  # SELL
                            if current_price <= tp:
                                tp_hit = True
                                await tg(f"✅ TP HIT: {symbol}\nEntry: {entry:.6f}\nTP: {tp:.6f}\nCurrent: {current_price:.6f}")
                            elif current_price >= sl:
                                sl_hit = True
                                await tg(f"❌ SL HIT: {symbol}\nEntry: {entry:.6f}\nSL: {sl:.6f}\nCurrent: {current_price:.6f}")
                        
                        if tp_hit or sl_hit:
                            await db_conn.execute("""
                                UPDATE signals SET status='CLOSED' WHERE id=?
                            """, (sig_id,))
                            await db_conn.commit()
                            
                    except Exception as e:
                        log.error(f"Error monitoring {symbol}: {e}")
                        continue
            
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(30)  # Check every 30 seconds

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "running",
        "method": "Your 6-Step Confluence Method",
        "steps": [
            "1. Multi-Timeframe Analysis",
            "2. Wave Analysis",
            "3. Strength/Momentum",
            "4. Technical Indicators",
            "5. Volume Analysis",
            "6. Trend Identification"
        ],
        "min_confidence": f"{MIN_CONFIDENCE}/6",
        "timeframes": TIMEFRAMES
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.now().isoformat()}

@app.get("/signals")
async def get_signals(limit: int = 20, status: str = "OPEN", min_confidence: int = 4):
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT * FROM signals 
                WHERE status=? AND confidence_score>=?
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (status, min_confidence, limit)) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
        
        signals = [dict(zip(columns, row)) for row in rows]
        return {
            "count": len(signals),
            "min_confidence": min_confidence,
            "signals": signals
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/analysis/{symbol}")
async def analyze_symbol(symbol: str):
    """Manual analysis of a specific symbol"""
    try:
        # Add USDT if not present
        if not symbol.endswith("/USDT"):
            symbol = f"{symbol}/USDT"
        
        # Initialize exchange if not already
        if not exchange:
            return {"error": "Exchange not initialized"}
        
        # Fetch data
        data = await fetch_multi_timeframe_data(exchange, symbol)
        
        if not data:
            return {"error": "Could not fetch data"}
        
        # Run analysis
        signal = execute_6step_analysis(data, symbol)
        
        if signal:
            # Remove database-specific fields
            for key in ['status', 'signal_hash']:
                signal.pop(key, None)
            
            return {
                "symbol": symbol,
                "signal": signal,
                "confluence_score": signal['confidence_score']
            }
        else:
            return {
                "symbol": symbol,
                "signal": None,
                "message": "No sufficient confluence found"
            }
            
    except Exception as e:
        return {"error": str(e)}

# ---------------- MAIN ----------------
async def main():
    global exchange
    
    log.info("=" * 60)
    log.info("🚀 STARTING YOUR 6-STEP CONFLUENCE SCANNER 🚀")
    log.info("=" * 60)
    
    # Display method details
    log.info("📊 YOUR METHOD:")
    log.info("1. أراقب كل الفريمات - Multi-Timeframe Analysis")
    log.info("2. المدى الموجي - Wave Analysis")
    log.info("3. القوة - Strength/Momentum")
    log.info("4. المؤشرات - Technical Indicators (RSI, MACD)")
    log.info("5. Volume - Volume Analysis")
    log.info("6. أحدد الاتجاه - Trend Identification")
    log.info(f"Minimum Confluence: {MIN_CONFIDENCE}/6 steps required")
    log.info(f"Timeframes: HTF={TIMEFRAMES['HTF']}, MTF={TIMEFRAMES['MTF']}, LTF={TIMEFRAMES['LTF']}")
    log.info("=" * 60)
    
    try:
        # Initialize database
        if not await init_db():
            log.error("❌ Failed to initialize database")
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
🚀 YOUR 6-STEP SCANNER IS RUNNING

Method: Your exact 6-step confluence approach
Steps: MTF → Wave → Strength → Indicators → Volume → Trend
Confidence: {MIN_CONFIDENCE}/6 minimum required

Timeframes:
• HTF ({TIMEFRAMES['HTF']}): Primary Trend
• MTF ({TIMEFRAMES['MTF']}): Wave Analysis  
• LTF ({TIMEFRAMES['LTF']}): Entry Timing

Scanner will only alert when ALL steps align with confluence.
        """)
        
        # Start scanning and monitoring
        await asyncio.gather(
            scan_symbols(exchange),
            monitor_open_signals(exchange)
        )
        
    except KeyboardInterrupt:
        log.info("👋 Stopped by user")
    except Exception as e:
        log.error(f"💥 Fatal error: {e}")
    finally:
        # Cleanup
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("✅ Scanner stopped cleanly")

if __name__ == "__main__":
    # Run scanner
    asyncio.run(main())