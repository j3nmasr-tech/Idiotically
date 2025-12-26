#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
6-STEP CONFLUENCE SCANNER
Clean minimal alerts with logic-based SL/TP - FIXED VERSION
"""

import os
import time
import asyncio
import logging
import datetime
import hashlib
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
from typing import Dict, List, Optional, Tuple, Any

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 60))
MIN_CONFIDENCE = 4  # Need at least 4 out of 5 steps (wave analysis excluded)

# Timeframes for MTF analysis
TIMEFRAMES = {
    "HTF": "4h",    # Primary Trend
    "MTF": "1h",    # Wave Analysis  
    "LTF": "15m"    # Entry & Indicators
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("6step_clean")
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
        log.warning(f"Telegram failed: {e}")

# ---------------- DATABASE ----------------
async def init_db():
    """Initialize database"""
    global db_conn
    try:
        # Remove old database
        if os.path.exists(DB_PATH):
            log.info(f"Removing old database")
            os.remove(DB_PATH)
        
        # Create directory
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        # Create fresh database
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Create table
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
                step1_result INTEGER DEFAULT 0,
                step2_direction TEXT DEFAULT 'NEUTRAL',
                step3_result INTEGER DEFAULT 0,
                step4_result INTEGER DEFAULT 0,
                step5_result INTEGER DEFAULT 0,
                step6_result INTEGER DEFAULT 0,
                confidence_score INTEGER DEFAULT 0,
                logic_breakdown TEXT,
                timeframe_combo TEXT,
                trend_direction TEXT,
                wave_pattern TEXT,
                rsi_value REAL,
                macd_hist REAL,
                volume_ratio REAL,
                strength_level TEXT,
                rr_ratio REAL,
                risk_pct REAL,
                reward_pct REAL,
                signal_hash TEXT UNIQUE
            )
        """)
        
        # Indexes
        await db_conn.execute("CREATE INDEX idx_symbol_status ON signals(symbol, status);")
        await db_conn.execute("CREATE INDEX idx_timestamp ON signals(timestamp);")
        
        await db_conn.commit()
        log.info("✅ Database created")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
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
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=150)
            if ohlcv and len(ohlcv) > 50:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                data[tf_type] = df
            else:
                return None
        except Exception as e:
            log.debug(f"Error fetching {tf}: {e}")
            return None
    return data

# ================ 6 STEPS - FIXED VERSION ================

def step1_mtf_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str]:
    """STEP 1: Multi-Timeframe Analysis - Simplified"""
    try:
        htf_df = data['HTF']
        mtf_df = data['MTF']
        ltf_df = data['LTF']
        
        htf_trend = "UP" if htf_df['close'].iloc[-1] > htf_df['close'].iloc[-50] else "DOWN"
        mtf_trend = "UP" if mtf_df['close'].iloc[-1] > mtf_df['close'].iloc[-20] else "DOWN"
        ltf_trend = "UP" if ltf_df['close'].iloc[-1] > ltf_df['close'].iloc[-10] else "DOWN"
        
        score = 0
        if htf_trend == mtf_trend: score += 1
        if mtf_trend == ltf_trend: score += 1
        if htf_trend == ltf_trend: score += 1
        
        trends = [htf_trend, mtf_trend, ltf_trend]
        dominant_trend = max(set(trends), key=trends.count)
        
        return (1 if score >= 2 else 0), dominant_trend
        
    except:
        return 0, "NEUTRAL"

def step2_wave_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[str, str]:
    """STEP 2: Wave Analysis - DIRECTION ONLY"""
    try:
        mtf_df = data['MTF']
        prices = mtf_df['close'].values[-30:]
        
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(prices)-2):
            if (prices[i] > prices[i-1] and prices[i] > prices[i-2] and
                prices[i] > prices[i+1] and prices[i] > prices[i+2]):
                swing_highs.append(prices[i])
            
            if (prices[i] < prices[i-1] and prices[i] < prices[i-2] and
                prices[i] < prices[i+1] and prices[i] < prices[i+2]):
                swing_lows.append(prices[i])
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            last_high_1 = swing_highs[-1]
            last_high_2 = swing_highs[-2]
            last_low_1 = swing_lows[-1]
            last_low_2 = swing_lows[-2]
            
            # Uptrend: Higher highs AND higher lows
            if last_high_1 > last_high_2 and last_low_1 > last_low_2:
                return "UP", "IMPULSE_UP"
            
            # Downtrend: Lower highs AND lower lows
            elif last_high_1 < last_high_2 and last_low_1 < last_low_2:
                return "DOWN", "IMPULSE_DOWN"
            
            # Correction/Consolidation
            elif (last_high_1 < last_high_2 and last_low_1 > last_low_2) or \
                 (last_high_1 > last_high_2 and last_low_1 < last_low_2):
                return "NEUTRAL", "CORRECTION"
        
        # Default to neutral if unclear
        return "NEUTRAL", "NO_CLEAR_PATTERN"
        
    except:
        return "NEUTRAL", "ERROR"

def step3_strength_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str]:
    """STEP 3: Strength Analysis - Simplified"""
    try:
        mtf_df = data['MTF']
        prices = mtf_df['close'].values[-20:]
        
        if len(prices) < 6:
            return 0, "WEAK_NEUTRAL"
        
        roc_5 = ((prices[-1] / prices[-6]) - 1) * 100
        
        if len(prices) >= 10:
            x = np.arange(len(prices[-10:]))
            y = prices[-10:]
            slope, _ = np.polyfit(x, y, 1)
            slope_pct = abs(slope / prices[-1]) * 100
        else:
            slope_pct = 0
        
        avg_candle_size = (mtf_df['high'].iloc[-10:] - mtf_df['low'].iloc[-10:]).mean()
        avg_candle_pct = (avg_candle_size / prices[-1]) * 100
        
        if abs(roc_5) > 2.0 and slope_pct > 0.1 and avg_candle_pct > 0.5:
            strength = "STRONG"
            passed = 1
        elif abs(roc_5) > 1.0 and slope_pct > 0.05 and avg_candle_pct > 0.3:
            strength = "MODERATE"
            passed = 1
        else:
            strength = "WEAK"
            passed = 0
        
        direction = "UP" if roc_5 > 0 else "DOWN"
        return passed, f"{strength}_{direction}"
        
    except:
        return 0, "WEAK_NEUTRAL"

def step4_indicators_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, float, float]:
    """STEP 4: Indicators - Simplified"""
    try:
        ltf_df = data['LTF']
        prices = ltf_df['close'].values
        
        # RSI
        if len(prices) >= 15:
            deltas = np.diff(prices[-15:])
            gains = deltas[deltas >= 0]
            losses = -deltas[deltas < 0]
            
            avg_gain = np.mean(gains) if len(gains) > 0 else 0
            avg_loss = np.mean(losses) if len(losses) > 0 else 0
            
            if avg_loss == 0:
                rsi = 100 if avg_gain > 0 else 0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 50
        
        # MACD Histogram
        if len(prices) >= 26:
            ema_12 = pd.Series(prices[-26:]).ewm(span=12, adjust=False).mean().iloc[-1]
            ema_26 = pd.Series(prices[-26:]).ewm(span=26, adjust=False).mean().iloc[-1]
            macd_line = ema_12 - ema_26
            macd_hist = pd.Series([macd_line]).ewm(span=9, adjust=False).mean().iloc[-1]
            histogram = macd_line - macd_hist
        else:
            histogram = 0
        
        # Signal
        rsi_signal = "BULLISH" if rsi < 35 else "BEARISH" if rsi > 65 else "NEUTRAL"
        macd_signal = "BULLISH" if histogram > 0 else "BEARISH" if histogram < 0 else "NEUTRAL"
        
        if rsi_signal == macd_signal and rsi_signal != "NEUTRAL":
            signal = rsi_signal
            passed = 1
        elif rsi_signal != "NEUTRAL" or macd_signal != "NEUTRAL":
            signal = rsi_signal if rsi_signal != "NEUTRAL" else macd_signal
            passed = 1
        else:
            signal = "NEUTRAL"
            passed = 0
        
        return passed, signal, rsi, histogram
        
    except:
        return 0, "NEUTRAL", 50, 0

def step5_volume_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str, float]:
    """STEP 5: Volume - Simplified"""
    try:
        ltf_df = data['LTF']
        
        recent_vol = ltf_df['volume'].values[-10:]
        prev_vol = ltf_df['volume'].values[-20:-10]
        
        if len(recent_vol) > 0 and len(prev_vol) > 0:
            avg_recent = np.mean(recent_vol)
            avg_prev = np.mean(prev_vol)
            volume_ratio = avg_recent / avg_prev if avg_prev > 0 else 1
        else:
            volume_ratio = 1
        
        # Volume confirmation
        price_change = ((ltf_df['close'].iloc[-1] / ltf_df['close'].iloc[-10]) - 1) * 100
        up_mask = ltf_df['close'] > ltf_df['open']
        down_mask = ltf_df['close'] < ltf_df['open']
        
        up_volume = ltf_df[up_mask]['volume'].mean() if up_mask.any() else 0
        down_volume = ltf_df[down_mask]['volume'].mean() if down_mask.any() else 0
        
        volume_confirmed = False
        if price_change > 0 and up_volume > down_volume:
            volume_confirmed = True
        elif price_change < 0 and down_volume > up_volume:
            volume_confirmed = True
        
        # Strength
        if volume_ratio > 1.5 and volume_confirmed:
            strength = "HIGH"
            passed = 1
        elif volume_ratio > 1.2 and volume_confirmed:
            strength = "MODERATE"
            passed = 1
        elif volume_ratio > 0.8 and volume_confirmed:
            strength = "LOW"
            passed = 1
        else:
            strength = "VERY_LOW"
            passed = 0
        
        return passed, strength, volume_ratio
        
    except:
        return 0, "VERY_LOW", 1

def step6_trend_identification(step1_trend: str, step2_direction: str,
                              step3_strength: str, step4_signal: str,
                              step5_strength: str) -> Tuple[int, str]:
    """STEP 6: Trend Identification - Uses wave direction"""
    try:
        trend_votes = []
        
        # Step 1 vote
        trend_votes.append(step1_trend)
        
        # Step 2 vote - USING WAVE DIRECTION
        if step2_direction in ["UP", "DOWN"]:
            trend_votes.append(step2_direction)
        
        # Step 3 vote
        if "UP" in step3_strength:
            trend_votes.append("UP")
        elif "DOWN" in step3_strength:
            trend_votes.append("DOWN")
        
        # Step 4 vote
        if step4_signal == "BULLISH":
            trend_votes.append("UP")
        elif step4_signal == "BEARISH":
            trend_votes.append("DOWN")
        
        # Step 5 vote (only if volume is good)
        if step5_strength in ["HIGH", "MODERATE"]:
            if "UP" in step3_strength:
                trend_votes.append("UP")
            elif "DOWN" in step3_strength:
                trend_votes.append("DOWN")
        
        # Count votes
        up_count = trend_votes.count("UP")
        down_count = trend_votes.count("DOWN")
        
        if up_count > down_count:
            return 1, "UP"
        elif down_count > up_count:
            return 1, "DOWN"
        else:
            return 0, "NEUTRAL"
        
    except:
        return 0, "NEUTRAL"

# ================ REALISTIC LOGIC-BASED SL/TP ================

def calculate_logic_based_sltp(current_price: float, side: str, data: Dict[str, pd.DataFrame],
                             step2_pattern: str, step3_strength: str, rsi_value: float,
                             volume_ratio: float, confidence_score: int) -> Tuple[float, float, str]:
    """
    Calculate REALISTIC SL and TP based on logic
    """
    logic_lines = []
    
    htf_df = data['HTF']
    mtf_df = data['MTF']
    
    # 1. SMART STOP-LOSS CALCULATION
    if side == "BUY":
        # Look for nearest support levels
        recent_lows = mtf_df['low'].values[-20:]
        support_levels = []
        
        # Find swing lows as potential support
        for i in range(2, len(recent_lows)-2):
            if (recent_lows[i] < recent_lows[i-1] and recent_lows[i] < recent_lows[i-2] and
                recent_lows[i] < recent_lows[i+1] and recent_lows[i] < recent_lows[i+2]):
                support_levels.append(recent_lows[i])
        
        # Use the most recent significant support
        if support_levels:
            # Take the lowest support (most conservative)
            base_sl = min(support_levels) * 0.99  # 1% buffer
            logic_lines.append(f"SL: Below recent swing low {min(support_levels):.1f}")
        else:
            # Fallback: Use ATR-based stop
            atr = (mtf_df['high'].iloc[-14:] - mtf_df['low'].iloc[-14:]).mean()
            base_sl = current_price - (atr * 1.5)
            logic_lines.append(f"SL: ATR-based {atr:.1f} x1.5")
    
    else:  # SELL
        # Look for nearest resistance levels
        recent_highs = mtf_df['high'].values[-20:]
        resistance_levels = []
        
        # Find swing highs as potential resistance
        for i in range(2, len(recent_highs)-2):
            if (recent_highs[i] > recent_highs[i-1] and recent_highs[i] > recent_highs[i-2] and
                recent_highs[i] > recent_highs[i+1] and recent_highs[i] > recent_highs[i+2]):
                resistance_levels.append(recent_highs[i])
        
        if resistance_levels:
            # Take the highest resistance (most conservative)
            base_sl = max(resistance_levels) * 1.01  # 1% buffer
            logic_lines.append(f"SL: Above recent swing high {max(resistance_levels):.1f}")
        else:
            atr = (mtf_df['high'].iloc[-14:] - mtf_df['low'].iloc[-14:]).mean()
            base_sl = current_price + (atr * 1.5)
            logic_lines.append(f"SL: ATR-based {atr:.1f} x1.5")
    
    # 2. REALISTIC TAKE-PROFIT CALCULATION
    
    # Base R:R ratio - be conservative
    base_rr_ratio = 1.5  # Max 1.5:1 (realistic for day trading)
    
    # Adjust based on confidence (but cap it!)
    if confidence_score == 5:
        rr_multiplier = 1.0  # 1.5:1 max for high confidence
    elif confidence_score >= 4:
        rr_multiplier = 0.9  # ~1.35:1 for medium confidence
    else:
        rr_multiplier = 0.7  # ~1.05:1 for low confidence
    
    # Volume adjustment (be conservative)
    volume_multiplier = 1.0
    if volume_ratio > 1.5:
        volume_multiplier = 1.05  # Only 5% increase
    elif volume_ratio < 0.8:
        volume_multiplier = 0.95  # Only 5% decrease
    
    # RSI adjustment (mean reversion aware)
    rsi_multiplier = 1.0
    if side == "BUY":
        if rsi_value < 30:  # Oversold - might mean weaker trend
            rsi_multiplier = 0.9
        elif rsi_value > 70:  # Overbought - might reverse soon
            rsi_multiplier = 0.8
    else:
        if rsi_value > 70:  # Overbought - might mean weaker downtrend
            rsi_multiplier = 0.9
        elif rsi_value < 30:  # Oversold - might bounce
            rsi_multiplier = 0.8
    
    # Calculate risk
    if side == "BUY":
        risk = current_price - base_sl
        # Realistic TP: Base R:R with capped multipliers
        tp_distance = risk * base_rr_ratio * rr_multiplier * volume_multiplier * rsi_multiplier
        
        # CAP THE TP DISTANCE TO PREVENT UNREALISTIC TARGETS
        # Max 5% move from entry for realistic trades
        max_tp_pct = 0.05  # 5% max
        max_tp_absolute = current_price * max_tp_pct
        tp_distance = min(tp_distance, max_tp_absolute)
        
        tp = current_price + tp_distance
        
    else:  # SELL
        risk = base_sl - current_price
        tp_distance = risk * base_rr_ratio * rr_multiplier * volume_multiplier * rsi_multiplier
        
        # Same cap for sells
        max_tp_pct = 0.05
        max_tp_absolute = current_price * max_tp_pct
        tp_distance = min(tp_distance, max_tp_absolute)
        
        tp = current_price - tp_distance
    
    # Calculate final R:R
    rr_ratio = tp_distance / risk if risk > 0 else 0
    
    logic_lines.append(f"TP: R:R {base_rr_ratio:.1f}:1 base | Confidence: {rr_multiplier:.1f}x")
    logic_lines.append(f"Volume: {volume_multiplier:.1f}x | RSI: {rsi_multiplier:.1f}x")
    logic_lines.append(f"Final R:R: {rr_ratio:.2f}:1 | Max TP: ±{max_tp_pct*100:.0f}%")
    
    return base_sl, tp, " | ".join(logic_lines)

# ================ MAIN ANALYSIS - FIXED ================

def analyze_symbol_with_6steps(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """Main analysis function - FIXED VERSION"""
    try:
        log.info(f"Analyzing {symbol}...")
        
        # Execute all 6 steps
        step1_passed, step1_trend = step1_mtf_analysis(data)
        step2_direction, step2_pattern = step2_wave_analysis(data)  # Direction only
        step3_passed, step3_strength = step3_strength_analysis(data)
        step4_passed, step4_signal, rsi_value, macd_hist = step4_indicators_analysis(data)
        step5_passed, step5_strength, volume_ratio = step5_volume_analysis(data)
        
        # Step 6 uses wave direction
        step6_passed, step6_trend = step6_trend_identification(
            step1_trend, step2_direction, step3_strength, step4_signal, step5_strength
        )
        
        # Calculate total - WAVE ANALYSIS (step2) NOT COUNTED in confluence score
        steps_passed = [step1_passed, step3_passed, step4_passed, step5_passed, step6_passed]  # 5 steps now
        total_passed = sum(steps_passed)
        
        log.info(f"Confluence: {total_passed}/5 for {symbol}")
        
        # Check confluence AND ensure wave direction agrees with trend
        if (total_passed >= MIN_CONFIDENCE and 
            step6_trend in ["UP", "DOWN"] and
            step2_direction in ["UP", "DOWN"] and
            step2_direction == step6_trend):  # Wave must confirm trend
            
            current_price = data['LTF']['close'].iloc[-1]
            side = "BUY" if step6_trend == "UP" else "SELL"
            
            # Calculate REALISTIC logic-based SL/TP
            sl, tp, sl_tp_logic = calculate_logic_based_sltp(
                current_price, side, data, step2_pattern, step3_strength,
                rsi_value, volume_ratio, total_passed
            )
            
            # Additional reality check: TP shouldn't be > 10% move typically
            if side == "BUY":
                tp_pct = (tp - current_price) / current_price * 100
            else:
                tp_pct = (current_price - tp) / current_price * 100
            
            # Skip if TP is unrealistic (>10% move) or risk is too small
            if tp_pct > 10:
                log.warning(f"Skipping {symbol}: TP target too high ({tp_pct:.1f}%)")
                return None
            
            # Risk metrics
            risk = abs(current_price - sl)
            reward = abs(tp - current_price)
            rr_ratio = reward / risk if risk > 0 else 0
            risk_pct = (risk / current_price) * 100
            reward_pct = (reward / current_price) * 100
            
            # Skip if risk is too small (<0.5%) - likely poor SL placement
            if risk_pct < 0.5:
                log.warning(f"Skipping {symbol}: Risk too small ({risk_pct:.1f}%)")
                return None
            
            # Skip if R:R is poor (<1:1)
            if rr_ratio < 1.0:
                log.warning(f"Skipping {symbol}: Poor R:R ({rr_ratio:.2f}:1)")
                return None
            
            # Create minimal logic breakdown
            logic_breakdown = f"""
Step 1 (MTF): {'✓' if step1_passed else '✗'} - {step1_trend}
Step 2 (Wave): Direction: {step2_direction} - Pattern: {step2_pattern}
Step 3 (Strength): {'✓' if step3_passed else '✗'} - {step3_strength}
Step 4 (Indicators): {'✓' if step4_passed else '✗'} - {step4_signal} (RSI:{rsi_value:.1f})
Step 5 (Volume): {'✓' if step5_passed else '✗'} - {step5_strength} ({volume_ratio:.1f}x)
Step 6 (Trend): {'✓' if step6_passed else '✗'} - {step6_trend}

SL/TP Logic: {sl_tp_logic}
"""
            
            # Create signal
            signal = {
                'symbol': symbol,
                'side': side,
                'entry': current_price,
                'sl': sl,
                'tp': tp,
                'status': 'OPEN',
                
                'step1_result': step1_passed,
                'step2_direction': step2_direction,
                'step3_result': step3_passed,
                'step4_result': step4_passed,
                'step5_result': step5_passed,
                'step6_result': step6_passed,
                'confidence_score': total_passed,
                
                'logic_breakdown': logic_breakdown.strip(),
                
                'timeframe_combo': f"{TIMEFRAMES['HTF']}|{TIMEFRAMES['MTF']}|{TIMEFRAMES['LTF']}",
                'trend_direction': step6_trend,
                'wave_pattern': step2_pattern,
                'rsi_value': rsi_value,
                'macd_hist': macd_hist,
                'volume_ratio': volume_ratio,
                'strength_level': step3_strength,
                
                'rr_ratio': rr_ratio,
                'risk_pct': risk_pct,
                'reward_pct': reward_pct,
                
                'signal_hash': hashlib.md5(
                    f"{symbol}:{side}:{current_price:.8f}:{total_passed}:{int(time.time())}".encode()
                ).hexdigest()
            }
            
            log.info(f"✅ Signal: {symbol} {side} at {current_price:.2f} | R:R {rr_ratio:.2f}:1")
            return signal
        
        log.info(f"❌ No signal: {symbol} - Confluence: {total_passed}/5, Wave: {step2_direction}, Trend: {step6_trend}")
        return None
        
    except Exception as e:
        log.error(f"Analysis error {symbol}: {e}")
        return None

# ---------------- DATABASE ----------------
async def save_signal(signal: Dict) -> bool:
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
                return False
            
            # Insert
            await db_conn.execute("""
                INSERT INTO signals (
                    symbol, side, entry, sl, tp, status,
                    step1_result, step2_direction, step3_result, step4_result,
                    step5_result, step6_result, confidence_score, logic_breakdown,
                    timeframe_combo, trend_direction, wave_pattern, rsi_value,
                    macd_hist, volume_ratio, strength_level, rr_ratio, risk_pct,
                    reward_pct, signal_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                signal['tp'], signal['status'], signal['step1_result'],
                signal['step2_direction'], signal['step3_result'], signal['step4_result'],
                signal['step5_result'], signal['step6_result'], signal['confidence_score'],
                signal['logic_breakdown'], signal['timeframe_combo'], signal['trend_direction'],
                signal['wave_pattern'], signal['rsi_value'], signal['macd_hist'],
                signal['volume_ratio'], signal['strength_level'], signal['rr_ratio'],
                signal['risk_pct'], signal['reward_pct'], signal['signal_hash']
            ))
            
            await db_conn.commit()
            return True
            
    except Exception as e:
        log.error(f"Save error: {e}")
        return False

# ---------------- CLEAN ALERTS ----------------
async def send_clean_alert(signal: Dict):
    """Send MINIMAL clean alert to Telegram"""
    try:
        # Create clean, minimal message
        message = f"""
🎯 **6-STEP CONFLUENCE SIGNAL** 🎯

**{signal['symbol']}** | **{signal['side']}**
Confluence: {signal['confidence_score']}/5 ✅

**TRADE SETUP:**
Entry: {signal['entry']:.2f}
SL: {signal['sl']:.2f} ({signal['risk_pct']:.1f}%)
TP: {signal['tp']:.2f} ({signal['reward_pct']:.1f}%)
R:R: {signal['rr_ratio']:.2f}:1

**KEY METRICS:**
Trend: {signal['trend_direction']}
Wave: {signal['wave_pattern']}
RSI: {signal['rsi_value']:.1f}
Volume: {signal['volume_ratio']:.1f}x
Strength: {signal['strength_level']}

**LOGIC-BASED SL/TP:**
{signal['logic_breakdown'].split('SL/TP Logic: ')[-1] if 'SL/TP Logic:' in signal['logic_breakdown'] else 'Based on 6-step analysis'}

#6StepMethod #{signal['side']}
"""
        
        await tg(message)
        log.info(f"Alert sent: {signal['symbol']}")
        
    except Exception as e:
        log.error(f"Alert error: {e}")

# ---------------- SCANNING LOOP ----------------
async def scan_loop(exchange):
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
                    # Fetch data
                    data = await fetch_multi_timeframe_data(exchange, symbol)
                    if not data:
                        continue
                    
                    # Analyze
                    signal = analyze_symbol_with_6steps(data, symbol)
                    
                    if signal:
                        # Save and alert
                        if await save_signal(signal):
                            await send_clean_alert(signal)
                            signals_found += 1
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    log.error(f"Error {symbol}: {e}")
                    continue
            
            log.info(f"Scan complete. Found {signals_found} signals.")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan error: {e}")
            await asyncio.sleep(30)

# ---------------- MONITORING LOOP - FIXED ----------------
async def monitor_loop(exchange):
    """Monitor TP/SL - FIXED VERSION"""
    while True:
        try:
            async with db_lock:
                # Fetch all open signals
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
                        
                        if side == "BUY":
                            if current_price >= tp:
                                await tg(f"✅ {symbol} TP HIT | Entry: {entry:.2f} → TP: {tp:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price <= sl:
                                await tg(f"❌ {symbol} SL HIT | Entry: {entry:.2f} → SL: {sl:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                        else:  # SELL
                            if current_price <= tp:
                                await tg(f"✅ {symbol} TP HIT | Entry: {entry:.2f} → TP: {tp:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price >= sl:
                                await tg(f"❌ {symbol} SL HIT | Entry: {entry:.2f} → SL: {sl:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                    
                    except Exception as e:
                        log.error(f"Monitor error {symbol}: {e}")
                        continue
                
                await db_conn.commit()
            
            await asyncio.sleep(30)
            
        except Exception as e:
            log.error(f"Monitor loop error: {e}")
            await asyncio.sleep(30)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "6-Step Confluence Scanner",
        "min_confidence": MIN_CONFIDENCE,
        "timeframes": TIMEFRAMES
    }

@app.get("/signals")
async def get_signals(limit: int = 10):
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT id, symbol, side, entry, sl, tp, confidence_score, 
                       timestamp, trend_direction, rr_ratio, risk_pct, reward_pct
                FROM signals WHERE status='OPEN' ORDER BY timestamp DESC LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
        
        signals = [dict(zip(columns, row)) for row in rows]
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        return {"error": str(e)}

@app.get("/stats")
async def get_stats():
    try:
        async with db_lock:
            # Total signals
            async with db_conn.execute("SELECT COUNT(*) FROM signals") as cursor:
                total = (await cursor.fetchone())[0]
            
            # Open signals
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status='OPEN'") as cursor:
                open_count = (await cursor.fetchone())[0]
            
            # Closed signals
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status='CLOSED'") as cursor:
                closed = (await cursor.fetchone())[0]
            
            # Average confidence
            async with db_conn.execute("SELECT AVG(confidence_score) FROM signals") as cursor:
                avg_conf = (await cursor.fetchone())[0]
            
            return {
                "total_signals": total,
                "open_signals": open_count,
                "closed_signals": closed,
                "average_confidence": round(avg_conf, 2) if avg_conf else 0
            }
    except Exception as e:
        return {"error": str(e)}

# ---------------- MAIN ----------------
async def main():
    global exchange
    
    log.info("="*50)
    log.info("🚀 6-STEP SCANNER STARTING - FIXED VERSION")
    log.info("="*50)
    
    try:
        # Initialize database
        if not await init_db():
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
🚀 6-STEP SCANNER STARTED - FIXED VERSION

Features:
• Wave analysis for direction only (not scored)
• Realistic TP targets (max 5% move)
• {MIN_CONFIDENCE}/5 confluence required
• Timeframes: 4h, 1h, 15m
• Smart SL based on support/resistance
• Fixed monitor loop

Ready to scan!
        """)
        
        # Start loops
        await asyncio.gather(
            scan_loop(exchange),
            monitor_loop(exchange)
        )
        
    except KeyboardInterrupt:
        log.info("Stopped by user")
        await tg("🛑 Scanner stopped by user")
    except Exception as e:
        log.error(f"Fatal: {e}")
        await tg(f"❌ Scanner crashed: {str(e)[:200]}")
    finally:
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("Clean shutdown")

if __name__ == "__main__":
    asyncio.run(main())