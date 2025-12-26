#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
6-STEP CONFLUENCE SCANNER
Clean minimal alerts with logic-based SL/TP
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
MIN_CONFIDENCE = 6  # Need at least 4 out of 6 steps

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
                step2_result INTEGER DEFAULT 0,
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

# ================ 6 STEPS - SIMPLIFIED ================

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

def step2_wave_analysis(data: Dict[str, pd.DataFrame]) -> Tuple[int, str]:
    """STEP 2: Wave Analysis - Simplified"""
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
            
            if last_high_1 > last_high_2 and last_low_1 > last_low_2:
                return 1, "IMPULSE_UP"
            elif last_high_1 < last_high_2 and last_low_1 < last_low_2:
                return 1, "IMPULSE_DOWN"
            elif (last_high_1 < last_high_2 and last_low_1 > last_low_2) or \
                 (last_high_1 > last_high_2 and last_low_1 < last_low_2):
                return 1, "CORRECTION"
            else:
                return 0, "CONSOLIDATION"
        
        return 0, "NO_PATTERN"
        
    except:
        return 0, "ERROR"

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

def step6_trend_identification(step1_trend: str, step2_pattern: str,
                              step3_strength: str, step4_signal: str,
                              step5_strength: str) -> Tuple[int, str]:
    """STEP 6: Trend Identification - Simplified"""
    try:
        trend_votes = []
        
        # Step 1 vote
        trend_votes.append(step1_trend)
        
        # Step 2 vote
        if "UP" in step2_pattern:
            trend_votes.append("UP")
        elif "DOWN" in step2_pattern:
            trend_votes.append("DOWN")
        
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

# ================ LOGIC-BASED SL/TP ================

def calculate_logic_based_sltp(current_price: float, side: str, data: Dict[str, pd.DataFrame],
                             step2_pattern: str, step3_strength: str, rsi_value: float,
                             volume_ratio: float, confidence_score: int) -> Tuple[float, float, str]:
    """
    Calculate SL and TP based on logic - MINIMAL OUTPUT
    """
    logic_lines = []
    
    htf_df = data['HTF']
    mtf_df = data['MTF']
    
    # Get key levels
    htf_swing_low = htf_df['low'].rolling(window=10).min().iloc[-1]
    htf_swing_high = htf_df['high'].rolling(window=10).max().iloc[-1]
    
    # Base SL from wave structure
    base_sl = None
    if "IMPULSE_UP" in step2_pattern and side == "BUY":
        correction_low = mtf_df['low'].rolling(window=20).min().iloc[-1]
        base_sl = correction_low * 0.995
        logic_lines.append(f"SL: Below wave correction ({correction_low:.1f})")
    elif "IMPULSE_DOWN" in step2_pattern and side == "SELL":
        correction_high = mtf_df['high'].rolling(window=20).max().iloc[-1]
        base_sl = correction_high * 1.005
        logic_lines.append(f"SL: Above wave correction ({correction_high:.1f})")
    else:
        # Use HTF swing levels
        if side == "BUY":
            base_sl = htf_swing_low * 0.99
            logic_lines.append(f"SL: Below HTF swing ({htf_swing_low:.1f})")
        else:
            base_sl = htf_swing_high * 1.01
            logic_lines.append(f"SL: Above HTF swing ({htf_swing_high:.1f})")
    
    # Strength adjustment
    strength_multiplier = 1.0
    if "STRONG" in step3_strength:
        strength_multiplier = 0.8
    elif "WEAK" in step3_strength:
        strength_multiplier = 1.3
    
    # RSI TP adjustment
    rsi_tp_adjustment = 1.0
    if side == "BUY":
        if rsi_value < 40:
            rsi_tp_adjustment = 1.2
        elif rsi_value > 60:
            rsi_tp_adjustment = 0.8
    else:
        if rsi_value > 60:
            rsi_tp_adjustment = 1.2
        elif rsi_value < 40:
            rsi_tp_adjustment = 0.8
    
    # Volume adjustment
    volume_multiplier = 1.0
    if volume_ratio > 1.5:
        volume_multiplier = 1.15
    elif volume_ratio < 0.8:
        volume_multiplier = 0.85
    
    # Confluence adjustment
    trend_strength = 1.0
    if confidence_score == 6:
        trend_strength = 1.2
    elif confidence_score >= 4:
        trend_strength = 1.0
    else:
        trend_strength = 0.8
    
    # Final calculations
    if side == "BUY":
        distance_to_sl = abs(current_price - base_sl)
        sl = current_price - (distance_to_sl * strength_multiplier)
        
        base_risk = abs(current_price - sl)
        tp_distance = base_risk * 2.0 * rsi_tp_adjustment * volume_multiplier * trend_strength
        tp = current_price + tp_distance
        
        rr_ratio = tp_distance / base_risk if base_risk > 0 else 0
        
    else:  # SELL
        distance_to_sl = abs(base_sl - current_price)
        sl = current_price + (distance_to_sl * strength_multiplier)
        
        base_risk = abs(sl - current_price)
        tp_distance = base_risk * 2.0 * rsi_tp_adjustment * volume_multiplier * trend_strength
        tp = current_price - tp_distance
        
        rr_ratio = tp_distance / base_risk if base_risk > 0 else 0
    
    # Add TP logic
    logic_lines.append(f"TP: RSI:{rsi_tp_adjustment:.1f}x, Vol:{volume_multiplier:.1f}x, Conf:{trend_strength:.1f}x")
    logic_lines.append(f"Final R:R: {rr_ratio:.2f}:1")
    
    return sl, tp, " | ".join(logic_lines)

# ================ MAIN ANALYSIS ================

def analyze_symbol_with_6steps(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """Main analysis function - CLEAN & MINIMAL"""
    try:
        log.info(f"Analyzing {symbol}...")
        
        # Execute all 6 steps
        step1_passed, step1_trend = step1_mtf_analysis(data)
        step2_passed, step2_pattern = step2_wave_analysis(data)
        step3_passed, step3_strength = step3_strength_analysis(data)
        step4_passed, step4_signal, rsi_value, macd_hist = step4_indicators_analysis(data)
        step5_passed, step5_strength, volume_ratio = step5_volume_analysis(data)
        
        step6_passed, step6_trend = step6_trend_identification(
            step1_trend, step2_pattern, step3_strength, step4_signal, step5_strength
        )
        
        # Calculate total
        steps_passed = [step1_passed, step2_passed, step3_passed, step4_passed, step5_passed, step6_passed]
        total_passed = sum(steps_passed)
        
        log.info(f"Confluence: {total_passed}/6 for {symbol}")
        
        # Check confluence
        if total_passed >= MIN_CONFIDENCE and step6_trend in ["UP", "DOWN"]:
            current_price = data['LTF']['close'].iloc[-1]
            side = "BUY" if step6_trend == "UP" else "SELL"
            
            # Calculate logic-based SL/TP
            sl, tp, sl_tp_logic = calculate_logic_based_sltp(
                current_price, side, data, step2_pattern, step3_strength,
                rsi_value, volume_ratio, total_passed
            )
            
            # Risk metrics
            risk = abs(current_price - sl)
            reward = abs(tp - current_price)
            rr_ratio = reward / risk if risk > 0 else 0
            risk_pct = (risk / current_price) * 100
            reward_pct = (reward / current_price) * 100
            
            # Create minimal logic breakdown
            logic_breakdown = f"""
Step 1 (MTF): {'✓' if step1_passed else '✗'} - {step1_trend}
Step 2 (Wave): {'✓' if step2_passed else '✗'} - {step2_pattern}
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
                'step2_result': step2_passed,
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
            
            log.info(f"✅ Signal: {symbol} {side} at {current_price:.2f}")
            return signal
        
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
                    step1_result, step2_result, step3_result, step4_result,
                    step5_result, step6_result, confidence_score, logic_breakdown,
                    timeframe_combo, trend_direction, wave_pattern, rsi_value,
                    macd_hist, volume_ratio, strength_level, rr_ratio, risk_pct,
                    reward_pct, signal_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                signal['tp'], signal['status'], signal['step1_result'],
                signal['step2_result'], signal['step3_result'], signal['step4_result'],
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
Confluence: {signal['confidence_score']}/6 ✅

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

# ---------------- MONITORING ----------------
async def monitor_loop(exchange):
    """Monitor TP/SL"""
    while True:
        try:
            async with db_lock:
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
                                await tg(f"✅ {symbol} TP HIT")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price <= sl:
                                await tg(f"❌ {symbol} SL HIT")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                        else:
                            if current_price <= tp:
                                await tg(f"✅ {symbol} TP HIT")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price >= sl:
                                await tg(f"❌ {symbol} SL HIT")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?,", (sig_id,))
                    
                    except Exception as e:
                        log.error(f"Monitor error {symbol}: {e}")
                
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
                       timestamp, trend_direction, rr_ratio 
                FROM signals WHERE status='OPEN' ORDER BY timestamp DESC LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
        
        signals = [dict(zip(columns, row)) for row in rows]
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        return {"error": str(e)}

# ---------------- MAIN ----------------
async def main():
    global exchange
    
    log.info("="*50)
    log.info("🚀 6-STEP SCANNER STARTING")
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
🚀 6-STEP SCANNER STARTED

Features:
• Minimal clean alerts
• Logic-based SL/TP
• {MIN_CONFIDENCE}/6 confluence required
• Timeframes: 4h, 1h, 15m

Ready to scan!
        """)
        
        # Start loops
        await asyncio.gather(
            scan_loop(exchange),
            monitor_loop(exchange)
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