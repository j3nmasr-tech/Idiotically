#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
6-STEP EXPANSION SCANNER
Detects START of big moves, not just confluence
"""

import os
import time
import asyncio
import logging
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
MIN_CONFIDENCE = 4  # Need ALL 5 conditions for expansion start

# Timeframes - Focused on pressure detection
TIMEFRAMES = {
    "HTF": "4h",    # Overall pressure
    "MTF": "1h",    # Wave exhaustion
    "LTF": "15m"    # Entry & explosion detection
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("expansion_scanner")
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
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
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
                timeframe_pressure TEXT,
                wave_exhaustion TEXT,
                strength_shift TEXT,
                volume_confirmation TEXT,
                expansion_trigger TEXT,
                expansion_score INTEGER DEFAULT 0,
                logic_breakdown TEXT,
                signal_hash TEXT UNIQUE
            )
        """)
        
        await db_conn.commit()
        log.info("✅ Database created")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ EXPANSION DETECTION LOGIC ================

def detect_timeframe_pressure(data: Dict[str, pd.DataFrame]) -> Tuple[bool, str, str]:
    """
    1️⃣ TIMEFRAME PRESSURE
    HTF + MTF + LTF aligned with NO opposing force
    """
    try:
        htf_df = data['HTF']
        mtf_df = data['MTF']
        ltf_df = data['LTF']
        
        # Recent trend direction for each timeframe
        htf_dir = "UP" if htf_df['close'].iloc[-1] > htf_df['close'].iloc[-20] else "DOWN"
        mtf_dir = "UP" if mtf_df['close'].iloc[-1] > mtf_df['close'].iloc[-10] else "DOWN"
        ltf_dir = "UP" if ltf_df['close'].iloc[-1] > ltf_df['close'].iloc[-5] else "DOWN"
        
        # Check alignment (all same direction = maximum pressure)
        directions = [htf_dir, mtf_dir, ltf_dir]
        
        if all(d == "UP" for d in directions):
            pressure = True
            direction = "UP"
            reason = f"ALL TIMEFRAMES UP | No opposing force detected"
        
        elif all(d == "DOWN" for d in directions):
            pressure = True
            direction = "DOWN"
            reason = f"ALL TIMEFRAMES DOWN | No opposing force detected"
        
        else:
            # Check if one timeframe is opposing (weakens pressure)
            up_count = directions.count("UP")
            down_count = directions.count("DOWN")
            
            if up_count == 2 and down_count == 1:
                # Majority UP but one timeframe DOWN
                opposing_tf = "LTF" if ltf_dir == "DOWN" else ("MTF" if mtf_dir == "DOWN" else "HTF")
                pressure = False
                direction = "UP"
                reason = f"OPPOSITION: {opposing_tf} is DOWN | Pressure weakened"
            
            elif down_count == 2 and up_count == 1:
                opposing_tf = "LTF" if ltf_dir == "UP" else ("MTF" if mtf_dir == "UP" else "HTF")
                pressure = False
                direction = "DOWN"
                reason = f"OPPOSITION: {opposing_tf} is UP | Pressure weakened"
            
            else:
                pressure = False
                direction = "NEUTRAL"
                reason = f"NO CLEAR PRESSURE | Mixed: HTF:{htf_dir} MTF:{mtf_dir} LTF:{ltf_dir}"
        
        return pressure, direction, reason
        
    except Exception as e:
        return False, "NEUTRAL", f"Error: {str(e)}"

def detect_wave_exhaustion(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[bool, str]:
    """
    2️⃣ WAVE EXHAUSTION → WAVE RELEASE
    Small waves failing, then one breaks the rhythm
    """
    try:
        mtf_df = data['MTF']
        prices = mtf_df['close'].values[-30:]
        highs = mtf_df['high'].values[-30:]
        lows = mtf_df['low'].values[-30:]
        
        # Find recent swing points
        swing_highs = []
        swing_lows = []
        
        for i in range(3, len(prices)-3):
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i-3] and
                highs[i] > highs[i+1] and highs[i] > highs[i+2] and highs[i] > highs[i+3]):
                swing_highs.append((i, highs[i]))
            
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i-3] and
                lows[i] < lows[i+1] and lows[i] < lows[i+2] and lows[i] < lows[i+3]):
                swing_lows.append((i, lows[i]))
        
        if len(swing_highs) >= 3 and len(swing_lows) >= 3:
            # Get last 3 swings
            last_highs = [h[1] for h in swing_highs[-3:]]
            last_lows = [l[1] for l in swing_lows[-3:]]
            
            # Check for compression (smaller waves)
            high_range = max(last_highs) - min(last_highs)
            low_range = max(last_lows) - min(last_lows)
            price_range = prices[-1] * 0.02  # 2% of price as threshold
            
            compression_detected = high_range < price_range and low_range < price_range
            
            if direction == "UP":
                # For uptrend: Look for higher lows failing to get lower
                if (last_lows[-1] > last_lows[-2] and  # Still making higher lows
                    last_lows[-2] > last_lows[-3] and
                    compression_detected):
                    return True, f"COMPRESSION → RELEASE UP | Waves compressed {high_range:.2f}/{low_range:.2f}"
            
            elif direction == "DOWN":
                # For downtrend: Look for lower highs failing to get higher
                if (last_highs[-1] < last_highs[-2] and  # Still making lower highs
                    last_highs[-2] < last_highs[-3] and
                    compression_detected):
                    return True, f"COMPRESSION → RELEASE DOWN | Waves compressed {high_range:.2f}/{low_range:.2f}"
        
        # Look for recent breakout of compression
        recent_prices = prices[-10:]
        price_std = np.std(recent_prices)
        if price_std < (prices[-1] * 0.005):  # Very low volatility
            return True, f"EXTREME COMPRESSION | Std: {price_std:.4f}"
        
        return False, "NO WAVE EXHAUSTION | Normal wave activity"
        
    except Exception as e:
        return False, f"Error: {str(e)}"

def detect_strength_shift(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[bool, str]:
    """
    3️⃣ STRENGTH SHIFT
    Bodies getting larger, speed increasing, closes becoming aggressive
    """
    try:
        ltf_df = data['LTF']
        
        # Last 5 candles analysis
        recent = ltf_df.iloc[-5:]
        
        # Candle body size increase
        body_sizes = abs(recent['close'] - recent['open'])
        avg_body = body_sizes.mean()
        last_body = body_sizes.iloc[-1]
        
        body_increasing = last_body > avg_body * 1.5
        
        # Close aggression
        if direction == "UP":
            # Bullish aggression: Closing near highs
            recent_high_pct = (recent['close'] - recent['low']) / (recent['high'] - recent['low'])
            avg_high_pct = recent_high_pct.mean()
            last_high_pct = recent_high_pct.iloc[-1]
            
            close_aggressive = last_high_pct > 0.7 and last_high_pct > avg_high_pct
        
        else:  # DOWN
            # Bearish aggression: Closing near lows
            recent_low_pct = (recent['high'] - recent['close']) / (recent['high'] - recent['low'])
            avg_low_pct = recent_low_pct.mean()
            last_low_pct = recent_low_pct.iloc[-1]
            
            close_aggressive = last_low_pct > 0.7 and last_low_pct > avg_low_pct
        
        # Speed increase (momentum)
        price_changes = recent['close'].pct_change().dropna()
        if len(price_changes) > 1:
            momentum_increasing = abs(price_changes.iloc[-1]) > abs(price_changes.iloc[-2]) * 1.3
        else:
            momentum_increasing = False
        
        # Control shift detection
        if direction == "UP":
            # For bullish shift: More green candles, bigger bodies
            green_candles = (recent['close'] > recent['open']).sum()
            control_shift = green_candles >= 3 and body_increasing
        else:
            red_candles = (recent['close'] < recent['open']).sum()
            control_shift = red_candles >= 3 and body_increasing
        
        if control_shift and (close_aggressive or momentum_increasing):
            return True, f"CONTROL SHIFT | Bodies: +{(last_body/avg_body-1)*100:.0f}% | Aggressive closes"
        
        return False, "NO STRENGTH SHIFT | Normal candle patterns"
        
    except Exception as e:
        return False, f"Error: {str(e)}"

def detect_volume_confirmation(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[bool, str, float]:
    """
    4️⃣ VOLUME CONFIRMATION
    Not stop hunt, not noise - BIG PARTICIPATION
    """
    try:
        mtf_df = data['MTF']
        ltf_df = data['LTF']
        
        # MTF volume trend (big picture)
        mtf_volumes = mtf_df['volume'].values[-20:]
        mtf_avg_volume = np.mean(mtf_volumes[:-5])  # Average of first 15
        mtf_recent_volume = np.mean(mtf_volumes[-5:])  # Last 5
        
        mtf_volume_spike = mtf_recent_volume > mtf_avg_volume * 1.3
        
        # LTF volume for entry confirmation
        ltf_volumes = ltf_df['volume'].values[-10:]
        ltf_avg_volume = np.mean(ltf_volumes[:-3])
        ltf_last_volume = ltf_volumes[-1]
        
        ltf_volume_spike = ltf_last_volume > ltf_avg_volume * 1.5
        
        # Volume-by-price analysis
        recent = ltf_df.iloc[-5:]
        
        if direction == "UP":
            up_candles = recent[recent['close'] > recent['open']]
            down_candles = recent[recent['close'] < recent['open']]
            
            if not up_candles.empty and not down_candles.empty:
                up_volume = up_candles['volume'].sum()
                down_volume = down_candles['volume'].sum()
                volume_ratio = up_volume / down_volume if down_volume > 0 else 10
                
                volume_confirms = volume_ratio > 1.5
            else:
                volume_confirms = False
        
        else:  # DOWN
            up_candles = recent[recent['close'] > recent['open']]
            down_candles = recent[recent['close'] < recent['open']]
            
            if not up_candles.empty and not down_candles.empty:
                up_volume = up_candles['volume'].sum()
                down_volume = down_candles['volume'].sum()
                volume_ratio = down_volume / up_volume if up_volume > 0 else 10
                
                volume_confirms = volume_ratio > 1.5
            else:
                volume_confirms = False
        
        # Big money participation detection
        big_participation = (mtf_volume_spike or ltf_volume_spike) and volume_confirms
        
        volume_ratio_value = ltf_last_volume / ltf_avg_volume if ltf_avg_volume > 0 else 1
        
        if big_participation:
            reason = f"BIG PARTICIPATION | Volume spike: {volume_ratio_value:.1f}x | Confirms direction"
            return True, reason, volume_ratio_value
        else:
            reason = f"WEAK PARTICIPATION | Volume: {volume_ratio_value:.1f}x | No confirmation"
            return False, reason, volume_ratio_value
        
    except Exception as e:
        return False, f"Error: {str(e)}", 1.0

def detect_expansion_trigger(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[bool, str]:
    """
    5️⃣ EXPANSION TRIGGER
    The actual moment price starts expanding
    """
    try:
        ltf_df = data['LTF']
        
        # Last 3 candles pattern
        recent = ltf_df.iloc[-3:]
        
        if direction == "UP":
            # Bullish expansion trigger
            # 1. Higher lows
            higher_lows = (recent['low'].iloc[-1] > recent['low'].iloc[-2] and 
                          recent['low'].iloc[-2] > recent['low'].iloc[-3])
            
            # 2. Increasing range
            ranges = recent['high'] - recent['low']
            range_increasing = ranges.iloc[-1] > ranges.iloc[-2] > ranges.iloc[-3]
            
            # 3. Strong close
            last_close_pct = (recent['close'].iloc[-1] - recent['low'].iloc[-1]) / (recent['high'].iloc[-1] - recent['low'].iloc[-1])
            strong_close = last_close_pct > 0.7
            
            if higher_lows and range_increasing and strong_close:
                return True, f"EXPANSION TRIGGER UP | Higher lows + Range expanding + Strong close"
        
        else:  # DOWN
            # Bearish expansion trigger
            # 1. Lower highs
            lower_highs = (recent['high'].iloc[-1] < recent['high'].iloc[-2] and 
                          recent['high'].iloc[-2] < recent['high'].iloc[-3])
            
            # 2. Increasing range
            ranges = recent['high'] - recent['low']
            range_increasing = ranges.iloc[-1] > ranges.iloc[-2] > ranges.iloc[-3]
            
            # 3. Weak close
            last_close_pct = (recent['high'].iloc[-1] - recent['close'].iloc[-1]) / (recent['high'].iloc[-1] - recent['low'].iloc[-1])
            weak_close = last_close_pct > 0.7
            
            if lower_highs and range_increasing and weak_close:
                return True, f"EXPANSION TRIGGER DOWN | Lower highs + Range expanding + Weak close"
        
        # Check for breakout of recent range
        recent_high = ltf_df['high'].iloc[-10:].max()
        recent_low = ltf_df['low'].iloc[-10:].min()
        current_close = ltf_df['close'].iloc[-1]
        
        range_size = recent_high - recent_low
        if range_size > 0:
            if direction == "UP" and current_close > recent_high + (range_size * 0.1):
                return True, f"BREAKOUT TRIGGER | Broke above range by {((current_close-recent_high)/recent_low)*100:.1f}%"
            
            if direction == "DOWN" and current_close < recent_low - (range_size * 0.1):
                return True, f"BREAKDOWN TRIGGER | Broke below range by {((recent_low-current_close)/recent_high)*100:.1f}%"
        
        return False, "NO EXPANSION TRIGGER | Still inside range"
        
    except Exception as e:
        return False, f"Error: {str(e)}"

def calculate_expansion_targets(current_price: float, side: str, data: Dict[str, pd.DataFrame]) -> Tuple[float, float, str]:
    """
    Calculate SL/TP for expansion moves
    Entry at START of expansion, TP at logical expansion targets
    """
    logic_lines = []
    
    mtf_df = data['MTF']
    
    if side == "BUY":
        # Find nearest support (logical invalidation)
        recent_lows = mtf_df['low'].values[-20:]
        support = np.min(recent_lows[-5:])  # Most recent low cluster
        sl = support * 0.995  # Just below support
        
        # Expansion target: Previous swing high or measured move
        recent_highs = mtf_df['high'].values[-20:]
        resistance = np.max(recent_highs[-10:])
        
        # Risk-based TP with expansion logic
        risk = current_price - sl
        expansion_multiplier = 2.5  # Expansion moves give 2.5:1+ R:R
        tp = current_price + (risk * expansion_multiplier)
        
        # Ensure TP doesn't exceed major resistance
        if tp > resistance * 1.05:  # Don't go too far beyond resistance
            tp = resistance * 1.02
        
        logic_lines.append(f"SL: Below support {support:.2f} (invalidation)")
        logic_lines.append(f"TP: Expansion target ~{((tp-current_price)/current_price*100):.1f}% move")
        logic_lines.append(f"R:R: {expansion_multiplier:.1f}:1 (expansion phase)")
    
    else:  # SELL
        # Find nearest resistance (logical invalidation)
        recent_highs = mtf_df['high'].values[-20:]
        resistance = np.max(recent_highs[-5:])
        sl = resistance * 1.005  # Just above resistance
        
        # Expansion target: Previous swing low
        recent_lows = mtf_df['low'].values[-20:]
        support = np.min(recent_lows[-10:])
        
        risk = sl - current_price
        expansion_multiplier = 2.5
        tp = current_price - (risk * expansion_multiplier)
        
        if tp < support * 0.95:
            tp = support * 0.98
        
        logic_lines.append(f"SL: Above resistance {resistance:.2f} (invalidation)")
        logic_lines.append(f"TP: Expansion target ~{((current_price-tp)/current_price*100):.1f}% move")
        logic_lines.append(f"R:R: {expansion_multiplier:.1f}:1 (expansion phase)")
    
    return sl, tp, " | ".join(logic_lines)

# ================ MAIN DETECTION ================

async def detect_expansion_start(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """
    Detect the START of an expansion phase
    Returns signal if ALL 5 conditions are met
    """
    try:
        log.info(f"🔍 Checking {symbol} for expansion start...")
        
        # 1. TIMEFRAME PRESSURE
        pressure, direction, pressure_reason = detect_timeframe_pressure(data)
        
        if not pressure:
            log.debug(f"❌ {symbol}: No timeframe pressure - {pressure_reason}")
            return None
        
        log.info(f"✅ {symbol}: Timeframe pressure {direction} - {pressure_reason}")
        
        # 2. WAVE EXHAUSTION
        wave_exhaustion, wave_reason = detect_wave_exhaustion(data, direction)
        
        if not wave_exhaustion:
            log.debug(f"❌ {symbol}: No wave exhaustion - {wave_reason}")
            return None
        
        log.info(f"✅ {symbol}: Wave exhaustion detected - {wave_reason}")
        
        # 3. STRENGTH SHIFT
        strength_shift, strength_reason = detect_strength_shift(data, direction)
        
        if not strength_shift:
            log.debug(f"❌ {symbol}: No strength shift - {strength_reason}")
            return None
        
        log.info(f"✅ {symbol}: Strength shift detected - {strength_reason}")
        
        # 4. VOLUME CONFIRMATION
        volume_confirm, volume_reason, volume_ratio = detect_volume_confirmation(data, direction)
        
        if not volume_confirm:
            log.debug(f"❌ {symbol}: No volume confirmation - {volume_reason}")
            return None
        
        log.info(f"✅ {symbol}: Volume confirms - {volume_reason}")
        
        # 5. EXPANSION TRIGGER
        expansion_trigger, trigger_reason = detect_expansion_trigger(data, direction)
        
        if not expansion_trigger:
            log.debug(f"❌ {symbol}: No expansion trigger - {trigger_reason}")
            return None
        
        log.info(f"🎯 {symbol}: EXPANSION TRIGGERED! - {trigger_reason}")
        
        # ALL 5 CONDITIONS MET - EXPANSION STARTING
        current_price = data['LTF']['close'].iloc[-1]
        side = "BUY" if direction == "UP" else "SELL"
        
        # Calculate expansion targets
        sl, tp, sl_tp_logic = calculate_expansion_targets(current_price, side, data)
        
        # Risk metrics
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
            
            'timeframe_pressure': pressure_reason,
            'wave_exhaustion': wave_reason,
            'strength_shift': strength_reason,
            'volume_confirmation': volume_reason,
            'expansion_trigger': trigger_reason,
            'expansion_score': 5,  # All 5 conditions met
            
            'logic_breakdown': f"""
🚀 **EXPANSION PHASE STARTING** 🚀

**{symbol} {side}**

1️⃣ TIMEFRAME PRESSURE:
{pressure_reason}

2️⃣ WAVE EXHAUSTION → RELEASE:
{wave_reason}

3️⃣ STRENGTH SHIFT (Control Change):
{strength_reason}

4️⃣ VOLUME CONFIRMATION (Big Participation):
{volume_reason}

5️⃣ EXPANSION TRIGGER:
{trigger_reason}

**ENTRY: {current_price:.2f}**
**SL: {sl:.2f}** (Invalidation)
**TP: {tp:.2f}** (Expansion target)

Risk: {risk_pct:.1f}% | Reward: {reward_pct:.1f}% | R:R: {rr_ratio:.2f}:1

{sl_tp_logic}
""".strip(),
            
            'signal_hash': hashlib.md5(
                f"{symbol}:{side}:{current_price:.8f}:{int(time.time())}".encode()
            ).hexdigest()
        }
        
        log.info(f"🔥 EXPANSION SIGNAL: {symbol} {side} at {current_price:.2f}")
        log.info(f"   R:R {rr_ratio:.2f}:1 | Risk {risk_pct:.1f}% | Reward {reward_pct:.1f}%")
        
        return signal
        
    except Exception as e:
        log.error(f"Detection error {symbol}: {e}")
        return None

# ================ MAIN SCANNING ================

async def fetch_multi_timeframe_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Fetch data for expansion detection"""
    data = {}
    for tf_type, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=100)
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

async def save_signal(signal: Dict) -> bool:
    """Save expansion signal"""
    try:
        async with db_lock:
            async with db_conn.execute(
                "SELECT COUNT(*) FROM signals WHERE signal_hash = ?",
                (signal['signal_hash'],)
            ) as cursor:
                exists = (await cursor.fetchone())[0]
            
            if exists > 0:
                return False
            
            await db_conn.execute("""
                INSERT INTO signals (
                    symbol, side, entry, sl, tp, status,
                    timeframe_pressure, wave_exhaustion, strength_shift,
                    volume_confirmation, expansion_trigger, expansion_score,
                    logic_breakdown, signal_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                signal['tp'], signal['status'], signal['timeframe_pressure'],
                signal['wave_exhaustion'], signal['strength_shift'],
                signal['volume_confirmation'], signal['expansion_trigger'],
                signal['expansion_score'], signal['logic_breakdown'],
                signal['signal_hash']
            ))
            
            await db_conn.commit()
            return True
            
    except Exception as e:
        log.error(f"Save error: {e}")
        return False

async def send_expansion_alert(signal: Dict):
    """Send expansion alert"""
    try:
        message = f"""
⚡ **EXPANSION PHASE DETECTED** ⚡

**{signal['symbol']}** | **ENTER {signal['side']}**

🎯 **ALL 5 CONDITIONS MET:**
1️⃣ Timeframe Pressure ✓
2️⃣ Wave Exhaustion ✓  
3️⃣ Strength Shift ✓
4️⃣ Volume Confirmation ✓
5️⃣ Expansion Trigger ✓

**TRADE:**
Entry: {signal['entry']:.2f}
SL: {signal['sl']:.2f} (invalidation)
TP: {signal['tp']:.2f} (expansion target)

**EXPECT:**
• Price to expand in next 5min-2hrs
• Hard move in {signal['side']} direction
• Big participation move

#ExpansionPhase #{signal['side']}
"""
        
        await tg(message)
        log.info(f"Expansion alert sent: {signal['symbol']}")
        
    except Exception as e:
        log.error(f"Alert error: {e}")

async def scan_for_expansions(exchange):
    """Scan for expansion starts"""
    while True:
        try:
            log.info("🔭 Scanning for expansion phases...")
            
            # Get top pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, t['quoteVolume']) for s, t in tickers.items() 
                         if s.endswith('/USDT') and t.get('quoteVolume', 0) > 1000000]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            expansions_found = 0
            
            for symbol, volume in top_pairs:
                try:
                    data = await fetch_multi_timeframe_data(exchange, symbol)
                    if not data:
                        continue
                    
                    signal = await detect_expansion_start(data, symbol)
                    
                    if signal:
                        if await save_signal(signal):
                            await send_expansion_alert(signal)
                            expansions_found += 1
                    
                    await asyncio.sleep(0.3)
                    
                except Exception as e:
                    log.debug(f"Scan error {symbol}: {e}")
                    continue
            
            log.info(f"Scan complete. Found {expansions_found} expansion starts.")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
            await asyncio.sleep(30)

async def monitor_expansions(exchange):
    """Monitor expansion trades"""
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
                                await tg(f"✅ {symbol} EXPANSION COMPLETE | Target hit")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price <= sl:
                                await tg(f"❌ {symbol} EXPANSION FAILED | Invalidated")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                        else:
                            if current_price <= tp:
                                await tg(f"✅ {symbol} EXPANSION COMPLETE | Target hit")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price >= sl:
                                await tg(f"❌ {symbol} EXPANSION FAILED | Invalidated")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                    
                    except Exception as e:
                        log.debug(f"Monitor error {symbol}: {e}")
                
                await db_conn.commit()
            
            await asyncio.sleep(15)  # Check every 15 seconds
            
        except Exception as e:
            log.error(f"Monitor error: {e}")
            await asyncio.sleep(30)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "scanning_for_expansions",
        "min_conditions": 5,
        "target": "detect_start_of_big_moves"
    }

@app.get("/expansions")
async def get_expansions():
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT symbol, side, entry, sl, tp, expansion_score, timestamp 
                FROM signals WHERE status='OPEN' ORDER BY timestamp DESC LIMIT 10
            """) as cursor:
                rows = await cursor.fetchall()
        
        expansions = []
        for row in rows:
            expansions.append({
                "symbol": row[0],
                "side": row[1],
                "entry": row[2],
                "sl": row[3],
                "tp": row[4],
                "score": row[5],
                "time": row[6]
            })
        
        return {"expansions": expansions}
    except Exception as e:
        return {"error": str(e)}

# ---------------- MAIN ----------------
async def main():
    global exchange
    
    log.info("="*60)
    log.info("🚀 EXPANSION PHASE SCANNER STARTING")
    log.info("Looking for START of big moves")
    log.info("="*60)
    
    try:
        if not await init_db():
            return
        
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        
        await exchange.fetch_ticker("BTC/USDT")
        log.info("✅ Exchange connected")
        
        # Startup message
        await tg(f"""
🔥 EXPANSION PHASE SCANNER STARTED

I scan for the START of big moves when:

1️⃣ All timeframes align (no opposing force)
2️⃣ Waves exhaust → release
3️⃣ Strength shifts (control changes)
4️⃣ Volume confirms (big participation)
5️⃣ Expansion triggers

When all 5 hit → "ENTER LONG/SHORT"
Then price expands in next 5min-2hrs

Scanning {TOP_N} pairs...
        """)
        
        # Start scanning
        await asyncio.gather(
            scan_for_expansions(exchange),
            monitor_expansions(exchange)
        )
        
    except KeyboardInterrupt:
        log.info("Stopped by user")
        await tg("🛑 Expansion scanner stopped")
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