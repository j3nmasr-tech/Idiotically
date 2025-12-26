#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visual Synthesis Scanner - الطريقة البصرية
Watches all timeframes, wave range, strength, indicators, volume → determines direction
FIXED VERSION
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

# Timeframes for visual analysis
TIMEFRAMES = {
    "DAILY": "1d",      # Primary direction  
    "H4": "4h",         # Main wave
    "H1": "1h",         # Strength analysis
    "M15": "15m"        # Entry & indicators
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("visual_scanner")
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
                timeframe_alignment TEXT,
                wave_structure TEXT,
                strength_level TEXT,
                indicators_signal TEXT,
                volume_status TEXT,
                synthesis_score REAL,
                signal_hash TEXT UNIQUE
            )
        """)
        
        await db_conn.commit()
        log.info("✅ Database created")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ VISUAL SYNTHESIS METHOD - FIXED ================

def analyze_timeframes_visual(data: Dict[str, pd.DataFrame]) -> Tuple[str, float, str]:
    """
    ١. كل الفريمات (All timeframes)
    Visual check: Are they aligned? Which direction?
    Returns: (direction, alignment_score, reason)
    """
    try:
        tf_directions = {}
        tf_strengths = {}
        
        for tf_name, df in data.items():
            # Simple visual trend detection
            prices = df['close'].values
            
            # Short-term (last 5% of data)
            short_len = max(5, len(prices) // 20)
            short_trend = "UP" if prices[-1] > prices[-short_len] else "DOWN"
            
            # Medium-term (last 20% of data)
            med_len = max(10, len(prices) // 5)
            med_trend = "UP" if prices[-1] > prices[-med_len] else "DOWN"
            
            # Long-term (full period)
            long_trend = "UP" if prices[-1] > prices[0] else "DOWN"
            
            # Determine overall direction for this timeframe
            trends = [short_trend, med_trend, long_trend]
            direction = max(set(trends), key=trends.count)
            
            # Strength: How clear is the trend?
            trend_agreement = trends.count(direction) / len(trends)
            
            tf_directions[tf_name] = direction
            tf_strengths[tf_name] = trend_agreement
        
        # Check alignment across timeframes
        directions = list(tf_directions.values())
        
        if all(d == "UP" for d in directions):
            alignment_score = min(tf_strengths.values())  # Weakest link
            return "UP", alignment_score, "✅ جميع الفريمات صاعدة"
        
        elif all(d == "DOWN" for d in directions):
            alignment_score = min(tf_strengths.values())
            return "DOWN", alignment_score, "✅ جميع الفريمات هابطة"
        
        else:
            # Mixed - find dominant direction
            up_count = directions.count("UP")
            down_count = directions.count("DOWN")
            
            if up_count > down_count:
                dominant = "UP"
                alignment_score = sum([tf_strengths[tf] for tf, d in tf_directions.items() if d == "UP"]) / up_count
                opposing = "DOWN"
            else:
                dominant = "DOWN"
                alignment_score = sum([tf_strengths[tf] for tf, d in tf_directions.items() if d == "DOWN"]) / down_count
                opposing = "UP"
            
            return dominant, alignment_score * 0.7, f"⚠️ {dominant} مهيمن ولكن {opposing} في بعض الفريمات"
    
    except Exception as e:
        return "NEUTRAL", 0, f"خطأ: {str(e)}"

def analyze_wave_range_visual(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """
    ٢. المدى الموجي (Wave range)
    Visual check: What's the wave structure? Compression? Expansion?
    Returns: (wave_score, reason)
    """
    try:
        # Use H4 for primary wave analysis - FIXED: Use 'H4' instead of old timeframe name
        h4_df = data['H4']
        prices = h4_df['close'].values[-50:]  # Last 50 candles
        
        # Find swing points
        swing_highs = []
        swing_lows = []
        
        for i in range(3, len(prices)-3):
            if all(prices[i] > prices[i+j] for j in [-3, -2, -1, 1, 2, 3]):
                swing_highs.append(prices[i])
            
            if all(prices[i] < prices[i+j] for j in [-3, -2, -1, 1, 2, 3]):
                swing_lows.append(prices[i])
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            # Check wave structure
            last_highs = swing_highs[-2:]
            last_lows = swing_lows[-2:]
            
            if direction == "UP":
                # Uptrend should have higher highs AND higher lows
                higher_highs = last_highs[-1] > last_highs[-2]
                higher_lows = last_lows[-1] > last_lows[-2]
                
                if higher_highs and higher_lows:
                    wave_score = 0.9
                    reason = "📈 موجات صاعدة واضحة (قمم وقيعان مرتفعة)"
                elif higher_lows:  # At least higher lows
                    wave_score = 0.7
                    reason = "📈 قيعان مرتفعة ولكن القمم متساوية"
                else:
                    wave_score = 0.3
                    reason = "⚠️ موجة صاعدة ضعيفة"
            
            else:  # DOWN
                # Downtrend should have lower highs AND lower lows
                lower_highs = last_highs[-1] < last_highs[-2]
                lower_lows = last_lows[-1] < last_lows[-2]
                
                if lower_highs and lower_lows:
                    wave_score = 0.9
                    reason = "📉 موجات هابطة واضحة (قمم وقيعان منخفضة)"
                elif lower_highs:  # At least lower highs
                    wave_score = 0.7
                    reason = "📉 قمم منخفضة ولكن القيعان متساوية"
                else:
                    wave_score = 0.3
                    reason = "⚠️ موجة هابطة ضعيفة"
            
            # Check for compression (tight range before expansion)
            recent_range = max(prices[-20:]) - min(prices[-20:])
            avg_range = np.mean([max(prices[i-20:i]) - min(prices[i-20:i]) for i in range(40, len(prices), 10) if i-20 >= 0])
            
            if recent_range < avg_range * 0.7:
                wave_score += 0.1
                reason += " | 📊 مدى مضغوط (قبل التوسع)"
        
        else:
            wave_score = 0.5
            reason = "🔍 موجات غير واضحة"
        
        return min(wave_score, 1.0), reason
    
    except Exception as e:
        return 0.5, f"خطأ في تحليل الموجات: {str(e)}"

def analyze_strength_visual(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """
    ٣. القوة (Strength)
    Visual check: How strong is the move? Momentum? Candle bodies?
    Returns: (strength_score, reason)
    """
    try:
        # Use H1 for strength analysis - FIXED: Changed from 'strength' to 'H1'
        h1_df = data['H1']
        
        # Last 10 candles analysis
        recent = h1_df.iloc[-10:]
        
        # 1. Candle body strength
        body_sizes = abs(recent['close'] - recent['open'])
        avg_body = body_sizes.mean()
        
        if direction == "UP":
            bullish_bodies = recent[recent['close'] > recent['open']]
            if not bullish_bodies.empty:
                avg_bullish_body = bullish_bodies['close'] - bullish_bodies['open']
                body_strength = avg_bullish_body.mean() / avg_body if avg_body > 0 else 0
            else:
                body_strength = 0
        else:  # DOWN
            bearish_bodies = recent[recent['close'] < recent['open']]
            if not bearish_bodies.empty:
                avg_bearish_body = bearish_bodies['open'] - bearish_bodies['close']
                body_strength = avg_bearish_body.mean() / avg_body if avg_body > 0 else 0
            else:
                body_strength = 0
        
        # 2. Close position strength
        if direction == "UP":
            # Bullish: Closing near highs is strong
            close_positions = (recent['close'] - recent['low']) / (recent['high'] - recent['low']).replace(0, 0.001)
            close_strength = close_positions.mean()
        else:
            # Bearish: Closing near lows is strong
            close_positions = (recent['high'] - recent['close']) / (recent['high'] - recent['low']).replace(0, 0.001)
            close_strength = close_positions.mean()
        
        # 3. Momentum
        price_changes = recent['close'].pct_change().dropna()
        if len(price_changes) > 0:
            if direction == "UP":
                positive_changes = price_changes[price_changes > 0]
                momentum = len(positive_changes) / len(price_changes) if len(price_changes) > 0 else 0
            else:
                negative_changes = price_changes[price_changes < 0]
                momentum = len(negative_changes) / len(price_changes) if len(price_changes) > 0 else 0
        else:
            momentum = 0.5
        
        # Combine strength factors
        strength_score = (body_strength * 0.4 + close_strength * 0.4 + momentum * 0.2)
        
        if strength_score > 0.7:
            strength_text = "قوية جدا"
        elif strength_score > 0.5:
            strength_text = "قوية"
        elif strength_score > 0.3:
            strength_text = "متوسطة"
        else:
            strength_text = "ضعيفة"
        
        reason = f"💪 القوة: {strength_text} | الأجساد: {body_strength:.1f}x | الإغلاق: {close_strength:.0%}"
        
        return min(strength_score, 1.0), reason
    
    except Exception as e:
        return 0.5, f"خطأ في تحليل القوة: {str(e)}"

def analyze_indicators_visual(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """
    ٤. المؤشرات (Indicators)
    Simple RSI + MACD visual check
    Returns: (indicators_score, reason)
    """
    try:
        # Use M15 for entry indicators - FIXED: 'M15' is correct
        m15_df = data['M15']
        prices = m15_df['close'].values
        
        # RSI
        if len(prices) >= 14:
            deltas = np.diff(prices[-14:])
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
        
        # Simple MACD signal
        if len(prices) >= 26:
            ema_12 = pd.Series(prices[-26:]).ewm(span=12, adjust=False).mean().iloc[-1]
            ema_26 = pd.Series(prices[-26:]).ewm(span=26, adjust=False).mean().iloc[-1]
            macd_line = ema_12 - ema_26
            macd_hist = pd.Series([macd_line]).ewm(span=9, adjust=False).mean().iloc[-1]
            histogram = macd_line - macd_hist
        else:
            histogram = 0
        
        # Check signals
        rsi_signal = ""
        if direction == "UP":
            if rsi < 40:
                rsi_score = 0.8
                rsi_signal = "RSI منخفض (شراء)"
            elif rsi < 60:
                rsi_score = 0.5
                rsi_signal = "RSI محايد"
            else:
                rsi_score = 0.2
                rsi_signal = "RSI مرتفع (احتراس)"
        else:  # DOWN
            if rsi > 60:
                rsi_score = 0.8
                rsi_signal = "RSI مرتفع (بيع)"
            elif rsi > 40:
                rsi_score = 0.5
                rsi_signal = "RSI محايد"
            else:
                rsi_score = 0.2
                rsi_signal = "RSI منخفض (احتراس)"
        
        macd_signal = ""
        if direction == "UP":
            if histogram > 0:
                macd_score = 0.8
                macd_signal = "MACD إيجابي"
            else:
                macd_score = 0.3
                macd_signal = "MACD سلبي"
        else:  # DOWN
            if histogram < 0:
                macd_score = 0.8
                macd_signal = "MACD سلبي"
            else:
                macd_score = 0.3
                macd_signal = "MACD إيجابي"
        
        # Combined score
        indicators_score = (rsi_score + macd_score) / 2
        
        reason = f"📊 المؤشرات: {rsi_signal} ({rsi:.0f}) | {macd_signal}"
        
        return indicators_score, reason
    
    except Exception as e:
        return 0.5, f"خطأ في المؤشرات: {str(e)}"

def analyze_volume_visual(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """
    ٥. الفوليوم (Volume)
    Visual check: Is volume confirming? Increasing?
    Returns: (volume_score, reason)
    """
    try:
        # Check multiple timeframes - FIXED: Using 'H1' and 'M15'
        volume_checks = []
        reasons = []
        
        for tf_name in ['H1', 'M15']:
            if tf_name in data:
                df = data[tf_name]
                
                # Recent volume vs average
                recent_vol = df['volume'].values[-10:]
                prev_vol = df['volume'].values[-20:-10]
                
                if len(recent_vol) > 0 and len(prev_vol) > 0:
                    avg_recent = np.mean(recent_vol)
                    avg_prev = np.mean(prev_vol)
                    
                    if avg_prev > 0:
                        volume_ratio = avg_recent / avg_prev
                    else:
                        volume_ratio = 1
                    
                    # Volume confirmation
                    price_change = (df['close'].iloc[-1] / df['close'].iloc[-10] - 1) * 100
                    
                    if direction == "UP":
                        volume_confirms = price_change > 0 and volume_ratio > 1
                    else:
                        volume_confirms = price_change < 0 and volume_ratio > 1
                    
                    if volume_confirms:
                        if volume_ratio > 1.5:
                            score = 0.9
                            strength = "عالية"
                        elif volume_ratio > 1.2:
                            score = 0.7
                            strength = "جيدة"
                        else:
                            score = 0.5
                            strength = "متوسطة"
                    else:
                        score = 0.3
                        strength = "ضعيفة"
                    
                    volume_checks.append(score)
                    reasons.append(f"{tf_name}: {strength} ({volume_ratio:.1f}x)")
        
        if volume_checks:
            volume_score = np.mean(volume_checks)
            reason = "📈 الفوليوم: " + " | ".join(reasons)
        else:
            volume_score = 0.5
            reason = "📈 الفوليوم: غير متوفر"
        
        return volume_score, reason
    
    except Exception as e:
        return 0.5, f"خطأ في الفوليوم: {str(e)}"

def synthesize_direction_visual(scores: Dict[str, float], reasons: Dict[str, str]) -> Tuple[str, float, str]:
    """
    ٦. أحدد الاتجاه (Determine direction)
    Visual synthesis of all factors
    """
    try:
        # Weighted average (visual trader's intuition)
        weights = {
            'timeframes': 0.25,      # Most important: alignment
            'wave': 0.20,           # Wave structure
            'strength': 0.20,        # Momentum
            'indicators': 0.15,      # Confirmation
            'volume': 0.20          # Participation
        }
        
        total_score = 0
        for factor, score in scores.items():
            total_score += score * weights.get(factor, 0.2)
        
        # Determine if signal is strong enough
        if total_score >= 0.7:
            direction = "UP" if scores.get('timeframes_score', 0) > 0.5 else "DOWN"
            signal_strength = "قوية جدا 🔥"
        elif total_score >= 0.6:
            direction = "UP" if scores.get('timeframes_score', 0) > 0.5 else "DOWN"
            signal_strength = "قوية ✅"
        elif total_score >= 0.5:
            direction = "UP" if scores.get('timeframes_score', 0) > 0.5 else "DOWN"
            signal_strength = "متوسطة ⚠️"
        else:
            return "NEUTRAL", total_score, "❌ الإشارة ضعيفة"
        
        # Create synthesis reason
        synthesis_reason = f"""
🎯 التوليف البصري:
• الفريمات: {reasons['timeframes']}
• الموجة: {reasons['wave']}
• القوة: {reasons['strength']}
• المؤشرات: {reasons['indicators']}
• الفوليوم: {reasons['volume']}

📊 النتيجة: {total_score:.1%} - {signal_strength}
        """.strip()
        
        return direction, total_score, synthesis_reason
    
    except Exception as e:
        return "NEUTRAL", 0, f"خطأ في التوليف: {str(e)}"

def calculate_visual_entry(scores: Dict[str, float], direction: str, current_price: float, data: Dict[str, pd.DataFrame]) -> Tuple[float, float, str]:
    """
    Calculate entry based on visual analysis
    Simple and logical
    """
    logic_lines = []
    
    h4_df = data['H4']
    
    if direction == "UP":
        # For longs: Enter near support with buffer
        recent_low = h4_df['low'].iloc[-10:].min()
        sl = recent_low * 0.99  # 1% below recent low
        
        # TP: Reward based on strength
        base_risk = current_price - sl
        
        if scores.get('strength_score', 0) > 0.7:
            rr_ratio = 2.0  # Strong move
        elif scores.get('strength_score', 0) > 0.5:
            rr_ratio = 1.5  # Medium move
        else:
            rr_ratio = 1.2  # Weak move
        
        tp = current_price + (base_risk * rr_ratio)
        
        logic_lines.append(f"الدخول: قرب {current_price:.2f}")
        logic_lines.append(f"SL: تحت الدعم {recent_low:.2f}")
        logic_lines.append(f"TP: نسبة {rr_ratio:.1f}:1 حسب القوة")
    
    else:  # DOWN
        recent_high = h4_df['high'].iloc[-10:].max()
        sl = recent_high * 1.01  # 1% above recent high
        
        base_risk = sl - current_price
        
        if scores.get('strength_score', 0) > 0.7:
            rr_ratio = 2.0
        elif scores.get('strength_score', 0) > 0.5:
            rr_ratio = 1.5
        else:
            rr_ratio = 1.2
        
        tp = current_price - (base_risk * rr_ratio)
        
        logic_lines.append(f"الدخول: قرب {current_price:.2f}")
        logic_lines.append(f"SL: فوق المقاومة {recent_high:.2f}")
        logic_lines.append(f"TP: نسبة {rr_ratio:.1f}:1 حسب القوة")
    
    risk_pct = abs(current_price - sl) / current_price * 100
    reward_pct = abs(tp - current_price) / current_price * 100
    actual_rr = reward_pct / risk_pct if risk_pct > 0 else 0
    
    logic_lines.append(f"المخاطرة: {risk_pct:.1f}% | المكافأة: {reward_pct:.1f}%")
    logic_lines.append(f"نسبة R:R الفعلية: {actual_rr:.1f}:1")
    
    return sl, tp, "\n".join(logic_lines)

# ================ MAIN VISUAL ANALYSIS ================

async def visual_synthesis_analysis(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """
    Perform visual synthesis analysis
    """
    try:
        log.info(f"🔍 تحليل بصري لـ {symbol}...")
        
        # 1. كل الفريمات (All timeframes)
        direction, tf_score, tf_reason = analyze_timeframes_visual(data)
        
        if direction == "NEUTRAL":
            log.debug(f"❌ {symbol}: لا اتجاه واضح في الفريمات")
            return None
        
        # 2. المدى الموجي (Wave range)
        wave_score, wave_reason = analyze_wave_range_visual(data, direction)
        
        # 3. القوة (Strength)
        strength_score, strength_reason = analyze_strength_visual(data, direction)
        
        # 4. المؤشرات (Indicators)
        indicators_score, indicators_reason = analyze_indicators_visual(data, direction)
        
        # 5. الفوليوم (Volume)
        volume_score, volume_reason = analyze_volume_visual(data, direction)
        
        # Collect scores and reasons
        scores = {
            'timeframes_score': tf_score,
            'wave_score': wave_score,
            'strength_score': strength_score,
            'indicators_score': indicators_score,
            'volume_score': volume_score
        }
        
        reasons = {
            'timeframes': tf_reason,
            'wave': wave_reason,
            'strength': strength_reason,
            'indicators': indicators_reason,
            'volume': volume_reason
        }
        
        # 6. أحدد الاتجاه (Synthesize direction)
        final_direction, synthesis_score, synthesis_reason = synthesize_direction_visual(scores, reasons)
        
        if final_direction == "NEUTRAL" or synthesis_score < 0.6:
            log.info(f"❌ {symbol}: النتيجة غير كافية ({synthesis_score:.1%})")
            return None
        
        log.info(f"✅ {symbol}: {final_direction} - النتيجة: {synthesis_score:.1%}")
        
        # Get current price
        current_price = data['M15']['close'].iloc[-1]
        side = "BUY" if final_direction == "UP" else "SELL"
        
        # Calculate entry
        sl, tp, entry_logic = calculate_visual_entry(scores, final_direction, current_price, data)
        
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
            
            'timeframe_alignment': tf_reason[:100],
            'wave_structure': wave_reason[:100],
            'strength_level': strength_reason[:100],
            'indicators_signal': indicators_reason[:100],
            'volume_status': volume_reason[:100],
            'synthesis_score': synthesis_score,
            
            'signal_hash': hashlib.md5(
                f"{symbol}:{side}:{current_price:.8f}:{int(time.time())}".encode()
            ).hexdigest()
        }
        
        log.info(f"🎯 إشارة: {symbol} {side} عند {current_price:.2f}")
        log.info(f"   النتيجة: {synthesis_score:.1%} | R:R: {rr_ratio:.1f}:1")
        
        return signal
        
    except Exception as e:
        log.error(f"خطأ في التحليل البصري {symbol}: {e}")
        return None

# ================ MAIN SCANNING ================

async def fetch_all_timeframe_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Fetch data for all timeframes"""
    data = {}
    for tf_name, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=100)
            if ohlcv and len(ohlcv) > 30:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                data[tf_name] = df
            else:
                return None
        except Exception as e:
            log.debug(f"خطأ في جلب بيانات {tf}: {e}")
            return None
    return data

async def save_signal(signal: Dict) -> bool:
    """Save signal to database"""
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
                    timeframe_alignment, wave_structure, strength_level,
                    indicators_signal, volume_status, synthesis_score,
                    signal_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                signal['tp'], signal['status'], signal['timeframe_alignment'],
                signal['wave_structure'], signal['strength_level'],
                signal['indicators_signal'], signal['volume_status'],
                signal['synthesis_score'], signal['signal_hash']
            ))
            
            await db_conn.commit()
            return True
            
    except Exception as e:
        log.error(f"خطأ في الحفظ: {e}")
        return False

async def send_visual_alert(signal: Dict):
    """Send visual synthesis alert"""
    try:
        side_ar = "شراء" if signal['side'] == "BUY" else "بيع"
        
        message = f"""
🎯 **التوليف البصري - الطريقة العربية** 🎯

**{signal['symbol']}** | **دخول {side_ar}**

✅ **التحليل البصري:**
• **الفريمات:** {signal['timeframe_alignment']}
• **الموجة:** {signal['wave_structure']}
• **القوة:** {signal['strength_level']}
• **المؤشرات:** {signal['indicators_signal']}
• **الفوليوم:** {signal['volume_status']}

📊 **نتيجة التوليف:** {signal['synthesis_score']:.1%}

💰 **التنفيذ:**
الدخول: {signal['entry']:.2f}
وقف الخسارة: {signal['sl']:.2f}
هدف الربح: {signal['tp']:.2f}

🔍 **المنطق:**
أراقب كل الفريمات، أشوف المدى الموجي، القوة، المؤشرات، الفوليوم → أحدد الاتجاه

#الطريقة_البصرية #{side_ar}
"""
        
        await tg(message)
        log.info(f"تم إرسال التنبيه: {signal['symbol']}")
        
    except Exception as e:
        log.error(f"خطأ في التنبيه: {e}")

async def visual_scan_loop(exchange):
    """Main scanning loop"""
    while True:
        try:
            log.info("بدء المسح البصري...")
            
            # Get top pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, t['quoteVolume']) for s, t in tickers.items() 
                         if s.endswith('/USDT') and t.get('quoteVolume', 0) > 1000000]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                try:
                    data = await fetch_all_timeframe_data(exchange, symbol)
                    if not data:
                        continue
                    
                    signal = await visual_synthesis_analysis(data, symbol)
                    
                    if signal:
                        if await save_signal(signal):
                            await send_visual_alert(signal)
                            signals_found += 1
                    
                    await asyncio.sleep(0.2)
                    
                except Exception as e:
                    log.debug(f"خطأ {symbol}: {e}")
                    continue
            
            log.info(f"تم المسح. وجدت {signals_found} إشارة.")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"خطأ في حلقة المسح: {e}")
            await asyncio.sleep(30)

async def monitor_loop(exchange):
    """Monitor trades"""
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
                                await tg(f"✅ {symbol} الهدف تم | دخول: {entry:.2f} → هدف: {tp:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price <= sl:
                                await tg(f"❌ {symbol} وقف الخسارة | دخول: {entry:.2f} → وقف: {sl:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                        else:
                            if current_price <= tp:
                                await tg(f"✅ {symbol} الهدف تم | دخول: {entry:.2f} → هدف: {tp:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                            elif current_price >= sl:
                                await tg(f"❌ {symbol} وقف الخسارة | دخول: {entry:.2f} → وقف: {sl:.2f}")
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                    
                    except Exception as e:
                        log.debug(f"خطأ في المتابعة {symbol}: {e}")
                
                await db_conn.commit()
            
            await asyncio.sleep(15)
            
        except Exception as e:
            log.error(f"خطأ في المتابعة: {e}")
            await asyncio.sleep(30)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "visual_synthesis_scanner",
        "method": "الطريقة البصرية العربية",
        "timeframes": list(TIMEFRAMES.values())
    }

@app.get("/signals")
async def get_signals():
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT symbol, side, entry, sl, tp, synthesis_score, timestamp 
                FROM signals WHERE status='OPEN' ORDER BY timestamp DESC LIMIT 10
            """) as cursor:
                rows = await cursor.fetchall()
        
        signals = []
        for row in rows:
            signals.append({
                "symbol": row[0],
                "side": row[1],
                "entry": row[2],
                "sl": row[3],
                "tp": row[4],
                "score": row[5],
                "time": row[6]
            })
        
        return {"signals": signals}
    except Exception as e:
        return {"error": str(e)}

# ---------------- MAIN ----------------
async def main():
    global exchange
    
    log.info("="*60)
    log.info("🎯 الماسح الضوئي البصري - الطريقة العربية")
    log.info("أراقب كل الفريمات، المدى الموجي، القوة، المؤشرات، الفوليوم → أحدد الاتجاه")
    log.info("="*60)
    
    try:
        if not await init_db():
            return
        
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        
        await exchange.fetch_ticker("BTC/USDT")
        log.info("✅ تم الاتصال بالبورصة")
        
        # Startup message in Arabic
        await tg(f"""
🎯 **بدء الماسح الضوئي البصري**

**الطريقة:**
أراقب كل الفريمات، أشوف المدى الموجي، القوة، المؤشرات، الفوليوم → أحدد الاتجاه

**الفريمات المستخدمة:**
{', '.join(TIMEFRAMES.values())}

**المسح:**
{TOP_N} زوج

جاهز للبدء...
        """)
        
        # Start scanning
        await asyncio.gather(
            visual_scan_loop(exchange),
            monitor_loop(exchange)
        )
        
    except KeyboardInterrupt:
        log.info("تم الإيقاف بواسطة المستخدم")
        await tg("🛑 توقف الماسح الضوئي")
    except Exception as e:
        log.error(f"خطأ فادح: {e}")
        await tg(f"❌ تعطل الماسح: {str(e)[:200]}")
    finally:
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("إغلاق نظيف")

if __name__ == "__main__":
    asyncio.run(main())