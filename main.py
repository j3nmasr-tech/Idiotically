#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visual Synthesis Scanner - الطريقة البصرية
الإصدار الكامل مع المتابعة التلقائية والتقارير
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
    """Initialize database with proper schema"""
    global db_conn
    try:
        if os.path.exists(DB_PATH):
            log.info(f"Connecting to existing database")
        
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
        # Create table if doesn't exist
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
                timeframe_alignment TEXT,
                wave_structure TEXT,
                strength_level TEXT,
                indicators_signal TEXT,
                volume_status TEXT,
                synthesis_score REAL,
                close_reason TEXT,
                close_price REAL,
                close_timestamp DATETIME,
                pnl_percent REAL,
                signal_hash TEXT UNIQUE
            )
        """)
        
        # Add columns if they don't exist
        columns_to_add = [
            "close_reason TEXT",
            "close_price REAL",
            "close_timestamp DATETIME",
            "pnl_percent REAL"
        ]
        
        for column_def in columns_to_add:
            try:
                column_name = column_def.split()[0]
                await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {column_def}")
                log.info(f"Added column: {column_name}")
            except Exception:
                pass  # Column already exists
        
        await db_conn.commit()
        log.info("✅ Database ready")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ VISUAL SYNTHESIS METHOD ================

def analyze_timeframes_visual(data: Dict[str, pd.DataFrame]) -> Tuple[str, float, str]:
    """١. كل الفريمات (All timeframes)"""
    try:
        tf_directions = {}
        tf_strengths = {}
        
        for tf_name, df in data.items():
            prices = df['close'].values
            
            short_len = max(5, len(prices) // 20)
            short_trend = "UP" if prices[-1] > prices[-short_len] else "DOWN"
            
            med_len = max(10, len(prices) // 5)
            med_trend = "UP" if prices[-1] > prices[-med_len] else "DOWN"
            
            long_trend = "UP" if prices[-1] > prices[0] else "DOWN"
            
            trends = [short_trend, med_trend, long_trend]
            direction = max(set(trends), key=trends.count)
            
            trend_agreement = trends.count(direction) / len(trends)
            
            tf_directions[tf_name] = direction
            tf_strengths[tf_name] = trend_agreement
        
        directions = list(tf_directions.values())
        
        if all(d == "UP" for d in directions):
            alignment_score = min(tf_strengths.values())
            return "UP", alignment_score, "✅ جميع الفريمات صاعدة"
        
        elif all(d == "DOWN" for d in directions):
            alignment_score = min(tf_strengths.values())
            return "DOWN", alignment_score, "✅ جميع الفريمات هابطة"
        
        else:
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
    """٢. المدى الموجي (Wave range)"""
    try:
        h4_df = data['H4']
        prices = h4_df['close'].values[-50:]
        
        swing_highs = []
        swing_lows = []
        
        for i in range(3, len(prices)-3):
            if all(prices[i] > prices[i+j] for j in [-3, -2, -1, 1, 2, 3]):
                swing_highs.append(prices[i])
            
            if all(prices[i] < prices[i+j] for j in [-3, -2, -1, 1, 2, 3]):
                swing_lows.append(prices[i])
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            last_highs = swing_highs[-2:]
            last_lows = swing_lows[-2:]
            
            if direction == "UP":
                higher_highs = last_highs[-1] > last_highs[-2]
                higher_lows = last_lows[-1] > last_lows[-2]
                
                if higher_highs and higher_lows:
                    wave_score = 0.9
                    reason = "📈 موجات صاعدة واضحة"
                elif higher_lows:
                    wave_score = 0.7
                    reason = "📈 قيعان مرتفعة"
                else:
                    wave_score = 0.3
                    reason = "⚠️ موجة صاعدة ضعيفة"
            
            else:
                lower_highs = last_highs[-1] < last_highs[-2]
                lower_lows = last_lows[-1] < last_lows[-2]
                
                if lower_highs and lower_lows:
                    wave_score = 0.9
                    reason = "📉 موجات هابطة واضحة"
                elif lower_highs:
                    wave_score = 0.7
                    reason = "📉 قمم منخفضة"
                else:
                    wave_score = 0.3
                    reason = "⚠️ موجة هابطة ضعيفة"
            
            recent_range = max(prices[-20:]) - min(prices[-20:])
            avg_range = np.mean([max(prices[i-20:i]) - min(prices[i-20:i]) for i in range(40, len(prices), 10) if i-20 >= 0])
            
            if recent_range < avg_range * 0.7:
                wave_score += 0.1
                reason += " | 📊 مدى مضغوط"
        
        else:
            wave_score = 0.5
            reason = "🔍 موجات غير واضحة"
        
        return min(wave_score, 1.0), reason
    
    except Exception as e:
        return 0.5, f"خطأ في الموجات: {str(e)}"

def analyze_strength_visual(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """٣. القوة (Strength)"""
    try:
        h1_df = data['H1']
        recent = h1_df.iloc[-10:]
        
        body_sizes = abs(recent['close'] - recent['open'])
        avg_body = body_sizes.mean()
        
        if direction == "UP":
            bullish_bodies = recent[recent['close'] > recent['open']]
            if not bullish_bodies.empty:
                avg_bullish_body = bullish_bodies['close'] - bullish_bodies['open']
                body_strength = avg_bullish_body.mean() / avg_body if avg_body > 0 else 0
            else:
                body_strength = 0
        else:
            bearish_bodies = recent[recent['close'] < recent['open']]
            if not bearish_bodies.empty:
                avg_bearish_body = bearish_bodies['open'] - bearish_bodies['close']
                body_strength = avg_bearish_body.mean() / avg_body if avg_body > 0 else 0
            else:
                body_strength = 0
        
        if direction == "UP":
            close_positions = (recent['close'] - recent['low']) / (recent['high'] - recent['low']).replace(0, 0.001)
            close_strength = close_positions.mean()
        else:
            close_positions = (recent['high'] - recent['close']) / (recent['high'] - recent['low']).replace(0, 0.001)
            close_strength = close_positions.mean()
        
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
        
        strength_score = (body_strength * 0.4 + close_strength * 0.4 + momentum * 0.2)
        
        if strength_score > 0.7:
            strength_text = "قوية جدا"
        elif strength_score > 0.5:
            strength_text = "قوية"
        elif strength_score > 0.3:
            strength_text = "متوسطة"
        else:
            strength_text = "ضعيفة"
        
        reason = f"💪 القوة: {strength_text}"
        
        return min(strength_score, 1.0), reason
    
    except Exception as e:
        return 0.5, f"خطأ في القوة: {str(e)}"

def analyze_indicators_visual(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """٤. المؤشرات (Indicators)"""
    try:
        m15_df = data['M15']
        prices = m15_df['close'].values
        
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
        
        if len(prices) >= 26:
            ema_12 = pd.Series(prices[-26:]).ewm(span=12, adjust=False).mean().iloc[-1]
            ema_26 = pd.Series(prices[-26:]).ewm(span=26, adjust=False).mean().iloc[-1]
            macd_line = ema_12 - ema_26
            macd_hist = pd.Series([macd_line]).ewm(span=9, adjust=False).mean().iloc[-1]
            histogram = macd_line - macd_hist
        else:
            histogram = 0
        
        rsi_signal = ""
        if direction == "UP":
            if rsi < 40:
                rsi_score = 0.8
                rsi_signal = "RSI منخفض"
            elif rsi < 60:
                rsi_score = 0.5
                rsi_signal = "RSI محايد"
            else:
                rsi_score = 0.2
                rsi_signal = "RSI مرتفع"
        else:
            if rsi > 60:
                rsi_score = 0.8
                rsi_signal = "RSI مرتفع"
            elif rsi > 40:
                rsi_score = 0.5
                rsi_signal = "RSI محايد"
            else:
                rsi_score = 0.2
                rsi_signal = "RSI منخفض"
        
        macd_signal = ""
        if direction == "UP":
            if histogram > 0:
                macd_score = 0.8
                macd_signal = "MACD إيجابي"
            else:
                macd_score = 0.3
                macd_signal = "MACD سلبي"
        else:
            if histogram < 0:
                macd_score = 0.8
                macd_signal = "MACD سلبي"
            else:
                macd_score = 0.3
                macd_signal = "MACD إيجابي"
        
        indicators_score = (rsi_score + macd_score) / 2
        
        reason = f"📊 {rsi_signal} ({rsi:.0f}) | {macd_signal}"
        
        return indicators_score, reason
    
    except Exception as e:
        return 0.5, f"خطأ في المؤشرات: {str(e)}"

def analyze_volume_visual(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """٥. الفوليوم (Volume)"""
    try:
        volume_checks = []
        reasons = []
        
        for tf_name in ['H1', 'M15']:
            if tf_name in data:
                df = data[tf_name]
                
                recent_vol = df['volume'].values[-10:]
                prev_vol = df['volume'].values[-20:-10]
                
                if len(recent_vol) > 0 and len(prev_vol) > 0:
                    avg_recent = np.mean(recent_vol)
                    avg_prev = np.mean(prev_vol)
                    
                    if avg_prev > 0:
                        volume_ratio = avg_recent / avg_prev
                    else:
                        volume_ratio = 1
                    
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
                    reasons.append(f"{tf_name}: {strength}")
        
        if volume_checks:
            volume_score = np.mean(volume_checks)
            reason = "📈 " + " | ".join(reasons)
        else:
            volume_score = 0.5
            reason = "📈 غير متوفر"
        
        return volume_score, reason
    
    except Exception as e:
        return 0.5, f"خطأ في الفوليوم: {str(e)}"

def synthesize_direction_visual(scores: Dict[str, float], reasons: Dict[str, str]) -> Tuple[str, float, str]:
    """٦. أحدد الاتجاه (Determine direction)"""
    try:
        weights = {
            'timeframes': 0.25,
            'wave': 0.20,
            'strength': 0.20,
            'indicators': 0.15,
            'volume': 0.20
        }
        
        total_score = 0
        for factor, score in scores.items():
            total_score += score * weights.get(factor, 0.2)
        
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
            return "NEUTRAL", total_score, "❌ ضعيفة"
        
        synthesis_reason = f"📊 النتيجة: {total_score:.1%} - {signal_strength}"
        
        return direction, total_score, synthesis_reason
    
    except Exception as e:
        return "NEUTRAL", 0, f"خطأ في التوليف: {str(e)}"

def calculate_visual_entry(scores: Dict[str, float], direction: str, current_price: float, data: Dict[str, pd.DataFrame]) -> Tuple[float, float, str]:
    """Calculate entry based on visual analysis"""
    h4_df = data['H4']
    
    if direction == "UP":
        recent_low = h4_df['low'].iloc[-10:].min()
        sl = recent_low * 0.99
        
        base_risk = current_price - sl
        
        if scores.get('strength_score', 0) > 0.7:
            rr_ratio = 2.0
        elif scores.get('strength_score', 0) > 0.5:
            rr_ratio = 1.5
        else:
            rr_ratio = 1.2
        
        tp = current_price + (base_risk * rr_ratio)
        
        logic = f"SL: تحت {recent_low:.2f} | TP: نسبة {rr_ratio:.1f}:1"
    
    else:
        recent_high = h4_df['high'].iloc[-10:].max()
        sl = recent_high * 1.01
        
        base_risk = sl - current_price
        
        if scores.get('strength_score', 0) > 0.7:
            rr_ratio = 2.0
        elif scores.get('strength_score', 0) > 0.5:
            rr_ratio = 1.5
        else:
            rr_ratio = 1.2
        
        tp = current_price - (base_risk * rr_ratio)
        
        logic = f"SL: فوق {recent_high:.2f} | TP: نسبة {rr_ratio:.1f}:1"
    
    return sl, tp, logic

# ================ MAIN VISUAL ANALYSIS ================

async def visual_synthesis_analysis(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """Perform visual synthesis analysis"""
    try:
        log.info(f"🔍 تحليل {symbol}...")
        
        direction, tf_score, tf_reason = analyze_timeframes_visual(data)
        
        if direction == "NEUTRAL":
            log.debug(f"❌ {symbol}: لا اتجاه واضح")
            return None
        
        wave_score, wave_reason = analyze_wave_range_visual(data, direction)
        strength_score, strength_reason = analyze_strength_visual(data, direction)
        indicators_score, indicators_reason = analyze_indicators_visual(data, direction)
        volume_score, volume_reason = analyze_volume_visual(data, direction)
        
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
        
        final_direction, synthesis_score, synthesis_reason = synthesize_direction_visual(scores, reasons)
        
        if final_direction == "NEUTRAL" or synthesis_score < 0.6:
            log.info(f"❌ {symbol}: نتيجة غير كافية ({synthesis_score:.1%})")
            return None
        
        log.info(f"✅ {symbol}: {final_direction} - {synthesis_score:.1%}")
        
        current_price = data['M15']['close'].iloc[-1]
        side = "BUY" if final_direction == "UP" else "SELL"
        
        sl, tp, entry_logic = calculate_visual_entry(scores, final_direction, current_price, data)
        
        risk = abs(current_price - sl)
        reward = abs(tp - current_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
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
        
        log.info(f"🎯 {symbol} {side} @ {current_price:.2f} | R:R {rr_ratio:.1f}:1")
        
        return signal
        
    except Exception as e:
        log.error(f"خطأ في {symbol}: {e}")
        return None

# ================ DATA FETCHING ================

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
            log.debug(f"خطأ في {tf}: {e}")
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

# ================ MONITORING SYSTEM ================

async def monitor_open_positions(exchange):
    """Monitor and update open positions"""
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp FROM signals 
                    WHERE status='OPEN'
                """) as cursor:
                    open_signals = await cursor.fetchall()
            
            if not open_signals:
                await asyncio.sleep(10)
                continue
            
            log.debug(f"📊 Monitoring {len(open_signals)} open positions")
            
            for sig_id, symbol, side, entry, sl, tp in open_signals:
                try:
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker.get('last')
                    
                    if not current_price:
                        continue
                    
                    should_update = False
                    close_reason = ""
                    pnl_percent = 0
                    
                    if side == "BUY":
                        if current_price >= tp:
                            close_reason = "TP_HIT"
                            pnl_percent = ((current_price - entry) / entry) * 100
                            should_update = True
                            
                        elif current_price <= sl:
                            close_reason = "SL_HIT"
                            pnl_percent = ((current_price - entry) / entry) * 100
                            should_update = True
                    
                    else:
                        if current_price <= tp:
                            close_reason = "TP_HIT"
                            pnl_percent = ((entry - current_price) / entry) * 100
                            should_update = True
                            
                        elif current_price >= sl:
                            close_reason = "SL_HIT"
                            pnl_percent = ((entry - current_price) / entry) * 100
                            should_update = True
                    
                    if should_update:
                        async with db_lock:
                            await db_conn.execute("""
                                UPDATE signals SET 
                                    status = 'CLOSED',
                                    close_reason = ?,
                                    close_price = ?,
                                    close_timestamp = CURRENT_TIMESTAMP,
                                    pnl_percent = ?
                                WHERE id = ?
                            """, (close_reason, current_price, pnl_percent, sig_id))
                            
                            await db_conn.commit()
                        
                        if close_reason == "TP_HIT":
                            emoji = "✅"
                            result = "هدف الربح"
                        else:
                            emoji = "❌"
                            result = "وقف الخسارة"
                        
                        side_ar = "شراء" if side == "BUY" else "بيع"
                        await tg(f"""
{emoji} **تم إغلاق الصفقة**

**{symbol}** | **{side_ar}**
النتيجة: {result}

الدخول: {entry:.2f}
الإغلاق: {current_price:.2f}
{result}: {tp if close_reason == 'TP_HIT' else sl:.2f}

الأداء: {'+' if pnl_percent > 0 else ''}{pnl_percent:.1f}%

#إغلاق
""")
                        
                        log.info(f"{emoji} {symbol}: {close_reason} | PnL: {pnl_percent:.1f}%")
                
                except Exception as e:
                    log.error(f"Monitor error {symbol}: {e}")
                    continue
            
            await asyncio.sleep(10)
            
        except Exception as e:
            log.error(f"Monitor loop error: {e}")
            await asyncio.sleep(30)

async def generate_daily_report():
    """Generate daily performance report"""
    while True:
        try:
            await asyncio.sleep(86400)
            
            async with db_lock:
                async with db_conn.execute("""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN pnl_percent < 0 THEN 1 ELSE 0 END) as losing_trades,
                        AVG(pnl_percent) as avg_pnl,
                        SUM(pnl_percent) as total_pnl
                    FROM signals 
                    WHERE DATE(timestamp) = DATE('now', '-1 day')
                    AND status = 'CLOSED'
                """) as cursor:
                    stats = await cursor.fetchone()
                
                if stats and stats[0] > 0:
                    total_trades, winning, losing, avg_pnl, total_pnl = stats
                    
                    win_rate = (winning / total_trades * 100) if total_trades > 0 else 0
                    
                    await tg(f"""
📊 **تقرير الأمس**

الإحصائيات:
• إجمالي الصفقات: {total_trades}
• الصفقات الرابحة: {winning} ({win_rate:.1f}%)
• الصفقات الخاسرة: {losing}
• متوسط الربح/الخسارة: {avg_pnl:.1f}%
• إجمالي الربح/الخسارة: {total_pnl:.1f}%

{"🔥 أداء ممتاز" if win_rate > 60 and total_pnl > 0 else "✅ أداء جيد" if win_rate > 50 else "⚠️ يحتاج تحسين"}

#تقرير
""")
            
        except Exception as e:
            log.error(f"Report error: {e}")

# ================ SCANNING LOOP ================

async def send_visual_alert(signal: Dict):
    """Send visual synthesis alert"""
    try:
        side_ar = "شراء" if signal['side'] == "BUY" else "بيع"
        
        risk = abs(signal['entry'] - signal['sl'])
        reward = abs(signal['tp'] - signal['entry'])
        rr_ratio = reward / risk if risk > 0 else 0
        
        message = f"""
🎯 **إشارة جديدة**

**{signal['symbol']}** | **دخول {side_ar}**

📊 التحليل:
• الفريمات: {signal['timeframe_alignment']}
• الموجة: {signal['wave_structure']}
• القوة: {signal['strength_level']}
• المؤشرات: {signal['indicators_signal']}
• الفوليوم: {signal['volume_status']}

📈 الجودة: {signal['synthesis_score']:.1%}

💰 التنفيذ:
الدخول: {signal['entry']:.2f}
وقف الخسارة: {signal['sl']:.2f}
هدف الربح: {signal['tp']:.2f}
نسبة الربح/المخاطرة: {rr_ratio:.1f}:1

⏰ المتابعة التلقائية مفعلة

#إشارة_جديدة #{side_ar}
"""
        
        await tg(message)
        log.info(f"تم إرسال التنبيه: {signal['symbol']}")
        
    except Exception as e:
        log.error(f"خطأ في التنبيه: {e}")

async def visual_scan_loop(exchange):
    """Main scanning loop"""
    while True:
        try:
            log.info("بدء المسح...")
            
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
                        async with db_lock:
                            async with db_conn.execute("""
                                SELECT COUNT(*) FROM signals 
                                WHERE symbol = ? AND side = ? 
                                AND timestamp > datetime('now', '-6 hours')
                                AND status = 'OPEN'
                            """, (symbol, signal['side'])) as cursor:
                                recent_signals = (await cursor.fetchone())[0]
                            
                            if recent_signals == 0:
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
            log.error(f"خطأ في المسح: {e}")
            await asyncio.sleep(30)

# ================ WEB API ================

app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "Visual Synthesis Scanner",
        "version": "1.0",
        "features": ["Auto-scan", "Auto-monitoring", "TP/SL alerts", "Daily reports"]
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": time.time()}

@app.get("/stats")
async def stats():
    try:
        async with db_lock:
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status='OPEN'") as cursor:
                open_count = (await cursor.fetchone())[0]
            
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status='CLOSED'") as cursor:
                closed_count = (await cursor.fetchone())[0]
            
            async with db_conn.execute("""
                SELECT 
                    AVG(pnl_percent) as avg_pnl,
                    COUNT(*) as total_closed
                FROM signals 
                WHERE status = 'CLOSED'
            """) as cursor:
                performance = await cursor.fetchone()
        
        return {
            "open_positions": open_count,
            "closed_positions": closed_count,
            "average_pnl": round(performance[0], 2) if performance[0] else 0,
            "total_closed_trades": performance[1] if performance[1] else 0
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/signals/open")
async def get_open_signals():
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT symbol, side, entry, sl, tp, synthesis_score, timestamp 
                FROM signals WHERE status='OPEN' ORDER BY timestamp DESC
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
        
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        return {"error": str(e)}

@app.get("/signals/closed")
async def get_closed_signals(limit: int = 20):
    try:
        async with db_lock:
            async with db_conn.execute("""
                SELECT symbol, side, entry, close_price, pnl_percent, close_reason, timestamp 
                FROM signals WHERE status='CLOSED' 
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
        
        signals = []
        for row in rows:
            signals.append({
                "symbol": row[0],
                "side": row[1],
                "entry": row[2],
                "close_price": row[3],
                "pnl_percent": row[4],
                "close_reason": row[5],
                "time": row[6]
            })
        
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        return {"error": str(e)}

# ================ MAIN ================

async def main():
    global exchange
    
    log.info("="*60)
    log.info("🚀 Visual Synthesis Scanner Starting")
    log.info("Auto-scan + Auto-monitoring + Reports")
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
        
        await tg(f"""
🚀 **Visual Synthesis Scanner Started**

**Features:**
• Auto-scanning (visual synthesis method)
• Auto-monitoring of open positions
• Instant TP/SL notifications
• Daily performance reports
• Web API for monitoring

**Ready to scan {TOP_N} pairs!**
""")
        
        await asyncio.gather(
            visual_scan_loop(exchange),
            monitor_open_positions(exchange),
            generate_daily_report()
        )
        
    except KeyboardInterrupt:
        log.info("Stopped by user")
        await tg("🛑 Scanner stopped manually")
    except Exception as e:
        log.error(f"Fatal error: {e}")
        await tg(f"❌ Scanner crashed: {str(e)[:200]}")
    finally:
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()
        log.info("Clean shutdown")

if __name__ == "__main__":
    asyncio.run(main())