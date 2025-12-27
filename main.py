#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🚨 نظام الإنذار المبكر للحركات العنيفة
Early Warning System for Fast Drops/Explosions
"""

import os
import time
import asyncio
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import ccxt.async_support as ccxt
import aiosqlite
from datetime import datetime, timedelta
import json

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/violent_moves.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 120))  # 2 دقائق بين كل مسح
MIN_VOLUME_USDT = 50000  # أقل حجم لقبول الزوج ($50K)

# فريمات المراقبة للحركات العنيفة
TIMEFRAMES = {
    "H4": "4h",      # للموجه المتوسطة
    "H1": "1h",      # للموجه القصيرة
    "M15": "15m",    # للتغيرات السريعة
    "M5": "5m"       # للحركات العنيفة الفورية
}

# حدود الكشف للحركات العنيفة
VIOLENT_THRESHOLDS = {
    'FAST_DROP': -5.0,      # -5% في 15 دقيقة
    'FAST_EXPLOSION': 5.0,  # +5% في 15 دقيقة
    'VOLUME_SPIKE': 3.0,    # 3x حجم متوسط
    'VOLATILITY_SPIKE': 2.5 # 2.5x تقلب عادي
}

# إعدادات المؤشرات للكشف المبكر
INDICATOR_SETTINGS = {
    'RSI_PERIOD': 7,        # RSI قصير للحساسية
    'EMA_FAST': 9,          # EMA سريع
    'EMA_SLOW': 21,         # EMA بطيء
    'VOLUME_MA': 20,        # متوسط الحجم
    'PRICE_ACCELERATION': 1.5  # تسارع السعر
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("violent_move_detector")

# ================ TELEGRAM UTILITIES ================
async def send_telegram_alert(message: str):
    """إرسال إنذار إلى تليجرام"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
        return False
    
    try:
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }
            response = await client.post(url, json=payload)
            return response.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False

# ================ CALCULATION FUNCTIONS ================
def calculate_ema(prices: np.ndarray, period: int) -> np.ndarray:
    """حساب EMA"""
    if len(prices) < period:
        return np.full(len(prices), np.nan)
    
    alpha = 2 / (period + 1)
    ema = np.zeros_like(prices, dtype=float)
    ema[period-1] = np.mean(prices[:period])
    
    for i in range(period, len(prices)):
        ema[i] = alpha * prices[i] + (1 - alpha) * ema[i-1]
    
    ema[:period-1] = np.nan
    return ema

def calculate_rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """حساب RSI"""
    if len(prices) < period + 1:
        return np.full(len(prices), np.nan)
    
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    rsi = np.full(len(prices), np.nan)
    
    if avg_loss == 0:
        rsi[period] = 100
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100 - (100 / (1 + rs))
    
    for i in range(period + 1, len(prices)):
        gain = gains[i-1]
        loss = losses[i-1]
        
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        
        if avg_loss == 0:
            rsi[i] = 100
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """حساب Average True Range للتقلب"""
    if len(high) < period:
        return np.full(len(high), np.nan)
    
    tr = np.zeros(len(high))
    tr[0] = high[0] - low[0]
    
    for i in range(1, len(high)):
        hl = high[i] - low[i]
        hc = abs(high[i] - close[i-1])
        lc = abs(low[i] - close[i-1])
        tr[i] = max(hl, hc, lc)
    
    atr = np.zeros(len(high))
    atr[period-1] = np.mean(tr[:period])
    
    for i in range(period, len(high)):
        atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
    
    atr[:period-1] = np.nan
    return atr

# ================ VIOLENT MOVE DETECTORS ================
class ViolentMoveDetector:
    """كاشف الحركات العنيفة السريعة"""
    
    @staticmethod
    def detect_fast_drop(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
        """كشف الانهيار السريع (-5% أو أكثر في 15 دقيقة)"""
        try:
            m15_df = data.get('M15')
            if m15_df is None or len(m15_df) < 10:
                return None
            
            # تحليل آخر 6 شمعات M15 (90 دقيقة)
            recent_prices = m15_df['close'].values[-6:]
            recent_highs = m15_df['high'].values[-6:]
            recent_lows = m15_df['low'].values[-6:]
            recent_volumes = m15_df['volume'].values[-6:]
            
            current_price = recent_prices[-1]
            price_15m_ago = recent_prices[-2] if len(recent_prices) >= 2 else recent_prices[0]
            
            # حساب التغير في 15 دقيقة
            if price_15m_ago > 0:
                change_15m = ((current_price - price_15m_ago) / price_15m_ago) * 100
            else:
                change_15m = 0
            
            # حساب تسارع الهبوط
            if len(recent_prices) >= 3:
                change_1 = ((recent_prices[-1] - recent_prices[-2]) / recent_prices[-2]) * 100 if recent_prices[-2] > 0 else 0
                change_2 = ((recent_prices[-2] - recent_prices[-3]) / recent_prices[-3]) * 100 if recent_prices[-3] > 0 else 0
                acceleration = abs(change_1) - abs(change_2)  # تسارع سالب = هبوط أسرع
            else:
                acceleration = 0
            
            # تحليل الحجم
            avg_volume = np.mean(recent_volumes[:-1]) if len(recent_volumes) > 1 else recent_volumes[0]
            current_volume = recent_volumes[-1]
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            
            # تحليل التقلب
            ranges = [(recent_highs[i] - recent_lows[i]) / recent_prices[i] for i in range(len(recent_prices))]
            current_range = ranges[-1] if len(ranges) > 0 else 0
            avg_range = np.mean(ranges[:-1]) if len(ranges) > 1 else current_range
            volatility_ratio = current_range / avg_range if avg_range > 0 else 1
            
            # شروط الانهيار السريع
            is_fast_drop = (
                change_15m < VIOLENT_THRESHOLDS['FAST_DROP'] and  # هبوط أكثر من 5%
                volume_ratio > VIOLENT_THRESHOLDS['VOLUME_SPIKE'] and  # حجم مرتفع
                acceleration < -0.5  # تسارع في الهبوط
            )
            
            if not is_fast_drop:
                return None
            
            # حساب قوة الإشارة
            signal_strength = 0
            signals = []
            
            if change_15m < -7:
                signal_strength += 30
                signals.append(f"هبوط عنيف: {change_15m:.1f}%")
            elif change_15m < -5:
                signal_strength += 20
                signals.append(f"هبوط سريع: {change_15m:.1f}%")
            
            if volume_ratio > 4:
                signal_strength += 25
                signals.append(f"حجم هائل: {volume_ratio:.1f}x")
            elif volume_ratio > 3:
                signal_strength += 15
                signals.append(f"حجم كبير: {volume_ratio:.1f}x")
            
            if volatility_ratio > 3:
                signal_strength += 20
                signals.append(f"تقلب عالي: {volatility_ratio:.1f}x")
            
            if acceleration < -1:
                signal_strength += 15
                signals.append("تسارع هبوطي")
            
            # تحليل RSI قصير المدى
            rsi = calculate_rsi(recent_prices, INDICATOR_SETTINGS['RSI_PERIOD'])
            current_rsi = rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50
            
            if current_rsi < 25:
                signal_strength += 10
                signals.append(f"RSI تشبع بيع: {current_rsi:.1f}")
            
            # تحقق من وجود انهيار حقيقي (ليس مجرد تصحيح)
            if len(recent_prices) >= 10:
                price_1h_ago = recent_prices[0] if len(recent_prices) >= 4 else recent_prices[0]
                change_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100 if price_1h_ago > 0 else 0
                
                if change_1h < -8:  # هبوط أكثر من 8% في الساعة
                    signal_strength += 25
                    signals.append(f"هبوط قوي في الساعة: {change_1h:.1f}%")
            
            if signal_strength < 50:  # عتبة القبول
                return None
            
            return {
                'symbol': symbol,
                'type': 'FAST_DROP',
                'current_price': current_price,
                'change_15m_pct': change_15m,
                'change_1h_pct': change_1h if 'change_1h' in locals() else 0,
                'volume_ratio': volume_ratio,
                'volatility_ratio': volatility_ratio,
                'acceleration': acceleration,
                'current_rsi': current_rsi,
                'signal_strength': signal_strength,
                'signals': signals,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            log.error(f"Error detecting fast drop for {symbol}: {e}")
            return None
    
    @staticmethod
    def detect_fast_explosion(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
        """كشف الانفجار السريع (+5% أو أكثر في 15 دقيقة)"""
        try:
            m15_df = data.get('M15')
            h1_df = data.get('H1')
            
            if m15_df is None or len(m15_df) < 10:
                return None
            
            # تحليل M15 للحركة السريعة
            recent_prices = m15_df['close'].values[-6:]
            recent_highs = m15_df['high'].values[-6:]
            recent_lows = m15_df['low'].values[-6:]
            recent_volumes = m15_df['volume'].values[-6:]
            
            current_price = recent_prices[-1]
            price_15m_ago = recent_prices[-2] if len(recent_prices) >= 2 else recent_prices[0]
            
            # حساب التغير في 15 دقيقة
            if price_15m_ago > 0:
                change_15m = ((current_price - price_15m_ago) / price_15m_ago) * 100
            else:
                change_15m = 0
            
            # حساب تسارع الصعود
            if len(recent_prices) >= 3:
                change_1 = ((recent_prices[-1] - recent_prices[-2]) / recent_prices[-2]) * 100 if recent_prices[-2] > 0 else 0
                change_2 = ((recent_prices[-2] - recent_prices[-3]) / recent_prices[-3]) * 100 if recent_prices[-3] > 0 else 0
                acceleration = change_1 - change_2  # تسارع موجب = صعود أسرع
            else:
                acceleration = 0
            
            # تحليل الحجم
            avg_volume = np.mean(recent_volumes[:-1]) if len(recent_volumes) > 1 else recent_volumes[0]
            current_volume = recent_volumes[-1]
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            
            # تحليل كسر المقاومة على H1
            resistance_broken = False
            if h1_df is not None and len(h1_df) >= 20:
                h1_highs = h1_df['high'].values[-20:]
                significant_highs = []
                
                # البحث عن قمم سابقة كمقاومات
                for i in range(2, len(h1_highs)-2):
                    if (h1_highs[i] > h1_highs[i-1] and h1_highs[i] > h1_highs[i-2] and
                        h1_highs[i] > h1_highs[i+1] and h1_highs[i] > h1_highs[i+2]):
                        significant_highs.append(h1_highs[i])
                
                if significant_highs:
                    nearest_resistance = min(significant_highs, key=lambda x: abs(x - current_price))
                    if current_price > nearest_resistance:
                        resistance_broken = True
            
            # شروط الانفجار السريع
            is_fast_explosion = (
                change_15m > VIOLENT_THRESHOLDS['FAST_EXPLOSION'] and  # صعود أكثر من 5%
                volume_ratio > VIOLENT_THRESHOLDS['VOLUME_SPIKE'] and  # حجم مرتفع
                acceleration > 0.5  # تسارع في الصعود
            )
            
            if not is_fast_explosion:
                return None
            
            # حساب قوة الإشارة
            signal_strength = 0
            signals = []
            
            if change_15m > 7:
                signal_strength += 30
                signals.append(f"صعود عنيف: {change_15m:.1f}%")
            elif change_15m > 5:
                signal_strength += 20
                signals.append(f"صعود سريع: {change_15m:.1f}%")
            
            if volume_ratio > 4:
                signal_strength += 25
                signals.append(f"حجم هائل: {volume_ratio:.1f}x")
            elif volume_ratio > 3:
                signal_strength += 15
                signals.append(f"حجم كبير: {volume_ratio:.1f}x")
            
            if resistance_broken:
                signal_strength += 20
                signals.append("كسر مقاومة رئيسية")
            
            if acceleration > 1:
                signal_strength += 15
                signals.append("تسارع صعودي")
            
            # تحليل RSI
            rsi = calculate_rsi(recent_prices, INDICATOR_SETTINGS['RSI_PERIOD'])
            current_rsi = rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50
            
            if current_rsi > 75:
                signal_strength += 10
                signals.append(f"RSI تشبع شراء: {current_rsi:.1f}")
            
            # تحليل EMA للتأكد من الاتجاه
            ema_fast = calculate_ema(recent_prices, INDICATOR_SETTINGS['EMA_FAST'])
            ema_slow = calculate_ema(recent_prices, INDICATOR_SETTINGS['EMA_SLOW'])
            
            if len(ema_fast) > 0 and len(ema_slow) > 0 and not np.isnan(ema_fast[-1]) and not np.isnan(ema_slow[-1]):
                if ema_fast[-1] > ema_slow[-1]:
                    signal_strength += 10
                    signals.append("EMA سريع فوق البطيء")
            
            if signal_strength < 50:
                return None
            
            return {
                'symbol': symbol,
                'type': 'FAST_EXPLOSION',
                'current_price': current_price,
                'change_15m_pct': change_15m,
                'volume_ratio': volume_ratio,
                'acceleration': acceleration,
                'current_rsi': current_rsi,
                'resistance_broken': resistance_broken,
                'signal_strength': signal_strength,
                'signals': signals,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            log.error(f"Error detecting fast explosion for {symbol}: {e}")
            return None
    
    @staticmethod
    def detect_pre_explosion_signals(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
        """كشف إشارات ما قبل الانفجار (تراكم + تكثف)"""
        try:
            h1_df = data.get('H1')
            h4_df = data.get('H4')
            
            if h1_df is None or h4_df is None:
                return None
            
            # تحليل H4 للتكثف
            h4_prices = h4_df['close'].values[-30:]
            h4_highs = h4_df['high'].values[-30:]
            h4_lows = h4_df['low'].values[-30:]
            h4_volumes = h4_df['volume'].values[-30:]
            
            # حساب التقلب على H4
            ranges_h4 = [(h4_highs[i] - h4_lows[i]) / h4_prices[i] for i in range(len(h4_prices))]
            recent_range_h4 = np.mean(ranges_h4[-5:]) if len(ranges_h4) >= 5 else 0
            avg_range_h4 = np.mean(ranges_h4[:-5]) if len(ranges_h4) > 5 else recent_range_h4
            
            compression_ratio = recent_range_h4 / avg_range_h4 if avg_range_h4 > 0 else 1
            
            # تحليل H1 للتراكم
            h1_prices = h1_df['close'].values[-50:]
            h1_volumes = h1_df['volume'].values[-50:]
            
            avg_volume_h1 = np.mean(h1_volumes[:-10]) if len(h1_volumes) > 10 else np.mean(h1_volumes)
            recent_volume_h1 = np.mean(h1_volumes[-10:]) if len(h1_volumes) >= 10 else np.mean(h1_volumes)
            volume_accumulation = recent_volume_h1 / avg_volume_h1 if avg_volume_h1 > 0 else 1
            
            # شروط ما قبل الانفجار
            is_pre_explosion = (
                compression_ratio < 0.6 and  # تكثف شديد (نطاق أقل من 60% من المعتاد)
                volume_accumulation > 1.5 and  # تراكم حجم
                h1_prices[-1] > np.mean(h1_prices[-20:])  # سعر فوق المتوسط
            )
            
            if not is_pre_explosion:
                return None
            
            signal_strength = 0
            signals = []
            
            if compression_ratio < 0.5:
                signal_strength += 30
                signals.append(f"تكثف عالي: {compression_ratio:.2f}x")
            elif compression_ratio < 0.6:
                signal_strength += 20
                signals.append(f"تكثف: {compression_ratio:.2f}x")
            
            if volume_accumulation > 2:
                signal_strength += 25
                signals.append(f"تراكم حجم: {volume_accumulation:.1f}x")
            elif volume_accumulation > 1.5:
                signal_strength += 15
                signals.append(f"حجم مرتفع: {volume_accumulation:.1f}x")
            
            # تحليل RSI على H4
            rsi_h4 = calculate_rsi(h4_prices, 14)
            current_rsi_h4 = rsi_h4[-1] if len(rsi_h4) > 0 and not np.isnan(rsi_h4[-1]) else 50
            
            if current_rsi_h4 < 40:  # RSI منخفض قبل الانفجار
                signal_strength += 15
                signals.append(f"RSI منخفض: {current_rsi_h4:.1f}")
            
            if signal_strength < 40:
                return None
            
            return {
                'symbol': symbol,
                'type': 'PRE_EXPLOSION',
                'current_price': h4_prices[-1],
                'compression_ratio': compression_ratio,
                'volume_accumulation': volume_accumulation,
                'current_rsi': current_rsi_h4,
                'signal_strength': signal_strength,
                'signals': signals,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            log.error(f"Error detecting pre-explosion for {symbol}: {e}")
            return None

# ================ DATABASE FUNCTIONS ================
async def init_violent_moves_db():
    """تهيئة قاعدة بيانات الحركات العنيفة"""
    try:
        db = await aiosqlite.connect(DB_PATH)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS violent_moves (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                move_type TEXT NOT NULL,
                price REAL NOT NULL,
                change_pct REAL,
                volume_ratio REAL,
                signal_strength INTEGER,
                signals TEXT,
                detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                alerted BOOLEAN DEFAULT FALSE,
                move_hash TEXT UNIQUE
            )
        """)
        
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_detected 
            ON violent_moves(symbol, detected_at)
        """)
        
        await db.commit()
        await db.close()
        log.info("✅ Violent moves database initialized")
        return True
        
    except Exception as e:
        log.error(f"Database init error: {e}")
        return False

async def save_violent_move(move: Dict) -> bool:
    """حفظ الحركة العنيفة في قاعدة البيانات"""
    try:
        import hashlib
        move_hash = hashlib.md5(
            f"{move['symbol']}:{move['type']}:{move['timestamp']}".encode()
        ).hexdigest()
        
        db = await aiosqlite.connect(DB_PATH)
        
        # تحقق من عدم تكرار الحركة في آخر 30 دقيقة
        async with db.execute("""
            SELECT COUNT(*) FROM violent_moves 
            WHERE symbol = ? AND move_type = ? 
            AND detected_at > datetime('now', '-30 minutes')
        """, (move['symbol'], move['type'])) as cursor:
            result = await cursor.fetchone()
            recent_moves = result[0] if result else 0
        
        if recent_moves > 0:
            await db.close()
            return False
        
        # احفظ الحركة الجديدة
        await db.execute("""
            INSERT INTO violent_moves 
            (symbol, move_type, price, change_pct, volume_ratio, 
             signal_strength, signals, move_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            move['symbol'],
            move['type'],
            move['current_price'],
            move.get('change_15m_pct', move.get('change_1h_pct', 0)),
            move.get('volume_ratio', 1),
            move['signal_strength'],
            json.dumps(move['signals']),
            move_hash
        ))
        
        await db.commit()
        await db.close()
        return True
        
    except Exception as e:
        log.error(f"Save violent move error: {e}")
        return False

# ================ DATA FETCHING ================
async def fetch_ohlcv_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """جلب بيانات OHLCV لكل الفريمات"""
    data = {}
    
    for tf_name, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=100)
            
            if ohlcv and len(ohlcv) >= 30:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = df.dropna()
                
                if len(df) >= 20:
                    data[tf_name] = df
                    
        except Exception as e:
            log.debug(f"{symbol} {tf} fetch error: {e}")
            continue
    
    # تحقق من وجود الحد الأدنى من البيانات
    required = ['M15', 'H1']
    for tf in required:
        if tf not in data:
            return None
    
    return data

async def get_all_usdt_pairs(exchange, min_volume: float = MIN_VOLUME_USDT) -> List[str]:
    """الحصول على جميع أزواج USDT التي لها حجم كافٍ"""
    try:
        tickers = await exchange.fetch_tickers()
        usdt_pairs = []
        
        for symbol, ticker in tickers.items():
            if symbol.endswith('/USDT') and not symbol.startswith('LD'):
                volume = ticker.get('quoteVolume', 0)
                if volume >= min_volume:
                    usdt_pairs.append(symbol)
        
        log.info(f"Found {len(usdt_pairs)} USDT pairs with volume > ${min_volume:,.0f}")
        return usdt_pairs
        
    except Exception as e:
        log.error(f"Error fetching pairs: {e}")
        return []

# ================ MAIN SCANNING LOOP ================
async def violent_move_scanner(exchange):
    """الماسح الرئيسي للحركات العنيفة"""
    log.info("🚨 Starting VIOLENT MOVE scanner for fast drops/explosions")
    
    # إرسال رسالة البدء
    await send_telegram_alert(f"""
🚨 **بدء نظام الإنذار المبكر للحركات العنيفة**

🎯 **المهمة:** رصد جميع الألت كوينز للحركات العنيفة القريبة
⏰ **الفاصل الزمني:** كل {SCAN_INTERVAL} ثانية
📊 **عدد الأزواج:** جميع أزواج USDT بحجم > ${MIN_VOLUME_USDT:,}

🔍 **أنواع الحركات المرصودة:**
1. **الانهيار السريع** (-5% في 15 دقيقة)
2. **الانفجار السريع** (+5% في 15 دقيقة)  
3. **إشارات ما قبل الانفجار** (تراكم + تكثف)

⚠️ **جاهز للإنذار المبكر...**
""")
    
    detector = ViolentMoveDetector()
    
    while True:
        try:
            log.info("=" * 60)
            log.info("🔍 Scanning for violent moves...")
            
            # الحصول على جميع الأزواج
            all_pairs = await get_all_usdt_pairs(exchange)
            
            if not all_pairs:
                log.error("No pairs found!")
                await asyncio.sleep(30)
                continue
            
            violent_moves_detected = []
            
            # مسح كل زوج
            for i, symbol in enumerate(all_pairs):
                try:
                    if i % 20 == 0:
                        log.info(f"Scanning... {i}/{len(all_pairs)} pairs")
                    
                    # جلب البيانات
                    data = await fetch_ohlcv_data(exchange, symbol)
                    if not data:
                        continue
                    
                    # الكشف عن الحركات العنيفة
                    moves = []
                    
                    # 1. كشف الانهيار السريع
                    fast_drop = detector.detect_fast_drop(data, symbol)
                    if fast_drop:
                        moves.append(fast_drop)
                    
                    # 2. كشف الانفجار السريع
                    fast_explosion = detector.detect_fast_explosion(data, symbol)
                    if fast_explosion:
                        moves.append(fast_explosion)
                    
                    # 3. كشف إشارات ما قبل الانفجار
                    pre_explosion = detector.detect_pre_explosion_signals(data, symbol)
                    if pre_explosion:
                        moves.append(pre_explosion)
                    
                    # معالجة الحركات المكتشفة
                    for move in moves:
                        # حفظ في قاعدة البيانات
                        saved = await save_violent_move(move)
                        if saved:
                            violent_moves_detected.append(move)
                            
                            # إرسال إنذار تليجرام
                            await send_violent_move_alert(move)
                    
                    # احترام حدود المعدل
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    log.error(f"Error scanning {symbol}: {e}")
                    continue
            
            # تقرير المسح
            if violent_moves_detected:
                log.info(f"✅ Found {len(violent_moves_detected)} violent moves")
                summary = await create_scan_summary(violent_moves_detected)
                await send_telegram_alert(summary)
            else:
                log.info("✅ Scan complete - No violent moves detected")
            
            # الانتظار للمسح التالي
            log.info(f"⏰ Waiting {SCAN_INTERVAL} seconds for next scan...")
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
            await asyncio.sleep(30)

async def send_violent_move_alert(move: Dict):
    """إرسال إنذار حركة عنيفة"""
    try:
        move_type_ar = {
            'FAST_DROP': '📉 **انهيار سريع**',
            'FAST_EXPLOSION': '🚀 **انفجار سريع**',
            'PRE_EXPLOSION': '⚠️ **إشارة ما قبل انفجار**'
        }.get(move['type'], move['type'])
        
        change_text = ""
        if 'change_15m_pct' in move and move['change_15m_pct'] != 0:
            change_text = f"التغير: **{move['change_15m_pct']:+.1f}%** في 15 دقيقة"
        elif 'change_1h_pct' in move and move['change_1h_pct'] != 0:
            change_text = f"التغير: **{move['change_1h_pct']:+.1f}%** في ساعة"
        
        volume_text = f"الحجم: **{move.get('volume_ratio', 1):.1f}x** المتوسط" if move.get('volume_ratio', 1) > 1 else ""
        rsi_text = f"RSI: **{move.get('current_rsi', 50):.1f}**" if 'current_rsi' in move else ""
        
        signals_text = "\n".join([f"• {s}" for s in move.get('signals', [])])
        
        message = f"""
{move_type_ar}
──────────────
📊 **{move['symbol']}**
💰 السعر: `{move['current_price']:.8f}`
{change_text}
{volume_text}
{rsi_text}

📈 **الإشارات:**
{signals_text}

⚡ **قوة الإشارة:** {move['signal_strength']}/100
🕐 **الوقت:** {datetime.now().strftime('%H:%M:%S')}

#{move['symbol'].replace('/', '').replace('-', '')} #{'انفجار' if 'EXPLOSION' in move['type'] else 'انهيار'}
"""
        
        await send_telegram_alert(message)
        
    except Exception as e:
        log.error(f"Alert message error: {e}")

async def create_scan_summary(moves: List[Dict]) -> str:
    """إنشاء ملخص المسح"""
    try:
        drops = [m for m in moves if 'DROP' in m['type']]
        explosions = [m for m in moves if 'EXPLOSION' in m['type']]
        pre_signals = [m for m in moves if m['type'] == 'PRE_EXPLOSION']
        
        strongest_drop = max(drops, key=lambda x: x['signal_strength']) if drops else None
        strongest_explosion = max(explosions, key=lambda x: x['signal_strength']) if explosions else None
        
        summary = f"""
📊 **ملخص المسح - الحركات العنيفة**
────────────────
⏰ الوقت: {datetime.now().strftime('%H:%M:%S')}
📈 **الإحصائيات:**
• إجمالي الحركات: **{len(moves)}**
• انهيارات سريعة: **{len(drops)}**
• انفجارات سريعة: **{len(explosions)}**
• إشارات ما قبل انفجار: **{len(pre_signals)}**
"""
        
        if strongest_drop:
            summary += f"""
📉 **أقوى انهيار:**
• {strongest_drop['symbol']} ({strongest_drop['change_15m_pct']:+.1f}%)
• قوة: {strongest_drop['signal_strength']}/100
"""
        
        if strongest_explosion:
            summary += f"""
🚀 **أقوى انفجار:**
• {strongest_explosion['symbol']} ({strongest_explosion['change_15m_pct']:+.1f}%)
• قوة: {strongest_explosion['signal_strength']}/100
"""
        
        if pre_signals:
            summary += f"""
⚠️ **أزواج على وشك الانفجار:**
{', '.join([m['symbol'] for m in pre_signals[:5]])}
"""
        
        return summary
        
    except Exception as e:
        log.error(f"Summary error: {e}")
        return "ملخص المسح"

# ================ MAIN ================
async def main():
    """الدالة الرئيسية"""
    log.info("=" * 70)
    log.info("🚨 VIOLENT MOVE DETECTION SYSTEM - FAST DROPS/EXPLOSIONS")
    log.info("=" * 70)
    
    # تهيئة قاعدة البيانات
    if not await init_violent_moves_db():
        log.error("Failed to initialize database")
        return
    
    # الاتصال بالبورصة
    try:
        exchange = ccxt.binance({  # بينانس لديها معظم الألت كوينز
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 30000
        })
        
        # اختبار الاتصال
        await exchange.load_markets()
        log.info(f"✅ Connected to {exchange.name}")
        
    except Exception as e:
        log.error(f"Exchange connection error: {e}")
        # حاول OKX كبديل
        try:
            exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 30000
            })
            await exchange.load_markets()
            log.info(f"✅ Connected to {exchange.name} (fallback)")
        except Exception as e2:
            log.error(f"All exchanges failed: {e2}")
            return
    
    # بدء الماسح
    try:
        await violent_move_scanner(exchange)
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
    except Exception as e:
        log.error(f"Scanner crashed: {e}")
        await send_telegram_alert(f"❌ تعطل النظام: {str(e)[:100]}")
    finally:
        if exchange:
            await exchange.close()
        log.info("Violent move scanner shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("System stopped by user")
    except Exception as e:
        log.error(f"Fatal error: {e}")