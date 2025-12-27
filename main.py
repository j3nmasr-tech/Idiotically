#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visual Synthesis Scanner - نظام توقع الانفجار/الانهيار
Predictive Price Movement Analysis with Crash/Explosion Detection
"""

import os
import time
import asyncio
import logging
import hashlib
import aiosqlite
import httpx
import requests
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from fastapi import FastAPI
import json
from concurrent.futures import ThreadPoolExecutor

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 90))

# PREDICTIVE thresholds
MIN_PREDICTION_CONFIDENCE = 0.50  # Minimum confidence for predictions
MIN_EXPLOSION_SCORE = 0.50        # للانفجارات القوية
MIN_CRASH_SCORE = 0.50            # للانهيارات القوية
CONFLUENCE_REQUIRED = 3           # Minimum number of confirmations

# Timeframes for true multi-timeframe analysis
TIMEFRAMES = {
    "MONTHLY": "1M",
    "WEEKLY": "1w",
    "DAILY": "1d",
    "H4": "4h",
    "H1": "1h",
    "M15": "15m"
}

# EMA Periods for multi-EMA analysis
EMA_PERIODS = {
    "FAST": 9,
    "MEDIUM": 21,
    "SLOW": 50,
    "LONG": 200
}

# RSI Settings
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
RSI_NEUTRAL = 50

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("visual_scanner")
db_lock = asyncio.Lock()
db_conn = None
exchange = None
executor = ThreadPoolExecutor(max_workers=3)

# ================ TELEGRAM UTILITIES ================

async def tg_sync_backup(message: str):
    """Backup sync Telegram sender using requests"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set in backup method")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
            "disable_notification": False
        }
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            executor, 
            lambda: requests.post(url, json=payload, timeout=15)
        )
        
        if response.status_code == 200:
            log.info("✅ Telegram message sent successfully (backup method)")
            return True
        else:
            log.error(f"❌ Backup Telegram error {response.status_code}: {response.text[:100]}")
            return False
            
    except Exception as e:
        log.error(f"Backup Telegram error: {str(e)[:100]}")
        return False

async def tg(message: str, parse_mode: str = "Markdown"):
    """Send message to Telegram - primary with httpx, fallback to requests"""
    if not TELEGRAM_TOKEN:
        log.error("❌ TELEGRAM_BOT_TOKEN is not set!")
        log.error("Set it with: export TELEGRAM_BOT_TOKEN='your_bot_token'")
        return False
    
    if not TELEGRAM_CHAT_ID:
        log.error("❌ TELEGRAM_CHAT_ID is not set!")
        log.error("Set it with: export TELEGRAM_CHAT_ID='your_chat_id'")
        return False
    
    # Clean message for logging
    log_message = message.replace('\n', ' ').strip()[:100]
    log.info(f"📤 Attempting to send Telegram: {log_message}...")
    
    # Method 1: Try httpx first (async)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
                "disable_notification": False
            }
            
            response = await client.post(url, json=payload)
            
            if response.status_code == 200:
                log.info("✅ Telegram sent successfully via httpx")
                return True
            elif response.status_code == 400:
                log.warning(f"Bad request, trying without markdown...")
                # Try without markdown
                payload["parse_mode"] = None
                response2 = await client.post(url, json=payload)
                if response2.status_code == 200:
                    log.info("✅ Telegram sent without markdown")
                    return True
                else:
                    log.error(f"Still failed: {response2.status_code}")
                    return await tg_sync_backup(message)
            else:
                log.warning(f"httpx failed ({response.status_code}), trying backup...")
                return await tg_sync_backup(message)
                
    except httpx.TimeoutException:
        log.warning("httpx timeout, trying backup...")
        return await tg_sync_backup(message)
    except Exception as e:
        log.warning(f"httpx error: {str(e)[:100]}, trying backup...")
        return await tg_sync_backup(message)

async def test_telegram():
    """Test Telegram connection"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("❌ Telegram credentials are NOT set!")
        log.error("Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
        return False
    
    test_msg = """
🤖 *Telegram Connection Test*

✅ Scanner is starting...
✅ Predictive Analysis System Activated
✅ If you see this message, Telegram is working!

*Bot:* Predictive Crash/Explosion Scanner
*Time:* {time}
*Status:* READY
""".format(time=time.strftime("%Y-%m-%d %H:%M:%S"))
    
    log.info("Testing Telegram connection...")
    result = await tg(test_msg)
    
    if result:
        log.info("✅ Telegram test PASSED - Messages will be sent")
        return True
    else:
        log.error("❌ Telegram test FAILED - Check your credentials")
        log.error("1. Make sure TELEGRAM_BOT_TOKEN is correct")
        log.error("2. Make sure TELEGRAM_CHAT_ID is correct")
        log.error("3. Make sure the bot is started with /start")
        log.error("4. Make sure the bot has permission to send messages")
        return False

# ---------------- PURE PYTHON TECHNICAL INDICATORS ----------------
def calculate_ema(prices: np.ndarray, period: int) -> np.ndarray:
    """Calculate Exponential Moving Average (no TA-Lib)"""
    n = len(prices)
    if n < period:
        return np.full(n, np.nan)
    
    ema = np.zeros(n, dtype=float)
    ema[:period-1] = np.nan
    
    # Initial SMA
    sma = np.mean(prices[:period])
    ema[period-1] = sma
    
    # Multiplier
    multiplier = 2 / (period + 1)
    
    # Calculate EMA
    for i in range(period, n):
        ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]
    
    return ema

def calculate_sma(prices: np.ndarray, period: int) -> np.ndarray:
    """Calculate Simple Moving Average"""
    n = len(prices)
    if n < period:
        return np.full(n, np.nan)
    
    sma = np.zeros(n, dtype=float)
    sma[:period-1] = np.nan
    
    for i in range(period-1, n):
        sma[i] = np.mean(prices[i-period+1:i+1])
    
    return sma

def calculate_rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """Calculate RSI (no TA-Lib)"""
    n = len(prices)
    if n < period + 1:
        return np.full(n, np.nan)
    
    deltas = np.diff(prices)
    
    # Initialize arrays
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    rsi = np.full(n, np.nan)
    
    # Calculate initial averages
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    # Handle division by zero
    if avg_loss == 0:
        rsi[period] = 100 if avg_gain > 0 else 0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100 - (100 / (1 + rs))
    
    # Calculate remaining RSI values
    for i in range(period + 1, n):
        gain = gains[i-1]
        loss = losses[i-1]
        
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        
        if avg_loss == 0:
            rsi[i] = 100 if avg_gain > 0 else 0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100 / (1 + rs))
    
    return rsi

# ================ ADVANCED PREDICTIVE ANALYSIS MODULES ================

class TrendDirection(Enum):
    STRONG_BULLISH = "STRONG_BULLISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"
    STRONG_BEARISH = "STRONG_BEARISH"

class PredictionType(Enum):
    EXPLOSION = "EXPLOSION"            # انفجار صعودي قوي
    CRASH = "CRASH"                    # انهيار هبوطي قوي
    BREAKOUT = "BREAKOUT"              # اختراق صعودي
    DUMP = "DUMP"                      # تصحيح هبوطي
    CONSOLIDATION = "CONSOLIDATION"    # تكثف/تراكم
    UNCLEAR = "UNCLEAR"                # غير واضح

# ---------- 1. VOLATILITY COMPRESSION DETECTION (اكتشاف التكثف) ----------
def detect_volatility_compression(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """اكتشاف تقلص التقلبات - مؤشر لانفجار قادم"""
    try:
        h4_df = data.get('H4')
        h1_df = data.get('H1')
        
        if h4_df is None or h1_df is None:
            return 0.5, "لا توجد بيانات كافية", {}
        
        # تحليل H4
        h4_prices = h4_df['close'].values[-30:]
        h4_highs = h4_df['high'].values[-30:]
        h4_lows = h4_df['low'].values[-30:]
        
        # تحليل H1
        h1_prices = h1_df['close'].values[-50:]
        
        # 1. حساب تقلبات النطاق السعري
        h4_ranges = [(h4_highs[i] - h4_lows[i]) / h4_prices[i] for i in range(len(h4_prices))]
        avg_h4_range = np.mean(h4_ranges[-20:]) if len(h4_ranges) >= 20 else 0
        recent_h4_range = np.mean(h4_ranges[-5:]) if len(h4_ranges) >= 5 else 0
        
        # 2. حساب الانحراف المعياري
        h4_volatility = np.std(h4_prices[-20:]) / h4_prices[-1] if h4_prices[-1] != 0 else 0
        h1_volatility = np.std(h1_prices[-40:]) / h1_prices[-1] if h1_prices[-1] != 0 else 0
        
        # 3. تقييم التكثف
        compression_score = 0
        compression_signals = []
        
        # قاعدة: تقلبات أقل من 1% على H4 و 1.5% على H1
        if h4_volatility < 0.01 and h1_volatility < 0.015:
            compression_score = 0.85
            compression_signals.append("تكثف عالي جداً")
        elif h4_volatility < 0.015 and h1_volatility < 0.02:
            compression_score = 0.70
            compression_signals.append("تكثف مرتفع")
        elif h4_volatility < 0.02:
            compression_score = 0.55
            compression_signals.append("تكثف متوسط")
        else:
            compression_score = 0.30
            compression_signals.append("لا يوجد تكثف")
        
        # 4. اكتشاف تقلص النطاق مع الوقت
        range_decline = 0
        if len(h4_ranges) >= 10 and avg_h4_range > 0:
            range_decline = (avg_h4_range - recent_h4_range) / avg_h4_range
            if range_decline > 0.3:  # تقلص أكثر من 30%
                compression_score += 0.15
                compression_signals.append(f"تقلص النطاق: {range_decline:.1%}")
        
        # 5. قرب من مستويات حاسمة
        current_price = h4_prices[-1]
        h4_highs_sorted = sorted(h4_highs[-20:])
        h4_lows_sorted = sorted(h4_lows[-20:])
        
        resistance_band = h4_highs_sorted[-3:]  # أعلى 3 مستويات
        support_band = h4_lows_sorted[:3]       # أدنى 3 مستويات
        
        distance_to_resistance = 1
        distance_to_support = 1
        
        if resistance_band:
            distance_to_resistance = min([abs(current_price - r) / current_price for r in resistance_band])
            if distance_to_resistance < 0.02:  # أقل من 2% من المقاومة
                compression_score += 0.10
                compression_signals.append("قرب من مقاومة رئيسية")
        
        if support_band:
            distance_to_support = min([abs(current_price - s) / current_price for s in support_band])
            if distance_to_support < 0.02:  # أقل من 2% من الدعم
                compression_score += 0.10
                compression_signals.append("قرب من دعم رئيسي")
        
        # تحديد النتيجة النهائية
        compression_score = min(1.0, compression_score)
        analysis_text = " | ".join(compression_signals)
        
        details = {
            'compression_score': compression_score,
            'h4_volatility_pct': float(h4_volatility * 100),
            'h1_volatility_pct': float(h1_volatility * 100),
            'avg_h4_range_pct': float(avg_h4_range * 100),
            'recent_h4_range_pct': float(recent_h4_range * 100),
            'range_decline_pct': float(range_decline * 100),
            'distance_to_resistance_pct': float(distance_to_resistance * 100),
            'distance_to_support_pct': float(distance_to_support * 100),
            'resistance_levels': [float(r) for r in resistance_band],
            'support_levels': [float(s) for s in support_band]
        }
        
        return compression_score, analysis_text, details
        
    except Exception as e:
        log.error(f"Volatility compression detection error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 2. MOMENTUM DIVERGENCE DETECTION (التباعد الزخمي) ----------
def detect_momentum_divergence(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """اكتشاف التباعد بين السعر والزخم - مؤشر لانعكاس قوي"""
    try:
        h4_df = data.get('H4')
        h1_df = data.get('H1')
        
        if h4_df is None or h1_df is None:
            return 0.5, "لا توجد بيانات كافية", {}
        
        h4_prices = h4_df['close'].values[-30:]
        h1_prices = h1_df['close'].values[-50:]
        
        # 1. حساب RSI على فريمات مختلفة
        h4_rsi = calculate_rsi(h4_prices, 14)
        h1_rsi = calculate_rsi(h1_prices, 14)
        
        # 2. البحث عن التباعد الصعودي (الانهيار القادم)
        bearish_divergence_score = 0
        bearish_signals = []
        
        # تحليل H4 للتباعد الهبوطي
        if len(h4_prices) >= 15 and len(h4_rsi) >= 15:
            # البحث عن ارتفاع في السعر مع انخفاض في RSI
            recent_prices = h4_prices[-8:]
            recent_rsi = h4_rsi[-8:] if not np.isnan(h4_rsi[-8]) else []
            
            if len(recent_rsi) >= 5:
                price_high_idx = np.argmax(recent_prices)
                rsi_high_idx = np.argmax(recent_rsi)
                
                # إذا كان السعر عند قمة جديدة ولكن RSI ليس عند قمة جديدة
                if price_high_idx == len(recent_prices) - 1 and rsi_high_idx < len(recent_rsi) - 2:
                    bearish_divergence_score = 0.80
                    bearish_signals.append("تباعد هبوطي على H4")
        
        # 3. البحث عن التباعد الهبوطي (الانفجار القادم)
        bullish_divergence_score = 0
        bullish_signals = []
        
        # تحليل H1 للتباعد الصعودي
        if len(h1_prices) >= 20 and len(h1_rsi) >= 20:
            # البحث عن انخفاض في السعر مع ارتفاع في RSI
            recent_prices_h1 = h1_prices[-12:]
            recent_rsi_h1 = h1_rsi[-12:] if not np.isnan(h1_rsi[-12]) else []
            
            if len(recent_rsi_h1) >= 6:
                price_low_idx = np.argmin(recent_prices_h1)
                rsi_low_idx = np.argmin(recent_rsi_h1)
                
                # إذا كان السعر عند قاع جديد ولكن RSI ليس عند قاع جديد
                if price_low_idx == len(recent_prices_h1) - 1 and rsi_low_idx < len(recent_rsi_h1) - 2:
                    bullish_divergence_score = 0.80
                    bullish_signals.append("تباعد صعودي على H1")
        
        # 4. تحليل MACD مبسط (الفرق بين EMA)
        ema_12 = calculate_ema(h4_prices, 12) if len(h4_prices) >= 12 else np.array([])
        ema_26 = calculate_ema(h4_prices, 26) if len(h4_prices) >= 26 else np.array([])
        
        if len(ema_12) >= 5 and len(ema_26) >= 5 and not np.isnan(ema_12[-1]) and not np.isnan(ema_26[-1]):
            macd_line = ema_12[-5:] - ema_26[-5:]
            
            # تحقق من تباعد MACD
            if len(macd_line) >= 3:
                macd_trend = macd_line[-1] - macd_line[0]
                price_trend = h4_prices[-1] - h4_prices[-len(macd_line)]
                
                if price_trend > 0 and macd_trend < 0:  # سعر صاعد، MACD هابط
                    bearish_divergence_score = max(bearish_divergence_score, 0.70)
                    bearish_signals.append("تباعد MACD هبوطي")
                elif price_trend < 0 and macd_trend > 0:  # سعر هابط، MACD صاعد
                    bullish_divergence_score = max(bullish_divergence_score, 0.70)
                    bullish_signals.append("تباعد MACD صعودي")
        
        # 5. النتيجة النهائية (نأخذ الأعلى)
        divergence_score = max(bearish_divergence_score, bullish_divergence_score)
        
        # تحديد نوع التباعد
        if bearish_divergence_score > bullish_divergence_score:
            divergence_type = "BEARISH_DIVERGENCE"
            analysis_text = " | ".join(bearish_signals) if bearish_signals else "لا يوجد تباعد واضح"
        elif bullish_divergence_score > bearish_divergence_score:
            divergence_type = "BULLISH_DIVERGENCE"
            analysis_text = " | ".join(bullish_signals) if bullish_signals else "لا يوجد تباعد واضح"
        else:
            divergence_type = "NO_DIVERGENCE"
            analysis_text = "لا يوجد تباعد زخمي"
        
        details = {
            'divergence_score': divergence_score,
            'divergence_type': divergence_type,
            'bearish_divergence_score': bearish_divergence_score,
            'bullish_divergence_score': bullish_divergence_score,
            'h4_rsi_current': float(h4_rsi[-1]) if len(h4_rsi) > 0 and not np.isnan(h4_rsi[-1]) else 50,
            'h1_rsi_current': float(h1_rsi[-1]) if len(h1_rsi) > 0 and not np.isnan(h1_rsi[-1]) else 50,
            'signals': bearish_signals + bullish_signals
        }
        
        return divergence_score, analysis_text, details
        
    except Exception as e:
        log.error(f"Momentum divergence detection error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 3. VOLUME SPIKE ANALYSIS (الزيادة المفاجئة في الحجم) ----------
def analyze_volume_spikes(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """تحليل الزيادات المفاجئة في الحجم - مؤشر لحركة قوية قادمة"""
    try:
        # تحليل متعدد الفريمات للحجم
        volume_analysis = {}
        spike_signals = []
        total_spike_score = 0
        analyzed_tfs = 0
        
        for tf in ['H4', 'H1', 'M15']:
            df = data.get(tf)
            if df is None or len(df) < 30:
                continue
            
            volumes = df['volume'].values[-30:]
            prices = df['close'].values[-30:]
            
            # 1. حساب متوسط الحجم
            avg_volume = np.mean(volumes[:-5]) if len(volumes) > 5 else np.mean(volumes)
            
            # 2. تحليل آخر 5 شمعات
            recent_volumes = volumes[-5:] if len(volumes) >= 5 else volumes
            recent_avg_volume = np.mean(recent_volumes)
            
            # 3. نسبة زيادة الحجم
            volume_ratio = recent_avg_volume / avg_volume if avg_volume > 0 else 1
            
            # 4. كشف الزيادات المفاجئة
            spike_score = 0
            tf_signal = ""
            
            if volume_ratio > 3.0:
                spike_score = 0.90
                tf_signal = f"{tf}: حجم عالي جداً ({volume_ratio:.1f}x)"
            elif volume_ratio > 2.0:
                spike_score = 0.75
                tf_signal = f"{tf}: حجم مرتفع ({volume_ratio:.1f}x)"
            elif volume_ratio > 1.5:
                spike_score = 0.60
                tf_signal = f"{tf}: حجم فوق المتوسط ({volume_ratio:.1f}x)"
            
            # 5. تحليل اتجاه الحجم مع السعر
            if len(prices) >= 5:
                price_change = (prices[-1] - prices[-5]) / prices[-5] * 100 if prices[-5] != 0 else 0
                
                if len(volumes) >= 10:
                    volume_trend = (recent_avg_volume - np.mean(volumes[-10:-5])) / np.mean(volumes[-10:-5]) * 100 if np.mean(volumes[-10:-5]) > 0 else 0
                else:
                    volume_trend = 0
                
                # زيادة الحجم مع حركة سعر ضعيفة = تراكم
                if volume_ratio > 1.8 and abs(price_change) < 1.0:
                    spike_score += 0.15
                    tf_signal += " (تراكم)"
                # زيادة الحجم مع حركة سعر قوية = تأكيد
                elif volume_ratio > 1.5 and abs(price_change) > 2.0:
                    spike_score += 0.10
                    tf_signal += " (تأكيد حركة)"
            
            if tf_signal:
                spike_signals.append(tf_signal)
                total_spike_score += spike_score
                analyzed_tfs += 1
            
            volume_analysis[tf] = {
                'volume_ratio': float(volume_ratio),
                'spike_score': spike_score,
                'avg_volume': float(avg_volume),
                'recent_avg_volume': float(recent_avg_volume),
                'signal': tf_signal
            }
        
        # 6. النتيجة النهائية
        if analyzed_tfs > 0:
            avg_spike_score = total_spike_score / analyzed_tfs
        else:
            avg_spike_score = 0.5
        
        analysis_text = " | ".join(spike_signals) if spike_signals else "لا توجد زيادات حجم ملحوظة"
        
        details = {
            'avg_spike_score': avg_spike_score,
            'analyzed_timeframes': analyzed_tfs,
            'volume_analysis': volume_analysis,
            'spike_signals': spike_signals
        }
        
        return avg_spike_score, analysis_text, details
        
    except Exception as e:
        log.error(f"Volume spike analysis error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 4. SUPPORT/RESISTANCE BREAKDOWN (تحليل كسر المستويات) ----------
def analyze_support_resistance_breakdown(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """تحليل ضعف أو قوة مستويات الدعم والمقاومة"""
    try:
        daily_df = data.get('DAILY')
        h4_df = data.get('H4')
        
        if daily_df is None or h4_df is None:
            return 0.5, "لا توجد بيانات كافية", {}
        
        daily_prices = daily_df['close'].values[-100:]
        daily_highs = daily_df['high'].values[-100:]
        daily_lows = daily_df['low'].values[-100:]
        
        h4_prices = h4_df['close'].values[-50:]
        current_price = h4_prices[-1]
        
        # 1. تحديد مستويات الدعم والمقاومة الرئيسية
        support_levels = []
        resistance_levels = []
        
        # البحث عن قيعان وقمم على الفريم اليومي
        for i in range(2, len(daily_prices)-2):
            # قمم (مقاومات)
            if (daily_highs[i] > daily_highs[i-1] and daily_highs[i] > daily_highs[i-2] and
                daily_highs[i] > daily_highs[i+1] and daily_highs[i] > daily_highs[i+2]):
                resistance_levels.append(daily_highs[i])
            
            # قيعان (دعوم)
            if (daily_lows[i] < daily_lows[i-1] and daily_lows[i] < daily_lows[i-2] and
                daily_lows[i] < daily_lows[i+1] and daily_lows[i] < daily_lows[i+2]):
                support_levels.append(daily_lows[i])
        
        # 2. تحليل قرب السعر من المستويات
        breakdown_score = 0
        breakdown_signals = []
        
        nearest_resistance = None
        distance_to_res = 100
        nearest_support = None
        distance_to_sup = 100
        
        if resistance_levels:
            nearest_resistance = min(resistance_levels, key=lambda x: abs(x - current_price))
            distance_to_res = (nearest_resistance - current_price) / current_price * 100 if current_price != 0 else 100
            
            if distance_to_res < 1.0:  # أقل من 1%
                breakdown_score += 0.40
                breakdown_signals.append(f"عند مقاومة قوية: {nearest_resistance:.4f}")
            elif distance_to_res < 2.0:  # أقل من 2%
                breakdown_score += 0.25
                breakdown_signals.append(f"قرب مقاومة: {nearest_resistance:.4f}")
        
        if support_levels:
            nearest_support = min(support_levels, key=lambda x: abs(x - current_price))
            distance_to_sup = (current_price - nearest_support) / current_price * 100 if current_price != 0 else 100
            
            if distance_to_sup < 1.0:  # أقل من 1%
                breakdown_score += 0.40
                breakdown_signals.append(f"عند دعم قوي: {nearest_support:.4f}")
            elif distance_to_sup < 2.0:  # أقل من 2%
                breakdown_score += 0.25
                breakdown_signals.append(f"قرب دعم: {nearest_support:.4f}")
        
        # 3. تحليل اختبارات متعددة للمستويات
        test_breakdown_score = 0
        recent_tests = 0
        if len(h4_prices) >= 20:
            level_to_test = nearest_resistance if nearest_resistance is not None else nearest_support
            
            if level_to_test:
                for i in range(-10, 0):  # آخر 10 شمعات H4
                    if i < len(h4_prices):
                        if abs(h4_prices[i] - level_to_test) / level_to_test < 0.005:  # أقل من 0.5%
                            recent_tests += 1
            
            if recent_tests >= 3:
                test_breakdown_score = 0.35
                breakdown_signals.append(f"اختبار متعدد للمستوى ({recent_tests} مرات)")
        
        # 4. النتيجة النهائية
        total_score = min(1.0, breakdown_score + test_breakdown_score)
        analysis_text = " | ".join(breakdown_signals) if breakdown_signals else "لا يوجد ضغط على مستويات رئيسية"
        
        details = {
            'breakdown_score': total_score,
            'nearest_resistance': float(nearest_resistance) if nearest_resistance is not None else 0,
            'nearest_support': float(nearest_support) if nearest_support is not None else 0,
            'distance_to_resistance_pct': float(distance_to_res),
            'distance_to_support_pct': float(distance_to_sup),
            'support_levels': [float(s) for s in support_levels[-5:]],
            'resistance_levels': [float(r) for r in resistance_levels[-5:]],
            'recent_tests': recent_tests
        }
        
        return total_score, analysis_text, details
        
    except Exception as e:
        log.error(f"Support/resistance breakdown analysis error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 5. PRICE ACTION EXTREMES (النقاط القصوى في حركة السعر) ----------
def detect_price_action_extremes(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """اكتشاف النقاط القصوى في حركة السعر - مؤشر لانعكاس"""
    try:
        daily_df = data.get('DAILY')
        h4_df = data.get('H4')
        
        if daily_df is None or h4_df is None:
            return 0.5, "لا توجد بيانات كافية", {}
        
        daily_prices = daily_df['close'].values[-50:]
        daily_highs = daily_df['high'].values[-50:]
        daily_lows = daily_df['low'].values[-50:]
        
        h4_prices = h4_df['close'].values[-30:]
        
        # 1. تحليل المدى اليومي
        daily_ranges = []
        for i in range(len(daily_prices)):
            if daily_prices[i] != 0:
                daily_ranges.append((daily_highs[i] - daily_lows[i]) / daily_prices[i])
        
        avg_daily_range = np.mean(daily_ranges[-20:]) if len(daily_ranges) >= 20 else np.mean(daily_ranges) if daily_ranges else 0
        current_daily_range = (daily_highs[-1] - daily_lows[-1]) / daily_prices[-1] if daily_prices[-1] != 0 else 0
        
        # 2. تحديد إذا كان السعر عند أقصى المدى
        extremes_score = 0
        extremes_signals = []
        
        # نسبة المدى الحالي مقارنة بالمتوسط
        range_ratio = current_daily_range / avg_daily_range if avg_daily_range > 0 else 1
        
        if range_ratio > 1.8:  # مدى أوسع من 180% من المتوسط
            extremes_score = 0.80
            extremes_signals.append(f"مدى سعري واسع جداً ({range_ratio:.1f}x)")
        elif range_ratio > 1.5:
            extremes_score = 0.65
            extremes_signals.append(f"مدى سعري واسع ({range_ratio:.1f}x)")
        elif range_ratio < 0.5:  # مدى أضيق من 50% من المتوسط
            extremes_score = 0.60
            extremes_signals.append(f"مدى سعري ضيق ({range_ratio:.1f}x)")
        
        # 3. تحليل موقع السعر داخل الشمعة
        candle_position = 0.5
        if daily_highs[-1] != daily_lows[-1]:
            candle_position = (daily_prices[-1] - daily_lows[-1]) / (daily_highs[-1] - daily_lows[-1])
        
        if candle_position > 0.85:  # قرب قمة الشمعة
            extremes_score += 0.15
            extremes_signals.append("سعر عند قمة المدى اليومي")
        elif candle_position < 0.15:  # قرب قاع الشمعة
            extremes_score += 0.15
            extremes_signals.append("سعر عند قاع المدى اليومي")
        
        # 4. تحليل الاتجاه المفرط
        trend_strength = 0
        if len(daily_prices) >= 10 and daily_prices[-10] != 0:
            trend_strength = (daily_prices[-1] - daily_prices[-10]) / daily_prices[-10] * 100
            
            if trend_strength > 15:  # صعود أكثر من 15%
                extremes_score += 0.20
                extremes_signals.append(f"صعود مفرط ({trend_strength:.1f}%)")
            elif trend_strength < -15:  # هبوط أكثر من 15%
                extremes_score += 0.20
                extremes_signals.append(f"هبوط مفرط ({abs(trend_strength):.1f}%)")
        
        # 5. النتيجة النهائية
        extremes_score = min(1.0, extremes_score)
        analysis_text = " | ".join(extremes_signals) if extremes_signals else "لا يوجد تطرف في حركة السعر"
        
        details = {
            'extremes_score': extremes_score,
            'range_ratio': float(range_ratio),
            'current_daily_range_pct': float(current_daily_range * 100),
            'avg_daily_range_pct': float(avg_daily_range * 100),
            'candle_position': float(candle_position),
            'trend_strength_pct': float(trend_strength),
            'signals': extremes_signals
        }
        
        return extremes_score, analysis_text, details
        
    except Exception as e:
        log.error(f"Price action extremes detection error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 6. PREDICTIVE MOVEMENT ANALYSIS (التحليل التنبؤي الرئيسي) ----------
def predict_explosion_crash(data: Dict[str, pd.DataFrame]) -> Dict:
    """التنبؤ بالانفجار أو الانهيار القادم - التحليل الشامل"""
    try:
        log.info("Running predictive explosion/crash analysis...")
        
        # جمع جميع التحليلات
        analyses = {}
        
        # 1. تحليل التكثف (انفجار)
        compression_score, compression_text, compression_details = detect_volatility_compression(data)
        analyses['compression'] = {
            'score': compression_score,
            'text': compression_text,
            'details': compression_details
        }
        
        # 2. تحليل التباعد الزخمي (انعكاس)
        divergence_score, divergence_text, divergence_details = detect_momentum_divergence(data)
        analyses['divergence'] = {
            'score': divergence_score,
            'text': divergence_text,
            'details': divergence_details
        }
        
        # 3. تحليل زيادات الحجم (حركة قوية)
        volume_score, volume_text, volume_details = analyze_volume_spikes(data)
        analyses['volume'] = {
            'score': volume_score,
            'text': volume_text,
            'details': volume_details
        }
        
        # 4. تحليل كسر المستويات (ضغط)
        breakdown_score, breakdown_text, breakdown_details = analyze_support_resistance_breakdown(data)
        analyses['breakdown'] = {
            'score': breakdown_score,
            'text': breakdown_text,
            'details': breakdown_details
        }
        
        # 5. تحليل التطرف السعري (انعكاس)
        extremes_score, extremes_text, extremes_details = detect_price_action_extremes(data)
        analyses['extremes'] = {
            'score': extremes_score,
            'text': extremes_text,
            'details': extremes_details
        }
        
        # 6. تحليل إضافي: RSI التشبع
        daily_df = data.get('DAILY')
        rsi_extreme_score = 0.5
        rsi_signal = "RSI طبيعي"
        current_rsi = 50
        
        if daily_df is not None and len(daily_df) >= 30:
            daily_prices = daily_df['close'].values
            daily_rsi = calculate_rsi(daily_prices, 14)
            if len(daily_rsi) > 0 and not np.isnan(daily_rsi[-1]):
                current_rsi = daily_rsi[-1]
                
                if current_rsi > 80:
                    rsi_extreme_score = 0.85
                    rsi_signal = f"RSI تشبع شراء خطير ({current_rsi:.1f})"
                elif current_rsi > 75:
                    rsi_extreme_score = 0.70
                    rsi_signal = f"RSI تشبع شراء ({current_rsi:.1f})"
                elif current_rsi < 20:
                    rsi_extreme_score = 0.85
                    rsi_signal = f"RSI تشبع بيع خطير ({current_rsi:.1f})"
                elif current_rsi < 25:
                    rsi_extreme_score = 0.70
                    rsi_signal = f"RSI تشبع بيع ({current_rsi:.1f})"
        
        analyses['rsi'] = {
            'score': rsi_extreme_score,
            'text': rsi_signal,
            'details': {'current_rsi': current_rsi}
        }
        
        # 7. تحديد النتيجة النهائية
        scores = {
            'compression': compression_score,
            'divergence': divergence_score,
            'volume': volume_score,
            'breakdown': breakdown_score,
            'extremes': extremes_score,
            'rsi': rsi_extreme_score
        }
        
        # أوزان خاصة للتنبؤ
        weights = {
            'compression': 0.20,    # التكثف مؤشر قوي للانفجار
            'divergence': 0.25,     # التباعد مؤشر قوي للانعكاس
            'volume': 0.15,         # الحجم يؤكد القوة
            'breakdown': 0.20,      # ضغط المستويات
            'extremes': 0.10,       # التطرف السعري
            'rsi': 0.10             # التشبع
        }
        
        # حساب النتيجة المرجحة
        weighted_score = sum(scores[key] * weights[key] for key in weights)
        
        # تحديد نوع الحركة المتوقعة
        prediction_type = "UNCLEAR"
        confidence = weighted_score
        predicted_move_pct = 0
        timeframe = "H4-DAILY"
        
        # منطق التوقع
        compression_strong = compression_score > 0.75
        divergence_strong = divergence_score > 0.75
        breakdown_strong = breakdown_score > 0.70
        
        # تحديد نوع التباعد
        divergence_info = divergence_details.get('divergence_type', 'NO_DIVERGENCE')
        is_bullish_divergence = "BULLISH" in str(divergence_info)
        is_bearish_divergence = "BEARISH" in str(divergence_info)
        
        bearish_div_score = divergence_details.get('bearish_divergence_score', 0)
        bullish_div_score = divergence_details.get('bullish_divergence_score', 0)
        
        # سيناريو 1: انفجار صعودي قوي
        if (compression_strong and is_bullish_divergence and breakdown_strong and 
            bearish_div_score < bullish_div_score):
            prediction_type = "EXPLOSION"
            confidence = max(confidence, 0.75)
            predicted_move_pct = 3.0 + (compression_score * 2)  # 3-5%
            timeframe = "H1-H4"
        
        # سيناريو 2: انهيار هبوطي قوي
        elif (compression_strong and is_bearish_divergence and breakdown_strong and 
              bearish_div_score > bullish_div_score):
            prediction_type = "CRASH"
            confidence = max(confidence, 0.75)
            predicted_move_pct = -(2.5 + (compression_score * 2.5))  # -2.5 إلى -5%
            timeframe = "DAILY-H4"
        
        # سيناريو 3: اختراق صعودي
        elif compression_score > 0.65 and volume_score > 0.60:
            prediction_type = "BREAKOUT"
            confidence = max(confidence, 0.65)
            predicted_move_pct = 1.5 + (compression_score * 1.5)  # 1.5-3%
            timeframe = "H4"
        
        # سيناريو 4: تصحيح هبوطي
        elif extremes_score > 0.65 and rsi_extreme_score > 0.65 and "تشبع شراء" in rsi_signal:
            prediction_type = "DUMP"
            confidence = max(confidence, 0.65)
            predicted_move_pct = -(1.0 + (extremes_score * 1.5))  # -1 إلى -2.5%
            timeframe = "DAILY"
        
        # سيناريو 5: تكثف/تراكم
        elif compression_score > 0.60 and volume_score > 0.55:
            prediction_type = "CONSOLIDATION"
            confidence = max(confidence, 0.60)
            predicted_move_pct = 0  # حركة جانبية
            timeframe = "H4-H1"
        
        # جمع جميع الإشارات
        all_signals = []
        for analysis in analyses.values():
            if analysis['text'] and analysis['text'] != "لا توجد بيانات كافية":
                all_signals.append(analysis['text'])
        
        # تقييم المخاطرة
        risk_level = "MEDIUM"
        if prediction_type in ["EXPLOSION", "CRASH"]:
            risk_level = "HIGH" if confidence > 0.80 else "MEDIUM_HIGH"
        elif prediction_type in ["BREAKOUT", "DUMP"]:
            risk_level = "MEDIUM"
        elif prediction_type == "CONSOLIDATION":
            risk_level = "LOW"
        
        # التحقق من الثقة
        if confidence < MIN_PREDICTION_CONFIDENCE:
            prediction_type = "UNCLEAR"
        
        prediction_result = {
            'type': prediction_type,
            'confidence': confidence,
            'predicted_move_pct': predicted_move_pct,
            'timeframe': timeframe,
            'risk_level': risk_level,
            'all_signals': all_signals,
            'analyses': analyses,
            'weighted_score': weighted_score,
            'scores': scores
        }
        
        log.info(f"Prediction Result: {prediction_type} (Confidence: {confidence:.1%})")
        
        return prediction_result
        
    except Exception as e:
        log.error(f"Predictive analysis error: {e}")
        return {
            'type': "ERROR",
            'confidence': 0.5,
            'predicted_move_pct': 0,
            'timeframe': "UNKNOWN",
            'risk_level': "HIGH",
            'all_signals': [f"Error: {str(e)[:50]}"],
            'analyses': {},
            'weighted_score': 0.5,
            'scores': {}
        }

# ================ DATABASE FUNCTIONS ================

async def check_and_add_column(column_name: str, column_type: str):
    """Check if a column exists and add it if it doesn't"""
    try:
        # Check if column exists
        async with db_conn.execute(f"PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if column_name not in column_names:
                log.info(f"Adding missing column: {column_name}")
                await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_type}")
                await db_conn.commit()
                log.info(f"✅ Column {column_name} added successfully")
                return True
            else:
                log.debug(f"Column {column_name} already exists")
                return True
    except Exception as e:
        log.error(f"Error adding column {column_name}: {e}")
        return False

async def init_db():
    """Initialize database with predictive analysis columns"""
    global db_conn
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
        # Create basic table if it doesn't exist
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
                signal_hash TEXT UNIQUE,
                price_hash TEXT,
                close_reason TEXT,
                close_price REAL,
                close_timestamp DATETIME,
                pnl_percent REAL
            )
        """)
        
        await db_conn.commit()
        log.info("✅ Basic table created/verified")
        
        # Check and add ALL missing columns for predictive analysis
        required_columns = [
            # Predictive analysis fields
            ("prediction_type", "TEXT"),
            ("prediction_confidence", "REAL"),
            ("predicted_move_pct", "REAL"),
            ("prediction_timeframe", "TEXT"),
            ("risk_level", "TEXT"),
            
            # Analysis scores
            ("compression_score", "REAL"),
            ("divergence_score", "REAL"),
            ("volume_score", "REAL"),
            ("breakdown_score", "REAL"),
            ("extremes_score", "REAL"),
            ("rsi_score", "REAL"),
            
            # Confluence tracking
            ("confirmations", "INTEGER"),
            ("weighted_score", "REAL"),
            
            # Technical levels
            ("support_levels", "TEXT"),
            ("resistance_levels", "TEXT"),
            
            # For backward compatibility
            ("timeframe_alignment", "TEXT"),
            ("wave_structure", "TEXT"),
            ("strength_level", "TEXT"),
            ("indicators_signal", "TEXT"),
            ("volume_status", "TEXT"),
            ("synthesis_score", "REAL"),
        ]
        
        for column_name, column_type in required_columns:
            if not await check_and_add_column(column_name, column_type):
                log.warning(f"Failed to add column: {column_name}")
        
        # Create indexes (will ignore if already exist)
        try:
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON signals(symbol, timestamp)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_hash ON signals(signal_hash)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_prediction_type ON signals(prediction_type)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_prediction_confidence ON signals(prediction_confidence)")
            await db_conn.commit()
            log.info("✅ Database indexes created/verified")
        except Exception as e:
            log.warning(f"Index creation warning: {e}")
        
        log.info("✅ Database ready with all predictive columns")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ SIGNAL GENERATION WITH PREDICTIVE ANALYSIS ================

async def generate_predictive_signal(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """Generate signal with advanced predictive analysis for explosions/crashes"""
    try:
        log.info(f"🔮 Analyzing {symbol} for potential explosion/crash...")
        
        # 1. PREDICTIVE ANALYSIS FIRST
        prediction = predict_explosion_crash(data)
        
        # Check if prediction is strong enough
        if prediction['confidence'] < MIN_PREDICTION_CONFIDENCE:
            log.debug(f"{symbol}: Prediction confidence too low ({prediction['confidence']:.2f} < {MIN_PREDICTION_CONFIDENCE})")
            return None
        
        # Check specific thresholds for explosion/crash
        if prediction['type'] == "EXPLOSION" and prediction['confidence'] < MIN_EXPLOSION_SCORE:
            log.debug(f"{symbol}: Explosion confidence too low ({prediction['confidence']:.2f} < {MIN_EXPLOSION_SCORE})")
            return None
        
        if prediction['type'] == "CRASH" and prediction['confidence'] < MIN_CRASH_SCORE:
            log.debug(f"{symbol}: Crash confidence too low ({prediction['confidence']:.2f} < {MIN_CRASH_SCORE})")
            return None
        
        log.info(f"{symbol}: PREDICTION - {prediction['type']} with {prediction['confidence']:.1%} confidence")
        
        # 2. Get current price
        current_price = data['M15']['close'].iloc[-1]
        
        # 3. Determine side based on prediction
        if prediction['type'] in ["EXPLOSION", "BREAKOUT"]:
            side = "BUY"
            # For explosions, set more aggressive targets
            sl = current_price * 0.97  # 3% stop loss
            tp = current_price * (1 + abs(prediction['predicted_move_pct']) / 100)
        elif prediction['type'] in ["CRASH", "DUMP"]:
            side = "SELL"
            sl = current_price * 1.03  # 3% stop loss
            tp = current_price * (1 - abs(prediction['predicted_move_pct']) / 100)
        elif prediction['type'] == "CONSOLIDATION":
            # For consolidation, we might want to trade range or wait
            log.debug(f"{symbol}: Consolidation detected - no clear direction")
            return None
        else:
            log.debug(f"{symbol}: Unclear prediction type: {prediction['type']}")
            return None
        
        # 4. Calculate risk/reward
        risk = abs(current_price - sl)
        reward = abs(tp - current_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < 1.5:  # Minimum 1.5:1 R:R
            log.debug(f"{symbol}: Poor R:R ratio ({rr_ratio:.1f}:1)")
            return None
        
        # 5. Create unique hashes
        prediction_id = f"{symbol}:{prediction['type']}:{prediction['confidence']:.3f}"
        signal_hash = hashlib.md5(f"{prediction_id}:{time.time_ns()}".encode()).hexdigest()
        price_hash = hashlib.md5(f"{symbol}:{side}:{current_price:.8f}:{prediction['type']}".encode()).hexdigest()
        
        # 6. Get support/resistance levels
        support_levels = []
        resistance_levels = []
        
        h4_df = data.get('H4')
        if h4_df is not None and len(h4_df) >= 50:
            prices = h4_df['low'].values[-50:]
            current_h4_price = h4_df['close'].iloc[-1]
            
            # Simple support detection
            for i in range(2, len(prices)-2):
                if (prices[i] < prices[i-1] and prices[i] < prices[i-2] and
                    prices[i] < prices[i+1] and prices[i] < prices[i+2]):
                    if prices[i] < current_h4_price:
                        support_levels.append(float(prices[i]))
            
            prices_high = h4_df['high'].values[-50:]
            # Simple resistance detection
            for i in range(2, len(prices_high)-2):
                if (prices_high[i] > prices_high[i-1] and prices_high[i] > prices_high[i-2] and
                    prices_high[i] > prices_high[i+1] and prices_high[i] > prices_high[i+2]):
                    if prices_high[i] > current_h4_price:
                        resistance_levels.append(float(prices_high[i]))
        
        # 7. Count confirmations from analyses
        confirmations = sum(1 for score in prediction['scores'].values() if score > 0.6)
        
        # 8. Create signal with predictive analysis
        signal = {
            'symbol': symbol,
            'side': side,
            'entry': current_price,
            'sl': sl,
            'tp': tp,
            'status': 'OPEN',
            
            # Predictive analysis fields
            'prediction_type': prediction['type'],
            'prediction_confidence': prediction['confidence'],
            'predicted_move_pct': prediction['predicted_move_pct'],
            'prediction_timeframe': prediction['timeframe'],
            'risk_level': prediction['risk_level'],
            
            # Analysis summaries
            'compression_score': prediction['analyses'].get('compression', {}).get('score', 0),
            'divergence_score': prediction['analyses'].get('divergence', {}).get('score', 0),
            'volume_score': prediction['analyses'].get('volume', {}).get('score', 0),
            'breakdown_score': prediction['analyses'].get('breakdown', {}).get('score', 0),
            'extremes_score': prediction['analyses'].get('extremes', {}).get('score', 0),
            'rsi_score': prediction['analyses'].get('rsi', {}).get('score', 0),
            
            # Confluence tracking
            'confirmations': confirmations,
            'weighted_score': prediction['weighted_score'],
            
            # Technical levels
            'support_levels': json.dumps(support_levels[-3:]),
            'resistance_levels': json.dumps(resistance_levels[:3]),
            
            # Signal management
            'signal_hash': signal_hash,
            'price_hash': price_hash,
            
            # For backward compatibility
            'timeframe_alignment': f"Prediction: {prediction['type']}",
            'wave_structure': "Predictive Analysis",
            'strength_level': f"Confidence: {prediction['confidence']:.1%}",
            'indicators_signal': f"Move: {prediction['predicted_move_pct']:.1f}%",
            'volume_status': f"Risk: {prediction['risk_level']}",
            'synthesis_score': prediction['confidence']
        }
        
        log.info(f"✅ PREDICTIVE SIGNAL: {symbol} {side} @ {current_price:.4f}")
        log.info(f"   Type: {prediction['type']}, Confidence: {prediction['confidence']:.1%}")
        log.info(f"   Expected Move: {prediction['predicted_move_pct']:.1f}%, R:R: {rr_ratio:.1f}:1")
        
        return signal
        
    except Exception as e:
        log.error(f"Predictive signal generation error for {symbol}: {e}")
        return None

# ================ DATA FETCHING ================

async def fetch_ohlcv_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Fetch OHLCV data for all timeframes"""
    data = {}
    
    for tf_name, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=100)
            
            if ohlcv and len(ohlcv) >= 50:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                
                # Convert to numeric
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Remove any NaN values
                df = df.dropna()
                
                if len(df) >= 50:
                    data[tf_name] = df
                else:
                    log.debug(f"{symbol} {tf}: Not enough data after cleaning")
            else:
                log.debug(f"{symbol} {tf}: No data or insufficient length")
                
        except Exception as e:
            log.debug(f"{symbol} {tf} fetch error: {e}")
            continue
    
    # Check if we have minimum required data
    required_tfs = ['DAILY', 'H4', 'H1', 'M15']
    for tf in required_tfs:
        if tf not in data:
            log.debug(f"{symbol}: Missing {tf} data")
            return None
    
    return data

# ================ DUPLICATE PREVENTION ================

async def check_predictive_duplicate(symbol: str, prediction_type: str, confidence: float) -> bool:
    """Check for duplicate predictions"""
    try:
        async with db_lock:
            # Check for same symbol and prediction type in last 6 hours
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE symbol = ? AND prediction_type = ?
                AND timestamp > datetime('now', '-6 hours')
                AND status = 'OPEN'
            """, (symbol, prediction_type)) as cursor:
                result = await cursor.fetchone()
                recent_signals = result[0] if result else 0
            
            if recent_signals >= 1:
                log.debug(f"{symbol}: Already has open {prediction_type} prediction")
                return True
            
            # Check for similar confidence level in last 12 hours
            async with db_conn.execute("""
                SELECT prediction_confidence FROM signals 
                WHERE symbol = ? AND prediction_type = ?
                AND timestamp > datetime('now', '-12 hours')
                ORDER BY timestamp DESC LIMIT 1
            """, (symbol, prediction_type)) as cursor:
                result = await cursor.fetchone()
                if result:
                    last_confidence = result[0]
                    confidence_change = abs(confidence - last_confidence)
                    
                    # If confidence hasn't changed much, skip
                    if confidence_change < 0.1:  # Less than 10% change
                        log.debug(f"{symbol}: Similar confidence level ({confidence_change:.1%} change)")
                        return True
            
            return False
            
    except Exception as e:
        log.error(f"Duplicate check error: {e}")
        return False

# ================ MAIN SCANNING LOOP ================

async def scanning_loop(exchange):
    """Main scanning loop with predictive analysis"""
    log.info("🚀 Starting PREDICTIVE scanner for explosions/crashes")
    
    # Send startup message
    await tg("""
🚀 **بدء الماسح الضوئي التنبؤي - اكتشاف الانفجارات والانهيارات**

🔮 **نظام التحليل التنبؤي المتقدم:**
1. **اكتشاف التكثف السعري** (Volatility Compression)
2. **التباعد الزخمي** (Momentum Divergence)
3. **زيادات الحجم المفاجئة** (Volume Spikes)
4. **ضغط مستويات الدعم/المقاومة** (Support/Resistance Breakdown)
5. **التطرف في حركة السعر** (Price Action Extremes)
6. **تحليل تشبع RSI** (RSI Extreme Analysis)

🎯 **أهداف النظام:**
• **الانفجارات الصعودية** (EXPLOSION) - ثقة: {min_exp}%
• **الانهيارات الهبوطية** (CRASH) - ثقة: {min_crash}%
• **التنبؤ العام** - ثقة: {min_pred}%

📡 **جاهز لرصد الحركات الكبيرة القادمة...**
""".format(
        min_exp=MIN_EXPLOSION_SCORE * 100,
        min_crash=MIN_CRASH_SCORE * 100,
        min_pred=MIN_PREDICTION_CONFIDENCE * 100
    ))
    
    while True:
        try:
            log.info("=" * 60)
            log.info("Starting new PREDICTIVE scan cycle...")
            
            # Get top volume pairs
            try:
                tickers = await exchange.fetch_tickers()
                usdt_pairs = []
                
                for symbol, ticker in tickers.items():
                    if symbol.endswith('/USDT'):
                        volume = ticker.get('quoteVolume', 0)
                        if volume > 100000:  # $100K minimum volume
                            usdt_pairs.append((symbol, volume))
                
                usdt_pairs.sort(key=lambda x: x[1], reverse=True)
                top_pairs = usdt_pairs[:TOP_N]
                
                log.info(f"Found {len(top_pairs)} pairs (top {TOP_N} by volume)")
                
            except Exception as e:
                log.error(f"Error fetching tickers: {e}")
                await asyncio.sleep(30)
                continue
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                try:
                    log.debug(f"Processing {symbol} (Volume: ${volume:,.0f})...")
                    
                    # Fetch data
                    data = await fetch_ohlcv_data(exchange, symbol)
                    if not data:
                        continue
                    
                    # Generate predictive signal
                    signal = await generate_predictive_signal(data, symbol)
                    
                    if signal:
                        # Check for duplicate predictions
                        is_duplicate = await check_predictive_duplicate(
                            signal['symbol'], 
                            signal['prediction_type'], 
                            signal['prediction_confidence']
                        )
                        
                        if is_duplicate:
                            log.debug(f"{symbol}: Predictive duplicate check failed")
                            continue
                        
                        # Save to database
                        async with db_lock:
                            # Final duplicate check with signal hash
                            async with db_conn.execute(
                                "SELECT COUNT(*) FROM signals WHERE signal_hash = ?",
                                (signal['signal_hash'],)
                            ) as cursor:
                                result = await cursor.fetchone()
                                exists = result[0] if result else 0
                            
                            if exists == 0:
                                try:
                                    # Build column list and values
                                    columns = []
                                    values = []
                                    
                                    # Add all signal fields
                                    for key, value in signal.items():
                                        if key != 'status':  # status is set by default
                                            columns.append(key)
                                            values.append(value)
                                    
                                    # Create SQL query
                                    placeholders = ', '.join(['?'] * len(values))
                                    column_names = ', '.join(columns)
                                    
                                    await db_conn.execute(f"""
                                        INSERT INTO signals ({column_names}, status) 
                                        VALUES ({placeholders}, 'OPEN')
                                    """, values)
                                    
                                    await db_conn.commit()
                                    
                                    # Send detailed Telegram alert
                                    await send_predictive_telegram_alert(signal)
                                    signals_found += 1
                                    log.info(f"✅ Predictive signal sent for {symbol}")
                                except Exception as e:
                                    log.error(f"Database insert error for {symbol}: {e}")
                                    # Try simplified insert
                                    try:
                                        await db_conn.execute("""
                                            INSERT INTO signals (
                                                symbol, side, entry, sl, tp, status,
                                                signal_hash, price_hash,
                                                prediction_type, prediction_confidence
                                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                                            signal['tp'], 'OPEN',
                                            signal['signal_hash'], signal['price_hash'],
                                            signal['prediction_type'], signal['prediction_confidence']
                                        ))
                                        await db_conn.commit()
                                        await send_predictive_telegram_alert(signal)
                                        signals_found += 1
                                        log.info(f"✅ Predictive signal sent (simplified) for {symbol}")
                                    except Exception as e2:
                                        log.error(f"Simplified insert also failed: {e2}")
                            else:
                                log.debug(f"Final duplicate check failed for {symbol}")
                    
                    # Respect rate limits
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    log.error(f"Error processing {symbol}: {e}")
                    continue
            
            log.info(f"Predictive scan complete. Found {signals_found} high-confidence signals.")
            
            # Wait for next scan
            log.info(f"Waiting {SCAN_INTERVAL} seconds for next scan...")
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
            await asyncio.sleep(30)

async def send_predictive_telegram_alert(signal: Dict):
    """Send detailed Telegram alert for predictive signal"""
    try:
        side_ar = "شراء" if signal['side'] == "BUY" else "بيع"
        entry = signal['entry']
        sl = signal['sl']
        tp = signal['tp']
        
        risk_pct = abs(entry - sl) / entry * 100 if entry != 0 else 0
        reward_pct = abs(tp - entry) / entry * 100 if entry != 0 else 0
        rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0
        
        # Parse prediction type Arabic
        pred_type_ar = {
            "EXPLOSION": "💥 انفجار صعودي",
            "CRASH": "📉 انهيار هبوطي", 
            "BREAKOUT": "🚀 اختراق صعودي",
            "DUMP": "🔻 تصحيح هبوطي",
            "CONSOLIDATION": "⚖️ تكثف/تراكم",
            "UNCLEAR": "❓ غير واضح"
        }.get(signal['prediction_type'], signal['prediction_type'])
        
        # Parse risk level Arabic
        risk_ar = {
            "LOW": "منخفضة",
            "MEDIUM": "متوسطة",
            "MEDIUM_HIGH": "متوسطة-عالية",
            "HIGH": "عالية"
        }.get(signal['risk_level'], signal['risk_level'])
        
        # Parse support/resistance levels
        try:
            support_levels = json.loads(signal.get('support_levels', '[]'))
            resistance_levels = json.loads(signal.get('resistance_levels', '[]'))
        except:
            support_levels = []
            resistance_levels = []
        
        message = f"""
🎯 **تنبؤ حركة سعرية - تحليل متقدم**

**{signal['symbol']}** | **{side_ar}**
🔮 **التنبؤ:** {pred_type_ar}
📊 **الثقة:** {signal['prediction_confidence']:.1%}
⏱️ **الإطار الزمني:** {signal['prediction_timeframe']}
⚠️ **المخاطرة:** {risk_ar}

━━━━━━━━━━━━━━━━━━
📈 **التحليل التنبؤي:**

• **التكثف السعري:** {signal.get('compression_score', 0):.1%}
• **التباعد الزخمي:** {signal.get('divergence_score', 0):.1%}
• **زيادة الحجم:** {signal.get('volume_score', 0):.1%}
• **ضغط المستويات:** {signal.get('breakdown_score', 0):.1%}
• **التطرف السعري:** {signal.get('extremes_score', 0):.1%}
• **تشبع RSI:** {signal.get('rsi_score', 0):.1%}

• **التوكيدات:** {signal.get('confirmations', 0)}/6
• **الحركة المتوقعة:** {signal.get('predicted_move_pct', 0):+.1f}%

━━━━━━━━━━━━━━━━━━
💰 **مستويات التداول:**

• **الدخول:** `{entry:.4f}`
• **وقف الخسارة:** `{sl:.4f}` ({risk_pct:.1f}%)
• **هدف الربح:** `{tp:.4f}` ({reward_pct:.1f}%)
• **نسبة الربح/المخاطرة:** **{rr_ratio:.1f}:1**
"""
        
        if support_levels:
            message += f"• **الدعوم القريبة:** {', '.join([f'{s:.4f}' for s in support_levels])}\n"
        if resistance_levels:
            message += f"• **المقاومات القريبة:** {', '.join([f'{r:.4f}' for r in resistance_levels])}\n"
        
        message += f"""
━━━━━━━━━━━━━━━━━━
⚡ **هذا تنبؤ لحركة قوية محتملة**
⏰ **ينتهي الصلاحية:** خلال {signal['prediction_timeframe']}

#{side_ar} #تنبؤ_سعري #حركة_قوية
"""
        
        await tg(message)
        
    except Exception as e:
        log.error(f"Telegram alert error: {e}")
        # Send simplified alert
        simplified = f"""
✅ **تنبؤ جديد:** {signal['symbol']} {signal['side']}
النوع: {signal['prediction_type']}
الدخول: {signal['entry']:.4f} | الثقة: {signal['prediction_confidence']:.1%}
"""
        await tg(simplified)

# ================ WEB API ================

app = FastAPI(title="Predictive Visual Synthesis Scanner")

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "Predictive Visual Synthesis Scanner",
        "methodology": "Explosion/Crash Prediction Analysis",
        "version": "4.0 - Predictive Edition",
        "min_prediction_confidence": MIN_PREDICTION_CONFIDENCE,
        "min_explosion_score": MIN_EXPLOSION_SCORE,
        "min_crash_score": MIN_CRASH_SCORE,
        "min_confirmations": CONFLUENCE_REQUIRED,
        "top_n": TOP_N,
        "scan_interval": SCAN_INTERVAL
    }

# ================ MAIN ================

async def main():
    global exchange
    
    log.info("=" * 70)
    log.info("🚀 PREDICTIVE VISUAL SYNTHESIS SCANNER - EXPLOSION/CRASH DETECTION")
    log.info("=" * 70)
    
    # Check Telegram settings
    log.info(f"📱 Telegram Token: {'✅ SET' if TELEGRAM_TOKEN else '❌ NOT SET'}")
    log.info(f"📱 Telegram Chat ID: {'✅ SET' if TELEGRAM_CHAT_ID else '❌ NOT SET'}")
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ Telegram credentials are not set. Alerts will NOT be sent!")
        log.warning("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
    else:
        log.info("✅ Telegram credentials verified")
    
    log.info(f"🎯 Min Prediction Confidence: {MIN_PREDICTION_CONFIDENCE}")
    log.info(f"💥 Min Explosion Score: {MIN_EXPLOSION_SCORE}")
    log.info(f"📉 Min Crash Score: {MIN_CRASH_SCORE}")
    log.info(f"📊 Top N pairs: {TOP_N}, Scan Interval: {SCAN_INTERVAL}s")
    log.info("=" * 70)
    
    # Initialize database
    if not await init_db():
        log.error("Failed to initialize database")
        return
    
    # Initialize exchange
    try:
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 30000
        })
        
        # Test connection
        ticker = await exchange.fetch_ticker("BTC/USDT")
        log.info(f"✅ Exchange connected. BTC price: {ticker['last']}")
        
    except Exception as e:
        log.error(f"Failed to connect to exchange: {e}")
        return
    
    # Test Telegram connection
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        telegram_ok = await test_telegram()
        if not telegram_ok:
            log.warning("Proceeding without Telegram notifications...")
    else:
        log.warning("Telegram credentials missing. Running without notifications.")
    
    # Send startup message
    startup_msg = f"""
🚀 **الماسح الضوئي التنبؤي - اكتشاف الانفجارات والانهيارات**

✅ **تم بدء التشغيل بنجاح**
✅ **نظام التحليل التنبؤي المتقدم مفعل**

**الإعدادات:**
• ثقة التنبؤ الدنيا: {MIN_PREDICTION_CONFIDENCE:.0%}
• ثقة الانفجار الدنيا: {MIN_EXPLOSION_SCORE:.0%}
• ثقة الانهيار الدنيا: {MIN_CRASH_SCORE:.0%}
• عدد الأزواج: {TOP_N}
• فاصل المسح: {SCAN_INTERVAL} ثانية

**جاهز لرصد الحركات الكبيرة القادمة!**

{time.strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    await tg(startup_msg)
    
    # Start scanning loop
    try:
        await scanning_loop(exchange)
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
        await tg("🛑 توقف الماسح يدوياً")
    except Exception as e:
        log.error(f"Scanner crashed: {e}")
        await tg(f"❌ تعطل الماسح: {str(e)[:100]}")
    finally:
        if exchange:
            await exchange.close()
        if db_conn:
            await db_conn.close()
        executor.shutdown()
        log.info("Predictive scanner shutdown complete")

if __name__ == "__main__":
    # Check for required packages
    import subprocess
    import sys
    
    required_packages = ['ccxt', 'pandas', 'numpy', 'httpx', 'fastapi', 'aiosqlite', 'requests']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        log.warning(f"Installing missing packages: {missing_packages}")
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
        log.info("✅ All packages installed. Restarting...")
        # Restart after installation
        os.execv(sys.executable, [sys.executable] + sys.argv)
    
    # Run the scanner
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
    except Exception as e:
        log.error(f"Fatal error: {e}")