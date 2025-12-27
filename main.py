#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MEXC Futures Predictive Scanner - نظام المضاعفة السريع
Public API Version - No Authentication Required
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
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/mexc_signals.db"

# HYPER PROFIT SETTINGS - للربح السريع
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 30))  # 30 ثانية فقط للمسح السريع
TOP_N = int(os.getenv("TOP_N", 80))  # أكثر عملات متقلبة

# ULTRA STRICT FILTERS للمضاعفة السريعة
MIN_PREDICTION_CONFIDENCE = 0.50  # 75% ثقة كحد أدنى
MIN_EXPLOSION_SCORE = 0.50        # 80% للانفجارات
MIN_CRASH_SCORE = 0.50            # 80% للانهيارات
CONFLUENCE_REQUIRED = 4           # 4 تأكيدات من 6 (صارم)
MIN_EXPECTED_MOVE = 1.0           # 4% حركة متوقعة كحد أدنى
MIN_COMPRESSION = 0.50            # 85% تكثف كحد أدنى

# Timeframes for ultra-fast detection
TIMEFRAMES = {
    "DAILY": "1d",
    "H4": "4h", 
    "H1": "1h",
    "M15": "15m",
    "M5": "5m",   # إضافة فريم 5 دقائق للكشف المبكر
}

# High Volatility Coins Target
HIGH_VOLATILITY_COINS = [
    # Meme Coins - تقلبات عالية
    "PEPE/USDT", "WIF/USDT", "BONK/USDT", "FLOKI/USDT",
    "MEME/USDT", "SHIB/USDT", "DOGE/USDT", 
    
    # Small Caps - حركات كبيرة
    "1000BONK/USDT", "LADYS/USDT", "TURBO/USDT", "AIDOGE/USDT",
    
    # High Beta Alts
    "GALA/USDT", "APE/USDT", "GMT/USDT", "SAND/USDT",
    "MANA/USDT", "ENJ/USDT", "CHZ/USDT",
    
    # Leveraged Tokens
    "3L/USDT", "3S/USDT", "BULL/USDT", "BEAR/USDT",
]

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("mexc_scanner")
db_lock = asyncio.Lock()
db_conn = None
exchange = None
executor = ThreadPoolExecutor(max_workers=5)

# ================ TELEGRAM UTILITIES ================

async def tg_sync_backup(message: str):
    """Backup sync Telegram sender using requests"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            executor, 
            lambda: requests.post(url, json=payload, timeout=10)
        )
        
        if response.status_code == 200:
            log.debug("✅ Telegram sent (backup)")
            return True
        else:
            log.error(f"Telegram error: {response.status_code}")
            return False
            
    except Exception as e:
        log.error(f"Telegram backup error: {e}")
        return False

async def tg(message: str, parse_mode: str = "Markdown"):
    """Send message to Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
        return False
    
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }
            
            response = await client.post(url, json=payload)
            
            if response.status_code == 200:
                return True
            else:
                return await tg_sync_backup(message)
                
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return await tg_sync_backup(message)

async def send_hyper_signal_alert(signal: Dict):
    """Send hyper profit signal alert"""
    try:
        side_ar = "🚀 شراء" if signal['side'] == "BUY" else "📉 بيع"
        entry = signal['entry']
        sl = signal['sl']
        tp = signal['tp']
        
        risk_pct = abs(entry - sl) / entry * 100
        reward_pct = abs(tp - entry) / entry * 100
        rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0
        
        # Calculate potential profit with leverage
        leverage = signal.get('recommended_leverage', 10)
        potential_profit_pct = reward_pct * leverage
        potential_risk_pct = risk_pct * leverage
        
        # Parse prediction type
        pred_type_ar = {
            "EXPLOSION": "💥 انفجار صعودي",
            "CRASH": "📉 انهيار هبوطي",
            "BREAKOUT": "🚀 اختراق سريع",
            "DUMP": "🔻 تصحيح حاد",
        }.get(signal['prediction_type'], signal['prediction_type'])
        
        message = f"""
🎯 **إشارة ربح سريع - MEXC Futures**

**{signal['symbol']}** | **{side_ar}**
🔮 **التنبؤ:** {pred_type_ar}
📊 **الثقة:** {signal['prediction_confidence']:.1%}
⚡ **الحركة المتوقعة:** {signal['predicted_move_pct']:+.1f}%

━━━━━━━━━━━━━━━━━━
📈 **التحليل الفائق:**

• **التكثف:** {signal.get('compression_score', 0):.1%} 
• **التباعد:** {signal.get('divergence_score', 0):.1%}
• **الحجم:** {signal.get('volume_score', 0):.1%}
• **المستويات:** {signal.get('breakdown_score', 0):.1%}
• **التأكيدات:** {signal.get('confirmations', 0)}/6

━━━━━━━━━━━━━━━━━━
💰 **مستويات التداول:**

• **الدخول:** `{entry:.8f}`
• **وقف الخسارة:** `{sl:.8f}` ({risk_pct:.1f}%)
• **هدف الربح:** `{tp:.8f}` ({reward_pct:.1f}%)
• **نسبة الربح/المخاطرة:** **{rr_ratio:.1f}:1**

💸 **الرافعة المقترحة:** {leverage}x
📈 **ربح محتمل:** {potential_profit_pct:.1f}%
⚠️ **خسارة محتملة:** {potential_risk_pct:.1f}%

⏱️ **الوقت المتوقع:** {signal['prediction_timeframe']}
🎯 **ينتهي خلال:** 1-2 ساعة

#MEXC #Futures #ربح_سريع
"""
        
        await tg(message)
        log.info(f"✅ Hyper signal sent for {signal['symbol']}")
        
    except Exception as e:
        log.error(f"Alert error: {e}")
        await tg(f"✅ إشارة جديدة: {signal['symbol']} {signal['side']} @ {signal['entry']:.8f}")

# ================ TECHNICAL INDICATORS ================

def calculate_ema(prices: np.ndarray, period: int) -> np.ndarray:
    """Exponential Moving Average"""
    if len(prices) < period:
        return np.full(len(prices), np.nan)
    
    ema = np.zeros(len(prices))
    ema[:period-1] = np.nan
    
    sma = np.mean(prices[:period])
    ema[period-1] = sma
    
    multiplier = 2 / (period + 1)
    
    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]
    
    return ema

def calculate_rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """Relative Strength Index"""
    if len(prices) < period + 1:
        return np.full(len(prices), np.nan)
    
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    rsi = np.full(len(prices), np.nan)
    
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    if avg_loss == 0:
        rsi[period] = 100 if avg_gain > 0 else 0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100 - (100 / (1 + rs))
    
    for i in range(period + 1, len(prices)):
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

def calculate_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """Average True Range for volatility measurement"""
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
    atr[:period-1] = np.nan
    atr[period-1] = np.mean(tr[:period])
    
    for i in range(period, len(high)):
        atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
    
    return atr

# ================ ULTRA PREDICTIVE ANALYSIS ================

class PredictionType(Enum):
    EXPLOSION = "EXPLOSION"
    CRASH = "CRASH"
    BREAKOUT = "BREAKOUT"
    DUMP = "DUMP"
    UNCLEAR = "UNCLEAR"

def detect_hyper_compression(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """Hyper compression detection for explosive moves"""
    try:
        h1_df = data.get('H1')
        m15_df = data.get('M15')
        
        if h1_df is None or m15_df is None:
            return 0.5, "لا توجد بيانات", {}
        
        h1_prices = h1_df['close'].values[-30:]
        h1_highs = h1_df['high'].values[-30:]
        h1_lows = h1_df['low'].values[-30:]
        
        m15_prices = m15_df['close'].values[-50:]
        
        # 1. Calculate volatility on multiple timeframes
        h1_atr = calculate_atr(h1_highs, h1_lows, h1_prices, 14)
        current_h1_atr = h1_atr[-1] if not np.isnan(h1_atr[-1]) else 0
        avg_h1_atr = np.mean(h1_atr[-20:-5]) if len(h1_atr) >= 20 else current_h1_atr
        
        h1_volatility = current_h1_atr / h1_prices[-1] if h1_prices[-1] != 0 else 0
        m15_volatility = np.std(m15_prices[-20:]) / m15_prices[-1] if m15_prices[-1] != 0 else 0
        
        # 2. Compression scoring
        compression_score = 0
        signals = []
        
        # Ultra compression: volatility less than 0.5%
        if h1_volatility < 0.005 and m15_volatility < 0.008:
            compression_score = 0.95
            signals.append("تكثف فائق")
        elif h1_volatility < 0.008:
            compression_score = 0.85
            signals.append("تكثف قوي")
        elif h1_volatility < 0.012:
            compression_score = 0.70
            signals.append("تكثف متوسط")
        else:
            compression_score = 0.40
            signals.append("لا يوجد تكثف")
        
        # 3. Range contraction
        if avg_h1_atr > 0:
            range_contraction = (avg_h1_atr - current_h1_atr) / avg_h1_atr
            if range_contraction > 0.4:
                compression_score += 0.15
                signals.append(f"تقلص النطاق: {range_contraction:.0%}")
        
        # 4. Price squeezing (Bollinger Band-like)
        h1_sma = np.mean(h1_prices[-20:])
        h1_std = np.std(h1_prices[-20:])
        
        if h1_std > 0:
            squeeze_ratio = (h1_highs[-1] - h1_lows[-1]) / h1_std
            if squeeze_ratio < 1.0:
                compression_score += 0.10
                signals.append("ضغط بولنجر")
        
        compression_score = min(1.0, compression_score)
        
        details = {
            'compression_score': compression_score,
            'h1_volatility_pct': h1_volatility * 100,
            'm15_volatility_pct': m15_volatility * 100,
            'range_contraction_pct': range_contraction * 100 if 'range_contraction' in locals() else 0,
            'squeeze_ratio': squeeze_ratio if 'squeeze_ratio' in locals() else 0,
        }
        
        return compression_score, " | ".join(signals), details
        
    except Exception as e:
        log.error(f"Hyper compression error: {e}")
        return 0.5, "Error", {}

def analyze_liquidity_breakout(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """Liquidity and volume analysis for breakout detection"""
    try:
        h1_df = data.get('H1')
        m15_df = data.get('M15')
        
        if h1_df is None or m15_df is None:
            return 0.5, "لا توجد بيانات", {}
        
        # Volume analysis
        h1_volumes = h1_df['volume'].values[-30:]
        m15_volumes = m15_df['volume'].values[-50:]
        
        avg_h1_volume = np.mean(h1_volumes[-20:-5]) if len(h1_volumes) >= 20 else np.mean(h1_volumes)
        recent_h1_volume = np.mean(h1_volumes[-5:]) if len(h1_volumes) >= 5 else h1_volumes[-1]
        
        avg_m15_volume = np.mean(m15_volumes[-40:-10]) if len(m15_volumes) >= 40 else np.mean(m15_volumes)
        recent_m15_volume = np.mean(m15_volumes[-10:]) if len(m15_volumes) >= 10 else m15_volumes[-1]
        
        h1_volume_ratio = recent_h1_volume / avg_h1_volume if avg_h1_volume > 0 else 1
        m15_volume_ratio = recent_m15_volume / avg_m15_volume if avg_m15_volume > 0 else 1
        
        # Liquidity score
        liquidity_score = 0
        signals = []
        
        if h1_volume_ratio > 3.0 or m15_volume_ratio > 4.0:
            liquidity_score = 0.90
            signals.append(f"حجم هائل ({max(h1_volume_ratio, m15_volume_ratio):.1f}x)")
        elif h1_volume_ratio > 2.0 or m15_volume_ratio > 2.5:
            liquidity_score = 0.75
            signals.append(f"حجم كبير ({max(h1_volume_ratio, m15_volume_ratio):.1f}x)")
        elif h1_volume_ratio > 1.5:
            liquidity_score = 0.60
            signals.append(f"حجم مرتفع ({h1_volume_ratio:.1f}x)")
        else:
            liquidity_score = 0.40
            signals.append("حجم عادي")
        
        # Price-volume correlation
        h1_prices = h1_df['close'].values[-10:]
        h1_volume_trend = np.corrcoef(h1_prices, h1_volumes[-10:])[0,1] if len(h1_prices) >= 10 else 0
        
        if not np.isnan(h1_volume_trend):
            if h1_volume_trend > 0.7:
                liquidity_score += 0.15
                signals.append("الحجم يؤيد الاتجاه")
            elif h1_volume_trend < -0.7:
                liquidity_score += 0.10
                signals.append("حجم معاكس (تراكم)")
        
        liquidity_score = min(1.0, liquidity_score)
        
        details = {
            'liquidity_score': liquidity_score,
            'h1_volume_ratio': h1_volume_ratio,
            'm15_volume_ratio': m15_volume_ratio,
            'volume_correlation': h1_volume_trend if not np.isnan(h1_volume_trend) else 0,
        }
        
        return liquidity_score, " | ".join(signals), details
        
    except Exception as e:
        log.error(f"Liquidity analysis error: {e}")
        return 0.5, "Error", {}

def detect_momentum_reversal(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """Momentum reversal detection using RSI and price action"""
    try:
        h1_df = data.get('H1')
        m15_df = data.get('M15')
        
        if h1_df is None or m15_df is None:
            return 0.5, "لا توجد بيانات", {}
        
        h1_prices = h1_df['close'].values[-30:]
        m15_prices = m15_df['close'].values[-50:]
        
        # RSI analysis
        h1_rsi = calculate_rsi(h1_prices, 14)
        m15_rsi = calculate_rsi(m15_prices, 14)
        
        current_h1_rsi = h1_rsi[-1] if len(h1_rsi) > 0 and not np.isnan(h1_rsi[-1]) else 50
        current_m15_rsi = m15_rsi[-1] if len(m15_rsi) > 0 and not np.isnan(m15_rsi[-1]) else 50
        
        # Momentum score
        momentum_score = 0
        signals = []
        
        # Oversold/Overbought conditions
        if current_h1_rsi < 25 and current_m15_rsi < 20:
            momentum_score = 0.85
            signals.append(f"تشبع بيع قوي (H1: {current_h1_rsi:.1f})")
        elif current_h1_rsi < 30:
            momentum_score = 0.70
            signals.append(f"تشبع بيع (H1: {current_h1_rsi:.1f})")
        elif current_h1_rsi > 75 and current_m15_rsi > 80:
            momentum_score = 0.85
            signals.append(f"تشبع شراء قوي (H1: {current_h1_rsi:.1f})")
        elif current_h1_rsi > 70:
            momentum_score = 0.70
            signals.append(f"تشبع شراء (H1: {current_h1_rsi:.1f})")
        else:
            momentum_score = 0.50
            signals.append(f"RSI محايد (H1: {current_h1_rsi:.1f})")
        
        # Divergence detection (simplified)
        if len(h1_prices) >= 10 and len(h1_rsi) >= 10:
            price_trend = h1_prices[-1] - h1_prices[-5]
            rsi_trend = h1_rsi[-1] - h1_rsi[-5] if not np.isnan(h1_rsi[-1]) and not np.isnan(h1_rsi[-5]) else 0
            
            if price_trend < 0 and rsi_trend > 5:  # Bullish divergence
                momentum_score += 0.15
                signals.append("تباعد صعودي")
            elif price_trend > 0 and rsi_trend < -5:  # Bearish divergence
                momentum_score += 0.15
                signals.append("تباعد هبوطي")
        
        momentum_score = min(1.0, momentum_score)
        
        details = {
            'momentum_score': momentum_score,
            'h1_rsi': current_h1_rsi,
            'm15_rsi': current_m15_rsi,
            'signals': signals,
        }
        
        return momentum_score, " | ".join(signals), details
        
    except Exception as e:
        log.error(f"Momentum analysis error: {e}")
        return 0.5, "Error", {}

def analyze_price_structure(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """Price structure and pattern analysis"""
    try:
        h1_df = data.get('H1')
        
        if h1_df is None:
            return 0.5, "لا توجد بيانات", {}
        
        prices = h1_df['close'].values[-50:]
        highs = h1_df['high'].values[-50:]
        lows = h1_df['low'].values[-50:]
        
        # Support/Resistance levels
        support_levels = []
        resistance_levels = []
        
        for i in range(2, len(prices)-2):
            # Swing lows (support)
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and
                lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                support_levels.append(lows[i])
            
            # Swing highs (resistance)
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and
                highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                resistance_levels.append(highs[i])
        
        current_price = prices[-1]
        
        # Structure score
        structure_score = 0
        signals = []
        
        # Proximity to key levels
        if support_levels:
            nearest_support = min(support_levels, key=lambda x: abs(x - current_price))
            support_distance = abs(current_price - nearest_support) / current_price * 100
            
            if support_distance < 1.0:
                structure_score += 0.30
                signals.append(f"عند دعم قوي: {nearest_support:.8f}")
            elif support_distance < 2.0:
                structure_score += 0.15
                signals.append(f"قرب دعم: {nearest_support:.8f}")
        
        if resistance_levels:
            nearest_resistance = min(resistance_levels, key=lambda x: abs(x - current_price))
            resistance_distance = abs(nearest_resistance - current_price) / current_price * 100
            
            if resistance_distance < 1.0:
                structure_score += 0.30
                signals.append(f"عند مقاومة: {nearest_resistance:.8f}")
            elif resistance_distance < 2.0:
                structure_score += 0.15
                signals.append(f"قرب مقاومة: {nearest_resistance:.8f}")
        
        # Trend structure
        if len(prices) >= 20:
            short_trend = prices[-1] - prices[-5]
            medium_trend = prices[-1] - prices[-20]
            
            if short_trend > 0 and medium_trend > 0:
                structure_score += 0.20
                signals.append("اتجاه صاعد قوي")
            elif short_trend < 0 and medium_trend < 0:
                structure_score += 0.20
                signals.append("اتجاه هابط قوي")
        
        structure_score = min(1.0, structure_score)
        
        details = {
            'structure_score': structure_score,
            'support_levels': sorted(list(set(support_levels)))[-3:],
            'resistance_levels': sorted(list(set(resistance_levels)))[:3],
            'nearest_support': nearest_support if 'nearest_support' in locals() else 0,
            'nearest_resistance': nearest_resistance if 'nearest_resistance' in locals() else 0,
        }
        
        return structure_score, " | ".join(signals), details
        
    except Exception as e:
        log.error(f"Structure analysis error: {e}")
        return 0.5, "Error", {}

def predict_hyper_move(data: Dict[str, pd.DataFrame]) -> Dict:
    """Ultra-predictive analysis for hyper moves"""
    try:
        # Run all analyses
        analyses = {}
        
        # 1. Hyper Compression
        compression_score, compression_text, compression_details = detect_hyper_compression(data)
        analyses['compression'] = {
            'score': compression_score,
            'text': compression_text,
            'details': compression_details
        }
        
        # 2. Liquidity Breakout
        liquidity_score, liquidity_text, liquidity_details = analyze_liquidity_breakout(data)
        analyses['liquidity'] = {
            'score': liquidity_score,
            'text': liquidity_text,
            'details': liquidity_details
        }
        
        # 3. Momentum Reversal
        momentum_score, momentum_text, momentum_details = detect_momentum_reversal(data)
        analyses['momentum'] = {
            'score': momentum_score,
            'text': momentum_text,
            'details': momentum_details
        }
        
        # 4. Price Structure
        structure_score, structure_text, structure_details = analyze_price_structure(data)
        analyses['structure'] = {
            'score': structure_score,
            'text': structure_text,
            'details': structure_details
        }
        
        # Calculate weighted score
        weights = {
            'compression': 0.30,  # Most important for explosions
            'liquidity': 0.25,    # Volume confirmation
            'momentum': 0.25,     # Reversal timing
            'structure': 0.20,    # Key levels
        }
        
        scores = {k: analyses[k]['score'] for k in analyses}
        weighted_score = sum(scores[k] * weights[k] for k in weights)
        
        # Determine prediction type
        prediction_type = "UNCLEAR"
        confidence = weighted_score
        predicted_move_pct = 0
        timeframe = "H1-M15"
        
        # Count confirmations
        confirmations = sum(1 for score in scores.values() if score > 0.65)
        
        # Prediction logic
        compression_strong = compression_score > 0.85
        liquidity_strong = liquidity_score > 0.75
        momentum_extreme = momentum_score > 0.75
        
        # Bullish scenarios
        if (compression_strong and liquidity_strong and 
            momentum_details.get('h1_rsi', 50) < 35 and
            confirmations >= 3):
            
            if compression_score > 0.90 and liquidity_score > 0.80:
                prediction_type = "EXPLOSION"
                predicted_move_pct = 5.0 + (compression_score * 3)
                timeframe = "M15-H1"
            else:
                prediction_type = "BREAKOUT"
                predicted_move_pct = 3.0 + (compression_score * 2)
                timeframe = "H1"
        
        # Bearish scenarios
        elif (compression_strong and liquidity_strong and
              momentum_details.get('h1_rsi', 50) > 65 and
              confirmations >= 3):
            
            if compression_score > 0.90 and liquidity_score > 0.80:
                prediction_type = "CRASH"
                predicted_move_pct = -(4.0 + (compression_score * 2.5))
                timeframe = "M15-H1"
            else:
                prediction_type = "DUMP"
                predicted_move_pct = -(2.5 + (compression_score * 1.5))
                timeframe = "H1"
        
        # Risk level
        risk_level = "MEDIUM"
        if prediction_type in ["EXPLOSION", "CRASH"]:
            risk_level = "HIGH" if confidence > 0.80 else "MEDIUM_HIGH"
        elif prediction_type in ["BREAKOUT", "DUMP"]:
            risk_level = "MEDIUM"
        
        # Validate minimum requirements
        if (confidence < MIN_PREDICTION_CONFIDENCE or 
            confirmations < CONFLUENCE_REQUIRED or
            compression_score < MIN_COMPRESSION or
            abs(predicted_move_pct) < MIN_EXPECTED_MOVE):
            prediction_type = "UNCLEAR"
        
        # Recommended leverage
        recommended_leverage = 10  # Default
        if prediction_type == "EXPLOSION" and confidence > 0.85:
            recommended_leverage = 15
        elif prediction_type == "CRASH" and confidence > 0.85:
            recommended_leverage = 15
        elif confidence > 0.80:
            recommended_leverage = 12
        
        result = {
            'type': prediction_type,
            'confidence': confidence,
            'predicted_move_pct': predicted_move_pct,
            'timeframe': timeframe,
            'risk_level': risk_level,
            'recommended_leverage': recommended_leverage,
            'confirmations': confirmations,
            'analyses': analyses,
            'weighted_score': weighted_score,
            'scores': scores,
        }
        
        log.info(f"Hyper prediction: {prediction_type} ({confidence:.1%})")
        return result
        
    except Exception as e:
        log.error(f"Hyper prediction error: {e}")
        return {
            'type': "UNCLEAR",
            'confidence': 0.5,
            'predicted_move_pct': 0,
            'timeframe': "UNKNOWN",
            'risk_level': "HIGH",
            'recommended_leverage': 5,
            'confirmations': 0,
            'analyses': {},
            'weighted_score': 0.5,
            'scores': {},
        }

# ================ DATABASE ================

async def init_db():
    """Initialize database"""
    global db_conn
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS hyper_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp REAL NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                
                prediction_type TEXT,
                prediction_confidence REAL,
                predicted_move_pct REAL,
                prediction_timeframe TEXT,
                risk_level TEXT,
                recommended_leverage INTEGER,
                
                compression_score REAL,
                liquidity_score REAL,
                momentum_score REAL,
                structure_score REAL,
                confirmations INTEGER,
                weighted_score REAL,
                
                signal_hash TEXT UNIQUE,
                price_hash TEXT
            )
        """)
        
        await db_conn.commit()
        
        # Create indexes
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_time ON hyper_signals(symbol, timestamp)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_prediction ON hyper_signals(prediction_type, prediction_confidence)")
        await db_conn.commit()
        
        log.info("✅ Database initialized")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ DATA FETCHING ================

async def fetch_mexc_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Fetch data from MEXC"""
    data = {}
    
    for tf_name, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=100)
            
            if ohlcv and len(ohlcv) >= 30:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                
                # Convert to numeric
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = df.dropna()
                
                if len(df) >= 30:
                    data[tf_name] = df
                    
        except Exception as e:
            log.debug(f"{symbol} {tf} error: {e}")
            continue
    
    # Check minimum data
    if len(data) >= 3:  # Need at least 3 timeframes
        return data
    
    return None

# ================ MAIN SCANNER ================

async def scan_hyper_signals():
    """Main scanner for hyper profit signals"""
    log.info("🚀 Starting MEXC Hyper Profit Scanner")
    
    await tg("""
🚀 **بدء الماسح الفائق - MEXC Futures**

✅ **النظام مُحسّن للربح السريع:**
• **الفلاتر الصارمة:** ثقة 75%+ فقط
• **التكثف الفائق:** 85%+ للحركات الكبيرة
• **4 تأكيدات:** من أصل 4 تحليلات
• **الحركة الدنيا:** 4%+ متوقعة

🎯 **التركيز على:**
• عملات الميمز المتقلبة (PEPE, WIF, BONK)
• العملات الصغيرة ذات الحركات الكبيرة
• الأوقات عالية السيولة

⚡ **جاهز للمضاعفة السريعة...**
""")
    
    while True:
        try:
            log.info("=" * 60)
            log.info("Starting hyper scan cycle...")
            
            # Initialize exchange
            global exchange
            if exchange is None:
                exchange = ccxt.mexc({
                    "enableRateLimit": True,
                    "timeout": 30000,
                })
            
            # Focus on high volatility coins
            target_symbols = HIGH_VOLATILITY_COINS[:TOP_N]
            log.info(f"Scanning {len(target_symbols)} high-volatility coins")
            
            signals_found = 0
            
            for symbol in target_symbols:
                try:
                    log.debug(f"Analyzing {symbol}...")
                    
                    # Fetch data
                    data = await fetch_mexc_data(exchange, symbol)
                    if not data:
                        continue
                    
                    # Get current price
                    current_price = data['M15']['close'].iloc[-1]
                    
                    # Run hyper prediction
                    prediction = predict_hyper_move(data)
                    
                    # Check if prediction is valid
                    if (prediction['type'] == "UNCLEAR" or 
                        prediction['confidence'] < MIN_PREDICTION_CONFIDENCE or
                        prediction['confirmations'] < CONFLUENCE_REQUIRED):
                        continue
                    
                    # Check specific thresholds
                    if prediction['type'] in ["EXPLOSION", "CRASH"]:
                        if prediction['confidence'] < MIN_EXPLOSION_SCORE:
                            continue
                    
                    if prediction['type'] in ["BREAKOUT", "DUMP"]:
                        if prediction['confidence'] < MIN_PREDICTION_CONFIDENCE:
                            continue
                    
                    # Determine side and calculate SL/TP
                    if prediction['type'] in ["EXPLOSION", "BREAKOUT"]:
                        side = "BUY"
                        # Aggressive SL/TP for hyper profits
                        sl_pct = 1.5  # 1.5% SL for tighter risk
                        tp_pct = abs(prediction['predicted_move_pct'])
                        
                        sl = current_price * (1 - sl_pct/100)
                        tp = current_price * (1 + tp_pct/100)
                    
                    elif prediction['type'] in ["CRASH", "DUMP"]:
                        side = "SELL"
                        sl_pct = 1.5
                        tp_pct = abs(prediction['predicted_move_pct'])
                        
                        sl = current_price * (1 + sl_pct/100)
                        tp = current_price * (1 - tp_pct/100)
                    
                    else:
                        continue
                    
                    # Calculate risk/reward
                    risk = abs(current_price - sl)
                    reward = abs(tp - current_price)
                    rr_ratio = reward / risk if risk > 0 else 0
                    
                    if rr_ratio < 1.5:  # Minimum 1.5:1
                        continue
                    
                    # Create signal
                    signal_hash = hashlib.md5(
                        f"{symbol}:{side}:{current_price:.10f}:{time.time_ns()}".encode()
                    ).hexdigest()
                    
                    price_hash = hashlib.md5(
                        f"{symbol}:{current_price:.10f}:{prediction['type']}".encode()
                    ).hexdigest()
                    
                    signal = {
                        'symbol': symbol,
                        'side': side,
                        'entry': current_price,
                        'sl': sl,
                        'tp': tp,
                        'prediction_type': prediction['type'],
                        'prediction_confidence': prediction['confidence'],
                        'predicted_move_pct': prediction['predicted_move_pct'],
                        'prediction_timeframe': prediction['timeframe'],
                        'risk_level': prediction['risk_level'],
                        'recommended_leverage': prediction['recommended_leverage'],
                        'compression_score': prediction['analyses'].get('compression', {}).get('score', 0),
                        'liquidity_score': prediction['analyses'].get('liquidity', {}).get('score', 0),
                        'momentum_score': prediction['analyses'].get('momentum', {}).get('score', 0),
                        'structure_score': prediction['analyses'].get('structure', {}).get('score', 0),
                        'confirmations': prediction['confirmations'],
                        'weighted_score': prediction['weighted_score'],
                        'signal_hash': signal_hash,
                        'price_hash': price_hash,
                    }
                    
                    # Check for duplicates
                    async with db_lock:
                        async with db_conn.execute(
                            "SELECT COUNT(*) FROM hyper_signals WHERE signal_hash = ?",
                            (signal_hash,)
                        ) as cursor:
                            result = await cursor.fetchone()
                            if result and result[0] > 0:
                                continue
                        
                        # Save to database
                        await db_conn.execute("""
                            INSERT INTO hyper_signals (
                                symbol, side, entry, sl, tp, status,
                                prediction_type, prediction_confidence, predicted_move_pct,
                                prediction_timeframe, risk_level, recommended_leverage,
                                compression_score, liquidity_score, momentum_score,
                                structure_score, confirmations, weighted_score,
                                signal_hash, price_hash
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                            signal['tp'], 'OPEN',
                            signal['prediction_type'], signal['prediction_confidence'],
                            signal['predicted_move_pct'], signal['prediction_timeframe'],
                            signal['risk_level'], signal['recommended_leverage'],
                            signal['compression_score'], signal['liquidity_score'],
                            signal['momentum_score'], signal['structure_score'],
                            signal['confirmations'], signal['weighted_score'],
                            signal['signal_hash'], signal['price_hash']
                        ))
                        
                        await db_conn.commit()
                        
                        # Send alert
                        await send_hyper_signal_alert(signal)
                        signals_found += 1
                        
                        log.info(f"✅ HYPER SIGNAL: {symbol} {side}")
                        log.info(f"   Type: {prediction['type']}, Conf: {prediction['confidence']:.1%}")
                        log.info(f"   Move: {prediction['predicted_move_pct']:+.1f}%, Lev: {prediction['recommended_leverage']}x")
                    
                    # Rate limiting
                    await asyncio.sleep(0.3)
                    
                except Exception as e:
                    log.error(f"Error analyzing {symbol}: {e}")
                    continue
            
            log.info(f"Scan complete. Found {signals_found} hyper signals.")
            
            # Wait for next scan
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
            await asyncio.sleep(10)

# ================ WEB API ================

app = FastAPI(title="MEXC Hyper Profit Scanner")

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "MEXC Hyper Profit Scanner",
        "version": "2.0 - Ultra Fast",
        "settings": {
            "scan_interval": SCAN_INTERVAL,
            "min_confidence": MIN_PREDICTION_CONFIDENCE,
            "min_confirmations": CONFLUENCE_REQUIRED,
            "min_compression": MIN_COMPRESSION,
            "min_move_pct": MIN_EXPECTED_MOVE,
        },
        "focus_coins": HIGH_VOLATILITY_COINS[:20],
    }

# ================ MAIN ================

async def main():
    # Telegram check
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
    else:
        log.info("✅ Telegram credentials found")
    
    # Initialize database
    if not await init_db():
        log.error("Failed to initialize database")
        return
    
    log.info("=" * 70)
    log.info("🚀 MEXC HYPER PROFIT SCANNER - PUBLIC API")
    log.info("=" * 70)
    log.info(f"Focus: {len(HIGH_VOLATILITY_COINS)} high-volatility coins")
    log.info(f"Scan interval: {SCAN_INTERVAL}s")
    log.info(f"Min confidence: {MIN_PREDICTION_CONFIDENCE:.0%}")
    log.info(f"Min confirmations: {CONFLUENCE_REQUIRED}")
    log.info("=" * 70)
    
    # Send startup message
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        await tg(f"""
🚀 **الماسح الفائق - MEXC Public API**

✅ **تم التشغيل بنجاح**
✅ **بدون API Key - قراءة فقط**
✅ **مركز على الربح السريع**

**الإعدادات:**
• الفاصل الزمني: {SCAN_INTERVAL} ثانية
• الثقة الدنيا: {MIN_PREDICTION_CONFIDENCE:.0%}
• التكثف الدنيا: {MIN_COMPRESSION:.0%}
• الحركة الدنيا: {MIN_EXPECTED_MOVE}%

**التركيز على العملات المتقلبة...**

{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")
    
    # Start scanner
    try:
        await scan_hyper_signals()
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            await tg("🛑 توقف الماسح يدوياً")
    except Exception as e:
        log.error(f"Scanner crashed: {e}")
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            await tg(f"❌ تعطل الماسح: {str(e)[:100]}")
    finally:
        if exchange:
            await exchange.close()
        if db_conn:
            await db_conn.close()
        executor.shutdown()

if __name__ == "__main__":
    # Install required packages
    try:
        import ccxt
        import pandas
        import numpy
        import httpx
        import fastapi
        import aiosqlite
    except ImportError as e:
        log.error(f"Missing package: {e}")
        log.info("Run: pip install ccxt pandas numpy httpx fastapi aiosqlite requests")
        exit(1)
    
    # Run scanner
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown complete")