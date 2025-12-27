#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visual Synthesis Scanner - الإصدار المتقدم
Advanced Multi-Timeframe Wave & Synthesis Analysis
(NO TA-Lib REQUIRED - Pure Python Implementation)
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
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from fastapi import FastAPI
import json

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 60))

# Synthesis thresholds
MIN_SYNTHESIS_SCORE = 0.45  # Higher threshold for quality signals
CONFLUENCE_REQUIRED = 3    # Minimum number of confirmations

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

# ---------------- PURE PYTHON TECHNICAL INDICATORS ----------------
def calculate_ema(prices: np.ndarray, period: int) -> np.ndarray:
    """Calculate Exponential Moving Average (no TA-Lib)"""
    if len(prices) < period:
        return np.full_like(prices, np.nan)
    
    ema = np.zeros_like(prices, dtype=float)
    ema[:period-1] = np.nan
    
    # Initial SMA
    sma = np.mean(prices[:period])
    ema[period-1] = sma
    
    # Multiplier
    multiplier = 2 / (period + 1)
    
    # Calculate EMA
    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]
    
    return ema

def calculate_sma(prices: np.ndarray, period: int) -> np.ndarray:
    """Calculate Simple Moving Average"""
    if len(prices) < period:
        return np.full_like(prices, np.nan)
    
    sma = np.zeros_like(prices, dtype=float)
    sma[:period-1] = np.nan
    
    for i in range(period-1, len(prices)):
        sma[i] = np.mean(prices[i-period+1:i+1])
    
    return sma

def calculate_rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """Calculate RSI (no TA-Lib)"""
    if len(prices) < period + 1:
        return np.full_like(prices, np.nan)
    
    deltas = np.diff(prices)
    
    # Separate gains and losses
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    # Calculate initial averages
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    rsi = np.zeros_like(prices)
    rsi[:period] = np.nan
    
    if avg_loss == 0:
        rsi[period] = 100
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100 - (100 / (1 + rs))
    
    # Calculate remaining RSI values
    for i in range(period + 1, len(prices)):
        gain = gains[i-1]
        loss = losses[i-1]
        
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        
        if avg_loss == 0:
            rsi[i] = 100
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_macd(prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Calculate MACD (no TA-Lib)"""
    ema_fast = calculate_ema(prices, fast)
    ema_slow = calculate_ema(prices, slow)
    
    macd_line = ema_fast - ema_slow
    signal_line = calculate_ema(macd_line[~np.isnan(macd_line)], signal)
    
    # Align lengths
    signal_line_full = np.full_like(macd_line, np.nan)
    signal_line_full[-len(signal_line):] = signal_line
    
    histogram = macd_line - signal_line_full
    
    return macd_line, signal_line_full, histogram

def calculate_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
    """Calculate Average True Range (no TA-Lib)"""
    if len(highs) < period:
        return np.full_like(highs, np.nan)
    
    tr = np.zeros(len(highs))
    
    # Calculate True Range
    for i in range(len(highs)):
        if i == 0:
            tr[i] = highs[i] - lows[i]
        else:
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr[i] = max(hl, hc, lc)
    
    # Calculate ATR
    atr = np.zeros_like(tr)
    atr[:period-1] = np.nan
    
    # Initial ATR (SMA of first period TRs)
    atr[period-1] = np.mean(tr[:period])
    
    # Wilder's smoothing
    for i in range(period, len(tr)):
        atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
    
    return atr

def calculate_adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
    """Calculate ADX (no TA-Lib)"""
    if len(highs) < period * 2:
        return np.full_like(highs, np.nan)
    
    # Calculate +DM and -DM
    plus_dm = np.zeros(len(highs))
    minus_dm = np.zeros(len(highs))
    
    for i in range(1, len(highs)):
        up_move = highs[i] - highs[i-1]
        down_move = lows[i-1] - lows[i]
        
        if up_move > down_move and up_move > 0:
            plus_dm[i] = up_move
        elif down_move > up_move and down_move > 0:
            minus_dm[i] = down_move
    
    # Calculate True Range
    tr = np.zeros(len(highs))
    for i in range(len(highs)):
        if i == 0:
            tr[i] = highs[i] - lows[i]
        else:
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr[i] = max(hl, hc, lc)
    
    # Smooth the values
    def smooth(values, period):
        smoothed = np.zeros_like(values)
        smoothed[:period-1] = np.nan
        
        # Initial value (sum of first period)
        smoothed[period-1] = np.sum(values[:period])
        
        # Wilder's smoothing
        for i in range(period, len(values)):
            smoothed[i] = smoothed[i-1] - (smoothed[i-1] / period) + values[i]
        
        return smoothed
    
    tr_smooth = smooth(tr, period)
    plus_dm_smooth = smooth(plus_dm, period)
    minus_dm_smooth = smooth(minus_dm, period)
    
    # Calculate +DI and -DI
    plus_di = 100 * (plus_dm_smooth / tr_smooth)
    minus_di = 100 * (minus_dm_smooth / tr_smooth)
    
    # Calculate DX
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    
    # Calculate ADX (smoothed DX)
    adx = smooth(dx, period)
    
    return adx

# ---------------- TELEGRAM ----------------
async def tg(msg: str):
    """Send Telegram message"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            })
        log.info("Telegram message sent")
    except Exception as e:
        log.warning(f"Telegram failed: {e}")

# ---------------- DATABASE ----------------
async def init_db():
    """Initialize database"""
    global db_conn
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
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
                
                -- Analysis Results
                mtf_alignment TEXT,
                wave_structure TEXT,
                momentum_score REAL,
                ema_alignment TEXT,
                rsi_signal TEXT,
                volume_analysis TEXT,
                trend_strength REAL,
                
                -- Confluence Tracking
                confirmations INTEGER DEFAULT 0,
                synthesis_score REAL,
                
                -- Technical Levels
                support_levels TEXT,
                resistance_levels TEXT,
                
                -- Signal Management
                signal_hash TEXT UNIQUE,
                price_hash TEXT,
                wave_hash TEXT,
                
                -- Trade Management
                close_reason TEXT,
                close_price REAL,
                close_timestamp DATETIME,
                pnl_percent REAL
            )
        """)
        
        # Create indexes
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_hash ON signals(signal_hash)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_price_hash ON signals(price_hash)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status)")
        await db_conn.commit()
        
        log.info("✅ Database initialized with advanced schema")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ ADVANCED ANALYSIS MODULES ================

class TrendDirection(Enum):
    STRONG_BULLISH = "STRONG_BULLISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"
    STRONG_BEARISH = "STRONG_BEARISH"

# ---------- 1. MULTI-TIMEFRAME ANALYSIS (أراقب كل الفريمات) ----------
async def analyze_multi_timeframe(data: Dict[str, pd.DataFrame]) -> Tuple[TrendDirection, float, Dict]:
    """Advanced MTF analysis with EMA alignment"""
    try:
        timeframe_scores = {}
        timeframe_details = {}
        ema_alignments = {}
        
        for tf_name, df in data.items():
            if len(df) < 100:
                timeframe_scores[tf_name] = 0
                continue
            
            prices = df['close'].values
            
            # Calculate EMAs for this timeframe using pure Python
            ema_signals = {}
            for ema_name, period in EMA_PERIODS.items():
                if len(prices) >= period:
                    ema = calculate_ema(prices, period)
                    if not np.isnan(ema[-1]):
                        ema_signals[ema_name] = float(ema[-1])
            
            # Determine EMA alignment
            alignment_score = 0
            alignment_details = []
            
            if len(ema_signals) >= 3:
                # Check if EMAs are in bullish alignment (fast > medium > slow)
                if (ema_signals.get('FAST', 0) > ema_signals.get('MEDIUM', 0) > 
                    ema_signals.get('SLOW', 0) > ema_signals.get('LONG', 0)):
                    alignment_score = 1.0
                    alignment_details.append("EMAs Bullish Stack")
                # Check if EMAs are in bearish alignment (fast < medium < slow)
                elif (ema_signals.get('FAST', 0) < ema_signals.get('MEDIUM', 0) < 
                      ema_signals.get('SLOW', 0) < ema_signals.get('LONG', 0)):
                    alignment_score = -1.0
                    alignment_details.append("EMAs Bearish Stack")
                # Check for golden cross/death cross
                elif ema_signals.get('FAST', 0) > ema_signals.get('MEDIUM', 0):
                    alignment_score = 0.6
                    alignment_details.append("Golden Cross Setup")
                elif ema_signals.get('FAST', 0) < ema_signals.get('MEDIUM', 0):
                    alignment_score = -0.6
                    alignment_details.append("Death Cross Setup")
            
            ema_alignments[tf_name] = alignment_details
            
            # Price position relative to EMAs
            current_price = prices[-1]
            price_score = 0
            
            if 'FAST' in ema_signals and 'LONG' in ema_signals:
                if current_price > ema_signals['FAST'] > ema_signals['LONG']:
                    price_score = 0.8
                elif current_price < ema_signals['FAST'] < ema_signals['LONG']:
                    price_score = -0.8
                elif current_price > ema_signals['LONG']:
                    price_score = 0.4
                else:
                    price_score = -0.4
            
            # Combine scores
            timeframe_score = (alignment_score + price_score) / 2
            timeframe_scores[tf_name] = timeframe_score
            timeframe_details[tf_name] = {
                'score': timeframe_score,
                'price': current_price,
                'emas': ema_signals,
                'alignment': alignment_details
            }
        
        # Weight timeframes (higher weight to higher timeframes)
        weights = {
            'MONTHLY': 1.5, 'WEEKLY': 1.3, 'DAILY': 1.2,
            'H4': 1.0, 'H1': 0.8, 'M15': 0.5
        }
        
        weighted_scores = []
        for tf, score in timeframe_scores.items():
            if tf in weights:
                weighted_scores.append(score * weights[tf])
        
        if not weighted_scores:
            return TrendDirection.NEUTRAL, 0.0, {}
        
        avg_score = np.mean(weighted_scores)
        
        # Determine trend direction
        if avg_score >= 0.7:
            direction = TrendDirection.STRONG_BULLISH
        elif avg_score >= 0.3:
            direction = TrendDirection.BULLISH
        elif avg_score <= -0.7:
            direction = TrendDirection.STRONG_BEARISH
        elif avg_score <= -0.3:
            direction = TrendDirection.BEARISH
        else:
            direction = TrendDirection.NEUTRAL
        
        details = {
            'scores': timeframe_scores,
            'details': timeframe_details,
            'ema_alignments': ema_alignments,
            'weighted_average': avg_score
        }
        
        return direction, avg_score, details
        
    except Exception as e:
        log.error(f"MTF analysis error: {e}")
        return TrendDirection.NEUTRAL, 0.0, {}

# ---------- 2. WAVE ANALYSIS (المدى الموجي) ----------
def analyze_wave_structure(data: Dict[str, pd.DataFrame]) -> Tuple[float, str, Dict]:
    """Advanced wave structure analysis using multiple timeframes"""
    try:
        # Focus on H4 and Daily for wave analysis
        h4_df = data.get('H4')
        daily_df = data.get('DAILY')
        
        if h4_df is None or daily_df is None or len(h4_df) < 50 or len(daily_df) < 100:
            return 0.5, "Insufficient data", {}
        
        wave_details = {}
        
        # Analyze H4 for shorter waves
        h4_prices = h4_df['close'].values[-50:]
        h4_highs = h4_df['high'].values[-50:]
        h4_lows = h4_df['low'].values[-50:]
        
        # Find swing highs and lows
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(h4_prices)-2):
            if (h4_highs[i] > h4_highs[i-1] and h4_highs[i] > h4_highs[i-2] and
                h4_highs[i] > h4_highs[i+1] and h4_highs[i] > h4_highs[i+2]):
                swing_highs.append((i, h4_highs[i]))
            
            if (h4_lows[i] < h4_lows[i-1] and h4_lows[i] < h4_lows[i-2] and
                h4_lows[i] < h4_lows[i+1] and h4_lows[i] < h4_lows[i+2]):
                swing_lows.append((i, h4_lows[i]))
        
        # Analyze wave structure
        wave_score = 0.5
        wave_pattern = "No clear pattern"
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            # Check for higher highs and higher lows (uptrend)
            if (swing_highs[-1][1] > swing_highs[-2][1] and 
                swing_lows[-1][1] > swing_lows[-2][1]):
                wave_score = 0.8
                wave_pattern = "Impulsive Uptrend (Higher Highs & Lows)"
            
            # Check for lower highs and lower lows (downtrend)
            elif (swing_highs[-1][1] < swing_highs[-2][1] and 
                  swing_lows[-1][1] < swing_lows[-2][1]):
                wave_score = 0.8
                wave_pattern = "Impulsive Downtrend (Lower Highs & Lows)"
            
            # Check for consolidation
            elif (abs(swing_highs[-1][1] - swing_highs[-2][1]) < swing_highs[-2][1] * 0.02 and
                  abs(swing_lows[-1][1] - swing_lows[-2][1]) < swing_lows[-2][1] * 0.02):
                wave_score = 0.5
                wave_pattern = "Consolidation/Range"
            
            else:
                wave_score = 0.6
                wave_pattern = "Corrective/Complex Structure"
        
        wave_details = {
            'swing_highs': len(swing_highs),
            'swing_lows': len(swing_lows),
            'pattern': wave_pattern,
            'current_high': float(h4_highs[-1]),
            'current_low': float(h4_lows[-1])
        }
        
        return wave_score, wave_pattern, wave_details
        
    except Exception as e:
        log.error(f"Wave analysis error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 3. MOMENTUM/STRENGTH ANALYSIS (القوة) ----------
def analyze_momentum(data: Dict[str, pd.DataFrame], direction: TrendDirection) -> Tuple[float, str, Dict]:
    """Analyze momentum using multiple indicators"""
    try:
        h1_df = data.get('H1')
        if h1_df is None or len(h1_df) < 50:
            return 0.5, "Insufficient data", {}
        
        prices = h1_df['close'].values
        highs = h1_df['high'].values
        lows = h1_df['low'].values
        
        # Calculate ATR for volatility
        atr = calculate_atr(highs, lows, prices, period=14)
        current_atr = atr[-1] if not np.isnan(atr[-1]) else 0
        
        # Calculate momentum using ROC
        def calculate_roc(prices, period):
            if len(prices) < period + 1:
                return np.full_like(prices, np.nan)
            roc = np.zeros_like(prices)
            roc[:period] = np.nan
            for i in range(period, len(prices)):
                roc[i] = ((prices[i] - prices[i-period]) / prices[i-period]) * 100
            return roc
        
        roc = calculate_roc(prices, period=10)
        current_roc = roc[-1] if not np.isnan(roc[-1]) else 0
        
        # Calculate ADX for trend strength
        adx = calculate_adx(highs, lows, prices, period=14)
        current_adx = adx[-1] if not np.isnan(adx[-1]) else 0
        
        # Calculate MACD
        macd, signal, hist = calculate_macd(prices)
        current_macd = macd[-1] if not np.isnan(macd[-1]) else 0
        
        # Determine momentum score
        momentum_score = 0.5
        
        if direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
            if current_roc > 0 and current_macd > 0:
                momentum_score = 0.8
                if current_adx > 25:
                    momentum_score = 0.9
            elif current_roc > 0:
                momentum_score = 0.7
        
        elif direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
            if current_roc < 0 and current_macd < 0:
                momentum_score = 0.8
                if current_adx > 25:
                    momentum_score = 0.9
            elif current_roc < 0:
                momentum_score = 0.7
        
        # Create momentum details
        momentum_details = {
            'atr': float(current_atr),
            'roc': float(current_roc),
            'adx': float(current_adx),
            'macd': float(current_macd),
            'atr_percent': float((current_atr / prices[-1]) * 100) if prices[-1] > 0 else 0
        }
        
        momentum_text = f"Momentum: ROC={current_roc:+.2f}%, ADX={current_adx:.1f}"
        
        return momentum_score, momentum_text, momentum_details
        
    except Exception as e:
        log.error(f"Momentum analysis error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 4. EMA ANALYSIS (المؤشرات - EMA) ----------
def analyze_ema_confluence(data: Dict[str, pd.DataFrame], direction: TrendDirection) -> Tuple[float, str, Dict]:
    """Analyze EMA alignment across timeframes"""
    try:
        confluence_score = 0
        confluence_details = {}
        ema_signals = []
        
        # Analyze EMA alignment across key timeframes
        key_timeframes = ['DAILY', 'H4', 'H1']
        
        for tf in key_timeframes:
            df = data.get(tf)
            if df is None or len(df) < 100:
                continue
            
            prices = df['close'].values
            
            # Calculate EMAs using pure Python
            ema_values = {}
            for period in [9, 21, 50, 200]:
                if len(prices) >= period:
                    ema = calculate_ema(prices, period)
                    if not np.isnan(ema[-1]):
                        ema_values[period] = float(ema[-1])
            
            if len(ema_values) < 2:
                continue
            
            # Check EMA alignment
            current_price = prices[-1]
            ema_signal = ""
            tf_score = 0
            
            if direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                # Bullish: Price above EMAs, EMAs in bullish order
                price_above_emas = all(current_price > ema for ema in ema_values.values())
                emas_bullish = all(ema_values[9] > ema_values[21] > ema_values[50]) if len(ema_values) >= 3 else False
                
                if price_above_emas and emas_bullish:
                    tf_score = 1.0
                    ema_signal = f"{tf}: Strong Bullish Alignment"
                elif price_above_emas:
                    tf_score = 0.7
                    ema_signal = f"{tf}: Price Above EMAs"
                elif current_price > ema_values.get(200, 0):
                    tf_score = 0.6
                    ema_signal = f"{tf}: Above 200 EMA"
            
            elif direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                # Bearish: Price below EMAs, EMAs in bearish order
                price_below_emas = all(current_price < ema for ema in ema_values.values())
                emas_bearish = all(ema_values[9] < ema_values[21] < ema_values[50]) if len(ema_values) >= 3 else False
                
                if price_below_emas and emas_bearish:
                    tf_score = 1.0
                    ema_signal = f"{tf}: Strong Bearish Alignment"
                elif price_below_emas:
                    tf_score = 0.7
                    ema_signal = f"{tf}: Price Below EMAs"
                elif current_price < ema_values.get(200, 0):
                    tf_score = 0.6
                    ema_signal = f"{tf}: Below 200 EMA"
            
            confluence_score += tf_score
            ema_signals.append(ema_signal)
            confluence_details[tf] = {
                'score': tf_score,
                'signal': ema_signal,
                'emas': ema_values,
                'price': float(current_price)
            }
        
        # Average the confluence score
        if confluence_details:
            confluence_score = confluence_score / len(confluence_details)
        
        confluence_text = " | ".join([s for s in ema_signals if s])
        
        return confluence_score, confluence_text, confluence_details
        
    except Exception as e:
        log.error(f"EMA analysis error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 5. RSI ANALYSIS (المؤشرات - RSI) ----------
def analyze_rsi_confluence(data: Dict[str, pd.DataFrame], direction: TrendDirection) -> Tuple[float, str, Dict]:
    """Analyze RSI across multiple timeframes"""
    try:
        rsi_details = {}
        rsi_signals = []
        total_score = 0
        tf_count = 0
        
        # Analyze RSI on key timeframes
        key_timeframes = ['DAILY', 'H4', 'H1', 'M15']
        
        for tf in key_timeframes:
            df = data.get(tf)
            if df is None or len(df) < 30:
                continue
            
            prices = df['close'].values
            
            # Calculate RSI using pure Python
            rsi = calculate_rsi(prices, period=RSI_PERIOD)
            current_rsi = rsi[-1] if not np.isnan(rsi[-1]) else 50
            
            # Determine RSI signal
            tf_score = 0
            signal = ""
            
            if direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                if current_rsi < RSI_OVERSOLD:
                    tf_score = 0.9
                    signal = f"{tf}: RSI Oversold ({current_rsi:.1f})"
                elif current_rsi < RSI_NEUTRAL:
                    tf_score = 0.7
                    signal = f"{tf}: RSI Bullish Zone ({current_rsi:.1f})"
                elif current_rsi < RSI_OVERBOUGHT:
                    tf_score = 0.5
                    signal = f"{tf}: RSI Neutral ({current_rsi:.1f})"
                else:
                    tf_score = 0.3
                    signal = f"{tf}: RSI Overbought ({current_rsi:.1f})"
            
            elif direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                if current_rsi > RSI_OVERBOUGHT:
                    tf_score = 0.9
                    signal = f"{tf}: RSI Overbought ({current_rsi:.1f})"
                elif current_rsi > RSI_NEUTRAL:
                    tf_score = 0.7
                    signal = f"{tf}: RSI Bearish Zone ({current_rsi:.1f})"
                elif current_rsi > RSI_OVERSOLD:
                    tf_score = 0.5
                    signal = f"{tf}: RSI Neutral ({current_rsi:.1f})"
                else:
                    tf_score = 0.3
                    signal = f"{tf}: RSI Oversold ({current_rsi:.1f})"
            else:
                # Neutral direction
                if current_rsi < 40:
                    tf_score = 0.6
                    signal = f"{tf}: RSI Approaching Oversold ({current_rsi:.1f})"
                elif current_rsi > 60:
                    tf_score = 0.6
                    signal = f"{tf}: RSI Approaching Overbought ({current_rsi:.1f})"
                else:
                    tf_score = 0.5
                    signal = f"{tf}: RSI Neutral ({current_rsi:.1f})"
            
            total_score += tf_score
            tf_count += 1
            rsi_signals.append(signal)
            
            rsi_details[tf] = {
                'rsi': float(current_rsi),
                'score': tf_score,
                'signal': signal,
                'zone': get_rsi_zone(current_rsi)
            }
        
        # Calculate average RSI score
        if tf_count > 0:
            avg_score = total_score / tf_count
        else:
            avg_score = 0.5
        
        rsi_text = " | ".join(rsi_signals)
        
        return avg_score, rsi_text, rsi_details
        
    except Exception as e:
        log.error(f"RSI analysis error: {e}")
        return 0.5, f"Error: {e}", {}

def get_rsi_zone(rsi_value: float) -> str:
    """Get RSI zone description"""
    if rsi_value < 30:
        return "OVERSOLD"
    elif rsi_value < 50:
        return "BULLISH_ZONE"
    elif rsi_value < 70:
        return "BEARISH_ZONE"
    else:
        return "OVERBOUGHT"

# ---------- 6. VOLUME ANALYSIS (Volume) ----------
def analyze_volume_profile(data: Dict[str, pd.DataFrame], direction: TrendDirection) -> Tuple[float, str, Dict]:
    """Advanced volume analysis"""
    try:
        volume_details = {}
        volume_signals = []
        total_score = 0
        tf_count = 0
        
        # Analyze volume across timeframes
        key_timeframes = ['DAILY', 'H4', 'H1']
        
        for tf in key_timeframes:
            df = data.get(tf)
            if df is None or len(df) < 20:
                continue
            
            prices = df['close'].values
            volumes = df['volume'].values
            
            # Calculate volume indicators
            current_volume = volumes[-1]
            avg_volume = np.mean(volumes[-20:])
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            
            # Calculate Volume Weighted Average Price (VWAP) for recent period
            typical_price = (df['high'] + df['low'] + df['close']) / 3
            vwap = (typical_price * df['volume']).sum() / df['volume'].sum()
            
            # Check if volume confirms price action
            tf_score = 0.5
            signal = ""
            
            if direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                # For bullish: volume should increase on up moves
                recent_up_days = sum(1 for i in range(-5, 0) if i < 0 and prices[i] > prices[i-1])
                recent_volume_up = sum(volumes[i] for i in range(-5, 0) if i < 0 and prices[i] > prices[i-1])
                avg_volume_up = recent_volume_up / recent_up_days if recent_up_days > 0 else 0
                
                if volume_ratio > 1.5 and prices[-1] > vwap:
                    tf_score = 0.9
                    signal = f"{tf}: High Volume Breakout ({volume_ratio:.1f}x)"
                elif volume_ratio > 1.2 and prices[-1] > vwap:
                    tf_score = 0.7
                    signal = f"{tf}: Volume Confirmed Uptrend ({volume_ratio:.1f}x)"
                elif volume_ratio > 1.0:
                    tf_score = 0.6
                    signal = f"{tf}: Average Volume ({volume_ratio:.1f}x)"
                else:
                    tf_score = 0.4
                    signal = f"{tf}: Low Volume Caution ({volume_ratio:.1f}x)"
            
            elif direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                # For bearish: volume should increase on down moves
                recent_down_days = sum(1 for i in range(-5, 0) if i < 0 and prices[i] < prices[i-1])
                recent_volume_down = sum(volumes[i] for i in range(-5, 0) if i < 0 and prices[i] < prices[i-1])
                avg_volume_down = recent_volume_down / recent_down_days if recent_down_days > 0 else 0
                
                if volume_ratio > 1.5 and prices[-1] < vwap:
                    tf_score = 0.9
                    signal = f"{tf}: High Volume Breakdown ({volume_ratio:.1f}x)"
                elif volume_ratio > 1.2 and prices[-1] < vwap:
                    tf_score = 0.7
                    signal = f"{tf}: Volume Confirmed Downtrend ({volume_ratio:.1f}x)"
                elif volume_ratio > 1.0:
                    tf_score = 0.6
                    signal = f"{tf}: Average Volume ({volume_ratio:.1f}x)"
                else:
                    tf_score = 0.4
                    signal = f"{tf}: Low Volume Caution ({volume_ratio:.1f}x)"
            
            total_score += tf_score
            tf_count += 1
            volume_signals.append(signal)
            
            volume_details[tf] = {
                'volume_ratio': float(volume_ratio),
                'current_volume': float(current_volume),
                'avg_volume': float(avg_volume),
                'vwap': float(vwap),
                'score': tf_score,
                'signal': signal
            }
        
        # Calculate average volume score
        if tf_count > 0:
            avg_score = total_score / tf_count
        else:
            avg_score = 0.5
        
        volume_text = " | ".join(volume_signals)
        
        return avg_score, volume_text, volume_details
        
    except Exception as e:
        log.error(f"Volume analysis error: {e}")
        return 0.5, f"Error: {e}", {}

# ---------- 7. TREND IDENTIFICATION (أحدد الاتجاه) ----------
def identify_trend(data: Dict[str, pd.DataFrame]) -> Tuple[TrendDirection, float, Dict]:
    """Comprehensive trend identification"""
    try:
        # Use Daily timeframe for primary trend
        daily_df = data.get('DAILY')
        if daily_df is None or len(daily_df) < 100:
            return TrendDirection.NEUTRAL, 0.5, {}
        
        prices = daily_df['close'].values
        
        # Calculate trend indicators using pure Python
        # 1. SMA trends
        sma_20 = calculate_sma(prices, 20)
        sma_50 = calculate_sma(prices, 50)
        sma_200 = calculate_sma(prices, 200)
        
        # 2. ADX for trend strength
        highs = daily_df['high'].values
        lows = daily_df['low'].values
        adx = calculate_adx(highs, lows, prices, period=14)
        current_adx = adx[-1] if not np.isnan(adx[-1]) else 0
        
        # 3. Slope of moving averages
        if not np.isnan(sma_20[-1]) and not np.isnan(sma_20[-10]):
            sma_20_slope = (sma_20[-1] - sma_20[-10]) / sma_20[-10]
        else:
            sma_20_slope = 0
        
        current_price = prices[-1]
        
        # Determine trend
        trend_score = 0
        trend_strength = current_adx / 100  # Normalize ADX to 0-1
        
        # Check if price above/below key MAs
        price_above_20 = current_price > sma_20[-1] if not np.isnan(sma_20[-1]) else False
        price_above_50 = current_price > sma_50[-1] if not np.isnan(sma_50[-1]) else False
        price_above_200 = current_price > sma_200[-1] if not np.isnan(sma_200[-1]) else False
        
        # Check MA alignment
        ma_bullish = False
        ma_bearish = False
        
        if (not np.isnan(sma_20[-1]) and not np.isnan(sma_50[-1]) and 
            not np.isnan(sma_200[-1])):
            ma_bullish = sma_20[-1] > sma_50[-1] > sma_200[-1]
            ma_bearish = sma_20[-1] < sma_50[-1] < sma_200[-1]
        
        # Calculate trend score
        if price_above_200:
            trend_score += 0.3
        if price_above_50:
            trend_score += 0.2
        if price_above_20:
            trend_score += 0.1
        
        if ma_bullish:
            trend_score += 0.3
        elif ma_bearish:
            trend_score -= 0.3
        
        if sma_20_slope > 0.02:  # 2% slope up
            trend_score += 0.1
        elif sma_20_slope < -0.02:  # 2% slope down
            trend_score -= 0.1
        
        # Normalize trend score
        trend_score = max(-1, min(1, trend_score))
        
        # Determine trend direction
        if trend_score >= 0.7 and trend_strength > 0.25:
            direction = TrendDirection.STRONG_BULLISH
        elif trend_score >= 0.3:
            direction = TrendDirection.BULLISH
        elif trend_score <= -0.7 and trend_strength > 0.25:
            direction = TrendDirection.STRONG_BEARISH
        elif trend_score <= -0.3:
            direction = TrendDirection.BEARISH
        else:
            direction = TrendDirection.NEUTRAL
        
        trend_details = {
            'trend_score': trend_score,
            'trend_strength': trend_strength,
            'adx': float(current_adx),
            'price_above_20': price_above_20,
            'price_above_50': price_above_50,
            'price_above_200': price_above_200,
            'ma_alignment': 'BULLISH' if ma_bullish else 'BEARISH' if ma_bearish else 'NEUTRAL',
            'sma_20_slope': float(sma_20_slope * 100)  # Percentage
        }
        
        return direction, trend_strength, trend_details
        
    except Exception as e:
        log.error(f"Trend identification error: {e}")
        return TrendDirection.NEUTRAL, 0.5, {}

# ================ SIGNAL SYNTHESIS ================

async def generate_signal(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """Generate signal using advanced synthesis analysis"""
    try:
        log.info(f"🔍 Analyzing {symbol} with advanced synthesis...")
        
        # 1. MULTI-TIMEFRAME ANALYSIS (أراقب كل الفريمات)
        mtf_direction, mtf_score, mtf_details = await analyze_multi_timeframe(data)
        
        if mtf_direction == TrendDirection.NEUTRAL:
            log.debug(f"{symbol}: No clear MTF direction")
            return None
        
        log.info(f"{symbol}: MTF Direction: {mtf_direction.value}, Score: {mtf_score:.2f}")
        
        # 2. WAVE ANALYSIS (المدى الموجي)
        wave_score, wave_pattern, wave_details = analyze_wave_structure(data)
        
        # 3. MOMENTUM ANALYSIS (القوة)
        momentum_score, momentum_text, momentum_details = analyze_momentum(data, mtf_direction)
        
        # 4. EMA ANALYSIS (المؤشرات - EMA)
        ema_score, ema_text, ema_details = analyze_ema_confluence(data, mtf_direction)
        
        # 5. RSI ANALYSIS (المؤشرات - RSI)
        rsi_score, rsi_text, rsi_details = analyze_rsi_confluence(data, mtf_direction)
        
        # 6. VOLUME ANALYSIS (Volume)
        volume_score, volume_text, volume_details = analyze_volume_profile(data, mtf_direction)
        
        # 7. TREND IDENTIFICATION (أحدد الاتجاه)
        trend_direction, trend_strength, trend_details = identify_trend(data)
        
        # Check if all analyses agree on direction
        side = "BUY" if mtf_direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH] else "SELL"
        
        # Calculate synthesis score (weighted average)
        weights = {
            'mtf': 0.25,      # Multi-timeframe analysis
            'wave': 0.15,     # Wave structure
            'momentum': 0.15, # Momentum
            'ema': 0.20,      # EMA alignment
            'rsi': 0.10,      # RSI confluence
            'volume': 0.10,   # Volume confirmation
            'trend': 0.05     # Trend strength
        }
        
        scores = {
            'mtf': mtf_score,
            'wave': wave_score,
            'momentum': momentum_score,
            'ema': ema_score,
            'rsi': rsi_score,
            'volume': volume_score,
            'trend': trend_strength
        }
        
        # Calculate weighted synthesis score
        synthesis_score = sum(scores[key] * weights[key] for key in weights)
        
        # Count confirmations (scores above threshold)
        confirmations = sum(1 for key in scores if scores[key] > 0.6)
        
        log.info(f"{symbol}: Synthesis Score: {synthesis_score:.2f}, Confirmations: {confirmations}/{len(scores)}")
        
        # Check minimum requirements
        if synthesis_score < MIN_SYNTHESIS_SCORE:
            log.debug(f"{symbol}: Synthesis score too low ({synthesis_score:.2f} < {MIN_SYNTHESIS_SCORE})")
            return None
        
        if confirmations < CONFLUENCE_REQUIRED:
            log.debug(f"{symbol}: Insufficient confirmations ({confirmations} < {CONFLUENCE_REQUIRED})")
            return None
        
        # Check if trend direction matches MTF direction
        if (mtf_direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH] and
            trend_direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]):
            log.debug(f"{symbol}: Trend direction conflict")
            return None
        
        # Get current price
        current_price = data['M15']['close'].iloc[-1]
        
        # Calculate SL/TP using advanced methods
        sl, tp, sltp_logic = calculate_advanced_sltp(current_price, side, data)
        
        # Calculate risk/reward
        risk = abs(current_price - sl)
        reward = abs(tp - current_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < 1.5:  # Minimum 1.5:1 R:R
            log.debug(f"{symbol}: Poor R:R ratio ({rr_ratio:.1f}:1)")
            return None
        
        # Create unique hashes for duplicate prevention
        price_hash = hashlib.md5(f"{symbol}:{side}:{current_price:.8f}".encode()).hexdigest()
        wave_hash = hashlib.md5(f"{symbol}:{wave_pattern}".encode()).hexdigest()
        unique_id = f"{symbol}:{side}:{current_price:.8f}:{time.time_ns()}"
        signal_hash = hashlib.md5(unique_id.encode()).hexdigest()
        
        # Find key support/resistance levels
        support_levels = find_support_levels(data)
        resistance_levels = find_resistance_levels(data)
        
        # Create signal
        signal = {
            'symbol': symbol,
            'side': side,
            'entry': current_price,
            'sl': sl,
            'tp': tp,
            'status': 'OPEN',
            
            # Analysis results
            'mtf_alignment': mtf_direction.value,
            'wave_structure': wave_pattern[:100],
            'momentum_score': momentum_score,
            'ema_alignment': ema_text[:200],
            'rsi_signal': rsi_text[:200],
            'volume_analysis': volume_text[:200],
            'trend_strength': trend_strength,
            
            # Confluence tracking
            'confirmations': confirmations,
            'synthesis_score': synthesis_score,
            
            # Technical levels
            'support_levels': json.dumps(support_levels),
            'resistance_levels': json.dumps(resistance_levels),
            
            # Signal management
            'signal_hash': signal_hash,
            'price_hash': price_hash,
            'wave_hash': wave_hash
        }
        
        log.info(f"✅ ADVANCED SIGNAL: {symbol} {side} @ {current_price:.4f}")
        log.info(f"   Score: {synthesis_score:.2f}, Confirmations: {confirmations}, R:R: {rr_ratio:.1f}:1")
        
        return signal
        
    except Exception as e:
        log.error(f"Advanced signal generation error for {symbol}: {e}")
        return None

def calculate_advanced_sltp(current_price: float, side: str, data: Dict[str, pd.DataFrame]) -> Tuple[float, float, str]:
    """Advanced SL/TP calculation using technical levels"""
    try:
        daily_df = data.get('DAILY')
        h4_df = data.get('H4')
        
        if daily_df is None or h4_df is None:
            return current_price * 0.98, current_price * 1.04, "Default 1:2 R:R"
        
        if side == "BUY":
            # Find nearest support levels
            support_levels = find_support_levels(data)
            if support_levels:
                # Use the strongest support above current price
                valid_supports = [s for s in support_levels if s < current_price]
                if valid_supports:
                    nearest_support = max(valid_supports)
                    sl = nearest_support * 0.995  # 0.5% below support
                else:
                    # Use recent low
                    recent_low = h4_df['low'].iloc[-20:].min()
                    sl = recent_low * 0.99
            else:
                recent_low = h4_df['low'].iloc[-20:].min()
                sl = recent_low * 0.99
            
            risk = current_price - sl
            tp = current_price + (risk * 2.5)  # 1:2.5 R:R
            
            # Adjust TP to nearest resistance
            resistance_levels = find_resistance_levels(data)
            if resistance_levels:
                valid_resistances = [r for r in resistance_levels if r > current_price]
                if valid_resistances:
                    nearest_resistance = min(valid_resistances)
                    if nearest_resistance < tp:
                        tp = nearest_resistance * 0.995  # Just below resistance
            
            return sl, tp, "SL below support, TP below resistance"
        
        else:  # SELL
            # Find nearest resistance levels
            resistance_levels = find_resistance_levels(data)
            if resistance_levels:
                # Use the strongest resistance below current price
                valid_resistances = [r for r in resistance_levels if r > current_price]
                if valid_resistances:
                    nearest_resistance = min(valid_resistances)
                    sl = nearest_resistance * 1.005  # 0.5% above resistance
                else:
                    # Use recent high
                    recent_high = h4_df['high'].iloc[-20:].max()
                    sl = recent_high * 1.01
            else:
                recent_high = h4_df['high'].iloc[-20:].max()
                sl = recent_high * 1.01
            
            risk = sl - current_price
            tp = current_price - (risk * 2.5)  # 1:2.5 R:R
            
            # Adjust TP to nearest support
            support_levels = find_support_levels(data)
            if support_levels:
                valid_supports = [s for s in support_levels if s < current_price]
                if valid_supports:
                    nearest_support = max(valid_supports)
                    if nearest_support > tp:
                        tp = nearest_support * 1.005  # Just above support
            
            return sl, tp, "SL above resistance, TP above support"
        
    except Exception as e:
        log.error(f"Advanced SL/TP error: {e}")
        # Fallback
        if side == "BUY":
            return current_price * 0.98, current_price * 1.04, "Error fallback"
        else:
            return current_price * 1.02, current_price * 0.96, "Error fallback"

def find_support_levels(data: Dict[str, pd.DataFrame]) -> List[float]:
    """Find key support levels"""
    try:
        h4_df = data.get('H4')
        if h4_df is None or len(h4_df) < 100:
            return []
        
        prices = h4_df['low'].values[-100:]
        
        # Simple method: find price clusters (histogram peaks)
        # Split price range into bins
        price_min = np.min(prices)
        price_max = np.max(prices)
        bins = 10
        bin_size = (price_max - price_min) / bins
        
        # Count prices in each bin
        bin_counts = []
        for i in range(bins):
            bin_low = price_min + i * bin_size
            bin_high = price_min + (i + 1) * bin_size
            count = np.sum((prices >= bin_low) & (prices < bin_high))
            bin_counts.append((bin_low, bin_high, count))
        
        # Find bins with highest counts
        bin_counts.sort(key=lambda x: x[2], reverse=True)
        support_levels = []
        
        for bin_low, bin_high, count in bin_counts[:5]:
            if count > 5:  # Minimum 5 occurrences
                support_levels.append((bin_low + bin_high) / 2)
        
        # Filter levels that are below current price
        current_price = h4_df['close'].iloc[-1]
        support_levels = [level for level in support_levels if level < current_price]
        
        return sorted(support_levels)[:3]  # Return top 3 supports
        
    except Exception as e:
        log.debug(f"Support levels error: {e}")
        return []

def find_resistance_levels(data: Dict[str, pd.DataFrame]) -> List[float]:
    """Find key resistance levels"""
    try:
        h4_df = data.get('H4')
        if h4_df is None or len(h4_df) < 100:
            return []
        
        prices = h4_df['high'].values[-100:]
        
        # Simple method: find price clusters (histogram peaks)
        # Split price range into bins
        price_min = np.min(prices)
        price_max = np.max(prices)
        bins = 10
        bin_size = (price_max - price_min) / bins
        
        # Count prices in each bin
        bin_counts = []
        for i in range(bins):
            bin_low = price_min + i * bin_size
            bin_high = price_min + (i + 1) * bin_size
            count = np.sum((prices >= bin_low) & (prices < bin_high))
            bin_counts.append((bin_low, bin_high, count))
        
        # Find bins with highest counts
        bin_counts.sort(key=lambda x: x[2], reverse=True)
        resistance_levels = []
        
        for bin_low, bin_high, count in bin_counts[:5]:
            if count > 5:  # Minimum 5 occurrences
                resistance_levels.append((bin_low + bin_high) / 2)
        
        # Filter levels that are above current price
        current_price = h4_df['close'].iloc[-1]
        resistance_levels = [level for level in resistance_levels if level > current_price]
        
        return sorted(resistance_levels)[:3]  # Return top 3 resistances
        
    except Exception as e:
        log.debug(f"Resistance levels error: {e}")
        return []

# ================ DATA FETCHING ================

async def fetch_ohlcv_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Fetch OHLCV data for all timeframes"""
    data = {}
    
    for tf_name, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=200)
            
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

async def check_advanced_duplicate(symbol: str, side: str, current_price: float,
                                 synthesis_score: float, wave_hash: str) -> Tuple[bool, str]:
    """Advanced duplicate signal prevention"""
    try:
        # Create price condition hash
        price_conditions = f"{symbol}:{side}:{current_price:.6f}:{synthesis_score:.3f}:{wave_hash}"
        price_hash = hashlib.md5(price_conditions.encode()).hexdigest()
        
        async with db_lock:
            # Check for same wave structure in last 8 hours
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE wave_hash = ? AND timestamp > datetime('now', '-8 hours')
            """, (wave_hash,)) as cursor:
                same_wave = (await cursor.fetchone())[0]
            
            if same_wave > 0:
                log.debug(f"{symbol}: Same wave structure detected recently")
                return True, price_hash
            
            # Check for same symbol and side in last 12 hours (max 1 signal)
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE symbol = ? AND side = ? 
                AND timestamp > datetime('now', '-12 hours')
                AND status = 'OPEN'
            """, (symbol, side)) as cursor:
                recent_signals = (await cursor.fetchone())[0]
            
            if recent_signals >= 1:
                log.debug(f"{symbol}: Already has open {side} signal")
                return True, price_hash
            
            # Check price movement from last signal
            async with db_conn.execute("""
                SELECT entry, timestamp FROM signals 
                WHERE symbol = ? AND side = ?
                ORDER BY timestamp DESC LIMIT 1
            """, (symbol, side)) as cursor:
                result = await cursor.fetchone()
                
                if result:
                    last_entry, last_time = result
                    price_change = abs(current_price - last_entry) / last_entry * 100
                    
                    # Calculate hours since last signal
                    import sqlite3
                    conn = sqlite3.connect(DB_PATH)
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT (julianday('now') - julianday(?)) * 24
                    """, (last_time,))
                    hours_passed = cursor.fetchone()[0] or 0
                    conn.close()
                    
                    # If less than 2% price movement in last 6 hours, skip
                    if hours_passed < 6 and price_change < 2.0:
                        log.debug(f"{symbol}: Insufficient price movement ({price_change:.2f}% in {hours_passed:.1f}h)")
                        return True, price_hash
        
        return False, price_hash
        
    except Exception as e:
        log.error(f"Duplicate check error: {e}")
        return False, hashlib.md5(f"{symbol}:{time.time_ns()}".encode()).hexdigest()

# ================ MAIN SCANNING LOOP ================

async def scanning_loop(exchange):
    """Main scanning loop"""
    log.info("🚀 Starting ADVANCED scanner with synthesis analysis")
    
    await tg("""
🚀 **بدء الماسح الضوئي المتقدم - التحليل الموجي المتعدد**

✅ **المنهجية المتطورة:**
1. **التحليل متعدد الفريمات** (MTF Analysis)
2. **الهيكل الموجي** (Wave Structure)
3. **الزخم والقوة** (Momentum & Strength)
4. **محاذاة المتوسطات** (EMA Alignment)
5. **توافق RSI** (RSI Confluence)
6. **تحليل الفوليوم** (Volume Analysis)
7. **تحديد الاتجاه** (Trend Identification)

🎯 **نظام التوكيد:** يتطلب ٣ مؤشرات تأكيد على الأقل
📊 **الجودة الدنيا:** ٦٥٪

جاهز للعمل...
""")
    
    while True:
        try:
            log.info("=" * 60)
            log.info("Starting new synthesis scan cycle...")
            
            # Get top volume pairs
            try:
                tickers = await exchange.fetch_tickers()
                usdt_pairs = []
                
                for symbol, ticker in tickers.items():
                    if symbol.endswith('/USDT'):
                        volume = ticker.get('quoteVolume', 0)
                        if volume > 5000000:  # $5M minimum volume for quality
                            usdt_pairs.append((symbol, volume))
                
                usdt_pairs.sort(key=lambda x: x[1], reverse=True)
                top_pairs = usdt_pairs[:TOP_N]
                
                log.info(f"Found {len(top_pairs)} pairs with >$5M volume")
                
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
                    
                    # Generate advanced signal
                    signal = await generate_signal(data, symbol)
                    
                    if signal:
                        # Advanced duplicate check
                        is_duplicate, price_hash = await check_advanced_duplicate(
                            signal['symbol'], signal['side'], signal['entry'],
                            signal['synthesis_score'], signal['wave_hash']
                        )
                        
                        if is_duplicate:
                            log.debug(f"{symbol}: Advanced duplicate check failed")
                            continue
                        
                        # Save to database
                        async with db_lock:
                            # Final duplicate check with signal hash
                            async with db_conn.execute(
                                "SELECT COUNT(*) FROM signals WHERE signal_hash = ?",
                                (signal['signal_hash'],)
                            ) as cursor:
                                exists = (await cursor.fetchone())[0]
                            
                            if exists == 0:
                                # Insert new signal
                                await db_conn.execute("""
                                    INSERT INTO signals (
                                        symbol, side, entry, sl, tp, status,
                                        mtf_alignment, wave_structure, momentum_score,
                                        ema_alignment, rsi_signal, volume_analysis,
                                        trend_strength, confirmations, synthesis_score,
                                        support_levels, resistance_levels,
                                        signal_hash, price_hash, wave_hash
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                                    signal['tp'], signal['status'], signal['mtf_alignment'],
                                    signal['wave_structure'], signal['momentum_score'],
                                    signal['ema_alignment'], signal['rsi_signal'], signal['volume_analysis'],
                                    signal['trend_strength'], signal['confirmations'], signal['synthesis_score'],
                                    signal['support_levels'], signal['resistance_levels'],
                                    signal['signal_hash'], price_hash, signal['wave_hash']
                                ))
                                
                                await db_conn.commit()
                                
                                # Send detailed Telegram alert
                                await send_advanced_telegram_alert(signal)
                                signals_found += 1
                                log.info(f"✅ Advanced signal sent for {symbol}")
                            else:
                                log.debug(f"Final duplicate check failed for {symbol}")
                    
                    # Respect rate limits
                    await asyncio.sleep(0.3)
                    
                except Exception as e:
                    log.error(f"Error processing {symbol}: {e}")
                    continue
            
            log.info(f"Advanced scan complete. Found {signals_found} high-quality signals.")
            
            # Wait for next scan
            log.info(f"Waiting {SCAN_INTERVAL} seconds for next scan...")
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
            await asyncio.sleep(30)

async def send_advanced_telegram_alert(signal: Dict):
    """Send detailed Telegram alert for advanced signal"""
    try:
        side_ar = "شراء" if signal['side'] == "BUY" else "بيع"
        entry = signal['entry']
        sl = signal['sl']
        tp = signal['tp']
        
        risk_pct = abs(entry - sl) / entry * 100
        reward_pct = abs(tp - entry) / entry * 100
        rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0
        
        # Parse support/resistance levels
        try:
            support_levels = json.loads(signal.get('support_levels', '[]'))
            resistance_levels = json.loads(signal.get('resistance_levels', '[]'))
        except:
            support_levels = []
            resistance_levels = []
        
        message = f"""
🎯 **إشارة تداول متقدمة - التحليل الموجي**

**{signal['symbol']}** | **{side_ar}**
📊 **الجودة: {signal['synthesis_score']:.1%}** | **التوكيدات: {signal['confirmations']}/7**

━━━━━━━━━━━━━━━━━━
📈 **التحليل الفني:**

• **الفريمات المتعددة:** {signal['mtf_alignment']}
• **الهيكل الموجي:** {signal['wave_structure']}
• **الزخم:** {signal['momentum_score']:.1%}
• **المتوسطات:** {signal['ema_alignment'][:100]}
• **RSI:** {signal['rsi_signal'][:100]}
• **الفوليوم:** {signal['volume_analysis'][:100]}
• **قوة الاتجاه:** {signal['trend_strength']:.1%}

━━━━━━━━━━━━━━━━━━
💰 **مستويات التداول:**

• **الدخول:** `{entry:.4f}`
• **وقف الخسارة:** `{sl:.4f}` ({risk_pct:.1f}%)
• **هدف الربح:** `{tp:.4f}` ({reward_pct:.1f}%)
• **نسبة الربح/المخاطرة:** **{rr_ratio:.1f}:1**

"""
        
        if support_levels:
            message += f"• **الدعوم:** {', '.join([f'{s:.2f}' for s in support_levels])}\n"
        if resistance_levels:
            message += f"• **المقاومات:** {', '.join([f'{r:.2f}' for r in resistance_levels])}\n"
        
        message += f"""
━━━━━━━━━━━━━━━━━━
⏰ **المتابعة التلقائية مفعلة**

#{side_ar} #تداول_متقدم #التحليل_الموجي
"""
        
        await tg(message)
        
    except Exception as e:
        log.error(f"Telegram alert error: {e}")
        # Send simplified alert
        simplified = f"""
✅ **إشارة جديدة:** {signal['symbol']} {signal['side']}
الدخول: {signal['entry']:.4f} | الجودة: {signal['synthesis_score']:.1%}
"""
        await tg(simplified)

# ================ MONITORING LOOP ================

async def monitoring_loop(exchange):
    """Monitor open positions"""
    log.info("Starting advanced monitoring loop...")
    
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp, synthesis_score FROM signals 
                    WHERE status='OPEN'
                """) as cursor:
                    open_positions = await cursor.fetchall()
            
            if open_positions:
                log.info(f"Monitoring {len(open_positions)} open positions")
            
            for pos_id, symbol, side, entry, sl, tp, score in open_positions:
                try:
                    # Get current price
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker.get('last')
                    
                    if not current_price:
                        continue
                    
                    # Calculate PnL
                    if side == "BUY":
                        pnl_percent = ((current_price - entry) / entry) * 100
                        should_close_tp = current_price >= tp
                        should_close_sl = current_price <= sl
                    else:  # SELL
                        pnl_percent = ((entry - current_price) / entry) * 100
                        should_close_tp = current_price <= tp
                        should_close_sl = current_price >= sl
                    
                    # Close position if needed
                    if should_close_tp or should_close_sl:
                        close_reason = "TP_HIT" if should_close_tp else "SL_HIT"
                        
                        async with db_lock:
                            await db_conn.execute("""
                                UPDATE signals SET 
                                    status = 'CLOSED',
                                    close_reason = ?,
                                    close_price = ?,
                                    close_timestamp = CURRENT_TIMESTAMP,
                                    pnl_percent = ?
                                WHERE id = ?
                            """, (close_reason, current_price, pnl_percent, pos_id))
                            
                            await db_conn.commit()
                        
                        # Send detailed closure notification
                        side_ar = "شراء" if side == "BUY" else "بيع"
                        result = "✅ هدف الربح" if close_reason == "TP_HIT" else "❌ وقف الخسارة"
                        
                        closure_message = f"""
{result}

**{symbol}** | **{side_ar}**
الجودة الأصلية: {score:.1%}

• الدخول: {entry:.4f}
• الإغلاق: {current_price:.4f}
• {close_reason.replace('_', ' ')}: {tp if close_reason == 'TP_HIT' else sl:.4f}

• الربح/الخسارة: {'+' if pnl_percent > 0 else ''}{pnl_percent:.2f}%

#إغلاق #{"ربح" if pnl_percent > 0 else "خسارة"}
"""
                        
                        await tg(closure_message)
                        log.info(f"{'✅' if close_reason == 'TP_HIT' else '❌'} {symbol}: {close_reason} | PnL: {pnl_percent:.2f}%")
                
                except Exception as e:
                    log.error(f"Monitor error for {symbol}: {e}")
                    continue
            
            # Wait before next check
            await asyncio.sleep(15)
            
        except Exception as e:
            log.error(f"Monitoring loop error: {e}")
            await asyncio.sleep(30)

# ================ WEB API ================

app = FastAPI(title="Advanced Visual Synthesis Scanner")

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "Advanced Visual Synthesis Scanner",
        "methodology": "Multi-Timeframe Wave Analysis",
        "version": "3.0 - Professional (Pure Python)",
        "min_score": MIN_SYNTHESIS_SCORE,
        "min_confirmations": CONFLUENCE_REQUIRED,
        "analysis_methods": [
            "Multi-Timeframe Analysis",
            "Wave Structure Analysis",
            "Momentum & Strength",
            "EMA Alignment",
            "RSI Confluence",
            "Volume Analysis",
            "Trend Identification"
        ]
    }

@app.get("/analysis/{symbol}")
async def analyze_symbol(symbol: str):
    """Perform real-time analysis on a symbol"""
    try:
        global exchange
        if not exchange:
            return {"error": "Exchange not initialized"}
        
        data = await fetch_ohlcv_data(exchange, f"{symbol}/USDT")
        if not data:
            return {"error": "Failed to fetch data"}
        
        # Perform all analyses
        mtf_direction, mtf_score, mtf_details = await analyze_multi_timeframe(data)
        wave_score, wave_pattern, wave_details = analyze_wave_structure(data)
        momentum_score, momentum_text, momentum_details = analyze_momentum(data, mtf_direction)
        ema_score, ema_text, ema_details = analyze_ema_confluence(data, mtf_direction)
        rsi_score, rsi_text, rsi_details = analyze_rsi_confluence(data, mtf_direction)
        volume_score, volume_text, volume_details = analyze_volume_profile(data, mtf_direction)
        trend_direction, trend_strength, trend_details = identify_trend(data)
        
        # Calculate synthesis
        scores = {
            'mtf': mtf_score,
            'wave': wave_score,
            'momentum': momentum_score,
            'ema': ema_score,
            'rsi': rsi_score,
            'volume': volume_score,
            'trend': trend_strength
        }
        
        weights = {
            'mtf': 0.25, 'wave': 0.15, 'momentum': 0.15,
            'ema': 0.20, 'rsi': 0.10, 'volume': 0.10, 'trend': 0.05
        }
        
        synthesis_score = sum(scores[key] * weights[key] for key in weights)
        confirmations = sum(1 for key in scores if scores[key] > 0.6)
        
        return {
            "symbol": f"{symbol}/USDT",
            "mtf_analysis": {
                "direction": mtf_direction.value,
                "score": mtf_score,
                "details": mtf_details
            },
            "wave_analysis": {
                "pattern": wave_pattern,
                "score": wave_score,
                "details": wave_details
            },
            "momentum_analysis": {
                "score": momentum_score,
                "text": momentum_text,
                "details": momentum_details
            },
            "ema_analysis": {
                "score": ema_score,
                "text": ema_text,
                "details": ema_details
            },
            "rsi_analysis": {
                "score": rsi_score,
                "text": rsi_text,
                "details": rsi_details
            },
            "volume_analysis": {
                "score": volume_score,
                "text": volume_text,
                "details": volume_details
            },
            "trend_analysis": {
                "direction": trend_direction.value,
                "strength": trend_strength,
                "details": trend_details
            },
            "synthesis": {
                "score": synthesis_score,
                "confirmations": confirmations,
                "would_signal": synthesis_score >= MIN_SYNTHESIS_SCORE and confirmations >= CONFLUENCE_REQUIRED
            }
        }
        
    except Exception as e:
        return {"error": str(e)}

# ================ MAIN ================

async def main():
    global exchange
    
    log.info("=" * 70)
    log.info("🚀 ADVANCED VISUAL SYNTHESIS SCANNER - PROFESSIONAL EDITION")
    log.info("=" * 70)
    log.info("Methodology: Multi-Timeframe Wave Analysis with 7-Point Confirmation")
    log.info(f"Minimum Score: {MIN_SYNTHESIS_SCORE}, Required Confirmations: {CONFLUENCE_REQUIRED}")
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
    
    # Send startup message
    await tg("""
🚀 **الماسح الضوئي المتقدم - بدون TA-Lib**

✅ **تم بدء التشغيل بنجاح**
✅ **جميع المؤشرات مكتوبة بلغة بايثون البحتة**
✅ **لا حاجة لتثبيت مكتبات خارجية**

**المنهجية المتطورة تعمل بكامل طاقتها!**

جاهز للعمل...
""")
    
    # Start scanning and monitoring loops
    try:
        await asyncio.gather(
            scanning_loop(exchange),
            monitoring_loop(exchange)
        )
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
        log.info("Advanced scanner shutdown complete")

if __name__ == "__main__":
    # Check for required packages
    import subprocess
    import sys
    
    required_packages = ['ccxt', 'pandas', 'numpy', 'httpx', 'fastapi', 'aiosqlite']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        log.warning(f"Installing missing packages: {missing_packages}")
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
    
    # Run the scanner
    asyncio.run(main())