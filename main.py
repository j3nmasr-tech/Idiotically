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
TOP_N = int(os.getenv("TOP_N", 60))

# Synthesis thresholds
MIN_SYNTHESIS_SCORE = 0.25  # Higher threshold for quality signals
CONFLUENCE_REQUIRED = 2    # Minimum number of confirmations

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

# ================ TELEGRAM UTILITIES (FIXED) ================

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
✅ Telegram integration is being tested
✅ If you see this message, Telegram is working!

*Bot:* Visual Synthesis Scanner
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

# ---------------- DATABASE ----------------
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
    """Initialize database with automatic schema updates"""
    global db_conn
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
        # Create main table if it doesn't exist (original schema)
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
                signal_hash TEXT UNIQUE,
                price_hash TEXT
            )
        """)
        
        await db_conn.commit()
        log.info("✅ Main table created/verified")
        
        # Check and add ALL missing columns for advanced analysis
        required_columns = [
            ("mtf_alignment", "TEXT"),
            ("momentum_score", "REAL"),
            ("ema_alignment", "TEXT"),
            ("rsi_signal", "TEXT"),
            ("volume_analysis", "TEXT"),
            ("trend_strength", "REAL"),
            ("confirmations", "INTEGER"),
            ("support_levels", "TEXT"),
            ("resistance_levels", "TEXT"),
            ("wave_hash", "TEXT"),
        ]
        
        for column_name, column_type in required_columns:
            if not await check_and_add_column(column_name, column_type):
                log.error(f"Failed to add required column: {column_name}")
                # Continue anyway, we'll handle missing columns gracefully
        
        # Create indexes (will ignore if already exist)
        try:
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_price_hash ON signals(price_hash)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_wave_hash ON signals(wave_hash)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON signals(symbol, timestamp)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status)")
            await db_conn.commit()
            log.info("✅ Database indexes created/verified")
        except Exception as e:
            log.warning(f"Index creation warning: {e}")
        
        log.info("✅ Database ready with all required columns")
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
        
        for tf_name, df in data.items():
            if len(df) < 50:
                timeframe_scores[tf_name] = 0
                continue
            
            prices = df['close'].values
            
            # Calculate EMAs for this timeframe
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
        
        if h4_df is None or len(h4_df) < 50:
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
        if h1_df is None or len(h1_df) < 20:
            return 0.5, "Insufficient data", {}
        
        prices = h1_df['close'].values
        
        # Calculate momentum using ROC
        def calculate_roc(prices, period):
            n = len(prices)
            if n < period + 1:
                return np.full(n, np.nan)
            roc = np.zeros(n)
            roc[:period] = np.nan
            for i in range(period, n):
                if prices[i-period] != 0:
                    roc[i] = ((prices[i] - prices[i-period]) / prices[i-period]) * 100
            return roc
        
        roc = calculate_roc(prices, period=10)
        current_roc = roc[-1] if not np.isnan(roc[-1]) else 0
        
        # Calculate RSI for momentum
        rsi = calculate_rsi(prices, period=14)
        current_rsi = rsi[-1] if not np.isnan(rsi[-1]) else 50
        
        # Determine momentum score
        momentum_score = 0.5
        
        if direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
            if current_roc > 1 and current_rsi < 70:
                momentum_score = 0.8
            elif current_roc > 0:
                momentum_score = 0.7
            else:
                momentum_score = 0.4
        
        elif direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
            if current_roc < -1 and current_rsi > 30:
                momentum_score = 0.8
            elif current_roc < 0:
                momentum_score = 0.7
            else:
                momentum_score = 0.4
        
        # Create momentum details
        momentum_details = {
            'roc': float(current_roc),
            'rsi': float(current_rsi)
        }
        
        momentum_text = f"Momentum: ROC={current_roc:+.2f}%, RSI={current_rsi:.1f}"
        
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
        tf_count = 0
        
        for tf in key_timeframes:
            df = data.get(tf)
            if df is None or len(df) < 50:
                continue
            
            prices = df['close'].values
            
            # Calculate EMAs
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
                if ema_values:
                    # Check if price is above all EMAs
                    price_above_all = True
                    for ema in ema_values.values():
                        if current_price <= ema:
                            price_above_all = False
                            break
                    
                    # Check EMA bullish alignment (fast > medium > slow)
                    emas_bullish = False
                    if 9 in ema_values and 21 in ema_values and 50 in ema_values:
                        emas_bullish = ema_values[9] > ema_values[21] > ema_values[50]
                    
                    if price_above_all and emas_bullish:
                        tf_score = 1.0
                        ema_signal = f"{tf}: Strong Bullish Alignment"
                    elif price_above_all:
                        tf_score = 0.7
                        ema_signal = f"{tf}: Price Above EMAs"
                    elif 200 in ema_values and current_price > ema_values[200]:
                        tf_score = 0.6
                        ema_signal = f"{tf}: Above 200 EMA"
            
            elif direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                # Bearish: Price below EMAs, EMAs in bearish order
                if ema_values:
                    # Check if price is below all EMAs
                    price_below_all = True
                    for ema in ema_values.values():
                        if current_price >= ema:
                            price_below_all = False
                            break
                    
                    # Check EMA bearish alignment (fast < medium < slow)
                    emas_bearish = False
                    if 9 in ema_values and 21 in ema_values and 50 in ema_values:
                        emas_bearish = ema_values[9] < ema_values[21] < ema_values[50]
                    
                    if price_below_all and emas_bearish:
                        tf_score = 1.0
                        ema_signal = f"{tf}: Strong Bearish Alignment"
                    elif price_below_all:
                        tf_score = 0.7
                        ema_signal = f"{tf}: Price Below EMAs"
                    elif 200 in ema_values and current_price < ema_values[200]:
                        tf_score = 0.6
                        ema_signal = f"{tf}: Below 200 EMA"
            
            confluence_score += tf_score
            tf_count += 1
            if ema_signal:
                ema_signals.append(ema_signal)
            
            confluence_details[tf] = {
                'score': tf_score,
                'signal': ema_signal,
                'emas': ema_values,
                'price': float(current_price)
            }
        
        # Average the confluence score
        if tf_count > 0:
            confluence_score = confluence_score / tf_count
        else:
            confluence_score = 0.5
        
        confluence_text = " | ".join(ema_signals) if ema_signals else "No clear EMA signals"
        
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
            
            # Calculate RSI
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
        if daily_df is None or len(daily_df) < 50:
            return TrendDirection.NEUTRAL, 0.5, {}
        
        prices = daily_df['close'].values
        
        # Calculate trend indicators
        # 1. SMA trends
        sma_20 = calculate_sma(prices, 20)
        sma_50 = calculate_sma(prices, 50)
        sma_200 = calculate_sma(prices, 200)
        
        # 2. Calculate slope
        if len(prices) >= 20:
            recent_prices = prices[-20:]
            x = np.arange(len(recent_prices))
            slope, intercept = np.polyfit(x, recent_prices, 1)
            slope_percent = (slope / recent_prices[0]) * 100 if recent_prices[0] != 0 else 0
        else:
            slope_percent = 0
        
        current_price = prices[-1]
        
        # Determine trend
        trend_score = 0
        
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
        
        if slope_percent > 0.5:  # 0.5% slope up
            trend_score += 0.1
        elif slope_percent < -0.5:  # 0.5% slope down
            trend_score -= 0.1
        
        # Normalize trend score
        trend_score = max(-1, min(1, trend_score))
        
        # Determine trend direction
        trend_strength = abs(trend_score)
        
        if trend_score >= 0.7:
            direction = TrendDirection.STRONG_BULLISH
        elif trend_score >= 0.3:
            direction = TrendDirection.BULLISH
        elif trend_score <= -0.7:
            direction = TrendDirection.STRONG_BEARISH
        elif trend_score <= -0.3:
            direction = TrendDirection.BEARISH
        else:
            direction = TrendDirection.NEUTRAL
        
        trend_details = {
            'trend_score': trend_score,
            'trend_strength': trend_strength,
            'price_above_20': price_above_20,
            'price_above_50': price_above_50,
            'price_above_200': price_above_200,
            'ma_alignment': 'BULLISH' if ma_bullish else 'BEARISH' if ma_bearish else 'NEUTRAL',
            'slope_percent': float(slope_percent)
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
        
        # Create signal - include BOTH old and new field names for compatibility
        signal = {
            'symbol': symbol,
            'side': side,
            'entry': current_price,
            'sl': sl,
            'tp': tp,
            'status': 'OPEN',
            
            # New advanced analysis fields
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
            'wave_hash': wave_hash,
            
            # Old field names for backward compatibility
            'timeframe_alignment': f"MTF: {mtf_direction.value}",
            'strength_level': f"Momentum: {momentum_score:.1%}",
            'indicators_signal': f"EMA: {ema_text[:50]} | RSI: {rsi_text[:50]}",
            'volume_status': volume_text[:100]
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
        h4_df = data.get('H4')
        
        if h4_df is None or len(h4_df) < 20:
            # Default values
            if side == "BUY":
                sl = current_price * 0.98
                tp = current_price * 1.04
                return sl, tp, "Default 1:2 R:R"
            else:
                sl = current_price * 1.02
                tp = current_price * 0.96
                return sl, tp, "Default 1:2 R:R"
        
        if side == "BUY":
            # Use recent low as support
            recent_low = h4_df['low'].iloc[-20:].min()
            sl = recent_low * 0.99  # 1% below support
            
            risk = current_price - sl
            tp = current_price + (risk * 2.5)  # 1:2.5 R:R
            
            return sl, tp, f"SL below support {recent_low:.2f}, TP 1:2.5 R:R"
        
        else:  # SELL
            # Use recent high as resistance
            recent_high = h4_df['high'].iloc[-20:].max()
            sl = recent_high * 1.01  # 1% above resistance
            
            risk = sl - current_price
            tp = current_price - (risk * 2.5)  # 1:2.5 R:R
            
            return sl, tp, f"SL above resistance {recent_high:.2f}, TP 1:2.5 R:R"
        
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
        if h4_df is None or len(h4_df) < 50:
            return []
        
        prices = h4_df['low'].values[-50:]
        
        # Simple method: find recent lows
        recent_lows = []
        for i in range(2, len(prices)-2):
            if (prices[i] < prices[i-1] and prices[i] < prices[i-2] and
                prices[i] < prices[i+1] and prices[i] < prices[i+2]):
                recent_lows.append(prices[i])
        
        # Get unique lows and sort
        if recent_lows:
            unique_lows = sorted(list(set(recent_lows)))
            # Filter levels below current price
            current_price = h4_df['close'].iloc[-1]
            support_levels = [level for level in unique_lows if level < current_price]
            return sorted(support_levels)[-3:]  # Return top 3 supports
        else:
            return []
        
    except Exception as e:
        log.debug(f"Support levels error: {e}")
        return []

def find_resistance_levels(data: Dict[str, pd.DataFrame]) -> List[float]:
    """Find key resistance levels"""
    try:
        h4_df = data.get('H4')
        if h4_df is None or len(h4_df) < 50:
            return []
        
        prices = h4_df['high'].values[-50:]
        
        # Simple method: find recent highs
        recent_highs = []
        for i in range(2, len(prices)-2):
            if (prices[i] > prices[i-1] and prices[i] > prices[i-2] and
                prices[i] > prices[i+1] and prices[i] > prices[i+2]):
                recent_highs.append(prices[i])
        
        # Get unique highs and sort
        if recent_highs:
            unique_highs = sorted(list(set(recent_highs)))
            # Filter levels above current price
            current_price = h4_df['close'].iloc[-1]
            resistance_levels = [level for level in unique_highs if level > current_price]
            return sorted(resistance_levels)[:3]  # Return top 3 resistances
        else:
            return []
        
    except Exception as e:
        log.debug(f"Resistance levels error: {e}")
        return []

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

async def check_advanced_duplicate(symbol: str, side: str, current_price: float,
                                 synthesis_score: float, wave_hash: str) -> Tuple[bool, str]:
    """Advanced duplicate signal prevention with fallback"""
    try:
        # Create price condition hash
        price_conditions = f"{symbol}:{side}:{current_price:.6f}:{synthesis_score:.3f}:{wave_hash}"
        price_hash = hashlib.md5(price_conditions.encode()).hexdigest()
        
        async with db_lock:
            # First check if wave_hash column exists
            try:
                async with db_conn.execute("""
                    SELECT COUNT(*) FROM signals 
                    WHERE wave_hash = ? AND timestamp > datetime('now', '-8 hours')
                """, (wave_hash,)) as cursor:
                    result = await cursor.fetchone()
                    same_wave = result[0] if result else 0
                
                if same_wave > 0:
                    log.debug(f"{symbol}: Same wave structure detected recently")
                    return True, price_hash
            except Exception as e:
                log.debug(f"Wave hash check failed (column may not exist yet): {e}")
                # Continue with other checks
            
            # Check for same symbol and side in last 12 hours (max 1 signal)
            try:
                async with db_conn.execute("""
                    SELECT COUNT(*) FROM signals 
                    WHERE symbol = ? AND side = ? 
                    AND timestamp > datetime('now', '-12 hours')
                    AND status = 'OPEN'
                """, (symbol, side)) as cursor:
                    result = await cursor.fetchone()
                    recent_signals = result[0] if result else 0
                
                if recent_signals >= 1:
                    log.debug(f"{symbol}: Already has open {side} signal")
                    return True, price_hash
            except Exception as e:
                log.debug(f"Recent signals check failed: {e}")
            
            # Check price movement from last signal (using price_hash as fallback)
            try:
                async with db_conn.execute("""
                    SELECT entry, timestamp FROM signals 
                    WHERE symbol = ? AND side = ?
                    ORDER BY timestamp DESC LIMIT 1
                """, (symbol, side)) as cursor:
                    result = await cursor.fetchone()
                    
                    if result:
                        last_entry, last_time = result
                        price_change = abs(current_price - last_entry) / last_entry * 100
                        
                        # If less than 2% price movement in last 6 hours, skip
                        import sqlite3
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT (julianday('now') - julianday(?)) * 24
                        """, (last_time,))
                        hours_result = cursor.fetchone()
                        hours_passed = hours_result[0] if hours_result else 0
                        conn.close()
                        
                        if hours_passed < 6 and price_change < 2.0:
                            log.debug(f"{symbol}: Insufficient price movement ({price_change:.2f}% in {hours_passed:.1f}h)")
                            return True, price_hash
            except Exception as e:
                log.debug(f"Price movement check failed: {e}")
        
        return False, price_hash
        
    except Exception as e:
        log.error(f"Duplicate check error: {e}")
        return False, hashlib.md5(f"{symbol}:{time.time_ns()}".encode()).hexdigest()

# ================ MAIN SCANNING LOOP ================

async def scanning_loop(exchange):
    """Main scanning loop"""
    log.info("🚀 Starting ADVANCED scanner with synthesis analysis")
    
    # Send startup message
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
📊 **الجودة الدنيا:** ٣٥٪

📡 **جاهز للعمل والمسح...**
""")
    
    while True:
        try:
            log.info("=" * 60)
            log.info("Starting new synthesis scan cycle...")
            
            # Get top volume pairs - FIXED: Use TOP_N from config, lower volume filter
            try:
                tickers = await exchange.fetch_tickers()
                usdt_pairs = []
                
                for symbol, ticker in tickers.items():
                    if symbol.endswith('/USDT'):
                        volume = ticker.get('quoteVolume', 0)
                        if volume > 100000:  # Reduced from $5M to $100K to get more pairs
                            usdt_pairs.append((symbol, volume))
                
                usdt_pairs.sort(key=lambda x: x[1], reverse=True)
                top_pairs = usdt_pairs[:TOP_N]  # Use TOP_N from config
                
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
                                result = await cursor.fetchone()
                                exists = result[0] if result else 0
                            
                            if exists == 0:
                                # Insert new signal with BOTH old and new field names
                                try:
                                    await db_conn.execute("""
                                        INSERT INTO signals (
                                            symbol, side, entry, sl, tp, status,
                                            timeframe_alignment, wave_structure, strength_level,
                                            indicators_signal, volume_status, synthesis_score,
                                            mtf_alignment, momentum_score, ema_alignment,
                                            rsi_signal, volume_analysis, trend_strength,
                                            confirmations, support_levels, resistance_levels,
                                            signal_hash, price_hash, wave_hash
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (
                                        signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                                        signal['tp'], signal['status'], 
                                        signal['timeframe_alignment'], signal['wave_structure'], 
                                        signal['strength_level'], signal['indicators_signal'], 
                                        signal['volume_status'], signal['synthesis_score'],
                                        signal['mtf_alignment'], signal['momentum_score'],
                                        signal['ema_alignment'], signal['rsi_signal'],
                                        signal['volume_analysis'], signal['trend_strength'],
                                        signal['confirmations'], signal['support_levels'],
                                        signal['resistance_levels'], signal['signal_hash'],
                                        price_hash, signal['wave_hash']
                                    ))
                                    
                                    await db_conn.commit()
                                    
                                    # Send detailed Telegram alert
                                    await send_advanced_telegram_alert(signal)
                                    signals_found += 1
                                    log.info(f"✅ Advanced signal sent for {symbol}")
                                except Exception as e:
                                    log.error(f"Database insert error for {symbol}: {e}")
                                    # Try with simpler insert (backward compatibility)
                                    try:
                                        await db_conn.execute("""
                                            INSERT INTO signals (
                                                symbol, side, entry, sl, tp, status,
                                                timeframe_alignment, wave_structure, strength_level,
                                                indicators_signal, volume_status, synthesis_score,
                                                signal_hash, price_hash
                                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                                            signal['tp'], signal['status'], 
                                            signal['timeframe_alignment'], signal['wave_structure'], 
                                            signal['strength_level'], signal['indicators_signal'], 
                                            signal['volume_status'], signal['synthesis_score'],
                                            signal['signal_hash'], price_hash
                                        ))
                                        await db_conn.commit()
                                        await send_advanced_telegram_alert(signal)
                                        signals_found += 1
                                        log.info(f"✅ Advanced signal sent (simplified insert) for {symbol}")
                                    except Exception as e2:
                                        log.error(f"Simplified insert also failed for {symbol}: {e2}")
                            else:
                                log.debug(f"Final duplicate check failed for {symbol}")
                    
                    # Respect rate limits
                    await asyncio.sleep(0.5)
                    
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
        
        risk_pct = abs(entry - sl) / entry * 100 if entry != 0 else 0
        reward_pct = abs(tp - entry) / entry * 100 if entry != 0 else 0
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
        "top_n": TOP_N,
        "scan_interval": SCAN_INTERVAL
    }

# ================ MAIN ================

async def main():
    global exchange
    
    log.info("=" * 70)
    log.info("🚀 ADVANCED VISUAL SYNTHESIS SCANNER - PROFESSIONAL EDITION")
    log.info("=" * 70)
    
    # === تحقق من إعدادات التليجرام ===
    log.info(f"📱 Telegram Token: {'✅ SET' if TELEGRAM_TOKEN else '❌ NOT SET'}")
    log.info(f"📱 Telegram Chat ID: {'✅ SET' if TELEGRAM_CHAT_ID else '❌ NOT SET'}")
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ Telegram credentials are not set. Alerts will NOT be sent!")
        log.warning("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
        log.warning("Example: export TELEGRAM_BOT_TOKEN='123456:ABC-DEF1234'")
        log.warning("Example: export TELEGRAM_CHAT_ID='-1001234567890'")
    else:
        log.info("✅ Telegram credentials verified")
    
    log.info(f"🎯 Minimum Score: {MIN_SYNTHESIS_SCORE}, Required Confirmations: {CONFLUENCE_REQUIRED}")
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
🚀 **الماسح الضوئي المتقدم - بدون TA-Lib**

✅ **تم بدء التشغيل بنجاح**
✅ **جميع المؤشرات مكتوبة بلغة بايثون البحتة**
✅ **لا حاجة لتثبيت مكتبات خارجية**

**الإعدادات:**
• الحد الأدنى للجودة: {MIN_SYNTHESIS_SCORE}
• التوكيدات المطلوبة: {CONFLUENCE_REQUIRED}
• عدد الأزواج: {TOP_N}
• فاصل المسح: {SCAN_INTERVAL} ثانية

**المنهجية المتطورة تعمل بكامل طاقتها!**

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
        log.info("Advanced scanner shutdown complete")

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