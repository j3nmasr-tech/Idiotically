#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PRODUCTION ROMEOPT SCANNER - COMPLETE VERSION WITH HTF BIAS FILTER
- All 3 MUST-HAVE steps implemented
- Timeframe-specific liquidity rules:
  • 1m, 3m, 5m → ALWAYS use HTF liquidity
  • 15m, 30m → Use same TF liquidity (HTF as fallback)
- HTF BIAS FILTERING SYSTEM:
  • Rule 1: HTF Range Position (no mid-range trades)
  • Rule 2: HTF Liquidity Status (sweep alignment)
  • Rule 3: HTF Displacement (momentum continuation)
- Fixed logging and all errors
"""

import os
import sys
import time
import asyncio
import logging
import datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import json

# ==================== CONFIGURATION ====================
class Config:
    """Centralized configuration management"""
    # Telegram
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "romeopt_secure_key")
    
    # Database
    DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_signals.db")
    
    # Scanning
    SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "10"))
    TOP_N = int(os.getenv("TOP_N", "10"))
    TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
    
    # Signal thresholds
    MIN_SCORE = 2
    CRITICAL_FACTORS_MIN = 0
    
    # Forced Filter (RomeOPT Core)
    MOMENTUM_STRONG_THRESHOLD = 0.00
    MOMENTUM_GOOD_THRESHOLD = 0.00
    DISPLACEMENT_MIN_THRESHOLD = 0.00
    
    # RomeOPT Parameters
    ATR_PERIOD = 14
    SWEEP_LOOKBACK = 20
    OB_LOOKBACK = 30
    LIQUIDITY_LOOKBACK = 50
    ATR_TOLERANCE_MULTIPLIER = 0.15
    MIN_RISK_REWARD_RATIO = 0.5
    
    # Exchange
    EXCHANGE_ID = "okx"
    ENABLE_RATE_LIMIT = True
    
    # HTF BIAS CONFIGURATION
    HTF_BIAS_ENABLED = False
    RANGE_EXTREME_THRESHOLD = 0.25  # Top/bottom 25% = "near" range edge
    MIN_HTF_BIAS_SCORE = 0.6  # Minimum score to accept trade (0.0-1.0)
    MID_RANGE_REJECT = False   # Auto-reject mid-range entries
    
    # HTF Mapping (RomeOPT-style relative HTF)
    HTF_MAP = {
        '1m': ['5m', '15m'],      # Micro structure + local range
        '3m': ['15m'],            # Clean internal liquidity
        '5m': ['15m', '30m'],     # Avoid noise, still reactive
        '15m': ['30m', '1h'],     # Real range highs/lows
        '30m': ['1h', '4h']       # External liquidity
    }

# ==================== SETUP LOGGING FIRST ====================
def setup_logging():
    """Setup logging with console output only (no file logging)"""
    logger = logging.getLogger("romeopt")
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    # Also set for root logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[console_handler]
    )
    
    return logger

log = setup_logging()

# ==================== CREATE REQUIRED DIRECTORIES ====================
def create_directories():
    """Create required directories if they don't exist"""
    directories = [
        os.path.dirname(Config.DB_PATH),
        "/app/logs" if Config.DB_PATH.startswith("/app/") else "./logs"
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            log.info(f"Directory created/verified: {directory}")
        except Exception as e:
            log.warning(f"Could not create directory {directory}: {e}")

create_directories()

# ==================== DATA STRUCTURES ====================
@dataclass
class OrderBlock:
    """Order Block data structure"""
    type: str  # "BULLISH_OB" or "BEARISH_OB"
    index: int
    low: float
    high: float
    body_low: float
    body_high: float
    volume: float
    candle_size: float
    body_size: float
    wick_ratio: float
    strength: str = "UNKNOWN"
    tested: bool = False
    confluence_score: int = 0

@dataclass
class LiquiditySweep:
    """Liquidity Sweep analysis"""
    type: str
    direction: str
    swept_level: float
    strength: float
    respected: bool
    quality_score: float

@dataclass
class HtfBiasResult:
    """HTF Bias analysis result"""
    take_trade: bool
    score: float  # 0.0-1.0
    bias: str  # 'BULLISH', 'BEARISH', 'NEUTRAL'
    reasons: List[str]
    details: Dict[str, Any]
    rule_scores: Dict[str, float]  # Individual rule scores

@dataclass
class RomeOPTSignal:
    """Complete RomeOPT Signal"""
    symbol: str
    side: str
    entry: float
    sl: float
    tp: float
    score: int
    timeframe: str
    reasons: List[str]
    liquidity_sweep: LiquiditySweep
    order_block: OrderBlock
    market_state: str
    tp_type: str
    tp_locked: bool = True
    risk_reward_ratio: float = 0.0
    htf_bias_score: float = 0.0
    htf_bias_result: Optional[HtfBiasResult] = None
    
    def __post_init__(self):
        self.calculate_metrics()
    
    def calculate_metrics(self):
        """Calculate risk/reward metrics"""
        if self.side == "BUY":
            risk = self.entry - self.sl
            reward = self.tp - self.entry
        else:  # SELL
            risk = self.sl - self.entry
            reward = self.entry - self.tp
        
        if risk > 0:
            self.risk_reward_ratio = reward / risk

# ==================== HTF BIAS SYSTEM ====================
class HtfBiasSystem:
    """HTF Bias Filtering System based on 3 Rules"""
    
    def __init__(self, config: Config, exchange):
        self.config = config
        self.exchange = exchange
        self.htf_cache = {}  # Cache HTF data to reduce API calls
        self.cache_duration = 60  # Cache for 60 seconds
    
    async def get_htf_data(self, symbol: str, timeframe: str, limit: int = 50):
        """Get HTF data with caching"""
        cache_key = f"{symbol}:{timeframe}"
        now = time.time()
        
        if cache_key in self.htf_cache:
            data, timestamp = self.htf_cache[cache_key]
            if now - timestamp < self.cache_duration:
                return data
        
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol, 
                timeframe=timeframe, 
                limit=limit
            )
            if not ohlcv or len(ohlcv) < 10:
                return None
            
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            self.htf_cache[cache_key] = (df, now)
            return df
            
        except Exception as e:
            log.debug(f"HTF data fetch failed for {symbol} {timeframe}: {e}")
            return None
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate ATR for HTF"""
        try:
            high = pd.to_numeric(df["high"], errors='coerce')
            low = pd.to_numeric(df["low"], errors='coerce')
            close = pd.to_numeric(df["close"], errors='coerce')
            
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(period, min_periods=1).mean()
            
            return float(atr.iloc[-1])
        except Exception as e:
            log.debug(f"HTF ATR calculation error: {e}")
            return 0.0
    
    async def assess_htf_bias(self, symbol: str, entry_tf: str, entry_price: float, 
                             side: str) -> HtfBiasResult:
        """
        Assess HTF bias based on 3 rules
        Returns: HtfBiasResult with final decision
        """
        if not self.config.HTF_BIAS_ENABLED:
            return HtfBiasResult(
                take_trade=True,
                score=1.0,
                bias=side.upper(),
                reasons=["HTF_BIAS_DISABLED"],
                details={},
                rule_scores={"rule1": 1.0, "rule2": 1.0, "rule3": 1.0}
            )
        
        htf_tfs = self.config.HTF_MAP.get(entry_tf, [])
        if not htf_tfs:
            log.warning(f"No HTF mapping for {entry_tf}")
            return HtfBiasResult(
                take_trade=True,
                score=1.0,
                bias=side.upper(),
                reasons=["NO_HTF_MAPPING"],
                details={},
                rule_scores={"rule1": 1.0, "rule2": 1.0, "rule3": 1.0}
            )
        
        rule1_scores = []
        rule2_scores = []
        rule3_scores = []
        all_reasons = []
        details = {}
        
        # Analyze each HTF
        for htf_tf in htf_tfs:
            df = await self.get_htf_data(symbol, htf_tf, 50)
            if df is None or len(df) < 20:
                continue
            
            # RULE 1: HTF Range Position
            rule1_result = self._rule1_range_position(df, entry_price, side, htf_tf)
            rule1_scores.append(rule1_result["score"])
            
            # RULE 2: HTF Liquidity Status
            rule2_result = self._rule2_liquidity_status(df, side, htf_tf)
            rule2_scores.append(rule2_result["score"])
            
            # RULE 3: HTF Displacement
            rule3_result = self._rule3_displacement(df, side, htf_tf)
            rule3_scores.append(rule3_result["score"])
            
            # Store details
            details[htf_tf] = {
                "rule1": rule1_result,
                "rule2": rule2_result,
                "rule3": rule3_result
            }
            
            all_reasons.extend(rule1_result.get("reasons", []))
            all_reasons.extend(rule2_result.get("reasons", []))
            all_reasons.extend(rule3_result.get("reasons", []))
        
        # Calculate final scores
        rule1_final = np.mean(rule1_scores) if rule1_scores else 0
        rule2_final = np.mean(rule2_scores) if rule2_scores else 0
        rule3_final = np.mean(rule3_scores) if rule3_scores else 0
        
        # Weighted final score
        final_score = (
            rule1_final * 0.50 +  # Rule 1 weight: 50% (most important)
            rule2_final * 0.30 +  # Rule 2 weight: 30%
            rule3_final * 0.20    # Rule 3 weight: 20%
        )
        
        # Determine bias
        if side.upper() == "BUY":
            bias = "BULLISH"
        else:
            bias = "BEARISH"
        
        # Check for mid-range rejection
        if self.config.MID_RANGE_REJECT and rule1_final == 0:
            all_reasons.append("MID_RANGE_REJECTED")
            return HtfBiasResult(
                take_trade=False,
                score=final_score,
                bias="NEUTRAL",
                reasons=all_reasons,
                details=details,
                rule_scores={
                    "rule1": rule1_final,
                    "rule2": rule2_final,
                    "rule3": rule3_final
                }
            )
        
        # Final decision
        take_trade = final_score >= self.config.MIN_HTF_BIAS_SCORE
        
        if take_trade:
            all_reasons.append(f"HTF_BIAS_CONFIRMED (Score: {final_score:.2f})")
        else:
            all_reasons.append(f"HTF_BIAS_REJECTED (Score: {final_score:.2f} < {self.config.MIN_HTF_BIAS_SCORE})")
        
        return HtfBiasResult(
            take_trade=take_trade,
            score=final_score,
            bias=bias,
            reasons=all_reasons,
            details=details,
            rule_scores={
                "rule1": rule1_final,
                "rule2": rule2_final,
                "rule3": rule3_final
            }
        )
    
    def _rule1_range_position(self, df: pd.DataFrame, entry_price: float, 
                             side: str, htf_tf: str) -> Dict[str, Any]:
        """
        RULE 1: HTF Range Position
        price near HTF range LOW → bullish bias
        price near HTF range HIGH → bearish bias
        mid-range → NO BIAS (reject or downgrade)
        """
        # Get HTF range (last 20 candles)
        lookback = min(20, len(df))
        htf_high = float(df['high'].iloc[-lookback:].max())
        htf_low = float(df['low'].iloc[-lookback:].min())
        range_size = htf_high - htf_low
        
        if range_size <= 0:
            return {
                "score": 0.0,
                "reasons": [f"{htf_tf}: ZERO_RANGE"],
                "htf_high": htf_high,
                "htf_low": htf_low,
                "position": 0.5
            }
        
        # Calculate position in range (0-1)
        position = (entry_price - htf_low) / range_size
        
        # Determine score based on position and side
        if side.upper() == "BUY":
            # For BUY: Want to be near LOW
            if position <= self.config.RANGE_EXTREME_THRESHOLD:
                score = 1.0 - (position / self.config.RANGE_EXTREME_THRESHOLD)
                reason = f"{htf_tf}: NEAR_LOW_BUY"
            elif position >= (1 - self.config.RANGE_EXTREME_THRESHOLD):
                score = 0.0  # Wrong extreme
                reason = f"{htf_tf}: WRONG_EXTREME_BUY"
            else:
                score = 0.0  # Mid-range
                reason = f"{htf_tf}: MID_RANGE_BUY"
        else:  # SELL
            # For SELL: Want to be near HIGH
            if position >= (1 - self.config.RANGE_EXTREME_THRESHOLD):
                score = 1.0 - ((1 - position) / self.config.RANGE_EXTREME_THRESHOLD)
                reason = f"{htf_tf}: NEAR_HIGH_SELL"
            elif position <= self.config.RANGE_EXTREME_THRESHOLD:
                score = 0.0  # Wrong extreme
                reason = f"{htf_tf}: WRONG_EXTREME_SELL"
            else:
                score = 0.0  # Mid-range
                reason = f"{htf_tf}: MID_RANGE_SELL"
        
        return {
            "score": score,
            "reasons": [reason],
            "htf_high": htf_high,
            "htf_low": htf_low,
            "position": position,
            "range_size": range_size
        }
    
    def _rule2_liquidity_status(self, df: pd.DataFrame, side: str, 
                               htf_tf: str) -> Dict[str, Any]:
        """
        RULE 2: HTF Liquidity Status
        HTF high swept → bearish bias
        HTF low swept → bullish bias
        no sweep → neutral (allowed but weaker)
        """
        # Look for recent sweeps (last 10 candles)
        lookback = min(10, len(df) - 1)
        if lookback < 2:
            return {
                "score": 0.5,  # Neutral
                "reasons": [f"{htf_tf}: INSUFFICIENT_DATA"],
                "sweep_found": False
            }
        
        # Get recent extremes
        recent_highs = df['high'].iloc[-lookback-5:-1].values
        recent_lows = df['low'].iloc[-lookback-5:-1].values
        
        # Check last few candles for sweeps
        sweep_found = False
        sweep_score = 0.0
        
        for i in range(1, lookback + 1):
            current_high = float(df['high'].iloc[-i])
            current_low = float(df['low'].iloc[-i])
            
            # Check high sweep
            if side.upper() == "SELL":
                if any(current_high > high * 1.0001 for high in recent_highs):  # 0.01% above
                    sweep_found = True
                    age = i / lookback
                    sweep_score = max(sweep_score, 1.0 - age * 0.5)  # Recent sweeps get higher score
                    break
            
            # Check low sweep
            if side.upper() == "BUY":
                if any(current_low < low * 0.9999 for low in recent_lows):  # 0.01% below
                    sweep_found = True
                    age = i / lookback
                    sweep_score = max(sweep_score, 1.0 - age * 0.5)
                    break
        
        if sweep_found:
            return {
                "score": sweep_score,
                "reasons": [f"{htf_tf}: SWEEP_CONFIRMED"],
                "sweep_found": True,
                "sweep_score": sweep_score
            }
        else:
            return {
                "score": 0.5,  # Neutral score
                "reasons": [f"{htf_tf}: NO_SWEEP"],
                "sweep_found": False
            }
    
    def _rule3_displacement(self, df: pd.DataFrame, side: str, 
                           htf_tf: str) -> Dict[str, Any]:
        """
        RULE 3: HTF Displacement/Imbalance
        Strong HTF displacement → bias continues
        Balanced candles → bias weakens
        """
        # Analyze last 5 candles
        lookback = min(5, len(df))
        if lookback < 3:
            return {
                "score": 0.5,
                "reasons": [f"{htf_tf}: INSUFFICIENT_DATA"],
                "displacement": "NEUTRAL"
            }
        
        displacement_scores = []
        
        for i in range(1, lookback + 1):
            candle = df.iloc[-i]
            try:
                open_price = float(candle["open"])
                close_price = float(candle["close"])
                high_price = float(candle["high"])
                low_price = float(candle["low"])
            except (ValueError, TypeError):
                continue
            
            candle_range = high_price - low_price
            if candle_range <= 0:
                continue
            
            # Calculate body ratio and position
            body_size = abs(close_price - open_price)
            body_ratio = body_size / candle_range if candle_range > 0 else 0
            
            # For bullish bias: want strong bullish candles
            if side.upper() == "BUY":
                if close_price > open_price:  # Bullish candle
                    if body_ratio > 0.7:
                        displacement_scores.append(1.0)  # Strong bullish
                    elif body_ratio > 0.3:
                        displacement_scores.append(0.7)  # Moderate bullish
                    else:
                        displacement_scores.append(0.3)  # Weak bullish
                else:  # Bearish candle (contrary)
                    displacement_scores.append(0.1)
            
            # For bearish bias: want strong bearish candles
            else:  # SELL
                if close_price < open_price:  # Bearish candle
                    if body_ratio > 0.7:
                        displacement_scores.append(1.0)  # Strong bearish
                    elif body_ratio > 0.3:
                        displacement_scores.append(0.7)  # Moderate bearish
                    else:
                        displacement_scores.append(0.3)  # Weak bearish
                else:  # Bullish candle (contrary)
                    displacement_scores.append(0.1)
        
        if displacement_scores:
            avg_score = np.mean(displacement_scores)
            
            if avg_score >= 0.7:
                displacement_type = "STRONG"
            elif avg_score >= 0.4:
                displacement_type = "MODERATE"
            else:
                displacement_type = "WEAK"
            
            return {
                "score": avg_score,
                "reasons": [f"{htf_tf}: {displacement_type}_DISPLACEMENT"],
                "displacement": displacement_type,
                "avg_score": avg_score
            }
        else:
            return {
                "score": 0.5,
                "reasons": [f"{htf_tf}: NEUTRAL_DISPLACEMENT"],
                "displacement": "NEUTRAL"
            }

# ==================== DATABASE ====================
class RomeOPTDatabase:
    """Database management for RomeOPT signals"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize database with automatic schema migration"""
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        
        # First, create the table with all columns
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp REAL NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                status TEXT DEFAULT 'OPEN',
                reason TEXT,
                score INTEGER,
                tp_hit INTEGER DEFAULT 0,
                sl_hit INTEGER DEFAULT 0,
                market_state TEXT,
                tp_type TEXT,
                risk_reward REAL,
                htf_bias_score REAL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Check if htf_bias_score column exists, add it if missing
        try:
            # Get table info to check columns
            cursor = await self.conn.execute("PRAGMA table_info(signals)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]  # column name is at index 1
            
            if 'htf_bias_score' not in column_names:
                log.info("Adding missing column: htf_bias_score")
                await self.conn.execute("ALTER TABLE signals ADD COLUMN htf_bias_score REAL DEFAULT 0")
                await self.conn.commit()
                log.info("Column htf_bias_score added successfully")
        except Exception as e:
            log.error(f"Error checking/adding column: {e}")
            # Continue anyway - the save_signal will try to handle it
        
        await self.conn.commit()
        log.info(f"Database initialized at {self.db_path}")
    
    async def save_signal(self, signal: RomeOPTSignal):
        """Save a signal to database"""
        async with self.lock:
            try:
                await self.conn.execute("""
                    INSERT INTO signals (
                        symbol, side, entry, sl, tp, timeframe, timestamp,
                        status, reason, score, market_state, tp_type, risk_reward, htf_bias_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    signal.symbol, signal.side, signal.entry, signal.sl,
                    signal.tp, signal.timeframe,
                    datetime.datetime.utcnow().isoformat(),
                    'OPEN', ' | '.join(signal.reasons), signal.score,
                    signal.market_state, signal.tp_type, signal.risk_reward_ratio,
                    signal.htf_bias_score
                ))
                
                await self.conn.commit()
                log.info(f"Signal saved: {signal.symbol} {signal.side} (HTF Bias: {signal.htf_bias_score:.2f})")
                return True
            except Exception as e:
                log.error(f"Failed to save signal: {e}")
                return False
    
    async def get_open_signals(self):
        """Get all open signals"""
        async with self.lock:
            try:
                async with self.conn.execute(
                    "SELECT id, symbol, side, entry, sl, tp, tp_hit FROM signals WHERE status = 'OPEN'"
                ) as cursor:
                    return await cursor.fetchall()
            except Exception as e:
                log.error(f"Failed to get open signals: {e}")
                return []
    
    async def update_signal_status(self, signal_id: int, tp_hit: bool = False, sl_hit: bool = False):
        """Update signal status"""
        async with self.lock:
            try:
                await self.conn.execute("""
                    UPDATE signals 
                    SET status = 'CLOSED', 
                        tp_hit = ?,
                        sl_hit = ?
                    WHERE id = ?
                """, (1 if tp_hit else 0, 1 if sl_hit else 0, signal_id))
                await self.conn.commit()
                return True
            except Exception as e:
                log.error(f"Failed to update signal: {e}")
                return False
    
    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()
            log.info("Database connection closed")

# ==================== TELEGRAM ====================
class TelegramBot:
    """Telegram notifications"""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.client = httpx.AsyncClient(timeout=10.0)
        
        if not token or not chat_id:
            log.warning("Telegram credentials not provided. Notifications disabled.")
    
    async def send_message(self, message: str, parse_mode: str = "HTML"):
        """Send message to Telegram"""
        if not self.token or not self.chat_id:
            return False
        
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            # Basic HTML escaping
            safe_message = (message
                          .replace("&", "&amp;")
                          .replace("<", "&lt;")
                          .replace(">", "&gt;"))
            
            payload = {
                "chat_id": self.chat_id,
                "text": safe_message,
                "parse_mode": parse_mode,
            }
            
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
            return True
        except Exception as e:
            log.error(f"Telegram send failed: {e}")
            return False

# ==================== ROMEOPT ENGINE ====================
class RomeOPTEngine:
    """Core RomeOPT trading engine with HTF Bias filtering"""
    
    def __init__(self, config: Config):
        self.config = config
        self.exchange = None
        self.db = RomeOPTDatabase(config.DB_PATH)
        self.tg_bot = TelegramBot(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT_ID)
        self.htf_bias_system = None
        
        # Tracking
        self.recent_sl_hits = defaultdict(lambda: deque())
        self.last_signal_time = {}
        self.signals_generated = 0
    
    async def initialize(self):
        """Initialize the engine"""
        log.info("Initializing RomeOPT Engine...")
        
        await self.db.initialize()
        
        try:
            exchange_class = getattr(ccxt, self.config.EXCHANGE_ID)
            self.exchange = exchange_class({
                'enableRateLimit': self.config.ENABLE_RATE_LIMIT,
                'options': {'defaultType': 'spot'}
            })
            
            # Test connection
            await self.exchange.load_markets()
            log.info(f"Connected to {self.config.EXCHANGE_ID}")
            
            # Initialize HTF Bias System
            self.htf_bias_system = HtfBiasSystem(self.config, self.exchange)
            
        except Exception as e:
            log.error(f"Failed to initialize exchange: {e}")
            raise
        
        await self.tg_bot.send_message("🚀 ROMEOPT ENGINE STARTED\n"
                                      "✅ All 3 MUST-HAVE steps active\n"
                                      "✅ Timeframe-specific liquidity rules enabled\n"
                                      f"✅ HTF BIAS FILTERING: {'ENABLED' if self.config.HTF_BIAS_ENABLED else 'DISABLED'}")
        log.info("RomeOPT Engine initialized successfully")
    
    # ==================== TIME-SPECIFIC RULES ====================
    def should_use_htf_liquidity(self, timeframe: str) -> bool:
        """
        1m, 3m, 5m → ALWAYS use HTF liquidity
        15m, 30m → Use same TF liquidity
        """
        return timeframe in ["1m", "3m", "5m"]
    
    def get_htf_for_timeframe(self, timeframe: str) -> str:
        """Get appropriate HTF for each TF"""
        htf_map = {
            "1m": "5m",
            "3m": "15m", 
            "5m": "15m",
            "15m": "1h",   # For confluence only
            "30m": "4h"    # For confluence only
        }
        return htf_map.get(timeframe, "1h")
    
    # ==================== CORE ROMEOPT STEPS ====================
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 200):
        """Fetch OHLCV data"""
        try:
            data = await self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if data and len(data) > 0:
                log.debug(f"Fetched {len(data)} candles for {symbol} {timeframe}")
                return data
            else:
                log.debug(f"No data for {symbol} {timeframe}")
                return None
        except Exception as e:
            log.debug(f"OHLCV fetch failed for {symbol} {timeframe}: {e}")
            return None
    
    def detect_liquidity_sweep(self, df: pd.DataFrame) -> Optional[LiquiditySweep]:
        """MUST-HAVE STEP 1: Detect liquidity sweeps"""
        if len(df) < self.config.SWEEP_LOOKBACK + 5:
            log.debug("Insufficient data for sweep detection")
            return None
        
        last = df.iloc[-1]
        highs = df['high'].iloc[-self.config.SWEEP_LOOKBACK:-1]
        lows = df['low'].iloc[-self.config.SWEEP_LOOKBACK:-1]
        
        sweep_high = last["high"] > highs.max()
        sweep_low = last["low"] < lows.min()
        
        if not (sweep_high or sweep_low):
            log.debug("No sweep detected")
            return None
        
        if sweep_high:
            swept_level = float(highs.max())
            sweep_amount = last["high"] - swept_level
            candle_range = last["high"] - last["low"]
            strength = sweep_amount / candle_range if candle_range > 0 else 0
            respected = last["close"] < swept_level
            sweep_type = "HIGH_SWEEP_RESPECTED" if respected else "HIGH_SWEEP_UNRESPECTED"
            direction = "BEARISH"
        else:
            swept_level = float(lows.min())
            sweep_amount = swept_level - last["low"]
            candle_range = last["high"] - last["low"]
            strength = sweep_amount / candle_range if candle_range > 0 else 0
            respected = last["close"] > swept_level
            sweep_type = "LOW_SWEEP_RESPECTED" if respected else "LOW_SWEEP_UNRESPECTED"
            direction = "BULLISH"
        
        # Quality score
        quality_score = min(strength * 0.5 + (0.3 if respected else 0), 1.0)
        
        log.debug(f"Sweep detected: {sweep_type}, Strength: {strength:.2f}, Quality: {quality_score:.2f}")
        
        return LiquiditySweep(
            type=sweep_type,
            direction=direction,
            swept_level=swept_level,
            strength=strength,
            respected=respected,
            quality_score=quality_score
        )
    
    def detect_order_block(self, df: pd.DataFrame) -> Optional[OrderBlock]:
        """MUST-HAVE STEP 2: Detect order blocks"""
        if len(df) < self.config.OB_LOOKBACK + 2:
            log.debug("Insufficient data for OB detection")
            return None
        
        blocks = []
        lookback_start = max(2, len(df) - self.config.OB_LOOKBACK)
        
        for i in range(lookback_start, len(df) - 1):
            candle = df.iloc[i]
            prev_candle = df.iloc[i-1]
            
            # Ensure we have numeric values
            try:
                prev_close = float(prev_candle["close"])
                prev_open = float(prev_candle["open"])
                curr_close = float(candle["close"])
                curr_open = float(candle["open"])
            except (ValueError, TypeError):
                continue
            
            # Bullish OB: Bearish → Bullish reversal
            if (prev_close < prev_open and
                curr_close > curr_open and
                curr_close > prev_close):
                
                block = OrderBlock(
                    type="BULLISH_OB",
                    index=i,
                    low=min(float(candle["low"]), float(prev_candle["low"])),
                    high=max(curr_close, prev_close),
                    body_low=min(curr_open, curr_close),
                    body_high=max(curr_open, curr_close),
                    volume=float(candle.get("volume", 0)),
                    candle_size=float(candle["high"]) - float(candle["low"]),
                    body_size=abs(curr_close - curr_open),
                    wick_ratio=(float(candle["high"]) - max(curr_open, curr_close)) / 
                               (float(candle["high"]) - float(candle["low"])) if (float(candle["high"]) - float(candle["low"])) > 0 else 0
                )
                blocks.append(block)
            
            # Bearish OB: Bullish → Bearish reversal
            elif (prev_close > prev_open and
                  curr_close < curr_open and
                  curr_close < prev_close):
                
                block = OrderBlock(
                    type="BEARISH_OB",
                    index=i,
                    low=min(curr_close, prev_close),
                    high=max(float(candle["high"]), float(prev_candle["high"])),
                    body_low=min(curr_open, curr_close),
                    body_high=max(curr_open, curr_close),
                    volume=float(candle.get("volume", 0)),
                    candle_size=float(candle["high"]) - float(candle["low"]),
                    body_size=abs(curr_close - curr_open),
                    wick_ratio=(min(curr_open, curr_close) - float(candle["low"])) / 
                               (float(candle["high"]) - float(candle["low"])) if (float(candle["high"]) - float(candle["low"])) > 0 else 0
                )
                blocks.append(block)
        
        if not blocks:
            log.debug("No order blocks detected")
            return None
        
        latest = max(blocks, key=lambda x: x.index)
        
        # Classify strength
        body_ratio = latest.body_size / latest.candle_size if latest.candle_size > 0 else 0
        if body_ratio >= 0.7:
            latest.strength = "STRONG"
        elif body_ratio >= 0.5:
            latest.strength = "MODERATE"
        else:
            latest.strength = "WEAK"
        
        log.debug(f"OB detected: {latest.type}, Strength: {latest.strength}")
        
        return latest
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate ATR"""
        try:
            high = pd.to_numeric(df["high"], errors='coerce')
            low = pd.to_numeric(df["low"], errors='coerce')
            close = pd.to_numeric(df["close"], errors='coerce')
            
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(period, min_periods=1).mean()
            
            return float(atr.iloc[-1])
        except Exception as e:
            log.error(f"ATR calculation error: {e}")
            return 0.0
    
    def _determine_market_state(self, df: pd.DataFrame, atr_val: float) -> str:
        """
        REFINED RomeOPT market state detection
        Checks: Strong displacement + actual price movement
        """
        if len(df) < 3 or atr_val <= 0:
            return "BALANCED"
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        try:
            body_ratio = abs(float(last["close"]) - float(last["open"])) / (float(last["high"]) - float(last["low"]) + 1e-8)
            candle_size = float(last["high"]) - float(last["low"])
            price_movement = abs(float(last["close"]) - float(prev["close"]))
        except (ValueError, TypeError):
            return "BALANCED"
        
        # RomeOPT logic: Strong displacement with actual follow-through
        strong_displacement = (
            body_ratio > 0.7 and                    # Strong body (>70%)
            candle_size > atr_val * 1.2 and         # Large candle (>1.2 ATR)
            price_movement > atr_val * 0.5          # Actual price movement (>0.5 ATR)
        )
        
        return "IMBALANCED" if strong_displacement else "BALANCED"
    
    async def get_htf_liquidity(self, symbol: str, htf: str, side: str) -> Optional[Dict]:
        """Get HTF liquidity for 1m/3m/5m or confluence for 15m/30m"""
        try:
            log.debug(f"Fetching HTF liquidity for {symbol} {htf} {side}")
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=htf, limit=100)
            if not ohlcv or len(ohlcv) < 20:
                log.debug(f"Insufficient HTF data for {symbol} {htf}")
                return None
            
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
            
            # Convert to numeric
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            result = {"timeframe": htf, "side": side}
            
            if side == "BUY":
                # For BUY: Find HTF highs
                range_high = float(df['high'].iloc[-50:].max())
                result["range_high"] = range_high
                
                # Find clusters
                highs = df['high'].iloc[-30:].values
                atr_val = self._calculate_atr(df, 14)
                tolerance = atr_val * 0.15 if atr_val > 0 else 0.001
                
                from collections import defaultdict
                clusters = defaultdict(int)
                for high in highs:
                    if tolerance > 0:
                        rounded = round(float(high) / tolerance) * tolerance
                        clusters[rounded] += 1
                
                if clusters:
                    best_cluster = max(clusters.items(), key=lambda x: x[1])
                    if best_cluster[1] >= 2:
                        result["cluster_high"] = float(best_cluster[0])
                        result["target"] = result["cluster_high"]
                        result["target_type"] = "HTF_Equal_Highs_Cluster"
                    else:
                        result["target"] = range_high
                        result["target_type"] = "HTF_Range_High"
                else:
                    result["target"] = range_high
                    result["target_type"] = "HTF_Range_High"
                    
            else:  # SELL
                # For SELL: Find HTF lows
                range_low = float(df['low'].iloc[-50:].min())
                result["range_low"] = range_low
                
                # Find clusters
                lows = df['low'].iloc[-30:].values
                atr_val = self._calculate_atr(df, 14)
                tolerance = atr_val * 0.15 if atr_val > 0 else 0.001
                
                from collections import defaultdict
                clusters = defaultdict(int)
                for low in lows:
                    if tolerance > 0:
                        rounded = round(float(low) / tolerance) * tolerance
                        clusters[rounded] += 1
                
                if clusters:
                    best_cluster = min(clusters.items(), key=lambda x: x[0])
                    if best_cluster[1] >= 2:
                        result["cluster_low"] = float(best_cluster[0])
                        result["target"] = result["cluster_low"]
                        result["target_type"] = "HTF_Equal_Lows_Cluster"
                    else:
                        result["target"] = range_low
                        result["target_type"] = "HTF_Range_Low"
                else:
                    result["target"] = range_low
                    result["target_type"] = "HTF_Range_Low"
            
            log.debug(f"HTF liquidity found for {symbol} {htf}: {result.get('target_type', 'N/A')}")
            return result
            
        except Exception as e:
            log.error(f"HTF liquidity error for {symbol} {htf}: {e}")
            return None
    
    def _find_internal_liquidity(self, df: pd.DataFrame, side: str, atr_val: float) -> Optional[float]:
        """Find internal liquidity clusters"""
        lookback = 15
        tolerance = atr_val * self.config.ATR_TOLERANCE_MULTIPLIER if atr_val > 0 else 0.001
        
        if side == "SELL":
            lows = pd.to_numeric(df['low'].iloc[-lookback:], errors='coerce').dropna()
            if len(lows) < 5:
                return None
            
            clusters = []
            for i in range(len(lows)):
                current = lows.iloc[i]
                nearby = (abs(lows - current) <= tolerance).sum()
                if nearby >= 2:
                    clusters.append((float(current), nearby))
            
            if clusters:
                best_target = min(clusters, key=lambda x: x[0])
                log.debug(f"Internal liquidity (SELL): {best_target[0]}, cluster size: {best_target[1]}")
                return best_target[0]
        
        else:  # BUY
            highs = pd.to_numeric(df['high'].iloc[-lookback:], errors='coerce').dropna()
            if len(highs) < 5:
                return None
            
            clusters = []
            for i in range(len(highs)):
                current = highs.iloc[i]
                nearby = (abs(highs - current) <= tolerance).sum()
                if nearby >= 2:
                    clusters.append((float(current), nearby))
            
            if clusters:
                best_target = max(clusters, key=lambda x: x[0])
                log.debug(f"Internal liquidity (BUY): {best_target[0]}, cluster size: {best_target[1]}")
                return best_target[0]
        
        return None
    
    def _find_external_liquidity(self, df: pd.DataFrame, side: str) -> Optional[float]:
        """Find external liquidity"""
        lookback = min(self.config.LIQUIDITY_LOOKBACK, len(df))
        
        if side == "SELL":
            target = float(df['low'].iloc[-lookback:].min())
            log.debug(f"External liquidity (SELL): {target}")
            return target
        else:  # BUY
            target = float(df['high'].iloc[-lookback:].max())
            log.debug(f"External liquidity (BUY): {target}")
            return target
    
    async def calculate_romeopt_tp_sl(self, entry: float, side: str, df: pd.DataFrame, 
                                    ob: OrderBlock, atr_val: float, 
                                    symbol: str, timeframe: str) -> Optional[Tuple[float, float, str, Dict]]:
        """
        MUST-HAVE STEP 3: Calculate TP/SL with timeframe-specific rules
        Returns: (sl, tp, tp_type, htf_liquidity)
        """
        use_htf = self.should_use_htf_liquidity(timeframe)
        htf = self.get_htf_for_timeframe(timeframe)
        htf_liquidity = None
        tp = None
        tp_type = ""
        
        log.info(f"Calculating TP/SL for {symbol} {timeframe} {side} (use_htf={use_htf}, htf={htf})")
        
        # ========== APPLY TIME-SPECIFIC RULES ==========
        if use_htf:
            # 1m, 3m, 5m: ALWAYS USE HTF LIQUIDITY
            htf_liquidity = await self.get_htf_liquidity(symbol, htf, side)
            if not htf_liquidity or "target" not in htf_liquidity:
                log.debug(f"No HTF liquidity for {symbol} {timeframe} → {htf}")
                return None
            
            tp = htf_liquidity["target"]
            tp_type = f"HTF ({htf}): {htf_liquidity['target_type']}"
            log.info(f"Using HTF liquidity: {tp:.6f} ({htf_liquidity['target_type']})")
            
        else:
            # 15m, 30m: USE SAME TF LIQUIDITY FIRST
            market_state = self._determine_market_state(df, atr_val)
            
            if market_state == "BALANCED":
                tp = self._find_internal_liquidity(df, side, atr_val)
                if tp:
                    tp_type = f"SAME_TF: Visual {'Lows' if side == 'SELL' else 'Highs'} Cluster"
                    log.info(f"Using same TF internal liquidity: {tp:.6f}")
            else:
                tp = self._find_external_liquidity(df, side)
                if tp:
                    tp_type = f"SAME_TF: Range {'Low' if side == 'SELL' else 'High'}"
                    log.info(f"Using same TF external liquidity: {tp:.6f}")
            
            # Fallback to HTF if same TF has no liquidity
            if not tp:
                htf_liquidity = await self.get_htf_liquidity(symbol, htf, side)
                if htf_liquidity and "target" in htf_liquidity:
                    tp = htf_liquidity["target"]
                    tp_type = f"HTF_FALLBACK ({htf}): {htf_liquidity['target_type']}"
                    log.info(f"Using HTF fallback liquidity: {tp:.6f}")
                else:
                    log.debug(f"No liquidity found for {symbol} {timeframe}")
                    return None
        
        # Validate target
        if not self._validate_liquidity_target(tp, side, df, atr_val):
            log.debug(f"Liquidity target invalid for {side} at {tp}")
            return None
        
        # Calculate SL
        sl = self._calculate_stop_loss(entry, side, ob, df, atr_val)
        
        # Validate R:R
        if side == "BUY":
            risk = entry - sl
            reward = tp - entry
        else:
            risk = sl - entry
            reward = entry - tp
        
        if risk <= 0:
            log.debug(f"Invalid risk calculation: {risk}")
            return None
        
        rr_ratio = reward / risk
        if rr_ratio < self.config.MIN_RISK_REWARD_RATIO:
            log.debug(f"RR ratio too low: {rr_ratio:.2f}")
            return None
        
        log.info(f"✅ {timeframe} {side} | Entry: {entry:.6f} | SL: {sl:.6f} | TP: {tp:.6f} | R:R: {rr_ratio:.2f}")
        
        return sl, tp, tp_type, htf_liquidity
    
    def _validate_liquidity_target(self, tp: float, side: str, df: pd.DataFrame, atr_val: float) -> bool:
        """Validate liquidity target"""
        recent = min(10, len(df))
        tolerance = atr_val * 0.1 if atr_val > 0 else 0.001
        
        if side == "SELL":
            for i in range(1, recent):
                if abs(float(df['low'].iloc[-i]) - tp) <= tolerance:
                    log.debug(f"TP recently touched (SELL): {tp}")
                    return False
        else:
            for i in range(1, recent):
                if abs(float(df['high'].iloc[-i]) - tp) <= tolerance:
                    log.debug(f"TP recently touched (BUY): {tp}")
                    return False
        
        return True
    
    def _calculate_stop_loss(self, entry: float, side: str, ob: OrderBlock, 
                            df: pd.DataFrame, atr_val: float) -> float:
        """Calculate stop loss"""
        if atr_val <= 0:
            atr_val = 0.001
        
        if side == "BUY":
            sl = ob.low - (atr_val * 0.3)
            recent_low = float(df['low'].iloc[-10:].min())
            sl = min(sl, recent_low - (atr_val * 0.3))
            
            min_risk = atr_val * 0.5
            risk = entry - sl
            if risk < min_risk:
                sl = entry - min_risk
                log.debug(f"Adjusted SL for minimum risk: {sl}")
        else:
            sl = ob.high + (atr_val * 0.3)
            recent_high = float(df['high'].iloc[-10:].max())
            sl = max(sl, recent_high + (atr_val * 0.3))
            
            min_risk = atr_val * 0.5
            risk = sl - entry
            if risk < min_risk:
                sl = entry + min_risk
                log.debug(f"Adjusted SL for minimum risk: {sl}")
        
        log.debug(f"Calculated SL: {sl} (entry: {entry}, atr: {atr_val})")
        return sl
    
    def _pass_forced_filter(self, momentum: float, displacement: float) -> bool:
        """Apply forced filter"""
        if momentum >= self.config.MOMENTUM_STRONG_THRESHOLD:
            return True
        if (momentum >= self.config.MOMENTUM_GOOD_THRESHOLD and 
            displacement >= self.config.DISPLACEMENT_MIN_THRESHOLD):
            return True
        return False
    
    def calculate_romeopt_score(self, sweep: LiquiditySweep, ob: OrderBlock, 
                              momentum: float, displacement: float,
                              rr_ratio: float, use_htf: bool, 
                              htf_bias_score: float = 0.0) -> int:
        """Calculate 6-step RomeOPT score (0-6) with HTF bias integration"""
        score = 0
        
        # Step 1: Liquidity Sweep (max 2 points)
        if sweep.quality_score > 0.7:
            score += 2
        elif sweep.quality_score > 0.5:
            score += 1
        
        # Step 2: Order Block (max 1 point)
        if ob.strength in ["STRONG", "MODERATE"]:
            score += 1
        
        # Step 3: Entry Approach (max 1 point)
        # Entry is already validated in generate_romeopt_signal
        score += 1
        
        # Step 4: Displacement (max 1 point)
        if displacement >= self.config.DISPLACEMENT_MIN_THRESHOLD:
            score += 1
        
        # Step 5: HTF Alignment (max 1 point)
        if use_htf:
            score += 1  # HTF alignment confirmed
        
        # Step 6: Risk/Reward & HTF Bias Bonus (max 1 point)
        if rr_ratio >= 1.0:
            score += 0.5
        elif rr_ratio >= 0.5:
            score += 0.25  # Quarter point
        
        # HTF Bias bonus (up to 0.5 points)
        if htf_bias_score > 0.8:
            score += 0.5
        elif htf_bias_score > 0.6:
            score += 0.25
        
        return min(6, int(score))
    
    async def generate_romeopt_signal(self, symbol: str, timeframe: str) -> Optional[RomeOPTSignal]:
        """Generate a complete RomeOPT signal with HTF Bias filtering"""
        log.debug(f"Generating signal for {symbol} {timeframe}")
        
        # Fetch data
        ohlcv = await self.fetch_ohlcv(symbol, timeframe, 200)
        if not ohlcv or len(ohlcv) < 50:
            log.debug(f"Insufficient data for {symbol} {timeframe}")
            return None
        
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
        
        # Convert to numeric
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        if len(df) < 20:
            log.debug(f"Not enough candles for {symbol} {timeframe}: {len(df)}")
            return None
        
        # ========== MUST-HAVE STEP 1 ==========
        sweep = self.detect_liquidity_sweep(df)
        if not sweep:
            log.debug(f"No sweep for {symbol} {timeframe}")
            return None
        
        # ========== MUST-HAVE STEP 2 ==========
        ob = self.detect_order_block(df)
        if not ob:
            log.debug(f"No OB for {symbol} {timeframe}")
            return None
        
        side = "BUY" if ob.type == "BULLISH_OB" else "SELL"
        
        # Check OB approach
        last_close = float(df['close'].iloc[-1])
        if side == "BUY":
            distance_to_ob = (last_close - ob.high) / (ob.high - ob.low + 1e-8)
            if not (last_close <= ob.high or distance_to_ob < 0.1):
                log.debug(f"Not approaching OB for BUY: price={last_close}, ob_high={ob.high}, distance={distance_to_ob:.2%}")
                return None
        else:
            distance_to_ob = (ob.low - last_close) / (ob.high - ob.low + 1e-8)
            if not (last_close >= ob.low or distance_to_ob < 0.1):
                log.debug(f"Not approaching OB for SELL: price={last_close}, ob_low={ob.low}, distance={distance_to_ob:.2%}")
                return None
        
        # Momentum and displacement
        last = df.iloc[-1]
        try:
            momentum = abs(float(last["close"]) - float(last["open"])) / (float(last["high"]) - float(last["low"]) + 1e-8)
        except (ValueError, TypeError, ZeroDivisionError):
            momentum = 0
        
        displacement = momentum  # Simplified
        
        if not self._pass_forced_filter(momentum, displacement):
            log.debug(f"Forced filter failed: momentum={momentum:.2f}, displacement={displacement:.2f}")
            return None
        
        # ========== MUST-HAVE STEP 3 ==========
        atr_val = self._calculate_atr(df, self.config.ATR_PERIOD)
        entry = last_close
        
        # ========== HTF BIAS FILTERING ==========
        htf_bias_result = None
        if self.config.HTF_BIAS_ENABLED and self.htf_bias_system:
            htf_bias_result = await self.htf_bias_system.assess_htf_bias(
                symbol, timeframe, entry, side
            )
            
            if not htf_bias_result.take_trade:
                log.info(f"HTF Bias rejected: {symbol} {timeframe} {side} (Score: {htf_bias_result.score:.2f})")
                log.info(f"HTF Bias reasons: {', '.join(htf_bias_result.reasons)}")
                return None
            
            log.info(f"HTF Bias approved: {symbol} {timeframe} {side} (Score: {htf_bias_result.score:.2f})")
        
        use_htf = self.should_use_htf_liquidity(timeframe)
        tp_sl_result = await self.calculate_romeopt_tp_sl(
            entry, side, df, ob, atr_val, symbol, timeframe
        )
        
        if not tp_sl_result:
            log.debug(f"TP/SL calculation failed for {symbol} {timeframe}")
            return None
        
        sl, tp, tp_type, htf_liquidity = tp_sl_result
        
        # Calculate R:R
        if side == "BUY":
            risk = entry - sl
            reward = tp - entry
        else:
            risk = sl - entry
            reward = entry - tp
        
        rr_ratio = reward / risk if risk > 0 else 0
        
        # Calculate 6-step score with HTF bias integration
        htf_bias_score = htf_bias_result.score if htf_bias_result else 0.0
        score = self.calculate_romeopt_score(sweep, ob, momentum, displacement, rr_ratio, use_htf, htf_bias_score)
        
        # Determine market state
        market_state = self._determine_market_state(df, atr_val)
        
        reasons = [
            f"RomeOPT_6Step",
            f"Liquidity:{'HTF' if use_htf else 'SAME_TF'}",
            f"Sweep:{sweep.type}",
            f"OB:{ob.type}({ob.strength})",
            f"Mom:{momentum:.2f}|Disp:{displacement:.2f}",
            f"TP:{tp_type}",
        ]
        
        # Add HTF bias reasons if available
        if htf_bias_result:
            htf_reasons = [r for r in htf_bias_result.reasons if "CONFIRMED" in r or "REJECTED" in r]
            if htf_reasons:
                reasons.append(f"HTFBias:{htf_bias_result.score:.2f}")
        
        signal = RomeOPTSignal(
            symbol=symbol,
            side=side,
            entry=entry,
            sl=sl,
            tp=tp,
            score=score,
            timeframe=timeframe,
            reasons=reasons,
            liquidity_sweep=sweep,
            order_block=ob,
            market_state=market_state,
            tp_type=tp_type,
            tp_locked=True,
            htf_bias_score=htf_bias_score,
            htf_bias_result=htf_bias_result
        )
        
        # Send notification with HTF bias info
        await self.send_signal_notification(signal, htf_liquidity, use_htf, htf_bias_result)
        
        # Save to DB
        await self.db.save_signal(signal)
        
        self.signals_generated += 1
        log.info(f"Signal generated: {symbol} {timeframe} {side} | Score: {score} | HTF Bias: {htf_bias_score:.2f}")
        
        return signal
    
    async def send_signal_notification(self, signal: RomeOPTSignal, htf_liquidity: Dict = None, 
                                      use_htf: bool = False, htf_bias_result: HtfBiasResult = None):
        """Send Telegram notification with HTF bias info"""
        
        # Determine liquidity source
        if use_htf and htf_liquidity:
            htf = htf_liquidity.get("timeframe", "N/A")
            liquidity_source = f"HTF({htf})"
            target_type = htf_liquidity.get("target_type", "N/A")
        else:
            liquidity_source = "SAME_TF"
            target_type = signal.tp_type.split(":")[-1].strip() if ":" in signal.tp_type else signal.tp_type
        
        # Calculate additional metrics
        ob_range = signal.order_block.high - signal.order_block.low
        entry_distance = abs(signal.entry - signal.order_block.low) if signal.side == "BUY" else abs(signal.order_block.high - signal.entry)
        entry_proximity = (entry_distance / ob_range) if ob_range > 0 else 1.0
        momentum = abs(signal.entry - signal.order_block.body_low) / (ob_range + 1e-8)
        
        # Build message
        message = [
            f"ROMEOPT {signal.symbol} {signal.side} {signal.timeframe} SCORE:{signal.score}/6",
            f"SWEEP:{signal.liquidity_sweep.type} DIR:{signal.liquidity_sweep.direction}",
            f"SWEPT_LEVEL:{signal.liquidity_sweep.swept_level:.6f} STR:{signal.liquidity_sweep.strength:.2f}",
            f"RESPECTED:{signal.liquidity_sweep.respected} QUALITY:{signal.liquidity_sweep.quality_score:.2f}",
            f"OB:{signal.order_block.type} STR:{signal.order_block.strength}",
            f"OB_RANGE:{signal.order_block.low:.6f}-{signal.order_block.high:.6f}",
            f"OB_BODY:{signal.order_block.body_low:.6f}-{signal.order_block.body_high:.6f}",
            f"OB_WICK:{signal.order_block.wick_ratio:.2f} VOL:{signal.order_block.volume:.0f}",
            f"OB_TESTED:{signal.order_block.tested} CONF:{signal.order_block.confluence_score}",
            f"ENTRY:{signal.entry:.6f} OB_PROX:{entry_proximity:.1%} MOM:{momentum:.2f}",
            f"LIQ_SRC:{liquidity_source} TP_TYPE:{target_type} STATE:{signal.market_state}",
        ]
        
        # Add HTF bias info if available
        if htf_bias_result:
            message.append(f"HTF_BIAS:{htf_bias_result.score:.2f} RULE1:{htf_bias_result.rule_scores.get('rule1', 0):.2f} RULE2:{htf_bias_result.rule_scores.get('rule2', 0):.2f} RULE3:{htf_bias_result.rule_scores.get('rule3', 0):.2f}")
        
        message.append(f"SL:{signal.sl:.6f} TP:{signal.tp:.6f} RR:{signal.risk_reward_ratio:.2f}:1 TP_LOCKED")
        
        # Send as one compact message
        await self.tg_bot.send_message("\n".join(message))
    
    # ==================== MONITORING ====================
    def record_sl_hit(self, symbol: str, lookback_minutes: int = 30):
        """Record SL hit for deprioritization"""
        now = time.time()
        dq = self.recent_sl_hits[symbol]
        dq.append(now)
        cutoff = now - (lookback_minutes * 60)
        while dq and dq[0] < cutoff:
            dq.popleft()
    
    def is_deprioritized(self, symbol: str, threshold: int = 3) -> bool:
        """Check if symbol should be deprioritized"""
        return len(self.recent_sl_hits[symbol]) >= threshold
    
    async def monitor_open_signals(self):
        """Monitor and update open signals"""
        log.info("Starting signal monitor")
        
        while True:
            try:
                open_signals = await self.db.get_open_signals()
                
                for signal in open_signals:
                    signal_id, symbol, side, entry, sl, tp, tp_hit = signal
                    
                    if tp_hit:
                        continue
                    
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        price = ticker.get("last")
                        
                        if not price:
                            continue
                        
                        tp_hit_flag = False
                        sl_hit_flag = False
                        
                        if side == "BUY":
                            if price >= tp:
                                tp_hit_flag = True
                            elif price <= sl:
                                sl_hit_flag = True
                        else:
                            if price <= tp:
                                tp_hit_flag = True
                            elif price >= sl:
                                sl_hit_flag = True
                        
                        if tp_hit_flag or sl_hit_flag:
                            await self.db.update_signal_status(signal_id, tp_hit_flag, sl_hit_flag)
                            
                            # Minimal SL/TP hit notification
                            pnl_percent = ((price-entry)/entry*100) if side=='BUY' else ((entry-price)/entry*100)
                            alert_msg = f"{'TP' if tp_hit_flag else 'SL'} HIT {symbol} {side} ENTRY:{entry:.6f} EXIT:{price:.6f} PNL:{pnl_percent:+.2f}%"
                            
                            await self.tg_bot.send_message(alert_msg)
                            
                            if sl_hit_flag:
                                self.record_sl_hit(symbol)
                    
                    except Exception as e:
                        log.error(f"Signal monitor error for {symbol}: {e}")
                
                await asyncio.sleep(self.config.SCAN_INTERVAL)
                
            except Exception as e:
                log.error(f"Monitor error: {e}")
                await asyncio.sleep(self.config.SCAN_INTERVAL)
    
    # ==================== SCANNING ====================
    async def scan_markets(self):
        """Main scanning loop"""
        await self.tg_bot.send_message(f"SCANNING STARTED Top {self.config.TOP_N} pairs TFs:{','.join(self.config.TIMEFRAMES)} HTF_BIAS:{'ON' if self.config.HTF_BIAS_ENABLED else 'OFF'}")
        log.info(f"Starting market scan for top {self.config.TOP_N} pairs")
        
        while True:
            start_time = time.time()
            signals_found = 0
            
            try:
                # Get tickers
                markets = await self.exchange.load_markets()
                tickers = await self.exchange.fetch_tickers()
                
                # Filter for USDT pairs with volume
                usdt_pairs = []
                for symbol, ticker in tickers.items():
                    if symbol.endswith("/USDT") and ticker.get("quoteVolume", 0) > 0:
                        usdt_pairs.append((symbol, ticker.get("quoteVolume", 0)))
                
                if not usdt_pairs:
                    log.warning("No USDT pairs found")
                    await asyncio.sleep(self.config.SCAN_INTERVAL)
                    continue
                
                # Sort by volume
                usdt_pairs.sort(key=lambda x: x[1], reverse=True)
                top_pairs = usdt_pairs[:self.config.TOP_N]
                
                log.info(f"Scanning {len(top_pairs)} pairs")
                
                # Scan each pair
                for symbol, volume in top_pairs:
                    if self.is_deprioritized(symbol):
                        log.debug(f"Skipping deprioritized: {symbol}")
                        continue
                    
                    for timeframe in self.config.TIMEFRAMES:
                        key = f"{symbol}:{timeframe}"
                        
                        # Check cooldown
                        if key in self.last_signal_time:
                            elapsed = time.time() - self.last_signal_time[key]
                            if elapsed < 60:  # 1 minute cooldown
                                continue
                        
                        # Generate signal
                        signal = await self.generate_romeopt_signal(symbol, timeframe)
                        
                        if signal:
                            self.last_signal_time[key] = time.time()
                            signals_found += 1
                
                # Calculate sleep time
                elapsed = time.time() - start_time
                sleep_time = max(1, self.config.SCAN_INTERVAL - elapsed)
                
                log.info(f"Scan complete: {signals_found} signals found in {elapsed:.1f}s. Sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                log.error(f"Scan error: {e}")
                await asyncio.sleep(self.config.SCAN_INTERVAL)
    
    async def run(self):
        """Main execution loop"""
        try:
            # Start monitor in background
            monitor_task = asyncio.create_task(self.monitor_open_signals())
            
            # Start scanning
            await self.scan_markets()
            
            # This should run forever
            await monitor_task
            
        except KeyboardInterrupt:
            log.info("Stopped by user")
        except Exception as e:
            log.error(f"Fatal error: {e}")
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Clean shutdown"""
        log.info("Shutting down RomeOPT Engine...")
        await self.db.close()
        if self.exchange:
            await self.exchange.close()
        log.info("Shutdown complete")

# ==================== FASTAPI ====================
app = FastAPI(title="RomeOPT Scanner", version="1.0.0")
romeopt_engine = None

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    global romeopt_engine
    try:
        config = Config()
        romeopt_engine = RomeOPTEngine(config)
        await romeopt_engine.initialize()
        log.info("API server started")
    except Exception as e:
        log.error(f"Failed to start API: {e}")
        raise

@app.post("/webhook")
async def webhook_handler(request: Request):
    """Handle webhook requests"""
    token = request.headers.get("X-Auth", "")
    if token != Config.WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    try:
        data = await request.json()
        log.info(f"Webhook received: {data}")
        return {"status": "ok", "message": "Webhook processed"}
    except Exception as e:
        log.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "service": "RomeOPT Scanner"
    }

@app.get("/stats")
async def get_stats():
    """Get engine statistics"""
    global romeopt_engine
    if not romeopt_engine:
        return {"error": "Engine not initialized"}
    
    return {
        "signals_generated": romeopt_engine.signals_generated,
        "scan_interval": Config.SCAN_INTERVAL,
        "htf_bias_enabled": Config.HTF_BIAS_ENABLED,
        "status": "running"
    }

@app.on_event("shutdown")
async def shutdown_event():
    """Clean shutdown"""
    global romeopt_engine
    if romeopt_engine:
        await romeopt_engine.shutdown()

# ==================== MAIN ====================
def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="RomeOPT Trading Scanner")
    parser.add_argument("--mode", choices=["scanner", "api"], default="scanner",
                       help="Run mode: scanner (default) or api")
    parser.add_argument("--host", default="0.0.0.0", help="API host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=9000, help="API port (default: 9000)")
    
    args = parser.parse_args()
    
    if args.mode == "api":
        # Run FastAPI server
        log.info(f"Starting RomeOPT API server on {args.host}:{args.port}")
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            log_level="info",
            access_log=False
        )
    else:
        # Run scanner directly
        async def run_scanner():
            try:
                config = Config()
                engine = RomeOPTEngine(config)
                await engine.initialize()
                await engine.run()
            except KeyboardInterrupt:
                log.info("Scanner stopped by user")
            except Exception as e:
                log.error(f"Scanner error: {e}")
                raise
        
        try:
            asyncio.run(run_scanner())
        except KeyboardInterrupt:
            log.info("Program terminated")

if __name__ == "__main__":
    main()