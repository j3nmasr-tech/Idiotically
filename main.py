#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEXC FUTURES PUMP & DUMP SNIPER v2.0
OPTIMIZED: Faster scanning, parallel analysis, rate limiting
"""

import os
import sys
import time
import asyncio
import logging
import hashlib
import traceback
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, asdict
import json
from collections import deque

# ================ CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/mexc_pump_dump.db")

# Exchange config
EXCHANGE_NAME = "mexc"
CONTRACT_TYPE = "swap"
LEVERAGE = 3

# Low-cap targeting
MIN_PRICE = 0.00001
MAX_PRICE = 0.50
MIN_VOLUME_24H = 50_000  # Reduced for faster filtering
MAX_ANALYSIS_PAIRS = 15  # Analyze only top N pairs per scan

# Detection parameters
VOLUME_SPIKE_THRESHOLD = 3.5
VOLUME_CLIMAX_RATIO = 2.8
RSI_OVERBOUGHT = 75
RSI_OVERSOLD = 25

# Trade parameters
MIN_CONFLUENCE_SCORE = 2.5
MIN_RISK_REWARD = 2.5  # Slightly reduced for more signals

# Timeframes (reduced for speed)
TIMEFRAMES = {
    "4H": "4h",
    "1H": "1h",
    "15M": "15m"
}

# Rate limiting
MAX_CONCURRENT_ANALYSIS = 3  # Parallel analysis tasks
REQUEST_DELAY = 0.2  # Delay between API requests

# ================ LOGGING SETUP ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("mexc_sniper")

# ================ OPTIMIZED UTILITIES ================
def calculate_rsi_fast(close_prices: pd.Series, period: int = 14) -> float:
    """Fast RSI calculation for last value only"""
    if len(close_prices) < period + 1:
        return 50.0
    
    prices = close_prices.iloc[-period-1:]
    deltas = prices.diff()
    
    gain = deltas.where(deltas > 0, 0)
    loss = -deltas.where(deltas < 0, 0)
    
    avg_gain = gain.mean()
    avg_loss = loss.mean()
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi)

def calculate_ema_fast(prices: pd.Series, period: int) -> float:
    """Fast EMA calculation for last value"""
    if len(prices) < period:
        return float(prices.iloc[-1])
    
    alpha = 2 / (period + 1)
    ema = prices.iloc[0]
    
    for price in prices.iloc[1:]:
        ema = alpha * price + (1 - alpha) * ema
    
    return float(ema)

# ================ SIMPLIFIED LOGIC CLASSES ================
@dataclass
class QuickSignal:
    """Optimized signal structure"""
    signal_id: str
    symbol: str
    signal_type: str  # pump_long/dump_short
    timestamp: float
    current_price: float
    volume_24h: float
    
    # Quick scores (0-10)
    accumulation_score: float = 0.0
    volume_score: float = 0.0
    momentum_score: float = 0.0
    overall_score: float = 0.0
    
    # Trade parameters
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    risk_reward: float = 0.0
    
    # Key metrics
    rsi: float = 50.0
    volume_spike: float = 1.0
    price_range_pct: float = 0.0
    days_accumulating: float = 0.0
    
    # Conditions
    entry_conditions: List[str] = None
    timeframes: List[str] = None
    
    def __post_init__(self):
        if self.entry_conditions is None:
            self.entry_conditions = []
        if self.timeframes is None:
            self.timeframes = []

# ================ FAST DETECTION ENGINE ================
class FastDetector:
    """Optimized detector for quick analysis"""
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
    
    async def analyze_pair(self, exchange, symbol: str) -> Optional[QuickSignal]:
        """Fast analysis of a single pair"""
        try:
            # Get ticker for quick filtering
            ticker = await exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            volume_24h = ticker['quoteVolume']
            
            # Quick filter
            if not (MIN_PRICE <= current_price <= MAX_PRICE):
                return None
            
            if volume_24h < MIN_VOLUME_24H:
                return None
            
            # Fetch minimal data
            data_4h = await self._fetch_cached_ohlcv(exchange, symbol, "4h", 50)
            data_1h = await self._fetch_cached_ohlcv(exchange, symbol, "1h", 24)
            
            if data_4h is None or data_1h is None:
                return None
            
            # Analyze for pump
            pump_signal = await self._analyze_pump(data_4h, data_1h, symbol, current_price, volume_24h)
            
            # Analyze for dump
            dump_signal = await self._analyze_dump(data_4h, data_1h, symbol, current_price, volume_24h)
            
            # Return best signal
            if pump_signal and dump_signal:
                return pump_signal if pump_signal.overall_score >= dump_signal.overall_score else dump_signal
            elif pump_signal:
                return pump_signal
            elif dump_signal:
                return dump_signal
            
            return None
            
        except Exception as e:
            log.debug(f"Analysis error for {symbol}: {str(e)[:50]}")
            return None
    
    async def _fetch_cached_ohlcv(self, exchange, symbol: str, timeframe: str, limit: int):
        """Fetch with simple caching"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return data
        
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            
            if ohlcv and len(ohlcv) >= 20:
                df = pd.DataFrame(
                    ohlcv,
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                # Convert to numeric efficiently
                df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors='coerce')
                df = df.dropna()
                
                if len(df) >= 20:
                    self.cache[cache_key] = (df, time.time())
                    return df
            
            return None
            
        except Exception as e:
            log.debug(f"Fetch error {symbol} {timeframe}: {e}")
            return None
    
    async def _analyze_pump(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame, 
                          symbol: str, current_price: float, volume_24h: float) -> Optional[QuickSignal]:
        """Fast pump analysis"""
        try:
            # 1. Check accumulation
            accumulation_score = self._check_accumulation(df_4h)
            if accumulation_score < 0.6:
                return None
            
            # 2. Check volume spike
            volume_score = self._check_volume_spike(df_1h)
            if volume_score < 0.5:
                return None
            
            # 3. Check momentum
            momentum_score = self._check_momentum(df_1h, "pump")
            if momentum_score < 0.4:
                return None
            
            # 4. Find breakout level
            resistance = df_4h['high'].iloc[-20:].max()
            entry_price = resistance * 1.005
            
            # Check if we're near breakout
            if current_price < resistance * 0.995:
                return None
            
            # 5. Calculate risk/reward
            stop_loss = resistance * 0.97
            take_profit = entry_price * 1.12  # 12% target
            
            risk_pct = (entry_price - stop_loss) / entry_price * 100
            reward_pct = (take_profit - entry_price) / entry_price * 100
            risk_reward = reward_pct / risk_pct if risk_pct > 0 else 0
            
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            # 6. Calculate overall score
            overall_score = (
                accumulation_score * 0.4 +
                volume_score * 0.4 +
                momentum_score * 0.2
            ) * 10
            
            if overall_score < MIN_CONFLUENCE_SCORE:
                return None
            
            # 7. Create signal
            signal = QuickSignal(
                signal_id=hashlib.md5(f"{symbol}_pump_{time.time()}".encode()).hexdigest(),
                symbol=symbol,
                signal_type="pump_long",
                timestamp=time.time(),
                current_price=current_price,
                volume_24h=volume_24h,
                accumulation_score=accumulation_score * 10,
                volume_score=volume_score * 10,
                momentum_score=momentum_score * 10,
                overall_score=overall_score,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                risk_reward=risk_reward,
                rsi=self._calculate_rsi(df_1h),
                volume_spike=self._calculate_volume_spike(df_1h),
                price_range_pct=self._calculate_price_range(df_4h),
                days_accumulating=self._estimate_accumulation_days(df_4h),
                entry_conditions=[
                    f"Break above {resistance:.8f}",
                    f"Volume spike >{VOLUME_SPIKE_THRESHOLD:.1f}x",
                    f"RSI < 65"
                ],
                timeframes=["4H", "1H"]
            )
            
            return signal
            
        except Exception as e:
            log.debug(f"Pump analysis error: {e}")
            return None
    
    async def _analyze_dump(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame,
                          symbol: str, current_price: float, volume_24h: float) -> Optional[QuickSignal]:
        """Fast dump analysis"""
        try:
            # 1. Check for parabolic move
            recent_high = df_4h['high'].iloc[-10:].max()
            recent_low = df_4h['low'].iloc[-10:].min()
            move_pct = (recent_high - recent_low) / recent_low * 100
            
            if move_pct < 20:  # Not parabolic enough
                return None
            
            # 2. Check volume climax
            volume_climax = self._check_volume_climax(df_1h)
            if not volume_climax:
                return None
            
            # 3. Check RSI overbought
            rsi = self._calculate_rsi(df_1h)
            if rsi < RSI_OVERBOUGHT:
                return None
            
            # 4. Find breakdown level
            support = df_4h['low'].iloc[-20:].min()
            entry_price = support * 0.995
            
            # Check if we're near breakdown
            if current_price > support * 1.005:
                return None
            
            # 5. Calculate risk/reward
            stop_loss = recent_high * 1.03
            take_profit = entry_price * 0.92  # 8% down
            
            risk_pct = (stop_loss - current_price) / current_price * 100
            reward_pct = (current_price - take_profit) / current_price * 100
            risk_reward = reward_pct / risk_pct if risk_pct > 0 else 0
            
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            # 6. Calculate scores
            volume_score = 0.8 if volume_climax else 0.3
            momentum_score = 0.7 if rsi > 75 else 0.4
            
            overall_score = (
                volume_score * 0.5 +
                momentum_score * 0.3 +
                (move_pct / 100) * 0.2  # Parabolic component
            ) * 10
            
            if overall_score < MIN_CONFLUENCE_SCORE:
                return None
            
            # 7. Create signal
            signal = QuickSignal(
                signal_id=hashlib.md5(f"{symbol}_dump_{time.time()}".encode()).hexdigest(),
                symbol=symbol,
                signal_type="dump_short",
                timestamp=time.time(),
                current_price=current_price,
                volume_24h=volume_24h,
                volume_score=volume_score * 10,
                momentum_score=momentum_score * 10,
                overall_score=overall_score,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                risk_reward=risk_reward,
                rsi=rsi,
                volume_spike=self._calculate_volume_spike(df_1h),
                price_range_pct=move_pct,
                entry_conditions=[
                    f"Break below {support:.8f}",
                    f"Volume climax detected",
                    f"RSI > {RSI_OVERBOUGHT}"
                ],
                timeframes=["4H", "1H"]
            )
            
            return signal
            
        except Exception as e:
            log.debug(f"Dump analysis error: {e}")
            return None
    
    def _check_accumulation(self, df: pd.DataFrame) -> float:
        """Check for accumulation patterns"""
        if len(df) < 30:
            return 0.0
        
        # Price compression
        recent_high = df['high'].iloc[-20:].max()
        recent_low = df['low'].iloc[-20:].min()
        price_range = (recent_high - recent_low) / recent_low * 100
        
        if price_range < 15:
            compression_score = 0.8
        elif price_range < 25:
            compression_score = 0.6
        else:
            compression_score = 0.3
        
        # Volume during compression
        recent_volume = df['volume'].iloc[-10:].mean()
        older_volume = df['volume'].iloc[-20:-10].mean()
        
        if older_volume > 0:
            volume_ratio = recent_volume / older_volume
            if volume_ratio > 1.5:
                volume_score = 0.9
            elif volume_ratio > 1.2:
                volume_score = 0.7
            else:
                volume_score = 0.4
        else:
            volume_score = 0.5
        
        # Count accumulation candles
        acc_candles = 0
        for i in range(min(20, len(df))):
            idx = -i - 1
            if recent_low <= df.iloc[idx]['close'] <= recent_high:
                acc_candles += 1
            else:
                break
        
        time_score = min(acc_candles / 10, 1.0)
        
        return (compression_score * 0.4 + volume_score * 0.4 + time_score * 0.2)
    
    def _check_volume_spike(self, df: pd.DataFrame) -> float:
        """Check for volume spikes"""
        if len(df) < 10:
            return 0.0
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].iloc[-10:].mean()
        
        if avg_volume > 0:
            ratio = current_volume / avg_volume
            if ratio >= VOLUME_SPIKE_THRESHOLD:
                return 0.9
            elif ratio >= VOLUME_SPIKE_THRESHOLD * 0.7:
                return 0.6
            else:
                return 0.3
        
        return 0.0
    
    def _check_volume_climax(self, df: pd.DataFrame) -> bool:
        """Check for volume climax"""
        if len(df) < 10:
            return False
        
        recent_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].iloc[-10:].mean()
        
        return recent_volume > avg_volume * VOLUME_CLIMAX_RATIO
    
    def _check_momentum(self, df: pd.DataFrame, signal_type: str) -> float:
        """Check momentum conditions"""
        if len(df) < 14:
            return 0.5
        
        rsi = self._calculate_rsi(df)
        
        if signal_type == "pump":
            if rsi < 40:
                return 0.9  # Oversold bounce potential
            elif rsi < 60:
                return 0.7  # Good for pump
            else:
                return 0.3  # Overbought
        else:  # dump
            if rsi > 70:
                return 0.9  # Overbought reversal potential
            elif rsi > 50:
                return 0.7  # Good for dump
            else:
                return 0.3  # Oversold
        
        return 0.5
    
    def _calculate_rsi(self, df: pd.DataFrame) -> float:
        """Calculate RSI"""
        return calculate_rsi_fast(df['close'], period=14)
    
    def _calculate_volume_spike(self, df: pd.DataFrame) -> float:
        """Calculate current volume spike ratio"""
        if len(df) < 10:
            return 1.0
        
        current = df['volume'].iloc[-1]
        average = df['volume'].iloc[-10:].mean()
        
        return current / average if average > 0 else 1.0
    
    def _calculate_price_range(self, df: pd.DataFrame) -> float:
        """Calculate recent price range percentage"""
        if len(df) < 20:
            return 0.0
        
        high = df['high'].iloc[-20:].max()
        low = df['low'].iloc[-20:].min()
        
        if low > 0:
            return (high - low) / low * 100
        return 0.0
    
    def _estimate_accumulation_days(self, df: pd.DataFrame) -> float:
        """Estimate days in accumulation"""
        if len(df) < 10:
            return 0.0
        
        recent_high = df['high'].iloc[-20:].max()
        recent_low = df['low'].iloc[-20:].min()
        
        count = 0
        for i in range(min(30, len(df))):
            idx = -i - 1
            if recent_low <= df.iloc[idx]['close'] <= recent_high:
                count += 1
            else:
                break
        
        return (count * 4) / 24  # Convert 4h candles to days

# ================ FAST TELEGRAM FORMATTER ================
class FastFormatter:
    """Optimized formatter for Telegram"""
    
    @staticmethod
    def format_signal(signal: QuickSignal) -> str:
        """Format signal quickly"""
        
        if signal.signal_type == "pump_long":
            main_emoji = "🚀"
            side_emoji = "🟢"
        else:
            main_emoji = "💥"
            side_emoji = "🔴"
        
        # Quality emojis
        if signal.overall_score >= 9.0:
            quality_emoji = "🔥🔥🔥"
        elif signal.overall_score >= 8.0:
            quality_emoji = "🔥🔥"
        elif signal.overall_score >= 7.5:
            quality_emoji = "🔥"
        else:
            quality_emoji = "⚠️"
        
        # Build message efficiently
        message = f"""{main_emoji} <b>{side_emoji} {signal.signal_type.upper().replace('_', ' ')}</b> {quality_emoji}

<b>{signal.symbol}</b> | Score: <b>{signal.overall_score:.1f}/10</b>
Time: {datetime.fromtimestamp(signal.timestamp).strftime('%H:%M:%S')}

<b>📊 METRICS:</b>
• Price: {signal.current_price:.8f}
• 24h Volume: ${signal.volume_24h:,.0f}
• RSI: {signal.rsi:.1f}
• Volume Spike: {signal.volume_spike:.1f}x
• Price Range: {signal.price_range_pct:.1f}%
• Accumulation: {signal.days_accumulating:.1f} days

<b>🎯 TRADE SETUP:</b>
• Entry: {signal.entry_price:.8f}
• Stop Loss: {signal.stop_loss:.8f}
• Take Profit: {signal.take_profit:.8f}
• R:R Ratio: {signal.risk_reward:.1f}:1

<b>📈 SCORES:</b>
• Accumulation: {signal.accumulation_score:.1f}/10
• Volume: {signal.volume_score:.1f}/10
• Momentum: {signal.momentum_score:.1f}/10

<b>📋 CONDITIONS:</b>
{chr(10).join([f'• {cond}' for cond in signal.entry_conditions])}

<b>🏷️ TAGS:</b>
#{signal.symbol.replace('/', '').replace(':', '')} 
#{signal.signal_type.split('_')[0].upper()} #{signal.signal_type.split('_')[1].upper()}
#Score{int(signal.overall_score)} #MEXCFutures #FastScanner
"""
        
        return message

# ================ OPTIMIZED DATABASE ================
class FastDatabase:
    """Minimal database for speed"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = None
    
    async def initialize(self):
        """Initialize minimal database"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.db = await aiosqlite.connect(self.db_path)
            
            # Single table for speed
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS fast_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT,
                signal_type TEXT,
                timestamp REAL,
                score REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            await self.db.commit()
            log.info("✅ Fast database ready")
            
        except Exception as e:
            log.error(f"Database error: {e}")
    
    async def save_signal(self, signal: QuickSignal):
        """Save signal quickly"""
        try:
            await self.db.execute("""
            INSERT INTO fast_signals (id, symbol, signal_type, timestamp, score)
            VALUES (?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.signal_type,
                signal.timestamp,
                signal.overall_score
            ))
            
            await self.db.commit()
            
        except Exception as e:
            log.debug(f"Save error: {e}")
    
    async def close(self):
        """Close database"""
        if self.db:
            await self.db.close()

# ================ FAST TELEGRAM BOT ================
class FastTelegram:
    """Optimized Telegram sender"""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.client = None
        self.message_queue = asyncio.Queue()
        self.sending = False
    
    async def initialize(self):
        """Initialize HTTP client"""
        self.client = httpx.AsyncClient(timeout=10.0)
        self.sending = True
        # Start background sender
        asyncio.create_task(self._process_queue())
    
    async def send(self, message: str):
        """Queue message for sending"""
        await self.message_queue.put(message)
    
    async def _process_queue(self):
        """Process message queue"""
        while self.sending:
            try:
                message = await self.message_queue.get()
                
                if self.client and self.token and self.chat_id:
                    url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                    payload = {
                        "chat_id": self.chat_id,
                        "text": message,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True
                    }
                    
                    try:
                        await self.client.post(url, json=payload)
                        log.info("📤 Message sent")
                    except Exception as e:
                        log.error(f"Telegram send error: {e}")
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)
                
            except Exception as e:
                log.error(f"Queue error: {e}")
                await asyncio.sleep(1)
    
    async def close(self):
        """Close Telegram client"""
        self.sending = False
        if self.client:
            await self.client.aclose()

# ================ FAST SCANNER BOT ================
class FastScannerBot:
    """Optimized scanner bot"""
    
    def __init__(self):
        self.exchange = None
        self.detector = FastDetector()
        self.formatter = FastFormatter()
        self.database = FastDatabase(DB_PATH)
        self.telegram = None
        self.scanning = False
        self.last_scan_time = 0
        self.scan_count = 0
        
        # Statistics
        self.stats = {
            "total_pairs": 0,
            "pump_signals": 0,
            "dump_signals": 0,
            "scan_time": 0.0
        }
    
    async def initialize(self):
        """Initialize bot quickly"""
        log.info("=" * 60)
        log.info("⚡ FAST MEXC PUMP/DUMP SCANNER v2.0")
        log.info("=" * 60)
        
        # Initialize exchange
        self.exchange = ccxt.mexc({
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
            "timeout": 15000,  # Shorter timeout
        })
        
        # Test connection
        try:
            markets = await self.exchange.fetch_markets()
            log.info(f"✅ Connected to MEXC: {len(markets)} markets")
        except Exception as e:
            log.error(f"❌ Connection failed: {e}")
            return False
        
        # Initialize database
        await self.database.initialize()
        
        # Initialize Telegram if configured
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            self.telegram = FastTelegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
            await self.telegram.initialize()
            await self._send_startup()
        
        log.info("✅ Scanner ready")
        return True
    
    async def cleanup(self):
        """Cleanup resources"""
        log.info("🛑 Cleaning up...")
        
        # Close exchange
        if self.exchange:
            try:
                await self.exchange.close()
            except:
                pass
        
        # Close database
        await self.database.close()
        
        # Close Telegram
        if self.telegram:
            await self.telegram.close()
        
        log.info("✅ Cleanup complete")
    
    async def _send_startup(self):
        """Send startup message"""
        if not self.telegram:
            return
        
        message = """⚡ <b>FAST SCANNER - ONLINE</b>

<b>🎯 TARGET:</b> MEXC Futures (Low-cap focus)
<b>⚡ SPEED:</b> Optimized parallel scanning
<b>📊 STRATEGY:</b> Pump & dump detection
<b>🛡️ RISK:</b> {MIN_RISK_REWARD}:1 minimum

Scanning started...

#FastScanner #MEXC #Online"""
        
        await self.telegram.send(message)
    
    async def _get_top_pairs(self, count: int = MAX_ANALYSIS_PAIRS) -> List[str]:
        """Get top pairs for analysis (fast filtering)"""
        try:
            markets = await self.exchange.fetch_markets()
            pairs_data = []
            
            for market in markets:
                symbol = market['symbol']
                
                # Filter for USDT pairs only
                if '/USDT' in symbol:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        price = ticker['last']
                        volume = ticker['quoteVolume']
                        
                        # Quick filters
                        if (MIN_PRICE <= price <= MAX_PRICE and
                            volume >= MIN_VOLUME_24H):
                            pairs_data.append((symbol, volume))
                            
                        # Rate limiting
                        await asyncio.sleep(REQUEST_DELAY)
                            
                    except Exception as e:
                        log.debug(f"Ticker error for {symbol}: {e}")
                        continue
            
            # Sort by volume (descending) and take top N
            pairs_data.sort(key=lambda x: x[1], reverse=True)
            top_pairs = [pair[0] for pair in pairs_data[:count]]
            
            log.info(f"📊 Selected {len(top_pairs)} pairs for analysis")
            return top_pairs
            
        except Exception as e:
            log.error(f"Error getting pairs: {e}")
            return []
    
    async def scan_once(self):
        """Perform a single scan"""
        scan_start = time.time()
        self.scan_count += 1
        
        log.info(f"🔄 Scan #{self.scan_count} starting...")
        
        try:
            # Get pairs to analyze
            pairs = await self._get_top_pairs(MAX_ANALYSIS_PAIRS)
            self.stats["total_pairs"] += len(pairs)
            
            if not pairs:
                log.warning("No pairs to analyze")
                return
            
            # Analyze pairs in parallel with semaphore
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_ANALYSIS)
            tasks = []
            
            for symbol in pairs:
                task = asyncio.create_task(
                    self._analyze_with_semaphore(semaphore, symbol)
                )
                tasks.append(task)
            
            # Wait for all analysis to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count signals
            pump_count = 0
            dump_count = 0
            
            for result in results:
                if isinstance(result, QuickSignal):
                    if result.signal_type == "pump_long":
                        pump_count += 1
                    else:
                        dump_count += 1
            
            self.stats["pump_signals"] += pump_count
            self.stats["dump_signals"] += dump_count
            
            scan_time = time.time() - scan_start
            self.stats["scan_time"] = scan_time
            
            log.info(f"✅ Scan #{self.scan_count} complete in {scan_time:.1f}s")
            log.info(f"   Signals: {pump_count} pumps, {dump_count} dumps")
            log.info(f"   Total: {self.stats['pump_signals']} pumps, {self.stats['dump_signals']} dumps")
            
        except Exception as e:
            log.error(f"Scan error: {e}")
    
    async def _analyze_with_semaphore(self, semaphore: asyncio.Semaphore, symbol: str):
        """Analyze a single symbol with semaphore"""
        async with semaphore:
            try:
                signal = await self.detector.analyze_pair(self.exchange, symbol)
                
                if signal:
                    # Save to database
                    await self.database.save_signal(signal)
                    
                    # Send to Telegram
                    if self.telegram:
                        message = self.formatter.format_signal(signal)
                        await self.telegram.send(message)
                    
                    log.info(f"🎯 Signal found: {signal.symbol} ({signal.signal_type}) - Score: {signal.overall_score:.1f}")
                    return signal
                
                return None
                
            except Exception as e:
                log.debug(f"Analysis error for {symbol}: {e}")
                return None
    
    async def run_continuous(self, scan_interval: int = 45):
        """Run continuous scanning"""
        self.scanning = True
        
        try:
            while self.scanning:
                await self.scan_once()
                
                if self.scanning:
                    log.info(f"⏳ Next scan in {scan_interval}s...")
                    await asyncio.sleep(scan_interval)
                    
        except KeyboardInterrupt:
            log.info("⏹️ Scanning stopped by user")
        except Exception as e:
            log.error(f"❌ Scanner crashed: {e}")
        finally:
            self.scanning = False

# ================ MAIN ================
async def main():
    """Main function"""
    bot = FastScannerBot()
    
    try:
        # Initialize
        if not await bot.initialize():
            log.error("❌ Initialization failed")
            return
        
        # Run scanning
        await bot.run_continuous(scan_interval=45)
        
    except KeyboardInterrupt:
        log.info("🛑 Bot stopped")
    except Exception as e:
        log.error(f"❌ Bot error: {e}")
        log.error(traceback.format_exc())
    finally:
        # Always cleanup
        await bot.cleanup()

if __name__ == "__main__":
    # Run with high performance
    asyncio.run(main())