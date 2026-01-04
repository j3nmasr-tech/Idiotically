#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🎯 TRADER'S FRAMEWORK - SIMPLIFIED
1. Direction filter = scanner (7 tools)
2. Entry = price behavior (3 types)
3. Exit = momentum failure
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
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import json

# ================ CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/trader_framework.db"

EXCHANGE = "okx"
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 30))
MIN_VOLUME_USD = 1000000

# Risk Management
MAX_POSITIONS = 5
MAX_STOP_LOSS_PCT = 1.5
MIN_TARGET_PCT = 2.0
MIN_RISK_REWARD = 2.0

# Timeframes
TIMEFRAMES = {
    "1H": "1h",    # Context
    "15M": "15m",  # Strength  
    "5M": "5m",    # Entry
    "3M": "3m"     # Trigger
}

# ================ LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("trader_framework")

# ================ SAFE DATA ================
def is_valid_df(df) -> bool:
    """Safe DataFrame validation"""
    if df is None:
        return False
    if not isinstance(df, pd.DataFrame):
        return False
    if df.empty:
        return False
    if len(df) < 20:
        return False
    required = ['open', 'high', 'low', 'close', 'volume']
    for col in required:
        if col not in df.columns:
            return False
    return True

# ================ SIMPLE INDICATORS ================
class SimpleIndicators:
    
    @staticmethod
    def EMA(prices: pd.Series, period: int) -> pd.Series:
        return prices.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def RSI(prices: pd.Series, period: int = 14) -> pd.Series:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def ATR(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

# ================ DATA STRUCTURES ================
@dataclass
class DirectionSignal:
    """Output of the 7-tool scanner"""
    symbol: str
    direction: str  # "LONG" or "SHORT"
    strength: float  # 0-1
    timestamp: float
    
@dataclass
class EntrySignal:
    """Price behavior entry signal"""
    symbol: str
    direction: str
    entry_type: str  # "PULLBACK", "BREAKOUT", "STOPHUNT"
    entry_price: float
    timestamp: float
    
@dataclass
class ExitSignal:
    """Momentum failure exit signal"""
    symbol: str
    direction: str
    exit_price: float
    exit_reason: str  # "MOMENTUM_FAILURE"
    pnl_percent: float
    timestamp: float

# ================ 1. DIRECTION FILTER (SCANNER) ================
class DirectionScanner:
    """7-tool direction filter - ALL must agree"""
    
    def __init__(self):
        self.scan_count = 0
        self.directions_found = 0
    
    def scan_direction(self, symbol: str, multi_tf_data: Dict[str, pd.DataFrame]) -> Optional[DirectionSignal]:
        """Scan for direction using 7 tools"""
        self.scan_count += 1
        
        # Tool 1: Multi-Timeframe Agreement
        mtf_ok = self._check_mtf_agreement(multi_tf_data)
        if not mtf_ok[0]:
            return None
        direction = mtf_ok[1]
        
        # Primary timeframe for other tools
        primary_df = multi_tf_data.get("15M")
        if not is_valid_df(primary_df):
            return None
        
        # Tools 2-7
        tools = [
            ("Wave Length", self._check_wave_length(primary_df)),
            ("Momentum Strength", self._check_momentum_strength(primary_df)),
            ("Volume Participation", self._check_volume_participation(primary_df)),
            ("RSI Regime", self._check_rsi_regime(primary_df, direction)),
            ("EMA Structure", self._check_ema_structure(primary_df, direction)),
            ("Volatility Tradability", self._check_volatility_tradability(primary_df))
        ]
        
        # Check if ALL tools agree
        failed_tools = [name for name, result in tools if not result]
        
        if failed_tools:
            log.debug(f"{symbol}: Tools failed - {failed_tools}")
            return None
        
        # All tools agree - direction confirmed
        self.directions_found += 1
        strength = 0.9  # High confidence when all agree
        
        log.info(f"🎯 DIRECTION FOUND: {symbol} {direction} (Strength: {strength:.1%})")
        
        return DirectionSignal(
            symbol=symbol,
            direction=direction,
            strength=strength,
            timestamp=time.time()
        )
    
    def _check_mtf_agreement(self, multi_tf_data: Dict[str, pd.DataFrame]) -> Tuple[bool, str]:
        """Tool 1: Multi-Timeframe Agreement"""
        directions = []
        
        for tf_name in ["1H", "15M", "5M"]:
            df = multi_tf_data.get(tf_name)
            if not is_valid_df(df):
                continue
            
            try:
                ema20 = SimpleIndicators.EMA(df['close'], 20).iloc[-1]
                ema50 = SimpleIndicators.EMA(df['close'], 50).iloc[-1]
                rsi = SimpleIndicators.RSI(df['close'], 14).iloc[-1]
                
                if np.isnan(ema20) or np.isnan(ema50) or np.isnan(rsi):
                    continue
                
                if ema20 > ema50 and rsi > 50:
                    directions.append("LONG")
                elif ema20 < ema50 and rsi < 50:
                    directions.append("SHORT")
                    
            except Exception:
                continue
        
        if len(directions) >= 2:
            if all(d == "LONG" for d in directions):
                return True, "LONG"
            elif all(d == "SHORT" for d in directions):
                return True, "SHORT"
        
        return False, "NEUTRAL"
    
    def _check_wave_length(self, df: pd.DataFrame) -> bool:
        """Tool 2: Wave Length"""
        try:
            if len(df) < 20:
                return False
            
            # Impulse move (last 5 candles)
            impulse = df['high'].iloc[-1] - df['low'].iloc[-5]
            # Pullback (previous 5 candles)
            pullback = abs(df['high'].iloc[-5] - df['low'].iloc[-10])
            
            if pullback > 0:
                return impulse > pullback * 1.5
            return impulse > 0
            
        except Exception:
            return False
    
    def _check_momentum_strength(self, df: pd.DataFrame) -> bool:
        """Tool 3: Momentum Strength"""
        try:
            current = df.iloc[-1]
            body = abs(current['close'] - current['open'])
            
            atr = SimpleIndicators.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
            rsi = SimpleIndicators.RSI(df['close'], 14).iloc[-1]
            
            if atr <= 0 or np.isnan(atr) or np.isnan(rsi):
                return False
            
            body_strength = body / atr
            return body_strength > 0.7 and rsi > 55
            
        except Exception:
            return False
    
    def _check_volume_participation(self, df: pd.DataFrame) -> bool:
        """Tool 4: Volume Participation"""
        try:
            recent_volume = df['volume'].iloc[-1]
            avg_volume = df['volume'].iloc[-20:].mean()
            
            if avg_volume > 0:
                return recent_volume > avg_volume * 1.2
            return recent_volume > 0
            
        except Exception:
            return False
    
    def _check_rsi_regime(self, df: pd.DataFrame, direction: str) -> bool:
        """Tool 5: RSI Regime"""
        try:
            rsi = SimpleIndicators.RSI(df['close'], 14).iloc[-1]
            
            if np.isnan(rsi):
                return False
            
            if direction == "LONG":
                return rsi > 50
            else:  # SHORT
                return rsi < 50
                
        except Exception:
            return False
    
    def _check_ema_structure(self, df: pd.DataFrame, direction: str) -> bool:
        """Tool 6: EMA Structure"""
        try:
            ema20 = SimpleIndicators.EMA(df['close'], 20).iloc[-1]
            ema50 = SimpleIndicators.EMA(df['close'], 50).iloc[-1]
            
            if np.isnan(ema20) or np.isnan(ema50):
                return False
            
            if direction == "LONG":
                return ema20 > ema50
            else:  # SHORT
                return ema20 < ema50
                
        except Exception:
            return False
    
    def _check_volatility_tradability(self, df: pd.DataFrame) -> bool:
        """Tool 7: Volatility Tradability"""
        try:
            atr = SimpleIndicators.ATR(df['high'], df['low'], df['close'], 14)
            current_atr = atr.iloc[-1]
            avg_atr = atr.iloc[-20:].mean()
            
            if np.isnan(current_atr) or np.isnan(avg_atr) or avg_atr == 0:
                return False
            
            return current_atr > avg_atr * 0.7
            
        except Exception:
            return False

# ================ 2. ENTRY ENGINE (PRICE BEHAVIOR) ================
class PriceEntryEngine:
    """Entry based on pure price behavior - NO indicators"""
    
    def find_entry(self, df: pd.DataFrame, direction: DirectionSignal) -> Optional[EntrySignal]:
        """Find entry based on price behavior"""
        if not is_valid_df(df):
            return None
        
        log.info(f"🔍 Looking for {direction.direction} entry on {direction.symbol}")
        
        # Try different entry types
        entry_types = [
            ("PULLBACK", self._check_pullback_entry),
            ("BREAKOUT", self._check_breakout_entry),
            ("STOPHUNT", self._check_stophunt_entry)
        ]
        
        for entry_type, check_func in entry_types:
            entry_price = check_func(df, direction.direction)
            if entry_price > 0:
                log.info(f"✅ ENTRY: {direction.symbol} {direction.direction} @ {entry_price:.4f}")
                log.info(f"   Type: {entry_type}")
                
                return EntrySignal(
                    symbol=direction.symbol,
                    direction=direction.direction,
                    entry_type=entry_type,
                    entry_price=entry_price,
                    timestamp=time.time()
                )
        
        return None
    
    def _check_pullback_entry(self, df: pd.DataFrame, direction: str) -> float:
        """Pullback entry after move"""
        try:
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            if direction == "LONG":
                # Bullish candle after bearish candles
                if (prev['close'] < prev['open'] and  # Bearish
                    current['close'] > current['open'] and  # Bullish
                    current['close'] > (prev['high'] + prev['low']) / 2):  # Above midpoint
                    return current['close']
            else:  # SHORT
                # Bearish candle after bullish candles
                if (prev['close'] > prev['open'] and  # Bullish
                    current['close'] < current['open'] and  # Bearish
                    current['close'] < (prev['high'] + prev['low']) / 2):  # Below midpoint
                    return current['close']
            
            return 0.0
        except Exception:
            return 0.0
    
    def _check_breakout_entry(self, df: pd.DataFrame, direction: str) -> float:
        """Breakout entry from compression"""
        try:
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            if direction == "LONG":
                # Break above previous high with volume
                if (current['close'] > prev['high'] and
                    current['volume'] > df['volume'].iloc[-5:-1].mean() * 1.2):
                    return current['close']
            else:  # SHORT
                # Break below previous low with volume
                if (current['close'] < prev['low'] and
                    current['volume'] > df['volume'].iloc[-5:-1].mean() * 1.2):
                    return current['close']
            
            return 0.0
        except Exception:
            return 0.0
    
    def _check_stophunt_entry(self, df: pd.DataFrame, direction: str) -> float:
        """Re-entry after stop hunt"""
        try:
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            if direction == "LONG":
                # Bearish wick rejected, bullish recovery
                if (prev['low'] < min(prev['open'], prev['close']) and  # Bearish wick
                    current['close'] > prev['close'] and  # Recovery
                    current['close'] > (prev['open'] + prev['close']) / 2):  # Above midpoint
                    return current['close']
            else:  # SHORT
                # Bullish wick rejected, bearish recovery
                if (prev['high'] > max(prev['open'], prev['close']) and  # Bullish wick
                    current['close'] < prev['close'] and  # Recovery
                    current['close'] < (prev['open'] + prev['close']) / 2):  # Below midpoint
                    return current['close']
            
            return 0.0
        except Exception:
            return 0.0

# ================ 3. EXIT ENGINE (MOMENTUM FAILURE) ================
class MomentumExitEngine:
    """Exit based on momentum failure"""
    
    def check_exit(self, df: pd.DataFrame, position: EntrySignal) -> Optional[ExitSignal]:
        """Check for exit signals"""
        if not is_valid_df(df):
            return None
        
        current_price = df['close'].iloc[-1]
        
        # Check momentum failure
        momentum_failed = self._check_momentum_failure(df, position.direction)
        if momentum_failed:
            return self._create_exit_signal(position, current_price)
        
        return None
    
    def _check_momentum_failure(self, df: pd.DataFrame, direction: str) -> bool:
        """Check for momentum failure"""
        try:
            # Simple momentum check using recent candles
            recent = df.iloc[-3:]  # Last 3 candles
            
            if direction == "LONG":
                # Momentum failure: consecutive bearish candles
                bearish_candles = sum(1 for _, candle in recent.iterrows() 
                                    if candle['close'] < candle['open'])
                return bearish_candles >= 2
            else:  # SHORT
                # Momentum failure: consecutive bullish candles
                bullish_candles = sum(1 for _, candle in recent.iterrows() 
                                    if candle['close'] > candle['open'])
                return bullish_candles >= 2
                
        except Exception:
            return False
    
    def _create_exit_signal(self, position: EntrySignal, exit_price: float) -> ExitSignal:
        """Create exit signal"""
        # Calculate P&L
        if position.direction == "LONG":
            pnl_percent = ((exit_price - position.entry_price) / position.entry_price) * 100
        else:
            pnl_percent = ((position.entry_price - exit_price) / position.entry_price) * 100
        
        log.info(f"📤 EXIT: {position.symbol} {position.direction} @ {exit_price:.4f}")
        log.info(f"   Reason: MOMENTUM_FAILURE, P&L: {pnl_percent:+.2f}%")
        
        return ExitSignal(
            symbol=position.symbol,
            direction=position.direction,
            exit_price=exit_price,
            exit_reason="MOMENTUM_FAILURE",
            pnl_percent=pnl_percent,
            timestamp=time.time()
        )

# ================ MAIN TRADER SYSTEM ================
class TraderFramework:
    """Main system: Scanner → Entry → Exit"""
    
    def __init__(self):
        self.scanner = DirectionScanner()
        self.entry_engine = PriceEntryEngine()
        self.exit_engine = MomentumExitEngine()
        self.exchange = None
        self.db = None
        self.active_positions = {}  # symbol: EntrySignal
    
    async def initialize(self):
        """Initialize the framework"""
        log.info("=" * 70)
        log.info("🎯 TRADER'S FRAMEWORK")
        log.info("=" * 70)
        log.info("1. Direction filter = scanner (7 tools)")
        log.info("2. Entry = price behavior (3 types)")
        log.info("3. Exit = momentum failure")
        log.info("=" * 70)
        
        await self._init_database()
        await self._init_exchange()
    
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL,
                exit_reason TEXT,
                pnl_percent REAL,
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                exited_at TIMESTAMP
            )
            """)
            
            await self.db.commit()
            log.info("✅ Database initialized")
            
        except Exception as e:
            log.error(f"Database error: {e}")
            raise
    
    async def _init_exchange(self):
        """Initialize exchange"""
        try:
            self.exchange = getattr(ccxt, EXCHANGE)({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 20000
            })
            
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            raise
    
    async def fetch_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch multi-timeframe data"""
        data = {}
        
        for tf_name, tf in TIMEFRAMES.items():
            try:
                limit = 100 if tf_name == "1H" else 50
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                
                if ohlcv and len(ohlcv) >= 20:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = df.dropna()
                    
                    if len(df) >= 15:
                        data[tf_name] = df
                        
            except Exception:
                continue
        
        return data
    
    async def get_active_pairs(self) -> List[str]:
        """Get active trading pairs"""
        try:
            tickers = await self.exchange.fetch_tickers()
            pairs = []
            
            for symbol in tickers:
                if symbol.endswith('/USDT'):
                    volume = tickers[symbol].get('quoteVolume', 0)
                    if volume >= MIN_VOLUME_USD:
                        pairs.append(symbol)
            
            # Sort by volume and take top N
            pairs.sort(key=lambda x: tickers[x].get('quoteVolume', 0), reverse=True)
            return pairs[:TOP_N_VOLUME]
            
        except Exception as e:
            log.error(f"Error getting pairs: {e}")
            return []
    
    async def scan_loop(self):
        """Main scanning loop for direction signals"""
        log.info("🚀 Starting direction scanner...")
        
        cycle = 0
        
        while True:
            try:
                cycle += 1
                start_time = time.time()
                
                log.info(f"\n📊 Scan Cycle #{cycle}")
                
                # Get pairs
                pairs = await self.get_active_pairs()
                if not pairs:
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} pairs for direction")
                
                # Scan each pair
                for symbol in pairs:
                    # Skip if already in position
                    if symbol in self.active_positions:
                        continue
                    
                    # Fetch data
                    data = await self.fetch_data(symbol)
                    
                    # Check if we have required timeframes
                    if not all(tf in data for tf in ["1H", "15M", "5M"]):
                        continue
                    
                    # 1. DIRECTION FILTER (Scanner)
                    direction = self.scanner.scan_direction(symbol, data)
                    
                    if direction:
                        # 2. ENTRY (Price behavior)
                        entry_df = data.get("5M") or data.get("3M")
                        if is_valid_df(entry_df):
                            entry = self.entry_engine.find_entry(entry_df, direction)
                            
                            if entry and len(self.active_positions) < MAX_POSITIONS:
                                # Save position
                                self.active_positions[symbol] = entry
                                
                                # Save to database
                                await self.save_position(entry)
                                
                                log.info(f"✅ POSITION OPENED: {symbol} {entry.direction}")
                    
                    # Small delay between pairs
                    await asyncio.sleep(0.05)
                
                scan_time = time.time() - start_time
                wait_time = max(1, SCAN_INTERVAL - scan_time)
                
                log.info(f"Active positions: {len(self.active_positions)}/{MAX_POSITIONS}")
                log.info(f"Next scan in {wait_time:.1f}s")
                
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Scan loop error: {e}")
                await asyncio.sleep(10)
    
    async def monitor_loop(self):
        """Monitor loop for exits"""
        log.info("👀 Starting position monitor...")
        
        while True:
            try:
                # Check each active position
                for symbol, position in list(self.active_positions.items()):
                    try:
                        # Fetch current data
                        data = await self.fetch_data(symbol)
                        df = data.get("5M") or data.get("3M")
                        
                        if not is_valid_df(df):
                            continue
                        
                        # 3. EXIT (Momentum failure)
                        exit_signal = self.exit_engine.check_exit(df, position)
                        
                        if exit_signal:
                            # Remove from active positions
                            del self.active_positions[symbol]
                            
                            # Update database
                            await self.update_position_exit(exit_signal)
                            
                            log.info(f"📤 POSITION CLOSED: {symbol} {exit_signal.exit_reason}")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Clean up old positions
                await self.cleanup_positions()
                
                # Wait before next check
                await asyncio.sleep(5)
                
            except Exception as e:
                log.error(f"Monitor loop error: {e}")
                await asyncio.sleep(10)
    
    async def save_position(self, entry: EntrySignal):
        """Save position to database"""
        try:
            trade_id = hashlib.md5(
                f"{entry.symbol}:{entry.direction}:{entry.entry_price}:{entry.timestamp}".encode()
            ).hexdigest()
            
            await self.db.execute("""
                INSERT INTO trades (id, symbol, direction, entry_type,
                                  entry_price, status)
                VALUES (?, ?, ?, ?, ?, 'ACTIVE')
            """, (
                trade_id,
                entry.symbol,
                entry.direction,
                entry.entry_type,
                entry.entry_price
            ))
            
            await self.db.commit()
            
        except Exception as e:
            log.error(f"Error saving position: {e}")
    
    async def update_position_exit(self, exit_signal: ExitSignal):
        """Update position with exit"""
        try:
            await self.db.execute("""
                UPDATE trades SET 
                    exit_price = ?,
                    exit_reason = ?,
                    pnl_percent = ?,
                    status = 'CLOSED',
                    exited_at = CURRENT_TIMESTAMP
                WHERE symbol = ? AND status = 'ACTIVE'
            """, (
                exit_signal.exit_price,
                exit_signal.exit_reason,
                exit_signal.pnl_percent,
                exit_signal.symbol
            ))
            
            await self.db.commit()
            
        except Exception as e:
            log.error(f"Error updating position: {e}")
    
    async def cleanup_positions(self):
        """Clean up old positions"""
        try:
            # Remove positions older than 24 hours
            old_positions = []
            current_time = time.time()
            
            for symbol, position in self.active_positions.items():
                if current_time - position.timestamp > 86400:  # 24 hours
                    old_positions.append(symbol)
            
            for symbol in old_positions:
                del self.active_positions[symbol]
                log.info(f"🧹 Cleaned up old position: {symbol}")
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")
    
    async def run(self):
        """Run the framework"""
        try:
            await self.initialize()
            
            # Run both loops concurrently
            await asyncio.gather(
                self.scan_loop(),
                self.monitor_loop()
            )
            
        except KeyboardInterrupt:
            log.info("\n🛑 Framework stopped")
        except Exception as e:
            log.error(f"Framework error: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
            
            if self.db:
                await self.db.close()
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# ================ MAIN ================
async def main():
    framework = TraderFramework()
    await framework.run()

if __name__ == "__main__":
    asyncio.run(main())