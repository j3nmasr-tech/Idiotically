#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PROFESSIONAL WAVE MOMENTUM SCANNER
MTF + Elliott Wave Concept + RSI + EMA + Volume Analysis
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
from scipy import stats
from fastapi import FastAPI
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass

# ================ CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/pro_signals.db"

# Scanning parameters
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))  # 1 minute for high precision
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))    # Focus on high-volume pairs only
MIN_VOLUME_USD = 50000  # $5M minimum daily volume

# Timeframes for multi-timeframe analysis
TIMEFRAMES = {
    "4H": "4h",    # Primary direction
    "1H": "1h",    # Wave structure
    "15M": "15m",  # Momentum timing
    "5M": "5m"     # Entry precision
}

# EMA periods for structure analysis
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50,
    "trend": 200
}

# ================ DATA CLASSES ================
@dataclass
class MarketStructure:
    """Market structure analysis result"""
    direction: str  # BULLISH, BEARISH, NEUTRAL
    strength: float  # 0-1
    ema_alignment: Dict[str, float]
    higher_tf_bias: str

@dataclass
class WavePosition:
    """Wave position analysis"""
    wave_type: str  # IMPULSIVE, CORRECTIVE
    wave_phase: str  # EARLY, MID, LATE, EXHAUSTION
    completion_percent: float  # 0-100%
    next_move_direction: str  # CONTINUATION, REVERSAL

@dataclass
class MomentumAnalysis:
    """Momentum analysis using RSI"""
    rsi_value: float
    divergence: str  # BULLISH_DIV, BEARISH_DIV, NONE
    momentum_loss: bool
    failure_swing: bool
    hidden_divergence: str

@dataclass
class VolumeAnalysis:
    """Volume and market strength analysis"""
    volume_ratio: float  # Recent/Average
    volume_trend: str  # ACCUMULATING, DISTRIBUTING, EXHAUSTING
    price_volume_confirmation: bool
    climax_detected: bool

@dataclass
class TradeSignal:
    """Complete trading signal"""
    symbol: str
    side: str  # LONG, SHORT
    entry_price: float
    stop_loss: float
    take_profit: float
    confidence: float  # 0-1
    expected_move_pct: float  # Expected percentage move (typically 3%+)
    timeframes_aligned: List[str]
    wave_position: WavePosition
    momentum: MomentumAnalysis
    volume: VolumeAnalysis
    market_structure: MarketStructure
    signal_id: str
    timestamp: float

# ================ LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("wave_scanner")

# ================ UTILITIES ================
async def tg(msg: str):
    """Send Telegram message"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            })
    except Exception as e:
        log.error(f"Telegram error: {e}")

async def init_db():
    """Initialize database for professional signals"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = await aiosqlite.connect(DB_PATH)
    
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_price REAL NOT NULL,
        stop_loss REAL NOT NULL,
        take_profit REAL NOT NULL,
        confidence REAL NOT NULL,
        expected_move_pct REAL NOT NULL,
        market_structure TEXT,
        wave_position TEXT,
        momentum_analysis TEXT,
        volume_analysis TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expired_at TIMESTAMP,
        triggered BOOLEAN DEFAULT FALSE,
        trigger_price REAL,
        trigger_time TIMESTAMP,
        pnl_percent REAL
    )
    """)
    
    await conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_symbol_created ON signals(symbol, created_at)
    """)
    
    await conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_expired ON signals(expired_at, triggered)
    """)
    
    await conn.commit()
    return conn

# ================ CORE ANALYSIS ENGINE ================
class ProfessionalMarketAnalyzer:
    """Implements the master prompt methodology"""
    
    def __init__(self):
        self.signal_cache = set()
        
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """Calculate EMA series for all periods"""
        return {
            name: df['close'].ewm(span=period, adjust=False).mean()
            for name, period in EMA_PERIODS.items()
        }
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI with momentum analysis"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def analyze_market_structure(self, df: pd.DataFrame, 
                                higher_tf_direction: str = None) -> MarketStructure:
        """
        1️⃣ Multi-Timeframe Context & 2️⃣ Direction & Trend Identification
        """
        emas = self.calculate_emas(df)
        current_price = df['close'].iloc[-1]
        
        # Check EMA alignment
        ema_alignment = {}
        for name, ema_series in emas.items():
            ema_value = ema_series.iloc[-1]
            ema_alignment[name] = (current_price - ema_value) / ema_value * 100
        
        # Determine direction based on EMAs
        above_fast = current_price > emas['fast'].iloc[-1]
        above_medium = current_price > emas['medium'].iloc[-1]
        above_slow = current_price > emas['slow'].iloc[-1]
        above_trend = current_price > emas['trend'].iloc[-1]
        
        bullish_score = sum([above_fast, above_medium, above_slow, above_trend])
        bearish_score = 4 - bullish_score
        
        if bullish_score >= 3:
            direction = "BULLISH"
            strength = bullish_score / 4
        elif bearish_score >= 3:
            direction = "BEARISH"
            strength = bearish_score / 4
        else:
            direction = "NEUTRAL"
            strength = 0.5
        
        # Respect higher timeframe bias
        if higher_tf_direction and higher_tf_direction != "NEUTRAL":
            if direction != higher_tf_direction:
                # Downgrade strength if contradicting higher TF
                strength *= 0.5
                direction = f"MIXED_{direction}"
        
        return MarketStructure(
            direction=direction,
            strength=strength,
            ema_alignment=ema_alignment,
            higher_tf_bias=higher_tf_direction or "NEUTRAL"
        )
    
    def analyze_wave_position(self, df: pd.DataFrame, 
                             structure: MarketStructure) -> WavePosition:
        """
        3️⃣ Wave Position (Elliott Wave Concept)
        Simplified wave analysis without manual counting
        """
        prices = df['close'].values
        highs = df['high'].values
        lows = df['low'].values
        
        if len(prices) < 50:
            return WavePosition(
                wave_type="UNKNOWN",
                wave_phase="UNKNOWN",
                completion_percent=50,
                next_move_direction="UNKNOWN"
            )
        
        # Detect impulsive vs corrective movement
        recent_move = prices[-1] - prices[-20]
        volatility = np.std(prices[-20:])
        
        if abs(recent_move) > volatility * 2:
            wave_type = "IMPULSIVE"
        else:
            wave_type = "CORRECTIVE"
        
        # Determine wave phase using fractal analysis
        # Look for exhaustion patterns
        recent_highs = highs[-10:]
        recent_lows = lows[-10:]
        
        # Check if price is making new extremes
        if structure.direction == "BULLISH":
            is_new_high = prices[-1] > np.max(highs[-30:-10])
            # Calculate momentum divergence (simplified)
            if wave_type == "IMPULSIVE" and not is_new_high:
                wave_phase = "LATE"
                completion = 70
                next_move = "CORRECTION"  # Expect correction after impulse
            elif wave_type == "CORRECTIVE":
                wave_phase = "MID"
                completion = 40
                next_move = "CONTINUATION"  # Expect continuation of trend
            else:
                wave_phase = "EARLY"
                completion = 20
                next_move = "CONTINUATION"
        else:  # BEARISH
            is_new_low = prices[-1] < np.min(lows[-30:-10])
            if wave_type == "IMPULSIVE" and not is_new_low:
                wave_phase = "LATE"
                completion = 70
                next_move = "CORRECTION"
            elif wave_type == "CORRECTIVE":
                wave_phase = "MID"
                completion = 40
                next_move = "CONTINUATION"
            else:
                wave_phase = "EARLY"
                completion = 20
                next_move = "CONTINUATION"
        
        # Check for exhaustion (price extended from EMAs)
        emas = self.calculate_emas(df)
        fast_ema = emas['fast'].iloc[-1]
        price_distance = abs(prices[-1] - fast_ema) / fast_ema
        
        if price_distance > 0.05:  # 5% away from fast EMA
            wave_phase = "EXHAUSTION"
            completion = 85
            next_move = "REVERSAL"
        
        return WavePosition(
            wave_type=wave_type,
            wave_phase=wave_phase,
            completion_percent=completion,
            next_move_direction=next_move
        )
    
    def analyze_momentum(self, df: pd.DataFrame, 
                        wave: WavePosition) -> MomentumAnalysis:
        """
        4️⃣ RSI Momentum Analysis
        """
        rsi = self.calculate_rsi(df['close'])
        rsi_values = rsi.values[-30:]
        price_values = df['close'].values[-30:]
        
        current_rsi = rsi_values[-1]
        
        # Detect divergence
        divergence = "NONE"
        momentum_loss = False
        failure_swing = False
        hidden_divergence = "NONE"
        
        # Simple divergence detection
        if len(rsi_values) >= 10:
            # Look for RSI making lower highs while price makes higher highs (bearish div)
            rsi_highs = []
            price_highs = []
            
            for i in range(5, len(rsi_values)-5):
                if (rsi_values[i] > rsi_values[i-1] and 
                    rsi_values[i] > rsi_values[i+1]):
                    rsi_highs.append((i, rsi_values[i]))
                if (price_values[i] > price_values[i-1] and 
                    price_values[i] > price_values[i+1]):
                    price_highs.append((i, price_values[i]))
            
            if len(rsi_highs) >= 2 and len(price_highs) >= 2:
                rsi_trend = rsi_highs[-1][1] - rsi_highs[-2][1]
                price_trend = price_highs[-1][1] - price_highs[-2][1]
                
                if price_trend > 0 and rsi_trend < -5:  # Bearish divergence
                    divergence = "BEARISH_DIV"
                elif price_trend < 0 and rsi_trend > 5:  # Bullish divergence
                    divergence = "BULLISH_DIV"
        
        # Check for momentum loss (RSI flattening during price movement)
        recent_rsi = rsi_values[-5:]
        if len(recent_rsi) >= 5:
            rsi_slope = np.polyfit(range(5), recent_rsi, 1)[0]
            price_slope = np.polyfit(range(5), price_values[-5:], 1)[0]
            
            if abs(price_slope) > 0.001 and abs(rsi_slope) < 0.0001:
                momentum_loss = True
        
        # Failure swing detection
        if current_rsi > 70 and wave.wave_phase == "EXHAUSTION":
            failure_swing = True
        elif current_rsi < 30 and wave.wave_phase == "EXHAUSTION":
            failure_swing = True
        
        return MomentumAnalysis(
            rsi_value=current_rsi,
            divergence=divergence,
            momentum_loss=momentum_loss,
            failure_swing=failure_swing,
            hidden_divergence=hidden_divergence
        )
    
    def analyze_ema_interaction(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        5️⃣ EMA Interaction
        """
        emas = self.calculate_emas(df)
        current_price = df['close'].iloc[-1]
        
        interactions = {
            "rejections": [],
            "overstretched": False,
            "compression": False,
            "expansion": False
        }
        
        # Check for rejections
        for name, ema_series in emas.items():
            ema_value = ema_series.iloc[-1]
            prev_ema = ema_series.iloc[-2]
            
            # Check if price bounced off EMA
            candle = df.iloc[-1]
            if (candle['low'] < ema_value and candle['close'] > ema_value and
                candle['close'] > candle['open']):
                interactions["rejections"].append(f"BULLISH_REJECTION_{name}")
            elif (candle['high'] > ema_value and candle['close'] < ema_value and
                  candle['close'] < candle['open']):
                interactions["rejections"].append(f"BEARISH_REJECTION_{name}")
        
        # Check if price is overstretched from fast EMA
        fast_ema = emas['fast'].iloc[-1]
        distance_pct = abs(current_price - fast_ema) / fast_ema * 100
        if distance_pct > 3:  # More than 3% away
            interactions["overstretched"] = True
        
        # Check EMA compression/expansion
        fast_slow_diff = abs(emas['fast'].iloc[-1] - emas['slow'].iloc[-1]) / emas['slow'].iloc[-1]
        prev_fast_slow_diff = abs(emas['fast'].iloc[-2] - emas['slow'].iloc[-2]) / emas['slow'].iloc[-2]
        
        if fast_slow_diff < prev_fast_slow_diff * 0.8:
            interactions["compression"] = True
        elif fast_slow_diff > prev_fast_slow_diff * 1.2:
            interactions["expansion"] = True
        
        return interactions
    
    def analyze_volume(self, df: pd.DataFrame, 
                      structure: MarketStructure) -> VolumeAnalysis:
        """
        6️⃣ Volume & Market Strength
        """
        volumes = df['volume'].values
        prices = df['close'].values
        
        if len(volumes) < 20:
            return VolumeAnalysis(
                volume_ratio=1.0,
                volume_trend="UNKNOWN",
                price_volume_confirmation=False,
                climax_detected=False
            )
        
        # Calculate volume ratio
        recent_volume = np.mean(volumes[-5:])
        avg_volume = np.mean(volumes[-20:])
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
        
        # Determine volume trend
        volume_slope = np.polyfit(range(len(volumes[-10:])), volumes[-10:], 1)[0]
        price_slope = np.polyfit(range(len(prices[-10:])), prices[-10:], 1)[0]
        
        if volume_slope > 0 and price_slope > 0:
            volume_trend = "ACCUMULATING"
            confirmation = True
        elif volume_slope < 0 and price_slope > 0:
            volume_trend = "DISTRIBUTING"
            confirmation = False
        elif volume_slope < 0 and price_slope < 0:
            volume_trend = "EXHAUSTING"
            confirmation = True
        elif volume_slope > 0 and price_slope < 0:
            volume_trend = "ABSORPTION"
            confirmation = False
        else:
            volume_trend = "NEUTRAL"
            confirmation = volume_ratio > 1.0
        
        # Check for climax volume (spike > 3x average)
        climax_detected = False
        if len(volumes) >= 3:
            last_volume = volumes[-1]
            if last_volume > avg_volume * 3:
                climax_detected = True
        
        return VolumeAnalysis(
            volume_ratio=volume_ratio,
            volume_trend=volume_trend,
            price_volume_confirmation=confirmation,
            climax_detected=climax_detected
        )
    
    def generate_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                       symbol: str) -> Optional[TradeSignal]:
        """
        7️⃣ Confluence & Signal Timing
        Main signal generation logic
        """
        log.info(f"🔍 Analyzing {symbol} for professional setups...")
        
        # Get data from all timeframes
        tf_4h = multi_tf_data.get("4H")
        tf_1h = multi_tf_data.get("1H")
        tf_15m = multi_tf_data.get("15M")
        tf_5m = multi_tf_data.get("5M")
        
        if any(df is None for df in [tf_4h, tf_1h, tf_15m, tf_5m]):
            log.debug(f"{symbol}: Missing timeframe data")
            return None
        
        # ========== TOP-DOWN ANALYSIS ==========
        
        # 1. 4H - Highest timeframe for overall bias
        structure_4h = self.analyze_market_structure(tf_4h)
        if structure_4h.direction == "NEUTRAL":
            log.debug(f"{symbol}: No clear 4H bias")
            return None
        
        # 2. 1H - Wave structure analysis
        structure_1h = self.analyze_market_structure(tf_1h, structure_4h.direction)
        wave_1h = self.analyze_wave_position(tf_1h, structure_1h)
        
        # 3. 15M - Momentum timing
        structure_15m = self.analyze_market_structure(tf_15m, structure_1h.direction)
        momentum_15m = self.analyze_momentum(tf_15m, wave_1h)
        ema_interaction_15m = self.analyze_ema_interaction(tf_15m)
        volume_15m = self.analyze_volume(tf_15m, structure_15m)
        
        # 4. 5M - Entry precision
        structure_5m = self.analyze_market_structure(tf_5m, structure_15m.direction)
        
        # ========== CONFLUENCE CHECK ==========
        
        # Check 1: All timeframes aligned in same direction
        directions = [
            structure_4h.direction,
            structure_1h.direction,
            structure_15m.direction
        ]
        
        dominant_direction = max(set(directions), key=directions.count)
        alignment_score = directions.count(dominant_direction) / len(directions)
        
        if alignment_score < 0.67:  # At least 2/3 timeframes aligned
            log.debug(f"{symbol}: Timeframes not aligned ({alignment_score:.2f})")
            return None
        
        # Check 2: Wave position is favorable
        favorable_wave_phases = ["LATE", "EXHAUSTION", "MID"]
        if wave_1h.wave_phase not in favorable_wave_phases:
            log.debug(f"{symbol}: Wave phase not favorable ({wave_1h.wave_phase})")
            return None
        
        # Check 3: Momentum confirms
        momentum_confirms = False
        if dominant_direction == "BULLISH":
            momentum_confirms = (
                momentum_15m.divergence == "BULLISH_DIV" or
                (momentum_15m.rsi_value < 50 and not momentum_15m.momentum_loss)
            )
        else:  # BEARISH
            momentum_confirms = (
                momentum_15m.divergence == "BEARISH_DIV" or
                (momentum_15m.rsi_value > 50 and not momentum_15m.momentum_loss)
            )
        
        if not momentum_confirms:
            log.debug(f"{symbol}: Momentum doesn't confirm")
            return None
        
        # Check 4: Volume confirms
        if not volume_15m.price_volume_confirmation:
            log.debug(f"{symbol}: Volume doesn't confirm")
            return None
        
        # Check 5: EMA interaction shows opportunity
        has_ema_signal = (
            len(ema_interaction_15m["rejections"]) > 0 or
            ema_interaction_15m["compression"] or
            (ema_interaction_15m["overstretched"] and wave_1h.wave_phase == "EXHAUSTION")
        )
        
        if not has_ema_signal:
            log.debug(f"{symbol}: No EMA signal")
            return None
        
        # Check 6: Avoid choppy markets
        # Calculate ATR to filter low volatility
        atr = self.calculate_atr(tf_15m)
        current_atr = atr.iloc[-1]
        avg_atr = atr.mean()
        
        if current_atr < avg_atr * 0.3:  # Too low volatility
            log.debug(f"{symbol}: Market too choppy (ATR {current_atr/avg_atr:.2f})")
            return None
        
        # ========== SIGNAL GENERATION ==========
        
        current_price = tf_5m['close'].iloc[-1]
        
        # Determine side based on confluence
        if dominant_direction == "BULLISH" and wave_1h.next_move_direction == "CONTINUATION":
            side = "LONG"
        elif dominant_direction == "BEARISH" and wave_1h.next_move_direction == "CONTINUATION":
            side = "SHORT"
        elif wave_1h.wave_phase == "EXHAUSTION" and wave_1h.next_move_direction == "REVERSAL":
            # Counter-trend reversal signal (higher risk, higher reward)
            side = "LONG" if dominant_direction == "BEARISH" else "SHORT"
        else:
            log.debug(f"{symbol}: No clear trade direction")
            return None
        
        # Calculate SL/TP based on wave structure and volatility
        if side == "LONG":
            # Use recent swing low as SL basis
            recent_low = tf_1h['low'].iloc[-20:].min()
            stop_loss = recent_low * 0.995  # 0.5% below support
            
            # Expected move: 3% minimum, more if wave suggests
            expected_move = 0.03  # 3% baseline
            
            if wave_1h.wave_phase == "EXHAUSTION":
                expected_move *= 1.5  # Exhaustion moves often stronger
            
            take_profit = current_price * (1 + expected_move)
            
        else:  # SHORT
            # Use recent swing high as SL basis
            recent_high = tf_1h['high'].iloc[-20:].max()
            stop_loss = recent_high * 1.005  # 0.5% above resistance
            
            expected_move = 0.03  # 3% baseline
            
            if wave_1h.wave_phase == "EXHAUSTION":
                expected_move *= 1.5
            
            take_profit = current_price * (1 - expected_move)
        
        # Calculate confidence score
        confidence_factors = {
            "timeframe_alignment": alignment_score,
            "wave_position": wave_1h.completion_percent / 100,
            "momentum": 0.8 if momentum_confirms else 0.3,
            "volume": 0.8 if volume_15m.price_volume_confirmation else 0.4,
            "ema_signal": 0.7 if has_ema_signal else 0.3,
            "structure_strength": structure_15m.strength
        }
        
        confidence = np.mean(list(confidence_factors.values()))
        
        # Minimum confidence threshold
        if confidence < 0.65:
            log.debug(f"{symbol}: Confidence too low ({confidence:.2f})")
            return None
        
        # Risk/Reward check
        risk = abs(current_price - stop_loss)
        reward = abs(take_profit - current_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < 1.5:  # Minimum 1.5:1 R:R
            log.debug(f"{symbol}: Poor R:R ratio ({rr_ratio:.2f}:1)")
            return None
        
        # ========== CREATE SIGNAL ==========
        
        signal_id = hashlib.md5(
            f"{symbol}:{side}:{current_price:.8f}:{time.time_ns()}".encode()
        ).hexdigest()
        
        signal = TradeSignal(
            symbol=symbol,
            side=side,
            entry_price=current_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            confidence=confidence,
            expected_move_pct=expected_move * 100,
            timeframes_aligned=[tf for tf, dir in zip(["4H", "1H", "15M"], directions) 
                              if dir == dominant_direction],
            wave_position=wave_1h,
            momentum=momentum_15m,
            volume=volume_15m,
            market_structure=structure_15m,
            signal_id=signal_id,
            timestamp=time.time()
        )
        
        log.info(f"🎯 SIGNAL FOUND: {symbol} {side} @ {current_price:.4f}")
        log.info(f"   Confidence: {confidence:.2f}, Expected: {expected_move*100:.1f}%")
        log.info(f"   Wave: {wave_1h.wave_phase}, R:R: {rr_ratio:.2f}:1")
        
        return signal
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range"""
        high = df['high']
        low = df['low']
        close = df['close'].shift()
        
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        return atr

# ================ SCANNER MAIN ================
class ProfessionalWaveScanner:
    """Main scanner class"""
    
    def __init__(self):
        self.analyzer = ProfessionalMarketAnalyzer()
        self.exchange = None
        self.db = None
        self.recent_signals = {}
        
    async def initialize(self):
        """Initialize scanner"""
        log.info("=" * 60)
        log.info("🚀 PROFESSIONAL WAVE MOMENTUM SCANNER")
        log.info("⸻")
        log.info("Methodology: MTF + Wave + RSI + EMA + Volume")
        log.info("Expected moves: 3%+ within minutes to hours")
        log.info("Signal frequency: Low, High accuracy")
        log.info("=" * 60)
        
        # Initialize database
        self.db = await init_db()
        
        # Initialize exchange
        self.exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 30000
        })
        
        # Test connection
        ticker = await self.exchange.fetch_ticker("BTC/USDT")
        log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
        
        # Send startup message
        await tg(f"""
🚀 <b>PROFESSIONAL WAVE SCANNER STARTED</b>

<b>Methodology:</b>
• Multi-Timeframe Context (4H → 5M)
• Wave Position Analysis (Elliott Concept)
• RSI Momentum & Divergence
• EMA Interaction & Structure
• Volume Confirmation

<b>Target:</b> 3%+ moves within minutes to hours
<b>Signal Type:</b> Low frequency, High precision
<b>Pairs:</b> Top {TOP_N_VOLUME} by volume (>${MIN_VOLUME_USD/1e6}M)

✅ <b>Scanner is active and monitoring markets</b>

#ProfessionalScanner #WaveAnalysis #CryptoSignals
""")
    
    async def fetch_multi_tf_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch data for all timeframes"""
        data = {}
        
        for tf_name, tf in TIMEFRAMES.items():
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=100)
                
                if ohlcv and len(ohlcv) >= 50:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    
                    # Convert to numeric
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = df.dropna()
                    
                    if len(df) >= 50:
                        data[tf_name] = df
                    else:
                        log.debug(f"{symbol} {tf_name}: Insufficient data")
                else:
                    log.debug(f"{symbol} {tf_name}: No data")
                    
            except Exception as e:
                log.debug(f"{symbol} {tf_name} fetch error: {e}")
                continue
        
        return data
    
    async def get_top_volume_pairs(self) -> List[Tuple[str, float]]:
        """Get top pairs by volume"""
        try:
            tickers = await self.exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol, ticker in tickers.items():
                if symbol.endswith('/USDT'):
                    volume = ticker.get('quoteVolume', 0)
                    if volume >= MIN_VOLUME_USD:
                        usdt_pairs.append((symbol, volume))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            return usdt_pairs[:TOP_N_VOLUME]
            
        except Exception as e:
            log.error(f"Error fetching volume pairs: {e}")
            return []
    
    async def save_signal(self, signal: TradeSignal):
        """Save signal to database"""
        try:
            # Check if similar signal exists recently
            async with self.db.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE symbol = ? AND side = ? 
                AND created_at > datetime('now', '-2 hours')
            """, (signal.symbol, signal.side)) as cursor:
                result = await cursor.fetchone()
                if result and result[0] > 0:
                    log.debug(f"Similar signal for {signal.symbol} in last 2 hours")
                    return False
            
            # Insert new signal
            await self.db.execute("""
                INSERT INTO signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    confidence, expected_move_pct, market_structure,
                    wave_position, momentum_analysis, volume_analysis,
                    expired_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', '+6 hours'))
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.confidence,
                signal.expected_move_pct,
                str(signal.market_structure),
                str(signal.wave_position),
                str(signal.momentum),
                str(signal.volume)
            ))
            
            await self.db.commit()
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_signal_message(self, signal: TradeSignal) -> str:
        """Format signal for Telegram"""
        side_ar = "🔵 شراء" if signal.side == "LONG" else "🔴 بيع"
        
        # Calculate R:R
        risk = abs(signal.entry_price - signal.stop_loss)
        reward = abs(signal.take_profit - signal.entry_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        # Format wave info
        wave_ar = {
            "EARLY": "بداية الموجة",
            "MID": "وسط الموجة",
            "LATE": "نهاية الموجة",
            "EXHAUSTION": "مرحلة الإرهاق"
        }.get(signal.wave_position.wave_phase, signal.wave_position.wave_phase)
        
        # Format RSI info
        rsi_status = ""
        if signal.momentum.divergence != "NONE":
            rsi_status = "تضارب في RSI"
        elif signal.momentum.failure_swing:
            rsi_status = "فشل في RSI"
        
        message = f"""
🎯 <b>إشارة تداول محترفة</b>

<b>{signal.symbol}</b> | {side_ar}

<b>التحليل المتعدد الفريمات:</b>
• ٤ ساعات: {signal.market_structure.direction}
• ١ ساعة: {signal.wave_position.wave_type} - {wave_ar}
• ١٥ دقيقة: تأكيد الزخم{' ✅' if signal.momentum.divergence != 'NONE' else ''}
• ٥ دقائق: الدخول الدقيق

<b>المؤشرات:</b>
• RSI: {signal.momentum.rsi_value:.1f} {rsi_status}
• الفوليوم: {signal.volume.volume_trend} (×{signal.volume.volume_ratio:.1f})
• جودة الإشارة: {signal.confidence:.1%}

<b>التنفيذ:</b>
• الدخول: <code>{signal.entry_price:.4f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.4f}</code>
• هدف الربح: <code>{signal.take_profit:.4f}</code>

<b>الأهداف:</b>
• الحركة المتوقعة: {signal.expected_move_pct:.1f}%
• نسبة الربح/المخاطرة: {rr_ratio:.1f}:1
• الفريمات المتوافقة: {', '.join(signal.timeframes_aligned)}

<b>الملاحظة:</b> هذه الإشارة تستهدف حركة سريعة (دقائق إلى ساعات).

⏰ <i>الصفقة تنتهي تلقائياً بعد ٦ ساعات</i>

#{signal.side} #{signal.symbol.replace('/USDT', '')} #تداول_محترف
"""
        return message
    
    async def monitoring_loop(self):
        """Monitor open positions and trigger signals"""
        while True:
            try:
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit 
                    FROM signals 
                    WHERE triggered = FALSE 
                    AND expired_at > datetime('now')
                """) as cursor:
                    open_signals = await cursor.fetchall()
                
                for signal_id, symbol, side, entry, sl, tp in open_signals:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Check if price reached entry (within 0.5%)
                        if abs(current_price - entry) / entry <= 0.005:
                            # Trigger the signal
                            await self.db.execute("""
                                UPDATE signals SET 
                                    triggered = TRUE,
                                    trigger_price = ?,
                                    trigger_time = CURRENT_TIMESTAMP
                                WHERE id = ?
                            """, (current_price, signal_id))
                            
                            await self.db.commit()
                            
                            # Send triggered notification
                            await tg(f"""
✅ <b>تم تفعيل الإشارة</b>

<b>{symbol}</b>
• الإشارة: {side}
• سعر التفعيل: {current_price:.4f}
• الوقت: {time.strftime('%H:%M:%S')}

<b>بدء المتابعة التلقائية...</b>
""")
                        
                        # Check if SL or TP hit
                        pnl_percent = 0
                        close_reason = None
                        
                        if side == "LONG":
                            if current_price >= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                            elif current_price <= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((current_price - entry) / entry) * 100
                        else:  # SHORT
                            if current_price <= tp:
                                close_reason = "TP_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                            elif current_price >= sl:
                                close_reason = "SL_HIT"
                                pnl_percent = ((entry - current_price) / entry) * 100
                        
                        if close_reason:
                            await self.db.execute("""
                                UPDATE signals SET 
                                    pnl_percent = ?
                                WHERE id = ?
                            """, (pnl_percent, signal_id))
                            
                            await self.db.commit()
                            
                            result_emoji = "✅" if close_reason == "TP_HIT" else "❌"
                            await tg(f"""
{result_emoji} <b>تم إغلاق الصفقة</b>

<b>{symbol}</b>
• النتيجة: {'هدف الربح' if close_reason == 'TP_HIT' else 'وقف الخسارة'}
• الربح/الخسارة: {'+' if pnl_percent > 0 else ''}{pnl_percent:.2f}%
• المدة: منذ التفعيل

#{'ربح' if pnl_percent > 0 else 'خسارة'} #إغلاق
""")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(30)
    
    async def scanning_loop(self):
        """Main scanning loop"""
        log.info("🚀 Starting professional scanning loop...")
        
        while True:
            try:
                start_time = time.time()
                log.info("=" * 50)
                log.info("Starting new professional scan...")
                
                # Get top volume pairs
                pairs = await self.get_top_volume_pairs()
                log.info(f"Scanning {len(pairs)} high-volume pairs")
                
                signals_found = 0
                
                for symbol, volume in pairs:
                    try:
                        # Fetch multi-timeframe data
                        multi_tf_data = await self.fetch_multi_tf_data(symbol)
                        
                        if len(multi_tf_data) < 4:  # Need all 4 timeframes
                            continue
                        
                        # Generate professional signal
                        signal = self.analyzer.generate_signal(multi_tf_data, symbol)
                        
                        if signal:
                            # Save to database
                            saved = await self.save_signal(signal)
                            
                            if saved:
                                # Send Telegram alert
                                message = await self.format_signal_message(signal)
                                await tg(message)
                                
                                signals_found += 1
                                log.info(f"✅ Professional signal sent for {signal.symbol}")
                        
                        # Rate limiting
                        await asyncio.sleep(0.3)
                        
                    except Exception as e:
                        log.error(f"Error processing {symbol}: {e}")
                        continue
                
                scan_duration = time.time() - start_time
                log.info(f"Scan complete. Found {signals_found} professional signals in {scan_duration:.1f}s")
                
                # Wait for next scan
                log.info(f"Waiting {SCAN_INTERVAL}s for next professional scan...")
                await asyncio.sleep(SCAN_INTERVAL)
                
            except Exception as e:
                log.error(f"Scan loop error: {e}")
                await asyncio.sleep(30)
    
    async def run(self):
        """Main run method"""
        await self.initialize()
        
        # Start both loops
        try:
            await asyncio.gather(
                self.scanning_loop(),
                self.monitoring_loop()
            )
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")
            await tg("🛑 توقف الماسح المحترف يدوياً")
        finally:
            if self.exchange:
                await self.exchange.close()
            if self.db:
                await self.db.close()

# ================ FASTAPI ================
app = FastAPI(title="Professional Wave Scanner")

scanner = None

@app.on_event("startup")
async def startup():
    global scanner
    scanner = ProfessionalWaveScanner()

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "Professional Wave Momentum Scanner",
        "methodology": "MTF + Wave + RSI + EMA + Volume",
        "target_moves": "3%+ within minutes to hours",
        "signal_frequency": "Low, High accuracy"
    }

@app.get("/stats")
async def get_stats():
    """Get scanner statistics"""
    if not scanner or not scanner.db:
        return {"error": "Scanner not initialized"}
    
    try:
        async with scanner.db.execute("SELECT COUNT(*) FROM signals") as cursor:
            total = (await cursor.fetchone())[0]
        
        async with scanner.db.execute("SELECT COUNT(*) FROM signals WHERE triggered = TRUE") as cursor:
            triggered = (await cursor.fetchone())[0]
        
        async with scanner.db.execute("SELECT AVG(pnl_percent) FROM signals WHERE pnl_percent IS NOT NULL") as cursor:
            avg_pnl = (await cursor.fetchone())[0] or 0
        
        return {
            "total_signals": total,
            "triggered_signals": triggered,
            "average_pnl_percent": f"{avg_pnl:.2f}%",
            "active_since": "Scanner running"
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/recent")
async def get_recent_signals(limit: int = 10):
    """Get recent signals"""
    if not scanner or not scanner.db:
        return {"error": "Scanner not initialized"}
    
    try:
        scanner.db.row_factory = aiosqlite.Row
        async with scanner.db.execute("""
            SELECT symbol, side, entry_price, confidence, expected_move_pct, created_at
            FROM signals 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,)) as cursor:
            rows = await cursor.fetchall()
            
            signals = []
            for row in rows:
                signals.append(dict(row))
            
            return {
                "signals": signals,
                "count": len(signals)
            }
    except Exception as e:
        return {"error": str(e)}

# ================ MAIN ================
if __name__ == "__main__":
    # Run the scanner
    scanner_instance = ProfessionalWaveScanner()
    asyncio.run(scanner_instance.run())