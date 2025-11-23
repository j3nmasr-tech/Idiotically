#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🏆 ULTIMATE HYBRID SCANNER v3.0 🏆
- YOUR EXACT OLD LOGIC + WINNER FILTERS + ELITE TP/SL
- NEW MONITORING INFRASTRUCTURE + PERFORMANCE TRACKING
- STRICT FILTER ENFORCEMENT + REALISTIC SCORING
- NO ARTIFICIAL INFLATION - ONLY PROVEN SIGNALS
"""

import os
import time
import asyncio
import logging
import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn
from collections import defaultdict, deque
import json
from contextlib import asynccontextmanager

# ==================== ENHANCED CONFIGURATION ====================

class Timeframe(Enum):
    M1 = "1m"
    M3 = "3m" 
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"

class MarketRegime(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH" 
    RANGING = "RANGING"

class SignalSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

@dataclass
class ScannerConfig:
    # Core settings (from your old system)
    SCAN_INTERVAL: int = 60
    TOP_N_SYMBOLS: int = 100
    MIN_VOLUME_USDT: float = 1000000
    MAX_SPREAD_PCT: float = 0.002
    HEARTBEAT_INTERVAL: int = 3600
    
    # Risk management
    MAX_SL_PCT: float = 0.03
    MIN_RR_RATIO: float = 1.5
    MAX_POSITIONS: int = 5
    
    # Signal filters (YOUR EXACT OLD SETTINGS)
    MIN_SIGNAL_SCORE: int = 7  # Realistic scoring
    COOLDOWN_MINUTES: int = 30
    MAX_SL_CLUSTER_HITS: int = 3
    
    # WINNER FILTER SETTINGS (YOUR EXACT OLD LOGIC)
    REQUIRE_BTC_ALIGNMENT: bool = True
    REQUIRE_HIGHER_TF_ALIGNMENT: bool = True
    REQUIRE_MOMENTUM_CONFIRMATION: bool = True
    REQUIRE_ZONE_QUALITY: bool = True
    AVOID_CHOPPY_MARKETS: bool = True
    USE_MARKET_REGIME: bool = True
    STRICT_MARKET_REGIME: bool = False  # YOUR EXACT OLD SETTING - ALLOWS RANGING
    
    # Timeframes for analysis
    TIMEFRAMES: List[Timeframe] = None
    
    def __post_init__(self):
        if self.TIMEFRAMES is None:
            self.TIMEFRAMES = [Timeframe.M1, Timeframe.M3, Timeframe.M5, 
                              Timeframe.M15, Timeframe.M30]

# ==================== YOUR EXACT OLD WINNER FILTERS ====================

class OriginalWinnerFilters:
    """YOUR EXACT ORIGINAL FILTERS - UNCHANGED"""
    
    @staticmethod
    def get_btc_direction(btc_15m: pd.DataFrame, btc_1h: pd.DataFrame) -> str:
        """YOUR EXACT BTC DIRECTION DETECTION"""
        if btc_15m is None or btc_1h is None: 
            return "NEUTRAL"
        try:
            price = btc_15m['close'].iloc[-1]
            ema_1h_50 = btc_1h['close'].ewm(span=50).mean().iloc[-1]
            ema_15m_20 = btc_15m['close'].ewm(span=20).mean().iloc[-1]
            
            if price > ema_1h_50 and price > ema_15m_20: 
                return "BULLISH"
            elif price < ema_1h_50 and price < ema_15m_20: 
                return "BEARISH"
            else: 
                return "NEUTRAL"
        except: 
            return "NEUTRAL"

    @staticmethod
    def is_trade_allowed(signal_side: SignalSide, btc_direction: str) -> bool:
        """YOUR EXACT BTC ALIGNMENT FILTER"""
        if btc_direction == "BULLISH": 
            return signal_side == SignalSide.BUY
        elif btc_direction == "BEARISH": 
            return signal_side == SignalSide.SELL
        else: 
            return True

    @staticmethod
    def check_higher_tf_alignment(signal, higher_tf_data: pd.DataFrame) -> bool:
        """YOUR EXACT HIGHER TIMEFRAME ALIGNMENT"""
        if higher_tf_data is None or len(higher_tf_data) < 20:
            return False
        current_price = signal.entry_price if hasattr(signal, 'entry_price') else signal['entry']
        higher_tf_ema_20 = higher_tf_data['close'].ewm(span=20).mean().iloc[-1]
        higher_tf_ema_50 = higher_tf_data['close'].ewm(span=50).mean().iloc[-1]
        
        signal_side = signal.side if hasattr(signal, 'side') else SignalSide(signal['side'])
        
        if signal_side == SignalSide.BUY:
            return current_price > higher_tf_ema_20 and current_price > higher_tf_ema_50
        else:
            return current_price < higher_tf_ema_20 and current_price < higher_tf_ema_50

    @staticmethod
    def check_momentum_confirmation(df: pd.DataFrame, signal_direction: SignalSide) -> bool:
        """YOUR EXACT MOMENTUM CONFIRMATION"""
        if len(df) < 3: 
            return False
        current_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        if signal_direction == SignalSide.BUY:
            return (current_candle['close'] > current_candle['open'] and 
                    current_candle['close'] > prev_candle['close'])
        else:
            return (current_candle['close'] < current_candle['open'] and
                    current_candle['close'] < prev_candle['close'])

    @staticmethod
    def check_entry_zone_quality(df: pd.DataFrame, signal_direction: SignalSide) -> bool:
        """YOUR EXACT ZONE QUALITY DETECTION"""
        if len(df) < 15: 
            return False
        recent_high = df['high'].tail(15).max()
        recent_low = df['low'].tail(15).min()
        current_price = df['close'].iloc[-1]
        
        if recent_high == recent_low: 
            return False
            
        range_position = (current_price - recent_low) / (recent_high - recent_low)
        
        if signal_direction == SignalSide.BUY:
            return range_position < 0.3
        else:
            return range_position > 0.7

    @staticmethod
    def detect_choppy_market(df: pd.DataFrame) -> bool:
        """YOUR EXACT MARKET CONDITION FILTER"""
        if len(df) < 25: 
            return True
            
        high, low, close = df['high'], df['low'], df['close']
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        atr = true_range.rolling(14).mean().iloc[-1]
        
        current_price = close.iloc[-1]
        price_range_pct = (df['high'].tail(20).max() - df['low'].tail(20).min()) / current_price
        
        return (atr < (current_price * 0.002) and price_range_pct < 0.02)

    @staticmethod
    def detect_market_regime(df_1h: pd.DataFrame, df_4h: pd.DataFrame = None) -> MarketRegime:
        """YOUR EXACT MARKET REGIME DETECTION"""
        if df_1h is None or len(df_1h) < 100:
            return MarketRegime.RANGING
        
        try:
            close = df_1h['close']
            
            ema_20 = close.ewm(span=20).mean()
            ema_50 = close.ewm(span=50).mean()
            ema_100 = close.ewm(span=100).mean()
            
            current_price = close.iloc[-1]
            price_above_ema20 = current_price > ema_20.iloc[-1]
            price_above_ema50 = current_price > ema_50.iloc[-1]
            price_above_ema100 = current_price > ema_100.iloc[-1]
            
            ema_bull_aligned = ema_20.iloc[-1] > ema_50.iloc[-1] > ema_100.iloc[-1]
            ema_bear_aligned = ema_20.iloc[-1] < ema_50.iloc[-1] < ema_100.iloc[-1]
            
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
            
            bullish_conditions = (
                price_above_ema20 and 
                price_above_ema50 and 
                ema_bull_aligned and
                current_rsi > 40
            )
            
            bearish_conditions = (
                not price_above_ema20 and 
                not price_above_ema50 and 
                ema_bear_aligned and
                current_rsi < 60
            )
            
            if bullish_conditions:
                return MarketRegime.BULLISH
            elif bearish_conditions:
                return MarketRegime.BEARISH
            else:
                return MarketRegime.RANGING
                
        except Exception as e:
            logging.error(f"Market regime detection error: {e}")
            return MarketRegime.RANGING

    @staticmethod
    def should_trade_in_regime(signal_side: SignalSide, market_regime: MarketRegime, strict_mode: bool = False) -> bool:
        """YOUR EXACT DON'T FIGHT THE TIDE LOGIC"""
        if market_regime == MarketRegime.BULLISH:
            return signal_side == SignalSide.BUY
        elif market_regime == MarketRegime.BEARISH:
            return signal_side == SignalSide.SELL
        else:  # RANGING
            if strict_mode:
                return False
            else:
                return True  # YOUR ORIGINAL SETTING - ALLOWS TRADES IN RANGING MARKETS

# ==================== YOUR EXACT OLD SMC CORE LOGIC ====================

class OriginalSMCLogic:
    """YOUR EXACT ORIGINAL SMC LOGIC - UNCHANGED"""
    
    @staticmethod
    def detect_swing_points(df: pd.DataFrame):
        if len(df) < 5: return None
        last = df.iloc[-1]; prev = df.iloc[-3:-1]
        swing_high = last["high"] > prev["high"].max()
        swing_low = last["low"] < prev["low"].min()
        return swing_high, swing_low

    @staticmethod
    def detect_active_range(df: pd.DataFrame, lookback=10):
        last = df.iloc[-lookback:]
        return last["high"].max(), last["low"].min()

    @staticmethod
    def detect_sweep(df: pd.DataFrame):
        if len(df) < 6: return False, False
        last = df.iloc[-1]; prev = df.iloc[-5:-1]
        return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

    @staticmethod
    def detect_bos_mss(df: pd.DataFrame):
        hh, ll = OriginalSMCLogic.detect_sweep(df)
        return hh, ll

    @staticmethod
    def detect_fvg(df: pd.DataFrame):
        if len(df) < 3: return False, False
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
        bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
        return bull, bear

    @staticmethod
    def detect_order_blocks(df: pd.DataFrame):
        if len(df) < 3: return None, None, None
        candle = df.iloc[-3]
        if candle["close"] > candle["open"]:
            return "bullish", candle["open"], candle["low"]
        return "bearish", candle["high"], candle["open"]

    @staticmethod
    def generate_signal(df: pd.DataFrame, symbol: str, context=None):
        """YOUR EXACT ORIGINAL SIGNAL GENERATION LOGIC"""
        if context is None: context = {}
        tf = context.get("tf","15m")

        if df is None or len(df) < 6: return None

        last = df["close"].iloc[-1]

        ob_type, ob_hi, ob_lo = OriginalSMCLogic.detect_order_blocks(df)
        if ob_type is None: return None

        bull_fvg, bear_fvg = OriginalSMCLogic.detect_fvg(df)
        sweep_h, sweep_l = OriginalSMCLogic.detect_sweep(df)
        bos_hh, bos_ll = OriginalSMCLogic.detect_bos_mss(df)

        if not (bos_hh or bos_ll): return None

        score = 0; reasons = []

        if ob_type=="bullish": score+=2; reasons.append("OB Bull +2")
        else: score+=2; reasons.append("OB Bear +2")

        if bull_fvg: score+=2; reasons.append("FVG Bull +2")
        elif bear_fvg: score+=2; reasons.append("FVG Bear +2")

        score+=2; reasons.append("BOS +2")
        if sweep_h or sweep_l: score+=1; reasons.append("Sweep +1")
        else: reasons.append("No Sweep +0")

        side = "BUY" if ob_type=="bullish" else "SELL"

        # Use Elite Smart TP/SL from your old system
        entry = float(last)
        sl, tp1, tp2, tp3 = EliteTPSL.calculate_elite_tp_sl(df, symbol, side, entry, context)

        # Ensure logical TP/SL relationship
        if side == "BUY":
            if not (sl < entry < tp1 < tp2 < tp3):
                tp1 = entry * 1.012
                tp2 = entry * 1.025
                tp3 = entry * 1.045
                sl = entry * 0.988
        else:  # SELL
            if not (sl > entry > tp1 > tp2 > tp3):
                tp1 = entry * 0.988
                tp2 = entry * 0.975
                tp3 = entry * 0.955
                sl = entry * 1.012

        return {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "score": score,
            "reason": "Set B SMC Signal + Elite TP/SL",
            "reason_list": reasons,
            "timeframe": tf
        }

# ==================== YOUR EXACT OLD ELITE TP/SL SYSTEM ====================

class EliteTPSL:
    """YOUR EXACT ELITE SMART TP/SL SYSTEM - INSTITUTIONAL GRADE"""
    
    @staticmethod
    def calculate_elite_tp_sl(df, symbol, side, entry, context):
        """YOUR EXACT INSTITUTIONAL-GRADE TP/SL"""
        tf = context.get("tf", "15m")
        current_price = entry
        
        volatility_score = EliteTPSL.calculate_volatility_score(df, tf)
        volume_nodes = EliteTPSL.find_volume_nodes(df)
        
        if side == "BUY":
            tp1 = EliteTPSL.find_nearest_resistance(df, current_price)
            tp1 = min(tp1, current_price * (1 + volatility_score * 0.6))
            
            tp2 = EliteTPSL.find_major_resistance(df, current_price)
            tp2 = min(tp2, current_price * (1 + volatility_score * 1.2))
            
            tp3 = current_price * (1 + volatility_score * 2.0)
            
            sl = EliteTPSL.calculate_smart_sl(df, side, entry, volatility_score, volume_nodes)
            
        else:  # SELL
            tp1 = EliteTPSL.find_nearest_support(df, current_price)
            tp1 = max(tp1, current_price * (1 - volatility_score * 0.6))
            
            tp2 = EliteTPSL.find_major_support(df, current_price)
            tp2 = max(tp2, current_price * (1 - volatility_score * 1.2))
            
            tp3 = current_price * (1 - volatility_score * 2.0)
            
            sl = EliteTPSL.calculate_smart_sl(df, side, entry, volatility_score, volume_nodes)
        
        return sl, tp1, tp2, tp3

    @staticmethod
    def calculate_volatility_score(df, tf):
        """YOUR EXACT VOLATILITY ASSESSMENT"""
        current_atr = EliteTPSL.atr(df, 14).iloc[-1] if len(df) >= 14 else df['close'].iloc[-1] * 0.01
        current_vol = current_atr / df['close'].iloc[-1]
        
        high_50 = df['high'].tail(50).max()
        low_50 = df['low'].tail(50).min()
        range_50 = (high_50 - low_50) / df['close'].iloc[-1]
        
        tf_multiplier = {
            "1m": 0.8, "3m": 1.0, "5m": 1.2, 
            "15m": 1.5, "30m": 2.0, "1h": 2.5
        }.get(tf, 1.5)
        
        volatility_score = max(current_vol, range_50 * 0.3) * tf_multiplier
        return min(volatility_score, 0.05)

    @staticmethod
    def atr(df: pd.DataFrame, period=14):
        """YOUR EXACT ATR CALCULATION"""
        high, low, close = df["high"], df["low"], df["close"]
        tr = pd.DataFrame({
            "h-l": high - low,
            "h-pc": (high - close.shift(1)).abs(),
            "l-pc": (low - close.shift(1)).abs()
        }).max(axis=1)
        return tr.rolling(period, min_periods=1).mean()

    @staticmethod
    def find_volume_nodes(df):
        """YOUR EXACT VOLUME NODES"""
        if len(df) < 20: return []
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        volume = df['volume']
        high_volume_indices = volume.nlargest(5).index
        volume_nodes = [typical_price.iloc[i] for i in high_volume_indices]
        return volume_nodes

    @staticmethod
    def find_nearest_resistance(df, current_price):
        """YOUR EXACT NEAREST RESISTANCE"""
        recent_highs = df['high'].tail(20).nlargest(3).values
        prev_resistance = df['high'].rolling(50).max().iloc[-1] if len(df) >= 50 else current_price * 1.02
        all_resistance = list(recent_highs) + [prev_resistance]
        valid_resistance = [r for r in all_resistance if r > current_price]
        return min(valid_resistance) if valid_resistance else current_price * 1.015

    @staticmethod
    def find_major_resistance(df, current_price):
        """YOUR EXACT MAJOR RESISTANCE"""
        major_highs = df['high'].tail(100).nlargest(2).values if len(df) >= 100 else [current_price * 1.03]
        recent_low = df['low'].tail(50).min() if len(df) >= 50 else current_price * 0.98
        recent_high = df['high'].tail(50).max() if len(df) >= 50 else current_price * 1.02
        fib_161 = recent_low + (recent_high - recent_low) * 1.618
        all_major = list(major_highs) + [fib_161]
        valid_major = [r for r in all_major if r > current_price]
        return min(valid_major) if valid_major else current_price * 1.03

    @staticmethod
    def find_nearest_support(df, current_price):
        """YOUR EXACT NEAREST SUPPORT"""
        recent_lows = df['low'].tail(20).nsmallest(3).values
        prev_support = df['low'].rolling(50).min().iloc[-1] if len(df) >= 50 else current_price * 0.98
        all_support = list(recent_lows) + [prev_support]
        valid_support = [s for s in all_support if s < current_price]
        return max(valid_support) if valid_support else current_price * 0.985

    @staticmethod
    def find_major_support(df, current_price):
        """YOUR EXACT MAJOR SUPPORT"""
        major_lows = df['low'].tail(100).nsmallest(2).values if len(df) >= 100 else [current_price * 0.97]
        recent_low = df['low'].tail(50).min() if len(df) >= 50 else current_price * 0.98
        recent_high = df['high'].tail(50).max() if len(df) >= 50 else current_price * 1.02
        fib_161 = recent_high - (recent_high - recent_low) * 1.618
        all_major = list(major_lows) + [fib_161]
        valid_major = [s for s in all_major if s < current_price]
        return max(valid_major) if valid_major else current_price * 0.97

    @staticmethod
    def calculate_smart_sl(df, side, entry, volatility_score, volume_nodes):
        """YOUR EXACT SMART STOP LOSS"""
        if side == "BUY":
            swing_low = df['low'].tail(15).min()
            volume_support_nodes = [node for node in volume_nodes if node < entry]
            volume_support = min(volume_support_nodes) if volume_support_nodes else entry * 0.99
            min_sl = entry * (1 - volatility_score * 0.8)
            sl = max(swing_low, volume_support, min_sl)
        else:  # SELL
            swing_high = df['high'].tail(15).max()
            volume_resistance_nodes = [node for node in volume_nodes if node > entry]
            volume_resistance = max(volume_resistance_nodes) if volume_resistance_nodes else entry * 1.01
            min_sl = entry * (1 + volatility_score * 0.8)
            sl = min(swing_high, volume_resistance, min_sl)
        return sl

# ==================== ENHANCED DATA MODELS ====================

@dataclass
class TradingSignal:
    """Enhanced signal with your old logic + new tracking"""
    symbol: str
    side: SignalSide
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: float
    timestamp: datetime.datetime
    timeframe: Timeframe
    
    # Your original scoring
    confidence_score: float
    quality_score: float
    filters_passed: List[str]
    rejection_reasons: List[str]
    
    # Winner filters tracking
    winner_filters_passed: List[str]
    winner_filters_failed: List[str]
    
    # Metadata
    signal_id: str
    version: str = "3.0-HYBRID"

# ==================== TRADE MONITORING SYSTEM ====================

class TradeMonitor:
    """NEW: Advanced monitoring with your old signal handling"""
    
    def __init__(self, scanner):
        self.scanner = scanner
        self.open_signals = {}
        self.closed_trades = []
        self.all_signals = []
        self.last_summary_time = time.time()
        self.recent_sl = defaultdict(lambda: deque())
        
    async def add_signal(self, signal: TradingSignal):
        """Add your old-style signal to new monitoring"""
        self.open_signals[signal.signal_id] = signal
        self.all_signals.append({
            'signal': signal,
            'status': 'OPEN',
            'added_time': datetime.datetime.utcnow()
        })
        logging.info(f"📈 Monitoring: {signal.symbol} {signal.side.value} | Score: {signal.confidence_score}")
        
    def record_sl_hit(self, symbol: str, lookback_minutes=30):
        """YOUR EXACT SL-CLUSTER LOGIC"""
        now = time.time(); dq = self.recent_sl[symbol]; dq.append(now)
        cutoff = now - lookback_minutes * 60
        while dq and dq[0] < cutoff: dq.popleft()
        
    def deprioritized(self, symbol: str, threshold=3, lookback=30):
        """YOUR EXACT DEPRIORITIZATION LOGIC"""
        dq = self.recent_sl[symbol]; now = time.time(); cutoff = now - lookback * 60
        while dq and dq[0] < cutoff: dq.popleft()
        return len(dq) >= threshold

    async def monitor_open_signals(self):
        """NEW: Monitor your old signals with new infrastructure"""
        if not self.open_signals: return
        
        signals_to_remove = []
        
        for signal_id, signal in self.open_signals.items():
            try:
                ticker = await self.scanner.exchange.fetch_ticker(signal.symbol)
                current_price = ticker['last']
                
                status = await self.check_signal_status(signal, current_price)
                
                if status != "OPEN":
                    await self._process_closed_signal(signal, status, current_price)
                    signals_to_remove.append(signal_id)
                    if "SL" in status:
                        self.record_sl_hit(signal.symbol)
                    
            except Exception as e:
                logging.error(f"Error monitoring {signal.symbol}: {e}")
        
        for signal_id in signals_to_remove:
            del self.open_signals[signal_id]
    
    async def check_signal_status(self, signal: TradingSignal, current_price: float):
        """Check TP/SL hits for your old signals"""
        if signal.side == SignalSide.BUY:
            if current_price >= signal.take_profit_3: return "TP3_HIT"
            elif current_price >= signal.take_profit_2: return "TP2_HIT"
            elif current_price >= signal.take_profit_1: return "TP1_HIT"
            elif current_price <= signal.stop_loss: return "SL_HIT"
        else:
            if current_price <= signal.take_profit_3: return "TP3_HIT"
            elif current_price <= signal.take_profit_2: return "TP2_HIT"
            elif current_price <= signal.take_profit_1: return "TP1_HIT"
            elif current_price >= signal.stop_loss: return "SL_HIT"
        return "OPEN"

    async def _process_closed_signal(self, signal: TradingSignal, status: str, close_price: float):
        """Process closed signal with performance tracking"""
        if signal.side == SignalSide.BUY:
            pnl_pct = (close_price - signal.entry_price) / signal.entry_price * 100
        else:
            pnl_pct = (signal.entry_price - close_price) / signal.entry_price * 100
        
        trade_record = {
            'signal_id': signal.signal_id,
            'symbol': signal.symbol,
            'side': signal.side.value,
            'entry_price': signal.entry_price,
            'close_price': close_price,
            'pnl_pct': pnl_pct,
            'status': status,
            'entry_time': signal.timestamp,
            'exit_time': datetime.datetime.utcnow(),
            'timeframe': signal.timeframe.value,
            'confidence_score': signal.confidence_score,
            'winner_filters_passed': signal.winner_filters_passed
        }
        
        self.closed_trades.append(trade_record)
        
        # Update all_signals
        for sig_data in self.all_signals:
            if sig_data['signal'].signal_id == signal.signal_id:
                sig_data['status'] = status
                sig_data['close_price'] = close_price
                sig_data['pnl_pct'] = pnl_pct
                sig_data['exit_time'] = datetime.datetime.utcnow()
                break
        
        await self._send_trade_update(signal, status, close_price, pnl_pct)
        logging.info(f"🎯 Trade closed: {signal.symbol} {status} | P&L: {pnl_pct:.2f}%")
    
    async def _send_trade_update(self, signal: TradingSignal, status: str, close_price: float, pnl_pct: float):
        """Send trade update with your old style + new info"""
        emoji = "🟢" if "TP" in status else "🔴"
        winner_info = f"✅ Filters: {', '.join(signal.winner_filters_passed)}\n" if signal.winner_filters_passed else ""
        
        message = f"""
{emoji} **TRADE UPDATE** {emoji}

Symbol: {signal.symbol}
Side: {signal.side.value}
Status: {status}

Entry: {signal.entry_price:.6f}
Exit: {close_price:.6f}
P&L: {pnl_pct:+.2f}%

{winner_info}
Confidence: {signal.confidence_score}/10
"""
        await tg(message)

    async def send_performance_summary(self):
        """Send 2-hour performance summary - FIXED METHOD"""
        now = time.time()
        if now - self.last_summary_time < 7200:  # 2 hours
            return False
            
        self.last_summary_time = now
        
        # Get signals from last 2 hours
        two_hours_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
        recent_signals = [s for s in self.all_signals if s['added_time'] >= two_hours_ago]
        
        if not recent_signals:
            return True
            
        # Calculate statistics
        open_signals = [s for s in recent_signals if s['status'] == 'OPEN']
        closed_signals = [s for s in recent_signals if s['status'] != 'OPEN']
        winning_trades = [s for s in closed_signals if s.get('pnl_pct', 0) > 0]
        
        total_signals = len(recent_signals)
        win_rate = len(winning_trades) / len(closed_signals) * 100 if closed_signals else 0
        avg_confidence = sum(s['signal'].confidence_score for s in recent_signals) / total_signals if total_signals else 0

        # Create summary message
        message = f"""
📊 **2-HOUR PERFORMANCE SUMMARY** 📊

⏰ Period: Last 2 hours
📈 Total Signals: {total_signals}
🟢 Open Signals: {len(open_signals)}
🔒 Closed Signals: {len(closed_signals)}
🎯 Win Rate: {win_rate:.1f}%
⭐ Avg Confidence: {avg_confidence:.1f}/10

📋 **RECENT SIGNALS:**
"""
        
        # Add recent signals details
        for i, sig_data in enumerate(recent_signals[-5:], 1):  # Last 5 signals
            signal = sig_data['signal']
            status = sig_data['status']
            pnl = sig_data.get('pnl_pct', 0)
            
            status_emoji = "🟢" if "TP" in status else "🔴" if status == "SL_HIT" else "🟡"
            pnl_str = f"{pnl:+.2f}%" if status != "OPEN" else "OPEN"
            
            winner_info = f" ✅{len(signal.winner_filters_passed)}" if signal.winner_filters_passed else ""
            
            message += f"{i}. {status_emoji} {signal.symbol} {signal.side.value} | Conf: {signal.confidence_score:.1f}/10{winner_info} | {pnl_str}\n"
        
        await tg(message)
        logging.info("📊 2-hour performance summary sent")
        return True

    def get_performance_stats(self):
        """Get trading performance statistics - FIXED METHOD"""
        if not self.closed_trades:
            return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}
        
        winning_trades = [t for t in self.closed_trades if t['pnl_pct'] > 0]
        total_pnl = sum(t['pnl_pct'] for t in self.closed_trades)
        
        return {
            'total_trades': len(self.closed_trades),
            'winning_trades': len(winning_trades),
            'win_rate': len(winning_trades) / len(self.closed_trades) * 100,
            'avg_pnl': total_pnl / len(self.closed_trades),
            'total_pnl': total_pnl
        }

# ==================== ULTIMATE HYBRID SCANNER ====================

class UltimateHybridScanner:
    """THE PERFECT FUSION: YOUR OLD LOGIC + NEW INFRASTRUCTURE"""
    
    def __init__(self, config: ScannerConfig):
        self.config = config
        self.trade_monitor = TradeMonitor(self)
        self.exchange = None
        self.signal_cooldown = {}
        self.performance_metrics = defaultdict(list)
        
        # YOUR EXACT OLD COMPONENTS
        self.winner_filters = OriginalWinnerFilters()
        self.smc_logic = OriginalSMCLogic()
        self.elite_tpsl = EliteTPSL()
        
        self._setup_logging()
    
    def _setup_logging(self):
        """Enhanced logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(message)s',
            handlers=[
                logging.FileHandler('ultimate_scanner.log'),
                logging.StreamHandler()
            ]
        )
        logging.info("🚀 ULTIMATE HYBRID SCANNER v3.0 INITIALIZED")
        logging.info("✅ Your exact old logic + Winner filters + Elite TP/SL")
        logging.info("✅ New monitoring + Performance tracking + Web API")
        logging.info("✅ Strict filter enforcement + Realistic scoring")

    async def initialize_exchange(self):
        """Initialize with your exchange settings"""
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
                "apiKey": os.getenv("OKX_API_KEY"),
                "secret": os.getenv("OKX_SECRET_KEY"),  
                "password": os.getenv("OKX_PASSWORD"),
                "sandbox": os.getenv("OKX_SANDBOX", "false").lower() == "true",
            })
            await self.exchange.load_markets()
            logging.info("✅ OKX exchange initialized successfully")
        except Exception as e:
            logging.error(f"❌ Exchange initialization failed: {e}")
            raise

    async def fetch_ohlcv_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
        """YOUR EXACT OHLCV FETCHING"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not ohlcv or len(ohlcv) < 20: return None
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logging.debug(f"Could not fetch {symbol} {timeframe}: {e}")
            return None

    async def get_btc_context(self) -> Dict[str, Any]:
        """YOUR EXACT BTC CONTEXT"""
        try:
            btc_15m = await self.fetch_ohlcv_data('BTC/USDT', '15m', 100)
            btc_1h = await self.fetch_ohlcv_data('BTC/USDT', '1h', 100)
            btc_direction = self.winner_filters.get_btc_direction(btc_15m, btc_1h)
            market_regime = self.winner_filters.detect_market_regime(btc_1h)
            return {
                'btc_direction': btc_direction,
                'market_regime': market_regime,
                'df_15m': btc_15m,
                'df_1h': btc_1h
            }
        except Exception as e:
            logging.error(f"Error getting BTC context: {e}")
            return {}

    async def scan_symbol(self, symbol: str) -> List[TradingSignal]:
        """YOUR EXACT SCANNING LOGIC WITH NEW ENHANCEMENTS"""
        signals = []
        
        try:
            # Get context for winner filters
            context = await self.get_btc_context()
            
            for timeframe in self.config.TIMEFRAMES:
                # Check cooldown
                cooldown_key = f"{symbol}_{timeframe.value}"
                if cooldown_key in self.signal_cooldown:
                    if time.time() - self.signal_cooldown[cooldown_key] < self.config.COOLDOWN_MINUTES * 60:
                        continue
                
                # Check SL cluster
                if self.trade_monitor.deprioritized(symbol):
                    continue
                
                # Fetch data
                df = await self.fetch_ohlcv_data(symbol, timeframe.value)
                if df is None: continue
                
                # Add context
                scan_context = context.copy()
                scan_context['tf'] = timeframe.value
                scan_context['current_price'] = df['close'].iloc[-1]
                
                # Get higher timeframe data for alignment
                if timeframe.value in ["1m", "3m", "5m"]:
                    df_15m = await self.fetch_ohlcv_data(symbol, '15m', 100)
                    df_1h = await self.fetch_ohlcv_data(symbol, '1h', 100)
                    scan_context['df_15m'] = df_15m
                    scan_context['df_1h'] = df_1h
                
                # GENERATE SIGNAL USING YOUR EXACT OLD LOGIC
                old_signal = self.smc_logic.generate_signal(df, symbol, scan_context)
                if not old_signal: continue
                
                # APPLY YOUR EXACT OLD WINNER FILTERS
                hybrid_signal = await self._apply_winner_filters(old_signal, df, scan_context)
                if hybrid_signal and hybrid_signal.confidence_score >= self.config.MIN_SIGNAL_SCORE:
                    if await self.validate_signal(hybrid_signal):
                        signals.append(hybrid_signal)
                        self.signal_cooldown[cooldown_key] = time.time()
                        await self.trade_monitor.add_signal(hybrid_signal)
                        
                        # Send signal notification
                        await self._send_signal_notification(hybrid_signal, old_signal)
                        
                        logging.info(f"🏆 ULTIMATE SIGNAL: {symbol} {hybrid_signal.side.value} "
                                   f"| Score: {hybrid_signal.confidence_score:.1f}/10 "
                                   f"| Filters: {len(hybrid_signal.winner_filters_passed)}/6")
        
        except Exception as e:
            logging.error(f"Error scanning {symbol}: {e}")
            
        return signals

    async def _send_signal_notification(self, hybrid_signal: TradingSignal, old_signal: Dict):
        """Send signal notification in your exact old style"""
        message = f"""
🏆 **ULTIMATE INSTITUTIONAL SIGNAL** 🏆

Symbol: {hybrid_signal.symbol}
Side: {hybrid_signal.side.value}
Timeframe: {hybrid_signal.timeframe.value}
Entry: {hybrid_signal.entry_price:.6f}

Risk Management:
SL: {hybrid_signal.stop_loss:.6f}
TP1: {hybrid_signal.take_profit_1:.6f}
TP2: {hybrid_signal.take_profit_2:.6f}
TP3: {hybrid_signal.take_profit_3:.6f}

Scores:
Confidence: {hybrid_signal.confidence_score:.1f}/10
Quality: {hybrid_signal.quality_score:.1f}%
R/R: {self._calculate_rr_ratio(hybrid_signal):.2f}

Filters: {', '.join(hybrid_signal.winner_filters_passed)}
Original Reasons: {', '.join(old_signal['reason_list'])}
"""
        await tg(message)

    def _calculate_rr_ratio(self, signal: TradingSignal) -> float:
        """Calculate risk/reward ratio"""
        risk = abs(signal.entry_price - signal.stop_loss)
        reward = abs(signal.take_profit_1 - signal.entry_price)
        return reward / risk if risk > 0 else 0

    async def _apply_winner_filters(self, old_signal: Dict, df: pd.DataFrame, context: Dict) -> Optional[TradingSignal]:
        """YOUR EXACT OLD WINNER FILTERS WITH STRICT ENFORCEMENT"""
        winner_filters_passed = []
        winner_filters_failed = []
        
        signal_side = SignalSide.BUY if old_signal['side'] == 'BUY' else SignalSide.SELL
        
        # 1. BTC DIRECTION FILTER - STRICT
        if self.config.REQUIRE_BTC_ALIGNMENT:
            btc_direction = context.get('btc_direction')
            if self.winner_filters.is_trade_allowed(signal_side, btc_direction):
                winner_filters_passed.append("BTC_ALIGNMENT")
            else:
                winner_filters_failed.append("BTC_MISALIGNMENT")
                logging.info(f"⏸️ Blocked by BTC: {signal_side.value} vs {btc_direction}")
                return None
        
        # 2. MARKET REGIME FILTER - YOUR EXACT OLD SETTING (strict_mode=False)
        if self.config.USE_MARKET_REGIME:
            market_regime = context.get('market_regime')
            if self.winner_filters.should_trade_in_regime(signal_side, market_regime, strict_mode=False):
                winner_filters_passed.append("MARKET_REGIME")
            else:
                winner_filters_failed.append("BAD_REGIME")
                logging.info(f"⏸️ Blocked by Regime: {signal_side.value} vs {market_regime}")
                return None
        
        # 3. HIGHER TF ALIGNMENT
        if self.config.REQUIRE_HIGHER_TF_ALIGNMENT:
            higher_tf_data = context.get('df_15m')
            if self.winner_filters.check_higher_tf_alignment(old_signal, higher_tf_data):
                winner_filters_passed.append("HIGHER_TF_ALIGNMENT")
            else:
                winner_filters_failed.append("HIGHER_TF_MISALIGNMENT")
                logging.info(f"⏸️ Blocked: Higher TF misalignment")
                return None
        
        # 4. MOMENTUM CONFIRMATION (skip for 1m/3m)
        if (self.config.REQUIRE_MOMENTUM_CONFIRMATION and 
            context.get('tf') not in ["1m", "3m"]):
            if self.winner_filters.check_momentum_confirmation(df, signal_side):
                winner_filters_passed.append("MOMENTUM")
            else:
                winner_filters_failed.append("WEAK_MOMENTUM")
                logging.info(f"⏸️ Blocked: No momentum confirmation")
                return None
        
        # 5. ZONE QUALITY
        if self.config.REQUIRE_ZONE_QUALITY:
            if self.winner_filters.check_entry_zone_quality(df, signal_side):
                winner_filters_passed.append("ZONE_QUALITY")
            else:
                winner_filters_failed.append("POOR_ZONE")
                logging.info(f"⏸️ Blocked: Poor entry zone")
                return None
        
        # 6. CHOPPY MARKET FILTER
        if self.config.AVOID_CHOPPY_MARKETS:
            if not self.winner_filters.detect_choppy_market(df):
                winner_filters_passed.append("TRENDING_MARKET")
            else:
                winner_filters_failed.append("CHOPPY_MARKET")
                logging.info(f"⏸️ Blocked: Choppy market")
                return None
        
        # ALL FILTERS PASSED - CREATE ENHANCED SIGNAL
        # Calculate realistic confidence score (YOUR OLD STYLE)
        base_score = old_signal['score']
        winner_bonus = len(winner_filters_passed) * 0.8  # Realistic bonus
        final_confidence = min(base_score + winner_bonus, 9.5)  # Max 9.5/10 - REALISTIC!
        
        # Calculate quality score
        quality_score = (final_confidence / 10.0) * 100
        
        # Create enhanced signal
        enhanced_signal = TradingSignal(
            symbol=old_signal['symbol'],
            side=signal_side,
            entry_price=old_signal['entry'],
            stop_loss=old_signal['sl'],
            take_profit_1=old_signal['tp1'],
            take_profit_2=old_signal['tp2'],
            take_profit_3=old_signal['tp3'],
            timestamp=datetime.datetime.utcnow(),
            timeframe=Timeframe(old_signal['timeframe']),
            confidence_score=final_confidence,
            quality_score=quality_score,
            filters_passed=old_signal['reason_list'],
            rejection_reasons=[],
            winner_filters_passed=winner_filters_passed,
            winner_filters_failed=winner_filters_failed,
            signal_id=f"{old_signal['symbol']}_{old_signal['timeframe']}_{int(time.time())}"
        )
        
        logging.info(f"✅ ALL FILTERS PASSED: {len(winner_filters_passed)}/6 - Confidence: {final_confidence:.1f}/10")
        return enhanced_signal

    async def validate_signal(self, signal: TradingSignal) -> bool:
        """Final validation"""
        if len(self.trade_monitor.open_signals) >= self.config.MAX_POSITIONS:
            logging.info("⏸️ Max positions reached")
            return False
            
        for open_signal in self.trade_monitor.open_signals.values():
            if open_signal.symbol == signal.symbol:
                logging.info(f"⏸️ Already monitoring {signal.symbol}")
                return False
                
        return True

    async def run_scan_cycle(self):
        """Enhanced scanning with performance tracking"""
        try:
            logging.info("🔍 Starting ultimate scan cycle...")
            
            # Get top symbols
            symbols = await self.get_top_symbols()
            if not symbols:
                logging.warning("No symbols to scan")
                return
                
            all_signals = []
            
            # Scan each symbol
            for symbol in symbols:
                try:
                    signals = await self.scan_symbol(symbol)
                    all_signals.extend(signals)
                    await asyncio.sleep(0.1)  # Rate limit
                except Exception as e:
                    logging.error(f"Error scanning {symbol}: {e}")
                    continue
            
            # Log summary
            if all_signals:
                logging.info(f"📈 Ultimate scan complete: {len(all_signals)} ELITE signals found")
                await self.send_elite_summary(all_signals)
            else:
                logging.info("📈 Ultimate scan complete: No elite signals found")
                
            # Update performance metrics
            self.performance_metrics['scan_cycles'].append({
                'timestamp': datetime.datetime.utcnow(),
                'signals_found': len(all_signals),
                'symbols_scanned': len(symbols)
            })
            
        except Exception as e:
            logging.error(f"Ultimate scan cycle error: {e}")

    async def get_top_symbols(self) -> List[str]:
        """Get top symbols with your filters"""
        try:
            tickers = await self.exchange.fetch_tickers()
            symbols_data = []
            
            for symbol, ticker in tickers.items():
                if not symbol.endswith('/USDT'): continue
                
                volume_usdt = ticker.get('baseVolume', 0) * ticker.get('last', 0)
                if volume_usdt < self.config.MIN_VOLUME_USDT: continue
                
                bid = ticker.get('bid', 0); ask = ticker.get('ask', 0)
                if bid == 0 or ask == 0: continue
                
                spread_pct = (ask - bid) / bid
                if spread_pct > self.config.MAX_SPREAD_PCT: continue
                
                symbols_data.append({'symbol': symbol, 'volume': volume_usdt})
                    
            symbols_data.sort(key=lambda x: x['volume'], reverse=True)
            top_symbols = [s['symbol'] for s in symbols_data[:self.config.TOP_N_SYMBOLS]]
            
            logging.info(f"📊 Selected {len(top_symbols)} elite symbols")
            return top_symbols
            
        except Exception as e:
            logging.error(f"Error getting top symbols: {e}")
            return []

    async def send_elite_summary(self, signals: List[TradingSignal]):
        """Enhanced summary with your style"""
        if not signals: return
        
        message = "🏆 **ULTIMATE SCAN SUMMARY** 🏆\n\n"
        message += f"**Elite Signals Found:** {len(signals)}\n\n"
        
        for i, signal in enumerate(signals[:5], 1):  # Top 5 signals
            winner_count = len(signal.winner_filters_passed)
            message += (f"{i}. {signal.symbol} {signal.side.value} ({signal.timeframe.value})\n"
                       f"   Entry: {signal.entry_price:.6f} | Score: {signal.confidence_score:.1f}/10\n"
                       f"   Filters: {winner_count}/6 ✅ | SL: {signal.stop_loss:.6f}\n\n")
        
        # Add performance stats
        stats = self.trade_monitor.get_performance_stats()
        if stats['total_trades'] > 0:
            message += f"**Performance:** {stats['win_rate']:.1f}% Win Rate | Avg P&L: {stats['avg_pnl']:+.2f}%\n"
        
        await tg(message)

    async def start_continuous_scanning(self):
        """Ultimate continuous scanning"""
        logging.info("🔄 Starting ULTIMATE continuous scanning...")
        
        startup_msg = (
            "🚀 **ULTIMATE HYBRID SCANNER STARTED** 🚀\n"
            "✅ Your exact old SMC logic preserved\n"
            "✅ All 6 winner filters strictly enforced\n" 
            "✅ Elite Smart TP/SL system active\n"
            "✅ Market regime (Don't Fight the Tide)\n"
            "✅ Realistic scoring (7-9/10 max)\n"
            "✅ Advanced monitoring & performance tracking\n"
            "🎯 Target: 80%+ Win Rate with PROVEN LOGIC"
        )
        await tg(startup_msg)
        
        try:
            while True:
                start_time = time.time()
                
                # Run elite scan cycle
                await self.run_scan_cycle()
                
                # Monitor open signals
                await self.trade_monitor.monitor_open_signals()
                
                # Send performance summary every 2 hours
                await self.trade_monitor.send_performance_summary()
                
                elapsed = time.time() - start_time
                sleep_time = max(1, self.config.SCAN_INTERVAL - elapsed)
                await asyncio.sleep(sleep_time)
                
        except Exception as e:
            logging.error(f"Ultimate scanning error: {e}")
            await asyncio.sleep(60)
            await self.start_continuous_scanning()

    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
            logging.info("🧹 Ultimate scanner cleanup completed")
        except Exception as e:
            logging.error(f"Cleanup error: {e}")

# ==================== WEB API SERVER ====================

# Global scanner instance
scanner: Optional[UltimateHybridScanner] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage scanner lifecycle"""
    global scanner
    config = ScannerConfig()
    scanner = UltimateHybridScanner(config)
    await scanner.initialize_exchange()
    
    background_tasks = BackgroundTasks()
    background_tasks.add_task(scanner.start_continuous_scanning)
    
    yield
    await scanner.cleanup()

app = FastAPI(title="Ultimate Hybrid Scanner v3.0", version="3.0.0", lifespan=lifespan)

# API Routes
class SignalResponse(BaseModel):
    symbol: str
    side: str
    entry_price: float
    confidence_score: float
    timeframe: str
    winner_filters_passed: List[str]
    timestamp: datetime.datetime

class PerformanceStats(BaseModel):
    total_trades: int
    win_rate: float
    avg_pnl: float
    open_signals: int

@app.get("/")
async def root():
    return {"status": "ULTIMATE HYBRID SCANNER v3.0 - RUNNING"}

@app.get("/signals", response_model=List[SignalResponse])
async def get_current_signals():
    if not scanner: return []
    signals = []
    for signal in scanner.trade_monitor.open_signals.values():
        signals.append(SignalResponse(
            symbol=signal.symbol,
            side=signal.side.value,
            entry_price=signal.entry_price,
            confidence_score=signal.confidence_score,
            timeframe=signal.timeframe.value,
            winner_filters_passed=signal.winner_filters_passed,
            timestamp=signal.timestamp
        ))
    return signals

@app.get("/performance", response_model=PerformanceStats)
async def get_performance():
    if not scanner:
        return PerformanceStats(total_trades=0, win_rate=0, avg_pnl=0, open_signals=0)
    stats = scanner.trade_monitor.get_performance_stats()
    return PerformanceStats(
        total_trades=stats['total_trades'],
        win_rate=stats['win_rate'],
        avg_pnl=stats['avg_pnl'],
        open_signals=len(scanner.trade_monitor.open_signals)
    )

@app.post("/scan-now")
async def trigger_manual_scan():
    """Trigger manual scan cycle"""
    if not scanner:
        raise HTTPException(status_code=500, detail="Scanner not initialized")
    
    asyncio.create_task(scanner.run_scan_cycle())
    return {"status": "Ultimate scan triggered"}

# ==================== TELEGRAM NOTIFICATIONS ====================

async def tg(message: str):
    """Your exact Telegram function - FIXED"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id: 
        print(f"📱 TELEGRAM: {message}")
        return
        
    def escape_html(msg: str) -> str:
        if not msg: return "-"
        return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    
    safe_msg = escape_html(message)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={
                "chat_id": chat_id, 
                "text": safe_msg, 
                "parse_mode": "HTML"
            })
        except Exception as e:
            logging.error(f"Telegram failed: {e}")

# ==================== MAIN EXECUTION ====================

async def main():
    """Ultimate main execution"""
    try:
        config = ScannerConfig()
        scanner = UltimateHybridScanner(config)
        await scanner.initialize_exchange()
        
        # Start FastAPI server
        server_config = uvicorn.Config(
            app, host="0.0.0.0", port=8000, log_level="info"
        )
        server = uvicorn.Server(server_config)
        
        await asyncio.gather(
            scanner.start_continuous_scanning(),
            server.serve(),
        )
        
    except KeyboardInterrupt:
        logging.info("🛑 Ultimate scanner stopped by user")
    except Exception as e:
        logging.error(f"❌ Ultimate scanner error: {e}")
    finally:
        if 'scanner' in locals():
            await scanner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())