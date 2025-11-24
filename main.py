#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🏆 ULTIMATE HYBRID SCANNER v3.1 - OLD FILTER STRICTNESS 🏆
- YOUR EXACT OLD FILTER LOGIC & SCORING
- NEW ARCHITECTURE + MONITORING
- OLD MOMENTUM FILTER APPLICABILITY 
- OLD SCORING SYSTEM (BASE + 5 BONUS)
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

class SignalSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

@dataclass
class ScannerConfig:
    # Core settings (from OLD system)
    SCAN_INTERVAL: int = 60
    TOP_N_SYMBOLS: int = 80  # OLD: 40
    MIN_VOLUME_USDT: float = 1000000
    MAX_SPREAD_PCT: float = 0.002
    
    # OLD FILTER SETTINGS
    MIN_SIGNAL_SCORE: int = 0  # OLD: No minimum score in filters
    COOLDOWN_MINUTES: int = 30
    MAX_SL_CLUSTER_HITS: int = 3
    
    # OLD WINNER FILTER SETTINGS (EXACT OLD BEHAVIOR)
    REQUIRE_BTC_ALIGNMENT: bool = True
    REQUIRE_HIGHER_TF_ALIGNMENT: bool = True
    REQUIRE_MOMENTUM_CONFIRMATION: bool = True
    REQUIRE_ZONE_QUALITY: bool = True
    AVOID_CHOPPY_MARKETS: bool = True
    USE_MARKET_REGIME: bool = False  # OLD: No market regime filter!
    
    # OLD SCORING
    WINNER_BONUS: int = 5  # OLD: Fixed +5 bonus

# ==================== YOUR EXACT OLD WINNER FILTERS ====================

class OriginalWinnerFilters:
    """YOUR EXACT ORIGINAL FILTERS - UNCHANGED"""
    
    @staticmethod
    def get_btc_direction(btc_15m: pd.DataFrame, btc_1h: pd.DataFrame) -> str:
        """YOUR EXACT BTC DIRECTION DETECTION"""
        if btc_15m is None or btc_1h is None or btc_15m.empty or btc_1h.empty: 
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
        except Exception as e:
            logging.error(f"BTC direction error: {e}")
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
        try:
            current_price = signal.get('entry', 0) if isinstance(signal, dict) else signal.entry_price
            higher_tf_ema_20 = higher_tf_data['close'].ewm(span=20).mean().iloc[-1]
            higher_tf_ema_50 = higher_tf_data['close'].ewm(span=50).mean().iloc[-1]
            
            signal_side = SignalSide(signal['side']) if isinstance(signal, dict) else signal.side
            
            if signal_side == SignalSide.BUY:
                return current_price > higher_tf_ema_20 and current_price > higher_tf_ema_50
            else:
                return current_price < higher_tf_ema_20 and current_price < higher_tf_ema_50
        except Exception as e:
            logging.error(f"Higher TF alignment error: {e}")
            return False

    @staticmethod
    def check_momentum_confirmation(df: pd.DataFrame, signal_direction: SignalSide) -> bool:
        """YOUR EXACT MOMENTUM CONFIRMATION"""
        if df is None or len(df) < 3: 
            return False
        try:
            current_candle = df.iloc[-1]
            prev_candle = df.iloc[-2]
            
            if signal_direction == SignalSide.BUY:
                return (current_candle['close'] > current_candle['open'] and 
                        current_candle['close'] > prev_candle['close'])
            else:
                return (current_candle['close'] < current_candle['open'] and
                        current_candle['close'] < prev_candle['close'])
        except Exception as e:
            logging.error(f"Momentum confirmation error: {e}")
            return False

    @staticmethod
    def check_entry_zone_quality(df: pd.DataFrame, signal_direction: SignalSide) -> bool:
        """YOUR EXACT ZONE QUALITY DETECTION"""
        if df is None or len(df) < 15: 
            return False
        try:
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
        except Exception as e:
            logging.error(f"Zone quality error: {e}")
            return False

    @staticmethod
    def detect_choppy_market(df: pd.DataFrame) -> bool:
        """YOUR EXACT MARKET CONDITION FILTER"""
        if df is None or len(df) < 25: 
            return True
        try:
            high, low, close = df['high'], df['low'], df['close']
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
            atr = true_range.rolling(14).mean().iloc[-1]
            
            current_price = close.iloc[-1]
            price_range_pct = (df['high'].tail(20).max() - df['low'].tail(20).min()) / current_price
            
            return (atr < (current_price * 0.002) and price_range_pct < 0.02)
        except Exception as e:
            logging.error(f"Choppy market detection error: {e}")
            return True

# ==================== YOUR EXACT OLD SMC CORE LOGIC ====================

class OriginalSMCLogic:
    """YOUR EXACT ORIGINAL SMC LOGIC - UNCHANGED"""
    
    @staticmethod
    def detect_swing_points(df: pd.DataFrame):
        if df is None or len(df) < 5: 
            return None
        last = df.iloc[-1]; prev = df.iloc[-3:-1]
        swing_high = last["high"] > prev["high"].max()
        swing_low = last["low"] < prev["low"].min()
        return swing_high, swing_low

    @staticmethod
    def detect_active_range(df: pd.DataFrame, lookback=10):
        if df is None or len(df) < lookback:
            return 0, 0
        last = df.iloc[-lookback:]
        return last["high"].max(), last["low"].min()

    @staticmethod
    def detect_sweep(df: pd.DataFrame):
        if df is None or len(df) < 6: 
            return False, False
        last = df.iloc[-1]; prev = df.iloc[-5:-1]
        return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

    @staticmethod
    def detect_bos_mss(df: pd.DataFrame):
        hh, ll = OriginalSMCLogic.detect_sweep(df)
        return hh, ll

    @staticmethod
    def detect_fvg(df: pd.DataFrame):
        if df is None or len(df) < 3: 
            return False, False
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
        bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
        return bull, bear

    @staticmethod
    def detect_order_blocks(df: pd.DataFrame):
        if df is None or len(df) < 3: 
            return None, None, None
        candle = df.iloc[-3]
        if candle["close"] > candle["open"]:
            return "bullish", candle["open"], candle["low"]
        return "bearish", candle["high"], candle["open"]

    @staticmethod
    def generate_signal(df: pd.DataFrame, symbol: str, context=None):
        """YOUR EXACT ORIGINAL SIGNAL GENERATION LOGIC"""
        if context is None: 
            context = {}
        
        if df is None or len(df) < 6: 
            return None

        tf = context.get("tf", "15m")

        try:
            last = df["close"].iloc[-1]

            ob_type, ob_hi, ob_lo = OriginalSMCLogic.detect_order_blocks(df)
            if ob_type is None: 
                return None

            bull_fvg, bear_fvg = OriginalSMCLogic.detect_fvg(df)
            sweep_h, sweep_l = OriginalSMCLogic.detect_sweep(df)
            bos_hh, bos_ll = OriginalSMCLogic.detect_bos_mss(df)

            if not (bos_hh or bos_ll): 
                return None

            score = 0
            reasons = []

            if ob_type == "bullish": 
                score += 2
                reasons.append("OB Bull +2")
            else: 
                score += 2
                reasons.append("OB Bear +2")

            if bull_fvg: 
                score += 2
                reasons.append("FVG Bull +2")
            elif bear_fvg: 
                score += 2
                reasons.append("FVG Bear +2")

            score += 2
            reasons.append("BOS +2")
            if sweep_h or sweep_l: 
                score += 1
                reasons.append("Sweep +1")
            else: 
                reasons.append("No Sweep +0")

            side = "BUY" if ob_type == "bullish" else "SELL"

            # Use OLD SIMPLE ATR TP/SL
            entry = float(last)
            sl, tp1, tp2, tp3 = OldSimpleTPSL.calculate_old_tp_sl(df, symbol, side, entry, context)

            return {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "score": score,
                "reason": "Set B SMC Signal + OLD TP/SL",
                "reason_list": reasons,
                "timeframe": tf
            }
        except Exception as e:
            logging.error(f"Signal generation error for {symbol}: {e}")
            return None

# ==================== OLD SIMPLE ATR TP/SL SYSTEM ====================

class OldSimpleTPSL:
    """YOUR EXACT OLD SIMPLE ATR TP/SL SYSTEM"""
    
    @staticmethod
    def calculate_old_tp_sl(df, symbol, side, entry, context):
        """YOUR EXACT OLD ATR-BASED TP/SL"""
        try:
            # OLD ATR CALCULATION
            atr_val = OldSimpleTPSL.old_atr(df, 14).iloc[-1] if df is not None and len(df) >= 14 else None
            
            # OLD TP/SL MULTIPLIERS
            tp_mult, sl_mult = 0.8, 1.0
            
            if atr_val and atr_val > 0:
                if side == "BUY":
                    sl = entry - sl_mult * atr_val
                    tp1 = entry + tp_mult * atr_val
                    tp2 = entry + tp_mult * 1.5 * atr_val
                    tp3 = entry + tp_mult * 2.5 * atr_val
                else:
                    sl = entry + sl_mult * atr_val
                    tp1 = entry - tp_mult * atr_val
                    tp2 = entry - tp_mult * 1.5 * atr_val
                    tp3 = entry - tp_mult * 2.5 * atr_val
            else:
                # OLD FALLBACK TO PERCENTAGE
                if side == "BUY":
                    sl = entry * 0.998
                    tp1 = entry * 1.004
                    tp2 = entry * 1.008  
                    tp3 = entry * 1.012
                else:
                    sl = entry * 1.002
                    tp1 = entry * 0.996
                    tp2 = entry * 0.992
                    tp3 = entry * 0.988

            # OLD SL VALIDATION
            if sl == entry:
                sl = entry - entry * 0.002 if side == "BUY" else entry + entry * 0.002

            return sl, tp1, tp2, tp3
        except Exception as e:
            logging.error(f"TP/SL calculation error: {e}")
            # Fallback values
            if side == "BUY":
                return entry * 0.998, entry * 1.004, entry * 1.008, entry * 1.012
            else:
                return entry * 1.002, entry * 0.996, entry * 0.992, entry * 0.988

    @staticmethod
    def old_atr(df: pd.DataFrame, period=14):
        """YOUR EXACT OLD ATR CALCULATION"""
        if df is None or len(df) < period:
            return pd.Series([0] * len(df) if df is not None else [0])
        
        high, low, close = df["high"], df["low"], df["close"]
        tr = pd.DataFrame({
            "h-l": high - low,
            "h-pc": (high - close.shift(1)).abs(),
            "l-pc": (low - close.shift(1)).abs()
        }).max(axis=1)
        return tr.rolling(period, min_periods=1).mean()

# ==================== ENHANCED DATA MODELS ====================

@dataclass
class TradingSignal:
    """Enhanced signal with OLD scoring + new tracking"""
    symbol: str
    side: SignalSide
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: float
    timestamp: datetime.datetime
    timeframe: str
    
    # OLD scoring system
    base_score: int  # OLD SMC score (4-7)
    final_score: int  # OLD base_score + 5 bonus
    filters_passed: List[str]
    rejection_reasons: List[str]
    
    # OLD winner filters tracking
    winner_filters_passed: List[str]
    winner_filters_failed: List[str]
    
    # Metadata
    signal_id: str
    version: str = "3.1-OLD-FILTERS"

# ==================== OLD-STYLE FILTER APPLICATION ====================

class OldFilterApplicator:
    """APPLIES FILTERS EXACTLY LIKE OLD CODE"""
    
    @staticmethod
    async def apply_old_filters(old_signal: Dict, df: pd.DataFrame, context: Dict, config: ScannerConfig) -> Tuple[bool, List[str], List[str], List[str]]:
        """EXACT OLD CODE FILTER LOGIC"""
        winner_filters_passed = []
        winner_filters_failed = []
        filters_failed_reasons = []
        
        signal_side = SignalSide.BUY if old_signal['side'] == 'BUY' else SignalSide.SELL
        tf = context.get('tf', '15m')
        
        # OLD-STYLE: Start with filters_passed = True, set to False if any critical filter fails
        filters_passed = True
        
        # 1. BTC DIRECTION FILTER - OLD STYLE
        if config.REQUIRE_BTC_ALIGNMENT:
            btc_direction = context.get('btc_direction', 'NEUTRAL')
            if OriginalWinnerFilters.is_trade_allowed(signal_side, btc_direction):
                winner_filters_passed.append("BTC_ALIGNMENT")
            else:
                filters_passed = False
                filters_failed_reasons.append(f"BTC {btc_direction} misalignment")
                winner_filters_failed.append("BTC_MISALIGNMENT")
                logging.info(f"⏸️ OLD-STYLE Blocked: {signal_side.value} vs BTC {btc_direction}")
        
        # Continue checking other filters ONLY if filters_passed is still True
        if filters_passed and config.REQUIRE_HIGHER_TF_ALIGNMENT:
            higher_tf_data = context.get('df_15m')
            if OriginalWinnerFilters.check_higher_tf_alignment(old_signal, higher_tf_data):
                winner_filters_passed.append("HIGHER_TF_ALIGNMENT")
            else:
                filters_passed = False
                filters_failed_reasons.append("Higher TF misalignment")
                winner_filters_failed.append("HIGHER_TF_MISALIGNMENT")
                logging.info(f"⏸️ OLD-STYLE Blocked: Higher TF misalignment")
        
        # 3. MOMENTUM CONFIRMATION - OLD APPLICABILITY (skip for 1m/3m)
        if (filters_passed and config.REQUIRE_MOMENTUM_CONFIRMATION and 
            tf not in ["1m", "3m"]):  # OLD: Skip for 1m, 3m
            if OriginalWinnerFilters.check_momentum_confirmation(df, signal_side):
                winner_filters_passed.append("MOMENTUM")
            else:
                filters_passed = False
                filters_failed_reasons.append("No momentum confirmation")
                winner_filters_failed.append("WEAK_MOMENTUM")
                logging.info(f"⏸️ OLD-STYLE Blocked: No momentum confirmation")
        
        # 4. ZONE QUALITY
        if filters_passed and config.REQUIRE_ZONE_QUALITY:
            if OriginalWinnerFilters.check_entry_zone_quality(df, signal_side):
                winner_filters_passed.append("ZONE_QUALITY")
            else:
                filters_passed = False
                filters_failed_reasons.append("Poor entry zone")
                winner_filters_failed.append("POOR_ZONE")
                logging.info(f"⏸️ OLD-STYLE Blocked: Poor entry zone")
        
        # 5. CHOPPY MARKET FILTER
        if filters_passed and config.AVOID_CHOPPY_MARKETS:
            if not OriginalWinnerFilters.detect_choppy_market(df):
                winner_filters_passed.append("TRENDING_MARKET")
            else:
                filters_passed = False
                filters_failed_reasons.append("Choppy market")
                winner_filters_failed.append("CHOPPY_MARKET")
                logging.info(f"⏸️ OLD-STYLE Blocked: Choppy market")
        
        return filters_passed, winner_filters_passed, winner_filters_failed, filters_failed_reasons

# ==================== TRADE MONITORING SYSTEM ====================

class TradeMonitor:
    """Advanced monitoring with OLD signal handling"""
    
    def __init__(self, scanner):
        self.scanner = scanner
        self.open_signals = {}
        self.closed_trades = []
        self.all_signals = []
        self.last_summary_time = time.time()
        self.recent_sl = defaultdict(lambda: deque())
        
    async def add_signal(self, signal: TradingSignal):
        """Add OLD-style signal to monitoring"""
        self.open_signals[signal.signal_id] = signal
        self.all_signals.append({
            'signal': signal,
            'status': 'OPEN',
            'added_time': datetime.datetime.utcnow()
        })
        logging.info(f"📈 OLD Monitoring: {signal.symbol} {signal.side.value} | Final Score: {signal.final_score}")
        
    def record_sl_hit(self, symbol: str, lookback_minutes=30):
        """YOUR EXACT OLD SL-CLUSTER LOGIC"""
        now = time.time()
        dq = self.recent_sl[symbol]
        dq.append(now)
        cutoff = now - lookback_minutes * 60
        while dq and dq[0] < cutoff: 
            dq.popleft()
        
    def deprioritized(self, symbol: str, threshold=3, lookback=30):
        """YOUR EXACT OLD DEPRIORITIZATION LOGIC"""
        dq = self.recent_sl[symbol]
        now = time.time()
        cutoff = now - lookback * 60
        while dq and dq[0] < cutoff: 
            dq.popleft()
        return len(dq) >= threshold

    async def monitor_open_signals(self):
        """Monitor OLD signals"""
        if not self.open_signals: 
            return
        
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
            if signal_id in self.open_signals:
                del self.open_signals[signal_id]

    async def check_signal_status(self, signal: TradingSignal, current_price: float):
        """Check TP/SL hits for OLD signals"""
        try:
            if signal.side == SignalSide.BUY:
                if current_price >= signal.take_profit_3: 
                    return "TP3_HIT"
                elif current_price >= signal.take_profit_2: 
                    return "TP2_HIT"
                elif current_price >= signal.take_profit_1: 
                    return "TP1_HIT"
                elif current_price <= signal.stop_loss: 
                    return "SL_HIT"
            else:
                if current_price <= signal.take_profit_3: 
                    return "TP3_HIT"
                elif current_price <= signal.take_profit_2: 
                    return "TP2_HIT"
                elif current_price <= signal.take_profit_1: 
                    return "TP1_HIT"
                elif current_price >= signal.stop_loss: 
                    return "SL_HIT"
            return "OPEN"
        except Exception as e:
            logging.error(f"Signal status check error: {e}")
            return "OPEN"

    async def _process_closed_signal(self, signal: TradingSignal, status: str, close_price: float):
        """Process closed OLD signal"""
        try:
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
                'timeframe': signal.timeframe,
                'final_score': signal.final_score,
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
            logging.info(f"🎯 OLD Trade closed: {signal.symbol} {status} | P&L: {pnl_pct:.2f}% | Score: {signal.final_score}")
        except Exception as e:
            logging.error(f"Process closed signal error: {e}")

    async def _send_trade_update(self, signal: TradingSignal, status: str, close_price: float, pnl_pct: float):
        """Send OLD-style trade update"""
        try:
            emoji = "🟢" if "TP" in status else "🔴"
            winner_info = f"✅ Filters: {', '.join(signal.winner_filters_passed)}\n" if signal.winner_filters_passed else ""
            
            message = f"""
{emoji} **OLD-STYLE TRADE UPDATE** {emoji}

Symbol: {signal.symbol}
Side: {signal.side.value}
Status: {status}

Entry: {signal.entry_price:.6f}
Exit: {close_price:.6f}
P&L: {pnl_pct:+.2f}%

{winner_info}
OLD Final Score: {signal.final_score}
"""
            await send_telegram_message(message)
        except Exception as e:
            logging.error(f"Send trade update error: {e}")

    async def send_performance_summary(self):
        """Send 2-hour performance summary"""
        try:
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
            avg_final_score = sum(s['signal'].final_score for s in recent_signals) / total_signals if total_signals else 0

            # Create OLD-style summary message
            message = f"""
📊 **OLD-STYLE 2-HOUR PERFORMANCE** 📊

⏰ Period: Last 2 hours
📈 Total Signals: {total_signals}
🟢 Open Signals: {len(open_signals)}
🔒 Closed Signals: {len(closed_signals)}
🎯 Win Rate: {win_rate:.1f}%
⭐ Avg Final Score: {avg_final_score:.1f}

📋 **RECENT OLD-STYLE SIGNALS:**
"""
            
            # Add recent signals details
            for i, sig_data in enumerate(recent_signals[-5:], 1):
                signal = sig_data['signal']
                status = sig_data['status']
                pnl = sig_data.get('pnl_pct', 0)
                
                status_emoji = "🟢" if "TP" in status else "🔴" if status == "SL_HIT" else "🟡"
                pnl_str = f"{pnl:+.2f}%" if status != "OPEN" else "OPEN"
                
                winner_info = f" ✅{len(signal.winner_filters_passed)}" if signal.winner_filters_passed else ""
                
                message += f"{i}. {status_emoji} {signal.symbol} {signal.side.value} | Final: {signal.final_score}{winner_info} | {pnl_str}\n"
            
            await send_telegram_message(message)
            logging.info("📊 OLD-STYLE 2-hour performance summary sent")
            return True
        except Exception as e:
            logging.error(f"Performance summary error: {e}")
            return False

    def get_performance_stats(self):
        """Get OLD-style performance statistics"""
        try:
            if not self.closed_trades:
                return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}
            
            winning_trades = [t for t in self.closed_trades if t['pnl_pct'] > 0]
            total_pnl = sum(t['pnl_pct'] for t in self.closed_trades)
            
            return {
                'total_trades': len(self.closed_trades),
                'winning_trades': len(winning_trades),
                'win_rate': len(winning_trades) / len(self.closed_trades) * 100 if self.closed_trades else 0,
                'avg_pnl': total_pnl / len(self.closed_trades) if self.closed_trades else 0,
                'total_pnl': total_pnl
            }
        except Exception as e:
            logging.error(f"Performance stats error: {e}")
            return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}

# ==================== ULTIMATE HYBRID SCANNER ====================

class UltimateHybridScanner:
    """PERFECT FUSION: OLD FILTERS + NEW ARCHITECTURE"""
    
    def __init__(self, config: ScannerConfig):
        self.config = config
        self.trade_monitor = TradeMonitor(self)
        self.exchange = None
        self.signal_cooldown = {}
        
        # YOUR EXACT OLD COMPONENTS
        self.winner_filters = OriginalWinnerFilters()
        self.smc_logic = OriginalSMCLogic()
        self.old_tpsl = OldSimpleTPSL()
        self.old_filters = OldFilterApplicator()
        
        self._setup_logging()
    
    def _setup_logging(self):
        """Enhanced logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        logging.info("🚀 ULTIMATE HYBRID SCANNER v3.1 - OLD FILTERS INITIALIZED")
        logging.info("✅ Your exact old filter strictness & scoring")
        logging.info("✅ Old momentum filter applicability (skip 1m/3m)")
        logging.info("✅ Old scoring: base SMC + 5 bonus")
        logging.info("✅ Old simple ATR TP/SL system")

    async def initialize_exchange(self):
        """Initialize with your exchange settings"""
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
            })
            await self.exchange.load_markets()
            logging.info("✅ OKX exchange initialized successfully")
            return True
        except Exception as e:
            logging.error(f"❌ Exchange initialization failed: {e}")
            return False

    async def fetch_ohlcv_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
        """YOUR EXACT OHLCV FETCHING"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not ohlcv or len(ohlcv) < 20: 
                return None
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
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
            return {
                'btc_direction': btc_direction,
                'df_15m': btc_15m,
                'df_1h': btc_1h
            }
        except Exception as e:
            logging.error(f"Error getting BTC context: {e}")
            return {'btc_direction': 'NEUTRAL'}

    async def scan_symbol(self, symbol: str) -> List[TradingSignal]:
        """OLD-STYLE SCANNING WITH OLD FILTER STRICTNESS"""
        signals = []
        
        try:
            # Get context for winner filters
            context = await self.get_btc_context()
            
            # Define timeframes to scan (OLD STYLE)
            timeframes = ["1m", "3m", "5m", "15m", "30m"]
            
            for tf in timeframes:
                # Check cooldown (OLD STYLE)
                cooldown_key = f"{symbol}_{tf}"
                if cooldown_key in self.signal_cooldown:
                    if time.time() - self.signal_cooldown[cooldown_key] < self.config.COOLDOWN_MINUTES * 60:
                        continue
                
                # Check SL cluster (OLD STYLE)
                if self.trade_monitor.deprioritized(symbol):
                    continue
                
                # Fetch data
                df = await self.fetch_ohlcv_data(symbol, tf)
                if df is None: 
                    continue
                
                # Add context
                scan_context = context.copy()
                scan_context['tf'] = tf
                scan_context['current_price'] = df['close'].iloc[-1]
                
                # Get higher timeframe data for alignment (OLD STYLE)
                if tf in ["1m", "3m", "5m"]:
                    df_15m = await self.fetch_ohlcv_data(symbol, '15m', 100)
                    df_1h = await self.fetch_ohlcv_data(symbol, '1h', 100)
                    scan_context['df_15m'] = df_15m
                    scan_context['df_1h'] = df_1h
                
                # GENERATE SIGNAL USING YOUR EXACT OLD LOGIC
                old_signal = self.smc_logic.generate_signal(df, symbol, scan_context)
                if not old_signal: 
                    continue
                
                # APPLY YOUR EXACT OLD FILTER STRICTNESS
                hybrid_signal = await self._apply_old_style_filters(old_signal, df, scan_context)
                if hybrid_signal:
                    if await self._validate_signal(hybrid_signal):
                        signals.append(hybrid_signal)
                        self.signal_cooldown[cooldown_key] = time.time()
                        await self.trade_monitor.add_signal(hybrid_signal)
                        
                        # Send signal notification
                        await self._send_signal_notification(hybrid_signal, old_signal)
                        
                        logging.info(f"🏆 OLD-STYLE SIGNAL: {symbol} {hybrid_signal.side.value} "
                                   f"| Base: {hybrid_signal.base_score} "
                                   f"| Final: {hybrid_signal.final_score} "
                                   f"| Filters: {len(hybrid_signal.winner_filters_passed)}")
        
        except Exception as e:
            logging.error(f"Error scanning {symbol}: {e}")
            
        return signals

    async def _apply_old_style_filters(self, old_signal: Dict, df: pd.DataFrame, context: Dict) -> Optional[TradingSignal]:
        """APPLY FILTERS EXACTLY LIKE OLD CODE"""
        try:
            # OLD-STYLE FILTER APPLICATION
            filters_passed, winner_filters_passed, winner_filters_failed, filter_reasons = (
                await self.old_filters.apply_old_filters(old_signal, df, context, self.config)
            )
            
            # OLD-STYLE: Only proceed if filters_passed is True
            if not filters_passed:
                return None
            
            # OLD SCORING SYSTEM: base_score + 5 fixed bonus
            base_score = old_signal['score']  # OLD SMC score (4-7)
            final_score = base_score + self.config.WINNER_BONUS  # OLD: Fixed +5 bonus
            
            # Create enhanced signal with OLD scoring
            enhanced_signal = TradingSignal(
                symbol=old_signal['symbol'],
                side=SignalSide.BUY if old_signal['side'] == 'BUY' else SignalSide.SELL,
                entry_price=old_signal['entry'],
                stop_loss=old_signal['sl'],
                take_profit_1=old_signal['tp1'],
                take_profit_2=old_signal['tp2'],
                take_profit_3=old_signal['tp3'],
                timestamp=datetime.datetime.utcnow(),
                timeframe=old_signal['timeframe'],
                base_score=base_score,  # OLD SMC score
                final_score=final_score,  # OLD final score
                filters_passed=old_signal['reason_list'],
                rejection_reasons=filter_reasons,
                winner_filters_passed=winner_filters_passed,
                winner_filters_failed=winner_filters_failed,
                signal_id=f"{old_signal['symbol']}_{old_signal['timeframe']}_{int(time.time())}"
            )
            
            logging.info(f"✅ OLD FILTERS PASSED: {len(winner_filters_passed)} - Score: {base_score} + {self.config.WINNER_BONUS} = {final_score}")
            return enhanced_signal
        except Exception as e:
            logging.error(f"Apply old filters error: {e}")
            return None

    async def _send_signal_notification(self, hybrid_signal: TradingSignal, old_signal: Dict):
        """Send signal notification in OLD STYLE"""
        try:
            message = f"""
🏆 **OLD-STYLE INSTITUTIONAL SIGNAL** 🏆

Symbol: {hybrid_signal.symbol}
Side: {hybrid_signal.side.value}
Timeframe: {hybrid_signal.timeframe}
Entry: {hybrid_signal.entry_price:.6f}

Risk Management:
SL: {hybrid_signal.stop_loss:.6f}
TP1: {hybrid_signal.take_profit_1:.6f}
TP2: {hybrid_signal.take_profit_2:.6f}
TP3: {hybrid_signal.take_profit_3:.6f}

OLD SCORING:
Base SMC: {hybrid_signal.base_score}
Winner Bonus: +{self.config.WINNER_BONUS}
FINAL SCORE: {hybrid_signal.final_score}

Filters: {', '.join(hybrid_signal.winner_filters_passed)}
Original Reasons: {', '.join(old_signal['reason_list'])}
"""
            await send_telegram_message(message)
        except Exception as e:
            logging.error(f"Send signal notification error: {e}")

    async def _validate_signal(self, signal: TradingSignal) -> bool:
        """Final validation"""
        try:
            # Check if we're already monitoring this symbol
            for open_signal in self.trade_monitor.open_signals.values():
                if open_signal.symbol == signal.symbol:
                    logging.info(f"⏸️ Already monitoring {signal.symbol}")
                    return False
                    
            return True
        except Exception as e:
            logging.error(f"Signal validation error: {e}")
            return False

    async def get_top_symbols(self) -> List[str]:
        """Get top symbols with your filters"""
        try:
            tickers = await self.exchange.fetch_tickers()
            symbols_data = []
            
            for symbol, ticker in tickers.items():
                if not symbol.endswith('/USDT'): 
                    continue
                
                volume_usdt = ticker.get('baseVolume', 0) * ticker.get('last', 0)
                if volume_usdt < self.config.MIN_VOLUME_USDT: 
                    continue
                
                bid = ticker.get('bid', 0)
                ask = ticker.get('ask', 0)
                if bid == 0 or ask == 0: 
                    continue
                
                spread_pct = (ask - bid) / bid
                if spread_pct > self.config.MAX_SPREAD_PCT: 
                    continue
                
                symbols_data.append({'symbol': symbol, 'volume': volume_usdt})
                    
            symbols_data.sort(key=lambda x: x['volume'], reverse=True)
            top_symbols = [s['symbol'] for s in symbols_data[:self.config.TOP_N_SYMBOLS]]
            
            logging.info(f"📊 Selected {len(top_symbols)} elite symbols")
            return top_symbols
            
        except Exception as e:
            logging.error(f"Error getting top symbols: {e}")
            return []

    async def run_scan_cycle(self):
        """Enhanced scanning with performance tracking"""
        try:
            logging.info("🔍 Starting OLD-STYLE scan cycle...")
            
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
                logging.info(f"📈 OLD-STYLE scan complete: {len(all_signals)} ELITE signals found")
            else:
                logging.info("📈 OLD-STYLE scan complete: No elite signals found")
                
        except Exception as e:
            logging.error(f"OLD-STYLE scan cycle error: {e}")

    async def start_continuous_scanning(self):
        """Ultimate continuous scanning"""
        logging.info("🔄 Starting OLD-STYLE continuous scanning...")
        
        startup_msg = (
            "🚀 **OLD-STYLE HYBRID SCANNER STARTED** 🚀\n"
            "✅ Your exact old SMC logic preserved\n"
            "✅ All winner filters with OLD strictness\n" 
            "✅ Old simple ATR TP/SL system active\n"
            "✅ Momentum filter skips 1m/3m (OLD behavior)\n"
            "✅ Old scoring: base + 5 fixed bonus\n"
            "✅ Advanced monitoring & performance tracking\n"
            "🎯 Target: HIGH WIN RATE WITH PROVEN OLD LOGIC"
        )
        await send_telegram_message(startup_msg)
        
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
            logging.error(f"OLD-STYLE scanning error: {e}")
            await asyncio.sleep(60)

    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
            logging.info("🧹 OLD-STYLE scanner cleanup completed")
        except Exception as e:
            logging.error(f"Cleanup error: {e}")

# ==================== TELEGRAM NOTIFICATIONS ====================

async def send_telegram_message(message: str):
    """Your exact Telegram function - FIXED"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id: 
        print(f"📱 TELEGRAM: {message}")
        return
        
    def escape_html(msg: str) -> str:
        if not msg: 
            return "-"
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

# ==================== WEB API SERVER ====================

# Global scanner instance
scanner: Optional[UltimateHybridScanner] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage scanner lifecycle"""
    global scanner
    config = ScannerConfig()
    scanner = UltimateHybridScanner(config)
    success = await scanner.initialize_exchange()
    
    if success:
        background_tasks = BackgroundTasks()
        background_tasks.add_task(scanner.start_continuous_scanning)
    
    yield
    
    if scanner:
        await scanner.cleanup()

app = FastAPI(title="Ultimate Hybrid Scanner v3.1 - OLD FILTERS", version="3.1.0", lifespan=lifespan)

# API Routes
class SignalResponse(BaseModel):
    symbol: str
    side: str
    entry_price: float
    base_score: int
    final_score: int
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
    return {"status": "ULTIMATE HYBRID SCANNER v3.1 - OLD FILTERS - RUNNING"}

@app.get("/signals", response_model=List[SignalResponse])
async def get_current_signals():
    if not scanner: 
        return []
    signals = []
    for signal in scanner.trade_monitor.open_signals.values():
        signals.append(SignalResponse(
            symbol=signal.symbol,
            side=signal.side.value,
            entry_price=signal.entry_price,
            base_score=signal.base_score,
            final_score=signal.final_score,
            timeframe=signal.timeframe,
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
    """Trigger manual OLD-style scan cycle"""
    if not scanner:
        raise HTTPException(status_code=500, detail="Scanner not initialized")
    
    asyncio.create_task(scanner.run_scan_cycle())
    return {"status": "OLD-style scan triggered"}

# ==================== MAIN EXECUTION ====================

async def main():
    """Ultimate main execution with OLD filters"""
    try:
        config = ScannerConfig()
        scanner = UltimateHybridScanner(config)
        success = await scanner.initialize_exchange()
        
        if not success:
            logging.error("❌ Failed to initialize exchange. Exiting.")
            return
        
        # Start the scanner
        await scanner.start_continuous_scanning()
        
    except KeyboardInterrupt:
        logging.info("🛑 Ultimate OLD-filter scanner stopped by user")
    except Exception as e:
        logging.error(f"❌ Ultimate OLD-filter scanner error: {e}")
    finally:
        if 'scanner' in locals():
            await scanner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())