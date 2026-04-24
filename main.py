#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v5.0 - LIQUIDITY + DIRECTION ENGINE
Professional trading with forced move anticipation
DIRECTION LAYERS: Liquidity → Trapped → Bleeding → Micro Confirmation
"""

import os
import time
import asyncio
import logging
import datetime
import json
import math
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum

# ============ ENUMS ============
class DirectionTier(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM" 
    LOW = "LOW"

class TrappedSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"
    CONFLICT = "CONFLICT"

class MicroConfirmationType(str, Enum):
    WICK_REJECTION = "WICK_REJECTION"
    ABSORPTION = "ABSORPTION"
    BREAKOUT = "BREAKOUT"
    NONE = "NONE"

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v5_0.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))
TOP_N = int(os.getenv("TOP_N", 60))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 1))

# Direction Engine thresholds
MIN_DIRECTION_CONFIDENCE = float(os.getenv("MIN_DIRECTION_CONFIDENCE", 0.4))
FUNDING_EXTREME_THRESHOLD = float(os.getenv("FUNDING_EXTREME_THRESHOLD", 0.03))
OI_ACCUMULATION_THRESHOLD = float(os.getenv("OI_ACCUMULATION_THRESHOLD", 0.15))

# Signal thresholds
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 4.0))

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 48))

# Rate limiting settings
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 4))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 2.5))

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_v5_0")

# ============ DATA STRUCTURES ============
@dataclass
class InstitutionalData:
    """Exchange-specific institutional data"""
    open_interest: float = 0.0
    oi_change_24h: float = 0.0
    oi_change_1h: float = 0.0
    oi_timestamp: Optional[datetime.datetime] = None
    
    funding_rate: float = 0.0
    funding_history: List[float] = field(default_factory=list)
    funding_timestamp: Optional[datetime.datetime] = None
    
    basis_rate: float = 0.0
    perpetual_premium: float = 0.0
    
    top_bid_size: float = 0.0
    top_ask_size: float = 0.0
    bid_ask_ratio: float = 0.0
    
    liquidation_zones: Dict[str, List[float]] = field(default_factory=dict)
    
    @property
    def is_funding_extreme(self) -> bool:
        return abs(self.funding_rate) > FUNDING_EXTREME_THRESHOLD
    
    @property
    def funding_bleeding_side(self) -> str:
        if self.funding_rate > FUNDING_EXTREME_THRESHOLD:
            return "LONG"
        elif self.funding_rate < -FUNDING_EXTREME_THRESHOLD:
            return "SHORT"
        return ""

@dataclass
class DirectionMetrics:
    """Institutional direction signals"""
    trapped_side: TrappedSide = TrappedSide.NONE
    trapped_confidence: float = 0.0
    trapped_details: List[Dict] = field(default_factory=list)
    
    bleeding_side: str = ""
    funding_extreme: float = 0.0
    funding_analysis: Dict = field(default_factory=dict)
    
    micro_confirmation: bool = False
    micro_timeframe: str = ""
    rejection_type: MicroConfirmationType = MicroConfirmationType.NONE
    micro_details: Dict = field(default_factory=dict)
    
    orderbook_imbalance: float = 0.0
    
    direction_score: float = 0.0
    confidence_tier: DirectionTier = DirectionTier.LOW
    
    conflict_warnings: List[str] = field(default_factory=list)
    
    @property
    def is_high_confidence(self) -> bool:
        return (self.confidence_tier == DirectionTier.HIGH and 
                abs(self.direction_score) > 0.7)
    
    @property
    def has_major_conflicts(self) -> bool:
        return len(self.conflict_warnings) >= 2

@dataclass
class EnhancedSetup:
    """Enhanced setup with direction engine"""
    symbol: str = ""
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.utcnow)
    side: str = ""
    current_price: float = 0.0
    entry_price: float = 0.0
    entry_type: str = ""
    sl_price: float = 0.0
    tp_targets: List[float] = field(default_factory=list)
    tp_sources: List[Dict] = field(default_factory=list)
    risk: float = 0.0
    reward: float = 0.0
    rr_ratio: float = 0.0
    
    quality_tier: str = ""
    quality_score: float = 0.0
    eight_steps_status: Dict = field(default_factory=dict)
    
    liquidity_analysis: Dict = field(default_factory=dict)
    
    direction_metrics: DirectionMetrics = field(default_factory=DirectionMetrics)
    
    @property
    def weighted_score(self) -> float:
        base_score = self.quality_score
        tier_multiplier = {
            DirectionTier.HIGH: 1.5,
            DirectionTier.MEDIUM: 1.2,
            DirectionTier.LOW: 0.8
        }.get(self.direction_metrics.confidence_tier, 1.0)
        direction_bonus = abs(self.direction_metrics.direction_score) * 0.2 * base_score
        return (base_score * tier_multiplier) + direction_bonus
    
    @property
    def forced_move_probability(self) -> str:
        if self.direction_metrics.is_high_confidence:
            return "HIGH"
        elif self.direction_metrics.confidence_tier == DirectionTier.MEDIUM:
            return "MODERATE"
        return "LOW"

@dataclass
class SetupEligibility:
    eligible: bool = False
    side: str = ""
    entry_price: float = 0.0
    entry_type: str = ""
    disqualify_reason: str = ""

@dataclass
class LiquiditySetup:
    sl_price: float = 0.0
    tp_targets: List[float] = None
    tp_sources: List[Dict] = None
    liquidity_analysis: Dict = None
    rr_ratio: float = 0.0

@dataclass
class SetupQuality:
    sweep_strength: float = 0.0
    structure_shift: bool = False
    from_liquidity_exists: bool = False
    confirmation_candle: bool = False
    htfc_alignment_score: float = 0.0
    total_score: float = 0.0
    eight_steps_status: Dict = None
    
    @property
    def quality_tier(self) -> str:
        if self.total_score >= 4.0:
            return "A+"
        elif self.total_score >= 3.0:
            return "A"
        elif self.total_score >= 2.5:
            return "B"
        else:
            return "C"

# ---------------- ENHANCED RATE LIMITER ----------------
class EnhancedRateLimiter:
    def __init__(self):
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.max_concurrent = MAX_CONCURRENT
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.general_requests = []
        self.funding_requests = []
        self.oi_requests = []
        self.min_delay = 0.25
        self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR
        self.max_retries = RATE_LIMIT_RETRIES
        
    async def wait_for_endpoint(self, endpoint_type: str = "general"):
        now = time.time()
        if endpoint_type == "funding":
            request_list = self.funding_requests
            cooldown = 1.5
        elif endpoint_type == "oi":
            request_list = self.oi_requests
            cooldown = 2.0
        else:
            request_list = self.general_requests
            cooldown = 1.0
        
        request_list[:] = [t for t in request_list if now - t < cooldown]
        
        if len(request_list) >= 1:
            wait_time = cooldown - (now - request_list[0])
            if wait_time > 0:
                wait_time += np.random.uniform(0.1, 0.3)
                await asyncio.sleep(wait_time)
        
        request_list.append(now)
        await asyncio.sleep(0.1)
    
    async def execute_with_backoff(self, func, *args, endpoint_type="general", **kwargs):
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_for_endpoint(endpoint_type)
                    result = await func(*args, **kwargs)
                    extra_delay = {
                        "funding": 0.15,
                        "oi": 0.2,
                        "general": 0.05
                    }.get(endpoint_type, 0.05)
                    await asyncio.sleep(extra_delay)
                    return result
                except Exception as e:
                    error_str = str(e)
                    if any(phrase in error_str for phrase in ["Too Many Requests", "50011", "429", "rate limit"]):
                        wait_time = self.min_delay * (self.backoff_factor ** attempt)
                        wait_time += np.random.uniform(0.2, 0.5)
                        log.warning(f"Rate limited on {endpoint_type}, attempt {attempt+1}/{self.max_retries}, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        raise e
            raise Exception(f"Failed after {self.max_retries} retries")

rate_limiter = EnhancedRateLimiter()

# ---------------- INSTITUTIONAL DATA FETCHER ----------------
class InstitutionalDataFetcher:
    def __init__(self):
        self.cache = {}
        self.cache_ttl = {
            'funding': 300,
            'oi': 600,
            'ticker': 30
        }
        
    async def get_institutional_data(self, exchange, symbol: str) -> InstitutionalData:
        cache_key = f"{symbol}_institutional"
        now = time.time()
        
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if now - timestamp < 300:
                return data
        
        try:
            futures_symbol = self._get_futures_symbol(symbol)
            
            tasks = [
                self._fetch_funding_data(exchange, futures_symbol),
                self._fetch_open_interest(exchange, futures_symbol),
                self._fetch_spot_futures_spread(exchange, symbol, futures_symbol)
            ]
            
            funding_data, oi_data, spread_data = await asyncio.gather(*tasks, return_exceptions=True)
            
            data = InstitutionalData()
            
            if not isinstance(funding_data, Exception) and funding_data:
                data.funding_rate = funding_data.get('fundingRate', 0) * 100
                data.funding_timestamp = datetime.datetime.utcnow()
                
                try:
                    funding_history = await rate_limiter.execute_with_backoff(
                        exchange.fetch_funding_rate_history,
                        futures_symbol,
                        limit=8,
                        endpoint_type="funding"
                    )
                    if funding_history:
                        data.funding_history = [f['fundingRate'] * 100 for f in funding_history]
                except:
                    pass
            
            if not isinstance(oi_data, Exception) and oi_data:
                data.open_interest = oi_data.get('openInterest', 0)
                data.oi_timestamp = datetime.datetime.utcnow()
                
                try:
                    markets = await exchange.load_markets()
                    if 'openInterestHistory' in exchange.has:
                        oi_history = await rate_limiter.execute_with_backoff(
                            exchange.fetch_open_interest_history,
                            futures_symbol,
                            '1h',
                            limit=24,
                            endpoint_type="oi"
                        )
                        if oi_history and len(oi_history) >= 2:
                            latest = oi_history[0]['openInterest']
                            oldest = oi_history[-1]['openInterest']
                            if oldest > 0:
                                data.oi_change_24h = (latest - oldest) / oldest * 100
                except:
                    pass
            
            if not isinstance(spread_data, Exception) and spread_data:
                data.basis_rate = spread_data.get('basis', 0)
                data.perpetual_premium = spread_data.get('premium', 0)
            
            self.cache[cache_key] = (data, now)
            return data
            
        except Exception as e:
            log.warning(f"Failed to fetch institutional data for {symbol}: {e}")
            return InstitutionalData()
    
    def _get_futures_symbol(self, spot_symbol: str) -> str:
        if "USDT" in spot_symbol:
            return spot_symbol.replace("/USDT", "-USDT-SWAP")
        return spot_symbol
    
    async def _fetch_funding_data(self, exchange, futures_symbol: str) -> Dict:
        try:
            return await rate_limiter.execute_with_backoff(
                exchange.fetch_funding_rate,
                futures_symbol,
                endpoint_type="funding"
            )
        except Exception as e:
            log.debug(f"Funding fetch failed for {futures_symbol}: {e}")
            return {}
    
    async def _fetch_open_interest(self, exchange, futures_symbol: str) -> Dict:
        try:
            return await rate_limiter.execute_with_backoff(
                exchange.fetch_open_interest,
                futures_symbol,
                endpoint_type="oi"
            )
        except Exception as e:
            log.debug(f"OI fetch failed for {futures_symbol}: {e}")
            return {}
    
    async def _fetch_spot_futures_spread(self, exchange, spot_symbol: str, futures_symbol: str) -> Dict:
        try:
            spot_ticker = await rate_limiter.execute_with_backoff(
                exchange.fetch_ticker,
                spot_symbol,
                endpoint_type="general"
            )
            
            futures_ticker = await rate_limiter.execute_with_backoff(
                exchange.fetch_ticker,
                futures_symbol,
                endpoint_type="general"
            )
            
            if spot_ticker and futures_ticker:
                spot_price = spot_ticker.get('last', 0)
                futures_price = futures_ticker.get('last', futures_ticker.get('mark', 0))
                
                if spot_price > 0:
                    basis = (futures_price - spot_price) / spot_price * 100
                    premium = basis
                    
                    return {
                        'basis': basis,
                        'premium': premium,
                        'spot_price': spot_price,
                        'futures_price': futures_price
                    }
        except Exception as e:
            log.debug(f"Spread calculation failed: {e}")
        
        return {}

data_fetcher = InstitutionalDataFetcher()

# ---------------- DIRECTION ENGINE ----------------
class DirectionEngine:
    def __init__(self):
        self.layer_weights = {
            'liquidity': 0.30,
            'trapped': 0.35,
            'bleeding': 0.20,
            'micro': 0.15
        }
    
    async def analyze_direction(
        self,
        exchange,
        symbol: str,
        liquidity_setup: Dict,
        proposed_side: str,
        eligibility: SetupEligibility
    ) -> DirectionMetrics:
        
        metrics = DirectionMetrics()
        current_price = eligibility.entry_price
        liquidity_pools = liquidity_setup.get('liquidity_analysis', {}).get('identified_pools', {})
        
        try:
            institutional_data = await data_fetcher.get_institutional_data(exchange, symbol)
            
            trapped_side, trapped_conf, trapped_details = await self._detect_trapped_side(
                exchange, symbol, current_price, liquidity_pools, institutional_data
            )
            metrics.trapped_side = trapped_side
            metrics.trapped_confidence = trapped_conf
            metrics.trapped_details = trapped_details
            
            bleeding_side, funding_extreme, funding_analysis = self._detect_bleeding_side(institutional_data)
            metrics.bleeding_side = bleeding_side
            metrics.funding_extreme = funding_extreme
            metrics.funding_analysis = funding_analysis
            
            micro_confirm, micro_type, rejection_type, micro_details = await self._get_micro_confirmation(
                exchange, symbol, proposed_side, current_price
            )
            metrics.micro_confirmation = micro_confirm
            metrics.micro_timeframe = micro_type
            metrics.rejection_type = rejection_type
            metrics.micro_details = micro_details
            
            direction_score, confidence_tier = self._calculate_direction_score(
                proposed_side,
                trapped_side,
                trapped_conf,
                bleeding_side,
                funding_extreme,
                micro_confirm,
                institutional_data
            )
            
            conflicts = self._detect_conflicts(
                proposed_side,
                trapped_side,
                bleeding_side,
                micro_confirm
            )
            
            metrics.direction_score = direction_score
            metrics.confidence_tier = confidence_tier
            metrics.conflict_warnings = conflicts
            
            return metrics
            
        except Exception as e:
            log.error(f"Direction engine error for {symbol}: {e}")
            return metrics
    
    async def _detect_trapped_side(
        self,
        exchange,
        symbol: str,
        current_price: float,
        liquidity_pools: Dict,
        institutional_data: InstitutionalData
    ) -> Tuple[TrappedSide, float, List[Dict]]:
        
        trapped_signals = []
        
        try:
            for pool_type, pools in liquidity_pools.items():
                if pool_type not in ['equal_highs', 'equal_lows', 'buy_stops', 'sell_stops']:
                    continue
                
                for pool in pools[:3]:
                    pool_price = pool.get('price', 0)
                    if pool_price <= 0:
                        continue
                    
                    distance_pct = abs(current_price - pool_price) / current_price * 100
                    
                    if distance_pct < 3.0:
                        signal = self._analyze_pool_for_traps(
                            pool, pool_type, current_price, institutional_data
                        )
                        if signal:
                            trapped_signals.append(signal)
            
            if institutional_data.oi_change_24h != 0:
                price_ticker = await rate_limiter.execute_with_backoff(
                    exchange.fetch_ticker,
                    symbol,
                    endpoint_type="general"
                )
                
                if price_ticker:
                    price_change_24h = price_ticker.get('percentage', 0)
                    
                    if price_change_24h < -2.0 and institutional_data.oi_change_24h > 10:
                        trapped_signals.append({
                            'side': 'SHORT',
                            'confidence': 0.6,
                            'reason': f"Bullish divergence: Price ↓{abs(price_change_24h):.1f}%, OI ↑{institutional_data.oi_change_24h:.1f}%"
                        })
                    
                    elif price_change_24h > 2.0 and institutional_data.oi_change_24h < -10:
                        trapped_signals.append({
                            'side': 'LONG',
                            'confidence': 0.6,
                            'reason': f"Bearish divergence: Price ↑{price_change_24h:.1f}%, OI ↓{abs(institutional_data.oi_change_24h):.1f}%"
                        })
            
            if trapped_signals:
                return self._aggregate_trapped_signals(trapped_signals)
            
            return TrappedSide.NONE, 0.0, []
            
        except Exception as e:
            log.debug(f"Trapped side detection error: {e}")
            return TrappedSide.NONE, 0.0, []
    
    def _analyze_pool_for_traps(
        self,
        pool: Dict,
        pool_type: str,
        current_price: float,
        institutional_data: InstitutionalData
    ) -> Optional[Dict]:
        
        pool_price = pool.get('price', 0)
        
        if pool_type in ['equal_highs', 'sell_stops']:
            if current_price < pool_price:
                if institutional_data.oi_change_1h > 5:
                    return {
                        'side': 'SHORT',
                        'confidence': min(institutional_data.oi_change_1h / 20, 0.8),
                        'reason': f"OI ↑ near resistance {pool_price:.8f}",
                        'pool_price': pool_price
                    }
        
        elif pool_type in ['equal_lows', 'buy_stops']:
            if current_price > pool_price:
                if institutional_data.oi_change_1h > 5:
                    return {
                        'side': 'LONG',
                        'confidence': min(institutional_data.oi_change_1h / 20, 0.8),
                        'reason': f"OI ↑ near support {pool_price:.8f}",
                        'pool_price': pool_price
                    }
        
        return None
    
    def _aggregate_trapped_signals(
        self,
        signals: List[Dict]
    ) -> Tuple[TrappedSide, float, List[Dict]]:
        
        long_signals = [s for s in signals if s['side'] == 'LONG']
        short_signals = [s for s in signals if s['side'] == 'SHORT']
        
        if not long_signals and not short_signals:
            return TrappedSide.NONE, 0.0, signals
        
        if long_signals and short_signals:
            long_avg = np.mean([s['confidence'] for s in long_signals])
            short_avg = np.mean([s['confidence'] for s in short_signals])
            
            if long_avg > short_avg * 1.5:
                return TrappedSide.LONG, long_avg, signals
            elif short_avg > long_avg * 1.5:
                return TrappedSide.SHORT, short_avg, signals
            else:
                return TrappedSide.CONFLICT, max(long_avg, short_avg), signals
        
        elif long_signals:
            avg_conf = np.mean([s['confidence'] for s in long_signals])
            return TrappedSide.LONG, avg_conf, signals
        
        else:
            avg_conf = np.mean([s['confidence'] for s in short_signals])
            return TrappedSide.SHORT, avg_conf, signals
    
    def _detect_bleeding_side(
        self,
        institutional_data: InstitutionalData
    ) -> Tuple[str, float, Dict]:
        
        bleeding_side = ""
        funding_extreme = 0.0
        current_funding = institutional_data.funding_rate
        
        if current_funding > FUNDING_EXTREME_THRESHOLD:
            bleeding_side = "LONG"
            funding_extreme = current_funding
        elif current_funding < -FUNDING_EXTREME_THRESHOLD:
            bleeding_side = "SHORT"
            funding_extreme = abs(current_funding)
        
        analysis = {
            'current_funding_pct': current_funding,
            'avg_funding_pct': np.mean(institutional_data.funding_history) if institutional_data.funding_history else 0,
            'funding_history': institutional_data.funding_history,
            'is_extreme': institutional_data.is_funding_extreme,
            'bleeding_side': bleeding_side,
            'threshold': FUNDING_EXTREME_THRESHOLD
        }
        
        return bleeding_side, funding_extreme, analysis
    
    async def _get_micro_confirmation(
        self,
        exchange,
        symbol: str,
        proposed_side: str,
        entry_price: float
    ) -> Tuple[bool, str, MicroConfirmationType, Dict]:
        
        try:
            ohlcv_1m = await fetch_ohlcv(exchange, symbol, "1m", 20)
            ohlcv_3m = await fetch_ohlcv(exchange, symbol, "3m", 15)
            
            df_1m = create_dataframe(ohlcv_1m) if ohlcv_1m else None
            df_3m = create_dataframe(ohlcv_3m) if ohlcv_3m else None
            
            if df_1m is None or df_3m is None:
                return False, "", MicroConfirmationType.NONE, {}
            
            last_1m = df_1m.iloc[-1]
            micro_details = {
                '1m_candles': len(df_1m),
                '3m_candles': len(df_3m),
                'current_price': entry_price
            }
            
            if proposed_side == "BUY":
                result = self._check_bullish_micro_confirmation(last_1m, df_3m, micro_details)
                if result[0]:
                    return True, "1m", result[1], micro_details
            else:
                result = self._check_bearish_micro_confirmation(last_1m, df_3m, micro_details)
                if result[0]:
                    return True, "1m", result[1], micro_details
            
            return False, "", MicroConfirmationType.NONE, micro_details
            
        except Exception as e:
            log.debug(f"Micro confirmation error: {e}")
            return False, "", MicroConfirmationType.NONE, {}
    
    def _check_bullish_micro_confirmation(
        self,
        last_1m: pd.Series,
        df_3m: pd.DataFrame,
        details: Dict
    ) -> Tuple[bool, MicroConfirmationType]:
        
        lower_wick = min(last_1m['open'], last_1m['close']) - last_1m['low']
        body_size = abs(last_1m['close'] - last_1m['open'])
        
        if lower_wick > body_size * 1.8 and lower_wick > 0:
            details['wick_ratio'] = lower_wick / body_size
            details['wick_type'] = 'LOWER'
            return True, MicroConfirmationType.WICK_REJECTION
        
        if len(df_3m) >= 3:
            last_3m = df_3m.iloc[-1]
            prev_volume_avg = df_3m['volume'].iloc[-5:-1].mean() if len(df_3m) >= 5 else 0
            
            if prev_volume_avg > 0:
                volume_ratio = last_3m['volume'] / prev_volume_avg
                range_pct = (last_3m['high'] - last_3m['low']) / last_3m['close'] * 100
                
                if volume_ratio > 1.5 and range_pct < 0.3 and last_3m['close'] > last_3m['open']:
                    details['volume_ratio'] = volume_ratio
                    details['range_pct'] = range_pct
                    return True, MicroConfirmationType.ABSORPTION
        
        return False, MicroConfirmationType.NONE
    
    def _check_bearish_micro_confirmation(
        self,
        last_1m: pd.Series,
        df_3m: pd.DataFrame,
        details: Dict
    ) -> Tuple[bool, MicroConfirmationType]:
        
        upper_wick = last_1m['high'] - max(last_1m['open'], last_1m['close'])
        body_size = abs(last_1m['close'] - last_1m['open'])
        
        if upper_wick > body_size * 1.8 and upper_wick > 0:
            details['wick_ratio'] = upper_wick / body_size
            details['wick_type'] = 'UPPER'
            return True, MicroConfirmationType.WICK_REJECTION
        
        if len(df_3m) >= 3:
            last_3m = df_3m.iloc[-1]
            prev_volume_avg = df_3m['volume'].iloc[-5:-1].mean() if len(df_3m) >= 5 else 0
            
            if prev_volume_avg > 0:
                volume_ratio = last_3m['volume'] / prev_volume_avg
                range_pct = (last_3m['high'] - last_3m['low']) / last_3m['close'] * 100
                
                if volume_ratio > 1.5 and range_pct < 0.3 and last_3m['close'] < last_3m['open']:
                    details['volume_ratio'] = volume_ratio
                    details['range_pct'] = range_pct
                    return True, MicroConfirmationType.ABSORPTION
        
        return False, MicroConfirmationType.NONE
    
    def _calculate_direction_score(
        self,
        proposed_side: str,
        trapped_side: TrappedSide,
        trapped_conf: float,
        bleeding_side: str,
        funding_extreme: float,
        micro_confirm: bool,
        institutional_data: InstitutionalData
    ) -> Tuple[float, DirectionTier]:
        
        direction_score = 0.0
        contributing_factors = 0
        
        base_score = 0.3 if proposed_side == "BUY" else -0.3
        direction_score += base_score
        
        if trapped_side != TrappedSide.NONE:
            contributing_factors += 1
            if (trapped_side == TrappedSide.LONG and proposed_side == "SELL") or \
               (trapped_side == TrappedSide.SHORT and proposed_side == "BUY"):
                direction_score += trapped_conf * 0.4 if proposed_side == "BUY" else -trapped_conf * 0.4
            else:
                direction_score += -trapped_conf * 0.2 if proposed_side == "BUY" else trapped_conf * 0.2
        
        if bleeding_side:
            contributing_factors += 1
            if (bleeding_side == "LONG" and proposed_side == "SELL") or \
               (bleeding_side == "SHORT" and proposed_side == "BUY"):
                direction_score += min(funding_extreme / 0.1, 1.0) * 0.3 if proposed_side == "BUY" else -min(funding_extreme / 0.1, 1.0) * 0.3
        
        if micro_confirm:
            contributing_factors += 1
            direction_score += 0.2 if proposed_side == "BUY" else -0.2
        
        if institutional_data.is_funding_extreme:
            contributing_factors += 1
        
        direction_score = max(-1.0, min(1.0, direction_score))
        
        abs_score = abs(direction_score)
        confidence_multiplier = (contributing_factors / 4.0) * 0.5 + 0.5
        
        if abs_score > 0.7 and confidence_multiplier > 0.7:
            confidence_tier = DirectionTier.HIGH
        elif abs_score > 0.4 and confidence_multiplier > 0.5:
            confidence_tier = DirectionTier.MEDIUM
        else:
            confidence_tier = DirectionTier.LOW
        
        return direction_score, confidence_tier
    
    def _detect_conflicts(
        self,
        proposed_side: str,
        trapped_side: TrappedSide,
        bleeding_side: str,
        micro_confirm: bool
    ) -> List[str]:
        
        conflicts = []
        
        if trapped_side != TrappedSide.NONE:
            if (trapped_side == TrappedSide.LONG and proposed_side == "BUY") or \
               (trapped_side == TrappedSide.SHORT and proposed_side == "SELL"):
                conflicts.append(f"Proposed {proposed_side} vs Trapped {trapped_side.value}")
        
        if bleeding_side:
            if (bleeding_side == "LONG" and proposed_side == "BUY") or \
               (bleeding_side == "SHORT" and proposed_side == "SELL"):
                conflicts.append(f"Proposed {proposed_side} vs Bleeding {bleeding_side}")
        
        if not micro_confirm:
            conflicts.append("No micro confirmation")
        
        return conflicts

direction_engine = DirectionEngine()

# ---------------- SIGNAL TRACKER ----------------
class SignalTracker:
    def __init__(self):
        self.active_signals = {}
        self.outcome_stats = {
            'total_signals': 0,
            'tp1_hits': 0,
            'tp2_hits': 0,
            'tp3_hits': 0,
            'sl_hits': 0,
            'expired': 0,
            'active': 0,
            'win_rate': 0.0,
            'avg_pnl_pct': 0.0
        }
        self.bucket_hits = {}
    
    def get_signal_key(self, setup: Dict) -> tuple:
        symbol = setup.get('symbol', '')
        side = setup.get('side', '')
        quality_score = setup.get('quality', {}).get('total_score', 0)
        bucket = math.floor(quality_score * 2) / 2
        
        bucket_key = f"{symbol}_{side}_{bucket}"
        if bucket_key not in self.bucket_hits:
            self.bucket_hits[bucket_key] = 0
        self.bucket_hits[bucket_key] += 1
        
        return (symbol, side, bucket)
    
    def is_new_signal(self, setup: Dict) -> Tuple[bool, str]:
        key = self.get_signal_key(setup)
        
        if key in self.active_signals:
            signal = self.active_signals[key]
            
            if signal.get('status') == 'active':
                now = datetime.datetime.utcnow()
                age_minutes = (now - signal['first_seen']).total_seconds() / 60
                
                if age_minutes > (SIGNAL_VALIDITY_HOURS * 60):
                    self.remove_signal_by_key(key, f"Expired after {SIGNAL_VALIDITY_HOURS}h")
                    return True, "Old signal expired, allowing new one"
                
                symbol, side, bucket = key
                current_score = setup.get('quality', {}).get('total_score', 0)
                signal_score = signal.get('setup', {}).get('quality', {}).get('total_score', 0)
                
                log.debug(f"⏸️  Skipping {symbol} {side}: Already active in bucket {bucket} "
                         f"(Score: {signal_score:.2f}→{current_score:.2f}, {age_minutes:.1f}m old)")
                return False, f"Active signal in bucket {bucket} (Score: {current_score:.2f}, {age_minutes:.1f}m old)"
            
            return True, "Previous signal closed, allowing new one"
        
        return True, "No active signal for this symbol+side+0.5_bucket"
    
    def should_send_alert(self, setup: Dict) -> bool:
        is_new, reason = self.is_new_signal(setup)
        return is_new
    
    def update_signal(self, setup: Dict, alerted: bool = False):
        key = self.get_signal_key(setup)
        now = datetime.datetime.utcnow()
        
        if key not in self.active_signals:
            symbol, side, bucket = key
            quality_score = setup.get('quality', {}).get('total_score', 0)
            
            self.active_signals[key] = {
                'setup': setup,
                'first_seen': now,
                'last_alerted': now if alerted else None,
                'last_checked': now,
                'alert_count': 1 if alerted else 0,
                'status': 'active',
                'outcome': 'active',
                'highest_price': setup.get('current_price', 0),
                'lowest_price': setup.get('current_price', 0),
                'price_at_alert': setup.get('current_price', 0) if alerted else None,
                'outcome_details': None,
                'signal_key': key
            }
            self.outcome_stats['total_signals'] += 1
            self.outcome_stats['active'] += 1
            
            log.info(f"📝 NEW 0.5-BUCKET SIGNAL: {symbol} {side} | "
                    f"Score:{quality_score:.2f} → Bucket:{bucket} | "
                    f"TP1: {setup.get('tp_targets', [0])[0]:.8f}")
        else:
            current_price = setup.get('current_price', 0)
            self.active_signals[key]['highest_price'] = max(
                self.active_signals[key]['highest_price'],
                current_price
            )
            self.active_signals[key]['lowest_price'] = min(
                self.active_signals[key]['lowest_price'],
                current_price
            )
            self.active_signals[key]['last_checked'] = now
    
    def check_signal_outcome(self, setup: Dict, current_price: float) -> Optional[Dict]:
        key = self.get_signal_key(setup)
        
        if key not in self.active_signals:
            return None
        
        signal = self.active_signals[key]
        
        if signal.get('status') != 'active':
            return None
        
        now = datetime.datetime.utcnow()
        time_since_alert = (now - signal['first_seen']).total_seconds()
        if time_since_alert < 180:
            return None
        
        setup_data = signal.get('setup', {})
        if not setup_data:
            return None
        
        side = setup_data.get('side', '')
        entry = setup_data.get('entry_price', 0)
        tp_targets = setup_data.get('tp_targets', [])
        sl = setup_data.get('sl_price', 0)
        
        if entry == 0 or sl == 0:
            return None
        
        outcome = None
        
        for i, tp in enumerate(tp_targets):
            if tp == 0:
                continue
                
            if side == "BUY" and current_price >= tp:
                pnl_pct = (current_price - entry) / entry * 100
                outcome = {
                    'type': f'TP{i+1}_HIT',
                    'price': current_price,
                    'pnl_pct': pnl_pct,
                    'bars_held': int(time_since_alert / 60),
                    'max_favorable': (signal['highest_price'] - entry) / entry * 100,
                    'max_adverse': (entry - signal['lowest_price']) / entry * 100,
                    'tp_level': i+1,
                    'signal_key': key
                }
                break
            elif side == "SELL" and current_price <= tp:
                pnl_pct = (entry - current_price) / entry * 100
                outcome = {
                    'type': f'TP{i+1}_HIT',
                    'price': current_price,
                    'pnl_pct': pnl_pct,
                    'bars_held': int(time_since_alert / 60),
                    'max_favorable': (entry - signal['lowest_price']) / entry * 100,
                    'max_adverse': (signal['highest_price'] - entry) / entry * 100,
                    'tp_level': i+1,
                    'signal_key': key
                }
                break
        
        if not outcome:
            if (side == "BUY" and current_price <= sl) or (side == "SELL" and current_price >= sl):
                if side == "BUY":
                    pnl_pct = (current_price - entry) / entry * 100
                    max_fav = (signal['highest_price'] - entry) / entry * 100
                else:
                    pnl_pct = (entry - current_price) / entry * 100
                    max_fav = (entry - signal['lowest_price']) / entry * 100
                
                outcome = {
                    'type': 'SL_HIT',
                    'price': current_price,
                    'pnl_pct': pnl_pct,
                    'bars_held': int(time_since_alert / 60),
                    'max_favorable': max_fav,
                    'max_adverse': abs(entry - sl) / entry * 100,
                    'tp_level': 0,
                    'signal_key': key
                }
        
        if outcome:
            signal['outcome'] = outcome['type'].lower()
            signal['outcome_details'] = outcome
            signal['closed_at'] = now
            signal['closed_price'] = current_price
            signal['status'] = 'closed'
            
            self.outcome_stats['active'] -= 1
            
            if 'TP1_HIT' in outcome['type']:
                self.outcome_stats['tp1_hits'] += 1
            elif 'TP2_HIT' in outcome['type']:
                self.outcome_stats['tp2_hits'] += 1
            elif 'TP3_HIT' in outcome['type']:
                self.outcome_stats['tp3_hits'] += 1
            elif outcome['type'] == 'SL_HIT':
                self.outcome_stats['sl_hits'] += 1
            
            wins = self.outcome_stats['tp1_hits'] + self.outcome_stats['tp2_hits'] + self.outcome_stats['tp3_hits']
            losses = self.outcome_stats['sl_hits']
            total_closed = wins + losses
            
            if total_closed > 0:
                self.outcome_stats['win_rate'] = wins / total_closed * 100
                
                if 'avg_pnl_pct' not in self.outcome_stats or self.outcome_stats['avg_pnl_pct'] == 0:
                    self.outcome_stats['avg_pnl_pct'] = outcome['pnl_pct']
                else:
                    self.outcome_stats['avg_pnl_pct'] = (
                        self.outcome_stats['avg_pnl_pct'] * (total_closed - 1) + outcome['pnl_pct']
                    ) / total_closed
            
            return outcome
        
        return None
    
    def remove_signal_by_key(self, key: tuple, reason: str = "expired"):
        if key in self.active_signals:
            signal = self.active_signals.pop(key)
            signal['status'] = 'expired'
            signal['expired_at'] = datetime.datetime.utcnow()
            signal['expired_reason'] = reason
            
            self.outcome_stats['active'] -= 1
            self.outcome_stats['expired'] += 1
            
            symbol, side, bucket = key
            log.info(f"🗑️  Removed 0.5-bucket signal: {symbol} {side} | Bucket:{bucket} - {reason}")
    
    def cleanup_old_signals(self):
        now = datetime.datetime.utcnow()
        expired_keys = []
        
        for key, data in self.active_signals.items():
            if data.get('status') == 'active':
                age_minutes = (now - data['first_seen']).total_seconds() / 60
                if age_minutes > (SIGNAL_VALIDITY_HOURS * 60):
                    expired_keys.append(key)
        
        for key in expired_keys:
            self.remove_signal_by_key(key, f"Expired after {SIGNAL_VALIDITY_HOURS}h")
        
        if expired_keys:
            log.info(f"🧹 Cleaned up {len(expired_keys)} expired 0.5-bucket signals")
    
    def get_stats(self) -> Dict:
        active_count = len([s for s in self.active_signals.values() if s.get('status') == 'active'])
        
        buy_signals = 0
        sell_signals = 0
        
        for signal in self.active_signals.values():
            if signal.get('status') == 'active':
                setup = signal.get('setup', {})
                if setup.get('side') == 'BUY':
                    buy_signals += 1
                elif setup.get('side') == 'SELL':
                    sell_signals += 1
        
        bucket_distribution = {}
        for key in self.active_signals.keys():
            if self.active_signals[key].get('status') == 'active':
                bucket = key[2]
                bucket_distribution[bucket] = bucket_distribution.get(bucket, 0) + 1
        
        return {
            'active_signals': active_count,
            'signals_by_side': {
                'BUY': buy_signals,
                'SELL': sell_signals
            },
            'bucket_distribution': bucket_distribution,
            'outcome_stats': self.outcome_stats
        }

signal_tracker = SignalTracker()
db_lock = asyncio.Lock()
db_conn = None

# ---------------- TELEGRAM ----------------
async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- SAFE API WRAPPERS ----------------
async def safe_fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    return await rate_limiter.execute_with_backoff(
        exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit
    )

async def safe_fetch_ticker(exchange, symbol: str):
    return await rate_limiter.execute_with_backoff(
        exchange.fetch_ticker, symbol
    )

async def safe_fetch_tickers(exchange):
    return await rate_limiter.execute_with_backoff(
        exchange.fetch_tickers
    )

# ---------------- UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    try:
        return await asyncio.wait_for(
            safe_fetch_ohlcv(exchange, symbol, timeframe, limit),
            timeout=8.0
        )
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv):
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ---------------- LIQUIDITY POOL IDENTIFICATION ----------------
def identify_liquidity_pools(df, timeframe="1h"):
    pools = {
        'buy_stops': [],
        'sell_stops': [],
        'equal_highs': [],
        'equal_lows': []
    }
    
    if df is None or len(df) < 20:
        return pools
    
    window_size = 5 if timeframe == "15m" else 3
    
    for i in range(window_size, len(df)-window_size):
        window_highs = df['high'].iloc[i-window_size:i+window_size+1]
        current_high = df['high'].iloc[i]
        
        if current_high == window_highs.max():
            same_high_count = (window_highs == current_high).sum()
            
            if same_high_count >= 2:
                pools['equal_highs'].append({
                    'price': float(current_high),
                    'timeframe': timeframe,
                    'candle_index': i,
                    'count': same_high_count,
                    'type': 'equal_high'
                })
                
                pools['sell_stops'].append({
                    'price': float(current_high),
                    'reason': 'equal_high',
                    'timeframe': timeframe,
                    'strength': same_high_count
                })
    
    for i in range(window_size, len(df)-window_size):
        window_lows = df['low'].iloc[i-window_size:i+window_size+1]
        current_low = df['low'].iloc[i]
        
        if current_low == window_lows.min():
            same_low_count = (window_lows == current_low).sum()
            
            if same_low_count >= 2:
                pools['equal_lows'].append({
                    'price': float(current_low),
                    'timeframe': timeframe,
                    'candle_index': i,
                    'count': same_low_count,
                    'type': 'equal_low'
                })
                
                pools['buy_stops'].append({
                    'price': float(current_low),
                    'reason': 'equal_low',
                    'timeframe': timeframe,
                    'strength': same_low_count
                })
    
    recent_window = max(20, int(len(df) * 0.2))
    recent_data = df.iloc[-recent_window:]
    
    if len(recent_data) >= 10:
        recent_range = recent_data['high'].max() - recent_data['low'].min()
        avg_price = recent_data['close'].mean()
        
        if recent_range / avg_price < 0.01:
            consolidation_high = recent_data['high'].max()
            consolidation_low = recent_data['low'].min()
            
            pools['buy_stops'].append({
                'price': float(consolidation_high),
                'reason': 'consolidation_high',
                'timeframe': timeframe,
                'strength': 3
            })
            
            pools['sell_stops'].append({
                'price': float(consolidation_low),
                'reason': 'consolidation_low',
                'timeframe': timeframe,
                'strength': 3
            })
    
    for key in pools:
        if pools[key]:
            seen_prices = set()
            unique_pools = []
            for pool in pools[key]:
                if pool['price'] not in seen_prices:
                    seen_prices.add(pool['price'])
                    unique_pools.append(pool)
            pools[key] = unique_pools
            
            if key in ['buy_stops', 'equal_lows']:
                pools[key].sort(key=lambda x: x['price'])
            else:
                pools[key].sort(key=lambda x: x['price'], reverse=True)
    
    return pools

# ---------------- LIQUIDITY-BASED TP/SL CALCULATION ----------------
async def calculate_liquidity_tp_sl(exchange, symbol: str, side: str, entry_price: float, 
                                   entry_type: str) -> Tuple[float, List[float], List[Dict], Dict]:
    
    ohlcv_4h = await fetch_ohlcv(exchange, symbol, "4h", 100)
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 200)
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 300)
    
    df_4h = create_dataframe(ohlcv_4h)
    df_1h = create_dataframe(ohlcv_1h)
    df_15m = create_dataframe(ohlcv_15m)
    
    pools_4h = identify_liquidity_pools(df_4h, "4h") if df_4h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_1h = identify_liquidity_pools(df_1h, "1h") if df_1h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_15m = identify_liquidity_pools(df_15m, "15m") if df_15m is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    
    all_pools = {
        'buy_stops': [],
        'sell_stops': [],
        'equal_highs': [],
        'equal_lows': []
    }
    
    for pool in pools_4h['buy_stops']:
        pool['weight'] = 3.0
        all_pools['buy_stops'].append(pool)
    
    for pool in pools_1h['buy_stops']:
        pool['weight'] = 2.0
        all_pools['buy_stops'].append(pool)
    
    for pool in pools_15m['buy_stops']:
        pool['weight'] = 1.0
        all_pools['buy_stops'].append(pool)
    
    for pool_type in ['sell_stops', 'equal_highs', 'equal_lows']:
        for pool in pools_4h[pool_type]:
            pool['weight'] = 3.0
            all_pools[pool_type].append(pool)
        
        for pool in pools_1h[pool_type]:
            pool['weight'] = 2.0
            all_pools[pool_type].append(pool)
        
        for pool in pools_15m[pool_type]:
            pool['weight'] = 1.0
            all_pools[pool_type].append(pool)
    
    all_pools['buy_stops'].sort(key=lambda x: x['price'])
    all_pools['sell_stops'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_highs'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_lows'].sort(key=lambda x: x['price'])
    
    current_price = entry_price
    tp_targets = []
    tp_sources = []
    sl_price = 0.0
    sl_source = {}
    
    if side == "BUY":
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        
        if sell_stops_below:
            for timeframe_weight in [3.0, 2.0, 1.0]:
                timeframe_pools = [p for p in sell_stops_below if p.get('weight', 1.0) == timeframe_weight]
                if timeframe_pools:
                    strongest_pool = min(timeframe_pools, key=lambda x: x['price'])
                    sl_price = strongest_pool['price'] * 0.997
                    sl_source = {
                        'type': 'sell_stop_pool',
                        'timeframe': strongest_pool.get('timeframe', 'unknown'),
                        'reason': strongest_pool.get('reason', 'equal_high'),
                        'strength': strongest_pool.get('strength', 1),
                        'original_price': strongest_pool['price']
                    }
                    break
            
            if sl_price == 0:
                strongest_pool = min(sell_stops_below, key=lambda x: x['price'])
                sl_price = strongest_pool['price'] * 0.995
                sl_source = {
                    'type': 'sell_stop_pool',
                    'timeframe': strongest_pool.get('timeframe', 'unknown'),
                    'reason': strongest_pool.get('reason', 'equal_high'),
                    'strength': strongest_pool.get('strength', 1),
                    'original_price': strongest_pool['price']
                }
        else:
            equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
            if equal_lows_below:
                most_recent_low = max(equal_lows_below, key=lambda x: x.get('candle_index', 0))
                sl_price = most_recent_low['price'] * 0.99
                sl_source = {
                    'type': 'equal_low',
                    'timeframe': most_recent_low.get('timeframe', 'unknown'),
                    'reason': 'recent_equal_low',
                    'strength': most_recent_low.get('count', 1),
                    'original_price': most_recent_low['price']
                }
            else:
                log.debug(f"No valid liquidity-based SL found for {symbol} BUY")
                return None, [], [], None
        
        if sl_price > current_price * 0.995:
            sl_price = current_price * 0.985
            sl_source = {
                'type': 'adjusted',
                'timeframe': 'N/A',
                'reason': 'too_close_adjusted',
                'strength': 0,
                'original_price': current_price * 0.995
            }
        
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        
        if buy_stops_above:
            closest_buy_stop = min(buy_stops_above, key=lambda x: x['price'])
            tp1 = closest_buy_stop['price']
            tp_sources.append({
                'tp_level': 1,
                'type': 'buy_stop_pool',
                'timeframe': closest_buy_stop.get('timeframe', 'unknown'),
                'reason': closest_buy_stop.get('reason', 'equal_low'),
                'strength': closest_buy_stop.get('strength', 1),
                'original_price': closest_buy_stop['price']
            })
            
            if entry_type == "DISCOUNT_ZONE":
                equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
                if equal_highs_above:
                    closest_equal_high = min(equal_highs_above, key=lambda x: x['price'])
                    if abs(closest_equal_high['price'] - current_price) < abs(tp1 - current_price) * 1.5:
                        tp1 = closest_equal_high['price']
                        tp_sources[-1] = {
                            'tp_level': 1,
                            'type': 'equal_high',
                            'timeframe': closest_equal_high.get('timeframe', 'unknown'),
                            'reason': 'premium_zone',
                            'strength': closest_equal_high.get('count', 1),
                            'original_price': closest_equal_high['price']
                        }
        else:
            equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
            if equal_highs_above:
                tp1 = min(equal_highs_above, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 1,
                    'type': 'equal_high',
                    'timeframe': min(equal_highs_above, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'premium_zone',
                    'strength': min(equal_highs_above, key=lambda x: x['price']).get('count', 1),
                    'original_price': min(equal_highs_above, key=lambda x: x['price'])['price']
                })
            else:
                log.debug(f"No valid liquidity-based TP1 found for {symbol} BUY")
                return None, [], [], None
        
        buy_stops_above_tp1 = [p for p in all_pools['buy_stops'] if p['price'] > tp1]
        
        if buy_stops_above_tp1:
            significant_pools = [p for p in buy_stops_above_tp1 if p.get('weight', 1.0) >= 2.0]
            if significant_pools:
                tp2 = min(significant_pools, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'buy_stop_pool',
                    'timeframe': min(significant_pools, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'higher_timeframe_pool',
                    'strength': min(significant_pools, key=lambda x: x['price']).get('strength', 1),
                    'original_price': min(significant_pools, key=lambda x: x['price'])['price']
                })
            else:
                tp2 = min(buy_stops_above_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'buy_stop_pool',
                    'timeframe': min(buy_stops_above_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'next_pool',
                    'strength': min(buy_stops_above_tp1, key=lambda x: x['price']).get('strength', 1),
                    'original_price': min(buy_stops_above_tp1, key=lambda x: x['price'])['price']
                })
        else:
            equal_highs_above_tp1 = [p for p in all_pools['equal_highs'] if p['price'] > tp1]
            if equal_highs_above_tp1:
                tp2 = min(equal_highs_above_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'equal_high',
                    'timeframe': min(equal_highs_above_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'premium_zone',
                    'strength': min(equal_highs_above_tp1, key=lambda x: x['price']).get('count', 1),
                    'original_price': min(equal_highs_above_tp1, key=lambda x: x['price'])['price']
                })
            else:
                log.debug(f"No valid liquidity-based TP2 found for {symbol} BUY, using TP1 only")
                tp_targets = [tp1]
        
        if 'tp2' in locals():
            tp_targets = [tp1, tp2]
        
        if entry_type == "DISCOUNT_ZONE" and len(all_pools['equal_highs']) >= 2:
            if df_4h is not None and len(df_4h) >= 10:
                major_high_idx = df_4h['high'].iloc[-int(len(df_4h)*0.5):].idxmax()
                major_high = df_4h['high'].iloc[major_high_idx]
                
                if major_high > tp_targets[-1] * 1.05:
                    tp_targets.append(float(major_high))
                    tp_sources.append({
                        'tp_level': 3,
                        'type': 'major_swing_high',
                        'timeframe': '4h',
                        'reason': 'major_structure',
                        'strength': 3,
                        'original_price': float(major_high)
                    })
    
    else:
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        
        if buy_stops_above:
            for timeframe_weight in [3.0, 2.0, 1.0]:
                timeframe_pools = [p for p in buy_stops_above if p.get('weight', 1.0) == timeframe_weight]
                if timeframe_pools:
                    strongest_pool = max(timeframe_pools, key=lambda x: x['price'])
                    sl_price = strongest_pool['price'] * 1.003
                    sl_source = {
                        'type': 'buy_stop_pool',
                        'timeframe': strongest_pool.get('timeframe', 'unknown'),
                        'reason': strongest_pool.get('reason', 'equal_low'),
                        'strength': strongest_pool.get('strength', 1),
                        'original_price': strongest_pool['price']
                    }
                    break
            
            if sl_price == 0:
                strongest_pool = max(buy_stops_above, key=lambda x: x['price'])
                sl_price = strongest_pool['price'] * 1.005
                sl_source = {
                    'type': 'buy_stop_pool',
                    'timeframe': strongest_pool.get('timeframe', 'unknown'),
                    'reason': strongest_pool.get('reason', 'equal_low'),
                    'strength': strongest_pool.get('strength', 1),
                    'original_price': strongest_pool['price']
                }
        else:
            equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
            if equal_highs_above:
                most_recent_high = max(equal_highs_above, key=lambda x: x.get('candle_index', 0))
                sl_price = most_recent_high['price'] * 1.01
                sl_source = {
                    'type': 'equal_high',
                    'timeframe': most_recent_high.get('timeframe', 'unknown'),
                    'reason': 'recent_equal_high',
                    'strength': most_recent_high.get('count', 1),
                    'original_price': most_recent_high['price']
                }
            else:
                log.debug(f"No valid liquidity-based SL found for {symbol} SELL")
                return None, [], [], None
        
        if sl_price < current_price * 1.005:
            sl_price = current_price * 1.015
            sl_source = {
                'type': 'adjusted',
                'timeframe': 'N/A',
                'reason': 'too_close_adjusted',
                'strength': 0,
                'original_price': current_price * 1.005
            }
        
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        
        if sell_stops_below:
            closest_sell_stop = max(sell_stops_below, key=lambda x: x['price'])
            tp1 = closest_sell_stop['price']
            tp_sources.append({
                'tp_level': 1,
                'type': 'sell_stop_pool',
                'timeframe': closest_sell_stop.get('timeframe', 'unknown'),
                'reason': closest_sell_stop.get('reason', 'equal_high'),
                'strength': closest_sell_stop.get('strength', 1),
                'original_price': closest_sell_stop['price']
            })
            
            if entry_type == "PREMIUM_ZONE":
                equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
                if equal_lows_below:
                    closest_equal_low = max(equal_lows_below, key=lambda x: x['price'])
                    if abs(current_price - closest_equal_low['price']) < abs(current_price - tp1) * 1.5:
                        tp1 = closest_equal_low['price']
                        tp_sources[-1] = {
                            'tp_level': 1,
                            'type': 'equal_low',
                            'timeframe': closest_equal_low.get('timeframe', 'unknown'),
                            'reason': 'discount_zone',
                            'strength': closest_equal_low.get('count', 1),
                            'original_price': closest_equal_low['price']
                        }
        else:
            equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
            if equal_lows_below:
                tp1 = max(equal_lows_below, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 1,
                    'type': 'equal_low',
                    'timeframe': max(equal_lows_below, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'discount_zone',
                    'strength': max(equal_lows_below, key=lambda x: x['price']).get('count', 1),
                    'original_price': max(equal_lows_below, key=lambda x: x['price'])['price']
                })
            else:
                log.debug(f"No valid liquidity-based TP1 found for {symbol} SELL")
                return None, [], [], None
        
        sell_stops_below_tp1 = [p for p in all_pools['sell_stops'] if p['price'] < tp1]
        
        if sell_stops_below_tp1:
            significant_pools = [p for p in sell_stops_below_tp1 if p.get('weight', 1.0) >= 2.0]
            if significant_pools:
                tp2 = max(significant_pools, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'sell_stop_pool',
                    'timeframe': max(significant_pools, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'higher_timeframe_pool',
                    'strength': max(significant_pools, key=lambda x: x['price']).get('strength', 1),
                    'original_price': max(significant_pools, key=lambda x: x['price'])['price']
                })
            else:
                tp2 = max(sell_stops_below_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'sell_stop_pool',
                    'timeframe': max(sell_stops_below_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'next_pool',
                    'strength': max(sell_stops_below_tp1, key=lambda x: x['price']).get('strength', 1),
                    'original_price': max(sell_stops_below_tp1, key=lambda x: x['price'])['price']
                })
        else:
            equal_lows_below_tp1 = [p for p in all_pools['equal_lows'] if p['price'] < tp1]
            if equal_lows_below_tp1:
                tp2 = max(equal_lows_below_tp1, key=lambda x: x['price'])['price']
                tp_sources.append({
                    'tp_level': 2,
                    'type': 'equal_low',
                    'timeframe': max(equal_lows_below_tp1, key=lambda x: x['price']).get('timeframe', 'unknown'),
                    'reason': 'discount_zone',
                    'strength': max(equal_lows_below_tp1, key=lambda x: x['price']).get('count', 1),
                    'original_price': max(equal_lows_below_tp1, key=lambda x: x['price'])['price']
                })
            else:
                log.debug(f"No valid liquidity-based TP2 found for {symbol} SELL, using TP1 only")
                tp_targets = [tp1]
        
        if 'tp2' in locals():
            tp_targets = [tp1, tp2]
        
        if entry_type == "PREMIUM_ZONE" and len(all_pools['equal_lows']) >= 2:
            if df_4h is not None and len(df_4h) >= 10:
                major_low_idx = df_4h['low'].iloc[-int(len(df_4h)*0.5):].idxmin()
                major_low = df_4h['low'].iloc[major_low_idx]
                
                if major_low < tp_targets[-1] * 0.95:
                    tp_targets.append(float(major_low))
                    tp_sources.append({
                        'tp_level': 3,
                        'type': 'major_swing_low',
                        'timeframe': '4h',
                        'reason': 'major_structure',
                        'strength': 3,
                        'original_price': float(major_low)
                    })
    
    if side == "BUY":
        if sl_price >= current_price:
            sl_price = current_price * 0.99
        
        tp_targets = [max(tp, current_price * 1.005) for tp in tp_targets]
        
        filtered_tps = []
        filtered_sources = []
        prev_tp = 0
        for tp, source in zip(tp_targets, tp_sources):
            if prev_tp == 0 or tp > prev_tp * 1.02:
                filtered_tps.append(tp)
                filtered_sources.append(source)
                prev_tp = tp
        tp_targets = filtered_tps[:3]
        tp_sources = filtered_sources[:3]
        
    else:
        if sl_price <= current_price:
            sl_price = current_price * 1.01
        
        tp_targets = [min(tp, current_price * 0.995) for tp in tp_targets]
        
        filtered_tps = []
        filtered_sources = []
        prev_tp = float('inf')
        for tp, source in zip(tp_targets, tp_sources):
            if prev_tp == float('inf') or tp < prev_tp * 0.98:
                filtered_tps.append(tp)
                filtered_sources.append(source)
                prev_tp = tp
        tp_targets = filtered_tps[:3]
        tp_sources = filtered_sources[:3]
    
    risk = abs(current_price - sl_price)
    if risk > 0 and len(tp_targets) > 0:
        reward = abs(tp_targets[0] - current_price)
        rr_ratio = reward / risk
    else:
        rr_ratio = 0
    
    liquidity_analysis = {
        'side': side,
        'entry_type': entry_type,
        'identified_pools': {
            'buy_stops': len(all_pools['buy_stops']),
            'sell_stops': len(all_pools['sell_stops']),
            'equal_highs': len(all_pools['equal_highs']),
            'equal_lows': len(all_pools['equal_lows'])
        },
        'sl_source': sl_source,
        'tp_sources': tp_sources,
        'rr_ratio': rr_ratio,
        'risk_pct': risk / current_price * 100,
        'reward_pct': abs(tp_targets[0] - current_price) / current_price * 100 if tp_targets else 0
    }
    
    return sl_price, tp_targets, tp_sources, liquidity_analysis

# ---------------- LAYER 1: FAST ELIGIBILITY CHECK ----------------
async def check_eligibility_fast(exchange, symbol: str) -> SetupEligibility:
    
    try:
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", 0)
        if current_price == 0:
            return SetupEligibility(eligible=False, disqualify_reason="No price")
    except Exception as e:
        log.debug(f"Failed to get ticker for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Ticker error")
    
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 50)
    if not ohlcv_1h or len(ohlcv_1h) < 30:
        return SetupEligibility(eligible=False, disqualify_reason="Insufficient 1H data")
    
    df_1h = create_dataframe(ohlcv_1h)
    if df_1h is None:
        return SetupEligibility(eligible=False, disqualify_reason="Dataframe error")
    
    try:
        df_1h['ema_20'] = df_1h['close'].ewm(span=20).mean()
        df_1h['ema_50'] = df_1h['close'].ewm(span=50).mean()
        
        latest_ema20 = df_1h['ema_20'].iloc[-1]
        latest_ema50 = df_1h['ema_50'].iloc[-1]
        
        if latest_ema20 > latest_ema50:
            bias = "BULLISH"
            potential_side = "BUY"
        elif latest_ema20 < latest_ema50:
            bias = "BEARISH"
            potential_side = "SELL"
        else:
            if current_price > latest_ema20:
                bias = "BULLISH"
                potential_side = "BUY"
            else:
                bias = "BEARISH"
                potential_side = "SELL"
    except Exception as e:
        log.debug(f"Trend detection error for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Trend detection error")
    
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 30)
    if not ohlcv_15m or len(ohlcv_15m) < 10:
        return SetupEligibility(eligible=False, disqualify_reason="Insufficient 15m data")
    
    df_15m = create_dataframe(ohlcv_15m)
    if df_15m is None:
        return SetupEligibility(eligible=False, disqualify_reason="15m dataframe error")
    
    entry_found = False
    entry_price = current_price
    entry_type = ""
    
    try:
        recent_low_15m = df_15m['low'].iloc[-5:].min()
        recent_high_15m = df_15m['high'].iloc[-5:].max()
        
        if potential_side == "BUY":
            if current_price <= recent_low_15m * 1.01:
                entry_price = current_price
                entry_type = "DISCOUNT_ZONE"
                entry_found = True
            elif len(df_15m) >= 3:
                last_candle = df_15m.iloc[-1]
                prev_candle = df_15m.iloc[-2]
                
                if (prev_candle['close'] < prev_candle['open'] and
                    last_candle['close'] > last_candle['open'] and
                    last_candle['close'] > prev_candle['open'] and
                    last_candle['open'] < prev_candle['close']):
                    entry_price = last_candle['close']
                    entry_type = "BULLISH_ENGULFING"
                    entry_found = True
        else:
            if current_price >= recent_high_15m * 0.99:
                entry_price = current_price
                entry_type = "PREMIUM_ZONE"
                entry_found = True
            elif len(df_15m) >= 3:
                last_candle = df_15m.iloc[-1]
                prev_candle = df_15m.iloc[-2]
                
                if (prev_candle['close'] > prev_candle['open'] and
                    last_candle['close'] < last_candle['open'] and
                    last_candle['close'] < prev_candle['open'] and
                    last_candle['open'] > prev_candle['close']):
                    entry_price = last_candle['close']
                    entry_type = "BEARISH_ENGULFING"
                    entry_found = True
    except Exception as e:
        log.debug(f"Entry detection error for {symbol}: {e}")
        return SetupEligibility(eligible=False, disqualify_reason="Entry detection error")
    
    if not entry_found:
        return SetupEligibility(eligible=False, disqualify_reason="No entry setup detected")
    
    return SetupEligibility(
        eligible=True,
        side=potential_side,
        entry_price=entry_price,
        entry_type=entry_type
    )

# ---------------- LAYER 2: QUALITY ANALYSIS ----------------
async def analyze_quality(exchange, symbol: str, eligibility: SetupEligibility, 
                         liquidity_setup: LiquiditySetup) -> SetupQuality:
    
    side = eligibility.side
    entry_type = eligibility.entry_type
    entry_price = eligibility.entry_price
    
    sweep_strength = 0.0
    structure_shift = False
    from_liquidity_exists = False
    confirmation_candle = False
    htfc_alignment_score = 0.0
    
    step_details_dict = {}
    
    eight_steps = {
        'step_1_htf_bias': False,
        'step_2_zone_type': False,
        'step_3_liquidity_sweep': False,
        'step_4_structure_shift': False,
        'step_5_from_liquidity': False,
        'step_6_confirmation_candle': False,
        'step_7_entry_validity': False,
        'step_8_liquidity_alignment': False,
        
        'step_details': step_details_dict,
        'rr_ratio': liquidity_setup.rr_ratio,
        
        'step_specifics': {
            '1': {'trend': '', 'score': 0.0, 'alignment': ''},
            '2': {'entry_type': entry_type, 'zone_quality': ''},
            '3': {'strength': 0.0, 'sweep_type': '', 'details': ''},
            '4': {'shift_type': '', 'confirmed': False},
            '5': {'liquidity_type': '', 'present': False},
            '6': {'candle_type': '', 'direction': ''},
            '7': {'distance_pct': 0.0, 'in_zone': False},
            '8': {'pools_analyzed': 0, 'alignment_score': 0}
        }
    }
    
    try:
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", entry_price)
        
        ohlcv_4h = await fetch_ohlcv(exchange, symbol, "4h", 50)
        htf_trend = "NEUTRAL"
        
        if ohlcv_4h:
            df_4h = create_dataframe(ohlcv_4h)
            if df_4h is not None and len(df_4h) >= 20:
                df_4h['ema_20'] = df_4h['close'].ewm(span=20).mean()
                df_4h['ema_50'] = df_4h['close'].ewm(span=50).mean()
                
                ema20 = df_4h['ema_20'].iloc[-1]
                ema50 = df_4h['ema_50'].iloc[-1]
                
                if side == "BUY":
                    htfc_alignment_score = 1.0 if ema20 > ema50 else 0.5
                    eight_steps['step_1_htf_bias'] = htfc_alignment_score >= 0.7
                    htf_trend = "BULLISH" if ema20 > ema50 else "BEARISH" if ema20 < ema50 else "NEUTRAL"
                else:
                    htfc_alignment_score = 1.0 if ema20 < ema50 else 0.5
                    eight_steps['step_1_htf_bias'] = htfc_alignment_score >= 0.7
                    htf_trend = "BEARISH" if ema20 < ema50 else "BULLISH" if ema20 > ema50 else "NEUTRAL"
                
                eight_steps['step_specifics']['1']['trend'] = htf_trend
                eight_steps['step_specifics']['1']['score'] = htfc_alignment_score
                eight_steps['step_specifics']['1']['alignment'] = "Aligned" if eight_steps['step_1_htf_bias'] else "Not Aligned"
        
        step_details_dict['1'] = f"Higher Timeframe Bias: {htf_trend} (Score: {htfc_alignment_score:.2f}/1.0)"
        
        if side == "BUY" and entry_type in ["DISCOUNT_ZONE", "BULLISH_ENGULFING"]:
            eight_steps['step_2_zone_type'] = True
            zone_quality = "Optimal"
        elif side == "SELL" and entry_type in ["PREMIUM_ZONE", "BEARISH_ENGULFING"]:
            eight_steps['step_2_zone_type'] = True
            zone_quality = "Optimal"
        else:
            zone_quality = "Suboptimal"
        
        eight_steps['step_specifics']['2']['entry_type'] = entry_type
        eight_steps['step_specifics']['2']['zone_quality'] = zone_quality
        
        step_details_dict['2'] = f"Entry Type: {entry_type} ({zone_quality})"
        
        ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 50)
        sweep_type = "None"
        sweep_details = ""
        
        if ohlcv_15m:
            df_15m = create_dataframe(ohlcv_15m)
            if df_15m is not None and len(df_15m) >= 20:
                if side == "BUY":
                    recent_low = df_15m['low'].iloc[-5:].min()
                    prev_lows = df_15m['low'].iloc[-20:-5]
                    
                    if len(prev_lows) > 0:
                        prev_significant_low = prev_lows.min()
                        if recent_low < prev_significant_low * 0.995:
                            sweep_strength = 0.8
                            eight_steps['step_3_liquidity_sweep'] = True
                            sweep_type = "LOW_SWEEP"
                            sweep_details = f"Swept low: {prev_significant_low:.8f} → {recent_low:.8f}"
                            
                            sweep_idx = df_15m['low'].iloc[-5:].idxmin()
                            if sweep_idx < len(df_15m) - 2:
                                sweep_candle = df_15m.iloc[sweep_idx]
                                body_size = abs(sweep_candle['close'] - sweep_candle['open'])
                                lower_wick = min(sweep_candle['open'], sweep_candle['close']) - sweep_candle['low']
                                
                                if lower_wick > body_size * 1.5:
                                    sweep_strength = 1.0
                                    from_liquidity_exists = True
                                    eight_steps['step_5_from_liquidity'] = True
                                    sweep_details += " (Strong wick - smart money entry)"
                else:
                    recent_high = df_15m['high'].iloc[-5:].max()
                    prev_highs = df_15m['high'].iloc[-20:-5]
                    
                    if len(prev_highs) > 0:
                        prev_significant_high = prev_highs.max()
                        if recent_high > prev_significant_high * 1.005:
                            sweep_strength = 0.8
                            eight_steps['step_3_liquidity_sweep'] = True
                            sweep_type = "HIGH_SWEEP"
                            sweep_details = f"Swept high: {prev_significant_high:.8f} → {recent_high:.8f}"
                            
                            sweep_idx = df_15m['high'].iloc[-5:].idxmax()
                            if sweep_idx < len(df_15m) - 2:
                                sweep_candle = df_15m.iloc[sweep_idx]
                                body_size = abs(sweep_candle['close'] - sweep_candle['open'])
                                upper_wick = sweep_candle['high'] - max(sweep_candle['open'], sweep_candle['close'])
                                
                                if upper_wick > body_size * 1.5:
                                    sweep_strength = 1.0
                                    from_liquidity_exists = True
                                    eight_steps['step_5_from_liquidity'] = True
                                    sweep_details += " (Strong wick - smart money entry)"
        
        eight_steps['step_specifics']['3']['strength'] = sweep_strength
        eight_steps['step_specifics']['3']['sweep_type'] = sweep_type
        eight_steps['step_specifics']['3']['details'] = sweep_details
        
        step_details_dict['3'] = f"Liquidity Sweep: {sweep_type} (Strength: {sweep_strength:.2f}/1.0)"
        if sweep_details:
            step_details_dict['3'] += f"\n   {sweep_details}"
        
        shift_type = "None"
        if ohlcv_4h:
            df_4h = create_dataframe(ohlcv_4h)
            if df_4h is not None and len(df_4h) >= 11:
                if side == "BUY":
                    recent_highs = df_4h['high'].iloc[-10:-1]
                    if len(recent_highs) > 0:
                        previous_high = recent_highs.max()
                        current_high = df_4h['high'].iloc[-1]
                        
                        if current_high > previous_high:
                            structure_shift = True
                            eight_steps['step_4_structure_shift'] = True
                            shift_type = "HIGHER_HIGH"
                else:
                    recent_lows = df_4h['low'].iloc[-10:-1]
                    if len(recent_lows) > 0:
                        previous_low = recent_lows.min()
                        current_low = df_4h['low'].iloc[-1]
                        
                        if current_low < previous_low:
                            structure_shift = True
                            eight_steps['step_4_structure_shift'] = True
                            shift_type = "LOWER_LOW"
        
        eight_steps['step_specifics']['4']['shift_type'] = shift_type
        eight_steps['step_specifics']['4']['confirmed'] = structure_shift
        
        step_details_dict['4'] = f"Structure Shift: {shift_type} ({'✅ Confirmed' if structure_shift else '❌ Not Confirmed'})"
        
        liquidity_type = "None"
        if from_liquidity_exists:
            liquidity_type = "Smart Money Entry"
        
        eight_steps['step_specifics']['5']['liquidity_type'] = liquidity_type
        eight_steps['step_specifics']['5']['present'] = from_liquidity_exists
        
        step_details_dict['5'] = f"FROM Liquidity: {liquidity_type} ({'✅ Present' if from_liquidity_exists else '❌ Absent'})"
        
        ohlcv_5m = await fetch_ohlcv(exchange, symbol, "5m", 10)
        candle_type = "None"
        candle_direction = ""
        
        if ohlcv_5m:
            df_5m = create_dataframe(ohlcv_5m)
            if df_5m is not None and len(df_5m) >= 3:
                if side == "BUY":
                    last_candle = df_5m.iloc[-1]
                    if last_candle['close'] > last_candle['open']:
                        confirmation_candle = True
                        eight_steps['step_6_confirmation_candle'] = True
                        candle_type = "BULLISH"
                        candle_direction = "Up"
                else:
                    last_candle = df_5m.iloc[-1]
                    if last_candle['close'] < last_candle['open']:
                        confirmation_candle = True
                        eight_steps['step_6_confirmation_candle'] = True
                        candle_type = "BEARISH"
                        candle_direction = "Down"
        
        eight_steps['step_specifics']['6']['candle_type'] = candle_type
        eight_steps['step_specifics']['6']['direction'] = candle_direction
        
        step_details_dict['6'] = f"Confirmation Candle: {candle_type} ({'✅ Confirmed' if confirmation_candle else '❌ Not Confirmed'})"
        
        price_diff_pct = abs(current_price - entry_price) / entry_price * 100
        in_zone = price_diff_pct <= 1.5
        eight_steps['step_7_entry_validity'] = in_zone
        
        eight_steps['step_specifics']['7']['distance_pct'] = price_diff_pct
        eight_steps['step_specifics']['7']['in_zone'] = in_zone
        
        step_details_dict['7'] = f"Entry Validity: Distance {price_diff_pct:.2f}% ({'✅ In Zone' if in_zone else '❌ Outside Zone'})"
        
        liquidity_analysis = liquidity_setup.liquidity_analysis
        pools_analyzed = 0
        alignment_score = 0
        
        if liquidity_analysis:
            pools = liquidity_analysis.get('identified_pools', {})
            pools_analyzed = sum(pools.values())
            
            if pools_analyzed >= 8:
                alignment_score = 1.0
            elif pools_analyzed >= 4:
                alignment_score = 0.7
            elif pools_analyzed >= 2:
                alignment_score = 0.5
            
            eight_steps['step_8_liquidity_alignment'] = pools_analyzed >= 2
        
        eight_steps['step_specifics']['8']['pools_analyzed'] = pools_analyzed
        eight_steps['step_specifics']['8']['alignment_score'] = alignment_score
        
        step_details_dict['8'] = f"Liquidity Alignment: {pools_analyzed} pools analyzed ({'✅ Aligned' if eight_steps['step_8_liquidity_alignment'] else '❌ Weak Alignment'})"
        
    except Exception as e:
        log.debug(f"Quality analysis error for {symbol}: {e}")
    
    total_score = (
        sweep_strength +
        (1.0 if structure_shift else 0.0) +
        (0.5 if from_liquidity_exists else 0.0) +
        (0.5 if confirmation_candle else 0.0) +
        htfc_alignment_score +
        (0.5 if eight_steps['step_7_entry_validity'] else 0.0) +
        (0.5 if eight_steps['step_8_liquidity_alignment'] else 0.0)
    )
    
    return SetupQuality(
        sweep_strength=sweep_strength,
        structure_shift=structure_shift,
        from_liquidity_exists=from_liquidity_exists,
        confirmation_candle=confirmation_candle,
        htfc_alignment_score=htfc_alignment_score,
        total_score=total_score,
        eight_steps_status=eight_steps
    )

# ---------------- ENHANCED SCANNER WITH DIRECTION ENGINE ----------------
async def scan_symbol_with_direction(exchange, symbol: str) -> Optional[Dict]:
    """Enhanced scanner with direction engine"""
    
    try:
        # LAYER 1: Eligibility check
        eligibility = await check_eligibility_fast(exchange, symbol)
        
        if not eligibility.eligible:
            return None
        
        # Calculate liquidity-based TP/SL
        sl_price, tp_targets, tp_sources, liquidity_analysis = await calculate_liquidity_tp_sl(
            exchange, symbol, eligibility.side, eligibility.entry_price, eligibility.entry_type
        )
        
        # STRICT VALIDATION
        if sl_price is None or tp_targets is None or len(tp_targets) == 0:
            log.debug(f"Rejecting {symbol}: No valid liquidity pools found for TP/SL")
            return None
        
        if eligibility.entry_price <= 0 or sl_price <= 0:
            log.debug(f"Rejecting {symbol}: Invalid prices (entry={eligibility.entry_price}, sl={sl_price})")
            return None
        
        risk_pct = abs(eligibility.entry_price - sl_price) / eligibility.entry_price * 100
        if risk_pct > 10:
            log.debug(f"Rejecting {symbol}: Risk too high ({risk_pct:.1f}%)")
            return None
        
        # Create liquidity setup
        risk = abs(eligibility.entry_price - sl_price)
        reward = abs(tp_targets[0] - eligibility.entry_price) if tp_targets else 0
        rr_ratio = reward / risk if risk > 0 else 0
        
        liquidity_setup = LiquiditySetup(
            sl_price=sl_price,
            tp_targets=tp_targets,
            tp_sources=tp_sources,
            liquidity_analysis=liquidity_analysis,
            rr_ratio=rr_ratio
        )
        
        # ======= NEW: DIRECTION ENGINE =======
        direction_metrics = await direction_engine.analyze_direction(
            exchange, symbol, liquidity_setup.__dict__, eligibility.side, eligibility
        )
        
        # Apply direction confidence filter
        if direction_metrics.confidence_tier == DirectionTier.LOW and direction_metrics.has_major_conflicts:
            log.debug(f"Rejecting {symbol}: LOW direction confidence with conflicts")
            return None
        
        # If trapped side opposes proposed side, require higher quality
        if direction_metrics.trapped_side != TrappedSide.NONE:
            trapped_opposes = (
                (direction_metrics.trapped_side == TrappedSide.LONG and eligibility.side == "BUY") or
                (direction_metrics.trapped_side == TrappedSide.SHORT and eligibility.side == "SELL")
            )
            if trapped_opposes and direction_metrics.trapped_confidence > 0.6:
                log.debug(f"Rejecting {symbol}: Strong trapped {direction_metrics.trapped_side.value} vs proposed {eligibility.side}")
                return None
        
        # ======= CONTINUE WITH QUALITY ANALYSIS =======
        quality = await analyze_quality(exchange, symbol, eligibility, liquidity_setup)
        
        # Enhanced filtering with direction score
        required_score = MIN_QUALITY_SCORE
        if direction_metrics.confidence_tier == DirectionTier.HIGH:
            required_score -= 0.2
        elif direction_metrics.confidence_tier == DirectionTier.LOW:
            required_score += 0.3
        
        if quality.total_score < required_score:
            return None
        
        # Get current price
        ticker = await safe_fetch_ticker(exchange, symbol)
        current_price = ticker.get("last", eligibility.entry_price)
        
        # Build enhanced setup
        setup = {
            "symbol": symbol,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "side": eligibility.side,
            "current_price": current_price,
            "entry_price": eligibility.entry_price,
            "entry_type": eligibility.entry_type,
            "sl_price": sl_price,
            "tp_targets": tp_targets,
            "tp_sources": tp_sources,
            "risk": risk,
            "reward": reward,
            "rr_ratio": rr_ratio,
            
            "quality": {
                "tier": quality.quality_tier,
                "total_score": quality.total_score,
                "eight_steps": quality.eight_steps_status
            },
            
            "liquidity_analysis": liquidity_analysis,
            
            "direction_engine": {
                "confidence_tier": direction_metrics.confidence_tier.value,
                "direction_score": direction_metrics.direction_score,
                "trapped_side": direction_metrics.trapped_side.value,
                "trapped_confidence": direction_metrics.trapped_confidence,
                "bleeding_side": direction_metrics.bleeding_side,
                "funding_extreme": direction_metrics.funding_extreme,
                "micro_confirmation": direction_metrics.micro_confirmation,
                "micro_type": direction_metrics.micro_timeframe,
                "rejection_type": direction_metrics.rejection_type.value,
                "conflict_warnings": direction_metrics.conflict_warnings,
                "weighted_score": quality.total_score * (1 + abs(direction_metrics.direction_score))
            }
        }
        
        return setup
        
    except Exception as e:
        log.error(f"Enhanced scanner error for {symbol}: {e}")
        return None

# ---------------- ENHANCED ALERTS ----------------
async def send_enhanced_alert(setup: Dict):
    """Send enhanced alerts with direction engine context"""
    
    try:
        symbol = setup.get('symbol', 'UNKNOWN')
        quality = setup.get('quality', {})
        direction = setup.get('direction_engine', {})
        liquidity = setup.get('liquidity_analysis', {})
        eight_steps = quality.get('eight_steps', {})
        step_specifics = eight_steps.get('step_specifics', {})
        
        key = signal_tracker.get_signal_key(setup)
        symbol, side, bucket = key
        
        update_emoji = "🆕"
        
        tier_emoji = {
            "A+": "🔥",
            "A": "✅", 
            "B": "⚠️",
            "C": "📊"
        }.get(quality.get("tier", "C"), "📊")
        
        tp_targets = setup.get('tp_targets', [])
        tp_sources = setup.get('tp_sources', [])
        entry_price = setup.get('entry_price', 0)
        
        tp_lines = []
        for i, tp in enumerate(tp_targets):
            if entry_price > 0:
                distance_pct = abs(tp - entry_price) / entry_price * 100
                
                source_info = ""
                if i < len(tp_sources):
                    source = tp_sources[i]
                    source_type = source.get('type', 'unknown')
                    timeframe = source.get('timeframe', 'N/A')
                    reason = source.get('reason', 'unknown')
                    
                    source_emoji = {
                        'buy_stop_pool': '🛑',
                        'sell_stop_pool': '🛑',
                        'equal_high': '🏔️',
                        'equal_low': '🏞️',
                        'major_swing_high': '⛰️',
                        'major_swing_low': '🗻',
                        'recent_swing': '↕️',
                        'risk_based': '🎯',
                        'recent_low': '📉',
                        'recent_high': '📈',
                        'premium_zone': '💰',
                        'discount_zone': '💸',
                        'higher_timeframe_pool': '🕒',
                        'next_pool': '➡️',
                        'major_structure': '🏛️',
                    }.get(source_type, '📌')
                    
                    source_info = f" {source_emoji}{timeframe}:{reason}"
                
                tp_lines.append(f"TP{i+1}: {tp:.8f} ({distance_pct:.1f}%){source_info}")
        
        step_checks = []
        step_passes = sum([
            eight_steps.get('step_1_htf_bias', False),
            eight_steps.get('step_2_zone_type', False),
            eight_steps.get('step_3_liquidity_sweep', False),
            eight_steps.get('step_4_structure_shift', False),
            eight_steps.get('step_5_from_liquidity', False),
            eight_steps.get('step_6_confirmation_candle', False),
            eight_steps.get('step_7_entry_validity', False),
            eight_steps.get('step_8_liquidity_alignment', False)
        ])
        
        step_checks.append("✅" if eight_steps.get('step_1_htf_bias', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_2_zone_type', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_3_liquidity_sweep', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_4_structure_shift', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_5_from_liquidity', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_6_confirmation_candle', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_7_entry_validity', False) else "❌")
        step_checks.append("✅" if eight_steps.get('step_8_liquidity_alignment', False) else "❌")
        
        checklist_lines = []
        checklist_lines.append(f"1️⃣ HTF: {step_specifics.get('1', {}).get('trend', 'N/A')} ({step_specifics.get('1', {}).get('score', 0):.1f})")
        checklist_lines.append(f"2️⃣ Zone: {step_specifics.get('2', {}).get('entry_type', 'N/A')} ({step_specifics.get('2', {}).get('zone_quality', 'N/A')})")
        checklist_lines.append(f"3️⃣ Sweep: {step_specifics.get('3', {}).get('sweep_type', 'None')} ({step_specifics.get('3', {}).get('strength', 0):.1f})")
        checklist_lines.append(f"4️⃣ Shift: {step_specifics.get('4', {}).get('shift_type', 'None')}")
        checklist_lines.append(f"5️⃣ Smart $: {'Yes' if step_specifics.get('5', {}).get('present', False) else 'No'}")
        checklist_lines.append(f"6️⃣ Confirm: {step_specifics.get('6', {}).get('candle_type', 'None')}")
        checklist_lines.append(f"7️⃣ Distance: {step_specifics.get('7', {}).get('distance_pct', 0):.1f}%")
        checklist_lines.append(f"8️⃣ Pools: {step_specifics.get('8', {}).get('pools_analyzed', 0)}")
        
        liquidity_summary = ""
        if liquidity:
            pools = liquidity.get('identified_pools', {})
            liquidity_summary = f"💧 Pools: B{pools.get('buy_stops', 0)}/S{pools.get('sell_stops', 0)}/H{pools.get('equal_highs', 0)}/L{pools.get('equal_lows', 0)}"
            
            sl_source = liquidity.get('sl_source', {})
            if sl_source:
                sl_type = sl_source.get('type', 'unknown')
                sl_timeframe = sl_source.get('timeframe', 'N/A')
                sl_reason = sl_source.get('reason', 'unknown')
                
                sl_source_emoji = {
                    'buy_stop_pool': '🛑',
                    'sell_stop_pool': '🛑',
                    'equal_high': '🏔️',
                    'equal_low': '🏞️',
                    'recent_high': '📈',
                    'recent_low': '📉',
                    'fixed_percentage': '⚠️',
                    'adjusted': '⚙️'
                }.get(sl_type, '🛡️')
                
                liquidity_summary += f" | SL: {sl_source_emoji}{sl_timeframe}:{sl_reason}"
        
        risk = setup.get('risk', 0)
        reward = setup.get('reward', 0)
        rr_ratio = setup.get('rr_ratio', 0)
        
        if entry_price > 0:
            risk_pct = risk / entry_price * 100
            reward_pct = reward / entry_price * 100 if reward > 0 else 0
        else:
            risk_pct = 0
            reward_pct = 0
        
        current_price = setup.get('current_price', 0)
        entry_distance_pct = abs(current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        
        direction_confidence = direction.get('confidence_tier', 'LOW')
        direction_score = direction.get('direction_score', 0)
        trapped_side = direction.get('trapped_side', 'NONE')
        bleeding_side = direction.get('bleeding_side', '')
        funding_extreme = direction.get('funding_extreme', 0)
        
        direction_context = []
        
        if trapped_side != 'NONE':
            trapped_emoji = "🐻" if trapped_side == "LONG" else "🐂" if trapped_side == "SHORT" else "⚖️"
            direction_context.append(f"{trapped_emoji} Trapped {trapped_side} ({direction.get('trapped_confidence', 0):.0%})")
        
        if bleeding_side:
            bleeding_emoji = "🩸" if abs(funding_extreme) > 0.05 else "💸"
            direction_context.append(f"{bleeding_emoji} {bleeding_side} paying {funding_extreme:.3f}%")
        
        if direction.get('micro_confirmation', False):
            micro_emoji = "✅"
            micro_type = direction.get('micro_type', '')
            direction_context.append(f"{micro_emoji} {micro_type} confirmed")
        
        if direction.get('conflict_warnings') and len(direction['conflict_warnings']) > 1:
            direction_context.append(f"⚠️ {len(direction['conflict_warnings'])-1} conflicts")
        
        direction_emoji = {
            "HIGH": "🔮",
            "MEDIUM": "🎯",
            "LOW": "⚠️"
        }.get(direction_confidence, "⚡")
        
        msg = f"""🎯 <b>ROMEOTPT v5.0 - {symbol} | {side} | 0.5-Bucket:{bucket}</b>
{direction_emoji} <b>Direction Confidence: {direction_confidence}</b>

{chr(10).join(direction_context) if direction_context else "📊 Standard liquidity signal"}

⚖️ <b>Direction Score:</b> {direction_score:+.2f}/1.0
📈 <b>Weighted Quality:</b> {quality.get('total_score', 0) * (1 + abs(direction_score)):.2f}

Entry: <code>{entry_price:.8f}</code> | Now: <code>{current_price:.8f}</code> ({entry_distance_pct:.1f}%)
Type: {setup.get('entry_type', 'N/A')}

🎯 <b>Targets:</b>
{chr(10).join(tp_lines)}
🛡️ <b>SL:</b> <code>{setup.get('sl_price', 0):.8f}</code> ({abs(setup.get('sl_price', 0) - entry_price) / entry_price * 100:.1f}%)

⚖️ <b>Risk/Reward:</b> {risk_pct:.1f}% risk | {reward_pct:.1f}% reward | <b>{rr_ratio:.1f}:1</b>
{liquidity_summary}

🔬 <b>8-Step Analysis ({step_passes}/8):</b> {"".join(step_checks)}
{chr(10).join(checklist_lines)}

🏆 <b>Quality:</b> {quality.get('total_score', 0):.1f}/5.0 ({quality.get('tier', 'C')}) | {step_passes}/8 steps

<i>Forced Move Probability: {'HIGH' if direction_confidence == 'HIGH' else 'MODERATE' if direction_confidence == 'MEDIUM' else 'LOW'}</i>
<i>{datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>
"""
        
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Error sending enhanced alert: {e}")

async def send_deduped_enhanced_alert(setup: Dict):
    """Send alert ONLY if it's a NEW (symbol, side, 0.5-bucket) combination"""
    try:
        should_alert = signal_tracker.should_send_alert(setup)
        
        if should_alert:
            await send_enhanced_alert(setup)
            signal_tracker.update_signal(setup, alerted=True)
            
            key = signal_tracker.get_signal_key(setup)
            symbol, side, bucket = key
            quality_score = setup.get('quality', {}).get('total_score', 0)
            log.info(f"📨 ENHANCED SIGNAL sent: {symbol} {side} | "
                    f"Score:{quality_score:.2f} → Bucket:{bucket} | "
                    f"Direction: {setup.get('direction_engine', {}).get('confidence_tier', 'LOW')}")
            return True
        else:
            signal_tracker.update_signal(setup, alerted=False)
            
            if np.random.random() < 0.05:
                key = signal_tracker.get_signal_key(setup)
                if key in signal_tracker.active_signals:
                    signal = signal_tracker.active_signals[key]
                    time_active = (datetime.datetime.utcnow() - signal.get('first_seen', datetime.datetime.utcnow())).total_seconds() / 60
                    symbol, side, bucket = key
                    quality_score = setup.get('quality', {}).get('total_score', 0)
                    log.debug(f"⏸️  Skipping {symbol} {side}: Already has active signal in bucket {bucket} "
                             f"(Score:{quality_score:.2f}, {time_active:.1f}m old)")
            
            return False
    except Exception as e:
        log.error(f"Error in deduped enhanced alert: {e}")
        return False

async def send_outcome_alert(symbol: str, outcome: Dict):
    try:
        signal_key = outcome.get('signal_key')
        if not signal_key:
            return
        
        signal = signal_tracker.active_signals.get(signal_key, {})
        setup = signal.get('setup', {})
        
        if 'TP' in outcome['type']:
            emoji = "✅" if outcome['tp_level'] == 1 else "🎯" if outcome['tp_level'] == 2 else "🏆"
            result_text = f"TP{outcome['tp_level']} HIT"
            
            tp_sources = setup.get('tp_sources', [])
            tp_source_info = ""
            for source in tp_sources:
                if source.get('tp_level') == outcome['tp_level']:
                    source_type = source.get('type', 'unknown')
                    timeframe = source.get('timeframe', 'N/A')
                    reason = source.get('reason', 'unknown')
                    tp_source_info = f" | Source: {timeframe}:{reason}"
                    break
        else:
            emoji = "❌"
            result_text = "SL HIT"
            tp_source_info = ""
        
        bars_held = outcome.get('bars_held', 0)
        if bars_held < 60:
            time_str = f"{bars_held}min"
        else:
            time_str = f"{bars_held//60}h{bars_held%60}m"
        
        quality = setup.get('quality', {})
        
        eight_steps = quality.get('eight_steps', {})
        step_passes = sum([
            eight_steps.get('step_1_htf_bias', False),
            eight_steps.get('step_2_zone_type', False),
            eight_steps.get('step_3_liquidity_sweep', False),
            eight_steps.get('step_4_structure_shift', False),
            eight_steps.get('step_5_from_liquidity', False),
            eight_steps.get('step_6_confirmation_candle', False),
            eight_steps.get('step_7_entry_validity', False),
            eight_steps.get('step_8_liquidity_alignment', False)
        ])
        
        direction = setup.get('direction_engine', {})
        direction_conf = direction.get('confidence_tier', 'LOW')
        
        msg = f"""{emoji} <b>{result_text} - {symbol}</b>
Signal: {setup.get('side', 'N/A')} | Score: {quality.get('total_score', 0):.1f} | Steps: {step_passes}/8 | Direction: {direction_conf}{tp_source_info}

Entry: <code>{setup.get('entry_price', 0):.8f}</code>
Exit: <code>{outcome['price']:.8f}</code>
PnL: <code>{outcome['pnl_pct']:+.2f}%</code> | RR: {setup.get('rr_ratio', 0):.1f}:1

⏱️ {time_str} | Fav: {outcome.get('max_favorable', 0):.1f}% | Adv: {outcome.get('max_adverse', 0):.1f}%

<i>{datetime.datetime.utcnow().strftime('%H:%M')}</i>
"""
        
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Error sending outcome alert: {e}")

# ---------------- DATABASE ----------------
async def migrate_database():
    try:
        cursor = await db_conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='signals_v5_0'
        """)
        table_exists = await cursor.fetchone()
        
        if not table_exists:
            log.info("✅ No old database found, will create fresh")
            return True
        
        cursor = await db_conn.execute("PRAGMA table_info(signals_v5_0)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'direction_score' in column_names:
            log.info("✅ Database already has new schema with direction columns")
            return True
        
        log.warning("⚠️  Old database schema detected - migrating to new schema")
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_v5_0_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                side TEXT,
                score REAL DEFAULT 0.0,
                timestamp TEXT,
                entry_price REAL,
                sl_price REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                rr_ratio REAL,
                quality_tier TEXT,
                quality_score REAL,
                current_price REAL,
                liquidity_buy_stops INTEGER,
                liquidity_sell_stops INTEGER,
                eight_steps_passed INTEGER,
                direction_confidence_tier TEXT,
                direction_score REAL,
                trapped_side TEXT,
                trapped_confidence REAL,
                bleeding_side TEXT,
                funding_extreme REAL,
                micro_confirmation BOOLEAN,
                conflict_count INTEGER,
                status TEXT DEFAULT 'active',
                alert_sent BOOLEAN DEFAULT 1,
                closed_at TEXT,
                closed_price REAL,
                outcome TEXT,
                pnl_pct REAL,
                bars_held INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                UNIQUE(symbol, side, score)
            )
        """)
        
        await db_conn.execute("""
            INSERT INTO signals_v5_0_new 
            (symbol, side, score, timestamp, entry_price, sl_price, tp1, tp2, tp3, 
             rr_ratio, quality_tier, quality_score, current_price, 
             liquidity_buy_stops, liquidity_sell_stops, eight_steps_passed,
             direction_confidence_tier, direction_score, trapped_side, trapped_confidence,
             bleeding_side, funding_extreme, micro_confirmation, conflict_count,
             status, alert_sent, closed_at, closed_price, outcome,
             pnl_pct, bars_held, max_favorable_pct, max_adverse_pct)
            SELECT 
            symbol, side, score, timestamp, entry_price, sl_price, tp1, tp2, tp3,
            rr_ratio, quality_tier, quality_score, current_price,
            liquidity_buy_stops, liquidity_sell_stops, eight_steps_passed,
            'LOW', 0.0, 'NONE', 0.0, '', 0.0, 0, 0,
            status, alert_sent, closed_at, closed_price, outcome,
            pnl_pct, bars_held, max_favorable_pct, max_adverse_pct
            FROM signals_v5_0
        """)
        
        await db_conn.execute("DROP TABLE IF EXISTS signal_outcomes_v5_0")
        await db_conn.execute("DROP TABLE IF EXISTS signals_v5_0")
        
        await db_conn.execute("ALTER TABLE signals_v5_0_new RENAME TO signals_v5_0")
        
        await db_conn.commit()
        log.info("✅ Database migrated successfully to v5.0 schema")
        return True
        
    except Exception as e:
        log.error(f"❌ Database migration failed: {e}")
        try:
            await db_conn.execute("DROP TABLE IF EXISTS signals_v5_0")
            await db_conn.execute("DROP TABLE IF EXISTS signal_outcomes_v5_0")
            await db_conn.execute("DROP TABLE IF EXISTS signals_v5_0_new")
            await db_conn.commit()
            log.info("🔄 Dropped old tables, will create fresh")
            return True
        except Exception as e2:
            log.error(f"❌ Failed to drop old tables: {e2}")
            raise

async def init_database():
    try:
        await migrate_database()
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_v5_0 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                side TEXT,
                score REAL,
                timestamp TEXT,
                entry_price REAL,
                sl_price REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                rr_ratio REAL,
                quality_tier TEXT,
                quality_score REAL,
                current_price REAL,
                liquidity_buy_stops INTEGER,
                liquidity_sell_stops INTEGER,
                eight_steps_passed INTEGER,
                direction_confidence_tier TEXT,
                direction_score REAL,
                trapped_side TEXT,
                trapped_confidence REAL,
                bleeding_side TEXT,
                funding_extreme REAL,
                micro_confirmation BOOLEAN,
                conflict_count INTEGER,
                status TEXT DEFAULT 'active',
                alert_sent BOOLEAN DEFAULT 1,
                closed_at TEXT,
                closed_price REAL,
                outcome TEXT,
                pnl_pct REAL,
                bars_held INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                UNIQUE(symbol, side, score)
            )
        """)
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_outcomes_v5_0 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                symbol TEXT,
                side TEXT,
                score REAL,
                entry_price REAL,
                sl_price REAL,
                tp1_price REAL,
                tp2_price REAL,
                tp3_price REAL,
                quality_score REAL,
                quality_tier TEXT,
                eight_steps_passed INTEGER,
                liquidity_buy_stops INTEGER,
                liquidity_sell_stops INTEGER,
                direction_confidence_tier TEXT,
                direction_score REAL,
                trapped_side TEXT,
                trapped_confidence REAL,
                created_at TEXT,
                status TEXT DEFAULT 'active',
                closed_at TEXT,
                closed_price REAL,
                outcome_type TEXT,
                pnl_pct REAL,
                hold_time_minutes INTEGER,
                max_favorable_pct REAL,
                max_adverse_pct REAL,
                FOREIGN KEY (signal_id) REFERENCES signals_v5_0 (id)
            )
        """)
        
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v5_0_signals_symbol_side_score ON signals_v5_0 (symbol, side, score)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v5_0_signals_status ON signals_v5_0 (status)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v5_0_signals_direction ON signals_v5_0 (direction_confidence_tier)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v5_0_outcomes_symbol_side_score ON signal_outcomes_v5_0 (symbol, side, score)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v5_0_outcomes_direction ON signal_outcomes_v5_0 (direction_confidence_tier)")
        
        await db_conn.commit()
        log.info("✅ Database v5.0 initialized with direction engine tracking")
    except Exception as e:
        log.error(f"❌ Error initializing database: {e}")
        raise

# ============ FIXED STORE_SIGNAL FUNCTION ============
async def store_signal(setup: Dict):
    async with db_lock:
        try:
            tp_targets = setup.get("tp_targets", [])
            liquidity = setup.get("liquidity_analysis", {})
            pools = liquidity.get("identified_pools", {})
            quality = setup.get("quality", {})
            eight_steps = quality.get("eight_steps", {})
            direction = setup.get("direction_engine", {})
            
            step_passes = sum([
                eight_steps.get('step_1_htf_bias', False),
                eight_steps.get('step_2_zone_type', False),
                eight_steps.get('step_3_liquidity_sweep', False),
                eight_steps.get('step_4_structure_shift', False),
                eight_steps.get('step_5_from_liquidity', False),
                eight_steps.get('step_6_confirmation_candle', False),
                eight_steps.get('step_7_entry_validity', False),
                eight_steps.get('step_8_liquidity_alignment', False)
            ])
            
            key = signal_tracker.get_signal_key(setup)
            _, _, bucket = key
            
            conflict_count = len(direction.get('conflict_warnings', []))
            
            # COUNT THE COLUMNS: We have 26 columns in the INSERT statement
            # Let's match them exactly with 26 values
            
            cursor = await db_conn.execute("""
                INSERT OR REPLACE INTO signals_v5_0 (
                    symbol, side, score, timestamp, entry_price, sl_price, 
                    tp1, tp2, tp3, rr_ratio, quality_tier, quality_score,
                    current_price, liquidity_buy_stops, liquidity_sell_stops,
                    eight_steps_passed, direction_confidence_tier, direction_score,
                    trapped_side, trapped_confidence, bleeding_side, funding_extreme,
                    micro_confirmation, conflict_count, status, alert_sent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                setup.get("symbol", ""),
                setup.get("side", ""),
                float(bucket),
                setup.get("timestamp", ""),
                float(setup.get("entry_price", 0)),
                float(setup.get("sl_price", 0)),
                float(tp_targets[0]) if len(tp_targets) > 0 else None,
                float(tp_targets[1]) if len(tp_targets) > 1 else None,
                float(tp_targets[2]) if len(tp_targets) > 2 else None,
                float(setup.get("rr_ratio", 0)),
                quality.get("tier", "C"),
                float(quality.get("total_score", 0)),
                float(setup.get("current_price", 0)),
                int(pools.get("buy_stops", 0)),
                int(pools.get("sell_stops", 0)),
                int(step_passes),
                direction.get("confidence_tier", "LOW"),
                float(direction.get("direction_score", 0)),
                direction.get("trapped_side", "NONE"),
                float(direction.get("trapped_confidence", 0)),
                direction.get("bleeding_side", ""),
                float(direction.get("funding_extreme", 0)),
                1 if direction.get("micro_confirmation", False) else 0,
                int(conflict_count),
                'active',
                1
            ))
            
            signal_id = cursor.lastrowid
            
            # Also fix the outcomes table insert
            await db_conn.execute("""
                INSERT INTO signal_outcomes_v5_0 (
                    signal_id, symbol, side, score, entry_price, sl_price, tp1_price,
                    tp2_price, tp3_price, quality_score, quality_tier,
                    eight_steps_passed, liquidity_buy_stops, liquidity_sell_stops,
                    direction_confidence_tier, direction_score, trapped_side, trapped_confidence,
                    created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_id,
                setup.get("symbol", ""),
                setup.get("side", ""),
                float(bucket),
                float(setup.get("entry_price", 0)),
                float(setup.get("sl_price", 0)),
                float(tp_targets[0]) if len(tp_targets) > 0 else None,
                float(tp_targets[1]) if len(tp_targets) > 1 else None,
                float(tp_targets[2]) if len(tp_targets) > 2 else None,
                float(quality.get("total_score", 0)),
                quality.get("tier", "C"),
                int(step_passes),
                int(pools.get("buy_stops", 0)),
                int(pools.get("sell_stops", 0)),
                direction.get("confidence_tier", "LOW"),
                float(direction.get("direction_score", 0)),
                direction.get("trapped_side", "NONE"),
                float(direction.get("trapped_confidence", 0)),
                setup.get("timestamp", ""),
                'active'
            ))
            
            await db_conn.commit()
            log.info(f"📊 Stored v5.0 signal for {setup.get('symbol', 'UNKNOWN')} {setup.get('side', '')} Bucket:{bucket}")
            
        except Exception as e:
            log.error(f"❌ Error storing signal {setup.get('symbol', 'UNKNOWN')}: {e}")
            import traceback
            log.error(f"❌ Traceback: {traceback.format_exc()}")

# ============ FIXED STORE_OUTCOME FUNCTION ============
async def store_outcome(symbol: str, outcome: Dict):
    async with db_lock:
        try:
            signal_key = outcome.get('signal_key')
            if not signal_key:
                return
            
            symbol_key, side_key, bucket_key = signal_key
            now = datetime.datetime.utcnow().isoformat()
            
            await db_conn.execute("""
                UPDATE signals_v5_0 
                SET status = 'closed', closed_at = ?, closed_price = ?, outcome = ?,
                    pnl_pct = ?, bars_held = ?, max_favorable_pct = ?, max_adverse_pct = ?
                WHERE symbol = ? AND side = ? AND score = ? AND status = 'active'
            """, (
                now,
                float(outcome.get('price', 0)),
                outcome.get('type', ''),
                float(outcome.get('pnl_pct', 0)),
                int(outcome.get('bars_held', 0)),
                float(outcome.get('max_favorable', 0)),
                float(outcome.get('max_adverse', 0)),
                symbol_key,
                side_key,
                float(bucket_key)
            ))
            
            await db_conn.execute("""
                UPDATE signal_outcomes_v5_0 
                SET status = 'closed', closed_at = ?, closed_price = ?, outcome_type = ?,
                    pnl_pct = ?, hold_time_minutes = ?, max_favorable_pct = ?, max_adverse_pct = ?
                WHERE symbol = ? AND side = ? AND score = ? AND status = 'active'
            """, (
                now,
                float(outcome.get('price', 0)),
                outcome.get('type', ''),
                float(outcome.get('pnl_pct', 0)),
                int(outcome.get('bars_held', 0)),
                float(outcome.get('max_favorable', 0)),
                float(outcome.get('max_adverse', 0)),
                symbol_key,
                side_key,
                float(bucket_key)
            ))
            
            await db_conn.commit()
            log.info(f"📊 Stored outcome for {symbol_key} {side_key} Bucket:{bucket_key}: {outcome.get('type', 'UNKNOWN')}")
            
        except Exception as e:
            log.error(f"❌ Error storing outcome for {symbol}: {e}")

# ---------------- OUTCOME CHECKER ----------------
async def outcome_checker_task(exchange):
    log.info("🔄 Outcome checker started - checking ALL active signals")
    
    while True:
        try:
            outcomes_found = 0
            
            for key, signal_data in list(signal_tracker.active_signals.items()):
                if signal_data.get('status') != 'active':
                    continue
                
                symbol = key[0]
                setup = signal_data.get('setup', {})
                
                try:
                    ticker = await safe_fetch_ticker(exchange, symbol)
                    if not ticker:
                        continue
                    
                    current_price = ticker.get('last', 0)
                    if current_price == 0:
                        continue
                    
                    outcome = signal_tracker.check_signal_outcome(setup, current_price)
                    if outcome:
                        await send_outcome_alert(symbol, outcome)
                        await store_outcome(symbol, outcome)
                        outcomes_found += 1
                        
                        symbol_key, side_key, bucket_key = key
                        log.info(f"📊 Outcome: {symbol_key} {side_key} Bucket:{bucket_key} - {outcome.get('type', '')} | PnL: {outcome.get('pnl_pct', 0):+.2f}%")
                        
                except Exception as e:
                    log.debug(f"Error checking outcome for {symbol}: {e}")
                    continue
            
            if outcomes_found:
                log.info(f"📊 Found {outcomes_found} signal outcomes")
            
            signal_tracker.cleanup_old_signals()
            
            await asyncio.sleep(30)
            
        except Exception as e:
            log.error(f"Outcome checker error: {e}")
            await asyncio.sleep(60)

# ---------------- ENHANCED SCANNER MAIN ----------------
async def process_enhanced_results(results) -> int:
    alerts_sent = 0
    
    for result in results:
        if isinstance(result, Exception):
            log.error(f"Task error: {result}")
            continue
            
        if result:
            try:
                quality_score = result.get("quality", {}).get("total_score", 0)
                direction_conf = result.get("direction_engine", {}).get("confidence_tier", "LOW")
                
                adjusted_min_score = MIN_QUALITY_SCORE
                if direction_conf == "HIGH":
                    adjusted_min_score -= 0.2
                elif direction_conf == "LOW":
                    adjusted_min_score += 0.3
                
                if quality_score >= adjusted_min_score:
                    alerted = await send_deduped_enhanced_alert(result)
                    if alerted:
                        alerts_sent += 1
                    await store_signal(result)
            except Exception as e:
                log.error(f"Error processing result: {e}")
    
    return alerts_sent

async def enhanced_liquidity_scanner(exchange):
    """Main scanner with direction engine"""
    
    startup_msg = f"""🚀 <b>ROMEOTPT v5.0 Started - DIRECTION ENGINE ENABLED</b>
Scan: {SCAN_INTERVAL}s | Top {TOP_N} | Quality ≥{MIN_QUALITY_SCORE}
Direction Engine: Trapped + Bleeding + Micro Confirmation
ONE SIGNAL PER: (Symbol, Side, 0.5-Bucket)
Funding Threshold: {FUNDING_EXTREME_THRESHOLD}%
Direction Confidence: ≥{MIN_DIRECTION_CONFIDENCE}"""
    await send_telegram(startup_msg)
    
    asyncio.create_task(outcome_checker_task(exchange))
    
    scan_cycle = 0
    
    while True:
        scan_cycle += 1
        
        try:
            tickers = await safe_fetch_tickers(exchange)
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") and not symbol.startswith("USDT"):
                    volume = data.get("quoteVolume", 0)
                    if isinstance(volume, (int, float)) and volume > 100000:
                        usdt_pairs.append((symbol, float(volume)))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]
            
            stats = signal_tracker.get_stats()
            
            log.info(f"🔄 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | Active: {stats.get('active_signals', 0)}")
            
            if scan_cycle % 3 == 0 and stats.get('bucket_distribution'):
                bucket_stats = stats.get('bucket_distribution', {})
                if bucket_stats:
                    bucket_str = ", ".join([f"{bucket}:{count}" for bucket, count in sorted(bucket_stats.items())])
                    log.info(f"📊 0.5-Bucket Distribution: {bucket_str}")
            
            if scan_cycle % 5 == 0:
                outcome_stats = stats.get('outcome_stats', {})
                total_closed = outcome_stats.get('tp1_hits', 0) + outcome_stats.get('tp2_hits', 0) + outcome_stats.get('tp3_hits', 0) + outcome_stats.get('sl_hits', 0)
                if total_closed > 0:
                    win_rate = outcome_stats.get('win_rate', 0)
                    avg_pnl = outcome_stats.get('avg_pnl_pct', 0)
                    log.info(f"📈 Stats: WR={win_rate:.1f}% | Avg PnL={avg_pnl:+.2f}% | Active={outcome_stats.get('active', 0)}")
            
            alerts_this_scan = 0
            tasks = []
            
            batch_size = 1
            
            for i in range(0, len(symbols_to_scan), batch_size):
                batch = symbols_to_scan[i:i+batch_size]
                
                for symbol in batch:
                    task = asyncio.create_task(scan_symbol_with_direction(exchange, symbol))
                    tasks.append(task)
                
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    alerts_this_scan += await process_enhanced_results(results)
                    tasks = []
                
                if i + batch_size < len(symbols_to_scan):
                    await asyncio.sleep(0.5)
            
            signal_tracker.cleanup_old_signals()
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL * 2)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    stats = signal_tracker.get_stats()
    
    direction_stats = {
        'high_confidence': 0,
        'medium_confidence': 0,
        'low_confidence': 0
    }
    
    for key, data in signal_tracker.active_signals.items():
        if data.get('status') == 'active':
            setup = data.get('setup', {})
            direction = setup.get('direction_engine', {})
            confidence = direction.get('confidence_tier', 'LOW')
            
            if confidence == 'HIGH':
                direction_stats['high_confidence'] += 1
            elif confidence == 'MEDIUM':
                direction_stats['medium_confidence'] += 1
            else:
                direction_stats['low_confidence'] += 1
    
    return {
        "status": "healthy", 
        "version": "5.0 - DIRECTION ENGINE",
        "active_signals": stats.get('active_signals', 0),
        "signals_by_side": stats.get('signals_by_side', {}),
        "direction_stats": direction_stats,
        "bucket_distribution": stats.get('bucket_distribution', {}),
        "outcome_stats": stats.get('outcome_stats', {})
    }

@app.get("/signals/active")
async def get_active_signals():
    active = []
    for key, data in signal_tracker.active_signals.items():
        if data.get('status') == 'active':
            symbol, side, bucket = key
            setup = data.get('setup', {})
            quality = setup.get('quality', {})
            eight_steps = quality.get('eight_steps', {})
            direction = setup.get('direction_engine', {})
            
            step_passes = sum([
                eight_steps.get('step_1_htf_bias', False),
                eight_steps.get('step_2_zone_type', False),
                eight_steps.get('step_3_liquidity_sweep', False),
                eight_steps.get('step_4_structure_shift', False),
                eight_steps.get('step_5_from_liquidity', False),
                eight_steps.get('step_6_confirmation_candle', False),
                eight_steps.get('step_7_entry_validity', False),
                eight_steps.get('step_8_liquidity_alignment', False)
            ])
            
            tp_sources = setup.get('tp_sources', [])
            tp_source_info = []
            for source in tp_sources:
                tp_source_info.append({
                    'tp_level': source.get('tp_level', 0),
                    'type': source.get('type', 'unknown'),
                    'timeframe': source.get('timeframe', 'N/A'),
                    'reason': source.get('reason', 'unknown')
                })
            
            active.append({
                "symbol": symbol,
                "side": side,
                "bucket": bucket,
                "actual_score": quality.get('total_score', 0),
                "entry_price": setup.get('entry_price', 0),
                "current_price": setup.get('current_price', 0),
                "sl": setup.get('sl_price', 0),
                "tp1": setup.get('tp_targets', [0])[0] if len(setup.get('tp_targets', [])) > 0 else 0,
                "tp2": setup.get('tp_targets', [0, 0])[1] if len(setup.get('tp_targets', [])) > 1 else 0,
                "tp_sources": tp_source_info,
                "quality_score": quality.get('total_score', 0),
                "quality_tier": quality.get('tier', 'C'),
                "steps_passed": step_passes,
                "direction_confidence": direction.get('confidence_tier', 'LOW'),
                "direction_score": direction.get('direction_score', 0),
                "trapped_side": direction.get('trapped_side', 'NONE'),
                "trapped_confidence": direction.get('trapped_confidence', 0),
                "bleeding_side": direction.get('bleeding_side', ''),
                "micro_confirmation": direction.get('micro_confirmation', False),
                "rr_ratio": setup.get('rr_ratio', 0),
                "age_minutes": (datetime.datetime.utcnow() - data.get('first_seen', datetime.datetime.utcnow())).total_seconds() / 60
            })
    return {"active_signals": active, "count": len(active)}

@app.get("/outcomes/stats")
async def get_outcome_stats(hours: int = 24):
    async with db_lock:
        try:
            cursor = await db_conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN outcome_type LIKE 'TP%' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN outcome_type = 'SL_HIT' THEN 1 ELSE 0 END) as losses,
                    AVG(pnl_pct) as avg_pnl,
                    AVG(hold_time_minutes) as avg_hold_time,
                    AVG(direction_score) as avg_direction_score,
                    AVG(trapped_confidence) as avg_trapped_confidence,
                    SUM(CASE WHEN direction_confidence_tier = 'HIGH' THEN 1 ELSE 0 END) as high_confidence_count,
                    SUM(CASE WHEN direction_confidence_tier = 'MEDIUM' THEN 1 ELSE 0 END) as medium_confidence_count,
                    SUM(CASE WHEN direction_confidence_tier = 'LOW' THEN 1 ELSE 0 END) as low_confidence_count
                FROM signal_outcomes_v5_0 
                WHERE status = 'closed' 
                AND closed_at > datetime('now', ?)
            """, (f"-{hours} hours",))
            row = await cursor.fetchone()
            
            cursor = await db_conn.execute("""
                SELECT 
                    direction_confidence_tier,
                    COUNT(*) as count,
                    SUM(CASE WHEN outcome LIKE 'TP%' THEN 1 ELSE 0 END) as wins,
                    AVG(pnl_pct) as avg_pnl,
                    AVG(rr_ratio) as avg_rr,
                    AVG(eight_steps_passed) as avg_steps_passed,
                    AVG(trapped_confidence) as avg_trapped_conf
                FROM signals_v5_0 
                WHERE status = 'closed' 
                AND timestamp > datetime('now', ?)
                AND direction_confidence_tier IS NOT NULL
                GROUP BY direction_confidence_tier
                ORDER BY direction_confidence_tier DESC
            """, (f"-{hours} hours",))
            rows = await cursor.fetchall()
            tier_stats = {}
            for row in rows:
                if row[0]:
                    tier_stats[row[0]] = {
                        'count': row[1],
                        'wins': row[2],
                        'avg_pnl': row[3],
                        'avg_rr': row[4],
                        'avg_steps_passed': row[5],
                        'avg_trapped_confidence': row[6]
                    }
        except Exception as e:
            log.error(f"Error fetching outcome stats: {e}")
            return {"error": str(e)}
    
    total = row[0] if row else 0
    wins = row[1] if row else 0
    
    return {
        'period_hours': hours,
        'total_signals': total,
        'wins': wins,
        'losses': row[2] if row else 0,
        'win_rate': wins / total * 100 if total > 0 else 0,
        'avg_pnl_pct': row[3] if row else 0,
        'avg_hold_minutes': row[4] if row else 0,
        'direction_stats': {
            'avg_direction_score': row[5] if row else 0,
            'avg_trapped_confidence': row[6] if row else 0,
            'high_confidence': row[7] if row else 0,
            'medium_confidence': row[8] if row else 0,
            'low_confidence': row[9] if row else 0
        },
        'by_direction_tier': tier_stats,
        'memory_stats': signal_tracker.outcome_stats
    }

# ---------------- MAIN ----------------
async def main():
    global db_conn
    
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await init_database()
        
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                "fetchOpenInterest": True,
                "fetchFundingRateHistory": True
            },
            "rateLimit": 500,
            "timeout": 30000,
            "verbose": False,
        })
        
        log.info("🚀 ROMEOTPT v5.0 - DIRECTION ENGINE ENABLED")
        log.info(f"5-Layer Analysis: Liquidity → Trapped → Bleeding → Micro → Decision")
        log.info(f"Funding Threshold: {FUNDING_EXTREME_THRESHOLD}%")
        log.info(f"OI Threshold: {OI_ACCUMULATION_THRESHOLD}%")
        log.info(f"Min Direction Confidence: {MIN_DIRECTION_CONFIDENCE}")
        log.info(f"Scan: {SCAN_INTERVAL}s | Top {TOP_N} symbols")
        
        await enhanced_liquidity_scanner(exchange)
        
    except Exception as e:
        log.error(f"Fatal error: {e}")
        import traceback
        log.error(f"Traceback: {traceback.format_exc()}")
    finally:
        if db_conn:
            await db_conn.close()
        log.info("Scanner shutdown complete")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")