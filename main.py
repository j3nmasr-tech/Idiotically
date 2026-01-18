#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INSTITUTIONAL CRYPTO REGIME SCANNER v1.0
World's Best Crypto Trader, Quant Strategist & Hedge-Fund Market Maker Framework
"""

import os
import time
import asyncio
import logging
import hashlib
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Set
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
import aiosqlite
import ccxt.async_support as ccxt
import httpx

# ================ CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/regime_scanner.db"

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 30))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 50))
MIN_VOLUME_USD = 500000

# Exchange configuration
EXCHANGE_CONFIG = {
    "okx": {
        "class": ccxt.okx,
        "params": {
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 30000
        }
    },
    "bybit": {
        "class": ccxt.bybit,
        "params": {
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 20000
        }
    }
}

# Timeframes for analysis
TIMEFRAMES = {
    "WEEKLY": "1w",
    "DAILY": "1d",
    "4H": "4h",
    "1H": "1h",
    "15M": "15m"
}

# Major coins to analyze
MAJOR_COINS = [
    "ETH/USDT",
    "SOL/USDT",
    "BNB/USDT", 
    "XRP/USDT",
    "ADA/USDT",
    "AVAX/USDT",
    "DOT/USDT",
    "MATIC/USDT",
    "LINK/USDT",
    "DOGE/USDT",
    "TRX/USDT",
    "LTC/USDT",
    "ATOM/USDT"
]

# ================ DATA STRUCTURES ================
@dataclass
class BTCStructure:
    """Bitcoin structure analysis"""
    htf_high: float
    htf_low: float
    equal_highs: List[float]
    equal_lows: List[float]
    prior_weekly_high: float
    prior_weekly_low: float
    prior_daily_high: float
    prior_daily_low: float
    acceptance_score: float  # 0-1, higher = more acceptance
    rejection_score: float   # 0-1, higher = more rejection
    time_above_value: float  # % time above value area
    time_below_value: float  # % time below value area

@dataclass
class BTCDerivatives:
    """Bitcoin derivatives analysis"""
    open_interest_trend: str  # rising, falling, flat
    funding_bias: str  # positive, negative, neutral
    funding_persistence: int  # consecutive hours with same bias
    price_oi_divergence: bool  # True if price and OI diverging
    estimated_liquidations: Dict[str, float]  # long/short liq levels

@dataclass 
class BTCRole:
    """Bitcoin's market role"""
    primary_role: str  # Expansion leader, Range controller, etc.
    role_score: float  # 0-1 confidence
    secondary_roles: List[str]
    evidence: List[str]  # why this role

@dataclass
class AltcoinRelativeAnalysis:
    """Altcoin analysis relative to BTC"""
    symbol: str
    relative_performance: float  # % vs BTC
    relative_trend: str  # stronger, weaker, neutral
    move_speed: str  # fast_leverage, slow_strong
    accumulation_score: float  # 0-1
    distribution_score: float  # 0-1
    beta_coefficient: float  # responsiveness to BTC moves
    correlation_24h: float  # correlation with BTC last 24h

@dataclass
class LiquidityZone:
    """Liquidity zone identification"""
    price: float
    zone_type: str  # equal_high, equal_low, prior_high, prior_low, stop_cluster
    strength: float  # 0-1
    market_coverage: int  # how many markets share this level
    estimated_stops: float  # estimated $ value of stops
    recent_test: bool  # tested recently
    distance_pct: float  # distance from current price

@dataclass
class CrossMarketLiquidity:
    """Cross-market liquidity analysis"""
    shared_htf_levels: List[LiquidityZone]
    correlated_stop_clusters: List[Dict]
    trigger_market: str  # which market triggers first
    liquidation_cascades: List[Dict]
    max_liquidation_zone: LiquidityZone  # where most money gets liquidated next
    estimated_cascade_value: float  # estimated $ value of cascades

@dataclass
class MarketMakerIncentives:
    """Market maker incentive analysis"""
    trapped_traders: Dict[str, List]  # longs_trapped, shorts_trapped
    over_leveraged_side: str  # longs, shorts, balanced
    price_movement_purpose: str  # kill_leverage, build_leverage, rotate_capital
    optimal_direction: str  # continuation, reversal
    mm_payoff: Dict[str, float]  # continuation_payoff, reversal_payoff
    estimated_mm_inventory: Dict[str, float]  # long_inventory, short_inventory

@dataclass
class TradeDecision:
    """Final trade decision"""
    decision: str  # 🟢 Long, 🔴 Short, ⚫ No Trade
    symbol: Optional[str]
    side: Optional[str]
    entry_price: Optional[float]
    entry_type: str  # liquidity_sweep, stop_hunt, breakout, etc.
    missing_liquidity: Optional[str]  # if No Trade, what's missing

@dataclass
class PositionManagement:
    """Liquidity-based position management"""
    stop_loss: Dict[str, Any]
    take_profit: Dict[str, Any]
    invalidator: Dict[str, Any]
    time_stop: Dict[str, Any]

@dataclass
class RegimeAnalysis:
    """Complete regime analysis output"""
    timestamp: float
    global_regime: str
    regime_confidence: float
    btc_structure: BTCStructure
    btc_derivatives: BTCDerivatives
    btc_role: BTCRole
    altcoin_rankings: Dict[str, List[AltcoinRelativeAnalysis]]
    cross_market_liquidity: CrossMarketLiquidity
    market_maker_incentives: MarketMakerIncentives
    trade_decision: TradeDecision
    position_management: Optional[PositionManagement]
    next_move_prediction: str
    confidence_score: float
    analysis_id: str

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("RegimeScanner")

# ================ CORE REGIME SCANNER ================
class InstitutionalRegimeScanner:
    """
    World's Best Crypto Trader, Quant Strategist & Hedge-Fund Market Maker
    Thinks in: Liquidity, Leverage, Time, and Incentives
    Never trades narratives — trades who is trapped, where liquidity sits, 
    and when leverage must be reset.
    """
    
    def __init__(self, exchange_name: str = "okx"):
        self.exchange = None
        self.exchange_name = exchange_name
        self.db = None
        self.data_cache = {}
        self.cache_ttl = 300  # 5 minutes
        
        # Analysis state
        self.current_regime = None
        self.btc_analysis = None
        self.alt_rankings = None
        self.liquidity_map = None
        self.mm_incentives = None
        
        # Performance tracking
        self.scan_count = 0
        self.analysis_history = deque(maxlen=100)
        
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🏛️ INSTITUTIONAL REGIME SCANNER v1.0")
        log.info("World's Best Crypto Trader, Quant Strategist & Hedge-Fund Market Maker")
        log.info("=" * 70)
        log.info("CORE PRINCIPLES:")
        log.info("• Bitcoin as core liquidity engine")
        log.info("• Major coins as beta expressions") 
        log.info("• Think in liquidity, leverage, time, and incentives")
        log.info("• Trade who is trapped, where liquidity sits, when leverage resets")
        log.info("=" * 70)
        
        await self._init_exchange()
        await self._init_database()
        await self._send_startup_message()
        
    async def _init_exchange(self):
        """Initialize exchange connection"""
        try:
            config = EXCHANGE_CONFIG[self.exchange_name]
            self.exchange = config["class"](config["params"])
            
            # Test connection
            markets = await self.exchange.fetch_markets(params={'type': 'spot'})
            usdt_pairs = [m['symbol'] for m in markets if m['symbol'].endswith('/USDT')]
            
            log.info(f"✅ {self.exchange_name.upper()} connected: {len(usdt_pairs)} USDT pairs")
            
        except Exception as e:
            log.error(f"Exchange initialization failed: {e}")
            raise
    
    async def _init_database(self):
        """Initialize regime analysis database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Create regime analysis table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS regime_analysis (
                analysis_id TEXT PRIMARY KEY,
                timestamp REAL NOT NULL,
                global_regime TEXT NOT NULL,
                regime_confidence REAL NOT NULL,
                btc_role TEXT NOT NULL,
                
                altcoin_leaders TEXT,
                altcoin_weak TEXT,
                
                trade_decision TEXT NOT NULL,
                trade_symbol TEXT,
                trade_side TEXT,
                
                next_move_prediction TEXT,
                confidence_score REAL NOT NULL,
                
                btc_structure TEXT,
                btc_derivatives TEXT,
                cross_market_liquidity TEXT,
                market_maker_incentives TEXT,
                
                stop_loss_logic TEXT,
                take_profit_logic TEXT,
                
                raw_analysis TEXT
            )
            """)
            
            # Create performance tracking
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS regime_performance (
                date DATE PRIMARY KEY,
                total_scans INTEGER,
                risk_on_expansion INTEGER,
                accumulation INTEGER,
                distribution INTEGER,
                range_chop INTEGER,
                volatility_compression INTEGER,
                
                long_decisions INTEGER,
                short_decisions INTEGER,
                no_trade_decisions INTEGER,
                
                avg_confidence REAL,
                regime_persistence_hours REAL
            )
            """)
            
            await self.db.commit()
            log.info("✅ Regime database initialized")
            
        except Exception as e:
            log.error(f"Database initialization failed: {e}")
            raise
    
    async def _send_startup_message(self):
        """Send startup message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set")
            return
        
        try:
            message = """🏛️ <b>INSTITUTIONAL REGIME SCANNER v1.0 - ONLINE</b>

<b>₿ CORE PHILOSOPHY:</b>
• Bitcoin as liquidity engine
• Majors as beta expressions  
• Think: Liquidity, Leverage, Time, Incentives
• Trade: Who is trapped, where liquidity sits, when leverage resets

<b>🎯 FRAMEWORK:</b>
1. Global Regime Detection
2. Bitcoin Role Analysis
3. Altcoin Beta Alignment
4. Cross-Market Liquidity Map
5. Market Maker Incentives
6. Liquidity-Based Trade Decision
7. Institutional Position Management

<b>⚡ SCANNING:</b> All major coins vs BTC
<b>📊 EXCHANGE:</b> OKX
<b>🎯 FOCUS:</b> Institutional liquidity flows

#RegimeTrading #LiquidityFirst #Institutional #BTCAlpha #Ready"""
            
            await self._send_telegram(message)
            log.info("✅ Startup message sent")
            
        except Exception as e:
            log.error(f"Telegram startup error: {e}")
    
    # ========== STEP 1: GLOBAL REGIME DETECTION ==========
    async def determine_global_regime(self) -> Tuple[str, float]:
        """
        STEP 1 — GLOBAL REGIME (MANDATORY)
        Classify overall market into ONE:
        • 🟢 Risk-On Expansion
        • 🟡 Controlled Range / Chop  
        • 🔴 Distribution (top process)
        • 🔵 Accumulation (post-flush)
        • ⚫ Volatility Compression (pre-impulse)
        
        No trade decisions before regime is defined.
        """
        
        log.info("🔍 STEP 1: Determining Global Regime...")
        
        # Fetch BTC data for regime analysis
        btc_data = await self._fetch_multi_timeframe_btc()
        if not btc_data:
            return "⚫ Volatility Compression (pre-impulse)", 0.0
        
        # Calculate regime scores
        regime_scores = {
            "🟢 Risk-On Expansion": await self._score_risk_on_expansion(btc_data),
            "🟡 Controlled Range / Chop": await self._score_controlled_range(btc_data),
            "🔴 Distribution (top process)": await self._score_distribution(btc_data),
            "🔵 Accumulation (post-flush)": await self._score_accumulation(btc_data),
            "⚫ Volatility Compression (pre-impulse)": await self._score_vol_compression(btc_data)
        }
        
        # Normalize scores
        total_score = sum(regime_scores.values())
        if total_score > 0:
            regime_scores = {k: v/total_score for k, v in regime_scores.items()}
        
        # Select primary regime
        primary_regime = max(regime_scores.items(), key=lambda x: x[1])
        confidence = primary_regime[1]
        
        log.info(f"📊 REGIME: {primary_regime[0]} (Confidence: {confidence:.0%})")
        for regime, score in sorted(regime_scores.items(), key=lambda x: x[1], reverse=True):
            if score > 0.1:
                log.debug(f"  {regime}: {score:.0%}")
        
        return primary_regime[0], confidence
    
    async def _score_risk_on_expansion(self, btc_data: Dict) -> float:
        """Score Risk-On Expansion regime"""
        score = 0.0
        
        # Need daily and 4H data
        if "DAILY" not in btc_data or "4H" not in btc_data:
            return score
        
        df_daily = btc_data["DAILY"]
        df_4h = btc_data["4H"]
        
        if len(df_daily) < 20 or len(df_4h) < 40:
            return score
        
        # 1. Higher highs and higher lows on daily
        if len(df_daily) >= 10:
            highs = df_daily['high'].iloc[-10:].values
            lows = df_daily['low'].iloc[-10:].values
            
            # Check for ascending structure
            if (all(highs[i] > highs[i-1] for i in range(1, len(highs))) and
                all(lows[i] > lows[i-1] for i in range(1, len(lows)))):
                score += 0.4
        
        # 2. Strong acceptance above value area
        current_price = df_daily['close'].iloc[-1]
        value_area = self._calculate_value_area(df_daily)
        
        if current_price > value_area["vah"] * 1.02:  # 2% above value area high
            score += 0.3
        
        # 3. Expanding volume with price
        if len(df_daily) >= 20:
            recent_volume = df_daily['volume'].iloc[-5:].mean()
            prior_volume = df_daily['volume'].iloc[-20:-5].mean()
            
            if recent_volume > prior_volume * 1.3:  # 30% volume increase
                score += 0.2
        
        # 4. Strong momentum continuation
        if len(df_4h) >= 10:
            momentum = self._calculate_momentum(df_4h)
            if momentum > 0.7:
                score += 0.1
        
        return min(score, 1.0)
    
    async def _score_controlled_range(self, btc_data: Dict) -> float:
        """Score Controlled Range / Chop regime"""
        score = 0.0
        
        if "DAILY" not in btc_data or "4H" not in btc_data:
            return score
        
        df_daily = btc_data["DAILY"]
        df_4h = btc_data["4H"]
        
        if len(df_daily) < 20 or len(df_4h) < 40:
            return score
        
        # 1. Price oscillating in defined range
        recent_range = df_daily['high'].iloc[-20:].max() - df_daily['low'].iloc[-20:].min()
        avg_daily_range = (df_daily['high'] - df_daily['low']).mean()
        
        if recent_range < avg_daily_range * 0.7:  # Range contraction
            score += 0.3
        
        # 2. Clear support and resistance levels
        clear_levels = self._identify_clear_levels(df_daily)
        if len(clear_levels) >= 2:  # At least clear support and resistance
            score += 0.3
        
        # 3. Mean reversion behavior
        mean_reversion_score = self._calculate_mean_reversion(df_4h)
        score += mean_reversion_score * 0.3
        
        # 4. Declining volatility
        volatility = df_daily['close'].pct_change().std()
        if volatility < 0.02:  # Low volatility
            score += 0.1
        
        return min(score, 1.0)
    
    async def _score_distribution(self, btc_data: Dict) -> float:
        """Score Distribution (top process) regime"""
        score = 0.0
        
        if "DAILY" not in btc_data or "4H" not in btc_data:
            return score
        
        df_daily = btc_data["DAILY"]
        df_4h = btc_data["4H"]
        
        if len(df_daily) < 20 or len(df_4h) < 40:
            return score
        
        # 1. Price at resistance with weak momentum
        recent_high = df_daily['high'].iloc[-20:].max()
        current_price = df_daily['close'].iloc[-1]
        
        if abs(current_price - recent_high) / recent_high < 0.01:  # Within 1% of high
            score += 0.3
        
        # 2. Volume divergence (price high, volume decreasing)
        if len(df_daily) >= 10:
            price_change = (df_daily['close'].iloc[-1] - df_daily['close'].iloc[-10]) / df_daily['close'].iloc[-10]
            volume_change = df_daily['volume'].iloc[-5:].mean() / df_daily['volume'].iloc[-10:-5].mean()
            
            if price_change > 0.05 and volume_change < 0.8:  # Price up 5%, volume down 20%
                score += 0.4
        
        # 3. Failed breakout attempts on lower timeframes
        if len(df_4h) >= 20:
            failed_breakouts = self._count_failed_breakouts(df_4h)
            if failed_breakouts >= 3:
                score += 0.2
        
        # 4. Bearish order flow
        order_flow = self._analyze_order_flow(df_4h)
        if order_flow.get("bearish_dominance", False):
            score += 0.1
        
        return min(score, 1.0)
    
    async def _score_accumulation(self, btc_data: Dict) -> float:
        """Score Accumulation (post-flush) regime"""
        score = 0.0
        
        if "DAILY" not in btc_data or "4H" not in btc_data:
            return score
        
        df_daily = btc_data["DAILY"]
        df_4h = btc_data["4H"]
        
        if len(df_daily) < 20 or len(df_4h) < 40:
            return score
        
        # 1. Recent flush followed by basing
        flush_score = self._detect_post_flush_basing(df_daily)
        score += flush_score * 0.4
        
        # 2. Volume profile shows absorption
        volume_profile = self._analyze_volume_profile(df_daily)
        if volume_profile.get("absorption", False):
            score += 0.3
        
        # 3. Decreasing volatility after flush
        volatility_trend = self._analyze_volatility_trend(df_daily)
        if volatility_trend.get("decreasing", False):
            score += 0.2
        
        # 4. Support holding with increasing bids
        support_strength = self._assess_support_strength(df_4h)
        if support_strength > 0.7:
            score += 0.1
        
        return min(score, 1.0)
    
    async def _score_vol_compression(self, btc_data: Dict) -> float:
        """Score Volatility Compression (pre-impulse) regime"""
        score = 0.0
        
        if "DAILY" not in btc_data or "4H" not in btc_data:
            return score
        
        df_daily = btc_data["DAILY"]
        df_4h = btc_data["4H"]
        
        if len(df_daily) < 20 or len(df_4h) < 40:
            return score
        
        # 1. Extreme volatility compression
        recent_atr = self._calculate_atr(df_daily.iloc[-20:])
        historical_atr = self._calculate_atr(df_daily.iloc[-100:-20])
        
        if recent_atr < historical_atr * 0.5:  # 50% compression
            score += 0.5
        
        # 2. Symmetrical triangle/coiling pattern
        pattern_score = self._detect_compression_pattern(df_4h)
        score += pattern_score * 0.3
        
        # 3. Declining volume
        if len(df_daily) >= 20:
            recent_volume = df_daily['volume'].iloc[-5:].mean()
            prior_volume = df_daily['volume'].iloc[-20:-5].mean()
            
            if recent_volume < prior_volume * 0.7:  # 30% volume decline
                score += 0.2
        
        return min(score, 1.0)
    
    # ========== STEP 2: BITCOIN ANALYSIS ==========
    async def analyze_bitcoin_structure(self) -> BTCStructure:
        """
        STEP 2 — BITCOIN ANALYSIS (ENGINE)
        Analyze BTC first using:
        Structure: HTF highs/lows, Acceptance vs rejection, Time spent above/below value
        Liquidity: Equal highs/lows, Untaken stops, Prior daily/weekly highs & lows
        """
        
        log.info("🔍 STEP 2: Analyzing Bitcoin Structure...")
        
        # Fetch BTC data
        btc_data = await self._fetch_multi_timeframe_btc()
        if not btc_data or "DAILY" not in btc_data or "4H" not in btc_data:
            return self._get_default_btc_structure()
        
        df_daily = btc_data["DAILY"]
        df_4h = btc_data["4H"]
        
        if len(df_daily) < 20 or len(df_4h) < 40:
            return self._get_default_btc_structure()
        
        # Calculate structure metrics
        htf_highs_lows = self._calculate_htf_highs_lows(df_daily)
        acceptance_rejection = self._calculate_acceptance_rejection(df_4h)
        time_in_value = self._calculate_time_in_value_area(df_daily)
        
        # Find liquidity levels
        equal_highs = self._find_equal_highs(df_4h)
        equal_lows = self._find_equal_lows(df_4h)
        prior_levels = self._find_prior_key_levels(df_daily)
        
        # Estimate untaken stops
        untaken_stops = self._estimate_untaken_stops(df_4h)
        
        structure = BTCStructure(
            htf_high=htf_highs_lows["htf_high"],
            htf_low=htf_highs_lows["htf_low"],
            equal_highs=equal_highs,
            equal_lows=equal_lows,
            prior_weekly_high=prior_levels["weekly_high"],
            prior_weekly_low=prior_levels["weekly_low"],
            prior_daily_high=prior_levels["daily_high"],
            prior_daily_low=prior_levels["daily_low"],
            acceptance_score=acceptance_rejection["acceptance"],
            rejection_score=acceptance_rejection["rejection"],
            time_above_value=time_in_value["above"],
            time_below_value=time_in_value["below"]
        )
        
        log.info(f"₿ BTC Structure:")
        log.info(f"  HTF Range: {structure.htf_low:.2f} - {structure.htf_high:.2f}")
        log.info(f"  Equal Highs: {len(structure.equal_highs)}, Equal Lows: {len(structure.equal_lows)}")
        log.info(f"  Acceptance: {structure.acceptance_score:.0%}, Rejection: {structure.rejection_score:.0%}")
        log.info(f"  Time Above Value: {structure.time_above_value:.0%}, Below: {structure.time_below_value:.0%}")
        
        return structure
    
    async def analyze_bitcoin_derivatives(self) -> BTCDerivatives:
        """
        Analyze BTC derivatives:
        • Open Interest trend
        • Funding bias & persistence  
        • Price vs OI divergence
        """
        
        log.info("🔍 STEP 2b: Analyzing Bitcoin Derivatives...")
        
        try:
            # Fetch funding rate (simplified - in production use exchange-specific endpoints)
            tickers = await self.exchange.fetch_tickers(['BTC/USDT:USDT', 'BTC/USDT'])
            
            funding_rate = 0.0
            funding_bias = "neutral"
            
            if 'BTC/USDT:USDT' in tickers:
                ticker = tickers['BTC/USDT:USDT']
                funding_rate = float(ticker.get('info', {}).get('fundingRate', 0))
                funding_bias = "positive" if funding_rate > 0 else "negative" if funding_rate < 0 else "neutral"
            
            # Estimate OI trend (in production, fetch actual OI data)
            oi_trend = await self._estimate_oi_trend()
            
            # Check for price-OI divergence
            price_oi_divergence = await self._detect_price_oi_divergence()
            
            # Estimate liquidation levels
            liquidations = await self._estimate_liquidation_levels()
            
            derivatives = BTCDerivatives(
                open_interest_trend=oi_trend,
                funding_bias=funding_bias,
                funding_persistence=1,  # Would track over time
                price_oi_divergence=price_oi_divergence,
                estimated_liquidations=liquidations
            )
            
            log.info(f"₿ BTC Derivatives:")
            log.info(f"  OI Trend: {derivatives.open_interest_trend}")
            log.info(f"  Funding Bias: {derivatives.funding_bias} ({funding_rate:.6%})")
            log.info(f"  Price-OI Divergence: {derivatives.price_oi_divergence}")
            
            return derivatives
            
        except Exception as e:
            log.error(f"BTC derivatives analysis error: {e}")
            return self._get_default_btc_derivatives()
    
    async def determine_bitcoin_role(self, structure: BTCStructure, 
                                   derivatives: BTCDerivatives) -> BTCRole:
        """
        Define BTC as ONE:
        • Expansion leader
        • Range controller  
        • Distribution anchor
        • Accumulation base
        • Liquidity sweep instrument
        """
        
        # Score each potential role
        role_scores = {
            "Expansion leader": self._score_expansion_leader(structure, derivatives),
            "Range controller": self._score_range_controller(structure, derivatives),
            "Distribution anchor": self._score_distribution_anchor(structure, derivatives),
            "Accumulation base": self._score_accumulation_base(structure, derivatives),
            "Liquidity sweep instrument": self._score_liquidity_sweeper(structure, derivatives)
        }
        
        # Select primary role
        primary_role = max(role_scores.items(), key=lambda x: x[1])
        
        # Get secondary roles (scores > 0.3)
        secondary_roles = [
            role for role, score in role_scores.items() 
            if score > 0.3 and role != primary_role[0]
        ]
        
        # Build evidence list
        evidence = self._build_role_evidence(primary_role[0], structure, derivatives)
        
        role = BTCRole(
            primary_role=primary_role[0],
            role_score=primary_role[1],
            secondary_roles=secondary_roles,
            evidence=evidence
        )
        
        log.info(f"₿ BTC Role: {role.primary_role} (Score: {role.role_score:.0%})")
        if secondary_roles:
            log.info(f"  Secondary: {', '.join(secondary_roles)}")
        
        return role
    
    def _score_expansion_leader(self, structure: BTCStructure, 
                               derivatives: BTCDerivatives) -> float:
        """Score Expansion Leader role"""
        score = 0.0
        
        # Strong acceptance above value
        if structure.acceptance_score > 0.7:
            score += 0.3
        
        # Time mostly above value area
        if structure.time_above_value > 0.7:
            score += 0.2
        
        # Positive funding (traders paying to be long)
        if derivatives.funding_bias == "positive":
            score += 0.2
        
        # No equal highs resistance nearby
        if not structure.equal_highs:
            score += 0.1
        
        # OI rising with price
        if derivatives.open_interest_trend == "rising":
            score += 0.2
        
        return min(score, 1.0)
    
    def _score_range_controller(self, structure: BTCStructure,
                               derivatives: BTCDerivatives) -> float:
        """Score Range Controller role"""
        score = 0.0
        
        # Balanced acceptance/rejection
        if 0.3 < structure.acceptance_score < 0.7:
            score += 0.3
        
        # Clear equal highs and lows
        if structure.equal_highs and structure.equal_lows:
            score += 0.3
        
        # Time balanced between above/below value
        if 0.3 < structure.time_above_value < 0.7:
            score += 0.2
        
        # Neutral funding
        if derivatives.funding_bias == "neutral":
            score += 0.2
        
        return min(score, 1.0)
    
    def _score_distribution_anchor(self, structure: BTCStructure,
                                  derivatives: BTCDerivatives) -> float:
        """Score Distribution Anchor role"""
        score = 0.0
        
        # Strong rejection at highs
        if structure.rejection_score > 0.7:
            score += 0.3
        
        # Multiple equal highs resistance
        if len(structure.equal_highs) >= 2:
            score += 0.3
        
        # Price near HTF high
        current_price = self._get_current_btc_price()
        if current_price and abs(current_price - structure.htf_high) / structure.htf_high < 0.02:
            score += 0.2
        
        # Negative funding (traders paying to be short)
        if derivatives.funding_bias == "negative":
            score += 0.2
        
        return min(score, 1.0)
    
    def _score_accumulation_base(self, structure: BTCStructure,
                                derivatives: BTCDerivatives) -> float:
        """Score Accumulation Base role"""
        score = 0.0
        
        # Strong support at lows
        if len(structure.equal_lows) >= 2:
            score += 0.3
        
        # Time mostly below value area
        if structure.time_below_value > 0.7:
            score += 0.2
        
        # Price near HTF low
        current_price = self._get_current_btc_price()
        if current_price and abs(current_price - structure.htf_low) / structure.htf_low < 0.02:
            score += 0.2
        
        # Positive funding in downtrend (contrarian)
        if derivatives.funding_bias == "positive":
            score += 0.2
        
        # OI building
        if derivatives.open_interest_trend == "rising":
            score += 0.1
        
        return min(score, 1.0)
    
    def _score_liquidity_sweeper(self, structure: BTCStructure,
                                derivatives: BTCDerivatives) -> float:
        """Score Liquidity Sweep Instrument role"""
        score = 0.0
        
        # Equal highs/lows being taken
        if structure.equal_highs or structure.equal_lows:
            score += 0.3
        
        # High estimated liquidations nearby
        if derivatives.estimated_liquidations:
            total_liq = sum(derivatives.estimated_liquidations.values())
            if total_liq > 100000000:  # $100M+ estimated liq
                score += 0.3
        
        # Price-OI divergence (price moving against OI)
        if derivatives.price_oi_divergence:
            score += 0.2
        
        # Extreme funding (positive or negative)
        if derivatives.funding_bias in ["strong_positive", "strong_negative"]:
            score += 0.2
        
        return min(score, 1.0)
    
    # ========== STEP 3: MAJOR COINS ANALYSIS ==========
    async def analyze_majors_alignment(self) -> Dict[str, List[AltcoinRelativeAnalysis]]:
        """
        STEP 3 — MAJOR COINS (SATELLITES)
        For EACH major: Evaluate RELATIVE TO BTC, never alone
        • Stronger than BTC = accumulation
        • Weaker than BTC = distribution  
        • Faster moves = leverage-heavy
        • Slower moves = strong hands
        
        Create ranking:
        • Leaders
        • Neutral  
        • Weak / vulnerable
        """
        
        log.info("🔍 STEP 3: Analyzing Major Coins vs BTC...")
        
        rankings = {
            "Leaders": [],      # Stronger than BTC = accumulation
            "Neutral": [],      # Moving with BTC
            "Weak/Vulnerable": []  # Weaker than BTC = distribution
        }
        
        # Get BTC performance
        btc_performance = await self._calculate_btc_performance()
        
        for symbol in MAJOR_COINS:
            try:
                # Fetch altcoin data
                alt_data = await self._fetch_altcoin_data(symbol)
                if alt_data is None:
                    continue
                
                # Calculate relative performance
                relative_perf = await self._calculate_relative_performance(symbol, btc_performance)
                
                # Determine trend relative to BTC
                if relative_perf > 0.02:  # 2% stronger
                    relative_trend = "stronger"
                elif relative_perf < -0.02:  # 2% weaker
                    relative_trend = "weaker"
                else:
                    relative_trend = "neutral"
                
                # Analyze move speed
                move_speed = await self._analyze_move_speed(symbol)
                
                # Calculate accumulation/distribution scores
                acc_score = await self._calculate_accumulation_score(symbol, relative_perf)
                dist_score = await self._calculate_distribution_score(symbol, relative_perf)
                
                # Calculate beta coefficient (responsiveness to BTC)
                beta = await self._calculate_beta_coefficient(symbol)
                
                # Calculate 24h correlation
                correlation = await self._calculate_correlation_24h(symbol)
                
                analysis = AltcoinRelativeAnalysis(
                    symbol=symbol,
                    relative_performance=relative_perf,
                    relative_trend=relative_trend,
                    move_speed=move_speed,
                    accumulation_score=acc_score,
                    distribution_score=dist_score,
                    beta_coefficient=beta,
                    correlation_24h=correlation
                )
                
                # Classify into rankings
                if relative_trend == "stronger" and acc_score > 0.6:
                    rankings["Leaders"].append(analysis)
                elif relative_trend == "weaker" and dist_score > 0.6:
                    rankings["Weak/Vulnerable"].append(analysis)
                else:
                    rankings["Neutral"].append(analysis)
                    
            except Exception as e:
                log.debug(f"Error analyzing {symbol}: {e}")
                continue
        
        # Sort each ranking
        rankings["Leaders"].sort(key=lambda x: x.accumulation_score, reverse=True)
        rankings["Weak/Vulnerable"].sort(key=lambda x: x.distribution_score, reverse=True)
        rankings["Neutral"].sort(key=lambda x: abs(x.relative_performance))
        
        log.info(f"📈 Major Coins Alignment:")
        log.info(f"  Leaders: {len(rankings['Leaders'])} coins (accumulation)")
        log.info(f"  Weak/Vulnerable: {len(rankings['Weak/Vulnerable'])} coins (distribution)")
        log.info(f"  Neutral: {len(rankings['Neutral'])} coins")
        
        if rankings["Leaders"]:
            top_leader = rankings["Leaders"][0]
            log.info(f"  Top Leader: {top_leader.symbol} (+{top_leader.relative_performance:.1%} vs BTC)")
        
        if rankings["Weak/Vulnerable"]:
            top_weak = rankings["Weak/Vulnerable"][0]
            log.info(f"  Top Weak: {top_weak.symbol} ({top_weak.relative_performance:.1%} vs BTC)")
        
        return rankings
    
    # ========== STEP 4: CROSS-MARKET LIQUIDITY MAP ==========
    async def build_liquidity_map(self, btc_structure: BTCStructure,
                                 alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]]) -> CrossMarketLiquidity:
        """
        STEP 4 — CROSS-MARKET LIQUIDITY MAP
        Identify:
        • Shared HTF highs/lows
        • Correlated stop clusters
        • Which market will be used to trigger first
        • Where liquidation cascades are likely
        
        Answer: Where does the most money get liquidated next?
        """
        
        log.info("🔍 STEP 4: Building Cross-Market Liquidity Map...")
        
        # Get BTC liquidity zones
        btc_zones = self._extract_btc_liquidity_zones(btc_structure)
        
        # Analyze major coins for shared levels
        shared_levels = []
        correlated_clusters = []
        
        # Check each major coin
        for symbol in MAJOR_COINS[:8]:  # Check top 8 majors
            try:
                # Fetch major levels
                major_levels = await self._fetch_major_key_levels(symbol)
                if not major_levels:
                    continue
                
                # Find shared levels with BTC
                shared = self._find_shared_levels(btc_zones, major_levels)
                if shared:
                    shared_levels.extend(shared)
                
                # Identify stop clusters
                clusters = await self._identify_stop_clusters(symbol, major_levels)
                if clusters:
                    correlated_clusters.extend(clusters)
                    
            except Exception as e:
                log.debug(f"Liquidity map error for {symbol}: {e}")
                continue
        
        # Identify trigger market (usually BTC)
        trigger_market = self._determine_trigger_market(btc_structure, alt_rankings)
        
        # Identify liquidation cascades
        liquidation_cascades = await self._identify_liquidation_cascades(shared_levels, trigger_market)
        
        # Find where most money gets liquidated next
        max_liquidation_zone = self._identify_max_liquidation_zone(
            btc_zones, shared_levels, correlated_clusters
        )
        
        # Estimate cascade value
        cascade_value = self._estimate_cascade_value(liquidation_cascades)
        
        liquidity_map = CrossMarketLiquidity(
            shared_htf_levels=shared_levels[:10],  # Top 10
            correlated_stop_clusters=correlated_clusters[:5],  # Top 5
            trigger_market=trigger_market,
            liquidation_cascades=liquidation_cascades,
            max_liquidation_zone=max_liquidation_zone,
            estimated_cascade_value=cascade_value
        )
        
        log.info(f"🗺️ Cross-Market Liquidity:")
        log.info(f"  Shared Levels: {len(liquidity_map.shared_htf_levels)}")
        log.info(f"  Stop Clusters: {len(liquidity_map.correlated_stop_clusters)}")
        log.info(f"  Trigger Market: {liquidity_map.trigger_market}")
        log.info(f"  Max Liquidation: ${liquidity_map.estimated_cascade_value:,.0f}")
        
        if liquidity_map.max_liquidation_zone:
            log.info(f"  Next Big Liquidation: {liquidity_map.max_liquidation_zone.zone_type} "
                    f"@ {liquidity_map.max_liquidation_zone.price:.2f}")
        
        return liquidity_map
    
    # ========== STEP 5: MARKET MAKER INCENTIVE MODEL ==========
    def analyze_market_maker_incentives(self, regime: str, btc_role: BTCRole,
                                       alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                                       liquidity_map: CrossMarketLiquidity) -> MarketMakerIncentives:
        """
        STEP 5 — MARKET MAKER INCENTIVE MODEL
        Explicitly answer:
        • Who is trapped right now?
        • Which side is over-leveraged?
        • Is price being moved to: Kill leverage? Build leverage? Rotate capital?
        • Does continuation or reversal pay more?
        """
        
        log.info("🔍 STEP 5: Analyzing Market Maker Incentives...")
        
        # Identify trapped traders
        trapped_traders = self._identify_trapped_traders(btc_role, alt_rankings, liquidity_map)
        
        # Determine over-leveraged side
        over_leveraged_side = self._determine_over_leveraged_side(btc_role, alt_rankings)
        
        # Determine price movement purpose
        price_purpose = self._determine_price_purpose(regime, btc_role, trapped_traders)
        
        # Calculate optimal direction
        optimal_direction = self._calculate_optimal_direction(
            regime, btc_role, trapped_traders, over_leveraged_side
        )
        
        # Estimate payoffs
        mm_payoff = self._estimate_mm_payoff(
            optimal_direction, trapped_traders, liquidity_map
        )
        
        # Estimate MM inventory
        mm_inventory = self._estimate_mm_inventory(btc_role, alt_rankings)
        
        incentives = MarketMakerIncentives(
            trapped_traders=trapped_traders,
            over_leveraged_side=over_leveraged_side,
            price_movement_purpose=price_purpose,
            optimal_direction=optimal_direction,
            mm_payoff=mm_payoff,
            estimated_mm_inventory=mm_inventory
        )
        
        log.info(f"🎯 Market Maker Incentives:")
        log.info(f"  Trapped: {len(trapped_traders.get('longs_trapped', []))} longs, "
                f"{len(trapped_traders.get('shorts_trapped', []))} shorts")
        log.info(f"  Over-Leveraged: {incentives.over_leveraged_side}")
        log.info(f"  Price Purpose: {incentives.price_movement_purpose}")
        log.info(f"  Optimal Direction: {incentives.optimal_direction}")
        log.info(f"  MM Payoff: Continuation={incentives.mm_payoff.get('continuation', 0):.0%}, "
                f"Reversal={incentives.mm_payoff.get('reversal', 0):.0%}")
        
        return incentives
    
    # ========== STEP 6: TRADE DECISION ==========
    def make_trade_decision(self, regime: str, regime_confidence: float,
                           btc_role: BTCRole, alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                           market_maker_incentives: MarketMakerIncentives) -> TradeDecision:
        """
        STEP 6 — TRADE DECISION (MANDATORY FILTER)
        Choose ONE:
        • 🟢 Long
        • 🔴 Short  
        • ⚫ No Trade
        
        If No Trade, explain what liquidity is missing.
        """
        
        log.info("🔍 STEP 6: Making Trade Decision...")
        
        # ===== NO TRADE CONDITIONS =====
        
        # 1. Low regime confidence
        if regime_confidence < 0.6:
            return TradeDecision(
                decision="⚫ No Trade",
                symbol=None,
                side=None,
                entry_price=None,
                entry_type="",
                missing_liquidity=f"Low regime confidence ({regime_confidence:.0%})"
            )
        
        # 2. Volatility Compression regime
        if "Volatility Compression" in regime:
            return TradeDecision(
                decision="⚫ No Trade",
                symbol=None,
                side=None,
                entry_price=None,
                entry_type="",
                missing_liquidity="Awaiting impulse from volatility compression"
            )
        
        # 3. BTC in Distribution Anchor role
        if btc_role.primary_role == "Distribution anchor":
            return TradeDecision(
                decision="⚫ No Trade",
                symbol=None,
                side=None,
                entry_price=None,
                entry_type="",
                missing_liquidity="BTC in distribution - avoid longs"
            )
        
        # 4. No clear altcoin leadership
        leaders = alt_rankings.get("Leaders", [])
        weak = alt_rankings.get("Weak/Vulnerable", [])
        
        if not leaders and not weak:
            return TradeDecision(
                decision="⚫ No Trade",
                symbol=None,
                side=None,
                entry_price=None,
                entry_type="",
                missing_liquidity="No clear altcoin leadership/weakness"
            )
        
        # ===== TRADE DECISION LOGIC =====
        
        # Risk-On Expansion: Long leaders
        if "Risk-On Expansion" in regime and btc_role.primary_role == "Expansion leader":
            if leaders:
                best_leader = leaders[0]
                return TradeDecision(
                    decision="🟢 Long",
                    symbol=best_leader.symbol,
                    side="LONG",
                    entry_price=self._get_symbol_price(best_leader.symbol),
                    entry_type="expansion_leader",
                    missing_liquidity=None
                )
        
        # Accumulation regime: Long accumulation patterns
        if "Accumulation" in regime and btc_role.primary_role == "Accumulation base":
            # Find strongest accumulation pattern
            accumulation_candidates = [
                alt for alt in leaders 
                if alt.accumulation_score > 0.7 and alt.move_speed == "slow_strong"
            ]
            
            if accumulation_candidates:
                best = max(accumulation_candidates, key=lambda x: x.accumulation_score)
                return TradeDecision(
                    decision="🟢 Long",
                    symbol=best.symbol,
                    side="LONG",
                    entry_price=self._get_symbol_price(best.symbol),
                    entry_type="accumulation_breakout",
                    missing_liquidity=None
                )
        
        # Distribution regime: Short weak coins
        if "Distribution" in regime and btc_role.primary_role == "Distribution anchor":
            if weak:
                weakest = weak[0]
                return TradeDecision(
                    decision="🔴 Short",
                    symbol=weakest.symbol,
                    side="SHORT",
                    entry_price=self._get_symbol_price(weakest.symbol),
                    entry_type="distribution_breakdown",
                    missing_liquidity=None
                )
        
        # Range regime: Wait for breakout
        if "Range" in regime or "Chop" in regime:
            return TradeDecision(
                decision="⚫ No Trade",
                symbol=None,
                side=None,
                entry_price=None,
                entry_type="",
                missing_liquidity="Range environment - wait for liquidity sweep"
            )
        
        # Market Maker incentive alignment
        if market_maker_incentives.optimal_direction == "continuation" and leaders:
            best_leader = leaders[0]
            return TradeDecision(
                decision="🟢 Long",
                symbol=best_leader.symbol,
                side="LONG",
                entry_price=self._get_symbol_price(best_leader.symbol),
                entry_type="mm_incentive_aligned",
                missing_liquidity=None
            )
        
        elif market_maker_incentives.optimal_direction == "reversal" and weak:
            weakest = weak[0]
            return TradeDecision(
                decision="🔴 Short",
                symbol=weakest.symbol,
                side="SHORT",
                entry_price=self._get_symbol_price(weakest.symbol),
                entry_type="mm_incentive_aligned",
                missing_liquidity=None
            )
        
        # Default: No Trade
        return TradeDecision(
            decision="⚫ No Trade",
            symbol=None,
            side=None,
            entry_price=None,
            entry_type="",
            missing_liquidity="No clear edge in current regime setup"
        )
    
    # ========== STEP 7: TAKE PROFIT & STOP LOSS ==========
    def calculate_position_management(self, trade_decision: TradeDecision,
                                     btc_structure: BTCStructure,
                                     liquidity_map: CrossMarketLiquidity) -> Optional[PositionManagement]:
        """
        STEP 7 — TAKE PROFIT & STOP LOSS (CRITICAL)
        
        STOP LOSS LOGIC (NOT RANDOM):
        • Beyond the liquidity level that invalidates the idea
        • Where market makers would NOT hunt unless the bias is wrong
        • Outside obvious retail stop zones
        
        TAKE PROFIT LOGIC (LIQUIDITY-BASED):
        • Nearest opposing liquidity pool
        • Prior HTF high/low
        • Stop clusters of trapped traders
        • Point where leverage is expected to unwind
        """
        
        if trade_decision.decision == "⚫ No Trade":
            return None
        
        log.info("🔍 STEP 7: Calculating Position Management...")
        
        entry_price = trade_decision.entry_price
        side = trade_decision.side
        
        if not entry_price or not side:
            return None
        
        # ===== STOP LOSS CALCULATION =====
        if side == "LONG":
            # Find next major liquidity below
            stop_price = self._find_long_stop_loss(entry_price, btc_structure, liquidity_map)
            stop_logic = {
                "price": stop_price,
                "distance_pct": abs(stop_price - entry_price) / entry_price * 100,
                "logic": "Below HTF low & major liquidity zone",
                "placement": "Beyond market maker hunting range",
                "invalidator": "Break of accumulation structure"
            }
        else:  # SHORT
            stop_price = self._find_short_stop_loss(entry_price, btc_structure, liquidity_map)
            stop_logic = {
                "price": stop_price,
                "distance_pct": abs(stop_price - entry_price) / entry_price * 100,
                "logic": "Above HTF high & equal highs",
                "placement": "Beyond recent swing high",
                "invalidator": "Break of distribution structure"
            }
        
        # ===== TAKE PROFIT CALCULATION =====
        if side == "LONG":
            tp_levels = self._find_long_take_profits(entry_price, btc_structure, liquidity_map)
            tp_logic = {
                "tp1": {
                    "price": tp_levels["tp1"],
                    "pct_from_entry": abs(tp_levels["tp1"] - entry_price) / entry_price * 100,
                    "logic": "First opposing liquidity pool",
                    "size_pct": 0.3  # 30% position
                },
                "tp2": {
                    "price": tp_levels["tp2"],
                    "pct_from_entry": abs(tp_levels["tp2"] - entry_price) / entry_price * 100,
                    "logic": "Prior HTF high & stop cluster",
                    "size_pct": 0.5  # 50% position
                },
                "tp3": {
                    "price": tp_levels["tp3"],
                    "pct_from_entry": abs(tp_levels["tp3"] - entry_price) / entry_price * 100,
                    "logic": "Extreme overshoot / liquidity sweep",
                    "size_pct": 0.2  # 20% position
                }
            }
        else:  # SHORT
            tp_levels = self._find_short_take_profits(entry_price, btc_structure, liquidity_map)
            tp_logic = {
                "tp1": {
                    "price": tp_levels["tp1"],
                    "pct_from_entry": abs(tp_levels["tp1"] - entry_price) / entry_price * 100,
                    "logic": "First opposing liquidity pool",
                    "size_pct": 0.3
                },
                "tp2": {
                    "price": tp_levels["tp2"],
                    "pct_from_entry": abs(tp_levels["tp2"] - entry_price) / entry_price * 100,
                    "logic": "Prior HTF low & long liquidation zone",
                    "size_pct": 0.5
                },
                "tp3": {
                    "price": tp_levels["tp3"],
                    "pct_from_entry": abs(tp_levels["tp3"] - entry_price) / entry_price * 100,
                    "logic": "Extreme overshoot / stop hunt",
                    "size_pct": 0.2
                }
            }
        
        # ===== INVALIDATOR =====
        invalidator = {
            "structural": stop_logic["invalidator"],
            "time_based": "No progress in 4-6 hours",
            "regime_change": "Shift away from current regime",
            "btc_role_change": "BTC changes primary role"
        }
        
        # ===== TIME STOP =====
        time_stop = {
            "max_duration_hours": 12,
            "no_progress_hours": 4,
            "exit_condition": "Price stagnation after entry"
        }
        
        position_mgmt = PositionManagement(
            stop_loss=stop_logic,
            take_profit=tp_logic,
            invalidator=invalidator,
            time_stop=time_stop
        )
        
        log.info(f"💰 Position Management for {trade_decision.symbol} {side}:")
        log.info(f"  Entry: {entry_price:.4f}")
        log.info(f"  Stop Loss: {stop_logic['price']:.4f} ({stop_logic['distance_pct']:.1f}%)")
        log.info(f"  Take Profit 1: {tp_logic['tp1']['price']:.4f} ({tp_logic['tp1']['pct_from_entry']:.1f}%)")
        log.info(f"  Take Profit 2: {tp_logic['tp2']['price']:.4f} ({tp_logic['tp2']['pct_from_entry']:.1f}%)")
        
        return position_mgmt
    
    # ========== MAIN ANALYSIS METHOD ==========
    async def analyze_market(self) -> RegimeAnalysis:
        """
        Complete institutional market analysis following the 7-step framework
        """
        
        self.scan_count += 1
        analysis_start = time.time()
        
        log.info(f"\n{'='*70}")
        log.info(f"🔬 INSTITUTIONAL ANALYSIS #{self.scan_count}")
        log.info(f"{'='*70}")
        
        # ===== STEP 1: GLOBAL REGIME =====
        global_regime, regime_confidence = await self.determine_global_regime()
        
        # ===== STEP 2: BITCOIN ANALYSIS =====
        btc_structure = await self.analyze_bitcoin_structure()
        btc_derivatives = await self.analyze_bitcoin_derivatives()
        btc_role = await self.determine_bitcoin_role(btc_structure, btc_derivatives)
        
        # ===== STEP 3: MAJOR COINS ALIGNMENT =====
        alt_rankings = await self.analyze_majors_alignment()
        
        # ===== STEP 4: CROSS-MARKET LIQUIDITY MAP =====
        liquidity_map = await self.build_liquidity_map(btc_structure, alt_rankings)
        
        # ===== STEP 5: MARKET MAKER INCENTIVES =====
        mm_incentives = self.analyze_market_maker_incentives(
            global_regime, btc_role, alt_rankings, liquidity_map
        )
        
        # ===== STEP 6: TRADE DECISION =====
        trade_decision = self.make_trade_decision(
            global_regime, regime_confidence, btc_role, alt_rankings, mm_incentives
        )
        
        # ===== STEP 7: POSITION MANAGEMENT =====
        position_mgmt = None
        if trade_decision.decision != "⚫ No Trade":
            position_mgmt = self.calculate_position_management(
                trade_decision, btc_structure, liquidity_map
            )
        
        # ===== PREDICT NEXT MOVE =====
        next_move = self._predict_next_move(
            global_regime, btc_role, mm_incentives, liquidity_map
        )
        
        # ===== CALCULATE CONFIDENCE =====
        confidence = self._calculate_overall_confidence(
            regime_confidence, btc_role.role_score, trade_decision, position_mgmt
        )
        
        # ===== CREATE ANALYSIS =====
        analysis_id = hashlib.md5(f"{time.time()}{self.scan_count}".encode()).hexdigest()
        
        analysis = RegimeAnalysis(
            timestamp=time.time(),
            global_regime=global_regime,
            regime_confidence=regime_confidence,
            btc_structure=btc_structure,
            btc_derivatives=btc_derivatives,
            btc_role=btc_role,
            altcoin_rankings=alt_rankings,
            cross_market_liquidity=liquidity_map,
            market_maker_incentives=mm_incentives,
            trade_decision=trade_decision,
            position_management=position_mgmt,
            next_move_prediction=next_move,
            confidence_score=confidence,
            analysis_id=analysis_id
        )
        
        # Store in history
        self.analysis_history.append(analysis)
        
        # Save to database
        await self._save_analysis(analysis)
        
        # Send alert if trade signal
        if trade_decision.decision != "⚫ No Trade":
            await self._send_trade_alert(analysis)
        
        analysis_duration = time.time() - analysis_start
        log.info(f"\n✅ Analysis complete in {analysis_duration:.1f}s")
        log.info(f"📊 Confidence: {confidence:.0%}")
        log.info(f"🎯 Decision: {trade_decision.decision}")
        
        if trade_decision.decision != "⚫ No Trade":
            log.info(f"💰 Symbol: {trade_decision.symbol}")
            log.info(f"📈 Next Move: {next_move}")
        
        return analysis
    
    # ========== HELPER METHODS ==========
    async def _fetch_multi_timeframe_btc(self) -> Dict[str, pd.DataFrame]:
        """Fetch BTC data across multiple timeframes"""
        timeframes = {
            "WEEKLY": "1w",
            "DAILY": "1d",
            "4H": "4h",
            "1H": "1h"
        }
        
        data = {}
        tasks = []
        
        for tf_name, tf in timeframes.items():
            limit = 100 if tf_name == "WEEKLY" else 200
            tasks.append(self._fetch_timeframe_data("BTC/USDT", tf, limit, tf_name))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for tf_name, result in zip(timeframes.keys(), results):
            if isinstance(result, pd.DataFrame) and not result.empty:
                data[tf_name] = result
        
        return data
    
    async def _fetch_timeframe_data(self, symbol: str, timeframe: str, 
                                   limit: int, tf_name: str) -> pd.DataFrame:
        """Fetch OHLCV data for a timeframe"""
        cache_key = f"{symbol}_{tf_name}"
        
        if cache_key in self.data_cache:
            data, timestamp = self.data_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return data
        
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                limit=limit,
                params={'type': 'spot'}
            )
            
            if ohlcv and len(ohlcv) >= 20:
                df = pd.DataFrame(
                    ohlcv,
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = df.dropna()
                
                if len(df) >= 20:
                    self.data_cache[cache_key] = (df, time.time())
                    return df
            
            return pd.DataFrame()
            
        except Exception as e:
            log.debug(f"Fetch error {symbol} {tf_name}: {e}")
            return pd.DataFrame()
    
    def _calculate_value_area(self, df: pd.DataFrame) -> Dict:
        """Calculate value area (simplified)"""
        if len(df) < 20:
            return {"poc": 0, "vah": 0, "val": 0}
        
        # Use recent 20 periods
        recent = df.iloc[-20:]
        
        # Simple POC as VWAP
        vwap = (recent['close'] * recent['volume']).sum() / recent['volume'].sum()
        
        return {
            "poc": float(vwap),
            "vah": float(recent['high'].max()),
            "val": float(recent['low'].min())
        }
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        if len(df) < period:
            return 0.0
        
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        
        tr = np.zeros(len(df))
        for i in range(1, len(df)):
            tr1 = high[i] - low[i]
            tr2 = abs(high[i] - close[i-1])
            tr3 = abs(low[i] - close[i-1])
            tr[i] = max(tr1, tr2, tr3)
        
        atr = np.mean(tr[-period:])
        return float(atr)
    
    def _calculate_momentum(self, df: pd.DataFrame) -> float:
        """Calculate momentum score (0-1)"""
        if len(df) < 10:
            return 0.5
        
        # Simple momentum: price change and acceleration
        price_change = (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10]
        recent_change = (df['close'].iloc[-1] - df['close'].iloc[-3]) / df['close'].iloc[-3]
        
        # Combine with some volume confirmation
        volume_trend = df['volume'].iloc[-5:].mean() / df['volume'].iloc[-10:-5].mean()
        
        momentum = 0.5 + (price_change * 2)  # Normalize
        if recent_change > 0 and volume_trend > 1:
            momentum += 0.2
        
        return max(0.0, min(momentum, 1.0))
    
    def _identify_clear_levels(self, df: pd.DataFrame) -> List[float]:
        """Identify clear support/resistance levels"""
        levels = []
        
        if len(df) < 20:
            return levels
        
        # Look for price reactions
        for i in range(10, len(df) - 5):
            price = df['close'].iloc[i]
            
            # Check if price reacted at this level multiple times
            reactions = 0
            for j in range(max(0, i-10), min(len(df), i+10)):
                if j == i:
                    continue
                
                if abs(df['close'].iloc[j] - price) / price < 0.01:  # Within 1%
                    reactions += 1
            
            if reactions >= 2:
                levels.append(float(price))
        
        return list(set(levels))  # Remove duplicates
    
    def _calculate_mean_reversion(self, df: pd.DataFrame) -> float:
        """Calculate mean reversion tendency (0-1)"""
        if len(df) < 30:
            return 0.5
        
        # Check if price reverts to mean
        mean_price = df['close'].mean()
        std_price = df['close'].std()
        
        current_price = df['close'].iloc[-1]
        z_score = (current_price - mean_price) / std_price
        
        # Probability of mean reversion based on extremeness
        if abs(z_score) > 2:
            return 0.8
        elif abs(z_score) > 1:
            return 0.6
        else:
            return 0.4
    
    def _count_failed_breakouts(self, df: pd.DataFrame) -> int:
        """Count failed breakout attempts"""
        failed = 0
        
        if len(df) < 10:
            return failed
        
        recent_high = df['high'].iloc[-20:].max()
        
        for i in range(-5, 0):
            if i >= -len(df):
                candle = df.iloc[i]
                # Break above high but close back below
                if candle['high'] > recent_high * 1.005 and candle['close'] < recent_high * 0.995:
                    failed += 1
        
        return failed
    
    def _analyze_order_flow(self, df: pd.DataFrame) -> Dict:
        """Analyze order flow bias"""
        if len(df) < 10:
            return {"bullish_dominance": False, "bearish_dominance": False}
        
        bullish = 0
        bearish = 0
        
        for i in range(-5, 0):
            if i >= -len(df):
                candle = df.iloc[i]
                if candle['close'] > candle['open']:
                    bullish += 1
                else:
                    bearish += 1
        
        return {
            "bullish_dominance": bullish > bearish * 1.5,
            "bearish_dominance": bearish > bullish * 1.5
        }
    
    def _detect_post_flush_basing(self, df: pd.DataFrame) -> float:
        """Detect post-flush basing pattern"""
        if len(df) < 10:
            return 0.0
        
        score = 0.0
        
        # Look for big down candle followed by small range candles
        for i in range(-9, -4):
            if i >= -len(df):
                down_candle = df.iloc[i]
                # Big red candle
                if down_candle['close'] < down_candle['open'] * 0.95:
                    # Check next 3 candles for basing
                    basing = True
                    for j in range(i+1, i+4):
                        if j < len(df):
                            candle = df.iloc[j]
                            candle_range = candle['high'] - candle['low']
                            if candle_range > (down_candle['high'] - down_candle['low']) * 0.5:
                                basing = False
                                break
                    
                    if basing:
                        score += 0.5
        
        return min(score, 1.0)
    
    def _analyze_volume_profile(self, df: pd.DataFrame) -> Dict:
        """Analyze volume profile for absorption"""
        if len(df) < 20:
            return {"absorption": False}
        
        # Simple absorption detection: high volume down candles followed by low volume up candles
        absorption = False
        
        for i in range(-5, -1):
            if i >= -len(df):
                down_candle = df.iloc[i]
                if i+1 < len(df):
                    up_candle = df.iloc[i+1]
                    
                    if (down_candle['close'] < down_candle['open'] and
                        up_candle['close'] > up_candle['open'] and
                        down_candle['volume'] > up_candle['volume'] * 1.5):
                        absorption = True
        
        return {"absorption": absorption}
    
    def _analyze_volatility_trend(self, df: pd.DataFrame) -> Dict:
        """Analyze volatility trend"""
        if len(df) < 20:
            return {"decreasing": False, "increasing": False}
        
        recent_vol = self._calculate_atr(df.iloc[-10:])
        prior_vol = self._calculate_atr(df.iloc[-20:-10])
        
        return {
            "decreasing": recent_vol < prior_vol * 0.8,
            "increasing": recent_vol > prior_vol * 1.2
        }
    
    def _assess_support_strength(self, df: pd.DataFrame) -> float:
        """Assess support strength (0-1)"""
        if len(df) < 10:
            return 0.5
        
        recent_low = df['low'].iloc[-10:].min()
        touches = 0
        
        for i in range(-10, 0):
            if i >= -len(df):
                candle = df.iloc[i]
                if abs(candle['low'] - recent_low) / recent_low < 0.005:  # Within 0.5%
                    touches += 1
        
        # More touches = stronger support
        return min(touches / 10, 1.0)
    
    def _detect_compression_pattern(self, df: pd.DataFrame) -> float:
        """Detect volatility compression pattern"""
        if len(df) < 20:
            return 0.0
        
        # Check for converging highs and lows
        highs = df['high'].iloc[-20:].values
        lows = df['low'].iloc[-20:].values
        
        # Linear regression slopes
        if len(highs) > 1 and len(lows) > 1:
            x = np.arange(len(highs))
            high_slope = np.polyfit(x, highs, 1)[0]
            low_slope = np.polyfit(x, lows, 1)[0]
            
            # Converging pattern: highs sloping down, lows sloping up
            if high_slope < 0 and low_slope > 0:
                return 0.7
            # Parallel compression
            elif abs(high_slope) < 0.001 and abs(low_slope) < 0.001:
                return 0.5
        
        return 0.0
    
    def _calculate_htf_highs_lows(self, df: pd.DataFrame) -> Dict:
        """Calculate HTF highs and lows"""
        if len(df) < 20:
            return {"htf_high": 0, "htf_low": 0}
        
        return {
            "htf_high": float(df['high'].iloc[-20:].max()),
            "htf_low": float(df['low'].iloc[-20:].min())
        }
    
    def _calculate_acceptance_rejection(self, df: pd.DataFrame) -> Dict:
        """Calculate acceptance vs rejection scores"""
        if len(df) < 10:
            return {"acceptance": 0.5, "rejection": 0.5}
        
        acceptance = 0
        rejection = 0
        total = 0
        
        for i in range(-5, 0):
            if i >= -len(df):
                candle = df.iloc[i]
                body = abs(candle['close'] - candle['open'])
                total_range = candle['high'] - candle['low']
                
                if total_range > 0:
                    body_ratio = body / total_range
                    
                    if candle['close'] > candle['open']:  # Bullish
                        if body_ratio > 0.7:  # Strong acceptance
                            acceptance += 1
                        elif body_ratio < 0.3:  # Weak, possible rejection
                            rejection += 0.5
                    else:  # Bearish
                        if body_ratio > 0.7:  # Strong rejection
                            rejection += 1
                        elif body_ratio < 0.3:  # Weak, possible acceptance
                            acceptance += 0.5
                
                total += 1
        
        if total > 0:
            return {
                "acceptance": acceptance / total,
                "rejection": rejection / total
            }
        
        return {"acceptance": 0.5, "rejection": 0.5}
    
    def _calculate_time_in_value_area(self, df: pd.DataFrame) -> Dict:
        """Calculate time spent above/below value area"""
        if len(df) < 20:
            return {"above": 0.5, "below": 0.5}
        
        value_area = self._calculate_value_area(df)
        poc = value_area["poc"]
        
        above = sum(df['close'].iloc[-20:] > poc)
        below = sum(df['close'].iloc[-20:] < poc)
        
        total = above + below
        if total > 0:
            return {
                "above": above / total,
                "below": below / total
            }
        
        return {"above": 0.5, "below": 0.5}
    
    def _find_equal_highs(self, df: pd.DataFrame) -> List[float]:
        """Find equal highs (within 0.5%)"""
        if len(df) < 10:
            return []
        
        highs = []
        recent_highs = df['high'].iloc[-20:].values
        
        for i in range(len(recent_highs)):
            for j in range(i+1, len(recent_highs)):
                if abs(recent_highs[i] - recent_highs[j]) / recent_highs[i] < 0.005:  # 0.5%
                    avg_high = (recent_highs[i] + recent_highs[j]) / 2
                    highs.append(float(avg_high))
        
        return list(set(highs))  # Remove duplicates
    
    def _find_equal_lows(self, df: pd.DataFrame) -> List[float]:
        """Find equal lows (within 0.5%)"""
        if len(df) < 10:
            return []
        
        lows = []
        recent_lows = df['low'].iloc[-20:].values
        
        for i in range(len(recent_lows)):
            for j in range(i+1, len(recent_lows)):
                if abs(recent_lows[i] - recent_lows[j]) / recent_lows[i] < 0.005:  # 0.5%
                    avg_low = (recent_lows[i] + recent_lows[j]) / 2
                    lows.append(float(avg_low))
        
        return list(set(lows))
    
    def _find_prior_key_levels(self, df: pd.DataFrame) -> Dict:
        """Find prior key levels"""
        if len(df) < 14:
            return {"weekly_high": 0, "weekly_low": 0, "daily_high": 0, "daily_low": 0}
        
        return {
            "weekly_high": float(df['high'].iloc[-7:].max()),
            "weekly_low": float(df['low'].iloc[-7:].min()),
            "daily_high": float(df['high'].iloc[-2]),
            "daily_low": float(df['low'].iloc[-2])
        }
    
    def _estimate_untaken_stops(self, df: pd.DataFrame) -> Dict:
        """Estimate untaken stop levels"""
        if len(df) < 10:
            return {"above": 0, "below": 0, "estimated_value": 0}
        
        recent_high = df['high'].iloc[-10:].max()
        recent_low = df['low'].iloc[-10:].min()
        
        # Estimate stops at 1-2% beyond recent extremes
        return {
            "above": float(recent_high * 1.015),  # 1.5% above
            "below": float(recent_low * 0.985),   # 1.5% below
            "estimated_value": 10000000  # Placeholder $10M estimate
        }
    
    async def _estimate_oi_trend(self) -> str:
        """Estimate Open Interest trend (simplified)"""
        # In production, fetch actual OI data from exchange
        # For now, use price action as proxy
        try:
            df = await self._fetch_timeframe_data("BTC/USDT", "1h", 24, "1h_oi_proxy")
            if len(df) < 10:
                return "flat"
            
            # Simple trend detection
            price_change = (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10]
            
            if abs(price_change) < 0.01:
                return "flat"
            elif price_change > 0.02:
                return "rising"
            else:
                return "falling"
                
        except:
            return "unknown"
    
    async def _detect_price_oi_divergence(self) -> bool:
        """Detect price-OI divergence"""
        # In production, compare price trend with OI trend
        # For now, return False (no divergence detected)
        return False
    
    async def _estimate_liquidation_levels(self) -> Dict[str, float]:
        """Estimate liquidation levels"""
        # In production, fetch liquidation data
        # For now, estimate based on recent highs/lows
        try:
            df = await self._fetch_timeframe_data("BTC/USDT", "1h", 24, "1h_liq")
            if len(df) < 10:
                return {"longs": 0, "shorts": 0}
            
            recent_high = df['high'].iloc[-10:].max()
            recent_low = df['low'].iloc[-10:].min()
            
            # Estimate liquidation zones
            return {
                "longs": float(recent_low * 0.97),  # 3% below recent low
                "shorts": float(recent_high * 1.03)  # 3% above recent high
            }
            
        except:
            return {"longs": 0, "shorts": 0}
    
    def _build_role_evidence(self, role: str, structure: BTCStructure, 
                            derivatives: BTCDerivatives) -> List[str]:
        """Build evidence list for BTC role"""
        evidence = []
        
        if role == "Expansion leader":
            if structure.acceptance_score > 0.7:
                evidence.append("Strong acceptance above value")
            if structure.time_above_value > 0.7:
                evidence.append("Sustained time above value area")
            if derivatives.funding_bias == "positive":
                evidence.append("Positive funding rate")
                
        elif role == "Range controller":
            if structure.equal_highs and structure.equal_lows:
                evidence.append("Clear equal highs and lows")
            if 0.3 < structure.time_above_value < 0.7:
                evidence.append("Balanced time in value area")
                
        elif role == "Distribution anchor":
            if structure.rejection_score > 0.7:
                evidence.append("Strong rejection at highs")
            if len(structure.equal_highs) >= 2:
                evidence.append("Multiple equal highs resistance")
            if derivatives.funding_bias == "negative":
                evidence.append("Negative funding rate")
                
        elif role == "Accumulation base":
            if len(structure.equal_lows) >= 2:
                evidence.append("Multiple equal lows support")
            if structure.time_below_value > 0.7:
                evidence.append("Sustained time below value area")
                
        elif role == "Liquidity sweep instrument":
            if derivatives.estimated_liquidations:
                evidence.append("High estimated liquidations nearby")
            if derivatives.price_oi_divergence:
                evidence.append("Price-OI divergence detected")
        
        return evidence
    
    async def _calculate_btc_performance(self) -> float:
        """Calculate BTC performance over last 24 hours"""
        try:
            df = await self._fetch_timeframe_data("BTC/USDT", "1h", 24, "1h_btc_perf")
            if len(df) < 2:
                return 0.0
            
            return (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
            
        except:
            return 0.0
    
    async def _fetch_altcoin_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch altcoin data"""
        try:
            df = await self._fetch_timeframe_data(symbol, "1h", 24, f"1h_{symbol}")
            if len(df) >= 10:
                return df
        except:
            pass
        return None
    
    async def _calculate_relative_performance(self, symbol: str, btc_perf: float) -> float:
        """Calculate altcoin performance relative to BTC"""
        try:
            df = await self._fetch_altcoin_data(symbol)
            if df is None or len(df) < 2:
                return 0.0
            
            alt_perf = (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
            return alt_perf - btc_perf
            
        except:
            return 0.0
    
    async def _analyze_move_speed(self, symbol: str) -> str:
        """Analyze if moves are fast (leverage) or slow (strong hands)"""
        try:
            df = await self._fetch_altcoin_data(symbol)
            if df is None or len(df) < 10:
                return "unknown"
            
            # Calculate volatility
            volatility = df['close'].pct_change().std()
            
            if volatility > 0.03:  # High volatility = likely leverage
                return "fast_leverage"
            else:
                return "slow_strong"
                
        except:
            return "unknown"
    
    async def _calculate_accumulation_score(self, symbol: str, relative_perf: float) -> float:
        """Calculate accumulation score (0-1)"""
        try:
            df = await self._fetch_altcoin_data(symbol)
            if df is None or len(df) < 10:
                return 0.5
            
            score = 0.0
            
            # Relative outperformance
            if relative_perf > 0.02:
                score += 0.3
            
            # Volume trend
            volume_trend = df['volume'].iloc[-5:].mean() / df['volume'].iloc[-10:-5].mean()
            if volume_trend > 1.2:
                score += 0.3
            
            # Support holding
            recent_low = df['low'].iloc[-10:].min()
            current_price = df['close'].iloc[-1]
            if abs(current_price - recent_low) / recent_low > 0.05:  # 5% above recent low
                score += 0.2
            
            # Low volatility uptrend (strong hands)
            volatility = df['close'].pct_change().std()
            if volatility < 0.02 and relative_perf > 0:
                score += 0.2
            
            return min(score, 1.0)
            
        except:
            return 0.5
    
    async def _calculate_distribution_score(self, symbol: str, relative_perf: float) -> float:
        """Calculate distribution score (0-1)"""
        try:
            df = await self._fetch_altcoin_data(symbol)
            if df is None or len(df) < 10:
                return 0.5
            
            score = 0.0
            
            # Relative underperformance
            if relative_perf < -0.02:
                score += 0.3
            
            # Volume divergence (price down, volume up)
            price_change = (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10]
            volume_change = df['volume'].iloc[-5:].mean() / df['volume'].iloc[-10:-5].mean()
            
            if price_change < 0 and volume_change > 1.2:
                score += 0.3
            
            # Resistance holding
            recent_high = df['high'].iloc[-10:].max()
            current_price = df['close'].iloc[-1]
            if abs(current_price - recent_high) / recent_high < 0.02:  # Within 2% of high
                score += 0.2
            
            # High volatility downtrend (weak hands selling)
            volatility = df['close'].pct_change().std()
            if volatility > 0.03 and relative_perf < 0:
                score += 0.2
            
            return min(score, 1.0)
            
        except:
            return 0.5
    
    async def _calculate_beta_coefficient(self, symbol: str) -> float:
        """Calculate beta coefficient (responsiveness to BTC)"""
        try:
            # Fetch BTC and altcoin hourly data
            btc_df = await self._fetch_timeframe_data("BTC/USDT", "1h", 24, "1h_btc_beta")
            alt_df = await self._fetch_altcoin_data(symbol)
            
            if btc_df is None or alt_df is None or len(btc_df) < 10 or len(alt_df) < 10:
                return 1.0
            
            # Align data
            min_len = min(len(btc_df), len(alt_df))
            btc_returns = btc_df['close'].iloc[:min_len].pct_change().dropna().values
            alt_returns = alt_df['close'].iloc[:min_len].pct_change().dropna().values
            
            if len(btc_returns) < 5 or len(alt_returns) < 5:
                return 1.0
            
            # Simple beta calculation
            covariance = np.cov(alt_returns, btc_returns)[0, 1]
            btc_variance = np.var(btc_returns)
            
            if btc_variance > 0:
                beta = covariance / btc_variance
                return float(beta)
            
            return 1.0
            
        except:
            return 1.0
    
    async def _calculate_correlation_24h(self, symbol: str) -> float:
        """Calculate 24h correlation with BTC"""
        try:
            btc_df = await self._fetch_timeframe_data("BTC/USDT", "1h", 24, "1h_btc_corr")
            alt_df = await self._fetch_altcoin_data(symbol)
            
            if btc_df is None or alt_df is None or len(btc_df) < 10 or len(alt_df) < 10:
                return 0.0
            
            # Align data
            min_len = min(len(btc_df), len(alt_df))
            btc_prices = btc_df['close'].iloc[:min_len].values
            alt_prices = alt_df['close'].iloc[:min_len].values
            
            if len(btc_prices) < 5 or len(alt_prices) < 5:
                return 0.0
            
            correlation = np.corrcoef(btc_prices, alt_prices)[0, 1]
            return float(correlation) if not np.isnan(correlation) else 0.0
            
        except:
            return 0.0
    
    def _extract_btc_liquidity_zones(self, btc_structure: BTCStructure) -> List[LiquidityZone]:
        """Extract BTC liquidity zones from structure"""
        zones = []
        
        # Equal highs
        for price in btc_structure.equal_highs:
            zones.append(LiquidityZone(
                price=price,
                zone_type="equal_high",
                strength=0.8,
                market_coverage=1,  # BTC only
                estimated_stops=10000000,  # $10M estimate
                recent_test=False,
                distance_pct=0.0
            ))
        
        # Equal lows
        for price in btc_structure.equal_lows:
            zones.append(LiquidityZone(
                price=price,
                zone_type="equal_low",
                strength=0.8,
                market_coverage=1,
                estimated_stops=10000000,
                recent_test=False,
                distance_pct=0.0
            ))
        
        # Prior levels
        zones.append(LiquidityZone(
            price=btc_structure.prior_weekly_high,
            zone_type="prior_weekly_high",
            strength=0.7,
            market_coverage=1,
            estimated_stops=15000000,
            recent_test=False,
            distance_pct=0.0
        ))
        
        zones.append(LiquidityZone(
            price=btc_structure.prior_weekly_low,
            zone_type="prior_weekly_low",
            strength=0.7,
            market_coverage=1,
            estimated_stops=15000000,
            recent_test=False,
            distance_pct=0.0
        ))
        
        return zones
    
    async def _fetch_major_key_levels(self, symbol: str) -> List[LiquidityZone]:
        """Fetch key levels for a major coin"""
        try:
            df = await self._fetch_timeframe_data(symbol, "4h", 40, f"4h_{symbol}")
            if df is None or len(df) < 20:
                return []
            
            zones = []
            
            # Recent highs and lows
            recent_high = df['high'].iloc[-20:].max()
            recent_low = df['low'].iloc[-20:].min()
            
            zones.append(LiquidityZone(
                price=float(recent_high),
                zone_type="recent_high",
                strength=0.6,
                market_coverage=1,
                estimated_stops=5000000,  # $5M estimate
                recent_test=False,
                distance_pct=0.0
            ))
            
            zones.append(LiquidityZone(
                price=float(recent_low),
                zone_type="recent_low",
                strength=0.6,
                market_coverage=1,
                estimated_stops=5000000,
                recent_test=False,
                distance_pct=0.0
            ))
            
            return zones
            
        except:
            return []
    
    def _find_shared_levels(self, btc_zones: List[LiquidityZone], 
                          major_zones: List[LiquidityZone]) -> List[LiquidityZone]:
        """Find shared levels between BTC and major"""
        shared = []
        
        for btc_zone in btc_zones:
            for major_zone in major_zones:
                # Check if levels are within 1% of each other
                if abs(btc_zone.price - major_zone.price) / btc_zone.price < 0.01:
                    # Create combined zone
                    combined = LiquidityZone(
                        price=(btc_zone.price + major_zone.price) / 2,
                        zone_type=f"shared_{btc_zone.zone_type}",
                        strength=(btc_zone.strength + major_zone.strength) / 2,
                        market_coverage=btc_zone.market_coverage + major_zone.market_coverage,
                        estimated_stops=btc_zone.estimated_stops + major_zone.estimated_stops,
                        recent_test=btc_zone.recent_test or major_zone.recent_test,
                        distance_pct=0.0
                    )
                    shared.append(combined)
        
        return shared
    
    async def _identify_stop_clusters(self, symbol: str, 
                                    major_levels: List[LiquidityZone]) -> List[Dict]:
        """Identify stop clusters for a major coin"""
        clusters = []
        
        for zone in major_levels:
            clusters.append({
                "symbol": symbol,
                "price": zone.price,
                "zone_type": zone.zone_type,
                "estimated_stops": zone.estimated_stops,
                "likely_side": "long" if "low" in zone.zone_type else "short"
            })
        
        return clusters
    
    def _determine_trigger_market(self, btc_structure: BTCStructure,
                                 alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]]) -> str:
        """Determine which market triggers first"""
        # BTC usually triggers first
        return "BTC"
    
    async def _identify_liquidation_cascades(self, shared_levels: List[LiquidityZone],
                                           trigger_market: str) -> List[Dict]:
        """Identify potential liquidation cascades"""
        cascades = []
        
        for zone in shared_levels:
            if zone.market_coverage >= 2:  # Shared by at least 2 markets
                cascades.append({
                    "price": zone.price,
                    "zone_type": zone.zone_type,
                    "market_coverage": zone.market_coverage,
                    "trigger_market": trigger_market,
                    "estimated_cascade_value": zone.estimated_stops * zone.market_coverage
                })
        
        return sorted(cascades, key=lambda x: x["estimated_cascade_value"], reverse=True)
    
    def _identify_max_liquidation_zone(self, btc_zones: List[LiquidityZone],
                                     shared_levels: List[LiquidityZone],
                                     correlated_clusters: List[Dict]) -> Optional[LiquidityZone]:
        """Identify where most money gets liquidated next"""
        all_zones = btc_zones + shared_levels
        
        if not all_zones:
            return None
        
        # Find zone with highest estimated stops and market coverage
        max_zone = max(all_zones, key=lambda z: z.estimated_stops * z.market_coverage)
        
        # Update distance from current price
        current_price = self._get_current_btc_price()
        if current_price > 0:
            max_zone.distance_pct = abs(max_zone.price - current_price) / current_price * 100
        
        return max_zone
    
    def _estimate_cascade_value(self, liquidation_cascades: List[Dict]) -> float:
        """Estimate total cascade value"""
        if not liquidation_cascades:
            return 0.0
        
        return sum(c["estimated_cascade_value"] for c in liquidation_cascades)
    
    def _identify_trapped_traders(self, btc_role: BTCRole,
                                 alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                                 liquidity_map: CrossMarketLiquidity) -> Dict[str, List]:
        """Identify trapped traders"""
        trapped = {
            "longs_trapped": [],
            "shorts_trapped": []
        }
        
        # Based on BTC role
        if btc_role.primary_role == "Distribution anchor":
            # Longs are likely trapped at highs
            trapped["longs_trapped"].append({
                "market": "BTC",
                "reason": "Distribution at highs",
                "estimated_size": "large"
            })
        
        elif btc_role.primary_role == "Accumulation base":
            # Shorts are likely trapped at lows
            trapped["shorts_trapped"].append({
                "market": "BTC",
                "reason": "Accumulation at lows",
                "estimated_size": "large"
            })
        
        # Check weak altcoins for trapped longs
        for alt in alt_rankings.get("Weak/Vulnerable", []):
            if alt.distribution_score > 0.7:
                trapped["longs_trapped"].append({
                    "market": alt.symbol,
                    "reason": "Relative weakness vs BTC",
                    "estimated_size": "medium"
                })
        
        return trapped
    
    def _determine_over_leveraged_side(self, btc_role: BTCRole,
                                     alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]]) -> str:
        """Determine over-leveraged side"""
        
        # Analyze based on BTC role and funding
        if btc_role.primary_role == "Expansion leader":
            # Usually longs are over-leveraged in expansions
            return "longs"
        
        elif btc_role.primary_role == "Distribution anchor":
            # Could be either, but often longs trapped
            return "longs"
        
        elif btc_role.primary_role == "Accumulation base":
            # Often shorts over-leveraged at lows
            return "shorts"
        
        # Default balanced
        return "balanced"
    
    def _determine_price_purpose(self, regime: str, btc_role: BTCRole,
                                trapped_traders: Dict[str, List]) -> str:
        """Determine price movement purpose"""
        
        if "Distribution" in regime:
            return "kill_leverage"  # Kill over-leveraged longs
        
        elif "Accumulation" in regime:
            return "build_leverage"  # Build leverage for next move
        
        elif trapped_traders["longs_trapped"] or trapped_traders["shorts_trapped"]:
            return "rotate_capital"  # Rotate from trapped to free capital
        
        elif "Expansion" in regime:
            return "build_leverage"  # Build momentum
        
        else:
            return "kill_leverage"  # Default
    
    def _calculate_optimal_direction(self, regime: str, btc_role: BTCRole,
                                   trapped_traders: Dict[str, List],
                                   over_leveraged_side: str) -> str:
        """Calculate optimal direction (continuation vs reversal)"""
        
        # If one side is heavily trapped, reversal often pays more
        if (len(trapped_traders["longs_trapped"]) > 2 and 
            over_leveraged_side == "longs"):
            return "reversal"  # Squeeze shorts, trap longs
        
        elif (len(trapped_traders["shorts_trapped"]) > 2 and 
              over_leveraged_side == "shorts"):
            return "reversal"  # Squeeze longs, trap shorts
        
        # In strong trends, continuation pays
        if "Expansion" in regime and btc_role.primary_role == "Expansion leader":
            return "continuation"
        
        # In accumulation, continuation of basing
        if "Accumulation" in regime:
            return "continuation"  # Continue basing
        
        # Default: assess based on trapped traders
        if len(trapped_traders["longs_trapped"]) > len(trapped_traders["shorts_trapped"]):
            return "reversal"  # More longs trapped, reversal down
        elif len(trapped_traders["shorts_trapped"]) > len(trapped_traders["longs_trapped"]):
            return "reversal"  # More shorts trapped, reversal up
        
        return "continuation"  # Default
    
    def _estimate_mm_payoff(self, optimal_direction: str,
                           trapped_traders: Dict[str, List],
                           liquidity_map: CrossMarketLiquidity) -> Dict[str, float]:
        """Estimate market maker payoff"""
        
        continuation_payoff = 0.5
        reversal_payoff = 0.5
        
        # Adjust based on trapped traders
        trapped_long_count = len(trapped_traders.get("longs_trapped", []))
        trapped_short_count = len(trapped_traders.get("shorts_trapped", []))
        
        if trapped_long_count > trapped_short_count:
            # More longs trapped, reversal down pays more
            reversal_payoff = 0.7
            continuation_payoff = 0.3
        
        elif trapped_short_count > trapped_long_count:
            # More shorts trapped, reversal up pays more
            reversal_payoff = 0.7
            continuation_payoff = 0.3
        
        # Consider liquidation cascades
        if liquidity_map.estimated_cascade_value > 50000000:  # $50M+
            # Big liquidations = bigger payoff for MM
            if optimal_direction == "reversal":
                reversal_payoff *= 1.2
            else:
                continuation_payoff *= 1.2
        
        return {
            "continuation": min(continuation_payoff, 1.0),
            "reversal": min(reversal_payoff, 1.0)
        }
    
    def _estimate_mm_inventory(self, btc_role: BTCRole,
                              alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]]) -> Dict[str, float]:
        """Estimate market maker inventory"""
        
        # Simplified estimation
        if btc_role.primary_role == "Accumulation base":
            return {
                "long_inventory": 0.7,  # 70% long bias
                "short_inventory": 0.3   # 30% short bias
            }
        
        elif btc_role.primary_role == "Distribution anchor":
            return {
                "long_inventory": 0.3,
                "short_inventory": 0.7
            }
        
        else:
            return {
                "long_inventory": 0.5,
                "short_inventory": 0.5
            }
    
    def _get_symbol_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol"""
        # In production, fetch from exchange
        # For now, return placeholder
        return 100.0  # Placeholder
    
    def _get_current_btc_price(self) -> float:
        """Get current BTC price"""
        # In production, fetch from exchange
        # For now, return placeholder
        return 50000.0  # Placeholder
    
    def _find_long_stop_loss(self, entry_price: float, btc_structure: BTCStructure,
                            liquidity_map: CrossMarketLiquidity) -> float:
        """Find stop loss for long position"""
        
        # Below nearest major liquidity zone
        candidate_stops = []
        
        # Below equal lows
        for low in btc_structure.equal_lows:
            if low < entry_price:
                candidate_stops.append(low * 0.99)  # 1% below equal low
        
        # Below prior weekly low
        if btc_structure.prior_weekly_low < entry_price:
            candidate_stops.append(btc_structure.prior_weekly_low * 0.99)
        
        # Below HTF low
        if btc_structure.htf_low < entry_price:
            candidate_stops.append(btc_structure.htf_low * 0.98)  # 2% below HTF low
        
        if candidate_stops:
            return min(candidate_stops)
        
        # Default: 3% below entry
        return entry_price * 0.97
    
    def _find_short_stop_loss(self, entry_price: float, btc_structure: BTCStructure,
                             liquidity_map: CrossMarketLiquidity) -> float:
        """Find stop loss for short position"""
        
        # Above nearest major liquidity zone
        candidate_stops = []
        
        # Above equal highs
        for high in btc_structure.equal_highs:
            if high > entry_price:
                candidate_stops.append(high * 1.01)  # 1% above equal high
        
        # Above prior weekly high
        if btc_structure.prior_weekly_high > entry_price:
            candidate_stops.append(btc_structure.prior_weekly_high * 1.01)
        
        # Above HTF high
        if btc_structure.htf_high > entry_price:
            candidate_stops.append(btc_structure.htf_high * 1.02)  # 2% above HTF high
        
        if candidate_stops:
            return max(candidate_stops)
        
        # Default: 3% above entry
        return entry_price * 1.03
    
    def _find_long_take_profits(self, entry_price: float, btc_structure: BTCStructure,
                               liquidity_map: CrossMarketLiquidity) -> Dict[str, float]:
        """Find take profit levels for long position"""
        
        tps = {}
        
        # TP1: First opposing liquidity pool (equal high)
        for high in btc_structure.equal_highs:
            if high > entry_price:
                tps["tp1"] = high
                break
        
        if "tp1" not in tps:
            tps["tp1"] = entry_price * 1.02  # 2% default
        
        # TP2: Prior HTF high
        tps["tp2"] = btc_structure.htf_high
        
        # TP3: Extreme overshoot (beyond HTF high)
        tps["tp3"] = btc_structure.htf_high * 1.05  # 5% beyond
        
        return tps
    
    def _find_short_take_profits(self, entry_price: float, btc_structure: BTCStructure,
                                liquidity_map: CrossMarketLiquidity) -> Dict[str, float]:
        """Find take profit levels for short position"""
        
        tps = {}
        
        # TP1: First opposing liquidity pool (equal low)
        for low in btc_structure.equal_lows:
            if low < entry_price:
                tps["tp1"] = low
                break
        
        if "tp1" not in tps:
            tps["tp1"] = entry_price * 0.98  # 2% default
        
        # TP2: Prior HTF low
        tps["tp2"] = btc_structure.htf_low
        
        # TP3: Extreme overshoot (beyond HTF low)
        tps["tp3"] = btc_structure.htf_low * 0.95  # 5% beyond
        
        return tps
    
    def _predict_next_move(self, regime: str, btc_role: BTCRole,
                          mm_incentives: MarketMakerIncentives,
                          liquidity_map: CrossMarketLiquidity) -> str:
        """Predict next market move"""
        
        if "Expansion" in regime and btc_role.primary_role == "Expansion leader":
            return "Continuation of uptrend toward next liquidity pool"
        
        elif "Distribution" in regime:
            return "Breakdown toward liquidation zones below"
        
        elif "Accumulation" in regime:
            return "Breakout toward resistance after basing"
        
        elif mm_incentives.optimal_direction == "reversal":
            if len(mm_incentives.trapped_traders.get("longs_trapped", [])) > \
               len(mm_incentives.trapped_traders.get("shorts_trapped", [])):
                return "Reversal down to liquidate trapped longs"
            else:
                return "Reversal up to liquidate trapped shorts"
        
        else:
            return "Range expansion toward nearest liquidity cluster"
    
    def _calculate_overall_confidence(self, regime_confidence: float,
                                    btc_role_score: float,
                                    trade_decision: TradeDecision,
                                    position_mgmt: Optional[PositionManagement]) -> float:
        """Calculate overall confidence score"""
        
        confidence = 0.0
        
        # Regime confidence (40%)
        confidence += regime_confidence * 0.4
        
        # BTC role confidence (30%)
        confidence += btc_role_score * 0.3
        
        # Trade decision clarity (20%)
        if trade_decision.decision != "⚫ No Trade":
            confidence += 0.2
        
        # Position management clarity (10%)
        if position_mgmt:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _get_default_btc_structure(self) -> BTCStructure:
        """Get default BTC structure"""
        return BTCStructure(
            htf_high=0.0,
            htf_low=0.0,
            equal_highs=[],
            equal_lows=[],
            prior_weekly_high=0.0,
            prior_weekly_low=0.0,
            prior_daily_high=0.0,
            prior_daily_low=0.0,
            acceptance_score=0.5,
            rejection_score=0.5,
            time_above_value=0.5,
            time_below_value=0.5
        )
    
    def _get_default_btc_derivatives(self) -> BTCDerivatives:
        """Get default BTC derivatives"""
        return BTCDerivatives(
            open_interest_trend="unknown",
            funding_bias="neutral",
            funding_persistence=0,
            price_oi_divergence=False,
            estimated_liquidations={"longs": 0, "shorts": 0}
        )
    
    # ========== DATABASE METHODS ==========
    async def _save_analysis(self, analysis: RegimeAnalysis):
        """Save analysis to database"""
        try:
            # Convert dataclasses to JSON
            btc_structure_json = json.dumps(asdict(analysis.btc_structure))
            btc_derivatives_json = json.dumps(asdict(analysis.btc_derivatives))
            
            # Convert altcoin rankings
            leaders_json = json.dumps([asdict(alt) for alt in analysis.altcoin_rankings.get("Leaders", [])])
            weak_json = json.dumps([asdict(alt) for alt in analysis.altcoin_rankings.get("Weak/Vulnerable", [])])
            
            # Convert other structures
            liquidity_json = json.dumps(asdict(analysis.cross_market_liquidity))
            incentives_json = json.dumps(asdict(analysis.market_maker_incentives))
            
            # Position management
            stop_loss_json = ""
            take_profit_json = ""
            
            if analysis.position_management:
                stop_loss_json = json.dumps(analysis.position_management.stop_loss)
                take_profit_json = json.dumps(analysis.position_management.take_profit)
            
            # Raw analysis
            raw_analysis = json.dumps({
                "analysis_id": analysis.analysis_id,
                "timestamp": analysis.timestamp,
                "confidence_score": analysis.confidence_score
            })
            
            await self.db.execute("""
                INSERT INTO regime_analysis (
                    analysis_id, timestamp, global_regime, regime_confidence, btc_role,
                    altcoin_leaders, altcoin_weak,
                    trade_decision, trade_symbol, trade_side,
                    next_move_prediction, confidence_score,
                    btc_structure, btc_derivatives, cross_market_liquidity, market_maker_incentives,
                    stop_loss_logic, take_profit_logic,
                    raw_analysis
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.analysis_id,
                analysis.timestamp,
                analysis.global_regime,
                analysis.regime_confidence,
                analysis.btc_role.primary_role,
                leaders_json,
                weak_json,
                analysis.trade_decision.decision,
                analysis.trade_decision.symbol,
                analysis.trade_decision.side,
                analysis.next_move_prediction,
                analysis.confidence_score,
                btc_structure_json,
                btc_derivatives_json,
                liquidity_json,
                incentives_json,
                stop_loss_json,
                take_profit_json,
                raw_analysis
            ))
            
            await self.db.commit()
            log.debug(f"✅ Analysis saved: {analysis.analysis_id[:8]}")
            
        except Exception as e:
            log.error(f"Error saving analysis: {e}")
    
    # ========== TELEGRAM METHODS ==========
    async def _send_telegram(self, message: str):
        """Send message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json=payload)
                
                if response.status_code == 400:
                    # Fallback to plain text
                    plain_message = message.replace('<b>', '').replace('</b>', '')
                    payload = {
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": plain_message,
                        "disable_web_page_preview": True
                    }
                    await client.post(url, json=payload)
                    
        except Exception as e:
            log.error(f"Telegram send error: {e}")
    
    async def _send_trade_alert(self, analysis: RegimeAnalysis):
        """Send trade alert to Telegram"""
        if analysis.trade_decision.decision == "⚫ No Trade":
            return
        
        decision = analysis.trade_decision
        
        # Format message
        side_emoji = "🟢" if decision.side == "LONG" else "🔴"
        clean_symbol = decision.symbol.replace('/', '') if decision.symbol else ""
        
        # Get position management details
        sl_price = ""
        tp1_price = ""
        
        if analysis.position_management:
            sl_price = f"{analysis.position_management.stop_loss['price']:.2f}"
            tp1_price = f"{analysis.position_management.take_profit['tp1']['price']:.2f}"
        
        message = f"""{side_emoji} <b>INSTITUTIONAL TRADE SIGNAL</b>

<b>📊 REGIME:</b> {analysis.global_regime}
<b>₿ BTC ROLE:</b> {analysis.btc_role.primary_role}
<b>📈 CONFIDENCE:</b> {analysis.confidence_score:.0%}

<b>🎯 TRADE:</b> {decision.side} {decision.symbol}
<b>🔢 ENTRY:</b> {decision.entry_price:.2f}
<b>🎪 TYPE:</b> {decision.entry_type.replace('_', ' ').title()}

<b>🛡️ STOP LOSS:</b> {sl_price}
<b>🎯 TAKE PROFIT 1:</b> {tp1_price}

<b>📈 NEXT MOVE:</b> {analysis.next_move_prediction}

<b>🔍 CONTEXT:</b>
• Regime Confidence: {analysis.regime_confidence:.0%}
• BTC Role Score: {analysis.btc_role.role_score:.0%}
• MM Incentive: {analysis.market_maker_incentives.optimal_direction.title()}

#{clean_symbol} #{decision.side} #RegimeTrading #Institutional"""
        
        await self._send_telegram(message)
    
    # ========== MAIN SCANNING LOOP ==========
    async def run_scanning_loop(self):
        """Main scanning loop"""
        log.info("🚀 Starting Institutional Regime Scanner...")
        
        while True:
            try:
                # Run complete analysis
                analysis = await self.analyze_market()
                
                # Wait for next scan
                await asyncio.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                log.info("🛑 Scanner stopped by user")
                break
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(30)  # Wait before retry
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
            
            if self.db:
                await self.db.close()
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# ================ MAIN EXECUTION ================
async def main():
    """Main execution"""
    scanner = InstitutionalRegimeScanner(exchange_name="okx")
    
    try:
        await scanner.initialize()
        await scanner.run_scanning_loop()
        
    except KeyboardInterrupt:
        log.info("🛑 Institutional Regime Scanner stopped")
        
    finally:
        await scanner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())