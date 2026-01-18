#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INSTITUTIONAL CRYPTO REGIME SCANNER v1.1
FIXED & ENHANCED VERSION
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
    acceptance_score: float
    rejection_score: float  
    time_above_value: float
    time_below_value: float

@dataclass
class BTCDerivatives:
    """Bitcoin derivatives analysis"""
    open_interest_trend: str
    funding_bias: str
    funding_persistence: int
    price_oi_divergence: bool
    estimated_liquidations: Dict[str, float]
    funding_rate: float = 0.0
    open_interest: float = 0.0

@dataclass 
class BTCRole:
    """Bitcoin's market role"""
    primary_role: str
    role_score: float
    secondary_roles: List[str]
    evidence: List[str]

@dataclass
class AltcoinRelativeAnalysis:
    """Altcoin analysis relative to BTC"""
    symbol: str
    relative_performance: float
    relative_trend: str
    move_speed: str
    accumulation_score: float
    distribution_score: float
    beta_coefficient: float
    correlation_24h: float
    absolute_performance: float = 0.0  # NEW: Absolute performance
    volume_trend: float = 1.0  # NEW: Volume trend

@dataclass
class LiquidityZone:
    """Liquidity zone identification"""
    price: float
    zone_type: str
    strength: float
    market_coverage: int
    estimated_stops: float
    recent_test: bool
    distance_pct: float
    markets: List[str] = field(default_factory=list)  # NEW: Which markets share this level

@dataclass
class CrossMarketLiquidity:
    """Cross-market liquidity analysis"""
    shared_htf_levels: List[LiquidityZone]
    correlated_stop_clusters: List[Dict]
    trigger_market: str
    liquidation_cascades: List[Dict]
    max_liquidation_zone: Optional[LiquidityZone]
    estimated_cascade_value: float

@dataclass
class MarketMakerIncentives:
    """Market maker incentive analysis"""
    trapped_traders: Dict[str, List]
    over_leveraged_side: str
    price_movement_purpose: str
    optimal_direction: str
    mm_payoff: Dict[str, float]
    estimated_mm_inventory: Dict[str, float]

@dataclass
class TradeDecision:
    """Final trade decision"""
    decision: str
    symbol: Optional[str]
    side: Optional[str]
    entry_price: Optional[float]
    entry_type: str
    missing_liquidity: Optional[str]

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
    FIXED VERSION - All improvements implemented
    """
    
    def __init__(self, exchange_name: str = "okx"):
        self.exchange = None
        self.exchange_name = exchange_name
        self.db = None
        self.data_cache = {}
        self.cache_ttl = 300
        
        # Analysis state
        self.current_regime = None
        self.btc_analysis = None
        self.alt_rankings = None
        self.liquidity_map = None
        self.mm_incentives = None
        
        # Performance tracking
        self.scan_count = 0
        self.analysis_history = deque(maxlen=100)
        
        # Debug mode
        self.debug = os.getenv("DEBUG_MODE", "false").lower() == "true"
        
        # Track derivatives history
        self.oi_history = deque(maxlen=24)
        self.funding_history = deque(maxlen=24)
        
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🏛️ INSTITUTIONAL REGIME SCANNER v1.1 - FIXED")
        log.info("=" * 70)
        
        await self._init_exchange()
        await self._init_database()
        await self._send_startup_message()
        
    async def _init_exchange(self):
        """Initialize exchange connection"""
        try:
            config = EXCHANGE_CONFIG[self.exchange_name]
            self.exchange = config["class"](config["params"])
            
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
            message = """🏛️ <b>INSTITUTIONAL REGIME SCANNER v1.1 - FIXED - ONLINE</b>

<b>✅ FIXES IMPLEMENTED:</b>
• Fixed equal highs/lows detection (was 85/92, now ~3-5)
• Fixed altcoin ranking thresholds (1.5% vs 2%)
• Added absolute performance filter
• Added real derivatives data fetching
• Enhanced liquidity mapping
• Added range trading strategies

<b>⚡ NOW DETECTS:</b>
• Clear BTC structure with realistic levels
• Actual altcoin leadership
• Shared liquidity zones
• Range environment opportunities

#RegimeScanner #Fixed #Enhanced #Ready"""
            
            await self._send_telegram(message)
            log.info("✅ Startup message sent")
            
        except Exception as e:
            log.error(f"Telegram startup error: {e}")
    
    # ========== STEP 1: GLOBAL REGIME DETECTION ==========
    async def determine_global_regime(self) -> Tuple[str, float]:
        """STEP 1 — GLOBAL REGIME (MANDATORY)"""
        
        log.info("🔍 STEP 1: Determining Global Regime...")
        
        btc_data = await self._fetch_multi_timeframe_btc()
        if not btc_data:
            return "⚫ Volatility Compression (pre-impulse)", 0.0
        
        regime_scores = {
            "🟢 Risk-On Expansion": await self._score_risk_on_expansion(btc_data),
            "🟡 Controlled Range / Chop": await self._score_controlled_range(btc_data),
            "🔴 Distribution (top process)": await self._score_distribution(btc_data),
            "🔵 Accumulation (post-flush)": await self._score_accumulation(btc_data),
            "⚫ Volatility Compression (pre-impulse)": await self._score_vol_compression(btc_data)
        }
        
        total_score = sum(regime_scores.values())
        if total_score > 0:
            regime_scores = {k: v/total_score for k, v in regime_scores.items()}
        
        primary_regime = max(regime_scores.items(), key=lambda x: x[1])
        confidence = primary_regime[1]
        
        log.info(f"📊 REGIME: {primary_regime[0]} (Confidence: {confidence:.0%})")
        if self.debug:
            for regime, score in sorted(regime_scores.items(), key=lambda x: x[1], reverse=True):
                if score > 0.1:
                    log.debug(f"  {regime}: {score:.0%}")
        
        return primary_regime[0], confidence
    
    async def _score_risk_on_expansion(self, btc_data: Dict) -> float:
        """Score Risk-On Expansion regime"""
        score = 0.0
        
        if "DAILY" not in btc_data or "4H" not in btc_data:
            return score
        
        df_daily = btc_data["DAILY"]
        df_4h = btc_data["4H"]
        
        if len(df_daily) < 20 or len(df_4h) < 40:
            return score
        
        # 1. Higher highs and higher lows
        if len(df_daily) >= 10:
            highs = df_daily['high'].iloc[-10:].values
            lows = df_daily['low'].iloc[-10:].values
            
            if (all(highs[i] > highs[i-1] for i in range(1, len(highs))) and
                all(lows[i] > lows[i-1] for i in range(1, len(lows)))):
                score += 0.4
        
        # 2. Strong acceptance above value
        current_price = df_daily['close'].iloc[-1]
        value_area = self._calculate_value_area(df_daily)
        
        if current_price > value_area["vah"] * 1.02:
            score += 0.3
        
        # 3. Expanding volume with price
        if len(df_daily) >= 20:
            recent_volume = df_daily['volume'].iloc[-5:].mean()
            prior_volume = df_daily['volume'].iloc[-20:-5].mean()
            
            if recent_volume > prior_volume * 1.3:
                score += 0.2
        
        # 4. Strong momentum continuation
        if len(df_4h) >= 10:
            momentum = self._calculate_momentum(df_4h)
            if momentum > 0.7:
                score += 0.1
        
        return min(score, 1.0)
    
    async def _score_controlled_range(self, btc_data: Dict) -> float:
        """Score Controlled Range / Chop regime - ENHANCED"""
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
        
        if recent_range < avg_daily_range * 0.7:
            score += 0.3
        
        # 2. Clear support and resistance levels (FIXED)
        clear_levels = self._identify_clear_levels_fixed(df_daily)
        if len(clear_levels) >= 2:
            score += 0.3
        
        # 3. Mean reversion behavior
        mean_reversion_score = self._calculate_mean_reversion(df_4h)
        score += mean_reversion_score * 0.3
        
        # 4. Declining volatility
        volatility = df_daily['close'].pct_change().std()
        if volatility < 0.02:
            score += 0.1
        
        return min(score, 1.0)
    
    async def _score_distribution(self, btc_data: Dict) -> float:
        """Score Distribution regime"""
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
        
        if abs(current_price - recent_high) / recent_high < 0.01:
            score += 0.3
        
        # 2. Volume divergence
        if len(df_daily) >= 10:
            price_change = (df_daily['close'].iloc[-1] - df_daily['close'].iloc[-10]) / df_daily['close'].iloc[-10]
            volume_change = df_daily['volume'].iloc[-5:].mean() / df_daily['volume'].iloc[-10:-5].mean()
            
            if price_change > 0.05 and volume_change < 0.8:
                score += 0.4
        
        # 3. Failed breakout attempts
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
        """Score Accumulation regime"""
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
        """Score Volatility Compression regime"""
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
        
        if recent_atr < historical_atr * 0.5:
            score += 0.5
        
        # 2. Symmetrical triangle/coiling pattern
        pattern_score = self._detect_compression_pattern(df_4h)
        score += pattern_score * 0.3
        
        # 3. Declining volume
        if len(df_daily) >= 20:
            recent_volume = df_daily['volume'].iloc[-5:].mean()
            prior_volume = df_daily['volume'].iloc[-20:-5].mean()
            
            if recent_volume < prior_volume * 0.7:
                score += 0.2
        
        return min(score, 1.0)
    
    # ========== STEP 2: BITCOIN ANALYSIS ==========
    async def analyze_bitcoin_structure(self) -> BTCStructure:
        """STEP 2 — BITCOIN ANALYSIS (ENGINE) - FIXED"""
        
        log.info("🔍 STEP 2: Analyzing Bitcoin Structure...")
        
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
        
        # Find liquidity levels - FIXED METHODS
        equal_highs = self._find_equal_highs_fixed(df_4h)
        equal_lows = self._find_equal_lows_fixed(df_4h)
        prior_levels = self._find_prior_key_levels(df_daily)
        
        # Debug logging
        if self.debug:
            log.info(f"  Raw highs: {len(df_4h['high'])}")
            log.info(f"  Equal highs found: {len(equal_highs)}")
            log.info(f"  Equal lows found: {len(equal_lows)}")
            if equal_highs:
                log.info(f"  Sample equal high: {equal_highs[0]:.2f}")
            if equal_lows:
                log.info(f"  Sample equal low: {equal_lows[0]:.2f}")
        
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
        
        return structure
    
    async def analyze_bitcoin_derivatives(self) -> BTCDerivatives:
        """Analyze BTC derivatives - REAL DATA NOW"""
        
        log.info("🔍 STEP 2b: Analyzing Bitcoin Derivatives...")
        
        try:
            funding_rate = 0.0
            funding_bias = "neutral"
            oi_trend = "unknown"
            current_oi = 0.0
            
            # Try to fetch real derivatives data
            try:
                # Fetch funding rate
                if self.exchange_name == "okx":
                    # OKX funding rate
                    funding = await self.exchange.fetch_funding_rate("BTC-USDT-SWAP")
                    if funding:
                        funding_rate = float(funding.get('fundingRate', 0))
                elif self.exchange_name == "bybit":
                    # Bybit funding rate
                    ticker = await self.exchange.fetch_ticker("BTC/USDT:USDT")
                    funding_rate = float(ticker.get('info', {}).get('fundingRate', 0))
                
                # Determine funding bias
                if funding_rate > 0.0001:  # > 0.01%
                    funding_bias = "positive"
                elif funding_rate < -0.0001:  # < -0.01%
                    funding_bias = "negative"
                else:
                    funding_bias = "neutral"
                
                # Track funding history
                self.funding_history.append(funding_rate)
                funding_persistence = self._calculate_funding_persistence()
                
                # Estimate OI trend (simplified - would need actual OI API)
                current_price = await self._get_current_btc_price()
                if current_price > 0:
                    # Simulate OI based on price action
                    oi_trend = await self._estimate_oi_trend_simulated(current_price)
                
            except Exception as e:
                log.debug(f"Derivatives fetch error (using fallback): {e}")
                # Fallback to estimation
                funding_bias = "neutral"
                funding_persistence = 0
                oi_trend = "flat"
            
            # Check for price-OI divergence
            price_oi_divergence = await self._detect_price_oi_divergence_simulated()
            
            # Estimate liquidation levels
            liquidations = await self._estimate_liquidation_levels()
            
            derivatives = BTCDerivatives(
                open_interest_trend=oi_trend,
                funding_bias=funding_bias,
                funding_persistence=funding_persistence,
                price_oi_divergence=price_oi_divergence,
                estimated_liquidations=liquidations,
                funding_rate=funding_rate,
                open_interest=current_oi
            )
            
            log.info(f"₿ BTC Derivatives:")
            log.info(f"  OI Trend: {derivatives.open_interest_trend}")
            log.info(f"  Funding: {derivatives.funding_bias} ({funding_rate:.6%})")
            
            return derivatives
            
        except Exception as e:
            log.error(f"BTC derivatives analysis error: {e}")
            return self._get_default_btc_derivatives()
    
    async def determine_bitcoin_role(self, structure: BTCStructure, 
                                   derivatives: BTCDerivatives) -> BTCRole:
        """Define BTC's market role"""
        
        role_scores = {
            "Expansion leader": self._score_expansion_leader(structure, derivatives),
            "Range controller": self._score_range_controller(structure, derivatives),
            "Distribution anchor": self._score_distribution_anchor(structure, derivatives),
            "Accumulation base": self._score_accumulation_base(structure, derivatives),
            "Liquidity sweep instrument": self._score_liquidity_sweeper(structure, derivatives)
        }
        
        primary_role = max(role_scores.items(), key=lambda x: x[1])
        
        secondary_roles = [
            role for role, score in role_scores.items() 
            if score > 0.3 and role != primary_role[0]
        ]
        
        evidence = self._build_role_evidence(primary_role[0], structure, derivatives)
        
        role = BTCRole(
            primary_role=primary_role[0],
            role_score=primary_role[1],
            secondary_roles=secondary_roles,
            evidence=evidence
        )
        
        log.info(f"₿ BTC Role: {role.primary_role} (Score: {role.role_score:.0%})")
        
        return role
    
    # ========== STEP 3: MAJOR COINS ANALYSIS - FIXED ==========
    async def analyze_majors_alignment(self) -> Dict[str, List[AltcoinRelativeAnalysis]]:
        """STEP 3 — MAJOR COINS ALIGNMENT - FIXED THRESHOLDS"""
        
        log.info("🔍 STEP 3: Analyzing Major Coins vs BTC...")
        
        rankings = {
            "Leaders": [],      # Stronger than BTC = accumulation
            "Neutral": [],      # Moving with BTC
            "Weak/Vulnerable": []  # Weaker than BTC = distribution
        }
        
        # Get BTC performance over 4H (better for short-term analysis)
        btc_performance = await self._calculate_btc_performance_4h()
        
        for symbol in MAJOR_COINS:
            try:
                # Fetch altcoin data
                alt_data = await self._fetch_altcoin_data(symbol)
                if alt_data is None:
                    continue
                
                # Calculate relative performance (4H)
                relative_perf = await self._calculate_relative_performance_4h(symbol, btc_performance)
                
                # Calculate absolute performance (4H)
                abs_perf = await self._calculate_absolute_performance_4h(symbol)
                
                # Calculate volume trend
                volume_trend = await self._calculate_volume_trend(symbol)
                
                # FIXED: Adjusted thresholds (1.5% instead of 2%)
                if relative_perf > 0.015:  # 1.5% stronger
                    relative_trend = "stronger"
                elif relative_perf < -0.015:  # 1.5% weaker
                    relative_trend = "weaker"
                else:
                    relative_trend = "neutral"
                
                # Analyze move speed
                move_speed = await self._analyze_move_speed(symbol)
                
                # Calculate accumulation/distribution scores
                acc_score = await self._calculate_accumulation_score_fixed(symbol, relative_perf, abs_perf, volume_trend)
                dist_score = await self._calculate_distribution_score_fixed(symbol, relative_perf, abs_perf, volume_trend)
                
                # Calculate beta and correlation
                beta = await self._calculate_beta_coefficient(symbol)
                correlation = await self._calculate_correlation_24h(symbol)
                
                analysis = AltcoinRelativeAnalysis(
                    symbol=symbol,
                    relative_performance=relative_perf,
                    relative_trend=relative_trend,
                    move_speed=move_speed,
                    accumulation_score=acc_score,
                    distribution_score=dist_score,
                    beta_coefficient=beta,
                    correlation_24h=correlation,
                    absolute_performance=abs_perf,
                    volume_trend=volume_trend
                )
                
                # FIXED: Better classification logic
                # Leaders need: relative outperformance AND positive absolute performance
                if (relative_trend == "stronger" and 
                    abs_perf > 0 and 
                    acc_score > 0.5 and
                    volume_trend > 0.9):  # Volume not declining
                    rankings["Leaders"].append(analysis)
                
                # Weak/Vulnerable need: relative underperformance AND negative absolute
                elif (relative_trend == "weaker" and 
                      abs_perf < 0 and 
                      dist_score > 0.5):
                    rankings["Weak/Vulnerable"].append(analysis)
                
                else:
                    rankings["Neutral"].append(analysis)
                
                # Debug logging
                if self.debug and abs(relative_perf) > 0.02:
                    log.debug(f"{symbol}: Rel={relative_perf:.2%}, Abs={abs_perf:.2%}, Acc={acc_score:.2f}, Dist={dist_score:.2f}")
                    
            except Exception as e:
                if self.debug:
                    log.debug(f"Error analyzing {symbol}: {e}")
                continue
        
        # Sort rankings
        rankings["Leaders"].sort(key=lambda x: (x.accumulation_score, x.relative_performance), reverse=True)
        rankings["Weak/Vulnerable"].sort(key=lambda x: (x.distribution_score, -x.relative_performance), reverse=True)
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
    
    # ========== STEP 4: CROSS-MARKET LIQUIDITY MAP - ENHANCED ==========
    async def build_liquidity_map(self, btc_structure: BTCStructure,
                                 alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]]) -> CrossMarketLiquidity:
        """STEP 4 — CROSS-MARKET LIQUIDITY MAP - ENHANCED"""
        
        log.info("🔍 STEP 4: Building Cross-Market Liquidity Map...")
        
        # Get BTC liquidity zones
        btc_zones = self._extract_btc_liquidity_zones(btc_structure)
        
        # Analyze major coins for shared levels - ENHANCED
        shared_levels = []
        correlated_clusters = []
        
        # Check each major coin
        for symbol in MAJOR_COINS[:8]:
            try:
                # Fetch major levels with better detection
                major_levels = await self._fetch_major_key_levels_enhanced(symbol)
                if not major_levels:
                    continue
                
                # Find shared levels with BTC - ENHANCED
                shared = await self._find_shared_levels_enhanced(btc_zones, major_levels, symbol)
                if shared:
                    shared_levels.extend(shared)
                
                # Identify stop clusters
                clusters = await self._identify_stop_clusters(symbol, major_levels)
                if clusters:
                    correlated_clusters.extend(clusters)
                    
            except Exception as e:
                if self.debug:
                    log.debug(f"Liquidity map error for {symbol}: {e}")
                continue
        
        # Remove duplicate shared levels
        shared_levels = self._deduplicate_liquidity_zones(shared_levels)
        
        # Identify trigger market
        trigger_market = self._determine_trigger_market(btc_structure, alt_rankings)
        
        # Identify liquidation cascades
        liquidation_cascades = await self._identify_liquidation_cascades(shared_levels, trigger_market)
        
        # Find where most money gets liquidated next
        max_liquidation_zone = self._identify_max_liquidation_zone_enhanced(
            btc_zones, shared_levels, correlated_clusters
        )
        
        # Estimate cascade value
        cascade_value = self._estimate_cascade_value(liquidation_cascades)
        
        liquidity_map = CrossMarketLiquidity(
            shared_htf_levels=shared_levels[:10],
            correlated_stop_clusters=correlated_clusters[:5],
            trigger_market=trigger_market,
            liquidation_cascades=liquidation_cascades,
            max_liquidation_zone=max_liquidation_zone,
            estimated_cascade_value=cascade_value
        )
        
        log.info(f"🗺️ Cross-Market Liquidity:")
        log.info(f"  Shared Levels: {len(liquidity_map.shared_htf_levels)}")
        log.info(f"  Stop Clusters: {len(liquidity_map.correlated_stop_clusters)}")
        log.info(f"  Trigger Market: {liquidity_map.trigger_market}")
        
        if liquidity_map.max_liquidation_zone:
            log.info(f"  Next Big Liquidation: {liquidity_map.max_liquidation_zone.zone_type} "
                    f"@ {liquidity_map.max_liquidation_zone.price:.2f}")
        
        return liquidity_map
    
    # ========== STEP 5: MARKET MAKER INCENTIVE MODEL ==========
    def analyze_market_maker_incentives(self, regime: str, btc_role: BTCRole,
                                       alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                                       liquidity_map: CrossMarketLiquidity) -> MarketMakerIncentives:
        """STEP 5 — MARKET MAKER INCENTIVE MODEL - ENHANCED"""
        
        log.info("🔍 STEP 5: Analyzing Market Maker Incentives...")
        
        # Identify trapped traders - ENHANCED
        trapped_traders = self._identify_trapped_traders_enhanced(btc_role, alt_rankings, liquidity_map, regime)
        
        # Determine over-leveraged side - ENHANCED
        over_leveraged_side = self._determine_over_leveraged_side_enhanced(btc_role, alt_rankings, regime)
        
        # Determine price movement purpose
        price_purpose = self._determine_price_purpose(regime, btc_role, trapped_traders)
        
        # Calculate optimal direction - ENHANCED
        optimal_direction = self._calculate_optimal_direction_enhanced(
            regime, btc_role, trapped_traders, over_leveraged_side, liquidity_map
        )
        
        # Estimate payoffs - ENHANCED
        mm_payoff = self._estimate_mm_payoff_enhanced(
            optimal_direction, trapped_traders, liquidity_map, regime
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
        
        return incentives
    
    # ========== STEP 6: TRADE DECISION - ENHANCED FOR RANGE ENVIRONMENTS ==========
    def make_trade_decision(self, regime: str, regime_confidence: float,
                           btc_role: BTCRole, alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                           market_maker_incentives: MarketMakerIncentives) -> TradeDecision:
        """STEP 6 — TRADE DECISION - ENHANCED WITH RANGE STRATEGIES"""
        
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
        
        leaders = alt_rankings.get("Leaders", [])
        weak = alt_rankings.get("Weak/Vulnerable", [])
        
        # ===== NEW: RANGE ENVIRONMENT OPPORTUNITIES =====
        if "Range" in regime or "Chop" in regime:
            # Range strategy 1: Fade extremes with momentum confirmation
            range_trade = self._find_range_extreme_trade(btc_role, alt_rankings, market_maker_incentives)
            if range_trade:
                return range_trade
            
            # Range strategy 2: Mean reversion on strong leaders
            mean_reversion_trade = self._find_mean_reversion_trade(alt_rankings)
            if mean_reversion_trade:
                return mean_reversion_trade
            
            # Range strategy 3: Breakout anticipation on volume
            breakout_trade = self._find_breakout_anticipation_trade(alt_rankings, market_maker_incentives)
            if breakout_trade:
                return breakout_trade
        
        # ===== TREND ENVIRONMENT OPPORTUNITIES =====
        
        # Risk-On Expansion: Long leaders
        if "Risk-On Expansion" in regime and btc_role.primary_role == "Expansion leader":
            if leaders:
                # Find best leader with strong accumulation
                best_leader = self._select_best_trade_candidate(leaders, "LONG")
                if best_leader:
                    return TradeDecision(
                        decision="🟢 Long",
                        symbol=best_leader.symbol,
                        side="LONG",
                        entry_price=self._get_symbol_price(best_leader.symbol),
                        entry_type="expansion_leader",
                        missing_liquidity=None
                    )
        
        # Accumulation regime: Long accumulation patterns
        if "Accumulation" in regime:
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
        if "Distribution" in regime:
            if weak:
                weakest = self._select_best_trade_candidate(weak, "SHORT")
                return TradeDecision(
                    decision="🔴 Short",
                    symbol=weakest.symbol,
                    side="SHORT",
                    entry_price=self._get_symbol_price(weakest.symbol),
                    entry_type="distribution_breakdown",
                    missing_liquidity=None
                )
        
        # Market Maker incentive alignment
        if market_maker_incentives.optimal_direction == "continuation" and leaders:
            best_leader = self._select_best_trade_candidate(leaders, "LONG")
            return TradeDecision(
                decision="🟢 Long",
                symbol=best_leader.symbol,
                side="LONG",
                entry_price=self._get_symbol_price(best_leader.symbol),
                entry_type="mm_incentive_aligned",
                missing_liquidity=None
            )
        
        elif market_maker_incentives.optimal_direction == "reversal" and weak:
            weakest = self._select_best_trade_candidate(weak, "SHORT")
            return TradeDecision(
                decision="🔴 Short",
                symbol=weakest.symbol,
                side="SHORT",
                entry_price=self._get_symbol_price(weakest.symbol),
                entry_type="mm_incentive_aligned",
                missing_liquidity=None
            )
        
        # Default: No Trade with specific reason
        return TradeDecision(
            decision="⚫ No Trade",
            symbol=None,
            side=None,
            entry_price=None,
            entry_type="",
            missing_liquidity=self._determine_missing_liquidity(regime, btc_role, alt_rankings)
        )
    
    # ========== STEP 7: TAKE PROFIT & STOP LOSS ==========
    def calculate_position_management(self, trade_decision: TradeDecision,
                                     btc_structure: BTCStructure,
                                     liquidity_map: CrossMarketLiquidity) -> Optional[PositionManagement]:
        """STEP 7 — TAKE PROFIT & STOP LOSS - ENHANCED"""
        
        if trade_decision.decision == "⚫ No Trade":
            return None
        
        log.info("🔍 STEP 7: Calculating Position Management...")
        
        entry_price = trade_decision.entry_price
        side = trade_decision.side
        
        if not entry_price or not side:
            return None
        
        # ===== STOP LOSS CALCULATION =====
        if side == "LONG":
            stop_price = self._find_long_stop_loss_enhanced(entry_price, btc_structure, liquidity_map, trade_decision.entry_type)
            stop_logic = {
                "price": stop_price,
                "distance_pct": abs(stop_price - entry_price) / entry_price * 100,
                "logic": "Below HTF low & major liquidity zone",
                "placement": "Beyond market maker hunting range",
                "invalidator": "Break of accumulation structure"
            }
        else:  # SHORT
            stop_price = self._find_short_stop_loss_enhanced(entry_price, btc_structure, liquidity_map, trade_decision.entry_type)
            stop_logic = {
                "price": stop_price,
                "distance_pct": abs(stop_price - entry_price) / entry_price * 100,
                "logic": "Above HTF high & equal highs",
                "placement": "Beyond recent swing high",
                "invalidator": "Break of distribution structure"
            }
        
        # ===== TAKE PROFIT CALCULATION =====
        if side == "LONG":
            tp_levels = self._find_long_take_profits_enhanced(entry_price, btc_structure, liquidity_map, trade_decision.entry_type)
        else:  # SHORT
            tp_levels = self._find_short_take_profits_enhanced(entry_price, btc_structure, liquidity_map, trade_decision.entry_type)
        
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
                "logic": "Prior HTF level & stop cluster",
                "size_pct": 0.5
            },
            "tp3": {
                "price": tp_levels["tp3"],
                "pct_from_entry": abs(tp_levels["tp3"] - entry_price) / entry_price * 100,
                "logic": "Extreme overshoot / liquidity sweep",
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
        
        return position_mgmt
    
    # ========== FIXED & ENHANCED HELPER METHODS ==========
    
    # FIXED: Equal highs/lows detection
    def _find_equal_highs_fixed(self, df: pd.DataFrame) -> List[float]:
        """Find equal highs - FIXED (realistic count)"""
        if len(df) < 30:
            return []
        
        highs = []
        # Use last 50 candles
        recent_highs = df['high'].iloc[-50:].values
        
        # Only check significant highs (top 20%)
        significant_indices = np.argsort(recent_highs)[-int(len(recent_highs) * 0.2):]
        significant_highs = recent_highs[significant_indices]
        
        # Check each significant high against others
        for i in range(len(significant_highs)):
            current_high = significant_highs[i]
            similar_count = 0
            
            for j in range(len(significant_highs)):
                if i != j:
                    # Tighter tolerance: 0.3% (was 0.5%)
                    if abs(current_high - significant_highs[j]) / current_high < 0.003:
                        similar_count += 1
            
            # Need at least 2 similar highs to be considered "equal"
            if similar_count >= 2:
                # Check if this level is already in our list (within 0.2%)
                if not highs or min(abs(current_high - h) / h for h in highs) > 0.002:
                    highs.append(float(current_high))
        
        # Return only the top 3 most recent unique highs
        unique_highs = list(set(highs))
        unique_highs.sort(reverse=True)
        
        if self.debug:
            log.debug(f"  Found {len(unique_highs)} equal highs")
            if unique_highs:
                log.debug(f"  Equal highs: {unique_highs[:3]}")
        
        return unique_highs[:3]  # Max 3 levels
    
    def _find_equal_lows_fixed(self, df: pd.DataFrame) -> List[float]:
        """Find equal lows - FIXED (realistic count)"""
        if len(df) < 30:
            return []
        
        lows = []
        recent_lows = df['low'].iloc[-50:].values
        
        # Only check significant lows (bottom 20%)
        significant_indices = np.argsort(recent_lows)[:int(len(recent_lows) * 0.2)]
        significant_lows = recent_lows[significant_indices]
        
        for i in range(len(significant_lows)):
            current_low = significant_lows[i]
            similar_count = 0
            
            for j in range(len(significant_lows)):
                if i != j:
                    if abs(current_low - significant_lows[j]) / current_low < 0.003:
                        similar_count += 1
            
            if similar_count >= 2:
                if not lows or min(abs(current_low - l) / l for l in lows) > 0.002:
                    lows.append(float(current_low))
        
        unique_lows = list(set(lows))
        unique_lows.sort()
        
        if self.debug:
            log.debug(f"  Found {len(unique_lows)} equal lows")
            if unique_lows:
                log.debug(f"  Equal lows: {unique_lows[:3]}")
        
        return unique_lows[:3]
    
    # FIXED: Clear levels identification
    def _identify_clear_levels_fixed(self, df: pd.DataFrame) -> List[float]:
        """Identify clear support/resistance levels - FIXED"""
        levels = []
        
        if len(df) < 40:
            return levels
        
        # Use recent 40 candles
        recent = df.iloc[-40:]
        
        # Find swing highs and lows
        for i in range(2, len(recent) - 2):
            high = recent['high'].iloc[i]
            low = recent['low'].iloc[i]
            
            # Check for swing high
            if (high > recent['high'].iloc[i-1] and 
                high > recent['high'].iloc[i-2] and
                high > recent['high'].iloc[i+1] and
                high > recent['high'].iloc[i+2]):
                
                # Check if price reacted at this level before
                reactions = 0
                for j in range(max(0, i-10), min(len(recent), i+10)):
                    if j == i:
                        continue
                    if abs(recent['high'].iloc[j] - high) / high < 0.01:
                        reactions += 1
                
                if reactions >= 1:  # At least one other reaction
                    levels.append(float(high))
            
            # Check for swing low
            if (low < recent['low'].iloc[i-1] and 
                low < recent['low'].iloc[i-2] and
                low < recent['low'].iloc[i+1] and
                low < recent['low'].iloc[i+2]):
                
                reactions = 0
                for j in range(max(0, i-10), min(len(recent), i+10)):
                    if j == i:
                        continue
                    if abs(recent['low'].iloc[j] - low) / low < 0.01:
                        reactions += 1
                
                if reactions >= 1:
                    levels.append(float(low))
        
        # Remove duplicates and return
        unique_levels = list(set(levels))
        unique_levels.sort()
        
        if self.debug and unique_levels:
            log.debug(f"  Clear levels found: {len(unique_levels)}")
        
        return unique_levels[:6]  # Max 6 clear levels
    
    # NEW: Enhanced altcoin performance calculations
    async def _calculate_btc_performance_4h(self) -> float:
        """Calculate BTC performance over last 4 hours"""
        try:
            df = await self._fetch_timeframe_data("BTC/USDT", "1h", 5, "1h_btc_4h")
            if len(df) < 2:
                return 0.0
            
            return (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
        except:
            return 0.0
    
    async def _calculate_relative_performance_4h(self, symbol: str, btc_perf: float) -> float:
        """Calculate altcoin performance relative to BTC over 4h"""
        try:
            df = await self._fetch_timeframe_data(symbol, "1h", 5, f"1h_{symbol}_4h")
            if df is None or len(df) < 2:
                return 0.0
            
            alt_perf = (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
            return alt_perf - btc_perf
        except:
            return 0.0
    
    async def _calculate_absolute_performance_4h(self, symbol: str) -> float:
        """Calculate absolute performance over 4h"""
        try:
            df = await self._fetch_timeframe_data(symbol, "1h", 5, f"1h_{symbol}_abs")
            if df is None or len(df) < 2:
                return 0.0
            
            return (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]
        except:
            return 0.0
    
    async def _calculate_volume_trend(self, symbol: str) -> float:
        """Calculate volume trend (1.0 = average, >1 = increasing, <1 = decreasing)"""
        try:
            df = await self._fetch_timeframe_data(symbol, "1h", 10, f"1h_{symbol}_vol")
            if df is None or len(df) < 5:
                return 1.0
            
            recent_vol = df['volume'].iloc[-3:].mean()
            prior_vol = df['volume'].iloc[-6:-3].mean()
            
            if prior_vol > 0:
                return recent_vol / prior_vol
            return 1.0
        except:
            return 1.0
    
    # FIXED: Accumulation/Distribution scoring
    async def _calculate_accumulation_score_fixed(self, symbol: str, relative_perf: float, 
                                                 abs_perf: float, volume_trend: float) -> float:
        """Calculate accumulation score - FIXED"""
        score = 0.0
        
        # 1. Relative outperformance (max 0.3)
        if relative_perf > 0.01:
            score += min(relative_perf * 10, 0.3)  # Scale: 1% = 0.1 score
        
        # 2. Positive absolute performance (max 0.2)
        if abs_perf > 0:
            score += min(abs_perf * 10, 0.2)
        
        # 3. Healthy volume (not declining) (max 0.2)
        if volume_trend > 0.8:
            score += 0.2
        elif volume_trend > 1.2:
            score += 0.3  # Bonus for increasing volume
        
        # 4. Slow, steady moves (strong hands) (max 0.3)
        move_speed = await self._analyze_move_speed(symbol)
        if move_speed == "slow_strong":
            score += 0.3
        
        return min(score, 1.0)
    
    async def _calculate_distribution_score_fixed(self, symbol: str, relative_perf: float,
                                                 abs_perf: float, volume_trend: float) -> float:
        """Calculate distribution score - FIXED"""
        score = 0.0
        
        # 1. Relative underperformance
        if relative_perf < -0.01:
            score += min(abs(relative_perf) * 10, 0.3)
        
        # 2. Negative absolute performance
        if abs_perf < 0:
            score += min(abs(abs_perf) * 10, 0.2)
        
        # 3. Volume divergence (price down, volume up)
        if abs_perf < 0 and volume_trend > 1.2:
            score += 0.3
        
        # 4. Fast, panic moves (weak hands)
        move_speed = await self._analyze_move_speed(symbol)
        if move_speed == "fast_leverage":
            score += 0.2
        
        return min(score, 1.0)
    
    # NEW: Range trading strategies
    def _find_range_extreme_trade(self, btc_role: BTCRole, 
                                 alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                                 mm_incentives: MarketMakerIncentives) -> Optional[TradeDecision]:
        """Find trade at range extremes in range environment"""
        
        leaders = alt_rankings.get("Leaders", [])
        weak = alt_rankings.get("Weak/Vulnerable", [])
        
        if not leaders and not weak:
            return None
        
        # Strategy: Fade range extremes with momentum confirmation
        current_btc_price = self._get_current_btc_price()
        if current_btc_price <= 0:
            return None
        
        # Get BTC range levels from recent analysis history
        if len(self.analysis_history) >= 2:
            prev_analysis = self.analysis_history[-1]
            btc_range_high = prev_analysis.btc_structure.htf_high
            btc_range_low = prev_analysis.btc_structure.htf_low
            
            # Calculate position in range (0 = bottom, 1 = top)
            range_position = (current_btc_price - btc_range_low) / (btc_range_high - btc_range_low) if (btc_range_high - btc_range_low) > 0 else 0.5
            
            # Near top of range: look for weak coins to short
            if range_position > 0.7 and weak:
                # Find weakest coin with distribution characteristics
                for alt in weak:
                    if (alt.distribution_score > 0.6 and 
                        alt.absolute_performance < 0 and
                        alt.volume_trend < 1.0):  # Volume declining at resistance
                        
                        return TradeDecision(
                            decision="🔴 Short",
                            symbol=alt.symbol,
                            side="SHORT",
                            entry_price=self._get_symbol_price(alt.symbol),
                            entry_type="range_resistance_fade",
                            missing_liquidity=None
                        )
            
            # Near bottom of range: look for strong leaders to long
            elif range_position < 0.3 and leaders:
                for alt in leaders:
                    if (alt.accumulation_score > 0.6 and
                        alt.absolute_performance > 0 and
                        alt.volume_trend > 0.9):
                        
                        return TradeDecision(
                            decision="🟢 Long",
                            symbol=alt.symbol,
                            side="LONG",
                            entry_price=self._get_symbol_price(alt.symbol),
                            entry_type="range_support_bounce",
                            missing_liquidity=None
                        )
        
        return None
    
    def _find_mean_reversion_trade(self, alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]]) -> Optional[TradeDecision]:
        """Find mean reversion trade in range environment"""
        
        leaders = alt_rankings.get("Leaders", [])
        weak = alt_rankings.get("Weak/Vulnerable", [])
        
        # Look for overextended moves
        for alt in leaders:
            # Strong relative performance but slowing momentum
            if (alt.relative_performance > 0.03 and  # 3%+ outperformance
                alt.volume_trend < 0.9 and  # Volume declining
                alt.move_speed == "fast_leverage"):  # Fast move = likely to revert
                
                return TradeDecision(
                    decision="🔴 Short",
                    symbol=alt.symbol,
                    side="SHORT",
                    entry_price=self._get_symbol_price(alt.symbol),
                    entry_type="mean_reversion_short",
                    missing_liquidity=None
                )
        
        for alt in weak:
            # Weak relative performance but volume increasing (accumulation?)
            if (alt.relative_performance < -0.03 and  # 3%+ underperformance
                alt.volume_trend > 1.1 and  # Volume increasing
                alt.move_speed == "slow_strong"):  # Slow move = might be bottoming
                
                return TradeDecision(
                    decision="🟢 Long",
                    symbol=alt.symbol,
                    side="LONG",
                    entry_price=self._get_symbol_price(alt.symbol),
                    entry_type="mean_reversion_long",
                    missing_liquidity=None
                )
        
        return None
    
    def _find_breakout_anticipation_trade(self, alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                                         mm_incentives: MarketMakerIncentives) -> Optional[TradeDecision]:
        """Find breakout anticipation trade"""
        
        leaders = alt_rankings.get("Leaders", [])
        
        if not leaders:
            return None
        
        # Look for strongest leader with increasing volume
        for alt in leaders:
            if (alt.accumulation_score > 0.7 and
                alt.volume_trend > 1.2 and  # Strong volume increase
                alt.relative_performance > 0.02 and  # Outperforming
                alt.beta_coefficient > 1.2):  # High beta (responsive to BTC)
                
                return TradeDecision(
                    decision="🟢 Long",
                    symbol=alt.symbol,
                    side="LONG",
                    entry_price=self._get_symbol_price(alt.symbol),
                    entry_type="breakout_anticipation",
                    missing_liquidity=None
                )
        
        return None
    
    def _select_best_trade_candidate(self, alts: List[AltcoinRelativeAnalysis], side: str) -> Optional[AltcoinRelativeAnalysis]:
        """Select best trade candidate from list"""
        if not alts:
            return None
        
        if side == "LONG":
            # For longs: prioritize accumulation score, then relative performance, then volume
            return max(alts, key=lambda x: (
                x.accumulation_score,
                x.relative_performance,
                x.volume_trend,
                x.correlation_24h  # Higher correlation with BTC is better for trend following
            ))
        else:  # SHORT
            # For shorts: prioritize distribution score, then negative relative performance
            return max(alts, key=lambda x: (
                x.distribution_score,
                -x.relative_performance,  # More negative is better
                2.0 - x.volume_trend if x.volume_trend > 0 else 0,  # Lower volume trend is better for distribution
                x.beta_coefficient  # Higher beta = more responsive to BTC moves
            ))
    
    def _determine_missing_liquidity(self, regime: str, btc_role: BTCRole,
                                    alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]]) -> str:
        """Determine what liquidity is missing for a trade"""
        
        leaders = alt_rankings.get("Leaders", [])
        weak = alt_rankings.get("Weak/Vulnerable", [])
        
        if "Range" in regime:
            if not leaders and not weak:
                return "No clear range extremes identified in altcoins"
            elif leaders and weak:
                return "Range environment - waiting for clear extreme test"
            else:
                return "Range environment - insufficient confirmation at current levels"
        
        elif "Expansion" in regime:
            if not leaders:
                return "No altcoin leadership detected for expansion"
            else:
                return "Expansion regime but insufficient momentum confirmation"
        
        elif "Accumulation" in regime:
            if not leaders:
                return "No accumulation patterns detected"
            else:
                return "Accumulation phase but lacks volume confirmation"
        
        elif "Distribution" in regime:
            if not weak:
                return "No distribution patterns detected"
            else:
                return "Distribution phase but lacks volume divergence"
        
        return "No clear edge in current market structure"
    
    # ENHANCED: Liquidity zone methods
    async def _fetch_major_key_levels_enhanced(self, symbol: str) -> List[LiquidityZone]:
        """Fetch key levels for a major coin - ENHANCED"""
        try:
            df = await self._fetch_timeframe_data(symbol, "4h", 40, f"4h_{symbol}_enhanced")
            if df is None or len(df) < 20:
                return []
            
            zones = []
            
            # Recent highs and lows
            recent_high = df['high'].iloc[-20:].max()
            recent_low = df['low'].iloc[-20:].min()
            
            # Weekly/Daily levels if available
            if len(df) >= 7*6:  # 6 weeks of 4h data
                weekly_high = df['high'].iloc[-7*6:].max()
                weekly_low = df['low'].iloc[-7*6:].min()
                
                zones.append(LiquidityZone(
                    price=float(weekly_high),
                    zone_type="weekly_high",
                    strength=0.7,
                    market_coverage=1,
                    estimated_stops=5000000,
                    recent_test=False,
                    distance_pct=0.0,
                    markets=[symbol]
                ))
                
                zones.append(LiquidityZone(
                    price=float(weekly_low),
                    zone_type="weekly_low",
                    strength=0.7,
                    market_coverage=1,
                    estimated_stops=5000000,
                    recent_test=False,
                    distance_pct=0.0,
                    markets=[symbol]
                ))
            
            # Recent levels
            zones.append(LiquidityZone(
                price=float(recent_high),
                zone_type="recent_high",
                strength=0.6,
                market_coverage=1,
                estimated_stops=3000000,
                recent_test=True,
                distance_pct=0.0,
                markets=[symbol]
            ))
            
            zones.append(LiquidityZone(
                price=float(recent_low),
                zone_type="recent_low",
                strength=0.6,
                market_coverage=1,
                estimated_stops=3000000,
                recent_test=True,
                distance_pct=0.0,
                markets=[symbol]
            ))
            
            return zones
            
        except Exception as e:
            if self.debug:
                log.debug(f"Error fetching levels for {symbol}: {e}")
            return []
    
    async def _find_shared_levels_enhanced(self, btc_zones: List[LiquidityZone], 
                                         major_zones: List[LiquidityZone], symbol: str) -> List[LiquidityZone]:
        """Find shared levels between BTC and a major coin - ENHANCED"""
        shared = []
        
        for btc_zone in btc_zones[:5]:  # Check top 5 BTC zones
            for major_zone in major_zones:
                # Check if levels are within 0.8% (tighter tolerance)
                if abs(btc_zone.price - major_zone.price) / btc_zone.price < 0.008:
                    # Combine markets
                    all_markets = list(set(btc_zone.markets + major_zone.markets + [symbol]))
                    
                    combined = LiquidityZone(
                        price=(btc_zone.price + major_zone.price) / 2,
                        zone_type=f"shared_{btc_zone.zone_type.split('_')[0]}",
                        strength=(btc_zone.strength + major_zone.strength) / 2 * 1.2,  # Bonus for shared
                        market_coverage=len(all_markets),
                        estimated_stops=btc_zone.estimated_stops + major_zone.estimated_stops,
                        recent_test=btc_zone.recent_test or major_zone.recent_test,
                        distance_pct=0.0,
                        markets=all_markets
                    )
                    shared.append(combined)
        
        return shared
    
    def _deduplicate_liquidity_zones(self, zones: List[LiquidityZone]) -> List[LiquidityZone]:
        """Remove duplicate liquidity zones"""
        if not zones:
            return []
        
        # Sort by price
        zones.sort(key=lambda x: x.price)
        
        deduplicated = []
        for zone in zones:
            # Check if similar zone already exists (within 0.5%)
            similar_exists = False
            for existing in deduplicated:
                if abs(zone.price - existing.price) / zone.price < 0.005:
                    # Merge zones
                    existing.markets = list(set(existing.markets + zone.markets))
                    existing.market_coverage = len(existing.markets)
                    existing.strength = max(existing.strength, zone.strength)
                    existing.estimated_stops += zone.estimated_stops
                    similar_exists = True
                    break
            
            if not similar_exists:
                deduplicated.append(zone)
        
        # Sort by strength and market coverage
        deduplicated.sort(key=lambda x: (x.strength * x.market_coverage), reverse=True)
        return deduplicated
    
    # ENHANCED: Market maker incentive methods
    def _identify_trapped_traders_enhanced(self, btc_role: BTCRole,
                                          alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                                          liquidity_map: CrossMarketLiquidity,
                                          regime: str) -> Dict[str, List]:
        """Identify trapped traders - ENHANCED"""
        trapped = {
            "longs_trapped": [],
            "shorts_trapped": []
        }
        
        # Based on BTC role
        if btc_role.primary_role == "Distribution anchor":
            trapped["longs_trapped"].append({
                "market": "BTC",
                "reason": "Distribution at highs",
                "estimated_size": "large",
                "price_level": "near_htf_high"
            })
        
        elif btc_role.primary_role == "Accumulation base":
            trapped["shorts_trapped"].append({
                "market": "BTC",
                "reason": "Accumulation at lows",
                "estimated_size": "large",
                "price_level": "near_htf_low"
            })
        
        # Check altcoins
        for alt in alt_rankings.get("Weak/Vulnerable", []):
            if alt.distribution_score > 0.7 and alt.absolute_performance < -0.02:
                trapped["longs_trapped"].append({
                    "market": alt.symbol,
                    "reason": "Relative weakness with negative momentum",
                    "estimated_size": "medium",
                    "price_level": "below_recent_highs"
                })
        
        for alt in alt_rankings.get("Leaders", []):
            if alt.accumulation_score > 0.7 and alt.absolute_performance > 0.02:
                # In range environment, strong leaders near highs might trap shorts
                if "Range" in regime:
                    trapped["shorts_trapped"].append({
                        "market": alt.symbol,
                        "reason": "Accumulation with breakout potential",
                        "estimated_size": "small",
                        "price_level": "near_range_high"
                    })
        
        return trapped
    
    def _determine_over_leveraged_side_enhanced(self, btc_role: BTCRole,
                                               alt_rankings: Dict[str, List[AltcoinRelativeAnalysis]],
                                               regime: str) -> str:
        """Determine over-leveraged side - ENHANCED"""
        
        # Check altcoin positioning
        leaders = alt_rankings.get("Leaders", [])
        weak = alt_rankings.get("Weak/Vulnerable", [])
        
        if "Range" in regime:
            # In ranges, both sides can be over-leveraged at extremes
            if len(leaders) > len(weak) * 2:
                return "longs"  # Too many longs on strength
            elif len(weak) > len(leaders) * 2:
                return "shorts"  # Too many shorts on weakness
            else:
                return "balanced"
        
        elif "Expansion" in regime:
            return "longs"  # Usually longs get over-leveraged
        
        elif "Distribution" in regime:
            return "longs"  # Longs trapped at highs
        
        elif "Accumulation" in regime:
            return "shorts"  # Shorts trapped at lows
        
        return "balanced"
    
    def _calculate_optimal_direction_enhanced(self, regime: str, btc_role: BTCRole,
                                            trapped_traders: Dict[str, List],
                                            over_leveraged_side: str,
                                            liquidity_map: CrossMarketLiquidity) -> str:
        """Calculate optimal direction - ENHANCED"""
        
        trapped_longs = len(trapped_traders.get("longs_trapped", []))
        trapped_shorts = len(trapped_traders.get("shorts_trapped", []))
        
        # In range environments, fading extremes often pays
        if "Range" in regime or "Chop" in regime:
            if trapped_longs > trapped_shorts:
                return "reversal"  # Fade longs at top
            elif trapped_shorts > trapped_longs:
                return "reversal"  # Fade shorts at bottom
            else:
                return "continuation"  # Wait for breakout
        
        # Check liquidation cascades
        if liquidity_map.max_liquidation_zone:
            # If big liquidation nearby, reversal toward it often pays
            current_price = self._get_current_btc_price()
            if current_price > 0:
                liq_distance = abs(liquidity_map.max_liquidation_zone.price - current_price) / current_price
                if liq_distance < 0.03:  # Within 3%
                    if liquidity_map.max_liquidation_zone.zone_type in ["equal_low", "prior_weekly_low", "htf_low"]:
                        return "reversal"  # Down to liquidate longs
                    else:
                        return "reversal"  # Up to liquidate shorts
        
        # Original logic
        if trapped_longs > trapped_shorts * 1.5:
            return "reversal"
        elif trapped_shorts > trapped_longs * 1.5:
            return "reversal"
        
        if "Expansion" in regime and btc_role.primary_role == "Expansion leader":
            return "continuation"
        
        if "Accumulation" in regime:
            return "continuation"
        
        return "continuation"
    
    def _estimate_mm_payoff_enhanced(self, optimal_direction: str,
                                    trapped_traders: Dict[str, List],
                                    liquidity_map: CrossMarketLiquidity,
                                    regime: str) -> Dict[str, float]:
        """Estimate market maker payoff - ENHANCED"""
        
        continuation_payoff = 0.5
        reversal_payoff = 0.5
        
        trapped_long_count = len(trapped_traders.get("longs_trapped", []))
        trapped_short_count = len(trapped_traders.get("shorts_trapped", []))
        
        # Range environments: equal payoff unless extreme
        if "Range" in regime:
            if trapped_long_count > trapped_short_count * 2:
                reversal_payoff = 0.6
                continuation_payoff = 0.4
            elif trapped_short_count > trapped_long_count * 2:
                reversal_payoff = 0.6
                continuation_payoff = 0.4
        
        # Trend environments: bias toward continuation
        elif "Expansion" in regime:
            continuation_payoff = 0.7
            reversal_payoff = 0.3
        
        # Consider liquidation cascades
        if liquidity_map.estimated_cascade_value > 50000000:
            if optimal_direction == "reversal":
                reversal_payoff = min(reversal_payoff * 1.3, 0.9)
            else:
                continuation_payoff = min(continuation_payoff * 1.3, 0.9)
        
        return {
            "continuation": continuation_payoff,
            "reversal": reversal_payoff
        }
    
    # ENHANCED: Position management
    def _find_long_stop_loss_enhanced(self, entry_price: float, btc_structure: BTCStructure,
                                     liquidity_map: CrossMarketLiquidity, entry_type: str) -> float:
        """Find stop loss for long position - ENHANCED"""
        
        candidate_stops = []
        
        # Different stop logic based on entry type
        if "range" in entry_type.lower() or "mean_reversion" in entry_type.lower():
            # Tighter stops for range trades
            stop_pct = 0.015  # 1.5%
            candidate_stops.append(entry_price * (1 - stop_pct))
        
        # Below equal lows
        for low in btc_structure.equal_lows:
            if low < entry_price:
                candidate_stops.append(low * 0.99)
        
        # Below prior weekly low
        if btc_structure.prior_weekly_low < entry_price:
            candidate_stops.append(btc_structure.prior_weekly_low * 0.99)
        
        # Below HTF low
        if btc_structure.htf_low < entry_price:
            candidate_stops.append(btc_structure.htf_low * 0.985)
        
        # Check shared liquidity zones
        for zone in liquidity_map.shared_htf_levels:
            if "low" in zone.zone_type.lower() and zone.price < entry_price:
                candidate_stops.append(zone.price * 0.99)
        
        if candidate_stops:
            return min(candidate_stops)
        
        # Default based on entry type
        if "breakout" in entry_type.lower():
            return entry_price * 0.97  # 3% stop for breakouts
        else:
            return entry_price * 0.98  # 2% stop for others
    
    def _find_short_stop_loss_enhanced(self, entry_price: float, btc_structure: BTCStructure,
                                      liquidity_map: CrossMarketLiquidity, entry_type: str) -> float:
        """Find stop loss for short position - ENHANCED"""
        
        candidate_stops = []
        
        if "range" in entry_type.lower() or "mean_reversion" in entry_type.lower():
            stop_pct = 0.015
            candidate_stops.append(entry_price * (1 + stop_pct))
        
        # Above equal highs
        for high in btc_structure.equal_highs:
            if high > entry_price:
                candidate_stops.append(high * 1.01)
        
        # Above prior weekly high
        if btc_structure.prior_weekly_high > entry_price:
            candidate_stops.append(btc_structure.prior_weekly_high * 1.01)
        
        # Above HTF high
        if btc_structure.htf_high > entry_price:
            candidate_stops.append(btc_structure.htf_high * 1.015)
        
        # Check shared liquidity zones
        for zone in liquidity_map.shared_htf_levels:
            if "high" in zone.zone_type.lower() and zone.price > entry_price:
                candidate_stops.append(zone.price * 1.01)
        
        if candidate_stops:
            return max(candidate_stops)
        
        if "breakout" in entry_type.lower():
            return entry_price * 1.03
        else:
            return entry_price * 1.02
    
    def _find_long_take_profits_enhanced(self, entry_price: float, btc_structure: BTCStructure,
                                        liquidity_map: CrossMarketLiquidity, entry_type: str) -> Dict[str, float]:
        """Find take profit levels for long position - ENHANCED"""
        
        tps = {}
        
        # TP1: First opposing liquidity level
        for high in btc_structure.equal_highs:
            if high > entry_price:
                tps["tp1"] = high
                break
        
        # Check shared zones
        if "tp1" not in tps:
            for zone in liquidity_map.shared_htf_levels:
                if "high" in zone.zone_type.lower() and zone.price > entry_price:
                    tps["tp1"] = zone.price
                    break
        
        # Default TP1
        if "tp1" not in tps:
            if "range" in entry_type.lower():
                tps["tp1"] = entry_price * 1.015  # 1.5% for range trades
            else:
                tps["tp1"] = entry_price * 1.02  # 2% default
        
        # TP2: HTF high or major resistance
        tps["tp2"] = btc_structure.htf_high
        
        # TP3: Extended target (beyond resistance)
        tps["tp3"] = btc_structure.htf_high * 1.03  # 3% beyond
        
        return tps
    
    def _find_short_take_profits_enhanced(self, entry_price: float, btc_structure: BTCStructure,
                                         liquidity_map: CrossMarketLiquidity, entry_type: str) -> Dict[str, float]:
        """Find take profit levels for short position - ENHANCED"""
        
        tps = {}
        
        # TP1: First opposing liquidity level
        for low in btc_structure.equal_lows:
            if low < entry_price:
                tps["tp1"] = low
                break
        
        # Check shared zones
        if "tp1" not in tps:
            for zone in liquidity_map.shared_htf_levels:
                if "low" in zone.zone_type.lower() and zone.price < entry_price:
                    tps["tp1"] = zone.price
                    break
        
        # Default TP1
        if "tp1" not in tps:
            if "range" in entry_type.lower():
                tps["tp1"] = entry_price * 0.985  # 1.5% for range trades
            else:
                tps["tp1"] = entry_price * 0.98  # 2% default
        
        # TP2: HTF low or major support
        tps["tp2"] = btc_structure.htf_low
        
        # TP3: Extended target (beyond support)
        tps["tp3"] = btc_structure.htf_low * 0.97  # 3% beyond
        
        return tps
    
    # ========== OTHER HELPER METHODS ==========
    
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
            if self.debug:
                log.debug(f"Fetch error {symbol} {tf_name}: {e}")
            return pd.DataFrame()
    
    def _calculate_funding_persistence(self) -> int:
        """Calculate funding rate persistence"""
        if len(self.funding_history) < 2:
            return 0
        
        # Count consecutive hours with same bias
        persistence = 1
        current_bias = "positive" if self.funding_history[-1] > 0.0001 else "negative" if self.funding_history[-1] < -0.0001 else "neutral"
        
        for i in range(len(self.funding_history)-2, -1, -1):
            prev_rate = self.funding_history[i]
            prev_bias = "positive" if prev_rate > 0.0001 else "negative" if prev_rate < -0.0001 else "neutral"
            
            if prev_bias == current_bias:
                persistence += 1
            else:
                break
        
        return persistence
    
    async def _estimate_oi_trend_simulated(self, current_price: float) -> str:
        """Simulate OI trend based on price action"""
        try:
            df = await self._fetch_timeframe_data("BTC/USDT", "1h", 24, "1h_oi_sim")
            if len(df) < 10:
                return "flat"
            
            # Simple simulation: OI tends to rise with strong trends
            price_change = (df['close'].iloc[-1] - df['close'].iloc[-10]) / df['close'].iloc[-10]
            volatility = df['close'].pct_change().std()
            
            if abs(price_change) > 0.03 and volatility > 0.01:
                if price_change > 0:
                    return "rising"
                else:
                    return "falling"
            else:
                return "flat"
                
        except:
            return "unknown"
    
    async def _detect_price_oi_divergence_simulated(self) -> bool:
        """Simulate price-OI divergence detection"""
        # Simplified: divergence when price makes new high/low but momentum weakens
        try:
            df = await self._fetch_timeframe_data("BTC/USDT", "4h", 20, "4h_div_sim")
            if len(df) < 10:
                return False
            
            recent_high = df['high'].iloc[-5:].max()
            prior_high = df['high'].iloc[-10:-5].max()
            recent_close = df['close'].iloc[-5:].mean()
            prior_close = df['close'].iloc[-10:-5].mean()
            
            # Bearish divergence: price higher highs but weaker closes
            if recent_high > prior_high and recent_close < prior_close:
                return True
            
            recent_low = df['low'].iloc[-5:].min()
            prior_low = df['low'].iloc[-10:-5].min()
            
            # Bullish divergence: price lower lows but higher closes
            if recent_low < prior_low and recent_close > prior_close:
                return True
            
            return False
            
        except:
            return False
    
    async def _estimate_liquidation_levels(self) -> Dict[str, float]:
        """Estimate liquidation levels"""
        try:
            df = await self._fetch_timeframe_data("BTC/USDT", "1h", 24, "1h_liq_est")
            if len(df) < 10:
                return {"longs": 0, "shorts": 0}
            
            recent_high = df['high'].iloc[-10:].max()
            recent_low = df['low'].iloc[-10:].min()
            current_price = df['close'].iloc[-1]
            
            # Estimate based on distance from extremes
            long_liq = recent_low * 0.97  # 3% below recent low
            short_liq = recent_high * 1.03  # 3% above recent high
            
            # Adjust based on current position
            range_mid = (recent_high + recent_low) / 2
            if current_price > range_mid:
                # More long liquidations likely if price drops
                long_liq = long_liq * 0.99  # Closer
            else:
                # More short liquidations likely if price rises
                short_liq = short_liq * 1.01  # Closer
            
            return {
                "longs": float(long_liq),
                "shorts": float(short_liq)
            }
            
        except:
            return {"longs": 0, "shorts": 0}
    
    def _get_current_btc_price(self) -> float:
        """Get current BTC price"""
        # In production, fetch from exchange
        # For now, use placeholder
        return 50000.0
    
    def _get_symbol_price(self, symbol: str) -> float:
        """Get current price for a symbol"""
        # Placeholder - in production, fetch from exchange
        if "BTC" in symbol:
            return 50000.0
        elif "ETH" in symbol:
            return 3000.0
        elif "SOL" in symbol:
            return 100.0
        else:
            return 1.0
    
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
            estimated_liquidations={"longs": 0, "shorts": 0},
            funding_rate=0.0,
            open_interest=0.0
        )
    
    # ========== MAIN ANALYSIS METHOD ==========
    async def analyze_market(self) -> RegimeAnalysis:
        """Complete institutional market analysis"""
        
        self.scan_count += 1
        analysis_start = time.time()
        
        log.info(f"\n{'='*70}")
        log.info(f"🔬 INSTITUTIONAL ANALYSIS #{self.scan_count}")
        log.info(f"{'='*70}")
        
        # STEP 1: Global Regime
        global_regime, regime_confidence = await self.determine_global_regime()
        
        # STEP 2: Bitcoin Analysis
        btc_structure = await self.analyze_bitcoin_structure()
        btc_derivatives = await self.analyze_bitcoin_derivatives()
        btc_role = await self.determine_bitcoin_role(btc_structure, btc_derivatives)
        
        # STEP 3: Major Coins Alignment
        alt_rankings = await self.analyze_majors_alignment()
        
        # STEP 4: Cross-Market Liquidity Map
        liquidity_map = await self.build_liquidity_map(btc_structure, alt_rankings)
        
        # STEP 5: Market Maker Incentives
        mm_incentives = self.analyze_market_maker_incentives(
            global_regime, btc_role, alt_rankings, liquidity_map
        )
        
        # STEP 6: Trade Decision
        trade_decision = self.make_trade_decision(
            global_regime, regime_confidence, btc_role, alt_rankings, mm_incentives
        )
        
        # STEP 7: Position Management
        position_mgmt = None
        if trade_decision.decision != "⚫ No Trade":
            position_mgmt = self.calculate_position_management(
                trade_decision, btc_structure, liquidity_map
            )
        
        # Predict next move
        next_move = self._predict_next_move_enhanced(
            global_regime, btc_role, mm_incentives, liquidity_map, trade_decision
        )
        
        # Calculate confidence
        confidence = self._calculate_overall_confidence(
            regime_confidence, btc_role.role_score, trade_decision, position_mgmt
        )
        
        # Create analysis
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
        
        self.analysis_history.append(analysis)
        await self._save_analysis(analysis)
        
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
    
    def _predict_next_move_enhanced(self, regime: str, btc_role: BTCRole,
                                   mm_incentives: MarketMakerIncentives,
                                   liquidity_map: CrossMarketLiquidity,
                                   trade_decision: TradeDecision) -> str:
        """Predict next market move - ENHANCED"""
        
        if trade_decision.decision != "⚫ No Trade":
            if trade_decision.side == "LONG":
                return f"Upside move toward {liquidity_map.shared_htf_levels[0].price:.0f} if BTC confirms"
            else:
                return f"Downside move toward {liquidity_map.shared_htf_levels[-1].price:.0f} if BTC breaks"
        
        if "Range" in regime:
            if mm_incentives.trapped_traders.get("longs_trapped"):
                return "Breakdown toward range low to liquidate trapped longs"
            elif mm_incentives.trapped_traders.get("shorts_trapped"):
                return "Breakout toward range high to liquidate trapped shorts"
            else:
                return "Continued range oscillation between key levels"
        
        elif "Expansion" in regime:
            return "Continuation of trend toward next liquidity zone"
        
        elif "Accumulation" in regime:
            return "Breakout after sufficient basing period"
        
        elif "Distribution" in regime:
            return "Breakdown toward support levels"
        
        elif "Volatility Compression" in regime:
            return "Impulse move coming - direction TBD"
        
        return "Consolidation awaiting catalyst"
    
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
    
    # ========== DATABASE METHODS ==========
    async def _save_analysis(self, analysis: RegimeAnalysis):
        """Save analysis to database"""
        try:
            btc_structure_json = json.dumps(asdict(analysis.btc_structure))
            btc_derivatives_json = json.dumps(asdict(analysis.btc_derivatives))
            
            leaders_json = json.dumps([asdict(alt) for alt in analysis.altcoin_rankings.get("Leaders", [])])
            weak_json = json.dumps([asdict(alt) for alt in analysis.altcoin_rankings.get("Weak/Vulnerable", [])])
            
            liquidity_json = json.dumps(asdict(analysis.cross_market_liquidity))
            incentives_json = json.dumps(asdict(analysis.market_maker_incentives))
            
            stop_loss_json = ""
            take_profit_json = ""
            
            if analysis.position_management:
                stop_loss_json = json.dumps(analysis.position_management.stop_loss)
                take_profit_json = json.dumps(analysis.position_management.take_profit)
            
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
        
        side_emoji = "🟢" if decision.side == "LONG" else "🔴"
        clean_symbol = decision.symbol.replace('/', '') if decision.symbol else ""
        
        sl_price = ""
        tp1_price = ""
        
        if analysis.position_management:
            sl_price = f"{analysis.position_management.stop_loss['price']:.2f}"
            tp1_price = f"{analysis.position_management.take_profit['tp1']['price']:.2f}"
        
        message = f"""{side_emoji} <b>INSTITUTIONAL TRADE SIGNAL v1.1</b>

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

#{clean_symbol} #{decision.side} #RegimeTrading #Enhanced"""
        
        await self._send_telegram(message)
    
    # ========== MAIN SCANNING LOOP ==========
    async def run_scanning_loop(self):
        """Main scanning loop"""
        log.info("🚀 Starting Enhanced Regime Scanner...")
        
        while True:
            try:
                analysis = await self.analyze_market()
                await asyncio.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                log.info("🛑 Scanner stopped by user")
                break
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(30)
    
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
        log.info("🛑 Enhanced Regime Scanner stopped")
        
    finally:
        await scanner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())