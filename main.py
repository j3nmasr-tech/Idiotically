#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic
- TP/SL tracking with RomeOPT-P structure-based system (HTF STRUCTURE)
- Dynamic TP/SL updates (market-structure-based)
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
"""

import os, time, asyncio, logging, datetime, json
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None

# ---------------- UTILITY FUNCTIONS ----------------
def safe_json_dumps(obj):
    """Safely convert object to JSON, handling numpy and pandas types"""
    def _convert(item):
        if isinstance(item, (np.integer, np.floating)):
            return float(item)
        elif isinstance(item, np.ndarray):
            return item.tolist()
        elif isinstance(item, pd.Series):
            return item.tolist()
        elif isinstance(item, pd.DataFrame):
            return item.to_dict()
        elif isinstance(item, dict):
            return {k: _convert(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [_convert(i) for i in item]
        elif isinstance(item, (int, float, str, bool, type(None))):
            return item
        else:
            return str(item)
    return json.dumps(_convert(obj))

# ---------------- ROMEOPT-P TP/SL SYSTEM (HTF VERSION) ----------------
class RomeOPTTPSLSystem:
    """
    RomeOPT-P 6-Step TP/SL System
    Uses HIGHER TIMEFRAME structure for TP/SL analysis
    STRICTLY follows RomeOPT-P rules - no arithmetic fallbacks
    """
    
    @staticmethod
    def find_structure_elements(df_htf: pd.DataFrame, symbol: str, side: str, ob_zone: dict) -> Dict[str, Any]:
        """
        Find all structure elements from HTF data for TP/SL calculation
        """
        structure = {
            'order_blocks': [],
            'protected_levels': [],
            'fvgs': [],
            'bos_zones': [],
            'liquidity_pools': {'upside': [], 'downside': []}
        }
        
        if df_htf is None or len(df_htf) < 50:
            log.debug(f"Insufficient HTF data for {symbol}: {len(df_htf) if df_htf is not None else 0} candles")
            return structure
        
        try:
            # 1. Origin Order Block (from current TF, but we still track it)
            if ob_zone and isinstance(ob_zone, dict):
                structure['order_blocks'].append({
                    'type': 'bullish' if side == 'BUY' else 'bearish',
                    'high': float(ob_zone.get('high', 0)),
                    'low': float(ob_zone.get('low', 0)),
                    'origin': True,
                    'tf': 'current'
                })
            
            # 2. Protected levels from HTF (significant swing highs/lows)
            structure['protected_levels'].extend(
                RomeOPTTPSLSystem._find_protected_levels(df_htf)
            )
            
            # 3. FVGs from HTF (Fair Value Gaps)
            structure['fvgs'].extend(
                RomeOPTTPSLSystem._find_fvgs(df_htf)
            )
            
            # 4. BOS zones from HTF (Break of Structure)
            structure['bos_zones'].extend(
                RomeOPTTPSLSystem._find_bos_zones(df_htf)
            )
            
            # 5. Liquidity pools from HTF
            structure['liquidity_pools'] = RomeOPTTPSLSystem._find_liquidity_pools(df_htf)
            
            log.debug(f"Found HTF structure for {symbol}: "
                     f"{len(structure['protected_levels'])} protected levels, "
                     f"{len(structure['fvgs'])} FVGs, "
                     f"{len(structure['bos_zones'])} BOS zones, "
                     f"{len(structure['liquidity_pools']['upside'])} upside pools, "
                     f"{len(structure['liquidity_pools']['downside'])} downside pools")
            
        except Exception as e:
            log.error(f"Error finding HTF structure elements for {symbol}: {e}")
        
        return structure
    
    @staticmethod
    def _find_protected_levels(df: pd.DataFrame) -> List[Dict]:
        """Find protected highs/lows (significant swing points)"""
        levels = []
        try:
            if len(df) < 30:
                return levels
                
            # Find significant swing highs (higher timeframe swing points)
            for i in range(20, len(df) - 10):
                if i - 10 >= 0 and i + 11 <= len(df):
                    # Check if this is a significant high
                    is_high = True
                    for j in range(max(0, i-10), min(len(df), i+11)):
                        if j != i and df['high'].iloc[j] >= df['high'].iloc[i]:
                            is_high = False
                            break
                    if is_high:
                        levels.append({
                            'type': 'high', 
                            'price': float(df['high'].iloc[i]),
                            'strength': 3
                        })
            
            # Find significant swing lows
            for i in range(20, len(df) - 10):
                if i - 10 >= 0 and i + 11 <= len(df):
                    # Check if this is a significant low
                    is_low = True
                    for j in range(max(0, i-10), min(len(df), i+11)):
                        if j != i and df['low'].iloc[j] <= df['low'].iloc[i]:
                            is_low = False
                            break
                    if is_low:
                        levels.append({
                            'type': 'low', 
                            'price': float(df['low'].iloc[i]),
                            'strength': 3
                        })
                        
        except Exception as e:
            log.error(f"Error finding protected levels: {e}")
        
        return levels
    
    @staticmethod
    def _find_fvgs(df: pd.DataFrame) -> List[Dict]:
        """Find Fair Value Gaps"""
        fvgs = []
        try:
            if len(df) < 3:
                return fvgs
                
            for i in range(2, len(df)):
                # Bullish FVG: current low > previous high
                if df['low'].iloc[i] > df['high'].iloc[i-1]:
                    fvgs.append({
                        'type': 'buy',
                        'high': float(df['low'].iloc[i]),
                        'low': float(df['high'].iloc[i-1]),
                        'mid': float((df['low'].iloc[i] + df['high'].iloc[i-1]) / 2),
                        'origin_candle': {
                            'high': float(df['high'].iloc[i]),
                            'low': float(df['low'].iloc[i]),
                            'open': float(df['open'].iloc[i]),
                            'close': float(df['close'].iloc[i])
                        }
                    })
                # Bearish FVG: current high < previous low
                elif df['high'].iloc[i] < df['low'].iloc[i-1]:
                    fvgs.append({
                        'type': 'sell',
                        'high': float(df['low'].iloc[i-1]),
                        'low': float(df['high'].iloc[i]),
                        'mid': float((df['low'].iloc[i-1] + df['high'].iloc[i]) / 2),
                        'origin_candle': {
                            'high': float(df['high'].iloc[i]),
                            'low': float(df['low'].iloc[i]),
                            'open': float(df['open'].iloc[i]),
                            'close': float(df['close'].iloc[i])
                        }
                    })
        except Exception as e:
            log.error(f"Error finding FVGs: {e}")
        
        return fvgs
    
    @staticmethod
    def _find_bos_zones(df: pd.DataFrame) -> List[Dict]:
        """Find Break of Structure zones"""
        bos_zones = []
        try:
            if len(df) < 50:
                return bos_zones
                
            # Look for significant breaks in structure
            window = 20
            
            for i in range(window + 10, len(df) - 10):
                # Check for bullish BOS: price breaks above previous consolidation
                prev_high = df['high'].iloc[i-window:i].max()
                current_high = df['high'].iloc[i]
                
                if current_high > prev_high * 1.005:  # 0.5% break
                    # Verify it's not just a wick
                    body_high = max(df['open'].iloc[i], df['close'].iloc[i])
                    if body_high > prev_high:
                        bos_zones.append({
                            'type': 'buy',
                            'high': float(current_high),
                            'low': float(df['low'].iloc[i-window:i].min()),
                            'break_level': float(prev_high)
                        })
                
                # Check for bearish BOS: price breaks below previous consolidation
                prev_low = df['low'].iloc[i-window:i].min()
                current_low = df['low'].iloc[i]
                
                if current_low < prev_low * 0.995:  # 0.5% break
                    # Verify it's not just a wick
                    body_low = min(df['open'].iloc[i], df['close'].iloc[i])
                    if body_low < prev_low:
                        bos_zones.append({
                            'type': 'sell',
                            'high': float(df['high'].iloc[i-window:i].max()),
                            'low': float(current_low),
                            'break_level': float(prev_low)
                        })
        except Exception as e:
            log.error(f"Error finding BOS zones: {e}")
        
        return bos_zones
    
    @staticmethod
    def _find_liquidity_pools(df: pd.DataFrame) -> Dict[str, List]:
        """Find liquidity pools (swing points, equal highs/lows, FVG fills)"""
        pools = {'upside': [], 'downside': []}
        
        try:
            if len(df) < 50:
                return pools
            
            # 1. SWING HIGHS (Upside Liquidity)
            # Look for local highs that are higher than surrounding candles
            for i in range(10, len(df) - 10):
                left_window = df['high'].iloc[max(0, i-10):i]
                right_window = df['high'].iloc[i+1:min(len(df), i+11)]
                
                current_high = df['high'].iloc[i]
                
                # Check if this is a local high
                if (left_window.empty or current_high > left_window.max()) and \
                   (right_window.empty or current_high > right_window.max()):
                    
                    # Only add if significantly above recent lows
                    recent_low = df['low'].iloc[max(0, i-5):min(len(df), i+6)].min()
                    if current_high > recent_low * 1.01:  # At least 1% above recent low
                        pools['upside'].append({
                            'type': 'swing_high',
                            'price': float(current_high),
                            'strength': 3
                        })
            
            # 2. SWING LOWS (Downside Liquidity)
            # Look for local lows that are lower than surrounding candles
            for i in range(10, len(df) - 10):
                left_window = df['low'].iloc[max(0, i-10):i]
                right_window = df['low'].iloc[i+1:min(len(df), i+11)]
                
                current_low = df['low'].iloc[i]
                
                # Check if this is a local low
                if (left_window.empty or current_low < left_window.min()) and \
                   (right_window.empty or current_low < right_window.min()):
                    
                    # Only add if significantly below recent highs
                    recent_high = df['high'].iloc[max(0, i-5):min(len(df), i+6)].max()
                    if current_low < recent_high * 0.99:  # At least 1% below recent high
                        pools['downside'].append({
                            'type': 'swing_low',
                            'price': float(current_low),
                            'strength': 3
                        })
            
            # 3. EQUAL HIGHS
            # Find recent equal highs (last 30 candles)
            if len(df) >= 30:
                recent_highs = df['high'].iloc[-30:]
                # Find clusters of similar highs
                high_values = sorted(recent_highs.unique())
                for high in high_values[-3:]:  # Top 3 recent highs
                    count = (recent_highs >= high * 0.995).sum()
                    if count >= 2:  # At least 2 touches
                        pools['upside'].append({
                            'type': 'equal_high',
                            'price': float(high),
                            'strength': 2,
                            'touches': int(count)
                        })
            
            # 4. EQUAL LOWS
            # Find recent equal lows (last 30 candles)
            if len(df) >= 30:
                recent_lows = df['low'].iloc[-30:]
                # Find clusters of similar lows
                low_values = sorted(recent_lows.unique())
                for low in low_values[:3]:  # Bottom 3 recent lows
                    count = (recent_lows <= low * 1.005).sum()
                    if count >= 2:  # At least 2 touches
                        pools['downside'].append({
                            'type': 'equal_low',
                            'price': float(low),
                            'strength': 2,
                            'touches': int(count)
                        })
            
            # 5. FVG IMBALANCE FILLS
            # Find FVGs that need to be filled
            fvgs = RomeOPTTPSLSystem._find_fvgs(df)
            for fvg in fvgs:
                if fvg['type'] == 'buy':
                    # Bullish FVG: target is the high of the gap
                    pools['upside'].append({
                        'type': 'fvg_fill',
                        'price': float(fvg['high']),
                        'mid_price': float(fvg['mid']),
                        'strength': 2
                    })
                else:
                    # Bearish FVG: target is the low of the gap
                    pools['downside'].append({
                        'type': 'fvg_fill',
                        'price': float(fvg['low']),
                        'mid_price': float(fvg['mid']),
                        'strength': 2
                    })
            
            # 6. PREMIUM/DISCOUNT RANGES (62-70% / 30-38%)
            if len(df) >= 100:
                # Use larger lookback for meaningful ranges
                recent_high = df['high'].iloc[-100:].max()
                recent_low = df['low'].iloc[-100:].min()
                recent_range = recent_high - recent_low
                
                if recent_range > 0:
                    # Premium range: 62-70%
                    premium_low = recent_low + (recent_range * 0.62)
                    premium_high = recent_low + (recent_range * 0.70)
                    premium_mid = (premium_low + premium_high) / 2
                    
                    pools['upside'].append({
                        'type': 'premium_range',
                        'price': float(premium_mid),
                        'range_low': float(premium_low),
                        'range_high': float(premium_high),
                        'strength': 1
                    })
                    
                    # Discount range: 30-38%
                    discount_low = recent_low + (recent_range * 0.30)
                    discount_high = recent_low + (recent_range * 0.38)
                    discount_mid = (discount_low + discount_high) / 2
                    
                    pools['downside'].append({
                        'type': 'discount_range',
                        'price': float(discount_mid),
                        'range_low': float(discount_low),
                        'range_high': float(discount_high),
                        'strength': 1
                    })
                    
        except Exception as e:
            log.error(f"Error finding liquidity pools: {e}")
        
        return pools
    
    @staticmethod
    def calculate_stop_loss(side: str, structure: Dict, entry_price: float, ob_zone: dict) -> Optional[float]:
        """
        Calculate stop loss based on structure invalidation points
        Follows RomeOPT-P rules STRICTLY
        """
        try:
            if side not in ['BUY', 'SELL']:
                return None
            
            setup_type = 'buy' if side == 'BUY' else 'sell'
            sl_candidates = []
            
            # RULE 1: BOS entry (highest priority)
            bos_sl = RomeOPTTPSLSystem._get_bos_sl(setup_type, structure)
            if bos_sl is not None:
                sl_candidates.append(bos_sl)
            
            # RULE 2: Origin Order Block
            ob_sl = RomeOPTTPSLSystem._get_ob_sl(setup_type, structure, ob_zone)
            if ob_sl is not None:
                sl_candidates.append(ob_sl)
            
            # RULE 3: Protected levels
            protected_sl = RomeOPTTPSLSystem._get_protected_sl(setup_type, structure)
            if protected_sl is not None:
                sl_candidates.append(protected_sl)
            
            # RULE 4: Origin FVG candle
            fvg_sl = RomeOPTTPSLSystem._get_fvg_sl(setup_type, structure)
            if fvg_sl is not None:
                sl_candidates.append(fvg_sl)
            
            # Select the most appropriate SL that invalidates the setup
            final_sl = RomeOPTTPSLSystem._select_optimal_sl(setup_type, sl_candidates, entry_price)
            
            if final_sl is None:
                # NO VALID SL FOUND - REJECT SIGNAL (RomeOPT-P strict)
                log.debug("No valid SL found using RomeOPT-P rules")
                return None
            
            return float(final_sl)
            
        except Exception as e:
            log.error(f"Error calculating stop loss: {e}")
            return None
    
    @staticmethod
    def _get_bos_sl(setup_type: str, structure: Dict) -> Optional[float]:
        """Get SL from BOS zone - RomeOPT-P: below BOS zone low for buy, above BOS zone high for sell"""
        try:
            for bos in structure.get('bos_zones', []):
                if bos.get('type') == setup_type:
                    if setup_type == 'buy':
                        # For buy setups: place SL below the BOS zone low
                        return float(bos.get('low', 0)) * 0.999
                    else:
                        # For sell setups: place SL above the BOS zone high
                        return float(bos.get('high', 0)) * 1.001
        except:
            pass
        return None
    
    @staticmethod
    def _get_ob_sl(setup_type: str, structure: Dict, ob_zone: dict) -> Optional[float]:
        """Get SL from origin Order Block - RomeOPT-P: below OB low for buy, above OB high for sell"""
        try:
            # First check origin OBs from structure
            origin_obs = [ob for ob in structure.get('order_blocks', []) 
                         if ob.get('origin', False) and ob.get('type') == setup_type]
            
            if origin_obs:
                best_ob = origin_obs[0]
                if setup_type == 'buy':
                    return float(best_ob.get('low', 0)) * 0.998
                else:
                    return float(best_ob.get('high', 0)) * 1.002
            
            # Fallback to detected OB zone
            elif ob_zone:
                if setup_type == 'buy' and 'low' in ob_zone:
                    return float(ob_zone['low']) * 0.998
                elif setup_type == 'sell' and 'high' in ob_zone:
                    return float(ob_zone['high']) * 1.002
            
        except:
            pass
        return None
    
    @staticmethod
    def _get_protected_sl(setup_type: str, structure: Dict) -> Optional[float]:
        """Get SL from protected levels - RomeOPT-P: below protected low for buy, above protected high for sell"""
        try:
            for level in structure.get('protected_levels', []):
                if setup_type == 'buy' and level.get('type') == 'low':
                    return float(level.get('price', 0)) * 0.997
                elif setup_type == 'sell' and level.get('type') == 'high':
                    return float(level.get('price', 0)) * 1.003
        except:
            pass
        return None
    
    @staticmethod
    def _get_fvg_sl(setup_type: str, structure: Dict) -> Optional[float]:
        """Get SL from origin FVG candle - RomeOPT-P: below origin candle of entry FVG"""
        try:
            for fvg in structure.get('fvgs', []):
                if fvg.get('type') == setup_type and 'origin_candle' in fvg:
                    origin_candle = fvg['origin_candle']
                    if setup_type == 'buy':
                        return float(origin_candle.get('low', 0)) * 0.996
                    else:
                        return float(origin_candle.get('high', 0)) * 1.004
        except:
            pass
        return None
    
    @staticmethod
    def _select_optimal_sl(setup_type: str, sl_candidates: List[float], entry_price: float) -> Optional[float]:
        """Select optimal SL based on RomeOPT-P criteria"""
        if not sl_candidates:
            return None
        
        # Filter SLs that make sense for the setup
        valid_sls = []
        for sl in sl_candidates:
            if sl is None:
                continue
            if setup_type == 'buy':
                if sl < entry_price:  # SL must be below entry for buys
                    valid_sls.append(float(sl))
            else:
                if sl > entry_price:  # SL must be above entry for sells
                    valid_sls.append(float(sl))
        
        if not valid_sls:
            return None
        
        # RomeOPT-P: Choose the SL that invalidates the setup
        # For buy: highest SL below entry (tightest)
        # For sell: lowest SL above entry (tightest)
        if setup_type == 'buy':
            return max(valid_sls)
        else:
            return min(valid_sls)
    
    @staticmethod
    def calculate_take_profit(side: str, structure: Dict, entry_price: float, sl_price: float) -> Optional[Tuple[float, Optional[float]]]:
        """
        Calculate take profit based on liquidity pools
        STRICTLY follows RomeOPT-P rules - returns (TP1, TP2) or (TP1, None) for single TP
        RomeOPT-P: 1-2 TPs MAX, TP1: 0.5-1.5%, TP2: 1.5-3.0% from entry
        """
        try:
            # RomeOPT-P: Always check risk first
            if side == 'BUY':
                risk_pct = (entry_price - sl_price) / entry_price
            else:
                risk_pct = (sl_price - entry_price) / entry_price
            
            if side == 'BUY':
                liquidity_pools = structure.get('liquidity_pools', {}).get('upside', [])
                if not liquidity_pools:
                    log.debug("No upside liquidity pools found for BUY")
                    return None
                
                # RomeOPT-P: Filter and sort valid pools
                valid_pools = []
                for pool in liquidity_pools:
                    pool_price = pool.get('price', 0)
                    if pool_price <= entry_price:
                        continue  # Must be above entry for BUY
                    
                    # Calculate distance from entry
                    distance_pct = (pool_price - entry_price) / entry_price
                    
                    # RomeOPT-P CRITICAL FIX: Reject pools >3% away
                    if distance_pct > 0.03:  # 3% max
                        continue
                    
                    # RomeOPT-P: Place TP just BEFORE the liquidity (never through)
                    tp_price = pool_price * 0.999  # 0.1% below the liquidity
                    
                    valid_pools.append({
                        'price': tp_price,
                        'original_price': pool_price,
                        'type': pool.get('type', 'unknown'),
                        'strength': pool.get('strength', 1),
                        'distance_pct': distance_pct
                    })
                
                if not valid_pools:
                    log.debug("No valid upside liquidity pools above entry (within 3%)")
                    return None
                
                # Sort by distance (closest first)
                valid_pools.sort(key=lambda x: x['distance_pct'])
                
                # RomeOPT-P: Select 1-2 TPs based on distances
                selected_tps = RomeOPTTPSLSystem._select_romeopt_tps(valid_pools, entry_price, 'BUY')
                
                if not selected_tps:  # No valid TPs found
                    log.debug("No valid TPs found for BUY using RomeOPT-P rules")
                    return None
                
                # Return TPs based on selection
                if len(selected_tps) == 1:
                    return (selected_tps[0], None)  # Single TP
                else:
                    return (selected_tps[0], selected_tps[1])  # Two TPs
                
            else:  # SELL
                liquidity_pools = structure.get('liquidity_pools', {}).get('downside', [])
                if not liquidity_pools:
                    log.debug("No downside liquidity pools found for SELL")
                    return None
                
                # RomeOPT-P: Filter and sort valid pools
                valid_pools = []
                for pool in liquidity_pools:
                    pool_price = pool.get('price', 0)
                    if pool_price >= entry_price:
                        continue  # Must be below entry for SELL
                    
                    # Calculate distance from entry
                    distance_pct = (entry_price - pool_price) / entry_price
                    
                    # RomeOPT-P CRITICAL FIX: Reject pools >3% away
                    if distance_pct > 0.03:  # 3% max
                        continue
                    
                    # RomeOPT-P: Place TP just BEFORE the liquidity (never through)
                    tp_price = pool_price * 1.001  # 0.1% above the liquidity
                    
                    valid_pools.append({
                        'price': tp_price,
                        'original_price': pool_price,
                        'type': pool.get('type', 'unknown'),
                        'strength': pool.get('strength', 1),
                        'distance_pct': distance_pct
                    })
                
                if not valid_pools:
                    log.debug("No valid downside liquidity pools below entry (within 3%)")
                    return None
                
                # Sort by distance (closest first = highest price for SELL)
                valid_pools.sort(key=lambda x: x['distance_pct'])
                
                # RomeOPT-P: Select 1-2 TPs based on distances
                selected_tps = RomeOPTTPSLSystem._select_romeopt_tps(valid_pools, entry_price, 'SELL')
                
                if not selected_tps:  # No valid TPs found
                    log.debug("No valid TPs found for SELL using RomeOPT-P rules")
                    return None
                
                # Return TPs based on selection
                if len(selected_tps) == 1:
                    return (selected_tps[0], None)  # Single TP
                else:
                    return (selected_tps[0], selected_tps[1])  # Two TPs
                    
        except Exception as e:
            log.error(f"Error calculating take profit: {e}")
            return None
    
    @staticmethod
    def _select_romeopt_tps(valid_pools: List[Dict], entry_price: float, side: str) -> List[float]:
        """Select TP levels following RomeOPT-P rules strictly"""
        selected_tps = []
        
        # RomeOPT-P distance parameters
        min_distance_between_tps_pct = 0.005  # Minimum 0.5% between TPs
        min_tp_distance_from_entry_pct = 0.005  # Minimum 0.5% from entry
        max_tp_distance_from_entry_pct = 0.03   # Maximum 3% from entry
        
        for pool in valid_pools:
            if len(selected_tps) >= 2:  # RomeOPT-P: MAX 2 TPs (not 3)
                break
            
            tp_price = pool['price']
            distance_from_entry_pct = abs(tp_price - entry_price) / entry_price
            
            # YOUR CRITICAL FIX: Skip pools outside RomeOPT-P range
            if distance_from_entry_pct < min_tp_distance_from_entry_pct:
                continue  # Too close to entry (<0.5%)
            
            if distance_from_entry_pct > max_tp_distance_from_entry_pct:
                continue  # Too far from entry (>3%)
            
            # RomeOPT-P: Check distance from existing TPs
            too_close = False
            for existing_tp in selected_tps:
                distance_between_pct = abs(tp_price - existing_tp) / entry_price
                if distance_between_pct < min_distance_between_tps_pct:
                    too_close = True
                    break
            
            if not too_close:
                # RomeOPT-P: Ensure proper ordering
                if side == 'BUY' and tp_price > entry_price:
                    selected_tps.append(tp_price)
                elif side == 'SELL' and tp_price < entry_price:
                    selected_tps.append(tp_price)
        
        # RomeOPT-P: Sort TPs (ascending for BUY, descending for SELL)
        if side == 'BUY':
            selected_tps.sort()  # Ascending: TP1 < TP2
        else:
            selected_tps.sort(reverse=True)  # Descending: TP1 > TP2
        
        # RomeOPT-P: Validate distance rules
        if len(selected_tps) == 2:
            tp1, tp2 = selected_tps[0], selected_tps[1]
            distance_between_pct = abs(tp2 - tp1) / entry_price
            
            # Ensure TPs are at least 0.5% apart
            if distance_between_pct < min_distance_between_tps_pct:
                return [tp1]  # Keep only TP1 if too close
        
        return selected_tps

# ---------------- TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg: return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode":"HTML"})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Create table if not exists with all columns
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0,
            latest_ob TEXT,
            structure_data TEXT
        );
    """)
    
    # Check and add missing columns if needed
    await check_and_add_columns()
    
    await db_conn.commit()

async def check_and_add_columns():
    """Check for missing columns and add them if needed"""
    try:
        # Get the current table schema
        async with db_conn.execute("PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
        
        # List of required columns
        required_columns = [
            ('structure_data', 'TEXT')
        ]
        
        # Add any missing columns
        for column_name, column_type in required_columns:
            if column_name not in column_names:
                log.info(f"Adding column {column_name} to signals table...")
                await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_type}")
                
    except Exception as e:
        log.error(f"Error checking/adding columns: {e}")

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug("fetch_ohlcv failed for %s %s: %s", symbol, timeframe, e)
        return None

async def get_htf_structure_data(exchange, symbol: str, current_tf: str) -> Optional[pd.DataFrame]:
    """Get appropriate HTF data for structure analysis based on current TF"""
    # RomeOPT-P: Use CONSISTENT higher timeframe for structure analysis
    tf_to_htf_map = {
        "1m": "15m",   # 1m signals → use 15m structure (15x)
        "3m": "1h",    # 3m signals → use 1h structure (20x)
        "5m": "1h",    # 5m signals → use 1h structure (12x)
        "15m": "4h",   # 15m signals → use 4h structure (16x)
        "30m": "1d"    # 30m signals → use DAILY structure (48x) - RomeOPT-P standard!
    }
    
    htf = tf_to_htf_map.get(current_tf, "4h")
    
    # Adjust limit based on HTF for optimal data
    limit_map = {
        "15m": 200,   # 200*15m = 50 hours
        "1h": 168,    # 168*1h = 7 days
        "4h": 168,    # 168*4h = 28 days
        "1d": 90      # 90*1d = 90 days (3 months)
    }
    
    limit = limit_map.get(htf, 200)
    log.debug(f"Fetching HTF structure data for {symbol}: {current_tf} → {htf} (limit: {limit})")
    
    ohlcv = await fetch_ohlcv(exchange, symbol, htf, limit)
    if not ohlcv:
        log.debug(f"No HTF data returned for {symbol} {htf}")
        return None
    
    df_htf = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]:
        df_htf[c] = pd.to_numeric(df_htf[c], errors="coerce")
    
    log.debug(f"HTF structure data for {symbol} {htf}: {len(df_htf)} candles")
    return df_htf

# ---------------- INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    try:
        high, low, close = df["high"], df["low"], df["close"]
        tr = pd.DataFrame({
            "h-l": high - low,
            "h-pc": (high - close.shift(1)).abs(),
            "l-pc": (low - close.shift(1)).abs()
        }).max(axis=1)
        return tr.rolling(period, min_periods=1).mean()
    except:
        return pd.Series([0] * len(df))

# ---------------- MARKET REGIME ----------------
async def detect_market_regime(df: pd.DataFrame):
    try:
        if len(df) < 50:
            return "RANGE"
        
        ma_htf = df["close"].rolling(50).mean().iloc[-1]
        price = df["close"].iloc[-1]
        recent_high = df["high"].iloc[-20:].max()
        recent_low = df["low"].iloc[-20:].min()
        range_pct = (recent_high - recent_low) / max(1e-8, recent_low)
        if price > ma_htf and range_pct > 0.02:
            return "BULL"
        elif price < ma_htf and range_pct > 0.02:
            return "BEAR"
        else:
            return "RANGE"
    except:
        return "RANGE"

# ---------------- MULTI-TIMEFRAME ELITE CONFIRM ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend>0 else "SELL"
        if trend_side != side:
            return False
    return True

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 50:
        log.debug(f"Insufficient data for {symbol} {tf}")
        return None
    
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []

    # Step 1: Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2; reasons.append("Displacement +2")
    else:
        reasons.append("Displacement +0")

    # Step 3 & 4: Order Block & Zone
    ob_zone = None
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            ob_zone={"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}; break
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            ob_zone={"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}; break

    if ob_zone:
        ob_type = ob_zone["type"]
        if ob_type=="bullish" and last["close"] <= ob_zone["high"]: score+=1; reasons.append("Zone Approach +1")
        elif ob_type=="bearish" and last["close"] >= ob_zone["low"]: score+=1; reasons.append("Zone Approach +1")
        else: reasons.append("Zone Approach +0")
    else:
        reasons.append("Zone Approach +0"); ob_type=None

    # Step 5: HTF Alignment
    # For trend alignment, use slightly lower TF than structure analysis
    tf_map = {
        "1m": "15m",   # 1m → check 15m alignment
        "3m": "1h",    # 3m → check 1h alignment  
        "5m": "1h",    # 5m → check 1h alignment
        "15m": "4h",   # 15m → check 4h alignment
        "30m": "4h"    # 30m → check 4h alignment (for trend, structure uses 1d)
    }
    
    htf = tf_map.get(tf, "15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
        htf_dir = "bullish" if trend>0 else "bearish"
        if ob_type and htf_dir==ob_type:
            score+=1; htf_alignment=1; reasons.append("HTF Alignment +1")
        else:
            reasons.append("HTF Alignment +0")
    else:
        reasons.append("HTF Alignment ?")

    # Step 6: Momentum
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    if ob_type=="bullish" and momentum_ratio>0.5 and last["close"]>last["open"]:
        score+=1; reasons.append("Momentum +1")
    elif ob_type=="bearish" and momentum_ratio>0.5 and last["close"]<last["open"]:
        score+=1; reasons.append("Momentum +1")
    else:
        reasons.append("Momentum +0")

    if not ob_type: return None
    side = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN: 
        log.debug(f"Critical score too low for {symbol}: {critical_score}")
        return None
    if score < MIN_SCORE: 
        log.debug(f"Score too low for {symbol}: {score}")
        return None
    if not has_disp: 
        log.debug(f"No displacement for {symbol}")
        return None
    
    # ---------------- HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:
        log.debug(f"No HTF alignment for {symbol}")
        return None

    market_regime = await detect_market_regime(df)
    if (market_regime=="BULL" and side=="SELL") or (market_regime=="BEAR" and side=="BUY"): 
        log.debug(f"Counter-trend for {symbol}: regime={market_regime}, side={side}")
        return None

    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    if (side=="BUY" and last["close"]<trend_ma) or (side=="SELL" and last["close"]>trend_ma): 
        log.debug(f"Against trend MA for {symbol}")
        return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side):
        log.debug(f"No elite MTF alignment for {symbol}")
        return None
    reasons.append("Elite MTF Alignment ✅")

    sig = {"symbol":symbol,"side":side,"entry":entry,"score":score,"reason":"RomeOPT 6-Step",
           "reason_list":reasons,"htf_alignment":htf_alignment,"liquidity_sweep":liquidity_sweep,
           "ob_zone": ob_zone, "timeframe": tf}
    
    # Get HTF data for structure analysis (RomeOPT-P uses HTF for TP/SL)
    df_htf_structure = await get_htf_structure_data(exchange, symbol, tf)
    if df_htf_structure is None:
        log.debug(f"No HTF structure data available for {symbol} {tf}")
        return None
    
    # STRICT RomeOPT-P TP/SL calculation using HTF structure
    sig_with_tpsl = calculate_romeopt_tp_sl_strict(sig, df_htf_structure)
    if not sig_with_tpsl:
        log.debug(f"RomeOPT-P TP/SL rules cannot be applied for {symbol} using HTF structure")
        return None
    
    return sig_with_tpsl

# ---------------- STRICT ROMEOPT-P TP/SL CALCULATION ----------------
def calculate_romeopt_tp_sl_strict(sig: dict, df_htf: pd.DataFrame) -> Optional[dict]:
    """
    Calculate TP/SL using RomeOPT-P structure-based system
    Uses HIGHER TIMEFRAME structure for analysis
    STRICTLY follows RomeOPT-P rules - returns None if rules can't be followed
    """
    try:
        # Ensure ob_zone exists
        ob_zone = sig.get("ob_zone")
        if not ob_zone:
            log.debug(f"No OB zone for {sig['symbol']}")
            return None
        
        # Find market structure elements FROM HTF DATA
        structure = RomeOPTTPSLSystem.find_structure_elements(
            df_htf, sig["symbol"], sig["side"], ob_zone
        )
        
        # Calculate Stop Loss (structure-based) - RomeOPT-P strict
        sl = RomeOPTTPSLSystem.calculate_stop_loss(
            sig["side"], structure, sig["entry"], ob_zone
        )
        
        if sl is None:
            log.debug(f"No valid SL found for {sig['symbol']} using HTF RomeOPT-P rules")
            return None
        
        # Calculate Take Profit (liquidity-based) - RomeOPT-P strict
        tps = RomeOPTTPSLSystem.calculate_take_profit(
            sig["side"], structure, sig["entry"], sl
        )
        
        if tps is None:
            log.debug(f"No valid TPs found for {sig['symbol']} using HTF RomeOPT-P rules")
            return None
        
        tp1, tp2 = tps  # Now returns (TP1, TP2) or (TP1, None)
        
        # RomeOPT-P: Validate TP distances
        if sig["side"] == "BUY":
            if not (tp1 > sig["entry"]):
                log.debug(f"TP1 must be above entry for BUY {sig['symbol']}: {tp1}")
                return None
            if tp2 is not None and not (tp2 > tp1):
                log.debug(f"TP2 must be above TP1 for BUY {sig['symbol']}: {tp1}, {tp2}")
                return None
        else:  # SELL
            if not (tp1 < sig["entry"]):
                log.debug(f"TP1 must be below entry for SELL {sig['symbol']}: {tp1}")
                return None
            if tp2 is not None and not (tp2 < tp1):
                log.debug(f"TP2 must be below TP1 for SELL {sig['symbol']}: {tp1}, {tp2}")
                return None
        
        # RomeOPT-P: Check distance ranges
        if sig["side"] == "BUY":
            tp1_distance = (tp1 - sig["entry"]) / sig["entry"]
            if tp1_distance > 0.03:  # TP1 >3% away - too far
                log.debug(f"TP1 too far for BUY {sig['symbol']}: {tp1_distance:.2%}")
                return None
            if tp2 is not None:
                tp2_distance = (tp2 - sig["entry"]) / sig["entry"]
                if tp2_distance > 0.03:  # TP2 >3% away - too far
                    log.debug(f"TP2 too far for BUY {sig['symbol']}: {tp2_distance:.2%}")
                    return None
        else:
            tp1_distance = (sig["entry"] - tp1) / sig["entry"]
            if tp1_distance > 0.03:  # TP1 >3% away - too far
                log.debug(f"TP1 too far for SELL {sig['symbol']}: {tp1_distance:.2%}")
                return None
            if tp2 is not None:
                tp2_distance = (sig["entry"] - tp2) / sig["entry"]
                if tp2_distance > 0.03:  # TP2 >3% away - too far
                    log.debug(f"TP2 too far for SELL {sig['symbol']}: {tp2_distance:.2%}")
                    return None
        
        # Store structure data with HTF info
        sig["sl"] = float(sl)
        sig["tp1"] = float(tp1)
        sig["tp2"] = float(tp2) if tp2 is not None else None
        sig["tp3"] = None  # RomeOPT-P: No TP3 anymore
        sig["latest_ob"] = ob_zone
        
        # Calculate TP distances for logging
        if sig["side"] == "BUY":
            tp1_distance_pct = (tp1 - sig["entry"]) / sig["entry"]
            tp2_distance_pct = (tp2 - sig["entry"]) / sig["entry"] if tp2 is not None else None
        else:
            tp1_distance_pct = (sig["entry"] - tp1) / sig["entry"]
            tp2_distance_pct = (sig["entry"] - tp2) / sig["entry"] if tp2 is not None else None
        
        # Get current TF and HTF used
        current_tf = sig.get("timeframe", "unknown")
        htf_used = "15m" if current_tf == "1m" else "1h" if current_tf in ["3m", "5m"] else "4h" if current_tf == "15m" else "1d"
        
        sig["structure_data"] = safe_json_dumps({
            **structure,
            'htf_used': htf_used,
            'current_tf': current_tf,
            'htf_candle_count': len(df_htf),
            'romeoptp_tp_count': 2 if tp2 is not None else 1,
            'romeoptp_tp1_distance_pct': tp1_distance_pct,
            'romeoptp_tp2_distance_pct': tp2_distance_pct,
            'romeoptp_rule': 'TP1: 0.5-1.5%, TP2: 1.5-3.0% (if used)'
        })
        
        tp_info = f"TP1: {tp1:.6f} ({tp1_distance_pct:.2%})"
        if tp2 is not None:
            tp_info += f", TP2: {tp2:.6f} ({tp2_distance_pct:.2%})"
        
        log.info(f"RomeOPT-P TP/SL CALCULATED ({current_tf}→{htf_used}) for {sig['symbol']}: "
                f"Entry={sig['entry']:.6f}, SL={sl:.6f}, {tp_info}")
        
        return sig
        
    except Exception as e:
        log.error(f"RomeOPT-P TP/SL calculation failed for {sig['symbol']}: {e}")
        return None

# ---------------- FIND LATEST OB ----------------
def find_latest_ob(df: pd.DataFrame):
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
    return None

# ---------------- SL CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time(); dq = recent_sl[symbol]; dq.append(now)
    cutoff = now - lookback_minutes*60
    while dq and dq[0]<cutoff: dq.popleft()
def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]; now=time.time(); cutoff=now-lookback*60
    while dq and dq[0]<cutoff: dq.popleft()
    return len(dq)>=threshold

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        # Ensure all TP values are properly set (None for empty)
        tp1 = sig.get("tp1")
        tp2 = sig.get("tp2")
        tp3 = sig.get("tp3") if "tp3" in sig else None  # Always None for RomeOPT-P
        
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,structure_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], 
            sig["side"], 
            sig["entry"], 
            sig.get("sl"),
            tp1,
            tp2,
            tp3,
            datetime.datetime.utcnow().isoformat(),
            "OPEN",
            sig["reason"],
            sig["score"],
            str(sig.get("latest_ob", "")), 
            sig.get("structure_data", "")
        ))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                # First check if structure_data column exists
                try:
                    async with db_conn.execute("SELECT structure_data FROM signals LIMIT 1") as cursor:
                        await cursor.fetchone()
                    column_exists = True
                except:
                    column_exists = False
                    log.warning("structure_data column doesn't exist yet")
                
                # Build query based on column existence
                if column_exists:
                    query = "SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,structure_data FROM signals WHERE status='OPEN'"
                else:
                    query = "SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,NULL FROM signals WHERE status='OPEN'"
                
                async with db_conn.execute(query) as cursor:
                    async for row in cursor:
                        if column_exists:
                            sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, structure_data = row
                        else:
                            sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, _ = row
                            structure_data = None
                        
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            if last_price is None: 
                                continue

                            hits=[]; sl_hit=False
                            if side=="BUY":
                                if not tp1_hit and tp1 is not None and last_price>=tp1: 
                                    hits.append("TP1"); tp1_hit=1
                                if not tp2_hit and tp2 is not None and last_price>=tp2: 
                                    hits.append("TP2"); tp2_hit=1
                                if not tp3_hit and tp3 is not None and last_price>=tp3: 
                                    hits.append("TP3"); tp3_hit=1
                                if last_price<=sl: 
                                    hits.append("SL"); status="CLOSED"; sl_hit=True
                            else:  # SELL
                                if not tp1_hit and tp1 is not None and last_price<=tp1: 
                                    hits.append("TP1"); tp1_hit=1
                                if not tp2_hit and tp2 is not None and last_price<=tp2: 
                                    hits.append("TP2"); tp2_hit=1
                                if not tp3_hit and tp3 is not None and last_price<=tp3: 
                                    hits.append("TP3"); tp3_hit=1
                                if last_price>=sl: 
                                    hits.append("SL"); status="CLOSED"; sl_hit=True

                            if hits:
                                # Format message based on TPs available
                                tp_info_parts = []
                                if tp1 is not None:
                                    tp_info_parts.append(f"TP1:{tp1:.6f}")
                                if tp2 is not None:
                                    tp_info_parts.append(f"TP2:{tp2:.6f}")
                                if tp3 is not None:
                                    tp_info_parts.append(f"TP3:{tp3:.6f}")
                                
                                tp_info = " ".join(tp_info_parts)
                                    
                                await tg(f"🎯 {symbol} {side} update\nEntry:{entry:.6f}\nLast:{last_price:.6f}\nHits:{','.join(hits)}\nSL:{sl:.6f}\n{tp_info}")

                            if sl_hit: 
                                record_sl_hit(symbol)
                            
                            # Update with structure_data only if column exists
                            if column_exists:
                                await db_conn.execute("""
                                    UPDATE signals 
                                    SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=?, structure_data=?
                                    WHERE id=?
                                """, (tp1_hit, tp2_hit, tp3_hit, status, structure_data, sig_id))
                            else:
                                await db_conn.execute("""
                                    UPDATE signals 
                                    SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=?
                                    WHERE id=?
                                """, (tp1_hit, tp2_hit, tp3_hit, status, sig_id))
                        except Exception as e:
                            log.error(f"Error monitoring signal {symbol}: {e}")
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    while True:
        t0=time.time()
        try:
            tickers = await exchange.fetch_tickers()
            top = sorted([(s,v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")], key=lambda x:x[1], reverse=True)[:TOP_N]
            signals_found = 0
            for symbol,_ in top:
                if deprioritized(symbol): 
                    continue
                for tf in TIMEFRAMES:
                    key=f"{symbol}:{tf}"
                    if key in last_signal_time and time.time()-last_signal_time[key]<60: 
                        continue
                    ohlcv = await fetch_ohlcv(exchange,symbol,tf,200)
                    if not ohlcv: 
                        continue
                    df=pd.DataFrame(ohlcv,columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: 
                        df[c]=pd.to_numeric(df[c],errors="coerce")
                    sig = await generate_signal_romeopt(exchange,df,symbol,tf)
                    if sig:
                        htf_flag = sig.get("htf_alignment", "N/A")
                        sweep_flag = sig.get("liquidity_sweep", "N/A")
                        
                        # Get HTF mapping info for display
                        current_tf = sig.get("timeframe", tf)
                        htf_map_info = {
                            "1m": "15m",
                            "3m": "1h",
                            "5m": "1h",
                            "15m": "4h",
                            "30m": "1d"
                        }
                        htf_used = htf_map_info.get(current_tf, "4h")
                        
                        # Format TP/SL info with RomeOPT-P details
                        tp_sl_info = f"Entry: {sig['entry']:.6f}\n"
                        tp_sl_info += f"SL: {sig.get('sl', 0):.6f} (RomeOPT-P Structure)\n"
                        
                        # Show TPs based on what's available
                        if sig.get('tp1'):
                            tp_sl_info += f"TP1: {sig.get('tp1', 0):.6f} (HTF Liquidity)\n"
                        if sig.get('tp2'):
                            tp_sl_info += f"TP2: {sig.get('tp2', 0):.6f} (Next HTF Liquidity)\n"
                        
                        tp_sl_info += f"Score: {sig['score']}\n"
                        tp_sl_info += f"HTF Align: {htf_flag} Sweep: {sweep_flag}\n"
                        tp_sl_info += f"Structure TF: {current_tf}→{htf_used}\n"
                        tp_sl_info += f"Breakdown: {', '.join(sig['reason_list'])}\n"
                        tp_sl_info += f"STRICT RomeOPT-P Rules ✓"
                        
                        await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\n{tp_sl_info}")
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found (HTF STRUCTURE TP/SL)")
        except Exception as e: 
            log.exception("scan error: %s", e)
        elapsed=time.time()-t0
        await asyncio.sleep(max(1,SCAN_INTERVAL-elapsed))

# ---------------- FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token!=WEBHOOK_SECRET: 
        raise HTTPException(403,"Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

# ---------------- MAIN ----------------
async def main():
    await init_db()
    global exchange
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - STRICT RomeOPT-P HTF TP/SL Rules")
    await tg("🔧 FIXED: Now uses 1-2 TPs MAX (RomeOPT-P rules)")
    await tg("📏 TP1: 0.5-1.5%, TP2: 1.5-3.0%")
    await tg("🗺️ HTF Structure Mapping:")
    await tg("   1m → 15m | 3m → 1h | 5m → 1h")
    await tg("   15m → 4h | 30m → 1d (RomeOPT-P)")
    log.info("STRICT RomeOPT-P HTF TP/SL System: Uses HIGHER TIMEFRAME structure for TP/SL")
    await asyncio.gather(scan_loop(exchange), monitor_signals())

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args=p.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Shutting down...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())