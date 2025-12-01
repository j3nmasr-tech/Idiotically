#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic
- TP/SL tracking with RomeOPT-P structure-based system
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

# ---------------- ROMEOPT-P TP/SL SYSTEM ----------------
class RomeOPTTPSLSystem:
    """
    RomeOPT-P 6-Step TP/SL System
    Implements structure-based stop loss and liquidity-based take profit logic
    """
    
    @staticmethod
    def find_structure_elements(df: pd.DataFrame, symbol: str, side: str, ob_zone: dict) -> Dict[str, Any]:
        """
        Find all structure elements for TP/SL calculation
        """
        structure = {
            'order_blocks': [],
            'protected_levels': [],
            'fvgs': [],
            'bos_zones': [],
            'liquidity_pools': {'upside': [], 'downside': []}
        }
        
        if df is None or len(df) < 20:
            return structure
        
        try:
            # 1. Origin Order Block
            if ob_zone and isinstance(ob_zone, dict):
                structure['order_blocks'].append({
                    'type': 'bullish' if side == 'BUY' else 'bearish',
                    'high': float(ob_zone.get('high', 0)),
                    'low': float(ob_zone.get('low', 0)),
                    'origin': True
                })
            
            # 2. Protected levels (significant swing highs/lows)
            structure['protected_levels'].extend(
                RomeOPTTPSLSystem._find_protected_levels(df, side)
            )
            
            # 3. FVGs (Fair Value Gaps)
            structure['fvgs'].extend(
                RomeOPTTPSLSystem._find_fvgs(df, side)
            )
            
            # 4. BOS (Break of Structure) zones
            structure['bos_zones'].extend(
                RomeOPTTPSLSystem._find_bos_zones(df, side)
            )
            
            # 5. Liquidity pools
            structure['liquidity_pools'] = RomeOPTTPSLSystem._find_liquidity_pools(df, side)
            
        except Exception as e:
            log.error(f"Error finding structure elements for {symbol}: {e}")
        
        return structure
    
    @staticmethod
    def _find_protected_levels(df: pd.DataFrame, side: str) -> List[Dict]:
        """Find protected highs/lows (significant swing points)"""
        levels = []
        try:
            if len(df) < 20:
                return levels
                
            lookback = min(50, len(df) - 6)
            
            # Find swing highs
            for i in range(lookback, len(df) - 6):
                if i - 5 >= 0 and i + 6 <= len(df):
                    if float(df['high'].iloc[i]) == float(df['high'].iloc[i-5:i+6].max()):
                        levels.append({'type': 'high', 'price': float(df['high'].iloc[i])})
            
            # Find swing lows
            for i in range(lookback, len(df) - 6):
                if i - 5 >= 0 and i + 6 <= len(df):
                    if float(df['low'].iloc[i]) == float(df['low'].iloc[i-5:i+6].min()):
                        levels.append({'type': 'low', 'price': float(df['low'].iloc[i])})
                        
        except Exception as e:
            log.error(f"Error finding protected levels: {e}")
        
        return levels
    
    @staticmethod
    def _find_fvgs(df: pd.DataFrame, side: str) -> List[Dict]:
        """Find Fair Value Gaps"""
        fvgs = []
        try:
            if len(df) < 3:
                return fvgs
                
            for i in range(2, len(df)):
                # Bullish FVG: current low > previous high
                if float(df['low'].iloc[i]) > float(df['high'].iloc[i-1]):
                    fvgs.append({
                        'type': 'buy',
                        'high': float(df['low'].iloc[i]),
                        'low': float(df['high'].iloc[i-1]),
                        'origin_candle': {
                            'high': float(df['high'].iloc[i]),
                            'low': float(df['low'].iloc[i]),
                            'open': float(df['open'].iloc[i]),
                            'close': float(df['close'].iloc[i])
                        }
                    })
                # Bearish FVG: current high < previous low
                elif float(df['high'].iloc[i]) < float(df['low'].iloc[i-1]):
                    fvgs.append({
                        'type': 'sell',
                        'high': float(df['low'].iloc[i-1]),
                        'low': float(df['high'].iloc[i]),
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
    def _find_bos_zones(df: pd.DataFrame, side: str) -> List[Dict]:
        """Find Break of Structure zones"""
        bos_zones = []
        try:
            if len(df) < 25:
                return bos_zones
                
            window = min(20, len(df) - 5)
            
            for i in range(window, len(df) - 5):
                # Bullish BOS: price breaks above previous structure
                if float(df['high'].iloc[i]) > float(df['high'].iloc[i-window:i].max()):
                    bos_zones.append({
                        'type': 'buy',
                        'high': float(df['high'].iloc[i]),
                        'low': float(df['low'].iloc[i-window:i].min())
                    })
                # Bearish BOS: price breaks below previous structure
                elif float(df['low'].iloc[i]) < float(df['low'].iloc[i-window:i].min()):
                    bos_zones.append({
                        'type': 'sell',
                        'high': float(df['high'].iloc[i-window:i].max()),
                        'low': float(df['low'].iloc[i])
                    })
        except Exception as e:
            log.error(f"Error finding BOS zones: {e}")
        
        return bos_zones
    
    @staticmethod
    def _find_liquidity_pools(df: pd.DataFrame, side: str) -> Dict[str, List]:
        """Find liquidity pools (swing points, equal highs/lows, premium/discount zones)"""
        pools = {'upside': [], 'downside': []}
        
        try:
            if len(df) < 50:
                return pools
            
            # Swing highs (upside liquidity)
            for i in range(30, len(df) - 10):
                if i - 10 >= 0 and i + 11 <= len(df):
                    if float(df['high'].iloc[i]) == float(df['high'].iloc[i-10:i+11].max()):
                        pools['upside'].append({
                            'type': 'swing_high',
                            'price': float(df['high'].iloc[i]),
                            'strength': 3
                        })
            
            # Swing lows (downside liquidity)
            for i in range(30, len(df) - 10):
                if i - 10 >= 0 and i + 11 <= len(df):
                    if float(df['low'].iloc[i]) == float(df['low'].iloc[i-10:i+11].min()):
                        pools['downside'].append({
                            'type': 'swing_low',
                            'price': float(df['low'].iloc[i]),
                            'strength': 3
                        })
            
            # Equal highs
            if len(df) >= 20:
                recent_high = float(df['high'].iloc[-20:].max())
                pools['upside'].append({
                    'type': 'equal_high',
                    'price': recent_high,
                    'strength': 2
                })
            
            # Equal lows
            if len(df) >= 20:
                recent_low = float(df['low'].iloc[-20:].min())
                pools['downside'].append({
                    'type': 'equal_low',
                    'price': recent_low,
                    'strength': 2
                })
            
            # Premium range (62-70% of recent range)
            if len(df) >= 50:
                recent_high_max = float(df['high'].iloc[-50:].max())
                recent_low_min = float(df['low'].iloc[-50:].min())
                recent_range = recent_high_max - recent_low_min
                if recent_range > 0:
                    premium_level = recent_low_min + (recent_range * 0.66)
                    pools['upside'].append({
                        'type': 'premium_range',
                        'price': float(premium_level),
                        'strength': 1
                    })
                    
                    # Discount range (30-38% of recent range)
                    discount_level = recent_low_min + (recent_range * 0.34)
                    pools['downside'].append({
                        'type': 'discount_range',
                        'price': float(discount_level),
                        'strength': 1
                    })
                    
        except Exception as e:
            log.error(f"Error finding liquidity pools: {e}")
        
        return pools
    
    @staticmethod
    def calculate_stop_loss(side: str, structure: Dict, entry_price: float, ob_zone: dict) -> Optional[float]:
        """
        Calculate stop loss based on structure invalidation points
        Follows RomeOPT-P rules strictly
        """
        try:
            if side not in ['BUY', 'SELL']:
                return None
            
            setup_type = 'buy' if side == 'BUY' else 'sell'
            sl_candidates = []
            
            # 1. Check for BOS entry first (highest priority)
            bos_sl = RomeOPTTPSLSystem._get_bos_sl(setup_type, structure)
            if bos_sl is not None:
                sl_candidates.append(bos_sl)
            
            # 2. Check origin Order Block
            ob_sl = RomeOPTTPSLSystem._get_ob_sl(setup_type, structure, ob_zone)
            if ob_sl is not None:
                sl_candidates.append(ob_sl)
            
            # 3. Check protected levels
            protected_sl = RomeOPTTPSLSystem._get_protected_sl(setup_type, structure)
            if protected_sl is not None:
                sl_candidates.append(protected_sl)
            
            # 4. Check origin FVG candle
            fvg_sl = RomeOPTTPSLSystem._get_fvg_sl(setup_type, structure)
            if fvg_sl is not None:
                sl_candidates.append(fvg_sl)
            
            # Select the most appropriate SL
            final_sl = RomeOPTTPSLSystem._select_optimal_sl(setup_type, sl_candidates, entry_price)
            
            if final_sl is None:
                # Fallback: Use structure-based SL
                if side == 'BUY':
                    if ob_zone and 'low' in ob_zone:
                        final_sl = float(ob_zone['low']) * 0.998
                    else:
                        final_sl = entry_price * 0.99
                else:
                    if ob_zone and 'high' in ob_zone:
                        final_sl = float(ob_zone['high']) * 1.002
                    else:
                        final_sl = entry_price * 1.01
            
            return float(final_sl)
            
        except Exception as e:
            log.error(f"Error calculating stop loss: {e}")
            return None
    
    @staticmethod
    def _get_bos_sl(setup_type: str, structure: Dict) -> Optional[float]:
        """Get SL from BOS zone"""
        try:
            for bos in structure.get('bos_zones', []):
                if bos.get('type') == setup_type:
                    if setup_type == 'buy':
                        return float(bos.get('low', 0)) * 0.999
                    else:
                        return float(bos.get('high', 0)) * 1.001
        except:
            pass
        return None
    
    @staticmethod
    def _get_ob_sl(setup_type: str, structure: Dict, ob_zone: dict) -> Optional[float]:
        """Get SL from origin Order Block"""
        try:
            origin_obs = [ob for ob in structure.get('order_blocks', []) 
                         if ob.get('origin', False) and ob.get('type') == setup_type]
            
            if not origin_obs and ob_zone:
                if setup_type == 'buy' and 'low' in ob_zone:
                    return float(ob_zone['low']) * 0.998
                elif setup_type == 'sell' and 'high' in ob_zone:
                    return float(ob_zone['high']) * 1.002
            
            if not origin_obs:
                return None
            
            best_ob = origin_obs[0]
            if setup_type == 'buy':
                return float(best_ob.get('low', 0)) * 0.998
            else:
                return float(best_ob.get('high', 0)) * 1.002
        except:
            return None
    
    @staticmethod
    def _get_protected_sl(setup_type: str, structure: Dict) -> Optional[float]:
        """Get SL from protected levels"""
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
        """Get SL from origin FVG candle"""
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
        
        valid_sls = []
        for sl in sl_candidates:
            if sl is None:
                continue
            if setup_type == 'buy':
                if sl < entry_price:
                    valid_sls.append(float(sl))
            else:
                if sl > entry_price:
                    valid_sls.append(float(sl))
        
        if not valid_sls:
            return None
        
        if setup_type == 'buy':
            return max(valid_sls)
        else:
            return min(valid_sls)
    
    @staticmethod
    def calculate_take_profit(side: str, structure: Dict, entry_price: float, sl_price: float) -> Tuple[float, float, float]:
        """
        Calculate take profit based on liquidity pools
        Returns: (tp1, tp2, tp3)
        """
        try:
            if side == 'BUY':
                liquidity_pools = structure.get('liquidity_pools', {}).get('upside', [])
                liquidity_pools.sort(key=lambda x: x.get('price', 0))
                
                valid_pools = []
                for pool in liquidity_pools:
                    pool_price = pool.get('price', 0)
                    if pool_price <= entry_price:
                        continue
                    
                    risk = abs(entry_price - sl_price)
                    if risk == 0:
                        continue
                    
                    reward = abs(pool_price - entry_price)
                    rr = reward / risk
                    
                    if rr < 0.5:
                        continue
                    
                    tp_price = pool_price * 0.999
                    valid_pools.append((tp_price, pool.get('strength', 1)))
                
                return RomeOPTTPSLSystem._select_tp_levels(valid_pools, entry_price, side)
            
            else:  # SELL
                liquidity_pools = structure.get('liquidity_pools', {}).get('downside', [])
                liquidity_pools.sort(key=lambda x: x.get('price', 0), reverse=True)
                
                valid_pools = []
                for pool in liquidity_pools:
                    pool_price = pool.get('price', 0)
                    if pool_price >= entry_price:
                        continue
                    
                    risk = abs(sl_price - entry_price)
                    if risk == 0:
                        continue
                    
                    reward = abs(entry_price - pool_price)
                    rr = reward / risk
                    
                    if rr < 0.5:
                        continue
                    
                    tp_price = pool_price * 1.001
                    valid_pools.append((tp_price, pool.get('strength', 1)))
                
                return RomeOPTTPSLSystem._select_tp_levels(valid_pools, entry_price, side)
                
        except Exception as e:
            log.error(f"Error calculating take profit: {e}")
            # Fallback TPs
            if side == 'BUY':
                return entry_price * 1.005, entry_price * 1.01, entry_price * 1.015
            else:
                return entry_price * 0.995, entry_price * 0.99, entry_price * 0.985
    
    @staticmethod
    def _select_tp_levels(valid_pools: List[Tuple[float, int]], entry_price: float, side: str) -> Tuple[float, float, float]:
        """Select TP levels with proper spacing"""
        try:
            if not valid_pools:
                if side == 'BUY':
                    tp1 = entry_price * 1.005
                    tp2 = entry_price * 1.01
                    tp3 = entry_price * 1.015
                else:
                    tp1 = entry_price * 0.995
                    tp2 = entry_price * 0.99
                    tp3 = entry_price * 0.985
                return tp1, tp2, tp3
            
            valid_pools.sort(key=lambda x: x[1], reverse=True)
            
            selected_tps = []
            min_distance = abs(entry_price * 0.002)
            
            for tp_price, strength in valid_pools:
                if len(selected_tps) >= 3:
                    break
                
                if selected_tps:
                    too_close = False
                    for existing_tp in selected_tps:
                        if abs(tp_price - existing_tp) < min_distance:
                            too_close = True
                            break
                    if too_close:
                        continue
                
                selected_tps.append(tp_price)
            
            while len(selected_tps) < 3:
                if side == 'BUY':
                    last_tp = selected_tps[-1] if selected_tps else entry_price
                    selected_tps.append(last_tp * 1.005)
                else:
                    last_tp = selected_tps[-1] if selected_tps else entry_price
                    selected_tps.append(last_tp * 0.995)
            
            selected_tps.sort()
            if side == 'SELL':
                selected_tps.sort(reverse=True)
            
            return selected_tps[0], selected_tps[1], selected_tps[2]
            
        except Exception as e:
            log.error(f"Error selecting TP levels: {e}")
            if side == 'BUY':
                return entry_price * 1.005, entry_price * 1.01, entry_price * 1.015
            else:
                return entry_price * 0.995, entry_price * 0.99, entry_price * 0.985

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
    await db_conn.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug("fetch_ohlcv failed for %s %s: %s", symbol, timeframe, e)
        return None

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
    if df is None or len(df) < 20: return None
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
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
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
    if critical_score < CRITICAL_FACTORS_MIN: return None
    if score < MIN_SCORE: return None
    if not has_disp: return None
    
    # ---------------- NEW: HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:
        return None

    market_regime = await detect_market_regime(df)
    if (market_regime=="BULL" and side=="SELL") or (market_regime=="BEAR" and side=="BUY"): return None

    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    if (side=="BUY" and last["close"]<trend_ma) or (side=="SELL" and last["close"]>trend_ma): return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side):
        return None
    reasons.append("Elite MTF Alignment ✅")

    sig = {"symbol":symbol,"side":side,"entry":entry,"score":score,"reason":"RomeOPT 6-Step",
           "reason_list":reasons,"htf_alignment":htf_alignment,"liquidity_sweep":liquidity_sweep,
           "ob_zone": ob_zone, "timeframe": tf}
    
    # Try to calculate RomeOPT-P TP/SL - if fails, IGNORE THE SIGNAL
    sig_with_tpsl = calculate_romeopt_tp_sl(sig, df)
    if not sig_with_tpsl:
        log.debug(f"RomeOPT-P TP/SL calculation failed for {symbol}, ignoring signal")
        return None
    
    return sig_with_tpsl

# ---------------- ROMEOPT-P TP/SL CALCULATION ----------------
def calculate_romeopt_tp_sl(sig: dict, df: pd.DataFrame) -> Optional[dict]:
    """
    Calculate TP/SL using RomeOPT-P structure-based system
    Returns None if calculation fails
    """
    try:
        # Ensure ob_zone exists
        ob_zone = sig.get("ob_zone")
        if not ob_zone:
            log.debug(f"No OB zone for {sig['symbol']}")
            return None
        
        # Find market structure elements
        structure = RomeOPTTPSLSystem.find_structure_elements(
            df, sig["symbol"], sig["side"], ob_zone
        )
        
        # Calculate Stop Loss (structure-based)
        sl = RomeOPTTPSLSystem.calculate_stop_loss(
            sig["side"], structure, sig["entry"], ob_zone
        )
        
        if sl is None:
            log.debug(f"Failed to calculate SL for {sig['symbol']}")
            return None
        
        # Calculate Take Profit (liquidity-based)
        tp1, tp2, tp3 = RomeOPTTPSLSystem.calculate_take_profit(
            sig["side"], structure, sig["entry"], sl
        )
        
        # Validate TPs are reasonable
        if sig["side"] == "BUY":
            if tp1 <= sig["entry"] or tp2 <= tp1 or tp3 <= tp2:
                log.debug(f"Invalid TP ordering for BUY {sig['symbol']}")
                return None
        else:  # SELL
            if tp1 >= sig["entry"] or tp2 >= tp1 or tp3 >= tp2:
                log.debug(f"Invalid TP ordering for SELL {sig['symbol']}")
                return None
        
        # TP1 distance filter
        risk = abs(sig["entry"] - sl)
        tp1_distance = abs(tp1 - sig["entry"])
        
        # Reject if TP1 is less than 10% of risk
        if risk > 0 and tp1_distance < risk * 0.1:
            log.debug(f"TP1 distance too small for {sig['symbol']}")
            return None
        
        # Store structure data for monitoring
        sig["sl"] = float(sl)
        sig["tp1"] = float(tp1)
        sig["tp2"] = float(tp2)
        sig["tp3"] = float(tp3)
        sig["latest_ob"] = ob_zone
        sig["structure_data"] = safe_json_dumps(structure)
        
        log.info(f"RomeOPT-P TP/SL calculated for {sig['symbol']}: SL={sl:.6f}, TP1={tp1:.6f}, TP2={tp2:.6f}, TP3={tp3:.6f}")
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

# ---------------- UPDATE TP/SL LIVE (Original as fallback - NOT USED) ----------------
def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    """
    Original TP/SL logic - kept but not used when RomeOPT-P fails
    """
    try:
        latest_ob = find_latest_ob(df)
        if not latest_ob: return sig
        atr_val = float(atr(df,14).iloc[-1])
        entry = sig["entry"]
        side = sig["side"]
        
        recent_high = df['high'].iloc[-10:].max()
        recent_low = df['low'].iloc[-10:].min()
        
        if side == "BUY":
            sl_ob = latest_ob["low"] - (atr_val * 0.3)
            sl_structure = recent_low - (atr_val * 0.3)
            sl = min(sl_ob, sl_structure)
            risk = entry - sl
            min_risk = atr_val * 0.5
            if risk < min_risk:
                risk = min_risk
                sl = entry - risk
            tp1 = entry + (risk * 0.8)
            tp2 = entry + (risk * 1.5)
            tp3 = entry + (risk * 2.5)
        else:
            sl_ob = latest_ob["high"] + (atr_val * 0.3)
            sl_structure = recent_high + (atr_val * 0.3)
            sl = max(sl_ob, sl_structure)
            risk = sl - entry
            min_risk = atr_val * 0.5
            if risk < min_risk:
                risk = min_risk
                sl = entry + risk
            tp1 = entry - (risk * 0.8)
            tp2 = entry - (risk * 1.5)
            tp3 = entry - (risk * 2.5)
        
        sig["sl"]=sl; sig["tp1"]=tp1; sig["tp2"]=tp2; sig["tp3"]=tp3
        sig["latest_ob"]=latest_ob
        return sig
    except:
        return sig

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
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,structure_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["symbol"],sig["side"],sig["entry"],sig.get("sl"),sig.get("tp1"),sig.get("tp2"),sig.get("tp3"),
              datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"],
              str(sig.get("latest_ob","")), sig.get("structure_data", "")))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,structure_data FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, structure_data = row
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            if last_price is None: continue

                            hits=[]; sl_hit=False
                            if side=="BUY":
                                if not tp1_hit and last_price>=tp1: hits.append("TP1"); tp1_hit=1
                                if not tp2_hit and last_price>=tp2: hits.append("TP2"); tp2_hit=1
                                if not tp3_hit and last_price>=tp3: hits.append("TP3"); tp3_hit=1
                                if last_price<=sl: hits.append("SL"); status="CLOSED"; sl_hit=True
                            else:
                                if not tp1_hit and last_price<=tp1: hits.append("TP1"); tp1_hit=1
                                if not tp2_hit and last_price<=tp2: hits.append("TP2"); tp2_hit=1
                                if not tp3_hit and last_price<=tp3: hits.append("TP3"); tp3_hit=1
                                if last_price>=sl: hits.append("SL"); status="CLOSED"; sl_hit=True

                            if hits:
                                await tg(f"🎯 {symbol} {side} update\nEntry:{entry:.6f}\nLast:{last_price:.6f}\nHits:{','.join(hits)}\nSL:{sl:.6f}\nTP1:{tp1:.6f} TP2:{tp2:.6f} TP3:{tp3:.6f}")

                            if sl_hit: record_sl_hit(symbol)
                            await db_conn.execute("UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=? WHERE id=?",
                                                 (tp1_hit,tp2_hit,tp3_hit,status,sig_id))
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
                        
                        tp_sl_info = f"Entry: {sig['entry']:.6f}\n"
                        tp_sl_info += f"SL: {sig.get('sl', 0):.6f} (RomeOPT-P)\n"
                        tp_sl_info += f"TP1: {sig.get('tp1', 0):.6f}\n"
                        tp_sl_info += f"TP2: {sig.get('tp2', 0):.6f}\n"
                        tp_sl_info += f"TP3: {sig.get('tp3', 0):.6f}\n"
                        tp_sl_info += f"Score: {sig['score']}\n"
                        tp_sl_info += f"HTF: {htf_flag} Sweep: {sweep_flag}\n"
                        tp_sl_info += f"Breakdown: {', '.join(sig['reason_list'])}"
                        
                        await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\n{tp_sl_info}")
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
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
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals with RomeOPT-P TP/SL System")
    log.info("RomeOPT-P TP/SL System: Signals will be IGNORED if TP/SL calculation fails")
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