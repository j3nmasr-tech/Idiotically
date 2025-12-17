#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v2 - Complete with Trade Outcome Tracking
Multi-Timeframe Scanner with TP/SL Updates
"""

import os
import time
import asyncio
import logging
import datetime
import json
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from collections import defaultdict

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/romeopt_v2.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 25))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 10))

# Multi-timeframe config
TIMEFRAMES_TO_SCAN = os.getenv("TIMEFRAMES", "1m,3m,5m,15m,30m,1h,2h,4h").split(",")
TIMEFRAME_MIN_SCORES = {
    "1m": 3.8,
    "3m": 3.7,
    "5m": 3.6,
    "15m": 3.5,
    "30m": 3.5,
    "1h": 3.5,
    "2h": 3.5,
    "4h": 3.5
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_v2")
db_lock = asyncio.Lock()
db_conn = None
trade_tracker = None

# ---------------- DATA STRUCTURES ----------------
@dataclass
class HTFContext:
    bias: str
    range_high: float
    range_low: float
    range_mid: float
    premium_discount: str
    liquidity_zones: List[Dict]
    structure: List[Dict]
    skip_reason: Optional[str] = None
    valid: bool = False

@dataclass
class LiquidityMap:
    from_liquidity: List[Dict]
    to_liquidity: List[Dict]
    has_clear_target: bool = False

@dataclass
class SweepAnalysis:
    type: str
    candle_index: int
    swept_price: float
    previous_extreme: float
    impulsive: bool
    fake_sweep: bool = False
    strength: float = 0.0

@dataclass
class StructureShift:
    type: str
    confirmed: bool
    candle_index: int
    description: str = ""

@dataclass
class EntryZone:
    type: str
    price: float
    low: float
    high: float
    aligns_with_htf: bool
    candle_reaction: bool = False

@dataclass
class RiskManagement:
    sl_price: float
    invalidation_type: str
    risk_amount: float
    sl_to_entry_distance: float

@dataclass
class TakeProfitLevels:
    tp1: float
    tp2: float
    tp3: float
    tp1_type: str = "INTERNAL_LIQUIDITY"
    tp2_type: str = "RANGE_BOUNDARY"
    tp3_type: str = "HTF_LIQUIDITY"

@dataclass
class ProbabilityScore:
    htf_alignment: float
    liquidity_quality: float
    sweep_strength: float
    structure_clarity: float
    entry_precision: float
    total_score: float
    
    @property
    def acceptable(self) -> bool:
        return (self.total_score >= 3.5 and
                all([self.htf_alignment >= 0.5,
                     self.liquidity_quality >= 0.5,
                     self.sweep_strength >= 0.5,
                     self.structure_clarity >= 0.5,
                     self.entry_precision >= 0.5]))

# ---------------- TELEGRAM ----------------
async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    
    # Signals table
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timeframe TEXT,
            timestamp TEXT,
            side TEXT,
            htf_bias TEXT,
            htf_range_high REAL,
            htf_range_low REAL,
            htf_premium_discount TEXT,
            htf_liquidity_zones_json TEXT,
            htf_structure_json TEXT,
            liquidity_from_json TEXT,
            liquidity_to_json TEXT,
            has_clear_target BOOLEAN,
            sweep_type TEXT,
            swept_price REAL,
            sweep_impulsive BOOLEAN,
            sweep_strength REAL,
            structure_shift_type TEXT,
            structure_shift_confirmed BOOLEAN,
            structure_description TEXT,
            entry_type TEXT,
            entry_price REAL,
            entry_low REAL,
            entry_high REAL,
            entry_aligns_htf BOOLEAN,
            entry_reaction_confirmed BOOLEAN,
            sl_price REAL,
            sl_invalidation_type TEXT,
            risk_amount REAL,
            sl_distance_pct REAL,
            tp1_price REAL,
            tp1_type TEXT,
            tp2_price REAL,
            tp2_type TEXT,
            tp3_price REAL,
            tp3_type TEXT,
            prob_htf_alignment REAL,
            prob_liquidity_quality REAL,
            prob_sweep_strength REAL,
            prob_structure_clarity REAL,
            prob_entry_precision REAL,
            prob_total_score REAL,
            prob_acceptable BOOLEAN,
            current_price REAL,
            status TEXT DEFAULT 'DETECTED',
            notes TEXT
        )
    """)
    
    # Trade outcomes table
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            symbol TEXT,
            timeframe TEXT,
            side TEXT,
            entry_price REAL,
            current_price REAL,
            sl_price REAL,
            tp1_price REAL,
            tp2_price REAL,
            tp3_price REAL,
            hit_tp1 BOOLEAN DEFAULT 0,
            hit_tp2 BOOLEAN DEFAULT 0,
            hit_tp3 BOOLEAN DEFAULT 0,
            hit_sl BOOLEAN DEFAULT 0,
            tp1_time TEXT,
            tp2_time TEXT,
            tp3_time TEXT,
            sl_time TEXT,
            max_profit_pct REAL DEFAULT 0,
            max_drawdown_pct REAL DEFAULT 0,
            status TEXT DEFAULT 'OPEN',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (signal_id) REFERENCES signals (id) ON DELETE CASCADE
        )
    """)
    
    # Create indices
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_outcomes_status ON trade_outcomes(status)")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_outcomes_symbol ON trade_outcomes(symbol)")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_outcomes_signal ON trade_outcomes(signal_id)")
    
    await db_conn.commit()

# ---------------- TRADE OUTCOME TRACKER ----------------
class TradeOutcomeTracker:
    def __init__(self, db_conn, check_interval: int = 30):
        self.db_conn = db_conn
        self.check_interval = check_interval
        self.active_trade_ids = set()
        
    async def add_trade(self, setup: Dict) -> int:
        """Add a new trade to track"""
        
        async with db_lock:
            cursor = await self.db_conn.execute(
                "SELECT id FROM signals WHERE symbol = ? AND timestamp = ? ORDER BY id DESC LIMIT 1",
                (setup["symbol"], setup["timestamp"])
            )
            signal_row = await cursor.fetchone()
            signal_id = signal_row[0] if signal_row else None
            
            cursor = await self.db_conn.execute("""
                INSERT INTO trade_outcomes (
                    signal_id, symbol, timeframe, side, entry_price, current_price,
                    sl_price, tp1_price, tp2_price, tp3_price, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_id,
                setup["symbol"],
                setup.get("timeframe", "15m"),
                setup["side"],
                setup["entry_price"],
                setup["current_price"],
                setup["sl_price"],
                setup["tp1_price"],
                setup["tp2_price"],
                setup["tp3_price"],
                "OPEN"
            ))
            
            trade_id = cursor.lastrowid
            await self.db_conn.commit()
        
        self.active_trade_ids.add(trade_id)
        log.info(f"📊 Tracking trade #{trade_id}: {setup['symbol']} {setup['side']}")
        
        return trade_id
    
    async def check_trade_outcomes(self, exchange):
        """Check all open trades for TP/SL hits"""
        
        if not self.active_trade_ids:
            return
        
        trade_ids_to_remove = set()
        
        for trade_id in list(self.active_trade_ids):
            try:
                async with db_lock:
                    cursor = await self.db_conn.execute(
                        """SELECT symbol, side, entry_price, sl_price, 
                                  tp1_price, tp2_price, tp3_price, status,
                                  hit_tp1, hit_tp2, hit_tp3, hit_sl,
                                  max_profit_pct, max_drawdown_pct
                           FROM trade_outcomes 
                           WHERE id = ?""",
                        (trade_id,)
                    )
                    trade = await cursor.fetchone()
                
                if not trade:
                    trade_ids_to_remove.add(trade_id)
                    continue
                
                (symbol, side, entry_price, sl_price, tp1_price, tp2_price, tp3_price, 
                 status, hit_tp1, hit_tp2, hit_tp3, hit_sl, max_profit, max_drawdown) = trade
                
                if status != "OPEN":
                    trade_ids_to_remove.add(trade_id)
                    continue
                
                ticker = await exchange.fetch_ticker(symbol)
                current_price = ticker["last"]
                
                if side == "BUY":
                    profit_pct = ((current_price - entry_price) / entry_price) * 100
                    drawdown_pct = ((entry_price - current_price) / entry_price) * 100
                else:
                    profit_pct = ((entry_price - current_price) / entry_price) * 100
                    drawdown_pct = ((current_price - entry_price) / entry_price) * 100
                
                new_max_profit = max(max_profit or 0, profit_pct)
                new_max_drawdown = max(max_drawdown or 0, drawdown_pct)
                
                hit_tp1_new = hit_tp2_new = hit_tp3_new = hit_sl_new = False
                
                if side == "BUY":
                    if current_price >= tp1_price and not hit_tp1:
                        hit_tp1_new = True
                    if current_price >= tp2_price and not hit_tp2:
                        hit_tp2_new = True
                    if current_price >= tp3_price and not hit_tp3:
                        hit_tp3_new = True
                    if current_price <= sl_price and not hit_sl:
                        hit_sl_new = True
                else:
                    if current_price <= tp1_price and not hit_tp1:
                        hit_tp1_new = True
                    if current_price <= tp2_price and not hit_tp2:
                        hit_tp2_new = True
                    if current_price <= tp3_price and not hit_tp3:
                        hit_tp3_new = True
                    if current_price >= sl_price and not hit_sl:
                        hit_sl_new = True
                
                if hit_tp1_new:
                    await self.alert_tp_hit(trade_id, symbol, side, "TP1", entry_price, current_price, profit_pct)
                    await self.update_trade_hit(trade_id, "TP1", current_price, new_max_profit, new_max_drawdown)
                
                if hit_tp2_new:
                    await self.alert_tp_hit(trade_id, symbol, side, "TP2", entry_price, current_price, profit_pct)
                    await self.update_trade_hit(trade_id, "TP2", current_price, new_max_profit, new_max_drawdown)
                
                if hit_tp3_new:
                    await self.alert_tp_hit(trade_id, symbol, side, "TP3", entry_price, current_price, profit_pct)
                    await self.update_trade_hit(trade_id, "TP3", current_price, new_max_profit, new_max_drawdown)
                
                if hit_sl_new:
                    await self.alert_sl_hit(trade_id, symbol, side, entry_price, current_price, drawdown_pct)
                    await self.update_trade_hit(trade_id, "SL", current_price, new_max_profit, new_max_drawdown)
                    trade_ids_to_remove.add(trade_id)
                
                if new_max_profit != max_profit or new_max_drawdown != max_drawdown:
                    await self.update_trade_stats(trade_id, current_price, new_max_profit, new_max_drawdown)
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                log.error(f"Error checking trade {trade_id}: {e}")
        
        for trade_id in trade_ids_to_remove:
            self.active_trade_ids.discard(trade_id)
    
    async def alert_tp_hit(self, trade_id: int, symbol: str, side: str, 
                          tp_level: str, entry: float, price: float, profit_pct: float):
        msg = f"""
🎯 <b>TAKE PROFIT {tp_level} HIT!</b>

<b>Trade #{trade_id}:</b> {symbol} {side}
<b>TP Level:</b> {tp_level}
<b>Entry:</b> {entry:.8f}
<b>Hit Price:</b> {price:.8f}
<b>Profit:</b> {profit_pct:.2f}%

<i>Time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
        await send_telegram(msg)
        log.info(f"✅ {symbol} hit {tp_level} at {price:.8f} (+{profit_pct:.2f}%)")
    
    async def alert_sl_hit(self, trade_id: int, symbol: str, side: str, 
                          entry: float, price: float, loss_pct: float):
        msg = f"""
🛑 <b>STOP LOSS HIT!</b>

<b>Trade #{trade_id}:</b> {symbol} {side}
<b>Entry:</b> {entry:.8f}
<b>SL Price:</b> {price:.8f}
<b>Loss:</b> {loss_pct:.2f}%

<i>Time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
        await send_telegram(msg)
        log.info(f"🛑 {symbol} hit SL at {price:.8f} (-{loss_pct:.2f}%)")
    
    async def update_trade_hit(self, trade_id: int, hit_type: str, current_price: float,
                             max_profit: float, max_drawdown: float):
        now = datetime.datetime.utcnow().isoformat()
        
        async with db_lock:
            if hit_type == "TP1":
                await self.db_conn.execute("""
                    UPDATE trade_outcomes 
                    SET hit_tp1 = 1, tp1_time = ?, status = 'TP1_HIT',
                        current_price = ?, max_profit_pct = ?, max_drawdown_pct = ?,
                        updated_at = ?
                    WHERE id = ?
                """, (now, current_price, max_profit, max_drawdown, now, trade_id))
            elif hit_type == "TP2":
                await self.db_conn.execute("""
                    UPDATE trade_outcomes 
                    SET hit_tp2 = 1, tp2_time = ?, status = 'TP2_HIT',
                        current_price = ?, max_profit_pct = ?, max_drawdown_pct = ?,
                        updated_at = ?
                    WHERE id = ?
                """, (now, current_price, max_profit, max_drawdown, now, trade_id))
            elif hit_type == "TP3":
                await self.db_conn.execute("""
                    UPDATE trade_outcomes 
                    SET hit_tp3 = 1, tp3_time = ?, status = 'TP3_HIT',
                        current_price = ?, max_profit_pct = ?, max_drawdown_pct = ?,
                        updated_at = ?
                    WHERE id = ?
                """, (now, current_price, max_profit, max_drawdown, now, trade_id))
            elif hit_type == "SL":
                await self.db_conn.execute("""
                    UPDATE trade_outcomes 
                    SET hit_sl = 1, sl_time = ?, status = 'SL_HIT',
                        current_price = ?, max_profit_pct = ?, max_drawdown_pct = ?,
                        updated_at = ?
                    WHERE id = ?
                """, (now, current_price, max_profit, max_drawdown, now, trade_id))
            
            await self.db_conn.commit()
    
    async def update_trade_stats(self, trade_id: int, current_price: float,
                               max_profit: float, max_drawdown: float):
        now = datetime.datetime.utcnow().isoformat()
        
        async with db_lock:
            await self.db_conn.execute("""
                UPDATE trade_outcomes 
                SET current_price = ?, max_profit_pct = ?, max_drawdown_pct = ?, updated_at = ?
                WHERE id = ?
            """, (current_price, max_profit, max_drawdown, now, trade_id))
            await self.db_conn.commit()

# ---------------- OHLCV UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv):
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def safe_json_serialize(obj):
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, (np.ndarray, pd.Series)):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    elif isinstance(obj, dict):
        return {k: safe_json_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_json_serialize(item) for item in obj]
    else:
        return obj

# ---------------- STEP 1: HTF BIAS ----------------
async def analyze_htf_bias(exchange, symbol: str) -> HTFContext:
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, "4h", 100)
    timeframe_used = "4h"
    
    if not ohlcv_htf or len(ohlcv_htf) < 30:
        log.debug(f"{symbol}: 4H data insufficient, falling back to 1H...")
        ohlcv_htf = await fetch_ohlcv(exchange, symbol, "1h", 100)
        timeframe_used = "1h"
        
        if not ohlcv_htf or len(ohlcv_htf) < 30:
            return HTFContext(
                bias="UNKNOWN", range_high=0, range_low=0, range_mid=0,
                premium_discount="UNKNOWN", liquidity_zones=[], structure=[],
                skip_reason="Insufficient HTF data", valid=False
            )
    
    df_htf = create_dataframe(ohlcv_htf)
    current_price = float(df_htf["close"].iloc[-1])
    
    swing_highs = []
    swing_lows = []
    
    for i in range(3, len(df_htf) - 3):
        high_i = df_htf["high"].iloc[i]
        low_i = df_htf["low"].iloc[i]
        
        if (high_i > df_htf["high"].iloc[i-1] and 
            high_i > df_htf["high"].iloc[i-2] and
            high_i > df_htf["high"].iloc[i+1] and
            high_i > df_htf["high"].iloc[i+2]):
            swing_highs.append({
                "price": float(high_i),
                "index": int(i),
                "timestamp": int(df_htf["timestamp"].iloc[i])
            })
        
        if (low_i < df_htf["low"].iloc[i-1] and 
            low_i < df_htf["low"].iloc[i-2] and
            low_i < df_htf["low"].iloc[i+1] and
            low_i < df_htf["low"].iloc[i+2]):
            swing_lows.append({
                "price": float(low_i),
                "index": int(i),
                "timestamp": int(df_htf["timestamp"].iloc[i])
            })
    
    if len(df_htf) >= 20:
        recent_high = df_htf["high"].iloc[-20:].max()
        recent_low = df_htf["low"].iloc[-20:].min()
    else:
        recent_high = df_htf["high"].max()
        recent_low = df_htf["low"].min()
    
    range_high = float(recent_high)
    range_low = float(recent_low)
    range_mid = (range_high + range_low) / 2
    
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        last_two_highs = sorted([h["price"] for h in swing_highs[-2:]], reverse=True)
        last_two_lows = sorted([l["price"] for l in swing_lows[-2:]])
        
        if last_two_highs[0] > last_two_highs[1] and last_two_lows[0] < last_two_lows[1]:
            bias = "BULLISH"
        elif last_two_highs[0] < last_two_highs[1] and last_two_lows[0] > last_two_lows[1]:
            bias = "BEARISH"
        else:
            bias = "RANGING"
    else:
        bias = "RANGING"
    
    range_height = range_high - range_low
    if range_height > 0:
        position_pct = (current_price - range_low) / range_height * 100
        if position_pct > 60:
            premium_discount = "PREMIUM"
        elif position_pct < 40:
            premium_discount = "DISCOUNT"
        else:
            premium_discount = "MIDDLE"
    else:
        premium_discount = "MIDDLE"
    
    liquidity_zones = []
    
    liquidity_zones.append({
        "price": range_high,
        "type": "RANGE_HIGH",
        "timeframe": timeframe_used,
        "strength": 3
    })
    liquidity_zones.append({
        "price": range_low,
        "type": "RANGE_LOW",
        "timeframe": timeframe_used,
        "strength": 3
    })
    
    for swing in swing_highs[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_HIGH",
            "timeframe": timeframe_used,
            "strength": 2
        })
    
    for swing in swing_lows[-3:]:
        liquidity_zones.append({
            "price": swing["price"],
            "type": "SWING_LOW",
            "timeframe": timeframe_used,
            "strength": 2
        })
    
    skip_reason = None
    valid = True
    
    if premium_discount == "MIDDLE" and bias == "RANGING":
        skip_reason = "Price mid-range with no clear HTF alignment"
        valid = False
    elif range_height / range_low < 0.02:
        skip_reason = "Range too tight (<2%)"
        valid = False
    
    return HTFContext(
        bias=bias,
        range_high=range_high,
        range_low=range_low,
        range_mid=range_mid,
        premium_discount=premium_discount,
        liquidity_zones=liquidity_zones,
        structure=swing_highs[-5:] + swing_lows[-5:],
        skip_reason=skip_reason,
        valid=valid
    )

# ---------------- STEP 2: LIQUIDITY MAP ----------------
async def map_liquidity(exchange, symbol: str, htf_context: HTFContext, 
                       current_price: float) -> LiquidityMap:
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 100)
    if not ohlcv_1h:
        return LiquidityMap(from_liquidity=[], to_liquidity=[], has_clear_target=False)
    
    df_1h = create_dataframe(ohlcv_1h)
    
    from_liquidity = []
    
    recent_df = df_1h.iloc[-10:] if len(df_1h) >= 10 else df_1h
    
    for i in range(len(recent_df) - 1):
        candle = recent_df.iloc[i]
        next_candle = recent_df.iloc[i + 1] if i + 1 < len(recent_df) else candle
        
        if candle["high"] > recent_df["high"].iloc[max(0, i-5):i].max() and next_candle["close"] < candle["close"]:
            from_liquidity.append({
                "price": float(candle["high"]),
                "type": "SWEPT_HIGH",
                "timeframe": "1h",
                "direction": "FROM"
            })
        
        if candle["low"] < recent_df["low"].iloc[max(0, i-5):i].min() and next_candle["close"] > candle["close"]:
            from_liquidity.append({
                "price": float(candle["low"]),
                "type": "SWEPT_LOW",
                "timeframe": "1h",
                "direction": "FROM"
            })
    
    to_liquidity = []
    
    sorted_zones = sorted(htf_context.liquidity_zones, 
                         key=lambda z: abs(z["price"] - current_price))
    
    if htf_context.bias == "BULLISH":
        targets = [z for z in sorted_zones if z["price"] > current_price]
    elif htf_context.bias == "BEARISH":
        targets = [z for z in sorted_zones if z["price"] < current_price]
    else:
        targets = [z for z in sorted_zones if z["type"] in ["RANGE_HIGH", "RANGE_LOW"]]
    
    for target in targets[:3]:
        to_liquidity.append({
            "price": target["price"],
            "type": target["type"],
            "timeframe": target["timeframe"],
            "strength": int(target.get("strength", 1)),
            "direction": "TO"
        })
    
    if len(df_1h) >= 24:
        high_values = df_1h["high"].iloc[-24:].values
        for val in np.unique(np.round(high_values, 4)):
            count = int(np.sum(np.round(high_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_HIGH",
                    "timeframe": "1h",
                    "strength": int(min(2, count)),
                    "direction": "TO"
                })
        
        low_values = df_1h["low"].iloc[-24:].values
        for val in np.unique(np.round(low_values, 4)):
            count = int(np.sum(np.round(low_values, 4) == val))
            if count >= 2:
                to_liquidity.append({
                    "price": float(val),
                    "type": "EQUAL_LOW",
                    "timeframe": "1h",
                    "strength": int(min(2, count)),
                    "direction": "TO"
                })
    
    has_clear_target = len(to_liquidity) > 0 and len(from_liquidity) > 0
    
    return LiquidityMap(
        from_liquidity=from_liquidity,
        to_liquidity=to_liquidity,
        has_clear_target=has_clear_target
    )

# ---------------- STEP 3: LIQUIDITY SWEEP ----------------
async def analyze_sweep(exchange, symbol: str, htf_context: HTFContext, timeframe: str = "15m") -> SweepAnalysis:
    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 50)
    if not ohlcv or len(ohlcv) < 10:
        return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                           previous_extreme=0, impulsive=False)
    
    df = create_dataframe(ohlcv)
    
    lookback = min(5, len(df))
    
    for i in range(-lookback, 0):
        candle_idx = len(df) + i
        candle = df.iloc[candle_idx]
        
        start_idx = max(0, candle_idx - 5)
        prev_candles = df.iloc[start_idx:candle_idx]
        
        if len(prev_candles) == 0:
            continue
        
        previous_high = prev_candles["high"].max()
        previous_low = prev_candles["low"].min()
        
        if candle["high"] > previous_high:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] < candle["close"] and 
                             next_candle["low"] < candle["low"])
            else:
                fake_sweep = False
            
            strength = 0.0
            if impulsive and not fake_sweep:
                extension = (candle["high"] - previous_high) / previous_high
                strength = min(1.0, extension * 100)
            
            return SweepAnalysis(
                type="HIGH_SWEEP",
                candle_index=int(candle_idx),
                swept_price=float(candle["high"]),
                previous_extreme=float(previous_high),
                impulsive=impulsive,
                fake_sweep=fake_sweep,
                strength=float(strength)
            )
        
        elif candle["low"] < previous_low:
            body_size = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["open"], candle["close"])
            lower_wick = min(candle["open"], candle["close"]) - candle["low"]
            total_wick = upper_wick + lower_wick
            
            impulsive = body_size > total_wick
            
            if i < -1:
                next_candle = df.iloc[candle_idx + 1]
                fake_sweep = (next_candle["close"] > candle["close"] and 
                             next_candle["high"] > candle["high"])
            else:
                fake_sweep = False
            
            strength = 0.0
            if impulsive and not fake_sweep:
                extension = (previous_low - candle["low"]) / previous_low
                strength = min(1.0, extension * 100)
            
            return SweepAnalysis(
                type="LOW_SWEEP",
                candle_index=int(candle_idx),
                swept_price=float(candle["low"]),
                previous_extreme=float(previous_low),
                impulsive=impulsive,
                fake_sweep=fake_sweep,
                strength=float(strength)
            )
    
    return SweepAnalysis(type="NONE", candle_index=-1, swept_price=0, 
                       previous_extreme=0, impulsive=False)

# ---------------- STEP 4: STRUCTURE CHECK ----------------
async def check_structure_shift(exchange, symbol: str, sweep: SweepAnalysis, 
                               htf_context: HTFContext, timeframe: str = "15m") -> StructureShift:
    if sweep.type == "NONE":
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 50)
    if not ohlcv:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    df = create_dataframe(ohlcv)
    
    sweep_idx = sweep.candle_index
    if sweep_idx < 0 or sweep_idx >= len(df) - 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    post_sweep_candles = df.iloc[sweep_idx + 1:]
    if len(post_sweep_candles) < 3:
        return StructureShift(type="NONE", confirmed=False, candle_index=-1)
    
    if sweep.type == "HIGH_SWEEP":
        recent_low_before = df["low"].iloc[max(0, sweep_idx-5):sweep_idx].min()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["low"] < recent_low_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="High sweep followed by break below recent low"
                )
        
        if len(post_sweep_candles) >= 5:
            pullback_low = post_sweep_candles["low"].iloc[:3].min()
            subsequent_high = post_sweep_candles["high"].iloc[3:].max()
            
            if subsequent_high > sweep.swept_price:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=int(sweep_idx + 3),
                    description="High sweep followed by new higher high"
                )
    
    elif sweep.type == "LOW_SWEEP":
        recent_high_before = df["high"].iloc[max(0, sweep_idx-5):sweep_idx].max()
        
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            if candle["high"] > recent_high_before:
                return StructureShift(
                    type="CHoCH",
                    confirmed=True,
                    candle_index=int(sweep_idx + i + 1),
                    description="Low sweep followed by break above recent high"
                )
        
        if len(post_sweep_candles) >= 5:
            pullback_high = post_sweep_candles["high"].iloc[:3].max()
            subsequent_low = post_sweep_candles["low"].iloc[3:].min()
            
            if subsequent_low < sweep.swept_price:
                return StructureShift(
                    type="BOS",
                    confirmed=True,
                    candle_index=int(sweep_idx + 3),
                    description="Low sweep followed by new lower low"
                )
    
    return StructureShift(type="NONE", confirmed=False, candle_index=-1)

# ---------------- STEP 5: ENTRY ZONE ----------------
async def find_entry_zone(exchange, symbol: str, htf_context: HTFContext,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         side: str, timeframe: str = "5m") -> EntryZone:
    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 100)
    if not ohlcv:
        return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)
    
    df = create_dataframe(ohlcv)
    current_price = float(df["close"].iloc[-1])
    
    if structure_shift.type == "CHoCH":
        entry_type = "ORDER_BLOCK"
    elif structure_shift.type == "BOS":
        entry_type = "FAIR_VALUE_GAP"
    else:
        if htf_context.premium_discount == "DISCOUNT":
            entry_type = "DISCOUNT"
        elif htf_context.premium_discount == "PREMIUM":
            entry_type = "PREMIUM"
        else:
            entry_type = "NONE"
    
    if entry_type == "ORDER_BLOCK":
        for i in range(2, len(df) - 1):
            candle = df.iloc[i]
            next_candle = df.iloc[i + 1]
            
            if side == "BUY":
                if (candle["close"] < candle["open"] and 
                    next_candle["close"] > next_candle["open"]):
                    ob_low = min(candle["low"], next_candle["low"])
                    ob_high = next_candle["close"]
                    
                    aligns = (htf_context.bias == "BULLISH" or 
                             htf_context.premium_discount == "DISCOUNT")
                    
                    if current_price <= ob_high and current_price >= ob_low * 0.995:
                        current_candle = df.iloc[-1]
                        prev_candle = df.iloc[-2] if len(df) >= 2 else current_candle
                        
                        reaction = (current_candle["close"] > current_candle["open"] or
                                   (prev_candle["close"] > prev_candle["open"] and
                                    current_candle["close"] > prev_candle["close"]))
                        
                        return EntryZone(
                            type="ORDER_BLOCK",
                            price=float((ob_low + ob_high) / 2),
                            low=float(ob_low),
                            high=float(ob_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
            
            elif side == "SELL":
                if (candle["close"] > candle["open"] and 
                    next_candle["close"] < next_candle["open"]):
                    ob_low = next_candle["close"]
                    ob_high = max(candle["high"], next_candle["high"])
                    
                    aligns = (htf_context.bias == "BEARISH" or 
                             htf_context.premium_discount == "PREMIUM")
                    
                    if current_price >= ob_low and current_price <= ob_high * 1.005:
                        current_candle = df.iloc[-1]
                        prev_candle = df.iloc[-2] if len(df) >= 2 else current_candle
                        
                        reaction = (current_candle["close"] < current_candle["open"] or
                                   (prev_candle["close"] < prev_candle["open"] and
                                    current_candle["close"] < prev_candle["close"]))
                        
                        return EntryZone(
                            type="ORDER_BLOCK",
                            price=float((ob_low + ob_high) / 2),
                            low=float(ob_low),
                            high=float(ob_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
    
    elif entry_type == "FAIR_VALUE_GAP":
        for i in range(1, len(df) - 2):
            candle1 = df.iloc[i]
            candle2 = df.iloc[i + 1]
            candle3 = df.iloc[i + 2] if i + 2 < len(df) else candle2
            
            if side == "BUY":
                if candle2["low"] > candle1["high"]:
                    fvg_low = candle1["high"]
                    fvg_high = candle2["low"]
                    
                    if current_price <= fvg_high and current_price >= fvg_low:
                        aligns = (htf_context.bias == "BULLISH")
                        reaction = candle3["close"] > candle3["open"]
                        
                        return EntryZone(
                            type="FAIR_VALUE_GAP",
                            price=float((fvg_low + fvg_high) / 2),
                            low=float(fvg_low),
                            high=float(fvg_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
            
            elif side == "SELL":
                if candle2["high"] < candle1["low"]:
                    fvg_low = candle2["high"]
                    fvg_high = candle1["low"]
                    
                    if current_price >= fvg_low and current_price <= fvg_high:
                        aligns = (htf_context.bias == "BEARISH")
                        reaction = candle3["close"] < candle3["open"]
                        
                        return EntryZone(
                            type="FAIR_VALUE_GAP",
                            price=float((fvg_low + fvg_high) / 2),
                            low=float(fvg_low),
                            high=float(fvg_high),
                            aligns_with_htf=aligns,
                            candle_reaction=reaction
                        )
    
    elif entry_type in ["PREMIUM", "DISCOUNT"]:
        zone_price = htf_context.range_mid
        zone_width = (htf_context.range_high - htf_context.range_low) * 0.1
        
        aligns = True
        
        if (side == "BUY" and entry_type == "DISCOUNT" and
            current_price <= htf_context.range_mid * 1.02):
            reaction = df["close"].iloc[-1] > df["open"].iloc[-1]
            
            return EntryZone(
                type="DISCOUNT",
                price=float(zone_price),
                low=float(zone_price - zone_width),
                high=float(zone_price + zone_width),
                aligns_with_htf=aligns,
                candle_reaction=reaction
            )
        
        elif (side == "SELL" and entry_type == "PREMIUM" and
              current_price >= htf_context.range_mid * 0.98):
            reaction = df["close"].iloc[-1] < df["open"].iloc[-1]
            
            return EntryZone(
                type="PREMIUM",
                price=float(zone_price),
                low=float(zone_price - zone_width),
                high=float(zone_price + zone_width),
                aligns_with_htf=aligns,
                candle_reaction=reaction
            )
    
    return EntryZone(type="NONE", price=0, low=0, high=0, aligns_with_htf=False)

# ---------------- STEP 6: RISK/SL ----------------
def calculate_risk_sl(entry_zone: EntryZone, sweep: SweepAnalysis,
                     htf_context: HTFContext, side: str) -> RiskManagement:
    entry_price = entry_zone.price
    
    sl_price = 0.0
    invalidation_type = ""
    
    if sweep.type != "NONE" and sweep.swept_price > 0:
        if side == "BUY" and sweep.type == "LOW_SWEEP":
            sl_price = sweep.swept_price * 0.995
            invalidation_type = "SWEEP"
        elif side == "SELL" and sweep.type == "HIGH_SWEEP":
            sl_price = sweep.swept_price * 1.005
            invalidation_type = "SWEEP"
    
    if invalidation_type == "" and entry_zone.type == "ORDER_BLOCK":
        if side == "BUY":
            sl_price = entry_zone.low * 0.995
            invalidation_type = "ORDER_BLOCK"
        elif side == "SELL":
            sl_price = entry_zone.high * 1.005
            invalidation_type = "ORDER_BLOCK"
    
    if invalidation_type == "":
        if side == "BUY" and htf_context.structure:
            swing_lows = [s for s in htf_context.structure if "low" in str(s.get("type", "")).lower()]
            if swing_lows:
                recent_swing_low = min([s.get("price", entry_price * 0.9) for s in swing_lows])
                sl_price = recent_swing_low * 0.995
                invalidation_type = "STRUCTURE"
        elif side == "SELL" and htf_context.structure:
            swing_highs = [s for s in htf_context.structure if "high" in str(s.get("type", "")).lower()]
            if swing_highs:
                recent_swing_high = max([s.get("price", entry_price * 1.1) for s in swing_highs])
                sl_price = recent_swing_high * 1.005
                invalidation_type = "STRUCTURE"
    
    if invalidation_type == "":
        atr_approx = entry_price * 0.02
        if side == "BUY":
            sl_price = entry_price - (atr_approx * 1.5)
        else:
            sl_price = entry_price + (atr_approx * 1.5)
        invalidation_type = "ATR_FALLBACK"
    
    risk_amount = abs(entry_price - sl_price)
    distance_pct = (risk_amount / entry_price) * 100
    
    return RiskManagement(
        sl_price=float(sl_price),
        invalidation_type=invalidation_type,
        risk_amount=float(risk_amount),
        sl_to_entry_distance=float(distance_pct)
    )

# ---------------- STEP 7: TAKE PROFIT ----------------
def calculate_take_profits(entry_price: float, side: str, 
                          liquidity_map: LiquidityMap,
                          htf_context: HTFContext) -> TakeProfitLevels:
    if side == "BUY":
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] > entry_price]
        range_boundary = htf_context.range_high
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] > entry_price and z["type"] != "RANGE_HIGH"]
    else:
        potential_targets = [t for t in liquidity_map.to_liquidity 
                           if t["price"] < entry_price]
        range_boundary = htf_context.range_low
        htf_targets = [z for z in htf_context.liquidity_zones 
                      if z["price"] < entry_price and z["type"] != "RANGE_LOW"]
    
    tp1_candidates = [t for t in potential_targets if t["timeframe"] == "1h"]
    if tp1_candidates:
        tp1_candidates.sort(key=lambda t: abs(t["price"] - entry_price))
        tp1 = tp1_candidates[0]["price"]
        tp1_type = tp1_candidates[0]["type"]
    else:
        if side == "BUY":
            tp1 = entry_price * 1.02
        else:
            tp1 = entry_price * 0.98
        tp1_type = "RISK_REWARD_1_1"
    
    tp2 = range_boundary
    tp2_type = "RANGE_BOUNDARY"
    
    if htf_targets:
        htf_targets.sort(key=lambda z: z.get("strength", 0), reverse=True)
        tp3 = htf_targets[0]["price"]
        tp3_type = htf_targets[0]["type"]
    else:
        if side == "BUY":
            range_distance = htf_context.range_high - htf_context.range_low
            tp3 = htf_context.range_high + (range_distance * 0.5)
            tp3_type = "EXTENDED_TARGET"
        else:
            range_distance = htf_context.range_high - htf_context.range_low
            tp3 = htf_context.range_low - (range_distance * 0.5)
            tp3_type = "EXTENDED_TARGET"
    
    return TakeProfitLevels(
        tp1=float(tp1),
        tp1_type=tp1_type,
        tp2=float(tp2),
        tp2_type=tp2_type,
        tp3=float(tp3),
        tp3_type=tp3_type
    )

# ---------------- STEP 8: PROBABILITY CHECK ----------------
def calculate_probability(htf_context: HTFContext, liquidity_map: LiquidityMap,
                         sweep: SweepAnalysis, structure_shift: StructureShift,
                         entry_zone: EntryZone, side: str) -> ProbabilityScore:
    if htf_context.bias == side.upper() or htf_context.bias == "RANGING":
        htf_alignment = 1.0
    elif (htf_context.bias == "BULLISH" and side == "SELL") or \
         (htf_context.bias == "BEARISH" and side == "BUY"):
        htf_alignment = 0.3
    else:
        htf_alignment = 0.5
    
    if (side == "BUY" and htf_context.premium_discount == "DISCOUNT") or \
       (side == "SELL" and htf_context.premium_discount == "PREMIUM"):
        htf_alignment = min(1.0, htf_alignment + 0.2)
    
    if liquidity_map.has_clear_target:
        quality_targets = sum(1 for t in liquidity_map.to_liquidity 
                            if t.get("strength", 0) >= 2)
        liquidity_quality = min(1.0, quality_targets / 3.0)
    else:
        liquidity_quality = 0.2
    
    sweep_strength = sweep.strength
    if sweep.impulsive:
        sweep_strength = min(1.0, sweep_strength + 0.3)
    if sweep.fake_sweep:
        sweep_strength = max(0.0, sweep_strength - 0.5)
    
    if structure_shift.confirmed:
        if structure_shift.type == "CHoCH":
            structure_clarity = 0.9
        elif structure_shift.type == "BOS":
            structure_clarity = 0.8
        else:
            structure_clarity = 0.6
    else:
        structure_clarity = 0.2
    
    if entry_zone.type in ["ORDER_BLOCK", "FAIR_VALUE_GAP"]:
        entry_precision = 0.8
        if entry_zone.aligns_with_htf:
            entry_precision = min(1.0, entry_precision + 0.1)
        if entry_zone.candle_reaction:
            entry_precision = min(1.0, entry_precision + 0.1)
    elif entry_zone.type in ["PREMIUM", "DISCOUNT"]:
        entry_precision = 0.6
        if entry_zone.candle_reaction:
            entry_precision = 0.7
    else:
        entry_precision = 0.2
    
    total_score = (htf_alignment + liquidity_quality + sweep_strength + 
                   structure_clarity + entry_precision)
    
    return ProbabilityScore(
        htf_alignment=float(htf_alignment),
        liquidity_quality=float(liquidity_quality),
        sweep_strength=float(sweep_strength),
        structure_clarity=float(structure_clarity),
        entry_precision=float(entry_precision),
        total_score=float(total_score)
    )

# ---------------- MULTI-TF SCANNING ----------------
async def scan_symbol_multi_tf(exchange, symbol: str) -> List[Dict]:
    setups = []
    
    for timeframe in TIMEFRAMES_TO_SCAN:
        try:
            setup = await scan_symbol_single_tf(exchange, symbol, timeframe)
            if setup:
                setups.append(setup)
        except Exception as e:
            log.debug(f"Error scanning {symbol} on {timeframe}: {e}")
            continue
    
    return setups

async def scan_symbol_single_tf(exchange, symbol: str, timeframe: str) -> Optional[Dict]:
    ticker = await exchange.fetch_ticker(symbol)
    current_price = ticker.get("last", 0)
    if not current_price:
        return None
    
    log.debug(f"🔍 Scanning {symbol} on {timeframe} at {current_price}")
    
    htf_context = await analyze_htf_bias(exchange, symbol)
    if not htf_context.valid:
        return None
    
    liquidity_map = await map_liquidity(exchange, symbol, htf_context, current_price)
    if not liquidity_map.has_clear_target:
        return None
    
    if timeframe in ["1m", "3m", "5m"]:
        sweep_tf = timeframe
    elif timeframe in ["15m", "30m"]:
        sweep_tf = "15m"
    else:
        sweep_tf = "1h"
    
    sweep = await analyze_sweep(exchange, symbol, htf_context, sweep_tf)
    if sweep.type == "NONE" or not sweep.impulsive or sweep.fake_sweep:
        return None
    
    if sweep.type == "HIGH_SWEEP":
        side = "SELL"
    elif sweep.type == "LOW_SWEEP":
        side = "BUY"
    else:
        return None
    
    structure_shift = await check_structure_shift(exchange, symbol, sweep, htf_context, sweep_tf)
    if not structure_shift.confirmed:
        return None
    
    if timeframe in ["1m", "3m", "5m"]:
        entry_tf = "5m"
    else:
        entry_tf = "15m"
    
    entry_zone = await find_entry_zone(exchange, symbol, htf_context, sweep, structure_shift, side, entry_tf)
    if entry_zone.type == "NONE" or not entry_zone.candle_reaction:
        return None
    
    risk_sl = calculate_risk_sl(entry_zone, sweep, htf_context, side)
    if risk_sl.sl_price == 0:
        return None
    
    tp_levels = calculate_take_profits(entry_zone.price, side, liquidity_map, htf_context)
    
    probability = calculate_probability(
        htf_context, liquidity_map, sweep, structure_shift, entry_zone, side
    )
    
    min_score_required = TIMEFRAME_MIN_SCORES.get(timeframe, 3.5)
    if not probability.acceptable or probability.total_score < min_score_required:
        return None
    
    log.info(f"✅ {symbol} on {timeframe}: A+ Setup! Score: {probability.total_score:.2f}/5")
    
    setup = {
        "symbol": symbol,
        "timeframe": timeframe,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "side": side,
        "current_price": current_price,
        "htf_bias": htf_context.bias,
        "htf_range_high": htf_context.range_high,
        "htf_range_low": htf_context.range_low,
        "htf_premium_discount": htf_context.premium_discount,
        "htf_liquidity_zones": htf_context.liquidity_zones,
        "htf_structure": htf_context.structure,
        "liquidity_from": liquidity_map.from_liquidity,
        "liquidity_to": liquidity_map.to_liquidity,
        "has_clear_target": liquidity_map.has_clear_target,
        "sweep_type": sweep.type,
        "swept_price": sweep.swept_price,
        "sweep_impulsive": sweep.impulsive,
        "sweep_strength": sweep.strength,
        "structure_shift_type": structure_shift.type,
        "structure_shift_confirmed": structure_shift.confirmed,
        "structure_description": structure_shift.description,
        "entry_type": entry_zone.type,
        "entry_price": entry_zone.price,
        "entry_low": entry_zone.low,
        "entry_high": entry_zone.high,
        "entry_aligns_htf": entry_zone.aligns_with_htf,
        "entry_reaction_confirmed": entry_zone.candle_reaction,
        "sl_price": risk_sl.sl_price,
        "sl_invalidation_type": risk_sl.invalidation_type,
        "risk_amount": risk_sl.risk_amount,
        "sl_distance_pct": risk_sl.sl_to_entry_distance,
        "tp1_price": tp_levels.tp1,
        "tp1_type": tp_levels.tp1_type,
        "tp2_price": tp_levels.tp2,
        "tp2_type": tp_levels.tp2_type,
        "tp3_price": tp_levels.tp3,
        "tp3_type": tp_levels.tp3_type,
        "probability": {
            "htf_alignment": probability.htf_alignment,
            "liquidity_quality": probability.liquidity_quality,
            "sweep_strength": probability.sweep_strength,
            "structure_clarity": probability.structure_clarity,
            "entry_precision": probability.entry_precision,
            "total_score": probability.total_score,
            "acceptable": probability.acceptable
        }
    }
    
    return setup

# ---------------- ALERT FORMATTING ----------------
async def send_setup_alert(setup: Dict):
    entry = setup["entry_price"]
    sl = setup["sl_price"]
    tp1 = setup["tp1_price"]
    
    risk = abs(entry - sl)
    reward_tp1 = abs(tp1 - entry)
    rr_ratio = reward_tp1 / risk if risk > 0 else 0
    
    msg = f"""
🔥 <b>ROMEOTPT {setup['timeframe'].upper()} SETUP CONFIRMED</b>

<b>Symbol:</b> {setup['symbol']}
<b>Timeframe:</b> {setup['timeframe']}
<b>Side:</b> {setup['side']}
<b>Entry:</b> {setup['entry_price']:.8f}
<b>Current:</b> {setup['current_price']:.8f}

<b>Probability Score:</b> {setup['probability']['total_score']:.2f}/5.0
<b>RR Ratio:</b> {rr_ratio:.2f}:1

🎯 <b>Targets:</b>
TP1: {setup['tp1_price']:.8f} ({setup['tp1_type']})
TP2: {setup['tp2_price']:.8f} ({setup['tp2_type']})
TP3: {setup['tp3_price']:.8f} ({setup['tp3_type']})

🛡️ <b>Risk:</b>
SL: {setup['sl_price']:.8f} ({setup['sl_invalidation_type']})
Risk: {setup['risk_amount']:.8f} ({setup['sl_distance_pct']:.2f}%)

📊 <b>Analysis:</b>
• HTF: {setup['htf_bias']} in {setup['htf_premium_discount']}
• Sweep: {setup['sweep_type']} (strength: {setup['sweep_strength']:.2f})
• Structure: {setup['structure_shift_type']}
• Entry: {setup['entry_type']}

✅ <b>Probability Components:</b>
HTF Alignment: {setup['probability']['htf_alignment']:.2f}
Liquidity Quality: {setup['probability']['liquidity_quality']:.2f}
Sweep Strength: {setup['probability']['sweep_strength']:.2f}
Structure Clarity: {setup['probability']['structure_clarity']:.2f}
Entry Precision: {setup['probability']['entry_precision']:.2f}

<i>Detected: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    await send_telegram(msg)
    
    global trade_tracker
    if trade_tracker:
        await trade_tracker.add_trade(setup)
    
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals VALUES (
                NULL, :symbol, :timeframe, :timestamp, :side,
                :htf_bias, :htf_range_high, :htf_range_low, :htf_premium_discount,
                :htf_liquidity_zones, :htf_structure,
                :liquidity_from, :liquidity_to, :has_clear_target,
                :sweep_type, :swept_price, :sweep_impulsive, :sweep_strength,
                :structure_shift_type, :structure_shift_confirmed, :structure_description,
                :entry_type, :entry_price, :entry_low, :entry_high, :entry_aligns_htf, :entry_reaction_confirmed,
                :sl_price, :sl_invalidation_type, :risk_amount, :sl_distance_pct,
                :tp1_price, :tp1_type, :tp2_price, :tp2_type, :tp3_price, :tp3_type,
                :prob_htf_alignment, :prob_liquidity_quality, :prob_sweep_strength,
                :prob_structure_clarity, :prob_entry_precision, :prob_total_score, :prob_acceptable,
                :current_price, 'DETECTED', ''
            )
        """, {
            "symbol": setup["symbol"],
            "timeframe": setup["timeframe"],
            "timestamp": setup["timestamp"],
            "side": setup["side"],
            "htf_bias": setup["htf_bias"],
            "htf_range_high": float(setup["htf_range_high"]),
            "htf_range_low": float(setup["htf_range_low"]),
            "htf_premium_discount": setup["htf_premium_discount"],
            "htf_liquidity_zones": json.dumps(safe_json_serialize(setup["htf_liquidity_zones"])),
            "htf_structure": json.dumps(safe_json_serialize(setup["htf_structure"])),
            "liquidity_from": json.dumps(safe_json_serialize(setup["liquidity_from"])),
            "liquidity_to": json.dumps(safe_json_serialize(setup["liquidity_to"])),
            "has_clear_target": setup["has_clear_target"],
            "sweep_type": setup["sweep_type"],
            "swept_price": float(setup["swept_price"]),
            "sweep_impulsive": setup["sweep_impulsive"],
            "sweep_strength": float(setup["sweep_strength"]),
            "structure_shift_type": setup["structure_shift_type"],
            "structure_shift_confirmed": setup["structure_shift_confirmed"],
            "structure_description": setup["structure_description"],
            "entry_type": setup["entry_type"],
            "entry_price": float(setup["entry_price"]),
            "entry_low": float(setup["entry_low"]),
            "entry_high": float(setup["entry_high"]),
            "entry_aligns_htf": setup["entry_aligns_htf"],
            "entry_reaction_confirmed": setup["entry_reaction_confirmed"],
            "sl_price": float(setup["sl_price"]),
            "sl_invalidation_type": setup["sl_invalidation_type"],
            "risk_amount": float(setup["risk_amount"]),
            "sl_distance_pct": float(setup["sl_distance_pct"]),
            "tp1_price": float(setup["tp1_price"]),
            "tp1_type": setup["tp1_type"],
            "tp2_price": float(setup["tp2_price"]),
            "tp2_type": setup["tp2_type"],
            "tp3_price": float(setup["tp3_price"]),
            "tp3_type": setup["tp3_type"],
            "prob_htf_alignment": float(setup["probability"]["htf_alignment"]),
            "prob_liquidity_quality": float(setup["probability"]["liquidity_quality"]),
            "prob_sweep_strength": float(setup["probability"]["sweep_strength"]),
            "prob_structure_clarity": float(setup["probability"]["structure_clarity"]),
            "prob_entry_precision": float(setup["probability"]["entry_precision"]),
            "prob_total_score": float(setup["probability"]["total_score"]),
            "prob_acceptable": bool(setup["probability"]["acceptable"]),
            "current_price": float(setup["current_price"])
        })
        await db_conn.commit()

# ---------------- MAIN SCANNER ----------------
async def scanner_main(exchange):
    global trade_tracker
    
    await send_telegram(f"🚀 ROMEOTPT v2 Multi-TF Scanner Started!")
    await send_telegram(f"📊 Scanning {len(TIMEFRAMES_TO_SCAN)} timeframes: {', '.join(TIMEFRAMES_TO_SCAN)}")
    await send_telegram(f"🔢 Scanning top {TOP_N} symbols")
    
    daily_signals = 0
    last_reset = datetime.datetime.utcnow().date()
    target_reached = False
    
    while True:
        try:
            current_date = datetime.datetime.utcnow().date()
            if current_date != last_reset:
                daily_signals = 0
                last_reset = current_date
                target_reached = False
                await send_telegram(f"📅 New day started - Signal counter reset")
            
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT")]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            top_pairs = usdt_pairs[:TOP_N]
            
            log.info(f"📊 Scanning {len(top_pairs)} symbols across {len(TIMEFRAMES_TO_SCAN)} timeframes...")
            
            setups_found = 0
            for symbol, volume in top_pairs:
                try:
                    setups = await scan_symbol_multi_tf(exchange, symbol)
                    
                    for setup in setups:
                        if daily_signals >= 50 and not target_reached:
                            await send_telegram(f"🎯 DAILY TARGET ACHIEVED: {daily_signals} signals today!")
                            target_reached = True
                        
                        if daily_signals >= 100:
                            log.info(f"⚠️ Daily limit reached (100 signals), skipping further alerts")
                            break
                        
                        await send_setup_alert(setup)
                        setups_found += 1
                        daily_signals += 1
                        
                        await asyncio.sleep(1)
                        
                    if daily_signals >= 100:
                        break
                        
                except Exception as e:
                    log.error(f"Error scanning {symbol}: {e}")
                    continue
            
            if setups_found > 0:
                log.info(f"✅ Found {setups_found} A+ setups (Total today: {daily_signals})")
            else:
                log.info(f"⏳ No setups found this scan (Total today: {daily_signals})")
            
        except Exception as e:
            log.exception(f"Scanner error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- TRADE OUTCOME CHECKER ----------------
async def trade_outcome_checker(exchange):
    """Background task to check TP/SL outcomes"""
    global trade_tracker
    
    await send_telegram("📊 Trade Outcome Tracker Started")
    
    while True:
        try:
            if trade_tracker:
                await trade_tracker.check_trade_outcomes(exchange)
        except Exception as e:
            log.error(f"Trade outcome checker error: {e}")
        
        await asyncio.sleep(30)  # Check every 30 seconds

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    return {
        "status": "healthy", 
        "scanner": "ROMEOTPT v2 Multi-TF",
        "timeframes": TIMEFRAMES_TO_SCAN,
        "min_scores": TIMEFRAME_MIN_SCORES
    }

@app.get("/setups")
async def get_setups(limit: int = 20, min_score: float = 3.5, timeframe: str = None):
    query = """SELECT * FROM signals WHERE prob_total_score >= ?"""
    params = [min_score]
    
    if timeframe:
        query += " AND timeframe = ?"
        params.append(timeframe)
    
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    
    async with db_lock:
        async with db_conn.execute(query, params) as cursor:
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
        
        setups = []
        for row in rows:
            setup = dict(zip(columns, row))
            json_fields = ["htf_liquidity_zones_json", "htf_structure_json",
                          "liquidity_from_json", "liquidity_to_json"]
            for field in json_fields:
                if setup.get(field):
                    try:
                        key = field.replace("_json", "")
                        setup[key] = json.loads(setup[field])
                    except:
                        pass
            setups.append(setup)
        
        timeframe_stats = {}
        for setup in setups:
            tf = setup.get("timeframe", "unknown")
            timeframe_stats[tf] = timeframe_stats.get(tf, 0) + 1
        
        return {
            "setups": setups, 
            "count": len(setups),
            "timeframe_distribution": timeframe_stats
        }

@app.get("/trades")
async def get_trades(status: str = "OPEN", limit: int = 20):
    async with db_lock:
        async with db_conn.execute(
            """SELECT * FROM trade_outcomes WHERE status = ? ORDER BY id DESC LIMIT ?""",
            (status, limit)
        ) as cursor:
            columns = [description[0] for description in cursor.description]
            rows = await cursor.fetchall()
        
        trades = [dict(zip(columns, row)) for row in rows]
        
        if trades:
            win_count = sum(1 for t in trades if t.get("status") in ["TP1_HIT", "TP2_HIT", "TP3_HIT"])
            loss_count = sum(1 for t in trades if t.get("status") == "SL_HIT")
            open_count = sum(1 for t in trades if t.get("status") == "OPEN")
        else:
            win_count = loss_count = open_count = 0
        
        return {
            "trades": trades,
            "count": len(trades),
            "stats": {
                "open": open_count,
                "wins": win_count,
                "losses": loss_count,
                "win_rate": (win_count / (win_count + loss_count) * 100) if (win_count + loss_count) > 0 else 0
            }
        }

# ---------------- MAIN ----------------
async def main():
    global db_conn, trade_tracker
    
    await init_db()
    
    trade_tracker = TradeOutcomeTracker(db_conn)
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    await send_telegram("🚀 ROMEOTPT v2 System Started Successfully!")
    await send_telegram("✅ Signal Scanner: Running")
    await send_telegram("✅ Trade Tracker: Running")
    await send_telegram("✅ TP/SL Alerts: Enabled")
    
    # Run both tasks concurrently
    await asyncio.gather(
        scanner_main(exchange),
        trade_outcome_checker(exchange)
    )

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
            log.info("Shutting down ROMEOTPT v2 scanner...")
            if db_conn:
                asyncio.run(db_conn.close())