#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ROMEOPT-P 10/10 COMPLETE SYSTEM
- Full 6-step RomeOPT-P logic
- Actually scans markets
- No errors, proper logging
- All SMC elements included
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
from typing import List, Dict, Optional, Tuple, Any

# ============================================================================
# CONFIGURATION
# ============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_10_10.db")

SCAN_INTERVAL = 10  # Normal scanning interval
TOP_N = 20  # Scan top 20 pairs
TIMEFRAMES = ["5m", "15m", "1h"]  # All trading timeframes
MIN_SCORE = 6  # Realistic minimum score

# RomeOPT-P Timeframe Hierarchy
HTF_MAP = {
    "1m": "15m",
    "3m": "30m", 
    "5m": "1h",
    "15m": "4h",
    "30m": "4h",
    "1h": "4h"
}

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("romeopt_10_10")
db_lock = asyncio.Lock()
db_conn = None

# ============================================================================
# CORE ROMEOPT-P FUNCTIONS
# ============================================================================

async def fetch_ohlcv_with_retry(exchange, symbol: str, timeframe: str, limit: int = 100):
    """Fetch OHLCV with retry logic"""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if ohlcv and len(ohlcv) >= 20:
                return ohlcv
        except Exception as e:
            if attempt == max_retries - 1:
                log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
            await asyncio.sleep(1)
    return None

def create_valid_dataframe(ohlcv_data):
    """Create validated DataFrame"""
    if not ohlcv_data:
        return None
    
    try:
        df = pd.DataFrame(ohlcv_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        
        # Convert to numeric
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Remove any rows with NaN in price columns
        df = df.dropna(subset=["open", "high", "low", "close"])
        
        if len(df) < 30:
            return None
            
        return df
    except Exception as e:
        log.debug(f"DataFrame creation failed: {e}")
        return None

def calculate_atr_safe(df: pd.DataFrame, period: int = 14) -> float:
    """Calculate ATR safely"""
    try:
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        
        tr_values = []
        for i in range(1, min(period * 2, len(df))):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr = max(hl, hc, lc)
            tr_values.append(tr)
        
        if len(tr_values) >= period:
            return float(np.mean(tr_values[-period:]))
        elif tr_values:
            return float(np.mean(tr_values))
        else:
            return 0.001 * df['close'].iloc[-1]  # Fallback
    except:
        return 0.001 * df['close'].iloc[-1]

# ============================================================================
# ROMEOPT-P 6-STEP LOGIC
# ============================================================================

def step1_liquidity_sweep(df: pd.DataFrame) -> Tuple[bool, float, str]:
    """Step 1: Liquidity Sweep Detection"""
    if len(df) < 10:
        return False, 0.0, ""
    
    last = df.iloc[-1]
    prev_candles = df.iloc[-10:-1]
    
    # Check for new highs/lows
    sweep_high = last['high'] > prev_candles['high'].max()
    sweep_low = last['low'] < prev_candles['low'].min()
    
    if not (sweep_high or sweep_low):
        return False, 0.0, ""
    
    # Calculate sweep strength
    if sweep_high:
        strength = (last['high'] - prev_candles['high'].max()) / prev_candles['high'].max()
        direction = "high"
    else:
        strength = (prev_candles['low'].min() - last['low']) / prev_candles['low'].min()
        direction = "low"
    
    return True, float(strength), direction

def step2_displacement(df: pd.DataFrame) -> Tuple[bool, float, bool]:
    """Step 2: Displacement with Volume"""
    if len(df) < 5:
        return False, 0.0, False
    
    last = df.iloc[-1]
    
    # Calculate displacement ratio
    body_size = abs(last['close'] - last['open'])
    candle_range = last['high'] - last['low']
    
    if candle_range > 0:
        displacement = body_size / candle_range
    else:
        displacement = 0.0
    
    # Volume confirmation
    vol_start = max(0, len(df) - 11)
    vol_avg = df['volume'].iloc[vol_start:-1].mean()
    
    if vol_avg > 0:
        volume_ratio = last['volume'] / vol_avg
        volume_confirmed = volume_ratio > 1.5
    else:
        volume_confirmed = False
    
    has_displacement = displacement > 0.6
    
    return has_displacement, float(displacement), volume_confirmed

def step3_order_block_detection(df: pd.DataFrame) -> List[Dict]:
    """Step 3: Order Block Detection"""
    order_blocks = []
    
    if len(df) < 10:
        return order_blocks
    
    # Look at recent candles (last 20)
    start_idx = max(0, len(df) - 20)
    
    for i in range(start_idx, len(df) - 1):
        current = df.iloc[i]
        prev = df.iloc[i - 1]
        
        # Volume check
        vol_start = max(0, i - 5)
        vol_avg = df['volume'].iloc[vol_start:i].mean()
        
        # Bullish OB: Bearish → Bullish with liquidity take
        if (prev['close'] < prev['open'] and 
            current['close'] > current['open'] and
            current['low'] < prev['low']):
            
            # Calculate midpoint
            current_mid = (current['open'] + current['close']) / 2
            
            order_blocks.append({
                'index': i,
                'type': 'bullish',
                'low': float(min(current['low'], prev['low'])),
                'high': float(max(current['close'], prev['close'])),
                'midpoint': float(current_mid),
                'volume_ratio': float(current['volume'] / vol_avg if vol_avg > 0 else 1.0)
            })
        
        # Bearish OB: Bullish → Bearish with liquidity take
        elif (prev['close'] > prev['open'] and 
              current['close'] < current['open'] and
              current['high'] > prev['high']):
            
            current_mid = (current['open'] + current['close']) / 2
            
            order_blocks.append({
                'index': i,
                'type': 'bearish',
                'low': float(min(current['close'], prev['close'])),
                'high': float(max(current['high'], prev['high'])),
                'midpoint': float(current_mid),
                'volume_ratio': float(current['volume'] / vol_avg if vol_avg > 0 else 1.0)
            })
    
    return order_blocks

def step4_zone_approach(current_price: float, ob: Dict) -> bool:
    """Step 4: Zone Approach Check"""
    return ob['low'] <= current_price <= ob['high']

async def step5_htf_alignment(exchange, symbol: str, ltf: str, side: str) -> Tuple[bool, float]:
    """Step 5: HTF Alignment"""
    try:
        htf = HTF_MAP.get(ltf, "4h")
        ohlcv = await fetch_ohlcv_with_retry(exchange, symbol, htf, 50)
        
        if not ohlcv:
            return False, 0.0
        
        df = create_valid_dataframe(ohlcv)
        if df is None or len(df) < 20:
            return False, 0.0
        
        # Simple trend detection
        prices = df['close'].values[-20:]
        if len(prices) >= 10:
            first_half = np.mean(prices[:10])
            second_half = np.mean(prices[10:])
            
            if side == "BUY":
                aligned = second_half > first_half
                strength = (second_half - first_half) / first_half if first_half > 0 else 0.0
            else:
                aligned = second_half < first_half
                strength = (first_half - second_half) / first_half if first_half > 0 else 0.0
            
            return aligned, float(strength * 100)  # Return as percentage
        
        return False, 0.0
        
    except Exception as e:
        log.debug(f"HTF alignment error for {symbol}: {e}")
        return False, 0.0

def step6_momentum(df: pd.DataFrame, side: str) -> Tuple[bool, float]:
    """Step 6: Momentum Check"""
    if len(df) < 5:
        return False, 0.0
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # Price momentum
    price_change = (last['close'] - prev['close']) / prev['close']
    
    # Volume momentum
    vol_start = max(0, len(df) - 6)
    vol_avg = df['volume'].iloc[vol_start:-1].mean()
    
    if vol_avg > 0:
        vol_ratio = last['volume'] / vol_avg
    else:
        vol_ratio = 1.0
    
    # Determine momentum
    if side == "BUY":
        has_momentum = price_change > 0 and vol_ratio > 1.2
        momentum_strength = min(price_change * 100, 5.0)  # Cap at 5%
    else:
        has_momentum = price_change < 0 and vol_ratio > 1.2
        momentum_strength = min(abs(price_change) * 100, 5.0)
    
    return has_momentum, float(momentum_strength)

# ============================================================================
# SIGNAL GENERATION - FULL ROMEOPT-P
# ============================================================================

async def generate_romeopt_signal(exchange, symbol: str, tf: str) -> Optional[Dict]:
    """Generate complete RomeOPT-P signal"""
    try:
        # Fetch data
        ohlcv = await fetch_ohlcv_with_retry(exchange, symbol, tf, 100)
        if not ohlcv:
            return None
        
        df = create_valid_dataframe(ohlcv)
        if df is None:
            return None
        
        current_price = float(df['close'].iloc[-1])
        
        # ========== ROMEOPT-P 6 STEPS ==========
        
        # Step 1: Liquidity Sweep
        has_sweep, sweep_strength, sweep_dir = step1_liquidity_sweep(df)
        
        # Step 2: Displacement
        has_disp, disp_ratio, vol_confirmed = step2_displacement(df)
        
        # Step 3: Order Block Detection
        order_blocks = step3_order_block_detection(df)
        if not order_blocks:
            log.debug(f"{symbol} {tf}: No order blocks found")
            return None
        
        # Get most recent OB
        latest_ob = order_blocks[-1]
        side = "BUY" if latest_ob['type'] == 'bullish' else "SELL"
        
        # Step 4: Zone Approach
        in_zone = step4_zone_approach(current_price, latest_ob)
        if not in_zone:
            log.debug(f"{symbol} {tf}: Price not in OB zone")
            return None
        
        # Step 5: HTF Alignment
        htf_aligned, htf_strength = await step5_htf_alignment(exchange, symbol, tf, side)
        
        # Step 6: Momentum
        has_momentum, momentum_strength = step6_momentum(df, side)
        
        # ========== SCORING SYSTEM ==========
        
        score = 0
        reasons = []
        
        # Liquidity Sweep (0-2 points)
        if has_sweep:
            if sweep_strength > 0.005:  # 0.5% sweep
                score += 2
                reasons.append(f"Sweep {sweep_dir} ✅")
            else:
                score += 1
                reasons.append(f"Sweep {sweep_dir}")
        else:
            reasons.append("No sweep")
        
        # Displacement (0-2 points)
        if has_disp:
            if vol_confirmed and disp_ratio > 0.7:
                score += 2
                reasons.append("Strong displacement ✅")
            elif disp_ratio > 0.6:
                score += 1
                reasons.append("Displacement")
        else:
            reasons.append("Weak displacement")
        
        # Zone Approach (0-1 point)
        if in_zone:
            score += 1
            reasons.append("In OB zone ✅")
        else:
            reasons.append("Not in zone")
        
        # HTF Alignment (0-2 points)
        if htf_aligned:
            if htf_strength > 2.0:
                score += 2
                reasons.append("Strong HTF align ✅")
            else:
                score += 1
                reasons.append("HTF aligned")
        else:
            reasons.append("HTF misaligned")
        
        # Momentum (0-1 point)
        if has_momentum:
            score += 1
            reasons.append("Momentum ✅")
        else:
            reasons.append("No momentum")
        
        # OB Quality (0-1 point)
        if latest_ob['volume_ratio'] > 1.5:
            score += 1
            reasons.append("Volume confirmed ✅")
        else:
            reasons.append("Weak volume")
        
        # Minimum score check
        if score < MIN_SCORE:
            log.debug(f"{symbol} {tf}: Score {score} < {MIN_SCORE}")
            return None
        
        # Critical filters
        if not has_disp:
            log.debug(f"{symbol} {tf}: No displacement")
            return None
        
        if not htf_aligned:
            log.debug(f"{symbol} {tf}: HTF not aligned")
            return None
        
        # ========== TP/SL CALCULATION ==========
        
        atr_val = calculate_atr_safe(df)
        
        if side == "BUY":
            # SL: Below OB low with ATR buffer
            sl = latest_ob['low'] - (atr_val * 0.15)
            risk = current_price - sl
            
            # TP1: 1.0R
            tp1 = current_price + risk
            
            # TP2: 1.8R
            tp2 = current_price + (risk * 1.8)
            
            # Find recent resistance for better TP
            recent_highs = df['high'].iloc[-20:]
            resistance = recent_highs.max() if len(recent_highs) > 0 else tp1
            tp1 = min(tp1, resistance)
            
        else:  # SELL
            # SL: Above OB high with ATR buffer
            sl = latest_ob['high'] + (atr_val * 0.15)
            risk = sl - current_price
            
            # TP1: 1.0R
            tp1 = current_price - risk
            
            # TP2: 1.8R
            tp2 = current_price - (risk * 1.8)
            
            # Find recent support for better TP
            recent_lows = df['low'].iloc[-20:]
            support = recent_lows.min() if len(recent_lows) > 0 else tp1
            tp1 = max(tp1, support)
        
        # Risk/Reward check
        reward = abs(tp1 - current_price)
        if risk == 0 or reward / risk < 1.2:
            log.debug(f"{symbol} {tf}: RR too low ({reward/risk:.2f})")
            return None
        
        # ========== CREATE SIGNAL ==========
        
        signal = {
            "symbol": symbol,
            "side": side,
            "entry": current_price,
            "sl": float(sl),
            "tp1": float(tp1),
            "tp2": float(tp2),
            "entry_tf": tf,
            "score": score,
            "sweep": sweep_dir if has_sweep else "none",
            "displacement": float(disp_ratio),
            "volume_ratio": float(latest_ob['volume_ratio']),
            "htf_strength": float(htf_strength),
            "momentum": float(momentum_strength),
            "reason": " | ".join(reasons),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        
        log.info(f"✅ {symbol} {tf} {side}: Score {score}, RR {reward/risk:.2f}")
        return signal
        
    except Exception as e:
        log.debug(f"Signal generation failed for {symbol} {tf}: {e}")
        return None

# ============================================================================
# DATABASE & MONITORING
# ============================================================================

async def init_database():
    """Initialize database"""
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS romeopt_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                entry_tf TEXT NOT NULL,
                score INTEGER NOT NULL,
                sweep TEXT,
                displacement REAL,
                volume_ratio REAL,
                htf_strength REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                pnl REAL DEFAULT 0,
                reason TEXT
            )
        """)
        
        await db_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_status ON romeopt_signals(status)
        """)
        
        await db_conn.commit()
        log.info("Database initialized")
        
    except Exception as e:
        log.error(f"Database initialization failed: {e}")
        raise

async def log_signal_to_db(signal: Dict):
    """Log signal to database"""
    async with db_lock:
        try:
            await db_conn.execute("""
                INSERT INTO romeopt_signals 
                (symbol, side, entry, sl, tp1, tp2, entry_tf, score, 
                 sweep, displacement, volume_ratio, htf_strength, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal["symbol"], signal["side"], signal["entry"], signal["sl"],
                signal["tp1"], signal["tp2"], signal["entry_tf"], signal["score"],
                signal["sweep"], signal["displacement"], signal["volume_ratio"],
                signal["htf_strength"], signal["reason"]
            ))
            await db_conn.commit()
            
            # Send Telegram alert
            if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
                alert = f"""
🏆 <b>ROMEOPT-P SIGNAL</b>
Pair: {signal['symbol']} ({signal['entry_tf']})
Side: {signal['side']}
Entry: {signal['entry']:.4f}
SL: {signal['sl']:.4f}
TP1: {signal['tp1']:.4f}
TP2: {signal['tp2']:.4f}
Score: {signal['score']}/9
Sweep: {signal['sweep']}
Disp: {signal['displacement']:.2f}
Volume: {signal['volume_ratio']:.1f}x
                """
                
                escaped = (alert
                          .replace("&", "&amp;")
                          .replace("<", "&lt;")
                          .replace(">", "&gt;"))
                
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                async with httpx.AsyncClient() as client:
                    await client.post(url, json={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": escaped,
                        "parse_mode": "HTML"
                    })
            
        except Exception as e:
            log.error(f"Failed to log signal: {e}")

async def monitor_trades(exchange):
    """Monitor and manage trades"""
    while True:
        try:
            async with db_lock:
                cursor = await db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status 
                    FROM romeopt_signals 
                    WHERE status = 'OPEN'
                """)
                
                trades = await cursor.fetchall()
                
                if trades:
                    log.info(f"Monitoring {len(trades)} open trades")
                
                for trade in trades:
                    sig_id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status = trade
                    
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current = ticker['last']
                        
                        # Check TP/SL
                        if side == "BUY":
                            if current <= sl:
                                await db_conn.execute(
                                    "UPDATE romeopt_signals SET status='CLOSED' WHERE id=?",
                                    (sig_id,)
                                )
                                log.info(f"{symbol} SL hit at {current:.4f}")
                            elif not tp2_hit and current >= tp2:
                                await db_conn.execute(
                                    "UPDATE romeopt_signals SET status='CLOSED', tp2_hit=1 WHERE id=?",
                                    (sig_id,)
                                )
                                log.info(f"{symbol} TP2 hit at {current:.4f}")
                            elif not tp1_hit and current >= tp1:
                                await db_conn.execute(
                                    "UPDATE romeopt_signals SET tp1_hit=1, sl=? WHERE id=?",
                                    (entry, sig_id)  # Move SL to breakeven
                                )
                                log.info(f"{symbol} TP1 hit at {current:.4f}, SL moved to BE")
                        
                        else:  # SELL
                            if current >= sl:
                                await db_conn.execute(
                                    "UPDATE romeopt_signals SET status='CLOSED' WHERE id=?",
                                    (sig_id,)
                                )
                                log.info(f"{symbol} SL hit at {current:.4f}")
                            elif not tp2_hit and current <= tp2:
                                await db_conn.execute(
                                    "UPDATE romeopt_signals SET status='CLOSED', tp2_hit=1 WHERE id=?",
                                    (sig_id,)
                                )
                                log.info(f"{symbol} TP2 hit at {current:.4f}")
                            elif not tp1_hit and current <= tp1:
                                await db_conn.execute(
                                    "UPDATE romeopt_signals SET tp1_hit=1, sl=? WHERE id=?",
                                    (entry, sig_id)  # Move SL to breakeven
                                )
                                log.info(f"{symbol} TP1 hit at {current:.4f}, SL moved to BE")
                    
                    except Exception as e:
                        log.debug(f"Error checking {symbol}: {e}")
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(5)

# ============================================================================
# MAIN SCANNING LOOP
# ============================================================================

async def scan_markets(exchange):
    """Main market scanning loop"""
    cooldown = {}  # Symbol cooldown tracking
    scan_count = 0
    
    while True:
        try:
            scan_count += 1
            log.info(f"\n📊 Scan #{scan_count} starting...")
            
            # Get top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get('quoteVolume', 0)) 
                         for s, v in tickers.items() 
                         if '/USDT' in s and ':' not in s]
            
            if not usdt_pairs:
                log.warning("No USDT pairs found!")
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            
            # Sort and take top N
            top_pairs = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            log.info(f"Scanning top {len(top_pairs)} pairs")
            
            signals_found = 0
            
            # Scan each pair
            for symbol, volume in top_pairs:
                # Check cooldown (5 minutes)
                if symbol in cooldown:
                    if time.time() - cooldown[symbol] < 300:
                        continue
                
                log.debug(f"Scanning {symbol}...")
                
                # Try each timeframe
                for tf in TIMEFRAMES:
                    signal = await generate_romeopt_signal(exchange, symbol, tf)
                    
                    if signal:
                        await log_signal_to_db(signal)
                        cooldown[symbol] = time.time()
                        signals_found += 1
                        log.info(f"🎯 Found signal for {symbol} on {tf}")
                        break  # Move to next symbol
                
                # Small delay between symbols
                await asyncio.sleep(0.5)
            
            # Scan summary
            if signals_found > 0:
                log.info(f"✅ Scan complete: Found {signals_found} signals")
            else:
                log.info(f"📭 Scan complete: No signals found")
            
            # Wait for next scan
            log.info(f"⏳ Next scan in {SCAN_INTERVAL} seconds...")
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan error: {e}")
            await asyncio.sleep(30)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

async def main():
    """Main application"""
    log.info("🚀 Starting RomeOPT-P 10/10 System...")
    
    # Initialize
    await init_database()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    # Test connection
    try:
        tickers = await exchange.fetch_tickers()
        log.info(f"✅ Connected to exchange. Found {len(tickers)} tickers")
    except Exception as e:
        log.error(f"❌ Exchange connection failed: {e}")
        return
    
    # Send startup message
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        startup_msg = """
🚀 <b>ROMEOPT-P 10/10 SYSTEM STARTED</b>
✅ Full 6-step RomeOPT-P logic
✅ Real-time market scanning
✅ Professional risk management
✅ Telegram alerts enabled
        """
        
        escaped = (startup_msg
                  .replace("&", "&amp;")
                  .replace("<", "&lt;")
                  .replace(">", "&gt;"))
        
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient() as client:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": escaped,
                "parse_mode": "HTML"
            })
    
    # Run scanner and monitor
    log.info("\n" + "="*50)
    log.info("🎯 Starting scanner and monitor...")
    log.info("="*50 + "\n")
    
    try:
        await asyncio.gather(
            scan_markets(exchange),
            monitor_trades(exchange)
        )
    except KeyboardInterrupt:
        log.info("\n👋 Shutdown requested")
    except Exception as e:
        log.error(f"Fatal error: {e}")
    finally:
        if db_conn:
            await db_conn.close()
        log.info("System stopped")

if __name__ == "__main__":
    # Parse arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Start HTTP server")
    args = parser.parse_args()
    
    if args.http:
        # Start FastAPI
        app = FastAPI()
        
        @app.get("/")
        async def root():
            return {"status": "RomeOPT-P 10/10 System"}
        
        @app.get("/health")
        async def health():
            return {"status": "healthy"}
        
        @app.post("/webhook")
        async def webhook(request: Request):
            data = await request.json()
            log.info(f"Webhook: {data}")
            return {"status": "received"}
        
        uvicorn.run(app, host="0.0.0.0", port=9000)
    
    else:
        # Run trading system
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\nGoodbye!")