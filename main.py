#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ROMEOPT-P 10/10 ULTIMATE SCANNER - DEBUGGING VERSION
- Shows exactly what's happening during scanning
- No silent failures
- Live progress reporting
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
# CONFIGURATION - RELAXED FOR TESTING
# ============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/signals_10_10.db")

SCAN_INTERVAL = 5  # Faster scanning for debugging
TOP_N = 10  # Only scan top 10 pairs
TIMEFRAMES = ["5m", "15m"]  # Start with just 2 timeframes
MIN_SCORE = 5  # Lower minimum for testing

# RomeOPT-P Timeframe Hierarchy
HTF_MAP = {
    "5m": "1h",
    "15m": "4h", 
    "1h": "4h"
}

# ============================================================================
# LOGGING - VERBOSE FOR DEBUGGING
# ============================================================================

logging.basicConfig(
    level=logging.DEBUG,  # DEBUG level to see everything
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("romeopt_debug")
db_lock = asyncio.Lock()
db_conn = None

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def escape_html(msg: str) -> str:
    if not msg: return ""
    return (str(msg)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))

async def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": safe_msg,
                "parse_mode": "HTML"
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ============================================================================
# DATABASE - SIMPLIFIED
# ============================================================================

async def init_db():
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                entry_tf TEXT NOT NULL,
                score INTEGER NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                reason TEXT
            )
        """)
        
        await db_conn.commit()
        log.info("Database initialized")
        
    except Exception as e:
        log.error(f"Database init failed: {e}")
        raise

# ============================================================================
# DATA FETCHING WITH DEBUGGING
# ============================================================================

async def fetch_ohlcv_debug(exchange, symbol: str, timeframe: str, limit: int = 50):
    """Fetch OHLCV with detailed debugging"""
    try:
        log.debug(f"📡 Fetching {symbol} {timeframe}...")
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        
        if not ohlcv:
            log.debug(f"❌ No data for {symbol} {timeframe}")
            return None
        
        log.debug(f"✅ Got {len(ohlcv)} candles for {symbol} {timeframe}")
        
        # Validate structure
        if len(ohlcv[0]) < 6:
            log.debug(f"⚠️  Bad candle format for {symbol}: {ohlcv[0]}")
            return None
            
        return ohlcv
        
    except Exception as e:
        log.debug(f"❌ Fetch failed for {symbol} {timeframe}: {e}")
        return None

def create_dataframe_safe(ohlcv_data):
    """Create DataFrame safely"""
    if not ohlcv_data:
        log.debug("No OHLCV data provided")
        return None
    
    try:
        df = pd.DataFrame(ohlcv_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        
        # Quick validation
        log.debug(f"DataFrame shape: {df.shape}")
        log.debug(f"Columns: {df.columns.tolist()}")
        log.debug(f"Sample close prices: {df['close'].head(3).tolist()}")
        
        # Convert to numeric
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Check for NaNs
        nan_count = df[["open", "high", "low", "close"]].isnull().sum().sum()
        if nan_count > 0:
            log.debug(f"⚠️  Found {nan_count} NaN values, filling...")
            df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].ffill()
        
        if len(df) < 20:
            log.debug(f"❌ DataFrame too short: {len(df)} rows")
            return None
            
        log.debug(f"✅ DataFrame created successfully: {len(df)} rows")
        return df
        
    except Exception as e:
        log.debug(f"❌ DataFrame creation failed: {e}")
        return None

# ============================================================================
# SIMPLIFIED SIGNAL GENERATION
# ============================================================================

def find_simple_ob(df: pd.DataFrame) -> Optional[Dict]:
    """Find simple order block"""
    if len(df) < 10:
        return None
    
    # Look at last 5 candles
    for i in range(-5, -1):
        idx = len(df) + i
        if idx < 1:
            continue
            
        current = df.iloc[idx]
        prev = df.iloc[idx - 1]
        
        # Bullish OB: Bearish → Bullish
        if (prev['close'] < prev['open'] and 
            current['close'] > current['open'] and
            current['low'] < prev['low']):
            
            return {
                'type': 'bullish',
                'low': float(min(current['low'], prev['low'])),
                'high': float(max(current['close'], prev['close'])),
                'index': idx
            }
        
        # Bearish OB: Bullish → Bearish
        elif (prev['close'] > prev['open'] and 
              current['close'] < current['open'] and
              current['high'] > prev['high']):
            
            return {
                'type': 'bearish',
                'low': float(min(current['close'], prev['close'])),
                'high': float(max(current['high'], prev['high'])),
                'index': idx
            }
    
    return None

async def generate_simple_signal(exchange, symbol: str, tf: str) -> Optional[Dict]:
    """Generate simple signal for debugging"""
    try:
        log.debug(f"🔍 Scanning {symbol} {tf}...")
        
        # Fetch data
        ohlcv = await fetch_ohlcv_debug(exchange, symbol, tf, 50)
        if not ohlcv:
            log.debug(f"❌ No OHLCV for {symbol} {tf}")
            return None
        
        df = create_dataframe_safe(ohlcv)
        if df is None:
            log.debug(f"❌ No DataFrame for {symbol} {tf}")
            return None
        
        # Get current price
        current_price = float(df['close'].iloc[-1])
        log.debug(f"💰 Current price: {current_price}")
        
        # Find OB
        ob = find_simple_ob(df)
        if not ob:
            log.debug(f"❌ No OB found for {symbol} {tf}")
            return None
        
        log.debug(f"📊 OB found: {ob['type']} at {ob['low']}-{ob['high']}")
        
        # Check if price is in OB zone
        side = "BUY" if ob['type'] == 'bullish' else "SELL"
        in_zone = ob['low'] <= current_price <= ob['high']
        
        if not in_zone:
            log.debug(f"❌ Price {current_price} not in OB zone {ob['low']}-{ob['high']}")
            return None
        
        log.debug(f"✅ Price in OB zone!")
        
        # Calculate simple score
        score = 0
        reasons = []
        
        # 1. Liquidity sweep (simplified)
        last_high = df['high'].iloc[-1]
        prev_highs = df['high'].iloc[-10:-1]
        if last_high > prev_highs.max():
            score += 2
            reasons.append("Sweep High")
            log.debug(f"✅ Liquidity sweep detected")
        
        # 2. Displacement
        last_candle = df.iloc[-1]
        body_size = abs(last_candle['close'] - last_candle['open'])
        candle_range = last_candle['high'] - last_candle['low']
        if candle_range > 0:
            displacement = body_size / candle_range
            if displacement > 0.6:
                score += 2
                reasons.append(f"Disp {displacement:.2f}")
                log.debug(f"✅ Displacement: {displacement:.2f}")
        
        # 3. Volume check
        vol_avg = df['volume'].iloc[-10:-1].mean()
        if vol_avg > 0:
            vol_ratio = last_candle['volume'] / vol_avg
            if vol_ratio > 1.5:
                score += 1
                reasons.append(f"Vol {vol_ratio:.1f}x")
                log.debug(f"✅ Volume spike: {vol_ratio:.1f}x")
        
        # Minimum score
        if score < MIN_SCORE:
            log.debug(f"❌ Score {score} < minimum {MIN_SCORE}")
            return None
        
        # Calculate TP/SL
        atr = calculate_simple_atr(df)
        if side == "BUY":
            sl = ob['low'] - (atr * 0.1)
            risk = current_price - sl
            tp1 = current_price + risk
            tp2 = current_price + (risk * 1.5)
        else:
            sl = ob['high'] + (atr * 0.1)
            risk = sl - current_price
            tp1 = current_price - risk
            tp2 = current_price - (risk * 1.5)
        
        log.debug(f"🎯 TP/SL calculated: SL={sl:.4f}, TP1={tp1:.4f}, TP2={tp2:.4f}")
        
        # Create signal
        signal = {
            "symbol": symbol,
            "side": side,
            "entry": current_price,
            "sl": float(sl),
            "tp1": float(tp1),
            "tp2": float(tp2),
            "entry_tf": tf,
            "score": score,
            "reason": " | ".join(reasons),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        
        log.info(f"✅ SIGNAL GENERATED: {symbol} {tf} {side} Score: {score}")
        return signal
        
    except Exception as e:
        log.error(f"❌ Signal generation failed for {symbol} {tf}: {e}")
        import traceback
        log.debug(f"Traceback: {traceback.format_exc()}")
        return None

def calculate_simple_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Simple ATR calculation"""
    try:
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        
        tr_values = []
        for i in range(1, min(period, len(df))):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr = max(hl, hc, lc)
            tr_values.append(tr)
        
        if tr_values:
            return float(np.mean(tr_values[-period:]))
        return 0.001 * df['close'].iloc[-1]  # Fallback
    except:
        return 0.001 * df['close'].iloc[-1]  # Fallback

# ============================================================================
# DEBUGGING SCANNER - SHOWS EVERYTHING
# ============================================================================

async def debug_scanner(exchange):
    """Scanner that shows exactly what's happening"""
    scan_count = 0
    
    while True:
        try:
            scan_count += 1
            log.info(f"\n{'='*50}")
            log.info(f"SCAN #{scan_count} - {datetime.datetime.utcnow().strftime('%H:%M:%S')}")
            log.info(f"{'='*50}")
            
            # Get tickers
            log.info("📊 Fetching tickers...")
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get('quoteVolume', 0)) 
                         for s, v in tickers.items() 
                         if '/USDT' in s and ':' not in s]
            
            if not usdt_pairs:
                log.warning("❌ No USDT pairs found!")
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            
            # Sort by volume
            top_pairs = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            log.info(f"📈 Top {len(top_pairs)} pairs by volume:")
            
            for i, (symbol, volume) in enumerate(top_pairs, 1):
                log.info(f"  {i}. {symbol}: ${volume:,.0f}")
            
            signals_found = 0
            
            # Scan each pair
            for symbol, volume in top_pairs:
                log.info(f"\n🔎 Scanning {symbol}...")
                
                for tf in TIMEFRAMES:
                    log.info(f"  ⏰ Timeframe: {tf}")
                    
                    signal = await generate_simple_signal(exchange, symbol, tf)
                    
                    if signal:
                        signals_found += 1
                        
                        # Log to database
                        async with db_lock:
                            await db_conn.execute("""
                                INSERT INTO signals 
                                (symbol, side, entry, sl, tp1, tp2, entry_tf, score, reason)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                signal["symbol"], signal["side"], signal["entry"], signal["sl"],
                                signal["tp1"], signal["tp2"], signal["entry_tf"], signal["score"],
                                signal["reason"]
                            ))
                            await db_conn.commit()
                        
                        # Send Telegram
                        alert = f"""
🔔 <b>SIGNAL FOUND!</b>
Pair: {signal['symbol']} ({signal['entry_tf']})
Side: {signal['side']}
Entry: {signal['entry']:.4f}
SL: {signal['sl']:.4f}
TP1: {signal['tp1']:.4f}
TP2: {signal['tp2']:.4f}
Score: {signal['score']}/5
                        """
                        await send_telegram(alert)
                        
                        log.info(f"  🚨 SIGNAL LOGGED: {signal['side']} at {signal['entry']}")
                        break  # Move to next symbol
                    else:
                        log.info(f"  ➖ No signal on {tf}")
            
            # Scan summary
            if signals_found > 0:
                log.info(f"\n🎉 Scan complete: Found {signals_found} signals!")
            else:
                log.info(f"\n📭 Scan complete: No signals found")
            
            # Wait for next scan
            log.info(f"\n⏳ Next scan in {SCAN_INTERVAL} seconds...")
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"\n💥 SCAN ERROR: {e}")
            import traceback
            log.error(f"Traceback: {traceback.format_exc()}")
            await asyncio.sleep(30)  # Wait longer on error

# ============================================================================
# SIMPLE MONITOR
# ============================================================================

async def simple_monitor(exchange):
    """Simple trade monitor"""
    while True:
        try:
            async with db_lock:
                cursor = await db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp1, tp2, status 
                    FROM signals 
                    WHERE status = 'OPEN'
                """)
                
                open_trades = await cursor.fetchall()
                
                if open_trades:
                    log.info(f"\n👀 Monitoring {len(open_trades)} open trades...")
                
                for trade in open_trades:
                    sig_id, symbol, side, entry, sl, tp1, tp2, status = trade
                    
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current = ticker['last']
                        
                        log.debug(f"  {symbol}: Entry={entry:.4f}, Current={current:.4f}, SL={sl:.4f}")
                        
                        # Check TP/SL
                        if side == "BUY":
                            if current <= sl:
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                                log.info(f"  ❌ {symbol} SL hit at {current:.4f}")
                            elif current >= tp2:
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                                log.info(f"  ✅ {symbol} TP2 hit at {current:.4f}")
                            elif current >= tp1:
                                log.debug(f"  ⚠️  {symbol} TP1 reached")
                        else:
                            if current >= sl:
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                                log.info(f"  ❌ {symbol} SL hit at {current:.4f}")
                            elif current <= tp2:
                                await db_conn.execute("UPDATE signals SET status='CLOSED' WHERE id=?", (sig_id,))
                                log.info(f"  ✅ {symbol} TP2 hit at {current:.4f}")
                            elif current <= tp1:
                                log.debug(f"  ⚠️  {symbol} TP1 reached")
                    
                    except Exception as e:
                        log.debug(f"  ❗ Error checking {symbol}: {e}")
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(10)  # Check every 10 seconds

# ============================================================================
# MAIN WITH PROPER ERROR HANDLING
# ============================================================================

async def main():
    """Main function with startup checks"""
    log.info("🚀 STARTING ROMEOPT-P DEBUGGING SYSTEM...")
    
    # Initialize database
    try:
        await init_db()
        log.info("✅ Database ready")
    except Exception as e:
        log.error(f"❌ Database failed: {e}")
        return
    
    # Initialize exchange
    try:
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        
        # Test exchange connection
        log.info("🔗 Testing exchange connection...")
        tickers = await exchange.fetch_tickers()
        log.info(f"✅ Exchange connected! Found {len(tickers)} tickers")
        
    except Exception as e:
        log.error(f"❌ Exchange connection failed: {e}")
        return
    
    # Send startup message
    await send_telegram("🔧 <b>ROMEOPT-P DEBUG SYSTEM STARTED</b>\nNow scanning with full logging...")
    
    # Run scanner and monitor
    try:
        log.info("\n" + "="*60)
        log.info("🎯 STARTING SCANNER AND MONITOR")
        log.info("="*60)
        
        await asyncio.gather(
            debug_scanner(exchange),
            simple_monitor(exchange)
        )
        
    except KeyboardInterrupt:
        log.info("\n👋 Shutdown requested by user")
    except Exception as e:
        log.error(f"\n💥 Fatal error: {e}")
        import traceback
        log.error(f"Traceback: {traceback.format_exc()}")
    finally:
        log.info("🛑 System stopped")

if __name__ == "__main__":
    # Run with full debugging
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye!")
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")