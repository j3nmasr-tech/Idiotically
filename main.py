#!/usr/bin/env python3
# LIQUIDITY TRAP SCANNER - PURE LOGIC VERSION
# ONLY: Liquidity Zones → Trap Detection → Bleeding → Direction → 1m Confirmation

import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import json
from typing import Dict, List, Optional, Tuple, Any

# ===== CONFIG =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")

# Exchange API (Bybit for perpetual futures)
BYBIT_API = "https://api.bybit.com/v5"
TIMEFRAMES = ["15m", "1h"]  # For liquidity zones
TRAP_TF = "5m"  # For trap detection
ENTRY_TF = "1m"  # For micro timing

# Liquidity proximity threshold (in ticks/percentage)
LIQUIDITY_PROXIMITY_THRESHOLD = 0.002  # 0.2%
MIN_VOLUME_USDT = 100000  # Minimum 24h volume
MAX_ALERTS = 100  # Maximum alerts per cycle

# ===== STATE =====
active_alerts = {}
last_alert_time = {}

# ===== DATA FUNCTIONS =====
def get_perp_symbols(min_volume: float = MIN_VOLUME_USDT) -> List[str]:
    """Get all perpetual futures symbols with sufficient volume"""
    try:
        url = f"{BYBIT_API}/market/tickers"
        params = {"category": "linear"}
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        
        symbols = []
        if data.get("retCode") == 0:
            for ticker in data["result"]["list"]:
                symbol = ticker["symbol"]
                volume = float(ticker.get("volume24h", 0))
                last_price = float(ticker.get("lastPrice", 0))
                
                # Only USDT pairs, exclude BTC/ETH for meme coin focus
                if (symbol.endswith("USDT") and 
                    volume * last_price >= min_volume and
                    not symbol.startswith("BTC") and
                    not symbol.startswith("ETH")):
                    symbols.append(symbol)
        
        return symbols[:100]  # Limit to top 100 by volume
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return ["1000PEPEUSDT", "1000BONKUSDT", "WIFUSDT", "MEMEUSDT"]  # Fallback

def get_klines(symbol: str, interval: str, limit: int = 200) -> Optional[pd.DataFrame]:
    """Get OHLCV data"""
    try:
        tf_map = {"15m": "15", "1h": "60", "5m": "5", "1m": "1"}
        if interval not in tf_map:
            return None
            
        url = f"{BYBIT_API}/market/kline"
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": tf_map[interval],
            "limit": limit
        }
        
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        
        if data.get("retCode") == 0 and "list" in data["result"]:
            df = pd.DataFrame(
                data["result"]["list"],
                columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"]
            )
            df = df[["open", "high", "low", "close", "volume"]].astype(float)
            return df.iloc[::-1].reset_index(drop=True)  # Reverse to chronological
            
    except Exception as e:
        print(f"Error getting klines for {symbol} {interval}: {e}")
    return None

def get_market_data(symbol: str) -> Dict:
    """Get all required market data for a symbol"""
    try:
        # 1. Price data
        price_data = {}
        for tf in TIMEFRAMES + [TRAP_TF, ENTRY_TF]:
            df = get_klines(symbol, tf, limit=100)
            if df is not None and len(df) > 20:
                price_data[tf] = df
        
        # 2. Open Interest and Funding Rate
        url = f"{BYBIT_API}/market/tickers"
        params = {"category": "linear", "symbol": symbol}
        response = requests.get(url, params=params, timeout=5)
        ticker_data = response.json()
        
        current_price = None
        oi = None
        funding = None
        volume_24h = None
        
        if ticker_data.get("retCode") == 0 and ticker_data["result"]["list"]:
            ticker = ticker_data["result"]["list"][0]
            current_price = float(ticker.get("lastPrice", 0))
            oi = float(ticker.get("openInterest", 0))
            funding = float(ticker.get("fundingRate", 0)) * 100  # Convert to percentage
            volume_24h = float(ticker.get("volume24h", 0))
        
        return {
            "symbol": symbol,
            "price_data": price_data,
            "current_price": current_price,
            "open_interest": oi,
            "funding_rate": funding,
            "volume_24h": volume_24h,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error getting market data for {symbol}: {e}")
        return None

# ===== STEP 1: LIQUIDITY ZONES =====
def identify_liquidity_zones(df_15m: pd.DataFrame, df_1h: pd.DataFrame, current_price: float) -> Dict:
    """Identify liquidity zones on 15m and 1h timeframes"""
    zones = {
        "swing_highs": [],
        "swing_lows": [],
        "equal_highs": [],
        "equal_lows": [],
        "round_numbers": [],
        "proximity_score": 0
    }
    
    # 1. Swing highs/lows on 1h
    if df_1h is not None and len(df_1h) > 50:
        for i in range(10, len(df_1h) - 10):
            # Swing high
            if (df_1h["high"].iloc[i] > df_1h["high"].iloc[i-10:i].max() and
                df_1h["high"].iloc[i] > df_1h["high"].iloc[i+1:i+11].max()):
                zones["swing_highs"].append(float(df_1h["high"].iloc[i]))
            
            # Swing low
            if (df_1h["low"].iloc[i] < df_1h["low"].iloc[i-10:i].min() and
                df_1h["low"].iloc[i] < df_1h["low"].iloc[i+1:i+11].min()):
                zones["swing_lows"].append(float(df_1h["low"].iloc[i]))
    
    # 2. Equal highs/lows on 15m
    if df_15m is not None and len(df_15m) > 100:
        price_levels = {}
        tolerance = current_price * 0.001  # 0.1% tolerance
        
        for i in range(len(df_15m)):
            high = df_15m["high"].iloc[i]
            low = df_15m["low"].iloc[i]
            
            # Cluster highs
            found = False
            for level in price_levels.keys():
                if abs(high - level) <= tolerance:
                    price_levels[level] += 1
                    found = True
                    break
            if not found:
                price_levels[high] = 1
            
            # Cluster lows
            found = False
            for level in price_levels.keys():
                if abs(low - level) <= tolerance:
                    price_levels[level] += 1
                    found = True
                    break
            if not found:
                price_levels[low] = 1
        
        # Get levels with at least 3 touches
        for level, count in price_levels.items():
            if count >= 3:
                if level > current_price:
                    zones["equal_highs"].append(float(level))
                else:
                    zones["equal_lows"].append(float(level))
    
    # 3. Round numbers (psychological levels)
    base = round(current_price, -int(np.log10(current_price)) + 1)
    for i in range(-5, 6):
        level = base + (i * base * 0.01)  # ±5% in 1% increments
        zones["round_numbers"].append(float(level))
    
    # 4. Calculate proximity score
    all_zones = (zones["swing_highs"] + zones["swing_lows"] + 
                 zones["equal_highs"] + zones["equal_lows"] + 
                 zones["round_numbers"])
    
    if all_zones:
        min_distance = min(abs(current_price - zone) / current_price for zone in all_zones)
        zones["proximity_score"] = 1 if min_distance <= LIQUIDITY_PROXIMITY_THRESHOLD else 0
        zones["nearest_zone"] = min(all_zones, key=lambda x: abs(current_price - x))
        zones["zone_type"] = "resistance" if zones["nearest_zone"] > current_price else "support"
    else:
        zones["proximity_score"] = 0
        zones["nearest_zone"] = None
        zones["zone_type"] = None
    
    return zones

# ===== STEP 2: TRAP DETECTION =====
def detect_trap(df_5m: pd.DataFrame, current_price: float, oi: float, 
                zones: Dict, oi_history: List[float]) -> Tuple[bool, Optional[str]]:
    """Detect if longs or shorts are trapped"""
    if df_5m is None or len(df_5m) < 20 or oi is None or len(oi_history) < 5:
        return False, None
    
    # 1. Check if OI is rising (last 5 periods)
    if len(oi_history) >= 5:
        oi_trend = np.polyfit(range(5), oi_history[-5:], 1)[0]
        oi_rising = oi_trend > 0
    else:
        oi_rising = oi > np.mean(oi_history) if oi_history else False
    
    # 2. Check if price is stalling near liquidity zone
    if zones["proximity_score"] == 1 and zones["nearest_zone"]:
        distance_pct = abs(current_price - zones["nearest_zone"]) / current_price
        price_stalling = distance_pct <= LIQUIDITY_PROXIMITY_THRESHOLD
        
        # Check recent volatility (low volatility = stalling)
        if len(df_5m) >= 10:
            recent_volatility = df_5m["close"].pct_change().std()
            avg_volatility = df_5m["close"].pct_change().rolling(50).mean().iloc[-1]
            price_stalling = price_stalling and (recent_volatility < avg_volatility * 0.7)
    
    else:
        price_stalling = False
    
    # 3. Determine trapped side
    if oi_rising and price_stalling:
        if zones["zone_type"] == "resistance":  # Price stalled near high
            return True, "LONGS_TRAPPED"
        elif zones["zone_type"] == "support":  # Price stalled near low
            return True, "SHORTS_TRAPPED"
    
    return False, None

# ===== STEP 3: BLEEDING DETECTION =====
def detect_bleeding(funding_rate: float, funding_history: List[float], 
                   trapped_side: str) -> Tuple[bool, Optional[str]]:
    """Check if trapped side is bleeding via funding rate"""
    if funding_rate is None or len(funding_history) < 3:
        return False, None
    
    # 1. Check funding trend
    if len(funding_history) >= 3:
        funding_trend = np.polyfit(range(3), funding_history[-3:], 1)[0]
    else:
        funding_trend = 0
    
    # 2. Determine bleeding side
    if trapped_side == "LONGS_TRAPPED":
        # Longs bleed when funding > 0 and rising
        if funding_rate > 0 and funding_trend > 0:
            return True, "LONGS_BLEEDING"
    
    elif trapped_side == "SHORTS_TRAPPED":
        # Shorts bleed when funding < 0 and becoming more negative
        if funding_rate < 0 and funding_trend < 0:
            return True, "SHORTS_BLEEDING"
    
    return False, None

# ===== STEP 4: DIRECTION DECISION =====
def determine_direction(trapped_side: str, bleeding_side: str) -> Optional[str]:
    """Binary direction decision based on trap and bleeding"""
    if trapped_side == "LONGS_TRAPPED" and bleeding_side == "LONGS_BLEEDING":
        return "SHORT"  # Trap longs, they're bleeding → Short
    
    elif trapped_side == "SHORTS_TRAPPED" and bleeding_side == "SHORTS_BLEEDING":
        return "LONG"  # Trap shorts, they're bleeding → Long
    
    return None

# ===== STEP 5: 1m ENTRY CONFIRMATION =====
def confirm_1m_entry(df_1m: pd.DataFrame, direction: str, zones: Dict) -> Tuple[bool, Dict]:
    """Micro timing confirmation on 1m chart"""
    if df_1m is None or len(df_1m) < 10:
        return False, {}
    
    current_price = df_1m["close"].iloc[-1]
    recent_candles = df_1m.iloc[-5:]
    
    confirmation_signals = {
        "wick_rejection": False,
        "failed_breakout": False,
        "pattern": False,
        "volume_spike": False,
        "support_resistance_break": False
    }
    
    if direction == "SHORT":
        # 1. Upper wick rejection at liquidity
        last_candle = recent_candles.iloc[-1]
        if last_candle["high"] > last_candle["close"]:
            wick_ratio = (last_candle["high"] - max(last_candle["open"], last_candle["close"])) / (last_candle["high"] - last_candle["low"])
            if wick_ratio > 0.4:  # Upper wick > 40% of candle
                confirmation_signals["wick_rejection"] = True
        
        # 2. Failed breakout
        if zones["nearest_zone"]:
            if (max(recent_candles["high"]) > zones["nearest_zone"] and 
                current_price < zones["nearest_zone"]):
                confirmation_signals["failed_breakout"] = True
        
        # 3. Lower high formation
        highs = recent_candles["high"].values
        if len(highs) >= 3 and highs[-1] < highs[-2] < highs[-3]:
            confirmation_signals["pattern"] = True
        
        # 4. Micro support break
        if len(df_1m) > 20:
            recent_low = df_1m["low"].iloc[-10:-1].min()
            if current_price < recent_low:
                confirmation_signals["support_resistance_break"] = True
    
    elif direction == "LONG":
        # 1. Lower wick rejection at liquidity
        last_candle = recent_candles.iloc[-1]
        if last_candle["low"] < last_candle["close"]:
            wick_ratio = (min(last_candle["open"], last_candle["close"]) - last_candle["low"]) / (last_candle["high"] - last_candle["low"])
            if wick_ratio > 0.4:  # Lower wick > 40% of candle
                confirmation_signals["wick_rejection"] = True
        
        # 2. Failed breakdown
        if zones["nearest_zone"]:
            if (min(recent_candles["low"]) < zones["nearest_zone"] and 
                current_price > zones["nearest_zone"]):
                confirmation_signals["failed_breakout"] = True
        
        # 3. Higher low formation
        lows = recent_candles["low"].values
        if len(lows) >= 3 and lows[-1] > lows[-2] > lows[-3]:
            confirmation_signals["pattern"] = True
        
        # 4. Micro resistance break
        if len(df_1m) > 20:
            recent_high = df_1m["high"].iloc[-10:-1].max()
            if current_price > recent_high:
                confirmation_signals["support_resistance_break"] = True
    
    # 5. Volume spike
    if len(df_1m) > 20:
        avg_volume = df_1m["volume"].rolling(20).mean().iloc[-1]
        current_volume = df_1m["volume"].iloc[-1]
        if current_volume > avg_volume * 1.5:
            confirmation_signals["volume_spike"] = True
    
    # Need at least 3 confirmations
    confirm_count = sum(confirmation_signals.values())
    return confirm_count >= 3, confirmation_signals

# ===== STEP 6: ALERT GENERATION =====
def generate_alert(symbol: str, direction: str, entry_price: float,
                  zones: Dict, confirmation_signals: Dict) -> Dict:
    """Generate complete trade alert with TP/SL levels"""
    
    # Calculate TP/SL based on liquidity zones
    if direction == "SHORT":
        # For shorts: TP1 at nearest support, TP2 at next, TP3 at major support
        supports = sorted(zones["swing_lows"] + zones["equal_lows"] + zones["round_numbers"])
        supports = [s for s in supports if s < entry_price]
        
        if len(supports) >= 2:
            tp1 = max(supports)  # Nearest support
            remaining = [s for s in supports if s < tp1]
            tp2 = max(remaining) if remaining else tp1 * 0.99
            tp3 = tp2 * 0.985 if len(remaining) > 1 else tp2 * 0.99
        else:
            tp1 = entry_price * 0.995
            tp2 = entry_price * 0.99
            tp3 = entry_price * 0.985
        
        # SL above resistance
        resistances = sorted(zones["swing_highs"] + zones["equal_highs"] + zones["round_numbers"])
        resistances = [r for r in resistances if r > entry_price]
        sl = min(resistances) if resistances else entry_price * 1.01
    
    else:  # LONG
        # For longs: TP1 at nearest resistance, TP2 at next, TP3 at major resistance
        resistances = sorted(zones["swing_highs"] + zones["equal_highs"] + zones["round_numbers"])
        resistances = [r for r in resistances if r > entry_price]
        
        if len(resistances) >= 2:
            tp1 = min(resistances)  # Nearest resistance
            remaining = [r for r in resistances if r > tp1]
            tp2 = min(remaining) if remaining else tp1 * 1.01
            tp3 = tp2 * 1.015 if len(remaining) > 1 else tp2 * 1.02
        else:
            tp1 = entry_price * 1.005
            tp2 = entry_price * 1.01
            tp3 = entry_price * 1.015
        
        # SL below support
        supports = sorted(zones["swing_lows"] + zones["equal_lows"] + zones["round_numbers"])
        supports = [s for s in supports if s < entry_price]
        sl = max(supports) if supports else entry_price * 0.99
    
    # Position sizing (simplified)
    risk_pct = 0.02  # 2% risk
    sl_distance_pct = abs(entry_price - sl) / entry_price
    position_size = risk_pct / sl_distance_pct if sl_distance_pct > 0 else 0.01
    
    alert = {
        "symbol": symbol,
        "direction": direction,
        "entry_price": round(entry_price, 6),
        "entry_zone": f"{entry_price * 0.998:.6f} - {entry_price * 1.002:.6f}",
        "tp1": round(tp1, 6),
        "tp2": round(tp2, 6),
        "tp3": round(tp3, 6),
        "sl": round(sl, 6),
        "position_size_pct": round(position_size * 100, 2),
        "risk_reward": round((tp1 - entry_price) / abs(entry_price - sl), 2) if direction == "LONG" else 
                      round((entry_price - tp1) / abs(entry_price - sl), 2),
        "confirmation_signals": confirmation_signals,
        "liquidity_zone": zones["nearest_zone"],
        "zone_type": zones["zone_type"],
        "timestamp": datetime.utcnow().isoformat(),
        "alert_id": f"{symbol}_{direction}_{int(time.time())}"
    }
    
    return alert

def send_telegram_alert(alert: Dict):
    """Send alert to Telegram"""
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured")
        return
    
    direction_emoji = "🔴" if alert["direction"] == "SHORT" else "🟢"
    
    message = f"""
{direction_emoji} **LIQUIDITY TRAP ALERT** {direction_emoji}

**Symbol:** `{alert['symbol']}`
**Direction:** {alert['direction']}
**Entry Zone:** `{alert['entry_zone']}`
**Current Price:** `{alert['entry_price']}`

**🎯 Take Profits:**
TP1: `{alert['tp1']}` (40-50%)
TP2: `{alert['tp2']}` (25-30%)
TP3: `{alert['tp3']}` (20-25%)

**🛑 Stop Loss:** `{alert['sl']}`
**📊 Risk/Reward:** {alert['risk_reward']}:1
**💰 Position Size:** {alert['position_size_pct']}% of capital

**📈 Reason:**
• Liquidity Zone: `{alert['liquidity_zone']}` ({alert['zone_type']})
• Confirmation Signals: {sum(alert['confirmation_signals'].values())}/5

**✅ Entry Conditions Met:**
{chr(10).join([f'• {k}: {"✅" if v else "❌"}' for k, v in alert['confirmation_signals'].items()])}

**🕒 Time:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
"""
    
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        requests.post(url, json=payload, timeout=10)
        print(f"Alert sent for {alert['symbol']}")
    except Exception as e:
        print(f"Error sending Telegram alert: {e}")

# ===== MAIN SCANNER LOOP =====
class LiquidityTrapScanner:
    def __init__(self):
        self.symbols = []
        self.history = {}  # Store OI and funding history
        self.cooldown = {}  # Alert cooldown per symbol
        self.alerts_sent = 0
        
    def update_symbols(self):
        """Update list of symbols to scan"""
        self.symbols = get_perp_symbols()
        print(f"Scanning {len(self.symbols)} symbols")
    
    def analyze_symbol(self, symbol: str) -> Optional[Dict]:
        """Complete analysis for a single symbol"""
        
        # Check cooldown
        if symbol in self.cooldown:
            if time.time() - self.cooldown[symbol] < 3600:  # 1 hour cooldown
                return None
        
        # 1. Get market data
        market_data = get_market_data(symbol)
        if not market_data:
            return None
        
        # Check if we have all required data
        required_tfs = TIMEFRAMES + [TRAP_TF, ENTRY_TF]
        if not all(tf in market_data["price_data"] for tf in required_tfs):
            return None
        
        # 2. Identify liquidity zones (Step 1)
        zones = identify_liquidity_zones(
            market_data["price_data"]["15m"],
            market_data["price_data"]["1h"],
            market_data["current_price"]
        )
        
        # Filter: Only coins near liquidity
        if zones["proximity_score"] == 0:
            return None
        
        # 3. Update history
        if symbol not in self.history:
            self.history[symbol] = {
                "oi": [],
                "funding": [],
                "timestamp": []
            }
        
        self.history[symbol]["oi"].append(market_data["open_interest"])
        self.history[symbol]["funding"].append(market_data["funding_rate"])
        self.history[symbol]["timestamp"].append(time.time())
        
        # Keep only last 10 entries
        for key in ["oi", "funding", "timestamp"]:
            if len(self.history[symbol][key]) > 10:
                self.history[symbol][key] = self.history[symbol][key][-10:]
        
        # 4. Detect trap (Step 2)
        trapped, trapped_side = detect_trap(
            market_data["price_data"][TRAP_TF],
            market_data["current_price"],
            market_data["open_interest"],
            zones,
            self.history[symbol]["oi"]
        )
        
        if not trapped:
            return None
        
        # 5. Detect bleeding (Step 3)
        bleeding, bleeding_side = detect_bleeding(
            market_data["funding_rate"],
            self.history[symbol]["funding"],
            trapped_side
        )
        
        if not bleeding:
            return None
        
        # 6. Determine direction (Step 4)
        direction = determine_direction(trapped_side, bleeding_side)
        if not direction:
            return None
        
        # 7. 1m confirmation (Step 5)
        confirmed, confirmation_signals = confirm_1m_entry(
            market_data["price_data"][ENTRY_TF],
            direction,
            zones
        )
        
        if not confirmed:
            return None
        
        # 8. Generate and send alert
        alert = generate_alert(
            symbol,
            direction,
            market_data["current_price"],
            zones,
            confirmation_signals
        )
        
        # Add trap and bleeding info
        alert["trap_side"] = trapped_side
        alert["bleeding_side"] = bleeding_side
        
        return alert
    
    def scan_cycle(self):
        """Run one complete scan cycle"""
        print(f"\n{'='*60}")
        print(f"SCAN CYCLE START: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"{'='*60}")
        
        self.update_symbols()
        
        alerts_found = []
        
        for i, symbol in enumerate(self.symbols, 1):
            print(f"[{i}/{len(self.symbols)}] Scanning {symbol}...")
            
            try:
                alert = self.analyze_symbol(symbol)
                if alert:
                    alerts_found.append(alert)
                    print(f"  ✅ ALERT FOUND: {symbol} {alert['direction']}")
                    
                    # Limit alerts per cycle
                    if len(alerts_found) >= MAX_ALERTS:
                        print(f"  ⚠️ Max alerts reached ({MAX_ALERTS})")
                        break
                
            except Exception as e:
                print(f"  ❌ Error analyzing {symbol}: {e}")
            
            time.sleep(0.1)  # Rate limiting
        
        # Sort alerts by R:R
        alerts_found.sort(key=lambda x: x["risk_reward"], reverse=True)
        
        # Send alerts
        for alert in alerts_found:
            send_telegram_alert(alert)
            self.cooldown[alert["symbol"]] = time.time()
            self.alerts_sent += 1
        
        print(f"\nCycle completed. Found {len(alerts_found)} alerts.")
        print(f"Total alerts sent: {self.alerts_sent}")
        
        return alerts_found

# ===== MAIN EXECUTION =====
if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════╗
║    LIQUIDITY TRAP SCANNER - PURE LOGIC EDITION   ║
║         LOW-CAP & MEME COIN FOCUS                ║
╚══════════════════════════════════════════════════╝
    
🔍 Scanning Logic Flow:
1. [LIQUIDITY] Price near swing highs/lows, equal highs/lows, round numbers
2. [TRAP] Open Interest rising + Price stalling near liquidity
3. [BLEEDING] Funding rate confirms trapped side is bleeding
4. [DIRECTION] Binary decision: LONG or SHORT
5. [CONFIRMATION] 1m micro timing patterns
6. [ALERT] Entry zone, TP1/TP2/TP3, SL, R:R
    
⚠️  Filters:
• Only coins near liquidity (proximity_score = 1)
• Only when one side is trapped AND bleeding
• Maximum {MAX_ALERTS} alerts per cycle
• 1-hour cooldown per symbol
    
🚀 Starting scanner...
    """)
    
    scanner = LiquidityTrapScanner()
    
    # Initial scan
    scanner.scan_cycle()
    
    # Continuous scanning
    while True:
        try:
            time.sleep(60)  # Scan every 1 minute
            scanner.scan_cycle()
        except KeyboardInterrupt:
            print("\nScanner stopped by user")
            break
        except Exception as e:
            print(f"Fatal error in main loop: {e}")
            time.sleep(30)