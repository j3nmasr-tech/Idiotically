import os
import asyncio
import websockets
import json
import hmac
import hashlib
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
from dataclasses import dataclass
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import asyncio
from asyncio import Semaphore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('romeopt_top40_scanner.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class TradeSignal:
    symbol: str
    direction: str  # "LONG" or "SHORT"
    entry_price: float
    timestamp: datetime
    liquidity_sweep: Dict
    displacement: Dict
    retracement_zone: Dict
    tp_levels: List[float]
    sl_level: float
    status: str = "ACTIVE"
    tp_hit: List[bool] = None
    
    def __post_init__(self):
        if self.tp_hit is None:
            self.tp_hit = [False, False, False]

class RomeOPTTop40Scanner:
    def __init__(self):
        # Load environment variables
        self.api_key = os.getenv('BINGX_API_KEY')
        self.api_secret = os.getenv('BINGX_API_SECRET')
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Validate credentials
        if not all([self.api_key, self.api_secret, self.telegram_token, self.telegram_chat_id]):
            raise ValueError("Missing required environment variables")
        
        # TOP 40 CRYPTOCURRENCIES (By Market Cap)
        self.symbols = [
            # Major Pairs
            'BTC-USDT', 'ETH-USDT', 'BNB-USDT', 'SOL-USDT', 'XRP-USDT',
            # Large Caps
            'ADA-USDT', 'AVAX-USDT', 'DOGE-USDT', 'DOT-USDT', 'TRX-USDT',
            'LINK-USDT', 'MATIC-USDT', 'LTC-USDT', 'BCH-USDT', 'ATOM-USDT',
            # Mid Caps
            'XLM-USDT', 'FIL-USDT', 'ETC-USDT', 'XTZ-USDT', 'XMR-USDT',
            'EOS-USDT', 'AAVE-USDT', 'ALGO-USDT', 'NEO-USDT', 'MKR-USDT',
            # DeFi & Emerging
            'COMP-USDT', 'YFI-USDT', 'SUSHI-USDT', 'SNX-USDT', 'UNI-USDT',
            'CRV-USDT', 'SAND-USDT', 'MANA-USDT', 'GALA-USDT', 'ENJ-USDT',
            # Layer 1 & Infrastructure
            'NEAR-USDT', 'FTM-USDT', 'ONE-USDT', 'VET-USDT', 'ICP-USDT',
            'FLOW-USDT', 'EGLD-USDT', 'THETA-USDT', 'HBAR-USDT', 'KLAY-USDT'
        ]
        
        self.timeframe = '1m'
        self.max_signal_age = timedelta(minutes=3)
        
        # Performance optimization for 40 coins
        self.max_concurrent_websockets = 20  # Batch connections
        self.analysis_semaphore = Semaphore(10)  # Limit concurrent analysis
        self.price_data: Dict[str, List] = {}
        self.active_signals: Dict[str, TradeSignal] = {}
        self.htf_bias: Dict[str, str] = {}
        
        # Rate limiting
        self.last_analysis_time: Dict[str, datetime] = {}
        self.analysis_cooldown = timedelta(seconds=2)  # Analyze each coin every 2 seconds
        
        # Thread pool for blocking operations
        self.thread_pool = ThreadPoolExecutor(max_workers=10)
        
        logging.info(f"🚀 RomeOPT Top 40 Scanner initialized - Monitoring {len(self.symbols)} coins")

    # ==================== PERFORMANCE-OPTIMIZED DATA FEED ====================

    async def start_websocket_feeds(self):
        """Start optimized WebSocket feeds for all 40 coins in batches"""
        logging.info("📡 Starting WebSocket feeds for Top 40 coins...")
        
        # Process in batches to avoid connection limits
        batch_size = 10
        for i in range(0, len(self.symbols), batch_size):
            batch = self.symbols[i:i + batch_size]
            tasks = [self.connect_bingx_websocket(symbol) for symbol in batch]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(1)  # Stagger connections

    async def connect_bingx_websocket(self, symbol: str):
        """Optimized WebSocket connection with error handling"""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                ws_url = "wss://open-api-swap.bingx.com/swap-market"
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as websocket:
                    logging.info(f"✅ WebSocket connected for {symbol}")
                    
                    # Subscribe to kline data
                    subscribe_msg = {
                        "id": f"{symbol}_{int(time.time())}",
                        "reqType": "sub",
                        "dataType": f"{symbol}@kline_{self.timeframe}"
                    }
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            data = json.loads(message)
                            await self.handle_websocket_data(symbol, data)
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            await websocket.ping()
                            continue
                            
            except Exception as e:
                logging.warning(f"⚠️ WebSocket connection failed for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                else:
                    logging.error(f"❌ Failed to connect WebSocket for {symbol} after {max_retries} attempts")
                    break

    async def handle_websocket_data(self, symbol: str, data: Dict):
        """Efficiently handle incoming WebSocket data for 40 coins"""
        try:
            if 'data' in data and data['data']:
                candle_data = data['data']
                
                # Convert to standard format
                current_candle = {
                    'timestamp': datetime.fromtimestamp(candle_data['t'] / 1000),
                    'open': float(candle_data['o']),
                    'high': float(candle_data['h']),
                    'low': float(candle_data['l']),
                    'close': float(candle_data['c']),
                    'volume': float(candle_data['v']),
                    'is_closed': candle_data['x']
                }
                
                # Initialize price data for symbol if needed
                if symbol not in self.price_data:
                    self.price_data[symbol] = []
                
                # Update price data (keep only last 30 candles for memory efficiency)
                self.price_data[symbol].append(current_candle)
                if len(self.price_data[symbol]) > 30:
                    self.price_data[symbol] = self.price_data[symbol][-30:]
                
                # Rate-limited analysis to prevent CPU overload
                await self.schedule_analysis(symbol, current_candle)
                
        except Exception as e:
            logging.error(f"Data handling error for {symbol}: {e}")

    async def schedule_analysis(self, symbol: str, current_candle: Dict):
        """Rate-limited analysis scheduling to handle 40 coins efficiently"""
        current_time = datetime.now()
        
        # Check if we should analyze this symbol (cooldown period)
        if symbol in self.last_analysis_time:
            time_since_last = current_time - self.last_analysis_time[symbol]
            if time_since_last < self.analysis_cooldown:
                return
        
        # Update last analysis time
        self.last_analysis_time[symbol] = current_time
        
        # Analyze only forming candles (not closed ones)
        if not current_candle['is_closed']:
            async with self.analysis_semaphore:
                try:
                    signal = await asyncio.get_event_loop().run_in_executor(
                        self.thread_pool, 
                        self.generate_signal, 
                        symbol, 
                        current_candle
                    )
                    if signal and self.is_live_signal(signal):
                        signal_id = f"{symbol}_{signal.timestamp.timestamp()}"
                        if signal_id not in self.active_signals:
                            self.active_signals[signal_id] = signal
                            await self.send_signal_alert(signal)
                except Exception as e:
                    logging.error(f"Analysis error for {symbol}: {e}")

    # ==================== OPTIMIZED 6-STEP SEQUENCE ====================

    def generate_signal(self, symbol: str, current_data: Dict) -> Optional[TradeSignal]:
        """
        PERFORMANCE-OPTIMIZED 6-step RomeOPT sequence for 40 coins
        """
        try:
            # Quick pre-filter: need minimum data
            if symbol not in self.price_data or len(self.price_data[symbol]) < 10:
                return None
            
            # STEP 1: Liquidity Sweep
            sweep_ok, sweep_info = self.step_1_liquidity_sweep(symbol, current_data)
            if not sweep_ok:
                return None
            
            # STEP 2: Displacement  
            displacement_ok, displacement_info = self.step_2_displacement(symbol, current_data, sweep_info)
            if not displacement_ok:
                return None
            
            direction = displacement_info['direction']
            
            # STEP 3: Retracement into Zone
            retracement_ok, zone_info = self.step_3_retracement_into_zone(
                symbol, current_data, sweep_info, displacement_info)
            if not retracement_ok:
                return None
            
            # STEP 4: Premium/Discount
            premium_ok, eq_info = self.step_4_premium_discount(symbol, current_data, direction)
            if not premium_ok:
                return None
            
            # STEP 5: HTF Bias Alignment
            htf_ok, bias_info = self.step_5_htf_bias_alignment(symbol, direction)
            if not htf_ok and bias_info.get('alignment') == 'MISALIGNED':
                return None  # Only reject if clearly misaligned
            
            # STEP 6: Momentum & Volatility Confirmation
            momentum_ok, confirmation_info = self.step_6_momentum_volatility_confirmation(
                symbol, current_data, direction)
            if not momentum_ok:
                return None
            
            # ALL STEPS PASSED - Generate signal
            entry_price = current_data['close']
            tp_levels, sl_level = self.calculate_tp_sl(
                symbol, direction, entry_price, sweep_info, displacement_info)
            
            signal = TradeSignal(
                symbol=symbol,
                direction=direction,
                entry_price=entry_price,
                timestamp=datetime.now(),
                liquidity_sweep=sweep_info,
                displacement=displacement_info,
                retracement_zone=zone_info,
                tp_levels=tp_levels,
                sl_level=sl_level
            )
            
            logging.info(f"🎯 SIGNAL: {direction} {symbol} @ {entry_price}")
            return signal
            
        except Exception as e:
            logging.error(f"Signal generation error for {symbol}: {e}")
            return None

    # ==================== OPTIMIZED STEP IMPLEMENTATIONS ====================

    def step_1_liquidity_sweep(self, symbol: str, current_data: Dict) -> Tuple[bool, Optional[Dict]]:
        """Optimized liquidity sweep detection"""
        try:
            recent_data = self.price_data.get(symbol, [])
            if len(recent_data) < 8:
                return False, None
            
            current_price = current_data['close']
            current_high = current_data['high']
            current_low = current_data['low']
            
            # Use last 6 candles for efficiency
            recent_lows = [candle['low'] for candle in recent_data[-6:]]
            recent_highs = [candle['high'] for candle in recent_data[-6:]]
            
            # Bullish sweep detection
            if len(recent_lows) >= 3:
                min_low = min(recent_lows[:-1])
                if current_low < min_low and current_price > min_low:
                    return True, {
                        'type': 'BULLISH_SWEEP',
                        'sweep_level': min_low,
                        'current_low': current_low
                    }
            
            # Bearish sweep detection  
            if len(recent_highs) >= 3:
                max_high = max(recent_highs[:-1])
                if current_high > max_high and current_price < max_high:
                    return True, {
                        'type': 'BEARISH_SWEEP',
                        'sweep_level': max_high,
                        'current_high': current_high
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"Step 1 error for {symbol}: {e}")
            return False, None

    def step_2_displacement(self, symbol: str, current_data: Dict, sweep_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """Optimized displacement detection"""
        try:
            current_open = current_data['open']
            current_high = current_data['high']
            current_low = current_data['low']
            current_close = current_data['close']
            
            candle_range = current_high - current_low
            if candle_range == 0:
                return False, None
            
            body_size = abs(current_close - current_open)
            body_percentage = (body_size / candle_range) * 100
            
            # Check impulse candle
            if body_percentage >= 60:
                direction = "BULLISH" if current_close > current_open else "BEARISH"
                
                # Validate alignment with sweep
                if (sweep_info['type'] == 'BULLISH_SWEEP' and direction == "BULLISH") or \
                   (sweep_info['type'] == 'BEARISH_SWEEP' and direction == "BEARISH"):
                    return True, {
                        'direction': direction,
                        'body_percentage': body_percentage,
                        'impulse_candle': current_data
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"Step 2 error for {symbol}: {e}")
            return False, None

    def step_3_retracement_into_zone(self, symbol: str, current_data: Dict, 
                                   sweep_info: Dict, displacement_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """Optimized zone retracement detection"""
        try:
            current_price = current_data['close']
            direction = displacement_info['direction']
            displacement_candle = displacement_info['impulse_candle']
            
            fvg_low = displacement_candle['low']
            fvg_high = displacement_candle['high']
            
            if direction == "BULLISH":
                if fvg_low <= current_price <= fvg_high:
                    return True, {
                        'type': 'BULLISH_ZONE',
                        'zone_low': fvg_low,
                        'zone_high': fvg_high
                    }
            else:  # BEARISH
                if fvg_low <= current_price <= fvg_high:
                    return True, {
                        'type': 'BEARISH_ZONE',
                        'zone_low': fvg_low,
                        'zone_high': fvg_high
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"Step 3 error for {symbol}: {e}")
            return False, None

    def step_4_premium_discount(self, symbol: str, current_data: Dict, direction: str) -> Tuple[bool, Optional[Dict]]:
        """Optimized premium/discount check"""
        try:
            current_price = current_data['close']
            equilibrium = self.calculate_equilibrium(symbol)
            
            if direction == "BULLISH" and current_price < equilibrium:
                return True, {'position': 'DISCOUNT'}
            elif direction == "BEARISH" and current_price > equilibrium:
                return True, {'position': 'PREMIUM'}
            
            return False, None
            
        except Exception as e:
            logging.error(f"Step 4 error for {symbol}: {e}")
            return False, None

    def step_5_htf_bias_alignment(self, symbol: str, direction: str) -> Tuple[bool, Optional[Dict]]:
        """Optimized HTF bias check with caching"""
        try:
            # Cache HTF bias to avoid recalculating frequently
            if symbol not in self.htf_bias:
                self.htf_bias[symbol] = self.calculate_htf_bias(symbol)
            
            htf_bias = self.htf_bias[symbol]
            
            if htf_bias == "UNKNOWN":
                return True, {'htf_bias': 'UNKNOWN', 'alignment': 'UNKNOWN'}
            
            if (direction == "BULLISH" and htf_bias == "BULLISH") or \
               (direction == "BEARISH" and htf_bias == "BEARISH"):
                return True, {'htf_bias': htf_bias, 'alignment': 'PERFECT'}
            else:
                return False, {'htf_bias': htf_bias, 'alignment': 'MISALIGNED'}
                
        except Exception as e:
            logging.error(f"Step 5 error for {symbol}: {e}")
            return True, {'htf_bias': 'UNKNOWN', 'alignment': 'UNKNOWN'}  # Don't reject on error

    def step_6_momentum_volatility_confirmation(self, symbol: str, current_data: Dict, 
                                              direction: str) -> Tuple[bool, Optional[Dict]]:
        """Optimized momentum/volatility check"""
        try:
            # Simplified checks for performance - can be enhanced
            volume_ok = current_data.get('volume', 0) > 1000  # Minimum volume threshold
            price_ok = current_data['close'] > 0.01  # Minimum price threshold
            
            if volume_ok and price_ok:
                return True, {
                    'volume_status': 'ADEQUATE',
                    'price_status': 'NORMAL'
                }
            return False, None
            
        except Exception as e:
            logging.error(f"Step 6 error for {symbol}: {e}")
            return False, None

    # ==================== OPTIMIZED UTILITIES ====================

    def calculate_equilibrium(self, symbol: str) -> float:
        """Fast equilibrium calculation"""
        recent_data = self.price_data.get(symbol, [])
        if not recent_data:
            return 0.0
        
        # Use last 10 candles for efficiency
        recent_data = recent_data[-10:]
        highs = [candle['high'] for candle in recent_data]
        lows = [candle['low'] for candle in recent_data]
        return (max(highs) + min(lows)) / 2

    def calculate_htf_bias(self, symbol: str) -> str:
        """Fast HTF bias calculation"""
        # Simplified - implement with actual 15min/1H data
        # For now, return neutral to avoid filtering out signals
        return "UNKNOWN"

    def is_live_signal(self, signal: TradeSignal) -> bool:
        """Check if signal is recent enough"""
        signal_age = datetime.now() - signal.timestamp
        return signal_age <= self.max_signal_age

    def calculate_tp_sl(self, symbol: str, direction: str, entry_price: float,
                       sweep_info: Dict, displacement_info: Dict) -> Tuple[List[float], float]:
        """Fast TP/SL calculation"""
        if direction == "BULLISH":
            sl = sweep_info['sweep_level'] * 0.998
            risk = abs(entry_price - sl)
            tp1 = entry_price + (risk * 1.5)
            tp2 = entry_price + (risk * 2.5)
            tp3 = displacement_info['impulse_candle']['high']
        else:  # BEARISH
            sl = sweep_info['sweep_level'] * 1.002
            risk = abs(sl - entry_price)
            tp1 = entry_price - (risk * 1.5)
            tp2 = entry_price - (risk * 2.5)
            tp3 = displacement_info['impulse_candle']['low']
        
        return [tp1, tp2, tp3], sl

    # ==================== OPTIMIZED TRACKING & ALERTS ====================

    async def track_active_signals(self):
        """Efficiently track all active signals"""
        while True:
            try:
                current_time = datetime.now()
                signals_to_remove = []
                
                for signal_id, signal in list(self.active_signals.items()):
                    # Simulate price check - replace with actual price feed
                    current_price = await self.get_current_price(signal.symbol)
                    
                    # Check TP levels
                    for i, tp_level in enumerate(signal.tp_levels):
                        if not signal.tp_hit[i]:
                            if (signal.direction == "BULLISH" and current_price >= tp_level) or \
                               (signal.direction == "BEARISH" and current_price <= tp_level):
                                signal.tp_hit[i] = True
                                await self.send_tp_alert(signal, i+1, tp_level, current_price)
                    
                    # Check SL
                    if (signal.direction == "BULLISH" and current_price <= signal.sl_level) or \
                       (signal.direction == "BEARISH" and current_price >= signal.sl_level):
                        signal.status = "SL_HIT"
                        await self.send_sl_alert(signal, current_price)
                        signals_to_remove.append(signal_id)
                    
                    # Remove old signals
                    signal_age = current_time - signal.timestamp
                    if signal_age > timedelta(hours=4):
                        signals_to_remove.append(signal_id)
                
                # Clean up
                for signal_id in signals_to_remove:
                    self.active_signals.pop(signal_id, None)
                
                await asyncio.sleep(2)  # Check every 2 seconds for efficiency
                
            except Exception as e:
                logging.error(f"Signal tracking error: {e}")
                await asyncio.sleep(5)

    async def get_current_price(self, symbol: str) -> float:
        """Get current price - optimize with bulk price fetching"""
        # Placeholder - implement with actual price feed
        # For now, return a simulated price
        return 100.0  # Replace with actual price fetch

    async def send_signal_alert(self, signal: TradeSignal):
        """Send optimized signal alert"""
        message = f"""
🎯 <b>ROMEOPT TOP 40 SIGNAL</b>

<b>Symbol:</b> {signal.symbol}
<b>Direction:</b> {signal.direction}
<b>Entry:</b> {signal.entry_price:.4f}

<b>TP1:</b> {signal.tp_levels[0]:.4f}
<b>TP2:</b> {signal.tp_levels[1]:.4f}
<b>TP3:</b> {signal.tp_levels[2]:.4f}
<b>SL:</b> {signal.sl_level:.4f}

<b>Time:</b> {signal.timestamp.strftime('%H:%M:%S')}
"""
        await self.send_telegram_alert(message)

    async def send_tp_alert(self, signal: TradeSignal, tp_level: int, target_price: float, current_price: float):
        """Send TP alert"""
        message = f"""
✅ <b>TP{tp_level} HIT - {signal.symbol}</b>

<b>Direction:</b> {signal.direction}
<b>Target:</b> {target_price:.4f}
<b>Current:</b> {current_price:.4f}
"""
        await self.send_telegram_alert(message)

    async def send_sl_alert(self, signal: TradeSignal, current_price: float):
        """Send SL alert"""
        message = f"""
❌ <b>SL HIT - {signal.symbol}</b>

<b>Direction:</b> {signal.direction}
<b>Entry:</b> {signal.entry_price:.4f}
<b>SL:</b> {signal.sl_level:.4f}
<b>Current:</b> {current_price:.4f}
"""
        await self.send_telegram_alert(message)

    async def send_telegram_alert(self, message: str):
        """Send Telegram alert with error handling"""
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            'chat_id': self.telegram_chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.post(url, json=payload) as response:
                    if response.status != 200:
                        logging.error(f"Telegram alert failed: {response.status}")
        except Exception as e:
            logging.error(f"Telegram alert error: {e}")

    # ==================== MAIN SCANNER ====================

    async def start_scanner(self):
        """Start the optimized Top 40 scanner"""
        logging.info("🚀 Starting RomeOPT Top 40 Scanner...")
        
        # Start WebSocket feeds
        asyncio.create_task(self.start_websocket_feeds())
        
        # Start signal tracking
        asyncio.create_task(self.track_active_signals())
        
        # Monitor performance
        asyncio.create_task(self.monitor_performance())
        
        # Keep alive
        while True:
            await asyncio.sleep(10)

    async def monitor_performance(self):
        """Monitor scanner performance"""
        while True:
            try:
                active_connections = len([task for task in asyncio.all_tasks() 
                                        if 'websocket' in task.get_name().lower()])
                active_signals = len(self.active_signals)
                coins_with_data = len(self.price_data)
                
                logging.info(f"📊 PERFORMANCE: {active_connections} websockets, "
                           f"{coins_with_data}/40 coins with data, "
                           f"{active_signals} active signals")
                
                await asyncio.sleep(60)  # Log every minute
                
            except Exception as e:
                logging.error(f"Performance monitoring error: {e}")
                await asyncio.sleep(60)

# ==================== EXECUTION ====================

async def main():
    """Main execution function"""
    try:
        scanner = RomeOPTTop40Scanner()
        await scanner.start_scanner()
    except Exception as e:
        logging.error(f"Scanner failed: {e}")

if __name__ == "__main__":
    # Set event loop policy for better performance on Windows
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    asyncio.run(main())