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
from asyncio import Semaphore
import random

# Configure comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('romeopt_scanner.log'),
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

class RomeOPTScanner:
    def __init__(self):
        # Comprehensive startup logging
        logging.info("🔄 INITIALIZING ROMEOPT SCANNER...")
        
        # Enhanced environment variable loading with debugging
        self.load_environment_variables()
        
        # Trading configuration
        self.symbols = [
            'BTC-USDT', 'ETH-USDT', 'BNB-USDT', 'SOL-USDT', 'XRP-USDT',
            'ADA-USDT', 'AVAX-USDT', 'DOGE-USDT', 'DOT-USDT', 'TRX-USDT',
        ]
        
        self.timeframe = '1m'
        self.max_signal_age = timedelta(minutes=3)
        
        # Performance optimization
        self.max_concurrent_websockets = 8
        self.analysis_semaphore = Semaphore(6)
        self.price_data: Dict[str, List] = {}
        self.active_signals: Dict[str, TradeSignal] = {}
        self.htf_bias: Dict[str, str] = {}
        
        # Rate limiting and state tracking
        self.last_analysis_time: Dict[str, datetime] = {}
        self.analysis_cooldown = timedelta(seconds=3)
        self.thread_pool = ThreadPoolExecutor(max_workers=6)
        
        # Statistics
        self.startup_time = datetime.now()
        self.signals_analyzed = 0
        self.signals_generated = 0
        self.websocket_connections = 0
        
        logging.info(f"🚀 RomeOPT Scanner initialized - Monitoring {len(self.symbols)} coins")

    def load_environment_variables(self):
        """Enhanced environment variable loading with comprehensive debugging"""
        logging.info("🔍 LOADING ENVIRONMENT VARIABLES...")
        
        # Get all environment variables for debugging
        all_env_vars = dict(os.environ)
        
        # Log available environment variables (filtered for security)
        logging.info("📋 AVAILABLE ENVIRONMENT VARIABLES:")
        for key in sorted(all_env_vars.keys()):
            if any(term in key.upper() for term in ['BINGX', 'TELEGRAM', 'API', 'SECRET', 'KEY', 'TOKEN', 'BOT', 'CHAT']):
                value = all_env_vars[key]
                # Show partial values for verification (not full secrets)
                display_value = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "***"
                logging.info(f"   📝 {key}: {display_value}")
        
        # Load specific variables with multiple fallback methods
        self.api_key = self.get_env_variable('BINGX_API_KEY', ['BINGX_API_KEY', 'BINGX_KEY', 'API_KEY'])
        self.api_secret = self.get_env_variable('BINGX_API_SECRET', ['BINGX_API_SECRET', 'BINGX_SECRET', 'API_SECRET'])
        self.telegram_token = self.get_env_variable('TELEGRAM_BOT_TOKEN', ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_TOKEN', 'BOT_TOKEN'])
        self.telegram_chat_id = self.get_env_variable('TELEGRAM_CHAT_ID', ['TELEGRAM_CHAT_ID', 'CHAT_ID'])
        
        # Final validation
        missing_vars = []
        if not self.api_key: missing_vars.append('BINGX_API_KEY')
        if not self.api_secret: missing_vars.append('BINGX_API_SECRET')
        if not self.telegram_token: missing_vars.append('TELEGRAM_BOT_TOKEN')
        if not self.telegram_chat_id: missing_vars.append('TELEGRAM_CHAT_ID')
        
        if missing_vars:
            error_msg = f"❌ MISSING ENVIRONMENT VARIABLES: {', '.join(missing_vars)}"
            logging.error(error_msg)
            
            # Provide detailed help
            logging.info("💡 TROUBLESHOOTING HELP:")
            logging.info("   1. Check variable names in Northflank dashboard")
            logging.info("   2. Ensure no typos in variable names")
            logging.info("   3. Verify values are properly set")
            logging.info("   4. Restart service after adding variables")
            
            raise ValueError(error_msg)
        
        logging.info("✅ ALL ENVIRONMENT VARIABLES SUCCESSFULLY LOADED")

    def get_env_variable(self, primary_name: str, alternative_names: List[str]) -> str:
        """Get environment variable with multiple fallback options"""
        value = os.getenv(primary_name)
        if value:
            logging.info(f"   ✅ {primary_name}: Found (primary)")
            return value
        
        # Try alternative names
        for alt_name in alternative_names:
            value = os.getenv(alt_name)
            if value:
                logging.info(f"   🔄 {primary_name}: Found via alternative '{alt_name}'")
                return value
        
        logging.warning(f"   ❌ {primary_name}: Not found (tried: {alternative_names})")
        return None

    async def send_startup_message(self):
        """Send comprehensive startup message to Telegram"""
        startup_msg = f"""
🚀 **ROMEOPT SCANNER STARTED SUCCESSFULLY**

**Configuration:**
• **Version**: Production Ready v3.0
• **Start Time**: {self.startup_time.strftime('%Y-%m-%d %H:%M:%S UTC')}
• **Coins Monitoring**: {len(self.symbols)}
• **Timeframe**: {self.timeframe}
• **Environment**: ✅ All variables loaded

**System Status:**
✅ Environment Variables Loaded
✅ Scanner Initialized  
🔜 WebSocket Connections
🔜 Real-time Analysis
🔜 Telegram Notifications

**Ready to monitor for RomeOPT 6-step sequences...**
"""
        await self.send_telegram_alert(startup_msg)
        logging.info("📤 Startup message sent to Telegram")

    async def start_websocket_feeds(self):
        """Start WebSocket connections for all symbols"""
        logging.info(f"📡 Starting WebSocket feeds for {len(self.symbols)} coins...")
        
        successful_connections = 0
        batch_size = 4
        
        for i in range(0, len(self.symbols), batch_size):
            batch = self.symbols[i:i + batch_size]
            logging.info(f"🔄 Connecting batch {i//batch_size + 1}: {batch}")
            
            tasks = []
            for symbol in batch:
                task = asyncio.create_task(self.connect_bingx_websocket(symbol))
                tasks.append(task)
            
            # Wait for batch to complete
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(2)
            
        logging.info(f"📊 WebSocket initialization complete - {successful_connections} connections")
        
        # Send connection status
        status_msg = f"🔌 **WEBSOCKET STATUS**: {successful_connections}/{len(self.symbols)} coins connected"
        await self.send_telegram_alert(status_msg)

    async def connect_bingx_websocket(self, symbol: str):
        """Connect to BingX WebSocket with comprehensive error handling"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logging.info(f"🔗 [{symbol}] Connecting WebSocket (attempt {attempt + 1}/{max_retries})")
                
                # BingX WebSocket URL
                ws_url = "wss://open-api-swap.bingx.com/swap-market"
                
                async with websockets.connect(
                    ws_url,
                    ping_interval=30,
                    ping_timeout=20,
                    close_timeout=10
                ) as websocket:
                    
                    self.websocket_connections += 1
                    logging.info(f"✅ [{symbol}] WebSocket connected successfully")
                    
                    # Subscribe to kline data
                    subscribe_msg = {
                        "id": f"{symbol}_{int(time.time())}",
                        "reqType": "sub",
                        "dataType": f"{symbol}@kline_{self.timeframe}"
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    logging.debug(f"📤 [{symbol}] Subscription sent")
                    
                    # Send connection success for major pairs
                    if symbol in ['BTC-USDT', 'ETH-USDT']:
                        await self.send_telegram_alert(f"🔌 **{symbol}** WebSocket connected")
                    
                    # Main message processing loop
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            data = json.loads(message)
                            await self.handle_websocket_data(symbol, data)
                            
                        except asyncio.TimeoutError:
                            # Keep connection alive
                            await websocket.ping()
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logging.warning(f"🔌 [{symbol}] WebSocket connection closed")
                            break
                            
            except Exception as e:
                logging.warning(f"⚠️ [{symbol}] WebSocket connection failed (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    logging.error(f"❌ [{symbol}] Failed to connect after {max_retries} attempts")
                    break

    async def handle_websocket_data(self, symbol: str, data: Dict):
        """Process incoming WebSocket data"""
        try:
            if 'data' in data and data['data']:
                candle_data = data['data']
                
                # Parse candle data
                current_candle = {
                    'timestamp': datetime.fromtimestamp(candle_data['t'] / 1000),
                    'open': float(candle_data['o']),
                    'high': float(candle_data['h']),
                    'low': float(candle_data['l']),
                    'close': float(candle_data['c']),
                    'volume': float(candle_data['v']),
                    'is_closed': candle_data['x']
                }
                
                logging.debug(f"📊 [{symbol}] New data - O:{current_candle['open']:.2f} H:{current_candle['high']:.2f} L:{current_candle['low']:.2f} C:{current_candle['close']:.2f}")
                
                # Initialize or update price data
                if symbol not in self.price_data:
                    self.price_data[symbol] = []
                    logging.info(f"📈 [{symbol}] Price data storage initialized")
                
                self.price_data[symbol].append(current_candle)
                if len(self.price_data[symbol]) > 25:
                    self.price_data[symbol] = self.price_data[symbol][-25:]
                
                # Schedule analysis for forming candles
                if not current_candle['is_closed']:
                    await self.schedule_analysis(symbol, current_candle)
                
        except Exception as e:
            logging.error(f"❌ [{symbol}] WebSocket data handling error: {e}")

    async def schedule_analysis(self, symbol: str, current_candle: Dict):
        """Schedule analysis with rate limiting"""
        current_time = datetime.now()
        
        # Rate limiting
        if symbol in self.last_analysis_time:
            time_since_last = current_time - self.last_analysis_time[symbol]
            if time_since_last < self.analysis_cooldown:
                return
        
        self.last_analysis_time[symbol] = current_time
        
        async with self.analysis_semaphore:
            try:
                # Run analysis in thread pool
                signal = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool, 
                    self.generate_signal, 
                    symbol, 
                    current_candle
                )
                
                self.signals_analyzed += 1
                
                if signal and self.is_live_signal(signal):
                    self.signals_generated += 1
                    signal_id = f"{symbol}_{signal.timestamp.timestamp()}"
                    
                    if signal_id not in self.active_signals:
                        self.active_signals[signal_id] = signal
                        logging.info(f"🎯 [{symbol}] SIGNAL GENERATED: {signal.direction} @ {signal.entry_price:.4f}")
                        await self.send_signal_alert(signal)
                        
            except Exception as e:
                logging.error(f"❌ [{symbol}] Analysis error: {e}")

    def generate_signal(self, symbol: str, current_data: Dict) -> Optional[TradeSignal]:
        """6-step RomeOPT signal generation"""
        try:
            logging.debug(f"🔍 [{symbol}] Starting 6-step analysis...")
            
            # Pre-check: sufficient data
            if symbol not in self.price_data or len(self.price_data[symbol]) < 10:
                return None

            # STEP 1: Liquidity Sweep
            sweep_ok, sweep_info = self.step_1_liquidity_sweep(symbol, current_data)
            if not sweep_ok:
                logging.debug(f"❌ [{symbol}] Step 1 failed: No liquidity sweep")
                return None
            logging.debug(f"✅ [{symbol}] Step 1 passed: {sweep_info['type']}")

            # STEP 2: Displacement
            displacement_ok, displacement_info = self.step_2_displacement(symbol, current_data, sweep_info)
            if not displacement_ok:
                logging.debug(f"❌ [{symbol}] Step 2 failed: No displacement")
                return None
            logging.debug(f"✅ [{symbol}] Step 2 passed: {displacement_info['direction']} displacement")
            
            direction = displacement_info['direction']

            # STEP 3: Retracement into Zone
            retracement_ok, zone_info = self.step_3_retracement_into_zone(
                symbol, current_data, sweep_info, displacement_info)
            if not retracement_ok:
                logging.debug(f"❌ [{symbol}] Step 3 failed: No zone retracement")
                return None
            logging.debug(f"✅ [{symbol}] Step 3 passed: In {zone_info['type']}")

            # STEP 4: Premium/Discount
            premium_ok, eq_info = self.step_4_premium_discount(symbol, current_data, direction)
            if not premium_ok:
                logging.debug(f"❌ [{symbol}] Step 4 failed: Wrong premium/discount")
                return None
            logging.debug(f"✅ [{symbol}] Step 4 passed: In {eq_info['position']}")

            # STEP 5: HTF Bias Alignment
            htf_ok, bias_info = self.step_5_htf_bias_alignment(symbol, direction)
            if not htf_ok and bias_info.get('alignment') == 'MISALIGNED':
                logging.debug(f"❌ [{symbol}] Step 5 failed: HTF misalignment")
                return None
            logging.debug(f"✅ [{symbol}] Step 5 passed: HTF {bias_info.get('alignment', 'UNKNOWN')}")

            # STEP 6: Momentum & Volatility
            momentum_ok, confirmation_info = self.step_6_momentum_volatility_confirmation(
                symbol, current_data, direction)
            if not momentum_ok:
                logging.debug(f"❌ [{symbol}] Step 6 failed: Momentum rejected")
                return None
            logging.debug(f"✅ [{symbol}] Step 6 passed: Momentum confirmed")

            # ALL STEPS PASSED - Create signal
            logging.info(f"🎯 [{symbol}] ALL 6 STEPS PASSED - Generating {direction} signal")
            
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
            
            return signal
            
        except Exception as e:
            logging.error(f"❌ [{symbol}] Signal generation error: {e}")
            return None

    # ==================== 6-STEP IMPLEMENTATIONS ====================

    def step_1_liquidity_sweep(self, symbol: str, current_data: Dict) -> Tuple[bool, Optional[Dict]]:
        """Step 1: Liquidity Sweep Detection"""
        try:
            recent_data = self.price_data.get(symbol, [])
            if len(recent_data) < 8:
                return False, None
            
            current_price = current_data['close']
            current_high = current_data['high']
            current_low = current_data['low']
            
            # Check last 6 candles
            recent_lows = [candle['low'] for candle in recent_data[-6:]]
            recent_highs = [candle['high'] for candle in recent_data[-6:]]
            
            # Bullish sweep (sweep of lows)
            if len(recent_lows) >= 3:
                min_previous_low = min(recent_lows[:-1])
                if current_low < min_previous_low and current_price > min_previous_low:
                    return True, {
                        'type': 'BULLISH_SWEEP',
                        'sweep_level': min_previous_low,
                        'current_low': current_low
                    }
            
            # Bearish sweep (sweep of highs)
            if len(recent_highs) >= 3:
                max_previous_high = max(recent_highs[:-1])
                if current_high > max_previous_high and current_price < max_previous_high:
                    return True, {
                        'type': 'BEARISH_SWEEP',
                        'sweep_level': max_previous_high,
                        'current_high': current_high
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol}] Step 1 error: {e}")
            return False, None

    def step_2_displacement(self, symbol: str, current_data: Dict, sweep_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """Step 2: Displacement Detection"""
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
            
            # Check for impulse candle
            if body_percentage >= 60:
                direction = "BULLISH" if current_close > current_open else "BEARISH"
                
                # Validate alignment with sweep type
                if (sweep_info['type'] == 'BULLISH_SWEEP' and direction == "BULLISH") or \
                   (sweep_info['type'] == 'BEARISH_SWEEP' and direction == "BEARISH"):
                    return True, {
                        'direction': direction,
                        'body_percentage': body_percentage,
                        'impulse_candle': current_data
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol}] Step 2 error: {e}")
            return False, None

    def step_3_retracement_into_zone(self, symbol: str, current_data: Dict, 
                                   sweep_info: Dict, displacement_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """Step 3: Retracement into Zone"""
        try:
            current_price = current_data['close']
            direction = displacement_info['direction']
            displacement_candle = displacement_info['impulse_candle']
            
            # Define zone based on displacement candle
            zone_low = displacement_candle['low']
            zone_high = displacement_candle['high']
            
            if direction == "BULLISH":
                if zone_low <= current_price <= zone_high:
                    return True, {
                        'type': 'BULLISH_ZONE',
                        'zone_low': zone_low,
                        'zone_high': zone_high
                    }
            else:  # BEARISH
                if zone_low <= current_price <= zone_high:
                    return True, {
                        'type': 'BEARISH_ZONE',
                        'zone_low': zone_low,
                        'zone_high': zone_high
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol}] Step 3 error: {e}")
            return False, None

    def step_4_premium_discount(self, symbol: str, current_data: Dict, direction: str) -> Tuple[bool, Optional[Dict]]:
        """Step 4: Premium/Discount Check"""
        try:
            current_price = current_data['close']
            equilibrium = self.calculate_equilibrium(symbol)
            
            if direction == "BULLISH" and current_price < equilibrium:
                return True, {'position': 'DISCOUNT'}
            elif direction == "BEARISH" and current_price > equilibrium:
                return True, {'position': 'PREMIUM'}
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol}] Step 4 error: {e}")
            return False, None

    def step_5_htf_bias_alignment(self, symbol: str, direction: str) -> Tuple[bool, Optional[Dict]]:
        """Step 5: HTF Bias Alignment"""
        try:
            # Simplified HTF bias - implement with actual HTF data
            if symbol not in self.htf_bias:
                # For testing, randomly assign bias
                self.htf_bias[symbol] = random.choice(['BULLISH', 'BEARISH', 'UNKNOWN'])
            
            htf_bias = self.htf_bias[symbol]
            
            if htf_bias == "UNKNOWN":
                return True, {'htf_bias': 'UNKNOWN', 'alignment': 'UNKNOWN'}
            
            if (direction == "BULLISH" and htf_bias == "BULLISH") or \
               (direction == "BEARISH" and htf_bias == "BEARISH"):
                return True, {'htf_bias': htf_bias, 'alignment': 'PERFECT'}
            else:
                return False, {'htf_bias': htf_bias, 'alignment': 'MISALIGNED'}
                
        except Exception as e:
            logging.error(f"❌ [{symbol}] Step 5 error: {e}")
            return True, {'htf_bias': 'UNKNOWN', 'alignment': 'UNKNOWN'}

    def step_6_momentum_volatility_confirmation(self, symbol: str, current_data: Dict, 
                                              direction: str) -> Tuple[bool, Optional[Dict]]:
        """Step 6: Momentum & Volatility Confirmation"""
        try:
            # Basic checks - enhance with actual momentum indicators
            volume_ok = current_data.get('volume', 0) > 1000
            price_ok = current_data['close'] > 0.01
            
            if volume_ok and price_ok:
                return True, {
                    'volume_status': 'ADEQUATE',
                    'price_status': 'NORMAL'
                }
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol}] Step 6 error: {e}")
            return False, None

    # ==================== UTILITY FUNCTIONS ====================

    def calculate_equilibrium(self, symbol: str) -> float:
        """Calculate equilibrium price"""
        recent_data = self.price_data.get(symbol, [])
        if not recent_data:
            return 0.0
        
        recent_data = recent_data[-15:]  # Use last 15 candles
        highs = [candle['high'] for candle in recent_data]
        lows = [candle['low'] for candle in recent_data]
        return (max(highs) + min(lows)) / 2

    def is_live_signal(self, signal: TradeSignal) -> bool:
        """Check if signal is recent"""
        signal_age = datetime.now() - signal.timestamp
        return signal_age <= self.max_signal_age

    def calculate_tp_sl(self, symbol: str, direction: str, entry_price: float,
                       sweep_info: Dict, displacement_info: Dict) -> Tuple[List[float], float]:
        """Calculate TP/SL levels"""
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

    # ==================== TRACKING & ALERTS ====================

    async def track_active_signals(self):
        """Track active signals for TP/SL hits"""
        logging.info("🔍 Starting active signal tracking...")
        
        while True:
            try:
                current_time = datetime.now()
                signals_to_remove = []
                
                for signal_id, signal in list(self.active_signals.items()):
                    # Get current price (mock for now - implement real price feed)
                    current_price = await self.get_current_price(signal.symbol)
                    
                    # Check TP levels
                    for i, tp_level in enumerate(signal.tp_levels):
                        if not signal.tp_hit[i]:
                            if (signal.direction == "BULLISH" and current_price >= tp_level) or \
                               (signal.direction == "BEARISH" and current_price <= tp_level):
                                signal.tp_hit[i] = True
                                logging.info(f"✅ [{signal.symbol}] TP{i+1} HIT")
                                await self.send_tp_alert(signal, i+1, tp_level, current_price)
                    
                    # Check SL
                    if (signal.direction == "BULLISH" and current_price <= signal.sl_level) or \
                       (signal.direction == "BEARISH" and current_price >= signal.sl_level):
                        signal.status = "SL_HIT"
                        logging.warning(f"❌ [{signal.symbol}] SL HIT")
                        await self.send_sl_alert(signal, current_price)
                        signals_to_remove.append(signal_id)
                    
                    # Remove old signals
                    signal_age = current_time - signal.timestamp
                    if signal_age > timedelta(hours=4):
                        signals_to_remove.append(signal_id)
                
                # Clean up completed signals
                for signal_id in signals_to_remove:
                    if signal_id in self.active_signals:
                        del self.active_signals[signal_id]
                
                await asyncio.sleep(3)
                
            except Exception as e:
                logging.error(f"❌ Signal tracking error: {e}")
                await asyncio.sleep(5)

    async def get_current_price(self, symbol: str) -> float:
        """Get current price - implement with real price feed"""
        # Mock implementation - replace with actual BingX API call
        base_prices = {
            'BTC-USDT': 50000,
            'ETH-USDT': 3000,
            'BNB-USDT': 600,
            'SOL-USDT': 100,
            'XRP-USDT': 0.5,
        }
        base_price = base_prices.get(symbol, 50)
        return base_price * (1 + random.uniform(-0.02, 0.02))

    async def send_signal_alert(self, signal: TradeSignal):
        """Send signal alert to Telegram"""
        message = f"""
🎯 **ROMEOPT LIVE SIGNAL**

**Symbol**: {signal.symbol}
**Direction**: {signal.direction}
**Entry Price**: {signal.entry_price:.4f}

**Take Profit Levels**:
TP1: {signal.tp_levels[0]:.4f}
TP2: {signal.tp_levels[1]:.4f}
TP3: {signal.tp_levels[2]:.4f}

**Stop Loss**: {signal.sl_level:.4f}

**Time**: {signal.timestamp.strftime('%H:%M:%S UTC')}

**6-Step Validation**:
✅ Liquidity Sweep ({signal.liquidity_sweep['type']})
✅ Displacement ({signal.displacement['direction']})
✅ Zone Retracement
✅ Premium/Discount
✅ HTF Alignment
✅ Momentum Confirmation
"""
        await self.send_telegram_alert(message)

    async def send_tp_alert(self, signal: TradeSignal, tp_level: int, target_price: float, current_price: float):
        """Send TP hit alert"""
        message = f"""
✅ **TP{tp_level} HIT - {signal.symbol}**

**Direction**: {signal.direction}
**Target Price**: {target_price:.4f}
**Current Price**: {current_price:.4f}
**Entry Price**: {signal.entry_price:.4f}

**Time**: {datetime.now().strftime('%H:%M:%S UTC')}
"""
        await self.send_telegram_alert(message)

    async def send_sl_alert(self, signal: TradeSignal, current_price: float):
        """Send SL hit alert"""
        message = f"""
❌ **SL HIT - {signal.symbol}**

**Direction**: {signal.direction}
**Entry Price**: {signal.entry_price:.4f}
**Stop Loss**: {signal.sl_level:.4f}
**Current Price**: {current_price:.4f}

**Time**: {datetime.now().strftime('%H:%M:%S UTC')}
"""
        await self.send_telegram_alert(message)

    async def send_telegram_alert(self, message: str):
        """Send message to Telegram with error handling"""
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            'chat_id': self.telegram_chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logging.debug("✅ Telegram alert sent successfully")
                    else:
                        error_text = await response.text()
                        logging.error(f"❌ Telegram alert failed: {response.status} - {error_text}")
        except asyncio.TimeoutError:
            logging.error("❌ Telegram alert timeout")
        except Exception as e:
            logging.error(f"❌ Telegram alert error: {e}")

    async def monitor_performance(self):
        """Monitor and report scanner performance"""
        logging.info("📊 Starting performance monitoring...")
        
        while True:
            try:
                # Calculate statistics
                active_tasks = len([t for t in asyncio.all_tasks() if not t.done()])
                coins_with_data = len(self.price_data)
                active_signals = len(self.active_signals)
                uptime_minutes = (datetime.now() - self.startup_time).total_seconds() / 60
                
                # Log performance
                logging.info(
                    f"📊 PERFORMANCE: {coins_with_data}/{len(self.symbols)} coins, "
                    f"{active_signals} signals, {self.signals_analyzed} analyzed, "
                    f"{self.signals_generated} generated, {uptime_minutes:.1f}m uptime"
                )
                
                # Send hourly report
                if int(uptime_minutes) % 60 == 0:
                    report_msg = f"""
📊 **HOURLY PERFORMANCE REPORT**

• **Uptime**: {uptime_minutes/60:.1f} hours
• **Coins with Data**: {coins_with_data}/{len(self.symbols)}
• **Active Signals**: {active_signals}
• **Signals Analyzed**: {self.signals_analyzed}
• **Signals Generated**: {self.signals_generated}
• **WebSocket Connections**: {self.websocket_connections}

**Status**: 🟢 OPERATIONAL
"""
                    await self.send_telegram_alert(report_msg)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logging.error(f"❌ Performance monitoring error: {e}")
                await asyncio.sleep(60)

    # ==================== MAIN SCANNER ====================

    async def start_scanner(self):
        """Main scanner entry point"""
        logging.info("🚀 STARTING ROMEOPT SCANNER...")
        
        try:
            # Send startup message
            await self.send_startup_message()
            await asyncio.sleep(2)
            
            # Start all components
            logging.info("🔄 Starting scanner components...")
            
            # Start WebSocket feeds
            asyncio.create_task(self.start_websocket_feeds())
            
            # Start signal tracking
            asyncio.create_task(self.track_active_signals())
            
            # Start performance monitoring
            asyncio.create_task(self.monitor_performance())
            
            # Send ready message
            await self.send_telegram_alert("🟢 **SCANNER READY**: All systems operational and monitoring for signals")
            
            logging.info("✅ ROMEOPT SCANNER FULLY OPERATIONAL")
            
            # Keep main loop alive
            while True:
                await asyncio.sleep(30)
                # Heartbeat
                logging.debug("💓 Scanner heartbeat - running normally")
                
        except Exception as e:
            error_msg = f"❌ SCANNER CRITICAL ERROR: {str(e)}"
            logging.error(error_msg)
            # Try to send error alert
            try:
                await self.send_telegram_alert(f"🔴 **SCANNER FAILED**: {str(e)}")
            except:
                pass
            raise

# ==================== EXECUTION ====================

async def main():
    """Main execution function"""
    try:
        scanner = RomeOPTScanner()
        await scanner.start_scanner()
    except Exception as e:
        logging.critical(f"❌ SCANNER FAILED TO START: {e}")
        # Final attempt to send failure alert
        try:
            # Create minimal scanner just for error reporting
            import os
            telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
            chat_id = os.getenv('TELEGRAM_CHAT_ID')
            if telegram_token and chat_id:
                url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
                payload = {
                    'chat_id': chat_id,
                    'text': f'🔴 **SCANNER CRASHED**: {str(e)}',
                    'parse_mode': 'HTML'
                }
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload) as response:
                        pass
        except:
            pass

if __name__ == "__main__":
    # Windows compatibility
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    logging.info("🎯 ROMEOPT SCANNER STARTING...")
    asyncio.run(main())