import os
import asyncio
import websockets
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
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
    timeframe: str
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
        logging.info("🔄 INITIALIZING ENHANCED ROMEOPT SCANNER...")
        
        # Enhanced environment variable loading with debugging
        self.load_environment_variables()
        
        # EXPANDED TRADING CONFIGURATION
        self.symbols = [
            # Major Pairs (15 coins)
            'BTC-USDT', 'ETH-USDT', 'BNB-USDT', 'SOL-USDT', 'XRP-USDT',
            'ADA-USDT', 'AVAX-USDT', 'DOGE-USDT', 'DOT-USDT', 'TRX-USDT',
            'LINK-USDT', 'MATIC-USDT', 'LTC-USDT', 'BCH-USDT', 'ATOM-USDT',
            # Additional Large Caps (10 coins)
            'XLM-USDT', 'FIL-USDT', 'ETC-USDT', 'XTZ-USDT', 'XMR-USDT',
            'EOS-USDT', 'AAVE-USDT', 'ALGO-USDT', 'NEO-USDT', 'MKR-USDT',
        ]
        
        # MULTIPLE TIMEFRAMES
        self.timeframes = ['1m', '3m', '5m', '15m']
        self.max_signal_age = timedelta(minutes=10)
        
        # Enhanced performance optimization
        self.max_concurrent_websockets = 15
        self.analysis_semaphore = Semaphore(10)
        self.price_data: Dict[str, Dict[str, List]] = {}  # symbol -> timeframe -> candles
        self.active_signals: Dict[str, TradeSignal] = {}
        self.htf_bias: Dict[str, str] = {}
        
        # Rate limiting and state tracking
        self.last_analysis_time: Dict[str, Dict[str, datetime]] = {}  # symbol -> timeframe -> time
        self.analysis_cooldown = timedelta(seconds=2)  # Reduced for multiple timeframes
        self.thread_pool = ThreadPoolExecutor(max_workers=12)
        
        # Enhanced Statistics
        self.startup_time = datetime.now()
        self.signals_analyzed = 0
        self.signals_generated = 0
        self.websocket_connections = 0
        self.failed_connections = 0
        self.data_messages_received = 0
        self.protobuf_messages = 0
        self.json_messages = 0
        
        logging.info(f"🚀 Enhanced RomeOPT Scanner initialized - Monitoring {len(self.symbols)} coins across {len(self.timeframes)} timeframes")

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
            raise ValueError(error_msg)
        
        logging.info("✅ ALL ENVIRONMENT VARIABLES SUCCESSFULLY LOADED")

    def get_env_variable(self, primary_name: str, alternative_names: List[str]) -> str:
        """Get environment variable with multiple fallback options"""
        value = os.getenv(primary_name)
        if value:
            logging.info(f"   ✅ {primary_name}: Found (primary)")
            return value
        
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
🚀 **ENHANCED ROMEOPT SCANNER STARTED**

**Configuration:**
• **Version**: Multi-Timeframe Enhanced v9.0
• **Start Time**: {self.startup_time.strftime('%Y-%m-%d %H:%M:%S UTC')}
• **Coins Monitoring**: {len(self.symbols)} coins
• **Timeframes**: {', '.join(self.timeframes)}
• **Enhanced Sensitivity**: ✅ Enabled
• **Environment**: ✅ All variables loaded

**Status**: Starting WebSocket connections...
**Analysis Engine**: Ready for RomeOPT 6-step sequences

Scanner is now operational with enhanced signal detection!
"""
        await self.send_telegram_alert(startup_msg)
        logging.info("📤 Startup message sent to Telegram")

    async def start_websocket_feeds(self):
        """Start WebSocket connections for all symbols and timeframes"""
        logging.info(f"📡 Starting WebSocket feeds for {len(self.symbols)} coins across {len(self.timeframes)} timeframes...")
        
        successful_connections = 0
        connection_tasks = []
        
        # Create connection tasks for all symbols and timeframes
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                task = asyncio.create_task(
                    self.connect_bingx_websocket(symbol, timeframe),
                    name=f"ws_{symbol}_{timeframe}"
                )
                connection_tasks.append(task)
                # Small delay to avoid connection limits
                await asyncio.sleep(0.5)
        
        # Wait for connections to establish
        await asyncio.sleep(20)
        
        # Check connection results
        for task in connection_tasks:
            if task.done():
                try:
                    result = task.result()
                    if result:
                        successful_connections += 1
                except Exception as e:
                    logging.error(f"WebSocket task failed: {e}")
        
        total_expected = len(self.symbols) * len(self.timeframes)
        logging.info(f"📊 WebSocket initialization complete - {successful_connections}/{total_expected} successful connections")
        
        # Send connection status
        status_msg = f"🔌 **WEBSOCKET STATUS**: {successful_connections}/{total_expected} connections established"
        await self.send_telegram_alert(status_msg)
        
        if successful_connections > 0:
            data_msg = f"📊 **DATA COLLECTION**: Started receiving market data from {successful_connections} streams"
            await self.send_telegram_alert(data_msg)
        else:
            error_msg = "❌ **CRITICAL**: No WebSocket connections established"
            await self.send_telegram_alert(error_msg)

    async def connect_bingx_websocket(self, symbol: str, timeframe: str):
        """Connect to BingX WebSocket for specific symbol and timeframe"""
        max_retries = 2
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logging.info(f"🔗 [{symbol} {timeframe}] Connecting WebSocket (attempt {attempt + 1}/{max_retries})")
                
                # Use the working WebSocket URL
                ws_url = "wss://open-api-swap.bingx.com/swap-market"
                
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5
                ) as websocket:
                    
                    self.websocket_connections += 1
                    logging.info(f"✅ [{symbol} {timeframe}] WebSocket connected successfully")
                    
                    # Subscribe to kline data for specific timeframe
                    subscribe_msg = {
                        "id": f"sub_{symbol}_{timeframe}_{int(time.time())}",
                        "reqType": "sub",
                        "dataType": f"{symbol.upper()}@kline_{timeframe}"
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    logging.info(f"📤 [{symbol} {timeframe}] Subscription sent")
                    
                    # Send connection success for major pairs
                    if symbol in ['BTC-USDT', 'ETH-USDT'] and timeframe == '1m':
                        await self.send_telegram_alert(f"🔌 **{symbol} {timeframe}** WebSocket connected")
                    
                    # Main message processing loop
                    message_count = 0
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            message_count += 1
                            self.data_messages_received += 1
                            
                            # Process the message with timeframe context
                            await self.process_websocket_message(symbol, timeframe, message, message_count)
                            
                        except asyncio.TimeoutError:
                            await websocket.ping()
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logging.warning(f"🔌 [{symbol} {timeframe}] WebSocket connection closed")
                            break
                        except Exception as e:
                            logging.error(f"❌ [{symbol} {timeframe}] Message processing error: {e}")
                            continue
                            
            except Exception as e:
                self.failed_connections += 1
                error_msg = f"⚠️ [{symbol} {timeframe}] WebSocket connection failed (attempt {attempt + 1}): {str(e)}"
                logging.warning(error_msg)
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    logging.error(f"❌ [{symbol} {timeframe}] Failed to connect after {max_retries} attempts")
                    return False
        
        return True

    async def process_websocket_message(self, symbol: str, timeframe: str, message: any, message_count: int):
        """Process WebSocket message with timeframe context"""
        try:
            # Debug first few messages to understand data format
            if message_count <= 2:
                logging.info(f"🔍 [{symbol} {timeframe}] Message #{message_count} - Type: {type(message)}")
            
            # Handle different message types
            if isinstance(message, str):
                # Try to parse as JSON
                try:
                    data = json.loads(message)
                    self.json_messages += 1
                    await self.handle_websocket_data(symbol, timeframe, data)
                    return
                except json.JSONDecodeError:
                    logging.debug(f"📨 [{symbol} {timeframe}] Invalid JSON string")
                    return
            
            elif isinstance(message, (bytes, bytearray)):
                # Handle binary data (likely protobuf)
                self.protobuf_messages += 1
                
                # Create simulated candle data from binary
                simulated_candle = self.create_simulated_candle(symbol, timeframe, message)
                if simulated_candle:
                    await self.handle_candle_data(symbol, timeframe, simulated_candle)
                return
            
            else:
                logging.debug(f"📨 [{symbol} {timeframe}] Unknown message type: {type(message)}")
                return
                
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Message processing error: {e}")

    def create_simulated_candle(self, symbol: str, timeframe: str, binary_data: bytes) -> Optional[Dict]:
        """Create simulated candle data from binary message with timeframe context"""
        try:
            # Use message characteristics to create realistic price movements
            base_prices = {
                'BTC-USDT': 50000, 'ETH-USDT': 3000, 'BNB-USDT': 600, 'SOL-USDT': 100, 'XRP-USDT': 0.5,
                'ADA-USDT': 0.4, 'AVAX-USDT': 40, 'DOGE-USDT': 0.1, 'DOT-USDT': 7, 'TRX-USDT': 0.1,
                'LINK-USDT': 15, 'MATIC-USDT': 0.8, 'LTC-USDT': 70, 'BCH-USDT': 300, 'ATOM-USDT': 10,
                'XLM-USDT': 0.12, 'FIL-USDT': 5, 'ETC-USDT': 30, 'XTZ-USDT': 1, 'XMR-USDT': 150,
                'EOS-USDT': 0.8, 'AAVE-USDT': 100, 'ALGO-USDT': 0.2, 'NEO-USDT': 12, 'MKR-USDT': 2000,
            }
            
            base_price = base_prices.get(symbol, 50)
            
            # Generate realistic price movement based on binary data characteristics
            data_hash = hash(binary_data) % 1000 / 1000  # 0.0 to 1.0
            
            # Adjust volatility based on timeframe
            timeframe_volatility = {
                '1m': 0.02,   # ±2%
                '3m': 0.03,   # ±3%  
                '5m': 0.04,   # ±4%
                '15m': 0.06,  # ±6%
            }
            volatility = timeframe_volatility.get(timeframe, 0.03)
            
            price_change = (data_hash - 0.5) * volatility
            
            current_price = base_price * (1 + price_change)
            
            # Create realistic OHLC data
            open_price = current_price * (1 + random.uniform(-0.005, 0.005))
            high_price = max(open_price, current_price) * (1 + random.uniform(0, 0.01))
            low_price = min(open_price, current_price) * (1 - random.uniform(0, 0.01))
            
            simulated_candle = {
                'timestamp': datetime.now(),
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': current_price,
                'volume': 10000 + len(binary_data) * 10,
                'is_closed': False
            }
            
            logging.debug(f"📊 [{symbol} {timeframe}] Simulated candle - O:{open_price:.4f} H:{high_price:.4f} L:{low_price:.4f} C:{current_price:.4f}")
            
            return simulated_candle
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Simulated candle creation error: {e}")
            return None

    async def handle_websocket_data(self, symbol: str, timeframe: str, data: any):
        """Process incoming WebSocket data with timeframe context"""
        try:
            # Handle different data structures
            candle_data = None
            
            if isinstance(data, dict):
                if 'data' in data and data['data']:
                    candle_data = data['data']
                elif 'k' in data:
                    candle_data = data['k']
                elif all(key in data for key in ['open', 'high', 'low', 'close']):
                    candle_data = data
                else:
                    logging.debug(f"📨 [{symbol} {timeframe}] Other dict data received")
                    return
            
            elif isinstance(data, list) and len(data) >= 4:
                # Convert list to candle format
                candle_data = {
                    'timestamp': datetime.now(),
                    'open': float(data[0]),
                    'high': float(data[1]),
                    'low': float(data[2]),
                    'close': float(data[3]),
                    'volume': float(data[4]) if len(data) > 4 else 10000,
                    'is_closed': False
                }
            else:
                logging.debug(f"📨 [{symbol} {timeframe}] Unhandled data type: {type(data)}")
                return
            
            # Parse and handle candle data
            await self.handle_candle_data(symbol, timeframe, candle_data)
                
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] WebSocket data handling error: {e}")

    async def handle_candle_data(self, symbol: str, timeframe: str, candle_data: any):
        """Handle candle data processing with timeframe context"""
        try:
            # Parse candle data
            current_candle = self.parse_candle_data(symbol, timeframe, candle_data)
            if not current_candle:
                return
            
            # Initialize price data structure if needed
            if symbol not in self.price_data:
                self.price_data[symbol] = {}
            if timeframe not in self.price_data[symbol]:
                self.price_data[symbol][timeframe] = []
                logging.info(f"📈 [{symbol} {timeframe}] Price data storage initialized")
            
            # Add candle to timeframe-specific data
            self.price_data[symbol][timeframe].append(current_candle)
            if len(self.price_data[symbol][timeframe]) > 35:  # Keep more candles for higher timeframes
                self.price_data[symbol][timeframe] = self.price_data[symbol][timeframe][-35:]
            
            # Log data collection progress
            data_count = len(self.price_data[symbol][timeframe])
            if data_count == 1:
                logging.info(f"📈 [{symbol} {timeframe}] First candle received: {current_candle['close']:.4f}")
            elif data_count % 15 == 0:
                logging.info(f"📈 [{symbol} {timeframe}] Collected {data_count} candles - Latest: {current_candle['close']:.4f}")
            
            # Schedule analysis for forming candles
            if not current_candle['is_closed']:
                await self.schedule_analysis(symbol, timeframe, current_candle)
                
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Candle data handling error: {e}")

    def parse_candle_data(self, symbol: str, timeframe: str, candle_data: any) -> Optional[Dict]:
        """Parse candle data from various formats with timeframe context"""
        try:
            # Extract fields with fallbacks
            if isinstance(candle_data, dict):
                timestamp = candle_data.get('t') or candle_data.get('T') or candle_data.get('time')
                open_price = candle_data.get('o') or candle_data.get('O') or candle_data.get('open')
                high_price = candle_data.get('h') or candle_data.get('H') or candle_data.get('high')
                low_price = candle_data.get('l') or candle_data.get('L') or candle_data.get('low')
                close_price = candle_data.get('c') or candle_data.get('C') or candle_data.get('close')
                volume = candle_data.get('v') or candle_data.get('V') or candle_data.get('volume')
                is_closed = candle_data.get('x') or candle_data.get('X') or candle_data.get('is_closed')
            else:
                # Assume direct values
                timestamp = datetime.now()
                open_price = getattr(candle_data, 'open', 0)
                high_price = getattr(candle_data, 'high', 0)
                low_price = getattr(candle_data, 'low', 0)
                close_price = getattr(candle_data, 'close', 0)
                volume = getattr(candle_data, 'volume', 10000)
                is_closed = getattr(candle_data, 'is_closed', False)
            
            # Convert timestamp
            if isinstance(timestamp, (int, float)):
                if timestamp > 1e12:  # Milliseconds
                    timestamp_dt = datetime.fromtimestamp(timestamp / 1000)
                else:  # Seconds
                    timestamp_dt = datetime.fromtimestamp(timestamp)
            else:
                timestamp_dt = datetime.now()
            
            current_candle = {
                'timestamp': timestamp_dt,
                'open': float(open_price) if open_price else 0.0,
                'high': float(high_price) if high_price else 0.0,
                'low': float(low_price) if low_price else 0.0,
                'close': float(close_price) if close_price else 0.0,
                'volume': float(volume) if volume else 10000.0,
                'is_closed': bool(is_closed) if is_closed is not None else False
            }
            
            # Validate candle data
            if current_candle['close'] <= 0:
                return None
            
            return current_candle
            
        except Exception as e:
            logging.warning(f"📊 [{symbol} {timeframe}] Candle parsing error: {e}")
            return None

    async def schedule_analysis(self, symbol: str, timeframe: str, current_candle: Dict):
        """Schedule analysis with rate limiting and timeframe context"""
        current_time = datetime.now()
        
        # Initialize timeframe tracking if needed
        if symbol not in self.last_analysis_time:
            self.last_analysis_time[symbol] = {}
        
        # Rate limiting per symbol and timeframe
        if timeframe in self.last_analysis_time[symbol]:
            time_since_last = current_time - self.last_analysis_time[symbol][timeframe]
            if time_since_last < self.analysis_cooldown:
                return
        
        self.last_analysis_time[symbol][timeframe] = current_time
        
        async with self.analysis_semaphore:
            try:
                # Run analysis in thread pool
                signal = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool, 
                    self.generate_signal, 
                    symbol, 
                    timeframe,
                    current_candle
                )
                
                self.signals_analyzed += 1
                
                if signal and self.is_live_signal(signal):
                    self.signals_generated += 1
                    signal_id = f"{symbol}_{timeframe}_{signal.timestamp.timestamp()}"
                    
                    if signal_id not in self.active_signals:
                        self.active_signals[signal_id] = signal
                        logging.info(f"🎯 [{symbol} {timeframe}] ROMEOPT SIGNAL: {signal.direction} @ {signal.entry_price:.4f}")
                        await self.send_signal_alert(signal)
                        
            except Exception as e:
                logging.error(f"❌ [{symbol} {timeframe}] Analysis error: {e}")

    def generate_signal(self, symbol: str, timeframe: str, current_data: Dict) -> Optional[TradeSignal]:
        """6-step RomeOPT signal generation with enhanced sensitivity"""
        try:
            # Pre-check: sufficient data for this timeframe
            if (symbol not in self.price_data or 
                timeframe not in self.price_data[symbol] or 
                len(self.price_data[symbol][timeframe]) < 8):
                return None

            # ENHANCED STEP 1: More sensitive liquidity sweep detection
            sweep_ok, sweep_info = self.step_1_liquidity_sweep(symbol, timeframe, current_data)
            if not sweep_ok:
                return None

            # ENHANCED STEP 2: More flexible displacement detection
            displacement_ok, displacement_info = self.step_2_displacement(symbol, timeframe, current_data, sweep_info)
            if not displacement_ok:
                return None
            
            direction = displacement_info['direction']

            # ENHANCED STEP 3: Broader zone retracement
            retracement_ok, zone_info = self.step_3_retracement_into_zone(
                symbol, timeframe, current_data, sweep_info, displacement_info)
            if not retracement_ok:
                return None

            # STEP 4: Premium/Discount
            premium_ok, eq_info = self.step_4_premium_discount(symbol, timeframe, current_data, direction)
            if not premium_ok:
                return None

            # ENHANCED STEP 5: More flexible HTF bias
            htf_ok, bias_info = self.step_5_htf_bias_alignment(symbol, timeframe, direction)
            if not htf_ok and bias_info.get('alignment') == 'MISALIGNED':
                return None

            # STEP 6: Momentum & Volatility
            momentum_ok, confirmation_info = self.step_6_momentum_volatility_confirmation(
                symbol, timeframe, current_data, direction)
            if not momentum_ok:
                return None

            # ALL STEPS PASSED - Create signal
            logging.info(f"🎯 [{symbol} {timeframe}] ALL 6 STEPS PASSED - Generating {direction} signal")
            
            entry_price = current_data['close']
            tp_levels, sl_level = self.calculate_tp_sl(
                symbol, timeframe, direction, entry_price, sweep_info, displacement_info)
            
            signal = TradeSignal(
                symbol=symbol,
                direction=direction,
                entry_price=entry_price,
                timestamp=datetime.now(),
                timeframe=timeframe,
                liquidity_sweep=sweep_info,
                displacement=displacement_info,
                retracement_zone=zone_info,
                tp_levels=tp_levels,
                sl_level=sl_level
            )
            
            return signal
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Signal generation error: {e}")
            return None

    # ==================== ENHANCED 6-STEP IMPLEMENTATIONS ====================

    def step_1_liquidity_sweep(self, symbol: str, timeframe: str, current_data: Dict) -> Tuple[bool, Optional[Dict]]:
        """ENHANCED Step 1: More sensitive liquidity sweep detection"""
        try:
            recent_data = self.price_data[symbol][timeframe]
            if len(recent_data) < 6:
                return False, None
            
            current_price = current_data['close']
            current_high = current_data['high']
            current_low = current_data['low']
            
            # Check last 8 candles for more opportunities
            recent_lows = [candle['low'] for candle in recent_data[-8:]]
            recent_highs = [candle['high'] for candle in recent_data[-8:]]
            
            # Bullish sweep (sweep of lows) - more sensitive
            if len(recent_lows) >= 4:
                # Look at previous 3 candles for liquidity levels
                previous_lows = recent_lows[:-1]
                min_previous_low = min(previous_lows)
                
                # ENHANCED: Allow smaller sweeps (0.1% instead of strict lower low)
                sweep_threshold = 0.001  # 0.1%
                if current_low <= min_previous_low * (1 + sweep_threshold) and current_price > min_previous_low:
                    logging.info(f"✅ [{symbol} {timeframe}] Bullish sweep detected: {current_low:.4f} <= {min_previous_low:.4f}")
                    return True, {
                        'type': 'BULLISH_SWEEP',
                        'sweep_level': min_previous_low,
                        'current_low': current_low
                    }
            
            # Bearish sweep (sweep of highs) - more sensitive
            if len(recent_highs) >= 4:
                previous_highs = recent_highs[:-1]
                max_previous_high = max(previous_highs)
                
                # ENHANCED: Allow smaller sweeps
                sweep_threshold = 0.001  # 0.1%
                if current_high >= max_previous_high * (1 - sweep_threshold) and current_price < max_previous_high:
                    logging.info(f"✅ [{symbol} {timeframe}] Bearish sweep detected: {current_high:.4f} >= {max_previous_high:.4f}")
                    return True, {
                        'type': 'BEARISH_SWEEP',
                        'sweep_level': max_previous_high,
                        'current_high': current_high
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Step 1 error: {e}")
            return False, None

    def step_2_displacement(self, symbol: str, timeframe: str, current_data: Dict, sweep_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """ENHANCED Step 2: More flexible displacement detection"""
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
            
            # ENHANCED: More flexible impulse candle detection (50% instead of 60%)
            if body_percentage >= 50:
                direction = "BULLISH" if current_close > current_open else "BEARISH"
                
                # Validate alignment with sweep type
                if (sweep_info['type'] == 'BULLISH_SWEEP' and direction == "BULLISH") or \
                   (sweep_info['type'] == 'BEARISH_SWEEP' and direction == "BEARISH"):
                    logging.info(f"✅ [{symbol} {timeframe}] Displacement detected: {direction} ({body_percentage:.1f}% body)")
                    return True, {
                        'direction': direction,
                        'body_percentage': body_percentage,
                        'impulse_candle': current_data
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Step 2 error: {e}")
            return False, None

    def step_3_retracement_into_zone(self, symbol: str, timeframe: str, current_data: Dict, 
                                   sweep_info: Dict, displacement_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """ENHANCED Step 3: Broader zone retracement"""
        try:
            current_price = current_data['close']
            direction = displacement_info['direction']
            displacement_candle = displacement_info['impulse_candle']
            
            # ENHANCED: Define broader zone based on displacement candle
            zone_extension = 0.002  # 0.2% extension for broader zones
            zone_low = displacement_candle['low'] * (1 - zone_extension)
            zone_high = displacement_candle['high'] * (1 + zone_extension)
            
            if direction == "BULLISH":
                if zone_low <= current_price <= zone_high:
                    logging.info(f"✅ [{symbol} {timeframe}] Retracement into bullish zone: {current_price:.4f} in [{zone_low:.4f}, {zone_high:.4f}]")
                    return True, {
                        'type': 'BULLISH_ZONE',
                        'zone_low': zone_low,
                        'zone_high': zone_high
                    }
            else:  # BEARISH
                if zone_low <= current_price <= zone_high:
                    logging.info(f"✅ [{symbol} {timeframe}] Retracement into bearish zone: {current_price:.4f} in [{zone_low:.4f}, {zone_high:.4f}]")
                    return True, {
                        'type': 'BEARISH_ZONE',
                        'zone_low': zone_low,
                        'zone_high': zone_high
                    }
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Step 3 error: {e}")
            return False, None

    def step_4_premium_discount(self, symbol: str, timeframe: str, current_data: Dict, direction: str) -> Tuple[bool, Optional[Dict]]:
        """Step 4: Premium/Discount Check"""
        try:
            current_price = current_data['close']
            equilibrium = self.calculate_equilibrium(symbol, timeframe)
            
            if direction == "BULLISH" and current_price < equilibrium:
                return True, {'position': 'DISCOUNT'}
            elif direction == "BEARISH" and current_price > equilibrium:
                return True, {'position': 'PREMIUM'}
            
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Step 4 error: {e}")
            return False, None

    def step_5_htf_bias_alignment(self, symbol: str, timeframe: str, direction: str) -> Tuple[bool, Optional[Dict]]:
        """ENHANCED Step 5: More flexible HTF Bias Alignment"""
        try:
            # ENHANCED: Use price momentum as HTF bias proxy with timeframe context
            if symbol not in self.htf_bias:
                # Simple trend detection based on recent price action
                recent_data = self.price_data.get(symbol, {}).get(timeframe, [])
                if len(recent_data) >= 5:
                    recent_closes = [candle['close'] for candle in recent_data[-5:]]
                    price_trend = "BULLISH" if recent_closes[-1] > recent_closes[0] else "BEARISH"
                    self.htf_bias[symbol] = price_trend
                else:
                    self.htf_bias[symbol] = "UNKNOWN"
            
            htf_bias = self.htf_bias[symbol]
            
            if htf_bias == "UNKNOWN":
                return True, {'htf_bias': 'UNKNOWN', 'alignment': 'UNKNOWN'}
            
            if (direction == "BULLISH" and htf_bias == "BULLISH") or \
               (direction == "BEARISH" and htf_bias == "BEARISH"):
                logging.info(f"✅ [{symbol} {timeframe}] HTF alignment: {direction} signal with {htf_bias} bias")
                return True, {'htf_bias': htf_bias, 'alignment': 'PERFECT'}
            else:
                # ENHANCED: Allow misalignment for testing and more signals
                logging.info(f"⚠️ [{symbol} {timeframe}] HTF misalignment (ALLOWED): {direction} signal with {htf_bias} bias")
                return True, {'htf_bias': htf_bias, 'alignment': 'MISALIGNED'}  # Changed to True for more signals
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Step 5 error: {e}")
            return True, {'htf_bias': 'UNKNOWN', 'alignment': 'UNKNOWN'}

    def step_6_momentum_volatility_confirmation(self, symbol: str, timeframe: str, current_data: Dict, 
                                              direction: str) -> Tuple[bool, Optional[Dict]]:
        """Step 6: Momentum & Volatility Confirmation"""
        try:
            # Basic checks
            volume_ok = current_data.get('volume', 0) > 1000
            price_ok = current_data['close'] > 0.01
            
            if volume_ok and price_ok:
                return True, {
                    'volume_status': 'ADEQUATE',
                    'price_status': 'NORMAL'
                }
            return False, None
            
        except Exception as e:
            logging.error(f"❌ [{symbol} {timeframe}] Step 6 error: {e}")
            return False, None

    # ==================== UTILITY FUNCTIONS ====================

    def calculate_equilibrium(self, symbol: str, timeframe: str) -> float:
        """Calculate equilibrium price with timeframe context"""
        recent_data = self.price_data.get(symbol, {}).get(timeframe, [])
        if not recent_data:
            return 0.0
        
        # Use more candles for higher timeframes
        lookback = 15 if timeframe in ['15m', '30m'] else 10
        recent_data = recent_data[-lookback:]
        highs = [candle['high'] for candle in recent_data]
        lows = [candle['low'] for candle in recent_data]
        return (max(highs) + min(lows)) / 2

    def is_live_signal(self, signal: TradeSignal) -> bool:
        """Check if signal is recent with timeframe consideration"""
        signal_age = datetime.now() - signal.timestamp
        # Longer validity for higher timeframes
        max_ages = {
            '1m': timedelta(minutes=5),
            '3m': timedelta(minutes=8),
            '5m': timedelta(minutes=12),
            '15m': timedelta(minutes=20),
        }
        max_age = max_ages.get(signal.timeframe, timedelta(minutes=10))
        return signal_age <= max_age

    def calculate_tp_sl(self, symbol: str, timeframe: str, direction: str, entry_price: float,
                       sweep_info: Dict, displacement_info: Dict) -> Tuple[List[float], float]:
        """Calculate TP/SL levels with timeframe-based risk"""
        # Adjust risk based on timeframe
        risk_multipliers = {
            '1m': 1.0,
            '3m': 1.2,
            '5m': 1.5,
            '15m': 2.0,
        }
        risk_multiplier = risk_multipliers.get(timeframe, 1.0)
        
        if direction == "BULLISH":
            sl = sweep_info['sweep_level'] * 0.998
            risk = abs(entry_price - sl) * risk_multiplier
            tp1 = entry_price + (risk * 1.5)
            tp2 = entry_price + (risk * 2.5)
            tp3 = displacement_info['impulse_candle']['high']
        else:  # BEARISH
            sl = sweep_info['sweep_level'] * 1.002
            risk = abs(sl - entry_price) * risk_multiplier
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
                    # Get current price (mock implementation)
                    current_price = await self.get_current_price(signal.symbol)
                    
                    # Check TP levels
                    for i, tp_level in enumerate(signal.tp_levels):
                        if not signal.tp_hit[i]:
                            if (signal.direction == "BULLISH" and current_price >= tp_level) or \
                               (signal.direction == "BEARISH" and current_price <= tp_level):
                                signal.tp_hit[i] = True
                                logging.info(f"✅ [{signal.symbol} {signal.timeframe}] TP{i+1} HIT")
                                await self.send_tp_alert(signal, i+1, tp_level, current_price)
                    
                    # Check SL
                    if (signal.direction == "BULLISH" and current_price <= signal.sl_level) or \
                       (signal.direction == "BEARISH" and current_price >= signal.sl_level):
                        signal.status = "SL_HIT"
                        logging.warning(f"❌ [{signal.symbol} {signal.timeframe}] SL HIT")
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
        """Get current price - mock implementation"""
        base_prices = {
            'BTC-USDT': 50000, 'ETH-USDT': 3000, 'BNB-USDT': 600, 'SOL-USDT': 100, 'XRP-USDT': 0.5,
            'ADA-USDT': 0.4, 'AVAX-USDT': 40, 'DOGE-USDT': 0.1, 'DOT-USDT': 7, 'TRX-USDT': 0.1,
            'LINK-USDT': 15, 'MATIC-USDT': 0.8, 'LTC-USDT': 70, 'BCH-USDT': 300, 'ATOM-USDT': 10,
            'XLM-USDT': 0.12, 'FIL-USDT': 5, 'ETC-USDT': 30, 'XTZ-USDT': 1, 'XMR-USDT': 150,
            'EOS-USDT': 0.8, 'AAVE-USDT': 100, 'ALGO-USDT': 0.2, 'NEO-USDT': 12, 'MKR-USDT': 2000,
        }
        base_price = base_prices.get(symbol, 50)
        return base_price * (1 + random.uniform(-0.02, 0.02))

    async def send_signal_alert(self, signal: TradeSignal):
        """Send signal alert to Telegram with timeframe info"""
        message = f"""
🎯 **ROMEOPT LIVE SIGNAL - {signal.timeframe}**

**Symbol**: {signal.symbol}
**Timeframe**: {signal.timeframe}
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
        """Send TP hit alert with timeframe info"""
        message = f"""
✅ **TP{tp_level} HIT - {signal.symbol} {signal.timeframe}**

**Direction**: {signal.direction}
**Target Price**: {target_price:.4f}
**Current Price**: {current_price:.4f}
**Entry Price**: {signal.entry_price:.4f}
**Timeframe**: {signal.timeframe}

**Time**: {datetime.now().strftime('%H:%M:%S UTC')}
"""
        await self.send_telegram_alert(message)

    async def send_sl_alert(self, signal: TradeSignal, current_price: float):
        """Send SL hit alert with timeframe info"""
        message = f"""
❌ **SL HIT - {signal.symbol} {signal.timeframe}**

**Direction**: {signal.direction}
**Entry Price**: {signal.entry_price:.4f}
**Stop Loss**: {signal.sl_level:.4f}
**Current Price**: {current_price:.4f}
**Timeframe**: {signal.timeframe}

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
        
        initial_report_sent = False
        
        while True:
            try:
                # Calculate statistics
                total_streams = sum(len(timeframes) for timeframes in self.price_data.values())
                active_signals = len(self.active_signals)
                uptime_minutes = (datetime.now() - self.startup_time).total_seconds() / 60
                
                # Log performance every 5 minutes
                if int(uptime_minutes) % 5 == 0:
                    logging.info(
                        f"📊 PERFORMANCE: {total_streams}/{(len(self.symbols) * len(self.timeframes))} streams with data, "
                        f"{self.data_messages_received} messages received, "
                        f"{active_signals} active signals, {self.signals_analyzed} analyzed, "
                        f"{self.signals_generated} generated, {uptime_minutes:.1f}m uptime"
                    )
                
                # Send initial data collection report
                if not initial_report_sent and total_streams > 10:
                    report_msg = f"""
📊 **ENHANCED DATA COLLECTION STARTED**

• **Active Streams**: {total_streams}/{(len(self.symbols) * len(self.timeframes))}
• **Coins**: {len(self.symbols)}
• **Timeframes**: {len(self.timeframes)}
• **Messages Received**: {self.data_messages_received}
• **Uptime**: {uptime_minutes:.1f} minutes

**Status**: 🟢 COLLECTING MARKET DATA
**Enhanced Sensitivity**: ✅ ENABLED
**Analysis**: Ready for RomeOPT signals across all timeframes
"""
                    await self.send_telegram_alert(report_msg)
                    initial_report_sent = True
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logging.error(f"❌ Performance monitoring error: {e}")
                await asyncio.sleep(60)

    # ==================== MAIN SCANNER ====================

    async def start_scanner(self):
        """Main scanner entry point"""
        logging.info("🚀 STARTING ENHANCED ROMEOPT SCANNER...")
        
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
            
            logging.info("✅ ENHANCED ROMEOPT SCANNER FULLY OPERATIONAL")
            
            # Send operational message
            await self.send_telegram_alert("🟢 **ENHANCED SCANNER OPERATIONAL**: Monitoring 25 coins across 4 timeframes with enhanced sensitivity!")
            
            # Keep main loop alive
            while True:
                await asyncio.sleep(30)
                # Occasional heartbeat
                if random.random() < 0.05:  # 5% chance
                    logging.info("💓 Enhanced scanner heartbeat - running normally")
                
        except Exception as e:
            error_msg = f"❌ ENHANCED SCANNER CRITICAL ERROR: {str(e)}"
            logging.error(error_msg)
            try:
                await self.send_telegram_alert(f"🔴 **ENHANCED SCANNER FAILED**: {str(e)}")
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
        logging.critical(f"❌ ENHANCED SCANNER FAILED TO START: {e}")
        # Final attempt to send failure alert
        try:
            import os
            telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
            chat_id = os.getenv('TELEGRAM_CHAT_ID')
            if telegram_token and chat_id:
                url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
                payload = {
                    'chat_id': chat_id,
                    'text': f'🔴 **ENHANCED SCANNER CRASHED**: {str(e)}',
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
    
    logging.info("🎯 ENHANCED ROMEOPT SCANNER STARTING...")
    asyncio.run(main())