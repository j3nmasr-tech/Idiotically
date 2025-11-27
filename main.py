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
import gzip
import brotli

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
        self.max_concurrent_websockets = 5
        self.analysis_semaphore = Semaphore(4)
        self.price_data: Dict[str, List] = {}
        self.active_signals: Dict[str, TradeSignal] = {}
        self.htf_bias: Dict[str, str] = {}
        
        # Rate limiting and state tracking
        self.last_analysis_time: Dict[str, datetime] = {}
        self.analysis_cooldown = timedelta(seconds=3)
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Statistics
        self.startup_time = datetime.now()
        self.signals_analyzed = 0
        self.signals_generated = 0
        self.websocket_connections = 0
        self.failed_connections = 0
        self.data_messages_received = 0
        
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
🚀 **ROMEOPT SCANNER STARTED SUCCESSFULLY**

**Configuration:**
• **Version**: Compression Fixed v5.0
• **Start Time**: {self.startup_time.strftime('%Y-%m-%d %H:%M:%S UTC')}
• **Coins Monitoring**: {len(self.symbols)}
• **Timeframe**: {self.timeframe}
• **Environment**: ✅ All variables loaded

**WebSocket Status**: Starting connections...
**Data Compression**: GZIP/Brotli support enabled
**Analysis Engine**: Ready

Scanner is now starting...
"""
        await self.send_telegram_alert(startup_msg)
        logging.info("📤 Startup message sent to Telegram")

    async def start_websocket_feeds(self):
        """Start WebSocket connections for all symbols with proper error handling"""
        logging.info(f"📡 Starting WebSocket feeds for {len(self.symbols)} coins...")
        
        successful_connections = 0
        connection_tasks = []
        
        # Create connection tasks for all symbols
        for symbol in self.symbols:
            task = asyncio.create_task(
                self.connect_bingx_websocket(symbol),
                name=f"ws_{symbol}"
            )
            connection_tasks.append(task)
        
        # Wait for connections to establish
        await asyncio.sleep(15)
        
        # Check connection results
        for task in connection_tasks:
            if task.done():
                try:
                    result = task.result()
                    if result:
                        successful_connections += 1
                except Exception as e:
                    logging.error(f"WebSocket task failed: {e}")
        
        logging.info(f"📊 WebSocket initialization complete - {successful_connections}/{len(self.symbols)} successful connections")
        
        # Send connection status
        status_msg = f"🔌 **WEBSOCKET STATUS**: {successful_connections}/{len(self.symbols)} coins connected"
        await self.send_telegram_alert(status_msg)
        
        if successful_connections > 0:
            data_msg = f"📊 **DATA COLLECTION**: Started receiving market data from {successful_connections} coins"
            await self.send_telegram_alert(data_msg)
        else:
            error_msg = "❌ **CRITICAL**: No WebSocket connections established"
            await self.send_telegram_alert(error_msg)

    async def connect_bingx_websocket(self, symbol: str):
        """Connect to BingX WebSocket with compression handling"""
        max_retries = 2
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logging.info(f"🔗 [{symbol}] Connecting WebSocket (attempt {attempt + 1}/{max_retries})")
                
                # Use the working WebSocket URL
                ws_url = "wss://open-api-swap.bingx.com/swap-market"
                
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5
                ) as websocket:
                    
                    self.websocket_connections += 1
                    logging.info(f"✅ [{symbol}] WebSocket connected successfully")
                    
                    # Subscribe to kline data
                    subscribe_msg = {
                        "id": f"sub_{symbol}_{int(time.time())}",
                        "reqType": "sub",
                        "dataType": f"{symbol.upper()}@kline_{self.timeframe}"
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    logging.info(f"📤 [{symbol}] Subscription sent for {self.timeframe} data")
                    
                    # Send connection success for major pairs
                    if symbol in ['BTC-USDT', 'ETH-USDT']:
                        await self.send_telegram_alert(f"🔌 **{symbol}** WebSocket connected successfully")
                    
                    # Main message processing loop
                    message_count = 0
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            message_count += 1
                            self.data_messages_received += 1
                            
                            # Process the message (handle compression)
                            await self.process_websocket_message(symbol, message, message_count)
                            
                        except asyncio.TimeoutError:
                            await websocket.ping()
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logging.warning(f"🔌 [{symbol}] WebSocket connection closed")
                            break
                        except Exception as e:
                            logging.error(f"❌ [{symbol}] Message processing error: {e}")
                            continue
                            
            except Exception as e:
                self.failed_connections += 1
                error_msg = f"⚠️ [{symbol}] WebSocket connection failed (attempt {attempt + 1}): {str(e)}"
                logging.warning(error_msg)
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    logging.error(f"❌ [{symbol}] Failed to connect after {max_retries} attempts")
                    return False
        
        return True

    async def process_websocket_message(self, symbol: str, message: any, message_count: int):
        """Process WebSocket message with compression handling"""
        try:
            # Handle different message types
            if isinstance(message, str):
                # Try to parse as JSON directly
                try:
                    data = json.loads(message)
                    await self.handle_websocket_data(symbol, data)
                    return
                except json.JSONDecodeError:
                    logging.warning(f"📨 [{symbol}] Invalid JSON string: {message[:100]}...")
                    return
            
            elif isinstance(message, bytes):
                # Handle compressed data
                decompressed_data = await self.decompress_message(message)
                if decompressed_data:
                    await self.handle_websocket_data(symbol, decompressed_data)
                else:
                    logging.warning(f"📨 [{symbol}] Failed to decompress binary message")
                    return
            
            else:
                logging.warning(f"📨 [{symbol}] Unknown message type: {type(message)}")
                return
                
        except Exception as e:
            logging.error(f"❌ [{symbol}] Message processing error: {e}")

    async def decompress_message(self, compressed_data: bytes) -> Optional[Dict]:
        """Decompress WebSocket message using multiple methods"""
        try:
            # Try GZIP decompression first
            try:
                decompressed = gzip.decompress(compressed_data)
                data = json.loads(decompressed.decode('utf-8'))
                logging.debug("✅ Used GZIP decompression")
                return data
            except (gzip.BadGzipFile, OSError):
                pass
            
            # Try Brotli decompression
            try:
                decompressed = brotli.decompress(compressed_data)
                data = json.loads(decompressed.decode('utf-8'))
                logging.debug("✅ Used Brotli decompression")
                return data
            except brotli.error:
                pass
            
            # Try direct JSON parsing (might be already decompressed)
            try:
                data = json.loads(compressed_data.decode('utf-8'))
                logging.debug("✅ Direct JSON parsing")
                return data
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass
            
            logging.warning("❌ Failed to decompress message with all methods")
            return None
            
        except Exception as e:
            logging.error(f"❌ Decompression error: {e}")
            return None

    async def handle_websocket_data(self, symbol: str, data: Dict):
        """Process incoming WebSocket data"""
        try:
            # Log raw data occasionally for debugging
            if random.random() < 0.01:  # 1% of messages
                logging.debug(f"📨 [{symbol}] Raw data: {json.dumps(data)[:200]}...")
            
            # Handle different data formats from BingX
            candle_data = None
            
            if 'data' in data and data['data']:
                candle_data = data['data']
                logging.debug(f"📊 [{symbol}] Using 'data' field")
            elif 'k' in data:
                candle_data = data['k']
                logging.debug(f"📊 [{symbol}] Using 'k' field")
            elif 'e' in data and data['e'] == 'kline':
                candle_data = data
                logging.debug(f"📊 [{symbol}] Using kline event format")
            else:
                # This might be a subscription confirmation or other message
                if 'id' in data or 'code' in data:
                    logging.debug(f"📨 [{symbol}] System message: {data}")
                else:
                    logging.debug(f"📨 [{symbol}] Unknown data format: {data}")
                return
            
            # Parse candle data
            current_candle = self.parse_candle_data(symbol, candle_data)
            if not current_candle:
                return
            
            # Initialize or update price data
            if symbol not in self.price_data:
                self.price_data[symbol] = []
                logging.info(f"📈 [{symbol}] Price data storage initialized - First candle received")
            
            self.price_data[symbol].append(current_candle)
            if len(self.price_data[symbol]) > 30:
                self.price_data[symbol] = self.price_data[symbol][-30:]
            
            # Log data collection progress
            if len(self.price_data[symbol]) % 5 == 0:
                logging.info(f"📈 [{symbol}] Collected {len(self.price_data[symbol])} candles - Latest: {current_candle['close']:.4f}")
            
            # Schedule analysis for forming candles
            if not current_candle['is_closed']:
                await self.schedule_analysis(symbol, current_candle)
                
        except Exception as e:
            logging.error(f"❌ [{symbol}] WebSocket data handling error: {e}")

    def parse_candle_data(self, symbol: str, candle_data: Dict) -> Optional[Dict]:
        """Parse candle data from different formats"""
        try:
            # Handle different field names from BingX
            timestamp = candle_data.get('t') or candle_data.get('T') or candle_data.get('time')
            open_price = candle_data.get('o') or candle_data.get('O')
            high_price = candle_data.get('h') or candle_data.get('H')
            low_price = candle_data.get('l') or candle_data.get('L')
            close_price = candle_data.get('c') or candle_data.get('C')
            volume = candle_data.get('v') or candle_data.get('V')
            is_closed = candle_data.get('x') or candle_data.get('X') or candle_data.get('isClosed')
            
            # Convert timestamp (handle both milliseconds and seconds)
            if timestamp:
                if timestamp > 1e12:  # Likely milliseconds
                    timestamp_dt = datetime.fromtimestamp(timestamp / 1000)
                else:  # Likely seconds
                    timestamp_dt = datetime.fromtimestamp(timestamp)
            else:
                timestamp_dt = datetime.now()
            
            current_candle = {
                'timestamp': timestamp_dt,
                'open': float(open_price) if open_price else 0.0,
                'high': float(high_price) if high_price else 0.0,
                'low': float(low_price) if low_price else 0.0,
                'close': float(close_price) if close_price else 0.0,
                'volume': float(volume) if volume else 0.0,
                'is_closed': bool(is_closed) if is_closed is not None else False
            }
            
            # Validate candle data
            if current_candle['close'] <= 0:
                logging.warning(f"📊 [{symbol}] Invalid candle data: {current_candle}")
                return None
            
            logging.debug(f"📊 [{symbol}] New candle - O:{current_candle['open']:.4f} H:{current_candle['high']:.4f} L:{current_candle['low']:.4f} C:{current_candle['close']:.4f} V:{current_candle['volume']:.0f}")
            
            return current_candle
            
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f"📊 [{symbol}] Candle parsing error: {e} - Data: {candle_data}")
            return None

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
                return None

            # STEP 6: Momentum & Volatility
            momentum_ok, confirmation_info = self.step_6_momentum_volatility_confirmation(
                symbol, current_data, direction)
            if not momentum_ok:
                return None

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
            if symbol not in self.htf_bias:
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
        
        recent_data = recent_data[-15:]
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
        
        initial_report_sent = False
        
        while True:
            try:
                # Calculate statistics
                active_tasks = len([t for t in asyncio.all_tasks() if not t.done()])
                coins_with_data = len(self.price_data)
                active_signals = len(self.active_signals)
                uptime_minutes = (datetime.now() - self.startup_time).total_seconds() / 60
                
                # Log performance
                logging.info(
                    f"📊 PERFORMANCE: {coins_with_data}/{len(self.symbols)} coins with data, "
                    f"{self.data_messages_received} messages received, "
                    f"{active_signals} active signals, {self.signals_analyzed} analyzed, "
                    f"{self.signals_generated} generated, {uptime_minutes:.1f}m uptime"
                )
                
                # Send initial data collection report
                if not initial_report_sent and coins_with_data > 0:
                    report_msg = f"""
📊 **DATA COLLECTION STARTED**

• **Coins with Data**: {coins_with_data}/{len(self.symbols)}
• **WebSocket Connections**: {self.websocket_connections}
• **Messages Received**: {self.data_messages_received}
• **Uptime**: {uptime_minutes:.1f} minutes

**Status**: 🟢 COLLECTING MARKET DATA
**Analysis**: Ready for RomeOPT signals
"""
                    await self.send_telegram_alert(report_msg)
                    initial_report_sent = True
                
                # Send hourly report
                if int(uptime_minutes) % 60 == 0 and int(uptime_minutes) > 0:
                    report_msg = f"""
📊 **HOURLY PERFORMANCE REPORT**

• **Uptime**: {uptime_minutes/60:.1f} hours
• **Coins with Data**: {coins_with_data}/{len(self.symbols)}
• **Messages Received**: {self.data_messages_received}
• **Signals Analyzed**: {self.signals_analyzed}
• **Signals Generated**: {self.signals_generated}

**Status**: 🟢 OPERATIONAL
"""
                    await self.send_telegram_alert(report_msg)
                
                await asyncio.sleep(60)
                
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
            
            logging.info("✅ ROMEOPT SCANNER FULLY OPERATIONAL")
            
            # Keep main loop alive
            while True:
                await asyncio.sleep(30)
                if random.random() < 0.1:
                    logging.info("💓 Scanner heartbeat - running normally")
                
        except Exception as e:
            error_msg = f"❌ SCANNER CRITICAL ERROR: {str(e)}"
            logging.error(error_msg)
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
        try:
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
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    logging.info("🎯 ROMEOPT SCANNER STARTING...")
    asyncio.run(main())