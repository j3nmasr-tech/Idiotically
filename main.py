import os
import asyncio
import aiohttp
import logging
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json

# Configure honest logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bingx_romeopt_scanner.log'),
        logging.StreamHandler()
    ]
)

class BingXRomeOPTScanner:
    def __init__(self):
        logging.info("🚀 INITIALIZING BINGX ROMEOPT SCANNER")
        
        # Load ALL your environment variables
        self.api_key = os.getenv('BINGX_API_KEY')
        self.api_secret = os.getenv('BINGX_API_SECRET')
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Validate all credentials
        missing_vars = []
        if not self.api_key: missing_vars.append('BINGX_API_KEY')
        if not self.api_secret: missing_vars.append('BINGX_API_SECRET')
        if not self.telegram_token: missing_vars.append('TELEGRAM_BOT_TOKEN')
        if not self.telegram_chat_id: missing_vars.append('TELEGRAM_CHAT_ID')
        
        if missing_vars:
            error_msg = f"❌ MISSING ENVIRONMENT VARIABLES: {', '.join(missing_vars)}"
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        logging.info("✅ ALL CREDENTIALS LOADED SUCCESSFULLY")

        # REALISTIC CONFIGURATION
        self.coins = ['BTC-USDT', 'ETH-USDT', 'BNB-USDT', 'SOL-USDT']  # 4 COINS MAX
        self.timeframe = '5m'  # ONE TIMEFRAME ONLY
        self.analysis_interval = 45  # SECONDS BETWEEN CYCLES
        
        # BingX API endpoints
        self.base_url = "https://open-api.bingx.com"
        
        # STATE
        self.price_data: Dict[str, List] = {}
        self.active_signals: Dict[str, Dict] = {}
        
        # PERFORMANCE TRACKING
        self.real_signals_generated = 0
        self.analysis_cycles = 0
        self.start_time = datetime.now()
        
        logging.info(f"✅ BINGX SCANNER READY: {len(self.coins)} coins, {self.timeframe} timeframe")

    def generate_signature(self, params: str) -> str:
        """Generate BingX API signature"""
        return hmac.new(
            self.api_secret.encode('utf-8'),
            params.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def bingx_api_request(self, endpoint: str, params: Dict = None, signed: bool = False) -> Optional[Dict]:
        """Make authenticated request to BingX API"""
        try:
            url = f"{self.base_url}{endpoint}"
            
            if params is None:
                params = {}
            
            if signed:
                params['timestamp'] = int(datetime.now().timestamp() * 1000)
                query_string = urllib.parse.urlencode(params)
                signature = self.generate_signature(query_string)
                params['signature'] = signature
                headers = {'X-BX-APIKEY': self.api_key}
            else:
                headers = {}
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                if signed:
                    async with session.get(url, params=params, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            logging.error(f"BingX API error: {response.status}")
                            return None
                else:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            logging.error(f"BingX API error: {response.status}")
                            return None
                            
        except Exception as e:
            logging.error(f"BingX API request failed: {e}")
            return None

    async def get_bingx_klines(self, symbol: str, limit: int = 25) -> Optional[List[Dict]]:
        """Get REAL klines data from BingX"""
        try:
            # Convert symbol format: BTC-USDT -> BTC-USDT (BingX uses this format)
            params = {
                'symbol': symbol,
                'interval': self.timeframe,
                'limit': limit
            }
            
            data = await self.bingx_api_request('/openApi/swap/v2/quote/klines', params)
            
            if data and 'data' in data:
                candles = []
                for candle in data['data']:
                    candles.append({
                        'timestamp': datetime.fromtimestamp(candle[0] / 1000),
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5]),
                        'is_closed': True
                    })
                
                logging.debug(f"📊 {symbol}: Got {len(candles)} REAL BingX candles")
                return candles
            else:
                logging.warning(f"❌ {symbol}: No klines data from BingX")
                return None
                
        except Exception as e:
            logging.error(f"❌ {symbol}: BingX klines failed: {e}")
            return None

    async def get_bingx_ticker(self, symbol: str) -> Optional[float]:
        """Get REAL current price from BingX"""
        try:
            params = {'symbol': symbol}
            data = await self.bingx_api_request('/openApi/swap/v2/quote/ticker', params)
            
            if data and 'data' in data and len(data['data']) > 0:
                price = float(data['data'][0]['lastPrice'])
                logging.debug(f"💰 {symbol}: Current price = {price}")
                return price
            else:
                logging.warning(f"❌ {symbol}: No ticker data from BingX")
                return None
                
        except Exception as e:
            logging.error(f"❌ {symbol}: BingX ticker failed: {e}")
            return None

    async def get_account_balance(self) -> Optional[float]:
        """Get account balance (optional - for position sizing)"""
        try:
            params = {}
            data = await self.bingx_api_request('/openApi/swap/v2/user/balance', params, signed=True)
            
            if data and 'data' in data and 'balance' in data['data']:
                balance = float(data['data']['balance'])
                logging.info(f"💳 Account balance: ${balance:.2f}")
                return balance
            return None
            
        except Exception as e:
            logging.error(f"Account balance check failed: {e}")
            return None

    # ==================== 6-STEP ROMEOPT ANALYSIS ====================

    def step_1_liquidity_sweep(self, symbol: str, candles: List[Dict]) -> Tuple[bool, Optional[Dict]]:
        """REAL liquidity sweep detection"""
        if len(candles) < 10:
            return False, None
            
        current_candle = candles[-1]
        current_low = current_candle['low']
        current_high = current_candle['high']
        
        # Look at previous candles for liquidity levels
        previous_candles = candles[-9:-1]
        if not previous_candles:
            return False, None
            
        previous_lows = [c['low'] for c in previous_candles]
        previous_highs = [c['high'] for c in previous_candles]
        
        min_previous_low = min(previous_lows)
        max_previous_high = max(previous_highs)
        
        # Bullish sweep: current low breaks previous lows but closes above
        sweep_threshold = 0.001  # 0.1% tolerance
        if current_low <= min_previous_low * (1 + sweep_threshold) and current_candle['close'] > min_previous_low:
            logging.info(f"✅ {symbol}: Bullish sweep - {current_low:.2f} < {min_previous_low:.2f}")
            return True, {
                'type': 'BULLISH_SWEEP', 
                'sweep_level': min_previous_low,
                'current_low': current_low
            }
        
        # Bearish sweep: current high breaks previous highs but closes below  
        if current_high >= max_previous_high * (1 - sweep_threshold) and current_candle['close'] < max_previous_high:
            logging.info(f"✅ {symbol}: Bearish sweep - {current_high:.2f} > {max_previous_high:.2f}")
            return True, {
                'type': 'BEARISH_SWEEP',
                'sweep_level': max_previous_high, 
                'current_high': current_high
            }
            
        return False, None

    def step_2_displacement(self, symbol: str, candles: List[Dict], sweep_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """REAL displacement detection"""
        current_candle = candles[-1]
        open_price = current_candle['open']
        close_price = current_candle['close']
        high = current_candle['high']
        low = current_candle['low']
        
        candle_range = high - low
        if candle_range == 0:
            return False, None
            
        body_size = abs(close_price - open_price)
        body_ratio = body_size / candle_range
        
        # Real impulse candle: at least 60% body
        if body_ratio >= 0.6:
            direction = "BULLISH" if close_price > open_price else "BEARISH"
            
            # Validate alignment with sweep
            if (sweep_info['type'] == 'BULLISH_SWEEP' and direction == "BULLISH") or \
               (sweep_info['type'] == 'BEARISH_SWEEP' and direction == "BEARISH"):
                logging.info(f"✅ {symbol}: Displacement - {direction} candle ({body_ratio:.1%} body)")
                return True, {
                    'direction': direction,
                    'body_ratio': body_ratio,
                    'impulse_candle': current_candle
                }
                
        return False, None

    def step_3_retracement_zone(self, symbol: str, candles: List[Dict], sweep_info: Dict, displacement_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """REAL retracement zone detection"""
        current_price = candles[-1]['close']
        displacement_candle = displacement_info['impulse_candle']
        direction = displacement_info['direction']
        
        # Define realistic zone around displacement candle
        zone_extension = 0.008  # 0.8% extension
        zone_low = displacement_candle['low'] * (1 - zone_extension)
        zone_high = displacement_candle['high'] * (1 + zone_extension)
        
        if zone_low <= current_price <= zone_high:
            logging.info(f"✅ {symbol}: Retracement - {current_price:.2f} in zone [{zone_low:.2f}, {zone_high:.2f}]")
            return True, {
                'zone_low': zone_low,
                'zone_high': zone_high
            }
            
        return False, None

    def step_4_premium_discount(self, symbol: str, candles: List[Dict], direction: str) -> Tuple[bool, Optional[Dict]]:
        """REAL premium/discount check"""
        if len(candles) < 15:
            return False, None
            
        current_price = candles[-1]['close']
        
        # Calculate equilibrium from recent highs/lows
        recent_high = max(c['high'] for c in candles[-15:])
        recent_low = min(c['low'] for c in candles[-15:])
        equilibrium = (recent_high + recent_low) / 2
        
        position = "DISCOUNT" if current_price < equilibrium else "PREMIUM"
        
        if (direction == "BULLISH" and position == "DISCOUNT") or \
           (direction == "BEARISH" and position == "PREMIUM"):
            logging.info(f"✅ {symbol}: Premium/Discount - Trading at {position}")
            return True, {
                'position': position, 
                'equilibrium': equilibrium,
                'current_price': current_price
            }
            
        return False, None

    def step_5_momentum_confirmation(self, symbol: str, candles: List[Dict], direction: str) -> Tuple[bool, Optional[Dict]]:
        """REAL momentum confirmation"""
        if len(candles) < 6:
            return False, None
            
        # Simple momentum: price above/below 5-candle MA
        recent_closes = [c['close'] for c in candles[-5:]]
        ma_5 = sum(recent_closes) / len(recent_closes)
        current_price = candles[-1]['close']
        
        momentum = "BULLISH" if current_price > ma_5 else "BEARISH"
        
        if momentum == direction:
            logging.info(f"✅ {symbol}: Momentum - {direction} confirmed")
            return True, {
                'momentum': momentum, 
                'ma_5': ma_5,
                'current_price': current_price
            }
            
        return False, None

    def step_6_volume_confirmation(self, symbol: str, candles: List[Dict]) -> Tuple[bool, Optional[Dict]]:
        """REAL volume confirmation"""
        if len(candles) < 10:
            return False, None
            
        current_volume = candles[-1]['volume']
        avg_volume = sum(c['volume'] for c in candles[-10:]) / 10
        
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0
        
        if volume_ratio >= 0.7:  # At least 70% of average volume
            logging.info(f"✅ {symbol}: Volume - Adequate ({volume_ratio:.1%} of average)")
            return True, {
                'volume_status': 'ADEQUATE',
                'current_volume': current_volume,
                'avg_volume': avg_volume,
                'volume_ratio': volume_ratio
            }
            
        logging.debug(f"❌ {symbol}: Volume too low ({volume_ratio:.1%} of average)")
        return False, None

    async def generate_romeopt_signal(self, symbol: str) -> Optional[Dict]:
        """REAL 6-step RomeOPT signal generation"""
        logging.info(f"🔍 {symbol}: Starting 6-step RomeOPT analysis...")
        
        # Get REAL BingX data
        candles = await self.get_bingx_klines(symbol, limit=20)
        if not candles or len(candles) < 15:
            logging.warning(f"❌ {symbol}: Insufficient BingX data")
            return None
        
        current_price = await self.get_bingx_ticker(symbol)
        if not current_price:
            logging.warning(f"❌ {symbol}: Could not get current price from BingX")
            return None
            
        # Update latest candle with current price
        candles[-1]['close'] = current_price
        if current_price > candles[-1]['high']:
            candles[-1]['high'] = current_price
        if current_price < candles[-1]['low']:
            candles[-1]['low'] = current_price

        # 6-STEP ROMEOPT VALIDATION
        steps_passed = []
        
        # Step 1: Liquidity Sweep
        step1_ok, step1_info = self.step_1_liquidity_sweep(symbol, candles)
        if not step1_ok:
            logging.debug(f"❌ {symbol}: Failed Step 1 - No liquidity sweep")
            return None
        steps_passed.append("Liquidity Sweep")

        # Step 2: Displacement
        step2_ok, step2_info = self.step_2_displacement(symbol, candles, step1_info)
        if not step2_ok:
            logging.debug(f"❌ {symbol}: Failed Step 2 - No displacement")
            return None
        steps_passed.append("Displacement")
        direction = step2_info['direction']

        # Step 3: Retracement Zone
        step3_ok, step3_info = self.step_3_retracement_zone(symbol, candles, step1_info, step2_info)
        if not step3_ok:
            logging.debug(f"❌ {symbol}: Failed Step 3 - No retracement")
            return None
        steps_passed.append("Zone Retracement")

        # Step 4: Premium/Discount
        step4_ok, step4_info = self.step_4_premium_discount(symbol, candles, direction)
        if not step4_ok:
            logging.debug(f"❌ {symbol}: Failed Step 4 - Wrong premium/discount")
            return None
        steps_passed.append("Premium/Discount")

        # Step 5: Momentum
        step5_ok, step5_info = self.step_5_momentum_confirmation(symbol, candles, direction)
        if not step5_ok:
            logging.debug(f"❌ {symbol}: Failed Step 5 - No momentum")
            return None
        steps_passed.append("Momentum")

        # Step 6: Volume
        step6_ok, step6_info = self.step_6_volume_confirmation(symbol, candles)
        if not step6_ok:
            logging.debug(f"❌ {symbol}: Failed Step 6 - Low volume")
            return None
        steps_passed.append("Volume Confirmation")

        # ALL 6 STEPS PASSED - REAL ROMEOPT SIGNAL!
        logging.info(f"🎯 {symbol}: ALL 6 STEPS PASSED - {direction} ROMEOPT SIGNAL!")
        
        # Calculate realistic TP/SL based on RomeOPT methodology
        entry_price = current_price
        if direction == "BULLISH":
            sl = step1_info['sweep_level'] * 0.995  # 0.5% below sweep low
            risk = entry_price - sl
            tp1 = entry_price + (risk * 1.2)
            tp2 = entry_price + (risk * 2.0)
            tp3 = entry_price + (risk * 3.0)
        else:  # BEARISH
            sl = step1_info['sweep_level'] * 1.005  # 0.5% above sweep high
            risk = sl - entry_price
            tp1 = entry_price - (risk * 1.2)
            tp2 = entry_price - (risk * 2.0)
            tp3 = entry_price - (risk * 3.0)
        
        risk_reward = f"1:{(tp1 - entry_price) / (entry_price - sl):.1f}" if direction == "BULLISH" else f"1:{(entry_price - tp1) / (sl - entry_price):.1f}"
        
        signal = {
            'symbol': symbol,
            'direction': direction,
            'entry_price': entry_price,
            'timestamp': datetime.now(),
            'timeframe': self.timeframe,
            'tp_levels': [tp1, tp2, tp3],
            'sl_level': sl,
            'current_price': current_price,
            'risk_reward': risk_reward,
            'steps_passed': steps_passed,
            'signal_id': f"{symbol}_{direction}_{int(datetime.now().timestamp())}"
        }
        
        self.real_signals_generated += 1
        return signal

    async def send_telegram_alert(self, message: str):
        """Send REAL signal alert to Telegram"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logging.info("✅ Telegram alert sent successfully")
                    else:
                        error_text = await response.text()
                        logging.error(f"❌ Telegram failed: {response.status} - {error_text}")
        except Exception as e:
            logging.error(f"❌ Telegram alert error: {e}")

    async def analyze_all_coins(self):
        """Realistic sequential analysis of all coins"""
        logging.info(f"🔄 Starting analysis cycle for {len(self.coins)} coins")
        
        signals_found = 0
        
        for symbol in self.coins:
            try:
                # Realistic delay between coin analyses
                await asyncio.sleep(3)
                
                signal = await self.generate_romeopt_signal(symbol)
                
                if signal:
                    signal_id = signal['signal_id']
                    
                    if signal_id not in self.active_signals:
                        self.active_signals[signal_id] = signal
                        signals_found += 1
                        
                        # Format beautiful Telegram message
                        emoji = "🟢" if signal['direction'] == "BULLISH" else "🔴"
                        
                        alert_msg = f"""
{emoji} **ROMEOPT LIVE SIGNAL - {signal['timeframe']}**

**Symbol**: {signal['symbol']}
**Direction**: {signal['direction']}
**Entry Price**: {signal['entry_price']:.4f}
**Current Price**: {signal['current_price']:.4f}

**Take Profit Levels**:
TP1: {signal['tp_levels'][0]:.4f}
TP2: {signal['tp_levels'][1]:.4f}
TP3: {signal['tp_levels'][2]:.4f}

**Stop Loss**: {signal['sl_level']:.4f}
**Risk/Reward**: {signal['risk_reward']}

**Time**: {signal['timestamp'].strftime('%H:%M:%S UTC')}

**6-Step Validation**:
✅ Liquidity Sweep ({signal['steps_passed'][0]})
✅ Displacement ({signal['direction']})
✅ Zone Retracement
✅ Premium/Discount
✅ Momentum Confirmation
✅ Volume Confirmation

*BingX API • Real-time Analysis*
"""
                        await self.send_telegram_alert(alert_msg)
                        logging.info(f"📨 Telegram alert sent for {signal['symbol']}")
                        
            except Exception as e:
                logging.error(f"❌ {symbol}: Analysis failed: {e}")
                continue
        
        self.analysis_cycles += 1
        logging.info(f"✅ Analysis cycle {self.analysis_cycles} completed - {signals_found} signals found")

    async def track_signal_performance(self):
        """Track active signals for TP/SL hits"""
        while True:
            try:
                current_time = datetime.now()
                signals_to_remove = []
                
                for signal_id, signal in list(self.active_signals.items()):
                    # Get current price
                    current_price = await self.get_bingx_ticker(signal['symbol'])
                    if not current_price:
                        continue
                    
                    # Check TP levels
                    for i, tp_level in enumerate(signal['tp_levels']):
                        tp_key = f'tp{i+1}_hit'
                        if tp_key not in signal:
                            if (signal['direction'] == "BULLISH" and current_price >= tp_level) or \
                               (signal['direction'] == "BEARISH" and current_price <= tp_level):
                                signal[tp_key] = True
                                logging.info(f"✅ {signal['symbol']} TP{i+1} HIT!")
                                
                                # Send TP alert
                                tp_msg = f"""
✅ **TP{i+1} HIT - {signal['symbol']} {signal['timeframe']}**

**Direction**: {signal['direction']}
**Target Price**: {tp_level:.4f}
**Current Price**: {current_price:.4f}
**Entry Price**: {signal['entry_price']:.4f}

**Time**: {datetime.now().strftime('%H:%M:%S UTC')}
"""
                                await self.send_telegram_alert(tp_msg)
                    
                    # Check SL
                    sl_key = 'sl_hit'
                    if sl_key not in signal:
                        if (signal['direction'] == "BULLISH" and current_price <= signal['sl_level']) or \
                           (signal['direction'] == "BEARISH" and current_price >= signal['sl_level']):
                            signal[sl_key] = True
                            logging.warning(f"❌ {signal['symbol']} SL HIT!")
                            signals_to_remove.append(signal_id)
                            
                            # Send SL alert
                            sl_msg = f"""
❌ **SL HIT - {signal['symbol']} {signal['timeframe']}**

**Direction**: {signal['direction']}
**Entry Price**: {signal['entry_price']:.4f}
**Stop Loss**: {signal['sl_level']:.4f}
**Current Price**: {current_price:.4f}

**Time**: {datetime.now().strftime('%H:%M:%S UTC')}
"""
                            await self.send_telegram_alert(sl_msg)
                    
                    # Remove old signals (4 hours max)
                    signal_age = current_time - signal['timestamp']
                    if signal_age > timedelta(hours=4):
                        signals_to_remove.append(signal_id)
                
                # Clean up completed signals
                for signal_id in signals_to_remove:
                    if signal_id in self.active_signals:
                        del self.active_signals[signal_id]
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logging.error(f"❌ Signal tracking error: {e}")
                await asyncio.sleep(30)

    async def send_status_update(self):
        """Send periodic status updates"""
        while True:
            try:
                uptime = datetime.now() - self.start_time
                hours = uptime.total_seconds() / 3600
                
                status_msg = f"""
📊 **BINGX SCANNER STATUS**

• **Uptime**: {hours:.1f} hours
• **Analysis Cycles**: {self.analysis_cycles}
• **Signals Generated**: {self.real_signals_generated}
• **Active Signals**: {len(self.active_signals)}
• **Coins**: {len(self.coins)}
• **Timeframe**: {self.timeframe}

**Status**: 🟢 OPERATIONAL
**Last Update**: {datetime.now().strftime('%H:%M UTC')}
"""
                await self.send_telegram_alert(status_msg)
                await asyncio.sleep(3600)  # Every hour
                
            except Exception as e:
                logging.error(f"Status update error: {e}")
                await asyncio.sleep(3600)

    async def start_scanner(self):
        """Main scanner loop"""
        logging.info("🚀 STARTING BINGX ROMEOPT SCANNER")
        
        # Send startup message
        startup_msg = f"""
🚀 **BINGX ROMEOPT SCANNER STARTED**

• **Coins**: {', '.join(self.coins)}
• **Timeframe**: {self.timeframe}
• **Analysis Interval**: {self.analysis_interval}s
• **Start Time**: {self.start_time.strftime('%Y-%m-%d %H:%M UTC')}

**Status**: 🟢 INITIALIZING
**Strategy**: 6-Step RomeOPT Validation
"""
        await self.send_telegram_alert(startup_msg)
        
        # Start background tasks
        asyncio.create_task(self.track_signal_performance())
        asyncio.create_task(self.send_status_update())
        
        # Main analysis loop
        while True:
            cycle_start = datetime.now()
            
            try:
                await self.analyze_all_coins()
            except Exception as e:
                logging.error(f"❌ Main analysis cycle failed: {e}")
                await asyncio.sleep(30)
                continue
            
            # Realistic timing control
            cycle_time = (datetime.now() - cycle_start).total_seconds()
            wait_time = max(5, self.analysis_interval - cycle_time)
            
            logging.info(f"⏰ Cycle took {cycle_time:.1f}s, waiting {wait_time:.1f}s")
            await asyncio.sleep(wait_time)

# 🎯 RUN THE REAL SCANNER
async def main():
    scanner = BingXRomeOPTScanner()
    await scanner.start_scanner()

if __name__ == "__main__":
    logging.info("🎯 STARTING BINGX ROMEOPT SCANNER - PRODUCTION READY")
    asyncio.run(main())