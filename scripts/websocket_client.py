#!/usr/bin/env python3
"""
websocket_client.py - High-Performance Low-Level Implementation
Using specialized libraries for real-time market analytics

Libraries:
- websockets: Direct WebSocket connection (not Upstox SDK)
- redis: Direct Redis operations with proper error handling
- numpy: High-performance numerical calculations
- asyncio: Asynchronous processing for better performance
- numba: JIT compilation for critical calculations
"""

import asyncio
import websockets
import json
import time
import logging
import os
import signal
import sys
from datetime import datetime
import pytz
import numpy as np
import redis
from collections import deque, defaultdict
from typing import Dict, List, Optional, NamedTuple, Tuple
from dataclasses import dataclass
import requests
import threading
from numba import jit
import warnings
warnings.filterwarnings('ignore')

# Configuration
CREDENTIALS_PATH = os.getenv('CUPCAKE_CREDENTIALS_PATH', '/home/abhishek/projects/CUPCAKE/authentication/credentials.json')
SYMBOLS_PATH = os.getenv('CUPCAKE_SYMBOLS_PATH', '/home/abhishek/projects/CUPCAKE/scripts/symbols.json')
LOG_DIR = os.getenv('CUPCAKE_LOG_DIR', '/home/abhishek/projects/CUPCAKE/logs')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '1'))

# Analytics parameters
ROLLING_WINDOW_SIZE = 600
VWAP_PERIODS = [30, 60, 300]
MA_PERIODS = [20, 50]
VOLATILITY_THRESHOLD = 0.02
LIQUIDITY_THRESHOLD = 1000

class TickData(NamedTuple):
    """Optimized tick data structure"""
    timestamp: float
    ltp: float
    bid: float
    ask: float
    bid_qty: int
    ask_qty: int
    volume: int

@dataclass
class RealTimeMetrics:
    """Real-time calculated metrics with validation"""
    timestamp: float
    instrument: str
    ltp: float
    mid_price: float
    spread: float
    spread_bps: float
    vwap_30s: float
    vwap_60s: float
    vwap_300s: float
    order_imbalance: float
    total_depth: int
    tick_return: float
    volatility_5min: float
    liquidity_flag: bool
    volatility_flag: bool
    vwap_ma_signal: int
    trend_strength: float
    
    def validate_values(self) -> bool:
        """Validate all float values for Redis storage"""
        float_fields = [
            'ltp', 'mid_price', 'spread', 'spread_bps',
            'vwap_30s', 'vwap_60s', 'vwap_300s',
            'order_imbalance', 'tick_return', 'volatility_5min',
            'trend_strength'
        ]
        
        for field in float_fields:
            value = getattr(self, field)
            if not isinstance(value, (int, float)) or np.isnan(value) or np.isinf(value):
                return False
        
        return True

@jit(nopython=True)
def calculate_vwap_numba(prices: np.ndarray, volumes: np.ndarray, timestamps: np.ndarray, 
                        current_time: float, period_seconds: int) -> float:
    """Fast VWAP calculation using Numba JIT"""
    cutoff_time = current_time - period_seconds
    total_pv = 0.0
    total_volume = 0.0
    
    for i in range(len(timestamps) - 1, -1, -1):
        if timestamps[i] < cutoff_time:
            break
        if volumes[i] > 0:
            total_pv += prices[i] * volumes[i]
            total_volume += volumes[i]
    
    return total_pv / total_volume if total_volume > 0 else 0.0

@jit(nopython=True)
def calculate_volatility_numba(returns: np.ndarray) -> float:
    """Fast volatility calculation using Numba"""
    if len(returns) < 2:
        return 0.0
    
    mean_return = np.mean(returns)
    variance = np.mean((returns - mean_return) ** 2)
    return np.sqrt(variance * len(returns)) if variance > 0 else 0.0

class HighPerformanceBuffer:
    """Ultra-fast rolling buffer using NumPy arrays"""
    
    def __init__(self, size: int = ROLLING_WINDOW_SIZE):
        self.size = size
        self.index = 0
        self.count = 0
        
        # Pre-allocated NumPy arrays for speed
        self.timestamps = np.full(size, np.nan, dtype=np.float64)
        self.ltp = np.full(size, np.nan, dtype=np.float64)
        self.bid = np.full(size, np.nan, dtype=np.float64)
        self.ask = np.full(size, np.nan, dtype=np.float64)
        self.bid_qty = np.full(size, 0, dtype=np.int32)
        self.ask_qty = np.full(size, 0, dtype=np.int32)
        self.volume = np.full(size, 0, dtype=np.int32)
        self.tick_returns = np.full(size, np.nan, dtype=np.float64)
        
        # Thread lock for safety
        self.lock = threading.Lock()
    
    def add_tick(self, tick: TickData) -> bool:
        """Add tick with ultra-fast circular buffer"""
        with self.lock:
            # Validation
            if tick.ltp <= 0 or not np.isfinite(tick.ltp):
                return False
            
            # Calculate tick return
            tick_return = 0.0
            if self.count > 0:
                prev_idx = (self.index - 1) % self.size if self.count == self.size else self.index - 1
                if prev_idx >= 0 and self.ltp[prev_idx] > 0:
                    tick_return = (tick.ltp - self.ltp[prev_idx]) / self.ltp[prev_idx]
            
            # Store in circular buffer
            self.timestamps[self.index] = tick.timestamp
            self.ltp[self.index] = tick.ltp
            self.bid[self.index] = tick.bid
            self.ask[self.index] = tick.ask
            self.bid_qty[self.index] = tick.bid_qty
            self.ask_qty[self.index] = tick.ask_qty
            self.volume[self.index] = tick.volume
            self.tick_returns[self.index] = tick_return
            
            # Update pointers
            self.index = (self.index + 1) % self.size
            self.count = min(self.count + 1, self.size)
            
            return True
    
    def get_valid_data(self) -> Tuple[np.ndarray, ...]:
        """Get valid data arrays"""
        with self.lock:
            if self.count == 0:
                return tuple(np.array([]) for _ in range(8))
            
            if self.count < self.size:
                # Buffer not full yet
                end_idx = self.count
                return (
                    self.timestamps[:end_idx].copy(),
                    self.ltp[:end_idx].copy(),
                    self.bid[:end_idx].copy(),
                    self.ask[:end_idx].copy(),
                    self.bid_qty[:end_idx].copy(),
                    self.ask_qty[:end_idx].copy(),
                    self.volume[:end_idx].copy(),
                    self.tick_returns[:end_idx].copy()
                )
            else:
                # Buffer is full, handle wraparound
                if self.index == 0:
                    # No wraparound needed
                    return (
                        self.timestamps.copy(),
                        self.ltp.copy(),
                        self.bid.copy(),
                        self.ask.copy(),
                        self.bid_qty.copy(),
                        self.ask_qty.copy(),
                        self.volume.copy(),
                        self.tick_returns.copy()
                    )
                else:
                    # Reconstruct in chronological order
                    timestamps = np.concatenate([self.timestamps[self.index:], self.timestamps[:self.index]])
                    ltp = np.concatenate([self.ltp[self.index:], self.ltp[:self.index]])
                    bid = np.concatenate([self.bid[self.index:], self.bid[:self.index]])
                    ask = np.concatenate([self.ask[self.index:], self.ask[:self.index]])
                    bid_qty = np.concatenate([self.bid_qty[self.index:], self.bid_qty[:self.index]])
                    ask_qty = np.concatenate([self.ask_qty[self.index:], self.ask_qty[:self.index]])
                    volume = np.concatenate([self.volume[self.index:], self.volume[:self.index]])
                    tick_returns = np.concatenate([self.tick_returns[self.index:], self.tick_returns[:self.index]])
                    
                    return timestamps, ltp, bid, ask, bid_qty, ask_qty, volume, tick_returns
    
    def calculate_metrics(self, instrument: str) -> Optional[RealTimeMetrics]:
        """Calculate all metrics using NumPy vectorization"""
        timestamps, ltp, bid, ask, bid_qty, ask_qty, volume, tick_returns = self.get_valid_data()
        
        if len(timestamps) == 0:
            return None
        
        try:
            current_time = timestamps[-1]
            current_ltp = ltp[-1]
            current_bid = bid[-1]
            current_ask = ask[-1]
            current_bid_qty = int(bid_qty[-1])
            current_ask_qty = int(ask_qty[-1])
            current_tick_return = tick_returns[-1] if not np.isnan(tick_returns[-1]) else 0.0
            
            # Mid-price and spread
            mid_price = (current_bid + current_ask) / 2 if current_bid > 0 and current_ask > 0 else current_ltp
            spread = current_ask - current_bid if current_bid > 0 and current_ask > 0 else 0.0
            spread_bps = (spread / mid_price * 10000) if mid_price > 0 else 0.0
            
            # VWAP calculations using Numba
            vwap_30s = calculate_vwap_numba(ltp, volume.astype(np.float64), timestamps, current_time, 30)
            vwap_60s = calculate_vwap_numba(ltp, volume.astype(np.float64), timestamps, current_time, 60)
            vwap_300s = calculate_vwap_numba(ltp, volume.astype(np.float64), timestamps, current_time, 300)
            
            # Order book imbalance
            total_qty = current_bid_qty + current_ask_qty
            order_imbalance = (current_bid_qty - current_ask_qty) / total_qty if total_qty > 0 else 0.0
            
            # Volatility using Numba
            valid_returns = tick_returns[~np.isnan(tick_returns)]
            if len(valid_returns) > 10:
                # Use last 300 returns (roughly 5 minutes)
                recent_returns = valid_returns[-min(300, len(valid_returns)):]
                volatility_5min = calculate_volatility_numba(recent_returns)
            else:
                volatility_5min = 0.0
            
            # Moving averages for signals
            ma_20 = np.mean(ltp[-20:]) if len(ltp) >= 20 else np.mean(ltp)
            ma_50 = np.mean(ltp[-50:]) if len(ltp) >= 50 else np.mean(ltp)
            
            # VWAP-MA crossover signal
            vwap_ma_signal = 0
            if vwap_60s > 0 and ma_20 > 0:
                if vwap_60s > ma_20 * 1.001:
                    vwap_ma_signal = 1
                elif vwap_60s < ma_20 * 0.999:
                    vwap_ma_signal = -1
            
            # Trend strength
            trend_strength = (ma_20 - ma_50) / ma_50 if ma_50 > 0 else 0.0
            
            # Flags
            liquidity_flag = total_qty >= LIQUIDITY_THRESHOLD
            volatility_flag = volatility_5min > VOLATILITY_THRESHOLD
            
            # Create metrics object
            metrics = RealTimeMetrics(
                timestamp=current_time,
                instrument=instrument,
                ltp=float(current_ltp),
                mid_price=float(mid_price),
                spread=float(spread),
                spread_bps=float(spread_bps),
                vwap_30s=float(vwap_30s),
                vwap_60s=float(vwap_60s),
                vwap_300s=float(vwap_300s),
                order_imbalance=float(order_imbalance),
                total_depth=int(total_qty),
                tick_return=float(current_tick_return),
                volatility_5min=float(volatility_5min),
                liquidity_flag=liquidity_flag,
                volatility_flag=volatility_flag,
                vwap_ma_signal=int(vwap_ma_signal),
                trend_strength=float(trend_strength)
            )
            
            # Validate before returning
            if metrics.validate_values():
                return metrics
            else:
                logging.warning(f"Invalid metrics calculated for {instrument}")
                return None
                
        except Exception as e:
            logging.error(f"Error calculating metrics for {instrument}: {e}")
            return None

class RobustRedisManager:
    """Robust Redis manager with proper error handling"""
    
    def __init__(self):
        self.redis_client = None
        self.redis_connected = False
        self._setup_redis()
    
    def _setup_redis(self):
        """Setup Redis with robust connection handling"""
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # Test connection
            self.redis_client.ping()
            self.redis_connected = True
            logging.info(f"✅ Redis connected: {REDIS_HOST}:{REDIS_PORT} DB:{REDIS_DB}")
            
        except Exception as e:
            logging.warning(f"⚠️ Redis connection failed: {e}")
            self.redis_client = None
            self.redis_connected = False
    
    def create_timeseries_keys(self, instruments: List[str], symbol_mapping: Dict[str, str]):
        """Create TimeSeries keys with error handling"""
        if not self.redis_connected:
            return
        
        metrics = [
            'ltp', 'mid_price', 'spread', 'spread_bps',
            'vwap_30s', 'vwap_60s', 'vwap_300s',
            'order_imbalance', 'total_depth', 'tick_return',
            'volatility_5min', 'trend_strength'
        ]
        
        try:
            for instrument_key in instruments:
                instrument_name = symbol_mapping[instrument_key].replace(' ', '_').upper()
                
                for metric in metrics:
                    key = f"ts:{metric}:{instrument_name}"
                    try:
                        self.redis_client.execute_command(
                            'TS.CREATE', key, 
                            'RETENTION', 86400000,  # 24 hours
                            'ON_DUPLICATE', 'LAST'
                        )
                    except redis.ResponseError as e:
                        if "key already exists" not in str(e).lower():
                            logging.warning(f"TimeSeries create warning for {key}: {e}")
            
            logging.info("✅ TimeSeries keys initialized")
            
        except Exception as e:
            logging.error(f"❌ TimeSeries setup failed: {e}")
    
    def store_metrics_safe(self, metrics: RealTimeMetrics, symbol_mapping: Dict[str, str]):
        """Store metrics with robust error handling and validation"""
        if not self.redis_connected:
            return
        
        try:
            # Validate metrics first
            if not metrics.validate_values():
                logging.warning(f"Skipping invalid metrics for {metrics.instrument}")
                return
            
            instrument_name = symbol_mapping[metrics.instrument].replace(' ', '_').upper()
            timestamp_ms = int(metrics.timestamp * 1000)
            
            # Prepare validated data
            metric_data = {
                'ltp': metrics.ltp,
                'mid_price': metrics.mid_price,
                'spread': metrics.spread,
                'spread_bps': metrics.spread_bps,
                'vwap_30s': metrics.vwap_30s,
                'vwap_60s': metrics.vwap_60s,
                'vwap_300s': metrics.vwap_300s,
                'order_imbalance': metrics.order_imbalance,
                'total_depth': float(metrics.total_depth),
                'tick_return': metrics.tick_return,
                'volatility_5min': metrics.volatility_5min,
                'trend_strength': metrics.trend_strength
            }
            
            # Store each metric individually to handle errors better
            for metric_name, value in metric_data.items():
                try:
                    # Final validation
                    if np.isfinite(value) and not np.isnan(value):
                        key = f"ts:{metric_name}:{instrument_name}"
                        self.redis_client.execute_command('TS.ADD', key, timestamp_ms, float(value))
                    else:
                        logging.debug(f"Skipping invalid value {metric_name}={value} for {instrument_name}")
                        
                except Exception as e:
                    logging.error(f"Error storing {metric_name} for {instrument_name}: {e}")
            
        except Exception as e:
            logging.error(f"❌ Error storing metrics: {e}")

class LowLevelWebSocketClient:
    """Low-level WebSocket client for maximum performance"""
    
    def __init__(self):
        self.access_token = None
        self.instruments = []
        self.symbol_mapping = {}
        self.buffers = {}
        self.redis_manager = RobustRedisManager()
        self.shutdown_requested = False
        
        # Performance counters
        self.tick_count = 0
        self.metrics_count = 0
        self.error_count = 0
        self.last_display_time = defaultdict(float)
        self.display_interval = 2.0
        
        # Setup
        self._setup_logging()
        self._load_configuration()
        self._setup_shutdown_handlers()
        self._initialize_buffers()
    
    def _setup_logging(self):
        """Setup high-performance logging"""
        os.makedirs(LOG_DIR, exist_ok=True)
        
        ist = pytz.timezone('Asia/Kolkata')
        timestamp = datetime.now(ist).strftime("%Y%m%d_%H%M%S")
        log_filename = f"{LOG_DIR}/lowlevel_analytics_{timestamp}.log"
        
        # Configure logging for performance
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),  # Fixed: removed buffering parameter
                logging.StreamHandler()
            ]
        )
        
        logging.info("🚀 Low-Level High-Performance Analytics Engine")
        logging.info("📡 Using direct WebSocket + NumPy + Numba + Redis")
    
    def _setup_shutdown_handlers(self):
        """Setup shutdown handlers"""
        def signal_handler(signum, frame):
            logging.info(f"📡 Shutdown signal {signum}")
            self.shutdown_requested = True
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    def _load_configuration(self):
        """Load configuration"""
        try:
            # Load credentials
            with open(CREDENTIALS_PATH, 'r') as f:
                creds = json.load(f)
            
            self.access_token = creds.get('access_token')
            if not self.access_token:
                raise ValueError("No access_token found")
            
            # Test token with direct HTTP request
            self._validate_token()
            
            # Load symbols
            with open(SYMBOLS_PATH, 'r') as f:
                symbols_data = json.load(f)
            
            self.instruments = []
            self.symbol_mapping = {}
            
            for symbol in symbols_data['symbols']:
                instrument_key = symbol['instrument_key']
                name = symbol['name']
                self.instruments.append(instrument_key)
                self.symbol_mapping[instrument_key] = name
            
            logging.info(f"✅ Loaded {len(self.instruments)} instruments")
            
        except Exception as e:
            logging.error(f"❌ Configuration failed: {e}")
            raise
    
    def _validate_token(self):
        """Validate token with direct HTTP request"""
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                'https://api.upstox.com/v2/user/profile',
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                profile = response.json()
                user_name = profile.get('data', {}).get('user_name', 'Unknown')
                logging.info(f"✅ Token valid - {user_name}")
            else:
                raise ValueError(f"Token validation failed: {response.status_code}")
                
        except Exception as e:
            logging.error(f"❌ Token validation failed: {e}")
            raise
    
    def _initialize_buffers(self):
        """Initialize high-performance buffers"""
        for instrument_key in self.instruments:
            self.buffers[instrument_key] = HighPerformanceBuffer(ROLLING_WINDOW_SIZE)
        
        # Setup Redis
        self.redis_manager.create_timeseries_keys(self.instruments, self.symbol_mapping)
        
        logging.info(f"✅ Initialized {len(self.buffers)} high-performance buffers")
    
    async def connect_and_stream(self):
        """Main async streaming function"""
        try:
            # Get WebSocket URL
            ws_url = await self._get_websocket_url()
            
            logging.info(f"🔗 Connecting to: {ws_url}")
            
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as websocket:
                
                logging.info("✅ WebSocket connected")
                
                # Subscribe to instruments
                await self._subscribe_to_instruments(websocket)
                
                # Main message loop
                async for message in websocket:
                    if self.shutdown_requested:
                        break
                    
                    try:
                        await self._process_message(message)
                    except Exception as e:
                        self.error_count += 1
                        logging.error(f"❌ Message processing error: {e}")
                        
                        if self.error_count > 100:
                            logging.error("Too many errors, shutting down")
                            break
                
        except Exception as e:
            logging.error(f"❌ WebSocket connection failed: {e}")
            raise
    
    async def _get_websocket_url(self) -> str:
        """Get WebSocket URL from Upstox API"""
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                'https://api.upstox.com/v2/feed/market-data-feed/authorize',
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                ws_url = data.get('data', {}).get('authorizedRedirectUri', '')
                if ws_url:
                    return ws_url
            
            raise ValueError(f"Failed to get WebSocket URL: {response.status_code}")
            
        except Exception as e:
            logging.error(f"❌ Error getting WebSocket URL: {e}")
            raise
    
    async def _subscribe_to_instruments(self, websocket):
        """Subscribe to instruments"""
        try:
            subscription_message = {
                "guid": "someguid",
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": self.instruments
                }
            }
            
            await websocket.send(json.dumps(subscription_message))
            logging.info(f"✅ Subscribed to {len(self.instruments)} instruments")
            
        except Exception as e:
            logging.error(f"❌ Subscription failed: {e}")
            raise
    
    async def _process_message(self, message):
        """Process incoming message with high performance"""
        try:
            self.tick_count += 1
            
            # Parse JSON
            data = json.loads(message)
            
            # Handle different message types
            if data.get("type") == "live_feed":
                feeds = data.get("feeds", {})
                await self._process_feeds(feeds)
            elif "feeds" in data:
                await self._process_feeds(data["feeds"])
            
            # Progress logging
            if self.tick_count % 1000 == 0:
                logging.info(f"📊 {self.tick_count:,} ticks, {self.metrics_count:,} metrics, {self.error_count} errors")
                
        except Exception as e:
            logging.error(f"❌ Message parsing error: {e}")
    
    async def _process_feeds(self, feeds):
        """Process feeds with high performance"""
        for instrument_key, feed_data in feeds.items():
            if instrument_key not in self.instruments:
                continue
            
            try:
                tick = self._extract_tick_data(instrument_key, feed_data)
                if tick:
                    buffer = self.buffers[instrument_key]
                    if buffer.add_tick(tick):
                        # Calculate metrics
                        metrics = buffer.calculate_metrics(instrument_key)
                        if metrics:
                            self.metrics_count += 1
                            
                            # Store to Redis (non-blocking)
                            self.redis_manager.store_metrics_safe(metrics, self.symbol_mapping)
                            
                            # Display (throttled)
                            self._display_metrics(metrics)
                            
            except Exception as e:
                logging.error(f"❌ Error processing {instrument_key}: {e}")
    
    def _extract_tick_data(self, instrument_key: str, feed_data: dict) -> Optional[TickData]:
        """Extract tick data efficiently"""
        try:
            ltp = 0.0
            bid = 0.0
            ask = 0.0
            bid_qty = 0
            ask_qty = 0
            volume = 0
            
            # Handle different feed structures
            if 'fullFeed' in feed_data:
                full_feed = feed_data['fullFeed']
                
                # Price data
                market_ff = full_feed.get('marketFF', {})
                ltpc = market_ff.get('ltpc', {})
                ltp = float(ltpc.get('ltp', 0.0))
                
                # Order book
                market_level = full_feed.get('marketLevel', {})
                bid_ask_quotes = market_level.get('bidAskQuote', [])
                if bid_ask_quotes:
                    best_quote = bid_ask_quotes[0]
                    bid = float(best_quote.get('bidP', 0.0))
                    ask = float(best_quote.get('askP', 0.0))
                    bid_qty = int(best_quote.get('bidQ', 0))
                    ask_qty = int(best_quote.get('askQ', 0))
                
                volume = int(market_ff.get('vtt', 0))
                
            elif 'ltpc' in feed_data:
                ltpc = feed_data['ltpc']
                ltp = float(ltpc.get('ltp', 0.0))
            
            if ltp > 0:
                return TickData(
                    timestamp=time.time(),
                    ltp=ltp,
                    bid=bid,
                    ask=ask,
                    bid_qty=bid_qty,
                    ask_qty=ask_qty,
                    volume=volume
                )
            
            return None
            
        except Exception as e:
            logging.error(f"❌ Error extracting tick: {e}")
            return None
    
    def _display_metrics(self, metrics: RealTimeMetrics):
        """Display metrics with throttling"""
        current_time = time.time()
        
        if current_time - self.last_display_time[metrics.instrument] < self.display_interval:
            return
        
        self.last_display_time[metrics.instrument] = current_time
        
        instrument_name = self.symbol_mapping[metrics.instrument]
        timestamp = datetime.fromtimestamp(metrics.timestamp).strftime("%H:%M:%S")
        
        # Generate signals
        signals = []
        if metrics.vwap_ma_signal == 1:
            signals.append("🔥 BULL")
        elif metrics.vwap_ma_signal == -1:
            signals.append("🧊 BEAR")
        
        if metrics.liquidity_flag:
            signals.append("💧 LIQ")
        
        if metrics.volatility_flag:
            signals.append("⚡ VOL")
        
        signal_str = " | ".join(signals) if signals else "😴 QUIET"
        
        # TCS special display
        if "TCS" in instrument_name.upper() or "TATA CONSULTANCY" in instrument_name.upper():
            print("\n" + "="*90)
            print(f"🎯 {instrument_name} - {timestamp}")
            print("="*90)
            print(f"💰 LTP: ₹{metrics.ltp:.2f} | Mid: ₹{metrics.mid_price:.2f} | Spread: ₹{metrics.spread:.2f} ({metrics.spread_bps:.1f} bps)")
            print(f"📊 VWAP: 30s=₹{metrics.vwap_30s:.2f} | 60s=₹{metrics.vwap_60s:.2f} | 5m=₹{metrics.vwap_300s:.2f}")
            print(f"📈 Return: {metrics.tick_return:.4f} | Vol: {metrics.volatility_5min:.4f} | Trend: {metrics.trend_strength:.4f}")
            print(f"⚖️  Imbalance: {metrics.order_imbalance:.3f} | Depth: {metrics.total_depth:,}")
            print(f"🎯 SIGNALS: {signal_str}")
            print("="*90)
        else:
            print(f"📊 {instrument_name}: LTP=₹{metrics.ltp:.2f} | "
                  f"VWAP60=₹{metrics.vwap_60s:.2f} | Ret={metrics.tick_return:.4f} | "
                  f"Imb={metrics.order_imbalance:.2f} | {signal_str}")
    
    def show_summary(self):
        """Show session summary"""
        print("\n" + "="*100)
        print("📊 LOW-LEVEL HIGH-PERFORMANCE ANALYTICS SUMMARY")
        print("="*100)
        print(f"📨 Ticks processed: {self.tick_count:,}")
        print(f"🧮 Metrics calculated: {self.metrics_count:,}")
        print(f"❌ Errors: {self.error_count}")
        print(f"📡 Instruments: {len(self.instruments)}")
        print(f"🗃️  Buffer size: {ROLLING_WINDOW_SIZE} per instrument")
        
        print(f"\n📊 BUFFER STATUS:")
        for instrument_key, buffer in self.buffers.items():
            name = self.symbol_mapping[instrument_key]
            utilization = (buffer.count / ROLLING_WINDOW_SIZE) * 100
            print(f"   📈 {name}: {buffer.count:,} ticks ({utilization:.1f}%)")
        
        redis_status = "✅ Connected" if self.redis_manager.redis_connected else "❌ Disconnected"
        print(f"\n🗃️  Redis TimeSeries: {redis_status}")
        
        print("\n🚀 PERFORMANCE FEATURES:")
        print("   ✅ Direct WebSocket connection (no SDK overhead)")
        print("   ✅ NumPy circular buffers for speed")
        print("   ✅ Numba JIT compilation for calculations")
        print("   ✅ Robust Redis error handling")
        print("   ✅ Validated data storage")
        print("   ✅ Async processing pipeline")
        print("="*100)

async def main():
    """Main async function"""
    
    # Check required files
    required_files = [CREDENTIALS_PATH, SYMBOLS_PATH]
    for file_path in required_files:
        if not os.path.exists(file_path):
            print(f"❌ Required file missing: {file_path}")
            return
    
    client = LowLevelWebSocketClient()
    
    try:
        print("🚀 Low-Level High-Performance Market Analytics")
        print("📡 Direct WebSocket + NumPy + Numba + Redis TimeSeries")
        print("🎯 600-row rolling window with validated metrics")
        print("⏹️  Press Ctrl+C for graceful shutdown")
        print("🔥 HIGH-PERFORMANCE STREAMING:")
        print("-" * 80)
        
        await client.connect_and_stream()
        
    except KeyboardInterrupt:
        print("\n👋 Graceful shutdown...")
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
        print(f"\n💥 Error: {e}")
    finally:
        client.show_summary()

if __name__ == "__main__":
    # Check dependencies
    try:
        import websockets
        import numpy
        import redis
        import numba
        print("✅ All low-level libraries ready")
    except ImportError as e:
        print(f"❌ Missing library: {e}")
        print("🔧 Install: pip install websockets numpy redis numba requests")
        exit(1)
    
    # Run async main
    asyncio.run(main())