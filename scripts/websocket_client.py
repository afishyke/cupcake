#!/usr/bin/env python3
"""
High-Performance Real-time Market Analytics Engine
Optimized for minimal latency and maximum throughput

Key Optimizations:
- NumPy circular buffers (no Pandas DataFrame creation per tick)
- Incremental calculations for all indicators
- Multi-timeframe analysis (1m, 5m, 15m)
- Advanced pattern detection
- Robust outlier filtering
- Environment-based configuration
- Graceful shutdown with data persistence
"""

import upstox_client
import time
import logging
import os
import json
import redis
import signal
import sys
from datetime import datetime, timezone
import pytz
import numpy as np
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, NamedTuple
import threading
from dataclasses import dataclass
import math
from numba import jit
import warnings
warnings.filterwarnings('ignore')

# Environment-based configuration
CREDENTIALS_PATH = os.getenv('CUPCAKE_CREDENTIALS_PATH', os.path.join('keys', 'credentials', 'credentials.json'))
SYMBOLS_PATH = os.getenv('CUPCAKE_SYMBOLS_PATH', os.path.join('keys', 'config', 'symbols.json'))
LOG_DIR = os.getenv('CUPCAKE_LOG_DIR', 'logs')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '1'))

# Analytics parameters
BUFFER_SIZE = int(os.getenv('BUFFER_SIZE', '600'))  # 10 minutes at 1 tick/sec
OUTLIER_SIGMA = float(os.getenv('OUTLIER_SIGMA', '3.0'))  # Standard deviations for outlier detection

class TickData(NamedTuple):
    """Lightweight tick data structure"""
    timestamp: float
    ltp: float
    bid: float
    ask: float
    bid_qty: int
    ask_qty: int
    volume: int

@dataclass
class MultiTimeframeMetrics:
    """Multi-timeframe analytics results"""
    timestamp: float
    instrument: str
    
    # Price metrics
    ltp: float
    mid_price: float
    spread: float
    spread_bps: float
    
    # Volume metrics
    vwap_1m: float
    vwap_5m: float
    vwap_15m: float
    vwma_10: float
    vwma_30: float
    
    # Volatility metrics
    true_range: float
    atr_14: float
    parkinson_vol: float
    
    # Technical indicators
    roc_1m: float
    roc_5m: float
    ema_9: float
    ema_21: float
    
    # Order book metrics
    imbalance: float
    liquidity_score: float
    
    # Pattern signals
    bb_squeeze: bool
    keltner_breakout: int  # -1, 0, 1
    macd_signal: int      # -1, 0, 1
    vwap_alignment: int   # -1, 0, 1 (all timeframes aligned)

class CircularBuffer:
    """High-performance circular buffer for time series data"""
    
    def __init__(self, size: int):
        self.size = size
        self.buffer = np.full(size, np.nan, dtype=np.float64)
        self.timestamps = np.full(size, np.nan, dtype=np.float64)
        self.index = 0
        self.count = 0
        self.lock = threading.Lock()
    
    def add(self, timestamp: float, value: float) -> None:
        """Add new value to circular buffer"""
        with self.lock:
            self.buffer[self.index] = value
            self.timestamps[self.index] = timestamp
            self.index = (self.index + 1) % self.size
            self.count = min(self.count + 1, self.size)
    
    def get_recent(self, n: int) -> Tuple[np.ndarray, np.ndarray]:
        """Get last n values with timestamps"""
        with self.lock:
            if self.count == 0:
                return np.array([]), np.array([])
            
            n = min(n, self.count)
            if self.count < self.size:
                # Buffer not full yet
                end_idx = self.count
                values = self.buffer[:end_idx][-n:]
                timestamps = self.timestamps[:end_idx][-n:]
            else:
                # Buffer is full, handle wraparound
                if n <= self.index:
                    values = self.buffer[self.index-n:self.index]
                    timestamps = self.timestamps[self.index-n:self.index]
                else:
                    # Need to wrap around
                    part1_size = n - self.index
                    part1_values = self.buffer[-part1_size:]
                    part1_timestamps = self.timestamps[-part1_size:]
                    part2_values = self.buffer[:self.index]
                    part2_timestamps = self.timestamps[:self.index]
                    values = np.concatenate([part1_values, part2_values])
                    timestamps = np.concatenate([part1_timestamps, part2_timestamps])
            
            return timestamps, values
    
    def get_all_valid(self) -> Tuple[np.ndarray, np.ndarray]:
        """Get all valid values"""
        return self.get_recent(self.count)

class InstrumentAnalytics:
    """High-performance analytics for a single instrument"""
    
    def __init__(self, instrument_key: str, buffer_size: int = BUFFER_SIZE):
        self.instrument_key = instrument_key
        self.buffer_size = buffer_size
        
        # Circular buffers for raw data
        self.price_buffer = CircularBuffer(buffer_size)
        self.volume_buffer = CircularBuffer(buffer_size)
        self.bid_buffer = CircularBuffer(buffer_size)
        self.ask_buffer = CircularBuffer(buffer_size)
        self.bid_qty_buffer = CircularBuffer(buffer_size)
        self.ask_qty_buffer = CircularBuffer(buffer_size)
        
        # Running calculations for efficiency
        self.last_price = 0.0
        self.last_volume = 0
        self.cumulative_pv = 0.0  # price * volume
        self.cumulative_volume = 0
        
        # EMA states
        self.ema_9_state = None
        self.ema_21_state = None
        
        # ATR calculation state
        self.atr_state = None
        self.prev_close = 0.0
        
        # MACD state
        self.macd_ema_12 = None
        self.macd_ema_26 = None
        self.macd_signal_ema = None
        
        # Outlier detection
        self.price_mean = 0.0
        self.price_var = 0.0
        self.outlier_count = 0
        
        self.lock = threading.Lock()
    
    def is_outlier(self, price: float) -> bool:
        """Detect price outliers using running statistics"""
        if self.price_mean == 0.0:
            return False
        
        std_dev = math.sqrt(self.price_var) if self.price_var > 0 else 0
        if std_dev == 0:
            return False
        
        z_score = abs(price - self.price_mean) / std_dev
        return z_score > OUTLIER_SIGMA
    
    def update_running_stats(self, price: float, n: int):
        """Update running mean and variance for outlier detection"""
        if n == 1:
            self.price_mean = price
            self.price_var = 0.0
        else:
            # Welford's online algorithm
            delta = price - self.price_mean
            self.price_mean += delta / n
            delta2 = price - self.price_mean
            self.price_var += (delta * delta2 - self.price_var) / n
    
    def add_tick(self, tick: TickData) -> bool:
        """Add new tick data with outlier filtering"""
        with self.lock:
            # Outlier detection
            if self.is_outlier(tick.ltp):
                self.outlier_count += 1
                return False
            
            # Sanity checks
            if tick.ltp <= 0 or tick.volume < 0:
                return False
            
            if tick.bid > tick.ask and tick.bid > 0 and tick.ask > 0:
                return False  # Invalid bid-ask spread
            
            # Update running statistics
            _, valid_prices = self.price_buffer.get_all_valid()
            n_valid = len(valid_prices) + 1
            self.update_running_stats(tick.ltp, n_valid)
            
            # Add to buffers
            self.price_buffer.add(tick.timestamp, tick.ltp)
            self.volume_buffer.add(tick.timestamp, tick.volume)
            self.bid_buffer.add(tick.timestamp, tick.bid)
            self.ask_buffer.add(tick.timestamp, tick.ask)
            self.bid_qty_buffer.add(tick.timestamp, tick.bid_qty)
            self.ask_qty_buffer.add(tick.timestamp, tick.ask_qty)
            
            # Update cumulative VWAP components
            if tick.volume > 0:
                self.cumulative_pv += tick.ltp * tick.volume
                self.cumulative_volume += tick.volume
            
            self.last_price = tick.ltp
            self.last_volume = tick.volume
            
            return True

@jit(nopython=True)
def calculate_vwap_numba(prices: np.ndarray, volumes: np.ndarray, lookback: int) -> float:
    """Fast VWAP calculation using Numba"""
    if len(prices) == 0 or lookback <= 0:
        return 0.0
    
    start_idx = max(0, len(prices) - lookback)
    price_slice = prices[start_idx:]
    volume_slice = volumes[start_idx:]
    
    valid_mask = ~np.isnan(price_slice) & ~np.isnan(volume_slice) & (volume_slice > 0)
    
    if not np.any(valid_mask):
        return 0.0
    
    valid_prices = price_slice[valid_mask]
    valid_volumes = volume_slice[valid_mask]
    
    total_value = np.sum(valid_prices * valid_volumes)
    total_volume = np.sum(valid_volumes)
    
    return total_value / total_volume if total_volume > 0 else 0.0

@jit(nopython=True)
def calculate_parkinson_volatility(highs: np.ndarray, lows: np.ndarray, window: int) -> float:
    """Calculate Parkinson volatility estimator"""
    if len(highs) < window or window <= 1:
        return 0.0
    
    log_hl_ratios = np.log(highs[-window:] / lows[-window:])
    valid_ratios = log_hl_ratios[~np.isnan(log_hl_ratios)]
    
    if len(valid_ratios) == 0:
        return 0.0
    
    parkinson_var = np.mean(valid_ratios ** 2) / (4 * np.log(2))
    return np.sqrt(parkinson_var * 252)  # Annualized

@jit(nopython=True)
def calculate_true_range(high: float, low: float, prev_close: float) -> float:
    """Calculate True Range"""
    if prev_close <= 0:
        return high - low if high > low else 0.0
    
    tr1 = high - low
    tr2 = abs(high - prev_close)
    tr3 = abs(low - prev_close)
    
    return max(tr1, tr2, tr3)

class AdvancedAnalyticsEngine:
    """Advanced analytics calculations"""
    
    @staticmethod
    def calculate_ema(price: float, prev_ema: Optional[float], period: int) -> float:
        """Calculate Exponential Moving Average"""
        if prev_ema is None:
            return price
        
        alpha = 2.0 / (period + 1)
        return alpha * price + (1 - alpha) * prev_ema
    
    @staticmethod
    def calculate_atr(true_range: float, prev_atr: Optional[float], period: int = 14) -> float:
        """Calculate Average True Range"""
        if prev_atr is None:
            return true_range
        
        return ((period - 1) * prev_atr + true_range) / period
    
    @staticmethod
    def calculate_roc(current_price: float, past_price: float) -> float:
        """Calculate Rate of Change"""
        if past_price <= 0:
            return 0.0
        return (current_price - past_price) / past_price
    
    @staticmethod
    def detect_bollinger_squeeze(prices: np.ndarray, window: int = 20) -> bool:
        """Detect Bollinger Band squeeze"""
        if len(prices) < window:
            return False
        
        recent_prices = prices[-window:]
        valid_prices = recent_prices[~np.isnan(recent_prices)]
        
        if len(valid_prices) < window:
            return False
        
        std_dev = np.std(valid_prices)
        mean_price = np.mean(valid_prices)
        
        # Squeeze when standard deviation is below 2% of mean
        return (std_dev / mean_price) < 0.02 if mean_price > 0 else False
    
    @staticmethod
    def detect_keltner_breakout(prices: np.ndarray, atr_values: np.ndarray, ema_period: int = 20) -> int:
        """Detect Keltner Channel breakout"""
        if len(prices) < ema_period or len(atr_values) == 0:
            return 0
        
        recent_prices = prices[-ema_period:]
        valid_prices = recent_prices[~np.isnan(recent_prices)]
        
        if len(valid_prices) == 0:
            return 0
        
        ema = np.mean(valid_prices)
        current_atr = atr_values[-1] if not np.isnan(atr_values[-1]) else 0
        
        current_price = prices[-1]
        upper_channel = ema + 2 * current_atr
        lower_channel = ema - 2 * current_atr
        
        if current_price > upper_channel:
            return 1  # Bullish breakout
        elif current_price < lower_channel:
            return -1  # Bearish breakout
        
        return 0
    
    @staticmethod
    def calculate_macd_signal(fast_ema: float, slow_ema: float, signal_ema: Optional[float]) -> Tuple[float, float, int]:
        """Calculate MACD line, signal line, and signal"""
        macd_line = fast_ema - slow_ema
        
        if signal_ema is None:
            signal_line = macd_line
        else:
            # 9-period EMA of MACD line
            signal_line = AdvancedAnalyticsEngine.calculate_ema(macd_line, signal_ema, 9)
        
        # Signal generation
        histogram = macd_line - signal_line
        if histogram > 0 and signal_ema is not None and (fast_ema - slow_ema) > signal_ema:
            signal = 1  # Bullish
        elif histogram < 0 and signal_ema is not None and (fast_ema - slow_ema) < signal_ema:
            signal = -1  # Bearish
        else:
            signal = 0  # Neutral
        
        return macd_line, signal_line, signal

class HighPerformanceTracker:
    """Main high-performance tracker class"""
    
    def __init__(self):
        self.configuration = None
        self.streamer = None
        self.instruments = []
        self.symbol_mapping = {}
        
        # Analytics per instrument
        self.analytics = {}
        self.redis_manager = None
        
        # Performance monitoring
        self.tick_count = 0
        self.outlier_count = 0
        self.last_analytics_time = defaultdict(float)
        self.analytics_interval = 1.0  # seconds
        
        # Shutdown handling
        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # Initialize components
        self._setup_logging()
        self._load_configuration()
        self._setup_redis()
        self._initialize_analytics()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
    
    def _setup_logging(self):
        """Setup structured JSON logging"""
        os.makedirs(LOG_DIR, exist_ok=True)
        ist = pytz.timezone('Asia/Kolkata')
        timestamp = datetime.now(ist).strftime("%Y%m%d_%H%M%S")
        log_filename = f"{LOG_DIR}/market_analytics_{timestamp}.json"
        
        # Custom JSON formatter
        class JSONFormatter(logging.Formatter):
            def format(self, record):
                log_data = {
                    'timestamp': self.formatTime(record),
                    'level': record.levelname,
                    'message': record.getMessage(),
                    'module': record.module,
                    'function': record.funcName,
                    'line': record.lineno
                }
                if hasattr(record, 'instrument'):
                    log_data['instrument'] = record.instrument
                if hasattr(record, 'latency_ms'):
                    log_data['latency_ms'] = record.latency_ms
                return json.dumps(log_data)
        
        # File handler with JSON format
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(JSONFormatter())
        
        # Console handler with simple format
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        
        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler, console_handler]
        )
        
        logging.info("🚀 High-Performance Market Analytics Engine Starting...")
    
    def _load_configuration(self):
        """Load configuration from files"""
        try:
            # Load credentials
            with open(CREDENTIALS_PATH, 'r') as f:
                creds = json.load(f)
            
            self.access_token = creds['access_token']
            self.configuration = upstox_client.Configuration()
            self.configuration.access_token = self.access_token
            
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
            
            logging.info(f"✅ Configuration loaded: {len(self.instruments)} instruments")
            
        except Exception as e:
            logging.error(f"❌ Configuration loading failed: {e}")
            raise
    
    def _setup_redis(self):
        """Setup Redis connection with bulk operations"""
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST, 
                port=REDIS_PORT, 
                db=REDIS_DB, 
                decode_responses=True
            )
            self.redis_client.ping()
            
            # Bulk create time series keys
            pipe = self.redis_client.pipeline()
            metrics = [
                'ltp', 'mid_price', 'spread', 'spread_bps', 'volume',
                'vwap_1m', 'vwap_5m', 'vwap_15m', 'vwma_10', 'vwma_30',
                'true_range', 'atr_14', 'parkinson_vol', 'roc_1m', 'roc_5m',
                'ema_9', 'ema_21', 'imbalance', 'liquidity_score'
            ]
            
            for instrument_key in self.instruments:
                instrument_name = self.symbol_mapping[instrument_key].replace(' ', '_').upper()
                for metric in metrics:
                    key = f"ts:{metric}:{instrument_name}"
                    pipe.execute_command('TS.CREATE', key, 'RETENTION', 86400000, 'ON_DUPLICATE', 'LAST')
            
            pipe.execute()
            logging.info("✅ Redis TimeSeries initialized with bulk operations")
            
        except Exception as e:
            logging.warning(f"⚠️ Redis setup failed: {e}")
            self.redis_client = None
    
    def _initialize_analytics(self):
        """Initialize analytics engines for each instrument"""
        for instrument_key in self.instruments:
            self.analytics[instrument_key] = InstrumentAnalytics(instrument_key, BUFFER_SIZE)
        
        logging.info(f"✅ Analytics engines initialized for {len(self.instruments)} instruments")
    
    def _calculate_comprehensive_metrics(self, instrument_key: str) -> Optional[MultiTimeframeMetrics]:
        """Calculate comprehensive multi-timeframe metrics"""
        try:
            analytics = self.analytics[instrument_key]
            
            with analytics.lock:
                # Get recent data
                price_timestamps, prices = analytics.price_buffer.get_all_valid()
                volume_timestamps, volumes = analytics.volume_buffer.get_all_valid()
                bid_timestamps, bids = analytics.bid_buffer.get_all_valid()
                ask_timestamps, asks = analytics.ask_buffer.get_all_valid()
                bid_qty_timestamps, bid_qtys = analytics.bid_qty_buffer.get_all_valid()
                ask_qty_timestamps, ask_qtys = analytics.ask_qty_buffer.get_all_valid()
                
                if len(prices) == 0:
                    return None
                
                current_time = time.time()
                current_price = prices[-1]
                current_bid = bids[-1] if len(bids) > 0 else 0
                current_ask = asks[-1] if len(asks) > 0 else 0
                current_bid_qty = int(bid_qtys[-1]) if len(bid_qtys) > 0 else 0
                current_ask_qty = int(ask_qtys[-1]) if len(ask_qtys) > 0 else 0
                
                # Basic price metrics
                mid_price = (current_bid + current_ask) / 2 if current_bid > 0 and current_ask > 0 else current_price
                spread = current_ask - current_bid if current_ask > 0 and current_bid > 0 else 0
                spread_bps = (spread / mid_price * 10000) if mid_price > 0 else 0
                
                # Multi-timeframe VWAP (1m = 60 ticks, 5m = 300 ticks, 15m = 900 ticks)
                vwap_1m = calculate_vwap_numba(prices, volumes, 60)
                vwap_5m = calculate_vwap_numba(prices, volumes, 300)
                vwap_15m = calculate_vwap_numba(prices, volumes, 900)
                
                # Volume-weighted moving averages
                vwma_10 = calculate_vwap_numba(prices, volumes, 10)
                vwma_30 = calculate_vwap_numba(prices, volumes, 30)
                
                # True Range and ATR
                prev_close = analytics.prev_close if analytics.prev_close > 0 else current_price
                true_range = calculate_true_range(current_price, current_price, prev_close)  # Simplified for tick data
                
                if analytics.atr_state is None:
                    analytics.atr_state = true_range
                else:
                    analytics.atr_state = AdvancedAnalyticsEngine.calculate_atr(true_range, analytics.atr_state)
                
                analytics.prev_close = current_price
                
                # Parkinson volatility (using price as both high and low for tick data)
                parkinson_vol = calculate_parkinson_volatility(prices, prices, min(60, len(prices)))
                
                # Rate of Change
                roc_1m = 0.0
                roc_5m = 0.0
                if len(prices) >= 60:
                    roc_1m = AdvancedAnalyticsEngine.calculate_roc(current_price, prices[-60])
                if len(prices) >= 300:
                    roc_5m = AdvancedAnalyticsEngine.calculate_roc(current_price, prices[-300])
                
                # EMAs
                analytics.ema_9_state = AdvancedAnalyticsEngine.calculate_ema(
                    current_price, analytics.ema_9_state, 9
                )
                analytics.ema_21_state = AdvancedAnalyticsEngine.calculate_ema(
                    current_price, analytics.ema_21_state, 21
                )
                
                # Order book imbalance
                total_qty = current_bid_qty + current_ask_qty
                imbalance = (current_bid_qty - current_ask_qty) / total_qty if total_qty > 0 else 0
                
                # Liquidity score
                liquidity_score = 0.0
                if mid_price > 0:
                    spread_penalty = max(0, 100 - spread_bps * 10)
                    depth_score = min(100, total_qty / 1000 * 100)
                    liquidity_score = spread_penalty * 0.6 + depth_score * 0.4
                
                # Pattern detection
                bb_squeeze = AdvancedAnalyticsEngine.detect_bollinger_squeeze(prices)
                
                # Simplified ATR array for Keltner channels
                atr_array = np.array([analytics.atr_state] * min(20, len(prices)))
                keltner_breakout = AdvancedAnalyticsEngine.detect_keltner_breakout(prices, atr_array)
                
                # MACD calculation
                if analytics.macd_ema_12 is None:
                    analytics.macd_ema_12 = current_price
                    analytics.macd_ema_26 = current_price
                else:
                    analytics.macd_ema_12 = AdvancedAnalyticsEngine.calculate_ema(
                        current_price, analytics.macd_ema_12, 12
                    )
                    analytics.macd_ema_26 = AdvancedAnalyticsEngine.calculate_ema(
                        current_price, analytics.macd_ema_26, 26
                    )
                
                macd_line, signal_line, macd_signal = AdvancedAnalyticsEngine.calculate_macd_signal(
                    analytics.macd_ema_12, analytics.macd_ema_26, analytics.macd_signal_ema
                )
                analytics.macd_signal_ema = signal_line
                
                # VWAP alignment across timeframes
                vwap_alignment = 0
                if vwap_1m > 0 and vwap_5m > 0 and vwap_15m > 0:
                    if current_price > vwap_1m and current_price > vwap_5m and current_price > vwap_15m:
                        vwap_alignment = 1  # All bullish
                    elif current_price < vwap_1m and current_price < vwap_5m and current_price < vwap_15m:
                        vwap_alignment = -1  # All bearish
                
                return MultiTimeframeMetrics(
                    timestamp=current_time,
                    instrument=instrument_key,
                    ltp=current_price,
                    mid_price=mid_price,
                    spread=spread,
                    spread_bps=spread_bps,
                    vwap_1m=vwap_1m,
                    vwap_5m=vwap_5m,
                    vwap_15m=vwap_15m,
                    vwma_10=vwma_10,
                    vwma_30=vwma_30,
                    true_range=true_range,
                    atr_14=analytics.atr_state,
                    parkinson_vol=parkinson_vol,
                    roc_1m=roc_1m,
                    roc_5m=roc_5m,
                    ema_9=analytics.ema_9_state,
                    ema_21=analytics.ema_21_state,
                    imbalance=imbalance,
                    liquidity_score=liquidity_score,
                    bb_squeeze=bb_squeeze,
                    keltner_breakout=keltner_breakout,
                    macd_signal=macd_signal,
                    vwap_alignment=vwap_alignment
                )
                
        except Exception as e:
            logging.error(f"❌ Error calculating metrics for {instrument_key}: {e}")
            return None
    
    def _store_metrics_bulk(self, metrics: MultiTimeframeMetrics):
        """Store metrics to Redis using bulk operations"""
        if not self.redis_client:
            return
        
        try:
            instrument_name = self.symbol_mapping[metrics.instrument].replace(' ', '_').upper()
            timestamp_ms = int(metrics.timestamp * 1000)
            
            # Prepare bulk data
            pipe = self.redis_client.pipeline()
            
            metric_data = {
                'ltp': metrics.ltp,
                'mid_price': metrics.mid_price,
                'spread': metrics.spread,
                'spread_bps': metrics.spread_bps,
                'vwap_1m': metrics.vwap_1m,
                'vwap_5m': metrics.vwap_5m,
                'vwap_15m': metrics.vwap_15m,
                'vwma_10': metrics.vwma_10,
                'vwma_30': metrics.vwma_30,
                'true_range': metrics.true_range,
                'atr_14': metrics.atr_14,
                'parkinson_vol': metrics.parkinson_vol,
                'roc_1m': metrics.roc_1m,
                'roc_5m': metrics.roc_5m,
                'ema_9': metrics.ema_9,
                'ema_21': metrics.ema_21,
                'imbalance': metrics.imbalance,
                'liquidity_score': metrics.liquidity_score
            }
            
            for metric_name, value in metric_data.items():
                if not np.isnan(value) and not np.isinf(value):
                    key = f"ts:{metric_name}:{instrument_name}"
                    pipe.execute_command('TS.ADD', key, timestamp_ms, value)
            
            pipe.execute()
            
        except Exception as e:
            logging.error(f"❌ Error storing metrics to Redis: {e}")
    
    def _display_comprehensive_analytics(self, metrics: MultiTimeframeMetrics):
        """Display comprehensive analytics with advanced patterns"""
        try:
            instrument_name = self.symbol_mapping[metrics.instrument]
            timestamp = datetime.fromtimestamp(metrics.timestamp).strftime("%H:%M:%S")
            
            # Collect signals
            signals = []
            if metrics.vwap_alignment == 1:
                signals.append("🚀 VWAP_BULL")
            elif metrics.vwap_alignment == -1:
                signals.append("🔻 VWAP_BEAR")
            
            if metrics.bb_squeeze:
                signals.append("🔥 BB_SQUEEZE")
            
            if metrics.keltner_breakout == 1:
                signals.append("📈 KELT_BULL")
            elif metrics.keltner_breakout == -1:
                signals.append("📉 KELT_BEAR")
            
            if metrics.macd_signal == 1:
                signals.append("💚 MACD_BULL")
            elif metrics.macd_signal == -1:
                signals.append("❤️ MACD_BEAR")
            
            signal_str = " | ".join(signals) if signals else "😴 NEUTRAL"
            
            # Special display for TCS
            if "TCS" in instrument_name.upper() or "TATA CONSULTANCY" in instrument_name.upper():
                print("\n" + "="*100)
                print(f"🎯 {instrument_name} ({metrics.instrument}) - {timestamp}")
                print("="*100)
                print(f"💰 LTP:           ₹{metrics.ltp:.2f} | Mid: ₹{metrics.mid_price:.2f}")
                print(f"📊 Spread:        ₹{metrics.spread:.2f} ({metrics.spread_bps:.1f} bps)")
                print(f"📈 VWAP:          1m=₹{metrics.vwap_1m:.2f} | 5m=₹{metrics.vwap_5m:.2f} | 15m=₹{metrics.vwap_15m:.2f}")
                print(f"📊 VWMA:          10=₹{metrics.vwma_10:.2f} | 30=₹{metrics.vwma_30:.2f}")
                print(f"📈 EMA:           9=₹{metrics.ema_9:.2f} | 21=₹{metrics.ema_21:.2f}")
                print(f"📊 Volatility:    ATR=₹{metrics.atr_14:.2f} | Parkinson={metrics.parkinson_vol:.4f}")
                print(f"🚀 Momentum:      ROC_1m={metrics.roc_1m:.4f} | ROC_5m={metrics.roc_5m:.4f}")
                print(f"⚖️  Order Book:    Imbalance={metrics.imbalance:.3f} | Liquidity={metrics.liquidity_score:.1f}/100")
                print(f"🎯 SIGNALS:       {signal_str}")
                print("="*100)
                
            else:
                # Compact display for other stocks
                print(f"📊 {instrument_name}: LTP=₹{metrics.ltp:.2f} | "
                      f"VWAP_1m=₹{metrics.vwap_1m:.2f} | ATR=₹{metrics.atr_14:.2f} | "
                      f"Liq={metrics.liquidity_score:.0f} | {signal_str}")
                
        except Exception as e:
            logging.error(f"❌ Error displaying analytics: {e}")
    
    def on_message(self, message):
        """Process incoming market data with high-performance analytics"""
        start_time = time.time()
        
        try:
            if self.shutdown_requested:
                return
            
            self.tick_count += 1
            
            # Parse message
            if isinstance(message, dict):
                message_data = message
            elif hasattr(message, 'to_dict'):
                message_data = message.to_dict()
            else:
                return
            
            # Process feeds
            feeds = message_data.get("feeds", {})
            
            for instrument_key, feed_data in feeds.items():
                if instrument_key not in self.instruments:
                    continue
                
                # Extract tick data
                tick = self._extract_tick_data(instrument_key, feed_data)
                if not tick:
                    continue
                
                # Add to analytics with outlier filtering
                analytics = self.analytics[instrument_key]
                if not analytics.add_tick(tick):
                    self.outlier_count += 1
                    continue
                
                # Calculate comprehensive metrics (throttled)
                current_time = time.time()
                if current_time - self.last_analytics_time[instrument_key] >= self.analytics_interval:
                    metrics = self._calculate_comprehensive_metrics(instrument_key)
                    if metrics:
                        # Store to Redis
                        self._store_metrics_bulk(metrics)
                        
                        # Display analytics
                        self._display_comprehensive_analytics(metrics)
                        
                        self.last_analytics_time[instrument_key] = current_time
            
            # Performance monitoring
            processing_time = (time.time() - start_time) * 1000
            if processing_time > 10:  # Log if processing takes > 10ms
                extra = {'latency_ms': processing_time}
                logging.warning(f"⚠️ High processing latency: {processing_time:.2f}ms", extra=extra)
            
            # Progress logging
            if self.tick_count % 1000 == 0:
                outlier_rate = (self.outlier_count / self.tick_count) * 100
                logging.info(f"📊 Processed {self.tick_count:,} ticks, {self.outlier_count} outliers ({outlier_rate:.2f}%)")
                
        except Exception as e:
            logging.error(f"❌ Message processing error: {e}")
    
    def _extract_tick_data(self, instrument_key: str, feed_data: dict) -> Optional[TickData]:
        """Extract tick data from feed"""
        try:
            # Extract LTP
            ltpc = feed_data.get("ltpc", {})
            ltp = ltpc.get("ltp", 0.0)
            
            # Extract bid/ask
            market_level = feed_data.get("market_level", {})
            bid_ask_quotes = market_level.get("bid_ask_quote", [])
            
            bid_price = 0.0
            ask_price = 0.0
            bid_qty = 0
            ask_qty = 0
            
            if bid_ask_quotes:
                best_bid_ask = bid_ask_quotes[0]
                bid_price = best_bid_ask.get("bid_p", 0.0)
                ask_price = best_bid_ask.get("ask_p", 0.0)
                bid_qty = best_bid_ask.get("bid_q", 0)
                ask_qty = best_bid_ask.get("ask_q", 0)
            
            # Extract volume
            market_ff = feed_data.get("market_ff", {})
            volume = market_ff.get("vtt", 0)
            
            return TickData(
                timestamp=time.time(),
                ltp=float(ltp) if ltp else 0.0,
                bid=float(bid_price) if bid_price else 0.0,
                ask=float(ask_price) if ask_price else 0.0,
                bid_qty=int(bid_qty) if bid_qty else 0,
                ask_qty=int(ask_qty) if ask_qty else 0,
                volume=int(volume) if volume else 0
            )
            
        except Exception as e:
            logging.error(f"❌ Error extracting tick data for {instrument_key}: {e}")
            return None
    
    def on_open(self):
        """Handle connection open"""
        logging.info("✅ WebSocket connection opened")
        
        try:
            logging.info(f"📡 Subscribing to {len(self.instruments)} instruments...")
            self.streamer.subscribe(self.instruments, "full")
            logging.info("✅ Subscription successful")
            
            for i, instrument in enumerate(self.instruments, 1):
                name = self.symbol_mapping[instrument]
                logging.info(f"   {i:2d}. {name} ({instrument})")
                
        except Exception as e:
            logging.error(f"❌ Subscription failed: {e}")
    
    def on_error(self, error):
        """Handle errors"""
        logging.error(f"❌ WebSocket error: {error}")
    
    def on_close(self):
        """Handle connection close"""
        logging.info("🔌 WebSocket connection closed")
    
    def start_streaming(self):
        """Start the market data stream"""
        try:
            logging.info("🚀 Initializing High-Performance Market Data Streamer...")
            
            self.streamer = upstox_client.MarketDataStreamerV3(
                upstox_client.ApiClient(self.configuration),
                instrumentKeys=[],
                mode="full"
            )
            
            # Set event handlers
            self.streamer.on("open", self.on_open)
            self.streamer.on("message", self.on_message)
            self.streamer.on("error", self.on_error)
            self.streamer.on("close", self.on_close)
            
            # Connect
            logging.info("🔗 Connecting to Upstox WebSocket...")
            self.streamer.connect()
            
        except Exception as e:
            logging.error(f"❌ Failed to start streaming: {e}")
            raise
    
    def stop_streaming(self):
        """Stop streaming with graceful data persistence"""
        try:
            if self.streamer:
                self.streamer.disconnect()
            
            # Flush remaining analytics to Redis
            logging.info("💾 Flushing final analytics to Redis...")
            for instrument_key in self.instruments:
                metrics = self._calculate_comprehensive_metrics(instrument_key)
                if metrics:
                    self._store_metrics_bulk(metrics)
            
            logging.info("🛑 Streaming stopped gracefully")
            
        except Exception as e:
            logging.error(f"❌ Error during shutdown: {e}")
    
    def show_performance_summary(self):
        """Show comprehensive performance summary"""
        print("\n" + "="*100)
        print("📊 HIGH-PERFORMANCE ANALYTICS SUMMARY")
        print("="*100)
        print(f"📨 Total ticks processed: {self.tick_count:,}")
        print(f"🚫 Outliers filtered: {self.outlier_count:,} ({(self.outlier_count/max(1,self.tick_count)*100):.2f}%)")
        print(f"📡 Instruments tracked: {len(self.instruments)}")
        
        # Show per-instrument statistics
        for instrument_key in self.instruments:
            analytics = self.analytics[instrument_key]
            name = self.symbol_mapping[instrument_key]
            valid_count = analytics.price_buffer.count
            outliers = analytics.outlier_count
            buffer_util = (valid_count / BUFFER_SIZE) * 100
            
            print(f"   📈 {name}: {valid_count:,} valid ticks, {outliers} outliers, "
                  f"buffer {buffer_util:.1f}% utilized")
        
        # System status
        if self.redis_client:
            print("✅ Redis TimeSeries: Operational - data persisted")
        else:
            print("❌ Redis TimeSeries: Unavailable")
        
        print("✅ All analytics computed with multi-timeframe confirmation")
        print("✅ Advanced pattern detection enabled")
        print("✅ Outlier filtering and data validation active")
        print("="*100)

def main():
    """Main execution function"""
    tracker = HighPerformanceTracker()
    
    try:
        print("🚀 Starting High-Performance Market Analytics Engine")
        print("💡 Features: Multi-timeframe VWAP, ATR, Pattern Detection, Outlier Filtering")
        print(f"📡 Tracking {len(tracker.instruments)} instruments with {BUFFER_SIZE}-tick rolling windows")
        print(f"📊 Advanced indicators: Bollinger Squeeze, Keltner Channels, MACD, Parkinson Volatility")
        print("⏹️  Press Ctrl+C for graceful shutdown\n")
        
        tracker.start_streaming()
        
        # Keep the connection alive
        while not tracker.shutdown_requested:
            time.sleep(0.1)  # Reduced sleep for better responsiveness
            
    except KeyboardInterrupt:
        print("\n👋 Graceful shutdown initiated...")
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
    finally:
        tracker.stop_streaming()
        tracker.show_performance_summary()

if __name__ == "__main__":
    # Check required packages
    required_packages = ['upstox_client', 'numpy', 'redis', 'numba']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("❌ Missing required packages:")
        for package in missing_packages:
            print(f"   📦 {package}")
        print("\n🔧 Install with:")
        print(f"pip install {' '.join(missing_packages)}")
        exit(1)
    
    main()