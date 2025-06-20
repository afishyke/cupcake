#!/usr/bin/env python3
"""
Live Technical Indicator Calculator with TimeSeries Storage for CUPCAKE Trading System

This system monitors Redis DB0 for new OHLCV data and calculates comprehensive
technical indicators in real-time, storing results as TimeSeries in Redis DB2.

KEY FEATURES:
- Real-time monitoring of OHLCV data in Redis DB0
- Comprehensive technical indicators using TA-Lib
- TIME SERIES STORAGE for indicators with timestamps
- Fast indicator calculation and storage in Redis DB2 TimeSeries
- Handles missing data gracefully with proper flagging
- Trend, Momentum, Volume, and Volatility indicators
- Optimized for day trading requirements
- Historical indicator analysis capabilities
- Detailed logging and error handling
"""

import json
import logging
import redis
import numpy as np
import pandas as pd
import talib
import os
import threading
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pytz
from dataclasses import dataclass
import time as time_module
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

# Suppress TA-Lib warnings
warnings.filterwarnings('ignore')

# Setup IST timezone
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

# Global flag for graceful shutdown
SHUTDOWN_FLAG = threading.Event()

def get_log_filename():
    """Generate log filename with IST date and time"""
    ist_now = datetime.now(IST_TIMEZONE)
    timestamp = ist_now.strftime('%Y%m%d_%H%M%S_IST')
    log_dir = "/home/abhishek/projects/CUPCAKE/logs"
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"live_technical_indicators_ts_{timestamp}.log")

# Configure logging
LOG_FILENAME = get_log_filename()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILENAME),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Log the filename being used
logger.info(f"Technical Indicators TimeSeries logging to: {LOG_FILENAME}")

@dataclass
class IndicatorConfig:
    """Configuration for technical indicators"""
    # Trend Indicators
    sma_periods: List[int] = None
    ema_periods: List[int] = None
    macd_params: Dict[str, int] = None
    bb_period: int = 20
    bb_std: float = 2.0
    
    # Momentum Indicators
    rsi_period: int = 14
    stoch_k_period: int = 14
    stoch_d_period: int = 3
    cci_period: int = 14
    williams_r_period: int = 14
    
    # Volume Indicators
    volume_sma_periods: List[int] = None
    
    # Volatility Indicators
    atr_period: int = 14
    std_period: int = 20
    
    def __post_init__(self):
        if self.sma_periods is None:
            self.sma_periods = [5, 10, 20, 50, 100, 200]
        if self.ema_periods is None:
            self.ema_periods = [5, 10, 20, 50]
        if self.macd_params is None:
            self.macd_params = {'fast': 12, 'slow': 26, 'signal': 9}
        if self.volume_sma_periods is None:
            self.volume_sma_periods = [10, 20, 50]

@dataclass
class SymbolData:
    """Represents OHLCV data for a symbol"""
    symbol: str
    timestamps: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    data_points: int
    last_update: datetime

@dataclass
class IndicatorPoint:
    """Represents a single indicator data point with timestamp"""
    timestamp: datetime
    name: str
    value: float
    symbol: str
    category: str  # trend, momentum, volume, volatility

class RedisDataMonitor:
    """Monitors Redis DB0 for OHLCV data and manages DB2 for indicators"""
    
    def __init__(self, source_db=0, target_db=2):
        self.source_db = source_db
        self.target_db = target_db
        
        # Connect to source Redis DB (OHLCV data)
        try:
            self.source_redis = redis.Redis(
                host='localhost', port=6379, db=source_db,
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=30,
                retry_on_timeout=True
            )
            self.source_redis.ping()
            logger.info(f"Connected to Redis DB{source_db} (OHLCV source)")
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis DB{source_db}: {e}")
            raise
        
        # Connect to target Redis DB (Technical Indicators)
        try:
            self.target_redis = redis.Redis(
                host='localhost', port=6379, db=target_db,
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=30,
                retry_on_timeout=True
            )
            self.target_redis.ping()
            logger.info(f"Connected to Redis DB{target_db} (Technical Indicators)")
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis DB{target_db}: {e}")
            raise
        
        # Flush target database at startup
        self.flush_indicators_database()
        
        # Track last known timestamps for each symbol
        self.last_timestamps = {}
        
        # Statistics
        self.stats = {
            'symbols_monitored': 0,
            'total_calculations': 0,
            'successful_calculations': 0,
            'failed_calculations': 0,
            'missing_data_flags': 0,
            'indicators_stored': 0,
            'timeseries_created': 0,
            'start_time': datetime.now(IST_TIMEZONE),
            'last_update': None
        }
    
    def flush_indicators_database(self):
        """Flush the technical indicators database (DB2)"""
        try:
            self.target_redis.flushdb()
            logger.info("✅ Flushed Redis DB2 (Technical Indicators TimeSeries) - Starting fresh session")
            print("🗑️ Flushed Technical Indicators TimeSeries database - Starting fresh session")
        except redis.RedisError as e:
            logger.error(f"Failed to flush Redis DB{self.target_db}: {e}")
            raise
    
    def get_available_symbols(self) -> List[str]:
        """Get all available symbols from source Redis DB"""
        try:
            # Look for all stock:*:close keys to get symbol names
            pattern = "stock:*:close"
            keys = self.source_redis.keys(pattern)
            
            symbols = []
            for key in keys:
                # Extract symbol name from pattern: stock:{symbol}:close
                parts = key.split(':')
                if len(parts) >= 3:
                    symbol = parts[1]
                    symbols.append(symbol)
            
            logger.info(f"Found {len(symbols)} symbols in Redis DB{self.source_db}")
            return sorted(list(set(symbols)))  # Remove duplicates and sort
            
        except redis.RedisError as e:
            logger.error(f"Failed to get available symbols: {e}")
            return []
    
    def get_symbol_ohlcv_data(self, symbol: str, limit: int = 250) -> Optional[SymbolData]:
        """Retrieve OHLCV data for a symbol from Redis TimeSeries"""
        try:
            data_types = ['open', 'high', 'low', 'close', 'volume']
            ohlcv_data = {}
            
            # Check if all required data exists
            for data_type in data_types:
                key = f"stock:{symbol}:{data_type}"
                
                if not self.source_redis.exists(key):
                    logger.warning(f"⚠️ Missing {data_type} data for {symbol}")
                    self.stats['missing_data_flags'] += 1
                    return None
                
                try:
                    # Get the latest data points using TS.RANGE
                    result = self.source_redis.execute_command(
                        'TS.RANGE', key, '-', '+', 'COUNT', limit
                    )
                    
                    if not result:
                        logger.warning(f"⚠️ No data points found for {symbol}:{data_type}")
                        self.stats['missing_data_flags'] += 1
                        return None
                    
                    # Extract timestamps and values
                    timestamps = []
                    values = []
                    
                    for timestamp_ms, value_str in result:
                        timestamps.append(timestamp_ms)
                        values.append(float(value_str))
                    
                    ohlcv_data[data_type] = {
                        'timestamps': np.array(timestamps),
                        'values': np.array(values)
                    }
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error fetching {data_type} for {symbol}: {e}")
                    self.stats['missing_data_flags'] += 1
                    return None
            
            # Validate data consistency
            if not self._validate_ohlcv_consistency(ohlcv_data):
                logger.warning(f"⚠️ Inconsistent OHLCV data for {symbol}")
                self.stats['missing_data_flags'] += 1
                return None
            
            # Use timestamps from close data (they should all be the same)
            timestamps = ohlcv_data['close']['timestamps']
            
            # Get the latest timestamp for monitoring
            latest_timestamp = datetime.fromtimestamp(
                timestamps[-1] / 1000, tz=IST_TIMEZONE
            ) if len(timestamps) > 0 else datetime.now(IST_TIMEZONE)
            
            symbol_data = SymbolData(
                symbol=symbol,
                timestamps=timestamps,
                open=ohlcv_data['open']['values'],
                high=ohlcv_data['high']['values'],
                low=ohlcv_data['low']['values'],
                close=ohlcv_data['close']['values'],
                volume=ohlcv_data['volume']['values'],
                data_points=len(timestamps),
                last_update=latest_timestamp
            )
            
            logger.debug(f"✅ Retrieved {len(timestamps)} data points for {symbol}")
            return symbol_data
            
        except Exception as e:
            logger.error(f"❌ Error retrieving OHLCV data for {symbol}: {e}")
            self.stats['missing_data_flags'] += 1
            return None
    
    def _validate_ohlcv_consistency(self, ohlcv_data: Dict) -> bool:
        """Validate that all OHLCV data arrays have the same length and timestamps"""
        try:
            reference_length = len(ohlcv_data['close']['timestamps'])
            reference_timestamps = ohlcv_data['close']['timestamps']
            
            for data_type in ['open', 'high', 'low', 'volume']:
                if len(ohlcv_data[data_type]['timestamps']) != reference_length:
                    return False
                
                # Check if timestamps match (with small tolerance for floating point)
                if not np.allclose(ohlcv_data[data_type]['timestamps'], reference_timestamps):
                    return False
            
            return True
            
        except Exception:
            return False
    
    def has_new_data(self, symbol: str) -> bool:
        """Check if symbol has new data since last check"""
        try:
            key = f"stock:{symbol}:close"
            
            # Get latest timestamp
            result = self.source_redis.execute_command('TS.GET', key)
            if not result or len(result) < 2:
                return False
            
            latest_timestamp_ms = result[0]
            
            # Check against last known timestamp
            last_known = self.last_timestamps.get(symbol, 0)
            
            if latest_timestamp_ms > last_known:
                self.last_timestamps[symbol] = latest_timestamp_ms
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error checking new data for {symbol}: {e}")
            return False

class TechnicalIndicatorCalculator:
    """Calculates comprehensive technical indicators using TA-Lib"""
    
    def __init__(self, config: IndicatorConfig):
        self.config = config
        
        # Verify TA-Lib is available
        try:
            # TA-Lib requires float64 arrays
            test_data = np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float64)
            talib.SMA(test_data, timeperiod=3)
            logger.info("✅ TA-Lib initialized successfully")
        except Exception as e:
            logger.error(f"❌ TA-Lib initialization failed: {e}")
            raise
    
    def calculate_all_indicators(self, symbol_data: SymbolData) -> List[IndicatorPoint]:
        """Calculate all technical indicators for a symbol, returning time series points"""
        try:
            if symbol_data.data_points < 200:  # Need sufficient data for reliable indicators
                logger.warning(f"⚠️ Insufficient data for {symbol_data.symbol}: {symbol_data.data_points} points")
                return []
            
            indicator_points = []
            
            # Calculate trend indicators
            trend_points = self._calculate_trend_indicators(symbol_data)
            indicator_points.extend(trend_points)
            
            # Calculate momentum indicators
            momentum_points = self._calculate_momentum_indicators(symbol_data)
            indicator_points.extend(momentum_points)
            
            # Calculate volume indicators
            volume_points = self._calculate_volume_indicators(symbol_data)
            indicator_points.extend(volume_points)
            
            # Calculate volatility indicators
            volatility_points = self._calculate_volatility_indicators(symbol_data)
            indicator_points.extend(volatility_points)
            
            logger.debug(f"✅ Calculated {len(indicator_points)} indicator points for {symbol_data.symbol}")
            return indicator_points
            
        except Exception as e:
            logger.error(f"❌ Error calculating indicators for {symbol_data.symbol}: {e}")
            return []
    
    def _calculate_trend_indicators(self, data: SymbolData) -> List[IndicatorPoint]:
        """Calculate trend-following indicators and return time series points"""
        points = []
        
        try:
            # Ensure data arrays are float64 for TA-Lib
            close = data.close.astype(np.float64)
            high = data.high.astype(np.float64)
            low = data.low.astype(np.float64)
            open_price = data.open.astype(np.float64)
            
            # Simple Moving Averages
            for period in self.config.sma_periods:
                if data.data_points >= period:
                    sma = talib.SMA(close, timeperiod=period)
                    points.extend(self._create_timeseries_points(
                        data, sma, f'sma_{period}', 'trend'
                    ))
            
            # Exponential Moving Averages
            for period in self.config.ema_periods:
                if data.data_points >= period:
                    ema = talib.EMA(close, timeperiod=period)
                    points.extend(self._create_timeseries_points(
                        data, ema, f'ema_{period}', 'trend'
                    ))
            
            # MACD
            if data.data_points >= self.config.macd_params['slow'] + self.config.macd_params['signal']:
                macd, macd_signal, macd_hist = talib.MACD(
                    close,
                    fastperiod=self.config.macd_params['fast'],
                    slowperiod=self.config.macd_params['slow'],
                    signalperiod=self.config.macd_params['signal']
                )
                points.extend(self._create_timeseries_points(data, macd, 'macd', 'trend'))
                points.extend(self._create_timeseries_points(data, macd_signal, 'macd_signal', 'trend'))
                points.extend(self._create_timeseries_points(data, macd_hist, 'macd_histogram', 'trend'))
            
            # Bollinger Bands
            if data.data_points >= self.config.bb_period:
                bb_upper, bb_middle, bb_lower = talib.BBANDS(
                    close,
                    timeperiod=self.config.bb_period,
                    nbdevup=self.config.bb_std,
                    nbdevdn=self.config.bb_std
                )
                points.extend(self._create_timeseries_points(data, bb_upper, 'bb_upper', 'trend'))
                points.extend(self._create_timeseries_points(data, bb_middle, 'bb_middle', 'trend'))
                points.extend(self._create_timeseries_points(data, bb_lower, 'bb_lower', 'trend'))
                
                # Bollinger Band Width
                bb_width = (bb_upper - bb_lower) / bb_middle * 100
                points.extend(self._create_timeseries_points(data, bb_width, 'bb_width', 'trend'))
            
            # Parabolic SAR
            if data.data_points >= 10:
                sar = talib.SAR(high, low)
                points.extend(self._create_timeseries_points(data, sar, 'sar', 'trend'))
            
        except Exception as e:
            logger.warning(f"Error in trend indicators for {data.symbol}: {e}")
        
        return points
    
    def _calculate_momentum_indicators(self, data: SymbolData) -> List[IndicatorPoint]:
        """Calculate momentum oscillators and return time series points"""
        points = []
        
        try:
            # Ensure data arrays are float64 for TA-Lib
            close = data.close.astype(np.float64)
            high = data.high.astype(np.float64)
            low = data.low.astype(np.float64)
            volume = data.volume.astype(np.float64)
            
            # RSI
            if data.data_points >= self.config.rsi_period + 1:
                rsi = talib.RSI(close, timeperiod=self.config.rsi_period)
                points.extend(self._create_timeseries_points(data, rsi, 'rsi', 'momentum'))
            
            # Stochastic Oscillator
            if data.data_points >= self.config.stoch_k_period:
                stoch_k, stoch_d = talib.STOCH(
                    high, low, close,
                    fastk_period=self.config.stoch_k_period,
                    slowk_period=self.config.stoch_d_period,
                    slowd_period=self.config.stoch_d_period
                )
                points.extend(self._create_timeseries_points(data, stoch_k, 'stoch_k', 'momentum'))
                points.extend(self._create_timeseries_points(data, stoch_d, 'stoch_d', 'momentum'))
            
            # Commodity Channel Index
            if data.data_points >= self.config.cci_period:
                cci = talib.CCI(high, low, close, timeperiod=self.config.cci_period)
                points.extend(self._create_timeseries_points(data, cci, 'cci', 'momentum'))
            
            # Williams %R
            if data.data_points >= self.config.williams_r_period:
                williams_r = talib.WILLR(
                    high, low, close,
                    timeperiod=self.config.williams_r_period
                )
                points.extend(self._create_timeseries_points(data, williams_r, 'williams_r', 'momentum'))
            
            # Rate of Change
            if data.data_points >= 12:
                roc = talib.ROC(close, timeperiod=10)
                points.extend(self._create_timeseries_points(data, roc, 'roc', 'momentum'))
            
            # Money Flow Index (if volume data is available)
            if data.data_points >= 14 and np.any(volume > 0):
                mfi = talib.MFI(high, low, close, volume, timeperiod=14)
                points.extend(self._create_timeseries_points(data, mfi, 'mfi', 'momentum'))
            
        except Exception as e:
            logger.warning(f"Error in momentum indicators for {data.symbol}: {e}")
        
        return points
    
    def _calculate_volume_indicators(self, data: SymbolData) -> List[IndicatorPoint]:
        """Calculate volume-based indicators and return time series points"""
        points = []
        
        try:
            # Ensure data arrays are float64 for TA-Lib
            close = data.close.astype(np.float64)
            high = data.high.astype(np.float64)
            low = data.low.astype(np.float64)
            volume = data.volume.astype(np.float64)
            
            # Volume SMAs
            for period in self.config.volume_sma_periods:
                if data.data_points >= period and np.any(volume > 0):
                    vol_sma = talib.SMA(volume, timeperiod=period)
                    points.extend(self._create_timeseries_points(
                        data, vol_sma, f'volume_sma_{period}', 'volume'
                    ))
            
            # On-Balance Volume
            if data.data_points >= 2 and np.any(volume > 0):
                obv = talib.OBV(close, volume)
                points.extend(self._create_timeseries_points(data, obv, 'obv', 'volume'))
            
            # Volume Rate of Change
            if data.data_points >= 12 and np.any(volume > 0):
                vol_roc = talib.ROC(volume, timeperiod=10)
                points.extend(self._create_timeseries_points(data, vol_roc, 'volume_roc', 'volume'))
            
            # Accumulation/Distribution Line
            if data.data_points >= 2 and np.any(volume > 0):
                ad_line = talib.AD(high, low, close, volume)
                points.extend(self._create_timeseries_points(data, ad_line, 'ad_line', 'volume'))
            
            # Volume Ratio (current vs average)
            if data.data_points >= 20 and np.any(volume > 0):
                # Calculate rolling average volume
                vol_avg = talib.SMA(volume, timeperiod=20)
                vol_ratio = volume / vol_avg
                points.extend(self._create_timeseries_points(data, vol_ratio, 'volume_ratio', 'volume'))
            
        except Exception as e:
            logger.warning(f"Error in volume indicators for {data.symbol}: {e}")
        
        return points
    
    def _calculate_volatility_indicators(self, data: SymbolData) -> List[IndicatorPoint]:
        """Calculate volatility indicators and return time series points"""
        points = []
        
        try:
            # Ensure data arrays are float64 for TA-Lib
            close = data.close.astype(np.float64)
            high = data.high.astype(np.float64)
            low = data.low.astype(np.float64)
            
            # Average True Range
            if data.data_points >= self.config.atr_period:
                atr = talib.ATR(high, low, close, timeperiod=self.config.atr_period)
                points.extend(self._create_timeseries_points(data, atr, 'atr', 'volatility'))
                
                # ATR as percentage of price
                atr_percent = (atr / close) * 100
                points.extend(self._create_timeseries_points(data, atr_percent, 'atr_percent', 'volatility'))
            
            # Standard Deviation
            if data.data_points >= self.config.std_period:
                std_dev = talib.STDDEV(close, timeperiod=self.config.std_period)
                points.extend(self._create_timeseries_points(data, std_dev, 'std_dev', 'volatility'))
            
            # True Range
            if data.data_points >= 2:
                true_range = talib.TRANGE(high, low, close)
                points.extend(self._create_timeseries_points(data, true_range, 'true_range', 'volatility'))
            
            # Normalized ATR (ATR relative to recent price range)
            if data.data_points >= 20:
                # Calculate rolling high and low
                highest = talib.MAX(high, timeperiod=20)
                lowest = talib.MIN(low, timeperiod=20)
                price_range = highest - lowest
                
                # Avoid division by zero
                price_range = np.where(price_range == 0, np.nan, price_range)
                
                if data.data_points >= self.config.atr_period:
                    atr = talib.ATR(high, low, close, timeperiod=self.config.atr_period)
                    normalized_atr = atr / price_range
                    points.extend(self._create_timeseries_points(data, normalized_atr, 'normalized_atr', 'volatility'))
            
        except Exception as e:
            logger.warning(f"Error in volatility indicators for {data.symbol}: {e}")
        
        return points
    
    def _create_timeseries_points(self, data: SymbolData, values: np.ndarray, 
                                 indicator_name: str, category: str) -> List[IndicatorPoint]:
        """Create IndicatorPoint objects from numpy array values"""
        points = []
        
        try:
            for i, value in enumerate(values):
                # Skip NaN values
                if np.isnan(value) or np.isinf(value):
                    continue
                
                # Convert timestamp from milliseconds to datetime
                timestamp = datetime.fromtimestamp(
                    data.timestamps[i] / 1000, tz=IST_TIMEZONE
                )
                
                point = IndicatorPoint(
                    timestamp=timestamp,
                    name=indicator_name,
                    value=float(value),
                    symbol=data.symbol,
                    category=category
                )
                points.append(point)
                
        except Exception as e:
            logger.warning(f"Error creating time series points for {indicator_name}: {e}")
        
        return points

class TechnicalIndicatorTimeSeriesStorage:
    """Stores technical indicators in Redis TimeSeries DB2 with organized structure"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.created_keys = set()  # Track created TimeSeries keys
    
    def ensure_timeseries_exists(self, key: str, labels: Dict[str, str]) -> bool:
        """Ensure TimeSeries exists with proper labels"""
        try:
            if key in self.created_keys or self.redis_client.exists(key):
                return True
            
            label_args = []
            for k, v in labels.items():
                label_args.extend([k, v])
            
            self.redis_client.execute_command(
                'TS.CREATE', key,
                'DUPLICATE_POLICY', 'LAST',
                'LABELS', *label_args
            )
            
            self.created_keys.add(key)
            logger.debug(f"Created TimeSeries: {key}")
            return True
            
        except redis.RedisError as e:
            logger.error(f"Failed to ensure TimeSeries {key}: {e}")
            return False
    
    def store_indicator_points(self, points: List[IndicatorPoint]) -> Dict[str, int]:
        """Store multiple indicator points efficiently using pipeline"""
        results = {
            'stored': 0,
            'failed': 0,
            'timeseries_created': 0
        }
        
        if not points:
            return results
        
        try:
            # Group points by TimeSeries key for batch processing
            grouped_points = {}
            
            for point in points:
                # Clean symbol name
                symbol_clean = point.symbol.replace(' ', '_').replace('&', 'and')
                
                # Create TimeSeries key: indicator:{symbol}:{category}:{name}
                key = f"indicator:{symbol_clean}:{point.category}:{point.name}"
                
                if key not in grouped_points:
                    grouped_points[key] = []
                
                grouped_points[key].append(point)
            
            # Process each TimeSeries
            for key, key_points in grouped_points.items():
                try:
                    # Extract metadata from first point
                    first_point = key_points[0]
                    symbol_clean = first_point.symbol.replace(' ', '_').replace('&', 'and')
                    
                    # Create labels for the TimeSeries
                    labels = {
                        'symbol': symbol_clean,
                        'indicator': first_point.name,
                        'category': first_point.category,
                        'source': 'talib_live'
                    }
                    
                    # Ensure TimeSeries exists
                    if not self.ensure_timeseries_exists(key, labels):
                        results['failed'] += len(key_points)
                        continue
                    
                    if key not in self.created_keys:
                        results['timeseries_created'] += 1
                        self.created_keys.add(key)
                    
                    # Batch insert points using pipeline
                    pipeline = self.redis_client.pipeline()
                    
                    for point in key_points:
                        timestamp_ms = int(point.timestamp.timestamp() * 1000)
                        pipeline.execute_command('TS.ADD', key, timestamp_ms, point.value)
                    
                    # Execute batch
                    pipeline.execute()
                    results['stored'] += len(key_points)
                    
                    logger.debug(f"Stored {len(key_points)} points for {key}")
                    
                except redis.RedisError as e:
                    logger.error(f"Failed to store points for {key}: {e}")
                    results['failed'] += len(key_points)
                except Exception as e:
                    logger.error(f"Unexpected error storing {key}: {e}")
                    results['failed'] += len(key_points)
            
            # Set expiration for all keys (7 days for indicators)
            if results['stored'] > 0:
                try:
                    pipeline = self.redis_client.pipeline()
                    for key in grouped_points.keys():
                        pipeline.expire(key, 604800)  # 7 days
                    pipeline.execute()
                except Exception as e:
                    logger.warning(f"Failed to set expiration: {e}")
            
        except Exception as e:
            logger.error(f"Error in batch storage: {e}")
            results['failed'] = len(points)
        
        return results
    
    def get_indicator_timeseries(self, symbol: str, indicator: str, category: str, 
                               start_time: str = '-', end_time: str = '+') -> List[Tuple[datetime, float]]:
        """Retrieve indicator time series data"""
        try:
            symbol_clean = symbol.replace(' ', '_').replace('&', 'and')
            key = f"indicator:{symbol_clean}:{category}:{indicator}"
            
            result = self.redis_client.execute_command(
                'TS.RANGE', key, start_time, end_time
            )
            
            timeseries_data = []
            for timestamp_ms, value_str in result:
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=IST_TIMEZONE)
                value = float(value_str)
                timeseries_data.append((timestamp, value))
            
            return timeseries_data
            
        except Exception as e:
            logger.error(f"Error retrieving timeseries for {symbol}:{indicator}: {e}")
            return []
    
    def get_latest_indicator_value(self, symbol: str, indicator: str, category: str) -> Optional[Tuple[datetime, float]]:
        """Get the latest value for a specific indicator"""
        try:
            symbol_clean = symbol.replace(' ', '_').replace('&', 'and')
            key = f"indicator:{symbol_clean}:{category}:{indicator}"
            
            result = self.redis_client.execute_command('TS.GET', key)
            if result and len(result) >= 2:
                timestamp_ms, value_str = result
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=IST_TIMEZONE)
                value = float(value_str)
                return (timestamp, value)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest value for {symbol}:{indicator}: {e}")
            return None
    
    def get_symbol_indicators_summary(self, symbol: str) -> Dict[str, Any]:
        """Get summary of all indicators for a symbol"""
        try:
            symbol_clean = symbol.replace(' ', '_').replace('&', 'and')
            pattern = f"indicator:{symbol_clean}:*"
            keys = self.redis_client.keys(pattern)
            
            summary = {
                'symbol': symbol,
                'total_indicators': len(keys),
                'categories': {},
                'latest_update': None
            }
            
            for key in keys:
                try:
                    # Parse key: indicator:{symbol}:{category}:{indicator_name}
                    parts = key.split(':')
                    if len(parts) >= 4:
                        category = parts[2]
                        indicator_name = parts[3]
                        
                        if category not in summary['categories']:
                            summary['categories'][category] = []
                        
                        # Get latest value
                        result = self.redis_client.execute_command('TS.GET', key)
                        if result and len(result) >= 2:
                            timestamp_ms, value_str = result
                            timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=IST_TIMEZONE)
                            
                            summary['categories'][category].append({
                                'name': indicator_name,
                                'latest_value': float(value_str),
                                'latest_timestamp': timestamp.isoformat()
                            })
                            
                            # Track latest update time
                            if summary['latest_update'] is None or timestamp > summary['latest_update']:
                                summary['latest_update'] = timestamp
                
                except Exception as e:
                    logger.debug(f"Error processing key {key}: {e}")
                    continue
            
            if summary['latest_update']:
                summary['latest_update'] = summary['latest_update'].isoformat()
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting summary for {symbol}: {e}")
            return {'symbol': symbol, 'error': str(e)}

class LiveTechnicalIndicatorSystem:
    """Main system for live technical indicator calculation and TimeSeries storage"""
    
    def __init__(self, symbols_path: str = None):
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Initialize components
        self.config = IndicatorConfig()
        self.data_monitor = RedisDataMonitor(source_db=0, target_db=2)
        self.calculator = TechnicalIndicatorCalculator(self.config)
        self.storage = TechnicalIndicatorTimeSeriesStorage(self.data_monitor.target_redis)
        
        # Load symbols if path provided
        self.symbols = self._load_symbols(symbols_path) if symbols_path else []
        
        # Performance tracking
        self.performance_stats = {
            'calculations_per_minute': 0,
            'avg_calculation_time': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'total_indicators_stored': 0,
            'timeseries_created': 0,
            'start_time': datetime.now(self.ist_timezone)
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("🚀 Live Technical Indicator TimeSeries System initialized")
        print("🚀 Live Technical Indicator TimeSeries System ready")
    
    def _load_symbols(self, symbols_path: str) -> List[str]:
        """Load symbols from JSON file"""
        try:
            with open(symbols_path, 'r') as f:
                data = json.load(f)
            
            symbols = []
            for symbol_data in data.get('symbols', []):
                # Extract clean symbol name
                symbol_name = symbol_data['name'].replace(' ', '_').replace('&', 'and')
                symbols.append(symbol_name)
            
            logger.info(f"Loaded {len(symbols)} symbols from {symbols_path}")
            return symbols
            
        except Exception as e:
            logger.warning(f"Failed to load symbols from {symbols_path}: {e}")
            return []
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        SHUTDOWN_FLAG.set()
    
    def calculate_indicators_for_symbol(self, symbol: str) -> bool:
        """Calculate and store indicators for a single symbol"""
        start_time = time_module.time()
        
        try:
            # Get OHLCV data
            symbol_data = self.data_monitor.get_symbol_ohlcv_data(symbol)
            
            if not symbol_data:
                logger.warning(f"⚠️ Skipping {symbol} - insufficient or missing OHLCV data")
                self.data_monitor.stats['missing_data_flags'] += 1
                return False
            
            # Calculate indicators
            indicator_points = self.calculator.calculate_all_indicators(symbol_data)
            
            if not indicator_points:
                logger.warning(f"⚠️ No indicators calculated for {symbol}")
                return False
            
            # Store indicators in TimeSeries
            storage_results = self.storage.store_indicator_points(indicator_points)
            
            if storage_results['stored'] > 0:
                calculation_time = time_module.time() - start_time
                self.data_monitor.stats['successful_calculations'] += 1
                self.data_monitor.stats['total_calculations'] += 1
                self.data_monitor.stats['indicators_stored'] += storage_results['stored']
                self.data_monitor.stats['timeseries_created'] += storage_results['timeseries_created']
                
                # Update performance stats
                self.performance_stats['successful_updates'] += 1
                self.performance_stats['total_indicators_stored'] += storage_results['stored']
                self.performance_stats['timeseries_created'] += storage_results['timeseries_created']
                self.performance_stats['avg_calculation_time'] = (
                    (self.performance_stats['avg_calculation_time'] * 
                     (self.performance_stats['successful_updates'] - 1) + calculation_time) /
                    self.performance_stats['successful_updates']
                )
                
                logger.debug(f"✅ Updated {storage_results['stored']} indicators for {symbol} ({calculation_time:.3f}s)")
                return True
            else:
                self.data_monitor.stats['failed_calculations'] += 1
                self.performance_stats['failed_updates'] += 1
                return False
                
        except Exception as e:
            logger.error(f"❌ Error processing {symbol}: {e}")
            self.data_monitor.stats['failed_calculations'] += 1
            self.performance_stats['failed_updates'] += 1
            return False
    
    def run_initial_calculation(self, max_workers: int = 3) -> Dict[str, Any]:
        """Run initial calculation for all available symbols"""
        logger.info("🔄 Starting initial technical indicator calculation...")
        print("🔄 Calculating initial technical indicators TimeSeries for all symbols...")
        
        # Get available symbols from Redis
        available_symbols = self.data_monitor.get_available_symbols()
        
        if not available_symbols:
            logger.warning("⚠️ No symbols found in Redis DB0")
            print("⚠️ No symbols found in Redis DB0 - make sure OHLCV data is available")
            return {'success': False, 'message': 'No symbols available'}
        
        logger.info(f"📊 Processing {len(available_symbols)} symbols")
        print(f"📊 Processing {len(available_symbols)} symbols...")
        
        successful = 0
        failed = 0
        missing_data = 0
        
        # Process symbols concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.calculate_indicators_for_symbol, symbol): symbol
                for symbol in available_symbols
            }
            
            for future in as_completed(future_to_symbol):
                if SHUTDOWN_FLAG.is_set():
                    break
                
                symbol = future_to_symbol[future]
                try:
                    success = future.result()
                    if success:
                        successful += 1
                    else:
                        failed += 1
                        missing_data += 1
                        
                except Exception as e:
                    logger.error(f"Future execution error for {symbol}: {e}")
                    failed += 1
                
                # Progress update
                completed = successful + failed
                if completed % 10 == 0 or completed == len(available_symbols):
                    progress = (completed / len(available_symbols)) * 100
                    print(f"Progress: {completed}/{len(available_symbols)} ({progress:.1f}%) - "
                          f"Success: {successful}, Failed: {failed}")
                
                # Small delay to be respectful
                time_module.sleep(0.1)
        
        # Update monitoring stats
        self.data_monitor.stats['symbols_monitored'] = len(available_symbols)
        self.data_monitor.stats['last_update'] = datetime.now(self.ist_timezone)
        
        # Print summary
        print("\n" + "="*80)
        print("INITIAL TECHNICAL INDICATOR TIMESERIES CALCULATION SUMMARY")
        print("="*80)
        print(f"Total symbols processed: {len(available_symbols)}")
        print(f"Successful calculations: {successful}")
        print(f"Failed calculations: {failed}")
        print(f"Missing/insufficient data: {missing_data}")
        print(f"Success rate: {(successful/len(available_symbols)*100):.1f}%" if available_symbols else "0%")
        print(f"Total indicators stored: {self.performance_stats['total_indicators_stored']:,}")
        print(f"TimeSeries created: {self.performance_stats['timeseries_created']:,}")
        print(f"Average calculation time: {self.performance_stats['avg_calculation_time']:.3f}s")
        print("="*80)
        
        return {
            'success': True,
            'total_symbols': len(available_symbols),
            'successful': successful,
            'failed': failed,
            'missing_data': missing_data,
            'success_rate': (successful/len(available_symbols)*100) if available_symbols else 0,
            'indicators_stored': self.performance_stats['total_indicators_stored'],
            'timeseries_created': self.performance_stats['timeseries_created']
        }
    
    def run_continuous_monitoring(self, check_interval: int = 30):
        """Run continuous monitoring for new data and update indicators"""
        logger.info("🔄 Starting continuous technical indicator TimeSeries monitoring...")
        print("🔄 Starting continuous monitoring for new OHLCV data...")
        print(f"Check interval: {check_interval} seconds")
        print("Press Ctrl+C to stop...")
        
        cycle_count = 0
        last_stats_time = time_module.time()
        
        while not SHUTDOWN_FLAG.is_set():
            try:
                cycle_start = time_module.time()
                cycle_count += 1
                
                logger.info(f"📊 Monitoring cycle {cycle_count}")
                
                # Get available symbols
                available_symbols = self.data_monitor.get_available_symbols()
                
                if not available_symbols:
                    logger.warning("⚠️ No symbols available, waiting...")
                    time_module.sleep(check_interval)
                    continue
                
                # Check for new data and update indicators
                updated_symbols = []
                
                for symbol in available_symbols:
                    if SHUTDOWN_FLAG.is_set():
                        break
                    
                    if self.data_monitor.has_new_data(symbol):
                        logger.info(f"🆕 New data detected for {symbol}")
                        success = self.calculate_indicators_for_symbol(symbol)
                        
                        if success:
                            updated_symbols.append(symbol)
                        
                        # Small delay between symbols
                        time_module.sleep(0.05)
                
                # Update performance tracking
                cycle_time = time_module.time() - cycle_start
                
                if updated_symbols:
                    logger.info(f"✅ Updated indicators for {len(updated_symbols)} symbols in {cycle_time:.2f}s")
                    print(f"✅ Cycle {cycle_count}: Updated {len(updated_symbols)} symbols "
                          f"({', '.join(updated_symbols[:5])}" +
                          (f" +{len(updated_symbols)-5} more" if len(updated_symbols) > 5 else "") + ")")
                else:
                    logger.debug(f"💤 No new data detected in cycle {cycle_count}")
                    if cycle_count % 10 == 0:  # Print status every 10 cycles
                        print(f"💤 Cycle {cycle_count}: No new data detected")
                
                # Print stats every 5 minutes
                if time_module.time() - last_stats_time > 300:
                    self._print_monitoring_stats()
                    last_stats_time = time_module.time()
                
                # Calculate sleep time
                elapsed = time_module.time() - cycle_start
                sleep_time = max(0, check_interval - elapsed)
                
                # Sleep with interrupt checking
                sleep_start = time_module.time()
                while time_module.time() - sleep_start < sleep_time and not SHUTDOWN_FLAG.is_set():
                    time_module.sleep(min(1.0, sleep_time - (time_module.time() - sleep_start)))
                
            except Exception as e:
                logger.error(f"❌ Error in monitoring cycle {cycle_count}: {e}")
                time_module.sleep(5)  # Brief pause before retry
        
        logger.info("🛑 Continuous monitoring stopped")
        print("🛑 Technical Indicator TimeSeries monitoring stopped")
        self._print_final_stats()
    
    def _print_monitoring_stats(self):
        """Print current monitoring statistics"""
        runtime = datetime.now(self.ist_timezone) - self.data_monitor.stats['start_time']
        
        print("\n" + "="*60)
        print("TECHNICAL INDICATOR TIMESERIES MONITORING STATS")
        print("="*60)
        print(f"Runtime: {runtime}")
        print(f"Symbols monitored: {self.data_monitor.stats['symbols_monitored']}")
        print(f"Total calculations: {self.data_monitor.stats['total_calculations']}")
        print(f"Successful calculations: {self.data_monitor.stats['successful_calculations']}")
        print(f"Failed calculations: {self.data_monitor.stats['failed_calculations']}")
        print(f"Total indicators stored: {self.data_monitor.stats['indicators_stored']:,}")
        print(f"TimeSeries keys created: {self.data_monitor.stats['timeseries_created']:,}")
        print(f"Missing data flags: {self.data_monitor.stats['missing_data_flags']}")
        
        if self.data_monitor.stats['total_calculations'] > 0:
            success_rate = (self.data_monitor.stats['successful_calculations'] / 
                          self.data_monitor.stats['total_calculations'] * 100)
            print(f"Success rate: {success_rate:.1f}%")
        
        print(f"Avg calculation time: {self.performance_stats['avg_calculation_time']:.3f}s")
        
        if self.data_monitor.stats['last_update']:
            print(f"Last update: {self.data_monitor.stats['last_update'].strftime('%H:%M:%S')}")
        
        print("="*60)
    
    def _print_final_stats(self):
        """Print final statistics on shutdown"""
        runtime = datetime.now(self.ist_timezone) - self.data_monitor.stats['start_time']
        
        print("\n" + "="*80)
        print("FINAL TECHNICAL INDICATOR TIMESERIES SYSTEM STATISTICS")
        print("="*80)
        print(f"Total runtime: {runtime}")
        print(f"Symbols processed: {self.data_monitor.stats['symbols_monitored']}")
        print(f"Total indicator calculations: {self.data_monitor.stats['total_calculations']}")
        print(f"Successful calculations: {self.data_monitor.stats['successful_calculations']}")
        print(f"Failed calculations: {self.data_monitor.stats['failed_calculations']}")
        print(f"Total indicators stored: {self.data_monitor.stats['indicators_stored']:,}")
        print(f"TimeSeries keys created: {self.data_monitor.stats['timeseries_created']:,}")
        print(f"Data quality issues flagged: {self.data_monitor.stats['missing_data_flags']}")
        
        if self.data_monitor.stats['total_calculations'] > 0:
            success_rate = (self.data_monitor.stats['successful_calculations'] / 
                          self.data_monitor.stats['total_calculations'] * 100)
            print(f"Overall success rate: {success_rate:.1f}%")
        
        print(f"Average calculation time: {self.performance_stats['avg_calculation_time']:.3f}s")
        print(f"Log file: {LOG_FILENAME}")
        print("="*80)
    
    def get_trading_signals(self, symbol: str) -> Dict[str, Any]:
        """Get current trading signals for a symbol"""
        try:
            signals = {
                'symbol': symbol,
                'timestamp': datetime.now(self.ist_timezone).isoformat(),
                'trend': {},
                'momentum': {},
                'volume': {},
                'volatility': {},
                'overall_signal': 'neutral'
            }
            
            # Get latest indicator values
            rsi_data = self.storage.get_latest_indicator_value(symbol, 'rsi', 'momentum')
            sma_20_data = self.storage.get_latest_indicator_value(symbol, 'sma_20', 'trend')
            sma_50_data = self.storage.get_latest_indicator_value(symbol, 'sma_50', 'trend')
            volume_ratio_data = self.storage.get_latest_indicator_value(symbol, 'volume_ratio', 'volume')
            atr_percent_data = self.storage.get_latest_indicator_value(symbol, 'atr_percent', 'volatility')
            
            # Analyze momentum
            if rsi_data:
                rsi_value = rsi_data[1]
                signals['momentum']['rsi'] = rsi_value
                signals['momentum']['rsi_signal'] = (
                    'oversold' if rsi_value < 30 else
                    'overbought' if rsi_value > 70 else
                    'neutral'
                )
            
            # Analyze trend
            if sma_20_data and sma_50_data:
                sma_20 = sma_20_data[1]
                sma_50 = sma_50_data[1]
                signals['trend']['sma_20'] = sma_20
                signals['trend']['sma_50'] = sma_50
                signals['trend']['trend_signal'] = 'bullish' if sma_20 > sma_50 else 'bearish'
            
            # Analyze volume
            if volume_ratio_data:
                vol_ratio = volume_ratio_data[1]
                signals['volume']['volume_ratio'] = vol_ratio
                signals['volume']['volume_signal'] = (
                    'high' if vol_ratio > 1.5 else
                    'low' if vol_ratio < 0.5 else
                    'normal'
                )
            
            # Analyze volatility
            if atr_percent_data:
                atr_pct = atr_percent_data[1]
                signals['volatility']['atr_percent'] = atr_pct
                signals['volatility']['volatility_signal'] = (
                    'high' if atr_pct > 3.0 else
                    'low' if atr_pct < 1.0 else
                    'normal'
                )
            
            return signals
            
        except Exception as e:
            logger.error(f"Error getting trading signals for {symbol}: {e}")
            return {'symbol': symbol, 'error': str(e)}

def main():
    """Main function for live technical indicator TimeSeries system"""
    # Look for symbols.json in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    symbols_path = os.path.join(script_dir, "symbols.json")
    
    try:
        # Initialize the technical indicator system
        indicator_system = LiveTechnicalIndicatorSystem(symbols_path)
        
        print("🚀 CUPCAKE Live Technical Indicator TimeSeries System")
        print("="*60)
        print("Features:")
        print("• Real-time monitoring of Redis DB0 for new OHLCV data")
        print("• Comprehensive technical indicators using TA-Lib")
        print("• TIME SERIES STORAGE with timestamps in Redis DB2")
        print("• Historical indicator analysis capabilities")
        print("• Trend, Momentum, Volume, and Volatility indicators")
        print("• Optimized for day trading requirements")
        print("• Automatic handling of missing data with flagging")
        print("• Professional TimeSeries data structure")
        print("="*60)
        
        # Step 1: Run initial calculation for all symbols
        print("\n🔄 STEP 1: Initial TimeSeries calculation for all available symbols...")
        initial_result = indicator_system.run_initial_calculation(max_workers=3)
        
        if not initial_result['success']:
            print("❌ Initial calculation failed - exiting")
            return {'success': False, 'error': 'Initial calculation failed'}
        
        print(f"✅ Initial calculation complete: {initial_result['successful']}/{initial_result['total_symbols']} symbols")
        print(f"📊 Created {initial_result['timeseries_created']:,} TimeSeries with {initial_result['indicators_stored']:,} data points")
        
        # Step 2: Start continuous monitoring
        print("\n🔄 STEP 2: Starting continuous TimeSeries monitoring...")
        indicator_system.run_continuous_monitoring(check_interval=30)
        
        return {'success': True}
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\n⚠️ Process interrupted by user")
        return {'success': True}  # Graceful shutdown is success
        
    except Exception as e:
        logger.error(f"Fatal error in technical indicator TimeSeries system: {e}", exc_info=True)
        print(f"\n❌ Fatal error: {e}")
        print(f"Check log file for details: {LOG_FILENAME}")
        return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    result = main()
    
    # Exit with appropriate code
    exit_code = 0 if result.get('success', False) else 1
    exit(exit_code)