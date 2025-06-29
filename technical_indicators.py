# Enhanced Technical Indicators with Rolling Window Cache and TA-Lib
# Optimized for performance with in-memory data management

import os
import redis
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Tuple, Optional
import json
import logging
from collections import defaultdict
import threading
import time

# Import TA-Lib for optimized calculations
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logging.warning("TA-Lib not available. Install with: pip install TA-Lib")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RollingWindowCache:
    """In-memory rolling window cache for OHLCV data"""
    def __init__(self, window_size: int = 250):
        self.window_size = window_size
        self.data = {}  # symbol -> DataFrame
        self.lock = threading.RLock()
        self.last_update = {}  # symbol -> last_timestamp
        
    def initialize(self, symbol: str, df: pd.DataFrame):
        """Initialize cache with historical data"""
        with self.lock:
            # Keep only the latest window_size rows
            self.data[symbol] = df.tail(self.window_size).copy()
            if not df.empty:
                self.last_update[symbol] = df.index[-1]
            logger.info(f"Initialized cache for {symbol} with {len(self.data[symbol])} rows")
    
    def append_candle(self, symbol: str, timestamp: datetime, ohlcv: Dict[str, float]):
        """Append new candle and maintain window size"""
        with self.lock:
            if symbol not in self.data:
                # Create new DataFrame if symbol not in cache
                self.data[symbol] = pd.DataFrame()
            
            # Create new row
            new_row = pd.DataFrame([ohlcv], index=[timestamp])
            
            # Append and maintain window size
            self.data[symbol] = pd.concat([self.data[symbol], new_row])
            if len(self.data[symbol]) > self.window_size:
                self.data[symbol] = self.data[symbol].iloc[-self.window_size:]
            
            self.last_update[symbol] = timestamp
    
    def get_data(self, symbol: str) -> pd.DataFrame:
        """Get cached data for symbol"""
        with self.lock:
            if symbol in self.data:
                return self.data[symbol].copy()
            return pd.DataFrame()
    
    def needs_refresh(self, symbol: str, current_time: datetime) -> bool:
        """Check if cache needs refresh (data older than 2 minutes)"""
        with self.lock:
            if symbol not in self.last_update:
                return True
            time_diff = current_time - self.last_update[symbol]
            return time_diff > timedelta(minutes=2)

class EnhancedTechnicalIndicators:
    def __init__(self, redis_host=None, redis_port=None, redis_db=0):
        """Initialize enhanced technical indicators with rolling window cache"""
        # Use environment variables if not provided
        if redis_host is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
        if redis_port is None:
            redis_port = int(os.environ.get('REDIS_PORT', '6379'))
            
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db,
            decode_responses=True
        )
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Initialize rolling window cache
        self.cache = RollingWindowCache(window_size=250)
        
        # Initialize trajectory signal generator - Import only when needed to avoid circular dependency
        self.trajectory_generator = None
        
        # Store recent indicator calculations for trend analysis
        self.indicator_history = defaultdict(lambda: [])
        
        # Performance metrics
        self.perf_metrics = {
            'redis_queries': 0,
            'cache_hits': 0,
            'calculations': 0,
            'total_time': 0
        }
        
        logger.info("Enhanced Technical Indicators initialized with Rolling Window Cache")
        
    def _get_trajectory_generator(self):
        """Lazy initialization of trajectory generator to avoid circular imports"""
        if self.trajectory_generator is None:
            try:
                from enhanced_signal_generator import TrajectorySignalGenerator
                self.trajectory_generator = TrajectorySignalGenerator(
                    redis_host=self.redis_client.connection_pool.connection_kwargs['host'],
                    redis_port=self.redis_client.connection_pool.connection_kwargs['port'],
                    redis_db=self.redis_client.connection_pool.connection_kwargs['db']
                )
            except ImportError as e:
                logger.warning(f"Could not import TrajectorySignalGenerator: {e}")
                self.trajectory_generator = None
        return self.trajectory_generator
    
    def _load_initial_data(self, symbol: str):
        """Load initial data from Redis only once"""
        try:
            symbol_clean = symbol.replace(' ', '_').replace('&', 'and')
            
            # Get current timestamp
            now_ts = int(datetime.now().timestamp() * 1000)
            
            # Calculate start timestamp (250 minutes ago)
            start_ts = now_ts - (250 * 60 * 1000)
            
            # Fetch data for each OHLCV component
            data = {}
            for data_type in ['open', 'high', 'low', 'close', 'volume']:
                key = f"stock:{symbol_clean}:{data_type}"
                try:
                    result = self.redis_client.execute_command(
                        'TS.RANGE', key, start_ts, now_ts
                    )
                    
                    if result:
                        timestamps, values = zip(*result)
                        data[data_type] = {
                            'timestamps': [datetime.fromtimestamp(ts/1000, tz=self.ist_timezone) for ts in timestamps],
                            'values': [float(v) for v in values]
                        }
                    else:
                        logger.warning(f"No data found for {key}")
                        return pd.DataFrame()
                        
                except Exception as e:
                    logger.error(f"Error fetching {data_type} for {symbol}: {e}")
                    return pd.DataFrame()
            
            # Create DataFrame
            if not data or not data['close']['values']:
                return pd.DataFrame()
            
            df = pd.DataFrame({
                'timestamp': data['close']['timestamps'],
                'open': data['open']['values'],
                'high': data['high']['values'], 
                'low': data['low']['values'],
                'close': data['close']['values'],
                'volume': data['volume']['values']
            })
            
            df = df.sort_values('timestamp').set_index('timestamp')
            
            self.perf_metrics['redis_queries'] += 5  # One query per OHLCV component
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading initial data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_ohlcv_data(self, symbol: str, periods: int = 250) -> pd.DataFrame:
        """Get OHLCV data using rolling window cache"""
        try:
            # Check cache first
            df = self.cache.get_data(symbol)
            
            # If cache is empty or stale, load from Redis
            if df.empty or self.cache.needs_refresh(symbol, datetime.now(self.ist_timezone)):
                logger.info(f"Cache miss for {symbol}, loading from Redis")
                df = self._load_initial_data(symbol)
                if not df.empty:
                    self.cache.initialize(symbol, df)
            else:
                self.perf_metrics['cache_hits'] += 1
                logger.debug(f"Cache hit for {symbol}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting OHLCV data for {symbol}: {e}")
            return pd.DataFrame()
    
    def update_cache_with_new_candle(self, symbol: str, timestamp: datetime, ohlcv: Dict[str, float]):
        """Update cache with new candle from live feed"""
        try:
            self.cache.append_candle(symbol, timestamp, ohlcv)
            logger.debug(f"Updated cache for {symbol} with new candle at {timestamp}")
        except Exception as e:
            logger.error(f"Error updating cache for {symbol}: {e}")
    
    # Optimized indicator calculations using TA-Lib
    def calculate_sma(self, data: pd.Series, window: int) -> pd.Series:
        """Calculate Simple Moving Average using TA-Lib if available"""
        if TALIB_AVAILABLE and len(data) >= window:
            return pd.Series(talib.SMA(data.values, timeperiod=window), index=data.index)
        else:
            return data.rolling(window=window, min_periods=1).mean()
    
    def calculate_ema(self, data: pd.Series, window: int) -> pd.Series:
        """Calculate Exponential Moving Average using TA-Lib if available"""
        if TALIB_AVAILABLE and len(data) >= window:
            return pd.Series(talib.EMA(data.values, timeperiod=window), index=data.index)
        else:
            return data.ewm(span=window, adjust=False).mean()
    
    def calculate_rsi(self, data: pd.Series, window: int = 14) -> pd.Series:
        """Calculate Relative Strength Index using TA-Lib if available"""
        if TALIB_AVAILABLE and len(data) > window:
            return pd.Series(talib.RSI(data.values, timeperiod=window), index=data.index)
        else:
            # Fallback to manual calculation
            delta = data.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
    
    def calculate_macd(self, data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
        """Calculate MACD using TA-Lib if available"""
        if TALIB_AVAILABLE and len(data) > slow:
            macd_line, signal_line, histogram = talib.MACD(data.values, 
                                                           fastperiod=fast, 
                                                           slowperiod=slow, 
                                                           signalperiod=signal)
            return {
                'macd': pd.Series(macd_line, index=data.index),
                'signal': pd.Series(signal_line, index=data.index),
                'histogram': pd.Series(histogram, index=data.index)
            }
        else:
            # Fallback to manual calculation
            ema_fast = self.calculate_ema(data, fast)
            ema_slow = self.calculate_ema(data, slow)
            macd_line = ema_fast - ema_slow
            signal_line = self.calculate_ema(macd_line, signal)
            histogram = macd_line - signal_line
            return {
                'macd': macd_line,
                'signal': signal_line,
                'histogram': histogram
            }
    
    def calculate_bollinger_bands(self, data: pd.Series, window: int = 20, std_dev: int = 2) -> Dict:
        """Calculate Bollinger Bands using TA-Lib if available"""
        if TALIB_AVAILABLE and len(data) >= window:
            upper, middle, lower = talib.BBANDS(data.values, 
                                               timeperiod=window, 
                                               nbdevup=std_dev, 
                                               nbdevdn=std_dev)
            return {
                'upper': pd.Series(upper, index=data.index),
                'middle': pd.Series(middle, index=data.index),
                'lower': pd.Series(lower, index=data.index)
            }
        else:
            # Fallback to manual calculation
            sma = self.calculate_sma(data, window)
            std = data.rolling(window=window).std()
            upper_band = sma + (std * std_dev)
            lower_band = sma - (std * std_dev)
            return {
                'upper': upper_band,
                'middle': sma,
                'lower': lower_band
            }
    
    def calculate_atr(self, df: pd.DataFrame, window: int = 14) -> pd.Series:
        """Calculate Average True Range using TA-Lib if available"""
        if TALIB_AVAILABLE and len(df) > window:
            return pd.Series(talib.ATR(df['high'].values, 
                                      df['low'].values, 
                                      df['close'].values, 
                                      timeperiod=window), 
                            index=df.index)
        else:
            # Fallback to manual calculation
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)
            return true_range.rolling(window=window).mean()

    def calculate_adx(self, df: pd.DataFrame, window: int = 14) -> pd.Series:
        """Calculate Average Directional Index using TA-Lib if available"""
        if TALIB_AVAILABLE and len(df) > window * 2:
            return pd.Series(talib.ADX(df['high'].values,
                                       df['low'].values,
                                       df['close'].values,
                                       timeperiod=window),
                             index=df.index)
        else:
            # Fallback to manual calculation
            plus_dm = df['high'].diff()
            minus_dm = df['low'].diff()
            plus_dm[plus_dm < 0] = 0
            minus_dm[minus_dm > 0] = 0
            
            tr1 = pd.DataFrame(df['high'] - df['low'])
            tr2 = pd.DataFrame(abs(df['high'] - df['close'].shift(1)))
            tr3 = pd.DataFrame(abs(df['low'] - df['close'].shift(1)))
            frames = [tr1, tr2, tr3]
            tr = pd.concat(frames, axis = 1, join = 'inner').max(axis = 1)
            atr = tr.rolling(window).mean()
            
            plus_di = 100 * (plus_dm.ewm(alpha = 1/window).mean() / atr)
            minus_di = abs(100 * (minus_dm.ewm(alpha = 1/window).mean() / atr))
            dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
            adx = ((dx.shift(1) * (window - 1)) + dx) / window
            return adx.ewm(alpha = 1/window).mean()

    def get_market_regime(self, symbol: str, adx_threshold_trending: float = 25, adx_threshold_weak: float = 20) -> str:
        """
        Determines the market regime (TRENDING, RANGING, WEAK) based on ADX.
        ADX values:
        - Above 25: Strong Trend
        - 20-25: Emerging Trend / Weak Trend
        - Below 20: Ranging / No Trend
        """
        df = self.get_ohlcv_data(symbol)
        if df.empty:
            return "UNKNOWN"

        adx_series = self.calculate_adx(df)
        if adx_series.empty or adx_series.iloc[-1] is None or pd.isna(adx_series.iloc[-1]):
            return "UNKNOWN"

        current_adx = adx_series.iloc[-1]

        if current_adx > adx_threshold_trending:
            return "TRENDING"
        elif current_adx < adx_threshold_weak:
            return "RANGING"
        else:
            return "WEAK"

    def get_adaptive_rsi_levels(self, symbol: str, periods: int = 252) -> Dict:
        """Replace hardcoded 30/70 with percentile-based levels for RSI"""
        df = self.get_ohlcv_data(symbol, periods=periods)  # Use existing method
        if df.empty or len(df) < periods:
            return {'oversold': 30, 'overbought': 70}  # Fallback
        
        rsi_history = self.calculate_rsi(df['close'])
        # Ensure we have enough data for meaningful quantiles
        if len(rsi_history.dropna()) < periods * 0.8: # At least 80% of periods
            return {'oversold': 30, 'overbought': 70} # Fallback

        return {
            'oversold': rsi_history.quantile(0.1),    # 10th percentile
            'overbought': rsi_history.quantile(0.9)   # 90th percentile
        }

    def get_adaptive_volume_threshold(self, symbol: str, periods: int = 252) -> Dict:
        """Calculate adaptive volume thresholds based on historical percentiles."""
        df = self.get_ohlcv_data(symbol, periods=periods)
        if df.empty or len(df) < periods:
            return {'high_volume': 1.5, 'low_volume': 0.5} # Fallback

        volume_ratios = (df['volume'] / self.calculate_sma(df['volume'], 20)).dropna()
        if len(volume_ratios) < periods * 0.8:
            return {'high_volume': 1.5, 'low_volume': 0.5} # Fallback

        return {
            'high_volume': volume_ratios.quantile(0.9), # 90th percentile
            'low_volume': volume_ratios.quantile(0.1)  # 10th percentile
        }

    def _find_swing_points(self, df: pd.DataFrame, window: int = 10) -> Dict:
        """Find recent significant swing highs and lows."""
        swing_high = df['high'].rolling(window=window, center=True).max().iloc[-window]
        swing_low = df['low'].rolling(window=window, center=True).min().iloc[-window]
        return {'swing_high': swing_high, 'swing_low': swing_low}
    
    def calculate_volume_indicators(self, df: pd.DataFrame) -> Dict:
        """Calculate Volume-based indicators using TA-Lib where possible"""
        # Volume Moving Average
        volume_sma_20 = self.calculate_sma(df['volume'], 20)
        
        # Volume Rate of Change
        if TALIB_AVAILABLE and len(df) > 1:
            volume_roc = pd.Series(talib.ROC(df['volume'].values, timeperiod=1), index=df.index)
        else:
            volume_roc = df['volume'].pct_change(periods=1) * 100
        
        # On Balance Volume (OBV)
        if TALIB_AVAILABLE:
            obv = pd.Series(talib.OBV(df['close'].values, df['volume'].values), index=df.index)
        else:
            # Manual OBV calculation
            obv = []
            obv_val = 0
            for i in range(len(df)):
                if i == 0:
                    obv_val = df.iloc[i]['volume']
                else:
                    if df.iloc[i]['close'] > df.iloc[i-1]['close']:
                        obv_val += df.iloc[i]['volume']
                    elif df.iloc[i]['close'] < df.iloc[i-1]['close']:
                        obv_val -= df.iloc[i]['volume']
                obv.append(obv_val)
            obv = pd.Series(obv, index=df.index)
        
        # Volume Weighted Average Price (VWAP) - Not in TA-Lib
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        cumulative_typical_price_volume = (typical_price * df['volume']).cumsum()
        cumulative_volume = df['volume'].cumsum()
        vwap = cumulative_typical_price_volume / cumulative_volume
        
        return {
            'volume_sma_20': volume_sma_20,
            'volume_roc': volume_roc,
            'obv': obv,
            'vwap': vwap,
            'volume_ratio': df['volume'] / volume_sma_20
        }
    
    def calculate_all_indicators(self, symbol: str) -> Dict:
        """Calculate all technical indicators with optimized performance"""
        try:
            start_time = time.time()
            
            # Get data from cache
            df = self.get_ohlcv_data(symbol)
            
            if df.empty:
                logger.warning(f"No data available for {symbol}")
                return {}
            
            # Price-based indicators (using TA-Lib)
            sma_5 = self.calculate_sma(df['close'], 5)
            sma_10 = self.calculate_sma(df['close'], 10)
            sma_20 = self.calculate_sma(df['close'], 20)
            sma_50 = self.calculate_sma(df['close'], 50)
            
            ema_5 = self.calculate_ema(df['close'], 5)
            ema_10 = self.calculate_ema(df['close'], 10)
            ema_20 = self.calculate_ema(df['close'], 20)
            
            rsi = self.calculate_rsi(df['close'])
            macd = self.calculate_macd(df['close'])
            bollinger = self.calculate_bollinger_bands(df['close'])
            atr = self.calculate_atr(df)
            adx = self.calculate_adx(df)
            
            # Volume indicators
            volume_indicators = self.calculate_volume_indicators(df)
            
            # Current values (latest)
            current_idx = -1
            current_price = df.iloc[current_idx]['close']

            # Find swing points for risk management
            swing_points = self._find_swing_points(df)
            
            indicators = {
                'symbol': symbol,
                'timestamp': df.index[current_idx].isoformat(),
                'current_price': current_price,
                'ohlcv': {
                    'open': df.iloc[current_idx]['open'],
                    'high': df.iloc[current_idx]['high'],
                    'low': df.iloc[current_idx]['low'],
                    'close': current_price,
                    'volume': df.iloc[current_idx]['volume']
                },
                'sma': {
                    'sma_5': sma_5.iloc[current_idx] if not sma_5.empty else None,
                    'sma_10': sma_10.iloc[current_idx] if not sma_10.empty else None,
                    'sma_20': sma_20.iloc[current_idx] if not sma_20.empty else None,
                    'sma_50': sma_50.iloc[current_idx] if not sma_50.empty else None
                },
                'ema': {
                    'ema_5': ema_5.iloc[current_idx] if not ema_5.empty else None,
                    'ema_10': ema_10.iloc[current_idx] if not ema_10.empty else None,
                    'ema_20': ema_20.iloc[current_idx] if not ema_20.empty else None
                },
                'rsi': rsi.iloc[current_idx] if not rsi.empty else None,
                'macd': {
                    'macd': macd['macd'].iloc[current_idx] if not macd['macd'].empty else None,
                    'signal': macd['signal'].iloc[current_idx] if not macd['signal'].empty else None,
                    'histogram': macd['histogram'].iloc[current_idx] if not macd['histogram'].empty else None
                },
                'bollinger': {
                    'upper': bollinger['upper'].iloc[current_idx] if not bollinger['upper'].empty else None,
                    'middle': bollinger['middle'].iloc[current_idx] if not bollinger['middle'].empty else None,
                    'lower': bollinger['lower'].iloc[current_idx] if not bollinger['lower'].empty else None
                },
                'atr': atr.iloc[current_idx] if not atr.empty else None,
                'adx': adx.iloc[current_idx] if not adx.empty else None,
                'swing_points': swing_points,
                'volume': {
                    'current_volume': df.iloc[current_idx]['volume'],
                    'volume_sma_20': volume_indicators['volume_sma_20'].iloc[current_idx] if not volume_indicators['volume_sma_20'].empty else None,
                    'volume_roc': volume_indicators['volume_roc'].iloc[current_idx] if not volume_indicators['volume_roc'].empty else None,
                    'obv': volume_indicators['obv'].iloc[current_idx] if not volume_indicators['obv'].empty else None,
                    'vwap': volume_indicators['vwap'].iloc[current_idx] if not volume_indicators['vwap'].empty else None,
                    'volume_ratio': volume_indicators['volume_ratio'].iloc[current_idx] if not volume_indicators['volume_ratio'].empty else None
                },
                'market_regime': self.get_market_regime(symbol)
            }
            
            # Store indicator history for trajectory analysis
            self.indicator_history[symbol].append({
                'timestamp': datetime.now(self.ist_timezone),
                'rsi': indicators['rsi'],
                'macd': indicators['macd']['macd'],
                'macd_signal': indicators['macd']['signal'],
                'sma_5': indicators['sma']['sma_5'],
                'sma_20': indicators['sma']['sma_20'],
                'volume_ratio': indicators['volume']['volume_ratio']
            })
            
            # Keep only recent history
            if len(self.indicator_history[symbol]) > 100:
                self.indicator_history[symbol] = self.indicator_history[symbol][-100:]
            
            # Generate basic trading signals (for backward compatibility)
            indicators['signals'] = self.generate_basic_signals(indicators)
            
            # Update performance metrics
            calc_time = time.time() - start_time
            self.perf_metrics['calculations'] += 1
            self.perf_metrics['total_time'] += calc_time
            
            logger.debug(f"Calculated indicators for {symbol} in {calc_time:.3f}s")
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating indicators for {symbol}: {e}")
            return {}
    
    def generate_basic_signals(self, indicators: Dict) -> Dict:
        """
        Generate basic trading signals with risk management and market regime adaptation.
        """
        signals = {
            'trend': 'NEUTRAL',
            'momentum': 'NEUTRAL',
            'volume': 'NORMAL',
            'overall_signal': 'HOLD',
            'confidence': 0.0,
            'reasons': [],
            'risk_management': {
                'stop_loss': None,
                'take_profit': None
            }
        }

        try:
            current_price = indicators['current_price']
            market_regime = indicators['market_regime']
            atr = indicators.get('atr')
            trend_score = 0
            reasons = []

            # --- Market Regime Adaptive Logic ---
            if market_regime == 'TRENDING':
                # In a trending market, prioritize trend-following indicators
                reasons.append('Market Regime: TRENDING')
                # SMA Crossover
                if indicators['sma'].get('sma_5') and indicators['sma'].get('sma_20'):
                    if indicators['sma']['sma_5'] > indicators['sma']['sma_20']:
                        trend_score += 1.5  # Higher weight in trending market
                        reasons.append('SMA5 > SMA20 (Trending Bullish)')
                    else:
                        trend_score -= 1.5
                        reasons.append('SMA5 < SMA20 (Trending Bearish)')
                # MACD
                macd_data = indicators.get('macd', {})
                if macd_data.get('macd') and macd_data.get('signal'):
                    if macd_data['macd'] > macd_data['signal']:
                        trend_score += 1
                        reasons.append('MACD bullish crossover')
                    else:
                        trend_score -= 1
                        reasons.append('MACD bearish crossover')
                # ADX confirmation
                if indicators.get('adx', 0) > 25:
                    trend_score += 0.5  # Small bonus for strong trend
                    reasons.append(f"ADX > 25, confirms trend")

            elif market_regime == 'RANGING':
                # In a ranging market, prioritize oscillators
                reasons.append('Market Regime: RANGING')
                # RSI Analysis
                rsi = indicators.get('rsi')
                if rsi:
                    adaptive_levels = self.get_adaptive_rsi_levels(indicators['symbol'])
                    oversold_level = adaptive_levels['oversold']
                    overbought_level = adaptive_levels['overbought']

                    if rsi < oversold_level:
                        trend_score += 1.5  # Higher weight in ranging market
                        reasons.append(f'RSI oversold ({rsi:.1f} < {oversold_level:.1f})')
                    elif rsi > overbought_level:
                        trend_score -= 1.5
                        reasons.append(f'RSI overbought ({rsi:.1f} > {overbought_level:.1f})')

                # Bollinger Bands
                bollinger = indicators.get('bollinger', {})
                if bollinger.get('lower') and bollinger.get('upper'):
                    if current_price < bollinger['lower']:
                        trend_score += 1
                        reasons.append('Price below lower Bollinger Band')
                    elif current_price > bollinger['upper']:
                        trend_score -= 1
                        reasons.append('Price above upper Bollinger Band')

            else:  # WEAK or UNKNOWN market
                reasons.append('Market Regime: WEAK/UNKNOWN')
                # Use a mix of signals but with lower conviction
                # SMA Crossover
                if indicators['sma'].get('sma_5') and indicators['sma'].get('sma_20'):
                    if indicators['sma']['sma_5'] > indicators['sma']['sma_20']:
                        trend_score += 0.5
                        reasons.append('SMA5 > SMA20 (Weak Bullish)')
                    else:
                        trend_score -= 0.5
                        reasons.append('SMA5 < SMA20 (Weak Bearish)')
                # RSI
                rsi = indicators.get('rsi')
                if rsi:
                    adaptive_levels = self.get_adaptive_rsi_levels(indicators['symbol'])
                    if rsi < adaptive_levels['oversold']:
                        trend_score += 0.5
                        reasons.append(f"RSI oversold ({rsi:.1f})")
                    elif rsi > adaptive_levels['overbought']:
                        trend_score -= 0.5
                        reasons.append(f"RSI overbought ({rsi:.1f})")

            # --- Volume Analysis (applies to all regimes) ---
            volume_data = indicators.get('volume', {})
            if volume_data.get('volume_ratio'):
                adaptive_volume_levels = self.get_adaptive_volume_threshold(indicators['symbol'])
                high_volume_threshold = adaptive_volume_levels['high_volume']
                low_volume_threshold = adaptive_volume_levels['low_volume']

                if volume_data['volume_ratio'] > high_volume_threshold:
                    signals['volume'] = 'HIGH'
                    reasons.append(f"High volume activity ({volume_data['volume_ratio']:.1f} > {high_volume_threshold:.1f})")
                elif volume_data['volume_ratio'] < low_volume_threshold:
                    signals['volume'] = 'LOW'
                    reasons.append(f"Low volume activity ({volume_data['volume_ratio']:.1f} < {low_volume_threshold:.1f})")

            signals['reasons'] = reasons

            # --- Final Signal and Risk Management ---
            # Increased threshold for high confidence signal
            if trend_score >= 2.0:
                signals['trend'] = 'BULLISH'
                signals['overall_signal'] = 'BUY'
                signals['confidence'] = min(trend_score / 4.0, 1.0)
                swing_low = indicators.get('swing_points', {}).get('swing_low')
                swing_high = indicators.get('swing_points', {}).get('swing_high')

                if swing_low and swing_high:
                    signals['risk_management']['suggested_entry'] = round(current_price * 0.998, 2) # Enter on a small dip
                    signals['risk_management']['stop_loss'] = round(swing_low * 0.99, 2) # Below recent low
                    signals['risk_management']['take_profit'] = round(swing_high, 2) # Target recent high

            elif trend_score <= -2.0:
                signals['trend'] = 'BEARISH'
                signals['overall_signal'] = 'SELL'
                signals['confidence'] = min(abs(trend_score) / 4.0, 1.0)
                swing_low = indicators.get('swing_points', {}).get('swing_low')
                swing_high = indicators.get('swing_points', {}).get('swing_high')

                if swing_low and swing_high:
                    signals['risk_management']['suggested_entry'] = round(current_price * 1.002, 2) # Enter on a small rally
                    signals['risk_management']['stop_loss'] = round(swing_high * 1.01, 2) # Above recent high
                    signals['risk_management']['take_profit'] = round(swing_low, 2) # Target recent low
            else:
                signals['overall_signal'] = 'HOLD'
                signals['confidence'] = max(0.3, 1.0 - (abs(trend_score) / 2.0))


            return signals

        except Exception as e:
            logger.error(f"Error generating basic signals: {e}", exc_info=True)
            return signals
    
    def generate_actionable_signals(self, symbol: str, market_data: Dict, orderbook_analysis: Dict = None):
        """Generate actionable signals using trajectory analysis"""
        try:
            # Calculate technical indicators
            technical_indicators = self.calculate_all_indicators(symbol)
            
            if not technical_indicators:
                return []
            
            # Try to get trajectory generator and use it if available
            trajectory_generator = self._get_trajectory_generator()
            if trajectory_generator:
                actionable_signals = trajectory_generator.generate_actionable_signals(
                    symbol, market_data, technical_indicators, orderbook_analysis or {}
                )
                return actionable_signals
            else:
                # Fallback to basic signals
                logger.warning(f"Trajectory generator not available for {symbol}, using basic signals")
                return []
            
        except Exception as e:
            logger.error(f"Error generating actionable signals for {symbol}: {e}")
            return []
    
    def get_signal_analytics(self, symbol: str) -> Dict:
        """Get analytics about signal generation for a symbol"""
        try:
            # Get trajectory generator
            trajectory_generator = self._get_trajectory_generator()
            if not trajectory_generator:
                return {'error': 'Trajectory generator not available'}
            
            # Get recent signals from trajectory generator
            signals = self.generate_actionable_signals(symbol, {'ltp': 0})  # Dummy data for analytics
            
            # Get trajectory analysis
            price_trajectory = trajectory_generator.analyze_price_trajectory(symbol)
            momentum_trajectory = trajectory_generator.analyze_momentum_trajectory(symbol)
            
            # Get performance metrics
            avg_calc_time = self.perf_metrics['total_time'] / max(self.perf_metrics['calculations'], 1)
            cache_hit_rate = self.perf_metrics['cache_hits'] / max(self.perf_metrics['calculations'], 1)
            
            return {
                'symbol': symbol,
                'total_signals': len(signals),
                'price_trajectory': price_trajectory,
                'momentum_trajectory': momentum_trajectory,
                'signal_history_length': len(self.indicator_history[symbol]),
                'performance': {
                    'avg_calculation_time_ms': avg_calc_time * 1000,
                    'cache_hit_rate': cache_hit_rate,
                    'redis_queries': self.perf_metrics['redis_queries'],
                    'total_calculations': self.perf_metrics['calculations'],
                    'using_talib': TALIB_AVAILABLE
                },
                'last_updated': datetime.now(self.ist_timezone).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting signal analytics for {symbol}: {e}")
            return {'error': str(e)}
    
    def bulk_generate_actionable_signals(self, market_data_dict: Dict, orderbook_data_dict: Dict = None):
        """Generate actionable signals for multiple symbols"""
        results = {}
        
        for symbol, market_data in market_data_dict.items():
            try:
                orderbook_data = orderbook_data_dict.get(symbol, {}) if orderbook_data_dict else {}
                signals = self.generate_actionable_signals(symbol, market_data, orderbook_data)
                if signals:
                    results[symbol] = signals
                    
            except Exception as e:
                logger.error(f"Error generating signals for {symbol}: {e}")
                continue
        
        return results

    def validate_signal_quality(self, symbol: str, signal_type: str) -> float:
        """Simple validation using recent historical data"""
        df = self.get_ohlcv_data(symbol, periods=100)
        if df.empty:
            return 0.5  # Default confidence
        
        # Simple validation: how often did similar setups work?
        # This is a placeholder for actual backtesting logic.
        # In a real scenario, you would define what constitutes a "similar setup"
        # and then check if the price moved in the expected direction.
        
        # For demonstration, let's simulate some success rate based on signal type
        if signal_type == 'BUY':
            # Simulate 70% success for buy signals in a simplified manner
            success_rate = 0.7
        elif signal_type == 'SELL':
            # Simulate 60% success for sell signals
            success_rate = 0.6
        else:
            success_rate = 0.5
            
        # You would replace this with actual backtesting logic:
        # similar_setups = self._find_similar_setups(df, signal_type)
        # if len(similar_setups) < 5:
        #     return 0.5
        # success_rate = len([s for s in similar_setups if s['success']]) / len(similar_setups)
        
        return success_rate

# Maintain backward compatibility
class TechnicalIndicators(EnhancedTechnicalIndicators):
    """Backward compatible class that extends EnhancedTechnicalIndicators"""
    pass
