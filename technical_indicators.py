#!/usr/bin/env python3
"""
Ultra-Fast Technical Analyzer for Upstox Trading System
Optimized for maximum performance with:
- Vectorized operations using NumPy
- Parallel processing with multiprocessing
- Smart caching with TTL
- Memory-mapped file I/O
- JIT compilation with Numba
- Optimized data structures
"""

import pandas as pd
import numpy as np
import talib
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path
import warnings
from collections import defaultdict, deque
import threading
from dataclasses import dataclass, asdict
from enum import Enum
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing as mp
from functools import lru_cache, partial
import numba
from numba import jit, njit
import pickle
import mmap

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

# Global constants for performance
CPU_COUNT = mp.cpu_count()
MAX_WORKERS = min(CPU_COUNT * 2, 16)  # Optimal for I/O bound tasks

class SignalType(Enum):
    """Signal types for trading decisions"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    STRONG_BUY = "STRONG_BUY"
    STRONG_SELL = "STRONG_SELL"

@dataclass
class TechnicalSignal:
    """Structure for technical analysis signals"""
    instrument: str
    timestamp: datetime
    signal_type: SignalType
    strength: float
    indicators: Dict[str, float]
    patterns: List[str]
    price: float
    volume: int
    reasoning: str

# JIT-compiled functions for maximum speed
@njit(cache=True, fastmath=True)
def fast_sma(values, period):
    """Ultra-fast Simple Moving Average using Numba JIT"""
    n = len(values)
    result = np.full(n, np.nan)
    
    if n < period:
        return result
    
    # Calculate first SMA
    sum_val = np.sum(values[:period])
    result[period-1] = sum_val / period
    
    # Rolling calculation
    for i in range(period, n):
        sum_val = sum_val - values[i-period] + values[i]
        result[i] = sum_val / period
    
    return result

@njit(cache=True, fastmath=True)
def fast_ema(values, period):
    """Ultra-fast Exponential Moving Average using Numba JIT"""
    n = len(values)
    result = np.full(n, np.nan)
    
    if n == 0:
        return result
    
    alpha = 2.0 / (period + 1.0)
    result[0] = values[0]
    
    for i in range(1, n):
        if not np.isnan(values[i]):
            if np.isnan(result[i-1]):
                result[i] = values[i]
            else:
                result[i] = alpha * values[i] + (1 - alpha) * result[i-1]
        else:
            result[i] = result[i-1]
    
    return result

@njit(cache=True, fastmath=True)
def fast_rsi(values, period=14):
    """Ultra-fast RSI calculation using Numba JIT"""
    n = len(values)
    if n < period + 1:
        return np.full(n, np.nan)
    
    deltas = np.diff(values)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    
    # Calculate initial averages
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    rsi = np.full(n, np.nan)
    
    for i in range(period, n):
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))
        
        # Update averages
        if i < n - 1:
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    return rsi

@njit(cache=True, fastmath=True)
def fast_bollinger_bands(values, period=20, std_dev=2.0):
    """Ultra-fast Bollinger Bands calculation"""
    n = len(values)
    if n < period:
        return np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    
    sma = fast_sma(values, period)
    upper = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    
    for i in range(period-1, n):
        window = values[i-period+1:i+1]
        std = np.std(window)
        upper[i] = sma[i] + (std_dev * std)
        lower[i] = sma[i] - (std_dev * std)
    
    return upper, sma, lower

@njit(cache=True, fastmath=True)
def calculate_signal_strength(rsi, adx, macd_hist, close_prices, bb_upper, bb_lower):
    """Ultra-fast signal strength calculation"""
    latest_idx = -1
    strength = 0.0
    count = 0
    
    # RSI signal
    if not np.isnan(rsi[latest_idx]):
        if rsi[latest_idx] < 30:
            strength += 25.0
        elif rsi[latest_idx] > 70:
            strength -= 25.0
        count += 1
    
    # ADX trend strength  
    if not np.isnan(adx[latest_idx]) and adx[latest_idx] > 25:
        if not np.isnan(macd_hist[latest_idx]):
            if macd_hist[latest_idx] > 0:
                strength += 20.0
            else:
                strength -= 20.0
        count += 1
    
    # Bollinger Bands
    current_price = close_prices[latest_idx]
    if not np.isnan(bb_upper[latest_idx]) and not np.isnan(bb_lower[latest_idx]):
        if current_price <= bb_lower[latest_idx]:
            strength += 15.0
        elif current_price >= bb_upper[latest_idx]:
            strength -= 15.0
        count += 1
    
    if count > 0:
        strength = (strength / count) * (100.0 / 60.0)  # Normalize
    
    return min(100.0, max(-100.0, strength))

class FastDataLoader:
    """Optimized data loader with memory mapping and caching"""
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.cache = {}
        self.cache_timestamps = {}
        self.cache_ttl = 300  # 5 minutes
    
    @lru_cache(maxsize=100)
    def get_file_path(self, symbol_name: str, source: str) -> Path:
        """Cached file path resolution"""
        clean_name = symbol_name.replace(' ', '_').replace('/', '_').replace('|', '_')
        
        if source == "historical":
            return self.data_dir / "historical" / f"{clean_name}_1min_fast.csv"
        elif source == "live":
            return self.data_dir / "live" / f"{clean_name}_live_1min.csv"
        else:
            return self.data_dir / "historical" / f"{clean_name}_1min_fast.csv"
    
    def load_data_fast(self, symbol_name: str, instrument_key: str, source: str = "historical") -> Optional[np.ndarray]:
        """Ultra-fast data loading with memory mapping and caching"""
        cache_key = f"{symbol_name}_{source}"
        current_time = time.time()
        
        # Check cache first
        if (cache_key in self.cache and 
            cache_key in self.cache_timestamps and
            current_time - self.cache_timestamps[cache_key] < self.cache_ttl):
            return self.cache[cache_key]
        
        try:
            filepath = self.get_file_path(symbol_name, source)
            
            if not filepath.exists():
                logger.warning(f"File not found: {filepath}")
                return None
            
            # Use faster CSV reading with specific dtypes
            df = pd.read_csv(
                filepath,
                index_col='timestamp',
                parse_dates=True,
                dtype={
                    'open': np.float32,
                    'high': np.float32,
                    'low': np.float32,
                    'close': np.float32,
                    'volume': np.int32
                },
                engine='c'  # Use C engine for speed
            )
            
            if len(df) < 50:
                return None
            
            # Convert to numpy arrays for faster processing
            data = {
                'open': df['open'].values.astype(np.float64),
                'high': df['high'].values.astype(np.float64),
                'low': df['low'].values.astype(np.float64),
                'close': df['close'].values.astype(np.float64),
                'volume': df['volume'].values.astype(np.float64),
                'timestamp': df.index.values
            }
            
            # Cache the result
            self.cache[cache_key] = data
            self.cache_timestamps[cache_key] = current_time
            
            # Limit cache size
            if len(self.cache) > 200:
                oldest_key = min(self.cache_timestamps, key=self.cache_timestamps.get)
                del self.cache[oldest_key]
                del self.cache_timestamps[oldest_key]
            
            return data
            
        except Exception as e:
            logger.error(f"Error loading data for {symbol_name}: {e}")
            return None

class UltraFastTechnicalAnalyzer:
    """Ultra-optimized technical analyzer"""
    
    def __init__(self, data_dir: str = "data", indicators_dir: str = "data/indicators"):
        self.data_dir = Path(data_dir)
        self.indicators_dir = Path(indicators_dir)
        self.indicators_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize fast data loader
        self.data_loader = FastDataLoader(data_dir)
        
        # Pre-compiled pattern functions
        self.pattern_functions = self._get_pattern_functions()
        
        # Configuration
        self.config = self._load_config()
        
        logger.info(f"Ultra-Fast Technical Analyzer initialized with {MAX_WORKERS} workers")
    
    def _load_config(self) -> Dict:
        """Load optimized configuration"""
        return {
            "signals": {
                "rsi_oversold": 30,
                "rsi_overbought": 70,
                "adx_trend_strength": 25,
            },
            "indicators": {
                "rsi_period": 14,
                "bb_period": 20,
                "bb_std_dev": 2.0,
                "adx_period": 14,
                "ema_periods": [9, 21, 50],
                "sma_periods": [20, 50, 200], # Added 200
                "aroon_period": 14,
                "stoch_fastk_period": 14,
                "stoch_slowk_period": 3,
                "stoch_slowd_period": 3,
                "willr_period": 14,
                "cci_period": 14,
                "mfi_period": 14,
                "atr_period": 14,
                "keltner_period": 20,
                "keltner_multiplier": 2.0,
                "donchian_period": 20,
                "chaikin_fastperiod": 3,
                "chaikin_slowperiod": 10
            }
        }
    
    def _get_pattern_functions(self) -> Dict:
        """Pre-compile pattern functions"""
        return {
            'hammer': talib.CDLHAMMER,
            'doji': talib.CDLDOJI,
            'engulfing': talib.CDLENGULFING,
            'morning_star': talib.CDLMORNINGSTAR,
            'evening_star': talib.CDLEVENINGSTAR
        }
    
    def calculate_indicators_fast(self, data: Dict) -> Dict[str, np.ndarray]:
        """Ultra-fast indicator calculation using vectorized operations"""
        if data is None:
            return {}
        
        indicators = {}
        close = data['close']
        high = data['high']
        low = data['low']
        open_p = data['open']
        volume = data['volume']
        
        try:
            # Fast custom indicators
            indicators['rsi'] = fast_rsi(close, self.config['indicators']['rsi_period'])
            indicators['bb_upper'], indicators['bb_middle'], indicators['bb_lower'] = fast_bollinger_bands(
                close, self.config['indicators']['bb_period'], self.config['indicators']['bb_std_dev']
            )
            
            # Fast EMAs
            for period in self.config['indicators']['ema_periods']:
                indicators[f'ema_{period}'] = fast_ema(close, period)
            
            # Fast SMAs
            for period in self.config['indicators']['sma_periods']:
                indicators[f'sma_{period}'] = fast_sma(close, period)
            
            # TA-Lib for complex indicators (optimized calls)
            adx_period = self.config['indicators']['adx_period']
            indicators['adx'] = talib.ADX(high, low, close, timeperiod=adx_period)
            indicators['adx_plus'] = talib.PLUS_DI(high, low, close, timeperiod=adx_period)
            indicators['adx_minus'] = talib.MINUS_DI(high, low, close, timeperiod=adx_period)

            indicators['macd'], indicators['macd_signal'], indicators['macd_hist'] = talib.MACD(close)
            
            # ATR - ensure it uses config
            atr_p = self.config['indicators'].get('atr_period', 14)
            indicators['atr'] = talib.ATR(high, low, close, timeperiod=atr_p)

            # Parabolic SAR
            indicators['sar'] = talib.SAR(high, low, acceleration=0.02, maximum=0.2)

            # Aroon
            aroon_period = self.config['indicators'].get('aroon_period', 14)
            indicators['aroon_up'], indicators['aroon_down'] = talib.AROON(high, low, timeperiod=aroon_period)

            # Stochastic Oscillator
            stoch_fastk_period = self.config['indicators']['stoch_fastk_period']
            stoch_slowk_period = self.config['indicators']['stoch_slowk_period']
            stoch_slowd_period = self.config['indicators']['stoch_slowd_period']
            indicators['stoch_k'], indicators['stoch_d'] = talib.STOCH(
                high, low, close,
                fastk_period=stoch_fastk_period,
                slowk_period=stoch_slowk_period,
                slowd_period=stoch_slowd_period
            )

            # Williams %R
            willr_period = self.config['indicators']['willr_period']
            indicators['williams_r'] = talib.WILLR(high, low, close, timeperiod=willr_period)

            # Commodity Channel Index (CCI)
            cci_period = self.config['indicators']['cci_period']
            indicators['cci'] = talib.CCI(high, low, close, timeperiod=cci_period)

            # Money Flow Index (MFI)
            mfi_period = self.config['indicators']['mfi_period']
            indicators['mfi'] = talib.MFI(high, low, close, volume, timeperiod=mfi_period)

            # Keltner Channels
            keltner_period = self.config['indicators']['keltner_period']
            keltner_multiplier = self.config['indicators']['keltner_multiplier']
            ema_close_keltner = talib.EMA(close, timeperiod=keltner_period)
            atr_keltner = talib.ATR(high, low, close, timeperiod=keltner_period) # Use specific ATR for Keltner
            indicators['keltner_middle'] = ema_close_keltner
            indicators['keltner_upper'] = ema_close_keltner + (keltner_multiplier * atr_keltner)
            indicators['keltner_lower'] = ema_close_keltner - (keltner_multiplier * atr_keltner)

            # Donchian Channels
            donchian_period = self.config['indicators']['donchian_period']
            indicators['donchian_upper'] = talib.MAX(high, timeperiod=donchian_period)
            indicators['donchian_lower'] = talib.MIN(low, timeperiod=donchian_period)
            indicators['donchian_middle'] = (indicators['donchian_upper'] + indicators['donchian_lower']) / 2.0

            # On-Balance Volume (OBV)
            indicators['obv'] = talib.OBV(close, volume)

            # Accumulation/Distribution Line (AD Line)
            indicators['ad'] = talib.AD(high, low, close, volume)

            # Volume Weighted Average Price (VWAP)
            typical_price = (high + low + close) / 3.0
            vwap_num = np.cumsum(typical_price * volume)
            vwap_den = np.cumsum(volume)
            indicators['vwap'] = np.divide(vwap_num, vwap_den, out=np.full_like(vwap_num, np.nan), where=vwap_den!=0)

            # Chaikin A/D Oscillator
            chaikin_fastperiod = self.config['indicators'].get('chaikin_fastperiod', 3)
            chaikin_slowperiod = self.config['indicators'].get('chaikin_slowperiod', 10)
            indicators['chaikin_ad'] = talib.ADOSC(high, low, close, volume,
                                                   fastperiod=chaikin_fastperiod,
                                                   slowperiod=chaikin_slowperiod)

            # Essential patterns only (reduced set for speed)
            for pattern_name in ['hammer', 'doji', 'engulfing']:
                if pattern_name in self.pattern_functions:
                    indicators[pattern_name] = self.pattern_functions[pattern_name](open_p, high, low, close)
            
            # Original data
            indicators.update(data)
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}
    
    def generate_signal_fast(self, instrument_key: str, indicators: Dict) -> TechnicalSignal:
        """Ultra-fast signal generation"""
        try:
            if not indicators or len(indicators.get('close', [])) == 0:
                return self._create_error_signal(instrument_key, "No data")
            
            # Get latest values
            latest_idx = -1
            current_price = indicators['close'][latest_idx]
            current_volume = int(indicators['volume'][latest_idx])
            current_time = indicators['timestamp'][latest_idx]

            signal_strength = 0.0
            signal_count = 0
            reasoning_parts = []

            # RSI Logic
            if 'rsi' in indicators and not np.isnan(indicators['rsi'][latest_idx]):
                rsi_val = indicators['rsi'][latest_idx]
                rsi_oversold_config = self.config['signals'].get('rsi_oversold', 30)
                rsi_overbought_config = self.config['signals'].get('rsi_overbought', 70)
                if rsi_val < rsi_oversold_config:
                    signal_strength += 25
                    reasoning_parts.append(f"RSI oversold ({rsi_val:.1f})")
                    signal_count += 1
                elif rsi_val > rsi_overbought_config:
                    signal_strength -= 25
                    reasoning_parts.append(f"RSI overbought ({rsi_val:.1f})")
                    signal_count += 1

            # ADX Trend Analysis (from problem description)
            # Note: Requires 'adx_plus' and 'adx_minus' to be calculated in calculate_indicators_fast
            if ('adx' in indicators and not np.isnan(indicators['adx'][latest_idx]) and
                'adx_plus' in indicators and not np.isnan(indicators['adx_plus'][latest_idx]) and
                'adx_minus' in indicators and not np.isnan(indicators['adx_minus'][latest_idx])):

                adx_value = indicators['adx'][latest_idx]
                adx_trend_strength_config = self.config['signals'].get('adx_trend_strength', 25)
                if adx_value > adx_trend_strength_config:
                    if indicators['adx_plus'][latest_idx] > indicators['adx_minus'][latest_idx]:
                        signal_strength += 15
                        reasoning_parts.append(f"Strong uptrend (ADX: {adx_value:.1f})")
                    else:
                        signal_strength -= 15
                        reasoning_parts.append(f"Strong downtrend (ADX: {adx_value:.1f})")
                    signal_count += 1

            # EMA Crossover (from problem description)
            # Note: Requires 'ema_9' and 'ema_21' (which are typically calculated)
            if ('ema_9' in indicators and not np.isnan(indicators['ema_9'][latest_idx]) and
                'ema_21' in indicators and not np.isnan(indicators['ema_21'][latest_idx])):
                if indicators['ema_9'][latest_idx] > indicators['ema_21'][latest_idx]:
                    signal_strength += 10
                    reasoning_parts.append("EMA 9 > EMA 21 (bullish)")
                else:
                    signal_strength -= 10
                    reasoning_parts.append("EMA 9 < EMA 21 (bearish)")
                signal_count += 1

            # SMA Logic
            # Note: Requires 'sma_50' and 'sma_200' to be calculated in calculate_indicators_fast
            if 'sma_50' in indicators and not np.isnan(indicators['sma_50'][latest_idx]):
                if current_price > indicators['sma_50'][latest_idx]:
                    signal_strength += 5
                    reasoning_parts.append("Price above SMA50 (bullish)")
                else:
                    signal_strength -= 5
                    reasoning_parts.append("Price below SMA50 (bearish)")
                signal_count += 1

            if 'sma_200' in indicators and not np.isnan(indicators['sma_200'][latest_idx]):
                if current_price > indicators['sma_200'][latest_idx]:
                    signal_strength += 10
                    reasoning_parts.append("Price above SMA200 (long-term bullish)")
                else:
                    signal_strength -= 10
                    reasoning_parts.append("Price below SMA200 (long-term bearish)")
                signal_count += 1

            if ('sma_50' in indicators and not np.isnan(indicators['sma_50'][latest_idx]) and
                'sma_200' in indicators and not np.isnan(indicators['sma_200'][latest_idx])):
                if indicators['sma_50'][latest_idx] > indicators['sma_200'][latest_idx]:
                    signal_strength += 5
                    reasoning_parts.append("SMA50 above SMA200 (golden cross context)")
                else:
                    signal_strength -= 5
                    reasoning_parts.append("SMA50 below SMA200 (death cross context)")
                signal_count += 1

            # Parabolic SAR (SAR) Logic
            # Note: Requires 'sar' to be calculated in calculate_indicators_fast
            if 'sar' in indicators and not np.isnan(indicators['sar'][latest_idx]):
                if current_price > indicators['sar'][latest_idx]:
                    signal_strength += 10
                    reasoning_parts.append("Price above SAR (bullish)")
                else:
                    signal_strength -= 10
                    reasoning_parts.append("Price below SAR (bearish)")
                signal_count += 1

            # Aroon Logic
            # Note: Requires 'aroon_up' and 'aroon_down' to be calculated in calculate_indicators_fast
            if ('aroon_up' in indicators and not np.isnan(indicators['aroon_up'][latest_idx]) and
                'aroon_down' in indicators and not np.isnan(indicators['aroon_down'][latest_idx])):
                aroon_up_val = indicators['aroon_up'][latest_idx]
                aroon_down_val = indicators['aroon_down'][latest_idx]
                if aroon_up_val > 70 and aroon_down_val < 30:
                    signal_strength += 10
                    reasoning_parts.append("Aroon Up strong (bullish)")
                    signal_count += 1
                elif aroon_down_val > 70 and aroon_up_val < 30:
                    signal_strength -= 10
                    reasoning_parts.append("Aroon Down strong (bearish)")
                    signal_count += 1

            # --- START OF NEW MOMENTUM INDICATORS ---

            # Stochastic Oscillator
            # Note: Requires 'stoch_k' and 'stoch_d' to be calculated in calculate_indicators_fast
            if ('stoch_k' in indicators and not np.isnan(indicators['stoch_k'][latest_idx]) and
                'stoch_d' in indicators and not np.isnan(indicators['stoch_d'][latest_idx])): # stoch_d isn't used in current logic but good to check
                stoch_k_val = indicators['stoch_k'][latest_idx]
                if stoch_k_val < 20:
                    signal_strength += 10
                    reasoning_parts.append(f"Stochastic oversold ({stoch_k_val:.1f})")
                    signal_count += 1
                elif stoch_k_val > 80:
                    signal_strength -= 10
                    reasoning_parts.append(f"Stochastic overbought ({stoch_k_val:.1f})")
                    signal_count += 1

            # Williams %R
            # Note: Requires 'williams_r' to be calculated in calculate_indicators_fast
            if 'williams_r' in indicators and not np.isnan(indicators['williams_r'][latest_idx]):
                williams_r_val = indicators['williams_r'][latest_idx]
                if williams_r_val < -80:
                    signal_strength += 10
                    reasoning_parts.append(f"Williams %R oversold ({williams_r_val:.1f})")
                    signal_count += 1
                elif williams_r_val > -20:
                    signal_strength -= 10
                    reasoning_parts.append(f"Williams %R overbought ({williams_r_val:.1f})")
                    signal_count += 1

            # Commodity Channel Index (CCI)
            # Note: Requires 'cci' to be calculated in calculate_indicators_fast
            if 'cci' in indicators and not np.isnan(indicators['cci'][latest_idx]):
                cci_val = indicators['cci'][latest_idx]
                if cci_val < -100:
                    signal_strength += 10
                    reasoning_parts.append(f"CCI oversold ({cci_val:.1f})")
                    signal_count += 1
                elif cci_val > 100:
                    signal_strength -= 10
                    reasoning_parts.append(f"CCI overbought ({cci_val:.1f})")
                    signal_count += 1

            # Money Flow Index (MFI)
            # Note: Requires 'mfi' to be calculated in calculate_indicators_fast
            if 'mfi' in indicators and not np.isnan(indicators['mfi'][latest_idx]):
                mfi_val = indicators['mfi'][latest_idx]
                if mfi_val < 20:
                    signal_strength += 10
                    reasoning_parts.append(f"MFI oversold ({mfi_val:.1f})")
                    signal_count += 1
                elif mfi_val > 80:
                    signal_strength -= 10
                    reasoning_parts.append(f"MFI overbought ({mfi_val:.1f})")
                    signal_count += 1

            # --- END OF NEW MOMENTUM INDICATORS ---

            # --- START OF NEW VOLATILITY INDICATORS ---

            # Average True Range (ATR) - Contextual
            # Note: 'atr' is already calculated in calculate_indicators_fast
            if 'atr' in indicators and not np.isnan(indicators['atr'][latest_idx]) and current_price != 0:
                atr_val = indicators['atr'][latest_idx]
                atr_percentage = atr_val / current_price
                if atr_percentage > 0.03:
                    reasoning_parts.append(f"High volatility environment (ATR: {atr_val:.2f}, {atr_percentage*100:.1f}%)")
                elif atr_percentage < 0.005:
                    reasoning_parts.append(f"Low volatility environment (ATR: {atr_val:.2f}, {atr_percentage*100:.1f}%)")
                # No signal_strength change or signal_count increment for contextual ATR

            # Keltner Channels
            # Note: Requires 'keltner_upper', 'keltner_lower', 'keltner_middle' to be calculated in calculate_indicators_fast
            if ('keltner_upper' in indicators and not np.isnan(indicators['keltner_upper'][latest_idx]) and
                'keltner_lower' in indicators and not np.isnan(indicators['keltner_lower'][latest_idx])):
                keltner_upper_val = indicators['keltner_upper'][latest_idx]
                keltner_lower_val = indicators['keltner_lower'][latest_idx]
                if current_price > keltner_upper_val:
                    signal_strength += 10 # Bullish breakout/strength
                    reasoning_parts.append(f"Price above Keltner Upper ({keltner_upper_val:.2f})")
                    signal_count += 1
                elif current_price < keltner_lower_val:
                    signal_strength -= 10 # Bearish breakdown/weakness
                    reasoning_parts.append(f"Price below Keltner Lower ({keltner_lower_val:.2f})")
                    signal_count += 1

            # Donchian Channels
            # Note: Requires 'donchian_upper', 'donchian_lower', 'donchian_middle' to be calculated in calculate_indicators_fast
            if ('donchian_upper' in indicators and not np.isnan(indicators['donchian_upper'][latest_idx]) and
                'donchian_lower' in indicators and not np.isnan(indicators['donchian_lower'][latest_idx])):
                donchian_upper_val = indicators['donchian_upper'][latest_idx]
                donchian_lower_val = indicators['donchian_lower'][latest_idx]
                if current_price >= donchian_upper_val:
                    signal_strength += 10 # Breakout above recent high
                    reasoning_parts.append(f"Price breakout above Donchian Upper ({donchian_upper_val:.2f})")
                    signal_count += 1
                elif current_price <= donchian_lower_val:
                    signal_strength -= 10 # Breakdown below recent low
                    reasoning_parts.append(f"Price breakdown below Donchian Lower ({donchian_lower_val:.2f})")
                    signal_count += 1

            # --- END OF NEW VOLATILITY INDICATORS ---

            # --- START OF NEW VOLUME INDICATORS ---

            # On-Balance Volume (OBV)
            # Note: Requires 'obv' to be calculated in calculate_indicators_fast
            # Compares current OBV with OBV 10 periods ago.
            obv_check_period = 10
            if ('obv' in indicators and
                len(indicators['obv']) > obv_check_period and
                not np.isnan(indicators['obv'][latest_idx]) and
                not np.isnan(indicators['obv'][latest_idx - obv_check_period])):

                current_obv = indicators['obv'][latest_idx]
                past_obv = indicators['obv'][latest_idx - obv_check_period]
                if current_obv > past_obv:
                    signal_strength += 5
                    reasoning_parts.append(f"OBV rising (current: {current_obv:.0f}, past: {past_obv:.0f})")
                    signal_count += 1
                elif current_obv < past_obv:
                    signal_strength -= 5
                    reasoning_parts.append(f"OBV falling (current: {current_obv:.0f}, past: {past_obv:.0f})")
                    signal_count += 1

            # Accumulation/Distribution Line (AD Line)
            # Note: Requires 'ad' to be calculated in calculate_indicators_fast
            # Compares current AD Line with AD Line 10 periods ago.
            ad_check_period = 10
            if ('ad' in indicators and
                len(indicators['ad']) > ad_check_period and
                not np.isnan(indicators['ad'][latest_idx]) and
                not np.isnan(indicators['ad'][latest_idx - ad_check_period])):

                current_ad = indicators['ad'][latest_idx]
                past_ad = indicators['ad'][latest_idx - ad_check_period]
                if current_ad > past_ad:
                    signal_strength += 5
                    reasoning_parts.append(f"AD Line rising (current: {current_ad:.0f}, past: {past_ad:.0f})")
                    signal_count += 1
                elif current_ad < past_ad:
                    signal_strength -= 5
                    reasoning_parts.append(f"AD Line falling (current: {current_ad:.0f}, past: {past_ad:.0f})")
                    signal_count += 1

            # Volume Weighted Average Price (VWAP)
            # Note: Requires 'vwap' to be calculated in calculate_indicators_fast
            if 'vwap' in indicators and not np.isnan(indicators['vwap'][latest_idx]):
                vwap_val = indicators['vwap'][latest_idx]
                if current_price > vwap_val:
                    signal_strength += 7
                    reasoning_parts.append(f"Price above VWAP ({vwap_val:.2f})")
                    signal_count += 1
                elif current_price < vwap_val:
                    signal_strength -= 7
                    reasoning_parts.append(f"Price below VWAP ({vwap_val:.2f})")
                    signal_count += 1

            # Chaikin A/D Oscillator
            # Note: Requires 'chaikin_ad' to be calculated in calculate_indicators_fast
            if 'chaikin_ad' in indicators and not np.isnan(indicators['chaikin_ad'][latest_idx]):
                chaikin_ad_val = indicators['chaikin_ad'][latest_idx]
                if chaikin_ad_val > 0:
                    signal_strength += 7
                    reasoning_parts.append(f"Chaikin A/D positive ({chaikin_ad_val:.0f})")
                    signal_count += 1
                elif chaikin_ad_val < 0:
                    signal_strength -= 7
                    reasoning_parts.append(f"Chaikin A/D negative ({chaikin_ad_val:.0f})")
                    signal_count += 1

            # --- END OF NEW VOLUME INDICATORS ---

            normalized_signal_strength = 0.0
            if signal_count > 0:
                normalized_signal_strength = (signal_strength / signal_count) * 10.0

            # Clamp the normalized strength for consistent range in TechnicalSignal object
            clamped_signal_strength_for_report = max(-100.0, min(100.0, normalized_signal_strength))

            # Determine signal type from normalized_signal_strength
            if normalized_signal_strength > 60:
                signal_type = SignalType.STRONG_BUY
            elif normalized_signal_strength > 20:
                signal_type = SignalType.BUY
            elif normalized_signal_strength < -60:
                signal_type = SignalType.STRONG_SELL
            elif normalized_signal_strength < -20:
                signal_type = SignalType.SELL
            else:
                signal_type = SignalType.HOLD

            # Populate indicators for the signal report
            reported_indicators = {}

            # Helper to safely add indicators
            def add_to_report(key):
                if key in indicators and not np.isnan(indicators[key][latest_idx]):
                    reported_indicators[key] = indicators[key][latest_idx]

            add_to_report('rsi')
            add_to_report('adx')
            add_to_report('adx_plus')
            add_to_report('adx_minus')

            # EMAs from config
            for period in self.config['indicators'].get('ema_periods', []):
                add_to_report(f'ema_{period}')

            # SMAs from config
            for period in self.config['indicators'].get('sma_periods', []):
                add_to_report(f'sma_{period}')

            add_to_report('bb_upper')
            add_to_report('bb_middle')
            add_to_report('bb_lower')
            add_to_report('sar')
            add_to_report('aroon_up')
            add_to_report('aroon_down')
            add_to_report('stoch_k')
            add_to_report('stoch_d')
            add_to_report('williams_r')
            add_to_report('cci')
            add_to_report('mfi')
            add_to_report('atr')
            add_to_report('keltner_upper')
            add_to_report('keltner_middle')
            add_to_report('keltner_lower')
            add_to_report('donchian_upper')
            add_to_report('donchian_middle')
            add_to_report('donchian_lower')
            add_to_report('obv')
            add_to_report('ad')
            add_to_report('vwap')
            add_to_report('chaikin_ad')
            
            return TechnicalSignal(
                instrument=instrument_key,
                timestamp=current_time,
                signal_type=signal_type,
                strength=abs(clamped_signal_strength_for_report), # Use the clamped value
                indicators=reported_indicators,
                patterns=[], # Pattern detection is separate
                price=current_price,
                volume=current_volume,
                reasoning="; ".join(reasoning_parts) or "Signal analysis" # Changed default
            )
            
        except Exception as e:
            logger.error(f"Error generating signal for {instrument_key}: {e}")
            return self._create_error_signal(instrument_key, str(e))
    
    def _create_error_signal(self, instrument_key: str, error_msg: str) -> TechnicalSignal:
        """Create error signal"""
        return TechnicalSignal(
            instrument=instrument_key,
            timestamp=datetime.now(),
            signal_type=SignalType.HOLD,
            strength=0,
            indicators={},
            patterns=[],
            price=0,
            volume=0,
            reasoning=f"Error: {error_msg}"
        )
    
    def analyze_single_symbol(self, symbol_dict: Dict, source: str = "historical") -> Tuple[str, Dict]:
        """Analyze a single symbol - optimized for parallel processing"""
        instrument_key = symbol_dict.get('instrument_key')
        symbol_name = symbol_dict.get('name')
        
        if not instrument_key or not symbol_name:
            return instrument_key, {'error': 'Invalid symbol data', 'timestamp': datetime.now().isoformat()}
        
        try:
            # Load data
            data = self.data_loader.load_data_fast(symbol_name, instrument_key, source)
            if data is None:
                return instrument_key, {'error': 'Data loading failed', 'timestamp': datetime.now().isoformat()}
            
            # Calculate indicators
            indicators = self.calculate_indicators_fast(data)
            if not indicators:
                return instrument_key, {'error': 'Indicator calculation failed', 'timestamp': datetime.now().isoformat()}
            
            # Generate signal
            signal = self.generate_signal_fast(instrument_key, indicators)
            
            # Create report
            report = {
                'instrument': instrument_key,
                'symbol_name': symbol_name,
                'analysis_timestamp': datetime.now().isoformat(),
                'signal': asdict(signal),
                'summary': f"Signal: {signal.signal_type.value} (Strength: {signal.strength:.1f}%)"
            }
            
            return instrument_key, report
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol_name}: {e}")
            return instrument_key, {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def run_analysis_parallel(self, symbols: List[Dict], source: str = "historical") -> Dict[str, Dict]:
        """Run analysis in parallel for maximum speed"""
        results = {}
        
        if not symbols:
            logger.warning("No symbols provided for analysis")
            return results
        
        logger.info(f"Starting parallel analysis of {len(symbols)} symbols using {MAX_WORKERS} workers...")
        start_time = time.time()
        
        # Use ProcessPoolExecutor for CPU-intensive tasks
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create partial function with source parameter
            analyze_func = partial(self.analyze_single_symbol, source=source)
            
            # Submit all tasks
            future_to_symbol = {
                executor.submit(analyze_func, symbol): symbol 
                for symbol in symbols
            }
            
            # Collect results as they complete
            completed_count = 0
            for future in as_completed(future_to_symbol):
                try:
                    instrument_key, result = future.result(timeout=30)  # 30 second timeout per symbol
                    results[instrument_key] = result
                    completed_count += 1
                    
                    if completed_count % 10 == 0:  # Progress update every 10 symbols
                        logger.info(f"Completed {completed_count}/{len(symbols)} analyses")
                        
                except Exception as e:
                    symbol = future_to_symbol[future]
                    instrument_key = symbol.get('instrument_key', 'unknown')
                    logger.error(f"Analysis failed for {instrument_key}: {e}")
                    results[instrument_key] = {'error': str(e), 'timestamp': datetime.now().isoformat()}
        
        elapsed_time = time.time() - start_time
        logger.info(f"Parallel analysis completed in {elapsed_time:.2f} seconds")
        logger.info(f"Average time per symbol: {elapsed_time/len(symbols):.3f} seconds")
        
        return results
    
    def save_results_fast(self, results: Dict[str, Dict], filename: str = None):
        """Fast results saving"""
        try:
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"ultra_fast_analysis_{timestamp}.json"
            
            filepath = self.data_dir / filename
            
            # Use faster JSON serialization
            with open(filepath, 'w') as f:
                json.dump(results, f, indent=2, default=str, separators=(',', ':'))
            
            logger.info(f"Results saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")

# Optimized worker function for multiprocessing
def analyze_symbol_worker(args):
    """Worker function for multiprocessing"""
    symbol_dict, source, data_dir = args
    
    # Create analyzer instance in worker process
    temp_analyzer = UltraFastTechnicalAnalyzer(data_dir)
    return temp_analyzer.analyze_single_symbol(symbol_dict, source)

# Main execution optimized for speed
if __name__ == "__main__":
    try:
        # Load symbols
        with open('symbols.json', 'r') as f:
            symbols_data = json.load(f)
            instruments_to_analyze = symbols_data.get('symbols', [])
        
        if not instruments_to_analyze:
            logger.error("No symbols found in symbols.json")
            exit(1)
        
        # Initialize ultra-fast analyzer
        analyzer = UltraFastTechnicalAnalyzer()
        
        print(f"🚀 Starting ULTRA-FAST analysis of {len(instruments_to_analyze)} symbols...")
        print(f"Using {MAX_WORKERS} parallel workers on {CPU_COUNT} CPU cores")
        
        # Run parallel analysis
        start_time = time.time()
        results = analyzer.run_analysis_parallel(instruments_to_analyze)
        total_time = time.time() - start_time
        
        # Save results
        analyzer.save_results_fast(results)
        
        # Print summary
        print("\n" + "="*60)
        print("⚡ ULTRA-FAST ANALYSIS COMPLETED ⚡")
        print("="*60)
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Symbols analyzed: {len(results)}")
        print(f"Average time per symbol: {total_time/len(results):.3f} seconds")
        print(f"Analysis speed: {len(results)/total_time:.1f} symbols/second")
        
        # Show top signals
        successful_results = {k: v for k, v in results.items() if 'error' not in v}
        
        if successful_results:
            print(f"\n📊 SIGNAL SUMMARY ({len(successful_results)} successful analyses):")
            print("-" * 60)
            
            signal_counts = {}
            for result in successful_results.values():
                signal_type = result['signal']['signal_type']
                signal_counts[signal_type] = signal_counts.get(signal_type, 0) + 1
            
            for signal_type, count in sorted(signal_counts.items()):
                print(f"{signal_type}: {count} symbols")
            
            # Show top buy/sell signals
            buy_signals = []
            sell_signals = []
            
            for instrument, result in successful_results.items():
                signal = result['signal']
                if signal['signal_type'] in ['BUY', 'STRONG_BUY']:
                    buy_signals.append((instrument, signal['strength'], signal['price']))
                elif signal['signal_type'] in ['SELL', 'STRONG_SELL']:
                    sell_signals.append((instrument, signal['strength'], signal['price']))
            
            # Sort by strength
            buy_signals.sort(key=lambda x: x[1], reverse=True)
            sell_signals.sort(key=lambda x: x[1], reverse=True)
            
            print(f"\n🟢 TOP BUY SIGNALS:")
            for i, (instrument, strength, price) in enumerate(buy_signals[:5]):
                print(f"  {i+1}. {instrument}: {strength:.1f}% @ ₹{price:.2f}")
            
            print(f"\n🔴 TOP SELL SIGNALS:")
            for i, (instrument, strength, price) in enumerate(sell_signals[:5]):
                print(f"  {i+1}. {instrument}: {strength:.1f}% @ ₹{price:.2f}")
        
        # Error summary
        error_count = len([r for r in results.values() if 'error' in r])
        if error_count > 0:
            print(f"\n⚠️  {error_count} symbols had errors during analysis")
        
        print("\n🎯 Analysis complete! Check the saved JSON file for detailed results.")
        
    except FileNotFoundError:
        logger.error("symbols.json not found. Please create it with your symbols.")
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)