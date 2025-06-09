#!/usr/bin/env python3
"""
Advanced Technical Analyzer for Upstox Trading System
Integrates with historical_fetcher.py and live_data_manager.py
Provides comprehensive technical analysis using TA-Lib
Optimized for high-frequency trading with caching and performance optimization
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

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
    strength: float  # 0-100
    indicators: Dict[str, float]
    patterns: List[str]
    price: float
    volume: int
    reasoning: str

class AdvancedTechnicalAnalyzer:
    def __init__(self, data_dir: str = "data", indicators_dir: str = "data/indicators"):
        """
        Initialize the Technical Analyzer
        
        Args:
            data_dir: Base directory containing historical and live data
            indicators_dir: Directory to store calculated indicators
        """
        self.data_dir = Path(data_dir)
        self.indicators_dir = Path(indicators_dir)
        self.indicators_dir.mkdir(parents=True, exist_ok=True)
        
        # Data paths
        self.historical_dir = self.data_dir / "historical"
        self.live_dir = self.data_dir / "live"
        
        # Performance optimization - cached data
        self.data_cache = {}
        self.indicator_cache = {}
        self.last_update = {}
        
        # Threading for real-time updates
        self.update_lock = threading.Lock()
        
        # Configuration for indicators
        self.config = self._load_config()
        
        # Pattern recognition setup
        self.pattern_functions = self._setup_pattern_functions()
        
        logger.info(f"Technical Analyzer initialized")
        logger.info(f"Data directory: {self.data_dir}")
        logger.info(f"Indicators directory: {self.indicators_dir}")
    
    def _load_config(self) -> Dict:
        """Load configuration for indicators"""
        default_config = {
            "indicators": {
                "trend": {
                    "ema_periods": [9, 21, 50, 200],
                    "sma_periods": [20, 50, 200],
                    "dema_periods": [9, 21],
                    "adx_period": 14
                },
                "momentum": {
                    "rsi_period": 14,
                    "macd_fast": 12,
                    "macd_slow": 26,
                    "macd_signal": 9,
                    "stoch_k": 14,
                    "stoch_d": 3,
                    "williams_r_period": 14
                },
                "volatility": {
                    "bb_period": 20,
                    "bb_std_dev": 2,
                    "atr_period": 14,
                    "keltner_period": 20,
                    "keltner_multiplier": 2
                },
                "volume": {
                    "obv_enabled": True,
                    "ad_enabled": True,
                    "vwap_enabled": True,
                    "mfi_period": 14
                }
            },
            "signals": {
                "rsi_oversold": 30,
                "rsi_overbought": 70,
                "adx_trend_strength": 25,
                "volume_surge_multiplier": 2.0,
                "breakout_volume_multiplier": 1.5
            },
            "cache_settings": {
                "max_cache_size": 1000,
                "cache_ttl_minutes": 5
            }
        }
        
        config_file = self.data_dir / "analyzer_config.json"
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    loaded_config = json.load(f)
                    # Merge with defaults
                    default_config.update(loaded_config)
            except Exception as e:
                logger.warning(f"Error loading config, using defaults: {e}")
        
        return default_config
    
    def _setup_pattern_functions(self) -> Dict:
        """Setup candlestick pattern recognition functions"""
        return {
            # Reversal patterns
            'hammer': talib.CDLHAMMER,
            'hanging_man': talib.CDLHANGINGMAN,
            'inverted_hammer': talib.CDLINVERTEDHAMMER,
            'shooting_star': talib.CDLSHOOTINGSTAR,
            'doji': talib.CDLDOJI,
            'dragonfly_doji': talib.CDLDRAGONFLY,
            'gravestone_doji': talib.CDLGRAVESTONE,
            'morning_star': talib.CDLMORNINGSTAR,
            'evening_star': talib.CDLEVENINGSTAR,
            'engulfing_bullish': talib.CDLENGULFING,
            'harami': talib.CDLHARAMI,
            'piercing_line': talib.CDLPIERCING,
            'dark_cloud': talib.CDLDARKCLOUDCOVER,
            
            # Continuation patterns
            'rising_three': talib.CDLRISING,
            'falling_three': talib.CDLFALLING,
            'up_gap_three': talib.CDLUPGAPTHREE,
            'down_gap_three': talib.CDLDOWNGAPTHREE,
            
            # Powerful patterns
            'three_white_soldiers': talib.CDL3WHITESOLDIERS,
            'three_black_crows': talib.CDL3BLACKCROWS,
            'abandoned_baby': talib.CDLABANDONEDBABY,
            'belt_hold': talib.CDLBELTHOLD
        }
    
    def load_data(self, instrument_key: str, source: str = "historical") -> Optional[pd.DataFrame]:
        """
        Load OHLCV data for analysis
        
        Args:
            instrument_key: Instrument identifier
            source: 'historical', 'live', or 'combined'
            
        Returns:
            DataFrame with OHLCV data
        """
        try:
            clean_name = instrument_key.replace('|', '_').replace(':', '_')
            
            if source == "historical":
                filepath = self.historical_dir / f"{clean_name}_1min.csv"
            elif source == "live":
                filepath = self.live_dir / f"{clean_name}_live_1min.csv"
            elif source == "combined":
                # Combine historical and live data
                hist_df = self.load_data(instrument_key, "historical")
                live_df = self.load_data(instrument_key, "live")
                
                if hist_df is not None and live_df is not None:
                    combined_df = pd.concat([hist_df, live_df])
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                    combined_df.sort_index(inplace=True)
                    return combined_df
                elif hist_df is not None:
                    return hist_df
                elif live_df is not None:
                    return live_df
                else:
                    return None
            else:
                logger.error(f"Invalid source: {source}")
                return None
            
            if filepath.exists():
                df = pd.read_csv(filepath, index_col=0, parse_dates=True)
                logger.debug(f"Loaded {len(df)} records from {filepath}")
                return df
            else:
                logger.warning(f"Data file not found: {filepath}")
                return None
                
        except Exception as e:
            logger.error(f"Error loading data for {instrument_key}: {e}")
            return None
    
    def calculate_trend_indicators(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Calculate trend-following indicators"""
        indicators = {}
        
        # Extract OHLC data
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        
        # Exponential Moving Averages
        for period in self.config['indicators']['trend']['ema_periods']:
            indicators[f'ema_{period}'] = talib.EMA(close, timeperiod=period)
        
        # Simple Moving Averages
        for period in self.config['indicators']['trend']['sma_periods']:
            indicators[f'sma_{period}'] = talib.SMA(close, timeperiod=period)
        
        # Double Exponential Moving Averages
        for period in self.config['indicators']['trend']['dema_periods']:
            indicators[f'dema_{period}'] = talib.DEMA(close, timeperiod=period)
        
        # Average Directional Index (ADX)
        adx_period = self.config['indicators']['trend']['adx_period']
        indicators['adx'] = talib.ADX(high, low, close, timeperiod=adx_period)
        indicators['adx_plus'] = talib.PLUS_DI(high, low, close, timeperiod=adx_period)
        indicators['adx_minus'] = talib.MINUS_DI(high, low, close, timeperiod=adx_period)
        
        # Parabolic SAR
        indicators['sar'] = talib.SAR(high, low, acceleration=0.02, maximum=0.2)
        
        # Aroon
        indicators['aroon_up'], indicators['aroon_down'] = talib.AROON(high, low, timeperiod=14)
        
        return indicators
    
    def calculate_momentum_indicators(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Calculate momentum oscillators"""
        indicators = {}
        
        # Extract OHLC data
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        volume = df['volume'].values
        
        # RSI
        rsi_period = self.config['indicators']['momentum']['rsi_period']
        indicators['rsi'] = talib.RSI(close, timeperiod=rsi_period)
        
        # MACD
        macd_fast = self.config['indicators']['momentum']['macd_fast']
        macd_slow = self.config['indicators']['momentum']['macd_slow']
        macd_signal = self.config['indicators']['momentum']['macd_signal']
        
        indicators['macd'], indicators['macd_signal'], indicators['macd_hist'] = talib.MACD(
            close, fastperiod=macd_fast, slowperiod=macd_slow, signalperiod=macd_signal
        )
        
        # Stochastic
        stoch_k = self.config['indicators']['momentum']['stoch_k']
        stoch_d = self.config['indicators']['momentum']['stoch_d']
        indicators['stoch_k'], indicators['stoch_d'] = talib.STOCH(
            high, low, close, fastk_period=stoch_k, slowk_period=stoch_d, slowd_period=stoch_d
        )
        
        # Williams %R
        williams_period = self.config['indicators']['momentum']['williams_r_period']
        indicators['williams_r'] = talib.WILLR(high, low, close, timeperiod=williams_period)
        
        # Commodity Channel Index
        indicators['cci'] = talib.CCI(high, low, close, timeperiod=14)
        
        # Money Flow Index
        mfi_period = self.config['indicators']['volume']['mfi_period']
        indicators['mfi'] = talib.MFI(high, low, close, volume, timeperiod=mfi_period)
        
        return indicators
    
    def calculate_volatility_indicators(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Calculate volatility indicators"""
        indicators = {}
        
        # Extract OHLC data
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        
        # Bollinger Bands
        bb_period = self.config['indicators']['volatility']['bb_period']
        bb_std = self.config['indicators']['volatility']['bb_std_dev']
        
        indicators['bb_upper'], indicators['bb_middle'], indicators['bb_lower'] = talib.BBANDS(
            close, timeperiod=bb_period, nbdevup=bb_std, nbdevdn=bb_std
        )
        
        # Average True Range
        atr_period = self.config['indicators']['volatility']['atr_period']
        indicators['atr'] = talib.ATR(high, low, close, timeperiod=atr_period)
        
        # Keltner Channels (approximated using ATR)
        keltner_period = self.config['indicators']['volatility']['keltner_period']
        keltner_mult = self.config['indicators']['volatility']['keltner_multiplier']
        ema_close = talib.EMA(close, timeperiod=keltner_period)
        atr_keltner = talib.ATR(high, low, close, timeperiod=keltner_period)
        
        indicators['keltner_upper'] = ema_close + (keltner_mult * atr_keltner)
        indicators['keltner_lower'] = ema_close - (keltner_mult * atr_keltner)
        indicators['keltner_middle'] = ema_close
        
        # Donchian Channels
        indicators['donchian_upper'] = talib.MAX(high, timeperiod=20)
        indicators['donchian_lower'] = talib.MIN(low, timeperiod=20)
        indicators['donchian_middle'] = (indicators['donchian_upper'] + indicators['donchian_lower']) / 2
        
        return indicators
    
    def calculate_volume_indicators(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Calculate volume-based indicators"""
        indicators = {}
        
        # Extract OHLCV data
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        volume = df['volume'].values
        
        # On-Balance Volume
        if self.config['indicators']['volume']['obv_enabled']:
            indicators['obv'] = talib.OBV(close, volume)
        
        # Accumulation/Distribution Line
        if self.config['indicators']['volume']['ad_enabled']:
            indicators['ad'] = talib.AD(high, low, close, volume)
        
        # Volume Weighted Average Price (VWAP)
        if self.config['indicators']['volume']['vwap_enabled']:
            typical_price = (high + low + close) / 3
            vwap_num = np.cumsum(typical_price * volume)
            vwap_den = np.cumsum(volume)
            indicators['vwap'] = np.divide(vwap_num, vwap_den, 
                                         out=np.zeros_like(vwap_num), where=vwap_den!=0)
        
        # Chaikin A/D Oscillator
        indicators['chaikin_ad'] = talib.ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10)
        
        return indicators
    
    def detect_candlestick_patterns(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Detect candlestick patterns"""
        patterns = {}
        
        # Extract OHLC data
        open_prices = df['open'].values
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        
        # Calculate all patterns
        for pattern_name, pattern_func in self.pattern_functions.items():
            try:
                patterns[pattern_name] = pattern_func(open_prices, high, low, close)
            except Exception as e:
                logger.debug(f"Error calculating pattern {pattern_name}: {e}")
                patterns[pattern_name] = np.zeros(len(df))
        
        return patterns
    
    def calculate_all_indicators(self, instrument_key: str, source: str = "combined") -> Optional[Dict]:
        """
        Calculate all technical indicators for an instrument
        
        Args:
            instrument_key: Instrument identifier
            source: Data source ('historical', 'live', or 'combined')
            
        Returns:
            Dictionary containing all indicators
        """
        try:
            # Load data
            df = self.load_data(instrument_key, source)
            if df is None or len(df) < 50:  # Need minimum data for indicators
                logger.warning(f"Insufficient data for {instrument_key}")
                return None
            
            # Calculate indicators
            indicators = {}
            
            # Trend indicators
            trend_indicators = self.calculate_trend_indicators(df)
            indicators.update(trend_indicators)
            
            # Momentum indicators
            momentum_indicators = self.calculate_momentum_indicators(df)
            indicators.update(momentum_indicators)
            
            # Volatility indicators
            volatility_indicators = self.calculate_volatility_indicators(df)
            indicators.update(volatility_indicators)
            
            # Volume indicators
            volume_indicators = self.calculate_volume_indicators(df)
            indicators.update(volume_indicators)
            
            # Candlestick patterns
            patterns = self.detect_candlestick_patterns(df)
            indicators.update(patterns)
            
            # Add OHLCV data
            indicators['open'] = df['open'].values
            indicators['high'] = df['high'].values
            indicators['low'] = df['low'].values
            indicators['close'] = df['close'].values
            indicators['volume'] = df['volume'].values
            indicators['timestamp'] = df.index
            
            logger.info(f"Calculated {len(indicators)} indicators for {instrument_key}")
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating indicators for {instrument_key}: {e}")
            return None
    
    def generate_signals(self, instrument_key: str, indicators: Dict) -> TechnicalSignal:
        """
        Generate trading signals based on technical indicators
        
        Args:
            instrument_key: Instrument identifier
            indicators: Dictionary of calculated indicators
            
        Returns:
            TechnicalSignal object
        """
        try:
            # Get latest values (last non-NaN values)
            latest_idx = -1
            while np.isnan(indicators['close'][latest_idx]) and abs(latest_idx) < len(indicators['close']):
                latest_idx -= 1
            
            current_price = indicators['close'][latest_idx]
            current_volume = indicators['volume'][latest_idx]
            current_time = indicators['timestamp'][latest_idx]
            
            # Initialize signal components
            signal_strength = 0
            signal_count = 0
            detected_patterns = []
            reasoning_parts = []
            
            # Trend Analysis
            if not np.isnan(indicators['adx'][latest_idx]):
                adx_value = indicators['adx'][latest_idx]
                if adx_value > self.config['signals']['adx_trend_strength']:
                    if indicators['adx_plus'][latest_idx] > indicators['adx_minus'][latest_idx]:
                        signal_strength += 15
                        reasoning_parts.append(f"Strong uptrend (ADX: {adx_value:.1f})")
                    else:
                        signal_strength -= 15
                        reasoning_parts.append(f"Strong downtrend (ADX: {adx_value:.1f})")
                    signal_count += 1
            
            # EMA Crossover
            if not np.isnan(indicators['ema_9'][latest_idx]) and not np.isnan(indicators['ema_21'][latest_idx]):
                if indicators['ema_9'][latest_idx] > indicators['ema_21'][latest_idx]:
                    signal_strength += 10
                    reasoning_parts.append("EMA 9 > EMA 21 (bullish)")
                else:
                    signal_strength -= 10
                    reasoning_parts.append("EMA 9 < EMA 21 (bearish)")
                signal_count += 1
            
            # RSI Analysis
            if not np.isnan(indicators['rsi'][latest_idx]):
                rsi_value = indicators['rsi'][latest_idx]
                if rsi_value < self.config['signals']['rsi_oversold']:
                    signal_strength += 20
                    reasoning_parts.append(f"RSI oversold ({rsi_value:.1f})")
                elif rsi_value > self.config['signals']['rsi_overbought']:
                    signal_strength -= 20
                    reasoning_parts.append(f"RSI overbought ({rsi_value:.1f})")
                signal_count += 1
            
            # MACD Analysis
            if not np.isnan(indicators['macd'][latest_idx]) and not np.isnan(indicators['macd_signal'][latest_idx]):
                if indicators['macd'][latest_idx] > indicators['macd_signal'][latest_idx]:
                    signal_strength += 12
                    reasoning_parts.append("MACD bullish crossover")
                else:
                    signal_strength -= 12
                    reasoning_parts.append("MACD bearish crossover")
                signal_count += 1
            
            # Bollinger Bands Analysis
            if (not np.isnan(indicators['bb_upper'][latest_idx]) and 
                not np.isnan(indicators['bb_lower'][latest_idx])):
                if current_price < indicators['bb_lower'][latest_idx]:
                    signal_strength += 15
                    reasoning_parts.append("Price below BB lower band")
                elif current_price > indicators['bb_upper'][latest_idx]:
                    signal_strength -= 15
                    reasoning_parts.append("Price above BB upper band")
                signal_count += 1
            
            # Volume Analysis
            if len(indicators['volume']) > 20:
                avg_volume = np.mean(indicators['volume'][-20:-1])  # Last 20 periods average
                volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
                
                if volume_ratio > self.config['signals']['volume_surge_multiplier']:
                    signal_strength += 8
                    reasoning_parts.append(f"High volume ({volume_ratio:.1f}x avg)")
                    signal_count += 1
            
            # Candlestick Pattern Analysis
            pattern_signals = 0
            for pattern_name, pattern_values in indicators.items():
                if pattern_name in self.pattern_functions:
                    if pattern_values[latest_idx] > 0:  # Bullish pattern
                        detected_patterns.append(f"{pattern_name}_bullish")
                        pattern_signals += 5
                    elif pattern_values[latest_idx] < 0:  # Bearish pattern
                        detected_patterns.append(f"{pattern_name}_bearish")
                        pattern_signals -= 5
            
            signal_strength += pattern_signals
            if detected_patterns:
                reasoning_parts.append(f"Patterns: {', '.join(detected_patterns[:3])}")
                signal_count += 1
            
            # Normalize signal strength
            if signal_count > 0:
                signal_strength = signal_strength / signal_count * 10  # Scale to 0-100
                signal_strength = max(-100, min(100, signal_strength))  # Clamp to range
            
            # Determine signal type
            if signal_strength > 60:
                signal_type = SignalType.STRONG_BUY
            elif signal_strength > 20:
                signal_type = SignalType.BUY
            elif signal_strength < -60:
                signal_type = SignalType.STRONG_SELL
            elif signal_strength < -20:
                signal_type = SignalType.SELL
            else:
                signal_type = SignalType.HOLD
            
            # Create signal object
            signal = TechnicalSignal(
                instrument=instrument_key,
                timestamp=current_time,
                signal_type=signal_type,
                strength=abs(signal_strength),
                indicators={
                    'rsi': indicators['rsi'][latest_idx] if not np.isnan(indicators['rsi'][latest_idx]) else None,
                    'macd': indicators['macd'][latest_idx] if not np.isnan(indicators['macd'][latest_idx]) else None,
                    'adx': indicators['adx'][latest_idx] if not np.isnan(indicators['adx'][latest_idx]) else None,
                    'ema_9': indicators['ema_9'][latest_idx] if not np.isnan(indicators['ema_9'][latest_idx]) else None,
                    'ema_21': indicators['ema_21'][latest_idx] if not np.isnan(indicators['ema_21'][latest_idx]) else None,
                    'bb_position': self._calculate_bb_position(current_price, indicators, latest_idx)
                },
                patterns=detected_patterns,
                price=current_price,
                volume=int(current_volume),
                reasoning="; ".join(reasoning_parts) if reasoning_parts else "Neutral market conditions"
            )
            
            return signal
            
        except Exception as e:
            logger.error(f"Error generating signals for {instrument_key}: {e}")
            return TechnicalSignal(
                instrument=instrument_key,
                timestamp=datetime.now(),
                signal_type=SignalType.HOLD,
                strength=0,
                indicators={},
                patterns=[],
                price=0,
                volume=0,
                reasoning=f"Error in analysis: {e}"
            )
    
    def _calculate_bb_position(self, price: float, indicators: Dict, idx: int) -> Optional[float]:
        """Calculate position within Bollinger Bands (0-100)"""
        try:
            if (np.isnan(indicators['bb_upper'][idx]) or 
                np.isnan(indicators['bb_lower'][idx])):
                return None
            
            bb_upper = indicators['bb_upper'][idx]
            bb_lower = indicators['bb_lower'][idx]
            
            if bb_upper == bb_lower:
                return 50.0
            
            position = ((price - bb_lower) / (bb_upper - bb_lower)) * 100
            return max(0, min(100, position))
        except:
            return None
    
    def save_indicators_to_csv(self, instrument_key: str, indicators: Dict):
        """Save calculated indicators to CSV file"""
        try:
            clean_name = instrument_key.replace('|', '_').replace(':', '_')
            filename = f"{clean_name}_indicators.csv"
            filepath = self.indicators_dir / filename
            
            # Create DataFrame from indicators
            df_data = {}
            timestamps = indicators.get('timestamp', [])
            
            for key, values in indicators.items():
                if key != 'timestamp':
                    df_data[key] = values
            
            df = pd.DataFrame(df_data, index=timestamps)
            df.to_csv(filepath)
            logger.info(f"Saved indicators to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving indicators to CSV: {e}")
    
    def generate_analysis_report(self, instrument_key: str, indicators: Dict, signal: TechnicalSignal) -> Dict:
        """
        Generate comprehensive analysis report with raw values and insights
        
        Args:
            instrument_key: Instrument identifier
            indicators: Dictionary of calculated indicators
            signal: Generated signal
            
        Returns:
            Dictionary containing raw values and insights
        """
        try:
            latest_idx = -1
            
            # Get latest values
            while latest_idx >= -len(indicators['close']) and np.isnan(indicators['close'][latest_idx]):
                latest_idx -= 1
            
            current_price = indicators['close'][latest_idx]
            current_time = indicators['timestamp'][latest_idx]
            
            # Raw indicator values
            raw_values = {
                'basic_data': {
                    'timestamp': current_time.isoformat() if hasattr(current_time, 'isoformat') else str(current_time),
                    'price': float(current_price),
                    'volume': int(indicators['volume'][latest_idx]),
                    'open': float(indicators['open'][latest_idx]),
                    'high': float(indicators['high'][latest_idx]),
                    'low': float(indicators['low'][latest_idx])
                },
                'trend_indicators': {
                    'ema_9': float(indicators.get('ema_9', [np.nan])[latest_idx]) if not np.isnan(indicators.get('ema_9', [np.nan])[latest_idx]) else None,
                    'ema_21': float(indicators.get('ema_21', [np.nan])[latest_idx]) if not np.isnan(indicators.get('ema_21', [np.nan])[latest_idx]) else None,
                    'ema_50': float(indicators.get('ema_50', [np.nan])[latest_idx]) if not np.isnan(indicators.get('ema_50', [np.nan])[latest_idx]) else None,
                    'sma_20': float(indicators.get('sma_20', [np.nan])[latest_idx]) if not np.isnan(indicators.get('sma_20', [np.nan])[latest_idx]) else None,
                    'adx': float(indicators.get('adx', [np.nan])[latest_idx]) if not np.isnan(indicators.get('adx', [np.nan])[latest_idx]) else None,
                    'adx_plus': float(indicators.get('adx_plus', [np.nan])[latest_idx]) if not np.isnan(indicators.get('adx_plus', [np.nan])[latest_idx]) else None,
                    'adx_minus': float(indicators.get('adx_minus', [np.nan])[latest_idx]) if not np.isnan(indicators.get('adx_minus', [np.nan])[latest_idx]) else None,
                    'sar': float(indicators.get('sar', [np.nan])[latest_idx]) if not np.isnan(indicators.get('sar', [np.nan])[latest_idx]) else None
                },
                'momentum_indicators': {
                    'rsi': float(indicators.get('rsi', [np.nan])[latest_idx]) if not np.isnan(indicators.get('rsi', [np.nan])[latest_idx]) else None,
                    'macd': float(indicators.get('macd', [np.nan])[latest_idx]) if not np.isnan(indicators.get('macd', [np.nan])[latest_idx]) else None,
                    'macd_signal': float(indicators.get('macd_signal', [np.nan])[latest_idx]) if not np.isnan(indicators.get('macd_signal', [np.nan])[latest_idx]) else None,
                    'macd_hist': float(indicators.get('macd_hist', [np.nan])[latest_idx]) if not np.isnan(indicators.get('macd_hist', [np.nan])[latest_idx]) else None,
                    'stoch_k': float(indicators.get('stoch_k', [np.nan])[latest_idx]) if not np.isnan(indicators.get('stoch_k', [np.nan])[latest_idx]) else None,
                    'stoch_d': float(indicators.get('stoch_d', [np.nan])[latest_idx]) if not np.isnan(indicators.get('stoch_d', [np.nan])[latest_idx]) else None,
                    'williams_r': float(indicators.get('williams_r', [np.nan])[latest_idx]) if not np.isnan(indicators.get('williams_r', [np.nan])[latest_idx]) else None,
                    'cci': float(indicators.get('cci', [np.nan])[latest_idx]) if not np.isnan(indicators.get('cci', [np.nan])[latest_idx]) else None,
                    'mfi': float(indicators.get('mfi', [np.nan])[latest_idx]) if not np.isnan(indicators.get('mfi', [np.nan])[latest_idx]) else None
                },
                'volatility_indicators': {
                    'bb_upper': float(indicators.get('bb_upper', [np.nan])[latest_idx]) if not np.isnan(indicators.get('bb_upper', [np.nan])[latest_idx]) else None,
                    'bb_middle': float(indicators.get('bb_middle', [np.nan])[latest_idx]) if not np.isnan(indicators.get('bb_middle', [np.nan])[latest_idx]) else None,
                    'bb_lower': float(indicators.get('bb_lower', [np.nan])[latest_idx]) if not np.isnan(indicators.get('bb_lower', [np.nan])[latest_idx]) else None,
                    'bb_position': self._calculate_bb_position(current_price, indicators, latest_idx),
                    'atr': float(indicators.get('atr', [np.nan])[latest_idx]) if not np.isnan(indicators.get('atr', [np.nan])[latest_idx]) else None,
                    'keltner_upper': float(indicators.get('keltner_upper', [np.nan])[latest_idx]) if not np.isnan(indicators.get('keltner_upper', [np.nan])[latest_idx]) else None,
                    'keltner_middle': float(indicators.get('keltner_middle', [np.nan])[latest_idx]) if not np.isnan(indicators.get('keltner_middle', [np.nan])[latest_idx]) else None,
                    'keltner_lower': float(indicators.get('keltner_lower', [np.nan])[latest_idx]) if not np.isnan(indicators.get('keltner_lower', [np.nan])[latest_idx]) else None,
                    'donchian_upper': float(indicators.get('donchian_upper', [np.nan])[latest_idx]) if not np.isnan(indicators.get('donchian_upper', [np.nan])[latest_idx]) else None,
                    'donchian_middle': float(indicators.get('donchian_middle', [np.nan])[latest_idx]) if not np.isnan(indicators.get('donchian_middle', [np.nan])[latest_idx]) else None,
                    'donchian_lower': float(indicators.get('donchian_lower', [np.nan])[latest_idx]) if not np.isnan(indicators.get('donchian_lower', [np.nan])[latest_idx]) else None
                },
                'volume_indicators': {
                    'obv': float(indicators.get('obv', [np.nan])[latest_idx]) if not np.isnan(indicators.get('obv', [np.nan])[latest_idx]) else None,
                    'ad': float(indicators.get('ad', [np.nan])[latest_idx]) if not np.isnan(indicators.get('ad', [np.nan])[latest_idx]) else None,
                    'vwap': float(indicators.get('vwap', [np.nan])[latest_idx]) if not np.isnan(indicators.get('vwap', [np.nan])[latest_idx]) else None,
                    'chaikin_ad': float(indicators.get('chaikin_ad', [np.nan])[latest_idx]) if not np.isnan(indicators.get('chaikin_ad', [np.nan])[latest_idx]) else None
                },
                'patterns': {
                    'detected_patterns': signal.patterns,
                    'pattern_strength': len(signal.patterns)
                }
            }
            
            # Generate insights
            insights = {
                'trend_analysis': self._analyze_trend(indicators, latest_idx),
                'momentum_analysis': self._analyze_momentum(indicators, latest_idx),
                'volatility_analysis': self._analyze_volatility(indicators, latest_idx, current_price),
                'volume_analysis': self._analyze_volume(indicators, latest_idx),
                'support_resistance': self._find_support_resistance(indicators, latest_idx),
                'risk_assessment': self._assess_risk(indicators, latest_idx, current_price)
            }
            
            # Complete report
            report = {
                'instrument': instrument_key,
                'analysis_timestamp': datetime.now().isoformat(),
                'signal': asdict(signal),
                'raw_values': raw_values,
                'insights': insights,
                'summary': self._generate_summary(signal, insights)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating analysis report for {instrument_key}: {e}")
            return {
                'instrument': instrument_key,
                'analysis_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'signal': asdict(signal) if signal else None
            }
    
    def _analyze_trend(self, indicators: Dict, latest_idx: int) -> Dict:
        """Analyze trend strength and direction"""
        try:
            trend_analysis = {
                'direction': 'neutral',
                'strength': 'weak',
                'confirmation': [],
                'divergence': []
            }
            
            # EMA analysis
            ema_9 = indicators.get('ema_9', [np.nan])[latest_idx]
            ema_21 = indicators.get('ema_21', [np.nan])[latest_idx]
            ema_50 = indicators.get('ema_50', [np.nan])[latest_idx]
            
            if not np.isnan(ema_9) and not np.isnan(ema_21):
                if ema_9 > ema_21:
                    trend_analysis['direction'] = 'bullish'
                    trend_analysis['confirmation'].append('EMA 9 > EMA 21')
                else:
                    trend_analysis['direction'] = 'bearish'
                    trend_analysis['confirmation'].append('EMA 9 < EMA 21')
            
            # ADX strength
            adx = indicators.get('adx', [np.nan])[latest_idx]
            if not np.isnan(adx):
                if adx > 50:
                    trend_analysis['strength'] = 'very_strong'
                elif adx > 25:
                    trend_analysis['strength'] = 'strong'
                elif adx > 15:
                    trend_analysis['strength'] = 'moderate'
                else:
                    trend_analysis['strength'] = 'weak'
            
            # SAR confirmation
            sar = indicators.get('sar', [np.nan])[latest_idx]
            current_price = indicators['close'][latest_idx]
            if not np.isnan(sar):
                if current_price > sar:
                    trend_analysis['confirmation'].append('Price above SAR')
                else:
                    trend_analysis['confirmation'].append('Price below SAR')
            
            return trend_analysis
            
        except Exception as e:
            logger.error(f"Error in trend analysis: {e}")
            return {'direction': 'neutral', 'strength': 'unknown', 'error': str(e)}
    
    def _analyze_momentum(self, indicators: Dict, latest_idx: int) -> Dict:
        """Analyze momentum indicators"""
        try:
            momentum_analysis = {
                'overall': 'neutral',
                'rsi_condition': 'neutral',
                'macd_condition': 'neutral',
                'stoch_condition': 'neutral',
                'divergences': []
            }
            
            # RSI analysis
            rsi = indicators.get('rsi', [np.nan])[latest_idx]
            if not np.isnan(rsi):
                if rsi > 70:
                    momentum_analysis['rsi_condition'] = 'overbought'
                elif rsi < 30:
                    momentum_analysis['rsi_condition'] = 'oversold'
                elif rsi > 50:
                    momentum_analysis['rsi_condition'] = 'bullish'
                else:
                    momentum_analysis['rsi_condition'] = 'bearish'
            
            # MACD analysis
            macd = indicators.get('macd', [np.nan])[latest_idx]
            macd_signal = indicators.get('macd_signal', [np.nan])[latest_idx]
            if not np.isnan(macd) and not np.isnan(macd_signal):
                if macd > macd_signal:
                    momentum_analysis['macd_condition'] = 'bullish'
                else:
                    momentum_analysis['macd_condition'] = 'bearish'
            
            # Stochastic analysis
            stoch_k = indicators.get('stoch_k', [np.nan])[latest_idx]
            stoch_d = indicators.get('stoch_d', [np.nan])[latest_idx]
            if not np.isnan(stoch_k) and not np.isnan(stoch_d):
                if stoch_k > 80:
                    momentum_analysis['stoch_condition'] = 'overbought'
                elif stoch_k < 20:
                    momentum_analysis['stoch_condition'] = 'oversold'
                elif stoch_k > stoch_d:
                    momentum_analysis['stoch_condition'] = 'bullish'
                else:
                    momentum_analysis['stoch_condition'] = 'bearish'
            
            # Overall momentum
            bullish_count = sum(1 for condition in [
                momentum_analysis['rsi_condition'] in ['bullish', 'oversold'],
                momentum_analysis['macd_condition'] == 'bullish',
                momentum_analysis['stoch_condition'] in ['bullish', 'oversold']
            ] if condition)
            
            if bullish_count >= 2:
                momentum_analysis['overall'] = 'bullish'
            elif bullish_count <= 1:
                momentum_analysis['overall'] = 'bearish'
            
            return momentum_analysis
            
        except Exception as e:
            logger.error(f"Error in momentum analysis: {e}")
            return {'overall': 'neutral', 'error': str(e)}
    
    def _analyze_volatility(self, indicators: Dict, latest_idx: int, current_price: float) -> Dict:
        """Analyze volatility and price channels"""
        try:
            volatility_analysis = {
                'level': 'normal',
                'bb_squeeze': False,
                'breakout_potential': 'low',
                'channel_position': 'middle'
            }
            
            # ATR analysis for volatility level
            atr = indicators.get('atr', [np.nan])[latest_idx]
            if not np.isnan(atr) and len(indicators['atr']) > 20:
                avg_atr = np.nanmean(indicators['atr'][-20:])
                if atr > avg_atr * 1.5:
                    volatility_analysis['level'] = 'high'
                elif atr < avg_atr * 0.7:
                    volatility_analysis['level'] = 'low'
            
            # Bollinger Bands squeeze detection
            bb_upper = indicators.get('bb_upper', [np.nan])[latest_idx]
            bb_lower = indicators.get('bb_lower', [np.nan])[latest_idx]
            keltner_upper = indicators.get('keltner_upper', [np.nan])[latest_idx]
            keltner_lower = indicators.get('keltner_lower', [np.nan])[latest_idx]
            
            if (not np.isnan(bb_upper) and not np.isnan(bb_lower) and 
                not np.isnan(keltner_upper) and not np.isnan(keltner_lower)):
                
                if bb_upper < keltner_upper and bb_lower > keltner_lower:
                    volatility_analysis['bb_squeeze'] = True
                    volatility_analysis['breakout_potential'] = 'high'
            
            # Channel position
            bb_position = self._calculate_bb_position(current_price, indicators, latest_idx)
            if bb_position is not None:
                if bb_position > 80:
                    volatility_analysis['channel_position'] = 'upper'
                elif bb_position < 20:
                    volatility_analysis['channel_position'] = 'lower'
                else:
                    volatility_analysis['channel_position'] = 'middle'
            
            return volatility_analysis
            
        except Exception as e:
            logger.error(f"Error in volatility analysis: {e}")
            return {'level': 'unknown', 'error': str(e)}
    
    def _analyze_volume(self, indicators: Dict, latest_idx: int) -> Dict:
        """Analyze volume patterns"""
        try:
            volume_analysis = {
                'trend': 'neutral',
                'strength': 'normal',
                'confirmation': False,
                'accumulation_distribution': 'neutral'
            }
            
            current_volume = indicators['volume'][latest_idx]
            
            # Volume trend
            if len(indicators['volume']) > 10:
                recent_avg = np.mean(indicators['volume'][-10:])
                older_avg = np.mean(indicators['volume'][-20:-10]) if len(indicators['volume']) > 20 else recent_avg
                
                if recent_avg > older_avg * 1.2:
                    volume_analysis['trend'] = 'increasing'
                elif recent_avg < older_avg * 0.8:
                    volume_analysis['trend'] = 'decreasing'
            
            # Volume strength
            if len(indicators['volume']) > 20:
                avg_volume = np.mean(indicators['volume'][-20:])
                volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
                
                if volume_ratio > 2:
                    volume_analysis['strength'] = 'very_high'
                elif volume_ratio > 1.5:
                    volume_analysis['strength'] = 'high'
                elif volume_ratio < 0.5:
                    volume_analysis['strength'] = 'low'
            
            # OBV analysis
            if len(indicators.get('obv', [])) > 1:
                obv_current = indicators['obv'][latest_idx]
                obv_previous = indicators['obv'][latest_idx - 1]
                price_current = indicators['close'][latest_idx]
                price_previous = indicators['close'][latest_idx - 1]
                
                if ((price_current > price_previous and obv_current > obv_previous) or
                    (price_current < price_previous and obv_current < obv_previous)):
                    volume_analysis['confirmation'] = True
            
            # A/D Line analysis
            ad_line = indicators.get('ad', [np.nan])[latest_idx]
            if not np.isnan(ad_line) and len(indicators.get('ad', [])) > 10:
                recent_ad = np.mean(indicators['ad'][-5:])
                older_ad = np.mean(indicators['ad'][-15:-5])
                
                if recent_ad > older_ad:
                    volume_analysis['accumulation_distribution'] = 'accumulation'
                elif recent_ad < older_ad:
                    volume_analysis['accumulation_distribution'] = 'distribution'
            
            return volume_analysis
            
        except Exception as e:
            logger.error(f"Error in volume analysis: {e}")
            return {'trend': 'unknown', 'error': str(e)}
    
    def _find_support_resistance(self, indicators: Dict, latest_idx: int) -> Dict:
        """Find support and resistance levels"""
        try:
            sr_analysis = {
                'support_levels': [],
                'resistance_levels': [],
                'key_level': None,
                'distance_to_support': None,
                'distance_to_resistance': None
            }
            
            current_price = indicators['close'][latest_idx]
            
            # Use pivot points and recent highs/lows
            if len(indicators['high']) > 50:
                highs = indicators['high'][-50:]
                lows = indicators['low'][-50:]
                
                # Find recent pivot highs (resistance)
                for i in range(2, len(highs) - 2):
                    if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and
                        highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                        if highs[i] > current_price:
                            sr_analysis['resistance_levels'].append(float(highs[i]))
                
                # Find recent pivot lows (support)
                for i in range(2, len(lows) - 2):
                    if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and
                        lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                        if lows[i] < current_price:
                            sr_analysis['support_levels'].append(float(lows[i]))
                
                # Sort and keep only significant levels
                sr_analysis['resistance_levels'] = sorted(list(set(sr_analysis['resistance_levels'])))[:5]
                sr_analysis['support_levels'] = sorted(list(set(sr_analysis['support_levels'])), reverse=True)[:5]
                
                # Find nearest levels
                if sr_analysis['support_levels']:
                    nearest_support = max(sr_analysis['support_levels'])
                    sr_analysis['distance_to_support'] = (current_price - nearest_support) / current_price * 100
                
                if sr_analysis['resistance_levels']:
                    nearest_resistance = min(sr_analysis['resistance_levels'])
                    sr_analysis['distance_to_resistance'] = (nearest_resistance - current_price) / current_price * 100
            
            # Add moving averages as dynamic S/R
            for ma_key in ['ema_50', 'ema_200', 'sma_50', 'sma_200']:
                if ma_key in indicators:
                    ma_value = indicators[ma_key][latest_idx]
                    if not np.isnan(ma_value):
                        if ma_value > current_price:
                            sr_analysis['resistance_levels'].append(float(ma_value))
                        else:
                            sr_analysis['support_levels'].append(float(ma_value))
            
            return sr_analysis
            
        except Exception as e:
            logger.error(f"Error finding support/resistance: {e}")
            return {'support_levels': [], 'resistance_levels': [], 'error': str(e)}
    
    def _assess_risk(self, indicators: Dict, latest_idx: int, current_price: float) -> Dict:
        """Assess risk based on technical indicators"""
        try:
            risk_assessment = {
                'level': 'medium',
                'factors': [],
                'volatility_risk': 'medium',
                'trend_risk': 'medium',
                'recommended_stop_loss': None,
                'position_sizing': 'normal'
            }
            
            # Volatility risk
            atr = indicators.get('atr', [np.nan])[latest_idx]
            if not np.isnan(atr):
                atr_percentage = (atr / current_price) * 100
                if atr_percentage > 3:
                    risk_assessment['volatility_risk'] = 'high'
                    risk_assessment['factors'].append('High volatility')
                elif atr_percentage < 1:
                    risk_assessment['volatility_risk'] = 'low'
                    risk_assessment['factors'].append('Low volatility')
                
                # Suggest stop loss based on ATR
                risk_assessment['recommended_stop_loss'] = current_price - (atr * 2)
            
            # Trend risk
            adx = indicators.get('adx', [np.nan])[latest_idx]
            if not np.isnan(adx):
                if adx < 15:
                    risk_assessment['trend_risk'] = 'high'
                    risk_assessment['factors'].append('Weak trend - choppy market')
                elif adx > 40:
                    risk_assessment['trend_risk'] = 'low'
                    risk_assessment['factors'].append('Strong trend')
            
            # Volume risk
            if len(indicators['volume']) > 20:
                avg_volume = np.mean(indicators['volume'][-20:])
                current_volume = indicators['volume'][latest_idx]
                if current_volume < avg_volume * 0.5:
                    risk_assessment['factors'].append('Low volume - reduced liquidity')
            
            # Overall risk level
            high_risk_factors = sum(1 for factor in risk_assessment['factors'] 
                                  if any(word in factor.lower() for word in ['high', 'weak', 'low volume']))
            
            if high_risk_factors >= 2:
                risk_assessment['level'] = 'high'
                risk_assessment['position_sizing'] = 'reduced'
            elif high_risk_factors == 0:
                risk_assessment['level'] = 'low'
                risk_assessment['position_sizing'] = 'normal'
            
            return risk_assessment
            
        except Exception as e:
            logger.error(f"Error in risk assessment: {e}")
            return {'level': 'unknown', 'error': str(e)}
    
    def _generate_summary(self, signal: TechnicalSignal, insights: Dict) -> Dict:
        """Generate executive summary of analysis"""
        try:
            summary = {
                'recommendation': signal.signal_type.value,
                'confidence': signal.strength,
                'key_points': [],
                'risk_reward': 'balanced',
                'time_horizon': 'short_term',
                'action_items': []
            }
            
            # Add key points based on insights
            if insights.get('trend_analysis', {}).get('strength') == 'strong':
                summary['key_points'].append(f"Strong {insights['trend_analysis']['direction']} trend")
            
            if insights.get('momentum_analysis', {}).get('overall') != 'neutral':
                summary['key_points'].append(f"Momentum is {insights['momentum_analysis']['overall']}")
            
            if insights.get('volatility_analysis', {}).get('bb_squeeze'):
                summary['key_points'].append("Bollinger Band squeeze detected - breakout expected")
            
            if insights.get('volume_analysis', {}).get('confirmation'):
                summary['key_points'].append("Volume confirms price movement")
            
            # Risk-reward assessment
            risk_level = insights.get('risk_assessment', {}).get('level', 'medium')
            if risk_level == 'high':
                summary['risk_reward'] = 'high_risk'
            elif risk_level == 'low' and signal.strength > 60:
                summary['risk_reward'] = 'favorable'
            
            # Action items
            if signal.signal_type in [SignalType.BUY, SignalType.STRONG_BUY]:
                summary['action_items'].append("Consider long position")
                if insights.get('risk_assessment', {}).get('recommended_stop_loss'):
                    summary['action_items'].append(f"Set stop loss at {insights['risk_assessment']['recommended_stop_loss']:.2f}")
            elif signal.signal_type in [SignalType.SELL, SignalType.STRONG_SELL]:
                summary['action_items'].append("Consider short position or exit long")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return {'recommendation': 'HOLD', 'error': str(e)}
    
    def run_analysis(self, instrument_keys: List[str], source: str = "combined") -> Dict[str, Dict]:
        """
        Run complete analysis for multiple instruments
        
        Args:
            instrument_keys: List of instrument identifiers
            source: Data source to use
            
        Returns:
            Dictionary of analysis results for each instrument
        """
        results = {}
        
        for instrument_key in instrument_keys:
            try:
                logger.info(f"Analyzing {instrument_key}...")
                
                # Calculate indicators
                indicators = self.calculate_all_indicators(instrument_key, source)
                if indicators is None:
                    logger.warning(f"Skipping {instrument_key} - no data available")
                    continue
                
                # Generate signals
                signal = self.generate_signals(instrument_key, indicators)
                
                # Generate comprehensive report
                report = self.generate_analysis_report(instrument_key, indicators, signal)
                
                # Save indicators to CSV
                self.save_indicators_to_csv(instrument_key, indicators)
                
                results[instrument_key] = report
                
                logger.info(f"Analysis complete for {instrument_key}: {signal.signal_type.value} ({signal.strength:.1f}%)")
                
            except Exception as e:
                logger.error(f"Error analyzing {instrument_key}: {e}")
                results[instrument_key] = {
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
        
        return results
    
    def save_analysis_results(self, results: Dict[str, Dict], filename: str = None):
        """Save analysis results to JSON file"""
        try:
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"analysis_results_{timestamp}.json"
            
            filepath = self.data_dir / filename
            
            with open(filepath, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"Analysis results saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {e}")

# Example usage and testing
if __name__ == "__main__":
    # Initialize analyzer
    analyzer = AdvancedTechnicalAnalyzer()
    
    # Example instrument keys (replace with actual Upstox instrument keys)
    instruments = [
        "NSE_EQ|INE002A01018",  # Reliance
        "NSE_EQ|INE467B01029",  # TATASTEEL
        "NSE_EQ|INE040A01034"   # HDFC
    ]
    
    # Run analysis
    print("Starting technical analysis...")
    results = analyzer.run_analysis(instruments)
    
    # Save results
    analyzer.save_analysis_results(results)
    
    # Print summary
    print("\n" + "="*50)
    print("ANALYSIS SUMMARY")
    print("="*50)
    
    for instrument, result in results.items():
        if 'error' not in result:
            signal = result['signal']
            print(f"\n{instrument}:")
            print(f"  Signal: {signal['signal_type']} ({signal['strength']:.1f}%)")
            print(f"  Price: {signal['price']:.2f}")
            print(f"  Reasoning: {signal['reasoning']}")
        else:
            print(f"\n{instrument}: ERROR - {result['error']}")