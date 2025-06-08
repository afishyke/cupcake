#!/usr/bin/env python3
"""
TA-Lib indicators module for comprehensive technical analysis
Ultra-fast vectorized calculations using TA-Lib
"""
import numpy as np
import pandas as pd
import talib
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class TalibIndicators:
    def __init__(self):
        # Default parameters for indicators
        self.default_params = {
            'sma_periods': [5, 10, 20, 50, 100, 200],
            'ema_periods': [9, 12, 21, 26, 50],
            'rsi_period': 14,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            'bb_period': 20,
            'bb_std': 2,
            'atr_period': 14,
            'adx_period': 14,
            'cci_period': 20,
            'williams_period': 14,
            'stoch_fastk': 14,
            'stoch_slowk': 3,
            'stoch_slowd': 3
        }
    
    def calculate_moving_averages(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate SMA and EMA for multiple periods"""
        try:
            close = ohlcv_df['close'].values
            results = {'sma': {}, 'ema': {}}
            
            # Simple Moving Averages
            for period in self.default_params['sma_periods']:
                if len(close) >= period:
                    sma = talib.SMA(close, timeperiod=period)
                    results['sma'][f'sma_{period}'] = float(sma[-1]) if not np.isnan(sma[-1]) else 0
            
            # Exponential Moving Averages
            for period in self.default_params['ema_periods']:
                if len(close) >= period:
                    ema = talib.EMA(close, timeperiod=period)
                    results['ema'][f'ema_{period}'] = float(ema[-1]) if not np.isnan(ema[-1]) else 0
            
            return results
            
        except Exception as e:
            logger.error(f"Moving averages calculation failed: {e}")
            return {'sma': {}, 'ema': {}}
    
    def calculate_momentum_indicators(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate momentum indicators: RSI, MACD, Stochastic, Williams %R"""
        try:
            close = ohlcv_df['close'].values
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            results = {}
            
            # RSI
            if len(close) >= self.default_params['rsi_period']:
                rsi = talib.RSI(close, timeperiod=self.default_params['rsi_period'])
                results['rsi'] = float(rsi[-1]) if not np.isnan(rsi[-1]) else 50
            else:
                results['rsi'] = 50
            
            # MACD
            if len(close) >= self.default_params['macd_slow']:
                macd, macd_signal, macd_hist = talib.MACD(
                    close,
                    fastperiod=self.default_params['macd_fast'],
                    slowperiod=self.default_params['macd_slow'],
                    signalperiod=self.default_params['macd_signal']
                )
                results['macd'] = {
                    'macd': float(macd[-1]) if not np.isnan(macd[-1]) else 0,
                    'signal': float(macd_signal[-1]) if not np.isnan(macd_signal[-1]) else 0,
                    'histogram': float(macd_hist[-1]) if not np.isnan(macd_hist[-1]) else 0
                }
            else:
                results['macd'] = {'macd': 0, 'signal': 0, 'histogram': 0}
            
            # Stochastic
            if len(close) >= self.default_params['stoch_fastk']:
                slowk, slowd = talib.STOCH(
                    high, low, close,
                    fastk_period=self.default_params['stoch_fastk'],
                    slowk_period=self.default_params['stoch_slowk'],
                    slowd_period=self.default_params['stoch_slowd']
                )
                results['stochastic'] = {
                    'k': float(slowk[-1]) if not np.isnan(slowk[-1]) else 50,
                    'd': float(slowd[-1]) if not np.isnan(slowd[-1]) else 50
                }
            else:
                results['stochastic'] = {'k': 50, 'd': 50}
            
            # Williams %R
            if len(close) >= self.default_params['williams_period']:
                willr = talib.WILLR(high, low, close, timeperiod=self.default_params['williams_period'])
                results['williams_r'] = float(willr[-1]) if not np.isnan(willr[-1]) else -50
            else:
                results['williams_r'] = -50
            
            return results
            
        except Exception as e:
            logger.error(f"Momentum indicators calculation failed: {e}")
            return {}
    
    def calculate_volatility_indicators(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate volatility indicators: ATR, Bollinger Bands"""
        try:
            close = ohlcv_df['close'].values
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            results = {}
            
            # Average True Range
            if len(close) >= self.default_params['atr_period']:
                atr = talib.ATR(high, low, close, timeperiod=self.default_params['atr_period'])
                results['atr'] = float(atr[-1]) if not np.isnan(atr[-1]) else 0
            else:
                results['atr'] = 0
            
            # Bollinger Bands
            if len(close) >= self.default_params['bb_period']:
                bb_upper, bb_middle, bb_lower = talib.BBANDS(
                    close,
                    timeperiod=self.default_params['bb_period'],
                    nbdevup=self.default_params['bb_std'],
                    nbdevdn=self.default_params['bb_std']
                )
                
                current_price = close[-1]
                bb_width = bb_upper[-1] - bb_lower[-1]
                bb_position = (current_price - bb_lower[-1]) / bb_width if bb_width > 0 else 0.5
                
                results['bollinger_bands'] = {
                    'upper': float(bb_upper[-1]) if not np.isnan(bb_upper[-1]) else current_price,
                    'middle': float(bb_middle[-1]) if not np.isnan(bb_middle[-1]) else current_price,
                    'lower': float(bb_lower[-1]) if not np.isnan(bb_lower[-1]) else current_price,
                    'width': float(bb_width) if not np.isnan(bb_width) else 0,
                    'position': float(bb_position)
                }
            else:
                current_price = close[-1]
                results['bollinger_bands'] = {
                    'upper': current_price, 'middle': current_price, 'lower': current_price,
                    'width': 0, 'position': 0.5
                }
            
            return results
            
        except Exception as e:
            logger.error(f"Volatility indicators calculation failed: {e}")
            return {}
    
    def calculate_trend_indicators(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate trend indicators: ADX, PSAR, AROON"""
        try:
            close = ohlcv_df['close'].values
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            results = {}
            
            # ADX (Average Directional Index)
            if len(close) >= self.default_params['adx_period']:
                adx = talib.ADX(high, low, close, timeperiod=self.default_params['adx_period'])
                plus_di = talib.PLUS_DI(high, low, close, timeperiod=self.default_params['adx_period'])
                minus_di = talib.MINUS_DI(high, low, close, timeperiod=self.default_params['adx_period'])
                
                results['adx'] = {
                    'adx': float(adx[-1]) if not np.isnan(adx[-1]) else 0,
                    'plus_di': float(plus_di[-1]) if not np.isnan(plus_di[-1]) else 0,
                    'minus_di': float(minus_di[-1]) if not np.isnan(minus_di[-1]) else 0
                }
            else:
                results['adx'] = {'adx': 0, 'plus_di': 0, 'minus_di': 0}
            
            # Parabolic SAR
            if len(close) >= 10:
                psar = talib.SAR(high, low, acceleration=0.02, maximum=0.2)
                current_price = close[-1]
                psar_value = psar[-1] if not np.isnan(psar[-1]) else current_price
                
                results['parabolic_sar'] = {
                    'sar': float(psar_value),
                    'trend': 'bullish' if current_price > psar_value else 'bearish'
                }
            else:
                results['parabolic_sar'] = {'sar': close[-1], 'trend': 'neutral'}
            
            # AROON
            if len(close) >= 14:
                aroon_down, aroon_up = talib.AROON(high, low, timeperiod=14)
                aroon_osc = talib.AROONOSC(high, low, timeperiod=14)
                
                results['aroon'] = {
                    'up': float(aroon_up[-1]) if not np.isnan(aroon_up[-1]) else 0,
                    'down': float(aroon_down[-1]) if not np.isnan(aroon_down[-1]) else 0,
                    'oscillator': float(aroon_osc[-1]) if not np.isnan(aroon_osc[-1]) else 0
                }
            else:
                results['aroon'] = {'up': 0, 'down': 0, 'oscillator': 0}
            
            return results
            
        except Exception as e:
            logger.error(f"Trend indicators calculation failed: {e}")
            return {}
    
    def calculate_volume_indicators(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate volume indicators: OBV, AD, CMF"""
        try:
            close = ohlcv_df['close'].values
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            volume = ohlcv_df['volume'].values.astype(float)
            results = {}
            
            # On Balance Volume
            if len(close) >= 2:
                obv = talib.OBV(close, volume)
                results['obv'] = float(obv[-1]) if not np.isnan(obv[-1]) else 0
            else:
                results['obv'] = 0
            
            # Accumulation/Distribution Line
            if len(close) >= 2:
                ad = talib.AD(high, low, close, volume)
                results['ad_line'] = float(ad[-1]) if not np.isnan(ad[-1]) else 0
            else:
                results['ad_line'] = 0
            
            # Chaikin A/D Oscillator
            if len(close) >= 10:
                adosc = talib.ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10)
                results['ad_oscillator'] = float(adosc[-1]) if not np.isnan(adosc[-1]) else 0
            else:
                results['ad_oscillator'] = 0
            
            return results
            
        except Exception as e:
            logger.error(f"Volume indicators calculation failed: {e}")
            return {}
    
    def calculate_oscillators(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate oscillators: CCI, ROC, MOM"""
        try:
            close = ohlcv_df['close'].values
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            results = {}
            
            # Commodity Channel Index
            if len(close) >= self.default_params['cci_period']:
                cci = talib.CCI(high, low, close, timeperiod=self.default_params['cci_period'])
                results['cci'] = float(cci[-1]) if not np.isnan(cci[-1]) else 0
            else:
                results['cci'] = 0
            
            # Rate of Change
            if len(close) >= 10:
                roc = talib.ROC(close, timeperiod=10)
                results['roc'] = float(roc[-1]) if not np.isnan(roc[-1]) else 0
            else:
                results['roc'] = 0
            
            # Momentum
            if len(close) >= 10:
                mom = talib.MOM(close, timeperiod=10)
                results['momentum'] = float(mom[-1]) if not np.isnan(mom[-1]) else 0
            else:
                results['momentum'] = 0
            
            # Ultimate Oscillator
            if len(close) >= 28:
                ultosc = talib.ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
                results['ultimate_oscillator'] = float(ultosc[-1]) if not np.isnan(ultosc[-1]) else 50
            else:
                results['ultimate_oscillator'] = 50
            
            return results
            
        except Exception as e:
            logger.error(f"Oscillators calculation failed: {e}")
            return {}
    
    def calculate_candlestick_patterns(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate candlestick pattern recognition"""
        try:
            if len(ohlcv_df) < 5:
                return {}
            
            open_prices = ohlcv_df['open'].values
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            close = ohlcv_df['close'].values
            
            patterns = {}
            
            # Major reversal patterns
            patterns['doji'] = int(talib.CDLDOJI(open_prices, high, low, close)[-1])
            patterns['hammer'] = int(talib.CDLHAMMER(open_prices, high, low, close)[-1])
            patterns['hanging_man'] = int(talib.CDLHANGINGMAN(open_prices, high, low, close)[-1])
            patterns['shooting_star'] = int(talib.CDLSHOOTINGSTAR(open_prices, high, low, close)[-1])
            patterns['inverted_hammer'] = int(talib.CDLINVERTEDHAMMER(open_prices, high, low, close)[-1])
            
            # Engulfing patterns
            patterns['bullish_engulfing'] = int(talib.CDLENGULFING(open_prices, high, low, close)[-1])
            patterns['bearish_engulfing'] = int(talib.CDLENGULFING(open_prices, high, low, close)[-1])
            
            # Morning/Evening stars
            patterns['morning_star'] = int(talib.CDLMORNINGSTAR(open_prices, high, low, close)[-1])
            patterns['evening_star'] = int(talib.CDLEVENINGSTAR(open_prices, high, low, close)[-1])
            
            # Harami patterns
            patterns['harami'] = int(talib.CDLHARAMI(open_prices, high, low, close)[-1])
            patterns['harami_cross'] = int(talib.CDLHARAMICROSS(open_prices, high, low, close)[-1])
            
            # Three soldiers/crows
            patterns['three_white_soldiers'] = int(talib.CDL3WHITESOLDIERS(open_prices, high, low, close)[-1])
            patterns['three_black_crows'] = int(talib.CDL3BLACKCROWS(open_prices, high, low, close)[-1])
            
            # Count bullish and bearish signals
            bullish_signals = sum(1 for v in patterns.values() if v > 0)
            bearish_signals = sum(1 for v in patterns.values() if v < 0)
            
            patterns['summary'] = {
                'bullish_count': bullish_signals,
                'bearish_count': bearish_signals,
                'net_signal': bullish_signals - bearish_signals
            }
            
            return patterns
            
        except Exception as e:
            logger.error(f"Candlestick patterns calculation failed: {e}")
            return {}
    
    def calculate_all(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Calculate all technical indicators efficiently"""
        try:
            if len(ohlcv_df) < 5:
                logger.warning("Insufficient data for technical analysis")
                return {}
            
            results = {}
            
            # Calculate all indicator groups
            results.update(self.calculate_moving_averages(ohlcv_df))
            results.update(self.calculate_momentum_indicators(ohlcv_df))
            results.update(self.calculate_volatility_indicators(ohlcv_df))
            results.update(self.calculate_trend_indicators(ohlcv_df))
            results.update(self.calculate_volume_indicators(ohlcv_df))
            results.update(self.calculate_oscillators(ohlcv_df))
            
            # Candlestick patterns
            results['candlestick_patterns'] = self.calculate_candlestick_patterns(ohlcv_df)
            
            # Add current price info
            current_price = ohlcv_df['close'].iloc[-1]
            results['current_price'] = float(current_price)
            results['price_change'] = float(current_price - ohlcv_df['close'].iloc[-2]) if len(ohlcv_df) > 1 else 0
            results['price_change_pct'] = (results['price_change'] / ohlcv_df['close'].iloc[-2] * 100) if len(ohlcv_df) > 1 and ohlcv_df['close'].iloc[-2] > 0 else 0
            
            return results
            
        except Exception as e:
            logger.error(f"Technical indicators calculation failed: {e}")
            return {}