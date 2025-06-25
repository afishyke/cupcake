# Enhanced Technical Indicators with Trajectory-Based Signal Generation
# Fixed circular dependency issues

import redis
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Tuple, Optional
import json
import logging
from collections import defaultdict

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedTechnicalIndicators:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        """Initialize enhanced technical indicators with trajectory analysis"""
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db,
            decode_responses=True
        )
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Initialize trajectory signal generator - Import only when needed to avoid circular dependency
        self.trajectory_generator = None
        
        # Store recent indicator calculations for trend analysis
        self.indicator_history = defaultdict(lambda: [])
        
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
        
    def get_ohlcv_data(self, symbol: str, periods: int = 250) -> pd.DataFrame:
        """Get OHLCV data from Redis TimeSeries for a symbol"""
        try:
            symbol_clean = symbol.replace(' ', '_').replace('&', 'and')
            
            # Get current timestamp
            now_ts = int(datetime.now().timestamp() * 1000)
            
            # Calculate start timestamp (periods minutes ago)
            start_ts = now_ts - (periods * 60 * 1000)
            
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
            
            df = df.sort_values('timestamp').reset_index(drop=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting OHLCV data for {symbol}: {e}")
            return pd.DataFrame()
    
    def calculate_sma(self, data: pd.Series, window: int) -> pd.Series:
        """Calculate Simple Moving Average"""
        return data.rolling(window=window, min_periods=1).mean()
    
    def calculate_ema(self, data: pd.Series, window: int) -> pd.Series:
        """Calculate Exponential Moving Average"""
        return data.ewm(span=window, adjust=False).mean()
    
    def calculate_rsi(self, data: pd.Series, window: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
        """Calculate MACD (Moving Average Convergence Divergence)"""
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
        """Calculate Bollinger Bands"""
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
        """Calculate Average True Range"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        
        return true_range.rolling(window=window).mean()
    
    def calculate_volume_indicators(self, df: pd.DataFrame) -> Dict:
        """Calculate Volume-based indicators"""
        # Volume Moving Average
        volume_sma_20 = self.calculate_sma(df['volume'], 20)
        
        # Volume Rate of Change
        volume_roc = df['volume'].pct_change(periods=1) * 100
        
        # On Balance Volume (OBV)
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
        
        # Volume Weighted Average Price (VWAP)
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        cumulative_typical_price_volume = (typical_price * df['volume']).cumsum()
        cumulative_volume = df['volume'].cumsum()
        vwap = cumulative_typical_price_volume / cumulative_volume
        
        return {
            'volume_sma_20': volume_sma_20,
            'volume_roc': volume_roc,
            'obv': pd.Series(obv),
            'vwap': vwap,
            'volume_ratio': df['volume'] / volume_sma_20
        }
    
    def calculate_all_indicators(self, symbol: str) -> Dict:
        """Calculate all technical indicators for a symbol with enhanced analysis"""
        try:
            df = self.get_ohlcv_data(symbol)
            
            if df.empty:
                logger.warning(f"No data available for {symbol}")
                return {}
            
            # Price-based indicators
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
            
            # Volume indicators
            volume_indicators = self.calculate_volume_indicators(df)
            
            # Current values (latest)
            current_idx = -1
            current_price = df.iloc[current_idx]['close']
            
            indicators = {
                'symbol': symbol,
                'timestamp': df.iloc[current_idx]['timestamp'].isoformat(),
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
                'volume': {
                    'current_volume': df.iloc[current_idx]['volume'],
                    'volume_sma_20': volume_indicators['volume_sma_20'].iloc[current_idx] if not volume_indicators['volume_sma_20'].empty else None,
                    'volume_roc': volume_indicators['volume_roc'].iloc[current_idx] if not volume_indicators['volume_roc'].empty else None,
                    'obv': volume_indicators['obv'].iloc[current_idx] if not volume_indicators['obv'].empty else None,
                    'vwap': volume_indicators['vwap'].iloc[current_idx] if not volume_indicators['vwap'].empty else None,
                    'volume_ratio': volume_indicators['volume_ratio'].iloc[current_idx] if not volume_indicators['volume_ratio'].empty else None
                }
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
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating indicators for {symbol}: {e}")
            return {}
    
    def generate_basic_signals(self, indicators: Dict) -> Dict:
        """Generate basic trading signals (for backward compatibility)"""
        signals = {
            'trend': 'NEUTRAL',
            'momentum': 'NEUTRAL', 
            'volume': 'NORMAL',
            'overall_signal': 'HOLD',
            'confidence': 0.0,
            'reasons': []
        }
        
        try:
            current_price = indicators['current_price']
            
            # Trend Analysis
            trend_score = 0
            if indicators['sma']['sma_5'] and indicators['sma']['sma_20']:
                if indicators['sma']['sma_5'] > indicators['sma']['sma_20']:
                    trend_score += 1
                    signals['reasons'].append('SMA5 > SMA20 (Bullish)')
                else:
                    trend_score -= 1
                    signals['reasons'].append('SMA5 < SMA20 (Bearish)')
            
            if indicators['ema']['ema_5'] and indicators['ema']['ema_20']:
                if indicators['ema']['ema_5'] > indicators['ema']['ema_20']:
                    trend_score += 1
                    signals['reasons'].append('EMA5 > EMA20 (Bullish)')
                else:
                    trend_score -= 1
                    signals['reasons'].append('EMA5 < EMA20 (Bearish)')
            
            # RSI Analysis
            rsi = indicators.get('rsi')
            if rsi:
                if rsi < 30:
                    trend_score += 1
                    signals['reasons'].append(f'RSI oversold ({rsi:.1f})')
                elif rsi > 70:
                    trend_score -= 1
                    signals['reasons'].append(f'RSI overbought ({rsi:.1f})')
            
            # MACD Analysis
            macd_data = indicators.get('macd', {})
            if macd_data.get('macd') and macd_data.get('signal'):
                if macd_data['macd'] > macd_data['signal']:
                    trend_score += 1
                    signals['reasons'].append('MACD bullish crossover')
                else:
                    trend_score -= 1
                    signals['reasons'].append('MACD bearish crossover')
            
            # Volume Analysis
            volume_data = indicators.get('volume', {})
            if volume_data.get('volume_ratio'):
                if volume_data['volume_ratio'] > 1.5:
                    signals['volume'] = 'HIGH'
                    signals['reasons'].append('High volume activity')
                elif volume_data['volume_ratio'] < 0.5:
                    signals['volume'] = 'LOW'
                    signals['reasons'].append('Low volume activity')
            
            # Set trend
            if trend_score >= 2:
                signals['trend'] = 'BULLISH'
            elif trend_score <= -2:
                signals['trend'] = 'BEARISH'
            
            # Overall signal
            if trend_score >= 3:
                signals['overall_signal'] = 'BUY'
                signals['confidence'] = min(trend_score / 5.0, 1.0)
            elif trend_score <= -3:
                signals['overall_signal'] = 'SELL'  
                signals['confidence'] = min(abs(trend_score) / 5.0, 1.0)
            else:
                signals['overall_signal'] = 'HOLD'
                signals['confidence'] = 0.3
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating basic signals: {e}")
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
            
            return {
                'symbol': symbol,
                'total_signals': len(signals),
                'price_trajectory': price_trajectory,
                'momentum_trajectory': momentum_trajectory,
                'signal_history_length': len(self.indicator_history[symbol]),
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

# Maintain backward compatibility
class TechnicalIndicators(EnhancedTechnicalIndicators):
    """Backward compatible class that extends EnhancedTechnicalIndicators"""
    pass

# Example usage
if __name__ == "__main__":
    # Initialize enhanced technical indicators
    calc = EnhancedTechnicalIndicators()
    
    # Test with sample market data
    sample_market_data = {
        'ltp': 100.50,
        'ltq': 25,
        'best_bid': 100.45,
        'best_ask': 100.55,
        'spread_pct': 0.1
    }
    
    # Generate actionable signals
    signals = calc.generate_actionable_signals('RELIANCE', sample_market_data)
    
    print(f"Generated {len(signals)} actionable signals for RELIANCE")
    for signal in signals:
        print(f"\nSignal: {signal}")