# technical_analyzer.py - Advanced Technical Analysis using TA-Lib

import numpy as np
import pandas as pd
import talib
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

from config import TA_CONFIG, OUTPUT_CONFIG

logger = logging.getLogger(__name__)

class TechnicalAnalyzer:
    def __init__(self):
        self.config = TA_CONFIG
        
    def analyze_stock(self, stock_data: Dict) -> Dict[str, Any]:
        """
        Perform comprehensive technical analysis on stock data
        Returns dense, actionable insights optimized for LLM consumption
        """
        if not stock_data or len(stock_data['close']) < 50:
            logger.warning(f"Insufficient data for analysis: {stock_data.get('symbol', 'Unknown')}")
            return self._empty_analysis(stock_data.get('symbol', 'Unknown'))
        
        # Convert to numpy arrays for TA-Lib
        high = np.array(stock_data['high'], dtype=np.float64)
        low = np.array(stock_data['low'], dtype=np.float64)
        close = np.array(stock_data['close'], dtype=np.float64)
        volume = np.array(stock_data['volume'], dtype=np.float64)
        open_prices = np.array(stock_data['open'], dtype=np.float64)
        
        symbol = stock_data['symbol']
        current_price = close[-1]
        
        try:
            analysis = {
                'symbol': symbol,
                'current_price': round(current_price, OUTPUT_CONFIG['precision']),
                'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format']),
                'data_quality': {
                    'total_candles': len(close),
                    'time_range': f"{stock_data['timestamps'][0]} to {stock_data['timestamps'][-1]}"
                }
            }
            
            # 1. TREND ANALYSIS (30% weight - most important for LLM)
            analysis['trend_analysis'] = self._analyze_trend(high, low, close, open_prices, current_price)
            
            # 2. MOMENTUM ANALYSIS (25% weight)
            analysis['momentum_analysis'] = self._analyze_momentum(high, low, close, current_price)
            
            # 3. VOLATILITY & SUPPORT/RESISTANCE (20% weight)
            analysis['volatility_analysis'] = self._analyze_volatility(high, low, close, current_price)
            
            # 4. VOLUME ANALYSIS (15% weight)
            analysis['volume_analysis'] = self._analyze_volume(close, volume)
            
            # 5. PATTERN RECOGNITION (10% weight)
            analysis['pattern_analysis'] = self._analyze_patterns(open_prices, high, low, close)
            
            # 6. OVERALL SIGNAL SYNTHESIS
            analysis['trading_signals'] = self._synthesize_signals(analysis)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Technical analysis failed for {symbol}: {e}")
            return self._empty_analysis(symbol)
    
    def _analyze_trend(self, high, low, close, open_prices, current_price) -> Dict:
        """Comprehensive trend analysis"""
        try:
            # Moving Averages
            sma_5 = talib.SMA(close, timeperiod=5)
            sma_20 = talib.SMA(close, timeperiod=20)
            sma_50 = talib.SMA(close, timeperiod=50)
            ema_12 = talib.EMA(close, timeperiod=12)
            ema_26 = talib.EMA(close, timeperiod=26)
            
            # MACD
            macd, macd_signal, macd_hist = talib.MACD(close, 
                                                     fastperiod=self.config['macd_fast'],
                                                     slowperiod=self.config['macd_slow'], 
                                                     signalperiod=self.config['macd_signal'])
            
            # ADX for trend strength
            adx = talib.ADX(high, low, close, timeperiod=self.config['adx_period'])
            
            # Parabolic SAR
            sar = talib.SAR(high, low, acceleration=0.02, maximum=0.2)
            
            # Current trend determination
            ma_trend = "BULLISH" if current_price > sma_20[-1] > sma_50[-1] else "BEARISH"
            macd_trend = "BULLISH" if macd[-1] > macd_signal[-1] else "BEARISH"
            sar_trend = "BULLISH" if current_price > sar[-1] else "BEARISH"
            
            # Trend strength (0-100)
            trend_strength = min(100, max(0, adx[-1])) if not np.isnan(adx[-1]) else 50
            
            return {
                'primary_trend': ma_trend,
                'trend_strength': round(trend_strength, 2),
                'trend_quality': 'STRONG' if trend_strength > 25 else 'WEAK',
                'moving_averages': {
                    'sma_5': round(sma_5[-1], 2) if not np.isnan(sma_5[-1]) else None,
                    'sma_20': round(sma_20[-1], 2) if not np.isnan(sma_20[-1]) else None,
                    'sma_50': round(sma_50[-1], 2) if not np.isnan(sma_50[-1]) else None,
                    'position_vs_sma20': round(((current_price - sma_20[-1]) / sma_20[-1]) * 100, 2) if not np.isnan(sma_20[-1]) else None
                },
                'macd': {
                    'signal': macd_trend,
                    'macd_line': round(macd[-1], 4) if not np.isnan(macd[-1]) else None,
                    'signal_line': round(macd_signal[-1], 4) if not np.isnan(macd_signal[-1]) else None,
                    'histogram': round(macd_hist[-1], 4) if not np.isnan(macd_hist[-1]) else None,
                    'crossover_recent': self._detect_crossover(macd[-5:], macd_signal[-5:])
                },
                'parabolic_sar': {
                    'signal': sar_trend,
                    'sar_level': round(sar[-1], 2) if not np.isnan(sar[-1]) else None,
                    'distance_percent': round(((current_price - sar[-1]) / current_price) * 100, 2) if not np.isnan(sar[-1]) else None
                }
            }
        except Exception as e:
            logger.error(f"Trend analysis error: {e}")
            return {'error': 'Trend analysis failed'}
    
    def _analyze_momentum(self, high, low, close, current_price) -> Dict:
        """Momentum indicators analysis"""
        try:
            # RSI
            rsi = talib.RSI(close, timeperiod=self.config['rsi_period'])
            
            # Stochastic
            stoch_k, stoch_d = talib.STOCH(high, low, close, 
                                          fastk_period=self.config['stoch_k_period'],
                                          slowk_period=3, slowd_period=self.config['stoch_d_period'])
            
            # Williams %R
            williams_r = talib.WILLR(high, low, close, timeperiod=self.config['williams_r_period'])
            
            # CCI
            cci = talib.CCI(high, low, close, timeperiod=self.config['cci_period'])
            
            # Rate of Change
            roc = talib.ROC(close, timeperiod=10)
            
            # Momentum classification
            rsi_current = rsi[-1] if not np.isnan(rsi[-1]) else 50
            stoch_current = stoch_k[-1] if not np.isnan(stoch_k[-1]) else 50
            
            momentum_score = (
                (rsi_current - 50) + 
                (stoch_current - 50) + 
                (williams_r[-1] + 50 if not np.isnan(williams_r[-1]) else 0)
            ) / 3
            
            momentum_signal = 'BULLISH' if momentum_score > 10 else 'BEARISH' if momentum_score < -10 else 'NEUTRAL'
            
            return {
                'overall_momentum': momentum_signal,
                'momentum_score': round(momentum_score, 2),
                'rsi': {
                    'current': round(rsi_current, 2),
                    'signal': 'OVERBOUGHT' if rsi_current > 70 else 'OVERSOLD' if rsi_current < 30 else 'NEUTRAL',
                    'divergence_risk': 'HIGH' if rsi_current > 80 or rsi_current < 20 else 'LOW'
                },
                'stochastic': {
                    'k_line': round(stoch_current, 2),
                    'd_line': round(stoch_d[-1], 2) if not np.isnan(stoch_d[-1]) else None,
                    'signal': 'OVERBOUGHT' if stoch_current > 80 else 'OVERSOLD' if stoch_current < 20 else 'NEUTRAL'
                },
                'williams_r': {
                    'current': round(williams_r[-1], 2) if not np.isnan(williams_r[-1]) else None,
                    'signal': 'OVERBOUGHT' if williams_r[-1] > -20 else 'OVERSOLD' if williams_r[-1] < -80 else 'NEUTRAL'
                },
                'rate_of_change': {
                    'roc_10d': round(roc[-1], 2) if not np.isnan(roc[-1]) else None,
                    'momentum_direction': 'POSITIVE' if roc[-1] > 0 else 'NEGATIVE' if not np.isnan(roc[-1]) else 'NEUTRAL'
                }
            }
        except Exception as e:
            logger.error(f"Momentum analysis error: {e}")
            return {'error': 'Momentum analysis failed'}
    
    def _analyze_volatility(self, high, low, close, current_price) -> Dict:
        """Volatility and support/resistance analysis"""
        try:
            # Bollinger Bands
            bb_upper, bb_middle, bb_lower = talib.BBANDS(close, 
                                                        timeperiod=self.config['bb_period'],
                                                        nbdevup=self.config['bb_std'],
                                                        nbdevdn=self.config['bb_std'])
            
            # ATR for volatility
            atr = talib.ATR(high, low, close, timeperiod=self.config['atr_period'])
            
            # Keltner Channels
            kc_middle = talib.EMA(close, timeperiod=20)
            atr_20 = talib.ATR(high, low, close, timeperiod=20)
            kc_upper = kc_middle + (2 * atr_20)
            kc_lower = kc_middle - (2 * atr_20)
            
            # Current position analysis
            bb_position = ((current_price - bb_lower[-1]) / (bb_upper[-1] - bb_lower[-1])) * 100 if not np.isnan(bb_upper[-1]) else 50
            
            # Volatility level
            atr_current = atr[-1] if not np.isnan(atr[-1]) else 0
            volatility_percent = (atr_current / current_price) * 100
            
            # Support and Resistance levels
            recent_highs = high[-20:]
            recent_lows = low[-20:]
            resistance_level = np.max(recent_highs) if len(recent_highs) > 0 else current_price
            support_level = np.min(recent_lows) if len(recent_lows) > 0 else current_price
            
            return {
                'volatility_level': 'HIGH' if volatility_percent > 3 else 'MEDIUM' if volatility_percent > 1.5 else 'LOW',
                'volatility_percent': round(volatility_percent, 2),
                'atr': round(atr_current, 2),
                'bollinger_bands': {
                    'upper': round(bb_upper[-1], 2) if not np.isnan(bb_upper[-1]) else None,
                    'middle': round(bb_middle[-1], 2) if not np.isnan(bb_middle[-1]) else None,
                    'lower': round(bb_lower[-1], 2) if not np.isnan(bb_lower[-1]) else None,
                    'position_percent': round(bb_position, 2),
                    'squeeze_signal': 'SQUEEZE' if (bb_upper[-1] - bb_lower[-1]) < (kc_upper[-1] - kc_lower[-1]) else 'EXPANSION'
                },
                'support_resistance': {
                    'nearest_support': round(support_level, 2),
                    'nearest_resistance': round(resistance_level, 2),
                    'support_distance_percent': round(((current_price - support_level) / current_price) * 100, 2),
                    'resistance_distance_percent': round(((resistance_level - current_price) / current_price) * 100, 2)
                }
            }
        except Exception as e:
            logger.error(f"Volatility analysis error: {e}")
            return {'error': 'Volatility analysis failed'}
    
    def _analyze_volume(self, close, volume) -> Dict:
        """Volume analysis"""
        try:
            # Volume SMA
            volume_sma = talib.SMA(volume, timeperiod=self.config['volume_sma_period'])
            
            # On-Balance Volume
            obv = talib.OBV(close, volume)
            
            # Volume Rate of Change
            volume_roc = talib.ROC(volume, timeperiod=10)
            
            # Current volume analysis
            current_volume = volume[-1]
            avg_volume = volume_sma[-1] if not np.isnan(volume_sma[-1]) else current_volume
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            
            # OBV trend
            obv_trend = 'BULLISH' if obv[-1] > obv[-5] else 'BEARISH'
            
            return {
                'volume_signal': 'HIGH' if volume_ratio > 1.5 else 'LOW' if volume_ratio < 0.7 else 'NORMAL',
                'current_volume': int(current_volume),
                'average_volume': int(avg_volume),
                'volume_ratio': round(volume_ratio, 2),
                'obv_trend': obv_trend,
                'obv_current': round(obv[-1], 0) if not np.isnan(obv[-1]) else None,
                'volume_momentum': 'INCREASING' if volume_roc[-1] > 0 else 'DECREASING' if not np.isnan(volume_roc[-1]) else 'STABLE'
            }
        except Exception as e:
            logger.error(f"Volume analysis error: {e}")
            return {'error': 'Volume analysis failed'}
    
    def _analyze_patterns(self, open_prices, high, low, close) -> Dict:
        """Candlestick and pattern recognition"""
        try:
            patterns = {}
            
            # Major bullish patterns
            patterns['hammer'] = talib.CDLHAMMER(open_prices, high, low, close)
            patterns['engulfing_bullish'] = talib.CDLENGULFING(open_prices, high, low, close)
            patterns['morning_star'] = talib.CDLMORNINGSTAR(open_prices, high, low, close)
            patterns['piercing'] = talib.CDLPIERCING(open_prices, high, low, close)
            
            # Major bearish patterns
            patterns['hanging_man'] = talib.CDLHANGINGMAN(open_prices, high, low, close)
            patterns['dark_cloud'] = talib.CDLDARKCLOUDCOVER(open_prices, high, low, close)
            patterns['evening_star'] = talib.CDLEVENINGSTAR(open_prices, high, low, close)
            patterns['shooting_star'] = talib.CDLSHOOTINGSTAR(open_prices, high, low, close)
            
            # Doji patterns
            patterns['doji'] = talib.CDLDOJI(open_prices, high, low, close)
            patterns['dragonfly_doji'] = talib.CDLDRAGONFLYDOJI(open_prices, high, low, close)
            patterns['gravestone_doji'] = talib.CDLGRAVESTONEDOJI(open_prices, high, low, close)
            
            # Recent pattern detection (last 5 candles)
            recent_patterns = []
            for pattern_name, pattern_values in patterns.items():
                if len(pattern_values) > 5:
                    recent_signals = pattern_values[-5:]
                    if np.any(recent_signals != 0):
                        signal_strength = 'STRONG' if np.any(np.abs(recent_signals) > 100) else 'WEAK'
                        signal_type = 'BULLISH' if np.any(recent_signals > 0) else 'BEARISH'
                        recent_patterns.append({
                            'pattern': pattern_name,
                            'signal': signal_type,
                            'strength': signal_strength,
                            'candles_ago': int(np.argmax(np.abs(recent_signals)))
                        })
            
            return {
                'recent_patterns': recent_patterns[:3],  # Top 3 most recent patterns
                'pattern_count': len(recent_patterns),
                'dominant_pattern_signal': self._get_dominant_pattern_signal(recent_patterns)
            }
        except Exception as e:
            logger.error(f"Pattern analysis error: {e}")
            return {'error': 'Pattern analysis failed'}
    
    def _synthesize_signals(self, analysis) -> Dict:
        """Synthesize all signals into actionable trading signals"""
        try:
            signals = []
            confidence_score = 0
            
            # Trend signals (40% weight)
            if analysis.get('trend_analysis', {}).get('primary_trend') == 'BULLISH':
                signals.append('BULLISH_TREND')
                confidence_score += 40
            elif analysis.get('trend_analysis', {}).get('primary_trend') == 'BEARISH':
                signals.append('BEARISH_TREND')
                confidence_score -= 40
            
            # Momentum signals (30% weight)
            momentum_signal = analysis.get('momentum_analysis', {}).get('overall_momentum')
            if momentum_signal == 'BULLISH':
                signals.append('BULLISH_MOMENTUM')
                confidence_score += 30
            elif momentum_signal == 'BEARISH':
                signals.append('BEARISH_MOMENTUM')
                confidence_score -= 30
            
            # Volume confirmation (20% weight)
            volume_signal = analysis.get('volume_analysis', {}).get('volume_signal')
            if volume_signal == 'HIGH':
                signals.append('HIGH_VOLUME_CONFIRMATION')
                confidence_score += 20
            
            # Pattern signals (10% weight)
            pattern_signal = analysis.get('pattern_analysis', {}).get('dominant_pattern_signal')
            if pattern_signal == 'BULLISH':
                signals.append('BULLISH_PATTERNS')
                confidence_score += 10
            elif pattern_signal == 'BEARISH':
                signals.append('BEARISH_PATTERNS')
                confidence_score -= 10
            
            # Overall signal determination
            overall_signal = 'BULLISH' if confidence_score > 20 else 'BEARISH' if confidence_score < -20 else 'NEUTRAL'
            confidence_level = 'HIGH' if abs(confidence_score) > 60 else 'MEDIUM' if abs(confidence_score) > 30 else 'LOW'
            
            return {
                'overall_signal': overall_signal,
                'confidence_level': confidence_level,
                'confidence_score': confidence_score,
                'active_signals': signals,
                'signal_summary': f"{overall_signal} with {confidence_level} confidence ({confidence_score}% score)"
            }
        except Exception as e:
            logger.error(f"Signal synthesis error: {e}")
            return {'error': 'Signal synthesis failed'}
    
    def _detect_crossover(self, line1, line2) -> bool:
        """Detect recent crossover between two lines"""
        if len(line1) < 2 or len(line2) < 2:
            return False
        
        # Check if there was a crossover in recent candles
        for i in range(1, len(line1)):
            if not (np.isnan(line1[i]) or np.isnan(line2[i]) or np.isnan(line1[i-1]) or np.isnan(line2[i-1])):
                if (line1[i-1] <= line2[i-1] and line1[i] > line2[i]) or \
                   (line1[i-1] >= line2[i-1] and line1[i] < line2[i]):
                    return True
        return False
    
    def _get_dominant_pattern_signal(self, patterns) -> str:
        """Determine dominant pattern signal"""
        if not patterns:
            return 'NEUTRAL'
        
        bullish_count = sum(1 for p in patterns if p['signal'] == 'BULLISH')
        bearish_count = sum(1 for p in patterns if p['signal'] == 'BEARISH')
        
        if bullish_count > bearish_count:
            return 'BULLISH'
        elif bearish_count > bullish_count:
            return 'BEARISH'
        else:
            return 'NEUTRAL'
    
    def _empty_analysis(self, symbol) -> Dict:
        """Return empty analysis structure for failed cases"""
        return {
            'symbol': symbol,
            'error': 'Insufficient data or analysis failed',
            'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format']),
            'trading_signals': {
                'overall_signal': 'NEUTRAL',
                'confidence_level': 'LOW',
                'confidence_score': 0,
                'signal_summary': 'Analysis failed - insufficient data'
            }
        }

# Batch analysis function
class BatchTechnicalAnalyzer:
    def __init__(self):
        self.analyzer = TechnicalAnalyzer()
    
    def analyze_multiple_stocks(self, stocks_data: Dict[str, Dict]) -> Dict[str, Dict]:
        """Analyze multiple stocks and return consolidated results"""
        results = {}
        
        logger.info(f"Starting technical analysis for {len(stocks_data)} stocks")
        
        for symbol, stock_data in stocks_data.items():
            try:
                analysis = self.analyzer.analyze_stock(stock_data)
                results[symbol] = analysis
                logger.info(f"Completed analysis for {symbol}: {analysis.get('trading_signals', {}).get('overall_signal', 'UNKNOWN')}")
            except Exception as e:
                logger.error(f"Failed to analyze {symbol}: {e}")
                results[symbol] = self.analyzer._empty_analysis(symbol)
        
        return results
    
    def get_market_summary(self, analysis_results: Dict[str, Dict]) -> Dict:
        """Generate market-wide summary from individual stock analyses"""
        total_stocks = len(analysis_results)
        bullish_stocks = sum(1 for analysis in analysis_results.values() 
                           if analysis.get('trading_signals', {}).get('overall_signal') == 'BULLISH')
        bearish_stocks = sum(1 for analysis in analysis_results.values() 
                           if analysis.get('trading_signals', {}).get('overall_signal') == 'BEARISH')
        neutral_stocks = total_stocks - bullish_stocks - bearish_stocks
        
        market_sentiment = 'BULLISH' if bullish_stocks > bearish_stocks else 'BEARISH' if bearish_stocks > bullish_stocks else 'MIXED'
        
        return {
            'market_sentiment': market_sentiment,
            'total_analyzed': total_stocks,
            'bullish_count': bullish_stocks,
            'bearish_count': bearish_stocks,
            'neutral_count': neutral_stocks,
            'bullish_percentage': round((bullish_stocks / total_stocks) * 100, 1) if total_stocks > 0 else 0,
            'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format'])
        }

# Convenience functions
def analyze_single_stock(stock_data: Dict) -> Dict:
    """Analyze a single stock"""
    analyzer = TechnicalAnalyzer()
    return analyzer.analyze_stock(stock_data)

def analyze_multiple_stocks(stocks_data: Dict[str, Dict]) -> Dict[str, Dict]:
    """Analyze multiple stocks"""
    batch_analyzer = BatchTechnicalAnalyzer()
    return batch_analyzer.analyze_multiple_stocks(stocks_data)

# Main execution for testing
if __name__ == "__main__":
    # This would be used with data from moneycontrol_fetcher
    print("Technical Analyzer ready. Use with moneycontrol_fetcher data.")
    print("Example usage:")
    print("from moneycontrol_fetcher import get_stock_data")
    print("from technical_analyzer import analyze_single_stock")
    print("stock_data = await get_stock_data('TCS')")
    print("analysis = analyze_single_stock(stock_data)")