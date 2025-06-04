# main_stock_analyzer.py - Complete stock analysis integration

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import os

from config import STOCK_SYMBOLS, OUTPUT_CONFIG
from technical_analyzer import TechnicalAnalyzer
from yfinance_macro_analyzer import MacroAnalyzer, get_macro_summary
from moneycontrol_fetcher import MoneyControlFetcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ComprehensiveStockAnalyzer:
    def __init__(self):
        self.technical_analyzer = TechnicalAnalyzer()
        self.macro_analyzer = MacroAnalyzer()
        self.moneycontrol_fetcher = MoneyControlFetcher()
        
    def analyze_single_stock(self, symbol: str) -> Dict[str, Any]:
        """
        Complete analysis of a single stock combining all data sources
        Returns LLM-optimized JSON with 90% technical + 10% macro context
        """
        start_time = time.time()
        
        try:
            # Initialize result structure
            analysis = {
                'symbol': symbol,
                'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format']),
                'data_sources': ['yfinance_technical', 'yfinance_macro', 'moneycontrol'],
                'current_trend': {},
                'macro_context': {},
                'additional_data': {},
                'analysis_summary': {}
            }
            
            # 1. Technical Analysis (Primary - 90% weight)
            logger.info(f"Starting technical analysis for {symbol}")
            technical_data = self.technical_analyzer.analyze_stock(symbol)
            
            if technical_data and 'error' not in technical_data:
                analysis['current_trend'] = {
                    'price_action': {
                        'current_price': technical_data.get('current_price'),
                        'price_change': technical_data.get('price_change_percent'),
                        'trend_direction': self._extract_trend_direction(technical_data),
                        'support_resistance': self._extract_support_resistance(technical_data)
                    },
                    'technical_signals': self._extract_technical_signals(technical_data),
                    'momentum': self._extract_momentum_data(technical_data),
                    'trend_strength': self._calculate_trend_strength(technical_data),
                    'volume_analysis': self._extract_volume_data(technical_data)
                }
            else:
                analysis['current_trend']['error'] = 'Technical analysis failed'
            
            # 2. Macro Analysis (Secondary - 10% weight)  
            logger.info(f"Starting macro analysis for {symbol}")
            macro_data = self.macro_analyzer.analyze_stock_macro(symbol)
            
            if macro_data and 'error' not in macro_data:
                analysis['macro_context'] = {
                    'position_vs_52week': self._extract_52week_position(macro_data),
                    'recent_performance': self._extract_recent_performance(macro_data),
                    'volatility_profile': self._extract_volatility_data(macro_data),
                    'market_positioning': self._extract_market_positioning(macro_data),
                    'risk_metrics': self._extract_risk_metrics(macro_data)
                }
            else:
                analysis['macro_context']['error'] = 'Macro analysis failed'
            
            # 3. MoneyControl Data (Additional context)
            logger.info(f"Fetching MoneyControl data for {symbol}")
            try:
                mc_data = self.moneycontrol_fetcher.get_stock_data(symbol)
                if mc_data:
                    analysis['additional_data']['moneycontrol'] = {
                        'fundamentals': mc_data.get('fundamentals', {}),
                        'news_sentiment': mc_data.get('news', [])[:3],  # Top 3 news items
                        'peer_comparison': mc_data.get('peers', {})
                    }
            except Exception as e:
                logger.warning(f"MoneyControl data fetch failed for {symbol}: {e}")
                analysis['additional_data']['moneycontrol'] = {'error': 'Data fetch failed'}
            
            # 4. Generate Analysis Summary
            analysis['analysis_summary'] = self._generate_analysis_summary(
                analysis['current_trend'], 
                analysis['macro_context']
            )
            
            # 5. Calculate processing time
            processing_time = time.time() - start_time
            analysis['processing_time_seconds'] = round(processing_time, 2)
            
            logger.info(f"Completed analysis for {symbol} in {processing_time:.2f} seconds")
            return analysis
            
        except Exception as e:
            logger.error(f"Complete analysis failed for {symbol}: {e}")
            return {
                'symbol': symbol,
                'error': f'Complete analysis failed: {str(e)}',
                'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format'])
            }
    
    def analyze_batch_stocks(self, symbols: List[str], max_workers: int = 5) -> Dict[str, Dict]:
        """
        Analyze multiple stocks concurrently
        """
        start_time = time.time()
        results = {}
        
        logger.info(f"Starting batch analysis for {len(symbols)} stocks")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all analysis tasks
            future_to_symbol = {
                executor.submit(self.analyze_single_stock, symbol): symbol 
                for symbol in symbols
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result(timeout=60)  # 60 second timeout per stock
                    results[symbol] = result
                    logger.info(f"✓ Completed analysis for {symbol}")
                except Exception as e:
                    logger.error(f"✗ Failed analysis for {symbol}: {e}")
                    results[symbol] = {
                        'symbol': symbol,
                        'error': f'Analysis timeout or failed: {str(e)}',
                        'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format'])
                    }
        
        total_time = time.time() - start_time
        logger.info(f"Batch analysis completed in {total_time:.2f} seconds")
        
        # Add batch metadata
        batch_summary = {
            'batch_metadata': {
                'total_stocks': len(symbols),
                'successful_analyses': len([r for r in results.values() if 'error' not in r]),
                'failed_analyses': len([r for r in results.values() if 'error' in r]),
                'total_processing_time': round(total_time, 2),
                'timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format'])
            },
            'stock_analyses': results
        }
        
        return batch_summary
    
    # Helper methods for data extraction and processing
    
    def _extract_trend_direction(self, technical_data: Dict) -> str:
        """Extract overall trend direction"""
        if 'trend_analysis' in technical_data:
            return technical_data['trend_analysis'].get('overall_trend', 'NEUTRAL')
        
        # Fallback: determine from moving averages
        if 'indicators' in technical_data:
            indicators = technical_data['indicators']
            current_price = technical_data.get('current_price', 0)
            
            sma_20 = indicators.get('SMA_20', 0)
            sma_50 = indicators.get('SMA_50', 0)
            
            if current_price > sma_20 > sma_50:
                return 'UPTREND'
            elif current_price < sma_20 < sma_50:
                return 'DOWNTREND'
        
        return 'NEUTRAL'
    
    def _extract_support_resistance(self, technical_data: Dict) -> Dict:
        """Extract support and resistance levels"""
        if 'support_resistance' in technical_data:
            return technical_data['support_resistance']
        
        # Fallback: calculate from pivot points or bollinger bands
        result = {'support': [], 'resistance': []}
        
        if 'indicators' in technical_data:
            indicators = technical_data['indicators']
            
            # From Bollinger Bands
            bb_lower = indicators.get('BB_LOWER')
            bb_upper = indicators.get('BB_UPPER')
            if bb_lower and bb_upper:
                result['support'].append(bb_lower)
                result['resistance'].append(bb_upper)
            
            # From Pivot Points if available
            pivot = indicators.get('PIVOT')
            if pivot:
                result['support'].append(pivot)
                result['resistance'].append(pivot)
        
        return result
    
    def _extract_technical_signals(self, technical_data: Dict) -> Dict:
        """Extract key technical signals"""
        signals = {
            'macd_signal': 'NEUTRAL',
            'rsi_signal': 'NEUTRAL',
            'bollinger_signal': 'NEUTRAL',
            'moving_average_signal': 'NEUTRAL',
            'overall_signal': 'NEUTRAL'
        }
        
        if 'indicators' in technical_data:
            indicators = technical_data['indicators']
            
            # MACD Signal
            macd = indicators.get('MACD')
            macd_signal = indicators.get('MACD_SIGNAL')
            if macd and macd_signal:
                if macd > macd_signal:
                    signals['macd_signal'] = 'BULLISH'
                elif macd < macd_signal:
                    signals['macd_signal'] = 'BEARISH'
            
            # RSI Signal
            rsi = indicators.get('RSI')
            if rsi:
                if rsi > 70:
                    signals['rsi_signal'] = 'OVERBOUGHT'
                elif rsi < 30:
                    signals['rsi_signal'] = 'OVERSOLD'
                elif rsi > 50:
                    signals['rsi_signal'] = 'BULLISH'
                else:
                    signals['rsi_signal'] = 'BEARISH'
            
            # Bollinger Bands Signal
            current_price = technical_data.get('current_price', 0)
            bb_upper = indicators.get('BB_UPPER')
            bb_lower = indicators.get('BB_LOWER')
            if current_price and bb_upper and bb_lower:
                if current_price > bb_upper:
                    signals['bollinger_signal'] = 'OVERBOUGHT'
                elif current_price < bb_lower:
                    signals['bollinger_signal'] = 'OVERSOLD'
            
            # Moving Average Signal
            sma_20 = indicators.get('SMA_20')
            sma_50 = indicators.get('SMA_50')
            if current_price and sma_20 and sma_50:
                if current_price > sma_20 > sma_50:
                    signals['moving_average_signal'] = 'BULLISH'
                elif current_price < sma_20 < sma_50:
                    signals['moving_average_signal'] = 'BEARISH'
        
        # Calculate overall signal
        bullish_count = sum(1 for signal in signals.values() if signal in ['BULLISH', 'OVERSOLD'])
        bearish_count = sum(1 for signal in signals.values() if signal in ['BEARISH', 'OVERBOUGHT'])
        
        if bullish_count > bearish_count:
            signals['overall_signal'] = 'BULLISH'
        elif bearish_count > bullish_count:
            signals['overall_signal'] = 'BEARISH'
        
        return signals
    
    def _extract_momentum_data(self, technical_data: Dict) -> Dict:
        """Extract momentum indicators"""
        momentum = {
            'rsi': None,
            'stochastic': None,
            'williams_r': None,
            'momentum_score': 'NEUTRAL'
        }
        
        if 'indicators' in technical_data:
            indicators = technical_data['indicators']
            momentum['rsi'] = indicators.get('RSI')
            momentum['stochastic'] = indicators.get('STOCH_K')
            momentum['williams_r'] = indicators.get('WILLIAMS_R')
            
            # Calculate momentum score
            momentum_indicators = [v for v in [momentum['rsi'], momentum['stochastic']] if v is not None]
            if momentum_indicators:
                avg_momentum = sum(momentum_indicators) / len(momentum_indicators)
                if avg_momentum > 60:
                    momentum['momentum_score'] = 'STRONG_BULLISH'
                elif avg_momentum > 50:
                    momentum['momentum_score'] = 'BULLISH'
                elif avg_momentum < 40:
                    momentum['momentum_score'] = 'BEARISH'
                elif avg_momentum < 30:
                    momentum['momentum_score'] = 'STRONG_BEARISH'
        
        return momentum
    
    def _calculate_trend_strength(self, technical_data: Dict) -> Dict:
        """Calculate trend strength metrics"""
        strength = {
            'adx': None,
            'trend_strength': 'WEAK',
            'trend_consistency': 'LOW'
        }
        
        if 'indicators' in technical_data:
            indicators = technical_data['indicators']
            adx = indicators.get('ADX')
            
            if adx:
                strength['adx'] = adx
                if adx > 25:
                    strength['trend_strength'] = 'STRONG'
                elif adx > 20:
                    strength['trend_strength'] = 'MODERATE'
                
                # Calculate trend consistency based on multiple timeframes
                sma_20 = indicators.get('SMA_20')
                sma_50 = indicators.get('SMA_50')
                current_price = technical_data.get('current_price', 0)
                
                if current_price and sma_20 and sma_50:
                    if (current_price > sma_20 > sma_50) or (current_price < sma_20 < sma_50):
                        strength['trend_consistency'] = 'HIGH'
                    elif (current_price > sma_20) == (sma_20 > sma_50):
                        strength['trend_consistency'] = 'MEDIUM'
        
        return strength
    
    def _extract_volume_data(self, technical_data: Dict) -> Dict:
        """Extract volume analysis"""
        volume = {
            'current_volume': None,
            'volume_sma': None,
            'volume_trend': 'NORMAL',
            'obv': None
        }
        
        if 'volume_analysis' in technical_data:
            vol_data = technical_data['volume_analysis']
            volume.update(vol_data)
        elif 'indicators' in technical_data:
            indicators = technical_data['indicators']
            volume['obv'] = indicators.get('OBV')
            volume['current_volume'] = technical_data.get('current_volume')
            
            # Determine volume trend
            if volume['current_volume'] and 'volume_sma' in indicators:
                vol_sma = indicators['volume_sma']
                if volume['current_volume'] > vol_sma * 1.5:
                    volume['volume_trend'] = 'HIGH'
                elif volume['current_volume'] < vol_sma * 0.5:
                    volume['volume_trend'] = 'LOW'
        
        return volume
    
    def _extract_52week_position(self, macro_data: Dict) -> Dict:
        """Extract 52-week positioning data"""
        position = {
            'current_price': None,
            'week_52_high': None,
            'week_52_low': None,
            'position_percentage': None,
            'position_status': 'UNKNOWN'
        }
        
        if 'price_data' in macro_data:
            price_data = macro_data['price_data']
            position.update({
                'current_price': price_data.get('current_price'),
                'week_52_high': price_data.get('52_week_high'),
                'week_52_low': price_data.get('52_week_low')
            })
            
            # Calculate position percentage
            if all([position['current_price'], position['week_52_high'], position['week_52_low']]):
                high = position['week_52_high']
                low = position['week_52_low']
                current = position['current_price']
                
                position['position_percentage'] = round(((current - low) / (high - low)) * 100, 2)
                
                if position['position_percentage'] > 80:
                    position['position_status'] = 'NEAR_HIGH'
                elif position['position_percentage'] < 20:
                    position['position_status'] = 'NEAR_LOW'
                else:
                    position['position_status'] = 'MIDDLE_RANGE'
        
        return position
    
    def _extract_recent_performance(self, macro_data: Dict) -> Dict:
        """Extract recent performance metrics"""
        performance = {
            '1_day': None,
            '5_day': None,
            '1_month': None,
            '3_month': None,
            'performance_trend': 'NEUTRAL'
        }
        
        if 'performance' in macro_data:
            perf_data = macro_data['performance']
            performance.update(perf_data)
            
            # Determine performance trend
            recent_changes = [v for v in [performance['1_day'], performance['5_day']] if v is not None]
            if recent_changes:
                avg_recent = sum(recent_changes) / len(recent_changes)
                if avg_recent > 2:
                    performance['performance_trend'] = 'STRONG_POSITIVE'
                elif avg_recent > 0:
                    performance['performance_trend'] = 'POSITIVE'
                elif avg_recent < -2:
                    performance['performance_trend'] = 'STRONG_NEGATIVE'
                elif avg_recent < 0:
                    performance['performance_trend'] = 'NEGATIVE'
        
        return performance
    
    def _extract_volatility_data(self, macro_data: Dict) -> Dict:
        """Extract volatility metrics"""
        volatility = {
            'beta': None,
            'volatility_30d': None,
            'volatility_90d': None,
            'volatility_classification': 'UNKNOWN'
        }
        
        if 'risk_metrics' in macro_data:
            risk_data = macro_data['risk_metrics']
            volatility.update(risk_data)
            
            # Classify volatility
            if volatility['volatility_30d']:
                vol_30d = volatility['volatility_30d']
                if vol_30d > 0.3:
                    volatility['volatility_classification'] = 'HIGH'
                elif vol_30d > 0.2:
                    volatility['volatility_classification'] = 'MODERATE'
                else:
                    volatility['volatility_classification'] = 'LOW'
        
        return volatility
    
    def _extract_market_positioning(self, macro_data: Dict) -> Dict:
        """Extract market positioning data"""
        positioning = {
            'market_cap': None,
            'sector': None,
            'relative_strength': None,
            'market_position': 'UNKNOWN'
        }
        
        if 'company_info' in macro_data:
            company_info = macro_data['company_info']
            positioning.update({
                'market_cap': company_info.get('market_cap'),
                'sector': company_info.get('sector'),
                'relative_strength': company_info.get('relative_strength')
            })
        
        return positioning
    
    def _extract_risk_metrics(self, macro_data: Dict) -> Dict:
        """Extract risk assessment metrics"""
        risk = {
            'beta': None,
            'sharpe_ratio': None,
            'max_drawdown': None,
            'risk_level': 'MODERATE'
        }
        
        if 'risk_metrics' in macro_data:
            risk_data = macro_data['risk_metrics']
            risk.update(risk_data)
            
            # Determine risk level
            if risk['beta']:
                beta = risk['beta']
                if beta > 1.5:
                    risk['risk_level'] = 'HIGH'
                elif beta > 1.2:
                    risk['risk_level'] = 'MODERATE_HIGH'
                elif beta < 0.8:
                    risk['risk_level'] = 'LOW'
                else:
                    risk['risk_level'] = 'MODERATE'
        
        return risk
    
    def _generate_analysis_summary(self, current_trend: Dict, macro_context: Dict) -> Dict:
        """Generate comprehensive analysis summary"""
        summary = {
            'overall_sentiment': 'NEUTRAL',
            'confidence_score': 0,
            'key_signals': [],
            'risk_factors': [],
            'opportunities': [],
            'recommendation': 'HOLD'
        }
        
        # Analyze technical signals
        technical_signals = current_trend.get('technical_signals', {})
        momentum_data = current_trend.get('momentum', {})
        
        bullish_signals = 0
        bearish_signals = 0
        
        # Count bullish/bearish signals
        for signal_type, signal_value in technical_signals.items():
            if signal_value in ['BULLISH', 'OVERSOLD']:
                bullish_signals += 1
                summary['key_signals'].append(f"{signal_type}: {signal_value}")
            elif signal_value in ['BEARISH', 'OVERBOUGHT']:
                bearish_signals += 1
                summary['key_signals'].append(f"{signal_type}: {signal_value}")
        
        # Analyze macro context
        position_data = macro_context.get('position_vs_52week', {})
        performance_data = macro_context.get('recent_performance', {})
        
        # Position analysis
        if position_data.get('position_status') == 'NEAR_HIGH':
            summary['risk_factors'].append('Trading near 52-week high')
        elif position_data.get('position_status') == 'NEAR_LOW':
            summary['opportunities'].append('Trading near 52-week low')
        
        # Performance analysis
        if performance_data.get('performance_trend') in ['STRONG_POSITIVE', 'POSITIVE']:
            bullish_signals += 1
            summary['opportunities'].append('Positive recent performance trend')
        elif performance_data.get('performance_trend') in ['STRONG_NEGATIVE', 'NEGATIVE']:
            bearish_signals += 1
            summary['risk_factors'].append('Negative recent performance trend')
        
        # Calculate overall sentiment
        if bullish_signals > bearish_signals + 1:
            summary['overall_sentiment'] = 'BULLISH'
            summary['recommendation'] = 'BUY'
        elif bearish_signals > bullish_signals + 1:
            summary['overall_sentiment'] = 'BEARISH'
            summary['recommendation'] = 'SELL'
        else:
            summary['overall_sentiment'] = 'NEUTRAL'
            summary['recommendation'] = 'HOLD'
        
        # Calculate confidence score (0-100)
        total_signals = bullish_signals + bearish_signals
        if total_signals > 0:
            signal_strength = abs(bullish_signals - bearish_signals)
            summary['confidence_score'] = min(100, (signal_strength / total_signals) * 100)
        
        return summary
    
    def save_analysis_to_file(self, analysis_data: Dict, filename: Optional[str] = None) -> str:
        """Save analysis results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"stock_analysis_{timestamp}.json"
        
        try:
            os.makedirs('analysis_output', exist_ok=True)
            filepath = os.path.join('analysis_output', filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(analysis_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Analysis saved to: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to save analysis: {e}")
            return ""

def main():
    """Main execution function"""
    logger.info("Starting Comprehensive Stock Analysis System")
    
    # Initialize analyzer
    analyzer = ComprehensiveStockAnalyzer()
    
    # Get stocks to analyze from config
    stocks_to_analyze = STOCK_SYMBOLS[:10]  # Analyze first 10 stocks
    
    # Perform batch analysis
    logger.info(f"Analyzing {len(stocks_to_analyze)} stocks: {stocks_to_analyze}")
    
    batch_results = analyzer.analyze_batch_stocks(
        symbols=stocks_to_analyze,
        max_workers=3  # Adjust based on your system capacity
    )
    
    # Save results
    output_file = analyzer.save_analysis_to_file(batch_results)
    
    # Print summary
    metadata = batch_results.get('batch_metadata', {})
    logger.info(f"""
    Analysis Complete!
    ==================
    Total Stocks: {metadata.get('total_stocks', 0)}
    Successful: {metadata.get('successful_analyses', 0)}
    Failed: {metadata.get('failed_analyses', 0)}
    Total Time: {metadata.get('total_processing_time', 0)} seconds
    Output File: {output_file}
    """)
    
    # Print individual stock summaries
    stock_analyses = batch_results.get('stock_analyses', {})
    for symbol, analysis in stock_analyses.items():
        if 'error' not in analysis:
            summary = analysis.get('analysis_summary', {})
            print(f"\n{symbol}: {summary.get('overall_sentiment', 'N/A')} - "
                  f"{summary.get('recommendation', 'N/A')} "
                  f"(Confidence: {summary.get('confidence_score', 0):.1f}%)")
        else:
            print(f"\n{symbol}: ANALYSIS FAILED - {analysis.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()