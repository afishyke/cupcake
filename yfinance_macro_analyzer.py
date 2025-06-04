# yfinance_macro_analyzer.py - Macro-level stock analysis using yfinance

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import time
import json

from config import STOCK_SYMBOLS, YFINANCE_CONFIG, OUTPUT_CONFIG

logger = logging.getLogger(__name__)

class MacroAnalyzer:
    def __init__(self):
        self.config = YFINANCE_CONFIG
        self.executor = ThreadPoolExecutor(max_workers=10)
        
    def analyze_stock_macro(self, symbol: str) -> Dict[str, Any]:
        """
        Analyze macro-level data for a single stock
        Focus on 52-week performance, recent trends, and market positioning
        """
        try:
            # Add .NS suffix for yfinance (NSE stocks)
            yf_symbol = f"{symbol}.NS"
            
            # Download data
            ticker = yf.Ticker(yf_symbol)
            
            # Get historical data
            hist_data = ticker.history(period=self.config['period'], interval=self.config['interval'])
            
            if hist_data.empty:
                logger.warning(f"No data available for {symbol}")
                return self._empty_macro_analysis(symbol)
            
            # Get current price
            current_price = hist_data['Close'].iloc[-1]
            
            # Basic info
            info = ticker.info
            
            analysis = {
                'symbol': symbol,
                'current_price': round(current_price, OUTPUT_CONFIG['precision']),
                'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format']),
                'data_period': f"{len(hist_data)} trading days"
            }
            
            # 52-week analysis (most important for macro view)
            analysis['week_52_analysis'] = self._analyze_52_week_performance(hist_data, current_price)
            
            # Recent performance trends
            analysis['recent_performance'] = self._analyze_recent_performance(hist_data, current_price)
            
            # Volume and liquidity analysis
            analysis['liquidity_analysis'] = self._analyze_liquidity(hist_data)
            
            # Volatility profile
            analysis['volatility_profile'] = self._analyze_volatility_profile(hist_data)
            
            # Market positioning
            analysis['market_positioning'] = self._analyze_market_positioning(info, current_price)
            
            # Risk metrics
            analysis['risk_metrics'] = self._calculate_risk_metrics(hist_data)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Macro analysis failed for {symbol}: {e}")
            return self._empty_macro_analysis(symbol)
    
    def _analyze_52_week_performance(self, hist_data: pd.DataFrame, current_price: float) -> Dict:
        """Analyze 52-week high/low performance - critical for macro view"""
        try:
            # Calculate 52-week high and low
            week_52_high = hist_data['High'].max()
            week_52_low = hist_data['Low'].min()
            
            # Position relative to 52-week range
            range_position = ((current_price - week_52_low) / (week_52_high - week_52_low)) * 100
            
            # Distance from highs and lows
            distance_from_high = ((week_52_high - current_price) / week_52_high) * 100
            distance_from_low = ((current_price - week_52_low) / week_52_low) * 100
            
            # Classify position
            if range_position > 80:
                position_desc = "NEAR_52W_HIGH"
            elif range_position > 60:
                position_desc = "UPPER_RANGE"
            elif range_position > 40:
                position_desc = "MIDDLE_RANGE"
            elif range_position > 20:
                position_desc = "LOWER_RANGE"
            else:
                position_desc = "NEAR_52W_LOW"
            
            # Find dates of highs and lows
            high_date = hist_data[hist_data['High'] == week_52_high].index[0].strftime('%Y-%m-%d')
            low_date = hist_data[hist_data['Low'] == week_52_low].index[0].strftime('%Y-%m-%d')
            
            return {
                'week_52_high': round(week_52_high, 2),
                'week_52_low': round(week_52_low, 2),
                'high_date': high_date,
                'low_date': low_date,
                'range_position_percent': round(range_position, 1),
                'position_description': position_desc,
                'distance_from_high_percent': round(distance_from_high, 1),
                'distance_from_low_percent': round(distance_from_low, 1),
                'annual_range_percent': round(((week_52_high - week_52_low) / week_52_low) * 100, 1)
            }
        except Exception as e:
            logger.error(f"52-week analysis error: {e}")
            return {'error': '52-week analysis failed'}
    
    def _analyze_recent_performance(self, hist_data: pd.DataFrame, current_price: float) -> Dict:
        """Analyze recent performance trends"""
        try:
            performance = {}
            
            # Calculate performance for different periods
            for days in self.config['macro_lookback_days']:
                if len(hist_data) > days:
                    past_price = hist_data['Close'].iloc[-days-1]
                    performance_pct = ((current_price - past_price) / past_price) * 100
                    performance[f'performance_{days}d'] = round(performance_pct, 2)
            
            # Recent trend analysis
            recent_prices = hist_data['Close'].tail(10).values
            if len(recent_prices) > 1:
                trend_slope = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]
                trend_direction = 'UPTREND' if trend_slope > 0 else 'DOWNTREND'
                trend_strength = abs(trend_slope / current_price) * 100  # Normalize by current price
            else:
                trend_direction = 'NEUTRAL'
                trend_strength = 0
            
            # Consecutive days analysis
            consecutive_up = 0
            consecutive_down = 0
            daily_changes = hist_data['Close'].pct_change().tail(10)
            
            for change in reversed(daily_changes.values):
                if np.isnan(change):
                    break
                if change > 0:
                    consecutive_up += 1
                    consecutive_down = 0
                elif change < 0:
                    consecutive_down += 1
                    consecutive_up = 0
                else:
                    break
            
            return {
                'performance_periods': performance,
                'recent_trend': {
                    'direction': trend_direction,
                    'strength_percent': round(trend_strength, 3),
                    'consecutive_up_days': consecutive_up,
                    'consecutive_down_days': consecutive_down
                },
                'momentum_summary': self._classify_momentum(performance)
            }
        except Exception as e:
            logger.error(f"Recent performance analysis error: {e}")
            return {'error': 'Recent performance analysis failed'}
    
    def _analyze_liquidity(self, hist_data: pd.DataFrame) -> Dict:
        """Analyze trading liquidity and volume patterns"""
        try:
            # Volume statistics
            avg_volume = hist_data['Volume'].mean()
            recent_avg_volume = hist_data['Volume'].tail(10).mean()
            volume_trend = 'INCREASING' if recent_avg_volume > avg_volume else 'DECREASING'
            
            # Volume consistency (coefficient of variation)
            volume_cv = hist_data['Volume'].std() / avg_volume if avg_volume > 0 else 0
            liquidity_rating = 'HIGH' if volume_cv < 0.5 else 'MEDIUM' if volume_cv < 1.0 else 'LOW'
            
            # High volume days (above 1.5x average)
            high_volume_days = (hist_data['Volume'] > (avg_volume * 1.5)).sum()
            high_volume_percentage = (high_volume_days / len(hist_data)) * 100
            
            return {
                'average_volume': int(avg_volume),
                'recent_average_volume': int(recent_avg_volume),
                'volume_trend': volume_trend,
                'volume_ratio': round(recent_avg_volume / avg_volume, 2) if avg_volume > 0 else 1,
                'liquidity_rating': liquidity_rating,
                'high_volume_days_percent': round(high_volume_percentage, 1),
                'volume_consistency': 'CONSISTENT' if volume_cv < 0.5 else 'VARIABLE'
            }
        except Exception as e:
            logger.error(f"Liquidity analysis error: {e}")
            return {'error': 'Liquidity analysis failed'}
    
    def _analyze_volatility_profile(self, hist_data: pd.DataFrame) -> Dict:
        """Analyze volatility characteristics"""
        try:
            # Daily returns
            daily_returns = hist_data['Close'].pct_change().dropna()
            
            # Volatility measures
            volatility_daily = daily_returns.std()
            volatility_annualized = volatility_daily * np.sqrt(252)  # Annualized volatility
            
            # Recent volatility (last 30 days)
            recent_volatility = daily_returns.tail(30).std() * np.sqrt(252)
            
            # Volatility trend
            volatility_trend = 'INCREASING' if recent_volatility > volatility_annualized else 'DECREASING'
            
            # Risk classification
            if volatility_annualized < 0.15:
                risk_level = 'LOW'
            elif volatility_annualized < 0.25:
                risk_level = 'MEDIUM'
            else:
                risk_level = 'HIGH'
            
            # Extreme movement days
            extreme_moves = (abs(daily_returns) > 0.05).sum()  # Days with >5% moves
            extreme_percentage = (extreme_moves / len(daily_returns)) * 100
            
            return {
                'annualized_volatility': round(volatility_annualized * 100, 1),
                'recent_volatility': round(recent_volatility * 100, 1),
                'volatility_trend': volatility_trend,
                'risk_level': risk_level,
                'extreme_move_days_percent': round(extreme_percentage, 1),
                'volatility_stability': 'STABLE' if abs(recent_volatility - volatility_annualized) < 0.05 else 'CHANGING'
            }
        except Exception as e:
            logger.error(f"Volatility analysis error: {e}")
            return {'error': 'Volatility analysis failed'}
    
    def _analyze_market_positioning(self, info: Dict, current_price: float) -> Dict:
        """Analyze market positioning and valuation context"""
        try:
            positioning = {}
            
            # Market cap context
            market_cap = info.get('marketCap', 0)
            if market_cap > 0:
                if market_cap > 1000000000000:  # > 1 trillion
                    size_category = 'MEGA_CAP'
                elif market_cap > 100000000000:  # > 100 billion
                    size_category = 'LARGE_CAP'
                elif market_cap > 10000000000:   # > 10 billion
                    size_category = 'MID_CAP'
                else:
                    size_category = 'SMALL_CAP'
                
                positioning['market_cap_category'] = size_category
                positioning['market_cap_cr'] = round(market_cap / 10000000, 0)  # In crores
            
            # Valuation metrics (if available)
            pe_ratio = info.get('trailingPE', None)
            pb_ratio = info.get('priceToBook', None)
            
            if pe_ratio and pe_ratio > 0:
                positioning['pe_ratio'] = round(pe_ratio, 1)
                positioning['pe_classification'] = 'HIGH' if pe_ratio > 25 else 'MEDIUM' if pe_ratio > 15 else 'LOW'
            
            if pb_ratio and pb_ratio > 0:
                positioning['pb_ratio'] = round(pb_ratio, 1)
                positioning['pb_classification'] = 'HIGH' if pb_ratio > 3 else 'MEDIUM' if pb_ratio > 1 else 'LOW'
            
            # Beta (market risk)
            beta = info.get('beta', None)
            if beta is not None:
                positioning['beta'] = round(beta, 2)
                positioning['market_risk'] = 'HIGH' if beta > 1.5 else 'MEDIUM' if beta > 0.8 else 'LOW'
            
            # Sector context
            sector = info.get('sector', 'Unknown')
            industry = info.get('industry', 'Unknown')
            positioning['sector'] = sector
            positioning['industry'] = industry
            
            return positioning
        except Exception as e:
            logger.error(f"Market positioning analysis error: {e}")
            return {'error': 'Market positioning analysis failed'}
    
    def _calculate_risk_metrics(self, hist_data: pd.DataFrame) -> Dict:
        """Calculate risk-adjusted performance metrics"""
        try:
            # Daily returns
            daily_returns = hist_data['Close'].pct_change().dropna()
            
            # Sharpe ratio approximation (assuming risk-free rate of 6% annually)
            risk_free_rate = 0.06 / 252  # Daily risk-free rate
            excess_returns = daily_returns - risk_free_rate
            sharpe_ratio = excess_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0
            
            # Maximum drawdown
            cumulative_returns = (1 + daily_returns).cumprod()
            peak = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - peak) / peak
            max_drawdown = drawdown.min()
            
            # Downside volatility
            negative_returns = daily_returns[daily_returns < 0]
            downside_volatility = negative_returns.std() * np.sqrt(252) if len(negative_returns) > 0 else 0
            
            # Win rate
            positive_days = (daily_returns > 0).sum()
            win_rate = (positive_days / len(daily_returns)) * 100
            
            return {
                'sharpe_ratio': round(sharpe_ratio, 2),
                'max_drawdown_percent': round(max_drawdown * 100, 1),
                'downside_volatility_percent': round(downside_volatility * 100, 1),
                'win_rate_percent': round(win_rate, 1),
                'risk_adjusted_return': 'GOOD' if sharpe_ratio > 1 else 'AVERAGE' if sharpe_ratio > 0 else 'POOR'
            }
        except Exception as e:
            logger.error(f"Risk metrics calculation error: {e}")
            return {'error': 'Risk metrics calculation failed'}
    
    def _classify_momentum(self, performance: Dict) -> str:
        """Classify overall momentum based on multi-period performance"""
        if not performance:
            return 'NEUTRAL'
        
        # Weight recent performance more heavily
        score = 0
        weights = {'performance_1d': 0.1, 'performance_5d': 0.2, 'performance_10d': 0.3, 'performance_30d': 0.4}
        
        for period, weight in weights.items():
            if period in performance:
                score += performance[period] * weight
        
        if score > 5:
            return 'STRONG_POSITIVE'
        elif score > 2:
            return 'POSITIVE'
        elif score > -2:
            return 'NEUTRAL'
        elif score > -5:
            return 'NEGATIVE'
        else:
            return 'STRONG_NEGATIVE'
    
    def _empty_macro_analysis(self, symbol: str) -> Dict:
        """Return empty macro analysis for failed cases"""
        return {
            'symbol': symbol,
            'error': 'Macro analysis failed - no data available',
            'analysis_timestamp': datetime.now().strftime(OUTPUT_CONFIG['timestamp_format'])
        }

# Batch processing class
class BatchMacroAnalyzer:
    def __init__(self):
        self.analyzer = MacroAnalyzer()
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    def analyze_batch(self, symbols: List[str]) -> Dict[str, Dict]:
        """Analyze multiple stocks concurrently"""
        start_time = time.time()
        results = {}
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(self.analyzer.analyze_stock_macro, symbol): symbol 
                for symbol in symbols
            }
            
            # Collect results as they complete
            for future in future_to_symbol:
                symbol = future_to_symbol[future]
                try:
                    result = future.result(timeout=30)  # 30 second timeout per stock
                    results[symbol] = result
                    logger.info(f"Completed macro analysis for {symbol}")
                except Exception as e:
                    logger.error(f"Failed to analyze {symbol}: {e}")
                    results[symbol] = self.analyzer._empty_macro_analysis(symbol)
        
        end_time = time.time()
        logger.info(f"Batch macro analysis completed in {end_time - start_time:.2f} seconds")
        
        return results
    
    def analyze_batch_with_progress(self, symbols: List[str], progress_callback=None) -> Dict[str, Dict]:
        """Analyze multiple stocks with progress reporting"""
        results = {}
        total_symbols = len(symbols)
        
        for i, symbol in enumerate(symbols):
            try:
                result = self.analyzer.analyze_stock_macro(symbol)
                results[symbol] = result
                
                if progress_callback:
                    progress_callback(i + 1, total_symbols, symbol)
                    
            except Exception as e:
                logger.error(f"Failed to analyze {symbol}: {e}")
                results[symbol] = self.analyzer._empty_macro_analysis(symbol)
        
        return results

# Utility functions
def save_macro_analysis(data: Dict, filename: str = None):
    """Save macro analysis results to JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"macro_analysis_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    logger.info(f"Macro analysis saved to {filename}")

def load_macro_analysis(filename: str) -> Dict:
    """Load macro analysis results from JSON file"""
    with open(filename, 'r') as f:
        data = json.load(f)
    
    logger.info(f"Macro analysis loaded from {filename}")
    return data

def get_macro_summary(analysis: Dict) -> Dict:
    """Extract key macro insights from full analysis"""
    if 'error' in analysis:
        return {'symbol': analysis['symbol'], 'status': 'ERROR', 'error': analysis['error']}
    
    summary = {
        'symbol': analysis['symbol'],
        'current_price': analysis['current_price'],
        'timestamp': analysis['analysis_timestamp']
    }
    
    # 52-week position
    if 'week_52_analysis' in analysis and 'error' not in analysis['week_52_analysis']:
        w52 = analysis['week_52_analysis']
        summary['position_52w'] = w52['position_description']
        summary['range_position'] = w52['range_position_percent']
    
    # Recent momentum
    if 'recent_performance' in analysis and 'error' not in analysis['recent_performance']:
        perf = analysis['recent_performance']
        summary['momentum'] = perf['momentum_summary']
        if 'performance_periods' in perf:
            summary['performance_5d'] = perf['performance_periods'].get('performance_5d', 0)
            summary['performance_30d'] = perf['performance_periods'].get('performance_30d', 0)
    
    # Risk level
    if 'volatility_profile' in analysis and 'error' not in analysis['volatility_profile']:
        vol = analysis['volatility_profile']
        summary['risk_level'] = vol['risk_level']
        summary['volatility'] = vol['annualized_volatility']
    
    # Market positioning
    if 'market_positioning' in analysis and 'error' not in analysis['market_positioning']:
        pos = analysis['market_positioning']
        summary['market_cap_category'] = pos.get('market_cap_category', 'Unknown')
        summary['sector'] = pos.get('sector', 'Unknown')
    
    return summary

# Main execution functions
def analyze_single_stock(symbol: str) -> Dict:
    """Analyze a single stock - convenience function"""
    analyzer = MacroAnalyzer()
    return analyzer.analyze_stock_macro(symbol)

def analyze_multiple_stocks(symbols: List[str]) -> Dict:
    """Analyze multiple stocks - convenience function"""
    batch_analyzer = BatchMacroAnalyzer()
    return batch_analyzer.analyze_batch(symbols)

# Example usage and testing
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Test with a few stocks
    test_symbols = ['TCS', 'RELIANCE', 'HDFCBANK']
    
    print("Starting macro analysis...")
    
    # Analyze batch
    batch_analyzer = BatchMacroAnalyzer()
    results = batch_analyzer.analyze_batch(test_symbols)
    
    # Print summaries
    print("\n=== MACRO ANALYSIS SUMMARIES ===")
    for symbol, analysis in results.items():
        summary = get_macro_summary(analysis)
        print(f"\n{symbol}:")
        for key, value in summary.items():
            if key != 'symbol':
                print(f"  {key}: {value}")
    
    # Save results
    save_macro_analysis(results)
    print(f"\nFull analysis saved to file")
    