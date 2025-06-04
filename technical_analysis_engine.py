import asyncio
import aiohttp
import json
import logging
import talib
import yfinance as yf
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
from dataclasses import dataclass, asdict
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class TechnicalIndicators:
    """Data class for technical indicators"""
    # Trend Indicators
    sma_5: float = None
    sma_10: float = None
    sma_20: float = None
    sma_50: float = None
    sma_200: float = None
    ema_5: float = None
    ema_10: float = None
    ema_20: float = None
    ema_50: float = None
    ema_200: float = None
    
    # MACD
    macd: float = None
    macd_signal: float = None
    macd_histogram: float = None
    
    # Bollinger Bands
    bb_upper: float = None
    bb_middle: float = None
    bb_lower: float = None
    bb_width: float = None
    bb_position: float = None
    
    # ADX
    adx: float = None
    plus_di: float = None
    minus_di: float = None
    
    # Momentum Indicators
    rsi: float = None
    rsi_signal: str = None
    stoch_k: float = None
    stoch_d: float = None
    stoch_signal: str = None
    williams_r: float = None
    cci: float = None
    
    # Volume Indicators
    obv: float = None
    obv_trend: str = None
    vwap: float = None
    mfi: float = None
    ad_line: float = None
    
    # Pattern Recognition
    candlestick_patterns: Dict[str, int] = None
    trend_strength: str = None
    support_level: float = None
    resistance_level: float = None

@dataclass
class MacroData:
    """Data class for macro/fundamental data"""
    week_52_high: float = None
    week_52_low: float = None
    current_position_pct: float = None
    avg_volume_10d: float = None
    relative_volume: float = None
    beta: float = None
    pe_ratio: float = None
    market_cap: float = None
    atr: float = None
    volatility: float = None

@dataclass
class StockAnalysis:
    """Complete stock analysis data"""
    symbol: str
    timestamp: str
    current_price: float
    price_change: float
    price_change_pct: float
    volume: int
    technical_indicators: TechnicalIndicators
    macro_data: MacroData
    overall_signal: str
    confidence_score: float
    key_insights: List[str]

class TechnicalAnalysisEngine:
    def __init__(self, config_path: str = "config.json"):
        """Initialize the Technical Analysis Engine"""
        self.config = self._load_config(config_path)
        self.session = None
        self.cache = {}
        self.cache_timestamps = {}
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Config file {config_path} not found")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing config file: {e}")
            raise
    
    def _is_cache_valid(self, symbol: str) -> bool:
        """Check if cached data is still valid"""
        if symbol not in self.cache:
            return False
        
        cache_time = self.cache_timestamps.get(symbol)
        if not cache_time:
            return False
        
        ttl_minutes = self.config['performance_settings']['cache_settings']['ttl_minutes']
        return (datetime.now() - cache_time).total_seconds() < (ttl_minutes * 60)
    
    def _convert_resolution_to_minutes(self, resolution: str) -> int:
        """Convert resolution string to minutes"""
        resolution = resolution.strip().upper()
        if resolution.endswith('H'):
            return int(resolution[:-1]) * 60
        elif resolution.endswith('D'):
            return int(resolution[:-1]) * 24 * 60
        else:
            return int(resolution)
    
    async def _fetch_moneycontrol_data(self, symbol: str, resolution: str = "5", days_back: int = 5) -> Dict:
        """Fetch OHLCV data from Moneycontrol API"""
        if self._is_cache_valid(f"{symbol}_mc"):
            return self.cache[f"{symbol}_mc"]
        
        try:
            # Calculate date range
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days_back)
            
            # Convert to epoch
            from_epoch = int(start_time.timestamp())
            to_epoch = int(end_time.timestamp())
            
            # Calculate countback
            total_minutes = (to_epoch - from_epoch) / 60
            resolution_minutes = self._convert_resolution_to_minutes(resolution)
            countback = round(total_minutes / resolution_minutes)
            
            url = (
                f"https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"
                f"?symbol={symbol}&resolution={resolution}&from={from_epoch}"
                f"&to={to_epoch}&countback={countback}&currencyCode=INR"
            )
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Linux; Android 11.0; Surface Duo) AppleWebKit/537.36"
            }
            
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get("s") == "no_data":
                        logger.warning(f"No data available for {symbol}")
                        return None
                    
                    # Process data
                    df = pd.DataFrame({
                        'timestamp': data['t'],
                        'open': data['o'],
                        'high': data['h'],
                        'low': data['l'],
                        'close': data['c'],
                        'volume': data['v']
                    })
                    
                    # Convert timestamp to datetime
                    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
                    df.set_index('datetime', inplace=True)
                    
                    # Cache the data
                    self.cache[f"{symbol}_mc"] = df
                    self.cache_timestamps[f"{symbol}_mc"] = datetime.now()
                    
                    return df
                else:
                    logger.error(f"Error fetching data for {symbol}: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"Exception in _fetch_moneycontrol_data for {symbol}: {e}")
            return None
    
    def _fetch_yfinance_data(self, symbol: str) -> Dict:
        """Fetch macro data from yfinance"""
        if self._is_cache_valid(f"{symbol}_yf"):
            return self.cache[f"{symbol}_yf"]
        
        try:
            # Add .NS suffix for Indian stocks
            yf_symbol = f"{symbol}.NS"
            ticker = yf.Ticker(yf_symbol)
            
            # Get info
            info = ticker.info
            
            # Get historical data for 52-week calculations
            hist = ticker.history(period="1y")
            
            if hist.empty:
                logger.warning(f"No yfinance data for {symbol}")
                return None
            
            current_price = hist['Close'].iloc[-1]
            week_52_high = hist['High'].max()
            week_52_low = hist['Low'].min()
            
            # Calculate position in 52-week range
            current_position_pct = ((current_price - week_52_low) / (week_52_high - week_52_low)) * 100
            
            # Get recent volume data
            recent_volume = hist['Volume'].tail(10).mean()
            current_volume = hist['Volume'].iloc[-1]
            relative_volume = current_volume / recent_volume if recent_volume > 0 else 1.0
            
            macro_data = {
                'week_52_high': week_52_high,
                'week_52_low': week_52_low,
                'current_position_pct': current_position_pct,
                'avg_volume_10d': recent_volume,
                'relative_volume': relative_volume,
                'beta': info.get('beta', None),
                'pe_ratio': info.get('trailingPE', None),
                'market_cap': info.get('marketCap', None)
            }
            
            # Cache the data
            self.cache[f"{symbol}_yf"] = macro_data
            self.cache_timestamps[f"{symbol}_yf"] = datetime.now()
            
            return macro_data
            
        except Exception as e:
            logger.error(f"Exception in _fetch_yfinance_data for {symbol}: {e}")
            return None
    
    def _calculate_technical_indicators(self, df: pd.DataFrame) -> TechnicalIndicators:
        """Calculate all technical indicators"""
        try:
            high = df['high'].values
            low = df['low'].values
            close = df['close'].values
            volume = df['volume'].values
            
            indicators = TechnicalIndicators()
            
            # Moving Averages
            indicators.sma_5 = talib.SMA(close, timeperiod=5)[-1] if len(close) >= 5 else None
            indicators.sma_10 = talib.SMA(close, timeperiod=10)[-1] if len(close) >= 10 else None
            indicators.sma_20 = talib.SMA(close, timeperiod=20)[-1] if len(close) >= 20 else None
            indicators.sma_50 = talib.SMA(close, timeperiod=50)[-1] if len(close) >= 50 else None
            indicators.sma_200 = talib.SMA(close, timeperiod=200)[-1] if len(close) >= 200 else None
            
            indicators.ema_5 = talib.EMA(close, timeperiod=5)[-1] if len(close) >= 5 else None
            indicators.ema_10 = talib.EMA(close, timeperiod=10)[-1] if len(close) >= 10 else None
            indicators.ema_20 = talib.EMA(close, timeperiod=20)[-1] if len(close) >= 20 else None
            indicators.ema_50 = talib.EMA(close, timeperiod=50)[-1] if len(close) >= 50 else None
            indicators.ema_200 = talib.EMA(close, timeperiod=200)[-1] if len(close) >= 200 else None
            
            # MACD
            if len(close) >= 26:
                macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
                indicators.macd = macd[-1]
                indicators.macd_signal = macd_signal[-1]
                indicators.macd_histogram = macd_hist[-1]
            
            # Bollinger Bands
            if len(close) >= 20:
                bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
                indicators.bb_upper = bb_upper[-1]
                indicators.bb_middle = bb_middle[-1]
                indicators.bb_lower = bb_lower[-1]
                indicators.bb_width = (bb_upper[-1] - bb_lower[-1]) / bb_middle[-1] * 100
                indicators.bb_position = (close[-1] - bb_lower[-1]) / (bb_upper[-1] - bb_lower[-1]) * 100
            
            # ADX
            if len(close) >= 14:
                indicators.adx = talib.ADX(high, low, close, timeperiod=14)[-1]
                indicators.plus_di = talib.PLUS_DI(high, low, close, timeperiod=14)[-1]
                indicators.minus_di = talib.MINUS_DI(high, low, close, timeperiod=14)[-1]
            
            # RSI
            if len(close) >= 14:
                indicators.rsi = talib.RSI(close, timeperiod=14)[-1]
                if indicators.rsi >= 70:
                    indicators.rsi_signal = "OVERBOUGHT"
                elif indicators.rsi <= 30:
                    indicators.rsi_signal = "OVERSOLD"
                else:
                    indicators.rsi_signal = "NEUTRAL"
            
            # Stochastic
            if len(close) >= 14:
                stoch_k, stoch_d = talib.STOCH(high, low, close, fastk_period=14, slowk_period=3, slowd_period=3)
                indicators.stoch_k = stoch_k[-1]
                indicators.stoch_d = stoch_d[-1]
                if indicators.stoch_k >= 80:
                    indicators.stoch_signal = "OVERBOUGHT"
                elif indicators.stoch_k <= 20:
                    indicators.stoch_signal = "OVERSOLD"
                else:
                    indicators.stoch_signal = "NEUTRAL"
            
            # Williams %R
            if len(close) >= 14:
                indicators.williams_r = talib.WILLR(high, low, close, timeperiod=14)[-1]
            
            # CCI
            if len(close) >= 20:
                indicators.cci = talib.CCI(high, low, close, timeperiod=20)[-1]
            
            # Volume Indicators
            if len(close) >= 10:
                indicators.obv = talib.OBV(close, volume)[-1]
                # OBV trend (simplified)
                obv_ma = talib.SMA(talib.OBV(close, volume), timeperiod=5)
                if len(obv_ma) >= 2:
                    indicators.obv_trend = "UP" if obv_ma[-1] > obv_ma[-2] else "DOWN"
            
            # VWAP (simplified calculation)
            if len(close) >= 20:
                typical_price = (high + low + close) / 3
                vwap_data = (typical_price * volume).cumsum() / volume.cumsum()
                indicators.vwap = vwap_data[-1]
            
            # MFI
            if len(close) >= 14:
                indicators.mfi = talib.MFI(high, low, close, volume, timeperiod=14)[-1]
            
            # A/D Line
            if len(close) >= 10:
                indicators.ad_line = talib.AD(high, low, close, volume)[-1]
            
            # Candlestick Patterns
            patterns = {}
            if len(close) >= 10:
                patterns['DOJI'] = talib.CDLDOJI(df['open'].values, high, low, close)[-1]
                patterns['HAMMER'] = talib.CDLHAMMER(df['open'].values, high, low, close)[-1]
                patterns['ENGULFING'] = talib.CDLENGULFING(df['open'].values, high, low, close)[-1]
                patterns['MORNING_STAR'] = talib.CDLMORNINGSTAR(df['open'].values, high, low, close)[-1]
                patterns['EVENING_STAR'] = talib.CDLEVENINGSTAR(df['open'].values, high, low, close)[-1]
                patterns['SHOOTING_STAR'] = talib.CDLSHOOTINGSTAR(df['open'].values, high, low, close)[-1]
            
            indicators.candlestick_patterns = patterns
            
            # Support and Resistance (simplified)
            if len(close) >= 20:
                recent_highs = pd.Series(high[-20:])
                recent_lows = pd.Series(low[-20:])
                indicators.resistance_level = recent_highs.quantile(0.9)
                indicators.support_level = recent_lows.quantile(0.1)
            
            # Trend Strength
            if indicators.adx and indicators.adx > 25:
                if indicators.plus_di and indicators.minus_di:
                    if indicators.plus_di > indicators.minus_di:
                        indicators.trend_strength = "STRONG_UPTREND"
                    else:
                        indicators.trend_strength = "STRONG_DOWNTREND"
                else:
                    indicators.trend_strength = "STRONG_TREND"
            else:
                indicators.trend_strength = "WEAK_TREND"
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating technical indicators: {e}")
            return TechnicalIndicators()
    
    def _calculate_macro_indicators(self, df: pd.DataFrame, yf_data: Dict) -> MacroData:
        """Calculate macro indicators"""
        try:
            macro = MacroData()
            
            # From yfinance data
            if yf_data:
                macro.week_52_high = yf_data.get('week_52_high')
                macro.week_52_low = yf_data.get('week_52_low')
                macro.current_position_pct = yf_data.get('current_position_pct')
                macro.avg_volume_10d = yf_data.get('avg_volume_10d')
                macro.relative_volume = yf_data.get('relative_volume')
                macro.beta = yf_data.get('beta')
                macro.pe_ratio = yf_data.get('pe_ratio')
                macro.market_cap = yf_data.get('market_cap')
            
            # Calculate ATR and Volatility from OHLCV data
            if len(df) >= 14:
                high = df['high'].values
                low = df['low'].values
                close = df['close'].values
                
                macro.atr = talib.ATR(high, low, close, timeperiod=14)[-1]
                
                # Calculate 30-day volatility
                if len(close) >= 30:
                    returns = pd.Series(close).pct_change().dropna()
                    macro.volatility = returns.std() * np.sqrt(252) * 100  # Annualized volatility
            
            return macro
            
        except Exception as e:
            logger.error(f"Error calculating macro indicators: {e}")
            return MacroData()
    
    def _generate_signals_and_insights(self, symbol: str, df: pd.DataFrame, 
                                     technical: TechnicalIndicators, macro: MacroData) -> Tuple[str, float, List[str]]:
        """Generate trading signals and insights"""
        try:
            signals = []
            insights = []
            bullish_signals = 0
            bearish_signals = 0
            total_signals = 0
            
            current_price = df['close'].iloc[-1]
            
            # RSI Analysis
            if technical.rsi:
                total_signals += 1
                if technical.rsi < 30:
                    bullish_signals += 1
                    insights.append(f"RSI oversold at {technical.rsi:.1f} - potential buy signal")
                elif technical.rsi > 70:
                    bearish_signals += 1
                    insights.append(f"RSI overbought at {technical.rsi:.1f} - potential sell signal")
            
            # MACD Analysis
            if technical.macd and technical.macd_signal:
                total_signals += 1
                if technical.macd > technical.macd_signal and technical.macd_histogram > 0:
                    bullish_signals += 1
                    insights.append("MACD bullish crossover detected")
                elif technical.macd < technical.macd_signal and technical.macd_histogram < 0:
                    bearish_signals += 1
                    insights.append("MACD bearish crossover detected")
            
            # Moving Average Analysis
            if technical.sma_20 and technical.sma_50:
                total_signals += 1
                if technical.sma_20 > technical.sma_50 and current_price > technical.sma_20:
                    bullish_signals += 1
                    insights.append("Price above rising 20-day SMA - bullish trend")
                elif technical.sma_20 < technical.sma_50 and current_price < technical.sma_20:
                    bearish_signals += 1
                    insights.append("Price below falling 20-day SMA - bearish trend")
            
            # Bollinger Bands Analysis
            if technical.bb_position:
                total_signals += 1
                if technical.bb_position < 10:
                    bullish_signals += 1
                    insights.append("Price near lower Bollinger Band - oversold condition")
                elif technical.bb_position > 90:
                    bearish_signals += 1
                    insights.append("Price near upper Bollinger Band - overbought condition")
            
            # Volume Analysis
            if macro.relative_volume and macro.relative_volume > 1.5:
                insights.append(f"High volume activity - {macro.relative_volume:.1f}x average volume")
            
            # 52-week Position Analysis
            if macro.current_position_pct:
                if macro.current_position_pct > 80:
                    insights.append(f"Near 52-week high ({macro.current_position_pct:.1f}% of range)")
                elif macro.current_position_pct < 20:
                    insights.append(f"Near 52-week low ({macro.current_position_pct:.1f}% of range)")
            
            # Pattern Recognition
            if technical.candlestick_patterns:
                for pattern, value in technical.candlestick_patterns.items():
                    if value > 0:
                        insights.append(f"Bullish {pattern} pattern detected")
                        bullish_signals += 1
                        total_signals += 1
                    elif value < 0:
                        insights.append(f"Bearish {pattern} pattern detected")
                        bearish_signals += 1
                        total_signals += 1
            
            # Generate overall signal
            if total_signals == 0:
                overall_signal = "NEUTRAL"
                confidence_score = 0.0
            else:
                bullish_pct = bullish_signals / total_signals
                bearish_pct = bearish_signals / total_signals
                
                if bullish_pct >= 0.6:
                    overall_signal = "BUY"
                    confidence_score = bullish_pct
                elif bearish_pct >= 0.6:
                    overall_signal = "SELL"
                    confidence_score = bearish_pct
                else:
                    overall_signal = "HOLD"
                    confidence_score = max(bullish_pct, bearish_pct)
            
            return overall_signal, confidence_score, insights[:10]  # Limit insights
            
        except Exception as e:
            logger.error(f"Error generating signals for {symbol}: {e}")
            return "NEUTRAL", 0.0, ["Analysis error occurred"]
    
    async def analyze_stock(self, symbol: str) -> Optional[StockAnalysis]:
        """Analyze a single stock"""
        try:
            logger.info(f"Analyzing {symbol}")
            
            # Fetch OHLCV data from Moneycontrol
            df = await self._fetch_moneycontrol_data(symbol)
            if df is None or df.empty:
                logger.warning(f"No OHLCV data for {symbol}")
                return None
            
            # Fetch macro data from yfinance
            yf_data = self._fetch_yfinance_data(symbol)
            
            # Calculate technical indicators
            technical_indicators = self._calculate_technical_indicators(df)
            
            # Calculate macro indicators
            macro_data = self._calculate_macro_indicators(df, yf_data)
            
            # Generate signals and insights
            current_price = df['close'].iloc[-1]
            previous_price = df['close'].iloc[-2] if len(df) > 1 else current_price
            price_change = current_price - previous_price
            price_change_pct = (price_change / previous_price) * 100 if previous_price != 0 else 0
            
            overall_signal, confidence_score, insights = self._generate_signals_and_insights(
                symbol, df, technical_indicators, macro_data
            )
            
            # Create analysis object
            analysis = StockAnalysis(
                symbol=symbol,
                timestamp=datetime.now().isoformat(),
                current_price=round(current_price, 2),
                price_change=round(price_change, 2),
                price_change_pct=round(price_change_pct, 2),
                volume=int(df['volume'].iloc[-1]),
                technical_indicators=technical_indicators,
                macro_data=macro_data,
                overall_signal=overall_signal,
                confidence_score=round(confidence_score, 3),
                key_insights=insights
            )
            
            logger.info(f"Analysis completed for {symbol}: {overall_signal} ({confidence_score:.1%})")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            return None
    
    async def analyze_multiple_stocks(self, symbols: List[str]) -> Dict[str, StockAnalysis]:
        """Analyze multiple stocks concurrently"""
        logger.info(f"Starting analysis for {len(symbols)} stocks")
        
        # Create aiohttp session
        connector = aiohttp.TCPConnector(limit=self.config['performance_settings']['async_settings']['max_concurrent_requests'])
        timeout = aiohttp.ClientTimeout(total=self.config['performance_settings']['async_settings']['request_timeout'])
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self.session = session
            
            # Create tasks for all symbols
            tasks = [self.analyze_stock(symbol) for symbol in symbols]
            
            # Execute tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            analysis_results = {}
            for symbol, result in zip(symbols, results):
                if isinstance(result, Exception):
                    logger.error(f"Exception for {symbol}: {result}")
                elif result is not None:
                    analysis_results[symbol] = result
            
            logger.info(f"Analysis completed for {len(analysis_results)}/{len(symbols)} stocks")
            return analysis_results
    
    def export_to_json(self, analysis_results: Dict[str, StockAnalysis], filename: str = None) -> str:
        """Export analysis results to JSON format"""
        try:
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"stock_analysis_{timestamp}.json"
            
            # Convert to serializable format
            export_data = {
                "analysis_timestamp": datetime.now().isoformat(),
                "total_stocks": len(analysis_results),
                "stocks": {}
            }
            
            for symbol, analysis in analysis_results.items():
                # Convert dataclass to dict and handle numpy types
                stock_data = asdict(analysis)
                
                # Clean up None values and convert numpy types
                def clean_data(obj):
                    if isinstance(obj, dict):
                        return {k: clean_data(v) for k, v in obj.items() if v is not None}
                    elif isinstance(obj, (np.integer, np.floating)):
                        return float(obj)
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    else:
                        return obj
                
                export_data["stocks"][symbol] = clean_data(stock_data)
            
            # Create output directory if it doesn't exist
            os.makedirs("output", exist_ok=True)
            filepath = os.path.join("output", filename)
            
            # Write to file
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            logger.info(f"Analysis exported to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}")
            raise
    
    async def run_analysis(self) -> str:
        """Run complete analysis pipeline"""
        try:
            start_time = time.time()
            
            # Get stock list from config
            watchlist = self.config['stocks']['watchlist']
            high_priority = self.config['stocks'].get('high_priority', [])
            
            # Combine lists (high priority first)
            all_stocks = high_priority + watchlist
            
            logger.info(f"Starting analysis pipeline for {len(all_stocks)} stocks")
            
            # Run analysis
            results = await self.analyze_multiple_stocks(all_stocks)
            
            # Export results
            if results:
                filepath = self.export_to_json(results)
                
                execution_time = time.time() - start_time
                logger.info(f"Analysis pipeline completed in {execution_time:.2f} seconds")
                logger.info(f"Results saved to: {filepath}")
                
                return filepath
            else:
                logger.error("No analysis results to export")
                return None
                
        except Exception as e:
            logger.error(f"Error in analysis pipeline: {e}")
            raise

# Main execution
async def main():
    """Main execution function"""
    try:
        # Initialize engine
        engine = TechnicalAnalysisEngine()
        
        # Run analysis
        result_file = await engine.run_analysis()
        
        if result_file:
            print(f"\n✅ Analysis completed successfully!")
            print(f"📊 Results saved to: {result_file}")
            print(f"📈 Ready for LLM processing")
        else:
            print("❌ Analysis failed")
            
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())