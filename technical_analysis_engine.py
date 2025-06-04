import asyncio
import aiohttp
import json
import logging
import talib
import yfinance as yf
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
from dataclasses import dataclass, asdict
import warnings
try:
    import pytz
except ImportError:
    pytz = None
    logging.warning("pytz not installed. Market timing features will be limited.")

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
    # Enhanced fields
    market_status: str = None
    data_freshness: str = None

class TechnicalAnalysisEngine:
    def __init__(self, config_path: str = "config.json"):
        """Initialize the Technical Analysis Engine with market timing awareness"""
        self.config = self._load_config(config_path)
        self.session = None
        self.cache = {}
        self.cache_timestamps = {}
        
        # Market timing setup
        if pytz:
            self.indian_tz = pytz.timezone('Asia/Kolkata')
        else:
            self.indian_tz = None
            
        # Market hours (9:15 AM to 3:30 PM IST)
        self.market_open = dt_time(9, 15)
        self.market_close = dt_time(15, 30)
        
        # Market holidays (should be maintained externally or via API)
        self.market_holidays = self._load_market_holidays()
        
    def _load_market_holidays(self) -> List[datetime]:
        """Load market holidays from config or return default list"""
        try:
            holidays = self.config.get('market_holidays', [])
            return [datetime.strptime(h, '%Y-%m-%d') for h in holidays]
        except:
            # Default major holidays - should be updated annually
            return [
                datetime(2024, 1, 26),  # Republic Day
                datetime(2024, 3, 8),   # Holi
                datetime(2024, 8, 15),  # Independence Day
                datetime(2024, 10, 2),  # Gandhi Jayanti
                datetime(2024, 11, 1),  # Diwali (example)
            ]
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            # Return default config
            return {
                'performance_settings': {
                    'cache_settings': {'ttl_minutes': 5},
                    'async_settings': {
                        'max_concurrent_requests': 10,
                        'request_timeout': 30
                    }
                },
                'stocks': {
                    'watchlist': ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK'],
                    'high_priority': ['RELIANCE', 'TCS']
                },
                'market_holidays': []
            }
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing config file: {e}")
            raise
    
    def _is_market_open(self, check_time: datetime = None) -> bool:
        """Check if the market is currently open"""
        if not self.indian_tz:
            # Fallback: assume market is open during weekdays
            now = datetime.now()
            return now.weekday() < 5 and self.market_open <= now.time() <= self.market_close
            
        if check_time is None:
            check_time = datetime.now(self.indian_tz)
        else:
            check_time = check_time.astimezone(self.indian_tz)
        
        # Check if it's a weekend
        if check_time.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Check if it's a holiday
        if check_time.date() in [h.date() for h in self.market_holidays]:
            return False
        
        # Check if within market hours
        current_time = check_time.time()
        return self.market_open <= current_time <= self.market_close
    
    def _get_last_trading_day(self, from_date: datetime = None) -> datetime:
        """Get the last trading day before the given date"""
        if not self.indian_tz:
            # Fallback
            if from_date is None:
                from_date = datetime.now()
            check_date = from_date - timedelta(days=1)
            while check_date.weekday() >= 5:
                check_date -= timedelta(days=1)
            return check_date
            
        if from_date is None:
            from_date = datetime.now(self.indian_tz)
        else:
            from_date = from_date.astimezone(self.indian_tz)
        
        # Start from the previous day if market is closed
        if not self._is_market_open(from_date):
            check_date = from_date - timedelta(days=1)
        else:
            check_date = from_date
        
        # Go back until we find a trading day
        while check_date.weekday() >= 5 or check_date.date() in [h.date() for h in self.market_holidays]:
            check_date -= timedelta(days=1)
        
        return check_date
    
    def _get_optimal_data_range(self, days_back: int = 5) -> Tuple[datetime, datetime, str]:
        """Get optimal data range considering market timing"""
        if self.indian_tz:
            now = datetime.now(self.indian_tz)
        else:
            now = datetime.now()
            
        if self._is_market_open(now):
            # Market is open, get real-time data
            end_time = now
            start_time = now - timedelta(days=days_back)
            data_freshness = "REAL_TIME"
        else:
            # Market is closed, get data from last trading session
            last_trading_day = self._get_last_trading_day(now)
            if self.indian_tz:
                end_time = last_trading_day.replace(
                    hour=self.market_close.hour,
                    minute=self.market_close.minute,
                    second=0,
                    microsecond=0
                )
            else:
                end_time = last_trading_day
            start_time = end_time - timedelta(days=days_back)
            data_freshness = "LAST_SESSION"
        
        return start_time, end_time, data_freshness
    
    def _is_cache_valid(self, symbol: str) -> bool:
        """Enhanced cache validation with market timing awareness"""
        if symbol not in self.cache:
            return False
        
        cache_time = self.cache_timestamps.get(symbol)
        if not cache_time:
            return False
        
        ttl_minutes = self.config['performance_settings']['cache_settings']['ttl_minutes']
        
        # Market-aware caching
        if self._is_market_open():
            # During market hours, use shorter cache for real-time data
            effective_ttl = min(ttl_minutes, 2)  # Max 2 minutes during market hours
        else:
            # After market hours, cache can be longer
            effective_ttl = ttl_minutes * 2
        
        return (datetime.now() - cache_time).total_seconds() < (effective_ttl * 60)
    
    def _convert_resolution_to_minutes(self, resolution: str) -> int:
        """Convert resolution string to minutes"""
        resolution = resolution.strip().upper()
        if resolution.endswith('H'):
            return int(resolution[:-1]) * 60
        elif resolution.endswith('D'):
            return int(resolution[:-1]) * 24 * 60
        else:
            return int(resolution)
    
    async def _fetch_moneycontrol_data(self, symbol: str, resolution: str = "1D", days_back: int = 200) -> pd.DataFrame:
        """Enhanced Moneycontrol data fetching with debug logging"""
        cache_key = f"{symbol}_mc"
        
        if self._is_cache_valid(cache_key):
            logger.info(f"Using cached data for {symbol}")
            return self.cache[cache_key]
        
        try:
            # Get more historical data for technical analysis
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days_back)
            
            # Convert to epoch
            from_epoch = int(start_time.timestamp())
            to_epoch = int(end_time.timestamp())
            
            # Use daily data for better historical coverage
            url = (
                f"https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"
                f"?symbol={symbol}&resolution={resolution}&from={from_epoch}"
                f"&to={to_epoch}&countback=200&currencyCode=INR"
            )
            
            logger.info(f"Fetching URL: {url}")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.moneycontrol.com/"
            }
            
            async with self.session.get(url, headers=headers) as response:
                logger.info(f"Response status for {symbol}: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Raw API response keys: {list(data.keys())}")
                    
                    if data.get("s") == "no_data":
                        logger.warning(f"No data available for {symbol}")
                        return None
                    
                    # Debug the raw data
                    if 't' in data:
                        logger.info(f"Data points received for {symbol}: {len(data['t'])}")
                        logger.info(f"First few timestamps: {data['t'][:5] if len(data['t']) > 0 else 'None'}")
                        logger.info(f"Last few closes: {data['c'][-5:] if 'c' in data and len(data['c']) > 0 else 'None'}")
                    else:
                        logger.error(f"No timestamp data in response for {symbol}")
                        return None
                    
                    # Validate data structure
                    required_keys = ['t', 'o', 'h', 'l', 'c', 'v']
                    missing_keys = [key for key in required_keys if key not in data]
                    if missing_keys:
                        logger.error(f"Missing keys in API response for {symbol}: {missing_keys}")
                        return None
                    
                    # Check data length consistency
                    data_lengths = {key: len(data[key]) for key in required_keys}
                    if len(set(data_lengths.values())) > 1:
                        logger.error(f"Inconsistent data lengths for {symbol}: {data_lengths}")
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
                    
                    # Debug volume data
                    logger.info(f"Volume stats for {symbol}: min={df['volume'].min()}, max={df['volume'].max()}, mean={df['volume'].mean():.0f}")
                    
                    # Convert timestamp to datetime
                    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
                    df.set_index('datetime', inplace=True)
                    
                    # Remove rows with invalid data
                    initial_len = len(df)
                    df = df.dropna()
                    df = df[df['volume'] >= 0]  # Remove negative volumes
                    df = df[(df['high'] >= df['low']) & (df['high'] >= df['close']) & (df['low'] <= df['close'])]
                    
                    logger.info(f"Data cleaning for {symbol}: {initial_len} -> {len(df)} rows")
                    
                    if len(df) < 20:
                        logger.warning(f"Insufficient data after cleaning for {symbol}: {len(df)} rows")
                        return None
                    
                    # Add metadata
                    df.attrs['data_freshness'] = "HISTORICAL"
                    df.attrs['fetch_time'] = datetime.now()
                    df.attrs['symbol'] = symbol
                    
                    # Cache the data
                    self.cache[cache_key] = df
                    self.cache_timestamps[cache_key] = datetime.now()
                    
                    logger.info(f"Successfully processed {len(df)} candles for {symbol}")
                    return df
                else:
                    logger.error(f"HTTP Error for {symbol}: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"Exception in _fetch_moneycontrol_data for {symbol}: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _fetch_yfinance_data(self, symbol: str) -> Dict:
        """Enhanced yfinance data fetching with debug logging"""
        cache_key = f"{symbol}_yf"
        
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]
        
        try:
            # Try different symbol formats
            symbol_formats = [f"{symbol}.NS", f"{symbol}.BO", symbol]
            
            for yf_symbol in symbol_formats:
                logger.info(f"Trying yfinance symbol: {yf_symbol}")
                
                try:
                    ticker = yf.Ticker(yf_symbol)
                    
                    # Test if symbol is valid
                    info = ticker.info
                    if not info or 'symbol' not in info:
                        logger.warning(f"Invalid symbol or no info: {yf_symbol}")
                        continue
                    
                    # Get historical data
                    hist = ticker.history(period="1y", interval="1d")
                    
                    if hist.empty:
                        logger.warning(f"No historical data for {yf_symbol}")
                        continue
                    
                    logger.info(f"yfinance success for {yf_symbol}: {len(hist)} days of data")
                    
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
                        'week_52_high': float(week_52_high),
                        'week_52_low': float(week_52_low),
                        'current_position_pct': float(current_position_pct),
                        'avg_volume_10d': float(recent_volume),
                        'relative_volume': float(relative_volume),
                        'beta': info.get('beta', None),
                        'pe_ratio': info.get('trailingPE', None),
                        'market_cap': info.get('marketCap', None)
                    }
                    
                    # Cache the data
                    self.cache[cache_key] = macro_data
                    self.cache_timestamps[cache_key] = datetime.now()
                    
                    logger.info(f"yfinance data successfully cached for {symbol}")
                    return macro_data
                    
                except Exception as e:
                    logger.warning(f"yfinance failed for {yf_symbol}: {str(e)}")
                    continue
            
            logger.error(f"All yfinance symbol formats failed for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Exception in _fetch_yfinance_data for {symbol}: {str(e)}")
            return None
    
    def _calculate_technical_indicators(self, df: pd.DataFrame) -> TechnicalIndicators:
        """Calculate technical indicators with enhanced debugging"""
        try:
            logger.info(f"Calculating indicators for {len(df)} data points")
            
            if len(df) < 5:
                logger.warning("Insufficient data for any technical indicators")
                return TechnicalIndicators()
            
            high = df['high'].values.astype(float)
            low = df['low'].values.astype(float)
            close = df['close'].values.astype(float)
            volume = df['volume'].values.astype(float)
            open_price = df['open'].values.astype(float)
            
            # Validate data
            if np.any(np.isnan(close)) or np.any(np.isinf(close)):
                logger.error("Invalid price data detected")
                return TechnicalIndicators()
            
            indicators = TechnicalIndicators()
            
            # Calculate indicators with proper error handling
            try:
                # Moving Averages
                if len(close) >= 5:
                    indicators.sma_5 = float(talib.SMA(close, timeperiod=5)[-1])
                    indicators.ema_5 = float(talib.EMA(close, timeperiod=5)[-1])
                    logger.info("5-period averages calculated")
                
                if len(close) >= 10:
                    indicators.sma_10 = float(talib.SMA(close, timeperiod=10)[-1])
                    indicators.ema_10 = float(talib.EMA(close, timeperiod=10)[-1])
                    logger.info("10-period averages calculated")
                
                if len(close) >= 20:
                    indicators.sma_20 = float(talib.SMA(close, timeperiod=20)[-1])
                    indicators.ema_20 = float(talib.EMA(close, timeperiod=20)[-1])
                    
                    # Bollinger Bands
                    bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
                    indicators.bb_upper = float(bb_upper[-1])
                    indicators.bb_middle = float(bb_middle[-1])
                    indicators.bb_lower = float(bb_lower[-1])
                    indicators.bb_width = ((bb_upper[-1] - bb_lower[-1]) / bb_middle[-1]) * 100
                    indicators.bb_position = ((close[-1] - bb_lower[-1]) / (bb_upper[-1] - bb_lower[-1])) * 100
                    logger.info("20-period indicators calculated")
                
                if len(close) >= 50:
                    indicators.sma_50 = float(talib.SMA(close, timeperiod=50)[-1])
                    indicators.ema_50 = float(talib.EMA(close, timeperiod=50)[-1])
                    logger.info("50-period averages calculated")
                
                if len(close) >= 200:
                    indicators.sma_200 = float(talib.SMA(close, timeperiod=200)[-1])
                    indicators.ema_200 = float(talib.EMA(close, timeperiod=200)[-1])
                    logger.info("200-period averages calculated")
                
                # RSI
                if len(close) >= 14:
                    rsi_values = talib.RSI(close, timeperiod=14)
                    indicators.rsi = float(rsi_values[-1])
                    if indicators.rsi >= 70:
                        indicators.rsi_signal = "OVERBOUGHT"
                    elif indicators.rsi <= 30:
                        indicators.rsi_signal = "OVERSOLD"
                    else:
                        indicators.rsi_signal = "NEUTRAL"
                    logger.info(f"RSI calculated: {indicators.rsi:.2f}")
                
                # MACD
                if len(close) >= 26:
                    macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
                    indicators.macd = float(macd[-1])
                    indicators.macd_signal = float(macd_signal[-1])
                    indicators.macd_histogram = float(macd_hist[-1])
                    logger.info("MACD calculated")
                
                # ADX
                if len(close) >= 14:
                    indicators.adx = float(talib.ADX(high, low, close, timeperiod=14)[-1])
                    indicators.plus_di = float(talib.PLUS_DI(high, low, close, timeperiod=14)[-1])
                    indicators.minus_di = float(talib.MINUS_DI(high, low, close, timeperiod=14)[-1])
                
                # Stochastic
                if len(close) >= 14:
                    stoch_k, stoch_d = talib.STOCH(high, low, close, fastk_period=14, slowk_period=3, slowd_period=3)
                    indicators.stoch_k = float(stoch_k[-1])
                    indicators.stoch_d = float(stoch_d[-1])
                    if indicators.stoch_k >= 80:
                        indicators.stoch_signal = "OVERBOUGHT"
                    elif indicators.stoch_k <= 20:
                        indicators.stoch_signal = "OVERSOLD"
                    else:
                        indicators.stoch_signal = "NEUTRAL"
                
                # Williams %R
                if len(close) >= 14:
                    indicators.williams_r = float(talib.WILLR(high, low, close, timeperiod=14)[-1])
                
                # CCI
                if len(close) >= 20:
                    indicators.cci = float(talib.CCI(high, low, close, timeperiod=20)[-1])
                
                # Volume Indicators
                if len(close) >= 10:
                    indicators.obv = float(talib.OBV(close, volume)[-1])
                    # OBV trend (simplified)
                    obv_ma = talib.SMA(talib.OBV(close, volume), timeperiod=5)
                    if len(obv_ma) >= 2:
                        indicators.obv_trend = "UP" if obv_ma[-1] > obv_ma[-2] else "DOWN"
                
                # VWAP (simplified calculation)
                if len(close) >= 20:
                    typical_price = (high + low + close) / 3
                    vwap_data = (typical_price * volume).cumsum() / volume.cumsum()
                    indicators.vwap = float(vwap_data[-1])
                
                # MFI
                if len(close) >= 14:
                    indicators.mfi = float(talib.MFI(high, low, close, volume, timeperiod=14)[-1])
                
                # A/D Line
                if len(close) >= 10:
                    indicators.ad_line = float(talib.AD(high, low, close, volume)[-1])
                
                # Candlestick Patterns
                patterns = {}
                if len(close) >= 10:
                    patterns['DOJI'] = int(talib.CDLDOJI(open_price, high, low, close)[-1])
                    patterns['HAMMER'] = int(talib.CDLHAMMER(open_price, high, low, close)[-1])
                    patterns['ENGULFING'] = int(talib.CDLENGULFING(open_price, high, low, close)[-1])
                    patterns['MORNING_STAR'] = int(talib.CDLMORNINGSTAR(open_price, high, low, close)[-1])
                    patterns['EVENING_STAR'] = int(talib.CDLEVENINGSTAR(open_price, high, low, close)[-1])
                    patterns['SHOOTING_STAR'] = int(talib.CDLSHOOTINGSTAR(open_price, high, low, close)[-1])
                
                indicators.candlestick_patterns = patterns
                
                # Support and Resistance (simplified)
                if len(close) >= 20:
                    recent_highs = pd.Series(high[-20:])
                    recent_lows = pd.Series(low[-20:])
                    indicators.resistance_level = float(recent_highs.quantile(0.9))
                    indicators.support_level = float(recent_lows.quantile(0.1))
                
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
                
                logger.info(f"Technical indicators calculation completed. RSI: {indicators.rsi}, SMA20: {indicators.sma_20}")
                
            except Exception as e:
                logger.error(f"Error in specific indicator calculation: {str(e)}")
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating technical indicators: {str(e)}")
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
                
                macro.atr = float(talib.ATR(high, low, close, timeperiod=14)[-1])
                
                # Calculate 30-day volatility
                if len(close) >= 30:
                    returns = pd.Series(close).pct_change().dropna()
                    macro.volatility = float(returns.std() * np.sqrt(252) * 100)  # Annualized volatility
            
            return macro
            
        except Exception as e:
            logger.error(f"Error calculating macro indicators: {e}")
            return MacroData()
    
    def _generate_signals_and_insights(self, symbol: str, df: pd.DataFrame, 
                                     technical: TechnicalIndicators, macro: MacroData) -> Tuple[str, float, List[str]]:
        """Generate trading signals and insights"""
        try:
            signals = []
            bullish_signals = 0
            bearish_signals = 0
            insights = []
            
            current_price = df['close'].iloc[-1]
            
            # Price Action Analysis
            if technical.sma_20 and technical.sma_50:
                if current_price > technical.sma_20 > technical.sma_50:
                    signals.append("BULLISH")
                    bullish_signals += 2
                    insights.append(f"Price above key moving averages (SMA20: {technical.sma_20:.2f}, SMA50: {technical.sma_50:.2f})")
                elif current_price < technical.sma_20 < technical.sma_50:
                    signals.append("BEARISH")
                    bearish_signals += 2
                    insights.append(f"Price below key moving averages - downtrend confirmed")
            
            # RSI Analysis
            if technical.rsi:
                if technical.rsi > 70:
                    signals.append("OVERBOUGHT")
                    bearish_signals += 1
                    insights.append(f"RSI at {technical.rsi:.1f} - overbought territory")
                elif technical.rsi < 30:
                    signals.append("OVERSOLD")
                    bullish_signals += 1
                    insights.append(f"RSI at {technical.rsi:.1f} - oversold, potential bounce")
                elif 45 <= technical.rsi <= 55:
                    insights.append(f"RSI at {technical.rsi:.1f} - neutral momentum")
            
            # MACD Analysis
            if technical.macd and technical.macd_signal:
                if technical.macd > technical.macd_signal and technical.macd_histogram > 0:
                    signals.append("MACD_BULLISH")
                    bullish_signals += 1
                    insights.append("MACD showing bullish momentum")
                elif technical.macd < technical.macd_signal and technical.macd_histogram < 0:
                    signals.append("MACD_BEARISH")
                    bearish_signals += 1
                    insights.append("MACD showing bearish momentum")
            
            # Bollinger Bands Analysis
            if technical.bb_upper and technical.bb_lower and technical.bb_position:
                if technical.bb_position > 80:
                    signals.append("BB_OVERBOUGHT")
                    bearish_signals += 1
                    insights.append(f"Price near upper Bollinger Band ({technical.bb_position:.1f}%) - potential resistance")
                elif technical.bb_position < 20:
                    signals.append("BB_OVERSOLD")
                    bullish_signals += 1
                    insights.append(f"Price near lower Bollinger Band ({technical.bb_position:.1f}%) - potential support")
            
            # Volume Analysis
            if macro.relative_volume and macro.relative_volume > 1.5:
                insights.append(f"High relative volume ({macro.relative_volume:.1f}x) - increased interest")
                bullish_signals += 1
            elif macro.relative_volume and macro.relative_volume < 0.5:
                insights.append(f"Low relative volume ({macro.relative_volume:.1f}x) - reduced interest")
            
            # ADX Trend Strength
            if technical.adx:
                if technical.adx > 25:
                    if technical.plus_di and technical.minus_di:
                        if technical.plus_di > technical.minus_di:
                            insights.append(f"Strong uptrend (ADX: {technical.adx:.1f})")
                            bullish_signals += 1
                        else:
                            insights.append(f"Strong downtrend (ADX: {technical.adx:.1f})")
                            bearish_signals += 1
                else:
                    insights.append(f"Weak trend (ADX: {technical.adx:.1f}) - consolidation phase")
            
            # 52-Week Position Analysis
            if macro.current_position_pct:
                if macro.current_position_pct > 80:
                    insights.append(f"Near 52-week high ({macro.current_position_pct:.1f}%) - strong momentum")
                    bullish_signals += 1
                elif macro.current_position_pct < 20:
                    insights.append(f"Near 52-week low ({macro.current_position_pct:.1f}%) - potential value")
                    bearish_signals += 1
            
            # Candlestick Pattern Analysis
            if technical.candlestick_patterns:
                for pattern, value in technical.candlestick_patterns.items():
                    if value > 0:
                        if pattern in ['HAMMER', 'MORNING_STAR']:
                            insights.append(f"Bullish {pattern.replace('_', ' ').title()} pattern detected")
                            bullish_signals += 1
                        elif pattern in ['SHOOTING_STAR', 'EVENING_STAR']:
                            insights.append(f"Bearish {pattern.replace('_', ' ').title()} pattern detected")
                            bearish_signals += 1
                        elif pattern == 'DOJI':
                            insights.append("Doji pattern - indecision in market")
            
            # Support/Resistance Analysis
            if technical.support_level and technical.resistance_level:
                distance_to_resistance = ((technical.resistance_level - current_price) / current_price) * 100
                distance_to_support = ((current_price - technical.support_level) / current_price) * 100
                
                if distance_to_resistance < 2:
                    insights.append(f"Near resistance at ₹{technical.resistance_level:.2f}")
                    bearish_signals += 1
                elif distance_to_support < 2:
                    insights.append(f"Near support at ₹{technical.support_level:.2f}")
                    bullish_signals += 1
            
            # Generate Overall Signal
            total_signals = bullish_signals + bearish_signals
            if total_signals == 0:
                overall_signal = "NEUTRAL"
                confidence = 0.0
            else:
                bullish_ratio = bullish_signals / total_signals
                if bullish_ratio >= 0.7:
                    overall_signal = "STRONG_BUY"
                    confidence = min(bullish_ratio * 100, 95)
                elif bullish_ratio >= 0.6:
                    overall_signal = "BUY"
                    confidence = bullish_ratio * 100
                elif bullish_ratio <= 0.3:
                    overall_signal = "STRONG_SELL"
                    confidence = min((1 - bullish_ratio) * 100, 95)
                elif bullish_ratio <= 0.4:
                    overall_signal = "SELL"
                    confidence = (1 - bullish_ratio) * 100
                else:
                    overall_signal = "HOLD"
                    confidence = 50 + abs(bullish_ratio - 0.5) * 100
            
            # Add summary insight
            if insights:
                summary = f"Analysis based on {len(insights)} key factors"
                insights.insert(0, summary)
            
            return overall_signal, confidence, insights
            
        except Exception as e:
            logger.error(f"Error generating signals for {symbol}: {e}")
            return "NEUTRAL", 0.0, ["Analysis incomplete due to data issues"]
    
    async def analyze_stock(self, symbol: str) -> Optional[StockAnalysis]:
        """Main method to analyze a single stock"""
        try:
            logger.info(f"Starting analysis for {symbol}")
            
            # Determine market status and data freshness
            start_time, end_time, data_freshness = self._get_optimal_data_range()
            market_status = "OPEN" if self._is_market_open() else "CLOSED"
            
            # Fetch price data
            df = await self._fetch_moneycontrol_data(symbol)
            if df is None or df.empty:
                logger.error(f"No price data available for {symbol}")
                return None
            
            # Fetch fundamental data in parallel
            yf_data = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_yfinance_data, symbol
            )
            
            # Calculate indicators
            technical_indicators = self._calculate_technical_indicators(df)
            macro_data = self._calculate_macro_indicators(df, yf_data)
            
            # Generate signals
            overall_signal, confidence, insights = self._generate_signals_and_insights(
                symbol, df, technical_indicators, macro_data
            )
            
            # Get current price data
            current_price = float(df['close'].iloc[-1])
            previous_close = float(df['close'].iloc[-2]) if len(df) > 1 else current_price
            price_change = current_price - previous_close
            price_change_pct = (price_change / previous_close) * 100 if previous_close != 0 else 0
            current_volume = int(df['volume'].iloc[-1])
            
            # Create analysis result
            analysis = StockAnalysis(
                symbol=symbol,
                timestamp=datetime.now().isoformat(),
                current_price=current_price,
                price_change=price_change,
                price_change_pct=price_change_pct,
                volume=current_volume,
                technical_indicators=technical_indicators,
                macro_data=macro_data,
                overall_signal=overall_signal,
                confidence_score=confidence,
                key_insights=insights,
                market_status=market_status,
                data_freshness=data_freshness
            )
            
            logger.info(f"Analysis completed for {symbol}: {overall_signal} ({confidence:.1f}% confidence)")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    async def analyze_multiple_stocks(self, symbols: List[str]) -> Dict[str, StockAnalysis]:
        """Analyze multiple stocks concurrently"""
        try:
            logger.info(f"Starting batch analysis for {len(symbols)} stocks")
            
            # Create semaphore to limit concurrent requests
            max_concurrent = self.config['performance_settings']['async_settings']['max_concurrent_requests']
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def analyze_with_semaphore(symbol):
                async with semaphore:
                    return await self.analyze_stock(symbol)
            
            # Execute analyses concurrently
            tasks = [analyze_with_semaphore(symbol) for symbol in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            analysis_results = {}
            for symbol, result in zip(symbols, results):
                if isinstance(result, Exception):
                    logger.error(f"Analysis failed for {symbol}: {result}")
                elif result is not None:
                    analysis_results[symbol] = result
                else:
                    logger.warning(f"No analysis result for {symbol}")
            
            logger.info(f"Batch analysis completed: {len(analysis_results)}/{len(symbols)} successful")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error in batch analysis: {e}")
            return {}
    
    def format_analysis_report(self, analysis: StockAnalysis) -> str:
        """Format analysis results into a readable report"""
        try:
            report = []
            report.append(f"{'='*60}")
            report.append(f"STOCK ANALYSIS REPORT - {analysis.symbol}")
            report.append(f"{'='*60}")
            report.append(f"Timestamp: {analysis.timestamp}")
            report.append(f"Market Status: {analysis.market_status}")
            report.append(f"Data Freshness: {analysis.data_freshness}")
            report.append("")
            
            # Price Information
            report.append("PRICE INFORMATION:")
            report.append(f"Current Price: ₹{analysis.current_price:.2f}")
            change_symbol = "+" if analysis.price_change >= 0 else ""
            report.append(f"Change: {change_symbol}₹{analysis.price_change:.2f} ({change_symbol}{analysis.price_change_pct:.2f}%)")
            report.append(f"Volume: {analysis.volume:,}")
            report.append("")
            
            # Overall Signal
            report.append("TRADING SIGNAL:")
            report.append(f"Recommendation: {analysis.overall_signal}")
            report.append(f"Confidence: {analysis.confidence_score:.1f}%")
            report.append("")
            
            # Key Insights
            report.append("KEY INSIGHTS:")
            for i, insight in enumerate(analysis.key_insights, 1):
                report.append(f"{i}. {insight}")
            report.append("")
            
            # Technical Indicators Summary
            tech = analysis.technical_indicators
            report.append("TECHNICAL INDICATORS:")
            if tech.rsi:
                report.append(f"RSI (14): {tech.rsi:.1f} - {tech.rsi_signal}")
            if tech.sma_20 and tech.sma_50:
                report.append(f"SMA20: ₹{tech.sma_20:.2f}, SMA50: ₹{tech.sma_50:.2f}")
            if tech.macd and tech.macd_signal:
                report.append(f"MACD: {tech.macd:.4f}, Signal: {tech.macd_signal:.4f}")
            if tech.bb_position:
                report.append(f"Bollinger Band Position: {tech.bb_position:.1f}%")
            if tech.adx:
                report.append(f"ADX: {tech.adx:.1f} - Trend Strength: {tech.trend_strength}")
            report.append("")
            
            # Macro Data Summary
            macro = analysis.macro_data
            report.append("FUNDAMENTAL DATA:")
            if macro.week_52_high and macro.week_52_low:
                report.append(f"52-Week Range: ₹{macro.week_52_low:.2f} - ₹{macro.week_52_high:.2f}")
            if macro.current_position_pct:
                report.append(f"Position in 52W Range: {macro.current_position_pct:.1f}%")
            if macro.relative_volume:
                report.append(f"Relative Volume: {macro.relative_volume:.1f}x")
            if macro.volatility:
                report.append(f"Volatility (30D): {macro.volatility:.1f}%")
            if macro.pe_ratio:
                report.append(f"P/E Ratio: {macro.pe_ratio:.2f}")
            
            report.append(f"{'='*60}")
            
            return "\n".join(report)
            
        except Exception as e:
            logger.error(f"Error formatting report: {e}")
            return f"Error generating report for {analysis.symbol}"
    
    async def __aenter__(self):
        """Async context manager entry"""
        timeout = aiohttp.ClientTimeout(
            total=self.config['performance_settings']['async_settings']['request_timeout']
        )
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    def save_analysis_to_json(self, analysis_results: Dict[str, StockAnalysis], filename: str = None):
        """Save analysis results to JSON file"""
        try:
            if filename is None:
                filename = f"stock_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Convert to serializable format
            serializable_data = {}
            for symbol, analysis in analysis_results.items():
                serializable_data[symbol] = {
                    'symbol': analysis.symbol,
                    'timestamp': analysis.timestamp,
                    'current_price': analysis.current_price,
                    'price_change': analysis.price_change,
                    'price_change_pct': analysis.price_change_pct,
                    'volume': analysis.volume,
                    'overall_signal': analysis.overall_signal,
                    'confidence_score': analysis.confidence_score,
                    'key_insights': analysis.key_insights,
                    'market_status': analysis.market_status,
                    'data_freshness': analysis.data_freshness,
                    'technical_indicators': asdict(analysis.technical_indicators),
                    'macro_data': asdict(analysis.macro_data)
                }
            
            with open(filename, 'w') as f:
                json.dump(serializable_data, f, indent=2, default=str)
            
            logger.info(f"Analysis results saved to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")


# Example usage and main execution
async def main():
    """Main execution function with examples"""
    try:
        # Initialize the analysis engine
        async with TechnicalAnalysisEngine() as engine:
            
            # Single stock analysis
            print("Analyzing single stock...")
            analysis = await engine.analyze_stock("RELIANCE")
            if analysis:
                print(engine.format_analysis_report(analysis))
            
            # Multiple stocks analysis
            print("\nAnalyzing multiple stocks...")
            symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
            batch_results = await engine.analyze_multiple_stocks(symbols)
            
            # Print summary of batch results
            print(f"\nBatch Analysis Summary ({len(batch_results)} stocks):")
            print("-" * 60)
            for symbol, analysis in batch_results.items():
                print(f"{symbol:<12} | {analysis.overall_signal:<12} | {analysis.confidence_score:>5.1f}% | ₹{analysis.current_price:>8.2f}")
            
            # Save results to JSON
            if batch_results:
                engine.save_analysis_to_json(batch_results)
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}")


if __name__ == "__main__":
    # Run the analysis
    asyncio.run(main())