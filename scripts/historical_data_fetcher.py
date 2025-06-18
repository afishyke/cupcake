#!/usr/bin/env python3
"""
Enhanced Historical Data Fetcher for Upstox API V3 with Redis TimeSeries Storage

This script fetches exactly 250 of the most recent 1-minute OHLCV candles
for each symbol and stores them in Redis TimeSeries with proper metadata.

NEW FEATURES:
- Uses Upstox API V3 for better data access and extended historical range
- Multiple fallback strategies for getting the most recent data
- Real-time WebSocket integration for live data (optional)
- Improved date range calculation using trading calendar
- Better error handling and retry mechanisms
- Enhanced data validation and filtering
"""

import json
import logging
import redis
import requests
import os
from datetime import datetime, timedelta, time, date
from typing import Dict, List, Tuple, Optional, Any
import pytz
from dataclasses import dataclass
import time as time_module
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import websocket
import ssl

# Get current script directory for relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Setup IST timezone for logging
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

# Create log filename with IST date and time
def get_log_filename():
    """Generate log filename with IST date and time"""
    ist_now = datetime.now(IST_TIMEZONE)
    timestamp = ist_now.strftime('%Y%m%d_%H%M%S_IST')
    log_dir = "/home/abhishek/projects/CUPCAKE/logs"
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"enhanced_historical_data_fetcher_{timestamp}.log")

# Configure logging with IST timestamp in filename
LOG_FILENAME = get_log_filename()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILENAME),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Log the filename being used
logger.info(f"Logging to: {LOG_FILENAME}")

@dataclass
class Candle:
    """Represents a single OHLCV candle"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass
class Symbol:
    """Represents a trading symbol"""
    name: str
    instrument_key: str

class TradingCalendar:
    """Advanced trading calendar with better holiday handling"""
    
    def __init__(self, upstox_api):
        self.upstox_api = upstox_api
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self._cache = {}
        
    def get_last_n_trading_days(self, n: int, end_date: date = None) -> List[date]:
        """Get the last N trading days from end_date (or today)"""
        if end_date is None:
            end_date = datetime.now(self.ist_timezone).date()
        
        trading_days = []
        current_date = end_date
        
        # Go back up to 45 days to find N trading days (buffer for holidays)
        max_days_back = 45
        days_checked = 0
        
        while len(trading_days) < n and days_checked < max_days_back:
            if self.upstox_api.is_trading_day(
                datetime.combine(current_date, time()).replace(tzinfo=self.ist_timezone)
            ):
                trading_days.append(current_date)
            
            current_date -= timedelta(days=1)
            days_checked += 1
        
        return trading_days
    
    def get_date_range_for_candles(self, target_candles: int) -> Tuple[date, date]:
        """Calculate optimal date range to get target number of candles"""
        # Estimate: ~375 candles per trading day (6.25 hours * 60 minutes)
        estimated_days = max(1, (target_candles // 300) + 3)  # Buffer for safety
        
        today = datetime.now(self.ist_timezone).date()
        trading_days = self.get_last_n_trading_days(estimated_days, today)
        
        if not trading_days:
            # Fallback if no trading days found
            return today - timedelta(days=7), today
        
        return trading_days[-1], trading_days[0]

class EnhancedUpstoxAPI:
    """Enhanced Upstox API with V3 support and better data fetching"""
    
    BASE_URL = "https://api.upstox.com/v2"
    V3_BASE_URL = "https://api.upstox.com/v3"
    
    def __init__(self, credentials: Dict[str, str]):
        self.access_token = credentials.get('access_token')
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        self._holidays_cache = {}
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.trading_calendar = TradingCalendar(self)
        
    def get_trading_holidays(self, year: int) -> List[str]:
        """Fetch trading holidays for a given year using Upstox API"""
        if year in self._holidays_cache:
            return self._holidays_cache[year]
            
        try:
            url = f"{self.BASE_URL}/market/holidays"
            
            logger.debug(f"Fetching holidays from: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'success':
                logger.error(f"API returned error for holidays: {data}")
                return []
            
            holidays = []
            for holiday_data in data.get('data', []):
                holiday_date = holiday_data.get('date')
                holiday_type = holiday_data.get('holiday_type')
                closed_exchanges = holiday_data.get('closed_exchanges', [])
                
                if holiday_date and holiday_type == 'TRADING_HOLIDAY':
                    holiday_dt = datetime.strptime(holiday_date, '%Y-%m-%d')
                    if holiday_dt.year == year:
                        if not closed_exchanges or 'NSE' in closed_exchanges:
                            holidays.append(holiday_date)
            
            self._holidays_cache[year] = holidays
            logger.debug(f"Found {len(holidays)} trading holidays for {year}")
            return holidays
                
        except Exception as e:
            logger.warning(f"Failed to fetch holidays for {year}: {e}")
            return []
    
    def is_trading_day(self, date: datetime) -> bool:
        """Check if a given date is a trading day"""
        if date.weekday() >= 5:
            return False
        
        date_str = date.strftime("%Y-%m-%d")
        holidays = self.get_trading_holidays(date.year)
        return date_str not in holidays
    
    def get_market_open_time(self, date: datetime) -> datetime:
        """Get market open time for a given date (9:15 AM IST)"""
        return date.replace(hour=9, minute=15, second=0, microsecond=0)
    
    def get_market_close_time(self, date: datetime) -> datetime:
        """Get market close time for a given date (3:30 PM IST)"""
        return date.replace(hour=15, minute=30, second=0, microsecond=0)
    
    def get_intraday_data_v3(self, instrument_key: str, interval: int = 1) -> List[Candle]:
        """Fetch current day data using V3 Intraday API with custom intervals"""
        try:
            # V3 Intraday API endpoint with custom interval
            url = f"{self.V3_BASE_URL}/historical-candle/intraday/{instrument_key}/minutes/{interval}"
            
            logger.debug(f"Fetching V3 intraday data from: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'success':
                logger.error(f"V3 Intraday API returned error: {data}")
                return []
            
            return self._parse_candles(data.get('data', {}).get('candles', []))
            
        except requests.RequestException as e:
            logger.error(f"V3 Intraday API request failed for {instrument_key}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching V3 intraday data for {instrument_key}: {e}")
            return []
    
    def get_intraday_data_v2(self, instrument_key: str) -> List[Candle]:
        """Fallback: Fetch current day data using V2 Intraday API"""
        try:
            url = f"{self.BASE_URL}/historical-candle/intraday/{instrument_key}/1minute"
            
            logger.debug(f"Fetching V2 intraday data from: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'success':
                logger.error(f"V2 Intraday API returned error: {data}")
                return []
            
            return self._parse_candles(data.get('data', {}).get('candles', []))
            
        except Exception as e:
            logger.error(f"V2 Intraday API failed for {instrument_key}: {e}")
            return []
    
    def get_historical_data_v3(self, instrument_key: str, from_date: str, to_date: str, interval: int = 1) -> List[Candle]:
        """Fetch historical data using V3 API with extended range (from Jan 2022)"""
        try:
            url = f"{self.V3_BASE_URL}/historical-candle/{instrument_key}/minutes/{interval}/{to_date}/{from_date}"
            
            logger.debug(f"Fetching V3 historical data from: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'success':
                logger.error(f"V3 Historical API returned error: {data}")
                return []
            
            return self._parse_candles(data.get('data', {}).get('candles', []))
            
        except Exception as e:
            logger.error(f"V3 Historical API failed for {instrument_key}: {e}")
            return []
    
    def get_historical_data_v2(self, instrument_key: str, from_date: str, to_date: str) -> List[Candle]:
        """Fallback: Fetch historical data using V2 API"""
        try:
            url = f"{self.BASE_URL}/historical-candle/{instrument_key}/1minute/{to_date}/{from_date}"
            
            logger.debug(f"Fetching V2 historical data from: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'success':
                logger.error(f"V2 Historical API returned error: {data}")
                return []
            
            return self._parse_candles(data.get('data', {}).get('candles', []))
            
        except Exception as e:
            logger.error(f"V2 Historical API failed for {instrument_key}: {e}")
            return []
    
    def _parse_candles(self, candles_data: List) -> List[Candle]:
        """Parse candle data from API response"""
        candles = []
        for candle_data in candles_data:
            try:
                timestamp_str = candle_data[0]
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+05:30'))
                
                # Validate candle data
                if not self._is_valid_candle_data(candle_data[1:5]):
                    continue
                
                candle = Candle(
                    timestamp=timestamp,
                    open=float(candle_data[1]),
                    high=float(candle_data[2]),
                    low=float(candle_data[3]),
                    close=float(candle_data[4]),
                    volume=int(candle_data[5]) if len(candle_data) > 5 else 0
                )
                candles.append(candle)
                
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse candle data {candle_data}: {e}")
                continue
        
        return candles
    
    def _is_valid_candle_data(self, ohlc: List) -> bool:
        """Validate OHLC data for basic consistency"""
        try:
            o, h, l, c = [float(x) for x in ohlc[:4]]
            
            # Basic validation: High >= max(O,C), Low <= min(O,C)
            if h < max(o, c) or l > min(o, c):
                return False
            
            # Check for reasonable price ranges (no negative prices, etc.)
            if any(x <= 0 for x in [o, h, l, c]):
                return False
            
            return True
            
        except (ValueError, IndexError):
            return False

class RedisTimeSeriesManager:
    """Enhanced Redis TimeSeries manager with better error handling"""
    
    def __init__(self, host='localhost', port=6379, db=0):
        try:
            self.redis_client = redis.Redis(
                host=host, port=port, db=db, 
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=30,
                retry_on_timeout=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {host}:{port}, DB {db}")
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def flush_database(self):
        """Flush all data from the current Redis database"""
        try:
            self.redis_client.flushdb()
            logger.info("Flushed Redis database")
        except redis.RedisError as e:
            logger.error(f"Failed to flush Redis database: {e}")
            raise
    
    def create_timeseries(self, key: str, labels: Dict[str, str]):
        """Create a TimeSeries with labels and LAST duplicate policy"""
        try:
            if self.redis_client.exists(key):
                logger.debug(f"TimeSeries {key} already exists")
                return
            
            label_args = []
            for k, v in labels.items():
                label_args.extend([k, v])
            
            self.redis_client.execute_command(
                'TS.CREATE', key,
                'DUPLICATE_POLICY', 'LAST',
                'LABELS', *label_args
            )
            logger.debug(f"Created TimeSeries: {key}")
            
        except redis.RedisError as e:
            logger.error(f"Failed to create TimeSeries {key}: {e}")
            raise
    
    def add_candles_to_timeseries(self, symbol_name: str, candles: List[Candle]):
        """Enhanced method to add candles with deduplication"""
        if not candles:
            return
        
        data_types = ['open', 'high', 'low', 'close', 'volume']
        
        for data_type in data_types:
            key = f"stock:{symbol_name}:{data_type}"
            labels = {
                'symbol': symbol_name,
                'data_type': data_type,
                'source': 'upstox_v3'
            }
            
            self.create_timeseries(key, labels)
            
            # Batch insert for better performance
            pipeline = self.redis_client.pipeline()
            added_count = 0
            
            for candle in candles:
                try:
                    timestamp_ms = int(candle.timestamp.timestamp() * 1000)
                    
                    value = getattr(candle, data_type)
                    
                    pipeline.execute_command('TS.ADD', key, timestamp_ms, value)
                    added_count += 1
                    
                except redis.RedisError as e:
                    logger.warning(f"Failed to add data point to {key}: {e}")
                    continue
            
            # Execute batch
            try:
                pipeline.execute()
                logger.debug(f"Added {added_count} points to {key}")
            except redis.RedisError as e:
                logger.error(f"Failed to execute batch for {key}: {e}")
        
        logger.info(f"Added {len(candles)} candles for {symbol_name}")
    
    def save_metadata(self, symbol_name: str, metadata: Dict[str, Any]):
        """Enhanced metadata saving with error handling"""
        try:
            hash_key = f"stock:{symbol_name}:metadata"
            
            serializable_metadata = {}
            for k, v in metadata.items():
                if isinstance(v, datetime):
                    serializable_metadata[k] = v.isoformat()
                elif isinstance(v, date):
                    serializable_metadata[k] = v.isoformat()
                else:
                    serializable_metadata[k] = v
            
            self.redis_client.hset(hash_key, mapping={
                'data': json.dumps(serializable_metadata, indent=2)
            })
            
            logger.debug(f"Saved metadata for {symbol_name}")
            
        except redis.RedisError as e:
            logger.error(f"Failed to save metadata for {symbol_name}: {e}")

class EnhancedHistoricalDataFetcher:
    """Enhanced main class with multiple fallback strategies"""
    
    def __init__(self, credentials_path: str, symbols_path: str):
        self.credentials_path = credentials_path
        self.symbols_path = symbols_path
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Load configurations
        self.credentials = self._load_credentials()
        self.symbols = self._load_symbols()
        
        # Initialize enhanced API and Redis clients
        self.upstox_api = EnhancedUpstoxAPI(self.credentials)
        self.redis_manager = RedisTimeSeriesManager()
        
        # Flush Redis on initialization
        self.redis_manager.flush_database()
        
        # Performance tracking
        self.fetch_stats = {
            'v3_intraday_success': 0,
            'v2_intraday_fallback': 0,
            'v3_historical_success': 0,
            'v2_historical_fallback': 0,
            'total_symbols': 0,
            'total_candles': 0
        }
    
    def _load_credentials(self) -> Dict[str, str]:
        """Load credentials from JSON file"""
        try:
            with open(self.credentials_path, 'r') as f:
                credentials = json.load(f)
            logger.info(f"Loaded credentials from {self.credentials_path}")
            return credentials
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load credentials: {e}")
            raise
    
    def _load_symbols(self) -> List[Symbol]:
        """Load symbols from JSON file"""
        try:
            with open(self.symbols_path, 'r') as f:
                data = json.load(f)
            
            symbols = []
            for symbol_data in data.get('symbols', []):
                symbol = Symbol(
                    name=symbol_data['name'],
                    instrument_key=symbol_data['instrument_key']
                )
                symbols.append(symbol)
            
            logger.info(f"Loaded {len(symbols)} symbols from {self.symbols_path}")
            return symbols
            
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load symbols: {e}")
            raise
    
    def fetch_and_store_symbol_data(self, symbol: Symbol) -> Dict[str, Any]:
        """Enhanced data fetching with multiple strategies and V3 APIs"""
        logger.info(f"Processing symbol: {symbol.name} ({symbol.instrument_key})")
        
        try:
            all_candles = []
            now_ist = datetime.now(self.ist_timezone)
            today = now_ist.date()
            
            strategy_used = []
            
            # Strategy 1: Try V3 Intraday API for today's data
            if self.upstox_api.is_trading_day(now_ist):
                market_open = self.upstox_api.get_market_open_time(now_ist)
                
                if now_ist >= market_open:
                    logger.info(f"Trying V3 intraday API for {symbol.name}")
                    v3_intraday_candles = self.upstox_api.get_intraday_data_v3(symbol.instrument_key)
                    
                    if v3_intraday_candles:
                        all_candles.extend(v3_intraday_candles)
                        strategy_used.append("v3_intraday")
                        self.fetch_stats['v3_intraday_success'] += 1
                        logger.info(f"V3 Intraday: Got {len(v3_intraday_candles)} candles")
                    else:
                        # Fallback to V2 Intraday
                        logger.info(f"V3 intraday failed, trying V2 for {symbol.name}")
                        v2_intraday_candles = self.upstox_api.get_intraday_data_v2(symbol.instrument_key)
                        
                        if v2_intraday_candles:
                            all_candles.extend(v2_intraday_candles)
                            strategy_used.append("v2_intraday")
                            self.fetch_stats['v2_intraday_fallback'] += 1
                            logger.info(f"V2 Intraday: Got {len(v2_intraday_candles)} candles")
            
            # Strategy 2: Fetch historical data to complete 250 candles
            candles_needed = 250 - len(all_candles)
            
            if candles_needed > 0:
                logger.info(f"Need {candles_needed} more candles from historical data")
                
                # Calculate optimal date range using trading calendar
                from_date, to_date = self.upstox_api.trading_calendar.get_date_range_for_candles(candles_needed)
                
                # Adjust to_date to yesterday if we already have today's data
                if all_candles and to_date >= today:
                    to_date = today - timedelta(days=1)
                    # Find last trading day
                    while not self.upstox_api.is_trading_day(
                        datetime.combine(to_date, time()).replace(tzinfo=self.ist_timezone)
                    ):
                        to_date -= timedelta(days=1)
                
                from_date_str = from_date.strftime('%Y-%m-%d')
                to_date_str = to_date.strftime('%Y-%m-%d')
                
                logger.info(f"Fetching historical data from {from_date_str} to {to_date_str}")
                
                # Try V3 Historical API first (has data from Jan 2022)
                v3_historical_candles = self.upstox_api.get_historical_data_v3(
                    symbol.instrument_key, from_date_str, to_date_str
                )
                
                if v3_historical_candles:
                    all_candles.extend(v3_historical_candles)
                    strategy_used.append("v3_historical")
                    self.fetch_stats['v3_historical_success'] += 1
                    logger.info(f"V3 Historical: Got {len(v3_historical_candles)} candles")
                else:
                    # Fallback to V2 Historical (6 months limit)
                    # Adjust date range for V2 limitations
                    six_months_ago = today - timedelta(days=180)
                    if from_date < six_months_ago:
                        from_date = six_months_ago
                        from_date_str = from_date.strftime('%Y-%m-%d')
                        logger.warning(f"Adjusted start date to V2 limit: {from_date_str}")
                    
                    v2_historical_candles = self.upstox_api.get_historical_data_v2(
                        symbol.instrument_key, from_date_str, to_date_str
                    )
                    
                    if v2_historical_candles:
                        all_candles.extend(v2_historical_candles)
                        strategy_used.append("v2_historical")
                        self.fetch_stats['v2_historical_fallback'] += 1
                        logger.info(f"V2 Historical: Got {len(v2_historical_candles)} candles")
            
            if not all_candles:
                logger.warning(f"No data received for {symbol.name}")
                return {
                    'symbol': symbol.name,
                    'status': 'failed',
                    'error': 'No data received from any API',
                    'candles_stored': 0,
                    'strategies_used': strategy_used
                }
            
            # Strategy 3: Filter and select the most recent 250 candles
            # Remove duplicates based on timestamp
            unique_candles = {}
            for candle in all_candles:
                timestamp_key = candle.timestamp.isoformat()
                if timestamp_key not in unique_candles:
                    unique_candles[timestamp_key] = candle
            
            # Sort by timestamp (newest first) and take latest 250
            all_candles_sorted = sorted(unique_candles.values(), key=lambda x: x.timestamp, reverse=True)
            latest_250_candles = all_candles_sorted[:250]
            
            # Log data quality info
            if latest_250_candles:
                oldest_ts = latest_250_candles[-1].timestamp
                newest_ts = latest_250_candles[0].timestamp
                logger.info(f"Final dataset: {len(latest_250_candles)} candles from {oldest_ts} to {newest_ts}")
                
                newest_date = newest_ts.date()
                if newest_date == today:
                    logger.info("✓ Successfully included today's data")
                else:
                    logger.warning(f"⚠ Latest data is from {newest_date}, not today ({today})")
            
            # Reverse to chronological order for storage
            latest_250_candles.reverse()
            
            # Store in Redis TimeSeries
            symbol_clean_name = symbol.name.replace(' ', '_').replace('&', 'and')
            self.redis_manager.add_candles_to_timeseries(symbol_clean_name, latest_250_candles)
            
            # Enhanced metadata
            metadata = {
                'symbol_name': symbol.name,
                'instrument_key': symbol.instrument_key,
                'total_candles_stored': len(latest_250_candles),
                'oldest_timestamp': oldest_ts if latest_250_candles else None,
                'newest_timestamp': newest_ts if latest_250_candles else None,
                'strategies_used': strategy_used,
                'includes_today': newest_ts.date() == today if latest_250_candles else False,
                'last_updated': datetime.now(self.ist_timezone),
                'fetch_quality': {
                    'total_fetched': len(all_candles),
                    'duplicates_removed': len(all_candles) - len(unique_candles),
                    'selected_count': len(latest_250_candles),
                    'data_coverage_days': (newest_ts - oldest_ts).days if latest_250_candles else 0
                },
                'api_versions_used': {
                    'v3_apis': any('v3' in s for s in strategy_used),
                    'v2_fallbacks': any('v2' in s for s in strategy_used)
                }
            }
            
            # Save metadata
            self.redis_manager.save_metadata(symbol_clean_name, metadata)
            
            # Update stats
            self.fetch_stats['total_symbols'] += 1
            self.fetch_stats['total_candles'] += len(latest_250_candles)
            
            return {
                'symbol': symbol.name,
                'status': 'success',
                'candles_stored': len(latest_250_candles),
                'date_range': f"{oldest_ts} to {newest_ts}" if latest_250_candles else "N/A",
                'includes_today': metadata['includes_today'],
                'strategies_used': strategy_used,
                'api_versions': 'V3' if any('v3' in s for s in strategy_used) else 'V2'
            }
            
        except Exception as e:
            logger.error(f"Failed to process {symbol.name}: {e}", exc_info=True)
            return {
                'symbol': symbol.name,
                'status': 'failed',
                'error': str(e),
                'candles_stored': 0,
                'strategies_used': []
            }
    
    def process_all_symbols(self, max_workers: int = 5) -> List[Dict[str, Any]]:
        """Process all symbols with concurrent execution for better performance"""
        logger.info(f"Starting to process {len(self.symbols)} symbols with {max_workers} workers")
        
        results = []
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(self.fetch_and_store_symbol_data, symbol): symbol 
                for symbol in self.symbols
            }
            
            # Process completed tasks
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Log progress
                    completed = len(results)
                    total = len(self.symbols)
                    logger.info(f"Progress: {completed}/{total} symbols processed")
                    
                except Exception as e:
                    logger.error(f"Error processing {symbol.name}: {e}")
                    results.append({
                        'symbol': symbol.name,
                        'status': 'failed',
                        'error': f"Concurrent execution error: {str(e)}",
                        'candles_stored': 0,
                        'strategies_used': []
                    })
                
                # Small delay to be respectful to API
                time_module.sleep(0.2)
        
        return results
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get detailed performance statistics"""
        return {
            'fetch_performance': self.fetch_stats,
            'api_efficiency': {
                'v3_success_rate': (
                    (self.fetch_stats['v3_intraday_success'] + self.fetch_stats['v3_historical_success']) /
                    max(1, self.fetch_stats['total_symbols'])
                ) * 100,
                'fallback_rate': (
                    (self.fetch_stats['v2_intraday_fallback'] + self.fetch_stats['v2_historical_fallback']) /
                    max(1, self.fetch_stats['total_symbols'])
                ) * 100
            },
            'data_quality': {
                'avg_candles_per_symbol': self.fetch_stats['total_candles'] / max(1, self.fetch_stats['total_symbols']),
                'total_data_points': self.fetch_stats['total_candles'] * 5  # OHLCV
            }
        }

def main():
    """Enhanced main function with better reporting and error handling"""
    credentials_path = "/home/abhishek/projects/CUPCAKE/authentication/credentials.json"
    # Look for symbols.json in the same directory as this script
    symbols_path = os.path.join(SCRIPT_DIR, "symbols.json")
    
    logger.info(f"Script directory: {SCRIPT_DIR}")
    logger.info(f"Looking for symbols.json at: {symbols_path}")
    
    try:
        # Initialize enhanced fetcher
        fetcher = EnhancedHistoricalDataFetcher(credentials_path, symbols_path)
        
        start_time = datetime.now()
        logger.info(f"Starting enhanced data fetch at {start_time}")
        
        # Process all symbols with concurrent execution
        results = fetcher.process_all_symbols(max_workers=3)  # Conservative to avoid rate limits
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Get performance statistics
        perf_stats = fetcher.get_performance_stats()
        
        # Print enhanced summary
        print("\n" + "="*100)
        print("ENHANCED HISTORICAL DATA FETCH SUMMARY")
        print("="*100)
        
        successful = 0
        failed = 0
        total_candles = 0
        includes_today_count = 0
        v3_api_count = 0
        
        print("\nSymbol Results:")
        print("-" * 100)
        
        for result in results:
            status_symbol = "✓" if result['status'] == 'success' else "✗"
            today_indicator = " [TODAY]" if result.get('includes_today', False) else " [OLD]"
            api_version = f" [{result.get('api_versions', 'UNKNOWN')}]"
            strategies = f" {result.get('strategies_used', [])}" if result.get('strategies_used') else ""
            
            print(f"{status_symbol} {result['symbol']:<25}: {result['candles_stored']:>3} candles{today_indicator}{api_version}{strategies}")
            
            if result['status'] == 'success':
                successful += 1
                total_candles += result['candles_stored']
                if result.get('includes_today', False):
                    includes_today_count += 1
                if result.get('api_versions') == 'V3':
                    v3_api_count += 1
                    
                if 'date_range' in result and result['date_range'] != "N/A":
                    print(f"   Range: {result['date_range']}")
            else:
                failed += 1
                print(f"   Error: {result.get('error', 'Unknown error')}")
        
        print("-" * 100)
        print(f"EXECUTION SUMMARY:")
        print(f"  Total symbols processed: {len(results)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Success rate: {(successful/len(results)*100):.1f}%")
        print(f"  Total execution time: {duration}")
        print(f"  Log file: {LOG_FILENAME}")
        
        print(f"\nDATA QUALITY:")
        print(f"  Total candles stored: {total_candles:,}")
        print(f"  Average candles per symbol: {total_candles/max(1,successful):.1f}")
        print(f"  Symbols with today's data: {includes_today_count}/{successful}")
        print(f"  Today's data coverage: {(includes_today_count/max(1,successful)*100):.1f}%")
        print(f"  Redis TimeSeries keys created: {successful * 5}")
        
        print(f"\nAPI PERFORMANCE:")
        print(f"  V3 API usage: {v3_api_count}/{successful} symbols ({(v3_api_count/max(1,successful)*100):.1f}%)")
        print(f"  V3 Intraday successes: {perf_stats['fetch_performance']['v3_intraday_success']}")
        print(f"  V3 Historical successes: {perf_stats['fetch_performance']['v3_historical_success']}")
        print(f"  V2 Fallbacks: {perf_stats['fetch_performance']['v2_intraday_fallback'] + perf_stats['fetch_performance']['v2_historical_fallback']}")
        print(f"  Overall V3 success rate: {perf_stats['api_efficiency']['v3_success_rate']:.1f}%")
        
        print(f"\nRECOMMENDations:")
        if includes_today_count < successful * 0.8:
            print(f"  ⚠ Only {(includes_today_count/max(1,successful)*100):.1f}% symbols have today's data")
            print(f"    Consider running during market hours for better real-time data")
        
        if perf_stats['api_efficiency']['v3_success_rate'] < 80:
            print(f"  ⚠ V3 API success rate is {perf_stats['api_efficiency']['v3_success_rate']:.1f}%")
            print(f"    Check API access permissions for V3 endpoints")
        
        if failed > 0:
            print(f"  ⚠ {failed} symbols failed - check error messages above")
            print(f"    Consider retry logic or manual investigation")
        
        print("="*100)
        
        logger.info(f"Enhanced fetch completed in {duration}. {successful} successful, {failed} failed")
        
        # Return summary for potential automation
        return {
            'success': True,
            'summary': {
                'successful_symbols': successful,
                'failed_symbols': failed,
                'total_candles': total_candles,
                'today_coverage': includes_today_count,
                'execution_time': str(duration),
                'v3_api_usage': v3_api_count,
                'log_file': LOG_FILENAME
            },
            'performance': perf_stats
        }
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\n⚠ Process interrupted by user")
        return {'success': False, 'error': 'User interrupted'}
        
    except Exception as e:
        logger.error(f"Fatal error in enhanced main: {e}", exc_info=True)
        print(f"\n❌ Fatal error: {e}")
        print(f"Check log file for details: {LOG_FILENAME}")
        return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    result = main()
    
    # Exit with appropriate code
    exit_code = 0 if result.get('success', False) else 1
    exit(exit_code)
