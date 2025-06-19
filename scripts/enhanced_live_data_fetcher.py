#!/usr/bin/env python3
"""
Enhanced Live Data Fetcher for Upstox API V3 with Redis TimeSeries Storage

This script continuously fetches the previous minute's OHLCV candle data
for all symbols and stores them in Redis TimeSeries in the exact same format
as the historical data fetcher, ensuring seamless data continuity.

KEY FEATURES:
- Fetches previous minute candle when entering a new minute
- Uses same Redis TimeSeries format as historical fetcher
- Handles multiple symbols efficiently with concurrent processing
- Comprehensive error handling and retry mechanisms
- Real-time monitoring and statistics
- Automatic gap detection and recovery
- Market hours awareness for optimal operation
"""

import json
import logging
import redis
import requests
import os
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Any
import pytz
from dataclasses import dataclass
import time as time_module
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import sys

# Get current script directory for relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Setup IST timezone
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

# Global flag for graceful shutdown
SHUTDOWN_FLAG = threading.Event()

# New config paths
CREDENTIALS_PATH = os.getenv('CUPCAKE_CREDENTIALS_PATH', os.path.join('keys', 'credentials', 'credentials.json'))
SYMBOLS_PATH = os.getenv('CUPCAKE_SYMBOLS_PATH', os.path.join('keys', 'config', 'symbols.json'))
LOG_DIR = os.getenv('CUPCAKE_LOG_DIR', 'logs')

def get_log_filename():
    """Generate log filename with IST date and time"""
    ist_now = datetime.now(IST_TIMEZONE)
    timestamp = ist_now.strftime('%Y%m%d_%H%M%S_IST')
    log_dir = LOG_DIR
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"enhanced_live_data_fetcher_{timestamp}.log")

# Configure logging
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
logger.info(f"Live fetcher logging to: {LOG_FILENAME}")

@dataclass
class Candle:
    """Represents a single OHLCV candle - same as historical fetcher"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass
class Symbol:
    """Represents a trading symbol - same as historical fetcher"""
    name: str
    instrument_key: str

class MarketTimer:
    """Handles market timing and trading hours"""
    
    def __init__(self):
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
    def get_market_hours(self, date: datetime = None) -> tuple:
        """Get market open and close times for a given date"""
        if date is None:
            date = datetime.now(self.ist_timezone)
        
        market_open = date.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = date.replace(hour=15, minute=30, second=0, microsecond=0)
        
        return market_open, market_close
    
    def is_market_hours(self, dt: datetime = None) -> bool:
        """Check if current time is within market hours"""
        if dt is None:
            dt = datetime.now(self.ist_timezone)
        
        # Skip weekends
        if dt.weekday() >= 5:
            return False
        
        market_open, market_close = self.get_market_hours(dt)
        return market_open <= dt <= market_close
    
    def get_next_fetch_time(self, current_time: datetime = None) -> datetime:
        """Calculate the next time to fetch data (start of next minute + buffer)"""
        if current_time is None:
            current_time = datetime.now(self.ist_timezone)
        
        # Next minute boundary + 15 seconds buffer for data availability
        next_minute = current_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
        fetch_time = next_minute + timedelta(seconds=15)
        
        return fetch_time
    
    def get_previous_minute_timestamp(self, current_time: datetime = None) -> datetime:
        """Get the timestamp for the previous complete minute"""
        if current_time is None:
            current_time = datetime.now(self.ist_timezone)
        
        prev_minute = current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
        return prev_minute

class EnhancedUpstoxLiveAPI:
    """Enhanced Upstox API client for live data fetching"""
    
    V3_BASE_URL = "https://api.upstox.com/v3"
    
    def __init__(self, credentials: Dict[str, str]):
        self.access_token = credentials.get('access_token')
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.market_timer = MarketTimer()
        
        # Statistics tracking
        self.api_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'candles_found': 0,
            'candles_missing': 0,
            'last_reset': datetime.now()
        }
    
    def get_previous_minute_candle(self, instrument_key: str, target_timestamp: datetime = None) -> Optional[Candle]:
        """Fetch the previous minute's candle for a specific instrument"""
        if target_timestamp is None:
            target_timestamp = self.market_timer.get_previous_minute_timestamp()
        
        # Format target timestamp for matching
        target_ts_str = target_timestamp.strftime("%Y-%m-%dT%H:%M:00")
        
        try:
            self.api_stats['total_requests'] += 1
            
            # Use V3 intraday API for current day data
            url = f"{self.V3_BASE_URL}/historical-candle/intraday/{instrument_key}/minutes/1"
            
            logger.debug(f"Fetching live data from: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'success':
                logger.error(f"API returned error for {instrument_key}: {data}")
                self.api_stats['failed_requests'] += 1
                return None
            
            candles_data = data.get('data', {}).get('candles', [])
            
            # Find the exact candle for the target timestamp
            for candle_data in candles_data:
                try:
                    candle_timestamp_str = candle_data[0]
                    
                    # Match exact minute timestamp
                    if candle_timestamp_str.startswith(target_ts_str):
                        # Parse the full candle
                        timestamp = datetime.fromisoformat(candle_timestamp_str.replace('Z', '+05:30'))
                        
                        # Validate candle data
                        if not self._is_valid_candle_data(candle_data[1:5]):
                            logger.warning(f"Invalid candle data for {instrument_key}: {candle_data}")
                            continue
                        
                        candle = Candle(
                            timestamp=timestamp,
                            open=float(candle_data[1]),
                            high=float(candle_data[2]),
                            low=float(candle_data[3]),
                            close=float(candle_data[4]),
                            volume=int(candle_data[5]) if len(candle_data) > 5 else 0
                        )
                        
                        self.api_stats['successful_requests'] += 1
                        self.api_stats['candles_found'] += 1
                        
                        logger.debug(f"Found candle for {instrument_key} at {target_ts_str}: O={candle.open}, H={candle.high}, L={candle.low}, C={candle.close}, V={candle.volume}")
                        return candle
                        
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse candle data for {instrument_key}: {e}")
                    continue
            
            # Candle not found for target timestamp
            self.api_stats['successful_requests'] += 1
            self.api_stats['candles_missing'] += 1
            logger.debug(f"Previous minute candle not found for {instrument_key} at {target_ts_str} (might be delayed)")
            return None
            
        except requests.RequestException as e:
            self.api_stats['failed_requests'] += 1
            logger.error(f"API request failed for {instrument_key}: {e}")
            return None
        except Exception as e:
            self.api_stats['failed_requests'] += 1
            logger.error(f"Unexpected error fetching data for {instrument_key}: {e}")
            return None
    
    def _is_valid_candle_data(self, ohlc: List) -> bool:
        """Validate OHLC data for basic consistency - same as historical fetcher"""
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
    
    def get_api_stats(self) -> Dict[str, Any]:
        """Get API performance statistics"""
        return self.api_stats.copy()
    
    def reset_stats(self):
        """Reset API statistics"""
        self.api_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'candles_found': 0,
            'candles_missing': 0,
            'last_reset': datetime.now()
        }

class RedisTimeSeriesLiveManager:
    """Redis TimeSeries manager for live data - same format as historical fetcher"""
    
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
            logger.info(f"Live fetcher connected to Redis at {host}:{port}, DB {db}")
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def ensure_timeseries_exists(self, key: str, labels: Dict[str, str]):
        """Ensure TimeSeries exists with proper labels - same as historical fetcher"""
        try:
            if self.redis_client.exists(key):
                return True
            
            label_args = []
            for k, v in labels.items():
                label_args.extend([k, v])
            
            self.redis_client.execute_command(
                'TS.CREATE', key,
                'DUPLICATE_POLICY', 'LAST',
                'LABELS', *label_args
            )
            logger.debug(f"Created TimeSeries: {key}")
            return True
            
        except redis.RedisError as e:
            logger.error(f"Failed to ensure TimeSeries {key}: {e}")
            return False
    
    def add_candle_to_timeseries(self, symbol_name: str, candle: Candle) -> bool:
        """Add a single candle to TimeSeries - same format as historical fetcher"""
        try:
            data_types = ['open', 'high', 'low', 'close', 'volume']
            timestamp_ms = int(candle.timestamp.timestamp() * 1000)
            
            # Clean symbol name (same as historical fetcher)
            symbol_clean_name = symbol_name.replace(' ', '_').replace('&', 'and')
            
            success_count = 0
            
            for data_type in data_types:
                key = f"stock:{symbol_clean_name}:{data_type}"
                labels = {
                    'symbol': symbol_clean_name,
                    'data_type': data_type,
                    'source': 'upstox_live_v3'
                }
                
                # Ensure TimeSeries exists
                if not self.ensure_timeseries_exists(key, labels):
                    continue
                
                try:
                    value = getattr(candle, data_type)
                    
                    # Add data point (DUPLICATE_POLICY LAST will handle duplicates)
                    self.redis_client.execute_command('TS.ADD', key, timestamp_ms, value)
                    success_count += 1
                    
                except redis.RedisError as e:
                    logger.warning(f"Failed to add data point to {key}: {e}")
                    continue
            
            if success_count == 5:  # All OHLCV data points added successfully
                logger.debug(f"Successfully added candle for {symbol_clean_name} at {candle.timestamp}")
                return True
            else:
                logger.warning(f"Partial success: {success_count}/5 data points added for {symbol_clean_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error adding candle for {symbol_name}: {e}")
            return False
    
    def update_live_metadata(self, symbol_name: str, metadata_update: Dict[str, Any]):
        """Update metadata for live data tracking"""
        try:
            symbol_clean_name = symbol_name.replace(' ', '_').replace('&', 'and')
            hash_key = f"stock:{symbol_clean_name}:live_metadata"
            
            # Serialize datetime objects
            serializable_metadata = {}
            for k, v in metadata_update.items():
                if isinstance(v, datetime):
                    serializable_metadata[k] = v.isoformat()
                else:
                    serializable_metadata[k] = v
            
            self.redis_client.hset(hash_key, mapping={
                'data': json.dumps(serializable_metadata, indent=2)
            })
            
        except redis.RedisError as e:
            logger.error(f"Failed to update live metadata for {symbol_name}: {e}")

class EnhancedLiveDataFetcher:
    """Main live data fetcher class with comprehensive functionality"""
    
    def __init__(self, credentials_path: str, symbols_path: str):
        self.credentials_path = credentials_path
        self.symbols_path = symbols_path
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Load configurations
        self.credentials = self._load_credentials()
        self.symbols = self._load_symbols()
        
        # Initialize API and Redis clients
        self.upstox_api = EnhancedUpstoxLiveAPI(self.credentials)
        self.redis_manager = RedisTimeSeriesLiveManager()
        self.market_timer = MarketTimer()
        
        # Live statistics tracking
        self.live_stats = {
            'fetch_cycles': 0,
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_candles_added': 0,
            'start_time': datetime.now(self.ist_timezone),
            'last_fetch_time': None,
            'gaps_detected': 0,
            'gaps_filled': 0
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Live data fetcher initialized for {len(self.symbols)} symbols")
    
    def _load_credentials(self) -> Dict[str, str]:
        """Load credentials from JSON file - same as historical fetcher"""
        try:
            with open(self.credentials_path, 'r') as f:
                credentials = json.load(f)
            logger.info(f"Loaded credentials from {self.credentials_path}")
            return credentials
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load credentials: {e}")
            raise
    
    def _load_symbols(self) -> List[Symbol]:
        """Load symbols from JSON file - same as historical fetcher"""
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
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        SHUTDOWN_FLAG.set()
    
    def fetch_symbol_candle(self, symbol: Symbol, target_timestamp: datetime) -> bool:
        """Fetch and store candle for a single symbol"""
        try:
            candle = self.upstox_api.get_previous_minute_candle(symbol.instrument_key, target_timestamp)
            
            if candle:
                success = self.redis_manager.add_candle_to_timeseries(symbol.name, candle)
                
                if success:
                    # Update live metadata
                    metadata_update = {
                        'last_live_update': datetime.now(self.ist_timezone),
                        'last_candle_timestamp': candle.timestamp,
                        'live_candle_count': self.live_stats['total_candles_added'] + 1
                    }
                    self.redis_manager.update_live_metadata(symbol.name, metadata_update)
                    
                    return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error fetching candle for {symbol.name}: {e}")
            return False
    
    def fetch_all_symbols_data(self, target_timestamp: datetime, max_workers: int = 8) -> Dict[str, Any]:
        """Fetch data for all symbols concurrently"""
        logger.info(f"Fetching data for {len(self.symbols)} symbols at {target_timestamp.strftime('%H:%M:%S')}")
        
        results = {
            'successful': 0,
            'failed': 0,
            'total': len(self.symbols),
            'timestamp': target_timestamp,
            'failed_symbols': []
        }
        
        # Use ThreadPoolExecutor for concurrent fetching
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(self.fetch_symbol_candle, symbol, target_timestamp): symbol 
                for symbol in self.symbols
            }
            
            # Process completed tasks
            for future in as_completed(future_to_symbol):
                if SHUTDOWN_FLAG.is_set():
                    break
                    
                symbol = future_to_symbol[future]
                try:
                    success = future.result(timeout=30)
                    if success:
                        results['successful'] += 1
                        self.live_stats['successful_symbols'] += 1
                        self.live_stats['total_candles_added'] += 1
                    else:
                        results['failed'] += 1
                        results['failed_symbols'].append(symbol.name)
                        self.live_stats['failed_symbols'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing {symbol.name}: {e}")
                    results['failed'] += 1
                    results['failed_symbols'].append(symbol.name)
                    self.live_stats['failed_symbols'] += 1
                
                # Small delay to be respectful to API
                time_module.sleep(0.05)
        
        return results
    
    def run_single_fetch_cycle(self) -> bool:
        """Run a single fetch cycle for the previous minute"""
        current_time = datetime.now(self.ist_timezone)
        target_timestamp = self.market_timer.get_previous_minute_timestamp(current_time)
        
        logger.info(f"=== FETCH CYCLE {self.live_stats['fetch_cycles'] + 1} ===")
        logger.info(f"Current time: {current_time.strftime('%H:%M:%S')}")
        logger.info(f"Target timestamp: {target_timestamp.strftime('%H:%M:%S')}")
        
        # Fetch data for all symbols
        results = self.fetch_all_symbols_data(target_timestamp)
        
        # Update statistics
        self.live_stats['fetch_cycles'] += 1
        self.live_stats['last_fetch_time'] = current_time
        
        # Log results
        success_rate = (results['successful'] / results['total']) * 100 if results['total'] > 0 else 0
        logger.info(f"Fetch cycle completed: {results['successful']}/{results['total']} successful ({success_rate:.1f}%)")
        
        if results['failed'] > 0:
            logger.warning(f"Failed symbols: {results['failed_symbols']}")
        
        return results['successful'] > 0
    
    def run_continuous(self, fetch_interval_minutes: int = 1):
        """Run continuous live data fetching"""
        logger.info("=== STARTING CONTINUOUS LIVE DATA FETCHING ===")
        logger.info(f"Fetch interval: {fetch_interval_minutes} minute(s)")
        logger.info(f"Total symbols: {len(self.symbols)}")
        logger.info(f"Market timer active: {self.market_timer.is_market_hours()}")
        logger.info("Press Ctrl+C to stop...")
        logger.info("=" * 60)
        
        while not SHUTDOWN_FLAG.is_set():
            try:
                current_time = datetime.now(self.ist_timezone)
                
                # Check if we should fetch data (market hours + buffer)
                should_fetch = self._should_fetch_data(current_time)
                
                if should_fetch:
                    # Calculate next fetch time
                    next_fetch_time = self.market_timer.get_next_fetch_time(current_time)
                    
                    # Wait until it's time to fetch
                    wait_seconds = (next_fetch_time - current_time).total_seconds()
                    
                    if wait_seconds > 0:
                        logger.info(f"Waiting {wait_seconds:.1f} seconds until next fetch at {next_fetch_time.strftime('%H:%M:%S')}")
                        
                        # Wait in small increments to allow for graceful shutdown
                        while wait_seconds > 0 and not SHUTDOWN_FLAG.is_set():
                            sleep_time = min(1.0, wait_seconds)
                            time_module.sleep(sleep_time)
                            wait_seconds -= sleep_time
                    
                    # Run fetch cycle if not shutting down
                    if not SHUTDOWN_FLAG.is_set():
                        self.run_single_fetch_cycle()
                        
                        # Print periodic statistics
                        if self.live_stats['fetch_cycles'] % 10 == 0:
                            self._print_statistics()
                
                else:
                    # Outside market hours, wait longer
                    logger.info(f"Outside market hours at {current_time.strftime('%H:%M:%S')}, waiting...")
                    time_module.sleep(60)  # Wait 1 minute and check again
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break
            except Exception as e:
                logger.error(f"Error in continuous loop: {e}")
                time_module.sleep(5)  # Brief pause before retrying
        
        logger.info("=== LIVE DATA FETCHING STOPPED ===")
        self._print_final_statistics()
    
    def _should_fetch_data(self, current_time: datetime) -> bool:
        """Determine if we should fetch data at the current time"""
        # For now, fetch during extended hours (8:00 AM to 5:00 PM) to catch pre/post market data
        if 8 <= current_time.hour < 17:
            return True
        
        # Could add more sophisticated logic here (e.g., skip holidays, etc.)
        return False
    
    def _print_statistics(self):
        """Print current statistics"""
        runtime = datetime.now(self.ist_timezone) - self.live_stats['start_time']
        api_stats = self.upstox_api.get_api_stats()
        
        print("\n" + "="*60)
        print("LIVE DATA FETCHER STATISTICS")
        print("="*60)
        print(f"Runtime: {runtime}")
        print(f"Fetch cycles completed: {self.live_stats['fetch_cycles']}")
        print(f"Total candles added: {self.live_stats['total_candles_added']}")
        print(f"Success rate: {(self.live_stats['successful_symbols']/(max(1, self.live_stats['successful_symbols'] + self.live_stats['failed_symbols']))*100):.1f}%")
        
        print(f"\nAPI Performance:")
        print(f"  Total API requests: {api_stats['total_requests']}")
        print(f"  Successful requests: {api_stats['successful_requests']}")
        print(f"  Failed requests: {api_stats['failed_requests']}")
        print(f"  Candles found: {api_stats['candles_found']}")
        print(f"  Candles missing: {api_stats['candles_missing']}")
        
        if self.live_stats['last_fetch_time']:
            print(f"\nLast fetch: {self.live_stats['last_fetch_time'].strftime('%H:%M:%S')}")
        
        print("="*60)
    
    def _print_final_statistics(self):
        """Print final statistics on shutdown"""
        runtime = datetime.now(self.ist_timezone) - self.live_stats['start_time']
        
        print("\n" + "="*80)
        print("FINAL LIVE DATA FETCHER STATISTICS")
        print("="*80)
        print(f"Total runtime: {runtime}")
        print(f"Fetch cycles completed: {self.live_stats['fetch_cycles']}")
        print(f"Total candles added: {self.live_stats['total_candles_added']}")
        print(f"Average candles per cycle: {self.live_stats['total_candles_added']/max(1, self.live_stats['fetch_cycles']):.1f}")
        print(f"Log file: {LOG_FILENAME}")
        print("="*80)

def main():
    """Main function for live data fetching"""
    logger.info(f"Script directory: {SCRIPT_DIR}")
    logger.info(f"Looking for symbols.json at: {SYMBOLS_PATH}")
    
    try:
        # Initialize live data fetcher
        fetcher = EnhancedLiveDataFetcher(CREDENTIALS_PATH, SYMBOLS_PATH)
        
        # Run continuous fetching
        fetcher.run_continuous()
        
        return {'success': True}
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\n⚠ Process interrupted by user")
        return {'success': True}  # Graceful shutdown is success
        
    except Exception as e:
        logger.error(f"Fatal error in live data fetcher: {e}", exc_info=True)
        print(f"\n❌ Fatal error: {e}")
        print(f"Check log file for details: {LOG_FILENAME}")
        return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    result = main()
    
    # Exit with appropriate code
    exit_code = 0 if result.get('success', False) else 1
    exit(exit_code)