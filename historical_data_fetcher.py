#!/usr/bin/env python3
"""
Ultra-Fast Historical Data Fetcher with Backward Compatibility
FIXED: Made Symbol dataclass hashable to resolve ThreadPoolExecutor error
"""

import json
import logging
import redis
import requests
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta, time, date
from typing import Dict, List, Tuple, Optional, Any
import pytz
from dataclasses import dataclass
import time as time_module
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

# Get current script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# IST timezone
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

# Setup optimized logging
def setup_fast_logging():
    """Setup minimal logging for speed"""
    log_dir = "/home/abhishek/projects/CUPCAKE/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    ist_now = datetime.now(IST_TIMEZONE)
    timestamp = ist_now.strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"fast_historical_fetcher_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.WARNING,  # Reduced logging for speed
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__), log_file

logger, LOG_FILE = setup_fast_logging()

@dataclass
class HistoricalCandle:
    """Backward compatible candle representation"""
    symbol: str
    instrument_key: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    interval: str = "1minute"
    
    def to_dict(self):
        return {
            'symbol': self.symbol,
            'instrument_key': self.instrument_key,
            'timestamp': self.timestamp.isoformat(),
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'interval': self.interval
        }

@dataclass
class Candle:
    """Lightweight candle representation for internal use"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass(frozen=True)  # 🔧 FIX: Made frozen=True to make it hashable
class Symbol:
    """Symbol with cleaned name for consistency - FIXED: Now hashable!"""
    name: str
    instrument_key: str
    clean_name: str = None
    
    def __post_init__(self):
        # 🔧 FIX: Use object.__setattr__ for frozen dataclass
        if self.clean_name is None:
            # Match live fetcher naming convention exactly
            clean_name = self.name.replace(' ', '_').replace('&', 'and')
            object.__setattr__(self, 'clean_name', clean_name)

# Backward compatibility - maintain old enum/class names
class Interval:
    """Backward compatible interval class"""
    MINUTE_1 = "1minute"
    MINUTE_5 = "5minute"
    MINUTE_15 = "15minute"
    MINUTE_30 = "30minute"
    HOUR_1 = "1hour"
    DAY_1 = "1day"

class FastUpstoxAPI:
    """Ultra-fast Upstox API client with optimized requests"""
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        self.base_url = "https://api.upstox.com/v2"
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # API call tracking for rate limiting
        self.api_calls = []
        self.lock = threading.Lock()
        
    def _rate_limit_check(self):
        """Smart rate limiting - allow bursts but respect limits"""
        with self.lock:
            now = time_module.time()
            # Remove calls older than 1 minute
            self.api_calls = [t for t in self.api_calls if now - t < 60]
            
            # Allow up to 80 calls per minute (buffer for safety)
            if len(self.api_calls) >= 80:
                sleep_time = 60 - (now - self.api_calls[0])
                if sleep_time > 0:
                    time_module.sleep(sleep_time)
                    self.api_calls = []
            
            self.api_calls.append(now)
    
    def get_historical_data(self, instrument_key: str, days_back: int = 10) -> List[Candle]:
        """Fast historical data fetch with optimized date calculation"""
        try:
            self._rate_limit_check()
            
            # Simple date calculation - go back enough days to ensure 250+ candles
            end_date = datetime.now(IST_TIMEZONE).date()
            start_date = end_date - timedelta(days=days_back)
            
            url = f"{self.base_url}/historical-candle/{instrument_key}/1minute/{end_date}/{start_date}"
            
            response = self.session.get(url, timeout=10)  # Fast timeout
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') != 'success':
                return []
            
            return self._fast_parse_candles(data.get('data', {}).get('candles', []))
            
        except Exception as e:
            logger.warning(f"API call failed for {instrument_key}: {e}")
            return []
    
    def get_intraday_data(self, instrument_key: str) -> List[Candle]:
        """Fast intraday data fetch"""
        try:
            self._rate_limit_check()
            
            url = f"{self.base_url}/historical-candle/intraday/{instrument_key}/1minute"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') != 'success':
                return []
            
            return self._fast_parse_candles(data.get('data', {}).get('candles', []))
            
        except Exception as e:
            logger.warning(f"Intraday API failed for {instrument_key}: {e}")
            return []
    
    def _fast_parse_candles(self, candles_data: List) -> List[Candle]:
        """Ultra-fast candle parsing with minimal validation"""
        candles = []
        
        for candle_data in candles_data:
            try:
                # Fast timestamp parsing
                timestamp_str = candle_data[0]
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+05:30'
                
                timestamp = datetime.fromisoformat(timestamp_str)
                
                # Quick validation - just check for positive values
                ohlc = [float(x) for x in candle_data[1:5]]
                if all(x > 0 for x in ohlc):
                    candle = Candle(
                        timestamp=timestamp,
                        open=ohlc[0],
                        high=ohlc[1],
                        low=ohlc[2],
                        close=ohlc[3],
                        volume=int(candle_data[5]) if len(candle_data) > 5 else 0
                    )
                    candles.append(candle)
                    
            except (ValueError, IndexError):
                continue  # Skip invalid candles quickly
        
        return candles

class FastRedisManager:
    """Ultra-fast Redis operations with batch processing"""
    
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(
            host=host, port=port, db=db,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )
        
        # Test connection
        try:
            self.redis_client.ping()
        except redis.RedisError as e:
            raise Exception(f"Redis connection failed: {e}")
    
    def flush_and_prepare(self):
        """Fast database preparation"""
        try:
            self.redis_client.flushdb()
            print("✓ Redis database cleared")
        except redis.RedisError as e:
            raise Exception(f"Failed to flush Redis: {e}")
    
    def bulk_store_candles(self, symbol_data_map: Dict[Symbol, List[Candle]]):
        """Ultra-fast bulk storage using pipelines"""
        data_types = ['open', 'high', 'low', 'close', 'volume']
        
        # Group operations by data type for maximum efficiency
        for data_type in data_types:
            pipeline = self.redis_client.pipeline()
            operations_count = 0
            
            for symbol, candles in symbol_data_map.items():
                if not candles:
                    continue
                
                # Create TimeSeries key matching live fetcher format exactly
                key = f"stock:{symbol.clean_name}:{data_type}"
                
                # Create TimeSeries if needed
                try:
                    pipeline.execute_command(
                        'TS.CREATE', key,
                        'DUPLICATE_POLICY', 'LAST',
                        'LABELS', 'symbol', symbol.clean_name, 'data_type', data_type, 'source', 'historical_fast'
                    )
                except:
                    pass  # Key might exist
                
                # Batch add all candles for this symbol and data type
                for candle in candles:
                    timestamp_ms = int(candle.timestamp.timestamp() * 1000)
                    value = getattr(candle, data_type)
                    
                    pipeline.execute_command('TS.ADD', key, timestamp_ms, value)
                    operations_count += 1
            
            # Execute batch
            if operations_count > 0:
                try:
                    pipeline.execute()
                    print(f"✓ Bulk stored {data_type} data ({operations_count} operations)")
                except redis.RedisError as e:
                    print(f"⚠ Batch operation failed for {data_type}: {e}")
    
    def store_metadata_bulk(self, metadata_map: Dict[Symbol, Dict]):
        """Fast bulk metadata storage"""
        pipeline = self.redis_client.pipeline()
        
        for symbol, metadata in metadata_map.items():
            hash_key = f"stock:{symbol.clean_name}:metadata"
            
            # Serialize metadata quickly
            serialized = {}
            for k, v in metadata.items():
                if isinstance(v, (datetime, date)):
                    serialized[k] = v.isoformat()
                else:
                    serialized[k] = v
            
            pipeline.hset(hash_key, mapping={
                'data': json.dumps(serialized, separators=(',', ':'))  # Compact JSON
            })
        
        try:
            pipeline.execute()
            print(f"✓ Stored metadata for {len(metadata_map)} symbols")
        except redis.RedisError as e:
            print(f"⚠ Metadata storage failed: {e}")

class HistoricalDataFetcher:
    """Backward compatible class that wraps ultra-fast functionality"""
    
    def __init__(self, config=None, credentials_path=None, symbols_path=None):
        # Handle both old and new initialization patterns
        if config:
            self.config = config
            credentials_path = "/home/abhishek/projects/cupcake/credentials.json"
            symbols_path = os.path.join(os.path.dirname(__file__), "symbols.json")
        
        self.credentials_path = credentials_path or "/home/abhishek/projects/cupcake/credentials.json"
        self.symbols_path = symbols_path or os.path.join(os.path.dirname(__file__), "symbols.json")
        
        # Initialize the ultra-fast fetcher
        self.ultra_fast_fetcher = UltraFastHistoricalFetcher(self.credentials_path, self.symbols_path)
        
        # Backward compatibility properties
        self.ist_timezone = IST_TIMEZONE
        
    async def initialize(self):
        """Backward compatible initialization"""
        print("✓ Historical Data Fetcher initialized (ultra-fast mode)")
    
    async def backfill_historical_data(self, days_back: int = 30):
        """Backward compatible backfill method"""
        print(f"🚀 Starting ultra-fast backfill (ignoring days_back={days_back}, using optimized strategy)")
        
        # Use the ultra-fast fetcher
        result = self.ultra_fast_fetcher.process_all_symbols_ultra_fast(max_workers=8)
        
        if result.get('success'):
            stats = result['stats']
            print(f"✅ Ultra-fast backfill completed in {stats['total_time']}")
            print(f"📊 {stats['symbols_successful']}/{stats['symbols_processed']} symbols successful")
            print(f"🚀 {stats['total_candles']:,} candles stored")
        else:
            print(f"❌ Backfill failed: {result.get('error', 'Unknown error')}")
    
    def get_backfill_status(self) -> Optional[Dict]:
        """Backward compatible status check"""
        try:
            # Check if we have any data in Redis
            redis_client = self.ultra_fast_fetcher.redis.redis_client
            keys = redis_client.keys("stock:*:metadata")
            
            if keys:
                return {
                    'completed_at': datetime.now(IST_TIMEZONE).isoformat(),
                    'symbols_with_data': len(keys),
                    'ultra_fast_mode': True
                }
            return None
        except:
            return None
    
    async def cleanup(self):
        """Backward compatible cleanup"""
        print("✓ Historical Data Fetcher cleanup completed")

class UltraFastHistoricalFetcher:
    """Main fetcher optimized for maximum speed and compatibility"""
    
    def __init__(self, credentials_path: str, symbols_path: str):
        # Load configurations fast
        self.credentials = self._load_json_fast(credentials_path)
        self.symbols = self._load_symbols_fast(symbols_path)
        
        # Initialize clients
        self.api = FastUpstoxAPI(self.credentials['access_token'])
        self.redis = FastRedisManager()
        
        # Performance tracking
        self.stats = {
            'start_time': None,
            'total_symbols': len(self.symbols),
            'processed': 0,
            'successful': 0,
            'total_candles': 0,
            'api_calls': 0
        }
        
        print(f"🚀 Initialized for {len(self.symbols)} symbols")
    
    def _load_json_fast(self, path: str) -> Dict:
        """Fast JSON loading"""
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Failed to load {path}: {e}")
    
    def _load_symbols_fast(self, path: str) -> List[Symbol]:
        """Fast symbol loading with pre-computed clean names"""
        data = self._load_json_fast(path)
        
        symbols = []
        for symbol_data in data.get('symbols', []):
            symbol = Symbol(
                name=symbol_data['name'],
                instrument_key=symbol_data['instrument_key']
            )
            symbols.append(symbol)
        
        return symbols
    
    def fetch_symbol_data(self, symbol: Symbol) -> Tuple[Symbol, List[Candle], Dict]:
        """Ultra-fast single symbol processing"""
        try:
            all_candles = []
            strategies_used = []
            
            # Strategy 1: Try intraday first (usually faster and more recent)
            current_time = datetime.now(IST_TIMEZONE)
            if current_time.hour >= 9:  # Market hours or after
                intraday_candles = self.api.get_intraday_data(symbol.instrument_key)
                if intraday_candles:
                    all_candles.extend(intraday_candles)
                    strategies_used.append('intraday')
                    self.stats['api_calls'] += 1
            
            # Strategy 2: Get historical data if needed
            target_candles = 250
            candles_needed = target_candles - len(all_candles)
            
            if candles_needed > 0:
                # Dynamic days calculation for speed
                days_back = max(3, candles_needed // 300 + 2)  # Fast estimation
                historical_candles = self.api.get_historical_data(symbol.instrument_key, days_back)
                
                if historical_candles:
                    all_candles.extend(historical_candles)
                    strategies_used.append('historical')
                    self.stats['api_calls'] += 1
            
            if not all_candles:
                return symbol, [], {'error': 'No data received'}
            
            # Fast deduplication and selection
            # Use dict for O(1) deduplication
            unique_candles = {}
            for candle in all_candles:
                ts_key = int(candle.timestamp.timestamp())
                if ts_key not in unique_candles:
                    unique_candles[ts_key] = candle
            
            # Sort and take latest 250 (fast)
            sorted_candles = sorted(unique_candles.values(), key=lambda x: x.timestamp, reverse=True)
            final_candles = sorted_candles[:target_candles]
            final_candles.reverse()  # Chronological order for storage
            
            # Fast metadata
            metadata = {
                'symbol_name': symbol.name,
                'clean_name': symbol.clean_name,
                'instrument_key': symbol.instrument_key,
                'candles_count': len(final_candles),
                'oldest_time': final_candles[0].timestamp if final_candles else None,
                'newest_time': final_candles[-1].timestamp if final_candles else None,
                'strategies': strategies_used,
                'fetched_at': datetime.now(IST_TIMEZONE),
                'ready_for_live': True
            }
            
            self.stats['total_candles'] += len(final_candles)
            
            return symbol, final_candles, metadata
            
        except Exception as e:
            return symbol, [], {'error': str(e)}
    
    def process_all_symbols_ultra_fast(self, max_workers: int = 8) -> Dict:
        """Ultra-fast parallel processing - FIXED: Now works with hashable Symbol objects"""
        self.stats['start_time'] = datetime.now()
        
        print(f"🔥 Starting ultra-fast fetch with {max_workers} workers...")
        
        # Prepare storage containers
        symbol_data_map = {}
        metadata_map = {}
        
        # Process in parallel with optimized worker count
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 🔧 FIX: Now Symbol objects are hashable, this will work!
            future_to_symbol = {
                executor.submit(self.fetch_symbol_data, symbol): symbol 
                for symbol in self.symbols
            }
            
            # Process results as they complete
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                
                try:
                    result_symbol, candles, metadata = future.result()
                    
                    self.stats['processed'] += 1
                    
                    if candles and 'error' not in metadata:
                        symbol_data_map[symbol] = candles
                        metadata_map[symbol] = metadata
                        self.stats['successful'] += 1
                        status = f"✓ {len(candles)} candles"
                    else:
                        error = metadata.get('error', 'Unknown error')
                        status = f"✗ {error}"
                    
                    # Progress update
                    progress = (self.stats['processed'] / self.stats['total_symbols']) * 100
                    print(f"[{progress:5.1f}%] {symbol.name:<25}: {status}")
                    
                except Exception as e:
                    self.stats['processed'] += 1
                    print(f"[ERROR] {symbol.name}: {str(e)}")
        
        # Ultra-fast bulk storage
        print("\n🚀 Bulk storing to Redis...")
        storage_start = time_module.time()
        
        self.redis.bulk_store_candles(symbol_data_map)
        self.redis.store_metadata_bulk(metadata_map)
        
        storage_time = time_module.time() - storage_start
        
        # Calculate final stats
        total_time = datetime.now() - self.stats['start_time']
        
        return {
            'success': True,
            'stats': {
                'total_time': total_time,
                'storage_time': storage_time,
                'symbols_processed': self.stats['processed'],
                'symbols_successful': self.stats['successful'],
                'total_candles': self.stats['total_candles'],
                'api_calls': self.stats['api_calls'],
                'success_rate': (self.stats['successful'] / self.stats['total_symbols']) * 100,
                'candles_per_second': self.stats['total_candles'] / total_time.total_seconds(),
                'symbols_per_second': self.stats['successful'] / total_time.total_seconds()
            },
            'data_info': {
                'redis_keys_created': self.stats['successful'] * 5,  # OHLCV
                'avg_candles_per_symbol': self.stats['total_candles'] / max(1, self.stats['successful']),
                'ready_for_live_transition': True
            }
        }

def main():
    """Ultra-fast main execution"""
    print("🚀 ULTRA-FAST HISTORICAL DATA FETCHER - FIXED VERSION")
    print("=" * 60)
    
    # Paths
    credentials_path = "/home/abhishek/projects/cupcake/credentials.json"
    symbols_path = os.path.join(SCRIPT_DIR, "symbols.json")
    
    try:
        # Initialize fetcher
        fetcher = UltraFastHistoricalFetcher(credentials_path, symbols_path)
        
        # Clear Redis for fresh start
        fetcher.redis.flush_and_prepare()
        
        # Run ultra-fast processing
        result = fetcher.process_all_symbols_ultra_fast(max_workers=10)  # Aggressive parallelism
        
        # Print results
        stats = result['stats']
        data_info = result['data_info']
        
        print("\n" + "=" * 60)
        print("🎯 ULTRA-FAST EXECUTION COMPLETE")
        print("=" * 60)
        
        print(f"⏱️  PERFORMANCE:")
        print(f"   Total time: {stats['total_time']}")
        print(f"   Storage time: {stats['storage_time']:.2f}s")
        print(f"   Speed: {stats['candles_per_second']:.0f} candles/sec")
        print(f"   Throughput: {stats['symbols_per_second']:.1f} symbols/sec")
        
        print(f"\n📊 RESULTS:")
        print(f"   Symbols processed: {stats['symbols_processed']}")
        print(f"   Successful: {stats['symbols_successful']}")
        print(f"   Success rate: {stats['success_rate']:.1f}%")
        print(f"   Total candles: {stats['total_candles']:,}")
        print(f"   API calls: {stats['api_calls']}")
        
        print(f"\n🗄️  STORAGE:")
        print(f"   Redis keys created: {data_info['redis_keys_created']}")
        print(f"   Avg candles/symbol: {data_info['avg_candles_per_symbol']:.1f}")
        print(f"   Ready for live transition: {'✅' if data_info['ready_for_live_transition'] else '❌'}")
        
        print(f"\n📝 LOG FILE: {LOG_FILE}")
        
        print(f"\n🚀 NEXT: Start live fetcher for seamless transition!")
        print("=" * 60)
        
        return result
        
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
        return {'success': False, 'error': 'Interrupted'}
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    result = main()
    exit(0 if result.get('success', False) else 1)