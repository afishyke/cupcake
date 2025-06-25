import asyncio
import aiohttp
import json
import redis
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pytz
import pandas as pd
from dataclasses import dataclass
from enum import Enum
import time

logger = logging.getLogger(__name__)

class Interval(Enum):
    MINUTE_1 = "1minute"
    MINUTE_5 = "5minute"
    MINUTE_15 = "15minute"
    MINUTE_30 = "30minute"
    HOUR_1 = "1hour"
    DAY_1 = "1day"

@dataclass
class HistoricalCandle:
    symbol: str
    instrument_key: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    interval: str
    
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

class HistoricalDataFetcher:
    """
    Fetches and manages historical market data from Upstox API
    Handles rate limiting, caching, and data persistence
    """
    
    def __init__(self, config):
        self.config = config
        self.redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=0, decode_responses=True)
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # Rate limiting
        self.max_requests_per_minute = 100
        self.request_timestamps = []
        
        # Session for HTTP requests
        self.session = None
        
        # Symbol mapping
        self.symbol_mapping = {}
        
        logger.info("Historical Data Fetcher initialized")
    
    async def initialize(self):
        """Initialize the fetcher"""
        try:
            # Load symbol mapping
            await self.load_symbols()
            
            # Create HTTP session
            self.session = aiohttp.ClientSession()
            
            logger.info("Historical Data Fetcher ready")
            
        except Exception as e:
            logger.error(f"Failed to initialize Historical Data Fetcher: {e}")
            raise
    
    async def load_symbols(self):
        """Load symbols from configuration"""
        try:
            with open('symbols.json', 'r') as f:
                data = json.load(f)
            
            for symbol in data['symbols']:
                instrument_key = symbol['instrument_key']
                self.symbol_mapping[instrument_key] = symbol['name']
            
            logger.info(f"Loaded {len(self.symbol_mapping)} symbols for historical data")
            
        except Exception as e:
            logger.error(f"Failed to load symbols: {e}")
            raise
    
    async def rate_limit(self):
        """Implement rate limiting for API calls"""
        current_time = time.time()
        
        # Remove timestamps older than 1 minute
        self.request_timestamps = [
            ts for ts in self.request_timestamps 
            if current_time - ts < 60
        ]
        
        # Check if we need to wait
        if len(self.request_timestamps) >= self.max_requests_per_minute:
            sleep_time = 60 - (current_time - self.request_timestamps[0])
            if sleep_time > 0:
                logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                await asyncio.sleep(sleep_time)
        
        # Add current request timestamp
        self.request_timestamps.append(current_time)
    
    async def get_access_token(self) -> str:
        """Get access token from credentials file"""
        try:
            with open('credentials.json', 'r') as f:
                data = json.load(f)
            return data['access_token']
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            raise
    
    async def fetch_historical_data(
        self, 
        instrument_key: str, 
        interval: Interval, 
        from_date: datetime, 
        to_date: datetime
    ) -> List[HistoricalCandle]:
        """
        Fetch historical data for a specific instrument and time range
        """
        try:
            # Apply rate limiting
            await self.rate_limit()
            
            # Get access token
            access_token = await self.get_access_token()
            
            # Format dates
            from_date_str = from_date.strftime('%Y-%m-%d')
            to_date_str = to_date.strftime('%Y-%m-%d')
            
            # Construct API URL
            url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval.value}/{to_date_str}/{from_date_str}"
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {access_token}'
            }
            
            logger.info(f"Fetching historical data: {self.symbol_mapping.get(instrument_key, instrument_key)} "
                       f"({interval.value}) from {from_date_str} to {to_date_str}")
            
            async with self.session.get(url, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"API Error {response.status}: {error_text}")
                    return []
                
                data = await response.json()
                
                if data['status'] != 'success':
                    logger.error(f"API returned error: {data}")
                    return []
                
                # Parse candles
                candles = []
                symbol_name = self.symbol_mapping.get(instrument_key, instrument_key)
                
                for candle_data in data.get('data', {}).get('candles', []):
                    try:
                        # Parse timestamp (ISO format)
                        timestamp = datetime.fromisoformat(candle_data[0].replace('Z', '+00:00'))
                        timestamp = timestamp.astimezone(self.ist)
                        
                        candle = HistoricalCandle(
                            symbol=symbol_name,
                            instrument_key=instrument_key,
                            timestamp=timestamp,
                            open=float(candle_data[1]),
                            high=float(candle_data[2]),
                            low=float(candle_data[3]),
                            close=float(candle_data[4]),
                            volume=int(candle_data[5]),
                            interval=interval.value
                        )
                        
                        candles.append(candle)
                        
                    except Exception as e:
                        logger.error(f"Error parsing candle data: {e}")
                        continue
                
                logger.info(f"Fetched {len(candles)} candles for {symbol_name}")
                return candles
                
        except Exception as e:
            logger.error(f"Error fetching historical data for {instrument_key}: {e}")
            return []
    
    async def cache_historical_data(self, candles: List[HistoricalCandle], ttl: int = 3600):
        """Cache historical data in Redis"""
        try:
            for candle in candles:
                # Create cache key
                cache_key = f"historical:{candle.instrument_key}:{candle.interval}:{candle.timestamp.strftime('%Y%m%d_%H%M')}"
                
                # Store in Redis with TTL
                self.redis_client.setex(
                    cache_key,
                    ttl,
                    json.dumps(candle.to_dict())
                )
            
            if candles:
                logger.info(f"Cached {len(candles)} historical candles")
                
        except Exception as e:
            logger.error(f"Error caching historical data: {e}")
    
    async def get_cached_data(
        self, 
        instrument_key: str, 
        interval: Interval, 
        from_date: datetime, 
        to_date: datetime
    ) -> List[HistoricalCandle]:
        """Retrieve cached historical data from Redis"""
        try:
            candles = []
            current_date = from_date
            
            while current_date <= to_date:
                cache_key = f"historical:{instrument_key}:{interval.value}:{current_date.strftime('%Y%m%d_%H%M')}"
                cached_data = self.redis_client.get(cache_key)
                
                if cached_data:
                    candle_dict = json.loads(cached_data)
                    candle = HistoricalCandle(
                        symbol=candle_dict['symbol'],
                        instrument_key=candle_dict['instrument_key'],
                        timestamp=datetime.fromisoformat(candle_dict['timestamp']),
                        open=candle_dict['open'],
                        high=candle_dict['high'],
                        low=candle_dict['low'],
                        close=candle_dict['close'],
                        volume=candle_dict['volume'],
                        interval=candle_dict['interval']
                    )
                    candles.append(candle)
                
                # Move to next interval
                if interval == Interval.MINUTE_1:
                    current_date += timedelta(minutes=1)
                elif interval == Interval.MINUTE_5:
                    current_date += timedelta(minutes=5)
                elif interval == Interval.MINUTE_15:
                    current_date += timedelta(minutes=15)
                elif interval == Interval.MINUTE_30:
                    current_date += timedelta(minutes=30)
                elif interval == Interval.HOUR_1:
                    current_date += timedelta(hours=1)
                elif interval == Interval.DAY_1:
                    current_date += timedelta(days=1)
            
            if candles:
                logger.info(f"Retrieved {len(candles)} cached candles")
            
            return candles
            
        except Exception as e:
            logger.error(f"Error retrieving cached data: {e}")
            return []
    
    async def backfill_historical_data(
        self, 
        days_back: int = 30, 
        intervals: List[Interval] = None
    ):
        """
        Backfill historical data for all configured symbols
        """
        try:
            if intervals is None:
                intervals = [Interval.MINUTE_1, Interval.MINUTE_5, Interval.DAY_1]
            
            # Calculate date range
            end_date = datetime.now(self.ist).replace(hour=15, minute=30, second=0, microsecond=0)  # Market close
            start_date = end_date - timedelta(days=days_back)
            
            logger.info(f"Starting historical data backfill from {start_date.date()} to {end_date.date()}")
            logger.info(f"Intervals: {[i.value for i in intervals]}")
            logger.info(f"Symbols: {len(self.symbol_mapping)}")
            
            total_fetched = 0
            
            for instrument_key in self.symbol_mapping.keys():
                symbol_name = self.symbol_mapping[instrument_key]
                
                for interval in intervals:
                    try:
                        # Check if we already have recent data
                        cached_data = await self.get_cached_data(instrument_key, interval, start_date, end_date)
                        
                        if cached_data:
                            logger.info(f"Skipping {symbol_name} {interval.value} - already cached")
                            continue
                        
                        # Fetch data in chunks to respect API limits
                        chunk_days = 7 if interval in [Interval.MINUTE_1, Interval.MINUTE_5] else 30
                        current_start = start_date
                        
                        while current_start < end_date:
                            current_end = min(current_start + timedelta(days=chunk_days), end_date)
                            
                            candles = await self.fetch_historical_data(
                                instrument_key, interval, current_start, current_end
                            )
                            
                            if candles:
                                # Cache the data
                                await self.cache_historical_data(candles)
                                
                                # Store for analysis
                                await self.store_for_analysis(candles)
                                
                                total_fetched += len(candles)
                            
                            current_start = current_end
                            
                            # Small delay between requests
                            await asyncio.sleep(0.1)
                    
                    except Exception as e:
                        logger.error(f"Error fetching data for {symbol_name} {interval.value}: {e}")
                        continue
            
            logger.info(f"Backfill completed. Total candles fetched: {total_fetched}")
            
            # Store backfill completion status
            self.redis_client.setex(
                'historical_data:backfill_status',
                86400,  # 24 hours
                json.dumps({
                    'completed_at': datetime.now(self.ist).isoformat(),
                    'days_back': days_back,
                    'total_candles': total_fetched,
                    'intervals': [i.value for i in intervals]
                })
            )
            
        except Exception as e:
            logger.error(f"Error in historical data backfill: {e}")
    
    async def store_for_analysis(self, candles: List[HistoricalCandle]):
        """Store candles in Redis for technical analysis"""
        try:
            for candle in candles:
                # Store in time series for technical analysis
                ts_key = f"timeseries:{candle.instrument_key}:{candle.interval}"
                
                # Add to sorted set with timestamp as score
                timestamp_score = candle.timestamp.timestamp()
                
                self.redis_client.zadd(
                    ts_key,
                    {json.dumps(candle.to_dict()): timestamp_score}
                )
                
                # Keep only last 1000 candles per series
                self.redis_client.zremrangebyrank(ts_key, 0, -1001)
                
                # Set TTL on the key
                self.redis_client.expire(ts_key, 86400 * 7)  # 7 days
        
        except Exception as e:
            logger.error(f"Error storing candles for analysis: {e}")
    
    async def get_recent_candles(
        self, 
        instrument_key: str, 
        interval: Interval, 
        count: int = 100
    ) -> List[HistoricalCandle]:
        """Get recent candles for technical analysis"""
        try:
            ts_key = f"timeseries:{instrument_key}:{interval.value}"
            
            # Get recent candles from sorted set
            candle_data = self.redis_client.zrevrange(ts_key, 0, count-1)
            
            candles = []
            for data in candle_data:
                candle_dict = json.loads(data)
                candle = HistoricalCandle(
                    symbol=candle_dict['symbol'],
                    instrument_key=candle_dict['instrument_key'],
                    timestamp=datetime.fromisoformat(candle_dict['timestamp']),
                    open=candle_dict['open'],
                    high=candle_dict['high'],
                    low=candle_dict['low'],
                    close=candle_dict['close'],
                    volume=candle_dict['volume'],
                    interval=candle_dict['interval']
                )
                candles.append(candle)
            
            # Sort by timestamp (oldest first)
            candles.sort(key=lambda x: x.timestamp)
            
            return candles
            
        except Exception as e:
            logger.error(f"Error getting recent candles: {e}")
            return []
    
    async def get_candles_dataframe(
        self, 
        instrument_key: str, 
        interval: Interval, 
        count: int = 100
    ) -> Optional[pd.DataFrame]:
        """Get candles as pandas DataFrame for analysis"""
        try:
            candles = await self.get_recent_candles(instrument_key, interval, count)
            
            if not candles:
                return None
            
            # Convert to DataFrame
            data = []
            for candle in candles:
                data.append({
                    'timestamp': candle.timestamp,
                    'open': candle.open,
                    'high': candle.high,
                    'low': candle.low,
                    'close': candle.close,
                    'volume': candle.volume
                })
            
            df = pd.DataFrame(data)
            df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}")
            return None
    
    async def update_live_candles(self, tick_data: Dict):
        """Update current candles with live tick data"""
        try:
            instrument_key = tick_data.get('instrument_key')
            if not instrument_key:
                return
            
            current_time = datetime.now(self.ist)
            
            # Update 1-minute candle
            await self._update_candle(instrument_key, tick_data, Interval.MINUTE_1, current_time)
            
            # Update 5-minute candle
            await self._update_candle(instrument_key, tick_data, Interval.MINUTE_5, current_time)
            
        except Exception as e:
            logger.error(f"Error updating live candles: {e}")
    
    async def _update_candle(
        self, 
        instrument_key: str, 
        tick_data: Dict, 
        interval: Interval, 
        current_time: datetime
    ):
        """Update a specific candle with tick data"""
        try:
            # Calculate candle start time based on interval
            if interval == Interval.MINUTE_1:
                candle_start = current_time.replace(second=0, microsecond=0)
            elif interval == Interval.MINUTE_5:
                minute = (current_time.minute // 5) * 5
                candle_start = current_time.replace(minute=minute, second=0, microsecond=0)
            else:
                return
            
            # Get current candle
            candle_key = f"live_candle:{instrument_key}:{interval.value}:{candle_start.strftime('%Y%m%d_%H%M')}"
            
            existing_data = self.redis_client.get(candle_key)
            
            if existing_data:
                candle_dict = json.loads(existing_data)
                # Update high/low/close
                ltp = float(tick_data.get('ltp', 0))
                volume = int(tick_data.get('volume', 0))
                
                candle_dict['high'] = max(candle_dict['high'], ltp)
                candle_dict['low'] = min(candle_dict['low'], ltp)
                candle_dict['close'] = ltp
                candle_dict['volume'] += volume
                
            else:
                # Create new candle
                ltp = float(tick_data.get('ltp', 0))
                volume = int(tick_data.get('volume', 0))
                
                candle_dict = {
                    'symbol': self.symbol_mapping.get(instrument_key, instrument_key),
                    'instrument_key': instrument_key,
                    'timestamp': candle_start.isoformat(),
                    'open': ltp,
                    'high': ltp,
                    'low': ltp,
                    'close': ltp,
                    'volume': volume,
                    'interval': interval.value
                }
            
            # Store updated candle
            self.redis_client.setex(candle_key, 300, json.dumps(candle_dict))  # 5-minute TTL
            
        except Exception as e:
            logger.error(f"Error updating candle: {e}")
    
    async def get_market_hours(self) -> Tuple[datetime, datetime]:
        """Get market hours for the current day"""
        today = datetime.now(self.ist).date()
        
        # NSE trading hours: 9:15 AM to 3:30 PM
        market_open = datetime.combine(today, datetime.min.time().replace(hour=9, minute=15))
        market_close = datetime.combine(today, datetime.min.time().replace(hour=15, minute=30))
        
        market_open = self.ist.localize(market_open)
        market_close = self.ist.localize(market_close)
        
        return market_open, market_close
    
    async def is_market_open(self) -> bool:
        """Check if market is currently open"""
        current_time = datetime.now(self.ist)
        market_open, market_close = await self.get_market_hours()
        
        # Check if it's a weekday (Monday=0, Sunday=6)
        if current_time.weekday() >= 5:  # Saturday or Sunday
            return False
        
        return market_open <= current_time <= market_close
    
    def get_backfill_status(self) -> Optional[Dict]:
        """Get the status of the last backfill operation"""
        try:
            status_data = self.redis_client.get('historical_data:backfill_status')
            return json.loads(status_data) if status_data else None
        except Exception as e:
            logger.error(f"Error getting backfill status: {e}")
            return None
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
        
        logger.info("Historical Data Fetcher cleanup completed")

# Integration with existing market data service
class EnhancedMarketDataServiceWithHistory:
    """
    Enhanced version that integrates historical and live data
    """
    
    def __init__(self, config):
        self.config = config
        self.historical_fetcher = HistoricalDataFetcher(config)
        # ... existing initialization code
    
    async def initialize(self):
        """Initialize both historical and live data services"""
        # Initialize historical fetcher
        await self.historical_fetcher.initialize()
        
        # Check if we need to backfill data
        backfill_status = self.historical_fetcher.get_backfill_status()
        
        if not backfill_status:
            logger.info("No previous backfill found. Starting initial backfill...")
            await self.historical_fetcher.backfill_historical_data(days_back=30)
        else:
            logger.info(f"Previous backfill completed at: {backfill_status['completed_at']}")
            
            # Check if backfill is older than 1 day
            last_backfill = datetime.fromisoformat(backfill_status['completed_at'])
            if datetime.now(pytz.timezone('Asia/Kolkata')) - last_backfill > timedelta(days=1):
                logger.info("Backfill data is old. Refreshing...")
                await self.historical_fetcher.backfill_historical_data(days_back=7)
        
        # ... existing initialization code
    
    async def process_symbol_tick(self, instrument_key: str, feed_data: dict):
        """Enhanced tick processing with historical data integration"""
        # Process live tick (existing code)
        # ... existing tick processing code
        
        # Update live candles
        await self.historical_fetcher.update_live_candles({
            'instrument_key': instrument_key,
            'ltp': feed_data.get('fullFeed', {}).get('marketFF', {}).get('ltpc', {}).get('ltp'),
            'volume': feed_data.get('fullFeed', {}).get('marketFF', {}).get('ltpc', {}).get('ltq', 0)
        })
    
    async def get_technical_analysis_data(self, instrument_key: str) -> Optional[pd.DataFrame]:
        """Get historical data for technical analysis"""
        return await self.historical_fetcher.get_candles_dataframe(
            instrument_key, 
            Interval.MINUTE_5, 
            count=200
        )

async def main():
    """Test the historical data fetcher"""
    from enhanced_market_data_service import Config
    
    config = Config()
    fetcher = HistoricalDataFetcher(config)
    
    await fetcher.initialize()
    
    # Test backfill
    await fetcher.backfill_historical_data(days_back=7)
    
    # Test getting recent data
    candles = await fetcher.get_recent_candles(
        "NSE_EQ|INE002A01018", 
        Interval.MINUTE_5, 
        count=50
    )
    
    print(f"Retrieved {len(candles)} candles")
    for candle in candles[-5:]:  # Last 5 candles
        print(f"{candle.timestamp}: O={candle.open}, H={candle.high}, L={candle.low}, C={candle.close}")
    
    await fetcher.cleanup()

if __name__ == "__main__":
    asyncio.run(main())