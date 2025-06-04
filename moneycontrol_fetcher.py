# moneycontrol_fetcher.py - Efficient OHLCV data fetcher using the original logic

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor
import time

from config import STOCK_SYMBOLS, FETCH_CONFIG, REQUEST_HEADERS, MARKET_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MoneyControlFetcher:
    def __init__(self):
        self.session = None
        self.base_url = "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=FETCH_CONFIG['request_timeout'])
        self.session = aiohttp.ClientSession(
            headers=REQUEST_HEADERS,
            connector=connector,
            timeout=timeout
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def convert_resolution_to_minutes(self, resolution: str) -> int:
        """Convert resolution string to minutes (from original code)"""
        resolution = resolution.strip().upper()
        if resolution.endswith('H'):
            hours = int(resolution[:-1])
            return hours * 60
        elif resolution.endswith('D'):
            days = int(resolution[:-1])
            return days * 24 * 60
        else:
            return int(resolution)

    def calculate_date_range(self, days_back: int = None) -> Tuple[str, str, str, str]:
        """Calculate optimal date range for fetching data"""
        if days_back is None:
            days_back = FETCH_CONFIG['days_history']
            
        # End time is now or market close, whichever is earlier
        now = datetime.now()
        end_date = now.date()
        
        # If market is closed, use market close time, otherwise use current time
        market_close_today = datetime.combine(end_date, datetime.strptime(MARKET_CONFIG['market_close'], '%H:%M').time())
        
        if now > market_close_today:
            end_time = MARKET_CONFIG['market_close']
        else:
            end_time = now.strftime('%H:%M')
            
        # Start date is days_back ago
        start_date = (now - timedelta(days=days_back)).date()
        start_time = MARKET_CONFIG['market_open']
        
        return str(start_date), start_time, str(end_date), end_time

    async def fetch_single_stock(self, symbol: str, resolution: str = None, 
                               custom_date_range: Tuple[str, str, str, str] = None) -> Optional[Dict]:
        """Fetch OHLCV data for a single stock with retry logic"""
        if resolution is None:
            resolution = FETCH_CONFIG['resolution']
            
        if custom_date_range:
            start_date, start_time, end_date, end_time = custom_date_range
        else:
            start_date, start_time, end_date, end_time = self.calculate_date_range()

        # Convert to datetime and then to epoch (original logic)
        try:
            from_dt = datetime.fromisoformat(f"{start_date}T{start_time}")
            to_dt = datetime.fromisoformat(f"{end_date}T{end_time}")
        except Exception as e:
            logger.error(f"Invalid date format for {symbol}: {e}")
            return None

        if from_dt >= to_dt:
            logger.error(f"Invalid date range for {symbol}: start >= end")
            return None

        from_epoch = int(from_dt.timestamp())
        to_epoch = int(to_dt.timestamp())
        
        # Calculate countback
        total_interval_minutes = (to_epoch - from_epoch) / 60
        resolution_minutes = self.convert_resolution_to_minutes(resolution)
        countback = round(total_interval_minutes / resolution_minutes)

        # Build URL
        url = (
            f"{self.base_url}?"
            f"symbol={symbol}&"
            f"resolution={resolution}&"
            f"from={from_epoch}&"
            f"to={to_epoch}&"
            f"countback={countback}&"
            f"currencyCode=INR"
        )

        # Retry logic
        for attempt in range(FETCH_CONFIG['retry_attempts']):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Check for no_data response
                        if data.get("s") == "no_data":
                            logger.warning(f"No data available for {symbol}")
                            return None
                            
                        # Validate required keys
                        required_keys = {"o", "h", "l", "c", "v", "t"}
                        if not required_keys.issubset(data.keys()):
                            logger.error(f"Missing required keys for {symbol}: {required_keys - data.keys()}")
                            return None
                        
                        # Convert timestamps to readable format
                        timestamps = []
                        for ts in data["t"]:
                            dt_obj = datetime.fromtimestamp(ts)
                            timestamps.append(dt_obj.isoformat())
                        
                        result = {
                            'symbol': symbol,
                            'resolution': resolution,
                            'timestamps': timestamps,
                            'open': data["o"],
                            'high': data["h"],
                            'low': data["l"],
                            'close': data["c"],
                            'volume': data["v"],
                            'data_points': len(timestamps),
                            'date_range': {
                                'start': f"{start_date} {start_time}",
                                'end': f"{end_date} {end_time}"
                            }
                        }
                        
                        logger.info(f"Successfully fetched {len(timestamps)} data points for {symbol}")
                        return result
                        
                    else:
                        logger.warning(f"HTTP {response.status} for {symbol}, attempt {attempt + 1}")
                        
            except asyncio.TimeoutError:
                logger.warning(f"Timeout for {symbol}, attempt {attempt + 1}")
            except Exception as e:
                logger.error(f"Error fetching {symbol}, attempt {attempt + 1}: {e}")
            
            if attempt < FETCH_CONFIG['retry_attempts'] - 1:
                await asyncio.sleep(FETCH_CONFIG['retry_delay'] * (attempt + 1))
        
        logger.error(f"Failed to fetch data for {symbol} after {FETCH_CONFIG['retry_attempts']} attempts")
        return None

    async def fetch_multiple_stocks(self, symbols: List[str] = None, 
                                  resolution: str = None) -> Dict[str, Dict]:
        """Fetch OHLCV data for multiple stocks concurrently"""
        if symbols is None:
            symbols = STOCK_SYMBOLS[:FETCH_CONFIG['max_concurrent_requests']]
        
        if resolution is None:
            resolution = FETCH_CONFIG['resolution']
            
        logger.info(f"Starting to fetch data for {len(symbols)} stocks with {resolution} resolution")
        start_time = time.time()
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(FETCH_CONFIG['max_concurrent_requests'])
        
        async def fetch_with_semaphore(symbol):
            async with semaphore:
                return await self.fetch_single_stock(symbol, resolution)
        
        # Execute all requests concurrently
        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful_results = {}
        failed_symbols = []
        
        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                logger.error(f"Exception for {symbol}: {result}")
                failed_symbols.append(symbol)
            elif result is not None:
                successful_results[symbol] = result
            else:
                failed_symbols.append(symbol)
        
        elapsed_time = time.time() - start_time
        success_rate = len(successful_results) / len(symbols) * 100
        
        logger.info(f"Fetching completed in {elapsed_time:.2f}s")
        logger.info(f"Success rate: {success_rate:.1f}% ({len(successful_results)}/{len(symbols)})")
        
        if failed_symbols:
            logger.warning(f"Failed symbols: {failed_symbols}")
            
        return successful_results

    def to_dataframe(self, stock_data: Dict) -> pd.DataFrame:
        """Convert stock data to pandas DataFrame for easy analysis"""
        if not stock_data:
            return pd.DataFrame()
            
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(stock_data['timestamps']),
            'open': stock_data['open'],
            'high': stock_data['high'], 
            'low': stock_data['low'],
            'close': stock_data['close'],
            'volume': stock_data['volume']
        })
        
        df.set_index('timestamp', inplace=True)
        df['symbol'] = stock_data['symbol']
        
        return df

# Convenience functions for easy usage
async def get_stock_data(symbol: str, resolution: str = '5') -> Optional[Dict]:
    """Get data for a single stock"""
    async with MoneyControlFetcher() as fetcher:
        return await fetcher.fetch_single_stock(symbol, resolution)

async def get_multiple_stocks_data(symbols: List[str] = None, resolution: str = '5') -> Dict[str, Dict]:
    """Get data for multiple stocks"""
    async with MoneyControlFetcher() as fetcher:
        return await fetcher.fetch_multiple_stocks(symbols, resolution)

# Main execution for testing
if __name__ == "__main__":
    async def main():
        # Test with a few stocks
        test_symbols = ["TCS", "INFY", "RELIANCE"]
        
        async with MoneyControlFetcher() as fetcher:
            results = await fetcher.fetch_multiple_stocks(test_symbols)
            
            for symbol, data in results.items():
                print(f"\n{symbol}: {data['data_points']} data points")
                if data['data_points'] > 0:
                    print(f"Latest: {data['timestamps'][-1]} - Close: {data['close'][-1]}")
    
    # Run the test
    asyncio.run(main())