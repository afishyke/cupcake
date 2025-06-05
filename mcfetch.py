import json
import asyncio
import aiohttp
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import time
from dataclasses import dataclass

@dataclass
class StockData:
    """Data class for structured stock information"""
    symbol: str
    labels: List[str]
    open: List[float]
    high: List[float]
    low: List[float]
    close: List[float]
    volume: List[int]
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "labels": self.labels,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume
        }

class FastStockFetcher:
    """High-performance stock data fetcher with async operations"""
    
    def __init__(self, max_concurrent_requests: int = 50, request_timeout: int = 5):
        self.max_concurrent_requests = max_concurrent_requests
        self.request_timeout = request_timeout
        self.session = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent_requests,
            limit_per_host=self.max_concurrent_requests,
            ttl_dns_cache=300,  # DNS cache for 5 minutes
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive"
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    @staticmethod
    def convert_resolution_to_minutes(resolution: str) -> int:
        """Convert resolution string to minutes"""
        resolution = resolution.strip().upper()
        if resolution.endswith('H'):
            return int(resolution[:-1]) * 60
        elif resolution.endswith('D'):
            return int(resolution[:-1]) * 24 * 60
        else:
            return int(resolution)
    
    def prepare_request_params(self, symbol: str, resolution: str, 
                             start_date: str, start_time: str, 
                             end_date: str, end_time: str) -> Tuple[str, int, int, int]:
        """Prepare request parameters with validation"""
        try:
            from_dt = datetime.fromisoformat(f"{start_date}T{start_time}")
            to_dt = datetime.fromisoformat(f"{end_date}T{end_time}")
        except Exception as e:
            raise ValueError(f"Invalid date/time format for {symbol}: {e}")

        if from_dt >= to_dt:
            raise ValueError(f'The "from" datetime must be earlier than the "to" datetime for {symbol}.')

        from_epoch = int(from_dt.timestamp())
        to_epoch = int(to_dt.timestamp())
        total_interval_minutes = (to_epoch - from_epoch) / 60
        resolution_minutes = self.convert_resolution_to_minutes(resolution)
        countback = max(1, round(total_interval_minutes / resolution_minutes))

        url = (
            f"https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"
            f"?symbol={symbol}&resolution={resolution}&from={from_epoch}&to={to_epoch}"
            f"&countback={countback}&currencyCode=INR"
        )
        
        return url, from_epoch, to_epoch, countback
    
    async def fetch_single_stock(self, symbol: str, resolution: str, 
                               start_date: str, start_time: str, 
                               end_date: str, end_time: str) -> Optional[StockData]:
        """Fetch data for a single stock symbol asynchronously"""
        try:
            url, from_epoch, to_epoch, countback = self.prepare_request_params(
                symbol, resolution, start_date, start_time, end_date, end_time
            )
            
            async with self.session.get(url) as response:
                if response.status != 200:
                    print(f"[ERROR] HTTP {response.status} for {symbol}")
                    return None
                
                data = await response.json()
                
                if data.get("s") == "no_data":
                    print(f"[WARN] No data available for symbol '{symbol}'")
                    return None
                
                required_keys = {"o", "h", "l", "c", "v", "t"}
                if not required_keys.issubset(data.keys()):
                    print(f"[ERROR] Unexpected response structure for {symbol}")
                    return None
                
                # Process timestamps more efficiently
                labels = [datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') 
                         for ts in data["t"]]
                
                return StockData(
                    symbol=symbol,
                    labels=labels,
                    open=data["o"],
                    high=data["h"],
                    low=data["l"],
                    close=data["c"],
                    volume=data["v"]
                )
                
        except asyncio.TimeoutError:
            print(f"[ERROR] Timeout for {symbol}")
            return None
        except Exception as e:
            print(f"[ERROR] Failed to fetch {symbol}: {e}")
            return None
    
    async def fetch_all_stocks(self, symbols: List[str], resolution: str, 
                             start_date: str, start_time: str, 
                             end_date: str, end_time: str) -> Dict[str, dict]:
        """Fetch data for multiple stocks concurrently"""
        print(f"[INFO] Starting fetch for {len(symbols)} symbols...")
        start_time_fetch = time.time()
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def fetch_with_semaphore(symbol):
            async with semaphore:
                return await self.fetch_single_stock(
                    symbol, resolution, start_date, start_time, end_date, end_time
                )
        
        # Create tasks for all symbols
        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        all_data = {}
        successful_fetches = 0
        
        for symbol, result in zip(symbols, results):
            if isinstance(result, StockData):
                all_data[symbol] = result.to_dict()
                successful_fetches += 1
                print(f"[SUCCESS] {symbol} - {len(result.labels)} data points")
            elif isinstance(result, Exception):
                print(f"[ERROR] {symbol} - Exception: {result}")
            else:
                print(f"[WARN] {symbol} - No data returned")
        
        elapsed_time = time.time() - start_time_fetch
        print(f"[INFO] Completed in {elapsed_time:.2f}s - {successful_fetches}/{len(symbols)} successful")
        
        return all_data

# Synchronous wrapper functions for backward compatibility
def fetch_stock_history(symbol: str, resolution: str, start_date: str, 
                       start_time: str, end_date: str, end_time: str) -> dict:
    """Synchronous wrapper for single stock fetch"""
    async def _fetch():
        async with FastStockFetcher() as fetcher:
            result = await fetcher.fetch_single_stock(
                symbol, resolution, start_date, start_time, end_date, end_time
            )
            return result.to_dict() if result else {}
    
    return asyncio.run(_fetch())

def fetch_all_stocks_concurrently(symbols: list, resolution: str, 
                                start_date: str, start_time: str, 
                                end_date: str, end_time: str) -> dict:
    """Synchronous wrapper for multiple stocks fetch"""
    async def _fetch_all():
        async with FastStockFetcher(max_concurrent_requests=50) as fetcher:
            return await fetcher.fetch_all_stocks(
                symbols, resolution, start_date, start_time, end_date, end_time
            )
    
    return asyncio.run(_fetch_all())

# High-performance batch fetcher
async def fetch_stocks_batch(symbols: List[str], resolution: str, 
                           start_date: str, start_time: str, 
                           end_date: str, end_time: str,
                           max_concurrent: int = 100) -> Dict[str, dict]:
    """
    Ultra-fast batch fetcher with optimized settings
    
    Usage:
        data = await fetch_stocks_batch(['TCS', 'RELIANCE', 'INFY'], '1D', 
                                       '2024-01-01', '09:15:00', '2024-12-31', '15:30:00')
    """
    async with FastStockFetcher(max_concurrent_requests=max_concurrent, request_timeout=10) as fetcher:
        return await fetcher.fetch_all_stocks(
            symbols, resolution, start_date, start_time, end_date, end_time
        )

# JSON file loader
def load_symbols_from_json(json_file_path: str) -> List[str]:
    """
    Load symbols from JSON file
    
    Expected JSON format:
    {
        "symbols": [
            "ORICONENT",
            "BAJAJINDEF",
            "PKTEA",
            ...
        ]
    }
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            symbols = data.get('symbols', [])
            if not symbols:
                raise ValueError("No 'symbols' key found or empty symbols list in JSON file")
            print(f"[INFO] Loaded {len(symbols)} symbols from {json_file_path}")
            return symbols
    except FileNotFoundError:
        raise FileNotFoundError(f"JSON file not found: {json_file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in {json_file_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Error loading symbols from {json_file_path}: {e}")

# Convenience function to fetch from JSON file
def fetch_stocks_from_json(json_file_path: str, resolution: str, 
                          start_date: str, start_time: str, 
                          end_date: str, end_time: str) -> dict:
    """
    Load symbols from JSON file and fetch their data synchronously
    
    Args:
        json_file_path: Path to JSON file containing symbols
        resolution: Time resolution (e.g., '1D', '1H', '5')
        start_date: Start date in 'YYYY-MM-DD' format
        start_time: Start time in 'HH:MM:SS' format
        end_date: End date in 'YYYY-MM-DD' format
        end_time: End time in 'HH:MM:SS' format
    
    Returns:
        Dictionary with stock data for each symbol
    """
    symbols = load_symbols_from_json(json_file_path)
    return fetch_all_stocks_concurrently(symbols, resolution, start_date, start_time, end_date, end_time)

# Async version for JSON file
async def fetch_stocks_from_json_async(json_file_path: str, resolution: str, 
                                     start_date: str, start_time: str, 
                                     end_date: str, end_time: str,
                                     max_concurrent: int = 100) -> dict:
    """
    Load symbols from JSON file and fetch their data asynchronously (FASTEST)
    
    Args:
        json_file_path: Path to JSON file containing symbols
        resolution: Time resolution (e.g., '1D', '1H', '5')
        start_date: Start date in 'YYYY-MM-DD' format
        start_time: Start time in 'HH:MM:SS' format
        end_date: End date in 'YYYY-MM-DD' format
        end_time: End time in 'HH:MM:SS' format
        max_concurrent: Maximum concurrent requests (default: 100)
    
    Returns:
        Dictionary with stock data for each symbol
    """
    symbols = load_symbols_from_json(json_file_path)
    return await fetch_stocks_batch(symbols, resolution, start_date, start_time, end_date, end_time, max_concurrent)

# Example usage
if __name__ == "__main__":
    # Example JSON file path
    json_file = "symbols.json"
    
    # Method 1: Direct JSON loading (synchronous)
    print("=== Method 1: Synchronous JSON Fetch ===")
    try:
        data = fetch_stocks_from_json(
            json_file, '1D', '2024-01-01', '09:15:00', '2024-03-31', '15:30:00'
        )
        print(f"Successfully fetched data for {len(data)} stocks")
        
        # Print sample data
        for symbol, stock_data in list(data.items())[:2]:  # Show first 2 stocks
            print(f"\n{symbol}: {len(stock_data.get('labels', []))} data points")
            if stock_data.get('labels'):
                print(f"  Date range: {stock_data['labels'][0]} to {stock_data['labels'][-1]}")
                print(f"  Latest close: {stock_data['close'][-1] if stock_data.get('close') else 'N/A'}")
    
    except Exception as e:
        print(f"Error in synchronous fetch: {e}")
    
    # Method 2: Async JSON loading (recommended for speed)
    print("\n=== Method 2: Asynchronous JSON Fetch (FASTEST) ===")
    async def main():
        try:
            data = await fetch_stocks_from_json_async(
                json_file, '1D', '2024-01-01', '09:15:00', '2024-03-31', '15:30:00',
                max_concurrent=100
            )
            print(f"Successfully fetched data for {len(data)} stocks")
            return data
        except Exception as e:
            print(f"Error in async fetch: {e}")
            return {}
    
    # Run async example
    asyncio.run(main())
    
    # Method 3: Manual symbol loading
    print("\n=== Method 3: Manual Symbol Loading ===")
    try:
        symbols = load_symbols_from_json(json_file)
        print(f"Loaded symbols: {symbols}")
        
        # Use any of the existing functions
        # data = fetch_all_stocks_concurrently(symbols, ...)
        # data = await fetch_stocks_batch(symbols, ...)
        
    except Exception as e:
        print(f"Error loading symbols: {e}")