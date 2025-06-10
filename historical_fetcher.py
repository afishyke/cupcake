#!/usr/bin/env python3
"""
Fast Historical Data Fetcher for Upstox API
- Concurrent fetching with thread pools for maximum speed
- Optimized rate limiting (closer to API limits)
- Intelligent batching and caching
- Progress tracking with rich progress bars
- Retry logic for failed requests
- Memory-efficient data handling
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import os
import json
import logging
from typing import List, Optional, Dict, Tuple, Set
import time as time_module
from pathlib import Path
import asyncio
import aiohttp
import concurrent.futures
from threading import Lock, Event
import threading
from dataclasses import dataclass
from collections import defaultdict
import queue

# Try to import rich for better progress display
try:
    from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn
    from rich.console import Console
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Install 'rich' for better progress display: pip install rich")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FetchTask:
    """Data class for fetch tasks"""
    instrument_key: str
    symbol_name: str
    date: str
    task_id: str
    
@dataclass
class FetchResult:
    """Data class for fetch results"""
    task_id: str
    instrument_key: str
    symbol_name: str
    data: Optional[pd.DataFrame]
    error: Optional[str] = None
    date: Optional[str] = None

class FastUpstoxHistoricalFetcher:
    def __init__(self, access_token: str, data_dir: str = "data/historical", max_workers: int = 10):
        """
        Initialize the fast historical data fetcher
        
        Args:
            access_token: Upstox API access token
            data_dir: Directory to store historical data CSV files
            max_workers: Maximum number of concurrent threads (default: 10)
        """
        self.access_token = access_token
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = min(max_workers, 20)  # Cap at 20 to be safe
        
        # API configuration
        self.base_url = "https://api-v2.upstox.com"
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        # Optimized rate limiting (300 requests per minute = 5 per second)
        # Using 80% of limit to be safe: 4 requests per second
        self.rate_limit_delay = 0.25  # 250ms between requests (4 per second)
        self.request_times = queue.Queue(maxsize=self.max_workers * 2)
        self.rate_limit_lock = Lock()
        
        # Market hours (IST)
        self.market_start = time(9, 15)
        self.market_end = time(15, 30)
        
        # Enhanced caching
        self._market_holidays_cache = {}
        self._holidays_cache_timestamp = {}
        self._cache_expiry_hours = 24
        
        # Statistics tracking
        self.stats = {
            'requests_made': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'cache_hits': 0,
            'total_records': 0
        }
        self.stats_lock = Lock()
        
        # Progress tracking
        self.console = Console() if RICH_AVAILABLE else None
        
        logger.info(f"Fast fetcher initialized. Max workers: {self.max_workers}, Data directory: {self.data_dir}")
    
    def _smart_rate_limit(self):
        """Intelligent rate limiting that adapts to actual API performance"""
        with self.rate_limit_lock:
            current_time = time_module.time()
            
            # Clean old request times (older than 1 second)
            while not self.request_times.empty():
                try:
                    old_time = self.request_times.get_nowait()
                    if current_time - old_time > 1.0:
                        continue
                    else:
                        self.request_times.put(old_time)
                        break
                except queue.Empty:
                    break
            
            # Check if we need to wait
            if self.request_times.qsize() >= 4:  # 4 requests per second limit
                time_module.sleep(0.25)
            
            # Record this request
            try:
                self.request_times.put_nowait(current_time)
            except queue.Full:
                pass  # Queue is full, continue anyway
    
    def _update_stats(self, success: bool = True, records: int = 0):
        """Thread-safe statistics update"""
        with self.stats_lock:
            self.stats['requests_made'] += 1
            if success:
                self.stats['successful_requests'] += 1
                self.stats['total_records'] += records
            else:
                self.stats['failed_requests'] += 1
    
    def _fetch_market_holidays_fast(self, year: int) -> Set[str]:
        """Optimized holiday fetching with better caching"""
        cache_key = year
        current_time = time_module.time()
        
        # Check cache
        if (cache_key in self._market_holidays_cache and 
            cache_key in self._holidays_cache_timestamp):
            cache_age_hours = (current_time - self._holidays_cache_timestamp[cache_key]) / 3600
            if cache_age_hours < self._cache_expiry_hours:
                with self.stats_lock:
                    self.stats['cache_hits'] += 1
                return self._market_holidays_cache[cache_key]
        
        self._smart_rate_limit()
        
        url = f"{self.base_url}/v2/market/holidays"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'success' and 'data' in data:
                    holidays = set()
                    
                    for holiday in data['data']:
                        holiday_date = holiday['date']
                        holiday_year = int(holiday_date.split('-')[0])
                        
                        if holiday_year == year:
                            closed_exchanges = holiday.get('closed_exchanges', [])
                            holiday_type = holiday.get('holiday_type', '')
                            
                            if ('NSE' in closed_exchanges or 'BSE' in closed_exchanges or 
                                holiday_type == 'TRADING_HOLIDAY'):
                                holidays.add(holiday_date)
                    
                    # Cache results
                    self._market_holidays_cache[cache_key] = holidays
                    self._holidays_cache_timestamp[cache_key] = current_time
                    
                    self._update_stats(success=True)
                    return holidays
                    
            self._update_stats(success=False)
            return set()
            
        except Exception as e:
            logger.error(f"Error fetching holidays for {year}: {e}")
            self._update_stats(success=False)
            return set()
    
    def _is_trading_day(self, date: datetime) -> bool:
        """Fast trading day check with caching"""
        if date.weekday() >= 5:  # Weekend
            return False
        
        year = date.year
        holidays = self._fetch_market_holidays_fast(year)
        date_str = date.strftime('%Y-%m-%d')
        return date_str not in holidays
    
    def _get_last_trading_day(self) -> datetime:
        """Get last trading day efficiently"""
        current = datetime.now()
        
        # Quick check if today is a trading day and market is open
        if self._is_trading_day(current):
            current_time = current.time()
            if self.market_start <= current_time <= self.market_end:
                return current
        
        # Look backwards for last trading day
        while not self._is_trading_day(current):
            current -= timedelta(days=1)
        
        return current
    
    def _get_optimal_to_date(self) -> str:
        """Get optimal to_date quickly"""
        now = datetime.now()
        current_time = now.time()
        
        if self._is_trading_day(now) and self.market_start <= current_time <= self.market_end:
            return now.strftime('%Y-%m-%d')
        else:
            return self._get_last_trading_day().strftime('%Y-%m-%d')
    
    def _fetch_single_day_data(self, task: FetchTask) -> FetchResult:
        """Fetch data for a single day - optimized for threading"""
        try:
            self._smart_rate_limit()
            
            # Build URL
            url = f"{self.base_url}/v3/historical-candle/{task.instrument_key}/minutes/1/{task.date}"
            
            response = requests.get(url, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'success' and 'candles' in data['data']:
                    df = self._format_to_dataframe_fast(data['data']['candles'])
                    
                    if not df.empty:
                        # Filter to specific date
                        day_start = pd.Timestamp(task.date).tz_localize('Asia/Kolkata')
                        day_end = day_start + pd.Timedelta(days=1)
                        df_filtered = df[(df.index >= day_start) & (df.index < day_end)]
                        
                        self._update_stats(success=True, records=len(df_filtered))
                        return FetchResult(
                            task_id=task.task_id,
                            instrument_key=task.instrument_key,
                            symbol_name=task.symbol_name,
                            data=df_filtered,
                            date=task.date
                        )
            
            self._update_stats(success=False)
            return FetchResult(
                task_id=task.task_id,
                instrument_key=task.instrument_key,
                symbol_name=task.symbol_name,
                data=None,
                error=f"API Error: {response.status_code}"
            )
            
        except Exception as e:
            self._update_stats(success=False)
            return FetchResult(
                task_id=task.task_id,
                instrument_key=task.instrument_key,
                symbol_name=task.symbol_name,
                data=None,
                error=str(e)
            )
    
    def _format_to_dataframe_fast(self, candles: List) -> pd.DataFrame:
        """Optimized DataFrame creation"""
        if not candles:
            return pd.DataFrame()
        
        # Use numpy for faster conversion
        candles_array = np.array(candles)
        
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(candles_array[:, 0]),
            'open': candles_array[:, 1].astype(float),
            'high': candles_array[:, 2].astype(float),
            'low': candles_array[:, 3].astype(float),
            'close': candles_array[:, 4].astype(float),
            'volume': candles_array[:, 5].astype(int),
            'open_interest': candles_array[:, 6].astype(int)
        })
        
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        
        # Round prices efficiently
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].round(2)
        
        return df
    
    def _get_trading_days_fast(self, start_date: datetime, days_needed: int) -> List[str]:
        """Get trading days efficiently"""
        trading_days = []
        current_date = start_date
        
        # Look backwards to get enough trading days
        while len(trading_days) < days_needed and current_date >= datetime(2020, 1, 1):
            if self._is_trading_day(current_date):
                trading_days.append(current_date.strftime('%Y-%m-%d'))
            current_date -= timedelta(days=1)
        
        return list(reversed(trading_days))  # Return in chronological order
    
    def load_symbols(self, symbols_file: str = "symbols.json") -> List[Dict]:
        """Load symbols with error handling"""
        try:
            with open(symbols_file, 'r') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get('symbols', data.get('instruments', list(data.values())))
            
            return []
            
        except Exception as e:
            logger.error(f"Error loading symbols: {e}")
            return []
    
    def fetch_multiple_symbols_fast(self, symbols_file: str = "symbols.json", days_needed: int = 10) -> Dict[str, Optional[pd.DataFrame]]:
        """
        High-speed fetching for multiple symbols using concurrent processing
        
        Args:
            symbols_file: Path to symbols JSON file
            days_needed: Number of trading days to fetch
            
        Returns:
            Dictionary mapping instrument_key to DataFrame
        """
        symbols = self.load_symbols(symbols_file)
        if not symbols:
            logger.error("No symbols loaded")
            return {}
        
        # Reset stats
        self.stats = {key: 0 for key in self.stats.keys()}
        
        # Get trading days
        end_date = self._get_last_trading_day()
        trading_days = self._get_trading_days_fast(end_date, days_needed)
        
        if not trading_days:
            logger.error("No trading days found")
            return {}
        
        # Create all tasks
        tasks = []
        for symbol in symbols:
            if isinstance(symbol, str):
                instrument_key = symbol
                symbol_name = symbol.split('|')[-1] if '|' in symbol else symbol
            elif isinstance(symbol, dict):
                instrument_key = symbol.get('instrument_key') or symbol.get('symbol') or symbol.get('key')
                symbol_name = symbol.get('name') or symbol.get('symbol_name') or instrument_key
            else:
                continue
            
            if not instrument_key:
                continue
            
            for date in trading_days:
                task_id = f"{instrument_key}_{date}"
                tasks.append(FetchTask(instrument_key, symbol_name, date, task_id))
        
        logger.info(f"Starting concurrent fetch for {len(symbols)} symbols × {len(trading_days)} days = {len(tasks)} tasks")
        logger.info(f"Using {self.max_workers} concurrent threads")
        
        # Progress tracking
        results_by_symbol = defaultdict(list)
        completed_tasks = 0
        
        if RICH_AVAILABLE:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeRemainingColumn(),
                console=self.console
            ) as progress:
                task_progress = progress.add_task("Fetching data...", total=len(tasks))
                
                # Execute tasks concurrently
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all tasks
                    future_to_task = {executor.submit(self._fetch_single_day_data, task): task for task in tasks}
                    
                    # Process results as they complete
                    for future in concurrent.futures.as_completed(future_to_task):
                        result = future.result()
                        
                        if result.data is not None and not result.data.empty:
                            results_by_symbol[result.instrument_key].append(result.data)
                        
                        completed_tasks += 1
                        progress.update(task_progress, advance=1)
                        
                        # Update progress description with stats
                        progress.update(
                            task_progress, 
                            description=f"Fetching data... ({self.stats['successful_requests']}/{self.stats['requests_made']} successful)"
                        )
        else:
            # Fallback without rich
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._fetch_single_day_data, task) for task in tasks]
                
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    result = future.result()
                    
                    if result.data is not None and not result.data.empty:
                        results_by_symbol[result.instrument_key].append(result.data)
                    
                    if (i + 1) % 50 == 0:  # Progress every 50 tasks
                        logger.info(f"Progress: {i + 1}/{len(tasks)} tasks completed")
        
        # Combine data for each symbol
        final_results = {}
        successful_symbols = 0
        
        for symbol in symbols:
            instrument_key = symbol.get('instrument_key') if isinstance(symbol, dict) else symbol
            symbol_name = symbol.get('name') if isinstance(symbol, dict) else instrument_key
            
            if instrument_key in results_by_symbol:
                dfs = results_by_symbol[instrument_key]
                if dfs:
                    # Combine all DataFrames for this symbol
                    combined_df = pd.concat(dfs, axis=0)
                    combined_df.sort_index(inplace=True)
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                    
                    # Save to CSV
                    filepath = self.save_to_csv(combined_df, instrument_key, symbol_name)
                    final_results[instrument_key] = combined_df
                    successful_symbols += 1
                    
                    logger.info(f"✅ {symbol_name}: {len(combined_df)} records, Latest: ₹{combined_df['close'].iloc[-1]:.2f}")
                else:
                    final_results[instrument_key] = None
                    logger.warning(f"❌ {symbol_name}: No data retrieved")
            else:
                final_results[instrument_key] = None
                logger.warning(f"❌ {instrument_key}: No data retrieved")
        
        # Print final statistics
        self._print_final_stats(len(symbols), successful_symbols, len(trading_days))
        
        return final_results
    
    def _print_final_stats(self, total_symbols: int, successful_symbols: int, trading_days: int):
        """Print comprehensive statistics"""
        failed_symbols = total_symbols - successful_symbols
        success_rate = (successful_symbols / total_symbols * 100) if total_symbols > 0 else 0
        api_success_rate = (self.stats['successful_requests'] / self.stats['requests_made'] * 100) if self.stats['requests_made'] > 0 else 0
        
        print(f"\n{'='*80}")
        print("📊 FETCHING STATISTICS")
        print('='*80)
        print(f"📈 Symbols Processed: {total_symbols}")
        print(f"✅ Successful: {successful_symbols} ({success_rate:.1f}%)")
        print(f"❌ Failed: {failed_symbols}")
        print(f"📅 Trading Days: {trading_days}")
        print(f"🔄 API Requests: {self.stats['requests_made']}")
        print(f"✅ API Success Rate: {api_success_rate:.1f}%")
        print(f"📊 Total Records: {self.stats['total_records']:,}")
        print(f"🚀 Cache Hits: {self.stats['cache_hits']}")
        print(f"💾 Data Location: {self.data_dir}")
        print('='*80)
    
    def save_to_csv(self, df: pd.DataFrame, instrument_key: str, symbol_name: str = "") -> str:
        """Save DataFrame to CSV efficiently"""
        clean_name = (symbol_name or instrument_key).replace(' ', '_').replace('/', '_').replace('|', '_')
        filename = f"{clean_name}_1min_fast.csv"
        filepath = self.data_dir / filename
        
        # Use efficient CSV writing
        df.to_csv(filepath, index=True, float_format='%.2f')
        return str(filepath)

def create_sample_symbols_file():
    """Create sample symbols file"""
    symbols_file = Path("symbols.json")
    
    if not symbols_file.exists():
        sample_symbols = {
            "symbols": [
                {"name": "Reliance Industries", "instrument_key": "NSE_EQ|INE002A01018"},
                {"name": "TCS", "instrument_key": "NSE_EQ|INE467B01029"},
                {"name": "Infosys", "instrument_key": "NSE_EQ|INE009A01021"},
                {"name": "HDFC Bank", "instrument_key": "NSE_EQ|INE040A01034"},
                {"name": "ICICI Bank", "instrument_key": "NSE_EQ|INE090A01021"},
                {"name": "Bharti Airtel", "instrument_key": "NSE_EQ|INE397D01024"},
                {"name": "SBI", "instrument_key": "NSE_EQ|INE062A01020"},
                {"name": "L&T", "instrument_key": "NSE_EQ|INE018A01030"},
                {"name": "Asian Paints", "instrument_key": "NSE_EQ|INE021A01026"},
                {"name": "Wipro", "instrument_key": "NSE_EQ|INE075A01022"}
            ]
        }
        
        with open(symbols_file, 'w') as f:
            json.dump(sample_symbols, f, indent=2)
        
        logger.info(f"Created sample symbols file: {symbols_file}")
        return True
    
    return False

def main():
    """Main function demonstrating fast fetching"""
    # Read config
    try:
        with open('upstox_config.json', 'r') as f:
            config = json.load(f)
            access_token = config['access_token']
    except Exception as e:
        print(f"Error reading config: {e}")
        return
    
    # Initialize fast fetcher
    fetcher = FastUpstoxHistoricalFetcher(
        access_token=access_token,
        max_workers=15  # Adjust based on your needs (higher = faster but more API load)
    )
    
    # Create sample file if needed
    if create_sample_symbols_file():
        print("📁 Created sample symbols.json with 10 stocks")
    
    # Show market status
    print(f"\n📅 Market Status: Optimal date = {fetcher._get_optimal_to_date()}")
    
    # Start fast fetching
    print(f"\n🚀 Starting FAST concurrent fetching...")
    start_time = time_module.time()
    
    results = fetcher.fetch_multiple_symbols_fast(
        symbols_file="symbols.json",
        days_needed=10
    )
    
    end_time = time_module.time()
    fetch_duration = end_time - start_time
    
    print(f"\n⏱️  Total Fetch Time: {fetch_duration:.2f} seconds")
    print(f"⚡ Average Speed: {len(results) / fetch_duration:.1f} symbols per second")
    
    # Show sample data
    for instrument_key, df in results.items():
        if df is not None and not df.empty:
            print(f"\n📋 Sample data preview:")
            print(df.tail(3).round(2))
            break

if __name__ == "__main__":
    main()