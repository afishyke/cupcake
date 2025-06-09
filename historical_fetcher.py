#!/usr/bin/env python3
"""
Enhanced Historical Data Fetcher for Upstox API
- Fetches data for multiple symbols from symbols.json
- Automatically adjusts to_date to last available trading session
- Handles market hours and holidays intelligently
- Saves individual CSV files for each symbol
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import os
import json
import logging
from typing import List, Optional, Dict, Tuple
import time as time_module
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedUpstoxHistoricalFetcher:
    def __init__(self, access_token: str, data_dir: str = "data/historical"):
        """
        Initialize the historical data fetcher
        
        Args:
            access_token: Upstox API access token
            data_dir: Directory to store historical data CSV files
        """
        self.access_token = access_token
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # API configuration
        self.base_url = "https://api-v2.upstox.com"
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        
        # Rate limiting (300 requests per minute for historical API)
        self.rate_limit_delay = 0.2  # 200ms between requests
        self.last_request_time = 0
        
        # Market hours (IST)
        self.market_start = time(9, 15)
        self.market_end = time(15, 30)
        
        # Indian stock market holidays (you can expand this list)
        self.market_holidays = {
            # 2024 holidays
            '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11',
            '2024-04-17', '2024-05-01', '2024-06-17', '2024-08-15', '2024-10-02',
            '2024-11-01', '2024-11-15', '2024-12-25',
            # 2025 holidays (add more as needed)
            '2025-01-26', '2025-03-14', '2025-03-31', '2025-04-10', '2025-04-14',
            '2025-04-18', '2025-05-01', '2025-06-06', '2025-08-15', '2025-10-02',
            '2025-10-21', '2025-11-04', '2025-12-25'
        }
        
        logger.info(f"Enhanced historical fetcher initialized. Data directory: {self.data_dir}")
    
    def _rate_limit(self):
        """Implement rate limiting between API calls"""
        elapsed = time_module.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time_module.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time_module.time()
    
    def _is_trading_day(self, date: datetime) -> bool:
        """Check if a given date is a trading day"""
        # Weekend check
        if date.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Holiday check
        date_str = date.strftime('%Y-%m-%d')
        if date_str in self.market_holidays:
            return False
        
        return True
    
    def _get_last_trading_day(self, from_date: Optional[datetime] = None) -> datetime:
        """
        Get the last trading day (excluding weekends and holidays)
        
        Args:
            from_date: Starting date to look backwards from (default: current date)
        
        Returns:
            Last trading day as datetime object
        """
        if from_date is None:
            current = datetime.now()
        else:
            current = from_date
        
        # If it's currently market hours on a trading day, use today
        if from_date is None and self._is_trading_day(current):
            current_time = current.time()
            if self.market_start <= current_time <= self.market_end:
                return current
        
        # Otherwise, look for the last trading day
        while not self._is_trading_day(current):
            current -= timedelta(days=1)
        
        return current
    
    def _get_optimal_to_date(self) -> str:
        """
        Get the optimal 'to_date' for API calls based on current time and market status
        
        Returns:
            Date string in 'YYYY-MM-DD' format
        """
        now = datetime.now()
        current_time = now.time()
        
        # If it's a trading day and within market hours, use today
        if self._is_trading_day(now) and self.market_start <= current_time <= self.market_end:
            optimal_date = now
        else:
            # Use the last trading day
            optimal_date = self._get_last_trading_day()
        
        date_str = optimal_date.strftime('%Y-%m-%d')
        logger.info(f"Optimal to_date determined: {date_str}")
        return date_str
    
    def _get_trading_days(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Get list of trading days between start and end date"""
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self._is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    def load_symbols(self, symbols_file: str = "symbols.json") -> List[Dict]:
        """
        Load symbols from JSON file
        
        Args:
            symbols_file: Path to symbols JSON file
        
        Returns:
            List of symbol dictionaries
        """
        try:
            symbols_path = Path(symbols_file)
            if not symbols_path.exists():
                logger.error(f"Symbols file not found: {symbols_file}")
                return []
            
            with open(symbols_path, 'r') as f:
                symbols_data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(symbols_data, list):
                symbols = symbols_data
            elif isinstance(symbols_data, dict):
                if 'symbols' in symbols_data:
                    symbols = symbols_data['symbols']
                elif 'instruments' in symbols_data:
                    symbols = symbols_data['instruments']
                else:
                    # Assume the dict values are the symbols
                    symbols = list(symbols_data.values())
            else:
                logger.error(f"Unexpected symbols file format: {type(symbols_data)}")
                return []
            
            logger.info(f"Loaded {len(symbols)} symbols from {symbols_file}")
            return symbols
            
        except FileNotFoundError:
            logger.error(f"Symbols file not found: {symbols_file}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in symbols file: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading symbols: {e}")
            return []
    
    def get_historical_data(self, instrument_key: str, interval: str, 
                          to_date: Optional[str] = None, from_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetch historical data from Upstox API V3
        
        Args:
            instrument_key: Instrument identifier (e.g., 'NSE_EQ|INE002A01018')
            interval: For V3 API, use format like 'minutes/1' for 1-minute data
            to_date: End date in 'YYYY-MM-DD' format (auto-determined if None)
            from_date: Start date in 'YYYY-MM-DD' format (optional)
            
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        self._rate_limit()
        
        # Auto-determine optimal to_date if not provided
        if to_date is None:
            to_date = self._get_optimal_to_date()
        
        # Convert interval format for V3 API
        if interval == '1minute':
            api_interval = 'minutes/1'
        elif interval == '30minute':
            api_interval = 'minutes/30'
        elif interval == 'day':
            api_interval = 'days/1'
        elif interval == 'week':
            api_interval = 'weeks/1'
        elif interval == 'month':
            api_interval = 'months/1'
        else:
            api_interval = interval  # Use as-is if already in correct format
        
        # Build URL for V3 API
        if from_date:
            url = f"{self.base_url}/v3/historical-candle/{instrument_key}/{api_interval}/{to_date}/{from_date}"
        else:
            url = f"{self.base_url}/v3/historical-candle/{instrument_key}/{api_interval}/{to_date}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            logger.info(f"API Request: {url} - Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'success' and 'candles' in data['data']:
                    return self._format_to_dataframe(data['data']['candles'])
                else:
                    logger.error(f"API Error: {data}")
                    return None
            else:
                logger.error(f"HTTP Error {response.status_code}: {response.text}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def _format_to_dataframe(self, candles: List) -> pd.DataFrame:
        """Convert API candles data to pandas DataFrame"""
        if not candles:
            return pd.DataFrame()
        
        # Create DataFrame with proper column names
        df = pd.DataFrame(candles, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest'
        ])
        
        # Convert timestamp to datetime and set as index
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # Sort by timestamp (ascending - oldest first)
        df.sort_index(inplace=True)
        
        # Convert price columns to float and round to 2 decimal places
        price_cols = ['open', 'high', 'low', 'close']
        df[price_cols] = df[price_cols].astype(float).round(2)
        
        # Convert volume and OI to integers
        df['volume'] = df['volume'].astype(int)
        df['open_interest'] = df['open_interest'].astype(int)
        
        return df
    
    def get_sufficient_history(self, instrument_key: str, days_needed: int = 10) -> Optional[pd.DataFrame]:
        """
        Get sufficient historical 1-minute data for technical analysis
        
        Args:
            instrument_key: Instrument identifier
            days_needed: Number of trading days to fetch (default 10 for most indicators)
            
        Returns:
            DataFrame with historical 1-minute data
        """
        # Calculate date range using last trading day as reference
        end_date = self._get_last_trading_day()
        
        # Get enough calendar days to ensure we have sufficient trading days
        start_calendar_date = end_date - timedelta(days=days_needed * 2)
        
        # Get trading days
        trading_days = self._get_trading_days(start_calendar_date, end_date)
        
        # Limit to required number of trading days
        if len(trading_days) > days_needed:
            trading_days = trading_days[-days_needed:]
        
        all_data = []
        
        for day in trading_days:
            date_str = day.strftime('%Y-%m-%d')
            logger.info(f"Fetching data for {instrument_key} on {date_str}")
            
            # Fetch 1-minute data for this day
            df_day = self.get_historical_data(
                instrument_key=instrument_key,
                interval='1minute',
                to_date=date_str
            )
            
            if df_day is not None and not df_day.empty:
                # Filter to only the specific day we want
                day_start = pd.Timestamp(date_str).tz_localize('Asia/Kolkata')
                day_end = day_start + pd.Timedelta(days=1)
                
                # Filter the dataframe to only include data from the specific day
                df_day_filtered = df_day[(df_day.index >= day_start) & (df_day.index < day_end)]
                
                if not df_day_filtered.empty:
                    all_data.append(df_day_filtered)
                    logger.info(f"Fetched {len(df_day_filtered)} candles for {date_str}")
                else:
                    logger.warning(f"No data available for {date_str} after filtering")
            else:
                logger.warning(f"No data available for {date_str}")
        
        if all_data:
            # Combine all data
            combined_df = pd.concat(all_data, axis=0)
            combined_df.sort_index(inplace=True)
            
            # Remove duplicates (if any)
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            
            logger.info(f"Total historical data fetched: {len(combined_df)} candles")
            return combined_df
        else:
            logger.error("No historical data could be fetched")
            return None
    
    def save_to_csv(self, df: pd.DataFrame, instrument_key: str, symbol_name: str = "", suffix: str = "") -> str:
        """
        Save DataFrame to CSV file
        
        Args:
            df: DataFrame to save
            instrument_key: Instrument identifier for filename
            symbol_name: Human-readable symbol name
            suffix: Optional suffix for filename
            
        Returns:
            Path to saved file
        """
        # Use symbol name if available, otherwise clean instrument key
        if symbol_name:
            clean_name = symbol_name.replace(' ', '_').replace('/', '_')
        else:
            clean_name = instrument_key.replace('|', '_').replace(':', '_')
        
        filename = f"{clean_name}_1min{suffix}.csv"
        filepath = self.data_dir / filename
        
        # Save with timestamp as index
        df.to_csv(filepath, index=True)
        logger.info(f"Data saved to {filepath}")
        
        return str(filepath)
    
    def fetch_multiple_symbols(self, symbols_file: str = "symbols.json", days_needed: int = 10) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Fetch historical data for multiple symbols and save to individual CSV files
        
        Args:
            symbols_file: Path to symbols JSON file
            days_needed: Number of trading days to fetch for each symbol
            
        Returns:
            Dictionary mapping instrument_key to DataFrame (or None if failed)
        """
        # Load symbols
        symbols = self.load_symbols(symbols_file)
        if not symbols:
            logger.error("No symbols loaded. Exiting.")
            return {}
        
        results = {}
        total_symbols = len(symbols)
        
        logger.info(f"Starting to fetch data for {total_symbols} symbols...")
        logger.info(f"Optimal to_date: {self._get_optimal_to_date()}")
        
        for i, symbol in enumerate(symbols, 1):
            try:
                # Handle different symbol formats
                if isinstance(symbol, str):
                    instrument_key = symbol
                    symbol_name = symbol.split('|')[-1] if '|' in symbol else symbol
                elif isinstance(symbol, dict):
                    instrument_key = symbol.get('instrument_key') or symbol.get('symbol') or symbol.get('key')
                    symbol_name = symbol.get('name') or symbol.get('symbol_name') or instrument_key
                else:
                    logger.error(f"Invalid symbol format: {symbol}")
                    continue
                
                if not instrument_key:
                    logger.error(f"No instrument key found for symbol: {symbol}")
                    continue
                
                logger.info(f"[{i}/{total_symbols}] Processing: {symbol_name} ({instrument_key})")
                
                # Fetch historical data
                df = self.get_sufficient_history(instrument_key, days_needed)
                
                if df is not None and not df.empty:
                    # Save to CSV
                    filepath = self.save_to_csv(df, instrument_key, symbol_name)
                    
                    # Store result
                    results[instrument_key] = df
                    
                    logger.info(f"✅ Success: {symbol_name} - {len(df)} records saved to {filepath}")
                    logger.info(f"   Date range: {df.index[0]} to {df.index[-1]}")
                    logger.info(f"   Latest close: ₹{df['close'].iloc[-1]}")
                else:
                    logger.error(f"❌ Failed: {symbol_name} - No data retrieved")
                    results[instrument_key] = None
                
                # Add a small delay between symbols to be respectful to the API
                time_module.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error processing symbol {symbol}: {e}")
                results[getattr(symbol, 'get', lambda x: symbol)('instrument_key', str(symbol))] = None
        
        # Summary
        successful = sum(1 for df in results.values() if df is not None)
        failed = total_symbols - successful
        
        logger.info(f"\n{'='*60}")
        logger.info(f"SUMMARY: Processed {total_symbols} symbols")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"💾 Data saved to: {self.data_dir}")
        logger.info('='*60)
        
        return results
    
    def get_market_status(self) -> Dict[str, any]:
        """
        Get comprehensive market status information
        
        Returns:
            Dictionary with detailed market status
        """
        now = datetime.now()
        current_time = now.time()
        is_trading_day = self._is_trading_day(now)
        is_market_hours = self.market_start <= current_time <= self.market_end
        last_trading_day = self._get_last_trading_day()
        optimal_date = self._get_optimal_to_date()
        
        return {
            'current_datetime': now,
            'current_time': current_time,
            'is_trading_day': is_trading_day,
            'is_market_hours': is_market_hours,
            'is_market_open': is_trading_day and is_market_hours,
            'market_start': self.market_start,
            'market_end': self.market_end,
            'last_trading_day': last_trading_day,
            'optimal_to_date': optimal_date,
            'days_since_last_trading': (now.date() - last_trading_day.date()).days
        }

def create_sample_symbols_file():
    """Create a sample symbols.json file if it doesn't exist"""
    symbols_file = Path("symbols.json")
    
    if not symbols_file.exists():
        sample_symbols = {
            "symbols": [
                {
                    "name": "Reliance Industries",
                    "instrument_key": "NSE_EQ|INE002A01018"
                },
                {
                    "name": "TCS",
                    "instrument_key": "NSE_EQ|INE467B01029"
                },
                {
                    "name": "Infosys",
                    "instrument_key": "NSE_EQ|INE009A01021"
                },
                {
                    "name": "HDFC Bank",
                    "instrument_key": "NSE_EQ|INE040A01034"
                },
                {
                    "name": "ICICI Bank",
                    "instrument_key": "NSE_EQ|INE090A01021"
                }
            ]
        }
        
        with open(symbols_file, 'w') as f:
            json.dump(sample_symbols, f, indent=2)
        
        logger.info(f"Created sample symbols file: {symbols_file}")
        return True
    
    return False

def main():
    """Main function to demonstrate the enhanced fetcher"""
    # Read access token from config file
    try:
        with open('upstox_config.json', 'r') as f:
            config = json.load(f)
            access_token = config['access_token']
    except FileNotFoundError:
        print("upstox_config.json file not found. Please ensure the config file exists.")
        return
    except KeyError:
        print("access_token not found in config file.")
        return
    except json.JSONDecodeError:
        print("Invalid JSON in config file.")
        return
    
    # Initialize enhanced fetcher
    fetcher = EnhancedUpstoxHistoricalFetcher(access_token)
    
    # Display market status
    market_status = fetcher.get_market_status()
    print(f"\n{'='*60}")
    print("MARKET STATUS")
    print('='*60)
    for key, value in market_status.items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    # Create sample symbols file if it doesn't exist
    if create_sample_symbols_file():
        print(f"\n📁 Created sample symbols.json file with 5 stocks.")
        print("You can modify this file to add your own symbols.")
    
    # Fetch data for multiple symbols
    print(f"\n{'='*60}")
    print("FETCHING HISTORICAL DATA FOR MULTIPLE SYMBOLS")
    print('='*60)
    
    results = fetcher.fetch_multiple_symbols(
        symbols_file="symbols.json",
        days_needed=10  # Adjust as needed
    )
    
    # Display results summary
    if results:
        print(f"\n📊 Data fetching completed!")
        print(f"📁 Check the '{fetcher.data_dir}' directory for CSV files.")
        
        # Show a sample of the data for the first successful symbol
        for instrument_key, df in results.items():
            if df is not None and not df.empty:
                print(f"\n📈 Sample data from {instrument_key}:")
                print(df.head().round(2))
                break
    else:
        print("\n❌ No data was fetched successfully.")

if __name__ == "__main__":
    main()