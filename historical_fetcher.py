#!/usr/bin/env python3
"""
Historical Data Fetcher for Upstox API
Fetches required historical 1-minute data for technical analysis
Handles cold start problem by fetching sufficient historical context
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import os
import json
import logging
from typing import List, Optional, Dict
import time as time_module
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UpstoxHistoricalFetcher:
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
        
        logger.info(f"Historical fetcher initialized. Data directory: {self.data_dir}")
    
    def _rate_limit(self):
        """Implement rate limiting between API calls"""
        elapsed = time_module.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time_module.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time_module.time()
    
    def _get_trading_days(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Get list of trading days between start and end date (excludes weekends)"""
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            # Skip weekends (Saturday=5, Sunday=6)
            if current_date.weekday() < 5:
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days
    
    def get_historical_data(self, instrument_key: str, interval: str, 
                          to_date: str, from_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetch historical data from Upstox API
        
        Args:
            instrument_key: Instrument identifier (e.g., 'NSE_EQ|INE002A01018')
            interval: '1minute', '30minute', 'day', 'week', 'month'
            to_date: End date in 'YYYY-MM-DD' format
            from_date: Start date in 'YYYY-MM-DD' format (optional)
            
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        self._rate_limit()
        
        # Build URL
        if from_date:
            url = f"{self.base_url}/v3/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"
        else:
            url = f"{self.base_url}/v3/historical-candle/{instrument_key}/{interval}/{to_date}"
        
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
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_needed * 2)  # Account for weekends/holidays
        
        # Get trading days
        trading_days = self._get_trading_days(
            datetime.combine(start_date, datetime.min.time()),
            datetime.combine(end_date, datetime.min.time())
        )
        
        if len(trading_days) < days_needed:
            # Extend further back if needed
            extended_start = start_date - timedelta(days=days_needed)
            trading_days = self._get_trading_days(
                datetime.combine(extended_start, datetime.min.time()),
                datetime.combine(end_date, datetime.min.time())
            )
        
        # Take last N trading days
        recent_days = trading_days[-days_needed:] if len(trading_days) >= days_needed else trading_days
        
        all_data = []
        
        for day in recent_days:
            date_str = day.strftime('%Y-%m-%d')
            logger.info(f"Fetching data for {instrument_key} on {date_str}")
            
            # Fetch 1-minute data for this day
            df_day = self.get_historical_data(
                instrument_key=instrument_key,
                interval='1minute',
                to_date=date_str,
                from_date=date_str
            )
            
            if df_day is not None and not df_day.empty:
                all_data.append(df_day)
                logger.info(f"Fetched {len(df_day)} candles for {date_str}")
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
    
    def save_to_csv(self, df: pd.DataFrame, instrument_key: str, suffix: str = "") -> str:
        """
        Save DataFrame to CSV file
        
        Args:
            df: DataFrame to save
            instrument_key: Instrument identifier for filename
            suffix: Optional suffix for filename
            
        Returns:
            Path to saved file
        """
        # Clean instrument key for filename
        clean_name = instrument_key.replace('|', '_').replace(':', '_')
        filename = f"{clean_name}_1min{suffix}.csv"
        filepath = self.data_dir / filename
        
        # Save with timestamp as index
        df.to_csv(filepath, index=True)
        logger.info(f"Data saved to {filepath}")
        
        return str(filepath)
    
    def load_from_csv(self, instrument_key: str, suffix: str = "") -> Optional[pd.DataFrame]:
        """
        Load historical data from CSV file
        
        Args:
            instrument_key: Instrument identifier
            suffix: Optional suffix for filename
            
        Returns:
            DataFrame or None if file doesn't exist
        """
        clean_name = instrument_key.replace('|', '_').replace(':', '_')
        filename = f"{clean_name}_1min{suffix}.csv"
        filepath = self.data_dir / filename
        
        if filepath.exists():
            try:
                df = pd.read_csv(filepath, index_col=0, parse_dates=True)
                logger.info(f"Loaded {len(df)} records from {filepath}")
                return df
            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}")
                return None
        else:
            logger.warning(f"File not found: {filepath}")
            return None
    
    def update_historical_data(self, instrument_key: str, days_to_update: int = 1) -> Optional[pd.DataFrame]:
        """
        Update existing historical data with recent data
        
        Args:
            instrument_key: Instrument identifier
            days_to_update: Number of recent days to fetch and update
            
        Returns:
            Updated DataFrame
        """
        # Load existing data
        existing_df = self.load_from_csv(instrument_key)
        
        if existing_df is not None:
            # Get the last timestamp
            last_timestamp = existing_df.index[-1]
            logger.info(f"Last existing data: {last_timestamp}")
            
            # Fetch recent data
            end_date = datetime.now().date()
            start_date = last_timestamp.date() + timedelta(days=1)
            
            if start_date <= end_date:
                recent_df = self.get_historical_data(
                    instrument_key=instrument_key,
                    interval='1minute',
                    to_date=end_date.strftime('%Y-%m-%d'),
                    from_date=start_date.strftime('%Y-%m-%d')
                )
                
                if recent_df is not None and not recent_df.empty:
                    # Combine with existing data
                    combined_df = pd.concat([existing_df, recent_df])
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                    combined_df.sort_index(inplace=True)
                    
                    # Save updated data
                    self.save_to_csv(combined_df, instrument_key)
                    logger.info(f"Updated data: {len(recent_df)} new candles added")
                    
                    return combined_df
                else:
                    logger.info("No new data to add")
                    return existing_df
            else:
                logger.info("Data is already up to date")
                return existing_df
        else:
            # No existing data, fetch fresh
            logger.info("No existing data found, fetching fresh historical data")
            return self.fetch_and_save_historical_data(instrument_key)
    
    def fetch_and_save_historical_data(self, instrument_key: str, days_needed: int = 10) -> Optional[pd.DataFrame]:
        """
        Fetch historical data and save to CSV
        
        Args:
            instrument_key: Instrument identifier
            days_needed: Number of trading days to fetch
            
        Returns:
            DataFrame with historical data
        """
        logger.info(f"Fetching historical data for {instrument_key}")
        
        # Fetch historical data
        df = self.get_sufficient_history(instrument_key, days_needed)
        
        if df is not None and not df.empty:
            # Save to CSV
            filepath = self.save_to_csv(df, instrument_key)
            
            # Display summary
            logger.info(f"Historical data summary for {instrument_key}:")
            logger.info(f"  Records: {len(df)}")
            logger.info(f"  Date range: {df.index[0]} to {df.index[-1]}")
            logger.info(f"  Latest close: ₹{df['close'].iloc[-1]}")
            logger.info(f"  Saved to: {filepath}")
            
            return df
        else:
            logger.error(f"Failed to fetch historical data for {instrument_key}")
            return None
    
    def get_market_status(self) -> Dict[str, bool]:
        """
        Check if market is currently open
        
        Returns:
            Dictionary with market status information
        """
        now = datetime.now()
        current_time = now.time()
        is_trading_day = now.weekday() < 5  # Monday=0, Friday=4
        is_market_hours = self.market_start <= current_time <= self.market_end
        
        return {
            'is_trading_day': is_trading_day,
            'is_market_hours': is_market_hours,
            'is_market_open': is_trading_day and is_market_hours,
            'current_time': current_time,
            'market_start': self.market_start,
            'market_end': self.market_end
        }

def main():
    """Example usage of HistoricalFetcher"""
    # Read access token from file (update path as needed)
    try:
        with open('authentication/upstox_token.json', 'r') as f:
            access_token = f.read().strip().strip('"')
    except FileNotFoundError:
        print("Token file not found. Please run authentication first.")
        return
    
    # Initialize fetcher
    fetcher = UpstoxHistoricalFetcher(access_token)
    
    # Check market status
    market_status = fetcher.get_market_status()
    print(f"Market Status: {market_status}")
    
    # Sample instruments for testing
    instruments = [
        'NSE_EQ|INE002A01018',  # Reliance
        'NSE_EQ|INE009A01021',  # Infosys
        'NSE_EQ|INE467B01029'   # TCS
    ]
    
    # Fetch historical data for each instrument
    for instrument in instruments:
        print(f"\n{'='*60}")
        print(f"Fetching data for: {instrument}")
        print('='*60)
        
        # Check if we have existing data
        existing_df = fetcher.load_from_csv(instrument)
        
        if existing_df is not None:
            print(f"Found existing data: {len(existing_df)} records")
            print(f"Last update: {existing_df.index[-1]}")
            
            # Update with recent data
            updated_df = fetcher.update_historical_data(instrument)
        else:
            # Fetch fresh historical data
            updated_df = fetcher.fetch_and_save_historical_data(instrument, days_needed=15)
        
        if updated_df is not None:
            print(f"Final dataset: {len(updated_df)} records")
            print(f"Date range: {updated_df.index[0]} to {updated_df.index[-1]}")
            
            # Show sample data
            print("\nSample data (last 5 records):")
            print(updated_df.tail().round(2))

if __name__ == "__main__":
    main()