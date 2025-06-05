import requests
import json
import time
import yfinance as yf
import pandas as pd
import logging
from datetime import datetime
import os

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleStockDataFetcher:
    """
    Robust Indian stock data fetcher focusing on reliability over speed.
    Uses a session object and strategic delays to avoid rate-limiting.
    """

    def __init__(self):
        # A longer delay between fetching different symbols is safer.
        self.SYMBOL_FETCH_DELAY = 3.0  # seconds

        # Use a Session object for all yfinance requests for stability.
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

    def get_yahoo_data(self, symbol: str) -> dict:
        """
        Fetch data from Yahoo Finance with improved error handling and delays.
        """
        logger.info(f"Fetching Yahoo data for {symbol}")
        
        symbol_formats = [f"{symbol}.NS", f"{symbol}.BO"]
        
        for i, symbol_format in enumerate(symbol_formats):
            try:
                # Add delay BETWEEN .NS and .BO attempts
                if i > 0:
                    logger.debug(f"Waiting before trying next format for {symbol}...")
                    time.sleep(1.0)

                logger.debug(f"Attempting to use symbol format: {symbol_format}")
                
                ticker = yf.Ticker(symbol_format, session=self.session)
                
                info = ticker.info
                if not info or info.get('regularMarketPrice') is None:
                    logger.warning(f"Incomplete or invalid 'info' data for {symbol_format}.")
                    continue

                hist = ticker.history(period="1y")

                # --- Extract and Validate Key Metrics ---
                current_price = float(info.get('currentPrice') or info.get('regularMarketPrice') or 0.0)
                if current_price <= 0 and not hist.empty:
                    current_price = float(hist['Close'].iloc[-1])
                
                if current_price <= 0:
                    logger.warning(f"Could not determine a valid price for {symbol_format}.")
                    continue

                high_52w = float(info.get('fiftyTwoWeekHigh', 0.0))
                low_52w = float(info.get('fiftyTwoWeekLow', 0.0))
                
                if (high_52w == 0 or low_52w == 0) and not hist.empty:
                    high_52w = float(hist['High'].max())
                    low_52w = float(hist['Low'].min())
                
                if high_52w <= 0 or low_52w <= 0 or low_52w >= high_52w:
                     logger.warning(f"Invalid 52-week range for {symbol_format}.")
                     continue

                position_pct = round(((current_price - low_52w) / (high_52w - low_52w)) * 100, 2)
                
                result = {
                    'symbol': symbol,
                    'current_price': round(current_price, 2),
                    '52w_high': round(high_52w, 2),
                    '52w_low': round(low_52w, 2),
                    'position_pct': position_pct,
                    'pe_ratio': round(float(info.get('trailingPE') or 0.0), 2),
                    'market_cap': int(info.get('marketCap') or 0),
                    'volume': int(info.get('volume') or 0),
                    'change_pct': round(float(info.get('regularMarketChangePercent') or 0.0) * 100, 2),
                    'beta': round(float(info.get('beta') or 0.0), 2),
                    'source': f'yahoo ({symbol_format})',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                logger.info(f"✓ Successfully fetched data for {symbol} using {symbol_format}")
                return result
                
            except Exception as e:
                logger.warning(f"Failed to get data for {symbol_format}: {e}")
                continue
        
        logger.error(f"✗ All Yahoo Finance attempts failed for {symbol}.")
        return None

    def fetch_multiple_stocks(self, symbols: list) -> dict:
        """
        Fetch data for multiple stocks sequentially with a delay between each symbol.
        """
        logger.info(f"Starting sequential fetch for {len(symbols)} symbols")
        results = {}
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"--- Processing {i}/{len(symbols)}: {symbol} ---")
            
            data = self.get_yahoo_data(symbol.upper().strip())
            
            if data:
                results[symbol] = data
            
            if i < len(symbols):
                logger.debug(f"Waiting {self.SYMBOL_FETCH_DELAY} seconds before next symbol...")
                time.sleep(self.SYMBOL_FETCH_DELAY)
        
        success_rate = len(results) / len(symbols) * 100 if symbols else 0
        logger.info(f"Fetch complete: {len(results)}/{len(symbols)} symbols retrieved ({success_rate:.1f}%)")
        
        return results

    def save_to_csv(self, data_dict: dict, filename: str) -> pd.DataFrame:
        """
        Saves data to a CSV file. If the file exists, it updates existing
        stock data and adds new stocks. Otherwise, it creates a new file.
        """
        if not data_dict:
            logger.error("No new data to save!")
            return None

        try:
            # Prepare the new data with 'symbol' as the index
            new_df = pd.DataFrame(list(data_dict.values()))
            columns = [
                'symbol', 'current_price', 'position_pct', 'change_pct', 'volume', 
                'market_cap', 'pe_ratio', 'beta', '52w_high', '52w_low',
                'source', 'timestamp'
            ]
            new_df = new_df.reindex(columns=columns).set_index('symbol')

            # Check if the target CSV file exists
            if os.path.exists(filename):
                logger.info(f"Updating existing file: {filename}")
                try:
                    existing_df = pd.read_csv(filename).set_index('symbol')
                    
                    # Update existing rows with new data
                    existing_df.update(new_df)
                    
                    # Identify and add brand new rows
                    new_records_df = new_df[~new_df.index.isin(existing_df.index)]
                    
                    # Combine the updated old data with the brand new data
                    final_df = pd.concat([existing_df, new_records_df])
                    
                    logger.info(f"✓ Updated {len(new_df) - len(new_records_df)} records and added {len(new_records_df)} new records.")

                except pd.errors.EmptyDataError:
                    # If the file exists but is empty, treat it as a new file
                    logger.warning(f"'{filename}' is empty. Treating as a new file.")
                    final_df = new_df
            else:
                # If the file does not exist, the new data is the final data
                logger.info(f"Creating new file: {filename}")
                final_df = new_df

            # Reset index to make 'symbol' a column again before saving
            final_df.reset_index(inplace=True)
            
            # Save the final combined DataFrame
            final_df.to_csv(filename, index=False)
            
            logger.info(f"✓ Data for {len(final_df)} total stocks saved to {filename}")
            return final_df

        except Exception as e:
            logger.error(f"✗ Failed to save data to CSV: {e}")
            return None

    def print_summary(self, df: pd.DataFrame, newly_fetched_results: dict):
        """
        Prints a summary of the data, highlighting the most recent fetch.
        """
        if df is None or df.empty:
            print("\nNo data to generate a summary.")
            return
        
        print(f"\n{'='*60}")
        print(f"STOCK DATA FETCH SUMMARY")
        print(f"{'='*60}")
        print(f"Total stocks with valid data in CSV: {len(df)}")
        print(f"Average price of all stocks: ₹{df['current_price'].mean():.2f}")
        print(f"Data saved/updated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        if newly_fetched_results:
            print(f"\nSample of Fetched Data (from this run):")
            # Create a dataframe just from the new results for the sample display
            sample_df = pd.DataFrame(list(newly_fetched_results.values()))
            print(sample_df[['symbol', 'current_price', 'position_pct', 'change_pct', 'source']].head().to_string(index=False))


def main():
    """
    Main execution function.
    """
    JSON_FILE = "json.json"
    CSV_FILE = "indian_stocks_data.csv"
    
    if not os.path.exists(JSON_FILE):
        logger.warning(f"File '{JSON_FILE}' not found. Creating a sample file.")
        sample_data = {
            "stocks": [
                "RELIANCE", 
                "TCS", 
                "HDFCBANK",
                "INFY", 
                "ICICIBANK"
            ]
        }
        with open(JSON_FILE, 'w') as f:
            json.dump(sample_data, f, indent=4)
        logger.info(f"Created sample '{JSON_FILE}'. Please edit with your symbols and run again.")
        return
    
    try:
        with open(JSON_FILE, 'r') as f:
            symbols = json.load(f).get('stocks', [])
        if not symbols:
            logger.error(f"No symbols found in '{JSON_FILE}'.")
            return
        symbols = [s.strip().upper() for s in symbols if s and s.strip()]
        logger.info(f"Loaded {len(symbols)} symbols from {JSON_FILE}")
    except Exception as e:
        logger.error(f"Error loading {JSON_FILE}: {e}")
        return

    fetcher = SimpleStockDataFetcher()
    start_time = time.time()
    
    results = fetcher.fetch_multiple_stocks(symbols)
    
    end_time = time.time()
    logger.info(f"Total processing time: {end_time - start_time:.2f} seconds")
    
    if results:
        # 'df' now represents the complete, updated dataframe from the CSV
        df = fetcher.save_to_csv(results, CSV_FILE)
        if df is not None:
            # Pass the newly fetched 'results' to print a relevant sample
            fetcher.print_summary(df, results)
    else:
        logger.error("No data could be fetched for any of the provided symbols.")

if __name__ == "__main__":
    main()