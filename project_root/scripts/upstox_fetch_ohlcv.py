"""
upstox_fetch_ohlcv.py

— Reads `config/name.json` to get the list of symbols.
— For each symbol:
    1. Fetches the last 60 days of 1-minute OHLCV data from the Upstox API.
    2. Converts the data into a Pandas DataFrame.
    3. Saves the DataFrame to `outputs/upstox_1m_ohlcv/{symbol_name}_1m.csv`.
— Handles Upstox API authentication and rate limiting.
"""
import pandas as pd
import requests
import time
from datetime import datetime, timedelta

from upstox_helpers import get_upstox_access_token, get_symbol_config, get_ist_now, OUTPUT_DIR

API_VERSION = "v3"
BASE_URL = "https://api.upstox.com"

def fetch_historical_data(access_token: str, instrument_key: str, to_date: str, from_date: str) -> list:
    """Fetches 1-minute historical candle data for a single instrument."""
    endpoint = f"{BASE_URL}/{API_VERSION}/historical-candle/{instrument_key}/1minute/{to_date}/{from_date}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json().get("data", {}).get("candles", [])
        return data
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching OHLCV for {instrument_key}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"An error occurred fetching OHLCV for {instrument_key}: {e}")
    return []

def main():
    print("--- Starting OHLCV Data Fetch ---")
    
    # --- Basic Setup ---
    config = get_symbol_config()
    symbol_map = config["symbol_map"]
    instrument_keys = list(symbol_map.keys())
    access_token = get_upstox_access_token()

    # --- Date Range ---
    # Fetch data for the last 60 days. Upstox API has limits on date ranges.
    to_date = get_ist_now().strftime('%Y-%m-%d')
    from_date = (get_ist_now() - timedelta(days=60)).strftime('%Y-%m-%d')
    
    # --- Output Directory ---
    output_path = OUTPUT_DIR / "upstox_1m_ohlcv"
    output_path.mkdir(parents=True, exist_ok=True)
    
    # --- Main Loop ---
    for key in instrument_keys:
        symbol_name = symbol_map.get(key, key)
        print(f"Fetching 1-minute OHLCV for {symbol_name} ({key})...")
        
        # Fetch data from Upstox API
        raw_data = fetch_historical_data(access_token, key, to_date, from_date)
        
        if not raw_data:
            print(f"No data returned for {symbol_name}. Skipping.")
            continue
            
        # Convert to Pandas DataFrame
        df = pd.DataFrame(raw_data, columns=["timestamp", "open", "high", "low", "close", "volume", "open_interest"])
        
        # Data Cleaning and Formatting
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df = df.drop(columns=['open_interest']) # Not needed for this project
        df = df.sort_values(by="timestamp", ascending=True).reset_index(drop=True)
        
        # Save to CSV
        file_path = output_path / f"{symbol_name}_1m.csv"
        df.to_csv(file_path, index=False)
        print(f"Saved {len(df)} rows to {file_path}")
        
        # Be a good API citizen
        time.sleep(0.5)
        
    print("--- Finished OHLCV Data Fetch ---")

if __name__ == "__main__":
    main()