"""
upstox_fetch_metadata.py

— Reads `config/name.json` → list of symbols.
— Loads existing `outputs/upstox_meta.csv` (if present) to find which symbols already have metadata cached.
— For each symbol NOT in the CSV:
      1. Call Upstox’s “Full Market Quote” endpoint.
      2. Extract relevant static fields.
      3. Append a row to `outputs/upstox_meta.csv`.
— Authentication: Use the same `access_token` logic.
"""
import pandas as pd
import requests
import time

from upstox_helpers import get_upstox_access_token, get_symbol_config, OUTPUT_DIR

API_VERSION = "v3"
BASE_URL = "https://api.upstox.com"

def fetch_full_market_quote(access_token: str, instrument_keys: list) -> dict:
    """Fetches full market quote data for a list of instruments."""
    endpoint = f"{BASE_URL}/{API_VERSION}/market-quote/quotes"
    payload = {"instrument_key": instrument_keys}
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        response = requests.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("data", {})
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching quotes: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"An error occurred fetching quotes: {e}")
    return {}

def main():
    print("--- Starting Static Metadata Fetch ---")
    config = get_symbol_config()
    instrument_keys = config["symbols"]
    
    meta_path = OUTPUT_DIR / "upstox_meta.csv"
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    if meta_path.exists():
        existing_df = pd.read_csv(meta_path)
        cached_keys = set(existing_df["instrument_key"])
        keys_to_fetch = [k for k in instrument_keys if k not in cached_keys]
    else:
        existing_df = pd.DataFrame()
        cached_keys = set()
        keys_to_fetch = instrument_keys

    if not keys_to_fetch:
        print("All symbol metadata is already cached. Nothing to do.")
        return

    print(f"Fetching metadata for {len(keys_to_fetch)} new symbols...")
    access_token = get_upstox_access_token()
    
    # Batch requests to respect potential API limits
    batch_size = 100
    new_metadata = []

    for i in range(0, len(keys_to_fetch), batch_size):
        batch = keys_to_fetch[i:i+batch_size]
        quote_data = fetch_full_market_quote(access_token, batch)
        
        for key, data in quote_data.items():
            # Extract only fundamental/static data
            # Note: Upstox API v3 may have limited fundamental data here. 
            # These are examples based on common data points.
            ohlc = data.get('ohlc', {})
            market_stat = data.get('market_stat', {})
            
            record = {
                "instrument_key": key,
                "52w_high": ohlc.get('high_52w'),
                "52w_low": ohlc.get('low_52w'),
                "lot_size": data.get('lot_size'),
                # The following are often not in quote, placeholders for other sources
                "beta": market_stat.get('beta', None),
                "market_cap": market_stat.get('market_cap', None),
                "pe_ratio": market_stat.get('pe_ratio', None),
                "price_to_book": market_stat.get('pb_ratio', None),
            }
            new_metadata.append(record)
        time.sleep(0.5)

    if new_metadata:
        new_df = pd.DataFrame(new_metadata)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.to_csv(meta_path, index=False)
        print(f"Successfully fetched and saved metadata for {len(new_metadata)} symbols.")
    
    print("--- Finished Static Metadata Fetch ---")

if __name__ == "__main__":
    main()