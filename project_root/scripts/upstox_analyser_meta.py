"""
upstox_analyser_meta.py

— Reads `outputs/upstox_meta.csv` containing rows of instrument_key + static fields.
— Loads into a Pandas DataFrame, cleans data, and calculates derived metrics.
— Builds a JSON mapping and writes it to `outputs/signals/upstox_static_meta.json`.
"""
import pandas as pd
import json
import numpy as np

from upstox_helpers import OUTPUT_DIR

def main():
    print("--- Starting Metadata Analysis ---")
    meta_csv_path = OUTPUT_DIR / "upstox_meta.csv"
    if not meta_csv_path.exists():
        print("Metadata CSV not found. Skipping.")
        return

    df_meta = pd.read_csv(meta_csv_path)
    
    # Get latest OHLCV data to calculate derived metrics like 52w_range_pct
    # We need a reference price, let's use the last close from 1m data
    close_prices = {}
    for _, row in df_meta.iterrows():
        key = row["instrument_key"]
        # This part requires linking to the OHLCV data. A simpler way is to fetch it.
        # For this example, we will assume a 'last_price' column could be added during metadata fetch.
        # Let's add a placeholder.
        df_meta['last_price'] = np.nan # In a real system, populate this
    
    # Clean and cast numeric columns
    numeric_cols = ["52w_high", "52w_low", "last_price", "market_cap", "pe_ratio", "price_to_book", "lot_size"]
    for col in numeric_cols:
        if col in df_meta.columns:
            df_meta[col] = pd.to_numeric(df_meta[col], errors='coerce')

    # Calculate derived metric
    if 'last_price' in df_meta.columns:
        df_meta["52w_range_pct"] = (
            (df_meta["last_price"] - df_meta["52w_low"]) / 
            (df_meta["52w_high"] - df_meta["52w_low"]) * 100
        ).round(2)

    meta_json = {}
    for _, row in df_meta.iterrows():
        key = row["instrument_key"]
        meta_json[key] = {
            "52w_high": row.get("52w_high"),
            "52w_low": row.get("52w_low"),
            "beta": row.get("beta"),
            "market_cap_billion": round(row.get("market_cap", 0) / 1e9, 2) if pd.notna(row.get("market_cap")) else None,
            "pe_ratio": row.get("pe_ratio"),
            "price_to_book": row.get("price_to_book"),
            "lot_size": int(row.get("lot_size")) if pd.notna(row.get("lot_size")) else None,
            "52w_range_pct": row.get("52w_range_pct")
        }

    # Save to JSON
    output_path = OUTPUT_DIR / "signals" / "upstox_static_meta.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(meta_json, f, indent=2, ignore_nan=True)

    print(f"Static metadata JSON saved to {output_path}")
    print("--- Finished Metadata Analysis ---")

if __name__ == "__main__":
    main()