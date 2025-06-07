# Replace the contents of scripts/upstox_analyser_tf_optimized.py

import pandas as pd
import numpy as np
import talib as ta
import json
from pathlib import Path
import threading

from upstox_helpers import get_symbol_config, OUTPUT_DIR

class MarketDataAnalyzer:
    def __init__(self):
        self.data_store = {}
        self.all_signals = {}
        self.config = get_symbol_config()
        self.lock = threading.Lock() # To ensure thread-safe updates
        print("Initialized Thread-Safe MarketDataAnalyzer.")

    def bootstrap_history(self, lookback_period: int = 1500):
        # ... (This method remains the same) ...
        # ... (Just ensure it's here) ...
        print("--- Bootstrapping Historical Data into Memory ---")
        symbol_map = self.config["symbol_map"]
        for key, symbol_name in symbol_map.items():
            file_path = OUTPUT_DIR / "upstox_1m_ohlcv" / f"{symbol_name}_1m.csv"
            if file_path.exists():
                df_1m = pd.read_csv(file_path, parse_dates=["timestamp"]).tail(lookback_period)
                self.data_store[key] = df_1m
                print(f"Loaded {len(df_1m)} historical 1m candles for {symbol_name}.")
        print("--- History Bootstrap Complete ---")

    def analyze_single_candle(self, candle: dict) -> dict:
        """
        Analyzes one candle for one symbol. This is the main "job" for a worker thread.
        It returns the raw technical signals. THIS IS A NEW METHOD.
        """
        instrument_key = candle["instrument_key"]
        if instrument_key not in self.data_store: return None
        
        # Append new candle to a *copy* of the dataframe for thread-safe analysis
        df = self.data_store[instrument_key].copy()
        new_row = pd.DataFrame([candle])
        new_row['timestamp'] = pd.to_datetime(new_row['timestamp'])
        df = pd.concat([df, new_row], ignore_index=True)

        # Update the main dataframe in a thread-safe way
        with self.lock:
            self.data_store[instrument_key] = df

        # --- Compute Indicators ---
        if len(df) < 200: return {"error": "Not enough data points"}
        close = df["close"].values.astype(float)
        high = df["high"].values.astype(float)
        low = df["low"].values.astype(float)
        volume = df["volume"].values.astype(float)
        
        tech_signals = {}
        try:
            tech_signals["SMA_50"] = ta.SMA(close, 50)[-1]
            tech_signals["EMA_50"] = ta.EMA(close, 50)[-1]
            tech_signals["RSI_14"] = ta.RSI(close, 14)[-1]
            macd, signal, hist = ta.MACD(close, 12, 26, 9)
            tech_signals["MACD"] = {"macd": macd[-1], "signal": signal[-1], "hist": hist[-1]}
            upper, middle, lower = ta.BBANDS(close, 20)
            tech_signals["BBANDS"] = {"upper": upper[-1], "middle": middle[-1], "lower": lower[-1]}
            tech_signals["ADX_14"] = ta.ADX(high, low, close, 14)[-1]
            tech_signals["close"] = close[-1]
            tech_signals["volume"] = volume[-1]
        except Exception as e:
            return {"error": f"TA-Lib Error: {e}"}
            
        return tech_signals

    def get_complete_analytical_state(self) -> list:
        """
        Returns the latest final analysis for all symbols.
        This is called by the uploader thread.
        """
        with self.lock:
            # We assume self.all_signals holds the rich, final objects
            return list(self.all_signals.values())

    def update_final_analysis(self, instrument_key: str, final_data: dict):
        """Updates the master state with the final rich JSON for one symbol."""
        with self.lock:
            self.all_signals[instrument_key] = final_data