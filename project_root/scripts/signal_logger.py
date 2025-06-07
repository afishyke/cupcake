# Save this as scripts/signal_logger.py
import pandas as pd
import json
from upstox_helpers import OUTPUT_DIR

def log_signals_to_csv():
    summary_path = OUTPUT_DIR / "analytical_summary.json"
    output_csv_path = OUTPUT_DIR / "signal_history.csv"
    if not summary_path.exists(): return
    with open(summary_path, 'r') as f: json_data = json.load(f)
    if not json_data: return
    try:
        df = pd.json_normalize(json_data, sep='_')
        df.to_csv(output_csv_path, mode='a', header=not output_csv_path.exists(), index=False)
        print(f"Logged {len(df)} signals to {output_csv_path}")
    except Exception as e: print(f"Error during CSV logging: {e}")