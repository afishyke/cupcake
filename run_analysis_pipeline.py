# run_analysis_pipeline.py
import asyncio
from datetime import datetime, timedelta
from mcfetch import fetch_stocks_from_json_async
from mcanalyse import analyze_mcfetch_data

async def run_technical_analysis():
    """
    Fetches detailed OHLCV data for all symbols and runs technical analysis.
    """
    print("\n--- [Stage 2: Running Technical Analysis] ---")

    # Configuration
    SYMBOLS_FILE = "SYMBOLS.json"
    RESOLUTION = "1D"  # Changed to 1D for more meaningful TA
    DAYS_OF_DATA = 365 # 1 year of data for better indicator calculation

    try:
        print(f"Fetching {DAYS_OF_DATA} days of '{RESOLUTION}' data for all symbols in '{SYMBOLS_FILE}'...")
        
        # 1. Define the date range
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=DAYS_OF_DATA)
        start_date, start_time = start_dt.strftime('%Y-%m-%d'), "09:15:00"
        end_date, end_time = end_dt.strftime('%Y-%m-%d'), "15:30:00"

        # 2. Asynchronously fetch all OHLCV data
        fetched_data = await fetch_stocks_from_json_async(
            json_file_path=SYMBOLS_FILE,
            resolution=RESOLUTION,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            max_concurrent=50
        )

        if not fetched_data:
            print("✗ No OHLCV data was fetched. Aborting technical analysis.")
            return

        # 3. Pass fetched data to the analyzer
        print(f"\n✓ Data for {len(fetched_data)} stocks fetched successfully. Passing to analyzer...")
        analyze_mcfetch_data(raw_data=fetched_data)
        
        print("\n✓ Technical analysis pipeline finished successfully.")

    except FileNotFoundError:
        print(f"✗ Fatal Error: The symbols file was not found: {SYMBOLS_FILE}")
    except Exception as e:
        print(f"✗ Fatal Error: An unexpected error occurred in the technical analysis pipeline: {e}")

if __name__ == '__main__':
    # You can still run this file by itself to test Stage 2
    asyncio.run(run_technical_analysis())