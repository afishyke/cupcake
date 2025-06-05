# main_orchestrator.py
import asyncio
import time
from datetime import datetime

# Import the refactored functions from our other modules
from aunthenticator import find_and_fetch_new_symbols
from run_analysis_pipeline import run_technical_analysis

def print_header():
    """Prints a nice header for the whole process."""
    print("=" * 60)
    print(f" COMPREHENSIVE STOCK ANALYSIS PIPELINE")
    print(f" Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

async def main():
    """
    The main orchestrator that runs the entire data pipeline in order.
    """
    print_header()
    start_time = time.time()
    
    # --- STAGE 1: UPDATE FUNDAMENTAL DATA DATABASE ---
    # This runs synchronously as it involves subprocesses and file I/O checks.
    find_and_fetch_new_symbols()
    
    # --- STAGE 2: RUN DETAILED TECHNICAL ANALYSIS ---
    # This runs asynchronously for high-performance OHLCV fetching.
    await run_technical_analysis()
    
    end_time = time.time()
    print("\n--- [Pipeline Complete] ---")
    print(f"✓ All stages finished in {end_time - start_time:.2f} seconds.")
    print("=" * 60)

if __name__ == "__main__":
    # On Windows, you might need this policy setting for asyncio
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())