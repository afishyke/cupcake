#!/usr/bin/env python3
"""
Data Fetcher Runner Script

This script helps you manage both historical and live data fetching.
It provides options to:
1. Run historical data fetch only
2. Run live data fetch only
3. Run historical fetch followed by live fetch
4. Check data continuity
"""

import os
import sys
import subprocess
import argparse
import json
from datetime import datetime
import pytz

# Get current script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

def print_banner():
    """Print application banner"""
    print("="*80)
    print("           UPSTOX DATA FETCHER MANAGEMENT SYSTEM")
    print("="*80)
    print(f"Current IST Time: {datetime.now(IST_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"Script Directory: {SCRIPT_DIR}")
    print("="*80)

def check_files():
    """Check if required files exist"""
    files_to_check = {
        "Historical Fetcher": "historical_data_fetcher.py",
        "Live Data Fetcher": "live_data.py", 
        "Symbols File": "symbols.json",
        "Credentials": "/home/abhishek/projects/CUPCAKE/authentication/credentials.json"
    }
    
    print("\n📁 FILE CHECK:")
    all_exist = True
    
    for name, path in files_to_check.items():
        if not os.path.isabs(path):
            full_path = os.path.join(SCRIPT_DIR, path)
        else:
            full_path = path
            
        if os.path.exists(full_path):
            print(f"  ✅ {name}: {full_path}")
        else:
            print(f"  ❌ {name}: {full_path} (NOT FOUND)")
            all_exist = False
    
    if not all_exist:
        print("\n⚠️  Some required files are missing. Please ensure all files are in place.")
        return False
    
    return True

def run_historical_fetch():
    """Run historical data fetch"""
    print("\n🔄 RUNNING HISTORICAL DATA FETCH...")
    print("-" * 50)
    
    script_path = os.path.join(SCRIPT_DIR, "historical_data_fetcher.py")
    
    try:
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=False, 
                              text=True)
        
        if result.returncode == 0:
            print("✅ Historical data fetch completed successfully!")
            return True
        else:
            print(f"❌ Historical data fetch failed with exit code: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error running historical fetch: {e}")
        return False

def run_live_fetch():
    """Run live data fetch"""
    print("\n📡 STARTING LIVE DATA FETCH...")
    print("-" * 50)
    print("Press Ctrl+C to stop the live data fetcher")
    print("-" * 50)
    
    script_path = os.path.join(SCRIPT_DIR, "live_data.py")
    
    try:
        # Run live fetch - this will continue until interrupted
        subprocess.run([sys.executable, script_path])
        print("✅ Live data fetch stopped")
        return True
        
    except KeyboardInterrupt:
        print("\n⚠️  Live data fetch interrupted by user")
        return True
    except Exception as e:
        print(f"❌ Error running live fetch: {e}")
        return False

def get_redis_client():
    """Get Redis client connection"""
    try:
        import redis
        
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
        return redis_client
        
    except ImportError:
        print("❌ Redis library not available. Install with: pip install redis")
        return None
    except Exception as e:
        print(f"❌ Error connecting to Redis: {e}")
        return None

def flush_redis_database(show_info=True):
    """Flush Redis database before starting data fetch"""
    print("\n🗑️  FLUSHING REDIS DATABASE...")
    print("-" * 50)
    
    redis_client = get_redis_client()
    if not redis_client:
        return False
    
    try:
        # Check existing data first
        all_keys = redis_client.keys("*")
        stock_keys = redis_client.keys("stock:*")
        
        if all_keys:
            if show_info:
                print(f"Found {len(all_keys)} total keys in Redis database")
                print(f"Found {len(stock_keys)} stock-related keys")
                
                if stock_keys:
                    print("\nSample stock keys:")
                    for key in stock_keys[:5]:
                        print(f"  {key}")
                    if len(stock_keys) > 5:
                        print(f"  ... and {len(stock_keys) - 5} more")
            
            print(f"🗑️  Deleting all {len(all_keys)} keys from Redis database...")
        else:
            print("Redis database is already empty")
            return True
        
        # Perform flush automatically
        redis_client.flushdb()
        
        # Verify flush
        remaining_keys = redis_client.keys("*")
        if not remaining_keys:
            print("✅ Redis database successfully flushed!")
            return True
        else:
            print(f"⚠️  Warning: {len(remaining_keys)} keys still remain after flush")
            return False
            
    except Exception as e:
        print(f"❌ Error flushing Redis database: {e}")
        return False

def check_data_continuity():
    """Check data continuity in Redis"""
    print("\n🔍 CHECKING DATA CONTINUITY...")
    print("-" * 50)
    
    redis_client = get_redis_client()
    if not redis_client:
        return False
    
    try:
        # Get all stock keys
        keys = redis_client.keys("stock:*:close")
        symbols = set()
        
        for key in keys:
            parts = key.split(':')
            if len(parts) >= 3:
                symbol = parts[1]
                symbols.add(symbol)
        
        print(f"Found data for {len(symbols)} symbols in Redis")
        
        if symbols:
            # Check latest timestamps for a few symbols
            sample_symbols = list(symbols)[:5]
            
            print("\nLatest data timestamps (sample):")
            for symbol in sample_symbols:
                key = f"stock:{symbol}:close"
                try:
                    result = redis_client.execute_command('TS.GET', key)
                    if result and len(result) >= 2:
                        timestamp_ms = result[0]
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=IST_TIMEZONE)
                        print(f"  {symbol:<20}: {dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
                except Exception as e:
                    print(f"  {symbol:<20}: Error reading data")
        else:
            print("No stock data found in Redis database")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking Redis: {e}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Data Fetcher Management System")
    parser.add_argument("--mode", choices=["historical", "live", "both", "check", "flush"], 
                       default="both", help="Operation mode")
    parser.add_argument("--skip-checks", action="store_true", 
                       help="Skip file existence checks")
    parser.add_argument("--no-flush", action="store_true", 
                       help="Skip Redis database flush")
    parser.add_argument("--silent-flush", action="store_true", 
                       help="Flush database without showing details")
    
    args = parser.parse_args()
    
    print_banner()
    
    # Check files unless skipped
    if not args.skip_checks:
        if not check_files():
            sys.exit(1)
    
    # Execute based on mode
    if args.mode == "check":
        check_data_continuity()
        
    elif args.mode == "flush":
        success = flush_redis_database(show_info=not args.silent_flush)
        sys.exit(0 if success else 1)
        
    elif args.mode == "historical":
        # Flush database first unless skipped
        if not args.no_flush:
            flush_success = flush_redis_database(show_info=not args.silent_flush)
            if not flush_success:
                print("❌ Database flush failed. Use --no-flush to skip or fix Redis connection.")
                sys.exit(1)
        
        success = run_historical_fetch()
        sys.exit(0 if success else 1)
        
    elif args.mode == "live":
        # Flush database first unless skipped
        if not args.no_flush:
            flush_success = flush_redis_database(show_info=not args.silent_flush)
            if not flush_success:
                print("❌ Database flush failed. Use --no-flush to skip or fix Redis connection.")
                sys.exit(1)
        
        success = run_live_fetch()
        sys.exit(0 if success else 1)
        
    elif args.mode == "both":
        print("\n🚀 RUNNING COMPLETE DATA PIPELINE...")
        
        # Flush database first unless skipped
        if not args.no_flush:
            flush_success = flush_redis_database(show_info=not args.silent_flush)
            if not flush_success:
                print("❌ Database flush failed. Use --no-flush to skip or fix Redis connection.")
                sys.exit(1)
        
        # First run historical fetch
        hist_success = run_historical_fetch()
        
        if hist_success:
            print("\n✅ Historical fetch completed. Starting live fetch...")
            
            # Small delay to ensure historical fetch is complete
            print("Waiting 3 seconds before starting live fetch...")
            import time
            time.sleep(3)
            
            # Then run live fetch (no additional flush needed)
            live_success = run_live_fetch()
            
            print("\n✅ Complete pipeline finished!")
        else:
            print("\n❌ Historical fetch failed. Skipping live fetch.")
            sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)