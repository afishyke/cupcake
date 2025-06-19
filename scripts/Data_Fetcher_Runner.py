#!/usr/bin/env python3
"""
Updated Data Fetcher Runner Script for Enhanced Live Data Fetcher

This script manages both historical and enhanced live data fetching with better coordination.
It provides options to:
1. Run historical data fetch only
2. Run enhanced live data fetch only (using new V3 OHLC API with same Redis format)
3. Run historical fetch followed by live fetch with proper timing
4. Check data continuity
"""

import os
import sys
import subprocess
import argparse
import json
from datetime import datetime, timedelta
import pytz
import time

# Get current script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

def print_banner():
    """Print application banner"""
    print("="*80)
    print("        ENHANCED UPSTOX DATA FETCHER MANAGEMENT SYSTEM V2")
    print("="*80)
    print(f"Current IST Time: {datetime.now(IST_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"Script Directory: {SCRIPT_DIR}")
    print("="*80)

def check_files():
    """Check if required files exist"""
    files_to_check = {
        "Historical Fetcher": "historical_data_fetcher.py",
        "Enhanced Live Fetcher": "enhanced_live_data_fetcher.py",  # Updated filename
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

def run_enhanced_live_fetch():
    """Run enhanced live data fetch using V3 OHLC API with same Redis format"""
    print("\n📡 STARTING ENHANCED LIVE DATA FETCH...")
    print("-" * 50)
    print("Using Enhanced Live Data Fetcher with:")
    print("  • V3 OHLC API for faster, more reliable data fetching")
    print("  • Same Redis TimeSeries format as historical fetcher")
    print("  • Automatic previous minute candle detection")
    print("  • Concurrent processing for multiple symbols")
    print("  • Market hours awareness and optimal timing")
    print("  • Comprehensive error handling and statistics")
    print("Press Ctrl+C to stop the live data fetcher")
    print("-" * 50)
    
    script_path = os.path.join(SCRIPT_DIR, "enhanced_live_data_fetcher.py")
    
    try:
        # Run enhanced live fetch - this will continue until interrupted
        subprocess.run([sys.executable, script_path])
        print("✅ Enhanced live data fetch stopped")
        return True
        
    except KeyboardInterrupt:
        print("\n⚠️  Live data fetch interrupted by user")
        return True
    except Exception as e:
        print(f"❌ Error running enhanced live fetch: {e}")
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
    """Check data continuity in Redis with enhanced analysis"""
    print("\n🔍 CHECKING DATA CONTINUITY...")
    print("-" * 50)
    
    redis_client = get_redis_client()
    if not redis_client:
        return False
    
    try:
        # Get all stock keys
        close_keys = redis_client.keys("stock:*:close")
        symbols = set()
        
        for key in close_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                symbol = parts[1]
                symbols.add(symbol)
        
        print(f"Found data for {len(symbols)} symbols in Redis")
        
        if symbols:
            # Check latest timestamps for symbols
            sample_symbols = list(symbols)[:10]  # Check more symbols
            
            print("\nLatest data timestamps (sample):")
            print("-" * 60)
            
            current_time = datetime.now(IST_TIMEZONE)
            
            for symbol in sample_symbols:
                key = f"stock:{symbol}:close"
                try:
                    # Get latest timestamp
                    result = redis_client.execute_command('TS.GET', key)
                    if result and len(result) >= 2:
                        timestamp_ms = result[0]
                        value = result[1]
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=IST_TIMEZONE)
                        
                        # Calculate data age
                        age = current_time - dt
                        age_str = f"{age.total_seconds()/60:.0f}m ago" if age.total_seconds() < 3600 else f"{age.total_seconds()/3600:.1f}h ago"
                        
                        # Check if data is recent (within last hour)
                        status = "🟢" if age.total_seconds() < 3600 else "🟡" if age.total_seconds() < 7200 else "🔴"
                        
                        print(f"  {status} {symbol:<20}: {dt.strftime('%Y-%m-%d %H:%M:%S IST')} ({age_str}) Value: {value}")
                        
                        # Get data count for this symbol
                        try:
                            count_result = redis_client.execute_command('TS.INFO', key)
                            if count_result:
                                # Parse TS.INFO result to get sample count
                                info_dict = {}
                                for i in range(0, len(count_result), 2):
                                    if i + 1 < len(count_result):
                                        info_dict[count_result[i].decode() if hasattr(count_result[i], 'decode') else count_result[i]] = count_result[i + 1]
                                
                                sample_count = info_dict.get('totalSamples', info_dict.get(b'totalSamples', 'Unknown'))
                                print(f"    📊 Total samples: {sample_count}")
                        except:
                            pass
                            
                except Exception as e:
                    print(f"  ❌ {symbol:<20}: Error reading data - {e}")
            
            # Check for metadata
            print(f"\n📋 METADATA CHECK:")
            metadata_keys = redis_client.keys("stock:*:metadata")
            live_metadata_keys = redis_client.keys("stock:*:live_metadata")
            
            print(f"Historical metadata entries: {len(metadata_keys)}")
            print(f"Live metadata entries: {len(live_metadata_keys)}")
            
            if metadata_keys:
                # Show sample metadata
                sample_meta_key = metadata_keys[0]
                try:
                    meta_data = redis_client.hget(sample_meta_key, 'data')
                    if meta_data:
                        meta_json = json.loads(meta_data)
                        symbol_name = sample_meta_key.split(':')[1]
                        print(f"\nSample metadata for {symbol_name}:")
                        print(f"  Candles stored: {meta_json.get('total_candles_stored', 'Unknown')}")
                        print(f"  Date range: {meta_json.get('oldest_timestamp', 'Unknown')} to {meta_json.get('newest_timestamp', 'Unknown')}")
                        print(f"  Includes today: {meta_json.get('includes_today', 'Unknown')}")
                        print(f"  Strategies used: {meta_json.get('strategies_used', 'Unknown')}")
                except Exception as e:
                    print(f"Error reading metadata: {e}")
            
            # Data quality assessment
            print(f"\n📈 DATA QUALITY ASSESSMENT:")
            recent_data_count = 0
            old_data_count = 0
            
            for symbol in list(symbols)[:20]:  # Check more symbols for assessment
                key = f"stock:{symbol}:close"
                try:
                    result = redis_client.execute_command('TS.GET', key)
                    if result and len(result) >= 2:
                        timestamp_ms = result[0]
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=IST_TIMEZONE)
                        age = current_time - dt
                        
                        if age.total_seconds() < 3600:  # Less than 1 hour old
                            recent_data_count += 1
                        else:
                            old_data_count += 1
                except:
                    old_data_count += 1
            
            total_checked = recent_data_count + old_data_count
            if total_checked > 0:
                recent_percentage = (recent_data_count / total_checked) * 100
                print(f"Recent data (< 1h): {recent_data_count}/{total_checked} symbols ({recent_percentage:.1f}%)")
                
                if recent_percentage > 80:
                    print("✅ Data freshness: EXCELLENT")
                elif recent_percentage > 60:
                    print("🟡 Data freshness: GOOD")
                else:
                    print("🔴 Data freshness: NEEDS ATTENTION")
        else:
            print("No stock data found in Redis database")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking Redis: {e}")
        return False

def calculate_optimal_timing():
    """Calculate optimal timing for starting live fetch after historical"""
    now_ist = datetime.now(IST_TIMEZONE)
    
    # If market hours or near market hours, start live fetch at next minute boundary + 15 seconds
    if 8 <= now_ist.hour < 17:  # Extended hours for pre/post market
        next_minute = now_ist.replace(second=0, microsecond=0) + timedelta(minutes=1)
        optimal_start = next_minute + timedelta(seconds=15)
        
        wait_seconds = (optimal_start - now_ist).total_seconds()
        
        if wait_seconds > 0:
            print(f"\n⏰ OPTIMAL TIMING:")
            print(f"Current time: {now_ist.strftime('%H:%M:%S IST')}")
            print(f"Next live fetch: {optimal_start.strftime('%H:%M:%S IST')}")
            print(f"Wait time: {wait_seconds:.1f} seconds")
            print(f"This ensures optimal candle alignment with historical data")
            
            return wait_seconds
    
    return 0

def main():
    """Main function with enhanced coordination and new live fetcher"""
    parser = argparse.ArgumentParser(description="Enhanced Data Fetcher Management System V2")
    parser.add_argument("--mode", choices=["historical", "live", "both", "check", "flush"], 
                       default="both", help="Operation mode")
    parser.add_argument("--skip-checks", action="store_true", 
                       help="Skip file existence checks")
    parser.add_argument("--no-flush", action="store_true", 
                       help="Skip Redis database flush")
    parser.add_argument("--silent-flush", action="store_true", 
                       help="Flush database without showing details")
    parser.add_argument("--no-timing", action="store_true",
                       help="Skip optimal timing calculation for live fetch")
    
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
        # No flush for live-only mode to preserve existing data
        print("🔄 Starting enhanced live data fetch (preserving existing data)...")
        success = run_enhanced_live_fetch()
        sys.exit(0 if success else 1)
        
    elif args.mode == "both":
        print("\n🚀 RUNNING ENHANCED DATA PIPELINE V2...")
        
        # Flush database first unless skipped
        if not args.no_flush:
            flush_success = flush_redis_database(show_info=not args.silent_flush)
            if not flush_success:
                print("❌ Database flush failed. Use --no-flush to skip or fix Redis connection.")
                sys.exit(1)
        
        # First run historical fetch
        hist_success = run_historical_fetch()
        
        if hist_success:
            print("\n✅ Historical fetch completed successfully!")
            
            # Quick data continuity check
            print("\n🔍 Quick data verification...")
            check_data_continuity()
            
            # Calculate optimal timing for live fetch
            if not args.no_timing:
                wait_seconds = calculate_optimal_timing()
                
                if wait_seconds > 0:
                    print(f"\n⏰ Waiting {wait_seconds:.1f} seconds for optimal live fetch timing...")
                    time.sleep(wait_seconds)
                else:
                    print("\n🚀 Starting enhanced live fetch immediately...")
                    time.sleep(2)  # Small buffer
            else:
                print("\n🚀 Starting enhanced live fetch with standard delay...")
                time.sleep(3)
            
            # Then run enhanced live fetch
            live_success = run_enhanced_live_fetch()
            
            print("\n✅ Enhanced pipeline V2 finished!")
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