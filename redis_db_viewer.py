#!/usr/bin/env python3
"""
Redis Database Viewer for Trading Data
Comprehensive tool to inspect stored historical and live market data
"""

import os
import redis
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
import argparse
import sys

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

class TradingDataViewer:
    def __init__(self, host=None, port=None, db=0):
        """Initialize Redis connection"""
        # Use environment variables if not provided
        if host is None:
            host = os.environ.get('REDIS_HOST', 'localhost')
        if port is None:
            port = int(os.environ.get('REDIS_PORT', '6379'))
            
        try:
            self.redis_client = redis.Redis(
                host=host, port=port, db=db,
                decode_responses=True,
                socket_timeout=10
            )
            self.redis_client.ping()
            print(f"✅ Connected to Redis at {host}:{port} (DB: {db})")
        except redis.RedisError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            sys.exit(1)
    
    def get_database_overview(self):
        """Get overview of all data in the database"""
        print("\n" + "="*60)
        print("📊 TRADING DATABASE OVERVIEW")
        print("="*60)
        
        # Get all keys
        all_keys = self.redis_client.keys("*")
        print(f"Total Keys: {len(all_keys)}")
        
        # Categorize keys
        stock_ts_keys = [k for k in all_keys if k.startswith('stock:') and not k.endswith(':metadata')]
        metadata_keys = [k for k in all_keys if k.endswith(':metadata')]
        other_keys = [k for k in all_keys if not k.startswith('stock:')]
        
        print(f"Stock TimeSeries Keys: {len(stock_ts_keys)}")
        print(f"Metadata Keys: {len(metadata_keys)}")
        print(f"Other Keys: {len(other_keys)}")
        
        # Analyze stock data structure
        if stock_ts_keys:
            symbols = set()
            data_types = set()
            
            for key in stock_ts_keys:
                parts = key.split(':')
                if len(parts) >= 3:
                    symbol = parts[1]
                    data_type = parts[2]
                    symbols.add(symbol)
                    data_types.add(data_type)
            
            print(f"\nUnique Symbols: {len(symbols)}")
            print(f"Data Types: {sorted(data_types)}")
            
            # Show sample symbols
            sample_symbols = sorted(list(symbols))[:10]
            print(f"Sample Symbols: {sample_symbols}")
            
            if len(symbols) > 10:
                print(f"... and {len(symbols) - 10} more")
        
        # Show other keys if any
        if other_keys:
            print(f"\nOther Keys: {other_keys}")
        
        return {
            'total_keys': len(all_keys),
            'symbols': sorted(list(symbols)) if stock_ts_keys else [],
            'data_types': sorted(list(data_types)) if stock_ts_keys else [],
            'metadata_keys': metadata_keys,
            'other_keys': other_keys
        }
    
    def view_symbol_data(self, symbol_name, hours_back=24, show_samples=10):
        """View detailed data for a specific symbol"""
        # Clean symbol name
        symbol_clean = symbol_name.replace(' ', '_').replace('&', 'and')
        
        print(f"\n" + "="*60)
        print(f"📈 SYMBOL DATA: {symbol_name} ({symbol_clean})")
        print("="*60)
        
        # Check if symbol exists
        symbol_keys = self.redis_client.keys(f"stock:{symbol_clean}:*")
        if not symbol_keys:
            print(f"❌ No data found for symbol: {symbol_name}")
            return None
        
        print(f"Found {len(symbol_keys)} data keys for this symbol")
        
        # Get time range
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        
        data_summary = {}
        
        # Check each data type
        for data_type in ['open', 'high', 'low', 'close', 'volume']:
            key = f"stock:{symbol_clean}:{data_type}"
            
            try:
                # Get recent data
                result = self.redis_client.execute_command('TS.RANGE', key, start_time, end_time)
                
                if result:
                    timestamps, values = zip(*result)
                    
                    # Convert to readable format
                    readable_data = []
                    for ts, val in zip(timestamps, values):
                        dt = datetime.fromtimestamp(ts/1000, tz=IST)
                        readable_data.append((dt.strftime('%Y-%m-%d %H:%M:%S'), float(val)))
                    
                    data_summary[data_type] = {
                        'count': len(result),
                        'latest_value': float(values[-1]) if values else None,
                        'latest_time': readable_data[-1][0] if readable_data else None,
                        'oldest_time': readable_data[0][0] if readable_data else None,
                        'sample_data': readable_data[-show_samples:] if readable_data else []
                    }
                    
                    print(f"\n{data_type.upper()}:")
                    print(f"  Data Points: {len(result)}")
                    print(f"  Latest Value: {float(values[-1]):.2f}")
                    print(f"  Latest Time: {readable_data[-1][0]}")
                    print(f"  Time Range: {readable_data[0][0]} to {readable_data[-1][0]}")
                    
                    if show_samples > 0:
                        print(f"  Recent {min(show_samples, len(readable_data))} values:")
                        for time_str, value in readable_data[-show_samples:]:
                            print(f"    {time_str}: {value:.2f}")
                
                else:
                    print(f"\n{data_type.upper()}: No data in last {hours_back} hours")
                    data_summary[data_type] = {'count': 0}
                    
            except Exception as e:
                print(f"\n{data_type.upper()}: Error reading data - {e}")
                data_summary[data_type] = {'error': str(e)}
        
        # Check metadata
        metadata_key = f"stock:{symbol_clean}:metadata"
        if self.redis_client.exists(metadata_key):
            print(f"\n📋 METADATA:")
            metadata_raw = self.redis_client.hget(metadata_key, 'data')
            if metadata_raw:
                try:
                    metadata = json.loads(metadata_raw)
                    for key, value in metadata.items():
                        print(f"  {key}: {value}")
                except json.JSONDecodeError:
                    print(f"  Raw: {metadata_raw}")
        
        return data_summary
    
    def view_all_symbols_summary(self, hours_back=1):
        """View summary of all symbols"""
        print(f"\n" + "="*80)
        print(f"📊 ALL SYMBOLS SUMMARY (Last {hours_back} hours)")
        print("="*80)
        
        # Get all symbols
        stock_keys = self.redis_client.keys("stock:*:close")  # Use close price as reference
        symbols = []
        
        for key in stock_keys:
            symbol_clean = key.split(':')[1]
            symbols.append(symbol_clean)
        
        if not symbols:
            print("❌ No symbol data found")
            return
        
        print(f"Found {len(symbols)} symbols")
        print("-" * 80)
        print(f"{'Symbol':<25} {'Latest Price':<12} {'Data Points':<12} {'Last Update':<20}")
        print("-" * 80)
        
        # Get time range
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        
        for symbol in sorted(symbols):
            key = f"stock:{symbol}:close"
            
            try:
                result = self.redis_client.execute_command('TS.RANGE', key, start_time, end_time)
                
                if result:
                    latest_ts, latest_price = result[-1]
                    latest_time = datetime.fromtimestamp(latest_ts/1000, tz=IST)
                    
                    # Convert symbol back to readable format
                    display_name = symbol.replace('_', ' ').replace('and', '&')
                    
                    print(f"{display_name:<25} {float(latest_price):<12.2f} {len(result):<12} {latest_time.strftime('%m-%d %H:%M:%S'):<20}")
                else:
                    display_name = symbol.replace('_', ' ').replace('and', '&')
                    print(f"{display_name:<25} {'No data':<12} {'0':<12} {'N/A':<20}")
                    
            except Exception as e:
                display_name = symbol.replace('_', ' ').replace('and', '&')
                print(f"{display_name:<25} {'Error':<12} {'Error':<12} {str(e)[:20]:<20}")
    
    def export_symbol_data(self, symbol_name, hours_back=24, output_file=None):
        """Export symbol data to CSV"""
        symbol_clean = symbol_name.replace(' ', '_').replace('&', 'and')
        
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"{symbol_clean}_data_{timestamp}.csv"
        
        print(f"\n📁 Exporting {symbol_name} data to {output_file}")
        
        # Get time range
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        
        # Collect all data
        all_data = {}
        
        for data_type in ['open', 'high', 'low', 'close', 'volume']:
            key = f"stock:{symbol_clean}:{data_type}"
            
            try:
                result = self.redis_client.execute_command('TS.RANGE', key, start_time, end_time)
                if result:
                    for ts, val in result:
                        dt = datetime.fromtimestamp(ts/1000, tz=IST)
                        if dt not in all_data:
                            all_data[dt] = {}
                        all_data[dt][data_type] = float(val)
            except Exception as e:
                print(f"  Error reading {data_type}: {e}")
        
        if not all_data:
            print("❌ No data to export")
            return
        
        # Create DataFrame
        df = pd.DataFrame.from_dict(all_data, orient='index')
        df.index.name = 'timestamp'
        df = df.sort_index()
        
        # Reorder columns
        column_order = ['open', 'high', 'low', 'close', 'volume']
        df = df.reindex(columns=column_order)
        
        # Save to CSV
        df.to_csv(output_file)
        print(f"✅ Exported {len(df)} rows to {output_file}")
        
        # Show sample
        print(f"\nSample data (last 5 rows):")
        print(df.tail().to_string())
        
        return output_file
    
    def check_data_continuity(self, symbol_name, hours_back=24):
        """Check data continuity and gaps"""
        symbol_clean = symbol_name.replace(' ', '_').replace('&', 'and')
        
        print(f"\n🔍 DATA CONTINUITY CHECK: {symbol_name}")
        print("=" * 50)
        
        # Get close price data (most reliable)
        key = f"stock:{symbol_clean}:close"
        
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        
        try:
            result = self.redis_client.execute_command('TS.RANGE', key, start_time, end_time)
            
            if not result:
                print("❌ No data found")
                return
            
            timestamps, values = zip(*result)
            
            # Convert to datetime
            datetimes = [datetime.fromtimestamp(ts/1000, tz=IST) for ts in timestamps]
            
            print(f"Total data points: {len(result)}")
            print(f"Time range: {datetimes[0]} to {datetimes[-1]}")
            print(f"Duration: {datetimes[-1] - datetimes[0]}")
            
            # Check for gaps
            gaps = []
            for i in range(1, len(datetimes)):
                time_diff = datetimes[i] - datetimes[i-1]
                if time_diff > timedelta(minutes=2):  # Gap larger than 2 minutes
                    gaps.append({
                        'start': datetimes[i-1],
                        'end': datetimes[i],
                        'duration': time_diff
                    })
            
            if gaps:
                print(f"\n⚠️  Found {len(gaps)} data gaps:")
                for gap in gaps[:10]:  # Show first 10 gaps
                    print(f"  {gap['start']} to {gap['end']} (Duration: {gap['duration']})")
                if len(gaps) > 10:
                    print(f"  ... and {len(gaps) - 10} more gaps")
            else:
                print("\n✅ No significant data gaps found")
            
            # Data frequency analysis
            intervals = []
            for i in range(1, min(100, len(datetimes))):  # Sample first 100 intervals
                intervals.append((datetimes[i] - datetimes[i-1]).total_seconds())
            
            if intervals:
                avg_interval = sum(intervals) / len(intervals)
                print(f"\nAverage data interval: {avg_interval:.1f} seconds")
                print(f"Expected for 1-minute data: 60 seconds")
                
                if avg_interval < 10:
                    print("📊 Looks like tick-by-tick data")
                elif 50 <= avg_interval <= 70:
                    print("📊 Looks like 1-minute OHLCV data")
                else:
                    print(f"📊 Custom interval: ~{avg_interval:.0f} seconds")
            
        except Exception as e:
            print(f"❌ Error checking continuity: {e}")
    
    def interactive_menu(self):
        """Interactive menu for database exploration"""
        overview = self.get_database_overview()
        
        while True:
            print(f"\n" + "="*50)
            print("🗃️  REDIS TRADING DATABASE EXPLORER")
            print("="*50)
            print("1. Database Overview")
            print("2. View Specific Symbol")
            print("3. View All Symbols Summary")
            print("4. Export Symbol Data")
            print("5. Check Data Continuity")
            print("6. Search Symbols")
            print("7. Database Statistics")
            print("0. Exit")
            print("-" * 50)
            
            choice = input("Enter your choice (0-7): ").strip()
            
            if choice == '0':
                print("👋 Goodbye!")
                break
            elif choice == '1':
                self.get_database_overview()
            elif choice == '2':
                if overview['symbols']:
                    print(f"\nAvailable symbols: {', '.join(overview['symbols'][:10])}")
                    if len(overview['symbols']) > 10:
                        print(f"... and {len(overview['symbols']) - 10} more")
                symbol = input("Enter symbol name: ").strip()
                if symbol:
                    hours = input("Hours to look back (default 24): ").strip()
                    hours = int(hours) if hours else 24
                    self.view_symbol_data(symbol, hours_back=hours)
            elif choice == '3':
                hours = input("Hours to look back (default 1): ").strip()
                hours = int(hours) if hours else 1
                self.view_all_symbols_summary(hours_back=hours)
            elif choice == '4':
                symbol = input("Enter symbol name to export: ").strip()
                if symbol:
                    hours = input("Hours to export (default 24): ").strip()
                    hours = int(hours) if hours else 24
                    self.export_symbol_data(symbol, hours_back=hours)
            elif choice == '5':
                symbol = input("Enter symbol name to check: ").strip()
                if symbol:
                    hours = input("Hours to check (default 24): ").strip()
                    hours = int(hours) if hours else 24
                    self.check_data_continuity(symbol, hours_back=hours)
            elif choice == '6':
                search_term = input("Enter search term: ").strip().lower()
                if search_term:
                    matches = [s for s in overview['symbols'] if search_term in s.lower()]
                    if matches:
                        print(f"Found {len(matches)} matching symbols:")
                        for match in matches:
                            print(f"  - {match.replace('_', ' ').replace('and', '&')}")
                    else:
                        print("No matching symbols found")
            elif choice == '7':
                self.database_statistics()
            else:
                print("❌ Invalid choice")
    
    def database_statistics(self):
        """Show detailed database statistics"""
        print(f"\n" + "="*60)
        print("📈 DATABASE STATISTICS")
        print("="*60)
        
        # Memory usage
        try:
            info = self.redis_client.info('memory')
            used_memory = info.get('used_memory_human', 'Unknown')
            print(f"Redis Memory Usage: {used_memory}")
        except:
            print("Memory info not available")
        
        # Key count by type
        all_keys = self.redis_client.keys("*")
        ts_keys = [k for k in all_keys if k.startswith('stock:') and not k.endswith(':metadata')]
        
        print(f"Total Keys: {len(all_keys)}")
        print(f"TimeSeries Keys: {len(ts_keys)}")
        
        if ts_keys:
            # Sample a few keys for detailed stats
            sample_keys = ts_keys[:5]
            print(f"\nSample TimeSeries Info:")
            
            for key in sample_keys:
                try:
                    info = self.redis_client.execute_command('TS.INFO', key)
                    # Parse info response
                    info_dict = {}
                    for i in range(0, len(info), 2):
                        if i + 1 < len(info):
                            info_dict[info[i].decode() if isinstance(info[i], bytes) else info[i]] = info[i+1]
                    
                    print(f"  {key}:")
                    print(f"    Samples: {info_dict.get('totalSamples', 'Unknown')}")
                    print(f"    Memory: {info_dict.get('memoryUsage', 'Unknown')} bytes")
                    
                except Exception as e:
                    print(f"  {key}: Error getting info - {e}")

def main():
    parser = argparse.ArgumentParser(description='Redis Trading Database Viewer')
    parser.add_argument('--host', default='localhost', help='Redis host')
    parser.add_argument('--port', type=int, default=6379, help='Redis port')
    parser.add_argument('--db', type=int, default=0, help='Redis database number')
    parser.add_argument('--symbol', help='View specific symbol')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back')
    parser.add_argument('--export', help='Export symbol data to CSV')
    parser.add_argument('--overview', action='store_true', help='Show database overview only')
    parser.add_argument('--summary', action='store_true', help='Show all symbols summary')
    
    args = parser.parse_args()
    
    # Initialize viewer
    viewer = TradingDataViewer(host=args.host, port=args.port, db=args.db)
    
    if args.overview:
        viewer.get_database_overview()
    elif args.summary:
        viewer.view_all_symbols_summary(hours_back=args.hours)
    elif args.symbol:
        if args.export:
            viewer.export_symbol_data(args.symbol, hours_back=args.hours, output_file=args.export)
        else:
            viewer.view_symbol_data(args.symbol, hours_back=args.hours)
    else:
        viewer.interactive_menu()

if __name__ == "__main__":
    main()
