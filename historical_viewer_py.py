#!/usr/bin/env python3
"""
Historical Data Viewer - Integrated with Trading System
Shows all stored historical data from Redis, works anytime
"""

import redis
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
import sys
import os

# Get script directory for consistent paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

class HistoricalDataViewer:
    def __init__(self, host='localhost', port=6379, db=0):
        """Initialize Redis connection"""
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
            print("💡 Make sure Redis is running: sudo systemctl start redis")
            sys.exit(1)
    
    def get_database_status(self):
        """Get complete status of historical data in database"""
        all_keys = self.redis_client.keys("*")
        stock_ts_keys = [k for k in all_keys if k.startswith('stock:') and not k.endswith(':metadata')]
        metadata_keys = [k for k in all_keys if k.endswith(':metadata')]
        
        if not stock_ts_keys:
            return {
                'has_data': False,
                'total_keys': len(all_keys),
                'ts_keys': 0,
                'metadata_keys': len(metadata_keys),
                'symbols': [],
                'data_types': []
            }
        
        # Extract symbols and data types
        symbols = set()
        data_types = set()
        
        for key in stock_ts_keys:
            parts = key.split(':')
            if len(parts) >= 3:
                symbol = parts[1]
                data_type = parts[2]
                symbols.add(symbol)
                data_types.add(data_type)
        
        return {
            'has_data': True,
            'total_keys': len(all_keys),
            'ts_keys': len(stock_ts_keys),
            'metadata_keys': len(metadata_keys),
            'symbols': sorted(list(symbols)),
            'data_types': sorted(list(data_types)),
            'symbol_count': len(symbols)
        }
    
    def get_symbols_summary(self):
        """Get summary of all symbols with data ranges"""
        stock_keys = self.redis_client.keys("stock:*:close")
        symbols_data = []
        
        for key in stock_keys:
            symbol_clean = key.split(':')[1]
            
            try:
                # Get ALL data (no time filter)
                result = self.redis_client.execute_command('TS.RANGE', key, '-', '+')
                
                if result and len(result) > 0:
                    # Get first and last data points
                    first_ts, first_price = result[0]
                    last_ts, last_price = result[-1]
                    
                    first_time = datetime.fromtimestamp(first_ts/1000, tz=IST)
                    last_time = datetime.fromtimestamp(last_ts/1000, tz=IST)
                    
                    # Get price range
                    prices = [float(price) for _, price in result]
                    min_price = min(prices)
                    max_price = max(prices)
                    
                    # Convert symbol back to readable format
                    display_name = symbol_clean.replace('_', ' ').replace('and', '&')
                    
                    symbols_data.append({
                        'symbol_clean': symbol_clean,
                        'display_name': display_name,
                        'data_points': len(result),
                        'first_time': first_time,
                        'last_time': last_time,
                        'first_price': float(first_price),
                        'last_price': float(last_price),
                        'min_price': min_price,
                        'max_price': max_price,
                        'total_return_pct': ((float(last_price) - float(first_price)) / float(first_price)) * 100,
                        'duration_hours': (last_time - first_time).total_seconds() / 3600
                    })
                    
            except Exception as e:
                display_name = symbol_clean.replace('_', ' ').replace('and', '&')
                symbols_data.append({
                    'symbol_clean': symbol_clean,
                    'display_name': display_name,
                    'error': str(e),
                    'data_points': 0
                })
        
        return sorted(symbols_data, key=lambda x: x.get('data_points', 0), reverse=True)
    
    def get_symbol_complete_data(self, symbol_name):
        """Get complete historical data for a symbol"""
        symbol_clean = symbol_name.replace(' ', '_').replace('&', 'and')
        
        # Check if symbol exists
        key = f"stock:{symbol_clean}:close"
        if not self.redis_client.exists(key):
            return None
        
        all_data = {}
        data_summary = {}
        
        for data_type in ['open', 'high', 'low', 'close', 'volume']:
            ts_key = f"stock:{symbol_clean}:{data_type}"
            
            try:
                # Get ALL data (no time restrictions)
                result = self.redis_client.execute_command('TS.RANGE', ts_key, '-', '+')
                
                if result:
                    # Store data
                    for ts, val in result:
                        dt = datetime.fromtimestamp(ts/1000, tz=IST)
                        if dt not in all_data:
                            all_data[dt] = {}
                        all_data[dt][data_type] = float(val)
                    
                    # Summary stats
                    first_ts, first_val = result[0]
                    last_ts, last_val = result[-1]
                    values = [float(v) for _, v in result]
                    
                    data_summary[data_type] = {
                        'total_points': len(result),
                        'first_time': datetime.fromtimestamp(first_ts/1000, tz=IST),
                        'last_time': datetime.fromtimestamp(last_ts/1000, tz=IST),
                        'first_value': float(first_val),
                        'last_value': float(last_val),
                        'min_value': min(values),
                        'max_value': max(values),
                        'avg_value': sum(values) / len(values)
                    }
                    
            except Exception as e:
                data_summary[data_type] = {'error': str(e)}
        
        if not all_data:
            return None
        
        # Create DataFrame
        df = pd.DataFrame.from_dict(all_data, orient='index')
        df.index.name = 'timestamp'
        df = df.sort_index()
        
        # Get metadata
        metadata = {}
        metadata_key = f"stock:{symbol_clean}:metadata"
        if self.redis_client.exists(metadata_key):
            try:
                metadata_raw = self.redis_client.hget(metadata_key, 'data')
                if metadata_raw:
                    metadata = json.loads(metadata_raw)
            except:
                pass
        
        return {
            'symbol_name': symbol_name,
            'symbol_clean': symbol_clean,
            'dataframe': df,
            'summary': data_summary,
            'metadata': metadata,
            'total_timepoints': len(df),
            'date_range': {
                'start': df.index[0] if len(df) > 0 else None,
                'end': df.index[-1] if len(df) > 0 else None,
                'duration': df.index[-1] - df.index[0] if len(df) > 0 else None
            }
        }
    
    def export_symbol_to_csv(self, symbol_name, output_dir="exported_data"):
        """Export symbol data to CSV"""
        os.makedirs(output_dir, exist_ok=True)
        
        data = self.get_symbol_complete_data(symbol_name)
        if not data:
            return None
        
        df = data['dataframe']
        
        # Reorder columns
        column_order = ['open', 'high', 'low', 'close', 'volume']
        df = df.reindex(columns=[col for col in column_order if col in df.columns])
        
        # Create filename
        safe_filename = data['symbol_clean'].replace('_', '-')
        filename = f"{safe_filename}_historical_data.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Save to CSV
        df.to_csv(filepath)
        
        return {
            'filepath': filepath,
            'filename': filename,
            'rows': len(df),
            'symbol_name': symbol_name
        }
    
    def export_all_symbols(self, output_dir="exported_data"):
        """Export all symbols to CSV files"""
        os.makedirs(output_dir, exist_ok=True)
        
        symbols_data = self.get_symbols_summary()
        exported_files = []
        
        for symbol_info in symbols_data:
            if symbol_info.get('data_points', 0) > 0:
                try:
                    result = self.export_symbol_to_csv(
                        symbol_info['display_name'], 
                        output_dir
                    )
                    if result:
                        exported_files.append(result)
                except Exception as e:
                    print(f"❌ Export failed for {symbol_info['display_name']}: {e}")
        
        return exported_files
    
    def get_available_symbols(self):
        """Get list of available symbols"""
        stock_keys = self.redis_client.keys("stock:*:close")
        symbols = []
        
        for key in stock_keys:
            symbol_clean = key.split(':')[1]
            readable_name = symbol_clean.replace('_', ' ').replace('and', '&')
            symbols.append(readable_name)
        
        return sorted(symbols)
    
    def show_interactive_menu(self):
        """Interactive command-line menu"""
        while True:
            status = self.get_database_status()
            
            print(f"\n" + "="*70)
            print("📊 HISTORICAL DATA VIEWER - CUPCAKE TRADING SYSTEM")
            print("="*70)
            print(f"🕐 Current time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
            
            if not status['has_data']:
                print("\n❌ No historical data found in database")
                print("💡 Run the historical data fetcher first:")
                print("   python historical_data_fetcher.py")
                break
            
            print(f"\n📊 Database Status:")
            print(f"   💾 Total keys: {status['total_keys']}")
            print(f"   📈 Symbols with data: {status['symbol_count']}")
            print(f"   📋 Data types: {', '.join(status['data_types'])}")
            
            print(f"\n" + "="*50)
            print("🗂️  HISTORICAL DATA EXPLORER")
            print("="*50)
            print("1. Show all symbols with data ranges")
            print("2. View complete history for specific symbol")
            print("3. Export specific symbol to CSV")
            print("4. Export all symbols to CSV")
            print("5. Search symbols")
            print("6. Quick symbol lookup")
            print("0. Exit")
            print("-" * 50)
            
            choice = input("Enter your choice (0-6): ").strip()
            
            if choice == '0':
                print("👋 Goodbye!")
                break
            elif choice == '1':
                self.show_all_symbols_summary()
            elif choice == '2':
                self.show_symbol_selection_and_details()
            elif choice == '3':
                self.show_export_symbol_menu()
            elif choice == '4':
                self.show_export_all_menu()
            elif choice == '5':
                self.show_search_menu()
            elif choice == '6':
                self.show_quick_lookup()
            else:
                print("❌ Invalid choice")
    
    def show_all_symbols_summary(self):
        """Display all symbols with data ranges"""
        print("\n" + "="*100)
        print("📈 ALL SYMBOLS - COMPLETE DATA RANGE")
        print("="*100)
        
        symbols_data = self.get_symbols_summary()
        
        if not symbols_data:
            print("❌ No symbols found")
            return
        
        print(f"Found {len(symbols_data)} symbols with historical data")
        print("-" * 100)
        print(f"{'#':<3} {'Symbol':<30} {'Points':<8} {'Oldest Data':<17} {'Latest Data':<17} {'Duration':<10} {'Return %':<10}")
        print("-" * 100)
        
        for i, symbol in enumerate(symbols_data, 1):
            if 'error' in symbol:
                print(f"{i:<3} {symbol['display_name']:<30} {'Error':<8} {'N/A':<17} {'N/A':<17} {'N/A':<10} {'N/A':<10}")
            else:
                duration = f"{symbol['duration_hours']:.1f}h"
                return_str = f"{symbol['total_return_pct']:+.2f}%"
                
                print(f"{i:<3} {symbol['display_name']:<30} "
                     f"{symbol['data_points']:<8} "
                     f"{symbol['first_time'].strftime('%m-%d %H:%M'):<17} "
                     f"{symbol['last_time'].strftime('%m-%d %H:%M'):<17} "
                     f"{duration:<10} "
                     f"{return_str:<10}")
    
    def show_symbol_selection_and_details(self):
        """Show symbol selection and detailed view"""
        symbols = self.get_available_symbols()
        if not symbols:
            print("❌ No symbols available")
            return
        
        print(f"\n📈 Available symbols ({len(symbols)}):")
        for i, sym in enumerate(symbols, 1):
            print(f"  {i:2d}. {sym}")
        
        symbol_input = input(f"\nEnter symbol name or number (1-{len(symbols)}): ").strip()
        
        # Check if it's a number
        try:
            symbol_idx = int(symbol_input) - 1
            if 0 <= symbol_idx < len(symbols):
                symbol = symbols[symbol_idx]
            else:
                print("❌ Invalid number")
                return
        except ValueError:
            symbol = symbol_input
        
        if symbol:
            self.show_symbol_details(symbol)
    
    def show_symbol_details(self, symbol_name):
        """Show detailed view of a symbol"""
        print(f"\n📈 Loading complete history for: {symbol_name}")
        
        data = self.get_symbol_complete_data(symbol_name)
        if not data:
            print(f"❌ No data found for: {symbol_name}")
            return
        
        print(f"\n" + "="*80)
        print(f"📈 COMPLETE HISTORY: {symbol_name}")
        print("="*80)
        
        df = data['dataframe']
        summary = data['summary']
        
        print(f"📊 Data Overview:")
        print(f"   Total timepoints: {data['total_timepoints']:,}")
        if data['date_range']['start']:
            print(f"   Date range: {data['date_range']['start'].strftime('%Y-%m-%d %H:%M')} to "
                 f"{data['date_range']['end'].strftime('%Y-%m-%d %H:%M')}")
            print(f"   Duration: {data['date_range']['duration']}")
        
        # Show data type summaries
        for data_type in ['close', 'volume']:
            if data_type in summary and 'error' not in summary[data_type]:
                info = summary[data_type]
                print(f"\n📊 {data_type.upper()} Data:")
                print(f"   Points: {info['total_points']:,}")
                print(f"   Range: {info['min_value']:.2f} - {info['max_value']:.2f}")
                print(f"   Latest: {info['last_value']:.2f}")
                
                if data_type == 'close' and info['total_points'] > 1:
                    total_return = ((info['last_value'] - info['first_value']) / info['first_value']) * 100
                    print(f"   Total return: {total_return:+.2f}%")
        
        # Show recent data
        print(f"\n📋 Recent Data (Last 10 points):")
        recent_data = df.tail(10)
        for timestamp, row in recent_data.iterrows():
            timestamp_str = timestamp.strftime('%m-%d %H:%M')
            if pd.notna(row.get('close')):
                vol_str = f", Vol={int(row['volume']):,}" if pd.notna(row.get('volume')) else ""
                print(f"   {timestamp_str}: ₹{row['close']:.2f}{vol_str}")
        
        # Show metadata if available
        if data['metadata']:
            print(f"\n📋 Metadata:")
            metadata = data['metadata']
            print(f"   Source: {metadata.get('source', 'Unknown')}")
            print(f"   Fetched: {metadata.get('fetched_at', 'Unknown')}")
            print(f"   Strategies used: {metadata.get('strategies', 'Unknown')}")
        
        # Offer export
        export_choice = input(f"\n💾 Export {symbol_name} to CSV? (y/n): ").strip().lower()
        if export_choice in ['y', 'yes']:
            result = self.export_symbol_to_csv(symbol_name)
            if result:
                print(f"✅ Exported {result['rows']} rows to: {result['filepath']}")
    
    def show_export_symbol_menu(self):
        """Show export menu for specific symbol"""
        symbols = self.get_available_symbols()
        if not symbols:
            print("❌ No symbols available")
            return
        
        print(f"\n💾 EXPORT SYMBOL TO CSV")
        print(f"Available symbols ({len(symbols)}):")
        for i, sym in enumerate(symbols[:10], 1):
            print(f"  {i:2d}. {sym}")
        if len(symbols) > 10:
            print(f"  ... and {len(symbols) - 10} more")
        
        symbol_input = input(f"\nEnter symbol name or number: ").strip()
        
        # Check if it's a number
        try:
            symbol_idx = int(symbol_input) - 1
            if 0 <= symbol_idx < len(symbols):
                symbol = symbols[symbol_idx]
            else:
                print("❌ Invalid number")
                return
        except ValueError:
            symbol = symbol_input
        
        if symbol:
            output_dir = input("Output directory (default: exported_data): ").strip()
            if not output_dir:
                output_dir = "exported_data"
            
            result = self.export_symbol_to_csv(symbol, output_dir)
            if result:
                print(f"✅ Exported {result['rows']} rows to: {result['filepath']}")
            else:
                print(f"❌ Export failed for: {symbol}")
    
    def show_export_all_menu(self):
        """Show export all symbols menu"""
        print(f"\n💾 EXPORT ALL SYMBOLS TO CSV")
        
        output_dir = input("Output directory (default: exported_data): ").strip()
        if not output_dir:
            output_dir = "exported_data"
        
        confirm = input(f"Export all symbols to '{output_dir}'? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("❌ Export cancelled")
            return
        
        print("📊 Exporting all symbols...")
        exported_files = self.export_all_symbols(output_dir)
        
        if exported_files:
            print(f"\n✅ Export complete!")
            print(f"📁 {len(exported_files)} files saved to: {os.path.abspath(output_dir)}")
            
            total_rows = sum(f['rows'] for f in exported_files)
            print(f"📊 Total data points exported: {total_rows:,}")
            
            # Show first few files
            print(f"\nSample exported files:")
            for f in exported_files[:5]:
                print(f"  - {f['filename']} ({f['rows']:,} rows)")
            if len(exported_files) > 5:
                print(f"  ... and {len(exported_files) - 5} more files")
        else:
            print("❌ No files were exported")
    
    def show_search_menu(self):
        """Show search symbols menu"""
        search_term = input("Enter search term: ").strip().lower()
        if not search_term:
            return
        
        symbols = self.get_available_symbols()
        matches = [s for s in symbols if search_term in s.lower()]
        
        if matches:
            print(f"\n🔍 Found {len(matches)} matching symbols:")
            for i, match in enumerate(matches, 1):
                print(f"  {i:2d}. {match}")
            
            if len(matches) == 1:
                view_choice = input(f"\nView details for '{matches[0]}'? (y/n): ").strip().lower()
                if view_choice in ['y', 'yes']:
                    self.show_symbol_details(matches[0])
        else:
            print(f"❌ No symbols found matching: {search_term}")
    
    def show_quick_lookup(self):
        """Quick symbol lookup and basic info"""
        symbol_input = input("Enter symbol name: ").strip()
        if not symbol_input:
            return
        
        data = self.get_symbol_complete_data(symbol_input)
        if not data:
            print(f"❌ No data found for: {symbol_input}")
            return
        
        df = data['dataframe']
        if 'close' in df.columns:
            close_data = df['close'].dropna()
            if len(close_data) > 0:
                print(f"\n📊 Quick Info: {symbol_input}")
                print(f"   Data points: {len(close_data):,}")
                print(f"   Latest price: ₹{close_data.iloc[-1]:.2f}")
                print(f"   Price range: ₹{close_data.min():.2f} - ₹{close_data.max():.2f}")
                print(f"   Last update: {df.index[-1].strftime('%Y-%m-%d %H:%M:%S')}")
                
                if len(close_data) > 1:
                    total_return = ((close_data.iloc[-1] - close_data.iloc[0]) / close_data.iloc[0]) * 100
                    print(f"   Total return: {total_return:+.2f}%")

def main():
    """Main entry point"""
    print("🚀 CUPCAKE TRADING SYSTEM - HISTORICAL DATA VIEWER")
    print("⏰ Access your complete trading history anytime!")
    print("📊 All stored historical data from Redis TimeSeries")
    
    viewer = HistoricalDataViewer()
    viewer.show_interactive_menu()

if __name__ == "__main__":
    main()
