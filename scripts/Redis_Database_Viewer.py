#!/usr/bin/env python3
"""
Redis Database Viewer - Inspect Market Analytics Data
View all TimeSeries data stored by websocket_client.py
"""

import redis
import json
import os
from datetime import datetime, timedelta
import pytz
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '1'))
SYMBOLS_PATH = os.getenv('CUPCAKE_SYMBOLS_PATH', '/home/abhishek/projects/CUPCAKE/scripts/symbols.json')

class RedisTimeSeriesViewer:
    """Redis TimeSeries database viewer and analyzer"""
    
    def __init__(self):
        self.redis_client = None
        self.symbol_mapping = {}
        self._connect_redis()
        self._load_symbols()
    
    def _connect_redis(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True
            )
            self.redis_client.ping()
            print(f"✅ Connected to Redis: {REDIS_HOST}:{REDIS_PORT} DB:{REDIS_DB}")
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            exit(1)
    
    def _load_symbols(self):
        """Load symbol mappings"""
        try:
            with open(SYMBOLS_PATH, 'r') as f:
                symbols_data = json.load(f)
            
            for symbol in symbols_data['symbols']:
                instrument_key = symbol['instrument_key']
                name = symbol['name']
                clean_name = name.replace(' ', '_').upper()
                self.symbol_mapping[clean_name] = name
            
            print(f"✅ Loaded {len(self.symbol_mapping)} symbol mappings")
        except Exception as e:
            print(f"⚠️ Could not load symbols: {e}")
    
    def list_all_timeseries_keys(self):
        """List all TimeSeries keys in the database"""
        print("\n" + "="*80)
        print("📊 ALL TIMESERIES KEYS IN DATABASE")
        print("="*80)
        
        try:
            # Get all keys matching TimeSeries pattern
            keys = self.redis_client.keys("ts:*")
            
            if not keys:
                print("❌ No TimeSeries keys found")
                return []
            
            # Group by metric type
            metrics = {}
            for key in keys:
                parts = key.split(":")
                if len(parts) >= 3:
                    metric = parts[1]
                    instrument = parts[2]
                    if metric not in metrics:
                        metrics[metric] = []
                    metrics[metric].append(instrument)
            
            print(f"📈 Found {len(keys)} TimeSeries keys for {len(metrics)} metrics:")
            
            for metric, instruments in metrics.items():
                print(f"\n🔸 {metric.upper()}:")
                for instrument in sorted(instruments):
                    # Get key info
                    key = f"ts:{metric}:{instrument}"
                    try:
                        info = self.redis_client.execute_command('TS.INFO', key)
                        total_samples = info[1]  # Total samples
                        print(f"   📊 {instrument}: {total_samples:,} data points")
                    except:
                        print(f"   📊 {instrument}: (info unavailable)")
            
            return keys
            
        except Exception as e:
            print(f"❌ Error listing keys: {e}")
            return []
    
    def show_recent_data(self, instrument: str, metric: str, count: int = 10):
        """Show recent data for specific instrument and metric"""
        key = f"ts:{metric}:{instrument}"
        
        try:
            # Get recent data points
            result = self.redis_client.execute_command('TS.RANGE', key, '-', '+', 'COUNT', count)
            
            if not result:
                print(f"❌ No data found for {key}")
                return
            
            print(f"\n📊 RECENT {metric.upper()} DATA FOR {instrument}")
            print("-" * 60)
            print(f"{'Timestamp':<20} {'Value':<15} {'Time':<15}")
            print("-" * 60)
            
            for timestamp_ms, value in result[-count:]:
                timestamp = int(timestamp_ms) / 1000
                dt = datetime.fromtimestamp(timestamp)
                time_str = dt.strftime("%H:%M:%S")
                print(f"{timestamp_ms:<20} {float(value):<15.4f} {time_str:<15}")
                
        except Exception as e:
            print(f"❌ Error getting data for {key}: {e}")
    
    def show_summary_statistics(self):
        """Show summary statistics for all instruments"""
        print("\n" + "="*100)
        print("📊 SUMMARY STATISTICS - ALL INSTRUMENTS")
        print("="*100)
        
        metrics_to_check = ['ltp', 'vwap_60s', 'volatility_5min', 'order_imbalance']
        
        for instrument in sorted(self.symbol_mapping.keys()):
            instrument_name = self.symbol_mapping.get(instrument, instrument)
            print(f"\n🎯 {instrument_name} ({instrument})")
            print("-" * 80)
            
            for metric in metrics_to_check:
                key = f"ts:{metric}:{instrument}"
                try:
                    # Get last 100 data points
                    result = self.redis_client.execute_command('TS.RANGE', key, '-', '+', 'COUNT', 100)
                    
                    if result:
                        values = [float(value) for _, value in result]
                        if values:
                            latest = values[-1]
                            avg = sum(values) / len(values)
                            min_val = min(values)
                            max_val = max(values)
                            
                            print(f"   📈 {metric:<15}: Latest={latest:>8.4f} | "
                                  f"Avg={avg:>8.4f} | Min={min_val:>8.4f} | Max={max_val:>8.4f} | "
                                  f"Points={len(values):>3}")
                        else:
                            print(f"   📈 {metric:<15}: No valid data")
                    else:
                        print(f"   📈 {metric:<15}: No data")
                        
                except Exception as e:
                    print(f"   📈 {metric:<15}: Error - {e}")
    
    def export_data_to_csv(self, instrument: str, metric: str, hours: int = 1):
        """Export TimeSeries data to CSV"""
        key = f"ts:{metric}:{instrument}"
        
        try:
            # Calculate timestamp range
            now = int(datetime.now().timestamp() * 1000)
            start_time = now - (hours * 60 * 60 * 1000)
            
            # Get data
            result = self.redis_client.execute_command('TS.RANGE', key, start_time, now)
            
            if not result:
                print(f"❌ No data found for {key}")
                return
            
            # Create DataFrame
            data = []
            for timestamp_ms, value in result:
                timestamp = int(timestamp_ms) / 1000
                dt = datetime.fromtimestamp(timestamp)
                data.append({
                    'timestamp': dt,
                    'timestamp_ms': timestamp_ms,
                    'value': float(value),
                    'instrument': instrument,
                    'metric': metric
                })
            
            df = pd.DataFrame(data)
            
            # Export to CSV
            filename = f"market_data_{instrument}_{metric}_{hours}h.csv"
            df.to_csv(filename, index=False)
            
            print(f"✅ Exported {len(data)} data points to {filename}")
            print(f"📊 Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            return filename
            
        except Exception as e:
            print(f"❌ Error exporting data: {e}")
    
    def create_live_dashboard(self, instrument: str = "TATA_CONSULTANCY_SERVICES"):
        """Create live dashboard for an instrument"""
        print(f"\n📊 CREATING LIVE DASHBOARD FOR {instrument}")
        print("="*60)
        
        metrics = ['ltp', 'vwap_60s', 'volatility_5min', 'order_imbalance']
        
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(f'Live Market Analytics - {instrument}', fontsize=16)
            
            for i, metric in enumerate(metrics):
                ax = axes[i//2, i%2]
                key = f"ts:{metric}:{instrument}"
                
                try:
                    # Get last hour of data
                    now = int(datetime.now().timestamp() * 1000)
                    start_time = now - (60 * 60 * 1000)  # 1 hour ago
                    
                    result = self.redis_client.execute_command('TS.RANGE', key, start_time, now)
                    
                    if result:
                        timestamps = [int(ts)/1000 for ts, _ in result]
                        values = [float(val) for _, val in result]
                        
                        # Convert to datetime
                        dt_timestamps = [datetime.fromtimestamp(ts) for ts in timestamps]
                        
                        ax.plot(dt_timestamps, values, linewidth=1.5)
                        ax.set_title(f'{metric.replace("_", " ").title()}')
                        ax.set_xlabel('Time')
                        ax.set_ylabel('Value')
                        ax.grid(True, alpha=0.3)
                        
                        # Format x-axis
                        ax.tick_params(axis='x', rotation=45)
                        
                        # Add latest value annotation
                        if values:
                            ax.annotate(f'Latest: {values[-1]:.4f}', 
                                      xy=(0.02, 0.98), xycoords='axes fraction',
                                      bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))
                    else:
                        ax.text(0.5, 0.5, 'No Data Available', 
                               ha='center', va='center', transform=ax.transAxes)
                        ax.set_title(f'{metric.replace("_", " ").title()} - No Data')
                
                except Exception as e:
                    ax.text(0.5, 0.5, f'Error: {str(e)[:50]}...', 
                           ha='center', va='center', transform=ax.transAxes)
                    ax.set_title(f'{metric.replace("_", " ").title()} - Error')
            
            plt.tight_layout()
            
            # Save dashboard
            filename = f"live_dashboard_{instrument}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"✅ Dashboard saved as {filename}")
            
            # Show if possible
            try:
                plt.show()
            except:
                print("💡 Dashboard image saved. View the PNG file to see charts.")
            
        except Exception as e:
            print(f"❌ Error creating dashboard: {e}")
    
    def monitor_live_updates(self, duration_seconds: int = 30):
        """Monitor live updates to the database"""
        print(f"\n📡 MONITORING LIVE UPDATES FOR {duration_seconds} SECONDS")
        print("="*60)
        
        # Get initial counts
        initial_counts = {}
        keys = self.redis_client.keys("ts:*")
        
        for key in keys:
            try:
                info = self.redis_client.execute_command('TS.INFO', key)
                initial_counts[key] = info[1]  # Total samples
            except:
                initial_counts[key] = 0
        
        print(f"⏰ Starting monitoring... (Ctrl+C to stop early)")
        
        import time
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration_seconds:
                time.sleep(5)  # Check every 5 seconds
                
                current_time = datetime.now().strftime("%H:%M:%S")
                print(f"\n🕐 {current_time} - Checking for updates...")
                
                updates = 0
                for key in keys:
                    try:
                        info = self.redis_client.execute_command('TS.INFO', key)
                        current_count = info[1]
                        
                        if current_count > initial_counts[key]:
                            new_points = current_count - initial_counts[key]
                            print(f"   📊 {key}: +{new_points} new data points (total: {current_count:,})")
                            initial_counts[key] = current_count
                            updates += 1
                    except:
                        pass
                
                if updates == 0:
                    print("   😴 No new updates detected")
                else:
                    print(f"   🔥 {updates} keys updated!")
                    
        except KeyboardInterrupt:
            print("\n👋 Monitoring stopped by user")
        
        print(f"\n✅ Monitoring completed")

def main():
    """Main menu system"""
    viewer = RedisTimeSeriesViewer()
    
    while True:
        print("\n" + "="*80)
        print("🗃️  REDIS TIMESERIES DATABASE VIEWER")
        print("="*80)
        print("1. 📋 List all TimeSeries keys")
        print("2. 📊 Show recent data for specific instrument/metric")
        print("3. 📈 Summary statistics for all instruments")
        print("4. 💾 Export data to CSV")
        print("5. 📊 Create live dashboard")
        print("6. 📡 Monitor live updates")
        print("7. 🔍 Quick TCS overview")
        print("8. ❌ Exit")
        print("="*80)
        
        try:
            choice = input("Select option (1-8): ").strip()
            
            if choice == "1":
                viewer.list_all_timeseries_keys()
            
            elif choice == "2":
                print("\nAvailable instruments:")
                for i, (key, name) in enumerate(viewer.symbol_mapping.items(), 1):
                    print(f"  {i}. {name} ({key})")
                
                instrument = input("\nEnter instrument name (e.g., TATA_CONSULTANCY_SERVICES): ").strip()
                metric = input("Enter metric (e.g., ltp, vwap_60s, volatility_5min): ").strip()
                count = int(input("Number of recent points (default 10): ") or "10")
                
                viewer.show_recent_data(instrument, metric, count)
            
            elif choice == "3":
                viewer.show_summary_statistics()
            
            elif choice == "4":
                instrument = input("Enter instrument name: ").strip()
                metric = input("Enter metric: ").strip()
                hours = int(input("Hours of data (default 1): ") or "1")
                
                viewer.export_data_to_csv(instrument, metric, hours)
            
            elif choice == "5":
                instrument = input("Enter instrument (default TATA_CONSULTANCY_SERVICES): ").strip()
                if not instrument:
                    instrument = "TATA_CONSULTANCY_SERVICES"
                
                viewer.create_live_dashboard(instrument)
            
            elif choice == "6":
                duration = int(input("Monitor duration in seconds (default 30): ") or "30")
                viewer.monitor_live_updates(duration)
            
            elif choice == "7":
                # Quick TCS overview
                print("\n🎯 QUICK TCS OVERVIEW")
                print("="*50)
                viewer.show_recent_data("TATA_CONSULTANCY_SERVICES", "ltp", 5)
                viewer.show_recent_data("TATA_CONSULTANCY_SERVICES", "vwap_60s", 5)
                viewer.show_recent_data("TATA_CONSULTANCY_SERVICES", "volatility_5min", 5)
            
            elif choice == "8":
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice. Please select 1-8.")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    # Check dependencies
    try:
        import redis
        import pandas as pd
        import matplotlib.pyplot as plt
        print("✅ All database viewer libraries ready")
    except ImportError as e:
        print(f"❌ Missing library: {e}")
        print("🔧 Install: pip install redis pandas matplotlib seaborn")
        exit(1)
    
    main()