import os
import sys
import threading
import asyncio
import argparse
import pandas as pd
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO
from flask_cors import CORS
from datetime import datetime, timedelta

# Fix working directory and path issues
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)  # Ensure we're in the correct directory
sys.path.insert(0, SCRIPT_DIR)  # Add script directory to Python path

print(f"🔧 Working directory: {SCRIPT_DIR}")

# --- Import Fixed Modules ---
try:
    import enhanced_live_fetcher
    from enhanced_live_fetcher import fetch_enhanced_market_data
    print("✓ Using enhanced live fetcher")
except ImportError as e:
    print(f"Enhanced live fetcher import error: {e}")
    # Create a fallback if needed
    enhanced_live_fetcher = None

# Import the optimized historical data fetcher
try:
    from historical_data_fetcher import UltraFastHistoricalFetcher
    print("✓ Using ultra-fast historical data fetcher")
except ImportError as e:
    print(f"Historical fetcher import error: {e}")
    UltraFastHistoricalFetcher = None

# Import the new historical viewer
try:
    from historical_viewer import HistoricalDataViewer
    print("✓ Historical data viewer loaded")
except ImportError as e:
    print(f"Historical viewer import error: {e}")
    HistoricalDataViewer = None

from upstox_client import UpstoxAuthClient
from portfolio_tracker import PortfolioTracker

# --- Configuration ---
CREDENTIALS_PATH = os.path.join(SCRIPT_DIR, 'credentials.json')
SYMBOLS_PATH = os.path.join(SCRIPT_DIR, 'symbols.json')

class Config:
    """Simple configuration class for shared settings."""
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

# --- Main Application Setup with Fixed Paths ---
# Create templates directory if it doesn't exist
templates_dir = os.path.join(SCRIPT_DIR, 'templates')
if not os.path.exists(templates_dir):
    os.makedirs(templates_dir)
    print(f"📁 Created templates directory: {templates_dir}")

# Initialize Flask with explicit paths
try:
    app = Flask(__name__, 
                template_folder=templates_dir,
                static_folder=os.path.join(SCRIPT_DIR, 'static'),
                instance_path=SCRIPT_DIR)
    print("✓ Flask app initialized successfully")
except Exception as e:
    print(f"❌ Flask initialization error: {e}")
    # Fallback initialization
    app = Flask('cupcake_trading_system')
    print("✓ Flask app initialized with fallback method")

CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# --- Set SocketIO reference for live fetcher ---
if enhanced_live_fetcher:
    enhanced_live_fetcher.socketio = socketio

# --- Initialize Historical Viewer ---
historical_viewer = None
if HistoricalDataViewer:
    try:
        historical_viewer = HistoricalDataViewer()
        print("✓ Historical data viewer initialized")
    except Exception as e:
        print(f"⚠ Historical viewer initialization warning: {e}")
        historical_viewer = None

# --- Orchestration Functions ---

def run_enhanced_live_data_feed():
    """
    Starts the enhanced live data fetcher with actionable signal generation.
    """
    print("🚀 Starting Enhanced Live Data Feed with Actionable Signal Analysis...")
    if not enhanced_live_fetcher:
        print("❌ Enhanced live fetcher not available")
        return
        
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(fetch_enhanced_market_data())
    finally:
        loop.close()

def run_portfolio_tracker():
    """
    Starts the portfolio tracking service.
    """
    print("📈 Starting Portfolio Tracker...")
    tracker = PortfolioTracker(CREDENTIALS_PATH, socketio_instance=socketio)
    tracker.start_portfolio_tracking()

def run_historical_backfill():
    """
    Runs the historical data backfill process using ultra-fast fetcher.
    """
    print("⏳ Starting Ultra-Fast Historical Data Backfill...")
    
    if not UltraFastHistoricalFetcher:
        print("❌ Historical data fetcher not available")
        return
    
    try:
        # Initialize the ultra-fast fetcher
        fetcher = UltraFastHistoricalFetcher(CREDENTIALS_PATH, SYMBOLS_PATH)
        
        # Clear Redis and fetch data
        fetcher.redis.flush_and_prepare()
        
        # Run the ultra-fast fetch
        result = fetcher.process_all_symbols_ultra_fast(max_workers=8)
        
        if result.get('success'):
            stats = result['stats']
            print(f"   ✅ Historical backfill completed in {stats['total_time']}")
            print(f"   📊 {stats['symbols_successful']}/{stats['symbols_processed']} symbols successful")
            print(f"   🚀 {stats['total_candles']:,} candles stored at {stats['candles_per_second']:.0f} candles/sec")
        else:
            print(f"   ❌ Historical backfill failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error during ultra-fast historical backfill: {e}")

def run_historical_viewer_cli():
    """
    Run the historical data viewer in CLI mode
    """
    print("📊 Starting Historical Data Viewer...")
    if not historical_viewer:
        print("❌ Historical viewer not available")
        return
    
    historical_viewer.show_interactive_menu()

# --- Web Routes ---
@app.route('/')
def dashboard():
    """
    Serves the enhanced dashboard HTML file from the templates folder.
    """
    try:
        return render_template('enhanced_dashboard.html')
    except Exception as e:
        print(f"⚠ Template error: {e}")
        # Fallback: Return a basic HTML page
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Cupcake Trading Platform</title>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <script src="https://cdn.tailwindcss.com"></script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.js"></script>
        </head>
        <body class="bg-gray-900 text-white">
            <div class="container mx-auto p-8">
                <h1 class="text-4xl font-bold mb-4">🧁 Cupcake Trading Platform</h1>
                <div class="bg-blue-900 p-4 rounded-lg mb-4">
                    <p class="text-lg">✅ System is running successfully!</p>
                    <p class="text-sm text-gray-300">Dashboard template not found, using fallback interface.</p>
                </div>
                
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                    <div class="bg-gray-800 p-4 rounded-lg">
                        <h3 class="text-xl font-bold mb-2">🚀 System Status</h3>
                        <div id="system-status">
                            <p>Loading system status...</p>
                        </div>
                    </div>
                    
                    <div class="bg-gray-800 p-4 rounded-lg">
                        <h3 class="text-xl font-bold mb-2">📊 Market Data</h3>
                        <div id="market-status">
                            <p>Connecting to market data...</p>
                        </div>
                    </div>
                </div>
                
                <div class="bg-gray-800 p-4 rounded-lg mb-4">
                    <h3 class="text-xl font-bold mb-2">📊 Historical Data</h3>
                    <div id="historical-status">
                        <p>Loading historical data status...</p>
                    </div>
                    <a href="/historical" class="inline-block mt-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded">
                        View Historical Data
                    </a>
                </div>
                
                <div class="bg-gray-800 p-4 rounded-lg">
                    <h3 class="text-xl font-bold mb-2">📝 System Logs</h3>
                    <div id="logs" class="bg-black p-2 rounded text-green-400 font-mono text-sm max-h-64 overflow-y-auto">
                        <p>System initialized...</p>
                    </div>
                </div>
            </div>
            
            <script>
                const socket = io();
                
                socket.on('connect', () => {
                    document.getElementById('system-status').innerHTML = 
                        '<p class="text-green-400">✅ Connected to server</p>';
                    addLog('✅ Connected to trading system');
                });
                
                socket.on('disconnect', () => {
                    document.getElementById('system-status').innerHTML = 
                        '<p class="text-red-400">❌ Disconnected from server</p>';
                    addLog('❌ Disconnected from trading system');
                });
                
                socket.on('enhanced_market_update', (data) => {
                    const symbolCount = Object.keys(data).length;
                    document.getElementById('market-status').innerHTML = 
                        `<p class="text-blue-400">📈 Receiving data for ${symbolCount} symbols</p>`;
                    addLog(`📊 Market update: ${symbolCount} symbols`);
                });
                
                socket.on('debug_log', (data) => {
                    addLog(`[${data.level}] ${data.message}`);
                });
                
                function addLog(message) {
                    const logs = document.getElementById('logs');
                    const timestamp = new Date().toLocaleTimeString();
                    logs.innerHTML += `<p>[${timestamp}] ${message}</p>`;
                    logs.scrollTop = logs.scrollHeight;
                }
                
                // Load historical data status
                fetch('/api/historical/status')
                    .then(response => response.json())
                    .then(data => {
                        const statusElement = document.getElementById('historical-status');
                        if (data.has_data) {
                            statusElement.innerHTML = `
                                <p class="text-green-400">✅ ${data.symbol_count} symbols with historical data</p>
                                <p class="text-sm text-gray-400">Total data points: ${data.ts_keys.toLocaleString()}</p>
                            `;
                        } else {
                            statusElement.innerHTML = `
                                <p class="text-red-400">❌ No historical data found</p>
                                <p class="text-sm text-gray-400">Run historical backfill first</p>
                            `;
                        }
                    })
                    .catch(err => {
                        document.getElementById('historical-status').innerHTML = 
                            '<p class="text-red-400">❌ Error loading historical status</p>';
                    });
                
                // Check system status periodically
                setInterval(() => {
                    fetch('/api/system_status')
                        .then(response => response.json())
                        .then(data => {
                            addLog(`🔧 System check: ${data.status}`);
                        })
                        .catch(err => addLog(`⚠ System check failed: ${err}`));
                }, 30000);
            </script>
        </body>
        </html>
        """

@app.route('/historical')
def historical_dashboard():
    """Historical data dashboard"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Historical Data - Cupcake Trading</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-900 text-white">
        <div class="container mx-auto p-8">
            <div class="flex justify-between items-center mb-6">
                <h1 class="text-4xl font-bold">📊 Historical Data Viewer</h1>
                <a href="/" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded">
                    ← Back to Dashboard
                </a>
            </div>
            
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
                <div class="bg-gray-800 p-4 rounded-lg">
                    <h3 class="text-lg font-bold mb-2">Database Status</h3>
                    <div id="db-status">Loading...</div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg">
                    <h3 class="text-lg font-bold mb-2">Quick Actions</h3>
                    <div class="space-y-2">
                        <button onclick="exportAll()" class="w-full bg-green-600 hover:bg-green-700 text-white px-3 py-2 rounded text-sm">
                            Export All to CSV
                        </button>
                        <button onclick="refreshData()" class="w-full bg-blue-600 hover:bg-blue-700 text-white px-3 py-2 rounded text-sm">
                            Refresh Data
                        </button>
                    </div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg">
                    <h3 class="text-lg font-bold mb-2">Symbol Search</h3>
                    <input type="text" id="search-input" placeholder="Search symbols..." 
                           class="w-full bg-gray-700 text-white px-3 py-2 rounded mb-2">
                    <div id="search-results" class="text-sm"></div>
                </div>
            </div>
            
            <div class="bg-gray-800 p-6 rounded-lg">
                <h2 class="text-2xl font-bold mb-4">Symbols with Historical Data</h2>
                <div class="overflow-x-auto">
                    <table class="min-w-full divide-y divide-gray-700">
                        <thead class="bg-gray-700">
                            <tr>
                                <th class="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">Symbol</th>
                                <th class="px-4 py-3 text-right text-xs font-medium text-gray-300 uppercase">Data Points</th>
                                <th class="px-4 py-3 text-center text-xs font-medium text-gray-300 uppercase">Latest Price</th>
                                <th class="px-4 py-3 text-center text-xs font-medium text-gray-300 uppercase">Return %</th>
                                <th class="px-4 py-3 text-center text-xs font-medium text-gray-300 uppercase">Duration</th>
                                <th class="px-4 py-3 text-center text-xs font-medium text-gray-300 uppercase">Actions</th>
                            </tr>
                        </thead>
                        <tbody id="symbols-table" class="divide-y divide-gray-700">
                            <tr><td colspan="6" class="text-center py-4">Loading symbols...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <script>
            function loadDatabaseStatus() {
                fetch('/api/historical/status')
                    .then(response => response.json())
                    .then(data => {
                        const statusDiv = document.getElementById('db-status');
                        if (data.has_data) {
                            statusDiv.innerHTML = `
                                <p class="text-green-400">✅ Active</p>
                                <p class="text-sm text-gray-400">${data.symbol_count} symbols</p>
                                <p class="text-sm text-gray-400">${data.ts_keys} data series</p>
                            `;
                        } else {
                            statusDiv.innerHTML = `
                                <p class="text-red-400">❌ No Data</p>
                                <p class="text-sm text-gray-400">Run backfill first</p>
                            `;
                        }
                    })
                    .catch(err => {
                        document.getElementById('db-status').innerHTML = 
                            '<p class="text-red-400">❌ Error</p>';
                    });
            }
            
            function loadSymbols() {
                fetch('/api/historical/symbols')
                    .then(response => response.json())
                    .then(data => {
                        const tbody = document.getElementById('symbols-table');
                        
                        if (data.length === 0) {
                            tbody.innerHTML = '<tr><td colspan="6" class="text-center py-4 text-gray-500">No symbols found</td></tr>';
                            return;
                        }
                        
                        tbody.innerHTML = data.map(symbol => {
                            const returnClass = symbol.total_return_pct > 0 ? 'text-green-400' : 
                                              symbol.total_return_pct < 0 ? 'text-red-400' : 'text-gray-400';
                            
                            return `
                                <tr class="hover:bg-gray-700">
                                    <td class="px-4 py-3 font-medium">${symbol.display_name}</td>
                                    <td class="px-4 py-3 text-right">${symbol.data_points.toLocaleString()}</td>
                                    <td class="px-4 py-3 text-center">₹${symbol.last_price.toFixed(2)}</td>
                                    <td class="px-4 py-3 text-center ${returnClass}">${symbol.total_return_pct.toFixed(2)}%</td>
                                    <td class="px-4 py-3 text-center">${symbol.duration_hours.toFixed(1)}h</td>
                                    <td class="px-4 py-3 text-center">
                                        <button onclick="viewSymbol('${symbol.display_name}')" 
                                                class="bg-blue-600 hover:bg-blue-700 text-white px-2 py-1 rounded text-xs mr-1">
                                            View
                                        </button>
                                        <button onclick="exportSymbol('${symbol.display_name}')" 
                                                class="bg-green-600 hover:bg-green-700 text-white px-2 py-1 rounded text-xs">
                                            Export
                                        </button>
                                    </td>
                                </tr>
                            `;
                        }).join('');
                    })
                    .catch(err => {
                        document.getElementById('symbols-table').innerHTML = 
                            '<tr><td colspan="6" class="text-center py-4 text-red-400">Error loading symbols</td></tr>';
                    });
            }
            
            function viewSymbol(symbolName) {
                alert(`Viewing ${symbolName} - Feature coming soon!\\nFor now, use CLI: python historical_viewer.py`);
            }
            
            function exportSymbol(symbolName) {
                fetch('/api/historical/export', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({symbol: symbolName})
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        alert(`✅ Exported ${data.rows} rows to ${data.filename}`);
                    } else {
                        alert(`❌ Export failed: ${data.error}`);
                    }
                })
                .catch(err => alert(`❌ Export error: ${err}`));
            }
            
            function exportAll() {
                if (!confirm('Export all symbols to CSV? This may take a while.')) return;
                
                fetch('/api/historical/export-all', {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        alert(`✅ Exported ${data.files} files with ${data.total_rows} total rows`);
                    } else {
                        alert(`❌ Export failed: ${data.error}`);
                    }
                })
                .catch(err => alert(`❌ Export error: ${err}`));
            }
            
            function refreshData() {
                loadDatabaseStatus();
                loadSymbols();
            }
            
            // Search functionality
            document.getElementById('search-input').addEventListener('input', function(e) {
                const searchTerm = e.target.value.toLowerCase();
                const rows = document.querySelectorAll('#symbols-table tr');
                
                rows.forEach(row => {
                    const symbolName = row.cells[0]?.textContent?.toLowerCase() || '';
                    if (symbolName.includes(searchTerm) || searchTerm === '') {
                        row.style.display = '';
                    } else {
                        row.style.display = 'none';
                    }
                });
            });
            
            // Load data on page load
            loadDatabaseStatus();
            loadSymbols();
        </script>
    </body>
    </html>
    """

# --- Charting API Routes ---
@app.route('/api/chart/symbols')
def api_chart_symbols():
    """API endpoint for symbols available for charting"""
    if not historical_viewer:
        return jsonify([])
    
    try:
        symbols = historical_viewer.get_symbols_summary()
        # Return simplified list for charts
        chart_symbols = []
        for symbol in symbols:
            if symbol.get('data_points', 0) > 0:
                chart_symbols.append({
                    'symbol': symbol['display_name'],
                    'symbol_clean': symbol['symbol_clean'],
                    'data_points': symbol['data_points'],
                    'latest_price': symbol.get('last_price', 0),
                    'return_pct': symbol.get('total_return_pct', 0)
                })
        return jsonify(chart_symbols)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/chart/data/<symbol_name>')
def api_chart_data(symbol_name):
    """API endpoint for chart data of a specific symbol"""
    if not historical_viewer:
        return jsonify({'error': 'Historical viewer not available'})
    
    try:
        # Get query parameters
        timeframe = request.args.get('timeframe', '1h')  # 1h, 4h, 1d
        limit = min(int(request.args.get('limit', 100)), 500)  # Max 500 candles
        
        data = historical_viewer.get_symbol_complete_data(symbol_name)
        if not data:
            return jsonify({'error': 'Symbol not found'})
        
        df = data['dataframe']
        if df.empty:
            return jsonify({'error': 'No data available'})
        
        # Resample data based on timeframe
        if timeframe == '1h':
            # Group by hour
            df_resampled = df.resample('1H').agg({
                'open': 'first',
                'high': 'max', 
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
        elif timeframe == '4h':
            # Group by 4 hours
            df_resampled = df.resample('4H').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min', 
                'close': 'last',
                'volume': 'sum'
            }).dropna()
        elif timeframe == '1d':
            # Group by day
            df_resampled = df.resample('1D').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last', 
                'volume': 'sum'
            }).dropna()
        else:
            # Use minute data
            df_resampled = df
        
        # Get last N candles
        chart_data = df_resampled.tail(limit)
        
        # Format for Chart.js candlestick
        candles = []
        for timestamp, row in chart_data.iterrows():
            if pd.notna(row['open']) and pd.notna(row['close']):
                candles.append({
                    'x': timestamp.isoformat(),
                    'o': float(row['open']),
                    'h': float(row['high']),
                    'l': float(row['low']),
                    'c': float(row['close']),
                    'v': int(row['volume']) if pd.notna(row['volume']) else 0
                })
        
        # Calculate some stats
        latest_price = float(chart_data['close'].iloc[-1]) if len(chart_data) > 0 else 0
        first_price = float(chart_data['open'].iloc[0]) if len(chart_data) > 0 else 0
        price_change = latest_price - first_price
        price_change_pct = (price_change / first_price * 100) if first_price > 0 else 0
        
        return jsonify({
            'symbol': symbol_name,
            'timeframe': timeframe,
            'candles': candles,
            'stats': {
                'latest_price': latest_price,
                'price_change': price_change,
                'price_change_pct': price_change_pct,
                'volume_24h': int(chart_data['volume'].sum()) if len(chart_data) > 0 else 0,
                'high_24h': float(chart_data['high'].max()) if len(chart_data) > 0 else 0,
                'low_24h': float(chart_data['low'].min()) if len(chart_data) > 0 else 0,
                'data_points': len(candles)
            },
            'data_range': {
                'start': chart_data.index[0].isoformat() if len(chart_data) > 0 else None,
                'end': chart_data.index[-1].isoformat() if len(chart_data) > 0 else None
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/market/status')
def api_market_status():
    """API endpoint for market status (open/closed)"""
    try:
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Market hours: 9:15 AM to 3:30 PM IST, Monday to Friday
        market_open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        is_weekday = now.weekday() < 5  # Monday = 0, Friday = 4
        is_market_hours = market_open_time <= now <= market_close_time
        is_market_open = is_weekday and is_market_hours
        
        # Time until market open/close
        if is_market_open:
            time_until_close = market_close_time - now
            status_message = f"Market closes in {time_until_close}"
        elif is_weekday and now < market_open_time:
            time_until_open = market_open_time - now
            status_message = f"Market opens in {time_until_open}"
        elif is_weekday and now > market_close_time:
            # Next day
            next_open = market_open_time + timedelta(days=1)
            time_until_open = next_open - now
            status_message = f"Market opens tomorrow in {time_until_open}"
        else:
            # Weekend
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0:
                days_until_monday = 1  # If today is Sunday
            next_monday = now + timedelta(days=days_until_monday)
            next_open = next_monday.replace(hour=9, minute=15, second=0, microsecond=0)
            time_until_open = next_open - now
            status_message = f"Market opens Monday in {time_until_open}"
        
        return jsonify({
            'is_open': is_market_open,
            'current_time': now.isoformat(),
            'market_open': market_open_time.isoformat(),
            'market_close': market_close_time.isoformat(),
            'status_message': status_message,
            'is_weekend': not is_weekday
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

# --- Historical Data API Routes ---
@app.route('/api/historical/status')
def api_historical_status():
    """API endpoint for historical data status"""
    if not historical_viewer:
        return jsonify({'error': 'Historical viewer not available'})
    
    try:
        status = historical_viewer.get_database_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/historical/symbols')
def api_historical_symbols():
    """API endpoint for symbols summary"""
    if not historical_viewer:
        return jsonify([])
    
    try:
        symbols = historical_viewer.get_symbols_summary()
        return jsonify(symbols)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/historical/symbol/<symbol_name>')
def api_historical_symbol_data(symbol_name):
    """API endpoint for specific symbol data"""
    if not historical_viewer:
        return jsonify({'error': 'Historical viewer not available'})
    
    try:
        data = historical_viewer.get_symbol_complete_data(symbol_name)
        if not data:
            return jsonify({'error': 'Symbol not found'})
        
        # Convert DataFrame to JSON-serializable format
        df = data['dataframe']
        json_data = {
            'symbol_name': data['symbol_name'],
            'total_timepoints': data['total_timepoints'],
            'date_range': {
                'start': data['date_range']['start'].isoformat() if data['date_range']['start'] else None,
                'end': data['date_range']['end'].isoformat() if data['date_range']['end'] else None,
                'duration': str(data['date_range']['duration']) if data['date_range']['duration'] else None
            },
            'summary': data['summary'],
            'metadata': data['metadata'],
            'recent_data': df.tail(20).to_dict('records') if not df.empty else []
        }
        
        return jsonify(json_data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/historical/export', methods=['POST'])
def api_historical_export():
    """API endpoint to export symbol data"""
    if not historical_viewer:
        return jsonify({'success': False, 'error': 'Historical viewer not available'})
    
    try:
        data = request.get_json()
        symbol_name = data.get('symbol')
        
        if not symbol_name:
            return jsonify({'success': False, 'error': 'Symbol name required'})
        
        result = historical_viewer.export_symbol_to_csv(symbol_name)
        
        if result:
            return jsonify({
                'success': True,
                'filename': result['filename'],
                'filepath': result['filepath'],
                'rows': result['rows']
            })
        else:
            return jsonify({'success': False, 'error': 'Export failed'})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/historical/export-all', methods=['POST'])
def api_historical_export_all():
    """API endpoint to export all symbols"""
    if not historical_viewer:
        return jsonify({'success': False, 'error': 'Historical viewer not available'})
    
    try:
        exported_files = historical_viewer.export_all_symbols()
        
        total_rows = sum(f['rows'] for f in exported_files)
        
        return jsonify({
            'success': True,
            'files': len(exported_files),
            'total_rows': total_rows,
            'exported_files': exported_files
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/system_status')
def system_status():
    """
    API endpoint for system status information.
    """
    return {
        'status': 'running',
        'modules': {
            'enhanced_live_fetcher': 'active' if enhanced_live_fetcher else 'unavailable',
            'portfolio_tracker': 'active',
            'ultra_fast_historical': 'active' if UltraFastHistoricalFetcher else 'unavailable',
            'historical_viewer': 'active' if historical_viewer else 'unavailable',
            'signal_generator': 'active'
        },
        'features': {
            'actionable_signals': True,
            'trajectory_confirmation': True,
            'risk_reward_calculation': True,
            'multi_timeframe_analysis': True,
            'ultra_fast_historical': True,
            'historical_data_viewer': historical_viewer is not None
        }
    }

# --- SocketIO Connection Handlers ---
@socketio.on('connect')
def handle_connect():
    print("✅ Client connected to enhanced dashboard.")

@socketio.on('disconnect')
def handle_disconnect():
    print("🔥 Client disconnected from enhanced dashboard.")

@socketio.on('request_signal_analytics')
def handle_signal_analytics_request(data):
    """Handle request for detailed signal analytics for a specific symbol"""
    try:
        symbol = data.get('symbol')
        if symbol:
            # This would be implemented in the enhanced technical indicators
            analytics = {
                'symbol': symbol,
                'trajectory_analysis': 'Available in enhanced version',
                'signal_strength': 'Available in enhanced version',
                'risk_metrics': 'Available in enhanced version'
            }
            socketio.emit('signal_analytics_response', analytics)
    except Exception as e:
        print(f"Error handling signal analytics request: {e}")

# --- Main Execution Block ---
def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Cupcake Trading System')
    parser.add_argument('--mode', choices=['full', 'live', 'historical', 'backfill', 'viewer'], 
                       default='full', help='Running mode')
    parser.add_argument('--skip-auth', action='store_true', help='Skip authentication step')
    parser.add_argument('--skip-backfill', action='store_true', help='Skip historical backfill')
    parser.add_argument('--port', type=int, default=5000, help='Web server port')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    try:
        print("--- Enhanced Unified Trading System Initializing ---")
        print("🔥 Features: Ultra-Fast Historical | Actionable Signals | Portfolio Tracking | Historical Viewer")
        print(f"🔧 Working from: {SCRIPT_DIR}")
        print(f"🎯 Mode: {args.mode}")

        # Check if required files exist
        if not os.path.exists(CREDENTIALS_PATH):
            print(f"❌ Credentials file not found: {CREDENTIALS_PATH}")
            print("Please ensure credentials.json exists in the project directory")
            exit(1)
        
        if not os.path.exists(SYMBOLS_PATH):
            print(f"❌ Symbols file not found: {SYMBOLS_PATH}")
            print("Please ensure symbols.json exists in the project directory")
            exit(1)

        if args.mode == 'viewer':
            # Run only the historical viewer
            print("\n[HISTORICAL VIEWER MODE]")
            run_historical_viewer_cli()
            exit(0)
        
        elif args.mode == 'backfill':
            # Run only historical backfill
            print("\n[BACKFILL MODE]")
            run_historical_backfill()
            exit(0)

        # 1. Authenticate with Upstox (unless skipped)
        if not args.skip_auth:
            print("\n[STEP 1] Authenticating with Upstox...")
            try:
                auth_client = UpstoxAuthClient(config_file=CREDENTIALS_PATH)
                if not auth_client.authenticate():
                    print("❌ Authentication Failed. Please check your credentials and auth code. Exiting.")
                    exit()
                print("✅ Authentication Successful.")
            except Exception as e:
                print(f"❌ Authentication error: {e}")
                print("Continuing with other services...")
        else:
            print("\n[STEP 1] Skipping authentication as requested")

        # 2. Run Ultra-Fast Historical Data Backfill (unless skipped or in live mode)
        if args.mode != 'live' and not args.skip_backfill:
            print("\n[STEP 2] Setting up ultra-fast historical data foundation...")
            try:
                run_historical_backfill()
                print("✅ Ultra-fast historical data foundation ready for signal generation.")
            except Exception as e:
                print(f"❌ Historical backfill error: {e}")
                print("Continuing with other services...")
        else:
            print("\n[STEP 2] Skipping historical backfill")

        # 3. Start enhanced background services (unless in historical mode)
        if args.mode in ['full', 'live']:
            print("\n[STEP 3] Starting enhanced background services...")
            
            # Start enhanced live feed with actionable signals
            if enhanced_live_fetcher:
                try:
                    enhanced_feed_thread = threading.Thread(target=run_enhanced_live_data_feed, daemon=True)
                    enhanced_feed_thread.start()
                    print("   ✅ Enhanced live data feed with actionable signals started")
                except Exception as e:
                    print(f"   ❌ Live feed error: {e}")
            else:
                print("   ⚠️ Enhanced live data feed not available")

            # Start portfolio tracker
            try:
                portfolio_thread = threading.Thread(target=run_portfolio_tracker, daemon=True)
                portfolio_thread.start()
                print("   ✅ Portfolio tracker started")
            except Exception as e:
                print(f"   ❌ Portfolio tracker error: {e}")

        # 4. Copy dashboard template if it doesn't exist
        template_src = os.path.join(SCRIPT_DIR, 'enhanced_dashboard.html')
        template_dst = os.path.join(templates_dir, 'enhanced_dashboard.html')
        
        if os.path.exists(template_src) and not os.path.exists(template_dst):
            import shutil
            shutil.copy2(template_src, template_dst)
            print(f"📁 Copied dashboard template to {template_dst}")

        # 5. Start the Enhanced Flask-SocketIO web server
        if args.mode in ['full', 'live', 'historical']:
            print("\n[STEP 4] Starting Enhanced Dashboard Web Server...")
            print("==============================================")
            print(f"🌐 Enhanced Dashboard available at http://localhost:{args.port}")
            print(f"📊 Historical Data Viewer at http://localhost:{args.port}/historical")
            print("📊 Features:")
            print("   • Ultra-Fast Historical Data Fetching")
            print("   • Actionable Trading Signals")
            print("   • Trajectory Confirmation")
            print("   • Risk/Reward Analysis")
            print("   • Multi-timeframe Analysis")
            print("   • Real-time Portfolio Tracking")
            print("   • Historical Data Viewer (Web + CLI)")
            print("==============================================")
            print(f"💡 CLI Historical Viewer: python main.py --mode viewer")
            print(f"💡 Run Backfill Only: python main.py --mode backfill")
            print("==============================================")
            
            try:
                socketio.run(app, host='0.0.0.0', port=args.port, debug=False, use_reloader=False)
            except KeyboardInterrupt:
                print("\n👋 Shutting down Enhanced Trading System gracefully...")
            except Exception as e:
                print(f"❌ Error running server: {e}")
        
    except Exception as e:
        print(f"❌ Fatal system error: {e}")
        import traceback
        traceback.print_exc()