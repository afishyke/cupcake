import os
import threading
import asyncio
from flask import Flask, render_template
from flask_socketio import SocketIO
from flask_cors import CORS

# --- Import Enhanced Modules and Key Functions/Classes ---
try:
    import updated_enhanced_live_fetcher
    from updated_enhanced_live_fetcher import fetch_enhanced_market_data
except ImportError:
    print("Enhanced live fetcher not found, falling back to basic version")
    import enhanced_live_fetcher
    from enhanced_live_fetcher import fetch_enhanced_market_data

import portfolio_tracker
from historical_data_fetcher import HistoricalDataFetcher
from upstox_client import UpstoxAuthClient
from portfolio_tracker import PortfolioTracker

# --- Configuration ---
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), 'credentials.json')

class Config:
    """Simple configuration class for shared settings."""
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

# --- Main Application Setup ---
app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# --- CRITICAL INTEGRATION STEP ---
try:
    updated_enhanced_live_fetcher.socketio = socketio
except:
    enhanced_live_fetcher.socketio = socketio

# --- Orchestration Functions ---

def run_enhanced_live_data_feed():
    """
    Starts the enhanced live data fetcher with actionable signal generation.
    """
    print("🚀 Starting Enhanced Live Data Feed with Actionable Signal Analysis...")
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
    Runs the historical data backfill process.
    """
    print("⏳ Starting Historical Data Backfill Check...")
    # Create a config object to pass to the fetcher
    config = Config()
    fetcher = HistoricalDataFetcher(config=config)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(fetcher.initialize())
        if not fetcher.get_backfill_status():
             print("   - No previous backfill found. Running for the last 7 days.")
             loop.run_until_complete(fetcher.backfill_historical_data(days_back=7))
        else:
             print("   - Historical data already exists.")
        loop.run_until_complete(fetcher.cleanup())
    except Exception as e:
        print(f"❌ Error during historical backfill: {e}")
    finally:
        loop.close()

# --- Main Web Route ---
@app.route('/')
def dashboard():
    """
    Serves the enhanced dashboard HTML file from the templates folder.
    """
    return render_template('enhanced_dashboard.html')

@app.route('/api/system_status')
def system_status():
    """
    API endpoint for system status information.
    """
    return {
        'status': 'running',
        'modules': {
            'enhanced_live_fetcher': 'active',
            'portfolio_tracker': 'active',
            'signal_generator': 'active',
            'historical_data': 'active'
        },
        'features': {
            'actionable_signals': True,
            'trajectory_confirmation': True,
            'risk_reward_calculation': True,
            'multi_timeframe_analysis': True
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
if __name__ == '__main__':
    print("--- Enhanced Unified Trading System Initializing ---")
    print("🔥 Features: Actionable Signals | Trajectory Confirmation | Risk Management")

    # 1. Authenticate with Upstox
    print("\n[STEP 1] Authenticating with Upstox...")
    auth_client = UpstoxAuthClient(config_file=CREDENTIALS_PATH)
    if not auth_client.authenticate():
        print("❌ Authentication Failed. Please check your credentials and auth code. Exiting.")
        exit()
    print("✅ Authentication Successful.")

    # 2. Run Historical Data Backfill
    print("\n[STEP 2] Setting up historical data foundation...")
    run_historical_backfill()
    print("✅ Historical data foundation ready for signal generation.")

    # 3. Start enhanced background services
    print("\n[STEP 3] Starting enhanced background services...")
    
    # Start enhanced live feed with actionable signals
    enhanced_feed_thread = threading.Thread(target=run_enhanced_live_data_feed, daemon=True)
    enhanced_feed_thread.start()
    print("   ✅ Enhanced live data feed with actionable signals started")

    # Start portfolio tracker
    portfolio_thread = threading.Thread(target=run_portfolio_tracker, daemon=True)
    portfolio_thread.start()
    print("   ✅ Portfolio tracker started")

    # 4. Start the Enhanced Flask-SocketIO web server
    print("\n[STEP 4] Starting Enhanced Dashboard Web Server...")
    print("==============================================")
    print("🌐 Enhanced Dashboard available at http://localhost:5000")
    print("📊 Features:")
    print("   • Actionable Trading Signals")
    print("   • Trajectory Confirmation")
    print("   • Risk/Reward Analysis")
    print("   • Multi-timeframe Analysis")
    print("   • Real-time Portfolio Tracking")
    print("==============================================")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)
    except KeyboardInterrupt:
        print("\n👋 Shutting down Enhanced Trading System gracefully...")
    except Exception as e:
        print(f"❌ Error running server: {e}")