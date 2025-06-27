# Portfolio Tracker and PnL Analyzer
# Real-time portfolio tracking with WebSocket integration for Upstox

import os
import json
import requests
import websocket
import threading
import time
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional
import logging
from collections import defaultdict, deque
from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
import redis
import ssl
from flask_cors import CORS

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PortfolioTracker:
    def __init__(self, credentials_path: str, socketio_instance: Optional[SocketIO] = None):
        """Initialize Portfolio Tracker with Upstox credentials and an optional SocketIO instance"""
        self.credentials_path = credentials_path
        self.credentials = self._load_credentials()
        self.access_token = self.credentials.get('access_token')
        
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Portfolio data
        self.positions = {}
        self.holdings = {}
        self.orders = {}
        self.pnl_data = {
            'daily_pnl': 0.0,
            'total_pnl': 0.0,
            'realized_pnl': 0.0,
            'unrealized_pnl': 0.0,
            'total_investment': 0.0,
            'current_value': 0.0,
            'pnl_percentage': 0.0
        }
        
        # Historical data
        self.pnl_history = deque(maxlen=1000)  # Keep last 1000 PnL snapshots
        self.trade_history = deque(maxlen=500)  # Keep last 500 trades
        
        # WebSocket connection
        self.ws = None
        self.is_connected = False
        
        # Redis for storing portfolio data
        try:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
            redis_port = int(os.environ.get('REDIS_PORT', '6379'))
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=1, decode_responses=True)
            self.redis_client.ping()
            logger.info(f"Successfully connected to Redis at {redis_host}:{redis_port}.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Could not connect to Redis: {e}. Some features will be disabled.")
            self.redis_client = None
        
        # Flask-SocketIO instance for emitting events
        self.socketio = socketio_instance
        
        # Headers for API requests
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        logger.info("Portfolio Tracker initialized")

    def _load_credentials(self) -> Dict:
        """Load credentials from JSON file"""
        try:
            with open(self.credentials_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            return {}

    def get_portfolio_holdings(self) -> Dict:
        """Get long-term holdings from Upstox API"""
        try:
            url = "https://api.upstox.com/v2/portfolio/long-term-holdings"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'success':
                holdings_data = data.get('data', [])
                
                # Process holdings
                for holding in holdings_data:
                    instrument_key = holding.get('instrument_token')
                    self.holdings[instrument_key] = {
                        'symbol': holding.get('trading_symbol'),
                        'exchange': holding.get('exchange'),
                        'quantity': holding.get('quantity', 0),
                        'average_price': holding.get('average_price', 0),
                        'last_price': holding.get('last_price', 0),
                        'close_price': holding.get('close_price', 0),
                        'pnl': holding.get('pnl', 0),
                        'day_change': holding.get('day_change', 0),
                        'day_change_percentage': holding.get('day_change_percentage', 0),
                        'current_value': holding.get('quantity', 0) * holding.get('last_price', 0),
                        'investment_value': holding.get('quantity', 0) * holding.get('average_price', 0),
                        'product': holding.get('product', 'CNC'),
                        'isin': holding.get('isin'),
                        'last_update': datetime.now(self.ist_timezone).isoformat()
                    }
                
                logger.info(f"Loaded {len(self.holdings)} holdings")
                return self.holdings
            else:
                logger.error(f"Failed to get holdings: {data}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting portfolio holdings: {e}")
            return {}

    def get_portfolio_positions(self) -> Dict:
        """Get current positions from Upstox API"""
        try:
            url = "https://api.upstox.com/v2/portfolio/short-term-positions"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'success':
                positions_data = data.get('data', [])
                
                # Process positions
                for position in positions_data:
                    instrument_key = position.get('instrument_token')
                    self.positions[instrument_key] = {
                        'symbol': position.get('trading_symbol'),
                        'exchange': position.get('exchange'),
                        'quantity': position.get('quantity', 0),
                        'average_price': position.get('average_price', 0),
                        'last_price': position.get('last_price', 0),
                        'close_price': position.get('close_price', 0),
                        'pnl': position.get('pnl', 0),
                        'day_change': position.get('day_change', 0),
                        'day_change_percentage': position.get('day_change_percentage', 0),
                        'current_value': position.get('quantity', 0) * position.get('last_price', 0),
                        'investment_value': position.get('quantity', 0) * position.get('average_price', 0),
                        'product': position.get('product', 'MIS'),
                        'buy_quantity': position.get('buy_quantity', 0),
                        'sell_quantity': position.get('sell_quantity', 0),
                        'multiplier': position.get('multiplier', 1),
                        'last_update': datetime.now(self.ist_timezone).isoformat()
                    }
                
                logger.info(f"Loaded {len(self.positions)} positions")
                return self.positions
            else:
                logger.error(f"Failed to get positions: {data}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting portfolio positions: {e}")
            return {}

    def get_profit_loss_report(self) -> Dict:
        """Get profit and loss report"""
        try:
            url = "https://api.upstox.com/v2/portfolio/profit-and-loss"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'success':
                pnl_data = data.get('data', {})
                
                self.pnl_data.update({
                    'daily_pnl': pnl_data.get('daily_pnl', 0),
                    'total_pnl': pnl_data.get('total_pnl', 0),
                    'realized_pnl': pnl_data.get('realized_pnl', 0),
                    'unrealized_pnl': pnl_data.get('unrealized_pnl', 0),
                    'last_update': datetime.now(self.ist_timezone).isoformat()
                })
                
                return self.pnl_data
            else:
                logger.error(f"Failed to get PnL report: {data}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting PnL report: {e}")
            return {}

    def calculate_portfolio_metrics(self) -> Dict:
        """Calculate comprehensive portfolio metrics"""
        try:
            total_investment = 0
            current_value = 0
            total_pnl = 0
            daily_pnl = 0
            
            # Calculate from holdings
            for holding in self.holdings.values():
                investment = holding['quantity'] * holding['average_price']
                current = holding['quantity'] * holding['last_price']
                
                total_investment += investment
                current_value += current
                total_pnl += holding.get('pnl', 0)
                daily_pnl += (holding['last_price'] - holding['close_price']) * holding['quantity']
            
            # Calculate from positions
            for position in self.positions.values():
                investment = position['buy_quantity'] * position['average_price'] if position['quantity'] > 0 else position['sell_quantity'] * position['average_price']
                current = position['quantity'] * position['last_price']
                
                total_investment += abs(investment)  # Use absolute for short positions
                current_value += current
                total_pnl += position.get('pnl', 0)
                daily_pnl += position.get('pnl', 0) # For intraday, pnl is daily pnl
            
            # Calculate percentages
            pnl_percentage = (total_pnl / total_investment * 100) if total_investment > 0 else 0
            daily_pnl_percentage = (daily_pnl / total_investment * 100) if total_investment > 0 else 0
            
            # Update portfolio metrics
            self.pnl_data.update({
                'total_investment': total_investment,
                'current_value': current_value,
                'total_pnl': total_pnl,
                'daily_pnl': daily_pnl,
                'pnl_percentage': pnl_percentage,
                'daily_pnl_percentage': daily_pnl_percentage,
                'total_holdings': len(self.holdings),
                'total_positions': len(self.positions),
                'last_calculated': datetime.now(self.ist_timezone).isoformat()
            })
            
            # Store historical data
            pnl_snapshot = {
                'timestamp': datetime.now(self.ist_timezone).isoformat(),
                'total_pnl': total_pnl,
                'daily_pnl': daily_pnl,
                'current_value': current_value,
                'pnl_percentage': pnl_percentage
            }
            self.pnl_history.append(pnl_snapshot)
            
            return self.pnl_data
            
        except Exception as e:
            logger.error(f"Error calculating portfolio metrics: {e}")
            return self.pnl_data

    def save_portfolio_to_redis(self):
        """Save portfolio data to Redis if available"""
        if not self.redis_client:
            return
            
        try:
            # Save holdings
            if self.holdings:
                self.redis_client.hset('portfolio:holdings', mapping={
                    'data': json.dumps(self.holdings),
                    'last_update': datetime.now(self.ist_timezone).isoformat()
                })
            
            # Save positions
            if self.positions:
                self.redis_client.hset('portfolio:positions', mapping={
                    'data': json.dumps(self.positions),
                    'last_update': datetime.now(self.ist_timezone).isoformat()
                })
            
            # Save PnL data
            self.redis_client.hset('portfolio:pnl', mapping={
                'data': json.dumps(self.pnl_data),
                'last_update': datetime.now(self.ist_timezone).isoformat()
            })
            
            # Save PnL history (last 100 records)
            pnl_history_list = list(self.pnl_history)[-100:]
            self.redis_client.hset('portfolio:pnl_history', mapping={
                'data': json.dumps(pnl_history_list),
                'last_update': datetime.now(self.ist_timezone).isoformat()
            })
            
            logger.debug("Portfolio data saved to Redis")
            
        except Exception as e:
            logger.error(f"Error saving portfolio to Redis: {e}")

    def get_portfolio_websocket_url(self) -> str:
        """Get portfolio WebSocket URL from Upstox API"""
        try:
            url = "https://api.upstox.com/v2/feed/portfolio-stream-feed/authorize"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'success':
                return data['data']['authorized_redirect_uri']
            else:
                logger.error(f"Failed to get portfolio WebSocket URL: {data}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting portfolio WebSocket URL: {e}")
            return None

    def on_portfolio_message(self, ws, message):
        """Handle portfolio WebSocket messages"""
        try:
            data = json.loads(message)
            message_type = data.get('type')
            
            if message_type == 'order':
                self.process_order_update(data)
            elif message_type == 'position':
                self.process_position_update(data)
            elif message_type == 'holding':
                self.process_holding_update(data)
            else:
                logger.debug(f"Received live feed data: {data}")
            
            # Recalculate metrics and save to Redis
            self.calculate_portfolio_metrics()
            self.save_portfolio_to_redis()
            
            # Emit to dashboard via SocketIO
            self.emit_portfolio_update()
            
        except Exception as e:
            logger.error(f"Error processing portfolio message: {e}")

    def process_order_update(self, data):
        """Process order update from WebSocket"""
        try:
            order_data = data.get('data', {})
            order_id = order_data.get('order_id')
            
            if order_id:
                self.orders[order_id] = {
                    'order_id': order_id,
                    'symbol': order_data.get('trading_symbol'),
                    'exchange': order_data.get('exchange'),
                    'order_type': order_data.get('order_type'),
                    'transaction_type': order_data.get('transaction_type'),
                    'quantity': order_data.get('quantity', 0),
                    'price': order_data.get('price', 0),
                    'status': order_data.get('status'),
                    'product': order_data.get('product'),
                    'timestamp': datetime.now(self.ist_timezone).isoformat(),
                    'filled_quantity': order_data.get('filled_quantity', 0),
                    'average_price': order_data.get('average_price', 0)
                }
                
                # Add to trade history if order is filled
                if order_data.get('status') in ['COMPLETE', 'FILLED']:
                    trade_record = {
                        'timestamp': datetime.now(self.ist_timezone).isoformat(),
                        'symbol': order_data.get('trading_symbol'),
                        'type': order_data.get('transaction_type'),
                        'quantity': order_data.get('filled_quantity', 0),
                        'price': order_data.get('average_price', 0),
                        'value': order_data.get('filled_quantity', 0) * order_data.get('average_price', 0),
                        'order_id': order_id
                    }
                    self.trade_history.append(trade_record)
                
                logger.info(f"Order update: {order_data.get('trading_symbol')} - {order_data.get('status')}")
            
        except Exception as e:
            logger.error(f"Error processing order update: {e}")

    def process_position_update(self, data):
        """Process position update from WebSocket"""
        try:
            position_data = data.get('data', {})
            instrument_token = position_data.get('instrument_token')
            
            if instrument_token:
                self.positions[instrument_token].update({
                    'quantity': position_data.get('quantity', 0),
                    'average_price': position_data.get('average_price', 0),
                    'last_price': position_data.get('last_price', 0),
                    'pnl': position_data.get('pnl', 0),
                    'day_change': position_data.get('day_change', 0),
                    'current_value': position_data.get('quantity', 0) * position_data.get('last_price', 0),
                    'last_update': datetime.now(self.ist_timezone).isoformat()
                })
                
                logger.debug(f"Position update: {position_data.get('trading_symbol')}")
            
        except Exception as e:
            logger.error(f"Error processing position update: {e}")

    def process_holding_update(self, data):
        """Process holding update from WebSocket"""
        try:
            holding_data = data.get('data', {})
            instrument_token = holding_data.get('instrument_token')
            
            if instrument_token:
                self.holdings[instrument_token].update({
                    'quantity': holding_data.get('quantity', 0),
                    'last_price': holding_data.get('last_price', 0),
                    'pnl': holding_data.get('pnl', 0),
                    'day_change': holding_data.get('day_change', 0),
                    'current_value': holding_data.get('quantity', 0) * holding_data.get('last_price', 0),
                    'last_update': datetime.now(self.ist_timezone).isoformat()
                })
                
                logger.debug(f"Holding update: {holding_data.get('trading_symbol')}")
            
        except Exception as e:
            logger.error(f"Error processing holding update: {e}")

    def emit_portfolio_update(self):
        """Emit portfolio update to dashboard via SocketIO"""
        if not self.socketio:
            logger.debug("SocketIO not configured. Skipping emit.")
            return

        try:
            portfolio_summary = self.get_portfolio_summary()
            self.socketio.emit('portfolio_update', portfolio_summary)
            logger.debug("Portfolio update emitted via Socket.IO")
            
        except Exception as e:
            logger.error(f"Error emitting portfolio update: {e}")

    def on_ws_open(self, ws):
        """WebSocket connection opened"""
        self.is_connected = True
        logger.info("Portfolio WebSocket connected")

    def on_ws_close(self, ws, close_status_code, close_msg):
        """WebSocket connection closed"""
        self.is_connected = False
        logger.warning(f"Portfolio WebSocket closed: {close_status_code} - {close_msg}")

    def on_ws_error(self, ws, error):
        """WebSocket error"""
        logger.error(f"Portfolio WebSocket error: {error}")

    def start_portfolio_websocket(self):
        """Start portfolio WebSocket connection"""
        try:
            ws_url = self.get_portfolio_websocket_url()
            if not ws_url:
                logger.error("Failed to get portfolio WebSocket URL. Aborting WebSocket start.")
                return False
            
            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_message=self.on_portfolio_message,
                on_error=self.on_ws_error,
                on_close=self.on_ws_close,
                on_open=self.on_ws_open
            )
            
            # Start WebSocket in a separate thread
            ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={'sslopt': {"cert_reqs": ssl.CERT_NONE}})
            ws_thread.daemon = True
            ws_thread.start()
            
            logger.info("Portfolio WebSocket started")
            return True
            
        except Exception as e:
            logger.error(f"Error starting portfolio WebSocket: {e}")
            return False

    def start_portfolio_tracking(self):
        """Start comprehensive portfolio tracking"""
        try:
            logger.info("Starting portfolio tracking...")
            
            # Load initial data
            self.get_portfolio_holdings()
            self.get_portfolio_positions()
            # self.get_profit_loss_report() # This can be redundant if calculated manually
            self.calculate_portfolio_metrics()
            self.save_portfolio_to_redis()
            
            # Emit initial data
            self.emit_portfolio_update()
            
            # Start WebSocket for real-time updates
            self.start_portfolio_websocket()
            
            # Start periodic refresh
            self.start_periodic_refresh()
            
            logger.info("Portfolio tracking started successfully")
            
        except Exception as e:
            logger.error(f"Error starting portfolio tracking: {e}")

    def start_periodic_refresh(self):
        """Start periodic refresh of portfolio data in a background thread"""
        def refresh_loop():
            while True:
                try:
                    time.sleep(60)  # Refresh every 60 seconds
                    
                    logger.info("Performing periodic portfolio refresh...")
                    self.get_portfolio_holdings()
                    self.get_portfolio_positions()
                    self.calculate_portfolio_metrics()
                    self.save_portfolio_to_redis()
                    self.emit_portfolio_update() # Emit update after periodic refresh as well
                    
                    logger.info("Periodic portfolio refresh complete")
                    
                except Exception as e:
                    logger.error(f"Error in periodic refresh: {e}")
                    time.sleep(120)  # Wait longer on error
        
        refresh_thread = threading.Thread(target=refresh_loop)
        refresh_thread.daemon = True
        refresh_thread.start()
        
        logger.info("Periodic portfolio refresh thread started")

    def get_portfolio_summary(self) -> Dict:
        """Get complete portfolio summary"""
        return {
            'pnl_data': self.pnl_data,
            'holdings': self.holdings,
            'positions': self.positions,
            'recent_orders': dict(list(self.orders.items())[-10:]),  # Last 10 orders
            'recent_trades': list(self.trade_history)[-20:],  # Last 20 trades
            'pnl_history': list(self.pnl_history)[-50:],  # Last 50 PnL snapshots
            'connection_status': self.is_connected,
            'last_update': datetime.now(self.ist_timezone).isoformat()
        }

# Flask integration for portfolio API
class PortfolioAPI:
    def __init__(self, portfolio_tracker: PortfolioTracker):
        self.tracker = portfolio_tracker
    
    def setup_routes(self, app: Flask):
        """Setup Flask routes for portfolio API"""
        
        @app.route('/api/portfolio/summary')
        def portfolio_summary():
            return jsonify(self.tracker.get_portfolio_summary())
        
        @app.route('/api/portfolio/pnl')
        def portfolio_pnl():
            return jsonify(self.tracker.pnl_data)
        
        @app.route('/api/portfolio/holdings')
        def portfolio_holdings():
            return jsonify(self.tracker.holdings)
        
        @app.route('/api/portfolio/positions')
        def portfolio_positions():
            return jsonify(self.tracker.positions)
        
        @app.route('/api/portfolio/trades')
        def portfolio_trades():
            return jsonify(list(self.tracker.trade_history))
        
        @app.route('/api/portfolio/pnl_history')
        def portfolio_pnl_history():
            return jsonify(list(self.tracker.pnl_history))

# Main execution block
if __name__ == "__main__":
    # Initialize Flask app and SocketIO
    app = Flask(__name__)
    CORS(app) # Enable Cross-Origin Resource Sharing
    socketio = SocketIO(app, cors_allowed_origins="*")

    # Path to your Upstox credentials
    credentials_path = "credentials.json" # Assumes credentials.json is in the same directory
    
    # Initialize portfolio tracker with the SocketIO instance
    tracker = PortfolioTracker(credentials_path, socketio_instance=socketio)
    
    # Setup API routes
    api = PortfolioAPI(tracker)
    api.setup_routes(app)

    # SocketIO event handlers
    @socketio.on('connect')
    def handle_connect():
        logger.info("Client connected to SocketIO")
        # Emit the current summary to the newly connected client
        emit('portfolio_update', tracker.get_portfolio_summary())

    @socketio.on('disconnect')
    def handle_disconnect():
        logger.info("Client disconnected from SocketIO")

    # Start portfolio tracking in a background thread
    tracking_thread = threading.Thread(target=tracker.start_portfolio_tracking)
    tracking_thread.daemon = True
    tracking_thread.start()
    
    logger.info("Starting Flask-SocketIO server...")
    
    try:
        # Run the Flask-SocketIO server
        # use_reloader=False is important to prevent the tracking threads from starting twice
        socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
    finally:
        # Cleanly close the WebSocket connection on exit
        if tracker.ws:
            logger.info("Closing portfolio WebSocket connection.")
            tracker.ws.close()
        logger.info("Portfolio tracking stopped. Server shut down.")

