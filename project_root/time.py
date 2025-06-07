import time
import threading
import queue
import json
import pandas as pd
from datetime import datetime, timezone
import logging
from collections import defaultdict
import os

# Import your existing modules
# from advanced_indicators import calculate_vwap, analyze_order_book, calculate_cumulative_delta
# from risk_manager import RiskManager  
# from final_analyzer import synthesize_final_output
from upstox_stream_live_data import UpstoxDataStreamer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TradingPipelineOrchestrator:
    def __init__(self, config):
        """
        Initialize the central orchestrator
        
        Args:
            config (dict): Configuration containing API keys, symbols, etc.
        """
        self.config = config
        self.symbols = config.get('symbols', [])
        self.access_token = config.get('upstox_access_token', '')
        
        # Queues for inter-module communication
        self.candle_queue = queue.Queue()
        self.trade_tick_queue = queue.Queue()
        
        # Data storage
        self.ohlcv_data = defaultdict(list)  # Store historical OHLCV for each symbol
        self.risk_managers = {}  # One risk manager per symbol
        self.cumulative_deltas = defaultdict(lambda: {"value": 0, "last_direction": 0, "last_price": 0})
        
        # Initialize data streamer
        self.data_streamer = UpstoxDataStreamer(
            self.access_token, 
            self.symbols, 
            self.candle_queue, 
            self.trade_tick_queue
        )
        
        # Control flags
        self.is_running = False
        self.threads = []
        
        # Output configuration
        self.output_dir = config.get('output_dir', './trading_output')
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize risk managers for each symbol
        for symbol in self.symbols:
            self.risk_managers[symbol] = self.create_risk_manager(symbol)
    
    def create_risk_manager(self, symbol):
        """Create a risk manager instance for a symbol"""
        # Mock risk manager - replace with your actual RiskManager class
        class MockRiskManager:
            def __init__(self, symbol):
                self.symbol = symbol
                self.entry_price = 0
                self.stop_level = 0
                self.target_level = 0
                self.max_price_seen = 0
                
            def update(self, current_price):
                if self.entry_price == 0:
                    self.entry_price = current_price
                    self.stop_level = current_price * 0.98  # 2% stop loss
                    self.target_level = current_price * 1.05  # 5% target
                
                self.max_price_seen = max(self.max_price_seen, current_price)
                trailing_stop = self.max_price_seen * 0.99  # 1% trailing stop
                
                max_drawdown_pct = ((self.max_price_seen - current_price) / self.max_price_seen) * 100
                
                return {
                    "pre_set_stop_level": self.stop_level,
                    "profit_target_level": self.target_level,
                    "dynamic_trailing_stop": trailing_stop,
                    "max_drawdown_pct": max_drawdown_pct
                }
        
        return MockRiskManager(symbol)
    
    def start(self):
        """Start the orchestrator and all its components"""
        logger.info("Starting Trading Pipeline Orchestrator...")
        self.is_running = True
        
        # Start data streamer
        self.data_streamer.connect()
        time.sleep(2)  # Allow connection to establish
        
        # Start processing threads
        self.start_processing_threads()
        
        # Start minute timer
        self.start_minute_timer()
        
        logger.info("All components started successfully")
    
    def start_processing_threads(self):
        """Start background processing threads"""
        # Candle processing thread
        candle_thread = threading.Thread(target=self.process_candles, daemon=True)
        candle_thread.start()
        self.threads.append(candle_thread)
        
        # Trade tick processing thread  
        tick_thread = threading.Thread(target=self.process_trade_ticks, daemon=True)
        tick_thread.start()
        self.threads.append(tick_thread)
        
        logger.info("Processing threads started")
    
    def process_candles(self):
        """Process completed minute candles"""
        while self.is_running:
            try:
                # Wait for candle with timeout
                candle = self.candle_queue.get(timeout=1)
                
                symbol = candle['instrument_key']
                logger.info(f"Processing candle for {symbol}: OHLC={candle['open']:.2f}, {candle['high']:.2f}, {candle['low']:.2f}, {candle['close']:.2f}")
                
                # Store OHLCV data
                self.ohlcv_data[symbol].append(candle)
                
                # Keep only last 100 candles to manage memory
                if len(self.ohlcv_data[symbol]) > 100:
                    self.ohlcv_data[symbol] = self.ohlcv_data[symbol][-100:]
                
                # Update risk manager
                if symbol in self.risk_managers:
                    self.risk_managers[symbol].update(candle['close'])
                
                self.candle_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing candle: {e}")
    
    def process_trade_ticks(self):
        """Process individual trade ticks"""
        while self.is_running:
            try:
                # Wait for trade tick with timeout
                tick = self.trade_tick_queue.get(timeout=1)
                
                symbol = tick['instrument_key']
                price = tick['price']
                
                # Update cumulative delta (simplified version)
                self.update_cumulative_delta(symbol, price, tick['quantity'])
                
                # Log significant ticks
                if tick['quantity'] > 1000:  # Large trades
                    logger.info(f"Large trade in {symbol}: {tick['quantity]} @ {price}")
                
                self.trade_tick_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing trade tick: {e}")
    
    def update_cumulative_delta(self, symbol, price, volume):
        """Update cumulative delta for a symbol"""
        delta_data = self.cumulative_deltas[symbol]
        last_price = delta_data['last_price']
        
        if last_price == 0:
            delta_data['last_price'] = price
            return
        
        # Determine tick direction (simplified uptick rule)
        if price > last_price:
            tick_direction = 1  # Uptick
        elif price < last_price:
            tick_direction = -1  # Downtick
        else:
            tick_direction = delta_data['last_direction']  # Same as last
        
        # Update cumulative delta
        delta_data['value'] += tick_direction * volume
        delta_data['last_direction'] = tick_direction
        delta_data['last_price'] = price
    
    def start_minute_timer(self):
        """Start the minute-based analysis timer"""
        timer_thread = threading.Thread(target=self.minute_timer_loop, daemon=True)
        timer_thread.start()
        self.threads.append(timer_thread)
        logger.info("Minute timer started")
    
    def minute_timer_loop(self):
        """Main timer loop that triggers analysis every minute"""
        last_minute = None
        
        while self.is_running:
            try:
                current_time = datetime.now(timezone.utc)
                current_minute = current_time.replace(second=0, microsecond=0)
                
                # Check if we've entered a new minute
                if last_minute is not None and current_minute > last_minute:
                    # Trigger analysis for all symbols
                    self.trigger_minute_analysis(current_minute)
                
                last_minute = current_minute
                
                # Sleep until next check (every 10 seconds)
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in minute timer loop: {e}")
                time.sleep(5)
    
    def trigger_minute_analysis(self, timestamp):
        """Trigger comprehensive analysis for all symbols"""
        logger.info(f"Triggering minute analysis at {timestamp}")
        
        for symbol in self.symbols:
            try:
                self.analyze_symbol(symbol, timestamp)
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {e}")
    
    def analyze_symbol(self, symbol, timestamp):
        """Perform complete analysis for a single symbol"""
        # Get current data
        ohlcv_list = self.ohlcv_data.get(symbol, [])
        if len(ohlcv_list) < 5:  # Need minimum data
            logger.warning(f"Insufficient data for {symbol}")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(ohlcv_list)
        
        # Get current order book
        order_book = self.data_streamer.get_live_order_book(symbol)
        
        # Get risk data
        risk_data = {}
        if symbol in self.risk_managers:
            current_price = df['close'].iloc[-1]
            risk_data = self.risk_managers[symbol].update(current_price)
        
        # Get cumulative delta
        delta_data = self.cumulative_deltas[symbol]
        
        # Calculate basic indicators (mock implementation)
        vwap_data = self.calculate_mock_vwap(df)
        order_analysis = self.analyze_mock_order_book(order_book)
        
        # Generate final output
        final_output = self.generate_final_output(
            symbol, timestamp, df, vwap_data, order_book, 
            order_analysis, delta_data, risk_data
        )
        
        # Save output
        self.save_output(symbol, final_output)
        
        logger.info(f"Analysis completed for {symbol}")
    
    def calculate_mock_vwap(self, df):
        """Mock VWAP calculation"""
        if len(df) == 0:
            return {"vwap": 0, "upper_band_2sd": 0, "lower_band_2sd": 0}
        
        # Simple VWAP approximation
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        vwap = (typical_price * df['volume']).sum() / df['volume'].sum() if df['volume'].sum() > 0 else typical_price.mean()
        
        # Mock standard deviation bands
        price_std = typical_price.std()
        
        return {
            "vwap": float(vwap),
            "upper_band_2sd": float(vwap + 2 * price_std),
            "lower_band_2sd": float(vwap - 2 * price_std)
        }
    
    def analyze_mock_order_book(self, order_book):
        """Mock order book analysis"""
        bids = order_book.get('bids', [])
        asks = order_book.get('asks', [])
        
        if not bids or not asks:
            return {"order_flow_imbalance_pct": 0}
        
        bid_volume = sum(bid.get('quantity', 0) for bid in bids)
        ask_volume = sum(ask.get('quantity', 0) for ask in asks)
        
        total_volume = bid_volume + ask_volume
        if total_volume == 0:
            return {"order_flow_imbalance_pct": 0}
        
        imbalance_pct = ((bid_volume - ask_volume) / total_volume) * 100
        
        return {"order_flow_imbalance_pct": float(imbalance_pct)}
    
    def generate_final_output(self, symbol, timestamp, df, vwap_data, order_book, order_analysis, delta_data, risk_data):
        """Generate the final comprehensive output"""
        latest_candle = df.iloc[-1] if len(df) > 0 else {}
        
        # Simple signal generation (mock)
        signal = "HOLD"
        if len(df) > 1:
            price_change = (df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100
            if price_change > 1:
                signal = "BUY"
            elif price_change < -1:
                signal = "SELL"
        
        return {
            "symbol": symbol,
            "timestamp": timestamp.isoformat(),
            "overall_signal": signal,
            "data_layers": {
                "intraday_price_v": {
                    "ohlc": {
                        "open": float(latest_candle.get('open', 0)),
                        "high": float(latest_candle.get('high', 0)),
                        "low": float(latest_candle.get('low', 0)),
                        "close": float(latest_candle.get('close', 0)),
                        "volume": int(latest_candle.get('volume', 0))
                    },
                    "vwap_analysis": vwap_data
                },
                "order_book": {
                    "level2_depth": order_book,
                    "analysis": order_analysis
                },
                "time_and_sales": {
                    "analysis": {
                        "cumulative_delta_estimated": delta_data['value']
                    }
                }
            },
            "advanced_indicators_client_side": {
                "trend_strength": 50 + (delta_data['value'] / 1000),  # Mock calculation
                "momentum_score": abs(float(latest_candle.get('close', 0)) - vwap_data['vwap'])
            },
            "risk_controls_client_side": risk_data
        }
    
    def save_output(self, symbol, output):
        """Save output to file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{symbol}_{timestamp}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(output, f, indent=2)
            
            # Also save latest output
            latest_file = os.path.join(self.output_dir, f"{symbol}_latest.json")
            with open(latest_file, 'w') as f:
                json.dump(output, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving output for {symbol}: {e}")
    
    def stop(self):
        """Stop the orchestrator and all components"""
        logger.info("Stopping Trading Pipeline Orchestrator...")
        
        self.is_running = False
        
        # Stop data streamer
        self.data_streamer.stop()
        
        # Wait for threads to finish
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("Orchestrator stopped")

# Main execution
if __name__ == "__main__":
    # Configuration
    config = {
        'upstox_access_token': 'your_upstox_access_token_here',
        'symbols': [
            'NSE_EQ|INE002A01018',  # RELIANCE
            'NSE_EQ|INE009A01021',  # INFOSYS
            'NSE_EQ|INE467B01029'   # TCS
        ],
        'output_dir': './trading_analysis_output'
    }
    
    # Create and start orchestrator
    orchestrator = TradingPipelineOrchestrator(config)
    
    try:
        orchestrator.start()
        
        # Keep running until interrupted
        logger.info("Pipeline is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(60)
            logger.info("Pipeline running smoothly...")
            
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        orchestrator.stop()