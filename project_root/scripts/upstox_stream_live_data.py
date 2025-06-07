import websocket
import json
import threading
import time
from datetime import datetime, timezone
import queue
import pandas as pd
from collections import defaultdict
import struct
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UpstoxDataStreamer:
    def __init__(self, access_token, instrument_keys, candle_queue, trade_tick_queue):
        """
        Initialize the Upstox data streamer
        
        Args:
            access_token (str): Upstox API access token
            instrument_keys (list): List of instrument keys to subscribe
            candle_queue (queue.Queue): Queue for completed minute candles
            trade_tick_queue (queue.Queue): Queue for trade ticks
        """
        self.access_token = access_token
        self.instrument_keys = instrument_keys
        self.candle_queue = candle_queue
        self.trade_tick_queue = trade_tick_queue
        
        # Global state
        self.live_order_books = {}  # Global dict for order book data
        self.candle_data = defaultdict(list)  # Store ticks for candle aggregation
        self.current_minute = {}  # Track current minute for each symbol
        
        # WebSocket connection
        self.ws = None
        self.is_connected = False
        self.should_reconnect = True
        
        # Initialize order books for all instruments
        for key in instrument_keys:
            self.live_order_books[key] = {"bids": [], "asks": []}
            self.current_minute[key] = None
    
    def connect(self):
        """Establish WebSocket connection to Upstox"""
        try:
            # Upstox WebSocket URL (replace with actual URL)
            ws_url = f"wss://ws-api.upstox.com/v2/feed/market-data-feed/protobuf?access_token={self.access_token}"
            
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Start WebSocket in a separate thread
            self.ws_thread = threading.Thread(target=self.ws.run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            logger.info("WebSocket connection initiated")
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
    
    def on_open(self, ws):
        """Handle WebSocket connection open"""
        logger.info("WebSocket connection opened")
        self.is_connected = True
        
        # Subscribe to instruments
        subscription_message = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",  # full mode for complete data
                "instrumentKeys": self.instrument_keys
            }
        }
        
        ws.send(json.dumps(subscription_message))
        logger.info(f"Subscribed to {len(self.instrument_keys)} instruments")
    
    def on_message(self, ws, message):
        """Process incoming WebSocket messages"""
        try:
            # Decode protobuf message (simplified - actual implementation needs protobuf parsing)
            data = self.decode_protobuf_message(message)
            
            if data:
                self.process_market_data(data)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def decode_protobuf_message(self, message):
        """
        Decode protobuf message from Upstox
        Note: This is a simplified version. Actual implementation requires 
        proper protobuf schema and parsing
        """
        try:
            # This is a mock implementation - replace with actual protobuf decoding
            # For now, assuming JSON for demonstration
            if isinstance(message, bytes):
                # Simple binary parsing example
                if len(message) < 8:
                    return None
                
                # Mock data structure based on Upstox format
                mock_data = {
                    "type": "lf",  # lf, ff, or tf
                    "instrument_key": self.instrument_keys[0] if self.instrument_keys else "NSE_EQ|INE002A01018",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ltp": 1500.0 + (hash(str(time.time())) % 100),
                    "volume": 1000,
                    "open": 1495.0,
                    "high": 1505.0,
                    "low": 1490.0,
                    "close": 1500.0,
                    "bids": [{"price": 1499.0, "quantity": 100}, {"price": 1498.0, "quantity": 200}],
                    "asks": [{"price": 1501.0, "quantity": 150}, {"price": 1502.0, "quantity": 300}]
                }
                return mock_data
            
            return json.loads(message) if isinstance(message, str) else None
            
        except Exception as e:
            logger.error(f"Error decoding protobuf: {e}")
            return None
    
    def process_market_data(self, data):
        """Route market data based on message type"""
        message_type = data.get("type", "")
        instrument_key = data.get("instrument_key", "")
        
        if message_type == "lf":  # Price Update
            self.handle_price_update(instrument_key, data)
        elif message_type == "ff":  # Order Book Update
            self.handle_order_book_update(instrument_key, data)
        elif message_type == "tf":  # Trade Tick
            self.handle_trade_tick(instrument_key, data)
    
    def handle_price_update(self, instrument_key, data):
        """Handle price updates and aggregate into candles"""
        try:
            current_time = datetime.now(timezone.utc)
            current_minute_key = current_time.replace(second=0, microsecond=0)
            
            # Initialize or check minute boundary
            if (self.current_minute.get(instrument_key) != current_minute_key):
                # New minute started - finalize previous candle if exists
                if (self.current_minute.get(instrument_key) is not None and 
                    len(self.candle_data[instrument_key]) > 0):
                    self.finalize_candle(instrument_key)
                
                # Start new minute
                self.current_minute[instrument_key] = current_minute_key
                self.candle_data[instrument_key] = []
            
            # Add tick to current minute data
            tick_data = {
                "timestamp": data.get("timestamp", current_time.isoformat()),
                "price": data.get("ltp", 0),
                "volume": data.get("volume", 0),
                "open": data.get("open", 0),
                "high": data.get("high", 0),
                "low": data.get("low", 0),
                "close": data.get("close", 0)
            }
            
            self.candle_data[instrument_key].append(tick_data)
            
        except Exception as e:
            logger.error(f"Error handling price update: {e}")
    
    def finalize_candle(self, instrument_key):
        """Create and queue completed minute candle"""
        try:
            if not self.candle_data[instrument_key]:
                return
            
            ticks = self.candle_data[instrument_key]
            
            # Aggregate ticks into OHLCV
            prices = [tick["price"] for tick in ticks if tick["price"] > 0]
            volumes = [tick["volume"] for tick in ticks if tick["volume"] > 0]
            
            if not prices:
                return
            
            candle = {
                "instrument_key": instrument_key,
                "timestamp": self.current_minute[instrument_key].isoformat(),
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "volume": sum(volumes) if volumes else 0
            }
            
            # Put completed candle in queue
            self.candle_queue.put(candle)
            logger.info(f"Candle completed for {instrument_key}: {candle['close']}")
            
            # Clear data for next minute
            self.candle_data[instrument_key] = []
            
        except Exception as e:
            logger.error(f"Error finalizing candle: {e}")
    
    def handle_order_book_update(self, instrument_key, data):
        """Update global order book state"""
        try:
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            # Update global order book dictionary
            self.live_order_books[instrument_key] = {
                "bids": bids[:5],  # Top 5 bids
                "asks": asks[:5]   # Top 5 asks
            }
            
            logger.debug(f"Order book updated for {instrument_key}")
            
        except Exception as e:
            logger.error(f"Error updating order book: {e}")
    
    def handle_trade_tick(self, instrument_key, data):
        """Handle individual trade ticks"""
        try:
            trade_tick = {
                "instrument_key": instrument_key,
                "price": data.get("price", data.get("ltp", 0)),
                "quantity": data.get("quantity", data.get("volume", 0)),
                "timestamp": data.get("timestamp", datetime.now(timezone.utc).isoformat())
            }
            
            # Put trade tick in queue for immediate processing
            self.trade_tick_queue.put(trade_tick)
            logger.debug(f"Trade tick queued for {instrument_key}")
            
        except Exception as e:
            logger.error(f"Error handling trade tick: {e}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
        self.is_connected = False
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.is_connected = False
        
        # Attempt reconnection if needed
        if self.should_reconnect:
            logger.info("Attempting to reconnect in 5 seconds...")
            time.sleep(5)
            self.connect()
    
    def get_live_order_book(self, instrument_key):
        """Get current order book for an instrument"""
        return self.live_order_books.get(instrument_key, {"bids": [], "asks": []})
    
    def stop(self):
        """Stop the data streamer"""
        self.should_reconnect = False
        if self.ws:
            self.ws.close()
        logger.info("Data streamer stopped")

# Example usage and testing
if __name__ == "__main__":
    # Example configuration
    ACCESS_TOKEN = "your_upstox_access_token"
    INSTRUMENT_KEYS = [
        "NSE_EQ|INE002A01018",  # RELIANCE
        "NSE_EQ|INE009A01021"   # INFOSYS
    ]
    
    # Create queues
    candle_queue = queue.Queue()
    trade_tick_queue = queue.Queue()
    
    # Initialize and start streamer
    streamer = UpstoxDataStreamer(ACCESS_TOKEN, INSTRUMENT_KEYS, candle_queue, trade_tick_queue)
    streamer.connect()
    
    try:
        # Monitor queues
        while True:
            # Check for completed candles
            try:
                candle = candle_queue.get_nowait()
                print(f"New candle: {candle}")
            except queue.Empty:
                pass
            
            # Check for trade ticks
            try:
                tick = trade_tick_queue.get_nowait()
                print(f"New trade tick: {tick}")
            except queue.Empty:
                pass
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Stopping streamer...")
        streamer.stop()