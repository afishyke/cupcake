import asyncio
import websockets
import json
import logging
import time
from collections import deque, defaultdict
from queue import Queue
import threading
import requests
from google.protobuf.message import DecodeError
import struct
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stream.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UpstoxStreamManager:
    def __init__(self, access_token, config_path='config/subscriptions.json'):
        self.access_token = access_token
        self.config_path = config_path
        self.websocket = None
        self.running = False
        self.reconnect_count = 0
        self.max_reconnects = 5
        self.last_heartbeat = time.time()
        
        # High-performance queues
        self.trade_tick_queue = Queue(maxsize=10000)
        self.candle_queue = Queue(maxsize=5000)
        
        # Thread-safe order book storage
        self.live_order_books = defaultdict(dict)
        self.lock = threading.RLock()
        
        # Performance metrics
        self.messages_processed = 0
        self.start_time = time.time()
        self.last_stats_time = time.time()
        
        # Load subscription config
        self.load_subscription_config()
        
        # Circular buffer for raw data backup
        self.raw_data_buffer = deque(maxlen=1000)
        
    def load_subscription_config(self):
        """Load subscription configuration with error handling"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                self.subscription_mode = config.get('mode', 'full')
                self.instrument_keys = config.get('instrumentKeys', [])
                logger.info(f"Loaded {len(self.instrument_keys)} instruments for subscription")
        except Exception as e:
            logger.error(f"Failed to load subscription config: {e}")
            self.subscription_mode = 'full'
            self.instrument_keys = []
    
    async def get_websocket_url(self):
        """Get authorized WebSocket URL from Upstox API"""
        try:
            headers = {'Authorization': f'Bearer {self.access_token}'}
            response = requests.get(
                'https://api.upstox.com/v3/feed/authorize',
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            return data['data']['authorized_redirect_url']
        except Exception as e:
            logger.error(f"Failed to get WebSocket URL: {e}")
            raise
    
    def decode_protobuf_message(self, data):
        """Decode protobuf message with optimized error handling"""
        try:
            # Fast path for common message types
            if len(data) < 4:
                return None
                
            # Basic protobuf structure parsing
            # This is a simplified decoder - in production use proper protobuf
            message_type = struct.unpack('>I', data[:4])[0]
            payload = data[4:]
            
            if message_type == 1:  # Trade tick
                return self.parse_trade_tick(payload)
            elif message_type == 2:  # Order book update
                return self.parse_order_book(payload)
            elif message_type == 3:  # Candle data
                return self.parse_candle_data(payload)
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Protobuf decode error: {e}")
            return None
    
    def parse_trade_tick(self, payload):
        """Parse trade tick data - optimized for speed"""
        try:
            # Simplified parsing - adjust based on actual protobuf schema
            if len(payload) < 32:
                return None
                
            # Extract key fields quickly
            instrument_key = payload[:20].decode('utf-8', errors='ignore').rstrip('\x00')
            price = struct.unpack('>f', payload[20:24])[0]
            volume = struct.unpack('>I', payload[24:28])[0]
            timestamp = struct.unpack('>Q', payload[28:36])[0] if len(payload) >= 36 else int(time.time() * 1000)
            
            return {
                'type': 'trade_tick',
                'instrument_key': instrument_key,
                'price': price,
                'volume': volume,
                'timestamp': timestamp,
                'received_at': time.time()
            }
        except Exception as e:
            logger.debug(f"Trade tick parse error: {e}")
            return None
    
    def parse_order_book(self, payload):
        """Parse order book data with high performance"""
        try:
            if len(payload) < 50:
                return None
                
            instrument_key = payload[:20].decode('utf-8', errors='ignore').rstrip('\x00')
            
            # Parse bid/ask levels (simplified)
            bids = []
            asks = []
            
            offset = 20
            for i in range(5):  # Top 5 levels
                if offset + 16 <= len(payload):
                    bid_price = struct.unpack('>f', payload[offset:offset+4])[0]
                    bid_qty = struct.unpack('>I', payload[offset+4:offset+8])[0]
                    ask_price = struct.unpack('>f', payload[offset+8:offset+12])[0]
                    ask_qty = struct.unpack('>I', payload[offset+12:offset+16])[0]
                    
                    if bid_price > 0:
                        bids.append({'price': bid_price, 'quantity': bid_qty})
                    if ask_price > 0:
                        asks.append({'price': ask_price, 'quantity': ask_qty})
                    
                    offset += 16
            
            return {
                'type': 'order_book',
                'instrument_key': instrument_key,
                'bids': bids,
                'asks': asks,
                'timestamp': int(time.time() * 1000),
                'received_at': time.time()
            }
        except Exception as e:
            logger.debug(f"Order book parse error: {e}")
            return None
    
    def parse_candle_data(self, payload):
        """Parse 1-minute candle data"""
        try:
            if len(payload) < 40:
                return None
                
            instrument_key = payload[:20].decode('utf-8', errors='ignore').rstrip('\x00')
            
            # OHLCV data
            open_price = struct.unpack('>f', payload[20:24])[0]
            high_price = struct.unpack('>f', payload[24:28])[0]
            low_price = struct.unpack('>f', payload[28:32])[0]
            close_price = struct.unpack('>f', payload[32:36])[0]
            volume = struct.unpack('>I', payload[36:40])[0] if len(payload) >= 40 else 0
            
            return {
                'type': 'candle',
                'instrument_key': instrument_key,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'timestamp': int(time.time() * 1000),
                'received_at': time.time()
            }
        except Exception as e:
            logger.debug(f"Candle parse error: {e}")
            return None
    
    async def handle_message(self, message):
        """Process incoming WebSocket message with high performance"""
        try:
            self.messages_processed += 1
            self.last_heartbeat = time.time()
            
            # Store raw data for debugging
            self.raw_data_buffer.append({
                'data': message,
                'timestamp': time.time(),
                'size': len(message)
            })
            
            # Decode message
            parsed_data = self.decode_protobuf_message(message)
            if not parsed_data:
                return
            
            # Route to appropriate queue
            if parsed_data['type'] == 'trade_tick':
                if not self.trade_tick_queue.full():
                    self.trade_tick_queue.put(parsed_data, block=False)
                else:
                    logger.warning("Trade tick queue full, dropping message")
                    
            elif parsed_data['type'] == 'order_book':
                # Update live order book
                with self.lock:
                    self.live_order_books[parsed_data['instrument_key']] = {
                        'bids': parsed_data['bids'],
                        'asks': parsed_data['asks'],
                        'timestamp': parsed_data['timestamp']
                    }
                    
            elif parsed_data['type'] == 'candle':
                if not self.candle_queue.full():
                    self.candle_queue.put(parsed_data, block=False)
                else:
                    logger.warning("Candle queue full, dropping message")
            
            # Performance logging
            if time.time() - self.last_stats_time > 30:  # Every 30 seconds
                self.log_performance_stats()
                
        except Exception as e:
            logger.error(f"Message handling error: {e}")
    
    def log_performance_stats(self):
        """Log performance statistics"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        rate = self.messages_processed / elapsed if elapsed > 0 else 0
        
        logger.info(f"Performance Stats - Messages: {self.messages_processed}, "
                   f"Rate: {rate:.2f}/sec, "
                   f"Trade Queue: {self.trade_tick_queue.qsize()}, "
                   f"Candle Queue: {self.candle_queue.qsize()}")
        
        self.last_stats_time = current_time
    
    async def subscribe_to_instruments(self):
        """Subscribe to configured instruments"""
        try:
            subscription_message = {
                "guid": f"subscription-{int(time.time())}",
                "method": "sub",
                "data": {
                    "mode": self.subscription_mode,
                    "instrumentKeys": self.instrument_keys
                }
            }
            
            await self.websocket.send(json.dumps(subscription_message))
            logger.info(f"Subscribed to {len(self.instrument_keys)} instruments")
            
        except Exception as e:
            logger.error(f"Subscription error: {e}")
            raise
    
    async def send_heartbeat(self):
        """Send periodic heartbeat to maintain connection"""
        while self.running:
            try:
                if self.websocket and not self.websocket.closed:
                    heartbeat_msg = {
                        "guid": f"heartbeat-{int(time.time())}",
                        "method": "ping"
                    }
                    await self.websocket.send(json.dumps(heartbeat_msg))
                    logger.debug("Heartbeat sent")
                    
                await asyncio.sleep(30)  # Send heartbeat every 30 seconds
                
            except Exception as e:
                logger.warning(f"Heartbeat error: {e}")
                await asyncio.sleep(5)
    
    async def monitor_connection(self):
        """Monitor connection health and trigger reconnection if needed"""
        while self.running:
            try:
                current_time = time.time()
                
                # Check if we've received data recently
                if current_time - self.last_heartbeat > 60:  # No data for 1 minute
                    logger.warning("No data received for 60 seconds, triggering reconnection")
                    await self.reconnect()
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Connection monitoring error: {e}")
                await asyncio.sleep(5)
    
    async def reconnect(self):
        """Reconnect to WebSocket with exponential backoff"""
        if self.reconnect_count >= self.max_reconnects:
            logger.error("Max reconnection attempts reached")
            self.running = False
            return
        
        self.reconnect_count += 1
        backoff_time = min(2 ** self.reconnect_count, 60)  # Max 60 seconds
        
        logger.info(f"Reconnecting in {backoff_time} seconds (attempt {self.reconnect_count})")
        await asyncio.sleep(backoff_time)
        
        try:
            if self.websocket:
                await self.websocket.close()
            
            await self.connect()
            self.reconnect_count = 0  # Reset on successful connection
            
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
    
    async def connect(self):
        """Establish WebSocket connection"""
        try:
            websocket_url = await self.get_websocket_url()
            logger.info(f"Connecting to WebSocket: {websocket_url[:50]}...")
            
            self.websocket = await websockets.connect(
                websocket_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
                max_size=1024*1024,  # 1MB max message size
                compression=None  # Disable compression for speed
            )
            
            logger.info("WebSocket connected successfully")
            
            # Subscribe to instruments
            await self.subscribe_to_instruments()
            
            # Start background tasks
            asyncio.create_task(self.send_heartbeat())
            asyncio.create_task(self.monitor_connection())
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    async def start_streaming(self):
        """Start the streaming process"""
        self.running = True
        self.start_time = time.time()
        
        logger.info("Starting Upstox stream manager")
        
        try:
            await self.connect()
            
            # Main message loop
            async for message in self.websocket:
                if not self.running:
                    break
                
                if isinstance(message, bytes):
                    await self.handle_message(message)
                else:
                    # Handle text messages (control messages)
                    try:
                        text_data = json.loads(message)
                        logger.info(f"Control message: {text_data}")
                    except:
                        logger.debug(f"Non-JSON text message: {message}")
                        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            if self.running:
                await self.reconnect()
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            if self.running:
                await self.reconnect()
    
    def stop_streaming(self):
        """Stop the streaming process"""
        logger.info("Stopping stream manager")
        self.running = False
        
        if self.websocket:
            asyncio.create_task(self.websocket.close())
    
    def get_order_book(self, instrument_key):
        """Get current order book for an instrument"""
        with self.lock:
            return self.live_order_books.get(instrument_key, {})
    
    def save_raw_data_dump(self, filename=None):
        """Save raw data buffer to file for debugging"""
        if not filename:
            filename = f"data/raw/stream_dump_{int(time.time())}.bin"
        
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f:
                for entry in self.raw_data_buffer:
                    # Write timestamp, size, and data
                    f.write(struct.pack('>Q', int(entry['timestamp'] * 1000)))
                    f.write(struct.pack('>I', entry['size']))
                    f.write(entry['data'])
            
            logger.info(f"Raw data dump saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save raw data dump: {e}")

# Global instance for easy access
stream_manager = None

def initialize_stream_manager(access_token):
    """Initialize global stream manager"""
    global stream_manager
    stream_manager = UpstoxStreamManager(access_token)
    return stream_manager

async def main():
    """Main entry point for testing"""
    # You need to provide your access token
    ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN')
    if not ACCESS_TOKEN:
        logger.error("Please set UPSTOX_ACCESS_TOKEN environment variable")
        return
    
    manager = initialize_stream_manager(ACCESS_TOKEN)
    
    try:
        await manager.start_streaming()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        manager.stop_streaming()
        manager.save_raw_data_dump()

if __name__ == "__main__":
    # Create required directories
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/raw', exist_ok=True)
    
    asyncio.run(main())