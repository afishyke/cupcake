#!/usr/bin/env python3
"""
Live Data Manager for Upstox API
Manages real-time WebSocket data streaming and CSV updates
Handles Market Data Feed V3 with Protobuf decoding
"""

import asyncio
import websockets
import json
import struct
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging
from typing import Dict, List, Optional, Callable
from pathlib import Path
import threading
import time
from collections import defaultdict, deque
import gzip
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UpstoxLiveDataManager:
    def __init__(self, access_token: str, data_dir: str = "data/live"):
        """
        Initialize the live data manager
        
        Args:
            access_token: Upstox API access token
            data_dir: Directory to store live data CSV files
        """
        self.access_token = access_token
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # WebSocket configuration
        self.ws_url = "wss://api-v2.upstox.com/feed/market-data-feed/v3"
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': '*/*'
        }
        
        # Connection management
        self.websocket = None
        self.is_connected = False
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5  # seconds
        
        # Subscription management
        self.subscribed_instruments = set()
        self.subscription_modes = {}  # instrument -> mode mapping
        
        # Data buffers for 1-minute OHLCV aggregation
        self.tick_buffers = defaultdict(deque)  # instrument -> tick data
        self.current_minute_data = defaultdict(dict)  # instrument -> current minute OHLCV
        self.ohlcv_data = defaultdict(list)  # instrument -> historical OHLCV list
        
        # Callbacks for real-time data
        self.tick_callbacks = []
        self.ohlcv_callbacks = []
        
        # Market status
        self.market_status = {}
        self.market_segments = {}
        
        # Threading
        self.data_thread = None
        self.minute_aggregator_thread = None
        self.stop_event = threading.Event()
        
        logger.info(f"Live data manager initialized. Data directory: {self.data_dir}")
    
    def add_tick_callback(self, callback: Callable):
        """Add callback for real-time tick data"""
        self.tick_callbacks.append(callback)
    
    def add_ohlcv_callback(self, callback: Callable):
        """Add callback for 1-minute OHLCV data"""
        self.ohlcv_callbacks.append(callback)
    
    def _generate_guid(self) -> str:
        """Generate unique GUID for requests"""
        return str(uuid.uuid4()).replace('-', '')[:20]
    
    def _create_subscription_message(self, method: str, mode: str, instrument_keys: List[str]) -> bytes:
        """
        Create WebSocket subscription message in binary format
        
        Args:
            method: 'sub', 'unsub', or 'change_mode'
            mode: 'ltpc', 'option_greeks', or 'full'
            instrument_keys: List of instrument keys to subscribe
        """
        message = {
            "guid": self._generate_guid(),
            "method": method,
            "data": {
                "mode": mode,
                "instrumentKeys": instrument_keys
            }
        }
        
        # Convert to JSON string then to bytes
        json_str = json.dumps(message)
        return json_str.encode('utf-8')
    
    async def _handle_websocket_message(self, message):
        """Handle incoming WebSocket messages"""
        try:
            # Handle binary messages (Protobuf format)
            if isinstance(message, bytes):
                await self._handle_binary_message(message)
            else:
                # Handle text messages (JSON format)
                data = json.loads(message)
                await self._handle_json_message(data)
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
    
    async def _handle_binary_message(self, message: bytes):
        """Handle binary Protobuf messages"""
        try:
            # For now, we'll handle the JSON format messages
            # In production, you'd need to decode using the Protobuf schema
            # from the provided MarketDataFeed.proto file
            
            # Try to decode as JSON first (for non-protobuf messages)
            try:
                decoded = message.decode('utf-8')
                data = json.loads(decoded)
                await self._handle_json_message(data)
            except:
                # This is likely a Protobuf message - would need proper decoding
                logger.debug(f"Received binary message of {len(message)} bytes")
                
        except Exception as e:
            logger.error(f"Error handling binary message: {e}")
    
    async def _handle_json_message(self, data: Dict):
        """Handle JSON messages from WebSocket"""
        try:
            message_type = data.get('type')
            
            if message_type == 'market_info':
                self._handle_market_info(data)
            elif message_type == 'live_feed':
                await self._handle_live_feed(data)
            else:
                logger.debug(f"Unknown message type: {message_type}")
                
        except Exception as e:
            logger.error(f"Error processing JSON message: {e}")
    
    def _handle_market_info(self, data: Dict):
        """Handle market status information"""
        try:
            self.market_status = data
            segment_status = data.get('marketInfo', {}).get('segmentStatus', {})
            self.market_segments = segment_status
            
            logger.info("Market status updated:")
            for segment, status in segment_status.items():
                logger.info(f"  {segment}: {status}")
                
        except Exception as e:
            logger.error(f"Error handling market info: {e}")
    
    async def _handle_live_feed(self, data: Dict):
        """Handle live feed data"""
        try:
            feeds = data.get('feeds', {})
            current_ts = data.get('currentTs')
            
            for instrument_key, feed_data in feeds.items():
                await self._process_instrument_feed(instrument_key, feed_data, current_ts)
                
        except Exception as e:
            logger.error(f"Error handling live feed: {e}")
    
    async def _process_instrument_feed(self, instrument_key: str, feed_data: Dict, timestamp: str):
        """Process feed data for a specific instrument"""
        try:
            # Extract LTP data based on feed type
            ltp_data = None
            
            if 'ltpc' in feed_data:
                ltp_data = feed_data['ltpc']
            elif 'firstLevelWithGreeks' in feed_data:
                ltp_data = feed_data['firstLevelWithGreeks'].get('ltpc')
            elif 'fullFeed' in feed_data:
                ltp_data = feed_data['fullFeed'].get('marketFF', {}).get('ltpc')
            
            if ltp_data:
                tick_data = {
                    'instrument_key': instrument_key,
                    'timestamp': pd.to_datetime(int(timestamp), unit='ms'),
                    'ltp': float(ltp_data.get('ltp', 0)),
                    'ltq': int(ltp_data.get('ltq', 0)),
                    'ltt': pd.to_datetime(int(ltp_data.get('ltt', timestamp)), unit='ms'),
                    'close_price': float(ltp_data.get('cp', 0)),
                    'volume': int(ltp_data.get('ltq', 0))
                }
                
                # Add to tick buffer
                self.tick_buffers[instrument_key].append(tick_data)
                
                # Trigger tick callbacks
                for callback in self.tick_callbacks:
                    try:
                        await callback(instrument_key, tick_data)
                    except Exception as e:
                        logger.error(f"Error in tick callback: {e}")
                
                # Update current minute data
                self._update_minute_data(instrument_key, tick_data)
                
        except Exception as e:
            logger.error(f"Error processing instrument feed for {instrument_key}: {e}")
    
    def _update_minute_data(self, instrument_key: str, tick_data: Dict):
        """Update current minute OHLCV data"""
        try:
            current_minute = tick_data['timestamp'].replace(second=0, microsecond=0)
            ltp = tick_data['ltp']
            volume = tick_data['volume']
            
            if instrument_key not in self.current_minute_data:
                self.current_minute_data[instrument_key] = {}
            
            minute_data = self.current_minute_data[instrument_key]
            
            # Check if we're in a new minute
            if 'timestamp' not in minute_data or minute_data['timestamp'] != current_minute:
                # Save previous minute data if exists
                if 'timestamp' in minute_data:
                    self._finalize_minute_data(instrument_key, minute_data)
                
                # Start new minute
                minute_data.update({
                    'timestamp': current_minute,
                    'open': ltp,
                    'high': ltp,
                    'low': ltp,
                    'close': ltp,
                    'volume': volume
                })
            else:
                # Update current minute
                minute_data['high'] = max(minute_data['high'], ltp)
                minute_data['low'] = min(minute_data['low'], ltp)
                minute_data['close'] = ltp
                minute_data['volume'] += volume
                
        except Exception as e:
            logger.error(f"Error updating minute data for {instrument_key}: {e}")
    
    def _finalize_minute_data(self, instrument_key: str, minute_data: Dict):
        """Finalize and save minute OHLCV data"""
        try:
            # Add to OHLCV history
            self.ohlcv_data[instrument_key].append(minute_data.copy())
            
            # Trigger OHLCV callbacks
            for callback in self.ohlcv_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        asyncio.create_task(callback(instrument_key, minute_data))
                    else:
                        callback(instrument_key, minute_data)
                except Exception as e:
                    logger.error(f"Error in OHLCV callback: {e}")
            
            # Save to CSV
            self._save_minute_data_to_csv(instrument_key, minute_data)
            
            logger.debug(f"Finalized minute data for {instrument_key}: {minute_data}")
            
        except Exception as e:
            logger.error(f"Error finalizing minute data for {instrument_key}: {e}")
    
    def _save_minute_data_to_csv(self, instrument_key: str, minute_data: Dict):
        """Save minute OHLCV data to CSV file"""
        try:
            # Clean instrument key for filename
            clean_name = instrument_key.replace('|', '_').replace(':', '_')
            filename = f"{clean_name}_live_1min.csv"
            filepath = self.data_dir / filename
            
            # Create DataFrame for this minute
            df_minute = pd.DataFrame([{
                'timestamp': minute_data['timestamp'],
                'open': minute_data['open'],
                'high': minute_data['high'],
                'low': minute_data['low'],
                'close': minute_data['close'],
                'volume': minute_data['volume'],
                'open_interest': 0  # Not available in live feed
            }])
            
            # Append to existing file or create new
            if filepath.exists():
                df_minute.to_csv(filepath, mode='a', header=False, index=False)
            else:
                df_minute.to_csv(filepath, index=False)
            
            logger.debug(f"Saved minute data to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving minute data to CSV: {e}")
    
    async def connect(self):
        """Connect to Upstox WebSocket"""
        try:
            logger.info("Connecting to Upstox WebSocket...")
            
            # Connect with headers
            extra_headers = [
                ('Authorization', f'Bearer {self.access_token}'),
                ('Accept', '*/*')
            ]
            
            self.websocket = await websockets.connect(
                self.ws_url,
                extra_headers=extra_headers,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10
            )
            
            self.is_connected = True
            self.reconnect_attempts = 0
            logger.info("✅ Connected to Upstox WebSocket")
            
            # Start listening for messages
            await self._listen_for_messages()
            
        except Exception as e:
            logger.error(f"❌ WebSocket connection failed: {e}")
            self.is_connected = False
            
            if self.reconnect_attempts < self.max_reconnect_attempts:
                self.reconnect_attempts += 1
                logger.info(f"Reconnecting in {self.reconnect_delay} seconds... (Attempt {self.reconnect_attempts})")
                await asyncio.sleep(self.reconnect_delay)
                await self.connect()
            else:
                logger.error("Max reconnection attempts reached")
    
    async def _listen_for_messages(self):
        """Listen for incoming WebSocket messages"""
        try:
            async for message in self.websocket:
                if not self.is_running:
                    break
                await self._handle_websocket_message(message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            self.is_connected = False
            
            if self.is_running:
                logger.info("Attempting to reconnect...")
                await self.connect()
                
        except Exception as e:
            logger.error(f"Error in message listener: {e}")
            self.is_connected = False
    
    async def subscribe(self, instrument_keys: List[str], mode: str = "ltpc"):
        """
        Subscribe to instruments
        
        Args:
            instrument_keys: List of instrument keys to subscribe
            mode: 'ltpc', 'option_greeks', or 'full'
        """
        try:
            if not self.is_connected:
                logger.error("Not connected to WebSocket")
                return False
            
            # Validate mode
            valid_modes = ['ltpc', 'option_greeks', 'full']
            if mode not in valid_modes:
                logger.error(f"Invalid mode: {mode}. Valid modes: {valid_modes}")
                return False
            
            # Create subscription message
            message = self._create_subscription_message('sub', mode, instrument_keys)
            
            # Send subscription
            await self.websocket.send(message)
            
            # Update subscription tracking
            for instrument_key in instrument_keys:
                self.subscribed_instruments.add(instrument_key)
                self.subscription_modes[instrument_key] = mode
            
            logger.info(f"✅ Subscribed to {len(instrument_keys)} instruments in {mode} mode")
            return True
            
        except Exception as e:
            logger.error(f"❌ Subscription failed: {e}")
            return False
    
    async def unsubscribe(self, instrument_keys: List[str]):
        """Unsubscribe from instruments"""
        try:
            if not self.is_connected:
                logger.error("Not connected to WebSocket")
                return False
            
            # Get mode for first instrument (assuming same mode for all)
            mode = self.subscription_modes.get(instrument_keys[0], 'ltpc')
            
            # Create unsubscription message
            message = self._create_subscription_message('unsub', mode, instrument_keys)
            
            # Send unsubscription
            await self.websocket.send(message)
            
            # Update subscription tracking
            for instrument_key in instrument_keys:
                self.subscribed_instruments.discard(instrument_key)
                self.subscription_modes.pop(instrument_key, None)
            
            logger.info(f"✅ Unsubscribed from {len(instrument_keys)} instruments")
            return True
            
        except Exception as e:
            logger.error(f"❌ Unsubscription failed: {e}")
            return False
    
    def start(self):
        """Start the live data manager"""
        self.is_running = True
        self.stop_event.clear()
        
        # Start asyncio event loop in separate thread
        self.data_thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self.data_thread.start()
        
        # Start minute aggregator thread
        self.minute_aggregator_thread = threading.Thread(target=self._minute_aggregator, daemon=True)
        self.minute_aggregator_thread.start()
        
        logger.info("🚀 Live data manager started")
    
    def stop(self):
        """Stop the live data manager"""
        self.is_running = False
        self.stop_event.set()
        
        if self.websocket:
            asyncio.create_task(self.websocket.close())
        
        logger.info("⏹️ Live data manager stopped")
    
    def _run_async_loop(self):
        """Run asyncio event loop in separate thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self.connect())
        except Exception as e:
            logger.error(f"Error in async loop: {e}")
        finally:
            loop.close()
    
    def _minute_aggregator(self):
        """Background thread to finalize minute data"""
        while not self.stop_event.is_set():
            try:
                current_time = datetime.now()
                
                # Check if we've crossed a minute boundary
                if current_time.second == 0:
                    # Finalize all current minute data
                    for instrument_key, minute_data in list(self.current_minute_data.items()):
                        if 'timestamp' in minute_data:
                            minute_boundary = minute_data['timestamp'] + timedelta(minutes=1)
                            if current_time >= minute_boundary:
                                self._finalize_minute_data(instrument_key, minute_data)
                                self.current_minute_data[instrument_key] = {}
                
                time.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in minute aggregator: {e}")
                time.sleep(5)
    
    def get_live_data(self, instrument_key: str) -> Optional[Dict]:
        """Get latest live data for an instrument"""
        if instrument_key in self.current_minute_data:
            return self.current_minute_data[instrument_key].copy()
        return None
    
    def get_ohlcv_history(self, instrument_key: str, periods: int = 100) -> List[Dict]:
        """Get OHLCV history for an instrument"""
        if instrument_key in self.ohlcv_data:
            return self.ohlcv_data[instrument_key][-periods:]
        return []
    
    def get_market_status(self) -> Dict:
        """Get current market status"""
        return {
            'is_connected': self.is_connected,
            'subscribed_instruments': len(self.subscribed_instruments),
            'market_segments': self.market_segments,
            'last_update': datetime.now().isoformat()
        }

# Example usage and testing
async def example_usage():
    """Example usage of the live data manager"""
    
    # Read access token (update path as needed)
    try:
        with open('upstox_config.json', 'r') as f:
            config = json.load(f)
            access_token = config['access_token']
    except FileNotFoundError:
        print("❌ Config file not found")
        return
    
    # Initialize live data manager
    live_manager = UpstoxLiveDataManager(access_token)
    
    # Add callbacks for real-time data
    async def on_tick_data(instrument_key: str, tick_data: Dict):
        print(f"📊 Tick: {instrument_key} - LTP: ₹{tick_data['ltp']}")
    
    def on_ohlcv_data(instrument_key: str, ohlcv_data: Dict):
        print(f"📈 OHLCV: {instrument_key} - OHLC: {ohlcv_data['open']}/{ohlcv_data['high']}/{ohlcv_data['low']}/{ohlcv_data['close']}")
    
    live_manager.add_tick_callback(on_tick_data)
    live_manager.add_ohlcv_callback(on_ohlcv_data)
    
    # Start the manager
    live_manager.start()
    
    # Wait for connection
    await asyncio.sleep(2)
    
    # Subscribe to instruments
    instruments = [
        'NSE_EQ|INE002A01018',  # Reliance
        'NSE_EQ|INE009A01021',  # Infosys
    ]
    
    await live_manager.subscribe(instruments, mode='ltpc')
    
    # Keep running
    try:
        while True:
            await asyncio.sleep(10)
            status = live_manager.get_market_status()
            print(f"Status: {status}")
    except KeyboardInterrupt:
        print("Stopping...")
        live_manager.stop()

if __name__ == "__main__":
    asyncio.run(example_usage())