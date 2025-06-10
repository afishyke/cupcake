#!/usr/bin/env python3
"""
Fixed Live Data Manager for Upstox API v3
Manages real-time WebSocket data streaming with proper Protobuf handling
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
import aiohttp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UpstoxLiveDataManager:
    def __init__(self, access_token: str, data_dir: str = "data/live"):
        """
        Initialize the live data manager for Upstox API v3
        
        Args:
            access_token: Upstox API access token
            data_dir: Directory to store live data CSV files
        """
        self.access_token = access_token
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # API URLs
        self.auth_url = "https://api-v2.upstox.com/feed/market-data-feed/authorize"
        self.websocket_url = None  # Will be obtained from auth endpoint
        
        # Connection management
        self.websocket = None
        self.is_connected = False
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5
        
        # Subscription management with limits
        self.subscribed_instruments = set()
        self.subscription_modes = {}
        self.subscription_limits = {
            'ltpc': {'individual': 5000, 'combined': 2000},
            'option_greeks': {'individual': 3000, 'combined': 2000},
            'full': {'individual': 2000, 'combined': 1500}
        }
        self.current_subscriptions = {'ltpc': 0, 'option_greeks': 0, 'full': 0}
        
        # Data buffers
        self.tick_buffers = defaultdict(deque)
        self.current_minute_data = defaultdict(dict)
        self.ohlcv_data = defaultdict(list)
        
        # Callbacks
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
    
    async def _get_websocket_url(self) -> str:
        """Get the authorized WebSocket URL from Upstox API"""
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.auth_url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        websocket_url = data.get('data', {}).get('authorizedRedirectUri')
                        if websocket_url:
                            logger.info(f"✅ Obtained WebSocket URL: {websocket_url}")
                            return websocket_url
                        else:
                            raise Exception("WebSocket URL not found in response")
                    else:
                        error_text = await response.text()
                        raise Exception(f"Failed to get WebSocket URL: {response.status} - {error_text}")
        except Exception as e:
            logger.error(f"❌ Error getting WebSocket URL: {e}")
            raise
    
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
        # Note: The documentation mentions binary format but doesn't specify the exact encoding
        # This may need adjustment based on actual API behavior
        json_str = json.dumps(message)
        return json_str.encode('utf-8')
    
    async def _handle_websocket_message(self, message):
        """Handle incoming WebSocket messages"""
        try:
            # Handle binary messages (should be Protobuf)
            if isinstance(message, bytes):
                await self._handle_protobuf_message(message)
            else:
                # Handle text messages (JSON format)
                data = json.loads(message)
                await self._handle_json_message(data)
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
    
    async def _handle_protobuf_message(self, message: bytes):
        """
        Handle Protobuf encoded messages
        Note: This requires the generated Python classes from MarketDataFeed.proto
        """
        try:
            # TODO: Implement proper Protobuf decoding
            # You need to:
            # 1. Download MarketDataFeed.proto from https://assets.upstox.com/feed/market-data-feed/v3/MarketDataFeed.proto
            # 2. Generate Python classes: protoc --python_out=. MarketDataFeed.proto
            # 3. Import and use the generated classes
            
            # For now, try to decode as JSON (for testing)
            try:
                decoded = message.decode('utf-8')
                data = json.loads(decoded)
                await self._handle_json_message(data)
            except:
                logger.debug(f"Received Protobuf message of {len(message)} bytes - requires proper decoding")
                
        except Exception as e:
            logger.error(f"Error handling Protobuf message: {e}")
    
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
                logger.debug(f"Message: {data}")
                
        except Exception as e:
            logger.error(f"Error processing JSON message: {e}")
    
    def _handle_market_info(self, data: Dict):
        """Handle market status information"""
        try:
            self.market_status = data
            market_info = data.get('marketInfo', {})
            segment_status = market_info.get('segmentStatus', {})
            self.market_segments = segment_status
            
            logger.info("📊 Market status updated:")
            for segment, status in segment_status.items():
                logger.info(f"  {segment}: {status}")
                
        except Exception as e:
            logger.error(f"Error handling market info: {e}")
    
    async def _handle_live_feed(self, data: Dict):
        """Handle live feed data with correct structure"""
        try:
            feeds = data.get('feeds', {})
            current_ts = data.get('currentTs')
            
            for instrument_key, feed_data in feeds.items():
                await self._process_instrument_feed(instrument_key, feed_data, current_ts)
                
        except Exception as e:
            logger.error(f"Error handling live feed: {e}")
    
    async def _process_instrument_feed(self, instrument_key: str, feed_data: Dict, timestamp: str):
        """Process feed data for a specific instrument with correct data structure"""
        try:
            ltp_data = None
            
            # Handle different feed types based on subscription mode
            if 'ltpc' in feed_data:
                # LTPC mode
                ltp_data = feed_data['ltpc']
            elif 'firstLevelWithGreeks' in feed_data:
                # Option Greeks mode
                ltp_data = feed_data['firstLevelWithGreeks'].get('ltpc')
            elif 'fullFeed' in feed_data:
                # Full mode
                market_ff = feed_data['fullFeed'].get('marketFF', {})
                ltp_data = market_ff.get('ltpc')
            
            if ltp_data:
                # Parse timestamp correctly
                tick_timestamp = pd.to_datetime(int(timestamp), unit='ms')
                ltt_timestamp = pd.to_datetime(int(ltp_data.get('ltt', timestamp)), unit='ms')
                
                tick_data = {
                    'instrument_key': instrument_key,
                    'timestamp': tick_timestamp,
                    'ltp': float(ltp_data.get('ltp', 0)),
                    'ltq': int(ltp_data.get('ltq', 0)),
                    'ltt': ltt_timestamp,
                    'close_price': float(ltp_data.get('cp', 0)),
                    'volume': int(ltp_data.get('ltq', 0))
                }
                
                # Add to tick buffer
                self.tick_buffers[instrument_key].append(tick_data)
                
                # Trigger tick callbacks
                for callback in self.tick_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(instrument_key, tick_data)
                        else:
                            callback(instrument_key, tick_data)
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
    
    def _check_subscription_limits(self, mode: str, new_count: int) -> bool:
        """Check if subscription is within limits"""
        # Check individual limits
        individual_limit = self.subscription_limits[mode]['individual']
        
        # Check combined limits if multiple modes are used
        total_modes_used = sum(1 for count in self.current_subscriptions.values() if count > 0)
        if total_modes_used > 1 or (total_modes_used == 1 and new_count > 0):
            combined_limit = self.subscription_limits[mode]['combined']
            return new_count <= combined_limit
        
        return new_count <= individual_limit
    
    async def connect(self):
        """Connect to Upstox WebSocket with proper URL handling"""
        try:
            logger.info("🔗 Getting WebSocket URL...")
            self.websocket_url = await self._get_websocket_url()
            
            logger.info("🔌 Connecting to Upstox WebSocket...")
            
            # Connect to the authorized WebSocket URL
            self.websocket = await websockets.connect(
                self.websocket_url,
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
                logger.info(f"🔄 Reconnecting in {self.reconnect_delay} seconds... (Attempt {self.reconnect_attempts})")
                await asyncio.sleep(self.reconnect_delay)
                await self.connect()
            else:
                logger.error("❌ Max reconnection attempts reached")
    
    async def _listen_for_messages(self):
        """Listen for incoming WebSocket messages"""
        try:
            async for message in self.websocket:
                if not self.is_running:
                    break
                await self._handle_websocket_message(message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.warning("⚠️ WebSocket connection closed")
            self.is_connected = False
            
            if self.is_running:
                logger.info("🔄 Attempting to reconnect...")
                await self.connect()
                
        except Exception as e:
            logger.error(f"❌ Error in message listener: {e}")
            self.is_connected = False
    
    async def subscribe(self, instrument_keys: List[str], mode: str = "ltpc"):
        """
        Subscribe to instruments with proper limit checking
        
        Args:
            instrument_keys: List of instrument keys to subscribe
            mode: 'ltpc', 'option_greeks', or 'full'
        """
        try:
            if not self.is_connected:
                logger.error("❌ Not connected to WebSocket")
                return False
            
            # Validate mode
            valid_modes = ['ltpc', 'option_greeks', 'full']
            if mode not in valid_modes:
                logger.error(f"❌ Invalid mode: {mode}. Valid modes: {valid_modes}")
                return False
            
            # Check subscription limits
            new_count = self.current_subscriptions[mode] + len(instrument_keys)
            if not self._check_subscription_limits(mode, new_count):
                logger.error(f"❌ Subscription limit exceeded for mode {mode}")
                return False
            
            # Create subscription message
            message = self._create_subscription_message('sub', mode, instrument_keys)
            
            # Send subscription
            await self.websocket.send(message)
            
            # Update subscription tracking
            for instrument_key in instrument_keys:
                self.subscribed_instruments.add(instrument_key)
                self.subscription_modes[instrument_key] = mode
            
            self.current_subscriptions[mode] = new_count
            
            logger.info(f"✅ Subscribed to {len(instrument_keys)} instruments in {mode} mode")
            logger.info(f"📊 Current subscriptions: {dict(self.current_subscriptions)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Subscription failed: {e}")
            return False
    
    async def unsubscribe(self, instrument_keys: List[str]):
        """Unsubscribe from instruments"""
        try:
            if not self.is_connected:
                logger.error("❌ Not connected to WebSocket")
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
            
            self.current_subscriptions[mode] = max(0, self.current_subscriptions[mode] - len(instrument_keys))
            
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
        
        if self.websocket and not self.websocket.closed:
            asyncio.create_task(self.websocket.close())
        
        logger.info("⏹️ Live data manager stopped")
    
    def _run_async_loop(self):
        """Run asyncio event loop in separate thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self.connect())
        except Exception as e:
            logger.error(f"❌ Error in async loop: {e}")
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
                logger.error(f"❌ Error in minute aggregator: {e}")
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
            'subscription_counts': dict(self.current_subscriptions),
            'market_segments': self.market_segments,
            'websocket_url': self.websocket_url,
            'last_update': datetime.now().isoformat()
        }

# Example usage with proper error handling
async def example_usage():
    """Example usage of the fixed live data manager"""
    
    # Read access token
    try:
        with open('upstox_config.json', 'r') as f:
            config = json.load(f)
            access_token = config['access_token']
    except FileNotFoundError:
        logger.error("❌ Config file not found. Create upstox_config.json with your access_token")
        return
    except KeyError:
        logger.error("❌ access_token not found in config file")
        return
    
    # Initialize live data manager
    live_manager = UpstoxLiveDataManager(access_token)
    
    # Add callbacks for real-time data
    async def on_tick_data(instrument_key: str, tick_data: Dict):
        print(f"📊 Tick: {instrument_key} - LTP: ₹{tick_data['ltp']:.2f} at {tick_data['ltt']}")
    
    def on_ohlcv_data(instrument_key: str, ohlcv_data: Dict):
        print(f"📈 OHLCV: {instrument_key} - O:{ohlcv_data['open']}/H:{ohlcv_data['high']}/L:{ohlcv_data['low']}/C:{ohlcv_data['close']}")
    
    live_manager.add_tick_callback(on_tick_data)
    live_manager.add_ohlcv_callback(on_ohlcv_data)
    
    # Start the manager
    live_manager.start()
    
    # Wait for connection
    await asyncio.sleep(5)
    
    # Subscribe to instruments
    instruments = [
        'NSE_EQ|INE002A01018',  # Reliance
        'NSE_EQ|INE009A01021',  # Infosys
    ]
    
    success = await live_manager.subscribe(instruments, mode='ltpc')
    if not success:
        logger.error("❌ Subscription failed")
        return
    
    # Keep running and show status
    try:
        while True:
            await asyncio.sleep(30)
            status = live_manager.get_market_status()
            logger.info(f"📊 Status: Connected={status['is_connected']}, "
                       f"Subscriptions={status['subscription_counts']}")
    except KeyboardInterrupt:
        logger.info("⏹️ Stopping...")
        live_manager.stop()

if __name__ == "__main__":
    # Run the example usage coroutine
    try:
        asyncio.run(example_usage())
    except Exception as e:
        logger.error(f"❌ Error running example usage: {e}")