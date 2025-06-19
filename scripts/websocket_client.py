import asyncio
import ssl
import json
import requests
import websockets
import logging
import os
from datetime import datetime
from typing import List, Optional, Dict, Any
import pytz
import MarketDataFeed_pb2 as pb  # Generated from Upstox's .proto

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzV0NXTUQiLCJqdGkiOiI2ODUzOGYyMmM3ZGY1ODY5ZGNkMzFjN2UiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc1MDMwNjU5NCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzUwMzcwNDAwfQ.3iS8u5juSzMvacktFY1JCFoVnjSP2lxv0oL8BUv7ya8"
GUID = "unique-guid-001"  # Not used in v3 but kept for compatibility
INSTRUMENT_KEYS = ["NSE_EQ|RELIANCE", "NSE_EQ|TCS"]  # These are just for reference
MODE = "full"  # Not used in v3 - you get whatever is configured in Upstox app
MAX_RETRIES = 3
RETRY_DELAY = 5

# ─── LOGGING SETUP WITH IST TIMESTAMP ─────────────────────────────────────────
# Create logs directory
os.makedirs("CUPCAKE/logs", exist_ok=True)

# Get IST timestamp for filename
ist = pytz.timezone('Asia/Kolkata')
timestamp = datetime.now(ist).strftime("%Y%m%d_%H%M%S")
log_filename = f"CUPCAKE/logs/upstox_feed_{timestamp}.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Log the start
logger.info(f"🚀 Starting Upstox WebSocket v3 Client - Log: {log_filename}")

class UpstoxWebSocketV3Client:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.ws = None
        self.is_connected = False
        
        # Data storage
        self.market_data = {}
        self.market_status = {}
        
        # Callbacks
        self.on_tick_callback = None
        self.on_market_status_callback = None
        self.on_error_callback = None
        
        # Message counters
        self.total_messages = 0
        self.market_info_count = 0
        self.live_feed_count = 0

    def set_callbacks(self, on_tick=None, on_market_status=None, on_error=None):
        """Set callback functions for different events"""
        self.on_tick_callback = on_tick
        self.on_market_status_callback = on_market_status
        self.on_error_callback = on_error

    def get_ws_url(self) -> str:
        """Get authorized WebSocket URL from Upstox API"""
        try:
            resp = requests.get(
                "https://api.upstox.com/v3/feed/market-data-feed/authorize",
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Accept": "application/json"
                },
                timeout=30
            )
            resp.raise_for_status()
            
            data = resp.json().get("data", {})
            ws_url = data.get("authorized_redirect_uri")
            
            if not ws_url:
                raise RuntimeError(f"No WebSocket URL in response: {resp.text}")
                
            logger.info(f"✅ Got authorized WebSocket URL")
            return ws_url
            
        except requests.RequestException as e:
            logger.error(f"❌ Failed to get WebSocket URL: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error getting WebSocket URL: {e}")
            raise

    def parse_tick_data(self, instrument_key: str, feed) -> Dict[str, Any]:
        """Parse and structure tick data"""
        if not feed.HasField("fullFeed"):
            return {}
            
        ff = feed.fullFeed.marketFF
        
        # Basic tick data
        tick_data = {
            "instrument_key": instrument_key,
            "timestamp": datetime.now(ist).isoformat(),
        }
        
        # LTP data
        if ff.HasField("ltpc"):
            lt = ff.ltpc
            tick_data.update({
                "ltp": lt.ltp,
                "ltt": lt.ltt,  # Last trade time
                "ltq": lt.ltq,  # Last trade quantity
                "close": lt.cp   # Previous close price
            })
        
        # Bid/Ask data
        if ff.HasField("marketLevel") and ff.marketLevel.bidAskQuote:
            bq = ff.marketLevel.bidAskQuote[0]  # Best bid/ask
            tick_data.update({
                "bid_price": bq.bidP,
                "bid_quantity": bq.bidQ,
                "ask_price": bq.askP,
                "ask_quantity": bq.askQ
            })
        
        # OHLC data (1-minute)
        if ff.HasField("marketOHLC"):
            one_min = next((o for o in ff.marketOHLC.ohlc if o.interval == "I1"), None)
            if one_min:
                tick_data.update({
                    "ohlc_1m": {
                        "open": one_min.open,
                        "high": one_min.high,
                        "low": one_min.low,
                        "close": one_min.close,
                        "volume": one_min.vol
                    }
                })
        
        # Additional data
        tick_data.update({
            "open_interest": ff.oi,
            "average_trade_price": ff.atp,
            "implied_volatility": ff.iv,
            "total_buy_quantity": ff.tbq,
            "total_sell_quantity": ff.tsq,
            "volume": ff.vtt  # Volume traded today
        })
        
        return tick_data

    def get_market_status_name(self, status_code: int) -> str:
        """Convert numeric status code to readable name"""
        status_map = {
            0: "PRE_OPEN_START",
            1: "PRE_OPEN_END", 
            2: "NORMAL_OPEN",
            3: "NORMAL_CLOSE",
            4: "CLOSING_START",
            5: "CLOSING_END"
        }
        return status_map.get(status_code, f"UNKNOWN_{status_code}")

    async def process_messages(self):
        """Process incoming WebSocket messages - v3 is receive-only!"""
        try:
            logger.info("📨 Starting to receive messages (v3 receive-only mode)")
            
            while self.is_connected:
                raw_message = await self.ws.recv()
                self.total_messages += 1
                
                response = pb.FeedResponse()
                response.ParseFromString(raw_message)
                
                # Handle market status updates (type 2 = MARKET_INFO)
                if response.type == 2:
                    self.market_info_count += 1
                    
                    # Convert numeric status codes to readable names
                    market_info = {}
                    for segment, status_code in response.marketInfo.segmentStatus.items():
                        market_info[segment] = self.get_market_status_name(status_code)
                    
                    self.market_status = market_info
                    logger.info(f"📊 Market Status Update #{self.market_info_count}: {market_info}")
                    
                    if self.on_market_status_callback:
                        await self.on_market_status_callback(market_info)
                
                # Handle live feed data (type 1 = LIVE_FEED)
                elif response.type == 1:
                    self.live_feed_count += 1
                    
                    logger.info(f"📈 Live Feed #{self.live_feed_count}: {len(response.feeds)} instruments")
                    
                    # Process all instruments in the feed
                    for instrument_key, feed in response.feeds.items():
                        tick_data = self.parse_tick_data(instrument_key, feed)
                        if tick_data:
                            self.market_data[instrument_key] = tick_data
                            
                            if self.on_tick_callback:
                                await self.on_tick_callback(tick_data)
                            else:
                                # Default logging
                                ltp = tick_data.get('ltp', 'N/A')
                                bid = tick_data.get('bid_price', 'N/A')
                                ask = tick_data.get('ask_price', 'N/A')
                                volume = tick_data.get('volume', 'N/A')
                                logger.info(f"💰 {instrument_key}: LTP={ltp}, Bid={bid}, Ask={ask}, Vol={volume}")
                
                # Handle initial feed (type 0 = INITIAL_FEED)
                elif response.type == 0:
                    logger.info(f"🎯 Initial Feed: {len(response.feeds)} instruments")
                    # Same processing as live feed
                    for instrument_key, feed in response.feeds.items():
                        tick_data = self.parse_tick_data(instrument_key, feed)
                        if tick_data:
                            self.market_data[instrument_key] = tick_data
                            ltp = tick_data.get('ltp', 'N/A')
                            logger.info(f"🎯 Initial: {instrument_key} = {ltp}")
                
                else:
                    logger.debug(f"🔍 Unknown message type: {response.type}")
                
                # Log stats every 100 messages
                if self.total_messages % 100 == 0:
                    logger.info(f"📊 Stats: Total={self.total_messages}, MarketInfo={self.market_info_count}, LiveFeed={self.live_feed_count}")
                
        except websockets.ConnectionClosed as e:
            logger.warning(f"⚠️ WebSocket connection closed: code={e.code}, reason={e.reason}")
            self.is_connected = False
        except Exception as e:
            logger.error(f"❌ Error processing messages: {e}")
            if self.on_error_callback:
                await self.on_error_callback(e)
            raise

    async def connect_with_retry(self):
        """Connect with automatic retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                await self.connect()
                return
            except Exception as e:
                logger.error(f"❌ Connection attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"⏳ Retrying in {RETRY_DELAY} seconds...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error("❌ All connection attempts failed")
                    raise

    async def connect(self):
        """Main connection method - v3 WebSocket is receive-only"""
        try:
            # Get authorized WebSocket URL (pre-authenticated with embedded code)
            ws_url = self.get_ws_url()
            
            # Create SSL context
            ssl_context = ssl.create_default_context()
            
            # Connect to WebSocket
            logger.info(f"🔗 Connecting to WebSocket...")
            self.ws = await websockets.connect(ws_url, ssl=ssl_context)
            self.is_connected = True
            logger.info("✅ WebSocket connected")
            
            # v3 WebSocket is pre-authenticated and receive-only
            # The instruments you receive depend on your Upstox app subscriptions
            logger.info("✅ Ready to receive data (v3 is pre-authenticated, receive-only)")
            logger.info("ℹ️  Note: You'll receive data for instruments subscribed in your Upstox app")
            
            # Start processing messages - no subscription needed
            await self.process_messages()
            
        except Exception as e:
            self.is_connected = False
            logger.error(f"❌ Connection error: {e}")
            raise
        finally:
            if self.ws:
                await self.ws.close()

    async def disconnect(self):
        """Gracefully disconnect"""
        self.is_connected = False
        if self.ws:
            await self.ws.close()
        logger.info("🔌 Disconnected from WebSocket")

    def get_latest_data(self, instrument_key: str = None) -> Dict[str, Any]:
        """Get latest market data"""
        if instrument_key:
            return self.market_data.get(instrument_key, {})
        return self.market_data.copy()

    def get_stats(self) -> Dict[str, int]:
        """Get connection statistics"""
        return {
            "total_messages": self.total_messages,
            "market_info_count": self.market_info_count,
            "live_feed_count": self.live_feed_count,
            "instruments_tracked": len(self.market_data)
        }

# ─── USAGE EXAMPLE ─────────────────────────────────────────────────────────────
async def on_tick(tick_data):
    """Custom tick handler"""
    instrument = tick_data['instrument_key']
    ltp = tick_data.get('ltp', 'N/A')
    volume = tick_data.get('volume', 'N/A')
    bid = tick_data.get('bid_price', 'N/A')
    ask = tick_data.get('ask_price', 'N/A')
    
    # Only log major stocks to avoid spam
    if any(stock in instrument for stock in ['RELIANCE', 'TCS', 'INFY', 'HDFC']):
        print(f"🔥 {instrument}: LTP={ltp}, Volume={volume}, Spread={bid}-{ask}")

async def on_market_status(status):
    """Custom market status handler"""
    print(f"📈 Market Status: {status}")

async def on_error(error):
    """Custom error handler"""
    print(f"⚠️ Error: {error}")

async def main():
    # Initialize client - much simpler for v3!
    client = UpstoxWebSocketV3Client(access_token=ACCESS_TOKEN)
    
    # Set callbacks
    client.set_callbacks(
        on_tick=on_tick,
        on_market_status=on_market_status,
        on_error=on_error
    )
    
    try:
        # Connect with retry logic
        logger.info("🏁 Starting Upstox WebSocket v3 receive-only client...")
        await client.connect_with_retry()
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
        stats = client.get_stats()
        logger.info(f"📊 Final Stats: {stats}")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())