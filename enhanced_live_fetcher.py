# Enhanced Live Market Data Processor with Unified Signal Generation
# Fixed import issues and circular dependencies

import os
import asyncio
import json
import ssl
import websockets
import requests
from google.protobuf.json_format import MessageToDict
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
from typing import Dict
import time
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import threading
import statistics
import redis
import numpy as np

# Import modules with proper error handling
try:
    from historical_data_fetcher import FastUpstoxAPI, Candle
    from technical_indicators import EnhancedTechnicalIndicators
    from orderbook_analyzer import OrderBookAnalyzer
    from practical_quant_engine import PracticalQuantEngine
    from enhanced_signal_generator import TrajectorySignalGenerator
    from unified_signal_integration import build_unified_signal_manager
    print("✓ Successfully imported technical, quant, and orderbook analyzers")
except ImportError as e:
    print(f"⚠ Import error for enhanced modules: {e}")
    # Create dummy classes as fallback
    class EnhancedTechnicalIndicators:
        def __init__(self, *args, **kwargs):
            print("⚠ Using dummy EnhancedTechnicalIndicators")
        def get_signal_analytics(self, *args, **kwargs):
            return {}
        def calculate_all_indicators(self, *args, **kwargs):
            return {}
        def get_ohlcv_data(self, *args, **kwargs):
            import pandas as pd
            return pd.DataFrame()
    
    class OrderBookAnalyzer:
        def __init__(self, *args, **kwargs):
            print("⚠ Using dummy OrderBookAnalyzer")
        def comprehensive_orderbook_analysis(self, *args, **kwargs):
            return {}
    
    class PracticalQuantEngine:
        def calculate_comprehensive_score(self, *args, **kwargs):
            return {}
    
    class TrajectorySignalGenerator:
        def add_market_data(self, *args, **kwargs):
            return None
        def analyze_price_trajectory(self, *args, **kwargs):
            return {}

if 'build_unified_signal_manager' not in globals():
    def build_unified_signal_manager(*args, **kwargs):
        class _NullManager:
            def start(self):
                return None
            def stop(self):
                return None
            def update_symbol_snapshot(self, *args, **kwargs):
                return None
            def update_price(self, *args, **kwargs):
                return None
            def get_signal(self, *args, **kwargs):
                return None
            def get_all_signals(self):
                return []

        return _NullManager()

import MarketDataFeedV3_pb2 as pb

HERE = os.path.dirname(__file__)
JSON_PATH = os.path.join(HERE, 'credentials.json')
SYMBOLS_PATH = os.path.join(HERE, 'symbols.json')

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Flask app for web dashboard
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

try:
    unified_signal_manager = build_unified_signal_manager(socketio)
except Exception as manager_error:
    print(f"⚠ Could not start unified signal manager: {manager_error}")
    unified_signal_manager = None

# Initialize enhanced analyzers
tech_indicators = EnhancedTechnicalIndicators()
orderbook_analyzer = OrderBookAnalyzer()
quant_engine = PracticalQuantEngine()
trajectory_generator = TrajectorySignalGenerator()


def set_socketio_instance(new_socketio):
    """Use the parent app's Socket.IO instance for unified emissions."""
    global socketio, unified_signal_manager
    socketio = new_socketio
    if unified_signal_manager is not None:
        try:
            unified_signal_manager.socketio = new_socketio
        except Exception:
            pass


def bootstrap_unified_signal_inputs(instrument_keys):
    """Seed unified snapshots from latest historical candles when live ticks are absent."""
    if not unified_signal_manager:
        return

    seeded_count = 0
    for instrument_key in instrument_keys:
        symbol_name = symbol_mapping.get(instrument_key, instrument_key)
        try:
            candles = tech_indicators.get_ohlcv_data(symbol_name)
            if candles is None or candles.empty:
                continue

            last_row = candles.iloc[-1]
            close_price = float(last_row.get('close', 0) or 0)
            if close_price <= 0:
                continue

            market_snapshot = market_data[instrument_key]
            market_snapshot['name'] = symbol_name
            market_snapshot['ltp'] = close_price
            market_snapshot['ltq'] = int(last_row.get('volume', 0) or 0)
            market_snapshot['best_bid'] = close_price
            market_snapshot['best_ask'] = close_price
            market_snapshot['last_update'] = datetime.now(IST)
            seeded_count += 1
        except Exception as exc:
            debug_log(f"Failed to bootstrap {symbol_name}: {exc}", "ERROR")

    if seeded_count > 0:
        refresh_unified_signal_inputs()
        try:
            unified_signal_manager.recalculate_now()
        except Exception as exc:
            debug_log(f"Bootstrap signal recalculation failed: {exc}", "ERROR")
        debug_log(f"✅ Bootstrapped unified signals for {seeded_count} symbols", "SUCCESS")

# Redis client for data storage
try:
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', '6379'))
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
    redis_client.ping()  # Test connection
    print(f"✓ Redis connection successful at {redis_host}:{redis_port}")
except redis.RedisError as e:
    print(f"⚠ Redis connection failed: {e}")
    redis_client = None

if unified_signal_manager and redis_client:
    try:
        unified_signal_manager.set_redis_client(redis_client)
    except Exception:
        pass

# Symbol mapping and data structures
symbol_mapping = {}
debug_stats = {
    'total_messages': 0,
    'processed_ticks': 0,
    'dashboard_updates': 0,
    'last_message_time': None,
    'successful_ticks': 0,
    'unified_recalculations': 0
}

# Enhanced data structures
ohlcv_data = defaultdict(lambda: {
    'name': '',
    'open': None,
    'high': None,
    'low': None,
    'close': None,
    'volume': 0,
    'tick_count': 0,
    'first_tick_time': None,
    'last_tick_time': None
})

# Real-time market data
market_data = defaultdict(lambda: {
    'name': '',
    'ltp': None,
    'ltq': None,
    'ltt': None,
    'best_bid': None,
    'best_ask': None,
    'bid_size': None,
    'ask_size': None,
    'spread': None,
    'spread_pct': None,
    'total_bid_depth': 0,
    'total_ask_depth': 0,
    'bid_ask_ratio': None,
    'market_pressure': None,
    'tick_direction': None,
    'vwap_bid': None,
    'vwap_ask': None,
    'last_update': None,
    'tick_count': 0,
    'bid_levels': [],
    'ask_levels': []
})

# Tracking variables for minute intervals
current_minute_start = None
current_minute_end = None
collecting_data = False

def debug_log(message, level="INFO"):
    """Enhanced debug logging."""
    timestamp = datetime.now(IST).strftime('%H:%M:%S.%f')[:-3]
    print(f"[{timestamp}] {level}: {message}")

    try:
        if socketio:
            socketio.emit('debug_log', {
                'timestamp': timestamp,
                'level': level,
                'message': message
            })
    except Exception as e:
        pass  # Ignore SocketIO errors

def get_access_token():
    """Get access token from credentials file"""
    try:
        with open(JSON_PATH, 'r') as f:
            data = json.load(f)
        return data['access_token']
    except FileNotFoundError:
        debug_log(f"Credentials file not found: {JSON_PATH}", "ERROR")
        return None
    except Exception as e:
        debug_log(f"Error reading credentials: {e}", "ERROR")
        return None

def load_symbols():
    """Load symbols from symbols.json file."""
    try:
        with open(SYMBOLS_PATH, 'r') as f:
            data = json.load(f)

        instrument_keys = []
        for symbol in data['symbols']:
            instrument_key = symbol['instrument_key']
            instrument_keys.append(instrument_key)
            symbol_mapping[instrument_key] = symbol['name']
            ohlcv_data[instrument_key]['name'] = symbol['name']
            market_data[instrument_key]['name'] = symbol['name']

        debug_log(f"Loaded {len(instrument_keys)} symbols for enhanced analysis")
        return instrument_keys
    except FileNotFoundError:
        debug_log(f"Symbols file not found: {SYMBOLS_PATH}", "ERROR")
        return []
    except Exception as e:
        debug_log(f"Error loading symbols: {e}", "ERROR")
        return []

def save_minute_ohlcv_to_redis(instrument_key, ohlcv_record):
    """Save completed minute OHLCV data to Redis TimeSeries and update cache"""
    if not redis_client:
        return
        
    try:
        symbol_name = symbol_mapping.get(instrument_key, instrument_key)
        symbol_clean = symbol_name.replace(' ', '_').replace('&', 'and')

        if not ohlcv_record['open']:
            return

        # Create timestamp in milliseconds
        timestamp_ms = int(ohlcv_record['last_tick_time'].timestamp() * 1000)

        # Save each OHLCV component
        data_points = {
            'open': ohlcv_record['open'],
            'high': ohlcv_record['high'],
            'low': ohlcv_record['low'],
            'close': ohlcv_record['close'],
            'volume': ohlcv_record['volume']
        }

        for data_type, value in data_points.items():
            key = f"stock:{symbol_clean}:{data_type}"

            # Ensure TimeSeries exists
            try:
                if not redis_client.exists(key):
                    redis_client.execute_command(
                        'TS.CREATE', key,
                        'DUPLICATE_POLICY', 'LAST',
                        'LABELS', 'symbol', symbol_clean, 'data_type', data_type, 'source', 'live_feed'
                    )
            except:
                pass  # Key might already exist

            # Add data point
            try:
                redis_client.execute_command('TS.ADD', key, timestamp_ms, value)
            except Exception as e:
                debug_log(f"Error adding {data_type} for {symbol_name}: {e}", "ERROR")

        debug_log(f"💾 Saved OHLCV to Redis: {symbol_name} at {ohlcv_record['last_tick_time']}", "REDIS")
        
        # Update the rolling window cache in technical indicators
        tech_indicators.update_cache_with_new_candle(
            symbol_name, 
            ohlcv_record['last_tick_time'], 
            data_points
        )
        debug_log(f"Updated rolling window cache for {symbol_name}", "CACHE")

    except Exception as e:
        debug_log(f"Error saving to Redis for {instrument_key}: {e}", "ERROR")

# Add these new routes to your enhanced_live_fetcher.py file

# --- Enhanced Flask routes for Chart Support ---
@app.route('/api/live/symbols')
def api_live_symbols():
    """API endpoint for currently tracked live symbols"""
    try:
        symbols = []
        for instrument_key, data in market_data.items():
            if data['ltp'] is not None:
                symbols.append({
                    'symbol': data['name'],
                    'symbol_clean': data['name'].replace(' ', '_').replace('&', 'and'),
                    'instrument_key': instrument_key,
                    'latest_price': data['ltp'],
                    'data_points': data.get('tick_count', 0),
                    'return_pct': 0.0,  # Could calculate from first tick
                    'last_update': data.get('last_update').isoformat() if data.get('last_update') else None
                })
        
        return jsonify(symbols)
    except Exception as e:
        debug_log(f"Error in api_live_symbols: {e}", "ERROR")
        return jsonify({'error': str(e)})

@app.route('/api/live/chart_data/<symbol_name>')
def api_live_chart_data(symbol_name):
    """API endpoint for live chart data from Redis TimeSeries"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'})
        
        # Clean symbol name to match Redis keys
        symbol_clean = symbol_name.replace(' ', '_').replace('&', 'and')
        
        # Get query parameters
        timeframe = request.args.get('timeframe', '1h')
        limit = min(int(request.args.get('limit', 100)), 500)
        
        # Calculate time range
        end_time = int(time.time() * 1000)
        
        # Determine how far back to look based on timeframe and limit
        if timeframe == '1h':
            start_time = end_time - (limit * 60 * 60 * 1000)  # Hours back
        elif timeframe == '4h':
            start_time = end_time - (limit * 4 * 60 * 60 * 1000)  # 4-hour periods back
        elif timeframe == '1d':
            start_time = end_time - (limit * 24 * 60 * 60 * 1000)  # Days back
        else:
            start_time = end_time - (limit * 60 * 1000)  # Minutes back (default)
        
        # Fetch OHLCV data from Redis
        candles_data = {}
        for data_type in ['open', 'high', 'low', 'close', 'volume']:
            key = f"stock:{symbol_clean}:{data_type}"
            try:
                result = redis_client.execute_command('TS.RANGE', key, start_time, end_time)
                if result:
                    candles_data[data_type] = result
            except Exception as e:
                debug_log(f"Error fetching {data_type} for {symbol_name}: {e}", "ERROR")
        
        if not candles_data.get('close'):
            return jsonify({'error': f'No data found for {symbol_name}'})
        
        # Process data into OHLCV format
        close_data = candles_data['close']
        candles = []
        
        # Group data by timeframe if needed
        if timeframe in ['1h', '4h', '1d']:
            candles = aggregate_to_timeframe(candles_data, timeframe)
        else:
            # Use minute data as-is
            for i, (timestamp, close_price) in enumerate(close_data[-limit:]):
                candle = {
                    'x': datetime.fromtimestamp(timestamp/1000, tz=IST).isoformat(),
                    'c': float(close_price)
                }
                
                # Add OHLV if available
                if candles_data.get('open') and i < len(candles_data['open']):
                    candle['o'] = float(candles_data['open'][i][1])
                if candles_data.get('high') and i < len(candles_data['high']):
                    candle['h'] = float(candles_data['high'][i][1])
                if candles_data.get('low') and i < len(candles_data['low']):
                    candle['l'] = float(candles_data['low'][i][1])
                if candles_data.get('volume') and i < len(candles_data['volume']):
                    candle['v'] = int(candles_data['volume'][i][1])
                
                candles.append(candle)
        
        if not candles:
            return jsonify({'error': f'No processable data for {symbol_name}'})
        
        # Calculate statistics
        latest_price = candles[-1]['c']
        first_price = candles[0].get('c', latest_price)
        price_change = latest_price - first_price
        price_change_pct = (price_change / first_price * 100) if first_price > 0 else 0
        
        # Get high/low from candles
        all_prices = [c['c'] for c in candles]
        high_24h = max(all_prices)
        low_24h = min(all_prices)
        
        # Calculate volume
        volume_24h = sum(c.get('v', 0) for c in candles)
        
        return jsonify({
            'symbol': symbol_name,
            'timeframe': timeframe,
            'candles': candles,
            'stats': {
                'latest_price': latest_price,
                'price_change': price_change,
                'price_change_pct': price_change_pct,
                'volume_24h': volume_24h,
                'high_24h': high_24h,
                'low_24h': low_24h,
                'data_points': len(candles)
            },
            'data_range': {
                'start': candles[0]['x'],
                'end': candles[-1]['x']
            },
            'source': 'live_redis_timeseries'
        })
        
    except Exception as e:
        debug_log(f"Error in api_live_chart_data for {symbol_name}: {e}", "ERROR")
        return jsonify({'error': str(e)})

def aggregate_to_timeframe(candles_data, timeframe):
    """Aggregate minute data to specified timeframe"""
    try:
        import pandas as pd
        from collections import defaultdict
        
        # Convert to DataFrame for easier aggregation
        close_data = candles_data.get('close', [])
        if not close_data:
            return []
        
        # Create base DataFrame
        df_data = []
        for timestamp, close_price in close_data:
            dt = datetime.fromtimestamp(timestamp/1000, tz=IST)
            df_data.append({
                'timestamp': dt,
                'close': float(close_price),
                'open': float(close_price),  # Default values
                'high': float(close_price),
                'low': float(close_price),
                'volume': 0
            })
        
        # Add other OHLCV data if available
        for data_type in ['open', 'high', 'low', 'volume']:
            if candles_data.get(data_type):
                for i, (timestamp, value) in enumerate(candles_data[data_type]):
                    if i < len(df_data):
                        df_data[i][data_type] = float(value) if data_type != 'volume' else int(value)
        
        df = pd.DataFrame(df_data)
        df.set_index('timestamp', inplace=True)
        
        # Resample based on timeframe
        freq_map = {'1h': '1H', '4h': '4H', '1d': '1D'}
        freq = freq_map.get(timeframe, '1H')
        
        df_resampled = df.resample(freq).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # Convert back to candles format
        candles = []
        for timestamp, row in df_resampled.iterrows():
            candles.append({
                'x': timestamp.isoformat(),
                'o': float(row['open']),
                'h': float(row['high']),
                'l': float(row['low']),
                'c': float(row['close']),
                'v': int(row['volume'])
            })
        
        return candles
        
    except Exception as e:
        debug_log(f"Error aggregating to timeframe {timeframe}: {e}", "ERROR")
        return []

# Add this route to provide real-time market status
@app.route('/api/live/market_status')
def api_live_market_status():
    """Enhanced market status with live data statistics"""
    try:
        # Get basic market status
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        
        # Market hours: 9:15 AM to 3:30 PM IST, Monday to Friday
        market_open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        is_weekday = now.weekday() < 5
        is_market_hours = market_open_time <= now <= market_close_time
        is_market_open = is_weekday and is_market_hours
        
        # Calculate live data statistics
        active_symbols = len([k for k, v in market_data.items() if v['ltp'] is not None])
        total_ticks = debug_stats['processed_ticks']
        signals_generated = debug_stats['unified_recalculations']
        
        # Time until market open/close
        if is_market_open:
            time_until_close = market_close_time - now
            status_message = f"Market closes in {time_until_close}"
        elif is_weekday and now < market_open_time:
            time_until_open = market_open_time - now
            status_message = f"Market opens in {time_until_open}"
        else:
            status_message = "Market closed"
        
        return jsonify({
            'is_open': is_market_open,
            'current_time': now.isoformat(),
            'market_open': market_open_time.isoformat(),
            'market_close': market_close_time.isoformat(),
            'status_message': status_message,
            'is_weekend': not is_weekday,
            'live_stats': {
                'active_symbols': active_symbols,
                'total_ticks_processed': total_ticks,
                'signals_generated': signals_generated,
                'last_update': debug_stats.get('last_message_time').isoformat() if debug_stats.get('last_message_time') else None
            }
        })
        
    except Exception as e:
        debug_log(f"Error in api_live_market_status: {e}", "ERROR")
        return jsonify({'error': str(e)})

def _clamp(value: float, minimum: float = -1.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def build_price_block(symbol_name: str, snapshot: Dict, indicators: Dict) -> Dict:
    price_block = {
        'symbol': symbol_name,
        'ltp': snapshot.get('ltp'),
        'ltq': snapshot.get('ltq'),
        'best_bid': snapshot.get('best_bid'),
        'best_ask': snapshot.get('best_ask'),
        'spread_pct': snapshot.get('spread_pct'),
        'timestamp': snapshot.get('last_update').isoformat() if snapshot.get('last_update') else None
    }
    if indicators:
        price_block['atr'] = indicators.get('atr')
        price_block['close'] = indicators.get('ohlcv', {}).get('close')
        price_block['high'] = indicators.get('ohlcv', {}).get('high')
        price_block['low'] = indicators.get('ohlcv', {}).get('low')
    return price_block


def build_technical_component(indicators: Dict) -> Dict:
    if not indicators:
        return {}
    sma_values = indicators.get('sma', {})
    sma5 = sma_values.get('sma_5')
    sma20 = sma_values.get('sma_20')
    signal_info = indicators.get('signals', {})

    score = 0.0
    if sma5 and sma20 and sma20 != 0:
        score = _clamp((sma5 - sma20) / sma20, -1.0, 1.0)

    direction = signal_info.get('overall_signal', 'HOLD')
    confidence = signal_info.get('confidence', abs(score))
    reasons = signal_info.get('reasons', [])

    return {
        'score': score,
        'direction': direction,
        'confidence': min(1.0, confidence),
        'reason': '; '.join(reasons[:3]),
        'momentum': indicators.get('rsi'),
        'volatility': indicators.get('atr'),
        'signals': signal_info
    }


def build_quant_component(symbol_name: str) -> Dict:
    try:
        df = tech_indicators.get_ohlcv_data(symbol_name)
        if df.empty:
            return {}
        analysis = quant_engine.calculate_comprehensive_score(df)
        final_score = analysis.get('final_score', 0.5)
        normalized = _clamp((final_score - 0.5) * 2, -1.0, 1.0)
        recommendation = analysis.get('recommendation', 'HOLD')
        confidence = min(1.0, abs(normalized))
        return {
            'score': normalized,
            'confidence': confidence,
            'direction': 'BUY' if 'BUY' in recommendation else 'SELL' if 'SELL' in recommendation else 'HOLD',
            'reason': f"Quant composite {final_score:.2f} ({recommendation})",
            'details': analysis
        }
    except Exception as exc:
        debug_log(f"Quant analysis failed for {symbol_name}: {exc}", "ERROR")
        return {}


def build_orderbook_component(symbol_name: str, snapshot: Dict) -> Dict:
    try:
        analysis = orderbook_analyzer.comprehensive_orderbook_analysis(symbol_name, snapshot)
        assessment = analysis.get('overall_assessment', {})
        sentiment = assessment.get('market_sentiment', 'NEUTRAL')
        confidence = assessment.get('confidence', 0.5)
        direction = 'BUY' if sentiment == 'BULLISH' else 'SELL' if sentiment == 'BEARISH' else 'HOLD'
        score = _clamp(confidence * (1 if direction == 'BUY' else -1 if direction == 'SELL' else 0), -1.0, 1.0)
        return {
            'score': score,
            'confidence': confidence,
            'direction': direction,
            'reason': ', '.join(assessment.get('key_factors', [])),
            'analysis': analysis
        }
    except Exception as exc:
        debug_log(f"Orderbook analysis failed for {symbol_name}: {exc}", "ERROR")
        return {}


def build_market_regime_component(indicators: Dict, quant_block: Dict) -> Dict:
    if not indicators:
        return {}
    regime = indicators.get('market_regime', 'UNKNOWN')
    score = 0.0
    if regime == 'TRENDING':
        score = 0.6
    elif regime == 'RANGING':
        score = 0.2
    direction = 'BUY' if regime == 'TRENDING' else 'HOLD'
    if quant_block:
        vol_score = quant_block.get('details', {}).get('detailed_analysis', {}).get('volatility_regime', {}).get('vol_score')
        if vol_score is not None:
            score = _clamp(vol_score, 0.0, 1.0)
    return {
        'score': score,
        'direction': direction,
        'confidence': min(1.0, score),
        'regime': regime
    }


def build_daily_context(indicators: Dict) -> Dict:
    if not indicators:
        return {}
    sma_values = indicators.get('sma', {})
    sma20 = sma_values.get('sma_20')
    sma50 = sma_values.get('sma_50')
    if not sma20 or not sma50:
        return {}
    bias = _clamp((sma20 - sma50) / sma50 if sma50 else 0, -1.0, 1.0)
    direction = 'BUY' if bias >= 0 else 'SELL'
    confidence = min(1.0, abs(bias) * 2)
    return {
        'daily_trend': {
            'direction': direction,
            'bias': bias,
            'confidence': confidence
        }
    }


def build_trajectory_component(symbol_name: str, snapshot: Dict, indicators: Dict) -> Dict:
    if not trajectory_generator or not indicators:
        return {}
    try:
        trajectory_generator.add_market_data(symbol_name, snapshot, indicators)
        analysis = trajectory_generator.analyze_price_trajectory(symbol_name)
        if not analysis or analysis.get('status') == 'insufficient_data':
            return {}
        overall = analysis.get('overall_trajectory', 'NEUTRAL')
        direction = 'BUY' if 'BULLISH' in overall else 'SELL' if 'BEARISH' in overall else 'HOLD'
        consistency = analysis.get('trend_consistency', 0)
        score = _clamp(consistency, 0.0, 1.0)
        score = score if direction == 'BUY' else -score if direction == 'SELL' else 0
        confidence = min(1.0, abs(score) + abs(analysis.get('acceleration', 0)))
        return {
            'score': score,
            'confidence': confidence,
            'direction': direction,
            'reason': f"{overall} with consistency {consistency:.2f}",
            'analysis': analysis
        }
    except Exception as exc:
        debug_log(f"Trajectory analysis failed for {symbol_name}: {exc}", "ERROR")
        return {}


def refresh_unified_signal_inputs():
    if not unified_signal_manager:
        return
    updated_any = False
    for instrument_key, snapshot in market_data.items():
        if snapshot.get('ltp') is None:
            continue
        symbol_name = snapshot.get('name') or instrument_key
        try:
            indicators = tech_indicators.calculate_all_indicators(symbol_name)
            technical_component = build_technical_component(indicators)
            quant_component = build_quant_component(symbol_name)
            orderbook_component = build_orderbook_component(symbol_name, snapshot)
            context_block = build_daily_context(indicators)
            trajectory_component = build_trajectory_component(symbol_name, snapshot, indicators)
            regime_component = build_market_regime_component(indicators, quant_component)
            price_block = build_price_block(symbol_name, snapshot, indicators)

            unified_signal_manager.update_symbol_snapshot(
                symbol_name,
                price=price_block,
                technical=technical_component,
                quant=quant_component,
                orderbook=orderbook_component,
                market_regime=regime_component,
                trajectory=trajectory_component,
                context=context_block,
            )
            updated_any = True
        except Exception as exc:
            debug_log(f"Failed to refresh unified inputs for {symbol_name}: {exc}", "ERROR")
    if updated_any:
        debug_stats['unified_recalculations'] += 1


def process_tick_with_enhanced_analysis(instrument_key, tick_data):
    """Enhanced tick processing feeding unified signal inputs"""
    global collecting_data, current_minute_start, current_minute_end

    try:
        debug_stats['processed_ticks'] += 1

        if 'fullFeed' in tick_data and 'marketFF' in tick_data['fullFeed']:
            market_ff = tick_data['fullFeed']['marketFF']

            if 'ltpc' in market_ff and 'ltp' in market_ff['ltpc']:
                ltp = float(market_ff['ltpc']['ltp'])
                ltq = int(market_ff['ltpc'].get('ltq', 0))

                debug_stats['successful_ticks'] += 1
                symbol_name = symbol_mapping.get(instrument_key, instrument_key)

                # Extract exchange timestamp
                exchange_time = None
                if 'ltt' in market_ff['ltpc']:
                    exchange_time = parse_exchange_timestamp(market_ff['ltpc']['ltt'])
                if exchange_time is None:
                    exchange_time = datetime.now(IST)

                # Update market data with orderbook info
                process_bid_ask_data(instrument_key, market_ff, exchange_time)

                # Check if we should finalize current minute
                if collecting_data and should_finalize_minute(exchange_time):
                    finalize_current_minute(exchange_time)
                    return

                # Process OHLCV data if collecting
                if collecting_data and current_minute_start <= exchange_time < current_minute_end:
                    process_ohlcv_tick(instrument_key, ltp, ltq, exchange_time)

                # Minimal periodic logging
                if debug_stats['processed_ticks'] % 200 == 0:
                    debug_log(f"Processed {debug_stats['processed_ticks']} ticks", "STATS")

        else:
            if debug_stats['processed_ticks'] <= 10:
                debug_log(f"❌ WRONG STRUCTURE for {symbol_mapping.get(instrument_key, instrument_key)}", "ERROR")

    except Exception as e:
        debug_log(f"❌ ERROR processing enhanced tick for {instrument_key}: {e}", "ERROR")

def process_ohlcv_tick(instrument_key, ltp, ltq, exchange_time):
    """Process tick for OHLCV data collection"""
    if ohlcv_data[instrument_key]['open'] is None:
        ohlcv_data[instrument_key]['open'] = ltp
        ohlcv_data[instrument_key]['high'] = ltp
        ohlcv_data[instrument_key]['low'] = ltp
        ohlcv_data[instrument_key]['first_tick_time'] = exchange_time

    if ltp > ohlcv_data[instrument_key]['high']:
        ohlcv_data[instrument_key]['high'] = ltp
    if ltp < ohlcv_data[instrument_key]['low']:
        ohlcv_data[instrument_key]['low'] = ltp

    ohlcv_data[instrument_key]['close'] = ltp
    ohlcv_data[instrument_key]['volume'] += ltq
    ohlcv_data[instrument_key]['tick_count'] += 1
    ohlcv_data[instrument_key]['last_tick_time'] = exchange_time

def parse_exchange_timestamp(timestamp_str_or_int):
    """Parse exchange timestamp to IST datetime."""
    try:
        timestamp = float(timestamp_str_or_int)
        if timestamp > 1e12:
            timestamp = timestamp / 1000
        dt_utc = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        return dt_utc.astimezone(IST)
    except Exception as e:
        debug_log(f"Error parsing timestamp {timestamp_str_or_int}: {e}", "ERROR")
        return datetime.now(IST)

def process_bid_ask_data(instrument_key, market_ff, exchange_time):
    """Process bid/ask data for market analysis."""
    try:
        data = market_data[instrument_key]
        data['last_update'] = exchange_time
        data['tick_count'] += 1

        if 'ltpc' in market_ff:
            ltpc = market_ff['ltpc']
            data['ltp'] = float(ltpc.get('ltp', 0))
            data['ltq'] = int(ltpc.get('ltq', 0))
            data['ltt'] = ltpc.get('ltt')

        # Reset spread fields to ensure they are cleared if no data is present
        data['spread'] = 0.0
        data['spread_pct'] = 0.0

        if 'marketLevel' in market_ff and 'bidAskQuote' in market_ff['marketLevel']:
            bid_ask_quote = market_ff['marketLevel']['bidAskQuote']

            if bid_ask_quote and len(bid_ask_quote) > 0:
                best_level = bid_ask_quote[0]

                # Defensively get best bid/ask
                bid_price_raw = best_level.get('bp') or best_level.get('bidP')
                ask_price_raw = best_level.get('ap') or best_level.get('askP')
                bid_qty_raw = best_level.get('bq') or best_level.get('bidQ')
                ask_qty_raw = best_level.get('aq') or best_level.get('askQ')

                data['best_bid'] = float(bid_price_raw) if bid_price_raw is not None else 0.0
                data['best_ask'] = float(ask_price_raw) if ask_price_raw is not None else 0.0
                data['bid_size'] = int(bid_qty_raw) if bid_qty_raw is not None else 0
                data['ask_size'] = int(ask_qty_raw) if ask_qty_raw is not None else 0

                # Calculate spread only if we have valid bid and ask prices
                if data['best_bid'] > 0 and data['best_ask'] > 0:
                    data['spread'] = data['best_ask'] - data['best_bid']
                    data['spread_pct'] = (data['spread'] / data['best_bid']) * 100

                # Build bid and ask level lists
                data['bid_levels'] = [
                    {'price': float(level.get('bp') or level.get('bidP')), 'quantity': int(level.get('bq') or level.get('bidQ'))}
                    for level in bid_ask_quote if (level.get('bp') or level.get('bidP'))
                ][:5]

                data['ask_levels'] = [
                    {'price': float(level.get('ap') or level.get('askP')), 'quantity': int(level.get('aq') or level.get('askQ'))}
                    for level in bid_ask_quote if (level.get('ap') or level.get('askP'))
                ][:5]

    except Exception as e:
        debug_log(f"Error processing bid/ask data for {instrument_key}: {e}", "ERROR")

# Keep all the other functions as they were (wait_for_next_minute, start_minute_collection, etc.)
def wait_for_next_minute():
    """Calculate wait time for next minute boundary"""
    now_ist = datetime.now(IST)
    next_minute = now_ist.replace(second=0, microsecond=0) + timedelta(minutes=1)
    wait_seconds = (next_minute - now_ist).total_seconds()
    return wait_seconds, next_minute

def start_minute_collection(minute_start):
    """Start collecting data for a new minute interval."""
    global current_minute_start, current_minute_end, collecting_data

    current_minute_start = minute_start
    current_minute_end = minute_start + timedelta(minutes=1)
    collecting_data = True

    debug_log(f"🚀 STARTED MINUTE COLLECTION: {current_minute_start.strftime('%H:%M:%S')} - {current_minute_end.strftime('%H:%M:%S')} IST")

def should_finalize_minute(exchange_time):
    """Check if we should finalize current minute"""
    global current_minute_end
    return current_minute_end is not None and exchange_time >= current_minute_end

def finalize_current_minute(triggering_tick_time):
    """Finalize the current minute and save to Redis"""
    global collecting_data

    if not collecting_data:
        return

    collecting_data = False

    debug_log(f"⏹️  FINALIZING MINUTE at exchange time: {triggering_tick_time.strftime('%H:%M:%S.%f')[:-3]}")

    # Save OHLCV data to Redis
    for instrument_key, ohlcv_record in ohlcv_data.items():
        if ohlcv_record['open'] is not None:
            save_minute_ohlcv_to_redis(instrument_key, ohlcv_record)

    # Emit OHLCV update to dashboard
    ohlcv_summary = prepare_ohlcv_summary()
    if socketio:
        socketio.emit('ohlcv_update', ohlcv_summary)

    # Update unified signal inputs for the new minute
    refresh_unified_signal_inputs()

    # Reset for next minute
    reset_ohlcv_data()

def prepare_ohlcv_summary():
    """Prepare OHLCV summary for dashboard"""
    summary = {
        'period_start': current_minute_start.strftime('%H:%M:%S') if current_minute_start else None,
        'period_end': current_minute_end.strftime('%H:%M:%S') if current_minute_end else None,
        'symbols': {}
    }

    for instrument_key, data in ohlcv_data.items():
        if data['open'] is not None:
            summary['symbols'][instrument_key] = {
                'name': data['name'],
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume'],
                'tick_count': data['tick_count'],
                'change_pct': ((data['close'] - data['open']) / data['open'] * 100) if data['open'] > 0 else 0
            }

    return summary

def reset_ohlcv_data():
    """Reset OHLCV data for new minute"""
    for key in ohlcv_data:
        ohlcv_data[key].update({
            'open': None, 'high': None, 'low': None, 'close': None,
            'volume': 0, 'tick_count': 0, 'first_tick_time': None, 'last_tick_time': None
        })

def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
    access_token = get_access_token()
    if not access_token:
        return None
        
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    
    try:
        api_response = requests.get(url=url, headers=headers)
        api_response.raise_for_status()
        return api_response.json()
    except requests.RequestException as e:
        debug_log(f"Error getting market data feed authorization: {e}", "ERROR")
        return None

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

# Enhanced Flask routes
@app.route('/')
def dashboard():
    try:
        return render_template('enhanced_dashboard.html')
    except Exception as e:
        debug_log(f"Error rendering dashboard: {e}", "ERROR")
        return "Dashboard template not found", 404

@app.route('/api/market_data')
def api_market_data():
    """API endpoint for current market data."""
    return jsonify(dict(market_data))

@app.route('/api/signal_analytics/<symbol>')
def api_signal_analytics(symbol):
    """API endpoint for signal analytics of a specific symbol."""
    try:
        analytics = tech_indicators.get_signal_analytics(symbol)
        return jsonify(analytics)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/enhanced_debug_stats')
def api_enhanced_debug_stats():
    """API endpoint for enhanced debug statistics."""
    unified_count = 0
    if unified_signal_manager:
        try:
            unified_count = len(unified_signal_manager.get_all_signals())
        except Exception:
            unified_count = 0
    return jsonify({
        'total_messages': debug_stats['total_messages'],
        'processed_ticks': debug_stats['processed_ticks'],
        'successful_ticks': debug_stats['successful_ticks'],
        'unified_recalculations': debug_stats['unified_recalculations'],
        'dashboard_updates': debug_stats['dashboard_updates'],
        'last_message_time': debug_stats['last_message_time'].isoformat() if debug_stats['last_message_time'] else None,
        'active_symbols': len([k for k, v in market_data.items() if v['ltp'] is not None]),
        'symbols_with_unified_signals': unified_count,
        'success_rate': f"{(debug_stats['successful_ticks'] / debug_stats['processed_ticks'] * 100):.1f}%" if debug_stats['processed_ticks'] > 0 else "0%",
        'unified_cycle_rate': f"{(debug_stats['unified_recalculations'] / max(debug_stats['processed_ticks'], 1) * 100):.2f}%"
    })

async def backfill_missing_candles(instrument_keys: list, api_client: FastUpstoxAPI):
    """Checks for data gaps and backfills them before starting live feed."""
    print("🔍 Checking for any data gaps before starting live feed...")
    
    for instrument_key in instrument_keys:
        try:
            symbol_name = symbol_mapping.get(instrument_key, instrument_key)
            symbol_clean = symbol_name.replace(' ', '_').replace('&', 'and')
            redis_key = f"stock:{symbol_clean}:close"

            if not redis_client or not redis_client.exists(redis_key):
                print(f"⚪ No historical data for {symbol_name}, skipping gap fill.")
                continue

            # Get the last timestamp from Redis
            last_entry = redis_client.execute_command('TS.GET', redis_key)
            if not last_entry:
                continue
                
            last_ts_ms, _ = last_entry
            last_ts = datetime.fromtimestamp(last_ts_ms / 1000, tz=IST)
            
            # Check if the gap is significant
            time_gap = datetime.now(IST) - last_ts
            if time_gap > timedelta(minutes=2):
                print(f"🟡 Data gap found for {symbol_name}. Last data: {last_ts}. Gap: {time_gap}")
                
                # Fetch recent historical data to fill the gap
                days_to_fetch = max(1, (time_gap.days) + 1)
                print(f"   Fetching last {days_to_fetch} day(s) of data to fill gap...")
                
                historical_candles = await asyncio.to_thread(
                    api_client.get_historical_data,
                    instrument_key=instrument_key,
                    days_back=days_to_fetch
                )
                
                if not historical_candles:
                    print(f"   ❌ Could not fetch historical data for {symbol_name}.")
                    continue
                    
                # Filter for only the missing candles
                missing_candles = [
                    candle for candle in historical_candles if candle.timestamp > last_ts
                ]
                
                if not missing_candles:
                    print(f"   ✅ No new candles found. Data is up to date.")
                    continue
                    
                print(f"   Found {len(missing_candles)} missing candles. Storing in Redis...")
                
                # Store the missing candles
                symbol_data_map = {symbol_name: missing_candles}
                
                # This is a simplified version of the bulk store logic
                for data_type in ['open', 'high', 'low', 'close', 'volume']:
                    pipeline = redis_client.pipeline()
                    for candle in missing_candles:
                        key = f"stock:{symbol_clean}:{data_type}"
                        timestamp_ms = int(candle.timestamp.timestamp() * 1000)
                        value = getattr(candle, data_type)
                        pipeline.execute_command('TS.ADD', key, timestamp_ms, value)
                    pipeline.execute()
                
                print(f"   ✅ Gap filled for {symbol_name}. Database is now continuous.")

        except Exception as e:
            print(f"   ❌ Error during gap fill for {instrument_key}: {e}")


async def fetch_enhanced_market_data():
    """Enhanced market data fetching with unified signal aggregation"""
    global current_minute_start, current_minute_end, collecting_data

    instrument_keys = load_symbols()

    if not instrument_keys:
        debug_log("No symbols found in symbols.json", "ERROR")
        return

    # --- FIX: Backfill missing data before starting live feed ---
    try:
        access_token = get_access_token()
        if access_token:
            api_client = FastUpstoxAPI(access_token)
            await backfill_missing_candles(instrument_keys, api_client)
        else:
            print("⚠ Could not get access token, unable to perform gap-fill.")
    except Exception as e:
        print(f"❌ Error during pre-run gap-fill: {e}")
    # --- END FIX ---

    # Prime unified signals from historical cache before live ticks arrive.
    bootstrap_unified_signal_inputs(instrument_keys)

    wait_seconds, next_minute = wait_for_next_minute()

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    response = get_market_data_feed_authorize_v3()

    if not response or "data" not in response or "authorized_redirect_uri" not in response["data"]:
        debug_log(f"Invalid API response: {response}", "ERROR")
        return

    buffer_time = min(10, wait_seconds - 1)
    if buffer_time > 0:
        debug_log(f"Starting WebSocket {buffer_time:.1f} seconds early...")
        await asyncio.sleep(wait_seconds - buffer_time)

    try:
        async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as websocket:
            debug_log('Enhanced WebSocket connection established for unified signals', "SUCCESS")

            await asyncio.sleep(0.5)

            data = {
                "guid": "ACTIONABLE_TRADER",
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": instrument_keys
                }
            }

            binary_data = json.dumps(data).encode('utf-8')
            await websocket.send(binary_data)

            debug_log(f"Subscribed to {len(instrument_keys)} symbols for unified analysis", "SUCCESS")
            app_host = os.environ.get('APP_HOST', 'localhost')
            app_port = os.environ.get('APP_PORT', '5000')
            debug_log(f"🌐 Unified signal dashboard available at: http://{app_host}:{app_port}")

            remaining_wait = (next_minute - datetime.now(IST)).total_seconds()
            if remaining_wait > 0:
                await asyncio.sleep(remaining_wait)

            start_minute_collection(next_minute)

            while True:
                try:
                    message = await websocket.recv()
                    debug_stats['total_messages'] += 1
                    debug_stats['last_message_time'] = datetime.now(IST)

                    if debug_stats['total_messages'] % 200 == 1:
                        debug_log(f"📨 Processed message #{debug_stats['total_messages']} | "
                                  f"Unified cycles: {debug_stats['unified_recalculations']}", "STATS")

                    decoded_data = decode_protobuf(message)
                    data_dict = MessageToDict(decoded_data)

                    if 'feeds' in data_dict:
                        for instrument_key, feed_data in data_dict['feeds'].items():
                            if instrument_key in symbol_mapping:
                                process_tick_with_enhanced_analysis(instrument_key, feed_data)

                    if not collecting_data and current_minute_end is not None:
                        start_minute_collection(current_minute_end)

                except Exception as e:
                    debug_log(f"❌ Error processing enhanced message: {e}", "ERROR")
                    continue
                    
    except Exception as e:
        debug_log(f"❌ WebSocket connection error: {e}", "ERROR")

if __name__ == "__main__":
    try:
        debug_log("🚀 Starting Enhanced Live Market Data Processor with Unified Signals")
        asyncio.run(fetch_enhanced_market_data())

    except KeyboardInterrupt:
        debug_log("\n👋 Shutting down enhanced processor gracefully...")
        if collecting_data:
            finalize_current_minute(datetime.now(IST))
    except Exception as e:
        debug_log(f"❌ Error: {e}", "ERROR")
        import traceback
        debug_log(traceback.format_exc(), "ERROR")
