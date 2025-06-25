# Enhanced Live Market Data Processor with Actionable Signal Generation
# Updated to use TrajectorySignalGenerator for confirmed trading signals

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
import time
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import threading
import statistics
import redis
import numpy as np

# Import enhanced modules
try:
    from enhanced_signal_generator import TrajectorySignalGenerator, ActionableSignal, SignalStrength
    from updated_technical_indicators import EnhancedTechnicalIndicators
    from orderbook_analyzer import OrderBookAnalyzer
except ImportError as e:
    print(f"Import error: {e}")
    # Fallback to basic modules
    from technical_indicators import TechnicalIndicators as EnhancedTechnicalIndicators
    from orderbook_analyzer import OrderBookAnalyzer
    ActionableSignal = None
    SignalStrength = None

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

# Initialize enhanced technical indicators and orderbook analyzer
tech_indicators = EnhancedTechnicalIndicators()
orderbook_analyzer = OrderBookAnalyzer()

# Redis client for data storage
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Symbol mapping and data structures
symbol_mapping = {}
debug_stats = {
    'total_messages': 0,
    'processed_ticks': 0,
    'dashboard_updates': 0,
    'last_message_time': None,
    'successful_ticks': 0,
    'actionable_signals_generated': 0,
    'trajectory_confirmations': 0
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

# Real-time market data with enhanced signals
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
    'ask_levels': [],
    # Enhanced signal data
    'actionable_signals': [],
    'signal_summary': {},
    'trajectory_status': 'BUILDING',
    'signal_confidence': 0.0,
    'recommended_action': 'WAIT',
    'risk_reward_ratio': 0.0,
    'target_price': None,
    'stop_loss': None
})

# Enhanced stock screening with actionable signals
actionable_screener_results = {
    'high_confidence_buys': [],
    'high_confidence_sells': [],
    'trajectory_confirmed_signals': [],
    'emerging_opportunities': [],
    'risk_warnings': [],
    'last_updated': None
}

# Tracking variables for minute intervals
current_minute_start = None
current_minute_end = None
collecting_data = False

def debug_log(message, level="INFO"):
    """Enhanced debug logging."""
    timestamp = datetime.now(IST).strftime('%H:%M:%S.%f')[:-3]
    print(f"[{timestamp}] {level}: {message}")

    try:
        socketio.emit('debug_log', {
            'timestamp': timestamp,
            'level': level,
            'message': message
        })
    except:
        pass

def get_access_token():
    with open(JSON_PATH, 'r') as f:
        data = json.load(f)
    return data['access_token']

def load_symbols():
    """Load symbols from symbols.json file."""
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

def save_minute_ohlcv_to_redis(instrument_key, ohlcv_record):
    """Save completed minute OHLCV data to Redis TimeSeries"""
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

    except Exception as e:
        debug_log(f"Error saving to Redis for {instrument_key}: {e}", "ERROR")

def generate_actionable_signals(symbol_name, instrument_key, current_market_data):
    """Generate actionable trading signals with trajectory confirmation"""
    try:
        # Get orderbook analysis
        orderbook_analysis = orderbook_analyzer.comprehensive_orderbook_analysis(
            symbol_name, current_market_data
        )
        
        # Generate actionable signals
        actionable_signals = tech_indicators.generate_actionable_signals(
            symbol_name, current_market_data, orderbook_analysis
        )
        
        if actionable_signals:
            debug_stats['actionable_signals_generated'] += len(actionable_signals)
            
            # Count trajectory confirmations
            confirmed_signals = [s for s in actionable_signals if s.trajectory_confirmed]
            if confirmed_signals:
                debug_stats['trajectory_confirmations'] += len(confirmed_signals)
            
            # Update market data with actionable signals
            market_data[instrument_key]['actionable_signals'] = [
                {
                    'signal_type': signal.signal_type,
                    'strength': signal.strength.name,
                    'confidence': signal.confidence,
                    'entry_price': signal.entry_price,
                    'target_price': signal.target_price,
                    'stop_loss': signal.stop_loss,
                    'trajectory_confirmed': signal.trajectory_confirmed,
                    'time_horizon': signal.time_horizon,
                    'reasons': signal.reasons,
                    'risk_reward_ratio': signal.risk_reward_ratio,
                    'timestamp': signal.timestamp.isoformat()
                }
                for signal in actionable_signals
            ]
            
            # Update signal summary
            best_signal = max(actionable_signals, key=lambda x: x.confidence)
            market_data[instrument_key]['signal_summary'] = {
                'best_signal_type': best_signal.signal_type,
                'best_confidence': best_signal.confidence,
                'trajectory_confirmed': best_signal.trajectory_confirmed,
                'total_signals': len(actionable_signals)
            }
            
            market_data[instrument_key]['signal_confidence'] = best_signal.confidence
            market_data[instrument_key]['recommended_action'] = best_signal.signal_type
            market_data[instrument_key]['risk_reward_ratio'] = best_signal.risk_reward_ratio
            market_data[instrument_key]['target_price'] = best_signal.target_price
            market_data[instrument_key]['stop_loss'] = best_signal.stop_loss
            
            # Update trajectory status
            if any(s.trajectory_confirmed for s in actionable_signals):
                market_data[instrument_key]['trajectory_status'] = 'CONFIRMED'
            else:
                market_data[instrument_key]['trajectory_status'] = 'DEVELOPING'
                
            return actionable_signals
        else:
            market_data[instrument_key]['trajectory_status'] = 'BUILDING'
            market_data[instrument_key]['recommended_action'] = 'WAIT'
            
        return []
        
    except Exception as e:
        debug_log(f"Error generating actionable signals for {symbol_name}: {e}", "ERROR")
        return []

def update_enhanced_stock_screener():
    """Update stock screener with actionable signals"""
    try:
        high_confidence_buys = []
        high_confidence_sells = []
        trajectory_confirmed = []
        emerging_opportunities = []
        risk_warnings = []

        for instrument_key, data in market_data.items():
            if not data['ltp'] or not data.get('actionable_signals'):
                continue
                
            symbol_name = data['name']
            actionable_signals = data['actionable_signals']
            
            for signal_data in actionable_signals:
                signal_info = {
                    'symbol': symbol_name,
                    'instrument_key': instrument_key,
                    'price': data['ltp'],
                    'signal_type': signal_data['signal_type'],
                    'strength': signal_data['strength'],
                    'confidence': signal_data['confidence'],
                    'trajectory_confirmed': signal_data['trajectory_confirmed'],
                    'time_horizon': signal_data['time_horizon'],
                    'entry_price': signal_data['entry_price'],
                    'target_price': signal_data['target_price'],
                    'stop_loss': signal_data['stop_loss'],
                    'risk_reward_ratio': signal_data['risk_reward_ratio'],
                    'reasons': signal_data['reasons'][:3],  # Top 3 reasons
                    'spread_pct': data.get('spread_pct', 0),
                    'last_update': datetime.now(IST).isoformat()
                }
                
                # Categorize signals
                if signal_data['trajectory_confirmed']:
                    trajectory_confirmed.append(signal_info)
                    
                    if signal_data['signal_type'] == 'BUY' and signal_data['confidence'] > 0.75:
                        high_confidence_buys.append(signal_info)
                    elif signal_data['signal_type'] == 'SELL' and signal_data['confidence'] > 0.75:
                        high_confidence_sells.append(signal_info)
                        
                elif signal_data['confidence'] > 0.6:
                    emerging_opportunities.append(signal_info)
                
                # Risk warnings for low risk-reward ratios
                if signal_data['risk_reward_ratio'] < 1.5:
                    risk_warnings.append({
                        'symbol': symbol_name,
                        'warning': f"Low risk/reward ratio: {signal_data['risk_reward_ratio']:.2f}",
                        'signal_type': signal_data['signal_type'],
                        'confidence': signal_data['confidence']
                    })

        # Sort by confidence and limit results
        high_confidence_buys.sort(key=lambda x: x['confidence'], reverse=True)
        high_confidence_sells.sort(key=lambda x: x['confidence'], reverse=True)
        trajectory_confirmed.sort(key=lambda x: x['confidence'], reverse=True)
        emerging_opportunities.sort(key=lambda x: x['confidence'], reverse=True)

        # Update screener results
        actionable_screener_results.update({
            'high_confidence_buys': high_confidence_buys[:5],
            'high_confidence_sells': high_confidence_sells[:5],
            'trajectory_confirmed_signals': trajectory_confirmed[:10],
            'emerging_opportunities': emerging_opportunities[:8],
            'risk_warnings': risk_warnings[:5],
            'last_updated': datetime.now(IST).isoformat()
        })

        # Emit to dashboard
        socketio.emit('actionable_screener_update', actionable_screener_results)

        debug_log(f"📊 Enhanced screener updated: {len(high_confidence_buys)} high-conf buys, "
                 f"{len(high_confidence_sells)} high-conf sells, {len(trajectory_confirmed)} confirmed", "SCREENER")

    except Exception as e:
        debug_log(f"Error updating enhanced stock screener: {e}", "ERROR")

def process_tick_with_enhanced_analysis(instrument_key, tick_data):
    """Enhanced tick processing with actionable signal generation"""
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

                # Generate actionable signals every 20 ticks (more frequent analysis)
                if debug_stats['processed_ticks'] % 20 == 0:
                    try:
                        current_market_data = dict(market_data[instrument_key])
                        if current_market_data['ltp']:
                            actionable_signals = generate_actionable_signals(
                                symbol_name, instrument_key, current_market_data
                            )
                            
                            if actionable_signals:
                                debug_log(f"🎯 Generated {len(actionable_signals)} actionable signals for {symbol_name}", "SIGNALS")
                                
                    except Exception as e:
                        debug_log(f"Error generating actionable signals for {symbol_name}: {e}", "ERROR")

                # Emit real-time data
                emit_enhanced_real_time_data()

                # Update enhanced screener every 100 ticks
                if debug_stats['processed_ticks'] % 100 == 0:
                    update_enhanced_stock_screener()

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

def emit_enhanced_real_time_data():
    """Emit enhanced real-time data with actionable signals to dashboard"""
    try:
        debug_stats['dashboard_updates'] += 1

        dashboard_data = {}
        for instrument_key, data in market_data.items():
            if data['ltp'] is not None:
                # Get best actionable signal if available
                best_signal = None
                if data.get('actionable_signals'):
                    signals_with_conf = [(s, s['confidence']) for s in data['actionable_signals']]
                    if signals_with_conf:
                        best_signal = max(signals_with_conf, key=lambda x: x[1])[0]

                dashboard_data[instrument_key] = {
                    'name': data['name'],
                    'ltp': data['ltp'],
                    'best_bid': data['best_bid'],
                    'best_ask': data['best_ask'],
                    'spread': data['spread'],
                    'spread_pct': data['spread_pct'],
                    
                    # Enhanced signal information
                    'recommended_action': data.get('recommended_action', 'WAIT'),
                    'signal_confidence': data.get('signal_confidence', 0),
                    'trajectory_status': data.get('trajectory_status', 'BUILDING'),
                    'risk_reward_ratio': data.get('risk_reward_ratio', 0),
                    'target_price': data.get('target_price'),
                    'stop_loss': data.get('stop_loss'),
                    
                    # Signal details
                    'best_signal': {
                        'type': best_signal['signal_type'] if best_signal else None,
                        'strength': best_signal['strength'] if best_signal else None,
                        'confidence': best_signal['confidence'] if best_signal else 0,
                        'trajectory_confirmed': best_signal['trajectory_confirmed'] if best_signal else False,
                        'time_horizon': best_signal['time_horizon'] if best_signal else None,
                        'reasons': best_signal['reasons'][:2] if best_signal else []  # Top 2 reasons
                    },
                    
                    'total_signals': len(data.get('actionable_signals', [])),
                    'last_update': data['last_update'].strftime('%H:%M:%S.%f')[:-3] if data['last_update'] else None
                }

        if dashboard_data:
            socketio.emit('enhanced_market_update', dashboard_data)

    except Exception as e:
        debug_log(f"❌ Error emitting enhanced data: {e}", "ERROR")

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

# All the other functions remain the same (wait_for_next_minute, start_minute_collection, etc.)
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
    socketio.emit('ohlcv_update', ohlcv_summary)

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
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    api_response = requests.get(url=url, headers=headers)
    return api_response.json()

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

# Enhanced Flask routes
@app.route('/')
def dashboard():
    return render_template('enhanced_dashboard.html')

@app.route('/api/market_data')
def api_market_data():
    """API endpoint for current market data."""
    return jsonify(dict(market_data))

@app.route('/api/actionable_screener')
def api_actionable_screener():
    """API endpoint for actionable stock screener results."""
    return jsonify(actionable_screener_results)

@app.route('/api/signal_analytics/<symbol>')
def api_signal_analytics(symbol):
    """API endpoint for signal analytics of a specific symbol."""
    analytics = tech_indicators.get_signal_analytics(symbol)
    return jsonify(analytics)

@app.route('/api/enhanced_debug_stats')
def api_enhanced_debug_stats():
    """API endpoint for enhanced debug statistics."""
    return jsonify({
        'total_messages': debug_stats['total_messages'],
        'processed_ticks': debug_stats['processed_ticks'],
        'successful_ticks': debug_stats['successful_ticks'],
        'actionable_signals_generated': debug_stats['actionable_signals_generated'],
        'trajectory_confirmations': debug_stats['trajectory_confirmations'],
        'dashboard_updates': debug_stats['dashboard_updates'],
        'last_message_time': debug_stats['last_message_time'].isoformat() if debug_stats['last_message_time'] else None,
        'active_symbols': len([k for k, v in market_data.items() if v['ltp'] is not None]),
        'symbols_with_signals': len([k for k, v in market_data.items() if v.get('actionable_signals')]),
        'success_rate': f"{(debug_stats['successful_ticks'] / debug_stats['processed_ticks'] * 100):.1f}%" if debug_stats['processed_ticks'] > 0 else "0%",
        'signal_generation_rate': f"{(debug_stats['actionable_signals_generated'] / max(debug_stats['processed_ticks'], 1) * 100):.2f}%",
        'trajectory_confirmation_rate': f"{(debug_stats['trajectory_confirmations'] / max(debug_stats['actionable_signals_generated'], 1) * 100):.1f}%"
    })

async def fetch_enhanced_market_data():
    """Enhanced market data fetching with actionable signal analysis"""
    global current_minute_start, current_minute_end, collecting_data

    instrument_keys = load_symbols()

    if not instrument_keys:
        debug_log("No symbols found in symbols.json", "ERROR")
        return

    wait_seconds, next_minute = wait_for_next_minute()

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    response = get_market_data_feed_authorize_v3()

    if "data" not in response or "authorized_redirect_uri" not in response["data"]:
        debug_log(f"Invalid API response: {response}", "ERROR")
        return

    buffer_time = min(10, wait_seconds - 1)
    if buffer_time > 0:
        debug_log(f"Starting WebSocket {buffer_time:.1f} seconds early...")
        await asyncio.sleep(wait_seconds - buffer_time)

    async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as websocket:
        debug_log('Enhanced WebSocket connection established for actionable signals', "SUCCESS")

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

        debug_log(f"Subscribed to {len(instrument_keys)} symbols for actionable signal analysis", "SUCCESS")
        debug_log("🌐 Enhanced dashboard with actionable signals available at: http://localhost:5000")

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
                             f"Signals: {debug_stats['actionable_signals_generated']} | "
                             f"Confirmed: {debug_stats['trajectory_confirmations']}", "STATS")

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

def run_flask_app():
    """Run Flask app in a separate thread."""
    socketio.run(app, debug=False, host='0.0.0.0', port=5000, use_reloader=False)

if __name__ == "__main__":
    try:
        flask_thread = threading.Thread(target=run_flask_app)
        flask_thread.daemon = True
        flask_thread.start()

        debug_log("🚀 Starting Enhanced Live Market Data Processor with Actionable Signals")
        asyncio.run(fetch_enhanced_market_data())

    except KeyboardInterrupt:
        debug_log("\n👋 Shutting down enhanced processor gracefully...")
        if collecting_data:
            finalize_current_minute(datetime.now(IST))
    except Exception as e:
        debug_log(f"❌ Error: {e}", "ERROR")
        import traceback
        debug_log(traceback.format_exc(), "ERROR")