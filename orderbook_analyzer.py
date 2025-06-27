# Order Book Analyzer
# Analyzes bid/ask data, LTP, LTQ, market depth and pressure

import os
import redis
import json
import numpy as np
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, deque
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderBookAnalyzer:
    def __init__(self, redis_host=None, redis_port=None, redis_db=0):
        """Initialize Order Book Analyzer with Redis connection"""
        # Use environment variables if not provided
        if redis_host is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
        if redis_port is None:
            redis_port = int(os.environ.get('REDIS_PORT', '6379'))
            
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db,
            decode_responses=True
        )
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Store recent orderbook data for analysis
        self.orderbook_history = defaultdict(lambda: deque(maxlen=100))
        self.trade_flow = defaultdict(lambda: deque(maxlen=50))
        
    def analyze_bid_ask_spread(self, market_data: Dict) -> Dict:
        """Analyze bid-ask spread and its implications"""
        try:
            best_bid = market_data.get('best_bid')
            best_ask = market_data.get('best_ask')
            ltp = market_data.get('ltp')
            
            if not best_bid or not best_ask:
                return {'status': 'insufficient_data'}
            
            spread = best_ask - best_bid
            spread_pct = (spread / best_bid) * 100 if best_bid > 0 else 0
            
            # Spread analysis
            spread_analysis = 'TIGHT' if spread_pct < 0.1 else 'WIDE' if spread_pct > 0.5 else 'NORMAL'
            
            # LTP position in spread
            if ltp:
                if ltp <= best_bid:
                    ltp_position = 'AT_BID'
                elif ltp >= best_ask:
                    ltp_position = 'AT_ASK'
                else:
                    # LTP is between bid and ask (shouldn't happen in normal trading)
                    ltp_position = 'BETWEEN'
                    
                # Calculate how close LTP is to bid vs ask
                bid_distance = abs(ltp - best_bid) if best_bid else float('inf')
                ask_distance = abs(ltp - best_ask) if best_ask else float('inf')
                
                ltp_bias = 'BID_SIDE' if bid_distance < ask_distance else 'ASK_SIDE'
            else:
                ltp_position = 'UNKNOWN'
                ltp_bias = 'UNKNOWN'
            
            return {
                'spread_absolute': spread,
                'spread_percentage': spread_pct,
                'spread_analysis': spread_analysis,
                'ltp_position': ltp_position,
                'ltp_bias': ltp_bias,
                'bid_distance': bid_distance if ltp else None,
                'ask_distance': ask_distance if ltp else None
            }
            
        except Exception as e:
            logger.error(f"Error analyzing bid-ask spread: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def analyze_market_depth(self, market_data: Dict) -> Dict:
        """Analyze market depth from bid/ask levels"""
        try:
            bid_levels = market_data.get('bid_levels', [])
            ask_levels = market_data.get('ask_levels', [])
            
            if not bid_levels or not ask_levels:
                return {'status': 'insufficient_levels'}
            
            # Calculate depth metrics
            total_bid_quantity = sum(level['quantity'] for level in bid_levels)
            total_ask_quantity = sum(level['quantity'] for level in ask_levels)
            
            # Weighted average prices
            bid_weighted_price = sum(level['price'] * level['quantity'] for level in bid_levels) / total_bid_quantity if total_bid_quantity > 0 else 0
            ask_weighted_price = sum(level['price'] * level['quantity'] for level in ask_levels) / total_ask_quantity if total_ask_quantity > 0 else 0
            
            # Depth imbalance
            total_depth = total_bid_quantity + total_ask_quantity
            bid_percentage = (total_bid_quantity / total_depth * 100) if total_depth > 0 else 50
            ask_percentage = (total_ask_quantity / total_depth * 100) if total_depth > 0 else 50
            
            # Depth pressure analysis
            if bid_percentage > 60:
                depth_pressure = 'BUY_PRESSURE'
            elif ask_percentage > 60:
                depth_pressure = 'SELL_PRESSURE'
            else:
                depth_pressure = 'BALANCED'
            
            # Level concentration (how much quantity is in top level vs others)
            if len(bid_levels) > 1:
                bid_concentration = (bid_levels[0]['quantity'] / total_bid_quantity * 100) if total_bid_quantity > 0 else 0
            else:
                bid_concentration = 100
                
            if len(ask_levels) > 1:
                ask_concentration = (ask_levels[0]['quantity'] / total_ask_quantity * 100) if total_ask_quantity > 0 else 0
            else:
                ask_concentration = 100
            
            return {
                'total_bid_quantity': total_bid_quantity,
                'total_ask_quantity': total_ask_quantity,
                'bid_percentage': bid_percentage,
                'ask_percentage': ask_percentage,
                'depth_pressure': depth_pressure,
                'bid_weighted_price': bid_weighted_price,
                'ask_weighted_price': ask_weighted_price,
                'bid_concentration': bid_concentration,
                'ask_concentration': ask_concentration,
                'depth_levels': {
                    'bid_levels': len(bid_levels),
                    'ask_levels': len(ask_levels)
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing market depth: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def analyze_order_flow(self, symbol: str, current_ltp: float, current_ltq: int) -> Dict:
        """Analyze order flow and trade direction"""
        try:
            # Add current trade to flow history
            trade_data = {
                'timestamp': datetime.now(self.ist_timezone),
                'price': current_ltp,
                'quantity': current_ltq
            }
            
            self.trade_flow[symbol].append(trade_data)
            
            if len(self.trade_flow[symbol]) < 2:
                return {'status': 'insufficient_history'}
            
            recent_trades = list(self.trade_flow[symbol])
            
            # Calculate price movement
            price_changes = []
            volume_weighted_price = 0
            total_volume = 0
            
            for i in range(1, len(recent_trades)):
                prev_trade = recent_trades[i-1]
                curr_trade = recent_trades[i]
                
                price_change = curr_trade['price'] - prev_trade['price']
                price_changes.append(price_change)
                
                volume_weighted_price += curr_trade['price'] * curr_trade['quantity']
                total_volume += curr_trade['quantity']
            
            vwap = volume_weighted_price / total_volume if total_volume > 0 else current_ltp
            
            # Flow analysis
            positive_moves = sum(1 for change in price_changes if change > 0)
            negative_moves = sum(1 for change in price_changes if change < 0)
            neutral_moves = len(price_changes) - positive_moves - negative_moves
            
            if positive_moves > negative_moves * 1.5:
                flow_direction = 'BUYING'
            elif negative_moves > positive_moves * 1.5:
                flow_direction = 'SELLING'
            else:
                flow_direction = 'NEUTRAL'
            
            # Calculate trade intensity
            recent_volume = sum(trade['quantity'] for trade in recent_trades[-10:])
            avg_volume = sum(trade['quantity'] for trade in recent_trades) / len(recent_trades)
            
            intensity = 'HIGH' if recent_volume > avg_volume * 1.5 else 'LOW' if recent_volume < avg_volume * 0.5 else 'NORMAL'
            
            return {
                'flow_direction': flow_direction,
                'positive_moves': positive_moves,
                'negative_moves': negative_moves,
                'neutral_moves': neutral_moves,
                'vwap': vwap,
                'trade_intensity': intensity,
                'recent_volume': recent_volume,
                'avg_volume': avg_volume,
                'total_trades': len(recent_trades)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing order flow for {symbol}: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def calculate_support_resistance(self, market_data: Dict) -> Dict:
        """Calculate immediate support and resistance from orderbook"""
        try:
            bid_levels = market_data.get('bid_levels', [])
            ask_levels = market_data.get('ask_levels', [])
            current_price = market_data.get('ltp')
            
            if not bid_levels or not ask_levels or not current_price:
                return {'status': 'insufficient_data'}
            
            # Sort levels by price
            bid_levels_sorted = sorted(bid_levels, key=lambda x: x['price'], reverse=True)
            ask_levels_sorted = sorted(ask_levels, key=lambda x: x['price'])
            
            # Find significant support (high quantity bid levels)
            significant_bids = [level for level in bid_levels_sorted if level['quantity'] > 0]
            significant_asks = [level for level in ask_levels_sorted if level['quantity'] > 0]
            
            # Immediate support and resistance
            immediate_support = significant_bids[0]['price'] if significant_bids else None
            immediate_resistance = significant_asks[0]['price'] if significant_asks else None
            
            # Find stronger support/resistance based on quantity
            if len(significant_bids) > 1:
                max_bid_qty = max(level['quantity'] for level in significant_bids)
                strong_support_level = next((level for level in significant_bids if level['quantity'] == max_bid_qty), None)
                strong_support = strong_support_level['price'] if strong_support_level else immediate_support
            else:
                strong_support = immediate_support
            
            if len(significant_asks) > 1:
                max_ask_qty = max(level['quantity'] for level in significant_asks)
                strong_resistance_level = next((level for level in significant_asks if level['quantity'] == max_ask_qty), None)
                strong_resistance = strong_resistance_level['price'] if strong_resistance_level else immediate_resistance
            else:
                strong_resistance = immediate_resistance
            
            # Calculate distance from current price
            support_distance = ((current_price - immediate_support) / current_price * 100) if immediate_support else None
            resistance_distance = ((immediate_resistance - current_price) / current_price * 100) if immediate_resistance else None
            
            return {
                'immediate_support': immediate_support,
                'immediate_resistance': immediate_resistance,
                'strong_support': strong_support,
                'strong_resistance': strong_resistance,
                'support_distance_pct': support_distance,
                'resistance_distance_pct': resistance_distance
            }
            
        except Exception as e:
            logger.error(f"Error calculating support/resistance: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def analyze_tick_by_tick(self, symbol: str, market_data: Dict) -> Dict:
        """Analyze tick-by-tick data for micro movements"""
        try:
            # Store current tick data
            tick_data = {
                'timestamp': datetime.now(self.ist_timezone),
                'ltp': market_data.get('ltp'),
                'ltq': market_data.get('ltq'),
                'best_bid': market_data.get('best_bid'),
                'best_ask': market_data.get('best_ask'),
                'bid_size': market_data.get('bid_size'),
                'ask_size': market_data.get('ask_size'),
                'tick_direction': market_data.get('tick_direction')
            }
            
            self.orderbook_history[symbol].append(tick_data)
            
            if len(self.orderbook_history[symbol]) < 5:
                return {'status': 'building_history'}
            
            recent_ticks = list(self.orderbook_history[symbol])[-10:]  # Last 10 ticks
            
            # Analyze price momentum
            price_changes = []
            for i in range(1, len(recent_ticks)):
                if recent_ticks[i]['ltp'] and recent_ticks[i-1]['ltp']:
                    change = recent_ticks[i]['ltp'] - recent_ticks[i-1]['ltp']
                    price_changes.append(change)
            
            # Momentum analysis
            if len(price_changes) >= 3:
                positive_ticks = sum(1 for change in price_changes if change > 0)
                negative_ticks = sum(1 for change in price_changes if change < 0)
                
                if positive_ticks > len(price_changes) * 0.6:
                    micro_trend = 'STRONG_UP'
                elif positive_ticks > len(price_changes) * 0.4:
                    micro_trend = 'WEAK_UP'
                elif negative_ticks > len(price_changes) * 0.6:
                    micro_trend = 'STRONG_DOWN'
                elif negative_ticks > len(price_changes) * 0.4:
                    micro_trend = 'WEAK_DOWN'
                else:
                    micro_trend = 'SIDEWAYS'
            else:
                micro_trend = 'INSUFFICIENT_DATA'
            
            # Analyze bid-ask size changes
            current_bid_ask_ratio = tick_data['bid_size'] / tick_data['ask_size'] if tick_data['ask_size'] else 0
            
            # Check if sizes are increasing (interest building)
            if len(recent_ticks) >= 3:
                avg_bid_size = np.mean([tick['bid_size'] for tick in recent_ticks[-3:] if tick['bid_size']])
                avg_ask_size = np.mean([tick['ask_size'] for tick in recent_ticks[-3:] if tick['ask_size']])
                prev_avg_bid = np.mean([tick['bid_size'] for tick in recent_ticks[-6:-3] if tick['bid_size']]) if len(recent_ticks) >= 6 else avg_bid_size
                prev_avg_ask = np.mean([tick['ask_size'] for tick in recent_ticks[-6:-3] if tick['ask_size']]) if len(recent_ticks) >= 6 else avg_ask_size
                
                bid_size_trend = 'INCREASING' if avg_bid_size > prev_avg_bid * 1.1 else 'DECREASING' if avg_bid_size < prev_avg_bid * 0.9 else 'STABLE'
                ask_size_trend = 'INCREASING' if avg_ask_size > prev_avg_ask * 1.1 else 'DECREASING' if avg_ask_size < prev_avg_ask * 0.9 else 'STABLE'
            else:
                bid_size_trend = 'UNKNOWN'
                ask_size_trend = 'UNKNOWN'
            
            return {
                'micro_trend': micro_trend,
                'positive_ticks': positive_ticks if len(price_changes) >= 3 else 0,
                'negative_ticks': negative_ticks if len(price_changes) >= 3 else 0,
                'price_changes': price_changes,
                'bid_ask_ratio': current_bid_ask_ratio,
                'bid_size_trend': bid_size_trend,
                'ask_size_trend': ask_size_trend,
                'tick_count': len(recent_ticks)
            }
            
        except Exception as e:
            logger.error(f"Error in tick analysis for {symbol}: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def comprehensive_orderbook_analysis(self, symbol: str, market_data: Dict) -> Dict:
        """Perform comprehensive orderbook analysis"""
        try:
            analysis = {
                'symbol': symbol,
                'timestamp': datetime.now(self.ist_timezone).isoformat(),
                'basic_data': {
                    'ltp': market_data.get('ltp'),
                    'ltq': market_data.get('ltq'),
                    'best_bid': market_data.get('best_bid'),
                    'best_ask': market_data.get('best_ask'),
                    'bid_size': market_data.get('bid_size'),
                    'ask_size': market_data.get('ask_size')
                }
            }
            
            # Perform all analyses
            analysis['spread_analysis'] = self.analyze_bid_ask_spread(market_data)
            analysis['depth_analysis'] = self.analyze_market_depth(market_data)
            analysis['flow_analysis'] = self.analyze_order_flow(symbol, market_data.get('ltp', 0), market_data.get('ltq', 0))
            analysis['support_resistance'] = self.calculate_support_resistance(market_data)
            analysis['tick_analysis'] = self.analyze_tick_by_tick(symbol, market_data)
            
            # Generate overall assessment
            analysis['overall_assessment'] = self.generate_orderbook_signals(analysis)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in comprehensive analysis for {symbol}: {e}")
            return {'symbol': symbol, 'status': 'error', 'message': str(e)}
    
    def generate_orderbook_signals(self, analysis: Dict) -> Dict:
        """Generate trading signals based on orderbook analysis"""
        try:
            signals = {
                'market_sentiment': 'NEUTRAL',
                'short_term_direction': 'SIDEWAYS',
                'liquidity_quality': 'NORMAL',
                'entry_recommendation': 'WAIT',
                'confidence': 0.5,
                'key_factors': []
            }
            
            # Analyze depth pressure
            depth_analysis = analysis.get('depth_analysis', {})
            if depth_analysis.get('depth_pressure') == 'BUY_PRESSURE':
                signals['market_sentiment'] = 'BULLISH'
                signals['key_factors'].append('Strong buy depth')
            elif depth_analysis.get('depth_pressure') == 'SELL_PRESSURE':
                signals['market_sentiment'] = 'BEARISH'
                signals['key_factors'].append('Strong sell depth')
            
            # Analyze flow direction
            flow_analysis = analysis.get('flow_analysis', {})
            if flow_analysis.get('flow_direction') == 'BUYING':
                if signals['market_sentiment'] == 'BULLISH':
                    signals['short_term_direction'] = 'UP'
                    signals['confidence'] += 0.2
                signals['key_factors'].append('Buying flow detected')
            elif flow_analysis.get('flow_direction') == 'SELLING':
                if signals['market_sentiment'] == 'BEARISH':
                    signals['short_term_direction'] = 'DOWN'
                    signals['confidence'] += 0.2
                signals['key_factors'].append('Selling flow detected')
            
            # Analyze spread quality
            spread_analysis = analysis.get('spread_analysis', {})
            if spread_analysis.get('spread_analysis') == 'TIGHT':
                signals['liquidity_quality'] = 'HIGH'
                signals['confidence'] += 0.1
                signals['key_factors'].append('Tight spreads')
            elif spread_analysis.get('spread_analysis') == 'WIDE':
                signals['liquidity_quality'] = 'LOW'
                signals['confidence'] -= 0.1
                signals['key_factors'].append('Wide spreads')
            
            # Analyze micro trend
            tick_analysis = analysis.get('tick_analysis', {})
            micro_trend = tick_analysis.get('micro_trend')
            if micro_trend in ['STRONG_UP', 'WEAK_UP']:
                signals['short_term_direction'] = 'UP'
                signals['key_factors'].append(f'Micro trend: {micro_trend}')
            elif micro_trend in ['STRONG_DOWN', 'WEAK_DOWN']:
                signals['short_term_direction'] = 'DOWN'
                signals['key_factors'].append(f'Micro trend: {micro_trend}')
            
            # Generate entry recommendation
            if (signals['market_sentiment'] == 'BULLISH' and 
                signals['short_term_direction'] == 'UP' and 
                signals['liquidity_quality'] in ['HIGH', 'NORMAL']):
                signals['entry_recommendation'] = 'BUY'
                signals['confidence'] += 0.2
            elif (signals['market_sentiment'] == 'BEARISH' and 
                  signals['short_term_direction'] == 'DOWN' and 
                  signals['liquidity_quality'] in ['HIGH', 'NORMAL']):
                signals['entry_recommendation'] = 'SELL'
                signals['confidence'] += 0.2
            
            signals['confidence'] = min(signals['confidence'], 1.0)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating orderbook signals: {e}")
            return {'status': 'error', 'message': str(e)}

# Example usage and testing
if __name__ == "__main__":
    analyzer = OrderBookAnalyzer()
    
    # Test with sample market data
    sample_data = {
        'ltp': 100.50,
        'ltq': 25,
        'best_bid': 100.45,
        'best_ask': 100.55,
        'bid_size': 150,
        'ask_size': 200,
        'bid_levels': [
            {'price': 100.45, 'quantity': 150},
            {'price': 100.40, 'quantity': 300},
            {'price': 100.35, 'quantity': 500}
        ],
        'ask_levels': [
            {'price': 100.55, 'quantity': 200},
            {'price': 100.60, 'quantity': 180},
            {'price': 100.65, 'quantity': 250}
        ],
        'tick_direction': 'UP'
    }
    
    # Perform comprehensive analysis
    analysis = analyzer.comprehensive_orderbook_analysis('TEST_STOCK', sample_data)
    
    print(json.dumps(analysis, indent=2, default=str))