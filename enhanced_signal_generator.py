# Enhanced Signal Generator with Trajectory Confirmation
# Provides actionable signals based on confirmed trends and momentum

import redis
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, deque
import logging
from dataclasses import dataclass
from enum import Enum

from technical_indicators import EnhancedTechnicalIndicators

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SignalStrength(Enum):
    WEAK = 1
    MODERATE = 2
    STRONG = 3
    VERY_STRONG = 4

class TrendDirection(Enum):
    STRONG_BULLISH = "STRONG_BULLISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL" 
    BEARISH = "BEARISH"
    STRONG_BEARISH = "STRONG_BEARISH"

@dataclass
class ActionableSignal:
    symbol: str
    signal_type: str  # BUY, SELL, HOLD
    strength: SignalStrength
    confidence: float
    entry_price: float
    target_price: float
    stop_loss: float
    trajectory_confirmed: bool
    time_horizon: str  # SHORT, MEDIUM, LONG
    reasons: List[str]
    risk_reward_ratio: float
    timestamp: datetime
    signal_quality_score: float = 0.5
    market_regime: str = "UNKNOWN"
    position_size_pct: float = 1.0
    max_portfolio_exposure: float = 0.05

class MarketRegime(Enum):
    TRENDING = "TRENDING"
    RANGING = "RANGING"
    UNDEFINED = "UNDEFINED"

class TrajectorySignalGenerator:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        """Initialize enhanced signal generator with trajectory confirmation"""
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db,
            decode_responses=True
        )
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Store signal history for trajectory analysis
        self.signal_history = defaultdict(lambda: deque(maxlen=50))
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.volume_history = defaultdict(lambda: deque(maxlen=100))
        self.momentum_history = defaultdict(lambda: deque(maxlen=30))
        
        # Configuration for signal confirmation
        self.min_confirmation_periods = 3  # Minimum periods to confirm trend
        self.trend_strength_threshold = 0.6  # Minimum strength for trend confirmation
        self.volume_confirmation_multiplier = 1.2  # Volume should be 20% above average
        
        # Configurable RSI thresholds
        self.rsi_oversold_threshold = 30
        self.rsi_overbought_threshold = 70

        self.tech_indicators = EnhancedTechnicalIndicators()
        
    def add_market_data(self, symbol: str, market_data: Dict, technical_indicators: Dict):
        """Add new market data point for trajectory analysis"""
        try:
            current_time = datetime.now(self.ist_timezone)
            
            # Store price data
            if market_data.get('ltp'):
                self.price_history[symbol].append({
                    'timestamp': current_time,
                    'price': market_data['ltp'],
                    'volume': market_data.get('ltq', 0),
                    'bid': market_data.get('best_bid'),
                    'ask': market_data.get('best_ask'),
                    'spread': market_data.get('spread_pct', 0)
                })
            
            # Store technical indicator data
            if technical_indicators:
                self.momentum_history[symbol].append({
                    'timestamp': current_time,
                    'rsi': technical_indicators.get('rsi'),
                    'macd': technical_indicators.get('macd', {}).get('macd'),
                    'macd_signal': technical_indicators.get('macd', {}).get('signal'),
                    'sma_5': technical_indicators.get('sma', {}).get('sma_5'),
                    'sma_20': technical_indicators.get('sma', {}).get('sma_20'),
                    'volume_ratio': technical_indicators.get('volume', {}).get('volume_ratio'),
                    'signals': technical_indicators.get('signals', {})
                })
                
        except Exception as e:
            logger.error(f"Error adding market data for {symbol}: {e}")
    
    def analyze_price_trajectory(self, symbol: str) -> Dict:
        """Analyze price trajectory over multiple timeframes"""
        try:
            price_data = list(self.price_history[symbol])
            if len(price_data) < 10:
                return {'status': 'insufficient_data'}
            
            # Calculate returns over different periods
            prices = [d['price'] for d in price_data]
            
            # Short-term trajectory (last 5 periods)
            short_term_change = (prices[-1] - prices[-5]) / prices[-5] if len(prices) >= 5 else 0
            short_term_trend = self._calculate_trend_strength(prices[-5:] if len(prices) >= 5 else prices)
            
            # Medium-term trajectory (last 15 periods)
            medium_term_change = (prices[-1] - prices[-15]) / prices[-15] if len(prices) >= 15 else 0
            medium_term_trend = self._calculate_trend_strength(prices[-15:] if len(prices) >= 15 else prices)
            
            # Long-term trajectory (last 30 periods)
            long_term_change = (prices[-1] - prices[-30]) / prices[-30] if len(prices) >= 30 else 0
            long_term_trend = self._calculate_trend_strength(prices[-30:] if len(prices) >= 30 else prices)
            
            # Calculate acceleration (rate of change of change)
            acceleration = self._calculate_acceleration(prices)
            
            # Trend consistency across timeframes
            trend_consistency = self._calculate_trend_consistency([
                short_term_trend, medium_term_trend, long_term_trend
            ])
            
            return {
                'short_term': {
                    'change_pct': short_term_change * 100,
                    'trend_strength': short_term_trend,
                    'direction': 'UP' if short_term_change > 0 else 'DOWN'
                },
                'medium_term': {
                    'change_pct': medium_term_change * 100,
                    'trend_strength': medium_term_trend,
                    'direction': 'UP' if medium_term_change > 0 else 'DOWN'
                },
                'long_term': {
                    'change_pct': long_term_change * 100,
                    'trend_strength': long_term_trend,
                    'direction': 'UP' if long_term_change > 0 else 'DOWN'
                },
                'acceleration': acceleration,
                'trend_consistency': trend_consistency,
                'overall_trajectory': self._determine_overall_trajectory(
                    short_term_trend, medium_term_trend, long_term_trend, trend_consistency
                )
            }
            
        except Exception as e:
            logger.error(f"Error analyzing price trajectory for {symbol}: {e}")
            return {'status': 'error'}
    
    def analyze_momentum_trajectory(self, symbol: str) -> Dict:
        """Analyze momentum trajectory using technical indicators"""
        try:
            momentum_data = list(self.momentum_history[symbol])
            if len(momentum_data) < 5:
                return {'status': 'insufficient_data'}
            
            # RSI trajectory
            rsi_values = [d['rsi'] for d in momentum_data if d['rsi'] is not None]
            rsi_trajectory = self._analyze_indicator_trajectory(rsi_values, 'rsi')
            
            # MACD trajectory
            macd_values = [d['macd'] for d in momentum_data if d['macd'] is not None]
            macd_signal_values = [d['macd_signal'] for d in momentum_data if d['macd_signal'] is not None]
            macd_trajectory = self._analyze_macd_trajectory(macd_values, macd_signal_values)
            
            # Moving average trajectory
            sma_trajectory = self._analyze_ma_trajectory(momentum_data)
            
            # Volume trajectory
            volume_ratios = [d['volume_ratio'] for d in momentum_data if d['volume_ratio'] is not None]
            volume_trajectory = self._analyze_indicator_trajectory(volume_ratios, 'volume')
            
            # Combined momentum score
            momentum_score = self._calculate_momentum_score(
                rsi_trajectory, macd_trajectory, sma_trajectory, volume_trajectory
            )
            
            return {
                'rsi_trajectory': rsi_trajectory,
                'macd_trajectory': macd_trajectory,
                'sma_trajectory': sma_trajectory,
                'volume_trajectory': volume_trajectory,
                'momentum_score': momentum_score,
                'momentum_direction': 'BULLISH' if momentum_score > 0.3 else 'BEARISH' if momentum_score < -0.3 else 'NEUTRAL',
                'momentum_strength': abs(momentum_score)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing momentum trajectory for {symbol}: {e}")
            return {'status': 'error'}
    
    def generate_actionable_signals(self, symbol: str, current_market_data: Dict, 
                                  technical_indicators: Dict, orderbook_analysis: Dict) -> List[ActionableSignal]:
        """Generate actionable trading signals with trajectory confirmation"""
        try:
            # Add current data to history
            self.add_market_data(symbol, current_market_data, technical_indicators)
            
            # Analyze trajectories
            price_trajectory = self.analyze_price_trajectory(symbol)
            momentum_trajectory = self.analyze_momentum_trajectory(symbol)

            # Determine market regime
            current_adx = technical_indicators.get('adx')
            market_regime = self.get_market_regime(current_adx)
            logger.info(f"Market regime for {symbol}: {market_regime.value} (ADX: {current_adx:.2f} if current_adx is not None else 'N/A')")
            
            if price_trajectory.get('status') == 'insufficient_data':
                return []
            
            signals = []
            current_price = current_market_data.get('ltp', 0)
            
            # Generate BUY signals
            buy_signal = self._evaluate_buy_opportunity(
                symbol, current_price, price_trajectory, momentum_trajectory, 
                technical_indicators, orderbook_analysis, market_regime
            )
            if buy_signal:
                signals.append(buy_signal)
            
            # Generate SELL signals
            sell_signal = self._evaluate_sell_opportunity(
                symbol, current_price, price_trajectory, momentum_trajectory,
                technical_indicators, orderbook_analysis, market_regime
            )
            if sell_signal:
                signals.append(sell_signal)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating actionable signals for {symbol}: {e}")
            return []
    
    def _evaluate_buy_opportunity(self, symbol: str, current_price: float, 
                                price_trajectory: Dict, momentum_trajectory: Dict,
                                technical_indicators: Dict, orderbook_analysis: Dict, market_regime: MarketRegime) -> Optional[ActionableSignal]:
        """Evaluate if there's a confirmed buy opportunity"""
        try:
            reasons = []
            confidence_factors = []

            # ADD: Simple regime check at the beginning
            if market_regime == MarketRegime.RANGING and price_trajectory.get('overall_trajectory') in ['STRONG_BULLISH']:
                reasons.append("Skipping buy signal: Strong bullish trajectory in ranging market (potential false signal)")
                return None  # Skip momentum signals in ranging markets
            
            # Check price trajectory confirmation
            trajectory_confirmed = False

            # Apply market regime filter
            if market_regime == MarketRegime.TRENDING:
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BULLISH', 'BULLISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Confirmed bullish trajectory ({price_trajectory['trend_consistency']:.2f}) in TRENDING market")
                    confidence_factors.append(0.3)
            elif market_regime == MarketRegime.RANGING:
                rsi_traj = momentum_trajectory.get('rsi_trajectory', {})
                if (rsi_traj.get('direction') == 'RECOVERING' and 
                    rsi_traj.get('current_level') == 'OVERSOLD_RECOVERY'):
                    trajectory_confirmed = True
                    reasons.append("RSI recovering from oversold in RANGING market")
                    confidence_factors.append(0.3)
            else: # UNDEFINED or Transitional
                # Default to existing logic but with lower confidence
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BULLISH', 'BULLISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Bullish trajectory ({price_trajectory['trend_consistency']:.2f}) in UNDEFINED market")
                    confidence_factors.append(0.15) # Lower confidence

            # Momentum confirmation
            momentum_dir = momentum_trajectory.get('momentum_direction')
            momentum_strength = momentum_trajectory.get('momentum_strength', 0)
            
            if momentum_dir == 'BULLISH' and momentum_strength > 0.4:
                reasons.append(f"Strong bullish momentum ({momentum_strength:.2f})")
                confidence_factors.append(0.25)
            
            # RSI oversold recovery (only if not already covered by ranging market logic)
            if market_regime != MarketRegime.RANGING:
                rsi_traj = momentum_trajectory.get('rsi_trajectory', {})
                if (rsi_traj.get('direction') == 'RECOVERING' and 
                    rsi_traj.get('current_level') == 'OVERSOLD_RECOVERY'):
                    reasons.append("RSI recovering from oversold")
                    confidence_factors.append(0.2)
            
            # MACD bullish crossover confirmation
            macd_traj = momentum_trajectory.get('macd_trajectory', {})
            if macd_traj.get('signal') == 'BULLISH_CROSSOVER_CONFIRMED':
                reasons.append("MACD bullish crossover confirmed")
                confidence_factors.append(0.2)
            
            # Volume confirmation
            volume_traj = momentum_trajectory.get('volume_trajectory', {})
            if volume_traj.get('direction') == 'INCREASING':
                reasons.append("Increasing volume support")
                confidence_factors.append(0.15)
            
            # Orderbook support
            if orderbook_analysis:
                overall_assessment = orderbook_analysis.get('overall_assessment', {})
                if overall_assessment.get('entry_recommendation') == 'BUY':
                    reasons.append("Orderbook shows buying pressure")
                    confidence_factors.append(0.1)
            
            # Check if we have enough confirmation
            total_confidence = sum(confidence_factors)
            
            if trajectory_confirmed and total_confidence >= 0.6 and len(reasons) >= 3:
                # Calculate targets and stop loss
                atr = technical_indicators.get('atr', current_price * 0.02)  # Default 2% if no ATR
                
                target_price = current_price * (1 + 0.03 + momentum_strength * 0.02)  # 3-5% target
                stop_loss = current_price * (1 - 0.015)  # 1.5% stop loss
                risk_reward = (target_price - current_price) / (current_price - stop_loss)
                
                # Determine signal strength
                if total_confidence > 0.85:
                    strength = SignalStrength.VERY_STRONG
                elif total_confidence > 0.75:
                    strength = SignalStrength.STRONG
                elif total_confidence > 0.65:
                    strength = SignalStrength.MODERATE
                else:
                    strength = SignalStrength.WEAK
                
                # Determine time horizon
                if price_trajectory.get('acceleration', 0) > 0.5:
                    time_horizon = 'SHORT'
                elif momentum_strength > 0.6:
                    time_horizon = 'MEDIUM'
                else:
                    time_horizon = 'LONG'
                
                signal_quality = self.tech_indicators.validate_signal_quality(symbol, 'BUY')

                return ActionableSignal(
                    symbol=symbol,
                    signal_type='BUY',
                    strength=strength,
                    confidence=total_confidence,
                    entry_price=current_price,
                    target_price=target_price,
                    stop_loss=stop_loss,
                    trajectory_confirmed=True,
                    time_horizon=time_horizon,
                    reasons=reasons,
                    risk_reward_ratio=risk_reward,
                    timestamp=datetime.now(self.ist_timezone),
                    signal_quality_score=signal_quality
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error evaluating buy opportunity for {symbol}: {e}")
            return None
    
    def _evaluate_sell_opportunity(self, symbol: str, current_price: float,
                                 price_trajectory: Dict, momentum_trajectory: Dict,
                                 technical_indicators: Dict, orderbook_analysis: Dict, market_regime: MarketRegime) -> Optional[ActionableSignal]:
        """Evaluate if there's a confirmed sell opportunity"""
        try:
            reasons = []
            confidence_factors = []

            # ADD: Simple regime check at the beginning
            if market_regime == MarketRegime.RANGING and price_trajectory.get('overall_trajectory') in ['STRONG_BEARISH']:
                reasons.append("Skipping sell signal: Strong bearish trajectory in ranging market (potential false signal)")
                return None  # Skip momentum signals in ranging markets
            
            # Check price trajectory confirmation
            trajectory_confirmed = False

            # Apply market regime filter
            if market_regime == MarketRegime.TRENDING:
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BEARISH', 'BEARISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Confirmed bearish trajectory ({price_trajectory['trend_consistency']:.2f}) in TRENDING market")
                    confidence_factors.append(0.3)
            elif market_regime == MarketRegime.RANGING:
                rsi_traj = momentum_trajectory.get('rsi_trajectory', {})
                if (rsi_traj.get('direction') == 'DECLINING' and 
                    rsi_traj.get('current_level') == 'OVERBOUGHT_BREAKDOWN'):
                    trajectory_confirmed = True
                    reasons.append("RSI breaking down from overbought in RANGING market")
                    confidence_factors.append(0.3)
            else: # UNDEFINED or Transitional
                # Default to existing logic but with lower confidence
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BEARISH', 'BEARISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Bearish trajectory ({price_trajectory['trend_consistency']:.2f}) in UNDEFINED market")
                    confidence_factors.append(0.15) # Lower confidence
            
            # Momentum confirmation
            momentum_dir = momentum_trajectory.get('momentum_direction')
            momentum_strength = momentum_trajectory.get('momentum_strength', 0)
            
            if momentum_dir == 'BEARISH' and momentum_strength > 0.4:
                reasons.append(f"Strong bearish momentum ({momentum_strength:.2f})")
                confidence_factors.append(0.25)
            
            # RSI overbought breakdown (only if not already covered by ranging market logic)
            if market_regime != MarketRegime.RANGING:
                rsi_traj = momentum_trajectory.get('rsi_trajectory', {})
                if (rsi_traj.get('direction') == 'DECLINING' and 
                    rsi_traj.get('current_level') == 'OVERBOUGHT_BREAKDOWN'):
                    reasons.append("RSI breaking down from overbought")
                    confidence_factors.append(0.2)
            
            # MACD bearish crossover confirmation
            macd_traj = momentum_trajectory.get('macd_trajectory', {})
            if macd_traj.get('signal') == 'BEARISH_CROSSOVER_CONFIRMED':
                reasons.append("MACD bearish crossover confirmed")
                confidence_factors.append(0.2)
            
            # Volume confirmation on decline
            volume_traj = momentum_trajectory.get('volume_trajectory', {})
            if (volume_traj.get('direction') == 'INCREASING' and 
                price_trajectory.get('short_term', {}).get('direction') == 'DOWN'):
                reasons.append("Increasing volume on decline")
                confidence_factors.append(0.15)
            
            # Orderbook selling pressure
            if orderbook_analysis:
                overall_assessment = orderbook_analysis.get('overall_assessment', {})
                if overall_assessment.get('entry_recommendation') == 'SELL':
                    reasons.append("Orderbook shows selling pressure")
                    confidence_factors.append(0.1)
            
            # Check if we have enough confirmation
            total_confidence = sum(confidence_factors)
            
            if trajectory_confirmed and total_confidence >= 0.6 and len(reasons) >= 3:
                # Calculate targets and stop loss
                target_price = current_price * (1 - 0.03 - momentum_strength * 0.02)  # 3-5% target
                stop_loss = current_price * (1 + 0.015)  # 1.5% stop loss
                risk_reward = (current_price - target_price) / (stop_loss - current_price)
                
                # Determine signal strength
                if total_confidence > 0.85:
                    strength = SignalStrength.VERY_STRONG
                elif total_confidence > 0.75:
                    strength = SignalStrength.STRONG
                elif total_confidence > 0.65:
                    strength = SignalStrength.MODERATE
                else:
                    strength = SignalStrength.WEAK
                
                # Determine time horizon
                if price_trajectory.get('acceleration', 0) < -0.5:
                    time_horizon = 'SHORT'
                elif momentum_strength > 0.6:
                    time_horizon = 'MEDIUM'
                else:
                    time_horizon = 'LONG'
                
                signal_quality = self.tech_indicators.validate_signal_quality(symbol, 'SELL')

                return ActionableSignal(
                    symbol=symbol,
                    signal_type='SELL',
                    strength=strength,
                    confidence=total_confidence,
                    entry_price=current_price,
                    target_price=target_price,
                    stop_loss=stop_loss,
                    trajectory_confirmed=True,
                    time_horizon=time_horizon,
                    reasons=reasons,
                    risk_reward_ratio=risk_reward,
                    timestamp=datetime.now(self.ist_timezone),
                    signal_quality_score=signal_quality
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error evaluating sell opportunity for {symbol}: {e}")
            return None
    
    def _calculate_trend_strength(self, prices: List[float]) -> float:
        """Calculate trend strength using linear regression"""
        if len(prices) < 2:
            return 0
        
        x = np.arange(len(prices))
        y = np.array(prices)
        
        # Linear regression
        slope, intercept = np.polyfit(x, y, 1)
        
        # Calculate R-squared
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        
        # Trend strength is R-squared weighted by slope direction
        trend_strength = r_squared * (1 if slope > 0 else -1)
        return max(-1, min(1, trend_strength))
    
    def _calculate_acceleration(self, prices: List[float]) -> float:
        """Calculate price acceleration (rate of change of change)"""
        if len(prices) < 3:
            return 0
        
        # Calculate first differences (velocity)
        velocities = np.diff(prices)
        
        # Calculate second differences (acceleration)
        accelerations = np.diff(velocities)
        
        # Return average acceleration over the period
        return np.mean(accelerations) if len(accelerations) > 0 else 0
    
    def _calculate_trend_consistency(self, trend_strengths: List[float]) -> float:
        """Calculate consistency across different timeframes"""
        if not trend_strengths:
            return 0
        
        # Check if trends are in same direction
        positive_trends = sum(1 for t in trend_strengths if t > 0.2)
        negative_trends = sum(1 for t in trend_strengths if t < -0.2)
        
        # Consistency is higher when trends align
        if positive_trends >= 2:
            return np.mean([t for t in trend_strengths if t > 0])
        elif negative_trends >= 2:
            return abs(np.mean([t for t in trend_strengths if t < 0]))
        else:
            return 0.1  # Low consistency
    
    def _determine_overall_trajectory(self, short_trend: float, medium_trend: float, 
                                    long_trend: float, consistency: float) -> str:
        """Determine overall trajectory based on multiple timeframes"""
        avg_trend = (short_trend * 0.5 + medium_trend * 0.3 + long_trend * 0.2)
        
        if consistency > 0.7 and avg_trend > 0.4:
            return TrendDirection.STRONG_BULLISH.value
        elif consistency > 0.5 and avg_trend > 0.2:
            return TrendDirection.BULLISH.value
        elif consistency > 0.7 and avg_trend < -0.4:
            return TrendDirection.STRONG_BEARISH.value
        elif consistency > 0.5 and avg_trend < -0.2:
            return TrendDirection.BEARISH.value
        else:
            return TrendDirection.NEUTRAL.value
    
    def _analyze_indicator_trajectory(self, values: List[float], indicator_type: str) -> Dict:
        """Analyze trajectory of any indicator"""
        if len(values) < 3:
            return {'status': 'insufficient_data'}
        
        # Calculate trend
        trend_strength = self._calculate_trend_strength(values)
        
        # Determine current level for RSI
        current_level = 'NORMAL'
        if indicator_type == 'rsi' and values:
            current_rsi = values[-1]
            if current_rsi < self.rsi_oversold_threshold:
                current_level = 'OVERSOLD'
            elif current_rsi > self.rsi_overbought_threshold:
                current_level = 'OVERBOUGHT'
            elif self.rsi_oversold_threshold <= current_rsi <= (self.rsi_oversold_threshold + 10) and trend_strength > 0:
                current_level = 'OVERSOLD_RECOVERY'
            elif (self.rsi_overbought_threshold - 10) <= current_rsi <= self.rsi_overbought_threshold and trend_strength < 0:
                current_level = 'OVERBOUGHT_BREAKDOWN'
        
        return {
            'direction': 'INCREASING' if trend_strength > 0.2 else 'DECREASING' if trend_strength < -0.2 else 'SIDEWAYS',
            'strength': abs(trend_strength),
            'current_level': current_level,
            'values': values[-5:],  # Last 5 values
            'trend_strength': trend_strength
        }
    
    def _analyze_macd_trajectory(self, macd_values: List[float], signal_values: List[float]) -> Dict:
        """Analyze MACD trajectory"""
        if len(macd_values) < 3 or len(signal_values) < 3:
            return {'status': 'insufficient_data'}
        
        # Check for crossovers
        signal = 'NEUTRAL'
        
        # Recent crossover check
        if len(macd_values) >= 2 and len(signal_values) >= 2:
            current_diff = macd_values[-1] - signal_values[-1]
            prev_diff = macd_values[-2] - signal_values[-2]
            
            if prev_diff <= 0 and current_diff > 0:
                # Confirm if this crossover is sustained
                if len(macd_values) >= 3:
                    if (macd_values[-1] - signal_values[-1]) > (macd_values[-2] - signal_values[-2]):
                        signal = 'BULLISH_CROSSOVER_CONFIRMED'
                    else:
                        signal = 'BULLISH_CROSSOVER'
                        
            elif prev_diff >= 0 and current_diff < 0:
                # Confirm if this crossover is sustained
                if len(macd_values) >= 3:
                    if (macd_values[-1] - signal_values[-1]) < (macd_values[-2] - signal_values[-2]):
                        signal = 'BEARISH_CROSSOVER_CONFIRMED'
                    else:
                        signal = 'BEARISH_CROSSOVER'
        
        # MACD trend
        macd_trend = self._calculate_trend_strength(macd_values)
        
        return {
            'signal': signal,
            'macd_trend': macd_trend,
            'divergence': current_diff if len(macd_values) > 0 and len(signal_values) > 0 else 0,
            'strength': abs(macd_trend)
        }
    
    def _analyze_ma_trajectory(self, momentum_data: List[Dict]) -> Dict:
        """Analyze moving average trajectory"""
        sma_5_values = [d['sma_5'] for d in momentum_data if d['sma_5'] is not None]
        sma_20_values = [d['sma_20'] for d in momentum_data if d['sma_20'] is not None]
        
        if len(sma_5_values) < 3 or len(sma_20_values) < 3:
            return {'status': 'insufficient_data'}
        
        # Check for golden/death cross
        signal = 'NEUTRAL'
        if len(sma_5_values) >= 2 and len(sma_20_values) >= 2:
            current_diff = sma_5_values[-1] - sma_20_values[-1]
            prev_diff = sma_5_values[-2] - sma_20_values[-2]
            
            if prev_diff <= 0 and current_diff > 0:
                signal = 'GOLDEN_CROSS'
            elif prev_diff >= 0 and current_diff < 0:
                signal = 'DEATH_CROSS'
            elif current_diff > 0:
                signal = 'BULLISH_ALIGNMENT'
            else:
                signal = 'BEARISH_ALIGNMENT'
        
        return {
            'signal': signal,
            'sma_5_trend': self._calculate_trend_strength(sma_5_values),
            'sma_20_trend': self._calculate_trend_strength(sma_20_values),
            'alignment': 'BULLISH' if current_diff > 0 else 'BEARISH'
        }
    
    def _calculate_momentum_score(self, rsi_traj: Dict, macd_traj: Dict, 
                                sma_traj: Dict, volume_traj: Dict) -> float:
        """Calculate combined momentum score"""
        score = 0
        
        # RSI contribution
        if rsi_traj.get('current_level') == 'OVERSOLD_RECOVERY':
            score += 0.3
        elif rsi_traj.get('current_level') == 'OVERBOUGHT_BREAKDOWN':
            score -= 0.3
        
        if rsi_traj.get('direction') == 'INCREASING':
            score += 0.1 * rsi_traj.get('strength', 0)
        elif rsi_traj.get('direction') == 'DECREASING':
            score -= 0.1 * rsi_traj.get('strength', 0)
        
        # MACD contribution
        if macd_traj.get('signal') == 'BULLISH_CROSSOVER_CONFIRMED':
            score += 0.4
        elif macd_traj.get('signal') == 'BEARISH_CROSSOVER_CONFIRMED':
            score -= 0.4
        elif macd_traj.get('signal') == 'BULLISH_CROSSOVER':
            score += 0.2
        elif macd_traj.get('signal') == 'BEARISH_CROSSOVER':
            score -= 0.2
        
        # SMA contribution
        if sma_traj.get('signal') == 'GOLDEN_CROSS':
            score += 0.3
        elif sma_traj.get('signal') == 'DEATH_CROSS':
            score -= 0.3
        elif sma_traj.get('alignment') == 'BULLISH':
            score += 0.1
        elif sma_traj.get('alignment') == 'BEARISH':
            score -= 0.1
        
        # Volume contribution
        if volume_traj.get('direction') == 'INCREASING':
            score += 0.1 * volume_traj.get('strength', 0)
        
        return max(-1, min(1, score))
    
    def get_market_regime(self, adx_value: Optional[float]) -> MarketRegime:
        """Determine market regime based on ADX value."""
        if adx_value is None:
            return MarketRegime.UNDEFINED
        
        if adx_value > 25:
            return MarketRegime.TRENDING
        elif adx_value < 20:
            return MarketRegime.RANGING
        else:
            return MarketRegime.UNDEFINED # Or a 'TRANSITIONAL' regime
    
    def get_actionable_signals_summary(self, signals: List[ActionableSignal]) -> Dict:
        """Get summary of actionable signals"""
        if not signals:
            return {'total_signals': 0}
        
        buy_signals = [s for s in signals if s.signal_type == 'BUY']
        sell_signals = [s for s in signals if s.signal_type == 'SELL']
        
        return {
            'total_signals': len(signals),
            'buy_signals': len(buy_signals),
            'sell_signals': len(sell_signals),
            'avg_confidence': np.mean([s.confidence for s in signals]),
            'strong_signals': len([s for s in signals if s.strength in [SignalStrength.STRONG, SignalStrength.VERY_STRONG]]),
            'best_buy': max(buy_signals, key=lambda x: x.confidence) if buy_signals else None,
            'best_sell': max(sell_signals, key=lambda x: x.confidence) if sell_signals else None,
            'timestamp': datetime.now(self.ist_timezone).isoformat()
        }

# Example usage
if __name__ == "__main__":
    generator = TrajectorySignalGenerator()
    
    # Test with sample data
    sample_market_data = {
        'ltp': 100.50,
        'ltq': 25,
        'best_bid': 100.45,
        'best_ask': 100.55,
        'spread_pct': 0.1
    }
    
    sample_technical = {
        'rsi': 35,
        'macd': {'macd': 0.5, 'signal': 0.3},
        'sma': {'sma_5': 100.2, 'sma_20': 99.8},
        'volume': {'volume_ratio': 1.5}
    }
    
    # Generate signals
    signals = generator.generate_actionable_signals(
        'TEST_STOCK', sample_market_data, sample_technical, {}
    )
    
    print(f"Generated {len(signals)} actionable signals")
    for signal in signals:
        print(f"Signal: {signal.signal_type} - Confidence: {signal.confidence:.2f} - Reasons: {signal.reasons}")
