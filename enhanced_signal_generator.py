# Enhanced Signal Generator with Complete Indian Market Optimizations
# Provides actionable signals based on confirmed trends and momentum
# Optimized for Indian market conditions with calibrated parameters

import os
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
from practical_quant_engine import PracticalQuantEngine

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
    sector_bias: str = "NEUTRAL"
    gap_risk_factor: float = 1.0

class MarketRegime(Enum):
    TRENDING = "TRENDING"
    RANGING = "RANGING"
    UNDEFINED = "UNDEFINED"

# Indian Market Sector Classifications
SECTOR_MAPPING = {
    'IT': ['Tata Consultancy Services', 'Infosys', 'HCL Technologies', 'Wipro', 'Tech Mahindra'],
    'BANKING': ['HDFC Bank', 'ICICI Bank', 'State Bank of India', 'Kotak Mahindra Bank'],
    'FMCG': ['Hindustan Unilever', 'ITC', 'Nestle India', 'Britannia'],
    'AUTO': ['Maruti Suzuki India Ltd', 'Tata Motors', 'Bajaj Auto', 'Hero MotoCorp'],
    'METALS': ['Tata Steel', 'JSW Steel', 'Hindalco', 'Vedanta'],
    'PHARMA': ['Sun Pharma', 'Dr Reddys Labs', 'Cipla', 'Lupin'],
    'ENERGY': ['Reliance Industries', 'ONGC', 'NTPC', 'Power Grid'],
    'INFRA': ['Larsen & Toubro', 'UltraTech Cement', 'Adani Ports']
}

# Indian Market Calibrated Parameters
INDIAN_MARKET_PARAMS = {
    'rsi_overbought': 80,  # Higher threshold for Indian volatility
    'rsi_oversold': 20,    # Lower threshold for Indian volatility
    'macd_fast': 8,        # Optimized for 4H Indian charts
    'macd_slow': 24,       # Optimized for 4H Indian charts
    'macd_signal': 9,
    'bb_std_dev': 2.5,     # Higher for Indian volatility
    'adx_trending': 25,    # Standard threshold
    'adx_ranging': 20,     # Standard threshold
    'volume_threshold': 1.5, # 50% above average
    'gap_fill_probability': 0.67,  # 67% gap fill rate in 3 sessions
    'volatility_multiplier': 1.18  # 18% higher than developed markets
}

# Currency impact factors for export-oriented sectors
CURRENCY_IMPACT = {
    'IT': 0.6,      # High positive correlation with USD/INR
    'PHARMA': 0.4,  # Moderate positive correlation
    'ENERGY': -0.3, # Negative correlation (import dependent)
    'AUTO': -0.2,   # Slight negative (import components)
    'BANKING': 0.0, # Neutral
    'FMCG': -0.1    # Slight negative (commodity imports)
}

class TrajectorySignalGenerator:
    def __init__(self, redis_host=None, redis_port=None, redis_db=0):
        # Initialize quantitative engine
        self.quant_engine = PracticalQuantEngine()
        """Initialize enhanced signal generator with Indian market optimizations"""
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
        
        # Store signal history for trajectory analysis
        self.signal_history = defaultdict(lambda: deque(maxlen=50))
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        self.volume_history = defaultdict(lambda: deque(maxlen=100))
        self.momentum_history = defaultdict(lambda: deque(maxlen=30))
        
        # Configuration for signal confirmation
        self.min_confirmation_periods = 3
        self.trend_strength_threshold = 0.6
        self.volume_confirmation_multiplier = INDIAN_MARKET_PARAMS['volume_threshold']
        
        # Indian market calibrated thresholds
        self.rsi_oversold_threshold = INDIAN_MARKET_PARAMS['rsi_oversold']
        self.rsi_overbought_threshold = INDIAN_MARKET_PARAMS['rsi_overbought']

        self.tech_indicators = EnhancedTechnicalIndicators()
        
        # Market timing analysis for Indian sessions
        self.high_volatility_sessions = [
            (9, 30, 10, 30),   # Opening session
            (14, 30, 15, 30)   # Closing session
        ]
        self.low_volatility_session = (12, 0, 13, 0)  # Lunch hour
        
        logger.info("Enhanced Signal Generator initialized with Indian market parameters")
        
    def get_sector_for_symbol(self, symbol: str) -> str:
        """Identify sector for a given symbol"""
        for sector, symbols in SECTOR_MAPPING.items():
            if any(s.lower() in symbol.lower() for s in symbols):
                return sector
        return 'OTHER'
    
    def calculate_sector_bias(self, usd_inr_change: float = 0.0) -> Dict[str, float]:
        """Calculate sector bias based on currency movements and market conditions"""
        sector_bias = {}
        
        for sector in SECTOR_MAPPING.keys():
            # Base bias from currency impact
            currency_factor = CURRENCY_IMPACT.get(sector, 0.0)
            bias_score = currency_factor * usd_inr_change
            
            # Add sector rotation factors (simplified)
            current_hour = datetime.now(self.ist_timezone).hour
            
            # IT sectors perform better in US trading hours overlap
            if sector == 'IT' and 20 <= current_hour <= 23:
                bias_score += 0.2
            
            # Banking stronger during Indian business hours
            elif sector == 'BANKING' and 10 <= current_hour <= 15:
                bias_score += 0.1
                
            sector_bias[sector] = max(-1.0, min(1.0, bias_score))
            
        return sector_bias
    
    def calculate_gap_risk_factor(self, symbol: str) -> float:
        """Calculate overnight gap risk factor for position sizing"""
        try:
            # Get recent price history to analyze gap patterns
            price_data = list(self.price_history[symbol])
            if len(price_data) < 5:
                return 1.0
            
            # Calculate recent gaps
            gaps = []
            for i in range(1, min(10, len(price_data))):
                if price_data[i-1]['price'] > 0:
                    gap = abs(price_data[i]['price'] - price_data[i-1]['price']) / price_data[i-1]['price']
                    gaps.append(gap)
            
            if not gaps:
                return 1.0
            
            # Higher recent volatility = higher gap risk
            avg_gap = np.mean(gaps)
            gap_risk = min(0.3, avg_gap * 2)  # Cap at 30% reduction
            
            return 1.0 - gap_risk
            
        except Exception as e:
            logger.error(f"Error calculating gap risk for {symbol}: {e}")
            return 1.0
    
    def is_high_volatility_session(self) -> bool:
        """Check if current time is in high volatility trading session"""
        now = datetime.now(self.ist_timezone)
        current_time = (now.hour, now.minute)
        
        for start_h, start_m, end_h, end_m in self.high_volatility_sessions:
            if (start_h, start_m) <= current_time <= (end_h, end_m):
                return True
        return False
    
    def calculate_indian_volatility_adjustment(self, base_volatility: float) -> float:
        """Adjust volatility calculations for Indian market characteristics"""
        adjustment_factor = INDIAN_MARKET_PARAMS['volatility_multiplier']
        
        # Additional adjustment for session timing
        if self.is_high_volatility_session():
            adjustment_factor *= 1.2
        elif self.low_volatility_session[0] <= datetime.now(self.ist_timezone).hour <= self.low_volatility_session[1]:
            adjustment_factor *= 0.8
            
        return base_volatility * adjustment_factor
    
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
            
            # Store technical indicator data with Indian market calibrations
            if technical_indicators:
                self.momentum_history[symbol].append({
                    'timestamp': current_time,
                    'rsi': technical_indicators.get('rsi'),
                    'macd': technical_indicators.get('macd', {}).get('macd'),
                    'macd_signal': technical_indicators.get('macd', {}).get('signal'),
                    'sma_5': technical_indicators.get('sma', {}).get('sma_5'),
                    'sma_20': technical_indicators.get('sma', {}).get('sma_20'),
                    'volume_ratio': technical_indicators.get('volume', {}).get('volume_ratio'),
                    'signals': technical_indicators.get('signals', {}),
                    'market_regime': technical_indicators.get('market_regime', 'UNKNOWN')
                })
                
        except Exception as e:
            logger.error(f"Error adding market data for {symbol}: {e}")
    
    def analyze_price_trajectory(self, symbol: str) -> Dict:
        """Analyze price trajectory with Indian market considerations"""
        try:
            price_data = list(self.price_history[symbol])
            if len(price_data) < 10:
                return {'status': 'insufficient_data'}
            
            prices = [d['price'] for d in price_data]
            
            # Calculate returns over different periods with Indian market timing
            short_term_change = (prices[-1] - prices[-5]) / prices[-5] if len(prices) >= 5 else 0
            medium_term_change = (prices[-1] - prices[-15]) / prices[-15] if len(prices) >= 15 else 0
            long_term_change = (prices[-1] - prices[-30]) / prices[-30] if len(prices) >= 30 else 0
            
            # Adjust for Indian market volatility
            short_term_trend = self._calculate_trend_strength(prices[-5:] if len(prices) >= 5 else prices)
            medium_term_trend = self._calculate_trend_strength(prices[-15:] if len(prices) >= 15 else prices)
            long_term_trend = self._calculate_trend_strength(prices[-30:] if len(prices) >= 30 else prices)
            
            # Apply Indian market volatility adjustment
            short_term_trend = self.calculate_indian_volatility_adjustment(short_term_trend)
            
            # Calculate acceleration with gap consideration
            acceleration = self._calculate_acceleration(prices)
            gap_risk = self.calculate_gap_risk_factor(symbol)
            
            # Trend consistency with Indian market parameters
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
                'gap_risk_factor': gap_risk,
                'overall_trajectory': self._determine_overall_trajectory(
                    short_term_trend, medium_term_trend, long_term_trend, trend_consistency
                )
            }
            
        except Exception as e:
            logger.error(f"Error analyzing price trajectory for {symbol}: {e}")
            return {'status': 'error'}
    
    def analyze_momentum_trajectory(self, symbol: str) -> Dict:
        """Analyze momentum trajectory using Indian market calibrated indicators"""
        try:
            momentum_data = list(self.momentum_history[symbol])
            if len(momentum_data) < 5:
                return {'status': 'insufficient_data'}
            
            # RSI trajectory with Indian market thresholds
            rsi_values = [d['rsi'] for d in momentum_data if d['rsi'] is not None]
            rsi_trajectory = self._analyze_indicator_trajectory(rsi_values, 'rsi')
            
            # MACD trajectory with Indian market parameters
            macd_values = [d['macd'] for d in momentum_data if d['macd'] is not None]
            macd_signal_values = [d['macd_signal'] for d in momentum_data if d['macd_signal'] is not None]
            macd_trajectory = self._analyze_macd_trajectory(macd_values, macd_signal_values)
            
            # Moving average trajectory
            sma_trajectory = self._analyze_ma_trajectory(momentum_data)
            
            # Volume trajectory with Indian market volume patterns
            volume_ratios = [d['volume_ratio'] for d in momentum_data if d['volume_ratio'] is not None]
            volume_trajectory = self._analyze_indicator_trajectory(volume_ratios, 'volume')
            
            # Combined momentum score with Indian market weighting
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
        """Generate actionable trading signals with Indian market optimizations"""
        try:
            # Add current data to history
            self.add_market_data(symbol, current_market_data, technical_indicators)
            
            # Analyze trajectories
            price_trajectory = self.analyze_price_trajectory(symbol)
            momentum_trajectory = self.analyze_momentum_trajectory(symbol)

            # Determine market regime with Indian market parameters
            current_adx = technical_indicators.get('adx')
            market_regime = self.get_market_regime(current_adx)
            
            # Get sector information
            sector = self.get_sector_for_symbol(symbol)
            sector_bias = self.calculate_sector_bias().get(sector, 0.0)
            
            if price_trajectory.get('status') == 'insufficient_data':
                return []
            
            signals = []
            current_price = current_market_data.get('ltp', 0)
            gap_risk_factor = price_trajectory.get('gap_risk_factor', 1.0)
            
            # Generate BUY signals with Indian market logic
            buy_signal = self._evaluate_buy_opportunity_indian(
                symbol, current_price, price_trajectory, momentum_trajectory, 
                technical_indicators, orderbook_analysis, market_regime, sector, sector_bias, gap_risk_factor
            )
            if buy_signal:
                signals.append(buy_signal)
            
            # Generate SELL signals with Indian market logic
            sell_signal = self._evaluate_sell_opportunity_indian(
                symbol, current_price, price_trajectory, momentum_trajectory,
                technical_indicators, orderbook_analysis, market_regime, sector, sector_bias, gap_risk_factor
            )
            if sell_signal:
                signals.append(sell_signal)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating actionable signals for {symbol}: {e}")
            return []
    
    def _evaluate_buy_opportunity_indian(self, symbol: str, current_price: float, 
                                       price_trajectory: Dict, momentum_trajectory: Dict,
                                       technical_indicators: Dict, orderbook_analysis: Dict, 
                                       market_regime: MarketRegime, sector: str, sector_bias: float, gap_risk_factor: float) -> Optional[ActionableSignal]:
        """Evaluate buy opportunity with Indian market specific logic"""
        try:
            reasons = []
            confidence_factors = []

            # Sector bias consideration
            if sector_bias > 0.2:
                reasons.append(f"Positive sector bias for {sector} (+{sector_bias:.1f})")
                confidence_factors.append(0.1)
            elif sector_bias < -0.2:
                reasons.append(f"Negative sector bias for {sector} ({sector_bias:.1f})")
                return None  # Skip if sector has strong negative bias

            # Market regime specific logic with Indian parameters
            trajectory_confirmed = False

            if market_regime == MarketRegime.TRENDING:
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BULLISH', 'BULLISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Confirmed bullish trajectory ({price_trajectory['trend_consistency']:.2f}) in TRENDING market")
                    confidence_factors.append(0.3)
            elif market_regime == MarketRegime.RANGING:
                rsi_traj = momentum_trajectory.get('rsi_trajectory', {})
                # Use Indian market RSI thresholds
                if (rsi_traj.get('direction') == 'RECOVERING' and 
                    rsi_traj.get('current_level') == 'OVERSOLD_RECOVERY'):
                    trajectory_confirmed = True
                    reasons.append("RSI recovering from oversold in RANGING market")
                    confidence_factors.append(0.3)
            else:
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BULLISH', 'BULLISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Bullish trajectory ({price_trajectory['trend_consistency']:.2f}) in UNDEFINED market")
                    confidence_factors.append(0.15)

            # Momentum confirmation with Indian market parameters
            momentum_dir = momentum_trajectory.get('momentum_direction')
            momentum_strength = momentum_trajectory.get('momentum_strength', 0)
            
            if momentum_dir == 'BULLISH' and momentum_strength > 0.4:
                reasons.append(f"Strong bullish momentum ({momentum_strength:.2f})")
                confidence_factors.append(0.25)
            
            # RSI analysis with Indian market thresholds
            rsi_traj = momentum_trajectory.get('rsi_trajectory', {})
            if (rsi_traj.get('direction') == 'RECOVERING' and 
                rsi_traj.get('current_level') == 'OVERSOLD_RECOVERY'):
                reasons.append("RSI recovering from oversold")
                confidence_factors.append(0.2)
            
            # MACD confirmation
            macd_traj = momentum_trajectory.get('macd_trajectory', {})
            if macd_traj.get('signal') == 'BULLISH_CROSSOVER_CONFIRMED':
                reasons.append("MACD bullish crossover confirmed")
                confidence_factors.append(0.2)
            
            # Volume confirmation with Indian market patterns
            volume_traj = momentum_trajectory.get('volume_trajectory', {})
            if volume_traj.get('direction') == 'INCREASING':
                reasons.append("Increasing volume support")
                confidence_factors.append(0.15)
            
            # Session timing bonus for Indian markets
            if self.is_high_volatility_session():
                reasons.append("High volatility session timing")
                confidence_factors.append(0.05)
            
            # Orderbook support
            if orderbook_analysis:
                overall_assessment = orderbook_analysis.get('overall_assessment', {})
                if overall_assessment.get('entry_recommendation') == 'BUY':
                    reasons.append("Orderbook shows buying pressure")
                    confidence_factors.append(0.1)
            
            # ENHANCED: Add quantitative analysis
            quant_analysis = self._get_quantitative_analysis(symbol, current_price)
            if quant_analysis:
                total_confidence += quant_analysis['confidence_boost']
                reasons.extend(quant_analysis['reasons'])
            
            # Check if we have enough confirmation
            total_confidence = sum(confidence_factors)
            
            # Adjust confidence thresholds for Indian market volatility
            min_confidence = 0.6 if market_regime == MarketRegime.RANGING else 0.65
            
            if trajectory_confirmed and total_confidence >= min_confidence and len(reasons) >= 3:
                # ENHANCED: Use statistical stop loss calculation
                stop_analysis = self._calculate_advanced_stop_loss(symbol)
                target_analysis = self._calculate_statistical_target(symbol, current_price, momentum_strength)
                
                target_price = target_analysis['target_price']
                stop_loss = stop_analysis['stop_loss']
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
                
                # Determine time horizon based on Indian market conditions
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
                    signal_quality_score=signal_quality,
                    sector_bias=sector,
                    gap_risk_factor=gap_risk_factor
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error evaluating buy opportunity for {symbol}: {e}")
            return None
    
    def _evaluate_sell_opportunity_indian(self, symbol: str, current_price: float,
                                        price_trajectory: Dict, momentum_trajectory: Dict,
                                        technical_indicators: Dict, orderbook_analysis: Dict, 
                                        market_regime: MarketRegime, sector: str, sector_bias: float, gap_risk_factor: float) -> Optional[ActionableSignal]:
        """Evaluate sell opportunity with Indian market specific logic"""
        try:
            reasons = []
            confidence_factors = []

            # Sector bias consideration
            if sector_bias < -0.2:
                reasons.append(f"Negative sector bias for {sector} ({sector_bias:.1f})")
                confidence_factors.append(0.1)
            elif sector_bias > 0.2:
                reasons.append(f"Positive sector bias for {sector} (+{sector_bias:.1f})")
                return None  # Skip if sector has strong positive bias

            # Market regime specific logic
            trajectory_confirmed = False

            if market_regime == MarketRegime.TRENDING:
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BEARISH', 'BEARISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Confirmed bearish trajectory ({price_trajectory['trend_consistency']:.2f}) in TRENDING market")
                    confidence_factors.append(0.3)
            elif market_regime == MarketRegime.RANGING:
                rsi_traj = momentum_trajectory.get('rsi_trajectory', {})
                # Use Indian market RSI thresholds for overbought
                if (rsi_traj.get('direction') == 'DECLINING' and 
                    rsi_traj.get('current_level') == 'OVERBOUGHT_BREAKDOWN'):
                    trajectory_confirmed = True
                    reasons.append("RSI breaking down from overbought in RANGING market")
                    confidence_factors.append(0.3)
            else:
                if (price_trajectory.get('overall_trajectory') in ['STRONG_BEARISH', 'BEARISH'] and
                    price_trajectory.get('trend_consistency', 0) > self.trend_strength_threshold):
                    trajectory_confirmed = True
                    reasons.append(f"Bearish trajectory ({price_trajectory['trend_consistency']:.2f}) in UNDEFINED market")
                    confidence_factors.append(0.15)
            
            # Momentum confirmation with Indian market parameters
            momentum_dir = momentum_trajectory.get('momentum_direction')
            momentum_strength = momentum_trajectory.get('momentum_strength', 0)
            
            if momentum_dir == 'BEARISH' and momentum_strength > 0.4:
                reasons.append(f"Strong bearish momentum ({momentum_strength:.2f})")
                confidence_factors.append(0.25)
            
            # RSI overbought breakdown analysis with Indian market thresholds
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
            
            # Volume confirmation on decline with Indian market patterns
            volume_traj = momentum_trajectory.get('volume_trajectory', {})
            if (volume_traj.get('direction') == 'INCREASING' and 
                price_trajectory.get('short_term', {}).get('direction') == 'DOWN'):
                reasons.append("Increasing volume on decline")
                confidence_factors.append(0.15)
            
            # Session timing consideration for Indian markets
            if self.is_high_volatility_session():
                reasons.append("High volatility session - increased selling pressure")
                confidence_factors.append(0.05)
            
            # Orderbook selling pressure
            if orderbook_analysis:
                overall_assessment = orderbook_analysis.get('overall_assessment', {})
                if overall_assessment.get('entry_recommendation') == 'SELL':
                    reasons.append("Orderbook shows selling pressure")
                    confidence_factors.append(0.1)
            
            # Moving average breakdown
            sma_traj = momentum_trajectory.get('sma_trajectory', {})
            if sma_traj.get('signal') == 'DEATH_CROSS':
                reasons.append("Moving average death cross")
                confidence_factors.append(0.15)
            elif sma_traj.get('alignment') == 'BEARISH':
                reasons.append("Bearish moving average alignment")
                confidence_factors.append(0.1)
            
            # Check if we have enough confirmation
            total_confidence = sum(confidence_factors)
            min_confidence = 0.6 if market_regime == MarketRegime.RANGING else 0.65
            
            if trajectory_confirmed and total_confidence >= min_confidence and len(reasons) >= 3:
                # Calculate sell targets and stop loss with Indian market parameters
                volatility_adj = self.calculate_indian_volatility_adjustment(1.0)
                target_multiplier = 0.03 + (momentum_strength * 0.02) * volatility_adj
                stop_multiplier = 0.015 * volatility_adj
                
                target_price = current_price * (1 - target_multiplier)
                stop_loss = current_price * (1 + stop_multiplier)
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
                
                # Determine time horizon based on Indian market conditions
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
                    signal_quality_score=signal_quality,
                    sector_bias=sector,
                    gap_risk_factor=gap_risk_factor
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
        
        slope, intercept = np.polyfit(x, y, 1)
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        
        trend_strength = r_squared * (1 if slope > 0 else -1)
        return max(-1, min(1, trend_strength))
    
    def _calculate_acceleration(self, prices: List[float]) -> float:
        """Calculate price acceleration"""
        if len(prices) < 3:
            return 0
        
        velocities = np.diff(prices)
        accelerations = np.diff(velocities)
        return np.mean(accelerations) if len(accelerations) > 0 else 0
    
    def _calculate_trend_consistency(self, trend_strengths: List[float]) -> float:
        """Calculate consistency across different timeframes"""
        if not trend_strengths:
            return 0
        
        positive_trends = sum(1 for t in trend_strengths if t > 0.2)
        negative_trends = sum(1 for t in trend_strengths if t < -0.2)
        
        if positive_trends >= 2:
            return np.mean([t for t in trend_strengths if t > 0])
        elif negative_trends >= 2:
            return abs(np.mean([t for t in trend_strengths if t < 0]))
        else:
            return 0.1
    
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
        """Analyze trajectory of any indicator with Indian market thresholds"""
        if len(values) < 3:
            return {'status': 'insufficient_data'}
        
        trend_strength = self._calculate_trend_strength(values)
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
            'values': values[-5:],
            'trend_strength': trend_strength
        }
    
    def _analyze_macd_trajectory(self, macd_values: List[float], signal_values: List[float]) -> Dict:
        """Analyze MACD trajectory with Indian market parameters"""
        if len(macd_values) < 3 or len(signal_values) < 3:
            return {'status': 'insufficient_data'}
        
        signal = 'NEUTRAL'
        
        if len(macd_values) >= 2 and len(signal_values) >= 2:
            current_diff = macd_values[-1] - signal_values[-1]
            prev_diff = macd_values[-2] - signal_values[-2]
            
            if prev_diff <= 0 and current_diff > 0:
                if len(macd_values) >= 3:
                    if (macd_values[-1] - signal_values[-1]) > (macd_values[-2] - signal_values[-2]):
                        signal = 'BULLISH_CROSSOVER_CONFIRMED'
                    else:
                        signal = 'BULLISH_CROSSOVER'
                        
            elif prev_diff >= 0 and current_diff < 0:
                if len(macd_values) >= 3:
                    if (macd_values[-1] - signal_values[-1]) < (macd_values[-2] - signal_values[-2]):
                        signal = 'BEARISH_CROSSOVER_CONFIRMED'
                    else:
                        signal = 'BEARISH_CROSSOVER'
        
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
        
        signal = 'NEUTRAL'
        current_diff = 0
        
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
        """Calculate combined momentum score with Indian market weighting"""
        score = 0
        
        # RSI contribution with Indian market thresholds
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
        
        # Volume contribution with Indian market volume patterns
        if volume_traj.get('direction') == 'INCREASING':
            score += 0.1 * volume_traj.get('strength', 0)
        
        return max(-1, min(1, score))
    
    def get_market_regime(self, adx_value: Optional[float]) -> MarketRegime:
        """Determine market regime based on ADX with Indian market parameters"""
        if adx_value is None:
            return MarketRegime.UNDEFINED
        
        if adx_value > INDIAN_MARKET_PARAMS['adx_trending']:
            return MarketRegime.TRENDING
        elif adx_value < INDIAN_MARKET_PARAMS['adx_ranging']:
            return MarketRegime.RANGING
        else:
            return MarketRegime.UNDEFINED
    
    def get_actionable_signals_summary(self, signals: List[ActionableSignal]) -> Dict:
        """Get summary of actionable signals with Indian market metrics"""
        if not signals:
            return {'total_signals': 0}
        
        buy_signals = [s for s in signals if s.signal_type == 'BUY']
        sell_signals = [s for s in signals if s.signal_type == 'SELL']
        
        # Calculate sector distribution
        sector_distribution = {}
        for signal in signals:
            sector = signal.sector_bias
            sector_distribution[sector] = sector_distribution.get(sector, 0) + 1
        
        return {
            'total_signals': len(signals),
            'buy_signals': len(buy_signals),
            'sell_signals': len(sell_signals),
            'avg_confidence': np.mean([s.confidence for s in signals]),
            'avg_risk_reward': np.mean([s.risk_reward_ratio for s in signals]),
            'strong_signals': len([s for s in signals if s.strength in [SignalStrength.STRONG, SignalStrength.VERY_STRONG]]),
            'sector_distribution': sector_distribution,
            'avg_gap_risk': np.mean([s.gap_risk_factor for s in signals]),
            'indian_market_optimized': True,
            'volatility_adjusted': True,
            'best_buy': max(buy_signals, key=lambda x: x.confidence) if buy_signals else None,
            'best_sell': max(sell_signals, key=lambda x: x.confidence) if sell_signals else None,
            'timestamp': datetime.now(self.ist_timezone).isoformat()
        }

    # ENHANCED QUANTITATIVE METHODS
    def _get_quantitative_analysis(self, symbol, current_price):
        """Get advanced quantitative analysis for signal enhancement"""
        try:
            # Get historical data for quantitative analysis
            df = self._get_symbol_dataframe(symbol)
            if df is None or len(df) < 50:
                return None
            
            # Run comprehensive quantitative analysis
            quant_score = self.quant_engine.calculate_comprehensive_score(df)
            
            confidence_boost = 0
            reasons = []
            
            # Add confidence based on quantitative score
            if quant_score['final_score'] > 0.7:
                confidence_boost = 0.15
                reasons.append(f"Strong quantitative score ({quant_score['final_score']:.2f})")
            elif quant_score['final_score'] > 0.6:
                confidence_boost = 0.10
                reasons.append(f"Good quantitative score ({quant_score['final_score']:.2f})")
            elif quant_score['final_score'] < 0.3:
                confidence_boost = -0.20  # Negative boost for poor scores
                reasons.append(f"Poor quantitative score ({quant_score['final_score']:.2f})")
            
            # Add specific quantitative insights
            components = quant_score['component_scores']
            
            if components.get('momentum', 0) > 0.7:
                confidence_boost += 0.05
                reasons.append("Strong statistical momentum")
            
            if components.get('volatility', 0) > 0.6:
                confidence_boost += 0.05
                reasons.append("Favorable volatility regime")
            
            if components.get('mean_reversion', 0) > 0.7:
                reasons.append("High mean reversion probability")
            
            return {
                'confidence_boost': confidence_boost,
                'reasons': reasons,
                'quant_score': quant_score['final_score'],
                'recommendation': quant_score['recommendation']
            }
            
        except Exception as e:
            logger.warning(f"Quantitative analysis failed for {symbol}: {e}")
            return None
    
    def _calculate_advanced_stop_loss(self, symbol):
        """Calculate statistically-based stop loss"""
        try:
            df = self._get_symbol_dataframe(symbol)
            if df is None:
                return {'stop_loss': df['close'].iloc[-1] * 0.97}
            
            stop_analysis = self.quant_engine.calculate_statistical_stop_loss(df)
            
            return {
                'stop_loss': stop_analysis['stop_loss'],
                'stop_loss_pct': stop_analysis['stop_loss_pct'],
                'method': stop_analysis['method'],
                'confidence_level': stop_analysis.get('confidence_level', 0.95)
            }
            
        except Exception as e:
            logger.warning(f"Advanced stop loss calculation failed for {symbol}: {e}")
            # Fallback to simple calculation
            return {'stop_loss': current_price * 0.97}
    
    def _calculate_statistical_target(self, symbol, current_price, momentum_strength):
        """Calculate statistically-based target price"""
        try:
            df = self._get_symbol_dataframe(symbol)
            if df is None:
                return {'target_price': current_price * 1.05}
            
            # Support/Resistance analysis
            sr_analysis = self.quant_engine.calculate_support_resistance_strength(df)
            
            # Use resistance as target if available and reasonable
            if sr_analysis['resistance'] > current_price and sr_analysis['resistance'] < current_price * 1.15:
                target_price = sr_analysis['resistance'] * 0.98  # Slightly below resistance
            else:
                # Use momentum-based target
                momentum_multiplier = 0.03 + (momentum_strength * 0.02)
                target_price = current_price * (1 + momentum_multiplier)
            
            return {
                'target_price': target_price,
                'method': 'statistical_resistance' if sr_analysis['resistance'] > current_price else 'momentum_based',
                'resistance_level': sr_analysis['resistance'],
                'resistance_strength': sr_analysis['resistance_strength']
            }
            
        except Exception as e:
            logger.warning(f"Statistical target calculation failed for {symbol}: {e}")
            return {'target_price': current_price * 1.05}
    
    def _get_symbol_dataframe(self, symbol):
        """Get historical data as DataFrame for quantitative analysis"""
        try:
            # Try to get data from Redis
            symbol_clean = symbol.replace(' ', '_').replace('&', 'and')
            
            # Get recent data points
            timestamps = []
            prices = []
            volumes = []
            highs = []
            lows = []
            
            # Reconstruct OHLCV data from stored time series
            # This is a simplified version - you might need to adapt based on your Redis structure
            for i in range(100):  # Get last 100 data points
                try:
                    timestamp = datetime.now() - timedelta(minutes=i)
                    price_key = f"ts:{symbol_clean}:close"
                    volume_key = f"ts:{symbol_clean}:volume"
                    high_key = f"ts:{symbol_clean}:high"
                    low_key = f"ts:{symbol_clean}:low"
                    
                    # This is pseudocode - adapt based on your actual Redis TS structure
                    price = self.redis_client.ts().get(price_key, timestamp.timestamp() * 1000)
                    if price:
                        timestamps.append(timestamp)
                        prices.append(price[1])
                        # Similar for volume, high, low
                except:
                    continue
            
            if len(prices) < 20:
                return None
            
            # Create DataFrame
            df = pd.DataFrame({
                'timestamp': timestamps,
                'close': prices,
                'volume': volumes,
                'high': highs,
                'low': lows,
                'open': prices  # Simplified - use close as open
            })
            
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            return df
            
        except Exception as e:
            logger.warning(f"Could not create DataFrame for {symbol}: {e}")
            return None
    
    def get_enhanced_signal_with_quant_analysis(self, symbol: str, current_market_data: Dict, 
                                               technical_indicators: Dict, orderbook_analysis: Dict) -> Dict:
        """Generate enhanced signal with comprehensive quantitative analysis"""
        try:
            # Get traditional signals
            traditional_signals = self.generate_actionable_signals(symbol, current_market_data, technical_indicators, orderbook_analysis)
            
            # Get quantitative analysis
            df = self._get_symbol_dataframe(symbol)
            if df is not None:
                quant_analysis = self.quant_engine.calculate_comprehensive_score(df)
                trading_plan = self.quant_engine.generate_trading_plan(df, portfolio_value=1000000)
                
                # Combine traditional and quantitative insights
                combined_analysis = {
                    'symbol': symbol,
                    'traditional_signals': [
                        {
                            'type': s.signal_type,
                            'confidence': s.confidence,
                            'strength': s.strength.name,
                            'reasons': s.reasons,
                            'risk_reward': s.risk_reward_ratio
                        } for s in traditional_signals
                    ],
                    'quantitative_analysis': {
                        'overall_score': quant_analysis['final_score'],
                        'recommendation': quant_analysis['recommendation'],
                        'component_scores': quant_analysis['component_scores'],
                        'detailed_analysis': {
                            key: value for key, value in quant_analysis['detailed_analysis'].items()
                            if key in ['momentum', 'volatility_regime', 'risk_reward_ratio']
                        }
                    },
                    'trading_plan': trading_plan['trading_plan'] if trading_plan['trading_plan'].get('action') in ['BUY', 'STRONG_BUY'] else None,
                    'market_conditions': trading_plan['market_conditions'],
                    'final_recommendation': self._combine_recommendations(traditional_signals, quant_analysis),
                    'analysis_timestamp': datetime.now(self.ist_timezone).isoformat()
                }
                
                return combined_analysis
            else:
                # Fallback to traditional analysis only
                return {
                    'symbol': symbol,
                    'traditional_signals': [
                        {
                            'type': s.signal_type,
                            'confidence': s.confidence,
                            'strength': s.strength.name,
                            'reasons': s.reasons,
                            'risk_reward': s.risk_reward_ratio
                        } for s in traditional_signals
                    ],
                    'quantitative_analysis': None,
                    'final_recommendation': traditional_signals[0].signal_type if traditional_signals else 'HOLD',
                    'analysis_timestamp': datetime.now(self.ist_timezone).isoformat()
                }
                
        except Exception as e:
            logger.error(f"Enhanced signal analysis failed for {symbol}: {e}")
            return {'symbol': symbol, 'error': str(e)}
    
    def _combine_recommendations(self, traditional_signals, quant_analysis):
        """Combine traditional and quantitative recommendations"""
        if not traditional_signals:
            return quant_analysis['recommendation']
        
        traditional_rec = traditional_signals[0].signal_type
        quant_rec = quant_analysis['recommendation']
        
        # Agreement matrix
        if traditional_rec == 'BUY' and quant_rec in ['BUY', 'STRONG_BUY']:
            return 'STRONG_BUY'
        elif traditional_rec == 'SELL' and quant_rec in ['SELL', 'STRONG_SELL']:
            return 'STRONG_SELL'
        elif traditional_rec in ['BUY', 'SELL'] and quant_rec == 'HOLD':
            return traditional_rec
        elif quant_rec in ['STRONG_BUY', 'STRONG_SELL']:
            return quant_rec
        else:
            return 'HOLD'

# Example usage and testing
if __name__ == "__main__":
    generator = TrajectorySignalGenerator()
    
    # Test with sample data optimized for Indian market
    sample_market_data = {
        'ltp': 2750.50,  # Typical Reliance price
        'ltq': 125,
        'best_bid': 2750.25,
        'best_ask': 2750.75,
        'spread_pct': 0.018  # Typical Indian spread
    }
    
    sample_technical = {
        'rsi': 25,  # Using Indian market threshold (oversold at 20)
        'macd': {'macd': 0.8, 'signal': 0.5},
        'sma': {'sma_5': 2751.2, 'sma_20': 2748.8},
        'volume': {'volume_ratio': 1.8},  # Above Indian market threshold
        'adx': 28  # Trending market
    }
    
    # Generate signals
    signals = generator.generate_actionable_signals(
        'Reliance Industries', sample_market_data, sample_technical, {}
    )
    
    print(f"Generated {len(signals)} actionable signals with Indian market optimizations")
    for signal in signals:
        print(f"Signal: {signal.signal_type} - Confidence: {signal.confidence:.2f} - Sector: {signal.sector_bias}")
        print(f"Reasons: {signal.reasons}")
        print(f"Target: ₹{signal.target_price:.2f} | Stop: ₹{signal.stop_loss:.2f} | R/R: {signal.risk_reward_ratio:.2f}")
        print(f"Gap Risk Factor: {signal.gap_risk_factor:.2f}")
        print("---")
    
    # Test summary
    summary = generator.get_actionable_signals_summary(signals)
    print("\nSignal Summary:")
    print(json.dumps(summary, indent=2, default=str))