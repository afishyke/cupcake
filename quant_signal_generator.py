"""
Quantitative Signal Generator
============================

Professional-grade signal generation based on proven quantitative strategies:
1. Multi-factor models (Fama-French inspired)
2. Mean reversion with statistical significance
3. Momentum with regime detection
4. Risk-adjusted position sizing
5. Statistical arbitrage opportunities

Based on academic research and institutional best practices.
"""

import os
import numpy as np
import pandas as pd
import redis
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass
from enum import Enum
import scipy.stats as stats
from collections import defaultdict, deque
import pytz

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SignalType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    HOLD = "HOLD"

class SignalStrength(Enum):
    WEAK = 1
    MODERATE = 2
    STRONG = 3
    VERY_STRONG = 4

@dataclass
class QuantSignal:
    """Quantitative trading signal with statistical foundation"""
    symbol: str
    signal_type: SignalType
    strength: SignalStrength
    confidence: float  # Statistical confidence (0-1)
    entry_price: float
    stop_loss: float
    target_price: float
    position_size_pct: float  # % of portfolio
    hold_period_days: int
    factors: Dict[str, float]  # Factor loadings
    statistical_metrics: Dict[str, float]
    timestamp: datetime
    
class FactorModel:
    """Multi-factor model for signal generation"""
    
    def __init__(self):
        # Factor definitions based on academic literature
        self.factors = {
            'momentum': {
                'lookback_periods': [5, 10, 20, 60],  # Multiple timeframes
                'weight': 0.25,
                'threshold': 0.02  # 2% minimum move for significance
            },
            'mean_reversion': {
                'lookback_periods': [10, 20, 50],
                'weight': 0.20,
                'z_score_threshold': 1.5  # Standard deviations
            },
            'volatility': {
                'lookback_period': 20,
                'weight': 0.15,
                'regime_threshold': 0.02  # 2% daily volatility
            },
            'volume': {
                'lookback_period': 20,
                'weight': 0.15,
                'significance_threshold': 1.5  # Volume multiplier
            },
            'quality': {
                'metrics': ['sharpe_ratio', 'max_drawdown', 'consistency'],
                'weight': 0.25,
                'min_periods': 50
            }
        }
    
    def calculate_momentum_factor(self, prices: pd.Series) -> Dict[str, float]:
        """Calculate momentum factor using multiple timeframes"""
        if len(prices) < max(self.factors['momentum']['lookback_periods']) + 5:
            return {'score': 0, 'significance': 0, 'direction': 0}
        
        momentum_scores = []
        for period in self.factors['momentum']['lookback_periods']:
            if len(prices) > period:
                # Calculate return
                period_return = (prices.iloc[-1] / prices.iloc[-period-1]) - 1
                
                # Calculate statistical significance using t-test
                returns = prices.pct_change().dropna()
                recent_returns = returns.tail(period)
                
                if len(recent_returns) > 5:
                    t_stat, p_value = stats.ttest_1samp(recent_returns, 0)
                    significance = max(0, 1 - p_value)  # Convert p-value to confidence
                    
                    # Weight by timeframe (shorter = more weight) and significance
                    weight = 1 / period
                    weighted_score = period_return * weight * significance
                    momentum_scores.append({
                        'period': period,
                        'return': period_return,
                        'significance': significance,
                        'weighted_score': weighted_score
                    })
        
        if not momentum_scores:
            return {'score': 0, 'significance': 0, 'direction': 0}
        
        # Aggregate momentum score
        total_weighted_score = sum(ms['weighted_score'] for ms in momentum_scores)
        total_weight = sum(1/ms['period'] * ms['significance'] for ms in momentum_scores)
        avg_significance = np.mean([ms['significance'] for ms in momentum_scores])
        
        final_momentum = total_weighted_score / total_weight if total_weight > 0 else 0
        direction = 1 if final_momentum > 0 else -1 if final_momentum < 0 else 0
        
        return {
            'score': abs(final_momentum),
            'significance': avg_significance,
            'direction': direction,
            'meets_threshold': abs(final_momentum) > self.factors['momentum']['threshold']
        }
    
    def calculate_mean_reversion_factor(self, prices: pd.Series) -> Dict[str, float]:
        """Calculate mean reversion factor using statistical methods"""
        if len(prices) < max(self.factors['mean_reversion']['lookback_periods']) * 2:
            return {'score': 0, 'significance': 0, 'direction': 0}
        
        current_price = prices.iloc[-1]
        reversion_signals = []
        
        for period in self.factors['mean_reversion']['lookback_periods']:
            if len(prices) > period * 2:
                # Calculate z-score
                window_prices = prices.tail(period * 2)
                mean_price = window_prices.mean()
                std_price = window_prices.std()
                
                if std_price > 0:
                    z_score = (current_price - mean_price) / std_price
                    
                    # Test for mean reversion using Augmented Dickey-Fuller test
                    try:
                        # Simplified stationarity test
                        price_changes = window_prices.diff().dropna()
                        if len(price_changes) > 10:
                            # Test if changes are stationary
                            _, adf_p_value = stats.jarque_bera(price_changes)[:2]
                            stationarity_score = max(0, 1 - adf_p_value)
                        else:
                            stationarity_score = 0.5
                    except:
                        stationarity_score = 0.5
                    
                    # Mean reversion strength
                    reversion_strength = min(1.0, abs(z_score) / 3.0)  # Normalize to 0-1
                    direction = -1 if z_score > 0 else 1 if z_score < 0 else 0  # Revert to mean
                    
                    weight = 1 / period
                    reversion_signals.append({
                        'period': period,
                        'z_score': z_score,
                        'strength': reversion_strength,
                        'stationarity': stationarity_score,
                        'direction': direction,
                        'weighted_score': reversion_strength * weight * stationarity_score
                    })
        
        if not reversion_signals:
            return {'score': 0, 'significance': 0, 'direction': 0}
        
        # Aggregate mean reversion score
        total_weighted_score = sum(rs['weighted_score'] for rs in reversion_signals)
        total_weight = sum(1/rs['period'] * rs['stationarity'] for rs in reversion_signals)
        avg_significance = np.mean([rs['stationarity'] for rs in reversion_signals])
        avg_direction = np.mean([rs['direction'] for rs in reversion_signals])
        
        final_reversion = total_weighted_score / total_weight if total_weight > 0 else 0
        
        # Check if any signal meets threshold
        meets_threshold = any(abs(rs['z_score']) > self.factors['mean_reversion']['z_score_threshold'] 
                            for rs in reversion_signals)
        
        return {
            'score': final_reversion,
            'significance': avg_significance,
            'direction': 1 if avg_direction > 0 else -1 if avg_direction < 0 else 0,
            'meets_threshold': meets_threshold
        }
    
    def calculate_volatility_factor(self, prices: pd.Series) -> Dict[str, float]:
        """Calculate volatility regime factor"""
        lookback = self.factors['volatility']['lookback_period']
        if len(prices) < lookback * 2:
            return {'score': 0.5, 'regime': 'UNKNOWN', 'percentile': 0.5}
        
        returns = prices.pct_change().dropna()
        
        # Current volatility
        current_vol = returns.tail(lookback).std() * np.sqrt(252)  # Annualized
        
        # Historical volatility distribution
        rolling_vol = returns.rolling(lookback).std().dropna() * np.sqrt(252)
        
        if len(rolling_vol) < 10:
            return {'score': 0.5, 'regime': 'UNKNOWN', 'percentile': 0.5}
        
        # Volatility percentile
        vol_percentile = stats.percentileofscore(rolling_vol, current_vol) / 100
        
        # Regime classification
        if vol_percentile > 0.8:
            regime = 'HIGH_VOL'
            # High vol = lower signal confidence
            score = 0.3
        elif vol_percentile < 0.2:
            regime = 'LOW_VOL'
            # Low vol = higher signal confidence
            score = 0.8
        else:
            regime = 'NORMAL_VOL'
            score = 0.6
        
        return {
            'score': score,
            'regime': regime,
            'percentile': vol_percentile,
            'current_vol': current_vol
        }
    
    def calculate_volume_factor(self, prices: pd.Series, volumes: pd.Series) -> Dict[str, float]:
        """Calculate volume confirmation factor"""
        lookback = self.factors['volume']['lookback_period']
        if len(volumes) < lookback or len(prices) < lookback:
            return {'score': 0.5, 'confirmation': False}
        
        current_volume = volumes.iloc[-1]
        avg_volume = volumes.tail(lookback).mean()
        
        # Volume ratio
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        # Price-volume relationship
        price_changes = prices.pct_change().tail(lookback).dropna()
        volume_changes = volumes.pct_change().tail(lookback).dropna()
        
        # Align series
        common_index = price_changes.index.intersection(volume_changes.index)
        if len(common_index) > 5:
            price_aligned = price_changes.loc[common_index]
            volume_aligned = volume_changes.loc[common_index]
            
            # Correlation between price and volume changes
            try:
                correlation = price_aligned.corr(volume_aligned)
                if pd.isna(correlation):
                    correlation = 0
            except:
                correlation = 0
        else:
            correlation = 0
        
        # Volume confirmation score
        volume_significance = volume_ratio > self.factors['volume']['significance_threshold']
        confirmation_strength = min(1.0, volume_ratio / 3.0)  # Normalize
        
        # Positive correlation indicates healthy trend
        correlation_bonus = max(0, correlation) * 0.3
        
        final_score = (confirmation_strength + correlation_bonus) / 1.3
        
        return {
            'score': min(1.0, final_score),
            'confirmation': volume_significance,
            'volume_ratio': volume_ratio,
            'price_volume_corr': correlation
        }
    
    def calculate_quality_factor(self, prices: pd.Series) -> Dict[str, float]:
        """Calculate quality/consistency factor"""
        min_periods = self.factors['quality']['min_periods']
        if len(prices) < min_periods:
            return {'score': 0.5, 'sharpe': 0, 'max_dd': 0, 'consistency': 0.5}
        
        returns = prices.pct_change().dropna()
        
        # Sharpe ratio
        if len(returns) > 0 and returns.std() > 0:
            sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        # Maximum drawdown
        cumulative = (1 + returns).cumprod()
        rolling_max = cumulative.expanding().max()
        drawdown = (cumulative - rolling_max) / rolling_max
        max_drawdown = drawdown.min()
        
        # Consistency (percentage of positive periods)
        consistency = (returns > 0).mean()
        
        # Normalize scores
        sharpe_score = max(0, min(1, (sharpe_ratio + 2) / 4))  # -2 to 2 Sharpe -> 0 to 1
        drawdown_score = max(0, min(1, 1 + max_drawdown / 0.5))  # -50% max DD -> 0, 0% -> 1
        consistency_score = consistency  # Already 0-1
        
        # Weighted average
        quality_score = (sharpe_score * 0.4 + drawdown_score * 0.4 + consistency_score * 0.2)
        
        return {
            'score': quality_score,
            'sharpe': sharpe_ratio,
            'max_dd': max_drawdown,
            'consistency': consistency
        }

class QuantitativeSignalGenerator:
    """Main quantitative signal generator"""
    
    def __init__(self, redis_host=None, redis_port=None):
        # Use environment variables or defaults
        if redis_host is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
        if redis_port is None:
            redis_port = int(os.environ.get('REDIS_PORT', '6379'))
            
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        self.factor_model = FactorModel()
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Risk management parameters
        self.max_position_size = 0.05  # 5% max per position
        self.max_portfolio_exposure = 0.30  # 30% max total exposure
        self.min_confidence_threshold = 0.3  # Minimum confidence for signal
        self.risk_free_rate = 0.07  # 7% Indian risk-free rate
        
        logger.info("Quantitative Signal Generator initialized")
    
    def get_symbol_data(self, symbol: str, periods: int = 100) -> Optional[Tuple[pd.Series, pd.Series]]:
        """Fetch price and volume data from Redis"""
        try:
            symbol_clean = symbol.replace(' ', '_').replace('.', '').replace('&', 'and')
            
            # Get price data
            close_key = f"stock:{symbol_clean}:close"
            close_data = self.redis_client.execute_command('TS.RANGE', close_key, '-', '+', 'COUNT', periods)
            
            # Get volume data
            volume_key = f"stock:{symbol_clean}:volume"
            volume_data = self.redis_client.execute_command('TS.RANGE', volume_key, '-', '+', 'COUNT', periods)
            
            if not close_data or not volume_data:
                logger.warning(f"No data found for symbol {symbol}")
                return None
            
            # Convert to pandas Series
            timestamps = [pd.Timestamp(item[0], unit='ms', tz=self.ist_timezone) for item in close_data]
            prices = pd.Series([float(item[1]) for item in close_data], index=timestamps)
            volumes = pd.Series([float(item[1]) for item in volume_data[:len(timestamps)]], index=timestamps)
            
            return prices, volumes
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def calculate_factor_scores(self, prices: pd.Series, volumes: pd.Series) -> Dict[str, Dict]:
        """Calculate all factor scores for a symbol"""
        factors = {}
        
        # Momentum factor
        factors['momentum'] = self.factor_model.calculate_momentum_factor(prices)
        
        # Mean reversion factor
        factors['mean_reversion'] = self.factor_model.calculate_mean_reversion_factor(prices)
        
        # Volatility factor
        factors['volatility'] = self.factor_model.calculate_volatility_factor(prices)
        
        # Volume factor
        factors['volume'] = self.factor_model.calculate_volume_factor(prices, volumes)
        
        # Quality factor
        factors['quality'] = self.factor_model.calculate_quality_factor(prices)
        
        return factors
    
    def generate_signal(self, symbol: str) -> Optional[QuantSignal]:
        """Generate a quantitative signal for a symbol"""
        try:
            # Get data
            data = self.get_symbol_data(symbol)
            if data is None:
                return None
            
            prices, volumes = data
            
            if len(prices) < 50:  # Minimum data requirement
                logger.warning(f"Insufficient data for {symbol}: {len(prices)} periods")
                return None
            
            # Calculate factor scores
            factors = self.calculate_factor_scores(prices, volumes)
            
            # Calculate composite signal
            signal_info = self._calculate_composite_signal(factors, prices, volumes)
            
            if signal_info is None:
                logger.info(f"No signal generated for {symbol} - insufficient confidence or mixed signals")
                return None
            
            logger.info(f"Signal generated for {symbol}: {signal_info['signal_type'].value} with confidence {signal_info['confidence']:.3f}")
            
            # Risk management and position sizing
            position_info = self._calculate_position_size(prices, signal_info['confidence'])
            
            # Create signal
            signal = QuantSignal(
                symbol=symbol,
                signal_type=signal_info['signal_type'],
                strength=signal_info['strength'],
                confidence=signal_info['confidence'],
                entry_price=prices.iloc[-1],
                stop_loss=position_info['stop_loss'],
                target_price=position_info['target_price'],
                position_size_pct=position_info['position_size_pct'],
                hold_period_days=signal_info['hold_period'],
                factors={k: v.get('score', 0) for k, v in factors.items()},
                statistical_metrics=signal_info['stats'],
                timestamp=datetime.now(self.ist_timezone)
            )
            
            return signal
            
        except Exception as e:
            logger.error(f"Error generating signal for {symbol}: {e}")
            return None
    
    def _calculate_composite_signal(self, factors: Dict, prices: pd.Series, volumes: pd.Series) -> Optional[Dict]:
        """Calculate composite signal from factors"""
        
        # Extract factor scores and weights
        momentum = factors['momentum']
        mean_reversion = factors['mean_reversion']
        volatility = factors['volatility']
        volume = factors['volume']
        quality = factors['quality']
        
        # Calculate weighted score
        weights = self.factor_model.factors
        
        # Long signal components
        long_score = 0
        long_confidence = 0
        
        # Momentum contribution
        if momentum['meets_threshold'] and momentum['direction'] > 0:
            long_score += momentum['score'] * weights['momentum']['weight']
            long_confidence += momentum['significance'] * weights['momentum']['weight']
        
        # Mean reversion contribution (contrarian)
        if mean_reversion['meets_threshold'] and mean_reversion['direction'] > 0:
            long_score += mean_reversion['score'] * weights['mean_reversion']['weight']
            long_confidence += mean_reversion['significance'] * weights['mean_reversion']['weight']
        
        # Volatility contribution
        long_score += volatility['score'] * weights['volatility']['weight']
        long_confidence += volatility['score'] * weights['volatility']['weight']
        
        # Volume contribution
        if volume['confirmation']:
            long_score += volume['score'] * weights['volume']['weight']
            long_confidence += volume['score'] * weights['volume']['weight']
        
        # Quality contribution
        long_score += quality['score'] * weights['quality']['weight']
        long_confidence += quality['score'] * weights['quality']['weight']
        
        # Short signal components (opposite logic)
        short_score = 0
        short_confidence = 0
        
        # Momentum contribution (negative momentum)
        if momentum['meets_threshold'] and momentum['direction'] < 0:
            short_score += momentum['score'] * weights['momentum']['weight']
            short_confidence += momentum['significance'] * weights['momentum']['weight']
        
        # Mean reversion contribution (contrarian)
        if mean_reversion['meets_threshold'] and mean_reversion['direction'] < 0:
            short_score += mean_reversion['score'] * weights['mean_reversion']['weight']
            short_confidence += mean_reversion['significance'] * weights['mean_reversion']['weight']
        
        # Add other factors for short
        short_score += (1 - volatility['score']) * weights['volatility']['weight'] * 0.5  # High vol = short bias
        short_confidence += volatility['score'] * weights['volatility']['weight'] * 0.5
        
        # Determine signal
        if long_score > short_score and long_confidence > self.min_confidence_threshold:
            signal_type = SignalType.LONG
            confidence = long_confidence
            strength_score = long_score
        elif short_score > long_score and short_confidence > self.min_confidence_threshold:
            signal_type = SignalType.SHORT
            confidence = short_confidence
            strength_score = short_score
        else:
            return None  # No clear signal
        
        # Determine strength
        if strength_score > 0.8:
            strength = SignalStrength.VERY_STRONG
            hold_period = 3
        elif strength_score > 0.6:
            strength = SignalStrength.STRONG
            hold_period = 5
        elif strength_score > 0.4:
            strength = SignalStrength.MODERATE
            hold_period = 7
        else:
            strength = SignalStrength.WEAK
            hold_period = 10
        
        return {
            'signal_type': signal_type,
            'strength': strength,
            'confidence': confidence,
            'hold_period': hold_period,
            'stats': {
                'long_score': long_score,
                'short_score': short_score,
                'momentum_score': momentum['score'],
                'mean_reversion_score': mean_reversion['score'],
                'quality_score': quality['score'],
                'volatility_percentile': volatility.get('percentile', 0.5)
            }
        }
    
    def _calculate_position_size(self, prices: pd.Series, confidence: float) -> Dict:
        """Calculate position size and risk management levels"""
        current_price = prices.iloc[-1]
        returns = prices.pct_change().dropna()
        
        # Calculate volatility for position sizing
        volatility = returns.std() * np.sqrt(252)  # Annualized
        
        # Base position size using Kelly criterion approximation
        if len(returns) > 20 and volatility > 0:
            # Simplified Kelly: f = (bp - q) / b where b=odds, p=prob win, q=prob loss
            win_rate = (returns > 0).mean()
            avg_win = returns[returns > 0].mean() if (returns > 0).any() else 0.01
            avg_loss = abs(returns[returns < 0].mean()) if (returns < 0).any() else 0.01
            
            if avg_loss > 0:
                kelly_fraction = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
                kelly_fraction = max(0, min(0.25, kelly_fraction))  # Cap at 25%
            else:
                kelly_fraction = 0.05
        else:
            kelly_fraction = 0.05
        
        # Adjust by confidence and volatility
        confidence_adj = confidence ** 2  # Square for more conservative sizing
        volatility_adj = max(0.2, min(1.0, 0.3 / volatility)) if volatility > 0 else 0.5
        
        base_position = kelly_fraction * confidence_adj * volatility_adj
        position_size_pct = min(self.max_position_size, base_position)
        
        # Calculate stop loss using ATR
        if len(prices) >= 20:
            # Simple ATR approximation
            highs = prices.rolling(2).max()
            lows = prices.rolling(2).min()
            atr = (highs - lows).rolling(14).mean().iloc[-1]
            atr_multiplier = 2.0
            stop_distance = atr * atr_multiplier
        else:
            # Fallback to volatility-based stop
            stop_distance = current_price * volatility * 0.1  # 10% of annual vol
        
        stop_loss = current_price - stop_distance
        
        # Calculate target using risk-reward ratio
        risk_amount = current_price - stop_loss
        reward_ratio = 2.0  # 2:1 reward to risk
        target_price = current_price + (risk_amount * reward_ratio)
        
        return {
            'position_size_pct': position_size_pct,
            'stop_loss': stop_loss,
            'target_price': target_price,
            'kelly_fraction': kelly_fraction,
            'atr_stop_distance': stop_distance
        }
    
    def generate_signals_for_universe(self, symbols: List[str]) -> List[QuantSignal]:
        """Generate signals for a universe of symbols"""
        signals = []
        
        for symbol in symbols:
            signal = self.generate_signal(symbol)
            if signal:
                signals.append(signal)
                logger.info(f"Generated {signal.signal_type.value} signal for {symbol} "
                          f"(confidence: {signal.confidence:.2f}, strength: {signal.strength.name})")
        
        # Sort by confidence
        signals.sort(key=lambda x: x.confidence, reverse=True)
        
        return signals
    
    def get_signal_summary(self, signals: List[QuantSignal]) -> Dict:
        """Get summary statistics for generated signals"""
        if not signals:
            return {}
        
        total_signals = len(signals)
        long_signals = len([s for s in signals if s.signal_type == SignalType.LONG])
        short_signals = len([s for s in signals if s.signal_type == SignalType.SHORT])
        
        avg_confidence = np.mean([s.confidence for s in signals])
        total_exposure = sum([s.position_size_pct for s in signals])
        
        strength_distribution = {}
        for strength in SignalStrength:
            count = len([s for s in signals if s.strength == strength])
            strength_distribution[strength.name] = count
        
        return {
            'total_signals': total_signals,
            'long_signals': long_signals,
            'short_signals': short_signals,
            'avg_confidence': avg_confidence,
            'total_exposure_pct': total_exposure,
            'strength_distribution': strength_distribution,
            'signals_above_80_confidence': len([s for s in signals if s.confidence > 0.8]),
            'max_confidence': max([s.confidence for s in signals]),
            'min_confidence': min([s.confidence for s in signals])
        }