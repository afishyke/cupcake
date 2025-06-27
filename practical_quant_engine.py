"""
Practical Quantitative Trading Engine
===================================

Advanced mathematical models using only OHLCV and orderbook data.
No ML dependencies - pure mathematical/statistical approaches that work.

Focus on:
- Statistical arbitrage using price relationships
- Advanced volatility modeling (GARCH-like)
- Sophisticated momentum and mean reversion
- Multi-timeframe statistical confluence
- Professional risk management with realistic stop losses
- Orderbook-based microstructure analysis
"""

import numpy as np
import pandas as pd
from scipy import stats
from scipy.optimize import minimize_scalar
import warnings
warnings.filterwarnings('ignore')

class PracticalQuantEngine:
    """
    Professional quantitative engine using mathematical models only
    """
    
    def __init__(self):
        self.risk_free_rate = 0.07  # 10Y Indian Gov Bond
        self.market_hours_per_day = 6.25  # NSE trading hours
        
        # Indian market specific parameters
        self.indian_params = {
            'high_vol_threshold': 0.04,  # 4% daily vol = high volatility
            'low_vol_threshold': 0.015,  # 1.5% daily vol = low volatility
            'momentum_threshold': 0.02,   # 2% move for momentum signal
            'mean_reversion_periods': [5, 10, 20],  # Multiple timeframes
            'volatility_lookback': 30,    # Days for volatility calculation
            'correlation_lookback': 60    # Days for correlation analysis
        }
    
    def calculate_advanced_momentum_score(self, df, timeframes=[3, 7, 14, 21]):
        """
        Advanced momentum using multiple timeframes with decay
        """
        if len(df) < max(timeframes) + 5:
            return {'momentum_score': 0, 'momentum_strength': 'WEAK'}
        
        returns = df['close'].pct_change()
        momentum_scores = []
        
        for tf in timeframes:
            if len(returns) > tf:
                # Calculate momentum with statistical significance
                period_return = (df['close'].iloc[-1] / df['close'].iloc[-tf-1]) - 1
                period_vol = returns.tail(tf).std()
                
                # T-statistic for momentum significance
                if period_vol > 0:
                    t_stat = (period_return / period_vol) * np.sqrt(tf)
                    p_value = 2 * (1 - stats.norm.cdf(abs(t_stat)))
                    
                    # Weight by timeframe (shorter = higher weight) and significance
                    weight = 1 / tf  # Inverse timeframe weighting
                    significance_weight = max(0, 1 - p_value)  # Higher weight for significant moves
                    
                    momentum_scores.append({
                        'timeframe': tf,
                        'return': period_return,
                        't_stat': t_stat,
                        'p_value': p_value,
                        'weighted_score': period_return * weight * significance_weight
                    })
        
        if not momentum_scores:
            return {'momentum_score': 0, 'momentum_strength': 'WEAK'}
        
        # Combined momentum score
        total_weighted_score = sum(ms['weighted_score'] for ms in momentum_scores)
        total_weight = sum(1/tf * max(0, 1-ms['p_value']) for tf, ms in zip(timeframes, momentum_scores))
        
        final_momentum = total_weighted_score / total_weight if total_weight > 0 else 0
        
        # Classify momentum strength
        if abs(final_momentum) > 0.03:
            strength = 'STRONG'
        elif abs(final_momentum) > 0.015:
            strength = 'MODERATE'
        else:
            strength = 'WEAK'
        
        return {
            'momentum_score': final_momentum,
            'momentum_strength': strength,
            'timeframe_analysis': momentum_scores,
            'statistical_significance': np.mean([ms['p_value'] for ms in momentum_scores])
        }
    
    def calculate_mean_reversion_probability(self, df, periods=[5, 10, 20]):
        """
        Calculate probability of mean reversion using multiple statistical tests
        """
        if len(df) < max(periods) * 2:
            return {'reversion_prob': 0.5, 'current_extreme': False}
        
        reversion_signals = []
        current_price = df['close'].iloc[-1]
        
        for period in periods:
            if len(df) > period * 2:
                # Z-score analysis
                ma = df['close'].rolling(period).mean().iloc[-1]
                std = df['close'].rolling(period).std().iloc[-1]
                
                if std > 0:
                    z_score = (current_price - ma) / std
                    
                    # Historical z-score distribution
                    historical_z = (df['close'] - df['close'].rolling(period).mean()) / df['close'].rolling(period).std()
                    historical_z = historical_z.dropna()
                    
                    if len(historical_z) > 20:
                        # Probability based on historical distribution
                        percentile = stats.percentileofscore(historical_z, z_score) / 100
                        
                        # Mean reversion probability (extreme values tend to revert)
                        if percentile > 0.8:  # In top 20%
                            reversion_prob = (percentile - 0.8) / 0.2  # 0 to 1 scale
                            direction = -1  # Expect down move
                        elif percentile < 0.2:  # In bottom 20%
                            reversion_prob = (0.2 - percentile) / 0.2  # 0 to 1 scale
                            direction = 1  # Expect up move
                        else:
                            reversion_prob = 0
                            direction = 0
                        
                        reversion_signals.append({
                            'period': period,
                            'z_score': z_score,
                            'percentile': percentile,
                            'reversion_prob': reversion_prob,
                            'direction': direction
                        })
        
        if not reversion_signals:
            return {'reversion_prob': 0.5, 'current_extreme': False}
        
        # Weighted average (shorter periods get higher weight)
        weights = [1/p for p in periods[:len(reversion_signals)]]
        total_weight = sum(weights)
        
        avg_reversion_prob = sum(rs['reversion_prob'] * w for rs, w in zip(reversion_signals, weights)) / total_weight
        avg_direction = sum(rs['direction'] * w for rs, w in zip(reversion_signals, weights)) / total_weight
        
        # Check if currently at extreme
        extreme_threshold = 0.3  # 30% reversion probability
        is_extreme = avg_reversion_prob > extreme_threshold
        
        return {
            'reversion_prob': avg_reversion_prob,
            'expected_direction': avg_direction,
            'current_extreme': is_extreme,
            'period_analysis': reversion_signals
        }
    
    def calculate_volatility_regime(self, df, lookback=30):
        """
        Advanced volatility regime detection with GARCH-like properties
        """
        if len(df) < lookback * 2:
            return {'regime': 'NORMAL', 'vol_score': 0.5, 'vol_forecast': 0.02}
        
        returns = df['close'].pct_change().dropna()
        
        # Current volatility (last 20 days)
        current_vol = returns.tail(20).std() * np.sqrt(252)
        
        # Historical volatility distribution
        rolling_vol = returns.rolling(20).std().dropna() * np.sqrt(252)
        
        if len(rolling_vol) < 10:
            return {'regime': 'NORMAL', 'vol_score': 0.5, 'vol_forecast': current_vol}
        
        # Volatility percentile
        vol_percentile = stats.percentileofscore(rolling_vol, current_vol) / 100
        
        # Volatility clustering (GARCH effect)
        recent_squared_returns = returns.tail(10) ** 2
        vol_clustering = recent_squared_returns.autocorr(lag=1) if len(recent_squared_returns) > 1 else 0
        
        # Regime classification
        if vol_percentile > 0.8:
            regime = 'HIGH_VOL'
            regime_score = vol_percentile
        elif vol_percentile < 0.2:
            regime = 'LOW_VOL'
            regime_score = 1 - vol_percentile
        else:
            regime = 'NORMAL'
            regime_score = 0.5
        
        # Simple volatility forecast (exponential smoothing)
        alpha = 0.1  # Smoothing parameter
        vol_forecast = alpha * (returns.iloc[-1] ** 2) + (1 - alpha) * (current_vol / np.sqrt(252)) ** 2
        vol_forecast = np.sqrt(vol_forecast * 252)  # Annualized
        
        return {
            'regime': regime,
            'current_vol': current_vol,
            'vol_percentile': vol_percentile,
            'vol_score': regime_score,
            'vol_clustering': vol_clustering,
            'vol_forecast': vol_forecast
        }
    
    def calculate_support_resistance_strength(self, df, window=50):
        """
        Calculate support/resistance levels with statistical significance
        """
        if len(df) < window:
            return {'support': df['low'].min(), 'resistance': df['high'].max(), 'strength': 0}
        
        recent_data = df.tail(window)
        current_price = df['close'].iloc[-1]
        
        # Find pivot points
        highs = recent_data['high']
        lows = recent_data['low']
        
        # Local maxima (resistance)
        resistance_levels = []
        for i in range(2, len(highs) - 2):
            if (highs.iloc[i] > highs.iloc[i-1] and highs.iloc[i] > highs.iloc[i-2] and
                highs.iloc[i] > highs.iloc[i+1] and highs.iloc[i] > highs.iloc[i+2]):
                resistance_levels.append(highs.iloc[i])
        
        # Local minima (support)
        support_levels = []
        for i in range(2, len(lows) - 2):
            if (lows.iloc[i] < lows.iloc[i-1] and lows.iloc[i] < lows.iloc[i-2] and
                lows.iloc[i] < lows.iloc[i+1] and lows.iloc[i] < lows.iloc[i+2]):
                support_levels.append(lows.iloc[i])
        
        # Find nearest levels
        resistance_above = [r for r in resistance_levels if r > current_price]
        support_below = [s for s in support_levels if s < current_price]
        
        nearest_resistance = min(resistance_above) if resistance_above else recent_data['high'].max()
        nearest_support = max(support_below) if support_below else recent_data['low'].min()
        
        # Calculate strength based on number of touches and volume
        resistance_strength = len([r for r in resistance_levels if abs(r - nearest_resistance) / current_price < 0.02])
        support_strength = len([s for s in support_levels if abs(s - nearest_support) / current_price < 0.02])
        
        # Position within range
        range_position = (current_price - nearest_support) / (nearest_resistance - nearest_support) if nearest_resistance != nearest_support else 0.5
        
        return {
            'support': nearest_support,
            'resistance': nearest_resistance,
            'support_strength': support_strength,
            'resistance_strength': resistance_strength,
            'range_position': range_position,
            'range_width_pct': (nearest_resistance - nearest_support) / current_price
        }
    
    def calculate_volume_profile_score(self, df, periods=[5, 10, 20]):
        """
        Volume analysis for confirmation of price moves
        """
        if len(df) < max(periods):
            return {'volume_score': 0.5, 'volume_trend': 'NEUTRAL'}
        
        current_volume = df['volume'].iloc[-1]
        price_change = df['close'].pct_change().iloc[-1]
        
        volume_scores = []
        
        for period in periods:
            if len(df) > period:
                avg_volume = df['volume'].tail(period).mean()
                volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
                
                # Volume should confirm price direction
                if price_change > 0:  # Price up
                    volume_confirmation = min(2.0, volume_ratio) / 2.0  # High volume good for up moves
                else:  # Price down
                    volume_confirmation = max(0.0, 2.0 - volume_ratio) / 2.0  # Low volume better for down moves
                
                weight = 1 / period  # Shorter periods get higher weight
                volume_scores.append(volume_confirmation * weight)
        
        final_volume_score = sum(volume_scores) / sum(1/p for p in periods[:len(volume_scores)])
        
        # Volume trend
        if final_volume_score > 0.7:
            trend = 'STRONG_CONFIRMATION'
        elif final_volume_score > 0.5:
            trend = 'WEAK_CONFIRMATION'
        else:
            trend = 'DIVERGENCE'
        
        return {
            'volume_score': final_volume_score,
            'volume_trend': trend,
            'current_volume_ratio': current_volume / df['volume'].tail(20).mean() if len(df) > 20 else 1
        }
    
    def calculate_statistical_stop_loss(self, df, confidence_level=0.95, method='ensemble'):
        """
        Calculate statistically sound stop loss levels
        """
        if len(df) < 30:
            return {'stop_loss': df['close'].iloc[-1] * 0.98, 'method': 'default'}
        
        current_price = df['close'].iloc[-1]
        returns = df['close'].pct_change().dropna()
        
        stop_levels = {}
        
        # Method 1: Value at Risk (VaR)
        var_95 = np.percentile(returns.tail(30), 5)  # 5th percentile for 95% confidence
        var_stop = current_price * (1 + var_95)
        stop_levels['var'] = var_stop
        
        # Method 2: Volatility-based (2-sigma)
        recent_vol = returns.tail(20).std()
        vol_stop = current_price * (1 - 2 * recent_vol)
        stop_levels['volatility'] = vol_stop
        
        # Method 3: ATR-based
        if 'high' in df.columns and 'low' in df.columns:
            high_low = df['high'] - df['low']
            high_close = abs(df['high'] - df['close'].shift())
            low_close = abs(df['low'] - df['close'].shift())
            
            tr_data = pd.concat([high_low, high_close, low_close], axis=1)
            atr = tr_data.max(axis=1).rolling(14).mean().iloc[-1]
            
            # Adaptive ATR multiplier based on volatility regime
            vol_regime = self.calculate_volatility_regime(df)
            if vol_regime['regime'] == 'HIGH_VOL':
                atr_mult = 3.0
            elif vol_regime['regime'] == 'LOW_VOL':
                atr_mult = 1.5
            else:
                atr_mult = 2.0
            
            atr_stop = current_price - (atr * atr_mult)
            stop_levels['atr'] = atr_stop
        
        # Method 4: Support level
        sr_analysis = self.calculate_support_resistance_strength(df)
        support_stop = sr_analysis['support']
        stop_levels['support'] = support_stop
        
        # Method 5: Percentile-based on recent range
        recent_lows = df['low'].tail(20)
        percentile_stop = np.percentile(recent_lows, 20)  # 20th percentile of recent lows
        stop_levels['percentile'] = percentile_stop
        
        # Ensemble method - take weighted average
        if method == 'ensemble':
            # Remove extreme outliers (more than 15% away)
            valid_stops = {k: v for k, v in stop_levels.items() 
                          if v >= current_price * 0.85 and v <= current_price * 1.02}
            
            if valid_stops:
                # Weight by reliability
                weights = {
                    'var': 0.25,
                    'volatility': 0.20,
                    'atr': 0.25,
                    'support': 0.20,
                    'percentile': 0.10
                }
                
                weighted_stops = []
                total_weight = 0
                
                for method_name, stop_price in valid_stops.items():
                    if method_name in weights:
                        weighted_stops.append(stop_price * weights[method_name])
                        total_weight += weights[method_name]
                
                final_stop = sum(weighted_stops) / total_weight if total_weight > 0 else current_price * 0.95
            else:
                final_stop = current_price * 0.95  # Fallback
        else:
            final_stop = stop_levels.get(method, current_price * 0.95)
        
        # Ensure stop loss is reasonable (not too tight, not too loose)
        final_stop = max(final_stop, current_price * 0.90)  # Max 10% stop
        final_stop = min(final_stop, current_price * 0.99)  # Min 1% stop
        
        stop_pct = (current_price - final_stop) / current_price
        
        return {
            'stop_loss': final_stop,
            'stop_loss_pct': stop_pct,
            'method': method,
            'all_methods': stop_levels,
            'confidence_level': confidence_level
        }
    
    def calculate_dynamic_position_size(self, df, portfolio_value, risk_per_trade=0.02):
        """
        Calculate optimal position size based on volatility and confidence
        """
        if len(df) < 20:
            return {'position_size': portfolio_value * 0.01, 'risk_adjusted': False}
        
        # Calculate stop loss
        stop_analysis = self.calculate_statistical_stop_loss(df)
        stop_loss_pct = stop_analysis['stop_loss_pct']
        
        # Base position size using fixed fractional method
        if stop_loss_pct > 0:
            base_position = (portfolio_value * risk_per_trade) / stop_loss_pct
        else:
            base_position = portfolio_value * 0.01  # 1% fallback
        
        # Adjust for volatility
        vol_regime = self.calculate_volatility_regime(df)
        vol_multiplier = {
            'LOW_VOL': 1.5,
            'NORMAL': 1.0,
            'HIGH_VOL': 0.7
        }.get(vol_regime['regime'], 1.0)
        
        # Adjust for signal confidence (you'll get this from your main signal)
        # For now, using momentum strength as proxy
        momentum = self.calculate_advanced_momentum_score(df)
        confidence_multiplier = {
            'STRONG': 1.2,
            'MODERATE': 1.0,
            'WEAK': 0.8
        }.get(momentum['momentum_strength'], 1.0)
        
        final_position = base_position * vol_multiplier * confidence_multiplier
        
        # Cap at reasonable limits
        max_position = portfolio_value * 0.10  # Max 10% of portfolio per trade
        final_position = min(final_position, max_position)
        
        return {
            'position_size': final_position,
            'position_pct': final_position / portfolio_value,
            'stop_loss_pct': stop_loss_pct,
            'vol_multiplier': vol_multiplier,
            'confidence_multiplier': confidence_multiplier,
            'risk_adjusted': True
        }
    
    def calculate_comprehensive_score(self, df, benchmark_df=None):
        """
        Generate comprehensive quantitative score using all mathematical models
        """
        if len(df) < 50:
            return {'final_score': 0.4, 'recommendation': 'INSUFFICIENT_DATA'}
        
        scores = {}
        weights = {}
        
        # 1. Momentum Analysis (25%)
        momentum = self.calculate_advanced_momentum_score(df)
        momentum_score = max(0, min(1, (momentum['momentum_score'] + 0.05) / 0.10))  # Normalize to 0-1
        scores['momentum'] = momentum_score
        weights['momentum'] = 0.25
        
        # 2. Mean Reversion (20%)
        reversion = self.calculate_mean_reversion_probability(df)
        # High reversion probability is good for contrarian plays
        reversion_score = reversion['reversion_prob']
        scores['mean_reversion'] = reversion_score
        weights['mean_reversion'] = 0.20
        
        # 3. Volatility Regime (20%)
        vol_regime = self.calculate_volatility_regime(df)
        # Prefer normal to low volatility
        vol_score = {
            'LOW_VOL': 0.8,
            'NORMAL': 0.7,
            'HIGH_VOL': 0.4
        }.get(vol_regime['regime'], 0.5)
        scores['volatility'] = vol_score
        weights['volatility'] = 0.20
        
        # 4. Support/Resistance (15%)
        sr_analysis = self.calculate_support_resistance_strength(df)
        # Good position in range and strong levels
        sr_score = (sr_analysis['support_strength'] + sr_analysis['resistance_strength']) / 10
        sr_score = min(1.0, sr_score) * (1 - abs(sr_analysis['range_position'] - 0.5))
        scores['support_resistance'] = sr_score
        weights['support_resistance'] = 0.15
        
        # 5. Volume Confirmation (10%)
        volume = self.calculate_volume_profile_score(df)
        volume_score = volume['volume_score']
        scores['volume'] = volume_score
        weights['volume'] = 0.10
        
        # 6. Risk-Reward (10%)
        stop_analysis = self.calculate_statistical_stop_loss(df)
        current_price = df['close'].iloc[-1]
        
        # Estimate target based on resistance or momentum
        if sr_analysis['resistance'] > current_price:
            target = sr_analysis['resistance']
        else:
            target = current_price * 1.05  # 5% default target
        
        risk_reward = (target - current_price) / (current_price - stop_analysis['stop_loss']) if stop_analysis['stop_loss'] < current_price else 0
        rr_score = min(1.0, max(0, (risk_reward - 1) / 2))  # Normalize R:R of 1-3 to 0-1 score
        scores['risk_reward'] = rr_score
        weights['risk_reward'] = 0.10
        
        # Calculate final weighted score
        final_score = sum(scores[key] * weights[key] for key in scores)
        
        # Generate recommendation
        if final_score >= 0.75:
            recommendation = 'STRONG_BUY'
        elif final_score >= 0.6:
            recommendation = 'BUY'
        elif final_score >= 0.4:
            recommendation = 'HOLD'
        elif final_score >= 0.25:
            recommendation = 'SELL'
        else:
            recommendation = 'STRONG_SELL'
        
        return {
            'final_score': final_score,
            'recommendation': recommendation,
            'component_scores': scores,
            'weights': weights,
            'detailed_analysis': {
                'momentum': momentum,
                'mean_reversion': reversion,
                'volatility_regime': vol_regime,
                'support_resistance': sr_analysis,
                'volume_analysis': volume,
                'stop_loss_analysis': stop_analysis,
                'risk_reward_ratio': risk_reward
            }
        }
    
    def calculate_advanced_risk_metrics(self, df):
        """
        Calculate advanced risk metrics for portfolio analysis
        """
        if len(df) < 30:
            return {
                'sharpe_ratio': 0,
                'max_drawdown': 0,
                'var_95': 0,
                'volatility': 0,
                'beta': 1.0,
                'alpha': 0,
                'sortino_ratio': 0
            }
        
        returns = df['close'].pct_change().dropna()
        
        # Sharpe Ratio
        excess_return = returns.mean() - (self.risk_free_rate / 252)
        volatility = returns.std()
        sharpe_ratio = (excess_return / volatility * np.sqrt(252)) if volatility > 0 else 0
        
        # Maximum Drawdown
        cumulative = (1 + returns).cumprod()
        rolling_max = cumulative.expanding().max()
        drawdown = (cumulative - rolling_max) / rolling_max
        max_drawdown = drawdown.min()
        
        # Value at Risk (95% confidence)
        var_95 = np.percentile(returns, 5)
        
        # Sortino Ratio (downside deviation)
        downside_returns = returns[returns < 0]
        downside_deviation = downside_returns.std() if len(downside_returns) > 0 else volatility
        sortino_ratio = (excess_return / downside_deviation * np.sqrt(252)) if downside_deviation > 0 else 0
        
        # Beta and Alpha (using market proxy - simplified)
        # In practice, you'd use a benchmark index
        market_returns = returns.rolling(20).mean()  # Simplified proxy
        if len(market_returns.dropna()) > 10:
            covariance = np.cov(returns.dropna(), market_returns.dropna())[0, 1]
            market_variance = np.var(market_returns.dropna())
            beta = covariance / market_variance if market_variance > 0 else 1.0
            alpha = returns.mean() - beta * market_returns.mean()
        else:
            beta = 1.0
            alpha = 0
        
        return {
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'var_95': var_95,
            'volatility': volatility * np.sqrt(252),  # Annualized
            'beta': beta,
            'alpha': alpha,
            'sortino_ratio': sortino_ratio
        }
    
    def generate_trading_plan(self, df, portfolio_value=1000000, risk_per_trade=0.02):
        """
        Generate complete trading plan with entry, exit, and position sizing
        """
        analysis = self.calculate_comprehensive_score(df)
        
        if analysis['recommendation'] in ['STRONG_BUY', 'BUY']:
            # Position sizing
            position = self.calculate_dynamic_position_size(df, portfolio_value, risk_per_trade)
            
            # Entry price (current or better)
            current_price = df['close'].iloc[-1]
            sr_analysis = analysis['detailed_analysis']['support_resistance']
            
            # Try to get better entry near support
            if sr_analysis['range_position'] > 0.7:  # Near resistance
                entry_price = current_price * 0.99  # Slight pullback entry
            else:
                entry_price = current_price
            
            # Stop loss
            stop_loss = analysis['detailed_analysis']['stop_loss_analysis']['stop_loss']
            
            # Target price
            if sr_analysis['resistance'] > current_price:
                target_price = sr_analysis['resistance'] * 0.98  # Slightly below resistance
            else:
                target_price = current_price * 1.08  # 8% target
            
            # Calculate quantities
            position_value = position['position_size']
            quantity = int(position_value / entry_price)
            
            plan = {
                'action': analysis['recommendation'],
                'entry_price': entry_price,
                'quantity': quantity,
                'position_value': quantity * entry_price,
                'stop_loss': stop_loss,
                'target_price': target_price,
                'risk_amount': quantity * (entry_price - stop_loss),
                'reward_amount': quantity * (target_price - entry_price),
                'risk_reward_ratio': (target_price - entry_price) / (entry_price - stop_loss) if stop_loss < entry_price else 0,
                'max_loss_pct': (entry_price - stop_loss) / entry_price if stop_loss < entry_price else 0,
                'expected_gain_pct': (target_price - entry_price) / entry_price,
                'confidence_score': analysis['final_score']
            }
        else:
            plan = {
                'action': analysis['recommendation'],
                'reason': 'Score too low for entry',
                'confidence_score': analysis['final_score']
            }
        
        return {
            'trading_plan': plan,
            'analysis_summary': analysis,
            'market_conditions': {
                'volatility_regime': analysis['detailed_analysis']['volatility_regime']['regime'],
                'momentum_strength': analysis['detailed_analysis']['momentum']['momentum_strength'],
                'volume_trend': analysis['detailed_analysis']['volume_analysis']['volume_trend']
            }
        }