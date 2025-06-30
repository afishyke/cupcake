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
from scipy.signal import butter, filtfilt
from scipy.linalg import inv
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
        
        # Kalman filter parameters
        self.kalman_params = {
            'process_variance': 1e-5,
            'measurement_variance': 1e-1,
            'estimation_error': 1.0
        }
        
        # Cross-asset correlation cache
        self.correlation_cache = {}
        self.price_cache = {}
    
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
            
            # Align all series to have the same length (dropna removes first row from shifted series)
            high_close = high_close.dropna()
            low_close = low_close.dropna()
            
            # Align all to the same index
            common_index = high_low.index.intersection(high_close.index).intersection(low_close.index)
            if len(common_index) > 0:
                tr_data = pd.concat([
                    high_low.loc[common_index], 
                    high_close.loc[common_index], 
                    low_close.loc[common_index]
                ], axis=1)
            else:
                # Fallback if no common index
                tr_data = pd.DataFrame({'hl': high_low, 'hc': [0]*len(high_low), 'lc': [0]*len(high_low)})
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
        
        # Align the arrays to have the same length for covariance calculation
        aligned_returns = returns.dropna()
        aligned_market = market_returns.dropna()
        
        # Find common index to ensure same length
        common_index = aligned_returns.index.intersection(aligned_market.index)
        if len(common_index) > 10:
            aligned_returns = aligned_returns.loc[common_index]
            aligned_market = aligned_market.loc[common_index]
            
            covariance = np.cov(aligned_returns, aligned_market)[0, 1]
            market_variance = np.var(aligned_market)
            beta = covariance / market_variance if market_variance > 0 else 1.0
            alpha = aligned_returns.mean() - beta * aligned_market.mean()
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
    
    # ============================================================================
    # RENAISSANCE TECHNOLOGIES-STYLE ADVANCED QUANTITATIVE METHODS
    # ============================================================================
    
    def apply_kalman_filter(self, price_series, symbol=None):
        """
        Apply Kalman filter for signal smoothing and noise reduction
        Renaissance-style adaptive filtering
        """
        if len(price_series) < 10:
            return price_series.copy()
        
        # Kalman filter implementation
        n = len(price_series)
        
        # State transition model (prices follow random walk)
        A = 1  # State transition
        H = 1  # Observation model
        
        # Noise parameters
        Q = self.kalman_params['process_variance']  # Process noise
        R = self.kalman_params['measurement_variance']  # Measurement noise
        
        # Initialize
        x = np.zeros(n)  # State estimates
        P = np.zeros(n)  # Error covariances
        
        x[0] = price_series.iloc[0]
        P[0] = self.kalman_params['estimation_error']
        
        # Kalman filtering
        for k in range(1, n):
            # Prediction step
            x_pred = A * x[k-1]
            P_pred = A * P[k-1] * A + Q
            
            # Update step
            K = P_pred * H / (H * P_pred * H + R)  # Kalman gain
            x[k] = x_pred + K * (price_series.iloc[k] - H * x_pred)
            P[k] = (1 - K * H) * P_pred
        
        return pd.Series(x, index=price_series.index)
    
    def calculate_cross_asset_momentum(self, symbols_data):
        """
        Cross-asset momentum analysis for enhanced signal generation
        Analyze momentum spillover effects between related assets
        """
        if len(symbols_data) < 2:
            return {'cross_momentum_score': 0, 'momentum_consensus': 'NEUTRAL'}
        
        momentum_scores = {}
        price_changes = {}
        
        # Calculate momentum for each asset
        for symbol, df in symbols_data.items():
            if len(df) < 21:
                continue
                
            # Multiple timeframe momentum
            momentum_1w = (df['close'].iloc[-1] / df['close'].iloc[-5] - 1) if len(df) >= 5 else 0
            momentum_2w = (df['close'].iloc[-1] / df['close'].iloc[-10] - 1) if len(df) >= 10 else 0
            momentum_3w = (df['close'].iloc[-1] / df['close'].iloc[-21] - 1) if len(df) >= 21 else 0
            
            # Weighted momentum score
            momentum_score = (momentum_1w * 0.5 + momentum_2w * 0.3 + momentum_3w * 0.2)
            momentum_scores[symbol] = momentum_score
            price_changes[symbol] = momentum_1w
        
        if not momentum_scores:
            return {'cross_momentum_score': 0, 'momentum_consensus': 'NEUTRAL'}
        
        # Calculate consensus momentum
        avg_momentum = np.mean(list(momentum_scores.values()))
        momentum_std = np.std(list(momentum_scores.values()))
        
        # Calculate cross-asset correlation in momentum
        if len(momentum_scores) >= 3:
            momentum_values = list(momentum_scores.values())
            momentum_corr = np.corrcoef(momentum_values, momentum_values)[0, 1] if len(momentum_values) > 1 else 0
        else:
            momentum_corr = 0
        
        # Determine consensus strength
        consensus_threshold = 0.6
        positive_momentum_count = sum(1 for m in momentum_scores.values() if m > 0.01)
        negative_momentum_count = sum(1 for m in momentum_scores.values() if m < -0.01)
        total_assets = len(momentum_scores)
        
        if positive_momentum_count / total_assets > consensus_threshold:
            consensus = 'STRONG_BULLISH'
        elif negative_momentum_count / total_assets > consensus_threshold:
            consensus = 'STRONG_BEARISH'
        elif positive_momentum_count > negative_momentum_count:
            consensus = 'WEAK_BULLISH'
        elif negative_momentum_count > positive_momentum_count:
            consensus = 'WEAK_BEARISH'
        else:
            consensus = 'NEUTRAL'
        
        return {
            'cross_momentum_score': avg_momentum,
            'momentum_consensus': consensus,
            'momentum_correlation': momentum_corr,
            'momentum_dispersion': momentum_std,
            'individual_momentum': momentum_scores,
            'consensus_strength': max(positive_momentum_count, negative_momentum_count) / total_assets
        }
    
    def statistical_significance_test(self, signal_data, alpha=0.05):
        """
        Test statistical significance of trading signals
        Uses multiple statistical tests for robustness
        """
        if len(signal_data) < 30:
            return {'significant': False, 'p_value': 1.0, 'test_used': 'insufficient_data'}
        
        # Convert to returns if price data
        if signal_data.max() > 10:  # Likely price data
            returns = signal_data.pct_change().dropna()
        else:
            returns = signal_data.dropna()
        
        if len(returns) < 10:
            return {'significant': False, 'p_value': 1.0, 'test_used': 'insufficient_returns'}
        
        tests_results = {}
        
        # Test 1: T-test against zero (is mean return significantly different from 0?)
        t_stat, t_p_value = stats.ttest_1samp(returns, 0)
        tests_results['t_test'] = {
            'statistic': t_stat,
            'p_value': t_p_value,
            'significant': t_p_value < alpha
        }
        
        # Test 2: Jarque-Bera test for normality
        if len(returns) > 8:
            jb_stat, jb_p_value = stats.jarque_bera(returns)
            tests_results['normality_test'] = {
                'statistic': jb_stat,
                'p_value': jb_p_value,
                'normal_distribution': jb_p_value > alpha
            }
        
        # Test 3: Run test for randomness
        median_return = np.median(returns)
        runs, runs_p_value = self._runs_test(returns > median_return)
        tests_results['runs_test'] = {
            'statistic': runs,
            'p_value': runs_p_value,
            'random': runs_p_value > alpha
        }
        
        # Test 4: Augmented Dickey-Fuller test for stationarity
        try:
            from scipy.stats import adfuller
            adf_stat, adf_p_value = adfuller(returns, maxlag=1)[:2]
            tests_results['stationarity_test'] = {
                'statistic': adf_stat,
                'p_value': adf_p_value,
                'stationary': adf_p_value < alpha
            }
        except:
            tests_results['stationarity_test'] = {'p_value': 1.0, 'stationary': False}
        
        # Overall significance (conservative approach)
        significant_tests = sum(1 for test in tests_results.values() 
                              if test.get('significant', False) or test.get('p_value', 1) < alpha)
        
        overall_significant = significant_tests >= 2  # At least 2 tests must be significant
        min_p_value = min(test.get('p_value', 1) for test in tests_results.values())
        
        return {
            'significant': overall_significant,
            'p_value': min_p_value,
            'significant_tests_count': significant_tests,
            'total_tests': len(tests_results),
            'individual_tests': tests_results,
            'confidence_level': 1 - alpha
        }
    
    def _runs_test(self, binary_sequence):
        """
        Runs test for randomness
        """
        binary_sequence = np.array(binary_sequence, dtype=bool)
        n = len(binary_sequence)
        
        if n < 2:
            return 0, 1.0
        
        # Count runs
        runs = 1
        for i in range(1, n):
            if binary_sequence[i] != binary_sequence[i-1]:
                runs += 1
        
        # Calculate expected runs and standard deviation
        n1 = np.sum(binary_sequence)  # Number of True values
        n0 = n - n1  # Number of False values
        
        if n1 == 0 or n0 == 0:
            return runs, 1.0
        
        expected_runs = (2 * n1 * n0) / n + 1
        variance_runs = (2 * n1 * n0 * (2 * n1 * n0 - n)) / (n * n * (n - 1))
        
        if variance_runs <= 0:
            return runs, 1.0
        
        # Z-score
        z_score = (runs - expected_runs) / np.sqrt(variance_runs)
        p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
        
        return runs, p_value
    
    def calculate_regime_dependent_signals(self, df, regime_lookback=60):
        """
        Calculate signals that adapt to different market regimes
        Renaissance-style regime-dependent strategies
        """
        if len(df) < regime_lookback:
            return {'regime': 'UNKNOWN', 'signals': {}}
        
        returns = df['close'].pct_change().dropna()
        
        # Identify market regime
        recent_returns = returns.tail(regime_lookback)
        
        # Regime characteristics
        volatility = recent_returns.std() * np.sqrt(252)
        skewness = stats.skew(recent_returns)
        kurtosis = stats.kurtosis(recent_returns)
        autocorr_1 = recent_returns.autocorr(lag=1) if len(recent_returns) > 1 else 0
        
        # Classify regime
        if volatility > 0.25:  # High volatility
            if abs(skewness) > 1:
                regime = 'CRISIS'
            else:
                regime = 'HIGH_VOLATILITY'
        elif volatility < 0.12:  # Low volatility
            if kurtosis > 3:
                regime = 'LOW_VOLATILITY_COMPRESSED'
            else:
                regime = 'LOW_VOLATILITY_NORMAL'
        else:
            if autocorr_1 > 0.1:
                regime = 'TRENDING'
            elif autocorr_1 < -0.1:
                regime = 'MEAN_REVERTING'
            else:
                regime = 'NORMAL'
        
        # Regime-specific signals
        signals = {}
        
        if regime in ['TRENDING', 'HIGH_VOLATILITY']:
            # Momentum signals work better
            momentum = self.calculate_advanced_momentum_score(df)
            signals['primary_strategy'] = 'momentum'
            signals['momentum_weight'] = 0.7
            signals['mean_reversion_weight'] = 0.3
            
        elif regime in ['MEAN_REVERTING', 'LOW_VOLATILITY_COMPRESSED']:
            # Mean reversion signals work better
            mean_rev = self.calculate_mean_reversion_probability(df)
            signals['primary_strategy'] = 'mean_reversion'
            signals['momentum_weight'] = 0.3
            signals['mean_reversion_weight'] = 0.7
            
        elif regime == 'CRISIS':
            # Risk-off signals
            signals['primary_strategy'] = 'risk_off'
            signals['momentum_weight'] = 0.2
            signals['mean_reversion_weight'] = 0.2
            signals['risk_off_weight'] = 0.6
            
        else:
            # Balanced approach
            signals['primary_strategy'] = 'balanced'
            signals['momentum_weight'] = 0.5
            signals['mean_reversion_weight'] = 0.5
        
        return {
            'regime': regime,
            'regime_characteristics': {
                'volatility': volatility,
                'skewness': skewness,
                'kurtosis': kurtosis,
                'autocorrelation': autocorr_1
            },
            'signals': signals,
            'confidence': min(1.0, len(recent_returns) / regime_lookback)
        }
    
    def calculate_microstructure_alpha(self, df, orderbook_data=None):
        """
        Calculate alpha from market microstructure
        High-frequency patterns and orderbook dynamics
        """
        if len(df) < 50:
            return {'microstructure_alpha': 0, 'confidence': 0}
        
        alpha_signals = {}
        
        # 1. Intraday momentum patterns
        if 'timestamp' in df.columns or df.index.name == 'timestamp':
            # Time-based patterns
            df_copy = df.copy()
            if 'timestamp' not in df.columns:
                df_copy['timestamp'] = df.index
            
            df_copy['hour'] = pd.to_datetime(df_copy['timestamp']).dt.hour
            df_copy['minute'] = pd.to_datetime(df_copy['timestamp']).dt.minute
            
            # Opening auction effects (9:15-9:30)
            opening_returns = df_copy[df_copy['hour'] == 9]['close'].pct_change().mean()
            
            # Closing auction effects (15:15-15:30)
            closing_returns = df_copy[df_copy['hour'] == 15]['close'].pct_change().mean()
            
            alpha_signals['opening_bias'] = opening_returns
            alpha_signals['closing_bias'] = closing_returns
        
        # 2. Volume-price relationship
        if 'volume' in df.columns:
            price_changes = df['close'].pct_change()
            volume_changes = df['volume'].pct_change()
            
            # Volume-price correlation
            vol_price_corr = price_changes.corr(volume_changes)
            alpha_signals['volume_price_correlation'] = vol_price_corr
            
            # Unusual volume patterns
            volume_ma = df['volume'].rolling(20).mean()
            volume_ratio = df['volume'] / volume_ma
            high_volume_returns = price_changes[volume_ratio > 2].mean()
            alpha_signals['high_volume_alpha'] = high_volume_returns
        
        # 3. Price level effects
        df_copy = df.copy()  # Don't modify original df
        df_copy['returns'] = df_copy['close'].pct_change()
        
        try:
            df_copy['price_level'] = pd.qcut(df_copy['close'], q=5, labels=['very_low', 'low', 'medium', 'high', 'very_high'], duplicates='drop')
            level_returns = df_copy.groupby('price_level')['returns'].mean()
            alpha_signals['price_level_effects'] = level_returns.to_dict()
        except Exception as e:
            # Handle cases where qcut fails (e.g., too few unique values)
            alpha_signals['price_level_effects'] = {}
        
        # 4. Bid-ask spread analysis (if orderbook data available)
        if orderbook_data:
            spread = (orderbook_data['ask'] - orderbook_data['bid']) / orderbook_data['mid']
            spread_quartiles = pd.qcut(spread, q=4, labels=['tight', 'normal', 'wide', 'very_wide'])
            
            spread_returns = {}
            for quartile in spread_quartiles.categories:
                mask = spread_quartiles == quartile
                if mask.sum() > 0:
                    spread_returns[quartile] = df_copy.loc[mask, 'returns'].mean()
            
            alpha_signals['spread_effects'] = spread_returns
        
        # 5. Serial correlation in returns
        serial_corr_1 = df_copy['returns'].autocorr(lag=1)
        serial_corr_5 = df_copy['returns'].autocorr(lag=5)
        
        alpha_signals['serial_correlation_1'] = serial_corr_1
        alpha_signals['serial_correlation_5'] = serial_corr_5
        
        # Calculate overall microstructure alpha
        alpha_score = 0
        
        # Weight different alpha sources
        if abs(alpha_signals.get('opening_bias', 0)) > 0.001:
            alpha_score += alpha_signals['opening_bias'] * 0.2
        
        if abs(alpha_signals.get('high_volume_alpha', 0)) > 0.001:
            alpha_score += alpha_signals['high_volume_alpha'] * 0.3
        
        if abs(serial_corr_1) > 0.05:
            alpha_score += serial_corr_1 * 0.2
        
        confidence = min(1.0, len(df) / 100)  # More data = higher confidence
        
        return {
            'microstructure_alpha': alpha_score,
            'confidence': confidence,
            'alpha_components': alpha_signals,
            'dominant_pattern': max(alpha_signals.items(), key=lambda x: abs(x[1])) if alpha_signals else ('none', 0)
        }
    
    def calculate_enhanced_comprehensive_score(self, df, symbols_data=None, orderbook_data=None):
        """
        Enhanced comprehensive scoring with all Renaissance-style methods
        """
        base_analysis = self.calculate_comprehensive_score(df)
        
        enhancements = {}
        
        # 1. Kalman filtered signals
        if len(df) >= 20:
            filtered_prices = self.apply_kalman_filter(df['close'])
            filtered_df = df.copy()
            filtered_df['close'] = filtered_prices
            
            filtered_momentum = self.calculate_advanced_momentum_score(filtered_df)
            enhancements['kalman_momentum'] = filtered_momentum['momentum_score']
        
        # 2. Cross-asset momentum (if multi-asset data available)
        if symbols_data and len(symbols_data) > 1:
            cross_momentum = self.calculate_cross_asset_momentum(symbols_data)
            enhancements['cross_asset_momentum'] = cross_momentum['cross_momentum_score']
            enhancements['momentum_consensus'] = cross_momentum['momentum_consensus']
        
        # 3. Statistical significance
        recent_returns = df['close'].pct_change().tail(30)
        significance = self.statistical_significance_test(recent_returns)
        enhancements['signal_significance'] = significance['significant']
        enhancements['signal_p_value'] = significance['p_value']
        
        # 4. Regime-dependent signals
        regime_analysis = self.calculate_regime_dependent_signals(df)
        enhancements['market_regime'] = regime_analysis['regime']
        enhancements['regime_signals'] = regime_analysis['signals']
        
        # 5. Microstructure alpha
        microstructure = self.calculate_microstructure_alpha(df, orderbook_data)
        enhancements['microstructure_alpha'] = microstructure['microstructure_alpha']
        
        # Adjust final score based on enhancements
        base_score = base_analysis['final_score']
        
        # Kalman filter bonus (reduces noise)
        if 'kalman_momentum' in enhancements:
            if abs(enhancements['kalman_momentum']) > abs(base_analysis['component_scores'].get('momentum', 0)):
                base_score += 0.05
        
        # Cross-asset consensus bonus
        if enhancements.get('momentum_consensus') in ['STRONG_BULLISH', 'STRONG_BEARISH']:
            base_score += 0.08
        
        # Statistical significance bonus
        if enhancements.get('signal_significance'):
            base_score += 0.10
        
        # Regime adaptation bonus
        if regime_analysis['confidence'] > 0.8:
            base_score += 0.05
        
        # Microstructure alpha bonus
        if abs(enhancements.get('microstructure_alpha', 0)) > 0.01:
            base_score += min(0.1, abs(enhancements['microstructure_alpha']) * 5)
        
        # Cap the score between 0 and 1
        enhanced_score = max(0, min(1, base_score))
        
        # Update recommendation based on enhanced score
        if enhanced_score >= 0.8:
            recommendation = 'STRONG_BUY'
        elif enhanced_score >= 0.65:
            recommendation = 'BUY'
        elif enhanced_score >= 0.35:
            recommendation = 'HOLD'
        elif enhanced_score >= 0.2:
            recommendation = 'SELL'
        else:
            recommendation = 'STRONG_SELL'
        
        return {
            'enhanced_score': enhanced_score,
            'base_score': base_analysis['final_score'],
            'recommendation': recommendation,
            'base_analysis': base_analysis,
            'enhancements': enhancements,
            'confidence_factors': {
                'statistical_significance': enhancements.get('signal_significance', False),
                'cross_asset_consensus': enhancements.get('momentum_consensus', 'NEUTRAL'),
                'regime_confidence': regime_analysis['confidence'],
                'microstructure_confidence': microstructure['confidence']
            }
        }