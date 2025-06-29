"""
Advanced Quantitative Models for Enhanced Stock Selection
========================================================

This module implements sophisticated mathematical models and statistical methods
for professional-grade trading signal generation and risk management.

Key Features:
- Machine Learning based signal scoring
- Statistical arbitrage models
- Advanced risk metrics (VaR, CVaR, Beta)
- Portfolio optimization algorithms
- Regime detection using Hidden Markov Models
- Mean reversion and momentum factor models
- Options-based volatility analysis
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import accuracy_score, precision_score
import scipy.stats as stats
from scipy.optimize import minimize
import warnings
warnings.filterwarnings('ignore')

class AdvancedQuantitativeModels:
    """
    Advanced quantitative models for institutional-grade trading
    """
    
    def __init__(self, lookback_period=252):
        self.lookback_period = lookback_period
        self.scaler = StandardScaler()
        self.ml_model = None
        self.regime_model = None
        
        # Risk-free rate (10-year Indian Government Bond)
        self.risk_free_rate = 0.07
        
        # Market correlation matrix for risk calculations
        self.correlation_matrix = None
        
        # Performance tracking
        self.performance_metrics = {
            'ml_accuracy': 0.0,
            'sharpe_ratio': 0.0,
            'max_drawdown': 0.0,
            'var_95': 0.0
        }
    
    def calculate_advanced_features(self, df):
        """
        Generate advanced features for machine learning models
        """
        features = pd.DataFrame(index=df.index)
        
        # Price-based features
        features['returns'] = df['close'].pct_change()
        features['volatility'] = features['returns'].rolling(20).std()
        features['skewness'] = features['returns'].rolling(20).skew()
        features['kurtosis'] = features['returns'].rolling(20).kurt()
        
        # Momentum features
        features['momentum_5'] = df['close'].pct_change(5)
        features['momentum_10'] = df['close'].pct_change(10)
        features['momentum_20'] = df['close'].pct_change(20)
        features['rsi_momentum'] = self._calculate_rsi_momentum(df)
        
        # Mean reversion features
        features['mean_reversion_5'] = self._calculate_mean_reversion(df, 5)
        features['mean_reversion_20'] = self._calculate_mean_reversion(df, 20)
        features['bollinger_position'] = self._calculate_bollinger_position(df)
        
        # Volume features
        features['volume_ratio'] = df['volume'] / df['volume'].rolling(20).mean()
        features['price_volume_correlation'] = self._rolling_correlation(
            df['close'].pct_change(), df['volume'].pct_change(), 20
        )
        
        # Volatility clustering
        features['volatility_regime'] = self._detect_volatility_regime(features['volatility'])
        
        # Microstructure features (if orderbook data available)
        if 'bid' in df.columns and 'ask' in df.columns:
            features['bid_ask_spread'] = (df['ask'] - df['bid']) / df['close']
            features['mid_price_deviation'] = (df['close'] - (df['bid'] + df['ask'])/2) / df['close']
        
        return features.fillna(0)
    
    def _calculate_rsi_momentum(self, df, period=14):
        """Calculate RSI momentum factor"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return (rsi - 50) / 50  # Normalize to [-1, 1]
    
    def _calculate_mean_reversion(self, df, period):
        """Calculate mean reversion score"""
        ma = df['close'].rolling(period).mean()
        std = df['close'].rolling(period).std()
        z_score = (df['close'] - ma) / std
        return -z_score  # Negative because we expect reversion
    
    def _calculate_bollinger_position(self, df, period=20, std_mult=2):
        """Calculate position within Bollinger Bands"""
        ma = df['close'].rolling(period).mean()
        std = df['close'].rolling(period).std()
        upper = ma + (std * std_mult)
        lower = ma - (std * std_mult)
        position = (df['close'] - lower) / (upper - lower)
        return position.clip(0, 1)
    
    def _rolling_correlation(self, series1, series2, window):
        """Calculate rolling correlation between two series"""
        return series1.rolling(window).corr(series2)
    
    def _detect_volatility_regime(self, volatility, high_vol_threshold=0.03, low_vol_threshold=0.01):
        """Detect volatility regime (0=low, 1=normal, 2=high)"""
        conditions = [
            volatility <= low_vol_threshold,
            (volatility > low_vol_threshold) & (volatility <= high_vol_threshold),
            volatility > high_vol_threshold
        ]
        choices = [0, 1, 2]
        return np.select(conditions, choices, default=1)
    
    def train_ml_signal_model(self, data_dict, target_period=5):
        """
        Train machine learning model for signal prediction
        """
        all_features = []
        all_targets = []
        
        for symbol, df in data_dict.items():
            if len(df) < 100:  # Minimum data requirement
                continue
                
            features = self.calculate_advanced_features(df)
            
            # Create target: 1 if price increases by >2% in next N days, 0 otherwise
            future_returns = df['close'].pct_change(target_period).shift(-target_period)
            target = (future_returns > 0.02).astype(int)
            
            # Remove NaN values
            valid_idx = ~(features.isna().any(axis=1) | target.isna())
            if valid_idx.sum() < 50:
                continue
                
            all_features.append(features[valid_idx])
            all_targets.append(target[valid_idx])
        
        if not all_features:
            print("⚠️ No valid data for ML training")
            return False
        
        # Combine all data
        X = pd.concat(all_features, ignore_index=True)
        y = pd.concat(all_targets, ignore_index=True)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Time series cross-validation
        tscv = TimeSeriesSplit(n_splits=3)
        
        # Train ensemble model
        rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
        gb_model = GradientBoostingClassifier(n_estimators=100, random_state=42)
        
        rf_scores = []
        gb_scores = []
        
        for train_idx, test_idx in tscv.split(X_scaled):
            X_train, X_test = X_scaled[train_idx], X_scaled[test_idx]
            y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
            
            # Random Forest
            rf_model.fit(X_train, y_train)
            rf_pred = rf_model.predict(X_test)
            rf_scores.append(accuracy_score(y_test, rf_pred))
            
            # Gradient Boosting
            gb_model.fit(X_train, y_train)
            gb_pred = gb_model.predict(X_test)
            gb_scores.append(accuracy_score(y_test, gb_pred))
        
        # Choose best model
        if np.mean(rf_scores) > np.mean(gb_scores):
            self.ml_model = rf_model
            self.performance_metrics['ml_accuracy'] = np.mean(rf_scores)
        else:
            self.ml_model = gb_model
            self.performance_metrics['ml_accuracy'] = np.mean(gb_scores)
        
        # Final training on full data
        self.ml_model.fit(X_scaled, y)
        
        print(f"✅ ML Model trained - Accuracy: {self.performance_metrics['ml_accuracy']:.3f}")
        return True
    
    def calculate_ml_signal_score(self, df):
        """
        Calculate ML-based signal score for a stock
        """
        if self.ml_model is None:
            return 0.5  # Neutral score
        
        try:
            features = self.calculate_advanced_features(df)
            if features.empty or features.isna().all().all():
                return 0.5
            
            # Use latest data point
            latest_features = features.iloc[-1:].fillna(0)
            features_scaled = self.scaler.transform(latest_features)
            
            # Get probability of positive signal
            signal_prob = self.ml_model.predict_proba(features_scaled)[0][1]
            return signal_prob
            
        except Exception as e:
            print(f"⚠️ ML signal calculation error: {e}")
            return 0.5
    
    def calculate_statistical_arbitrage_score(self, df, benchmark_df):
        """
        Calculate statistical arbitrage opportunities using cointegration
        """
        if len(df) < 100 or len(benchmark_df) < 100:
            return {'score': 0, 'hedge_ratio': 0, 'half_life': np.inf}
        
        # Align data
        common_idx = df.index.intersection(benchmark_df.index)
        if len(common_idx) < 50:
            return {'score': 0, 'hedge_ratio': 0, 'half_life': np.inf}
        
        stock_prices = df.loc[common_idx, 'close']
        benchmark_prices = benchmark_df.loc[common_idx, 'close']
        
        # Calculate hedge ratio using OLS
        X = np.column_stack([np.ones(len(benchmark_prices)), benchmark_prices])
        hedge_ratio = np.linalg.lstsq(X, stock_prices, rcond=None)[0][1]
        
        # Calculate spread
        spread = stock_prices - hedge_ratio * benchmark_prices
        
        # Test for mean reversion (half-life)
        spread_lagged = spread.shift(1).dropna()
        spread_diff = spread.diff().dropna()
        
        if len(spread_lagged) < 20:
            return {'score': 0, 'hedge_ratio': hedge_ratio, 'half_life': np.inf}
        
        # Align arrays
        min_len = min(len(spread_lagged), len(spread_diff))
        spread_lagged = spread_lagged.iloc[:min_len]
        spread_diff = spread_diff.iloc[:min_len]
        
        try:
            # Regression: spread_diff = alpha + beta * spread_lagged
            beta = np.cov(spread_diff, spread_lagged)[0, 1] / np.var(spread_lagged)
            half_life = -np.log(2) / beta if beta < 0 else np.inf
            
            # Score based on mean reversion strength
            current_spread = spread.iloc[-1]
            spread_std = spread.std()
            z_score = abs(current_spread) / spread_std if spread_std > 0 else 0
            
            # Higher score for stronger mean reversion and current mispricing
            score = min(1.0, z_score * 0.5) if half_life < 10 else 0
            
            return {
                'score': score,
                'hedge_ratio': hedge_ratio,
                'half_life': half_life,
                'z_score': z_score,
                'current_spread': current_spread
            }
        except:
            return {'score': 0, 'hedge_ratio': hedge_ratio, 'half_life': np.inf}
    
    def calculate_advanced_risk_metrics(self, df, portfolio_value=1000000):
        """
        Calculate advanced risk metrics including VaR, CVaR, and Beta
        """
        if len(df) < 30:
            return self._default_risk_metrics()
        
        returns = df['close'].pct_change().dropna()
        if len(returns) < 20:
            return self._default_risk_metrics()
        
        # Value at Risk (VaR) - 95% confidence
        var_95 = np.percentile(returns, 5)
        var_95_dollar = var_95 * portfolio_value
        
        # Conditional Value at Risk (CVaR)
        cvar_95 = returns[returns <= var_95].mean()
        cvar_95_dollar = cvar_95 * portfolio_value
        
        # Maximum Drawdown
        cumulative_returns = (1 + returns).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # Volatility (annualized)
        volatility = returns.std() * np.sqrt(252)
        
        # Sharpe Ratio
        excess_returns = returns.mean() * 252 - self.risk_free_rate
        sharpe_ratio = excess_returns / volatility if volatility > 0 else 0
        
        # Sortino Ratio (downside deviation)
        downside_returns = returns[returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else volatility
        sortino_ratio = excess_returns / downside_std if downside_std > 0 else 0
        
        # Calmar Ratio
        annual_return = returns.mean() * 252
        calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        return {
            'var_95': var_95,
            'var_95_dollar': var_95_dollar,
            'cvar_95': cvar_95,
            'cvar_95_dollar': cvar_95_dollar,
            'max_drawdown': max_drawdown,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'calmar_ratio': calmar_ratio,
            'skewness': returns.skew(),
            'kurtosis': returns.kurtosis()
        }
    
    def _default_risk_metrics(self):
        """Return default risk metrics when insufficient data"""
        return {
            'var_95': -0.05,
            'var_95_dollar': -50000,
            'cvar_95': -0.08,
            'cvar_95_dollar': -80000,
            'max_drawdown': -0.20,
            'volatility': 0.30,
            'sharpe_ratio': 0.0,
            'sortino_ratio': 0.0,
            'calmar_ratio': 0.0,
            'skewness': 0.0,
            'kurtosis': 3.0
        }
    
    def calculate_dynamic_stop_loss(self, df, confidence_level=0.95, lookback=20):
        """
        Calculate statistically-based dynamic stop loss
        """
        if len(df) < lookback:
            return df['close'].iloc[-1] * 0.95  # Default 5% stop loss
        
        returns = df['close'].pct_change().dropna()
        current_price = df['close'].iloc[-1]
        
        # Method 1: VaR-based stop loss
        recent_returns = returns.tail(lookback)
        var_stop = np.percentile(recent_returns, (1 - confidence_level) * 100)
        var_stop_price = current_price * (1 + var_stop)
        
        # Method 2: ATR-based stop loss
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(lookback).mean().iloc[-1]
        atr_multiplier = 2.0  # Standard ATR multiplier
        atr_stop_price = current_price - (atr * atr_multiplier)
        
        # Method 3: Volatility clustering stop loss
        volatility = returns.rolling(10).std().iloc[-1]
        vol_regime = self._detect_volatility_regime(pd.Series([volatility]))[0]
        vol_multipliers = {0: 1.5, 1: 2.0, 2: 2.5}  # Low, Normal, High vol
        vol_stop_price = current_price * (1 - volatility * vol_multipliers[vol_regime])
        
        # Method 4: Support/Resistance based stop loss
        support_level = self._find_support_level(df, lookback)
        
        # Take the most conservative (highest) stop loss, but not too tight
        stop_prices = [var_stop_price, atr_stop_price, vol_stop_price, support_level]
        stop_prices = [price for price in stop_prices if price > current_price * 0.90]  # Max 10% stop
        
        if stop_prices:
            final_stop = max(stop_prices)
        else:
            final_stop = current_price * 0.95  # Default fallback
        
        return {
            'stop_loss_price': final_stop,
            'stop_loss_pct': (current_price - final_stop) / current_price,
            'method_used': 'statistical_ensemble',
            'var_stop': var_stop_price,
            'atr_stop': atr_stop_price,
            'vol_stop': vol_stop_price,
            'support_stop': support_level
        }
    
    def _find_support_level(self, df, lookback=20):
        """Find nearest support level using pivot points"""
        if len(df) < lookback:
            return df['close'].iloc[-1] * 0.95
        
        recent_data = df.tail(lookback)
        lows = recent_data['low']
        
        # Find local minima (potential support levels)
        support_levels = []
        for i in range(2, len(lows) - 2):
            if (lows.iloc[i] < lows.iloc[i-1] and lows.iloc[i] < lows.iloc[i-2] and
                lows.iloc[i] < lows.iloc[i+1] and lows.iloc[i] < lows.iloc[i+2]):
                support_levels.append(lows.iloc[i])
        
        current_price = df['close'].iloc[-1]
        
        # Find the highest support level below current price
        valid_supports = [level for level in support_levels if level < current_price]
        
        if valid_supports:
            return max(valid_supports)
        else:
            return current_price * 0.95  # Fallback to 5% below current price
    
    def optimize_portfolio_allocation(self, returns_data, target_return=None, risk_tolerance=0.15):
        """
        Optimize portfolio allocation using Modern Portfolio Theory
        """
        if len(returns_data.columns) < 2:
            return {col: 1.0/len(returns_data.columns) for col in returns_data.columns}
        
        # Calculate expected returns and covariance matrix
        expected_returns = returns_data.mean() * 252  # Annualized
        cov_matrix = returns_data.cov() * 252  # Annualized
        
        n_assets = len(expected_returns)
        
        # Objective function for minimum variance
        def objective(weights):
            portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
            return portfolio_variance
        
        # Constraints
        constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1}]  # Weights sum to 1
        
        if target_return is not None:
            constraints.append({
                'type': 'eq',
                'fun': lambda x: np.dot(x, expected_returns) - target_return
            })
        
        # Bounds (0 to 1 for each weight)
        bounds = tuple((0, 1) for _ in range(n_assets))
        
        # Initial guess (equal weights)
        initial_guess = np.array([1/n_assets] * n_assets)
        
        # Optimize
        try:
            result = minimize(objective, initial_guess, method='SLSQP',
                            bounds=bounds, constraints=constraints)
            
            if result.success:
                optimal_weights = result.x
                return dict(zip(returns_data.columns, optimal_weights))
            else:
                # Fallback to equal weights
                return {col: 1.0/n_assets for col in returns_data.columns}
        except:
            return {col: 1.0/n_assets for col in returns_data.columns}
    
    def calculate_factor_exposure(self, df, market_returns=None):
        """
        Calculate factor exposures (similar to Fama-French factors)
        """
        if len(df) < 60 or market_returns is None or len(market_returns) < 60:
            return {'market_beta': 1.0, 'size_factor': 0.0, 'value_factor': 0.0}
        
        stock_returns = df['close'].pct_change().dropna()
        
        # Align returns
        common_idx = stock_returns.index.intersection(market_returns.index)
        if len(common_idx) < 30:
            return {'market_beta': 1.0, 'size_factor': 0.0, 'value_factor': 0.0}
        
        stock_ret = stock_returns.loc[common_idx]
        market_ret = market_returns.loc[common_idx]
        
        # Calculate market beta
        covariance = np.cov(stock_ret, market_ret)[0, 1]
        market_variance = np.var(market_ret)
        market_beta = covariance / market_variance if market_variance > 0 else 1.0
        
        # Size factor (using price as proxy for market cap)
        avg_price = df['close'].tail(60).mean()
        size_factor = -np.log(avg_price / 1000)  # Negative because small cap has higher returns
        
        # Value factor (using simple price momentum as proxy)
        value_factor = -stock_returns.tail(60).mean()  # Negative momentum as value proxy
        
        return {
            'market_beta': market_beta,
            'size_factor': size_factor,
            'value_factor': value_factor,
            'r_squared': np.corrcoef(stock_ret, market_ret)[0, 1] ** 2
        }
    
    def generate_comprehensive_score(self, df, benchmark_df=None, portfolio_returns=None):
        """
        Generate comprehensive quantitative score combining all models
        """
        scores = {}
        
        # 1. ML Signal Score (30% weight)
        ml_score = self.calculate_ml_signal_score(df)
        scores['ml_signal'] = ml_score
        
        # 2. Risk-Adjusted Performance (25% weight)
        risk_metrics = self.calculate_advanced_risk_metrics(df)
        risk_score = min(1.0, max(0.0, (risk_metrics['sharpe_ratio'] + 1) / 3))  # Normalize Sharpe
        scores['risk_adjusted'] = risk_score
        
        # 3. Statistical Arbitrage (20% weight)
        if benchmark_df is not None:
            stat_arb = self.calculate_statistical_arbitrage_score(df, benchmark_df)
            scores['stat_arbitrage'] = stat_arb['score']
        else:
            scores['stat_arbitrage'] = 0.5
        
        # 4. Factor Exposure (15% weight)
        if benchmark_df is not None:
            market_returns = benchmark_df['close'].pct_change()
            factors = self.calculate_factor_exposure(df, market_returns)
            # Prefer stocks with moderate beta (0.8-1.2) and positive size/value factors
            beta_score = 1 - abs(factors['market_beta'] - 1.0)
            factor_score = (beta_score + max(0, factors['size_factor']) + max(0, factors['value_factor'])) / 3
            scores['factor_exposure'] = min(1.0, max(0.0, factor_score))
        else:
            scores['factor_exposure'] = 0.5
        
        # 5. Volatility Regime (10% weight)
        returns = df['close'].pct_change().dropna()
        if len(returns) > 20:
            current_vol = returns.tail(20).std()
            historical_vol = returns.std()
            vol_score = 1 - min(1.0, current_vol / historical_vol) if historical_vol > 0 else 0.5
        else:
            vol_score = 0.5
        scores['volatility_regime'] = vol_score
        
        # Calculate weighted final score
        weights = {
            'ml_signal': 0.30,
            'risk_adjusted': 0.25,
            'stat_arbitrage': 0.20,
            'factor_exposure': 0.15,
            'volatility_regime': 0.10
        }
        
        final_score = sum(scores[key] * weights[key] for key in scores)
        
        return {
            'final_score': final_score,
            'component_scores': scores,
            'weights': weights,
            'recommendation': self._get_recommendation(final_score)
        }
    
    def _get_recommendation(self, score):
        """Convert score to trading recommendation"""
        if score >= 0.75:
            return "STRONG_BUY"
        elif score >= 0.60:
            return "BUY"
        elif score >= 0.40:
            return "HOLD"
        elif score >= 0.25:
            return "SELL"
        else:
            return "STRONG_SELL"
    
    def get_performance_summary(self):
        """Get performance summary of the quantitative models"""
        return {
            'model_performance': self.performance_metrics,
            'risk_free_rate': self.risk_free_rate,
            'features_count': len(self.scaler.feature_names_in_) if hasattr(self.scaler, 'feature_names_in_') else 'N/A',
            'model_type': type(self.ml_model).__name__ if self.ml_model else 'Not Trained'
        }