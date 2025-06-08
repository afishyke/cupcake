#!/usr/bin/env python3
"""
Real-Time Trading Analysis - Scikit-Learn ML Runner
Fast, reliable, and accurate machine learning predictions for price movements.
"""

import numpy as np
import pandas as pd
import pickle
import logging
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

import talib
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import SelectKBest, mutual_info_classif
import joblib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SklearnRunner:
    """
    High-performance scikit-learn runner for real-time trading predictions.
    Optimized for speed, reliability, and accuracy.
    """
    
    def __init__(self, model_dir: str = "models/sklearn/"):
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        # Model components
        self.price_predictor = None
        self.scaler = None
        self.feature_selector = None
        self.pipeline = None
        
        # Feature engineering parameters
        self.lookback_periods = [5, 10, 14, 20, 50]
        self.feature_columns = []
        self.target_column = 'price_direction'
        
        # TA-Lib optimization settings
        self.talib_functions = [
            'SMA', 'EMA', 'WMA', 'RSI', 'MACD', 'BBANDS', 'ATR', 
            'STOCH', 'ADX', 'CCI', 'WILLR', 'OBV', 'AD'
        ]
        
        # Performance tracking
        self.prediction_buffer = []
        self.accuracy_window = 100
        self.last_accuracy = 0.0
        
        # Load existing models if available
        self._load_models()
    
    def _load_models(self) -> bool:
        """Load pre-trained models from disk."""
        try:
            predictor_path = self.model_dir / "price_predictor.pkl"
            scaler_path = self.model_dir / "scaler.pkl"
            
            if predictor_path.exists() and scaler_path.exists():
                self.price_predictor = joblib.load(predictor_path)
                self.scaler = joblib.load(scaler_path)
                logger.info("Loaded existing models successfully")
                return True
            else:
                logger.info("No existing models found, will train new ones")
                return False
                
        except Exception as e:
            logger.error(f"Error loading models: {e}")
            return False
    
    def _save_models(self):
        """Save trained models to disk."""
        try:
            joblib.dump(self.price_predictor, self.model_dir / "price_predictor.pkl")
            joblib.dump(self.scaler, self.model_dir / "scaler.pkl")
            logger.info("Models saved successfully")
        except Exception as e:
            logger.error(f"Error saving models: {e}")
    
    def _engineer_features(self, ohlcv_df: pd.DataFrame, 
                          tech_indicators: Dict, 
                          order_book: Dict = None) -> pd.DataFrame:
        """
        Fast feature engineering using TA-Lib for optimal performance.
        Optimized for speed and minimal memory usage.
        """
        df = ohlcv_df.copy()
        
        # Ensure we have numpy arrays for TA-Lib
        high = df['high'].values.astype(float)
        low = df['low'].values.astype(float)
        close = df['close'].values.astype(float)
        open_price = df['open'].values.astype(float)
        volume = df['volume'].values.astype(float)
        
        # Basic price features
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
        df['price_change'] = close - open_price
        df['price_range'] = high - low
        df['body_size'] = np.abs(close - open_price)
        df['upper_shadow'] = high - np.maximum(open_price, close)
        df['lower_shadow'] = np.minimum(open_price, close) - low
        
        # Volume features
        df['volume_change'] = df['volume'].pct_change()
        df['price_volume'] = close * volume
        
        # TA-Lib Technical Indicators (Ultra Fast)
        try:
            # Moving Averages
            for period in self.lookback_periods:
                if len(close) >= period:
                    df[f'sma_{period}'] = talib.SMA(close, timeperiod=period)
                    df[f'ema_{period}'] = talib.EMA(close, timeperiod=period)
                    df[f'wma_{period}'] = talib.WMA(close, timeperiod=period)
                    df[f'tema_{period}'] = talib.TEMA(close, timeperiod=period)
            
            # Momentum Indicators
            df['rsi_14'] = talib.RSI(close, timeperiod=14)
            df['rsi_21'] = talib.RSI(close, timeperiod=21)
            df['cci_14'] = talib.CCI(high, low, close, timeperiod=14)
            df['williams_r'] = talib.WILLR(high, low, close, timeperiod=14)
            df['roc_10'] = talib.ROC(close, timeperiod=10)
            df['mom_10'] = talib.MOM(close, timeperiod=10)
            
            # MACD
            macd, macd_signal, macd_hist = talib.MACD(close)
            df['macd'] = macd
            df['macd_signal'] = macd_signal
            df['macd_histogram'] = macd_hist
            
            # Stochastic
            slowk, slowd = talib.STOCH(high, low, close)
            df['stoch_k'] = slowk
            df['stoch_d'] = slowd
            
            # Bollinger Bands
            bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20)
            df['bb_upper'] = bb_upper
            df['bb_middle'] = bb_middle
            df['bb_lower'] = bb_lower
            df['bb_width'] = (bb_upper - bb_lower) / bb_middle
            df['bb_position'] = (close - bb_lower) / (bb_upper - bb_lower)
            
            # Volume Indicators
            df['ad'] = talib.AD(high, low, close, volume)
            df['obv'] = talib.OBV(close, volume)
            df['ad_osc'] = talib.ADOSC(high, low, close, volume)
            
            # Volatility Indicators
            df['atr_14'] = talib.ATR(high, low, close, timeperiod=14)
            df['natr'] = talib.NATR(high, low, close, timeperiod=14)
            df['trange'] = talib.TRANGE(high, low, close)
            
            # Overlap Studies
            df['midpoint'] = talib.MIDPOINT(close, timeperiod=14)
            df['midprice'] = talib.MIDPRICE(high, low, timeperiod=14)
            
            # Parabolic SAR
            df['sar'] = talib.SAR(high, low)
            
            # Average Directional Index
            df['adx'] = talib.ADX(high, low, close, timeperiod=14)
            df['plus_di'] = talib.PLUS_DI(high, low, close, timeperiod=14)
            df['minus_di'] = talib.MINUS_DI(high, low, close, timeperiod=14)
            
            # Price Transform
            df['avgprice'] = talib.AVGPRICE(open_price, high, low, close)
            df['medprice'] = talib.MEDPRICE(high, low)
            df['typprice'] = talib.TYPPRICE(high, low, close)
            df['wclprice'] = talib.WCLPRICE(high, low, close)
            
            # Candlestick Patterns (returns integer codes)
            df['cdl_doji'] = talib.CDLDOJI(open_price, high, low, close)
            df['cdl_hammer'] = talib.CDLHAMMER(open_price, high, low, close)
            df['cdl_engulfing'] = talib.CDLENGULFING(open_price, high, low, close)
            df['cdl_morning_star'] = talib.CDLMORNINGSTAR(open_price, high, low, close)
            df['cdl_evening_star'] = talib.CDLEVENINGSTAR(open_price, high, low, close)
            df['cdl_harami'] = talib.CDLHARAMI(open_price, high, low, close)
            
        except Exception as e:
            logger.warning(f"TA-Lib indicator calculation error: {e}")
        
        # VWAP and custom indicators integration
        if 'vwap' in tech_indicators:
            vwap_value = tech_indicators['vwap']
            if isinstance(vwap_value, (list, np.ndarray)) and len(vwap_value) == len(df):
                df['vwap'] = vwap_value
                df['vwap_deviation'] = close - vwap_value
                df['vwap_ratio'] = close / vwap_value
            elif isinstance(vwap_value, (int, float)):
                df['vwap'] = vwap_value
                df['vwap_deviation'] = close - vwap_value
                df['vwap_ratio'] = close / vwap_value if vwap_value != 0 else 1
        
        # Integrate other technical indicators
        for indicator, value in tech_indicators.items():
            if indicator != 'vwap':  # Already handled above
                if isinstance(value, (int, float, np.number)):
                    df[f'ta_{indicator}'] = value
                elif isinstance(value, (list, np.ndarray)) and len(value) == len(df):
                    df[f'ta_{indicator}'] = value
        
        # Order book features (if available)
        if order_book:
            df['bid_ask_spread'] = order_book.get('ask', 0) - order_book.get('bid', 0)
            df['bid_ask_ratio'] = order_book.get('bid_qty', 1) / max(order_book.get('ask_qty', 1), 1)
            df['order_imbalance'] = (order_book.get('bid_qty', 0) - order_book.get('ask_qty', 0)) / \
                                  max(order_book.get('bid_qty', 1) + order_book.get('ask_qty', 1), 1)
            df['depth_ratio'] = order_book.get('total_bid_qty', 0) / max(order_book.get('total_ask_qty', 1), 1)
        
        # Time-based features
        if hasattr(df.index, 'hour'):
            df['hour'] = pd.to_datetime(df.index).hour
            df['minute'] = pd.to_datetime(df.index).minute
            df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
            df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        else:
            df['hour'] = 0
            df['minute'] = 0
            df['hour_sin'] = 0
            df['hour_cos'] = 1
        
        # Advanced derived features
        df['price_momentum'] = close / close[0] if len(close) > 0 and close[0] != 0 else 1
        df['volume_momentum'] = volume / volume[0] if len(volume) > 0 and volume[0] != 0 else 1
        
        # Volatility clustering features
        df['volatility'] = df['returns'].rolling(20, min_periods=1).std()
        df['volatility_ratio'] = df['volatility'] / df['volatility'].rolling(50, min_periods=1).mean()
        
        # Remove infinite and NaN values
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna(method='forward').fillna(method='backward').fillna(0)
        
        return df
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Fast RSI calculation using vectorized operations."""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / loss.replace(0, np.inf)
        return 100 - (100 / (1 + rs))
    
    def _create_target(self, df: pd.DataFrame, future_periods: int = 3) -> pd.Series:
        """
        Create target variable for price direction prediction.
        1 = Price will go up, 0 = Price will go down/stay same
        """
        future_price = df['close'].shift(-future_periods)
        current_price = df['close']
        price_change_pct = (future_price - current_price) / current_price
        
        # Define threshold for significant price movement
        threshold = 0.001  # 0.1% threshold
        target = (price_change_pct > threshold).astype(int)
        
        return target
    
    def prepare_training_data(self, ohlcv_df: pd.DataFrame, 
                            tech_indicators_history: List[Dict]) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare training data from historical OHLCV and technical indicators.
        """
        logger.info("Preparing training data...")
        
        # Combine technical indicators with OHLCV data
        all_features = []
        
        for i, tech_data in enumerate(tech_indicators_history):
            if i < len(ohlcv_df):
                # Get current row of OHLCV data
                current_ohlcv = ohlcv_df.iloc[i:i+1].copy()
                
                # Engineer features for this row
                features = self._engineer_features(current_ohlcv, tech_data)
                all_features.append(features.iloc[-1])
        
        # Create feature DataFrame
        feature_df = pd.DataFrame(all_features)
        feature_df.index = ohlcv_df.index[:len(feature_df)]
        
        # Create target variable
        target = self._create_target(feature_df)
        
        # Remove last few rows where target is NaN
        valid_mask = ~target.isna()
        feature_df = feature_df[valid_mask]
        target = target[valid_mask]
        
        # Select relevant features (remove highly correlated and constant features)
        feature_df = self._select_features(feature_df)
        
        logger.info(f"Training data prepared: {len(feature_df)} samples, {len(feature_df.columns)} features")
        return feature_df, target
    
    def _select_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select most relevant features and remove problematic ones."""
        # Remove constant features
        constant_features = df.columns[df.var() == 0]
        df = df.drop(columns=constant_features)
        
        # Remove highly correlated features
        corr_matrix = df.corr().abs()
        upper_triangle = corr_matrix.where(
            np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
        )
        
        # Find features with correlation > 0.95
        high_corr_features = [column for column in upper_triangle.columns if any(upper_triangle[column] > 0.95)]
        df = df.drop(columns=high_corr_features)
        
        # Store feature columns for future use
        self.feature_columns = df.columns.tolist()
        
        return df
    
    def train_model(self, feature_df: pd.DataFrame, target: pd.Series) -> Dict[str, float]:
        """
        Train the price prediction model with optimized hyperparameters.
        """
        logger.info("Training machine learning model...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            feature_df, target, test_size=0.2, random_state=42, stratify=target
        )
        
        # Create preprocessing pipeline
        self.scaler = RobustScaler()  # More robust to outliers than StandardScaler
        
        # Feature selection
        self.feature_selector = SelectKBest(
            score_func=mutual_info_classif, 
            k=min(50, len(feature_df.columns))  # Select top 50 features or all if less
        )
        
        # Model selection - using ensemble for better performance
        base_models = {
            'rf': RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1,
                class_weight='balanced'
            ),
            'gb': GradientBoostingClassifier(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=6,
                min_samples_split=10,
                min_samples_leaf=4,
                random_state=42
            )
        }
        
        best_score = 0
        best_model = None
        
        # Test different models and select the best one
        for name, model in base_models.items():
            # Create pipeline
            pipeline = Pipeline([
                ('scaler', self.scaler),
                ('selector', self.feature_selector),
                ('model', model)
            ])
            
            # Cross-validation
            cv_scores = cross_val_score(pipeline, X_train, y_train, cv=5, scoring='accuracy')
            avg_score = cv_scores.mean()
            
            logger.info(f"{name} CV Accuracy: {avg_score:.4f} (+/- {cv_scores.std() * 2:.4f})")
            
            if avg_score > best_score:
                best_score = avg_score
                best_model = pipeline
        
        # Train the best model
        self.pipeline = best_model
        self.pipeline.fit(X_train, y_train)
        
        # Evaluate on test set
        y_pred = self.pipeline.predict(X_test)
        test_accuracy = accuracy_score(y_test, y_pred)
        
        # Store individual components for faster prediction
        self.price_predictor = self.pipeline.named_steps['model']
        self.scaler = self.pipeline.named_steps['scaler']
        self.feature_selector = self.pipeline.named_steps['selector']
        
        # Save models
        self._save_models()
        
        metrics = {
            'cv_accuracy': best_score,
            'test_accuracy': test_accuracy,
            'n_features': len(self.feature_columns),
            'n_selected_features': self.feature_selector.k
        }
        
        logger.info(f"Model training completed. Test Accuracy: {test_accuracy:.4f}")
        return metrics
    
    def predict_single(self, ohlcv_df: pd.DataFrame, 
                      tech_indicators: Dict, 
                      order_book: Dict = None) -> Dict[str, Any]:
        """
        Fast single prediction for real-time trading.
        Optimized for minimal latency.
        """
        try:
            if self.pipeline is None:
                logger.warning("No trained model available for prediction")
                return {'prediction': 0, 'probability': 0.5, 'confidence': 0.0}
            
            # Engineer features
            features_df = self._engineer_features(ohlcv_df, tech_indicators, order_book)
            
            # Get the latest row and ensure it has all required features
            latest_features = features_df.iloc[-1:].copy()
            
            # Align features with trained model
            missing_features = set(self.feature_columns) - set(latest_features.columns)
            for feature in missing_features:
                latest_features[feature] = 0
            
            # Select only the features used in training
            latest_features = latest_features[self.feature_columns]
            
            # Make prediction
            prediction = self.pipeline.predict(latest_features)[0]
            probabilities = self.pipeline.predict_proba(latest_features)[0]
            
            # Calculate confidence (distance from 0.5)
            max_prob = max(probabilities)
            confidence = abs(max_prob - 0.5) * 2  # Scale to 0-1
            
            result = {
                'prediction': int(prediction),
                'probability': float(max_prob),
                'confidence': float(confidence),
                'probabilities': {
                    'down': float(probabilities[0]),
                    'up': float(probabilities[1])
                },
                'timestamp': datetime.now().isoformat()
            }
            
            # Update prediction buffer for accuracy tracking
            self.prediction_buffer.append(result)
            if len(self.prediction_buffer) > self.accuracy_window:
                self.prediction_buffer.pop(0)
            
            return result
            
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return {
                'prediction': 0, 
                'probability': 0.5, 
                'confidence': 0.0,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def batch_predict(self, feature_dataframes: List[pd.DataFrame], 
                     tech_indicators_list: List[Dict]) -> List[Dict]:
        """
        Batch prediction for multiple instruments/timeframes.
        """
        predictions = []
        
        for i, (ohlcv_df, tech_indicators) in enumerate(zip(feature_dataframes, tech_indicators_list)):
            try:
                prediction = self.predict_single(ohlcv_df, tech_indicators)
                prediction['instrument_index'] = i
                predictions.append(prediction)
            except Exception as e:
                logger.error(f"Batch prediction error for instrument {i}: {e}")
                predictions.append({
                    'prediction': 0, 
                    'probability': 0.5, 
                    'confidence': 0.0,
                    'instrument_index': i,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
        
        return predictions
    
    def update_model_performance(self, actual_results: List[int]):
        """
        Update model performance tracking with actual results.
        """
        if len(actual_results) > len(self.prediction_buffer):
            logger.warning("More actual results than predictions in buffer")
            return
        
        # Calculate accuracy for recent predictions
        recent_predictions = [p['prediction'] for p in self.prediction_buffer[-len(actual_results):]]
        correct_predictions = sum(1 for pred, actual in zip(recent_predictions, actual_results) if pred == actual)
        
        if len(actual_results) > 0:
            self.last_accuracy = correct_predictions / len(actual_results)
            logger.info(f"Recent model accuracy: {self.last_accuracy:.4f}")
    
    def get_feature_importance(self) -> Dict[str, float]:
        """
        Get feature importance from the trained model.
        """
        if self.price_predictor is None:
            return {}
        
        try:
            if hasattr(self.price_predictor, 'feature_importances_'):
                # For tree-based models
                selected_features = self.feature_selector.get_feature_names_out(self.feature_columns)
                importance_dict = dict(zip(selected_features, self.price_predictor.feature_importances_))
                
                # Sort by importance
                return dict(sorted(importance_dict.items(), key=lambda x: x[1], reverse=True))
            else:
                logger.warning("Model does not support feature importance")
                return {}
        except Exception as e:
            logger.error(f"Error getting feature importance: {e}")
            return {}
    
    def retrain_if_needed(self, performance_threshold: float = 0.55) -> bool:
        """
        Retrain model if performance drops below threshold.
        """
        if self.last_accuracy < performance_threshold:
            logger.info(f"Model performance ({self.last_accuracy:.4f}) below threshold ({performance_threshold}). Retraining recommended.")
            return True
        return False
    
    def save_prediction_history(self, filepath: str = None):
        """
        Save prediction history to CSV for analysis.
        """
        if not self.prediction_buffer:
            logger.warning("No predictions to save")
            return
        
        if filepath is None:
            filepath = self.model_dir / f"prediction_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            df = pd.DataFrame(self.prediction_buffer)
            df.to_csv(filepath, index=False)
            logger.info(f"Prediction history saved to {filepath}")
        except Exception as e:
            logger.error(f"Error saving prediction history: {e}")
    
    def get_model_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive model statistics and metadata.
        """
        stats = {
            'model_loaded': self.pipeline is not None,
            'feature_count': len(self.feature_columns),
            'prediction_buffer_size': len(self.prediction_buffer),
            'last_accuracy': self.last_accuracy,
            'model_type': type(self.price_predictor).__name__ if self.price_predictor else None,
            'scaler_type': type(self.scaler).__name__ if self.scaler else None,
        }
        
        if self.feature_selector:
            stats['selected_features_count'] = self.feature_selector.k
        
        return stats

# Main execution for standalone testing
if __name__ == "__main__":
    # Example usage
    runner = SklearnRunner()
    
    # Generate sample data for testing
    dates = pd.date_range(start='2023-01-01', periods=1000, freq='1min')
    sample_ohlcv = pd.DataFrame({
        'open': np.random.normal(100, 5, 1000),
        'high': np.random.normal(102, 5, 1000),
        'low': np.random.normal(98, 5, 1000),
        'close': np.random.normal(100, 5, 1000),
        'volume': np.random.normal(10000, 1000, 1000)
    }, index=dates)
    
    # Generate sample technical indicators
    sample_tech_indicators = [
        {'vwap': np.random.normal(100, 2), 'rsi': np.random.uniform(30, 70)}
        for _ in range(len(sample_ohlcv))
    ]
    
    try:
        # Prepare training data
        features, target = runner.prepare_training_data(sample_ohlcv, sample_tech_indicators)
        
        # Train model
        metrics = runner.train_model(features, target)
        print(f"Training metrics: {metrics}")
        
        # Test single prediction
        test_ohlcv = sample_ohlcv.tail(5)
        test_indicators = sample_tech_indicators[-1]
        
        prediction = runner.predict_single(test_ohlcv, test_indicators)
        print(f"Test prediction: {prediction}")
        
        # Get model statistics
        stats = runner.get_model_stats()
        print(f"Model stats: {stats}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise