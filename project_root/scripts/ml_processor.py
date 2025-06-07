# Save this as scripts/ml_processor.py
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import MiniBatchKMeans
from sklearn.ensemble import IsolationForest
from numba import jit, njit
import warnings
warnings.filterwarnings('ignore')

# --- Configuration ---
# Define which features we will use for ML processing
ML_FEATURES = [
    'indicators.RSI_14', 'indicators.MACD.hist', 'indicators.ADX_14',
    'indicators.BBANDS.width_pct', 'volume.relative_pct'
]

# Pre-compiled numpy arrays for ultra-fast access
ML_FEATURES_ARRAY = np.array(ML_FEATURES, dtype='U50')
N_FEATURES = len(ML_FEATURES)

# Global model storage with optimized parameters
SCALER = StandardScaler()
CLUSTER_MODEL = MiniBatchKMeans(
    n_clusters=4, 
    random_state=42, 
    batch_size=512,  # Larger batch for speed
    n_init=3,        # Fewer initializations for speed
    max_iter=100,    # Fewer iterations for speed
    tol=1e-2         # Less strict tolerance for speed
)
ANOMALY_MODEL = IsolationForest(
    contamination='auto', 
    random_state=42,
    n_estimators=50,  # Fewer trees for speed
    max_samples=256,  # Smaller sample size for speed
    n_jobs=-1         # Use all CPU cores
)

# Cache for fitted model parameters (ultra-fast lookup)
_scaler_mean = None
_scaler_scale = None
_cluster_centers = None
_anomaly_threshold = None
_models_fitted = False

@njit(fastmath=True, cache=True)
def fast_standardize(data, mean_vals, scale_vals):
    """Ultra-fast standardization using numba JIT compilation"""
    return (data - mean_vals) / scale_vals

@njit(fastmath=True, cache=True)
def fast_euclidean_distance(point, centers):
    """Ultra-fast distance calculation for clustering"""
    n_centers = centers.shape[0]
    distances = np.empty(n_centers, dtype=np.float64)
    
    for i in range(n_centers):
        dist = 0.0
        for j in range(point.shape[0]):
            diff = point[j] - centers[i, j]
            dist += diff * diff
        distances[i] = np.sqrt(dist)
    
    return distances

@njit(fastmath=True, cache=True)
def fast_predict_cluster(scaled_data, cluster_centers):
    """Ultra-fast cluster prediction"""
    n_samples = scaled_data.shape[0]
    predictions = np.empty(n_samples, dtype=np.int32)
    
    for i in range(n_samples):
        distances = fast_euclidean_distance(scaled_data[i], cluster_centers)
        predictions[i] = np.argmin(distances)
    
    return predictions

def fit_ml_models(df_history):
    """
    Fits the ML models on historical data with optimizations.
    """
    global _scaler_mean, _scaler_scale, _cluster_centers, _anomaly_threshold, _models_fitted
    
    print("--- Fitting ML Models on Historical Data (Ultra-Fast Mode) ---")
    
    # Fast feature extraction with pre-allocated arrays
    available_features = [col for col in ML_FEATURES if col in df_history.columns]
    
    if len(available_features) < 3:  # Minimum features required
        print("Warning: Not enough features available for ML models.")
        return
    
    # Use only available features and drop NaN rows efficiently
    feature_data = df_history[available_features].dropna()
    
    if len(feature_data) < 50:  # Minimum samples required
        print("Warning: Not enough historical data to fit ML models.")
        return
    
    # Sample data for faster training if dataset is large
    if len(feature_data) > 10000:
        feature_data = feature_data.sample(n=10000, random_state=42)
    
    # Convert to numpy for speed
    feature_array = feature_data.values.astype(np.float64)
    
    # Fit scaler and cache parameters
    SCALER.fit(feature_array)
    _scaler_mean = SCALER.mean_.copy()
    _scaler_scale = SCALER.scale_.copy()
    
    # Scale data once
    scaled_data = SCALER.transform(feature_array)
    
    # Fit clustering model and cache centers
    CLUSTER_MODEL.fit(scaled_data)
    _cluster_centers = CLUSTER_MODEL.cluster_centers_.copy()
    
    # Fit anomaly detection model
    ANOMALY_MODEL.fit(scaled_data)
    
    # Pre-compute anomaly threshold for faster predictions
    sample_scores = ANOMALY_MODEL.decision_function(scaled_data[:1000])
    _anomaly_threshold = np.percentile(sample_scores, 10)  # Bottom 10% as anomalies
    
    _models_fitted = True
    print(f"--- ML Models Fitted Successfully on {len(feature_data)} samples ---")

def process_with_sklearn(analytical_data: list) -> list:
    """
    Ultra-fast enrichment with ML predictions using optimized algorithms.
    """
    if not analytical_data or not _models_fitted:
        return analytical_data
    
    n_samples = len(analytical_data)
    
    # Pre-allocate arrays for maximum speed
    feature_matrix = np.zeros((n_samples, N_FEATURES), dtype=np.float64)
    
    # Ultra-fast feature extraction with direct indexing
    try:
        # Convert to DataFrame only once
        live_df = pd.json_normalize(analytical_data, sep='_')
        
        # Fast feature extraction with vectorized operations
        for i, feature in enumerate(ML_FEATURES):
            if feature in live_df.columns:
                feature_matrix[:, i] = live_df[feature].fillna(0).values
            # Missing features remain as zeros (pre-allocated)
        
        # Ultra-fast standardization using cached parameters
        scaled_data = fast_standardize(feature_matrix, _scaler_mean, _scaler_scale)
        
        # Ultra-fast clustering prediction
        clusters = fast_predict_cluster(scaled_data, _cluster_centers)
        
        # Fast anomaly detection using pre-fitted model
        anomaly_scores = ANOMALY_MODEL.decision_function(scaled_data)
        
        # Vectorized anomaly classification
        is_anomaly = anomaly_scores < _anomaly_threshold
        
        # Ultra-fast result assignment using enumerate
        for i, (item, cluster, score, anomaly) in enumerate(
            zip(analytical_data, clusters, anomaly_scores, is_anomaly)
        ):
            item['ml_insights'] = {
                'cluster': int(cluster),
                'anomaly_score': round(float(score), 4),
                'is_anomaly': bool(anomaly)
            }
            
    except Exception as e:
        print(f"Error during ML processing: {e}. Adding empty insights.")
        # Fast fallback with list comprehension
        for item in analytical_data:
            item['ml_insights'] = {'cluster': 0, 'anomaly_score': 0.0, 'is_anomaly': False}
    
    return analytical_data

# Additional optimization functions for advanced use cases
@njit(fastmath=True, cache=True)
def fast_feature_engineering(rsi, macd, adx, bb_width, vol_rel):
    """
    Ultra-fast feature engineering with numba compilation.
    Add custom derived features here for better accuracy.
    """
    # Momentum composite score
    momentum_score = (rsi - 50) * 0.02 + macd * 10 + (adx - 25) * 0.04
    
    # Volatility composite score  
    volatility_score = bb_width * 0.5 + abs(vol_rel) * 0.1
    
    # Market regime indicator
    regime = 1 if rsi > 70 or rsi < 30 else 0
    
    return momentum_score, volatility_score, regime

def batch_process_with_sklearn(analytical_data_batches: list) -> list:
    """
    Ultra-fast batch processing for handling multiple data batches efficiently.
    """
    if not _models_fitted:
        return analytical_data_batches
    
    results = []
    for batch in analytical_data_batches:
        results.append(process_with_sklearn(batch))
    
    return results

# Performance monitoring
def get_performance_stats():
    """Returns current model performance statistics"""
    return {
        'models_fitted': _models_fitted,
        'n_features': N_FEATURES,
        'n_clusters': 4,
        'scaler_fitted': _scaler_mean is not None,
        'cluster_centers_cached': _cluster_centers is not None
    }