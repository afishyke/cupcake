"""
Advanced Indicators Module - Stateless library for complex market calculations
This module provides pure functions for calculating sophisticated market metrics.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import logging

logger = logging.getLogger(__name__)

def calculate_vwap(ohlcv_df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate Volume Weighted Average Price and standard deviation bands.
    
    Args:
        ohlcv_df: DataFrame with columns ['open', 'high', 'low', 'close', 'volume', 'timestamp']
    
    Returns:
        Dictionary with VWAP and standard deviation bands
    """
    try:
        if ohlcv_df.empty or len(ohlcv_df) < 2:
            return {"vwap": 0.0, "upper_band_2sd": 0.0, "lower_band_2sd": 0.0}
        
        # Calculate typical price (HLC/3)
        typical_price = (ohlcv_df['high'] + ohlcv_df['low'] + ohlcv_df['close']) / 3
        
        # Calculate VWAP
        cumulative_pv = (typical_price * ohlcv_df['volume']).cumsum()
        cumulative_volume = ohlcv_df['volume'].cumsum()
        
        # Avoid division by zero
        cumulative_volume = cumulative_volume.replace(0, np.nan)
        vwap_series = cumulative_pv / cumulative_volume
        current_vwap = vwap_series.iloc[-1] if not vwap_series.empty else 0.0
        
        # Calculate standard deviation for bands
        if len(ohlcv_df) >= 10:  # Need sufficient data for meaningful std dev
            price_variance = ((typical_price - vwap_series) ** 2 * ohlcv_df['volume']).cumsum()
            vwap_variance = price_variance / cumulative_volume
            vwap_std = np.sqrt(vwap_variance.iloc[-1]) if not np.isnan(vwap_variance.iloc[-1]) else 0.0
        else:
            vwap_std = 0.0
        
        return {
            "vwap": float(current_vwap) if not np.isnan(current_vwap) else 0.0,
            "upper_band_2sd": float(current_vwap + (2 * vwap_std)) if not np.isnan(current_vwap) else 0.0,
            "lower_band_2sd": float(current_vwap - (2 * vwap_std)) if not np.isnan(current_vwap) else 0.0
        }
        
    except Exception as e:
        logger.error(f"Error calculating VWAP: {e}")
        return {"vwap": 0.0, "upper_band_2sd": 0.0, "lower_band_2sd": 0.0}

def analyze_order_book(order_book_dict: Dict[str, List[Dict]]) -> Dict[str, float]:
    """
    Analyze Level-2 order book data to calculate order flow imbalance.
    
    Args:
        order_book_dict: Dictionary with 'bids' and 'asks' lists
                        Each containing dicts with 'price' and 'quantity'
    
    Returns:
        Dictionary with order flow imbalance percentage
    """
    try:
        bids = order_book_dict.get('bids', [])
        asks = order_book_dict.get('asks', [])
        
        if not bids or not asks:
            return {"order_flow_imbalance_pct": 0.0}
        
        # Calculate total bid and ask volumes (top 5 levels)
        total_bid_volume = sum(level.get('quantity', 0) for level in bids[:5])
        total_ask_volume = sum(level.get('quantity', 0) for level in asks[:5])
        
        total_volume = total_bid_volume + total_ask_volume
        
        if total_volume == 0:
            return {"order_flow_imbalance_pct": 0.0}
        
        # Calculate imbalance: positive = more bids, negative = more asks
        imbalance_pct = ((total_bid_volume - total_ask_volume) / total_volume) * 100
        
        return {"order_flow_imbalance_pct": float(imbalance_pct)}
        
    except Exception as e:
        logger.error(f"Error analyzing order book: {e}")
        return {"order_flow_imbalance_pct": 0.0}

def calculate_cumulative_delta(
    last_price: float, 
    current_trade: Dict[str, Any], 
    last_tick_direction: int, 
    cumulative_delta: float
) -> Tuple[float, int]:
    """
    Calculate cumulative delta using uptick/downtick rule (tick test).
    Since Upstox doesn't provide aggressor side, we estimate it.
    
    Args:
        last_price: Previous trade price
        current_trade: Dict with 'price' and 'quantity'
        last_tick_direction: Previous tick direction (1, -1, or 0)
        cumulative_delta: Running cumulative delta
    
    Returns:
        Tuple of (new_cumulative_delta, new_tick_direction)
    """
    try:
        current_price = current_trade.get('price', 0)
        current_quantity = current_trade.get('quantity', 0)
        
        if current_price == 0 or current_quantity == 0:
            return cumulative_delta, last_tick_direction
        
        # Determine tick direction using tick test
        if current_price > last_price:
            tick_direction = 1  # Uptick (likely buyer initiated)
        elif current_price < last_price:
            tick_direction = -1  # Downtick (likely seller initiated)
        else:
            # Zero tick - use last direction (zero tick rule)
            tick_direction = last_tick_direction
        
        # Update cumulative delta
        # Positive delta = net buying pressure, Negative = net selling pressure
        delta_change = tick_direction * current_quantity
        new_cumulative_delta = cumulative_delta + delta_change
        
        return float(new_cumulative_delta), tick_direction
        
    except Exception as e:
        logger.error(f"Error calculating cumulative delta: {e}")
        return cumulative_delta, last_tick_direction

def calculate_price_momentum(ohlcv_df: pd.DataFrame, periods: List[int] = [5, 10, 20]) -> Dict[str, float]:
    """
    Calculate price momentum over different periods.
    
    Args:
        ohlcv_df: DataFrame with OHLCV data
        periods: List of periods to calculate momentum for
    
    Returns:
        Dictionary with momentum values for each period
    """
    try:
        if ohlcv_df.empty:
            return {f"momentum_{p}": 0.0 for p in periods}
        
        close_prices = ohlcv_df['close']
        current_price = close_prices.iloc[-1]
        
        momentum_dict = {}
        
        for period in periods:
            if len(close_prices) >= period + 1:
                past_price = close_prices.iloc[-(period + 1)]
                if past_price != 0:
                    momentum = ((current_price - past_price) / past_price) * 100
                    momentum_dict[f"momentum_{period}"] = float(momentum)
                else:
                    momentum_dict[f"momentum_{period}"] = 0.0
            else:
                momentum_dict[f"momentum_{period}"] = 0.0
        
        return momentum_dict
        
    except Exception as e:
        logger.error(f"Error calculating price momentum: {e}")
        return {f"momentum_{p}": 0.0 for p in periods}

def calculate_volatility_metrics(ohlcv_df: pd.DataFrame, period: int = 20) -> Dict[str, float]:
    """
    Calculate various volatility metrics.
    
    Args:
        ohlcv_df: DataFrame with OHLCV data
        period: Period for calculations
    
    Returns:
        Dictionary with volatility metrics
    """
    try:
        if len(ohlcv_df) < period:
            return {
                "historical_volatility": 0.0,
                "true_range_avg": 0.0,
                "price_efficiency": 0.0
            }
        
        # Calculate returns
        returns = ohlcv_df['close'].pct_change().dropna()
        
        # Historical volatility (annualized)
        if len(returns) >= 2:
            hist_vol = returns.std() * np.sqrt(252 * 24 * 60)  # Assuming 1-minute data
        else:
            hist_vol = 0.0
        
        # Average True Range
        high_low = ohlcv_df['high'] - ohlcv_df['low']
        high_close = np.abs(ohlcv_df['high'] - ohlcv_df['close'].shift(1))
        low_close = np.abs(ohlcv_df['low'] - ohlcv_df['close'].shift(1))
        
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        atr = true_range.rolling(window=min(period, len(true_range))).mean().iloc[-1]
        
        # Price efficiency (how much price moved vs total range)
        if len(ohlcv_df) >= 2:
            net_change = abs(ohlcv_df['close'].iloc[-1] - ohlcv_df['close'].iloc[0])
            total_range = sum(ohlcv_df['high'] - ohlcv_df['low'])
            efficiency = (net_change / total_range) if total_range > 0 else 0.0
        else:
            efficiency = 0.0
        
        return {
            "historical_volatility": float(hist_vol) if not np.isnan(hist_vol) else 0.0,
            "true_range_avg": float(atr) if not np.isnan(atr) else 0.0,
            "price_efficiency": float(efficiency) if not np.isnan(efficiency) else 0.0
        }
        
    except Exception as e:
        logger.error(f"Error calculating volatility metrics: {e}")
        return {
            "historical_volatility": 0.0,
            "true_range_avg": 0.0,
            "price_efficiency": 0.0
        }