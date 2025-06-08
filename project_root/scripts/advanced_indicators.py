#!/usr/bin/env python3
"""
Advanced indicators module for VWAP bands, order flow imbalance, and cumulative delta
Uses optimized numpy operations for ultra-fast calculations
"""
import numpy as np
import pandas as pd
import logging
from typing import Dict, Tuple, Optional
import talib

logger = logging.getLogger(__name__)

class AdvancedIndicators:
    def __init__(self):
        self.vwap_periods = [20, 50, 100]  # Different VWAP calculation periods
        self.vwap_std_multipliers = [1.0, 1.5, 2.0]  # Standard deviation bands
        
    def calculate_vwap_bands(self, ohlcv_df: pd.DataFrame) -> Dict:
        """
        Calculate VWAP with standard deviation bands
        Returns dict with VWAP values and upper/lower bands
        """
        try:
            if len(ohlcv_df) < 20:
                return {}
                
            # Use typical price for VWAP calculation
            typical_price = (ohlcv_df['high'] + ohlcv_df['low'] + ohlcv_df['close']) / 3
            volume = ohlcv_df['volume']
            
            results = {}
            
            for period in self.vwap_periods:
                if len(ohlcv_df) < period:
                    continue
                    
                # Calculate VWAP using rolling window
                pv = (typical_price * volume).rolling(window=period).sum()
                v = volume.rolling(window=period).sum()
                vwap = pv / v
                
                # Calculate variance for standard deviation bands
                squared_diff = ((typical_price - vwap) ** 2 * volume).rolling(window=period).sum()
                variance = squared_diff / v
                std_dev = np.sqrt(variance)
                
                # Create bands with different multipliers
                bands = {}
                for multiplier in self.vwap_std_multipliers:
                    bands[f'upper_{multiplier}'] = vwap + (std_dev * multiplier)
                    bands[f'lower_{multiplier}'] = vwap - (std_dev * multiplier)
                
                results[f'vwap_{period}'] = {
                    'vwap': vwap.iloc[-1] if not pd.isna(vwap.iloc[-1]) else 0,
                    'std_dev': std_dev.iloc[-1] if not pd.isna(std_dev.iloc[-1]) else 0,
                    'bands': {k: v.iloc[-1] if not pd.isna(v.iloc[-1]) else 0 for k, v in bands.items()}
                }
            
            # Add current price position relative to VWAP
            current_price = ohlcv_df['close'].iloc[-1]
            if 'vwap_20' in results:
                vwap_20 = results['vwap_20']['vwap']
                if vwap_20 > 0:
                    results['price_vs_vwap'] = (current_price - vwap_20) / vwap_20 * 100
                else:
                    results['price_vs_vwap'] = 0
                    
            return results
            
        except Exception as e:
            logger.error(f"VWAP calculation failed: {e}")
            return {}
    
    def calculate_order_flow_imbalance(self, order_book_dict: Dict) -> Dict:
        """
        Calculate order flow imbalance from L2 order book data
        Returns imbalance ratios and pressure indicators
        """
        try:
            if not order_book_dict or 'buy' not in order_book_dict or 'sell' not in order_book_dict:
                return {'imbalance_ratio': 0, 'buy_pressure': 0, 'sell_pressure': 0}
            
            buy_orders = order_book_dict.get('buy', [])
            sell_orders = order_book_dict.get('sell', [])
            
            if not buy_orders or not sell_orders:
                return {'imbalance_ratio': 0, 'buy_pressure': 0, 'sell_pressure': 0}
            
            # Calculate total volume and weighted prices for top 5 levels
            buy_volume = sum(float(order.get('quantity', 0)) for order in buy_orders[:5])
            sell_volume = sum(float(order.get('quantity', 0)) for order in sell_orders[:5])
            
            # Calculate weighted average prices
            buy_weighted_price = 0
            sell_weighted_price = 0
            
            if buy_volume > 0:
                buy_weighted_price = sum(
                    float(order.get('price', 0)) * float(order.get('quantity', 0)) 
                    for order in buy_orders[:5]
                ) / buy_volume
            
            if sell_volume > 0:
                sell_weighted_price = sum(
                    float(order.get('price', 0)) * float(order.get('quantity', 0)) 
                    for order in sell_orders[:5]
                ) / sell_volume
            
            # Calculate imbalance ratio
            total_volume = buy_volume + sell_volume
            imbalance_ratio = (buy_volume - sell_volume) / total_volume if total_volume > 0 else 0
            
            # Calculate pressure indicators (0-100 scale)
            buy_pressure = (buy_volume / total_volume * 100) if total_volume > 0 else 0
            sell_pressure = (sell_volume / total_volume * 100) if total_volume > 0 else 0
            
            # Calculate spread
            best_bid = float(buy_orders[0].get('price', 0)) if buy_orders else 0
            best_ask = float(sell_orders[0].get('price', 0)) if sell_orders else 0
            spread = (best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 0
            spread_pct = (spread / best_bid * 100) if best_bid > 0 else 0
            
            return {
                'imbalance_ratio': round(imbalance_ratio, 4),
                'buy_pressure': round(buy_pressure, 2),
                'sell_pressure': round(sell_pressure, 2),
                'buy_volume': int(buy_volume),
                'sell_volume': int(sell_volume),
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': round(spread, 2),
                'spread_pct': round(spread_pct, 4),
                'buy_weighted_price': round(buy_weighted_price, 2),
                'sell_weighted_price': round(sell_weighted_price, 2)
            }
            
        except Exception as e:
            logger.error(f"Order flow calculation failed: {e}")
            return {'imbalance_ratio': 0, 'buy_pressure': 0, 'sell_pressure': 0}
    
    def calculate_cumulative_delta(self, ohlcv_df: pd.DataFrame) -> Tuple[float, Dict]:
        """
        Calculate cumulative delta and tick direction analysis
        Returns cumulative delta value and tick direction statistics
        """
        try:
            if len(ohlcv_df) < 2:
                return 0.0, {}
            
            # Calculate price changes and volume
            close_prices = ohlcv_df['close'].values
            volumes = ohlcv_df['volume'].values
            
            # Calculate tick direction based on price movement
            price_changes = np.diff(close_prices)
            tick_directions = np.zeros_like(price_changes)
            
            # Assign tick directions: 1 for uptick, -1 for downtick, 0 for no change
            tick_directions[price_changes > 0] = 1
            tick_directions[price_changes < 0] = -1
            
            # For no change, use previous direction or look at high/low
            for i in range(len(tick_directions)):
                if tick_directions[i] == 0:
                    if i > 0:
                        # Use previous tick direction
                        tick_directions[i] = tick_directions[i-1]
                    else:
                        # For first tick, compare close to midpoint of high-low
                        midpoint = (ohlcv_df.iloc[i+1]['high'] + ohlcv_df.iloc[i+1]['low']) / 2
                        if close_prices[i+1] > midpoint:
                            tick_directions[i] = 1
                        elif close_prices[i+1] < midpoint:
                            tick_directions[i] = -1
            
            # Calculate delta for each period (volume * tick direction)
            volume_deltas = volumes[1:] * tick_directions
            
            # Calculate cumulative delta
            cumulative_delta = np.cumsum(volume_deltas)
            current_cumulative_delta = cumulative_delta[-1] if len(cumulative_delta) > 0 else 0
            
            # Calculate tick direction statistics
            total_ticks = len(tick_directions)
            up_ticks = np.sum(tick_directions == 1)
            down_ticks = np.sum(tick_directions == -1)
            unchanged_ticks = np.sum(tick_directions == 0)
            
            # Calculate volume-weighted tick statistics
            up_volume = np.sum(volumes[1:][tick_directions == 1])
            down_volume = np.sum(volumes[1:][tick_directions == -1])
            total_volume = up_volume + down_volume
            
            # Recent delta (last 20 periods)
            recent_periods = min(20, len(volume_deltas))
            recent_delta = np.sum(volume_deltas[-recent_periods:]) if recent_periods > 0 else 0
            
            tick_stats = {
                'total_ticks': total_ticks,
                'up_ticks': up_ticks,
                'down_ticks': down_ticks,
                'unchanged_ticks': unchanged_ticks,
                'up_tick_pct': (up_ticks / total_ticks * 100) if total_ticks > 0 else 0,
                'down_tick_pct': (down_ticks / total_ticks * 100) if total_ticks > 0 else 0,
                'up_volume': int(up_volume),
                'down_volume': int(down_volume),
                'volume_ratio': (up_volume / down_volume) if down_volume > 0 else float('inf'),
                'recent_delta': float(recent_delta),
                'delta_momentum': float(recent_delta - (cumulative_delta[-recent_periods] if len(cumulative_delta) >= recent_periods else 0))
            }
            
            return float(current_cumulative_delta), tick_stats
            
        except Exception as e:
            logger.error(f"Cumulative delta calculation failed: {e}")
            return 0.0, {}
    
    def calculate_money_flow_index(self, ohlcv_df: pd.DataFrame, period: int = 14) -> float:
        """
        Calculate Money Flow Index using TA-Lib
        """
        try:
            if len(ohlcv_df) < period + 1:
                return 50.0  # Neutral MFI
            
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            close = ohlcv_df['close'].values
            volume = ohlcv_df['volume'].values.astype(float)
            
            mfi = talib.MFI(high, low, close, volume, timeperiod=period)
            
            return float(mfi[-1]) if not np.isnan(mfi[-1]) else 50.0
            
        except Exception as e:
            logger.error(f"MFI calculation failed: {e}")
            return 50.0
    
    def calculate_chaikin_money_flow(self, ohlcv_df: pd.DataFrame, period: int = 20) -> float:
        """
        Calculate Chaikin Money Flow
        """
        try:
            if len(ohlcv_df) < period:
                return 0.0
            
            high = ohlcv_df['high'].values
            low = ohlcv_df['low'].values
            close = ohlcv_df['close'].values
            volume = ohlcv_df['volume'].values
            
            # Money Flow Multiplier
            mfm = ((close - low) - (high - close)) / (high - low)
            mfm = np.where(high == low, 0, mfm)  # Handle division by zero
            
            # Money Flow Volume
            mfv = mfm * volume
            
            # Chaikin Money Flow
            cmf = np.convolve(mfv, np.ones(period), mode='valid') / np.convolve(volume, np.ones(period), mode='valid')
            
            return float(cmf[-1]) if len(cmf) > 0 and not np.isnan(cmf[-1]) else 0.0
            
        except Exception as e:
            logger.error(f"CMF calculation failed: {e}")
            return 0.0
    
    def calculate_volume_profile(self, ohlcv_df: pd.DataFrame, bins: int = 20) -> Dict:
        """
        Calculate volume profile for price levels
        """
        try:
            if len(ohlcv_df) < 10:
                return {}
            
            # Get price range
            price_high = ohlcv_df['high'].max()
            price_low = ohlcv_df['low'].min()
            
            if price_high == price_low:
                return {}
            
            # Create price bins
            price_bins = np.linspace(price_low, price_high, bins + 1)
            volume_profile = np.zeros(bins)
            
            # Distribute volume across price levels
            for _, row in ohlcv_df.iterrows():
                # Assume uniform distribution of volume across high-low range
                row_bins = np.digitize([row['low'], row['high']], price_bins)
                start_bin = max(0, row_bins[0] - 1)
                end_bin = min(bins, row_bins[1])
                
                # Distribute volume proportionally
                bin_count = end_bin - start_bin
                if bin_count > 0:
                    volume_per_bin = row['volume'] / bin_count
                    volume_profile[start_bin:end_bin] += volume_per_bin
            
            # Find POC (Point of Control) - price level with highest volume
            poc_index = np.argmax(volume_profile)
            poc_price = (price_bins[poc_index] + price_bins[poc_index + 1]) / 2
            
            # Calculate value area (70% of total volume)
            total_volume = np.sum(volume_profile)
            target_volume = total_volume * 0.7
            
            # Find value area high and low
            sorted_indices = np.argsort(volume_profile)[::-1]
            cumulative_volume = 0
            value_area_indices = []
            
            for idx in sorted_indices:
                cumulative_volume += volume_profile[idx]
                value_area_indices.append(idx)
                if cumulative_volume >= target_volume:
                    break
            
            if value_area_indices:
                vah_index = max(value_area_indices)
                val_index = min(value_area_indices)
                vah_price = (price_bins[vah_index] + price_bins[vah_index + 1]) / 2
                val_price = (price_bins[val_index] + price_bins[val_index + 1]) / 2
            else:
                vah_price = price_high
                val_price = price_low
            
            return {
                'poc_price': round(poc_price, 2),
                'vah_price': round(vah_price, 2),
                'val_price': round(val_price, 2),
                'total_volume': int(total_volume),
                'price_bins': price_bins.tolist(),
                'volume_profile': volume_profile.tolist()
            }
            
        except Exception as e:
            logger.error(f"Volume profile calculation failed: {e}")
            return {}
    
    def calculate_all_advanced_indicators(self, ohlcv_df: pd.DataFrame, order_book_dict: Dict = None) -> Dict:
        """
        Calculate all advanced indicators in one call for efficiency
        """
        try:
            results = {}
            
            # VWAP bands
            results['vwap_analysis'] = self.calculate_vwap_bands(ohlcv_df)
            
            # Order flow (if order book available)
            if order_book_dict:
                results['order_flow'] = self.calculate_order_flow_imbalance(order_book_dict)
            else:
                results['order_flow'] = {'imbalance_ratio': 0, 'buy_pressure': 0, 'sell_pressure': 0}
            
            # Cumulative delta
            results['cumulative_delta'], results['tick_stats'] = self.calculate_cumulative_delta(ohlcv_df)
            
            # Money flow indicators
            results['money_flow_index'] = self.calculate_money_flow_index(ohlcv_df)
            results['chaikin_money_flow'] = self.calculate_chaikin_money_flow(ohlcv_df)
            
            # Volume profile
            results['volume_profile'] = self.calculate_volume_profile(ohlcv_df)
            
            return results
            
        except Exception as e:
            logger.error(f"Advanced indicators calculation failed: {e}")
            return {}