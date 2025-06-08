#!/usr/bin/env python3
"""
High-Performance CSV Output Module
Handles ultra-fast CSV export with memory optimization and data integrity
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor
import psutil
from pathlib import Path
import gc
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/csv_output.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HighPerformanceCSVExporter:
    """Ultra-fast CSV exporter with memory optimization and parallel processing"""
    
    def __init__(self, output_dir: str = "data/output", max_workers: int = 4):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self.lock = threading.RLock()
        self.buffer_size = 10000  # Rows to buffer before write
        self.memory_threshold = 85  # Memory usage threshold (%)
        
        # Performance tracking
        self.stats = {
            'total_exports': 0,
            'total_rows': 0,
            'avg_export_time': 0.0,
            'memory_optimizations': 0
        }
        
        logger.info(f"CSV Exporter initialized - Output: {self.output_dir}, Workers: {max_workers}")
    
    def _optimize_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame memory usage"""
        start_memory = df.memory_usage(deep=True).sum()
        
        # Optimize numeric columns
        for col in df.select_dtypes(include=[np.number]).columns:
            if df[col].dtype == 'int64':
                if df[col].min() >= -128 and df[col].max() <= 127:
                    df[col] = df[col].astype('int8')
                elif df[col].min() >= -32768 and df[col].max() <= 32767:
                    df[col] = df[col].astype('int16')
                elif df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                    df[col] = df[col].astype('int32')
            elif df[col].dtype == 'float64':
                df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Optimize object columns
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:  # If less than 50% unique values
                df[col] = df[col].astype('category')
        
        end_memory = df.memory_usage(deep=True).sum()
        reduction = (start_memory - end_memory) / start_memory * 100
        
        if reduction > 10:  # Only log significant reductions
            logger.debug(f"Memory optimized: {reduction:.1f}% reduction")
            self.stats['memory_optimizations'] += 1
        
        return df
    
    def _check_memory_usage(self) -> bool:
        """Check if memory usage is below threshold"""
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > self.memory_threshold:
            logger.warning(f"High memory usage: {memory_percent:.1f}%")
            gc.collect()  # Force garbage collection
            return False
        return True
    
    def _prepare_ohlcv_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare and validate OHLCV data columns"""
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
        # Ensure required columns exist
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                raise ValueError(f"Missing required column: {col}")
        
        # Convert timestamp to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Validate OHLC relationships
        invalid_ohlc = (df['high'] < df['low']) | (df['high'] < df['open']) | \
                      (df['high'] < df['close']) | (df['low'] > df['open']) | \
                      (df['low'] > df['close'])
        
        if invalid_ohlc.any():
            logger.warning(f"Found {invalid_ohlc.sum()} invalid OHLC relationships")
            # Fix invalid relationships
            df.loc[invalid_ohlc, 'high'] = df.loc[invalid_ohlc, ['open', 'close']].max(axis=1)
            df.loc[invalid_ohlc, 'low'] = df.loc[invalid_ohlc, ['open', 'close']].min(axis=1)
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df
    
    def _add_technical_indicators(self, df: pd.DataFrame, indicators_data: Dict) -> pd.DataFrame:
        """Add technical indicators to DataFrame with validation"""
        if not indicators_data:
            return df
        
        # Add TA-Lib indicators
        for indicator_name, values in indicators_data.items():
            if isinstance(values, (list, np.ndarray)):
                # Ensure length matches
                if len(values) == len(df):
                    df[f'ta_{indicator_name.lower()}'] = values
                else:
                    logger.warning(f"Indicator {indicator_name} length mismatch: {len(values)} vs {len(df)}")
                    # Pad or truncate to match
                    if len(values) < len(df):
                        padded_values = np.full(len(df), np.nan)
                        padded_values[-len(values):] = values
                        df[f'ta_{indicator_name.lower()}'] = padded_values
                    else:
                        df[f'ta_{indicator_name.lower()}'] = values[:len(df)]
        
        return df
    
    def _add_advanced_indicators(self, df: pd.DataFrame, advanced_data: Dict) -> pd.DataFrame:
        """Add advanced indicators (VWAP, order flow, etc.)"""
        if not advanced_data:
            return df
        
        # VWAP bands
        if 'vwap_bands' in advanced_data:
            vwap_data = advanced_data['vwap_bands']
            for key, values in vwap_data.items():
                if isinstance(values, (list, np.ndarray)) and len(values) == len(df):
                    df[f'vwap_{key}'] = values
        
        # Order flow imbalance
        if 'order_flow_imbalance' in advanced_data:
            imbalance_data = advanced_data['order_flow_imbalance']
            if isinstance(imbalance_data, (list, np.ndarray)) and len(imbalance_data) == len(df):
                df['order_flow_imbalance'] = imbalance_data
        
        # Cumulative delta
        if 'cumulative_delta' in advanced_data:
            delta_data = advanced_data['cumulative_delta']
            if isinstance(delta_data, (list, np.ndarray)) and len(delta_data) == len(df):
                df['cumulative_delta'] = delta_data
        
        return df
    
    def export_symbol_data(self, 
                          symbol: str,
                          ohlcv_data: pd.DataFrame,
                          indicators_data: Dict = None,
                          advanced_data: Dict = None,
                          append_mode: bool = False) -> bool:
        """
        Export single symbol data to CSV with all indicators
        Ultra-fast with memory optimization
        """
        start_time = time.time()
        
        try:
            with self.lock:
                # Check memory before processing
                if not self._check_memory_usage():
                    logger.warning("Skipping export due to high memory usage")
                    return False
                
                # Prepare data
                df = ohlcv_data.copy()
                df = self._prepare_ohlcv_columns(df)
                
                # Add technical indicators
                if indicators_data:
                    df = self._add_technical_indicators(df, indicators_data)
                
                # Add advanced indicators
                if advanced_data:
                    df = self._add_advanced_indicators(df, advanced_data)
                
                # Optimize memory
                df = self._optimize_dataframe_memory(df)
                
                # Generate filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{symbol}_{timestamp}.csv" if not append_mode else f"{symbol}_continuous.csv"
                filepath = self.output_dir / filename
                
                # Export with high performance settings
                mode = 'a' if append_mode and filepath.exists() else 'w'
                header = not (append_mode and filepath.exists())
                
                df.to_csv(
                    filepath,
                    mode=mode,
                    header=header,
                    index=False,
                    float_format='%.6f',
                    chunksize=self.buffer_size,
                    compression=None  # No compression for speed
                )
                
                # Update stats
                export_time = time.time() - start_time
                self.stats['total_exports'] += 1
                self.stats['total_rows'] += len(df)
                self.stats['avg_export_time'] = (
                    (self.stats['avg_export_time'] * (self.stats['total_exports'] - 1) + export_time) /
                    self.stats['total_exports']
                )
                
                logger.info(f"Exported {symbol}: {len(df)} rows in {export_time:.3f}s -> {filepath}")
                return True
                
        except Exception as e:
            logger.error(f"Export failed for {symbol}: {str(e)}")
            return False
        finally:
            # Clean up
            gc.collect()
    
    def bulk_export(self, 
                   export_tasks: List[Dict[str, Any]],
                   parallel: bool = True) -> Dict[str, bool]:
        """
        Bulk export multiple symbols in parallel
        
        Args:
            export_tasks: List of dicts with keys: symbol, ohlcv_data, indicators_data, advanced_data
            parallel: Use parallel processing
        
        Returns:
            Dict mapping symbol to success status
        """
        results = {}
        
        if not parallel or len(export_tasks) == 1:
            # Sequential processing
            for task in export_tasks:
                symbol = task['symbol']
                results[symbol] = self.export_symbol_data(
                    symbol=symbol,
                    ohlcv_data=task['ohlcv_data'],
                    indicators_data=task.get('indicators_data'),
                    advanced_data=task.get('advanced_data'),
                    append_mode=task.get('append_mode', False)
                )
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                
                for task in export_tasks:
                    symbol = task['symbol']
                    future = executor.submit(
                        self.export_symbol_data,
                        symbol=symbol,
                        ohlcv_data=task['ohlcv_data'],
                        indicators_data=task.get('indicators_data'),
                        advanced_data=task.get('advanced_data'),
                        append_mode=task.get('append_mode', False)
                    )
                    futures[future] = symbol
                
                # Collect results
                for future in futures:
                    symbol = futures[future]
                    try:
                        results[symbol] = future.result(timeout=30)  # 30s timeout
                    except Exception as e:
                        logger.error(f"Bulk export failed for {symbol}: {str(e)}")
                        results[symbol] = False
        
        success_count = sum(results.values())
        logger.info(f"Bulk export completed: {success_count}/{len(export_tasks)} successful")
        
        return results
    
    def export_realtime_snapshot(self, 
                                live_data: Dict[str, Any],
                                max_age_minutes: int = 60) -> bool:
        """
        Export real-time snapshot for all active symbols
        Optimized for frequent updates
        """
        try:
            current_time = datetime.now()
            cutoff_time = current_time - timedelta(minutes=max_age_minutes)
            
            export_tasks = []
            
            for symbol, data in live_data.items():
                if 'ohlcv_data' in data and not data['ohlcv_data'].empty:
                    # Filter recent data only
                    df = data['ohlcv_data']
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df = df[df['timestamp'] >= cutoff_time]
                    
                    if not df.empty:
                        export_tasks.append({
                            'symbol': symbol,
                            'ohlcv_data': df,
                            'indicators_data': data.get('indicators'),
                            'advanced_data': data.get('advanced'),
                            'append_mode': True
                        })
            
            if export_tasks:
                results = self.bulk_export(export_tasks, parallel=True)
                success_rate = sum(results.values()) / len(results) * 100
                logger.info(f"Realtime snapshot: {success_rate:.1f}% success rate")
                return success_rate > 80  # Consider successful if >80% exports succeed
            
            return True
            
        except Exception as e:
            logger.error(f"Realtime snapshot failed: {str(e)}")
            return False
    
    def cleanup_old_files(self, days_to_keep: int = 7) -> int:
        """Clean up old CSV files to manage disk space"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            deleted_count = 0
            
            for file_path in self.output_dir.glob("*.csv"):
                if file_path.stat().st_mtime < cutoff_date.timestamp():
                    file_path.unlink()
                    deleted_count += 1
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old CSV files")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Cleanup failed: {str(e)}")
            return 0
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        return {
            **self.stats,
            'memory_usage_percent': psutil.virtual_memory().percent,
            'output_directory': str(self.output_dir),
            'file_count': len(list(self.output_dir.glob("*.csv")))
        }


# Global exporter instance
csv_exporter = HighPerformanceCSVExporter()

def export_data(symbol: str, 
               ohlcv_data: pd.DataFrame,
               indicators: Dict = None,
               advanced: Dict = None) -> bool:
    """
    Convenience function for single symbol export
    """
    return csv_exporter.export_symbol_data(
        symbol=symbol,
        ohlcv_data=ohlcv_data,
        indicators_data=indicators,
        advanced_data=advanced
    )

def bulk_export_data(export_tasks: List[Dict]) -> Dict[str, bool]:
    """
    Convenience function for bulk export
    """
    return csv_exporter.bulk_export(export_tasks)

def export_realtime_data(live_data: Dict) -> bool:
    """
    Convenience function for realtime export
    """
    return csv_exporter.export_realtime_snapshot(live_data)

if __name__ == "__main__":
    # Test the module
    logger.info("CSV Output module loaded successfully")
    print("Performance stats:", csv_exporter.get_performance_stats())