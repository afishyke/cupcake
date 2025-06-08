#!/usr/bin/env python3
"""
High-Performance Dashboard Data Pusher
Ultra-fast real-time data streaming to dashboard with reliability and error handling
"""

import asyncio
import aiohttp
import json
import logging
import time
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
import queue
import numpy as np
import pandas as pd
from collections import deque, defaultdict
import websockets
import gzip
import pickle
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dashboard_pusher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DashboardMetrics:
    """Structured metrics for dashboard"""
    timestamp: str
    symbol: str
    price: float
    volume: int
    change_percent: float
    rsi: Optional[float] = None
    macd: Optional[float] = None
    vwap: Optional[float] = None
    order_flow_imbalance: Optional[float] = None
    signals: Optional[List[str]] = None
    risk_metrics: Optional[Dict] = None

@dataclass
class SystemHealth:
    """System health metrics"""
    timestamp: str
    cpu_usage: float
    memory_usage: float
    active_symbols: int
    data_latency_ms: float
    websocket_status: str
    processed_ticks: int
    error_count: int

class HighPerformanceDashboardPusher:
    """Ultra-fast dashboard data pusher with WebSocket and HTTP fallback"""
    
    def __init__(self, 
                 dashboard_url: str = "ws://localhost:8000/ws",
                 http_fallback_url: str = "http://localhost:8000/api/v1/data",
                 batch_size: int = 50,
                 max_queue_size: int = 10000,
                 push_interval: float = 0.1):  # 100ms for ultra-fast updates
        
        self.dashboard_url = dashboard_url
        self.http_fallback_url = http_fallback_url
        self.batch_size = batch_size
        self.max_queue_size = max_queue_size
        self.push_interval = push_interval
        
        # High-performance queues
        self.metrics_queue = queue.Queue(maxsize=max_queue_size)
        self.health_queue = queue.Queue(maxsize=1000)
        
        # Connection management
        self.websocket = None
        self.http_session = None
        self.is_connected = False
        self.connection_retries = 0
        self.max_retries = 5
        
        # Performance tracking
        self.stats = {
            'total_sent': 0,
            'successful_sends': 0,
            'failed_sends': 0,
            'avg_latency_ms': 0.0,
            'connection_drops': 0,
            'last_send_time': None,
            'throughput_per_sec': 0.0
        }
        
        # Data compression and batching
        self.enable_compression = True
        self.batch_buffer = deque(maxlen=batch_size * 2)
        self.symbol_cache = {}  # Cache for reducing redundant data
        
        # Threading
        self.pusher_thread = None
        self.health_thread = None
        self.running = False
        self.lock = threading.RLock()
        
        logger.info(f"Dashboard Pusher initialized - URL: {dashboard_url}")
    
    async def _create_websocket_connection(self) -> bool:
        """Create WebSocket connection with retry logic"""
        try:
            self.websocket = await websockets.connect(
                self.dashboard_url,
                ping_interval=30,
                ping_timeout=10,
                max_size=2**20,  # 1MB max message size
                compression='deflate' if self.enable_compression else None
            )
            self.is_connected = True
            self.connection_retries = 0
            logger.info("WebSocket connection established")
            return True
            
        except Exception as e:
            self.connection_retries += 1
            logger.error(f"WebSocket connection failed (attempt {self.connection_retries}): {str(e)}")
            if self.connection_retries < self.max_retries:
                await asyncio.sleep(min(2 ** self.connection_retries, 30))  # Exponential backoff
                return await self._create_websocket_connection()
            return False
    
    async def _create_http_session(self) -> bool:
        """Create HTTP session for fallback"""
        try:
            timeout = aiohttp.ClientTimeout(total=5, connect=2)
            self.http_session = aiohttp.ClientSession(
                timeout=timeout,
                connector=aiohttp.TCPConnector(
                    limit=100,
                    limit_per_host=20,
                    keepalive_timeout=30
                )
            )
            logger.info("HTTP fallback session created")
            return True
            
        except Exception as e:
            logger.error(f"HTTP session creation failed: {str(e)}")
            return False
    
    def _compress_data(self, data: Any) -> bytes:
        """Compress data for efficient transmission"""
        if not self.enable_compression:
            return json.dumps(data).encode()
        
        try:
            json_data = json.dumps(data, separators=(',', ':'))  # Compact JSON
            compressed = gzip.compress(json_data.encode())
            return compressed
        except Exception as e:
            logger.warning(f"Compression failed: {str(e)}")
            return json.dumps(data).encode()
    
    def _optimize_metrics_data(self, metrics: DashboardMetrics) -> Dict:
        """Optimize metrics data to reduce size and improve speed"""
        # Round floating point numbers to reduce size
        optimized = asdict(metrics)
        
        for key, value in optimized.items():
            if isinstance(value, float):
                if key in ['price', 'vwap']:
                    optimized[key] = round(value, 4)  # 4 decimal places for prices
                elif key in ['change_percent', 'rsi', 'macd']:
                    optimized[key] = round(value, 2)  # 2 decimal places for percentages
                else:
                    optimized[key] = round(value, 6)
        
        # Remove None values to reduce payload size
        optimized = {k: v for k, v in optimized.items() if v is not None}
        
        return optimized
    
    def _should_send_update(self, symbol: str, metrics: DashboardMetrics) -> bool:
        """Intelligent filtering to avoid sending redundant updates"""
        cache_key = symbol
        
        if cache_key not in self.symbol_cache:
            self.symbol_cache[cache_key] = metrics
            return True
        
        cached = self.symbol_cache[cache_key]
        
        # Send if significant price change (>0.01%)
        price_change = abs((metrics.price - cached.price) / cached.price * 100) if cached.price > 0 else 100
        
        # Send if volume change is significant
        volume_change = abs(metrics.volume - cached.volume) / max(cached.volume, 1) * 100
        
        # Send if technical indicators changed significantly
        rsi_change = abs((metrics.rsi or 0) - (cached.rsi or 0)) > 0.5
        
        # Always send if there are new signals
        has_new_signals = bool(metrics.signals) and metrics.signals != cached.signals
        
        should_send = (
            price_change > 0.01 or 
            volume_change > 1.0 or 
            rsi_change or 
            has_new_signals or
            (time.time() - time.mktime(time.strptime(cached.timestamp, "%Y-%m-%d %H:%M:%S.%f"))) > 5  # Force update every 5s
        )
        
        if should_send:
            self.symbol_cache[cache_key] = metrics
        
        return should_send
    
    async def _send_websocket_batch(self, batch_data: List[Dict]) -> bool:
        """Send batch data via WebSocket"""
        try:
            if not self.websocket or self.websocket.closed:
                if not await self._create_websocket_connection():
                    return False
            
            # Prepare batch message
            message = {
                'type': 'batch_update',
                'timestamp': datetime.now().isoformat(),
                'data': batch_data,
                'count': len(batch_data)
            }
            
            # Compress and send
            compressed_data = self._compress_data(message)
            
            start_time = time.time()
            await self.websocket.send(compressed_data)
            
            # Update latency stats
            latency_ms = (time.time() - start_time) * 1000
            self.stats['avg_latency_ms'] = (
                (self.stats['avg_latency_ms'] * self.stats['successful_sends'] + latency_ms) /
                (self.stats['successful_sends'] + 1)
            )
            
            self.stats['successful_sends'] += 1
            return True
            
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            self.is_connected = False
            self.stats['connection_drops'] += 1
            return False
            
        except Exception as e:
            logger.error(f"WebSocket send failed: {str(e)}")
            self.stats['failed_sends'] += 1
            return False
    
    async def _send_http_fallback(self, batch_data: List[Dict]) -> bool:
        """Fallback HTTP sending when WebSocket fails"""
        try:
            if not self.http_session:
                if not await self._create_http_session():
                    return False
            
            payload = {
                'batch_data': batch_data,
                'timestamp': datetime.now().isoformat()
            }
            
            start_time = time.time()
            async with self.http_session.post(
                self.http_fallback_url,
                json=payload,
                headers={'Content-Type': 'application/json'}
            ) as response:
                
                if response.status == 200:
                    latency_ms = (time.time() - start_time) * 1000
                    self.stats['avg_latency_ms'] = (
                        (self.stats['avg_latency_ms'] * self.stats['successful_sends'] + latency_ms) /
                        (self.stats['successful_sends'] + 1)
                    )
                    self.stats['successful_sends'] += 1
                    return True
                else:
                    logger.warning(f"HTTP fallback failed with status: {response.status}")
                    self.stats['failed_sends'] += 1
                    return False
                    
        except Exception as e:
            logger.error(f"HTTP fallback failed: {str(e)}")
            self.stats['failed_sends'] += 1
            return False
    
    async def _process_batch(self, batch_data: List[Dict]) -> bool:
        """Process and send a batch of data"""
        if not batch_data:
            return True
        
        # Try WebSocket first
        success = await self._send_websocket_batch(batch_data)
        
        # Fallback to HTTP if WebSocket fails
        if not success:
            logger.info("Falling back to HTTP")
            success = await self._send_http_fallback(batch_data)
        
        self.stats['total_sent'] += len(batch_data)
        self.stats['last_send_time'] = time.time()
        
        return success
    
    async def _pusher_loop(self):
        """Main pusher loop - ultra-fast data processing"""
        logger.info("Dashboard pusher loop started")
        
        while self.running:
            try:
                batch_data = []
                start_time = time.time()
                
                # Collect batch data with timeout
                while len(batch_data) < self.batch_size and (time.time() - start_time) < self.push_interval:
                    try:
                        metrics = self.metrics_queue.get(timeout=0.01)  # 10ms timeout
                        
                        # Apply intelligent filtering
                        if self._should_send_update(metrics.symbol, metrics):
                            optimized_data = self._optimize_metrics_data(metrics)
                            batch_data.append(optimized_data)
                        
                        self.metrics_queue.task_done()
                        
                    except queue.Empty:
                        break
                
                # Send batch if we have data
                if batch_data:
                    await self._process_batch(batch_data)
                
                # Calculate and update throughput
                elapsed = time.time() - start_time
                if elapsed > 0:
                    self.stats['throughput_per_sec'] = len(batch_data) / elapsed
                
                # Small sleep to prevent CPU spinning
                if not batch_data:
                    await asyncio.sleep(0.001)  # 1ms sleep
                    
            except Exception as e:
                logger.error(f"Pusher loop error: {str(e)}")
                await asyncio.sleep(0.1)
    
    def _health_monitor_loop(self):
        """Health monitoring loop"""
        while self.running:
            try:
                if not self.health_queue.empty():
                    health_data = self.health_queue.get_nowait()
                    
                    # Send health data (non-blocking)
                    try:
                        asyncio.run_coroutine_threadsafe(
                            self._send_health_data(health_data),
                            asyncio.get_event_loop()
                        )
                    except Exception as e:
                        logger.warning(f"Health data send failed: {str(e)}")
                
                time.sleep(1.0)  # Health updates every second
                
            except Exception as e:
                logger.error(f"Health monitor error: {str(e)}")
                time.sleep(1.0)
    
    async def _send_health_data(self, health_data: SystemHealth):
        """Send system health data"""
        try:
            message = {
                'type': 'health_update',
                'data': asdict(health_data)
            }
            
            if self.websocket and not self.websocket.closed:
                compressed_data = self._compress_data(message)
                await self.websocket.send(compressed_data)
            
        except Exception as e:
            logger.warning(f"Health data transmission failed: {str(e)}")
    
    def start(self):
        """Start the dashboard pusher"""
        if self.running:
            logger.warning("Dashboard pusher already running")
            return
        
        self.running = True
        
        # Start async pusher loop
        loop = asyncio.new_event_loop()
        
        def run_pusher():
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._pusher_loop())
        
        self.pusher_thread = threading.Thread(target=run_pusher, daemon=True)
        self.pusher_thread.start()
        
        # Start health monitor
        self.health_thread = threading.Thread(target=self._health_monitor_loop, daemon=True)
        self.health_thread.start()
        
        logger.info("Dashboard pusher started successfully")
    
    def stop(self):
        """Stop the dashboard pusher"""
        self.running = False
        
        # Close connections
        if self.websocket:
            asyncio.create_task(self.websocket.close())
        
        if self.http_session:
            asyncio.create_task(self.http_session.close())
        
        # Wait for threads to finish
        if self.pusher_thread and self.pusher_thread.is_alive():
            self.pusher_thread.join(timeout=5)
        
        if self.health_thread and self.health_thread.is_alive():
            self.health_thread.join(timeout=2)
        
        logger.info("Dashboard pusher stopped")
    
    def push_metrics(self, 
                    symbol: str,
                    price: float,
                    volume: int,
                    change_percent: float,
                    indicators: Dict = None,
                    signals: List[str] = None,
                    risk_metrics: Dict = None) -> bool:
        """
        Push market metrics to dashboard
        Ultra-fast non-blocking operation
        """
        try:
            metrics = DashboardMetrics(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                symbol=symbol,
                price=price,
                volume=volume,
                change_percent=change_percent,
                rsi=indicators.get('RSI') if indicators else None,
                macd=indicators.get('MACD') if indicators else None,
                vwap=indicators.get('VWAP') if indicators else None,
                order_flow_imbalance=indicators.get('order_flow_imbalance') if indicators else None,
                signals=signals or [],
                risk_metrics=risk_metrics
            )
            
            # Non-blocking queue put
            try:
                self.metrics_queue.put_nowait(metrics)
                return True
            except queue.Full:
                logger.warning(f"Metrics queue full for {symbol}, dropping oldest data")
                # Drop oldest and add new
                try:
                    self.metrics_queue.get_nowait()
                    self.metrics_queue.put_nowait(metrics)
                    return True
                except queue.Empty:
                    pass
                return False
                
        except Exception as e:
            logger.error(f"Push metrics failed for {symbol}: {str(e)}")
            return False
    
    def push_health_metrics(self,
                           cpu_usage: float,
                           memory_usage: float,
                           active_symbols: int,
                           data_latency_ms: float,
                           websocket_status: str,
                           processed_ticks: int,
                           error_count: int) -> bool:
        """Push system health metrics"""
        try:
            health = SystemHealth(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                active_symbols=active_symbols,
                data_latency_ms=data_latency_ms,
                websocket_status=websocket_status,
                processed_ticks=processed_ticks,
                error_count=error_count
            )
            
            try:
                self.health_queue.put_nowait(health)
                return True
            except queue.Full:
                # Replace oldest health data
                try:
                    self.health_queue.get_nowait()
                    self.health_queue.put_nowait(health)
                    return True
                except queue.Empty:
                    pass
                return False
                
        except Exception as e:
            logger.error(f"Push health metrics failed: {str(e)}")
            return False
    
    def push_bulk_data(self, 
                      bulk_metrics: List[Dict[str, Any]],
                      priority: bool = False) -> bool:
        """
        Push bulk market data efficiently
        
        Args:
            bulk_metrics: List of metric dictionaries
            priority: If True, try to process immediately
        """
        try:
            success_count = 0
            
            for metric_data in bulk_metrics:
                success = self.push_metrics(
                    symbol=metric_data['symbol'],
                    price=metric_data['price'],
                    volume=metric_data['volume'],
                    change_percent=metric_data['change_percent'],
                    indicators=metric_data.get('indicators'),
                    signals=metric_data.get('signals'),
                    risk_metrics=metric_data.get('risk_metrics')
                )
                if success:
                    success_count += 1
            
            success_rate = success_count / len(bulk_metrics) * 100 if bulk_metrics else 100
            
            if success_rate < 80:  # Log if success rate is low
                logger.warning(f"Bulk push success rate: {success_rate:.1f}%")
            
            return success_rate > 50  # Consider successful if >50% pushed
            
        except Exception as e:
            logger.error(f"Bulk push failed: {str(e)}")
            return False
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        queue_utilization = (
            (self.metrics_queue.qsize() / self.max_queue_size * 100) 
            if self.max_queue_size > 0 else 0
        )
        
        success_rate = (
            (self.stats['successful_sends'] / max(self.stats['total_sent'], 1) * 100)
            if self.stats['total_sent'] > 0 else 0
        )
        
        return {
            'connection_status': 'connected' if self.is_connected else 'disconnected',
            'total_sent': self.stats['total_sent'],
            'successful_sends': self.stats['successful_sends'],
            'failed_sends': self.stats['failed_sends'],
            'success_rate_percent': round(success_rate, 2),
            'avg_latency_ms': round(self.stats['avg_latency_ms'], 2),
            'throughput_per_sec': round(self.stats['throughput_per_sec'], 2),
            'connection_drops': self.stats['connection_drops'],
            'queue_size': self.metrics_queue.qsize(),
            'queue_utilization_percent': round(queue_utilization, 2),
            'health_queue_size': self.health_queue.qsize(),
            'cached_symbols': len(self.symbol_cache),
            'last_send_time': self.stats['last_send_time'],
            'uptime_seconds': time.time() - (self.stats.get('start_time', time.time())),
            'compression_enabled': self.enable_compression
        }
    
    def reset_stats(self):
        """Reset performance statistics"""
        with self.lock:
            self.stats = {
                'total_sent': 0,
                'successful_sends': 0,
                'failed_sends': 0,
                'avg_latency_ms': 0.0,
                'connection_drops': 0,
                'last_send_time': None,
                'throughput_per_sec': 0.0,
                'start_time': time.time()
            }
            logger.info("Performance statistics reset")
    
    def clear_cache(self):
        """Clear symbol cache to force fresh updates"""
        with self.lock:
            self.symbol_cache.clear()
            logger.info("Symbol cache cleared")
    
    def set_compression(self, enabled: bool):
        """Enable/disable data compression"""
        self.enable_compression = enabled
        logger.info(f"Data compression {'enabled' if enabled else 'disabled'}")
    
    def adjust_push_interval(self, interval_seconds: float):
        """Dynamically adjust push interval for performance tuning"""
        if 0.01 <= interval_seconds <= 5.0:  # Between 10ms and 5s
            self.push_interval = interval_seconds
            logger.info(f"Push interval adjusted to {interval_seconds}s")
        else:
            logger.warning(f"Invalid push interval: {interval_seconds}s")


# Global dashboard pusher instance
dashboard_pusher = HighPerformanceDashboardPusher()

def start_dashboard_pusher(dashboard_url: str = None, http_fallback_url: str = None):
    """Start the global dashboard pusher"""
    global dashboard_pusher
    
    if dashboard_url:
        dashboard_pusher.dashboard_url = dashboard_url
    if http_fallback_url:
        dashboard_pusher.http_fallback_url = http_fallback_url
    
    dashboard_pusher.start()

def stop_dashboard_pusher():
    """Stop the global dashboard pusher"""
    global dashboard_pusher
    dashboard_pusher.stop()

def push_market_data(symbol: str, 
                    price: float, 
                    volume: int, 
                    change_percent: float,
                    **kwargs) -> bool:
    """Convenience function to push market data"""
    return dashboard_pusher.push_metrics(
        symbol=symbol,
        price=price,
        volume=volume,
        change_percent=change_percent,
        **kwargs
    )

def push_system_health(cpu_usage: float,
                      memory_usage: float,
                      active_symbols: int,
                      **kwargs) -> bool:
    """Convenience function to push system health"""
    return dashboard_pusher.push_health_metrics(
        cpu_usage=cpu_usage,
        memory_usage=memory_usage,
        active_symbols=active_symbols,
        data_latency_ms=kwargs.get('data_latency_ms', 0),
        websocket_status=kwargs.get('websocket_status', 'unknown'),
        processed_ticks=kwargs.get('processed_ticks', 0),
        error_count=kwargs.get('error_count', 0)
    )

def get_dashboard_stats() -> Dict[str, Any]:
    """Get dashboard pusher performance statistics"""
    return dashboard_pusher.get_performance_stats()

if __name__ == "__main__":
    # Test the module
    import psutil
    
    logger.info("Dashboard Pusher module loaded successfully")
    
    # Start pusher for testing
    start_dashboard_pusher()
    
    # Test data push
    test_success = push_market_data(
        symbol="RELIANCE",
        price=2456.75,
        volume=1000000,
        change_percent=1.25,
        indicators={'RSI': 65.5, 'MACD': 12.3},
        signals=['BUY_SIGNAL']
    )
    
    # Test health push
    health_success = push_system_health(
        cpu_usage=psutil.cpu_percent(),
        memory_usage=psutil.virtual_memory().percent,
        active_symbols=5,
        data_latency_ms=15.5,
        websocket_status='connected',
        processed_ticks=1500,
        error_count=0
    )
    
    print("Dashboard Pusher Test Results:")
    print(f"Market data push: {'SUCCESS' if test_success else 'FAILED'}")
    print(f"Health data push: {'SUCCESS' if health_success else 'FAILED'}")
    print("Performance stats:", get_dashboard_stats())
    
    # Keep running for a few seconds to test
    time.sleep(3)
    
    stop_dashboard_pusher()