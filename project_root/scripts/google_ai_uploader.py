#!/usr/bin/env python3
"""
High-performance AI prediction uploader for Upstox AI endpoints
Optimized for batch processing and minimal latency
"""

import json
import logging
import requests
import time
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue, Empty
import asyncio
import aiohttp

logger = logging.getLogger(__name__)

class GoogleAIUploader:
    """Async AI prediction uploader with batch processing"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.upstox.com"):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Performance settings
        self.batch_size = 50
        self.max_workers = 5
        self.timeout = 10.0
        self.retry_attempts = 3
        self.retry_delay = 1.0
        
        # Stats tracking
        self.upload_count = 0
        self.error_count = 0
        self.total_upload_time = 0
        self.last_upload_time = 0
        
        # Queue for async processing
        self.upload_queue = Queue()
        self.is_running = False
        self.worker_thread = None
        
    def start_async_worker(self) -> None:
        """Start background worker for async uploads"""
        if not self.is_running:
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._upload_worker, daemon=True)
            self.worker_thread.start()
            logger.info("AI uploader worker started")
    
    def stop_async_worker(self) -> None:
        """Stop background worker"""
        self.is_running = False
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5.0)
        logger.info("AI uploader worker stopped")
    
    def _upload_worker(self) -> None:
        """Background worker for processing upload queue"""
        batch = []
        
        while self.is_running:
            try:
                # Collect batch
                try:
                    item = self.upload_queue.get(timeout=1.0)
                    batch.append(item)
                    
                    # Collect more items for batch
                    while len(batch) < self.batch_size:
                        try:
                            item = self.upload_queue.get_nowait()
                            batch.append(item)
                        except Empty:
                            break
                    
                    # Process batch
                    if batch:
                        self._process_batch(batch)
                        batch.clear()
                        
                except Empty:
                    continue  # No items in queue
                    
            except Exception as e:
                logger.error(f"Upload worker error: {e}")
                batch.clear()
    
    def _process_batch(self, batch: List[Dict]) -> None:
        """Process a batch of predictions"""
        try:
            # Group by endpoint if multiple types
            prediction_batch = []
            
            for item in batch:
                if item.get('type') == 'prediction':
                    prediction_batch.append(item['data'])
            
            if prediction_batch:
                self._upload_predictions_sync(prediction_batch)
                
        except Exception as e:
            logger.error(f"Batch processing error: {e}")
    
    def upload_prediction_async(self, prediction_data: Dict) -> None:
        """Add prediction to async upload queue"""
        if not self.is_running:
            self.start_async_worker()
        
        self.upload_queue.put({
            'type': 'prediction',
            'data': prediction_data,
            'timestamp': time.time()
        })
    
    def upload_predictions_sync(self, predictions: List[Dict]) -> Dict[str, Any]:
        """Synchronous batch upload with retries"""
        return self._upload_predictions_sync(predictions)
    
    def _upload_predictions_sync(self, predictions: List[Dict]) -> Dict[str, Any]:
        """Internal sync upload method"""
        start_time = time.time()
        
        if not predictions:
            return {"status": "success", "message": "No predictions to upload"}
        
        # Prepare payload
        payload = {
            "instances": self._format_predictions(predictions),
            "timestamp": int(time.time()),
            "version": "v3"
        }
        
        # Upload with retries
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                response = self._make_request(payload)
                
                if response.get('status') == 'success':
                    # Update stats
                    upload_time = (time.time() - start_time) * 1000
                    self.upload_count += len(predictions)
                    self.total_upload_time += upload_time
                    self.last_upload_time = time.time()
                    
                    return {
                        "status": "success",
                        "uploaded_count": len(predictions),
                        "upload_time_ms": upload_time,
                        "response": response
                    }
                else:
                    last_error = response.get('message', 'Unknown error')
                    
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
        
        # All attempts failed
        self.error_count += 1
        logger.error(f"Failed to upload after {self.retry_attempts} attempts: {last_error}")
        
        return {
            "status": "error",
            "message": last_error,
            "uploaded_count": 0,
            "upload_time_ms": (time.time() - start_time) * 1000
        }
    
    def _make_request(self, payload: Dict) -> Dict:
        """Make HTTP request to AI endpoint"""
        url = f"{self.base_url}/v3/ai/predict"
        
        response = self.session.post(
            url,
            json=payload,
            timeout=self.timeout
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:  # Rate limited
            raise Exception(f"Rate limited: {response.text}")
        else:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
    
    def _format_predictions(self, predictions: List[Dict]) -> List[Dict]:
        """Format predictions for AI endpoint"""
        formatted = []
        
        for pred in predictions:
            try:
                # Extract key fields
                formatted_pred = {
                    "instrument_key": pred.get('instrument_key', ''),
                    "timestamp": pred.get('timestamp', time.time()),
                    "signal": pred.get('signal', {}),
                    "technical_score": pred.get('technical_score', 0.0),
                    "ml_prediction": pred.get('ml_prediction', 0.0),
                    "ml_confidence": pred.get('ml_confidence', 0.0),
                    "features": self._extract_key_features(pred.get('features', {})),
                    "risk_metrics": self._extract_risk_metrics(pred.get('risk_metrics', {}))
                }
                
                formatted.append(formatted_pred)
                
            except Exception as e:
                logger.warning(f"Failed to format prediction: {e}")
                continue
        
        return formatted
    
    def _extract_key_features(self, features: Dict) -> Dict:
        """Extract key features for upload"""
        key_features = [
            'current_price', 'price_change', 'volume_ratio',
            'rsi', 'macd_signal', 'sma_20', 'sma_50', 'atr'
        ]
        
        return {
            key: features.get(key, 0.0) 
            for key in key_features 
            if key in features
        }
    
    def _extract_risk_metrics(self, risk_metrics: Dict) -> Dict:
        """Extract key risk metrics for upload"""
        key_metrics = [
            'pre_set_stop_level', 'trailing_stop', 'position_size',
            'max_loss', 'profit_target', 'current_pnl'
        ]
        
        return {
            key: risk_metrics.get(key, 0.0) 
            for key in key_metrics 
            if key in risk_metrics
        }
    
    async def upload_predictions_async_batch(self, predictions: List[Dict]) -> Dict[str, Any]:
        """Async batch upload using aiohttp"""
        start_time = time.time()
        
        if not predictions:
            return {"status": "success", "message": "No predictions to upload"}
        
        payload = {
            "instances": self._format_predictions(predictions),
            "timestamp": int(time.time()),
            "version": "v3"
        }
        
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                }
            ) as session:
                
                url = f"{self.base_url}/v3/ai/predict"
                
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        upload_time = (time.time() - start_time) * 1000
                        self.upload_count += len(predictions)
                        self.total_upload_time += upload_time
                        self.last_upload_time = time.time()
                        
                        return {
                            "status": "success",
                            "uploaded_count": len(predictions),
                            "upload_time_ms": upload_time,
                            "response": result
                        }
                    else:
                        error_text = await response.text()
                        raise Exception(f"HTTP {response.status}: {error_text}")
                        
        except Exception as e:
            self.error_count += 1
            logger.error(f"Async upload failed: {e}")
            
            return {
                "status": "error",
                "message": str(e),
                "uploaded_count": 0,
                "upload_time_ms": (time.time() - start_time) * 1000
            }
    
    def upload_multiple_concurrent(self, prediction_batches: List[List[Dict]]) -> List[Dict]:
        """Upload multiple batches concurrently"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self._upload_predictions_sync, batch): batch 
                for batch in prediction_batches
            }
            
            for future in as_completed(future_to_batch):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Concurrent upload error: {e}")
                    results.append({
                        "status": "error",
                        "message": str(e),
                        "uploaded_count": 0
                    })
        
        return results
    
    def get_upload_stats(self) -> Dict:
        """Get upload statistics"""
        avg_upload_time = (
            self.total_upload_time / max(self.upload_count, 1)
        )
        
        return {
            "total_uploads": self.upload_count,
            "error_count": self.error_count,
            "success_rate": (self.upload_count / max(self.upload_count + self.error_count, 1)) * 100,
            "avg_upload_time_ms": avg_upload_time,
            "last_upload_time": self.last_upload_time,
            "queue_size": self.upload_queue.qsize() if hasattr(self.upload_queue, 'qsize') else 0,
            "worker_running": self.is_running
        }
    
    def reset_stats(self) -> None:
        """Reset upload statistics"""
        self.upload_count = 0
        self.error_count = 0
        self.total_upload_time = 0
        self.last_upload_time = 0
        logger.info("Upload statistics reset")
    
    def health_check(self) -> Dict:
        """Check uploader health"""
        try:
            # Test connection with minimal payload
            test_payload = {
                "instances": [],
                "timestamp": int(time.time()),
                "version": "v3"
            }
            
            response = self._make_request(test_payload)
            
            return {
                "status": "healthy",
                "api_accessible": True,
                "response_time_ms": 0,  # Could measure actual time
                "worker_running": self.is_running
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "api_accessible": False,
                "error": str(e),
                "worker_running": self.is_running
            }
    
    def __del__(self):
        """Cleanup on destruction"""
        self.stop_async_worker()
        if hasattr(self, 'session'):
            self.session.close()