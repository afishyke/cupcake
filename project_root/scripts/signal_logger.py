#!/usr/bin/env python3
"""
Ultra-fast signal logging system for real-time trading
Optimized for minimal I/O overhead and data integrity
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import time
from pathlib import Path
import threading
from queue import Queue, Empty
import csv
from datetime import datetime
import gzip

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(message)s'))
logger.addHandler(handler)

class SignalLogger:
    """High-performance signal logger with async I/O and optional compression"""

    def __init__(self, log_dir: str = "logs/signals/", batch_size: int = 100, flush_interval: float = 5.0):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # File paths
        self.signal_history_path = self.log_dir / "signal_history.csv"
        self.performance_log_path = self.log_dir / "performance.json"
        self.error_log_path = self.log_dir / "errors.log"

        # Async processing
        self.log_queue = Queue()
        self.is_running = False
        self.worker_thread: Optional[threading.Thread] = None
        self.batch_size = batch_size
        self.flush_interval = flush_interval  # seconds

        # Performance tracking
        self.signals_logged = 0
        self.buffer: List[Dict[str, Any]] = []
        self.lock = threading.Lock()

        # CSV headers
        self.csv_headers = [
            'timestamp', 'datetime', 'instrument_key', 'symbol_name',
            'action', 'confidence', 'price', 'quantity', 'signal_strength',
            'technical_score', 'risk_score', 'ml_prediction', 'ml_confidence',
            'ml_direction', 'reasons', 'processing_time_ms'
        ]

        # Initialize CSV file if not exists
        self._initialize_csv()

    def _initialize_csv(self) -> None:
        """Initialize CSV file with headers if it doesn't exist."""
        if not self.signal_history_path.exists():
            try:
                with open(self.signal_history_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(self.csv_headers)
                logger.info(f"Initialized signal history CSV: {self.signal_history_path}")
            except Exception as e:
                logger.error(f"Failed to initialize CSV: {e}")

    def start_async_logging(self) -> None:
        """Start async logging worker."""
        if not self.is_running:
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._logging_worker, daemon=True)
            self.worker_thread.start()
            logger.info("Signal logger worker started")

    def stop_async_logging(self, flush_remaining: bool = True) -> None:
        """Stop async logging worker, optionally flushing remaining buffer."""
        self.is_running = False

        if flush_remaining:
            self.flush_buffer()

        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=10.0)

        logger.info("Signal logger worker stopped")

    def _logging_worker(self) -> None:
        """Background worker for async logging."""
        last_flush = time.time()

        while self.is_running or not self.log_queue.empty():
            try:
                batch = []
                try:
                    item = self.log_queue.get(timeout=1.0)
                    batch.append(item)
                    while len(batch) < self.batch_size:
                        try:
                            item = self.log_queue.get_nowait()
                            batch.append(item)
                        except Empty:
                            break
                except Empty:
                    pass

                if batch:
                    with self.lock:
                        self.buffer.extend(batch)

                current_time = time.time()
                if (len(self.buffer) >= self.batch_size or
                    (current_time - last_flush) >= self.flush_interval) and self.buffer:
                    self._flush_buffer_internal()
                    last_flush = current_time

            except Exception as e:
                logger.error(f"Logging worker error: {e}")
                time.sleep(0.1)

    def log_signal_async(self, signal_data: Dict[str, Any]) -> None:
        """Enqueue a signal for asynchronous logging."""
        if not self.is_running:
            self.start_async_logging()

        flattened = self._flatten_signal_data(signal_data)
        self.log_queue.put(flattened)

    def log_signal_sync(self, signal_data: Dict[str, Any]) -> None:
        """Synchronously log a signal (blocks until it's appended, flushes if batch full)."""
        flattened = self._flatten_signal_data(signal_data)
        with self.lock:
            self.buffer.append(flattened)
            if len(self.buffer) >= self.batch_size:
                self._flush_buffer_internal()

    def flush_buffer(self) -> None:
        """Thread-safe manual flush of the in-memory buffer."""
        with self.lock:
            if self.buffer:
                self._flush_buffer_internal()

    def _flush_buffer_internal(self) -> None:
        """Internal flush logic: write buffer to CSV, clear buffer, update perf."""
        try:
            # Write to CSV (plain); to enable gzip compression, swap in gzip.open here
            df = pd.DataFrame(self.buffer, columns=self.csv_headers)
            df.to_csv(self.signal_history_path, mode='a', header=False, index=False)

            self.signals_logged += len(self.buffer)
            self.buffer.clear()

            self._update_performance_log()
            logger.info(f"Flushed {self.signals_logged} total signals to disk")

        except Exception as e:
            self._log_error(f"Flush error: {e}")

    def _flatten_signal_data(self, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested JSON into a flat dict matching CSV headers."""
        try:
            flat = pd.json_normalize(signal_data)
            if flat.empty:
                raise ValueError("Empty flattened data")
            flat_row = flat.iloc[0].to_dict()
            return {col: flat_row.get(col, None) for col in self.csv_headers}
        except Exception as e:
            self._log_error(f"Flattening error: {e}")
            return {col: None for col in self.csv_headers}

    def _update_performance_log(self) -> None:
        """Write out the JSON performance log."""
        try:
            perf_data = {
                "last_flush_time": datetime.utcnow().isoformat() + "Z",
                "signals_logged": self.signals_logged
            }
            with open(self.performance_log_path, 'w') as f:
                json.dump(perf_data, f, indent=2)
        except Exception as e:
            self._log_error(f"Performance log update failed: {e}")

    def _log_error(self, message: str) -> None:
        """Append an error message to the error log and to standard logger."""
        try:
            ts = datetime.utcnow().isoformat() + "Z"
            with open(self.error_log_path, 'a') as f:
                f.write(f"{ts} - {message}\n")
            logger.error(message)
        except Exception as ex:
            logger.critical(f"Critical logging error: {ex}")
