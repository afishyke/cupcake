"""Integration layer that feeds the unified signal generator every minute."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from unified_signal_generator import UnifiedSignalGenerator, UnifiedSignal


class UnifiedSignalManager:
    """Accepts data from upstream modules and emits unified signals."""

    def __init__(
        self,
        socketio=None,
        redis_client=None,
        portfolio_value: float = 100_000.0,
        risk_pct: float = 0.01,
        update_interval_seconds: int = 60,
    ) -> None:
        self.socketio = socketio
        self.redis_client = redis_client
        self.generator = UnifiedSignalGenerator(
            portfolio_value=portfolio_value,
            risk_pct=risk_pct,
        )
        self.update_interval = update_interval_seconds
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._latest_signals: Dict[str, UnifiedSignal] = {}
        self._signal_history: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.RLock()
        self._timer: Optional[threading.Timer] = None
        self._running = False
        self._history_limit = 240

    def set_redis_client(self, redis_client) -> None:
        self.redis_client = redis_client

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._schedule_next_cycle(immediate=True)

    def stop(self) -> None:
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _schedule_next_cycle(self, immediate: bool = False) -> None:
        if not self._running:
            return
        delay = 0 if immediate else self.update_interval
        self._timer = threading.Timer(delay, self._run_cycle)
        self._timer.daemon = True
        self._timer.start()

    def _run_cycle(self) -> None:
        try:
            self.recalculate_now()
        finally:
            self._schedule_next_cycle()

    def recalculate_now(self) -> List[Dict[str, Any]]:
        """Generate signals immediately without waiting for timer."""
        with self._lock:
            if not self._snapshots:
                return []

            batch_payload: List[Dict[str, Any]] = []
            for symbol, snapshot in self._snapshots.items():
                signal = self.generator.generate(symbol, snapshot)
                self._latest_signals[symbol] = signal
                payload = signal.to_payload()
                batch_payload.append(payload)
                self._append_history(symbol, payload)

            if self.socketio and batch_payload:
                self.socketio.emit(
                    "unified_signal_update",
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "signals": batch_payload,
                    },
                )

            return batch_payload

    # ------------------------------------------------------------------
    # Snapshot ingestion
    # ------------------------------------------------------------------
    def update_symbol_snapshot(self, symbol: str, **sections: Any) -> None:
        with self._lock:
            snapshot = self._snapshots.setdefault(symbol, {})
            for key, value in sections.items():
                if value is not None:
                    if isinstance(value, dict) and isinstance(snapshot.get(key), dict):
                        snapshot[key].update(value)
                    else:
                        snapshot[key] = value
            snapshot.setdefault("context", {}).setdefault(
                "as_of", datetime.utcnow().isoformat()
            )

    def update_price(self, symbol: str, price_block: Dict[str, Any]) -> None:
        self.update_symbol_snapshot(symbol, price=price_block)

    def update_trajectory(self, symbol: str, trajectory_block: Dict[str, Any]) -> None:
        self.update_symbol_snapshot(symbol, trajectory=trajectory_block)

    def update_quant(self, symbol: str, quant_block: Dict[str, Any]) -> None:
        self.update_symbol_snapshot(symbol, quant=quant_block)

    def update_technical(self, symbol: str, technical_block: Dict[str, Any]) -> None:
        context = {}
        if technical_block:
            daily_trend = technical_block.get("daily_trend") or technical_block.get("trend_daily")
            if daily_trend:
                context.setdefault("daily_trend", daily_trend)
        self.update_symbol_snapshot(symbol, technical=technical_block, context=context or None)

    def update_orderbook(self, symbol: str, orderbook_block: Dict[str, Any]) -> None:
        self.update_symbol_snapshot(symbol, orderbook=orderbook_block)

    def update_market_regime(self, symbol: str, regime_block: Dict[str, Any]) -> None:
        self.update_symbol_snapshot(symbol, market_regime=regime_block)

    # ------------------------------------------------------------------
    # Accessors for API/UI
    # ------------------------------------------------------------------
    def get_signal(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            signal = self._latest_signals.get(symbol)
            return signal.to_payload() if signal else None

    def get_all_signals(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [signal.to_payload() for signal in self._latest_signals.values()]

    def get_snapshot(self, symbol: str) -> Dict[str, Any]:
        with self._lock:
            return self._snapshots.get(symbol, {}).copy()

    def get_history(self, symbol: Optional[str] = None, limit: int = 60) -> List[Dict[str, Any]]:
        with self._lock:
            if symbol:
                redis_history = self._get_history_from_redis(symbol=symbol, limit=limit)
                if redis_history:
                    return redis_history
                return list(self._signal_history.get(symbol, [])[-limit:])

            merged: List[Dict[str, Any]] = []
            for _, history in self._signal_history.items():
                merged.extend(history[-limit:])
            merged.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
            return merged[:limit]

    def _append_history(self, symbol: str, payload: Dict[str, Any]) -> None:
        history = self._signal_history.setdefault(symbol, [])
        history.append(payload)
        if len(history) > self._history_limit:
            del history[: len(history) - self._history_limit]

        if not self.redis_client:
            return

        try:
            redis_key = self._history_redis_key(symbol)
            encoded = json.dumps(payload, ensure_ascii=True)
            self.redis_client.rpush(redis_key, encoded)
            self.redis_client.ltrim(redis_key, -self._history_limit, -1)
        except Exception:
            pass

    def _history_redis_key(self, symbol: str) -> str:
        return f"unified:history:{symbol}"

    def _get_history_from_redis(self, symbol: str, limit: int) -> List[Dict[str, Any]]:
        if not self.redis_client:
            return []
        try:
            key = self._history_redis_key(symbol)
            rows = self.redis_client.lrange(key, -limit, -1)
            out: List[Dict[str, Any]] = []
            for row in rows:
                try:
                    parsed = json.loads(row)
                    if isinstance(parsed, dict):
                        out.append(parsed)
                except Exception:
                    continue
            return out
        except Exception:
            return []


def build_unified_signal_manager(socketio=None) -> UnifiedSignalManager:
    manager = UnifiedSignalManager(socketio=socketio)
    manager.start()
    return manager
