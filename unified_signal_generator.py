"""Unified signal generation that combines every subsystem into one contract.

This module weights trajectory, quantitative, technical, order-book, and market
regime inputs according to the specification and enforces persistence rules so
the UI only receives a single BUY/SELL/HOLD decision with full context.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
from typing import Any, Dict, List, Optional, Tuple


def _json_safe(value: Any) -> Any:
    """Convert numpy/pandas/scalar objects to JSON-safe primitives."""
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value

    # Handle numpy/pandas scalar objects without importing them directly.
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if isinstance(value, (str, int, bool)) or value is None:
        return value

    return str(value)


class SignalDirection(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class ComponentContribution:
    """Normalized contribution from an upstream subsystem."""

    name: str
    weight: float
    raw_score: float
    confidence: float
    direction: SignalDirection
    narrative: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def weighted_score(self) -> float:
        return self.raw_score * self.weight


@dataclass
class UnifiedSignal:
    symbol: str
    direction: SignalDirection
    confidence: float
    score: float
    entry_price: float
    target_price: float
    stop_loss: float
    risk_per_unit: float
    position_size_value: float
    position_size_qty: float
    risk_reward: float
    updated_at: datetime
    persistence_reason: Optional[str]
    timeframe_execution: str
    timeframe_context: str
    rationale: List[str]
    components: List[ComponentContribution]
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        payload = {
            "symbol": self.symbol,
            "direction": self.direction.value,
            "confidence": round(self.confidence, 3),
            "score": round(self.score, 3),
            "entry_price": self.entry_price,
            "target_price": self.target_price,
            "stop_loss": self.stop_loss,
            "risk_per_unit": self.risk_per_unit,
            "position_size_value": self.position_size_value,
            "position_size_qty": self.position_size_qty,
            "risk_reward": round(self.risk_reward, 2) if self.risk_reward else None,
            "updated_at": self.updated_at.isoformat(),
            "timeframes": {
                "execution": self.timeframe_execution,
                "context": self.timeframe_context,
            },
            "persistence_reason": self.persistence_reason,
            "rationale": self.rationale,
            "components": [
                {
                    "name": c.name,
                    "weight": c.weight,
                    "score": round(c.raw_score, 3),
                    "direction": c.direction.value,
                    "confidence": round(c.confidence, 3),
                    "narrative": c.narrative,
                    "metadata": _json_safe(c.metadata),
                }
                for c in self.components
            ],
            "extra": _json_safe(self.extra),
        }
        return _json_safe(payload)


def _clamp(value: float, minimum: float = -1.0, maximum: float = 1.0) -> float:
    if isinstance(value, float) and not math.isfinite(value):
        return 0.0
    return max(minimum, min(maximum, value))


class UnifiedSignalGenerator:
    """Weights every subsystem into one authoritative trading signal."""

    DEFAULT_WEIGHTS = {
        "trajectory": 0.25,
        "quant": 0.20,
        "technical": 0.20,
        "orderbook": 0.15,
        "market_regime": 0.20,
    }

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        persistence_threshold: float = 0.60,
        hold_threshold: float = 0.20,
        portfolio_value: float = 100_000.0,
        risk_pct: float = 0.01,
        execution_timeframe: str = "1H",
        context_timeframe: str = "1D",
    ) -> None:
        self.weights = weights or self.DEFAULT_WEIGHTS.copy()
        self.persistence_threshold = persistence_threshold
        self.hold_threshold = hold_threshold
        self.portfolio_value = portfolio_value
        self.risk_pct = risk_pct
        self.execution_timeframe = execution_timeframe
        self.context_timeframe = context_timeframe
        self._previous_signals: Dict[str, UnifiedSignal] = {}
        self._last_committed_direction: Dict[str, SignalDirection] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def generate(self, symbol: str, snapshot: Dict[str, Any]) -> UnifiedSignal:
        """Generate a unified signal for the provided symbol.

        The snapshot should contain sub-dicts for trajectory, quant, technical,
        orderbook, and market_regime components plus a price/context section.
        """

        components = self._build_component_contributions(snapshot)
        weighted_score = self._combine_components(components)
        aggregate_confidence = self._calculate_base_confidence(components, weighted_score)
        adjusted_confidence, context_notes = self._apply_timeframe_context(snapshot, aggregate_confidence)
        direction = self._determine_direction(weighted_score, adjusted_confidence)
        direction, persistence_reason = self._enforce_persistence(
            symbol, direction, adjusted_confidence
        )

        price_block = snapshot.get("price", {})
        entry_price = float(price_block.get("ltp") or price_block.get("close") or 0.0)
        volatility = price_block.get("atr") or price_block.get("atr_1h") or price_block.get("volatility")
        if not volatility and entry_price > 0:
            volatility = entry_price * 0.01  # fallback to 1% ATR
        volatility = float(volatility or 1.0)

        stop_loss, target_price = self._derive_risk_levels(direction, entry_price, volatility)
        risk_per_unit = abs(entry_price - stop_loss)
        position_value, position_qty = self._calculate_position_sizing(entry_price, risk_per_unit)
        risk_reward = self._calculate_risk_reward(direction, entry_price, target_price, stop_loss)

        rationale = self._build_rationale(direction, adjusted_confidence, components, context_notes)

        unified = UnifiedSignal(
            symbol=symbol,
            direction=direction,
            confidence=adjusted_confidence,
            score=weighted_score,
            entry_price=entry_price,
            target_price=target_price,
            stop_loss=stop_loss,
            risk_per_unit=risk_per_unit,
            position_size_value=position_value,
            position_size_qty=position_qty,
            risk_reward=risk_reward,
            updated_at=datetime.now(timezone.utc),
            persistence_reason=persistence_reason,
            timeframe_execution=self.execution_timeframe,
            timeframe_context=self.context_timeframe,
            rationale=rationale,
            components=components,
            extra={
                "portfolio_value": self.portfolio_value,
                "risk_pct": self.risk_pct,
                "price_context": snapshot.get("price", {}),
            },
        )

        self._previous_signals[symbol] = unified
        return unified

    # ------------------------------------------------------------------
    # Component handling
    # ------------------------------------------------------------------
    def _build_component_contributions(self, snapshot: Dict[str, Any]) -> List[ComponentContribution]:
        contributions: List[ComponentContribution] = []
        for name, weight in self.weights.items():
            block = snapshot.get(name) or {}
            if not block:
                continue

            direction = self._infer_direction(block)
            base_conf = float(block.get("confidence", block.get("strength", 0)))
            base_conf = _clamp(base_conf, 0.0, 1.0)
            score = block.get("score")
            if score is None:
                bias = block.get("bias")
                if bias is None:
                    bias = 1.0 if direction == SignalDirection.BUY else -1.0 if direction == SignalDirection.SELL else 0.0
                score = bias * (block.get("momentum", 1.0))

            score = _clamp(float(score), -1.0, 1.0)
            if block.get("direction") in {"SELL", SignalDirection.SELL} and score > 0:
                score *= -1
            narrative = block.get("reason") or block.get("summary") or self._default_narrative(name, direction)

            contributions.append(
                ComponentContribution(
                    name=name,
                    weight=weight,
                    raw_score=score,
                    confidence=base_conf if base_conf > 0 else abs(score),
                    direction=direction,
                    narrative=narrative,
                    metadata={k: v for k, v in block.items() if k not in {"score", "confidence", "direction", "reason", "summary"}},
                )
            )

        return contributions

    def _combine_components(self, components: List[ComponentContribution]) -> float:
        if not components:
            return 0.0
        active_weight = sum(c.weight for c in components)
        if not active_weight:
            return 0.0
        total = sum(c.weighted_score() for c in components)
        return _clamp(total / active_weight, -1.0, 1.0)

    def _calculate_base_confidence(self, components: List[ComponentContribution], aggregate_score: float) -> float:
        if not components:
            return 0.0
        weight_sum = sum(c.weight for c in components)
        confidence = 0.0
        for comp in components:
            confidence += comp.confidence * (comp.weight / weight_sum)
        # Blend with magnitude of the combined score
        blended = (confidence * 0.6) + (abs(aggregate_score) * 0.4)
        return _clamp(blended, 0.0, 1.0)

    def _apply_timeframe_context(self, snapshot: Dict[str, Any], confidence: float) -> Tuple[float, List[str]]:
        context_notes: List[str] = []
        context_block = snapshot.get("context", {}) or snapshot.get("market", {})
        daily = context_block.get("daily_trend") or context_block.get("1d")
        if daily:
            bias = self._extract_bias(daily)
            if bias:
                confidence *= 1 + (0.15 * bias)
                context_notes.append(
                    f"Daily context ({self.context_timeframe}) bias {bias:+.2f} adjusted confidence"
                )
        return _clamp(confidence, 0.0, 1.0), context_notes

    def _determine_direction(self, score: float, confidence: float) -> SignalDirection:
        if confidence < self.hold_threshold:
            return SignalDirection.HOLD
        if score >= 0.05:
            return SignalDirection.BUY
        if score <= -0.05:
            return SignalDirection.SELL
        return SignalDirection.HOLD

    def _enforce_persistence(
        self, symbol: str, direction: SignalDirection, confidence: float
    ) -> Tuple[SignalDirection, Optional[str]]:
        last_committed = self._last_committed_direction.get(symbol, SignalDirection.HOLD)

        if direction == SignalDirection.HOLD:
            return SignalDirection.HOLD, (
                "Confidence dropped below actionable range" if confidence < self.hold_threshold else None
            )

        if direction != last_committed and confidence < self.persistence_threshold:
            return SignalDirection.HOLD, "Direction change gated by persistence rule (<0.60 confidence)"

        if direction != SignalDirection.HOLD:
            self._last_committed_direction[symbol] = direction

        return direction, None

    def _derive_risk_levels(
        self, direction: SignalDirection, entry: float, volatility: float
    ) -> Tuple[float, float]:
        if entry <= 0 or volatility <= 0:
            return entry, entry

        buffer = max(volatility * 1.5, entry * 0.003)
        if direction == SignalDirection.SELL:
            return entry + buffer, entry - (buffer * 2)
        if direction == SignalDirection.BUY:
            return entry - buffer, entry + (buffer * 2)
        return entry, entry

    def _calculate_position_sizing(self, entry_price: float, risk_per_unit: float) -> Tuple[float, float]:
        if entry_price <= 0 or risk_per_unit <= 0:
            return 0.0, 0.0
        capital_at_risk = self.portfolio_value * self.risk_pct
        qty = capital_at_risk / risk_per_unit
        return capital_at_risk, qty

    def _calculate_risk_reward(
        self, direction: SignalDirection, entry: float, target: float, stop: float
    ) -> float:
        if entry <= 0 or target <= 0 or stop <= 0 or entry == stop:
            return 0.0
        reward = (target - entry) if direction == SignalDirection.BUY else (entry - target)
        risk = (entry - stop) if direction == SignalDirection.BUY else (stop - entry)
        if risk == 0:
            return 0.0
        return round(reward / risk, 2)

    def _build_rationale(
        self,
        direction: SignalDirection,
        confidence: float,
        components: List[ComponentContribution],
        context_notes: List[str],
    ) -> List[str]:
        rationale = [
            f"Direction {direction.value} with {confidence*100:.1f}% confidence on {self.execution_timeframe} timeframe"
        ]
        for component in components:
            rationale.append(f"{component.name.title()}: {component.narrative} ({component.raw_score:+.2f})")
        rationale.extend(context_notes)
        return rationale

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _default_narrative(component: str, direction: SignalDirection) -> str:
        if direction == SignalDirection.HOLD:
            return f"{component.title()} neutral"
        side = "bullish" if direction == SignalDirection.BUY else "bearish"
        return f"{component.title()} skewed {side}"

    @staticmethod
    def _infer_direction(block: Dict[str, Any]) -> SignalDirection:
        direction = block.get("direction") or block.get("signal") or block.get("side")
        if isinstance(direction, SignalDirection):
            return direction
        if isinstance(direction, str):
            try:
                return SignalDirection[direction.upper()]
            except KeyError:
                pass

        bias = block.get("bias") or block.get("trend")
        if isinstance(bias, (int, float)) and bias != 0:
            return SignalDirection.BUY if bias > 0 else SignalDirection.SELL

        return SignalDirection.HOLD

    @staticmethod
    def _extract_bias(block: Any) -> float:
        if isinstance(block, (int, float)):
            return _clamp(float(block), -1.0, 1.0)
        if isinstance(block, dict):
            if "bias" in block:
                return _clamp(float(block["bias"]), -1.0, 1.0)
            if "score" in block:
                return _clamp(float(block["score"]), -1.0, 1.0)
            if "direction" in block:
                direction = block["direction"].upper()
                if direction == "BUY" or direction == "BULLISH":
                    return _clamp(block.get("confidence", 1.0), 0.0, 1.0)
                if direction == "SELL" or direction == "BEARISH":
                    return -_clamp(block.get("confidence", 1.0), 0.0, 1.0)
        return 0.0
