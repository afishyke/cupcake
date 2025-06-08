#!/usr/bin/env python3
"""
Ultra-fast final analysis engine for real-time trading decisions
Optimized for minimal latency and maximum throughput
"""

import json
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from numba import jit
import time

from risk_manager import RiskManager, RiskMetrics

logger = logging.getLogger(__name__)

@dataclass
class SignalOutput:
    """Structured output for trading signals"""
    timestamp: float
    instrument_key: str
    symbol_name: str
    action: str  # BUY, SELL, HOLD
    confidence: float
    price: float
    quantity: int
    signal_strength: float
    technical_score: float
    risk_score: float
    reasons: List[str]

class FinalAnalyzer:
    """High-performance analysis engine"""
    
    def __init__(self, risk_manager: RiskManager):
        self.risk_manager = risk_manager
        self.signal_weights = {
            'momentum': 0.25,
            'trend': 0.25,
            'volatility': 0.15,
            'volume': 0.15,
            'order_flow': 0.20
        }
        self.min_confidence = 0.65
        self.cache = {}
        self.last_analysis = {}
        
    @jit(nopython=True, cache=True)
    def _calculate_momentum_score(rsi: float, macd_signal: float, 
                                 price_change: float) -> float:
        """JIT-compiled momentum scoring"""
        rsi_score = 0.0
        if rsi > 70:
            rsi_score = -1.0
        elif rsi < 30:
            rsi_score = 1.0
        else:
            rsi_score = (50 - rsi) / 20.0
            
        macd_score = np.tanh(macd_signal / 10.0)
        price_score = np.tanh(price_change / 5.0)
        
        return (rsi_score + macd_score + price_score) / 3.0
    
    @jit(nopython=True, cache=True)
    def _calculate_trend_score(sma_20: float, sma_50: float, current_price: float,
                             bb_upper: float, bb_lower: float) -> float:
        """JIT-compiled trend scoring"""
        trend_score = 0.0
        
        # Moving average trend
        if sma_20 > sma_50:
            trend_score += 0.5
        else:
            trend_score -= 0.5
            
        # Price vs moving averages
        if current_price > sma_20:
            trend_score += 0.3
        else:
            trend_score -= 0.3
            
        # Bollinger band position
        bb_width = bb_upper - bb_lower
        if bb_width > 0:
            bb_position = (current_price - bb_lower) / bb_width
            if bb_position > 0.8:
                trend_score += 0.2
            elif bb_position < 0.2:
                trend_score -= 0.2
                
        return np.clip(trend_score, -1.0, 1.0)
    
    @jit(nopython=True, cache=True)
    def _calculate_volatility_score(atr: float, bb_width: float, 
                                  volume_ratio: float) -> float:
        """JIT-compiled volatility assessment"""
        # Normalize ATR (assuming typical range)
        atr_norm = np.clip(atr / 10.0, 0, 2.0)
        
        # High volatility can indicate opportunity or risk
        volatility_factor = np.tanh(atr_norm - 1.0)
        
        # Volume confirmation
        volume_factor = np.tanh((volume_ratio - 1.0) * 2.0)
        
        return (volatility_factor + volume_factor) / 2.0
    
    def _extract_technical_features(self, tech_data: Dict, ohlcv_df: pd.DataFrame) -> Dict:
        """Extract key technical features efficiently"""
        try:
            latest = ohlcv_df.iloc[-1]
            prev = ohlcv_df.iloc[-2] if len(ohlcv_df) > 1 else latest
            
            features = {
                'current_price': float(latest['close']),
                'price_change': float((latest['close'] - prev['close']) / prev['close'] * 100),
                'volume_ratio': float(latest['volume'] / ohlcv_df['volume'].rolling(20).mean().iloc[-1]),
                'rsi': float(tech_data.get('RSI', {}).get('values', [50])[-1]),
                'macd_signal': float(tech_data.get('MACD', {}).get('signal', [0])[-1]),
                'sma_20': float(tech_data.get('SMA_20', {}).get('values', [latest['close']])[-1]),
                'sma_50': float(tech_data.get('SMA_50', {}).get('values', [latest['close']])[-1]),
                'bb_upper': float(tech_data.get('BBANDS', {}).get('upper', [latest['close']])[-1]),
                'bb_lower': float(tech_data.get('BBANDS', {}).get('lower', [latest['close']])[-1]),
                'atr': float(tech_data.get('ATR', {}).get('values', [1.0])[-1]),
            }
            
            return features
            
        except Exception as e:
            logger.error(f"Feature extraction error: {e}")
            return self._get_default_features(ohlcv_df)
    
    def _get_default_features(self, ohlcv_df: pd.DataFrame) -> Dict:
        """Fallback feature extraction"""
        latest = ohlcv_df.iloc[-1]
        return {
            'current_price': float(latest['close']),
            'price_change': 0.0,
            'volume_ratio': 1.0,
            'rsi': 50.0,
            'macd_signal': 0.0,
            'sma_20': float(latest['close']),
            'sma_50': float(latest['close']),
            'bb_upper': float(latest['close'] * 1.02),
            'bb_lower': float(latest['close'] * 0.98),
            'atr': float(latest['close'] * 0.02),
        }
    
    def _calculate_order_flow_score(self, order_book: Dict, cumulative_delta: float) -> float:
        """Analyze order flow for market microstructure"""
        try:
            if not order_book or 'bids' not in order_book or 'asks' not in order_book:
                return 0.0
                
            bids = order_book['bids'][:5]  # Top 5 levels
            asks = order_book['asks'][:5]
            
            if not bids or not asks:
                return 0.0
                
            # Bid-ask imbalance
            bid_volume = sum(bid.get('quantity', 0) for bid in bids)
            ask_volume = sum(ask.get('quantity', 0) for ask in asks)
            
            if bid_volume + ask_volume == 0:
                return 0.0
                
            imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
            
            # Cumulative delta normalization
            delta_score = np.tanh(cumulative_delta / 1000.0)
            
            return (imbalance + delta_score) / 2.0
            
        except Exception as e:
            logger.warning(f"Order flow calculation error: {e}")
            return 0.0
    
    def analyze_instrument(self, instrument_key: str, symbol_name: str,
                          tech_data: Dict, ohlcv_df: pd.DataFrame,
                          order_book: Dict, cumulative_delta: float,
                          risk_controls: Dict) -> Dict:
        """Main analysis function - optimized for speed"""
        start_time = time.time()
        
        try:
            # Extract features efficiently
            features = self._extract_technical_features(tech_data, ohlcv_df)
            
            # Calculate component scores using JIT functions
            momentum_score = self._calculate_momentum_score(
                features['rsi'], features['macd_signal'], features['price_change']
            )
            
            trend_score = self._calculate_trend_score(
                features['sma_20'], features['sma_50'], features['current_price'],
                features['bb_upper'], features['bb_lower']
            )
            
            volatility_score = self._calculate_volatility_score(
                features['atr'], features['bb_upper'] - features['bb_lower'],
                features['volume_ratio']
            )
            
            order_flow_score = self._calculate_order_flow_score(order_book, cumulative_delta)
            
            # Volume score
            volume_score = np.tanh((features['volume_ratio'] - 1.0) * 2.0)
            
            # Weighted composite score
            technical_score = (
                momentum_score * self.signal_weights['momentum'] +
                trend_score * self.signal_weights['trend'] +
                volatility_score * self.signal_weights['volatility'] +
                volume_score * self.signal_weights['volume'] +
                order_flow_score * self.signal_weights['order_flow']
            )
            
            # Risk assessment
            risk_metrics = self.risk_manager.calculate_risk_metrics(
                instrument_key, features['current_price'], features['current_price']
            )
            
            # Generate signal
            signal = self._generate_signal(technical_score, risk_metrics, features)
            
            # Construct output
            result = {
                'timestamp': time.time(),
                'instrument_key': instrument_key,
                'symbol_name': symbol_name,
                'signal': signal,
                'technical_score': technical_score,
                'component_scores': {
                    'momentum': momentum_score,
                    'trend': trend_score,
                    'volatility': volatility_score,
                    'volume': volume_score,
                    'order_flow': order_flow_score
                },
                'features': features,
                'risk_metrics': asdict(risk_metrics),
                'processing_time_ms': (time.time() - start_time) * 1000
            }
            
            # Cache for repeated analysis
            self.last_analysis[instrument_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Analysis error for {instrument_key}: {e}")
            return self._get_default_analysis(instrument_key, symbol_name)
    
    def _generate_signal(self, technical_score: float, risk_metrics: RiskMetrics,
                        features: Dict) -> SignalOutput:
        """Generate trading signal from analysis"""
        
        # Determine action and confidence
        abs_score = abs(technical_score)
        confidence = min(abs_score * 1.5, 1.0)  # Scale to 0-1
        
        if confidence < self.min_confidence:
            action = "HOLD"
            quantity = 0
        elif technical_score > 0:
            action = "BUY"
            quantity = risk_metrics.position_size
        else:
            action = "SELL"
            quantity = risk_metrics.position_size
            
        # Generate reasons
        reasons = []
        if abs(technical_score) > 0.3:
            reasons.append(f"Strong {'bullish' if technical_score > 0 else 'bearish'} signal")
        if features['volume_ratio'] > 1.5:
            reasons.append("High volume confirmation")
        if features['rsi'] > 70:
            reasons.append("Overbought condition")
        elif features['rsi'] < 30:
            reasons.append("Oversold condition")
            
        return SignalOutput(
            timestamp=time.time(),
            instrument_key=features.get('instrument_key', ''),
            symbol_name=features.get('symbol_name', ''),
            action=action,
            confidence=confidence,
            price=features['current_price'],
            quantity=quantity,
            signal_strength=abs_score,
            technical_score=technical_score,
            risk_score=risk_metrics.drawdown_pct,
            reasons=reasons
        )
    
    def _get_default_analysis(self, instrument_key: str, symbol_name: str) -> Dict:
        """Fallback analysis result"""
        return {
            'timestamp': time.time(),
            'instrument_key': instrument_key,
            'symbol_name': symbol_name,
            'signal': SignalOutput(
                timestamp=time.time(),
                instrument_key=instrument_key,
                symbol_name=symbol_name,
                action="HOLD",
                confidence=0.0,
                price=0.0,
                quantity=0,
                signal_strength=0.0,
                technical_score=0.0,
                risk_score=0.0,
                reasons=["Analysis error - holding position"]
            ),
            'technical_score': 0.0,
            'component_scores': {},
            'features': {},
            'risk_metrics': {},
            'processing_time_ms': 0.0,
            'error': True
        }
    
    def batch_analyze(self, analysis_requests: List[Dict]) -> List[Dict]:
        """Process multiple instruments efficiently"""
        results = []
        
        for request in analysis_requests:
            try:
                result = self.analyze_instrument(**request)
                results.append(result)
            except Exception as e:
                logger.error(f"Batch analysis error: {e}")
                results.append(self._get_default_analysis(
                    request.get('instrument_key', ''),
                    request.get('symbol_name', '')
                ))
                
        return results
    
    def get_analysis_summary(self) -> Dict:
        """Get summary of recent analysis"""
        if not self.last_analysis:
            return {"message": "No analysis performed yet"}
            
        total_instruments = len(self.last_analysis)
        avg_processing_time = np.mean([
            a.get('processing_time_ms', 0) for a in self.last_analysis.values()
        ])
        
        signals = [a.get('signal', {}).get('action', 'HOLD') for a in self.last_analysis.values()]
        signal_counts = {
            'BUY': signals.count('BUY'),
            'SELL': signals.count('SELL'),
            'HOLD': signals.count('HOLD')
        }
        
        return {
            'total_instruments': total_instruments,
            'avg_processing_time_ms': avg_processing_time,
            'signal_distribution': signal_counts,
            'last_update': max(a.get('timestamp', 0) for a in self.last_analysis.values())
        }