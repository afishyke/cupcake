"""
Final Analyzer Module - Master synthesizer that combines all data layers
into a comprehensive trading analysis output.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import talib

from advanced_indicators import (
    calculate_vwap, 
    analyze_order_book, 
    calculate_price_momentum, 
    calculate_volatility_metrics
)

logger = logging.getLogger(__name__)

class FinalAnalyzer:
    """
    Master synthesizer that combines all analytical layers into final JSON output.
    """
    
    def __init__(self):
        self.signal_weights = {
            'technical_indicators': 0.4,
            'order_book': 0.3,
            'momentum': 0.2,
            'volume': 0.1
        }
    
    def analyze_symbol(self,
                      symbol: str,
                      ohlcv_df: pd.DataFrame,
                      order_book: Dict[str, List[Dict]],
                      cumulative_delta: float,
                      risk_controls: Dict[str, float],
                      technical_indicators: Dict[str, Any],
                      timestamp: Optional[str] = None) -> Dict[str, Any]:
        """
        Master analysis function that synthesizes all data layers.
        
        Args:
            symbol: Trading symbol
            ohlcv_df: DataFrame with OHLCV data
            order_book: Current order book data
            cumulative_delta: Current cumulative delta value
            risk_controls: Risk management metrics
            technical_indicators: Pre-calculated technical indicators
            timestamp: Analysis timestamp
            
        Returns:
            Comprehensive analysis dictionary
        """
        try:
            if timestamp is None:
                timestamp = datetime.now().isoformat()
            
            # Core analysis components
            vwap_analysis = calculate_vwap(ohlcv_df)
            order_book_analysis = analyze_order_book(order_book)
            momentum_analysis = calculate_price_momentum(ohlcv_df)
            volatility_analysis = calculate_volatility_metrics(ohlcv_df)
            
            # Generate overall signal
            overall_signal, signal_strength, key_insights = self._generate_trading_signal(
                technical_indicators, vwap_analysis, order_book_analysis, 
                momentum_analysis, ohlcv_df
            )
            
            # Build comprehensive output
            analysis_result = {
                "symbol": symbol,
                "timestamp": timestamp,
                "overall_signal": overall_signal,
                "signal_strength": signal_strength,
                "key_insights": key_insights,
                
                "data_layers": {
                    "intraday_price_v": {
                        "ohlc": self._extract_ohlc_summary(ohlcv_df),
                        "vwap_analysis": vwap_analysis,
                        "volume_profile": self._analyze_volume_profile(ohlcv_df),
                        "price_action": self._analyze_price_action(ohlcv_df)
                    },
                    
                    "order_book": {
                        "level2_depth": self._format_order_book(order_book),
                        "analysis": order_book_analysis,
                        "spread_analysis": self._analyze_spread(order_book),
                        "liquidity_metrics": self._calculate_liquidity_metrics(order_book)
                    },
                    
                    "time_and_sales": {
                        "analysis": {
                            "cumulative_delta_estimated": cumulative_delta,
                            "delta_trend": self._classify_delta_trend(cumulative_delta),
                            "trade_intensity": self._calculate_trade_intensity(ohlcv_df)
                        }
                    }
                },
                
                "advanced_indicators_client_side": {
                    **technical_indicators,
                    "momentum_metrics": momentum_analysis,
                    "volatility_metrics": volatility_analysis,
                    "trend_analysis": self._analyze_trend(ohlcv_df, technical_indicators),
                    "support_resistance": self._identify_support_resistance(ohlcv_df)
                },
                
                "risk_controls_client_side": {
                    **risk_controls,
                    "position_sizing": self._calculate_position_sizing(ohlcv_df, risk_controls),
                    "risk_assessment": self._assess_overall_risk(ohlcv_df, volatility_analysis, risk_controls)
                },
                
                "market_context": {
                    "market_regime": self._identify_market_regime(ohlcv_df, volatility_analysis),
                    "trading_session": self._identify_trading_session(),
                    "correlation_signals": self._analyze_correlation_signals(ohlcv_df)
                }
            }
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Error evaluating volume signal: {e}")
            return 0
    
    def _extract_ohlc_summary(self, ohlcv_df: pd.DataFrame) -> Dict[str, Any]:
        """Extract OHLC summary from DataFrame."""
        try:
            if ohlcv_df.empty:
                return {"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0}
            
            latest = ohlcv_df.iloc[-1]
            return {
                "open": float(latest['open']),
                "high": float(latest['high']),
                "low": float(latest['low']),
                "close": float(latest['close']),
                "volume": float(latest['volume']),
                "change_pct": float(((latest['close'] - latest['open']) / latest['open']) * 100) if latest['open'] != 0 else 0
            }
        except Exception as e:
            logger.error(f"Error extracting OHLC: {e}")
            return {"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "change_pct": 0}
    
    def _analyze_volume_profile(self, ohlcv_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze volume profile patterns."""
        try:
            if len(ohlcv_df) < 10:
                return {"volume_trend": "INSUFFICIENT_DATA", "volume_ratio": 1.0}
            
            recent_volume = ohlcv_df['volume'].tail(5).mean()
            historical_volume = ohlcv_df['volume'].head(-5).mean()
            
            volume_ratio = recent_volume / historical_volume if historical_volume > 0 else 1.0
            
            if volume_ratio > 1.3:
                trend = "INCREASING"
            elif volume_ratio < 0.7:
                trend = "DECREASING"
            else:
                trend = "STABLE"
            
            return {
                "volume_trend": trend,
                "volume_ratio": float(volume_ratio),
                "avg_volume_recent": float(recent_volume),
                "avg_volume_historical": float(historical_volume)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing volume profile: {e}")
            return {"volume_trend": "ERROR", "volume_ratio": 1.0}
    
    def _analyze_price_action(self, ohlcv_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze price action patterns."""
        try:
            if len(ohlcv_df) < 3:
                return {"pattern": "INSUFFICIENT_DATA", "strength": 0}
            
            recent_candles = ohlcv_df.tail(3)
            
            # Check for doji patterns
            doji_threshold = 0.1  # 0.1% body size
            latest = recent_candles.iloc[-1]
            body_size = abs(latest['close'] - latest['open']) / latest['open'] * 100
            
            if body_size < doji_threshold:
                pattern = "DOJI"
                strength = 70
            else:
                # Check for engulfing patterns
                if len(recent_candles) >= 2:
                    current = recent_candles.iloc[-1]
                    previous = recent_candles.iloc[-2]
                    
                    if (current['open'] < previous['close'] and current['close'] > previous['open'] and
                        current['close'] > current['open'] and previous['close'] < previous['open']):
                        pattern = "BULLISH_ENGULFING"
                        strength = 80
                    elif (current['open'] > previous['close'] and current['close'] < previous['open'] and
                          current['close'] < current['open'] and previous['close'] > previous['open']):
                        pattern = "BEARISH_ENGULFING"
                        strength = 80
                    else:
                        pattern = "CONTINUATION"
                        strength = 30
                else:
                    pattern = "SINGLE_CANDLE"
                    strength = 20
            
            return {
                "pattern": pattern,
                "strength": strength,
                "body_size_pct": float(body_size)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing price action: {e}")
            return {"pattern": "ERROR", "strength": 0}
    
    def _format_order_book(self, order_book: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Format order book data for output."""
        try:
            bids = order_book.get('bids', [])[:5]  # Top 5 levels
            asks = order_book.get('asks', [])[:5]  # Top 5 levels
            
            return {
                "bids": [{"price": float(b.get('price', 0)), "quantity": float(b.get('quantity', 0))} for b in bids],
                "asks": [{"price": float(a.get('price', 0)), "quantity": float(a.get('quantity', 0))} for a in asks],
                "bid_count": len(bids),
                "ask_count": len(asks)
            }
            
        except Exception as e:
            logger.error(f"Error formatting order book: {e}")
            return {"bids": [], "asks": [], "bid_count": 0, "ask_count": 0}
    
    def _analyze_spread(self, order_book: Dict[str, List[Dict]]) -> Dict[str, float]:
        """Analyze bid-ask spread."""
        try:
            bids = order_book.get('bids', [])
            asks = order_book.get('asks', [])
            
            if not bids or not asks:
                return {"spread_abs": 0.0, "spread_pct": 0.0, "mid_price": 0.0}
            
            best_bid = bids[0].get('price', 0)
            best_ask = asks[0].get('price', 0)
            
            spread_abs = best_ask - best_bid
            mid_price = (best_bid + best_ask) / 2
            spread_pct = (spread_abs / mid_price * 100) if mid_price > 0 else 0
            
            return {
                "spread_abs": float(spread_abs),
                "spread_pct": float(spread_pct),
                "mid_price": float(mid_price),
                "best_bid": float(best_bid),
                "best_ask": float(best_ask)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing spread: {e}")
            return {"spread_abs": 0.0, "spread_pct": 0.0, "mid_price": 0.0}
    
    def _calculate_liquidity_metrics(self, order_book: Dict[str, List[Dict]]) -> Dict[str, float]:
        """Calculate liquidity metrics from order book."""
        try:
            bids = order_book.get('bids', [])
            asks = order_book.get('asks', [])
            
            bid_liquidity = sum(level.get('quantity', 0) for level in bids[:5])
            ask_liquidity = sum(level.get('quantity', 0) for level in asks[:5])
            total_liquidity = bid_liquidity + ask_liquidity
            
            liquidity_ratio = bid_liquidity / ask_liquidity if ask_liquidity > 0 else 0
            
            return {
                "bid_liquidity": float(bid_liquidity),
                "ask_liquidity": float(ask_liquidity),
                "total_liquidity": float(total_liquidity),
                "liquidity_ratio": float(liquidity_ratio)
            }
            
        except Exception as e:
            logger.error(f"Error calculating liquidity metrics: {e}")
            return {"bid_liquidity": 0.0, "ask_liquidity": 0.0, "total_liquidity": 0.0, "liquidity_ratio": 0.0}
    
    def _classify_delta_trend(self, cumulative_delta: float) -> str:
        """Classify cumulative delta trend."""
        if cumulative_delta > 1000:
            return "STRONG_BUYING"
        elif cumulative_delta > 500:
            return "MODERATE_BUYING"
        elif cumulative_delta > -500:
            return "NEUTRAL"
        elif cumulative_delta > -1000:
            return "MODERATE_SELLING"
        else:
            return "STRONG_SELLING"
    
    def _calculate_trade_intensity(self, ohlcv_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate trade intensity metrics."""
        try:
            if len(ohlcv_df) < 5:
                return {"intensity_score": 0.0, "trades_per_minute": 0.0}
            
            recent_volume = ohlcv_df['volume'].tail(5).sum()
            recent_range = (ohlcv_df['high'].tail(5).max() - ohlcv_df['low'].tail(5).min())
            avg_price = ohlcv_df['close'].tail(5).mean()
            
            # Intensity based on volume and price movement
            intensity_score = (recent_volume * recent_range / avg_price) if avg_price > 0 else 0
            
            # Estimate trades per minute (rough approximation)
            trades_per_minute = recent_volume / 5 / 100  # Assuming avg trade size of 100
            
            return {
                "intensity_score": float(intensity_score),
                "trades_per_minute": float(trades_per_minute)
            }
            
        except Exception as e:
            logger.error(f"Error calculating trade intensity: {e}")
            return {"intensity_score": 0.0, "trades_per_minute": 0.0}
    
    def _analyze_trend(self, ohlcv_df: pd.DataFrame, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze overall trend direction and strength."""
        try:
            if len(ohlcv_df) < 20:
                return {"direction": "UNKNOWN", "strength": 0, "timeframe": "SHORT"}
            
            # Use moving averages for trend
            ma_short = ohlcv_df['close'].rolling(5).mean().iloc[-1]
            ma_medium = ohlcv_df['close'].rolling(10).mean().iloc[-1]
            ma_long = ohlcv_df['close'].rolling(20).mean().iloc[-1]
            current_price = ohlcv_df['close'].iloc[-1]
            
            # Trend direction
            if current_price > ma_short > ma_medium > ma_long:
                direction = "STRONG_UPTREND"
                strength = 90
            elif current_price > ma_short > ma_medium:
                direction = "UPTREND"
                strength = 70
            elif current_price < ma_short < ma_medium < ma_long:
                direction = "STRONG_DOWNTREND"
                strength = 90
            elif current_price < ma_short < ma_medium:
                direction = "DOWNTREND"
                strength = 70
            else:
                direction = "SIDEWAYS"
                strength = 30
            
            return {
                "direction": direction,
                "strength": strength,
                "timeframe": "INTRADAY",
                "ma_alignment": {
                    "ma5": float(ma_short),
                    "ma10": float(ma_medium),
                    "ma20": float(ma_long)
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing trend: {e}")
            return {"direction": "ERROR", "strength": 0, "timeframe": "UNKNOWN"}
    
    def _identify_support_resistance(self, ohlcv_df: pd.DataFrame) -> Dict[str, List[float]]:
        """Identify key support and resistance levels."""
        try:
            if len(ohlcv_df) < 20:
                return {"support_levels": [], "resistance_levels": []}
            
            highs = ohlcv_df['high'].values
            lows = ohlcv_df['low'].values
            
            # Find local peaks and troughs
            resistance_levels = []
            support_levels = []
            
            for i in range(2, len(highs) - 2):
                # Resistance: local high
                if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                    resistance_levels.append(float(highs[i]))
                
                # Support: local low
                if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                    support_levels.append(float(lows[i]))
            
            # Keep only the most significant levels (top 3 each)
            resistance_levels = sorted(resistance_levels, reverse=True)[:3]
            support_levels = sorted(support_levels)[:3]
            
            return {
                "support_levels": support_levels,
                "resistance_levels": resistance_levels
            }
            
        except Exception as e:
            logger.error(f"Error identifying S/R levels: {e}")
            return {"support_levels": [], "resistance_levels": []}
    
    def _calculate_position_sizing(self, ohlcv_df: pd.DataFrame, risk_controls: Dict[str, float]) -> Dict[str, float]:
        """Calculate appropriate position sizing based on risk."""
        try:
            if ohlcv_df.empty:
                return {"suggested_quantity": 0, "risk_per_share": 0, "max_loss_amount": 0}
            
            current_price = ohlcv_df['close'].iloc[-1]
            stop_level = risk_controls.get('dynamic_trailing_stop', current_price * 0.98)
            
            risk_per_share = abs(current_price - stop_level)
            max_portfolio_risk = 10000  # Assume $10k max risk per trade
            
            suggested_quantity = int(max_portfolio_risk / risk_per_share) if risk_per_share > 0 else 0
            max_loss_amount = suggested_quantity * risk_per_share
            
            return {
                "suggested_quantity": suggested_quantity,
                "risk_per_share": float(risk_per_share),
                "max_loss_amount": float(max_loss_amount),
                "risk_reward_ratio": risk_controls.get('current_risk_reward_ratio', 0)
            }
            
        except Exception as e:
            logger.error(f"Error calculating position sizing: {e}")
            return {"suggested_quantity": 0, "risk_per_share": 0, "max_loss_amount": 0}
    
    def _assess_overall_risk(self, ohlcv_df: pd.DataFrame, volatility: Dict[str, float], risk_controls: Dict[str, float]) -> Dict[str, Any]:
        """Assess overall risk level of the trade."""
        try:
            risk_score = 0
            risk_factors = []
            
            # Volatility risk
            hist_vol = volatility.get('historical_volatility', 0)
            if hist_vol > 30:
                risk_score += 3
                risk_factors.append("HIGH_VOLATILITY")
            elif hist_vol > 15:
                risk_score += 1
                risk_factors.append("MODERATE_VOLATILITY")
            
            # Drawdown risk
            max_dd = risk_controls.get('max_drawdown_pct', 0)
            if max_dd > 5:
                risk_score += 2
                risk_factors.append("HIGH_DRAWDOWN")
            
            # Price efficiency risk
            efficiency = volatility.get('price_efficiency', 1.0)
            if efficiency < 0.3:
                risk_score += 1
                risk_factors.append("LOW_EFFICIENCY")
            
            # Overall risk level
            if risk_score >= 5:
                risk_level = "HIGH"
            elif risk_score >= 3:
                risk_level = "MODERATE"
            else:
                risk_level = "LOW"
            
            return {
                "risk_level": risk_level,
                "risk_score": risk_score,
                "risk_factors": risk_factors,
                "recommendation": "REDUCE_SIZE" if risk_level == "HIGH" else "NORMAL_SIZE"
            }
            
        except Exception as e:
            logger.error(f"Error assessing risk: {e}")
            return {"risk_level": "UNKNOWN", "risk_score": 0, "risk_factors": []}
    
    def _identify_market_regime(self, ohlcv_df: pd.DataFrame, volatility: Dict[str, float]) -> str:
        """Identify current market regime."""
        try:
            if len(ohlcv_df) < 20:
                return "INSUFFICIENT_DATA"
            
            vol = volatility.get('historical_volatility', 0)
            returns = ohlcv_df['close'].pct_change().dropna()
            
            if len(returns) < 10:
                return "INSUFFICIENT_DATA"
            
            trend_strength = abs(returns.mean()) / returns.std() if returns.std() > 0 else 0
            
            if vol > 25 and trend_strength > 0.5:
                return "HIGH_VOLATILITY_TRENDING"
            elif vol > 25:
                return "HIGH_VOLATILITY_RANGING"
            elif trend_strength > 0.3:
                return "LOW_VOLATILITY_TRENDING"
            else:
                return "LOW_VOLATILITY_RANGING"
                
        except Exception as e:
            logger.error(f"Error identifying market regime: {e}")
            return "UNKNOWN"
    
    def _identify_trading_session(self) -> str:
        """Identify current trading session based on time."""
        try:
            current_hour = datetime.now().hour
            
            if 9 <= current_hour < 11:
                return "OPENING"
            elif 11 <= current_hour < 14:
                return "MID_DAY"
            elif 14 <= current_hour < 15:
                return "CLOSING"
            else:
                return "AFTER_HOURS"
                
        except Exception as e:
            logger.error(f"Error identifying trading session: {e}")
            return "UNKNOWN"
    
    def _analyze_correlation_signals(self, ohlcv_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze correlation with broader market signals."""
        try:
            # This is a placeholder for correlation analysis
            # In practice, you would compare with market indices
            
            if len(ohlcv_df) < 10:
                return {"correlation_strength": 0.0, "market_beta": 1.0}
            
            returns = ohlcv_df['close'].pct_change().dropna()
            volatility = returns.std()
            
            # Simplified correlation metrics
            return {
                "correlation_strength": 0.5,  # Placeholder
                "market_beta": float(volatility * 100),  # Simplified beta
                "relative_strength": "NEUTRAL"
            }
            
        except Exception as e:
            logger.error(f"Error analyzing correlations: {e}")
            return {"correlation_strength": 0.0, "market_beta": 1.0}
    
    def _create_error_response(self, symbol: str, error_msg: str) -> Dict[str, Any]:
        """Create error response structure."""
        return {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "overall_signal": "ERROR",
            "signal_strength": 0.0,
            "key_insights": [f"Analysis error: {error_msg}"],
            "data_layers": {},
            "advanced_indicators_client_side": {},
            "risk_controls_client_side": {},
            "market_context": {}
        }Error in final analysis for {symbol}: {e}")
            return self._create_error_response(symbol, str(e))
    
    def _generate_trading_signal(self, 
                               technical_indicators: Dict[str, Any],
                               vwap_analysis: Dict[str, float],
                               order_book_analysis: Dict[str, float],
                               momentum_analysis: Dict[str, float],
                               ohlcv_df: pd.DataFrame) -> tuple:
        """Generate overall trading signal with strength and insights."""
        try:
            signals = []
            insights = []
            
            # Technical indicators signal
            tech_signal = self._evaluate_technical_signals(technical_indicators)
            signals.append(tech_signal * self.signal_weights['technical_indicators'])
            
            # VWAP signal
            if not ohlcv_df.empty:
                current_price = ohlcv_df['close'].iloc[-1]
                vwap_price = vwap_analysis.get('vwap', current_price)
                
                if current_price > vwap_price:
                    vwap_signal = 1
                    insights.append(f"Price above VWAP ({vwap_price:.2f}) - bullish bias")
                else:
                    vwap_signal = -1
                    insights.append(f"Price below VWAP ({vwap_price:.2f}) - bearish bias")
            else:
                vwap_signal = 0
            
            # Order book signal
            imbalance = order_book_analysis.get('order_flow_imbalance_pct', 0)
            if imbalance > 10:
                ob_signal = 1
                insights.append(f"Strong bid imbalance ({imbalance:.1f}%) - buying pressure")
            elif imbalance < -10:
                ob_signal = -1
                insights.append(f"Strong ask imbalance ({imbalance:.1f}%) - selling pressure")
            else:
                ob_signal = 0
            
            signals.append(ob_signal * self.signal_weights['order_book'])
            
            # Momentum signal
            momentum_5 = momentum_analysis.get('momentum_5', 0)
            if momentum_5 > 0.5:
                mom_signal = 1
                insights.append(f"Positive momentum ({momentum_5:.2f}%)")
            elif momentum_5 < -0.5:
                mom_signal = -1
                insights.append(f"Negative momentum ({momentum_5:.2f}%)")
            else:
                mom_signal = 0
            
            signals.append(mom_signal * self.signal_weights['momentum'])
            
            # Volume signal
            volume_signal = self._evaluate_volume_signal(ohlcv_df)
            signals.append(volume_signal * self.signal_weights['volume'])
            
            # Calculate overall signal
            overall_score = sum(signals)
            signal_strength = abs(overall_score) * 100
            
            if overall_score > 0.3:
                overall_signal = "BUY"
            elif overall_score < -0.3:
                overall_signal = "SELL"
            else:
                overall_signal = "NEUTRAL"
            
            return overall_signal, signal_strength, insights
            
        except Exception as e:
            logger.error(f"Error generating trading signal: {e}")
            return "NEUTRAL", 0.0, ["Error in signal generation"]
    
    def _evaluate_technical_signals(self, indicators: Dict[str, Any]) -> float:
        """Evaluate technical indicators and return signal strength."""
        try:
            signal = 0
            count = 0
            
            # RSI signal
            rsi = indicators.get('rsi', 50)
            if rsi < 30:
                signal += 1  # Oversold - buy signal
            elif rsi > 70:
                signal -= 1  # Overbought - sell signal
            count += 1
            
            # MACD signal
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            if macd > macd_signal:
                signal += 1
            elif macd < macd_signal:
                signal -= 1
            count += 1
            
            # Bollinger Bands
            bb_upper = indicators.get('bb_upper', 0)
            bb_lower = indicators.get('bb_lower', 0)
            bb_middle = indicators.get('bb_middle', 0)
            close_price = indicators.get('close', bb_middle)
            
            if close_price > bb_upper:
                signal -= 1  # Overbought
            elif close_price < bb_lower:
                signal += 1  # Oversold
            count += 1
            
            return signal / count if count > 0 else 0
            
        except Exception as e:
            logger.error(f"Error evaluating technical signals: {e}")
            return 0
    
    def _evaluate_volume_signal(self, ohlcv_df: pd.DataFrame) -> float:
        """Evaluate volume-based signals."""
        try:
            if len(ohlcv_df) < 2:
                return 0
            
            current_volume = ohlcv_df['volume'].iloc[-1]
            avg_volume = ohlcv_df['volume'].rolling(20).mean().iloc[-1]
            
            if current_volume > avg_volume * 1.5:
                return 1  # High volume - trend confirmation
            elif current_volume < avg_volume * 0.5:
                return -1  # Low volume - trend weakness
            else:
                return 0
                
        except Exception as e:
            logger.error(f"