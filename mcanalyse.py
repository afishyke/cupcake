import pandas as pd
import numpy as np
import talib
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TechnicalAnalyzer:
    """Efficient Technical Analysis class for processing OHLCV data"""
    
    def analyze_stock(self, symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a single stock and return compact results"""
        try:
            # Convert to numpy arrays
            close = np.array(data['close'], dtype=float)
            high = np.array(data['high'], dtype=float)
            low = np.array(data['low'], dtype=float)
            open_prices = np.array(data['open'], dtype=float)
            volume = np.array(data['volume'], dtype=float)
            
            # Get current values (latest)
            current_price = close[-1]
            prev_price = close[-2] if len(close) > 1 else current_price
            price_change = current_price - prev_price
            price_change_pct = (price_change / prev_price) * 100 if prev_price != 0 else 0
            
            # Calculate key indicators efficiently
            analysis = {
                "symbol": symbol,
                "price": {
                    "current": round(current_price, 2),
                    "change": round(price_change, 2),
                    "change_pct": round(price_change_pct, 2),
                    "high_52w": round(np.max(high), 2),
                    "low_52w": round(np.min(low), 2)
                },
                "moving_averages": self._get_ma_signals(close),
                "momentum": self._get_momentum_signals(high, low, close),
                "volatility": self._get_volatility_signals(high, low, close),
                "volume": self._get_volume_signals(volume, close),
                "signals": {}
            }
            
            # Generate trading signals
            analysis["signals"] = self._generate_signals(analysis)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            return {"symbol": symbol, "error": str(e)}
    
    def _get_ma_signals(self, close: np.array) -> Dict[str, Any]:
        """Get moving average signals"""
        try:
            sma20 = talib.SMA(close, 20)[-1]
            sma50 = talib.SMA(close, 50)[-1]
            ema12 = talib.EMA(close, 12)[-1]
            ema26 = talib.EMA(close, 26)[-1]
            
            current = close[-1]
            
            return {
                "sma20": round(sma20, 2) if not np.isnan(sma20) else None,
                "sma50": round(sma50, 2) if not np.isnan(sma50) else None,
                "price_vs_sma20": "above" if current > sma20 else "below",
                "price_vs_sma50": "above" if current > sma50 else "below",
                "sma_trend": "bullish" if sma20 > sma50 else "bearish"
            }
        except:
            return {}
    
    def _get_momentum_signals(self, high: np.array, low: np.array, close: np.array) -> Dict[str, Any]:
        """Get momentum indicators"""
        try:
            rsi = talib.RSI(close, 14)[-1]
            macd, macd_signal, macd_hist = talib.MACD(close)
            stoch_k, stoch_d = talib.STOCH(high, low, close)
            
            return {
                "rsi": round(rsi, 1) if not np.isnan(rsi) else None,
                "rsi_signal": "oversold" if rsi < 30 else "overbought" if rsi > 70 else "neutral",
                "macd": round(macd[-1], 3) if not np.isnan(macd[-1]) else None,
                "macd_signal": "bullish" if macd[-1] > macd_signal[-1] else "bearish",
                "stoch": round(stoch_k[-1], 1) if not np.isnan(stoch_k[-1]) else None,
                "stoch_signal": "oversold" if stoch_k[-1] < 20 else "overbought" if stoch_k[-1] > 80 else "neutral"
            }
        except:
            return {}
    
    def _get_volatility_signals(self, high: np.array, low: np.array, close: np.array) -> Dict[str, Any]:
        """Get volatility indicators"""
        try:
            bb_upper, bb_middle, bb_lower = talib.BBANDS(close, 20, 2, 2)
            atr = talib.ATR(high, low, close, 14)[-1]
            
            current = close[-1]
            bb_pos = (current - bb_lower[-1]) / (bb_upper[-1] - bb_lower[-1]) * 100
            
            return {
                "atr": round(atr, 2) if not np.isnan(atr) else None,
                "bb_position": round(bb_pos, 1) if not np.isnan(bb_pos) else None,
                "bb_signal": "overbought" if bb_pos > 80 else "oversold" if bb_pos < 20 else "neutral"
            }
        except:
            return {}
    
    def _get_volume_signals(self, volume: np.array, close: np.array) -> Dict[str, Any]:
        """Get volume-based signals"""
        try:
            vol_sma = talib.SMA(volume, 20)
            current_vol = volume[-1]
            avg_vol = vol_sma[-1]
            
            # On-Balance Volume
            obv = talib.OBV(close, volume)[-1]
            
            return {
                "current": int(current_vol),
                "avg_20d": int(avg_vol) if not np.isnan(avg_vol) else None,
                "volume_ratio": round(current_vol / avg_vol, 2) if not np.isnan(avg_vol) else None,
                "obv_trend": "positive" if obv > 0 else "negative"
            }
        except:
            return {}
    
    def _generate_signals(self, analysis: Dict[str, Any]) -> Dict[str, str]:
        """Generate overall trading signals"""
        signals = {"overall": "neutral", "strength": "weak"}
        
        try:
            bullish_count = 0
            bearish_count = 0
            
            # MA signals
            if analysis.get("moving_averages", {}).get("sma_trend") == "bullish":
                bullish_count += 1
            else:
                bearish_count += 1
                
            # RSI signals
            rsi_signal = analysis.get("momentum", {}).get("rsi_signal")
            if rsi_signal == "oversold":
                bullish_count += 1
            elif rsi_signal == "overbought":
                bearish_count += 1
                
            # MACD signals
            if analysis.get("momentum", {}).get("macd_signal") == "bullish":
                bullish_count += 1
            else:
                bearish_count += 1
                
            # Volume confirmation
            vol_ratio = analysis.get("volume", {}).get("volume_ratio", 1)
            if vol_ratio > 1.5:
                signals["volume_confirmation"] = "strong"
            elif vol_ratio > 1.2:
                signals["volume_confirmation"] = "moderate"
            else:
                signals["volume_confirmation"] = "weak"
                
            # Overall signal
            if bullish_count > bearish_count:
                signals["overall"] = "bullish"
                signals["strength"] = "strong" if bullish_count >= 3 else "moderate"
            elif bearish_count > bullish_count:
                signals["overall"] = "bearish"
                signals["strength"] = "strong" if bearish_count >= 3 else "moderate"
                
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            
        return signals

def analyze_single_stock(symbol_data: tuple) -> Dict[str, Any]:
    """Function to analyze a single stock (for ThreadPoolExecutor)"""
    symbol, data = symbol_data
    analyzer = TechnicalAnalyzer()
    return analyzer.analyze_stock(symbol, data)

def analyze_mcfetch_data(raw_data: Dict[str, Any]) -> None:
    """
    Main function to analyze raw OHLCV data and generate JSON report
    
    Args:
        raw_data: Dictionary with stock symbols as keys and OHLCV data as values
    """
    logger.info(f"Starting analysis of {len(raw_data)} stocks")
    
    # Prepare data for parallel processing
    stock_items = list(raw_data.items())
    
    # Process stocks in parallel
    results = {}
    successful_analyses = 0
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_symbol = {
            executor.submit(analyze_single_stock, item): item[0] 
            for item in stock_items
        }
        
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                result = future.result()
                if "error" not in result:
                    results[symbol] = result
                    successful_analyses += 1
                else:
                    logger.error(f"Failed to analyze {symbol}: {result['error']}")
            except Exception as e:
                logger.error(f"Exception analyzing {symbol}: {e}")
    
    # Generate summary statistics
    summary = generate_summary(results)
    
    # Create final report structure
    report = {
        "metadata": {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_stocks": len(raw_data),
            "analyzed_stocks": successful_analyses,
            "failed_stocks": len(raw_data) - successful_analyses
        },
        "summary": summary,
        "stocks": results
    }
    
    # Save to file
    filename = f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Analysis complete. Report saved to {filename}")
        logger.info(f"Successfully analyzed {successful_analyses}/{len(raw_data)} stocks")
    except Exception as e:
        logger.error(f"Error saving report: {e}")

def generate_summary(results: Dict[str, Any]) -> Dict[str, Any]:
    """Generate summary statistics from analysis results"""
    if not results:
        return {"error": "No successful analyses"}
    
    try:
        # Top performers by price change
        sorted_by_change = sorted(
            results.items(),
            key=lambda x: x[1].get("price", {}).get("change_pct", 0),
            reverse=True
        )
        
        # Filter bullish signals
        bullish_stocks = [
            symbol for symbol, data in results.items()
            if data.get("signals", {}).get("overall") == "bullish"
        ]
        
        # Filter bearish signals
        bearish_stocks = [
            symbol for symbol, data in results.items()
            if data.get("signals", {}).get("overall") == "bearish"
        ]
        
        # High volume stocks
        high_volume_stocks = [
            symbol for symbol, data in results.items()
            if data.get("volume", {}).get("volume_ratio", 0) > 1.5
        ]
        
        return {
            "top_gainers": [
                {
                    "symbol": item[0],
                    "change_pct": item[1].get("price", {}).get("change_pct", 0)
                }
                for item in sorted_by_change[:5]
            ],
            "top_losers": [
                {
                    "symbol": item[0],
                    "change_pct": item[1].get("price", {}).get("change_pct", 0)
                }
                for item in sorted_by_change[-5:]
            ],
            "bullish_signals": len(bullish_stocks),
            "bearish_signals": len(bearish_stocks),
            "neutral_signals": len(results) - len(bullish_stocks) - len(bearish_stocks),
            "high_volume_activity": len(high_volume_stocks),
            "strong_buy_candidates": [
                symbol for symbol, data in results.items()
                if (data.get("signals", {}).get("overall") == "bullish" and
                    data.get("signals", {}).get("strength") == "strong")
            ][:5]
        }
        
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        return {"error": "Failed to generate summary"}

if __name__ == "__main__":
    # Test function - normally called by run_analysis_pipeline.py
    logger.info("mcanalyse.py - Technical Analysis Module")
    logger.info("This module should be called by run_analysis_pipeline.py")