#!/usr/bin/env python3
"""
technicaldata_batch.py

An extended version of technicaldata.py that integrates a batch‐mode “analyze many stocks at once” feature.
It uses TA‐Lib and yfinance for indicators, MoneyControl for intraday data, and outputs a “smartly arranged” JSON.

Dependencies (install via pip if needed):
    pip install yfinance pandas numpy talib requests
"""

import sys
import requests
import yfinance as yf
import pandas as pd
import numpy as np
import talib
import time
import json

from datetime import datetime, timedelta
from typing import Dict, Optional, List
from dataclasses import dataclass
from enum import Enum

import warnings
warnings.filterwarnings('ignore')


# ─── ENUMS & DATACLASSES ───────────────────────────────────────────────────────

class Signal(Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    NEUTRAL = "NEUTRAL"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"


@dataclass
class TradingSignal:
    signal: Signal
    confidence: float
    reasons: List[str]
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    risk_reward_ratio: float


# ─── CORE ANALYZER CLASS ────────────────────────────────────────────────────────

class ImprovedTradingAnalyzer:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://www.moneycontrol.com/"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Indicator weights
        self.weights = {
            'trend': 0.30,
            'momentum': 0.25,
            'volume': 0.20,
            'volatility': 0.15,
            'support_resistance': 0.10
        }
        
        # Cache for data (to avoid re-fetching too often)
        self.data_cache = {}
        self.cache_duration = 300  # 5 minutes cache (in seconds)
    
    def get_market_hours_info(self):
        """Get current market status and adjust for MoneyControl's 5-minute delay"""
        now = datetime.now()
        current_time = now.time()
        
        # IST Market hours: 9:15 AM to 3:30 PM
        market_open = datetime.strptime("09:15", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()
        
        is_market_hours = (market_open <= current_time <= market_close)
        
        # MoneyControl has a 5-minute delay during market hours
        if is_market_hours:
            adjusted_time = now - timedelta(minutes=5)
            if adjusted_time.time() < market_open:
                # If 5 minutes ago was before market open, use previous day's close
                adjusted_time = (now - timedelta(days=1)).replace(hour=15, minute=30)
        else:
            # After market close: use today’s 15:30; before open: previous day’s 15:30
            if current_time > market_close:
                adjusted_time = now.replace(hour=15, minute=30)
            else:
                adjusted_time = (now - timedelta(days=1)).replace(hour=15, minute=30)
        
        return {
            'is_market_hours': is_market_hours,
            'current_time': now,
            'adjusted_time': adjusted_time,
            'data_delay': '5 minutes' if is_market_hours else 'Previous close'
        }
    
    def fetch_moneycontrol_data(self, symbol: str, resolution: str = "5") -> Optional[pd.DataFrame]:
        """Fetch intraday data from MoneyControl with proper delay handling."""
        cache_key = f"MC_{symbol.upper()}_{resolution}_{int(time.time() // self.cache_duration)}"
        if cache_key in self.data_cache:
            # Return a copy of cached DataFrame
            return self.data_cache[cache_key].copy()
        
        try:
            market_info = self.get_market_hours_info()
            end_time = market_info['adjusted_time']
            start_time = end_time - timedelta(days=5)  # 5 calendar days to ensure 3 trading days
            
            from_epoch = int(start_time.timestamp())
            to_epoch = int(end_time.timestamp())
            
            url = (
                "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"
                f"?symbol={symbol.upper()}&resolution={resolution}"
                f"&from={from_epoch}&to={to_epoch}&countback=500&currencyCode=INR"
            )
            
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                return None
            
            data = response.json()
            if data.get("s") in ("no_data", "error"):
                return None
            
            required_fields = ["o", "h", "l", "c", "v", "t"]
            if not all(field in data for field in required_fields):
                return None
            if not all(len(data[field]) > 0 for field in required_fields):
                return None
            
            df = pd.DataFrame({
                'open': data["o"],
                'high': data["h"],
                'low': data["l"],
                'close': data["c"],
                'volume': data["v"]
            }, index=pd.to_datetime(data["t"], unit='s'))
            
            df = df.sort_index().dropna()
            if len(df) < 50:
                return None
            
            # Cache and return
            self.data_cache[cache_key] = df.copy()
            return df
        
        except Exception:
            return None
    
    def fetch_yfinance_data(self, symbol: str, period: str = "6mo") -> Optional[pd.DataFrame]:
        """Fetch daily data from Yahoo Finance (NSE/BSE) – for general outlook only."""
        cache_key = f"YF_{symbol.upper()}_{period}_{int(time.time() // (self.cache_duration * 12))}"
        if cache_key in self.data_cache:
            return self.data_cache[cache_key].copy()
        
        symbol_variations = [
            f"{symbol.upper()}.NS",  # NSE
            f"{symbol.upper()}.BO",  # BSE
            symbol.upper()
        ]
        
        for sym_variant in symbol_variations:
            try:
                ticker = yf.Ticker(sym_variant)
                data = ticker.history(period=period, timeout=15)
                if data.empty or len(data) < 100:
                    continue
                
                data.columns = [col.lower().replace(' ', '_') for col in data.columns]
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                data = data.dropna(subset=required_cols)
                
                if len(data) >= 100:
                    self.data_cache[cache_key] = data.copy()
                    return data
            except Exception:
                continue
        
        return None
    
    def calculate_talib_indicators(self, df: pd.DataFrame) -> Dict:
        """Calculate all technical indicators using TA‐Lib."""
        if df is None or len(df) < 50:
            return self._get_empty_indicators()
        
        try:
            high = df['high'].values.astype(float)
            low = df['low'].values.astype(float)
            close = df['close'].values.astype(float)
            open_price = df['open'].values.astype(float)
            volume = df['volume'].values.astype(float)
            
            indicators = {}
            # === TREND INDICATORS ===
            indicators['SMA_5'] = talib.SMA(close, timeperiod=5)
            indicators['SMA_10'] = talib.SMA(close, timeperiod=10)
            indicators['SMA_20'] = talib.SMA(close, timeperiod=20)
            indicators['SMA_50'] = talib.SMA(close, timeperiod=50)
            indicators['SMA_200'] = talib.SMA(close, timeperiod=200) if len(close) >= 200 else None
            
            indicators['EMA_9'] = talib.EMA(close, timeperiod=9)
            indicators['EMA_21'] = talib.EMA(close, timeperiod=21)
            indicators['EMA_50'] = talib.EMA(close, timeperiod=50)
            
            macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
            indicators['MACD'] = macd
            indicators['MACD_SIGNAL'] = macd_signal
            indicators['MACD_HIST'] = macd_hist
            
            indicators['ADX'] = talib.ADX(high, low, close, timeperiod=14)
            indicators['SAR'] = talib.SAR(high, low, acceleration=0.02, maximum=0.2)
            
            # === MOMENTUM INDICATORS ===
            indicators['RSI_14'] = talib.RSI(close, timeperiod=14)
            indicators['RSI_21'] = talib.RSI(close, timeperiod=21)
            
            slowk, slowd = talib.STOCH(high, low, close, fastk_period=5, slowk_period=3, slowd_period=3)
            indicators['STOCH_K'] = slowk
            indicators['STOCH_D'] = slowd
            
            indicators['WILLR'] = talib.WILLR(high, low, close, timeperiod=14)
            indicators['ROC'] = talib.ROC(close, timeperiod=10)
            indicators['CCI'] = talib.CCI(high, low, close, timeperiod=14)
            indicators['MFI'] = talib.MFI(high, low, close, volume, timeperiod=14)
            
            # === VOLATILITY INDICATORS ===
            bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
            indicators['BB_UPPER'] = bb_upper
            indicators['BB_MIDDLE'] = bb_middle
            indicators['BB_LOWER'] = bb_lower
            indicators['ATR'] = talib.ATR(high, low, close, timeperiod=14)
            
            # === VOLUME INDICATORS ===
            indicators['OBV'] = talib.OBV(close, volume)
            indicators['AD'] = talib.AD(high, low, close, volume)
            indicators['ADOSC'] = talib.ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10)
            
            # === CANDLESTICK PATTERNS ===
            patterns = {}
            pattern_functions = {
                'DOJI': talib.CDLDOJI,
                'HAMMER': talib.CDLHAMMER,
                'INVERTED_HAMMER': talib.CDLINVERTEDHAMMER,
                'SHOOTING_STAR': talib.CDLSHOOTINGSTAR,
                'HANGING_MAN': talib.CDLHANGINGMAN,
                'ENGULFING': talib.CDLENGULFING,
                'HARAMI': talib.CDLHARAMI,
                'PIERCING': talib.CDLPIERCING,
                'DARK_CLOUD': talib.CDLDARKCLOUDCOVER,
                'MORNING_STAR': talib.CDLMORNINGSTAR,
                'EVENING_STAR': talib.CDLEVENINGSTAR,
                'THREE_WHITE_SOLDIERS': talib.CDL3WHITESOLDIERS,
                'THREE_BLACK_CROWS': talib.CDL3BLACKCROWS
            }
            for pattern_name, pattern_func in pattern_functions.items():
                try:
                    patterns[pattern_name] = pattern_func(open_price, high, low, close)
                except Exception:
                    patterns[pattern_name] = np.zeros(len(close))
            indicators['PATTERNS'] = patterns
            
            # === DERIVED CALCULATIONS ===
            indicators['CURRENT_PRICE'] = close[-1]
            indicators['PREV_CLOSE'] = close[-2] if len(close) > 1 else close[-1]
            indicators['PRICE_CHANGE'] = indicators['CURRENT_PRICE'] - indicators['PREV_CLOSE']
            indicators['PRICE_CHANGE_PCT'] = (indicators['PRICE_CHANGE'] / indicators['PREV_CLOSE']) * 100
            
            if indicators['BB_UPPER'][-1] != indicators['BB_LOWER'][-1]:
                indicators['BB_POSITION'] = (
                    (close[-1] - indicators['BB_LOWER'][-1]) /
                    (indicators['BB_UPPER'][-1] - indicators['BB_LOWER'][-1])
                ) * 100
            else:
                indicators['BB_POSITION'] = 50
            
            vol_sma_20 = talib.SMA(volume, timeperiod=20)
            indicators['VOL_SMA_20'] = vol_sma_20
            indicators['VOL_RATIO'] = volume[-1] / vol_sma_20[-1] if vol_sma_20[-1] != 0 else 1
            
            window = min(20, len(close) // 4)
            recent_high = np.max(high[-window:])
            recent_low = np.min(low[-window:])
            indicators['SUPPORT'] = recent_low
            indicators['RESISTANCE'] = recent_high
            
            return indicators
        
        except Exception:
            return self._get_empty_indicators()
    
    def _get_empty_indicators(self):
        """Return a fallback dictionary of indicators when data is missing or errors occur."""
        return {
            'CURRENT_PRICE': 0,
            'PREV_CLOSE': 0,
            'PRICE_CHANGE': 0,
            'PRICE_CHANGE_PCT': 0,
            'RSI_14': np.array([50]),
            'MACD': np.array([0]),
            'MACD_SIGNAL': np.array([0]),
            'BB_POSITION': 50,
            'VOL_RATIO': 1,
            'SUPPORT': 0,
            'RESISTANCE': 0,
            'ATR': np.array([1]),
            'PATTERNS': {}
        }
    
    def generate_signals(self, daily_indicators: Dict, intraday_indicators: Dict) -> TradingSignal:
        """Generate a combined trading signal using multiple timeframe analysis."""
        def get_latest_value(indicator_array, default=0):
            if isinstance(indicator_array, np.ndarray) and len(indicator_array) > 0:
                return float(indicator_array[-1]) if not np.isnan(indicator_array[-1]) else default
            try:
                return float(indicator_array)
            except:
                return default
        
        current_price = daily_indicators.get('CURRENT_PRICE', 0)
        if current_price == 0:
            # No price → neutral
            return self._get_neutral_signal(current_price)
        
        signals = []
        scores = []
        
        # === TREND ANALYSIS (30%) ===
        trend_score = 0
        sma_20 = get_latest_value(daily_indicators.get('SMA_20', []), current_price)
        sma_50 = get_latest_value(daily_indicators.get('SMA_50', []), current_price)
        if current_price > sma_20 > sma_50:
            signals.append("Bullish MA alignment")
            trend_score += 20
        elif current_price < sma_20 < sma_50:
            signals.append("Bearish MA alignment")
            trend_score -= 20
        
        macd = get_latest_value(daily_indicators.get('MACD', []))
        macd_signal = get_latest_value(daily_indicators.get('MACD_SIGNAL', []))
        macd_hist = get_latest_value(daily_indicators.get('MACD_HIST', []))
        if macd > macd_signal and macd_hist > 0:
            signals.append("MACD bullish")
            trend_score += 15
        elif macd < macd_signal and macd_hist < 0:
            signals.append("MACD bearish")
            trend_score -= 15
        
        adx = get_latest_value(daily_indicators.get('ADX', []), 25)
        if adx > 25:
            signals.append(f"Strong trend (ADX: {adx:.1f})")
            if trend_score > 0:
                trend_score += 10
            else:
                trend_score -= 5
        
        scores.append(trend_score * self.weights['trend'])
        
        # === MOMENTUM ANALYSIS (25%) ===
        momentum_score = 0
        rsi = get_latest_value(daily_indicators.get('RSI_14', []), 50)
        if rsi < 30:
            signals.append(f"RSI oversold ({rsi:.1f})")
            momentum_score += 25
        elif rsi > 70:
            signals.append(f"RSI overbought ({rsi:.1f})")
            momentum_score -= 25
        elif 40 <= rsi <= 60:
            signals.append("RSI neutral zone")
            momentum_score += 5
        
        stoch_k = get_latest_value(daily_indicators.get('STOCH_K', []), 50)
        stoch_d = get_latest_value(daily_indicators.get('STOCH_D', []), 50)
        if stoch_k < 20 and stoch_d < 20:
            signals.append("Stochastic oversold")
            momentum_score += 15
        elif stoch_k > 80 and stoch_d > 80:
            signals.append("Stochastic overbought")
            momentum_score -= 15
        
        scores.append(momentum_score * self.weights['momentum'])
        
        # === VOLUME ANALYSIS (20%) ===
        volume_score = 0
        vol_ratio = daily_indicators.get('VOL_RATIO', 1)
        if vol_ratio > 2:
            signals.append(f"Very high volume ({vol_ratio:.1f}x)")
            volume_score += 20
        elif vol_ratio > 1.5:
            signals.append(f"High volume ({vol_ratio:.1f}x)")
            volume_score += 15
        elif vol_ratio < 0.5:
            signals.append(f"Low volume ({vol_ratio:.1f}x)")
            volume_score -= 10
        
        mfi = get_latest_value(daily_indicators.get('MFI', []), 50)
        if mfi < 20:
            signals.append(f"MFI oversold ({mfi:.1f})")
            volume_score += 10
        elif mfi > 80:
            signals.append(f"MFI overbought ({mfi:.1f})")
            volume_score -= 10
        
        scores.append(volume_score * self.weights['volume'])
        
        # === VOLATILITY ANALYSIS (15%) ===
        volatility_score = 0
        bb_position = daily_indicators.get('BB_POSITION', 50)
        if bb_position < 10:
            signals.append(f"BB lower band ({bb_position:.1f}%)")
            volatility_score += 20
        elif bb_position > 90:
            signals.append(f"BB upper band ({bb_position:.1f}%)")
            volatility_score -= 20
        
        scores.append(volatility_score * self.weights['volatility'])
        
        # === SUPPORT/RESISTANCE ANALYSIS (10%) ===
        sr_score = 0
        support = daily_indicators.get('SUPPORT', current_price * 0.95)
        resistance = daily_indicators.get('RESISTANCE', current_price * 1.05)
        support_dist = ((current_price - support) / support) * 100 if support != 0 else 0
        resistance_dist = ((resistance - current_price) / current_price) * 100 if current_price != 0 else 0
        
        if support_dist < 2:
            signals.append(f"Near support (₹{support:.2f})")
            sr_score += 15
        elif resistance_dist < 2:
            signals.append(f"Near resistance (₹{resistance:.2f})")
            sr_score -= 15
        
        scores.append(sr_score * self.weights['support_resistance'])
        
        # === FINAL SIGNAL CALCULATION ===
        total_score = sum(scores)
        confidence = max(0, min(100, 50 + total_score))
        
        if confidence >= 75:
            signal_type = Signal.STRONG_BUY
        elif confidence >= 60:
            signal_type = Signal.BUY
        elif confidence <= 25:
            signal_type = Signal.STRONG_SELL
        elif confidence <= 40:
            signal_type = Signal.SELL
        else:
            signal_type = Signal.NEUTRAL
        
        atr = get_latest_value(daily_indicators.get('ATR', []), current_price * 0.02)
        if signal_type in (Signal.BUY, Signal.STRONG_BUY):
            entry_price = current_price
            stop_loss = current_price - (2 * atr)
            target_1 = current_price + (1.5 * atr)
            target_2 = current_price + (3 * atr)
        elif signal_type in (Signal.SELL, Signal.STRONG_SELL):
            entry_price = current_price
            stop_loss = current_price + (2 * atr)
            target_1 = current_price - (1.5 * atr)
            target_2 = current_price - (3 * atr)
        else:
            entry_price = current_price
            stop_loss = current_price
            target_1 = current_price
            target_2 = current_price
        
        risk_reward = abs(target_1 - entry_price) / abs(stop_loss - entry_price) if (stop_loss != entry_price) else 1
        
        return TradingSignal(
            signal=signal_type,
            confidence=round(confidence, 1),
            reasons=signals[:10],
            entry_price=round(entry_price, 2),
            stop_loss=round(stop_loss, 2),
            target_1=round(target_1, 2),
            target_2=round(target_2, 2),
            risk_reward_ratio=round(risk_reward, 2)
        )
    
    def _get_neutral_signal(self, price: float) -> TradingSignal:
        """Return a neutral signal if analysis fails."""
        return TradingSignal(
            signal=Signal.NEUTRAL,
            confidence=50.0,
            reasons=["Insufficient data for analysis"],
            entry_price=price,
            stop_loss=price,
            target_1=price,
            target_2=price,
            risk_reward_ratio=1.0
        )
    
    def analyze_stock(self, symbol: str) -> Dict:
        """Main analysis function which returns a dict of results for one stock."""
        # Fetch data
        daily_df = self.fetch_yfinance_data(symbol, period="6mo")
        intraday_df = self.fetch_moneycontrol_data(symbol, resolution="5")
        
        results: Dict = {
            "symbol": symbol.upper(),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "daily_indicators": {},
            "intraday_indicators": {},
            "trading_signal": {},
            "data_status": {
                "daily_data": bool(daily_df is not None and len(daily_df) > 0),
                "intraday_data": bool(intraday_df is not None and len(intraday_df) > 0),
                "daily_candles": len(daily_df) if daily_df is not None else 0,
                "intraday_candles": len(intraday_df) if intraday_df is not None else 0
            }
        }
        
        # Calculate indicators (or fallback to empty)
        if daily_df is not None:
            results["daily_indicators"] = self.calculate_talib_indicators(daily_df)
            results["daily_indicators"]["TIMESTAMP"] = daily_df.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        else:
            results["daily_indicators"] = self._get_empty_indicators()
        
        if intraday_df is not None:
            results["intraday_indicators"] = self.calculate_talib_indicators(intraday_df)
        else:
            # If no intraday, reuse daily indicators
            results["intraday_indicators"] = results["daily_indicators"].copy()
        
        # Generate trading signal
        ts = self.generate_signals(results["daily_indicators"], results["intraday_indicators"])
        results["trading_signal"] = {
            "signal": ts.signal.value,
            "confidence": ts.confidence,
            "reasons": ts.reasons,
            "entry_price": ts.entry_price,
            "stop_loss": ts.stop_loss,
            "target_1": ts.target_1,
            "target_2": ts.target_2,
            "risk_reward_ratio": ts.risk_reward_ratio
        }
        
        return results


# ─── BATCH & INTERACTIVE MAIN ─────────────────────────────────────────────────

def main():
    """
    If command-line arguments are passed, assume they are stock symbols and run batch mode.
    Otherwise, enter an interactive prompt.
    
    Usage:
      python technicaldata_batch.py AAPL MSFT GOOG   # batch for AAPL, MSFT, GOOG
      python technicaldata_batch.py                  # interactive mode
    """
    analyzer = ImprovedTradingAnalyzer()
    
    # If user passed symbols on CLI, run batch:
    if len(sys.argv) > 1:
        # Skip script name
        symbols = [arg.upper() for arg in sys.argv[1:]]
        
        batch_results = []
        start_time = time.time()
        
        for sym in symbols:
            result = analyzer.analyze_stock(sym)
            daily = result["daily_indicators"]
            trading_signal = result["trading_signal"]
            data_status = result["data_status"]
            
            # Helper to grab single latest float
            def latest(val, default=0.0):
                try:
                    if isinstance(val, (list, tuple)) or hasattr(val, "__iter__"):
                        return float(val[-1])
                    return float(val)
                except:
                    return default
            
            summary = {
                "symbol": result["symbol"],
                "timestamp": result["timestamp"],
                "data_status": data_status,
                "trading_signal": trading_signal,
                "price_data": {
                    "current_price": latest(daily.get("CURRENT_PRICE")),
                    "price_change": latest(daily.get("PRICE_CHANGE")),
                    "price_change_pct": latest(daily.get("PRICE_CHANGE_PCT")),
                    "rsi_14": latest(daily.get("RSI_14", [50])),
                    "macd": latest(daily.get("MACD", [0])),
                    "bb_position": latest(daily.get("BB_POSITION")),
                    "vol_ratio": latest(daily.get("VOL_RATIO")),
                    "support": latest(daily.get("SUPPORT")),
                    "resistance": latest(daily.get("RESISTANCE"))
                }
            }
            
            batch_results.append(summary)
        
        duration = time.time() - start_time
        output = {
            "analysis_run_time": f"{duration:.2f} seconds",
            "stocks_analyzed": len(symbols),
            "results": batch_results
        }
        
        # Print JSON to stdout:
        print(json.dumps(output, indent=2))
        
        # Save to disk:
        out_filename = f"batch_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(out_filename, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\n✅ Batch results saved to {out_filename}")
        return
    
    # Otherwise, interactive mode (same as original):
    print("🚀 Indian Stock Technical Analyzer (Interactive Mode)")
    print("====================================================")
    while True:
        try:
            symbol = input("\n📈 Enter stock symbol (or 'quit' to exit): ").strip().upper()
            if symbol.lower() in ['quit', 'exit', 'q']:
                print("👋 Thank you for using the analyzer!")
                break
            if not symbol:
                print("❌ Please enter a valid stock symbol")
                continue
            
            results = analyzer.analyze_stock(symbol)
            
            # Print a detailed report (same as original print_analysis_results)
            print("\n" + "=" * 80)
            print(f"📊 TECHNICAL ANALYSIS REPORT: {results['symbol']}")
            print(f"⏰ Generated: {results['timestamp']}")
            print("=" * 80)
            
            data_status = results["data_status"]
            print(f"\n📋 DATA STATUS:")
            print(f"   Daily Data: {'✅' if data_status['daily_data'] else '❌'} ({data_status['daily_candles']} candles)")
            print(f"   Intraday Data: {'✅' if data_status['intraday_data'] else '❌'} ({data_status['intraday_candles']} candles)")
            
            ts = results["trading_signal"]
            emoji_map = {
                "STRONG_BUY": "🟢🚀", "BUY": "🟢",
                "NEUTRAL": "🟡", "SELL": "🔴", "STRONG_SELL": "🔴💥"
            }
            print(f"\n🎯 TRADING SIGNAL:")
            print(f"   Signal: {emoji_map.get(ts['signal'], '❓')} {ts['signal']}")
            print(f"   Confidence: {ts['confidence']}%")
            print(f"   Entry Price: ₹{ts['entry_price']}")
            print(f"   Stop Loss: ₹{ts['stop_loss']}")
            print(f"   Target 1: ₹{ts['target_1']}")
            print(f"   Target 2: ₹{ts['target_2']}")
            print(f"   Risk/Reward: {ts['risk_reward_ratio']}")
            
            daily = results["daily_indicators"]
            current_price = daily.get('CURRENT_PRICE', 0)
            price_change = daily.get('PRICE_CHANGE', 0)
            price_change_pct = daily.get('PRICE_CHANGE_PCT', 0)
            print(f"\n💰 PRICE DATA:")
            print(f"   Current Price: ₹{current_price:.2f}")
            print(f"   Price Time: {daily.get('TIMESTAMP', 'N/A')}")
            change_emoji = "📈" if price_change >= 0 else "📉"
            print(f"   Change: {change_emoji} ₹{price_change:.2f} ({price_change_pct:+.2f}%)")
            
            print(f"\n📊 KEY INDICATORS:")
            rsi = daily.get('RSI_14', np.array([50]))
            rsi_val = rsi[-1] if hasattr(rsi, "__iter__") and len(rsi) > 0 else 50
            rsi_status = "Oversold" if rsi_val < 30 else "Overbought" if rsi_val > 70 else "Neutral"
            print(f"   RSI (14): {rsi_val:.1f} ({rsi_status})")
            
            macd = daily.get('MACD', np.array([0]))
            macd_sig = daily.get('MACD_SIGNAL', np.array([0]))
            macd_val = macd[-1] if hasattr(macd, "__iter__") and len(macd) > 0 else 0
            macd_sig_val = macd_sig[-1] if hasattr(macd_sig, "__iter__") and len(macd_sig) > 0 else 0
            macd_status = "Bullish" if macd_val > macd_sig_val else "Bearish"
            print(f"   MACD: {macd_val:.3f} ({macd_status})")
            
            bb_pos = daily.get('BB_POSITION', 50)
            bb_status = "Lower Band" if bb_pos < 20 else "Upper Band" if bb_pos > 80 else "Middle Range"
            print(f"   BB Position: {bb_pos:.1f}% ({bb_status})")
            
            vol_ratio = daily.get('VOL_RATIO', 1)
            vol_status = (
                "Very High" if vol_ratio > 2 else
                "High" if vol_ratio > 1.5 else
                "Normal" if vol_ratio > 0.8 else
                "Low"
            )
            print(f"   Volume Ratio: {vol_ratio:.1f}x ({vol_status})")
            
            support = daily.get('SUPPORT', current_price * 0.95)
            resistance = daily.get('RESISTANCE', current_price * 1.05)
            print(f"   Support: ₹{support:.2f}")
            print(f"   Resistance: ₹{resistance:.2f}")
            
            print(f"\n🔍 ANALYSIS REASONS:")
            for idx, reason in enumerate(ts.get('reasons', []), start=1):
                print(f"   {idx}. {reason}")
            
            print(f"\n⚠️ RISK ASSESSMENT:")
            conf = ts['confidence']
            if conf >= 70:
                print("   Risk Level: 🟢 LOW (High confidence signal)")
            elif conf >= 50:
                print("   Risk Level: 🟡 MEDIUM (Moderate confidence)")
            else:
                print("   Risk Level: 🔴 HIGH (Low confidence signal)")
            
            print(f"\n💡 RECOMMENDATIONS:")
            if ts['signal'] in ['STRONG_BUY', 'BUY']:
                print("   • Consider buying with proper position sizing")
                print("   • Set stop loss strictly")
                print("   • Monitor volume and momentum")
            elif ts['signal'] in ['STRONG_SELL', 'SELL']:
                print("   • Consider selling or avoid buying")
                print("   • Wait for better entry opportunity")
                print("   • Watch for trend reversal signals")
            else:
                print("   • Stay neutral, wait for clearer signals")
                print("   • Monitor key support/resistance levels")
                print("   • Watch for volume confirmation")
            
            print("\n" + "=" * 80)
            print("⚠️  DISCLAIMER: This is for educational purposes only.")
            print("    Always do your own research before trading!")
            print("=" * 80)
            
            # Ask to save raw JSON if desired
            save_choice = input("\n💾 Save detailed JSON results to file? (y/n): ").strip().lower()
            if save_choice in ['y', 'yes']:
                filename = f"{symbol}_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                # Convert any numpy arrays to lists
                def convert_numpy(obj):
                    if isinstance(obj, np.ndarray):
                        return obj.tolist()
                    elif isinstance(obj, dict):
                        return {k: convert_numpy(v) for k, v in obj.items()}
                    elif isinstance(obj, list):
                        return [convert_numpy(v) for v in obj]
                    else:
                        return obj
                
                serializable = {k: convert_numpy(v) for k, v in results.items()}
                with open(filename, 'w') as f:
                    json.dump(serializable, f, indent=2, default=str)
                print(f"✅ Detailed JSON saved to {filename}")
            
            cont = input("\n🔄 Analyze another stock? (y/n): ").strip().lower()
            if cont not in ['y', 'yes']:
                print("👋 Thank you for using the analyzer!")
                break
        
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ An error occurred: {e}")
            print("Please try again with a different symbol.")


# ─── QUICK_ANALYSIS HELPER ─────────────────────────────────────────────────────

def quick_analysis(symbol: str):
    """Helper to quickly analyze one symbol from the CLI without interaction."""
    analyzer = ImprovedTradingAnalyzer()
    results = analyzer.analyze_stock(symbol)
    # Reuse the same printing logic as in interactive mode, or just return the dict:
    return results


# ─── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
