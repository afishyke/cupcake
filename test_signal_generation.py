#!/usr/bin/env python3
"""
Test Signal Generation Without SciPy Dependencies
=================================================

This script tests the signal generation pipeline with available data,
working around missing dependencies like scipy and sklearn.
"""

import sys
import os
import json
from datetime import datetime, timedelta
import pytz

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from technical_indicators import EnhancedTechnicalIndicators
    print("✓ Technical indicators module loaded")
except ImportError as e:
    print(f"✗ Cannot load technical indicators: {e}")
    sys.exit(1)

class SimplifiedSignalGenerator:
    """Simplified signal generator without scipy dependencies"""
    
    def __init__(self):
        self.tech_indicators = EnhancedTechnicalIndicators()
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
    def calculate_rsi(self, prices, period=14):
        """Calculate RSI without scipy"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        """Calculate MACD without scipy"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal).mean()
        macd_histogram = macd - macd_signal
        
        return {
            'macd': macd.iloc[-1] if not macd.empty else 0,
            'signal': macd_signal.iloc[-1] if not macd_signal.empty else 0,
            'histogram': macd_histogram.iloc[-1] if not macd_histogram.empty else 0
        }
    
    def calculate_bollinger_bands(self, prices, period=20, std_dev=2):
        """Calculate Bollinger Bands without scipy"""
        ma = prices.rolling(period).mean()
        std = prices.rolling(period).std()
        upper = ma + (std * std_dev)
        lower = ma - (std * std_dev)
        
        return {
            'upper': upper.iloc[-1] if not upper.empty else 0,
            'middle': ma.iloc[-1] if not ma.empty else 0,
            'lower': lower.iloc[-1] if not lower.empty else 0
        }
    
    def generate_signal(self, symbol):
        """Generate trading signal for a symbol"""
        try:
            # Get OHLCV data
            df = self.tech_indicators.get_ohlcv_data(symbol, periods=50)
            
            if df.empty:
                return {
                    'symbol': symbol,
                    'signal': 'NO_DATA',
                    'confidence': 0,
                    'reasons': ['No historical data available'],
                    'timestamp': datetime.now(self.ist_timezone).isoformat()
                }
            
            # Current market data
            current_price = df['close'].iloc[-1]
            current_volume = df['volume'].iloc[-1]
            
            # Calculate technical indicators
            ma_5 = df['close'].rolling(5).mean().iloc[-1]
            ma_20 = df['close'].rolling(20).mean().iloc[-1]
            ma_50 = df['close'].rolling(50).mean().iloc[-1] if len(df) >= 50 else ma_20
            
            rsi = self.calculate_rsi(df['close']).iloc[-1]
            macd_data = self.calculate_macd(df['close'])
            bb_data = self.calculate_bollinger_bands(df['close'])
            
            # Volume analysis
            avg_volume_20 = df['volume'].rolling(20).mean().iloc[-1]
            volume_ratio = current_volume / avg_volume_20 if avg_volume_20 > 0 else 1
            
            # Price position analysis
            price_vs_ma5 = (current_price - ma_5) / ma_5 * 100
            price_vs_ma20 = (current_price - ma_20) / ma_20 * 100
            
            # Bollinger Band position
            bb_position = (current_price - bb_data['lower']) / (bb_data['upper'] - bb_data['lower']) if bb_data['upper'] != bb_data['lower'] else 0.5
            
            # Signal generation logic
            signals = []
            reasons = []
            confidence_factors = []
            
            # Trend analysis
            if ma_5 > ma_20 > ma_50:
                reasons.append("Strong uptrend (MA5 > MA20 > MA50)")
                confidence_factors.append(0.3)
                signals.append('BUY')
            elif ma_5 < ma_20 < ma_50:
                reasons.append("Strong downtrend (MA5 < MA20 < MA50)")
                confidence_factors.append(0.3)
                signals.append('SELL')
            elif ma_5 > ma_20:
                reasons.append("Short-term uptrend (MA5 > MA20)")
                confidence_factors.append(0.15)
            else:
                reasons.append("Short-term downtrend (MA5 < MA20)")
                confidence_factors.append(0.15)
            
            # RSI analysis
            if rsi < 30:
                reasons.append(f"RSI oversold ({rsi:.1f})")
                confidence_factors.append(0.25)
                signals.append('BUY')
            elif rsi > 70:
                reasons.append(f"RSI overbought ({rsi:.1f})")
                confidence_factors.append(0.25)
                signals.append('SELL')
            elif 40 <= rsi <= 60:
                reasons.append(f"RSI neutral ({rsi:.1f})")
                confidence_factors.append(0.1)
            
            # MACD analysis
            if macd_data['macd'] > macd_data['signal'] and macd_data['histogram'] > 0:
                reasons.append("MACD bullish crossover")
                confidence_factors.append(0.2)
                signals.append('BUY')
            elif macd_data['macd'] < macd_data['signal'] and macd_data['histogram'] < 0:
                reasons.append("MACD bearish crossover")
                confidence_factors.append(0.2)
                signals.append('SELL')
            
            # Bollinger Bands analysis
            if bb_position < 0.2:
                reasons.append("Price near lower Bollinger Band")
                confidence_factors.append(0.15)
                signals.append('BUY')
            elif bb_position > 0.8:
                reasons.append("Price near upper Bollinger Band")
                confidence_factors.append(0.15)
                signals.append('SELL')
            
            # Volume confirmation
            if volume_ratio > 1.5:
                reasons.append(f"High volume confirmation ({volume_ratio:.1f}x avg)")
                confidence_factors.append(0.1)
            elif volume_ratio < 0.5:
                reasons.append(f"Low volume warning ({volume_ratio:.1f}x avg)")
                confidence_factors.append(-0.1)
            
            # Determine final signal
            buy_signals = signals.count('BUY')
            sell_signals = signals.count('SELL')
            
            if buy_signals > sell_signals:
                final_signal = 'BUY'
            elif sell_signals > buy_signals:
                final_signal = 'SELL'
            else:
                final_signal = 'HOLD'
            
            # Calculate confidence
            base_confidence = sum(confidence_factors)
            final_confidence = max(0, min(1, base_confidence))
            
            # Calculate targets and stop loss
            volatility = df['close'].rolling(20).std().iloc[-1] / current_price
            
            if final_signal == 'BUY':
                target_price = current_price * (1 + volatility * 2)
                stop_loss = current_price * (1 - volatility * 1.5)
            elif final_signal == 'SELL':
                target_price = current_price * (1 - volatility * 2)
                stop_loss = current_price * (1 + volatility * 1.5)
            else:
                target_price = current_price
                stop_loss = current_price * 0.95
            
            risk_reward = abs(target_price - current_price) / abs(current_price - stop_loss) if stop_loss != current_price else 1
            
            return {
                'symbol': symbol,
                'signal': final_signal,
                'confidence': final_confidence,
                'current_price': current_price,
                'target_price': target_price,
                'stop_loss': stop_loss,
                'risk_reward_ratio': risk_reward,
                'reasons': reasons,
                'technical_data': {
                    'rsi': rsi,
                    'ma_5': ma_5,
                    'ma_20': ma_20,
                    'macd': macd_data,
                    'bollinger_position': bb_position,
                    'volume_ratio': volume_ratio,
                    'price_vs_ma5_pct': price_vs_ma5,
                    'price_vs_ma20_pct': price_vs_ma20
                },
                'market_data': {
                    'data_points': len(df),
                    'price_range': {
                        'min': df['close'].min(),
                        'max': df['close'].max()
                    },
                    'volatility': volatility
                },
                'timestamp': datetime.now(self.ist_timezone).isoformat()
            }
            
        except Exception as e:
            return {
                'symbol': symbol,
                'signal': 'ERROR',
                'confidence': 0,
                'reasons': [f'Error generating signal: {str(e)}'],
                'timestamp': datetime.now(self.ist_timezone).isoformat()
            }


def test_signal_generation():
    """Test signal generation for available symbols"""
    print("=" * 60)
    print("🔍 TESTING SIGNAL GENERATION PIPELINE")
    print("=" * 60)
    
    generator = SimplifiedSignalGenerator()
    
    # Test symbols
    test_symbols = [
        'Reliance Industries',
        'Tata Consultancy Services', 
        'HDFC Bank',
        'Infosys',
        'ICICI Bank'
    ]
    
    results = []
    
    for symbol in test_symbols:
        print(f"\n🔍 Analyzing {symbol}...")
        signal_data = generator.generate_signal(symbol)
        results.append(signal_data)
        
        # Display results
        if signal_data['signal'] != 'ERROR' and signal_data['signal'] != 'NO_DATA':
            print(f"   Signal: {signal_data['signal']}")
            print(f"   Confidence: {signal_data['confidence']:.2f}")
            print(f"   Current Price: ₹{signal_data['current_price']:.2f}")
            
            if signal_data['signal'] in ['BUY', 'SELL']:
                print(f"   Target: ₹{signal_data['target_price']:.2f}")
                print(f"   Stop Loss: ₹{signal_data['stop_loss']:.2f}")
                print(f"   Risk/Reward: {signal_data['risk_reward_ratio']:.2f}")
            
            print(f"   Technical Data:")
            tech = signal_data['technical_data']
            print(f"     RSI: {tech['rsi']:.1f}")
            print(f"     MA5: ₹{tech['ma_5']:.2f}")
            print(f"     MA20: ₹{tech['ma_20']:.2f}")
            print(f"     Volume Ratio: {tech['volume_ratio']:.2f}")
            
            print(f"   Top Reasons:")
            for i, reason in enumerate(signal_data['reasons'][:3]):
                print(f"     {i+1}. {reason}")
        else:
            print(f"   Status: {signal_data['signal']}")
            print(f"   Reason: {signal_data['reasons'][0] if signal_data['reasons'] else 'Unknown'}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SIGNAL GENERATION SUMMARY")
    print("=" * 60)
    
    buy_signals = [r for r in results if r['signal'] == 'BUY']
    sell_signals = [r for r in results if r['signal'] == 'SELL']
    hold_signals = [r for r in results if r['signal'] == 'HOLD']
    error_signals = [r for r in results if r['signal'] in ['ERROR', 'NO_DATA']]
    
    print(f"✅ Total symbols analyzed: {len(results)}")
    print(f"📈 BUY signals: {len(buy_signals)}")
    print(f"📉 SELL signals: {len(sell_signals)}")
    print(f"⏸️  HOLD signals: {len(hold_signals)}")
    print(f"❌ Errors/No Data: {len(error_signals)}")
    
    if buy_signals:
        print(f"\n🟢 TOP BUY OPPORTUNITIES:")
        for signal in sorted(buy_signals, key=lambda x: x['confidence'], reverse=True):
            print(f"   {signal['symbol']}: {signal['confidence']:.2f} confidence")
    
    if sell_signals:
        print(f"\n🔴 TOP SELL OPPORTUNITIES:")
        for signal in sorted(sell_signals, key=lambda x: x['confidence'], reverse=True):
            print(f"   {signal['symbol']}: {signal['confidence']:.2f} confidence")
    
    # Check for common issues
    print(f"\n🔧 DIAGNOSTIC INFORMATION:")
    print(f"   Data availability: {len(results) - len(error_signals)}/{len(results)} symbols have data")
    
    if error_signals:
        print(f"   Common issues:")
        for signal in error_signals:
            print(f"     - {signal['symbol']}: {signal['reasons'][0] if signal['reasons'] else 'Unknown error'}")
    
    return results


if __name__ == "__main__":
    try:
        results = test_signal_generation()
        
        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"signal_test_results_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {output_file}")
        
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()