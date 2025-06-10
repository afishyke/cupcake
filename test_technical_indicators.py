import unittest
import numpy as np
import pandas as pd # For creating datetime index if needed
from datetime import datetime
from technical_indicators import UltraFastTechnicalAnalyzer, SignalType, TechnicalSignal

# Helper function to create mock OHLCV data
def create_mock_data(num_periods=100):
    return {
        'open': np.random.rand(num_periods) * 10 + 100,
        'high': np.random.rand(num_periods) * 10 + 105, # ensure high > open
        'low': np.random.rand(num_periods) * 5 + 95,   # ensure low < open
        'close': np.random.rand(num_periods) * 10 + 100,
        'volume': np.random.randint(1000, 10000, num_periods).astype(np.float64),
        'timestamp': pd.date_range(start='2023-01-01', periods=num_periods, freq='1min').values
    }

# Ensure low is actually lower than high, open, close for realistic data
def create_realistic_mock_data(num_periods=100):
    data = {}
    data['open'] = np.random.uniform(100, 110, num_periods)
    data['close'] = data['open'] + np.random.uniform(-2, 2, num_periods)
    data['high'] = np.maximum(data['open'], data['close']) + np.random.uniform(0, 2, num_periods)
    data['low'] = np.minimum(data['open'], data['close']) - np.random.uniform(0, 2, num_periods)
    data['volume'] = np.random.randint(1000, 10000, num_periods).astype(np.float64)
    data['timestamp'] = pd.date_range(start='2023-01-01', periods=num_periods, freq='1min').values
    # Ensure all prices are positive
    for key in ['open', 'high', 'low', 'close']:
        data[key] = np.maximum(data[key], 0.01)
    return data

class TestUltraFastTechnicalAnalyzer(unittest.TestCase):
    def setUp(self):
        self.analyzer = UltraFastTechnicalAnalyzer(data_dir="mock_data_dir") # data_dir won't be used if we mock load_data_fast
        self.mock_instrument_key = "TEST_INSTRUMENT"
        self.num_periods = 200 # Ensure enough for all indicator calculations

    def test_load_config(self):
        config = self.analyzer._load_config()
        self.assertIn("signals", config)
        self.assertIn("indicators", config)
        # Check for newly added periods/params
        ind_config = config["indicators"]
        self.assertIn("sma_periods", ind_config)
        self.assertIn(200, ind_config["sma_periods"])
        self.assertEqual(ind_config.get("aroon_period"), 14)
        self.assertEqual(ind_config.get("stoch_fastk_period"), 14)
        self.assertEqual(ind_config.get("willr_period"), 14)
        self.assertEqual(ind_config.get("cci_period"), 14)
        self.assertEqual(ind_config.get("mfi_period"), 14)
        self.assertEqual(ind_config.get("atr_period"), 14)
        self.assertEqual(ind_config.get("keltner_period"), 20)
        self.assertEqual(ind_config.get("keltner_multiplier"), 2.0)
        self.assertEqual(ind_config.get("donchian_period"), 20)
        self.assertEqual(ind_config.get("chaikin_fastperiod"), 3)
        self.assertEqual(ind_config.get("chaikin_slowperiod"), 10)

    def test_calculate_indicators_fast_all_present(self):
        mock_ohlcv_data = create_realistic_mock_data(self.num_periods)
        indicators = self.analyzer.calculate_indicators_fast(mock_ohlcv_data)

        self.assertGreater(len(indicators.get('close', [])), 0, "Close prices should be in indicators")

        # List all indicators that should be calculated and present
        expected_keys = [
            'rsi', 'bb_upper', 'bb_middle', 'bb_lower', 'adx', 'macd', 'macd_signal', 'macd_hist', 'atr',
            'adx_plus', 'adx_minus', 'sar', 'aroon_up', 'aroon_down',
            'stoch_k', 'stoch_d', 'williams_r', 'cci', 'mfi',
            'keltner_upper', 'keltner_middle', 'keltner_lower',
            'donchian_upper', 'donchian_middle', 'donchian_lower',
            'obv', 'ad', 'vwap', 'chaikin_ad'
        ]
        # Add SMAs and EMAs based on config
        for p in self.analyzer.config['indicators']['ema_periods']: expected_keys.append(f'ema_{p}')
        for p in self.analyzer.config['indicators']['sma_periods']: expected_keys.append(f'sma_{p}')

        for key in expected_keys:
            self.assertIn(key, indicators, f"{key} not found in calculated indicators")
            self.assertIsNotNone(indicators[key], f"{key} is None")
            self.assertEqual(len(indicators[key]), self.num_periods, f"Length mismatch for {key}")

    # --- Test generate_signal_fast ---
    def _run_signal_test(self, indicator_setup_fn, expected_signal_type, expected_reason_substring=None, expected_strength_approx=None):
        # Base indicators with neutral values
        indicators = {
            'close': np.array([100.0] * self.num_periods),
            'high': np.array([101.0] * self.num_periods),
            'low': np.array([99.0] * self.num_periods),
            'open': np.array([100.0] * self.num_periods),
            'volume': np.array([1000.0] * self.num_periods),
            'timestamp': pd.date_range(start='2023-01-01', periods=self.num_periods, freq='1min').values,
            # Neutral default values for most indicators
            'rsi': np.full(self.num_periods, 50.0),
            'adx': np.full(self.num_periods, 20.0), # Below trend strength
            'adx_plus': np.full(self.num_periods, 20.0),
            'adx_minus': np.full(self.num_periods, 20.0),
            'ema_9': np.full(self.num_periods, 100.0),
            'ema_21': np.full(self.num_periods, 100.0),
            'sma_50': np.full(self.num_periods, 100.0),
            'sma_200': np.full(self.num_periods, 100.0),
            'sar': np.full(self.num_periods, 98.0), # Price above SAR is bullish by default
            'aroon_up': np.full(self.num_periods, 50.0),
            'aroon_down': np.full(self.num_periods, 50.0),
            'stoch_k': np.full(self.num_periods, 50.0),
            'stoch_d': np.full(self.num_periods, 50.0),
            'williams_r': np.full(self.num_periods, -50.0),
            'cci': np.full(self.num_periods, 0.0),
            'mfi': np.full(self.num_periods, 50.0),
            'atr': np.full(self.num_periods, 1.0), # ATR is 1% of price 100
            'keltner_upper': np.full(self.num_periods, 102.0),
            'keltner_middle': np.full(self.num_periods, 100.0),
            'keltner_lower': np.full(self.num_periods, 98.0),
            'donchian_upper': np.full(self.num_periods, 101.0), # Price not breaking out
            'donchian_middle': np.full(self.num_periods, 100.0),
            'donchian_lower': np.full(self.num_periods, 99.0),
            'obv': np.arange(self.num_periods).astype(float) * 1000, # Steadily rising OBV
            'ad': np.arange(self.num_periods).astype(float) * 1000,   # Steadily rising AD
            'vwap': np.full(self.num_periods, 100.0),
            'chaikin_ad': np.full(self.num_periods, 0.0)
        }
        # Apply specific setup for the test
        indicators = indicator_setup_fn(indicators, self.num_periods)

        signal = self.analyzer.generate_signal_fast(self.mock_instrument_key, indicators)

        self.assertEqual(signal.signal_type, expected_signal_type, f"Expected {expected_signal_type}, got {signal.signal_type} with reasoning: {signal.reasoning}")
        if expected_reason_substring:
            self.assertIn(expected_reason_substring, signal.reasoning, f"Expected reason substring '{expected_reason_substring}' not in '{signal.reasoning}'")
        if expected_strength_approx is not None:
            self.assertAlmostEqual(signal.strength, expected_strength_approx, delta=1.0, msg=f"Strength mismatch. Reasoning: {signal.reasoning}")


    def test_signal_rsi_oversold(self):
        def setup(ind, n):
            ind['rsi'][-1] = 20 # RSI oversold (<30) -> +25 raw
            # Neutralize SAR and OBV/AD default bullishness for this specific test
            ind['sar'][-1] = 102.0 # price below sar
            ind['obv'][-1] = ind['obv'][-11] # neutral obv
            ind['ad'][-1] = ind['ad'][-11]   # neutral ad
            return ind
        # (25/1)*10 = 250 -> clamped to 100. STRONG_BUY
        self._run_signal_test(setup, SignalType.STRONG_BUY, "RSI oversold", 100.0)

    def test_signal_mfi_overbought_and_cci_overbought(self):
        def setup(ind, n):
            ind['mfi'][-1] = 85     # MFI overbought (>80) -> -10 raw
            ind['cci'][-1] = 150    # CCI overbought (>100) -> -10 raw
            # Neutralize SAR and OBV/AD default bullishness
            ind['sar'][-1] = 102.0
            ind['obv'][-1] = ind['obv'][-11]
            ind['ad'][-1] = ind['ad'][-11]
            return ind
        # raw_strength = -10 + -10 = -20. count = 2. norm = (-20/2)*10 = -100. STRONG_SELL
        self._run_signal_test(setup, SignalType.STRONG_SELL, "MFI overbought", 100.0)

    def test_signal_price_above_vwap_weak_bullish(self):
        def setup(ind, n):
            ind['vwap'][-1] = 99.0 # Price 100 > VWAP 99 -> +7 raw
            # Neutralize SAR and OBV/AD default bullishness
            ind['sar'][-1] = 102.0
            ind['obv'][-1] = ind['obv'][-11]
            ind['ad'][-1] = ind['ad'][-11]
            return ind
        # raw_strength = 7. count = 1. norm = (7/1)*10 = 70. STRONG_BUY
        self._run_signal_test(setup, SignalType.STRONG_BUY, "Price above VWAP", 70.0)

    def test_signal_hold_neutral_conditions(self):
        def setup(ind, n):
            # Ensure all default neutral values don't trigger strong signals
            ind['sar'][-1] = 102.0 # price below sar -> -10
            ind['obv'][-1] = ind['obv'][-11] # neutral obv
            ind['ad'][-1] = ind['ad'][-11]   # neutral ad
            # raw_strength = -10. count = 1. norm = (-10/1)*10 = -100. STRONG_SELL
            # This base setup actually gives STRONG_SELL. Let's make it truly neutral.
            # Price = 100. SAR = 100. No signal from SAR.
            ind['sar'][-1] = 100.0
            return ind
        # No rules fire. Norm strength = 0. HOLD
        self._run_signal_test(setup, SignalType.HOLD, "", 0.0)

    def test_signal_multiple_bullish_strong_buy(self):
        def setup(ind, n):
            ind['rsi'][-1] = 25     # +25
            ind['stoch_k'][-1] = 15 # +10
            ind['vwap'][-1] = 99    # +7
            # SAR already bullish (+10), OBV/AD already bullish (+5, +5)
            # Total raw = 25+10+7+10+5+5 = 62. Count = 6
            # Norm = (62/6)*10 = 10.33 * 10 = 103.33. Clamped = 100.
            return ind
        self._run_signal_test(setup, SignalType.STRONG_BUY, "RSI oversold", 100.0)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
