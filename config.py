# config.py - Configuration file for the trading analysis system

# Stock symbols configuration
# Note: yfinance requires .NS suffix for NSE stocks, moneycontrol doesn't
STOCK_SYMBOLS = [
    "TCS",      # Tata Consultancy Services
    "INFY",     # Infosys
    "RELIANCE", # Reliance Industries
    "HDFCBANK", # HDFC Bank
    "ICICIBANK",# ICICI Bank
    "ITC",      # ITC Limited
    "SBIN",     # State Bank of India
    "BHARTIARTL", # Bharti Airtel
    "KOTAKBANK",  # Kotak Mahindra Bank
    "LT",       # Larsen & Toubro
    "ASIANPAINT", # Asian Paints
    "MARUTI",   # Maruti Suzuki
    "HCLTECH",  # HCL Technologies
    "WIPRO",    # Wipro
    "AXISBANK", # Axis Bank
    "ULTRACEMCO", # UltraTech Cement
    "TITAN",    # Titan Company
    "NESTLEIND", # Nestle India
    "POWERGRID", # Power Grid Corporation
    "NTPC"      # NTPC Limited
]

# Technical Analysis Configuration
TA_CONFIG = {
    # Moving Averages periods
    'sma_periods': [5, 10, 20, 50, 200],
    'ema_periods': [9, 12, 21, 26, 50],
    
    # Momentum indicators
    'rsi_period': 14,
    'stoch_k_period': 14,
    'stoch_d_period': 3,
    'williams_r_period': 14,
    'cci_period': 20,
    
    # MACD parameters
    'macd_fast': 12,
    'macd_slow': 26,
    'macd_signal': 9,
    
    # Bollinger Bands
    'bb_period': 20,
    'bb_std': 2,
    
    # ATR and volatility
    'atr_period': 14,
    'adx_period': 14,
    
    # Volume indicators
    'volume_sma_period': 20,
    'obv_enabled': True,
}

# Market timing configuration (IST)
MARKET_CONFIG = {
    'market_open': '09:15',
    'market_close': '15:30',
    'pre_market_start': '09:00',
    'post_market_end': '16:00',
    'timezone': 'Asia/Kolkata'
}

# Data fetching configuration
FETCH_CONFIG = {
    'resolution': '5',  # 5-minute candles for intraday
    'days_history': 100,  # Days of historical data for calculations
    'max_concurrent_requests': 10,  # Limit concurrent API calls
    'request_timeout': 30,  # Timeout in seconds
    'retry_attempts': 3,
    'retry_delay': 1,  # Seconds between retries
}

# yfinance configuration  
YFINANCE_CONFIG = {
    'period': '1y',  # 1 year of data for macro analysis
    'interval': '1d',  # Daily data for macro trends
    'macro_lookback_days': [1, 5, 10, 30, 90],  # Performance periods to calculate
}

# Headers for API requests
REQUEST_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# JSON output configuration
OUTPUT_CONFIG = {
    'precision': 4,  # Decimal places for prices
    'include_raw_data': False,  # Set to True for debugging
    'compress_output': True,  # Remove unnecessary whitespace
    'timestamp_format': '%Y-%m-%d %H:%M:%S',
}

# Logging configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_file': 'trading_analysis.log'
}