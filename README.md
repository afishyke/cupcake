# Tick‑Tick Stock Monitoring Program

## 🎯 Project Overview

A sophisticated real‑time stock monitoring system designed for day traders, providing continuous market surveillance with AI-powered risk management and technical analysis capabilities.

### Key Features
- **Real‑time Market Data**: Continuous monitoring of multiple stock symbols
- **Advanced Technical Analysis**: Rapid computation of technical indicators
- **AI Risk Management**: Intelligent buy/sell signal generation
- **Live Portfolio Tracking**: Real‑time PnL and position monitoring
- **Interactive Dashboard**: Web‑based trading interface

---

## 🛠️ Technology Stack

| Component | Technology |
|-----------|------------|
| **Backend Languages** | Python, JavaScript |
| **Frontend** | HTML, CSS, JavaScript |
| **Data Processing** | TA‑Lib, NumPy, Pandas, Numba, Cython |
| **Data Structures** | `collections.deque` for rolling windows |
| **Networking** | `aiohttp`, `websockets`, `requests` |
| **Database** | Redis + RedisTimeSeries |
| **Market API** | Upstox Open API v3 |
| **Environment** | Conda |
| **Container** | Docker |

---

## 📁 Project Structure

```
tick-tick-stock-monitor/
├── authentication/
│   ├── __init__.py
│   ├── upstox_client.py
│   └── credentials.json
├── logs/
│   └── (runtime logs with GMT+5:30 timestamps)
├── scripts/
│   ├── MarketDataFeed.proto
│   ├── MarketDataFeed_pb2.py          # Generated from .proto
│   ├── historical_data_fetcher.py
│   ├── live_data.py
│   ├── websocket_client.py
│   ├── protofolio.py
│   ├── technical_indicators.py
│   ├── chart_analysis.py
│   ├── symbols.json
│   ├── orders.py
│   └── best_filtering.py
└── web/
    └── INDEX.html
```

---

## 📋 Module Descriptions

### 🔐 Authentication Module (`authentication/`)

#### `upstox_client.py`
- **Purpose**: Handles daily Upstox API authentication
- **Functionality**: 
  - Fetches fresh access tokens valid until 3:30 AM IST of next day
  - Manages API session lifecycle
  - Handles token refresh and error recovery

#### `credentials.json`
```json
{
  "api_key": "xxxxxxxx-xxxxx-xxxxx-xxxxx-xxxxxxx",
  "api_secret": "xxxxxxxxxxxxx",
  "redirect_uri": "http://localhost:8000",
  "access_token": "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzV0NXTUQiLCJqdGkiOiI2ODUyNDc1MmQ5ODQ5ODY0NzQ1MGUyNDciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc1MDIyMjY3NCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzUwMjg0MDAwfQ.ZdYgDgXZZjkhHGDGPLFvCerCBfSDVhhN1C5hKT6ALd0",
  "token_expires_at": "2025-06-19T09:27:54.437492"
}
```

#### `__init__.py`
- Marks the authentication directory as a Python package

---

### 📊 Core Scripts (`scripts/`)

#### `MarketDataFeed.proto`
- **Protocol Buffer Schema**: Defines structure for Upstox binary market data
- **Compilation**: Generates `MarketDataFeed_pb2.py` for data decoding
- **Usage**: Enables efficient parsing of real‑time market feeds

#### `historical_data_fetcher.py`
```python
# Key Functionality:
# - Fetches 250 × 1‑minute OHLCV candles on startup
# - Stores historical data in Redis DB 0
# - Maintains chronological order (latest candle at end)
```

#### `live_data.py`
```python
# Core Features:
# - Retrieves previous minute's OHLCV data
# - Updates real‑time pipeline every minute
# - Integrates with MarketDataFeed_pb2.py
```

#### `websocket_client.py`
```python
# Real‑time Metrics Calculation:
# - Maintains 300‑row rolling window
# - Mid‑price = (bid + ask) / 2
# - Spread = ask - bid
# - VWAP over N seconds
# - Order‑book imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)
# - Tick returns = ΔLTP / prev_LTP
# - Liquidity & volatility flags
# - VWAP‑MA crossover signals
# - Writes to RedisTimeSeries DB 1
```

#### `protofolio.py`
```python
# Portfolio Management:
# - Live PnL calculation
# - Position tracking
# - Minute‑by‑minute updates to Redis DB 2
# - Risk exposure monitoring
```

#### `technical_indicators.py`
```python
# Technical Analysis Indicators:
# - ATR (Average True Range)
# - EMA (Exponential Moving Average)
# - RSI (Relative Strength Index)
# - MACD (Moving Average Convergence Divergence)
# - Bollinger Bands
# - Stochastic Oscillator
# - Custom momentum indicators
# - Saves computed values to Redis DB 2
```

#### `chart_analysis.py`
```python
# Candlestick Pattern Recognition:
# - Doji patterns
# - Hammer & Inverted Hammer
# - Bullish/Bearish Engulfing
# - Morning Star & Evening Star
# - Three Black Crows
# - Analysis across multiple timeframes: [1, 3, 5, 10, 15, 60] minutes
# - Fetches data from Redis DB 0 & 2
```

#### `orders.py`
```python
# Order Management System:
# - Optimal entry/exit point calculation
# - User confirmation prompts
# - Upstox API integration for order placement
# - Order cancellation capabilities
# - Trade execution logging
```

#### `best_filtering.py`
```python
# AI Risk Management Engine:
# - Consolidates data from Redis DBs 0-2
# - Machine learning‑based signal generation
# - Risk‑adjusted buy/sell recommendations
# - Position sizing optimization
# - Market regime detection
```

#### `symbols.json`
```json
{
  "symbols": [
    {
      "name": "Reliance Industries",
      "instrument_key": "NSE_EQ|INE002A01018"
    },
    {
      "name": "Tata Consultancy Services",
      "instrument_key": "NSE_EQ|INE467B01029"
    },
    {
      "name": "Infosys",
      "instrument_key": "NSE_EQ|INE009A01021"
    },
    {
      "name": "HDFC Bank",
      "instrument_key": "NSE_EQ|INE040A01034"
    },
    {
      "name": "ICICI Bank",
      "instrument_key": "NSE_EQ|INE090A01021"
    },
    {
      "name": "Hindustan Unilever",
      "instrument_key": "NSE_EQ|INE030A01027"
    },
    {
      "name": "ITC",
      "instrument_key": "NSE_EQ|INE154A01025"
    },
    {
      "name": "State Bank of India",
      "instrument_key": "NSE_EQ|INE062A01020"
    },
    {
      "name": "Bharti Airtel",
      "instrument_key": "NSE_EQ|INE397D01024"
    },
    {
      "name": "Kotak Mahindra Bank",
      "instrument_key": "NSE_EQ|INE237A01028"
    },
    {
      "name": "Larsen & Toubro",
      "instrument_key": "NSE_EQ|INE018A01030"
    },
    {
      "name": "Asian Paints",
      "instrument_key": "NSE_EQ|INE021A01026"
    },
    {
      "name": "Maruti Suzuki",
      "instrument_key": "NSE_EQ|INE585B01010"
    },
    {
      "name": "Bajaj Finance",
      "instrument_key": "NSE_EQ|INE296A01024"
    },
    {
      "name": "HCL Technologies",
      "instrument_key": "NSE_EQ|INE860A01027"
    }
  ]
}
```

---

### 🌐 Web Interface (`web/`)

#### `INDEX.html`
```html
<!-- Interactive Trading Dashboard -->
<!-- Features: -->
<!-- - Real‑time best stock recommendations -->
<!-- - Live portfolio status display -->
<!-- - Trade execution controls -->
<!-- - Technical indicator visualizations -->
<!-- - Risk management alerts -->
```

---

### 📝 Logging System (`logs/`)

- **Timestamp Format**: GMT+5:30 (Indian Standard Time)
- **Log Categories**:
  - Error logs
  - Processing step logs
  - Trade execution events
  - System performance metrics
- **Management**: Manually clearable for debugging purposes

---

## 🗄️ Redis Database Architecture

| Database | Purpose | Data Type |
|----------|---------|-----------|
| **DB 0** | Historical OHLCV Data | Time‑series candles |
| **DB 1** | Real‑time Tick Data | WebSocket feed metrics |
| **DB 2** | Technical Indicators & Portfolio | Computed indicators, PnL |

---

## 🚀 Installation Guide

### Prerequisites
- Python 3.10
- Conda package manager
- Redis server
- Protocol Buffer compiler

### 1. Protocol Buffer Compiler Installation

#### Debian/Ubuntu
```bash
# Update package list
sudo apt update

# Install protobuf compiler
sudo apt install -y protobuf-compiler

# Verify installation
protoc --version
# Expected output: libprotoc 3.x.x
```

#### Windows (via Chocolatey)
```powershell
# Install Chocolatey (if not already installed)
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install Protocol Buffer compiler
choco install protoc -y

# Verify installation
protoc --version
```

#### Manual Installation (Windows/macOS/Linux)
1. Download from [Protocol Buffers Releases](https://github.com/protocolbuffers/protobuf/releases)
2. Extract the archive
3. Add `protoc/bin` to your system PATH
4. Verify with `protoc --version`

### 2. Environment Setup

```bash
# Create conda environment
conda create -n tick-tick-monitor python=3.9
conda activate tick-tick-monitor

# Install required packages
pip install numpy pandas ta-lib numba cython
pip install aiohttp websockets requests
pip install redis redis-py
pip install protobuf

# Install additional dependencies
pip install upstox-python-sdk
```

### 3. Project Setup

```bash
# Clone or create project directory
mkdir tick-tick-stock-monitor
cd tick-tick-stock-monitor

# Create directory structure
mkdir -p authentication logs scripts web

# Generate protobuf files
cd scripts
protoc --python_out=. MarketDataFeed.proto
# This creates MarketDataFeed_pb2.py
```

### 4. Redis Configuration

```bash
# Install Redis (Ubuntu/Debian)
sudo apt install redis-server

# Install Redis TimeSeries module
# Download from: https://github.com/RedisTimeSeries/RedisTimeSeries
# Or use Redis Stack which includes TimeSeries

# Start Redis server
redis-server

# Test Redis connection
redis-cli ping
# Expected response: PONG
```

### 5. Upstox API Setup

1. Register at [Upstox Developer Console](https://upstox.com/developer/api)
2. Create an application and get API credentials
3. Update `authentication/credentials.json` with your keys

---

## 🔧 Configuration

### Environment Variables
```bash
export UPSTOX_API_KEY="your_api_key"
export UPSTOX_API_SECRET="your_api_secret"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
```

### Symbol Configuration
Edit `scripts/symbols.json` to include your watchlist:
```json
{
  "symbols": [
    {
      "name": "Reliance Industries",
      "instrument_key": "NSE_EQ|INE002A01018"
    },
    {
      "name": "Tata Consultancy Services",
      "instrument_key": "NSE_EQ|INE467B01029"
    },
    {
      "name": "Infosys",
      "instrument_key": "NSE_EQ|INE009A01021"
    },
    {
      "name": "HDFC Bank",
      "instrument_key": "NSE_EQ|INE040A01034"
    },
    {
      "name": "ICICI Bank",
      "instrument_key": "NSE_EQ|INE090A01021"
    },
    {
      "name": "Hindustan Unilever",
      "instrument_key": "NSE_EQ|INE030A01027"
    },
    {
      "name": "ITC",
      "instrument_key": "NSE_EQ|INE154A01025"
    },
    {
      "name": "State Bank of India",
      "instrument_key": "NSE_EQ|INE062A01020"
    },
    {
      "name": "Bharti Airtel",
      "instrument_key": "NSE_EQ|INE397D01024"
    },
    {
      "name": "Kotak Mahindra Bank",
      "instrument_key": "NSE_EQ|INE237A01028"
    },
    {
      "name": "Larsen & Toubro",
      "instrument_key": "NSE_EQ|INE018A01030"
    },
    {
      "name": "Asian Paints",
      "instrument_key": "NSE_EQ|INE021A01026"
    },
    {
      "name": "Maruti Suzuki",
      "instrument_key": "NSE_EQ|INE585B01010"
    },
    {
      "name": "Bajaj Finance",
      "instrument_key": "NSE_EQ|INE296A01024"
    },
    {
      "name": "HCL Technologies",
      "instrument_key": "NSE_EQ|INE860A01027"
    }
  ]
}
```

---

## 🚀 Running the System

### 1. Start Core Services
```bash
# Start Redis server
redis-server

# Activate conda environment
conda activate cupcake-v2    (whatever the name of enviroment you made)
```

### 2. Initialize Historical Data
```bash
cd scripts
python historical_data_fetcher.py
```

### 3. Start Real‑time Components
```bash
# Terminal 1: Live data feed
python live_data.py

# Terminal 2: WebSocket client
python websocket_client.py

# Terminal 3: Technical analysis
python technical_indicators.py

# Terminal 4: Portfolio tracking
python protofolio.py
```

### 4. Launch Web Interface
```bash
# Start local web server
cd web
python -m http.server 8080

# Access dashboard at: http://localhost:8080
```

---

## 📊 Usage Workflow

1. **Authentication**: System authenticates with Upstox API daily
2. **Data Initialization**: Historical data is fetched and stored
3. **Real‑time Monitoring**: Live market data streams continuously
4. **Technical Analysis**: Indicators computed in real‑time
5. **Signal Generation**: AI engine analyzes patterns and generates signals
6. **Risk Management**: Position sizing and risk assessment
7. **Trade Execution**: User‑confirmed orders placed via API

---

## 🔍 Monitoring & Debugging

### Log Files
```bash
# View real‑time logs
tail -f logs/system.log

# Check error logs
grep "ERROR" logs/*.log

# Monitor performance
grep "PERFORMANCE" logs/*.log
```

### Redis Debugging
```bash
# Connect to Redis CLI
redis-cli

# Check database contents
SELECT 0  # Historical data
KEYS *

SELECT 1  # Live tick data
TS.RANGE symbol:RELIANCE - +

SELECT 2  # Technical indicators
HGETALL indicators:RELIANCE
```

---

## ⚠️ Risk Considerations

- **Paper Trading**: Test thoroughly before live trading
- **Position Limits**: Implement strict position sizing
- **Stop Losses**: Always use stop‑loss orders
- **Market Hours**: System operates during market hours only
- **API Limits**: Respect Upstox API rate limits
- **Network Latency**: Monitor connection quality

---

## 🔧 Maintenance

### Daily Tasks
- [ ] Verify API token refresh
- [ ] Check log file sizes
- [ ] Monitor Redis memory usage
- [ ] Validate data integrity

### Weekly Tasks
- [ ] Update symbol watchlist
- [ ] Review trading performance
- [ ] Backup configuration files
- [ ] Update technical indicators

---

## 📈 Performance Optimization

### Code Optimization
- Use Numba JIT compilation for hot paths
- Implement Cython for critical calculations
- Optimize Redis queries with pipelining
- Use `collections.deque` for rolling windows

### System Optimization
- Dedicated Redis instance for time‑series data
- SSD storage for database persistence
- Sufficient RAM for data caching
- Low‑latency network connection

---



---



---

## 🔮 Future Enhancements

- [ ] Machine learning model training pipeline
- [ ] Advanced options trading strategies
- [ ] Multi‑broker API support
- [ ] Mobile application interface
- [ ] Advanced charting capabilities
- [ ] Backtesting framework
- [ ] Social trading features

---

*Last Updated: June 2025*
