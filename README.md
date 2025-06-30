# Advanced Algorithmic Trading System for Indian Markets

## 🎯 **What This System Does**

This is a **professional algorithmic trading system** specifically designed for the **Indian Stock Market (NSE)** that automatically generates high-probability buy/sell signals using advanced quantitative analysis. The system monitors 19 major Indian stocks in real-time and provides actionable trading signals with built-in risk management.

---

## 🚀 **Getting Started - Quick Setup Guide**

### **System Requirements:**
- Docker and Docker Compose
- Real-time internet connection (minimum 10 Mbps)
- Upstox trading account with API access
- Minimum ₹1,00,000 trading capital (recommended ₹5,00,000+)
- Risk tolerance for algorithmic trading

### **Installation and Running the System**

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2.  **Set up your credentials:**
    -   Rename `.env.example` to `.env` and fill in your Upstox API key and secret.
    -   Place your `credentials.json` file (if you have one) in the root directory.

3.  **Build and run with Docker Compose:**
    ```bash
    docker-compose up --build
    ```
    This command will build the Docker image, start the application and a Redis container.

4.  **Access the Dashboard:**
    Open your web browser and go to `http://localhost:5000` to see the live trading dashboard.

### **Daily Workflow:**

**Pre-Market (8:45-9:15 AM):**
1. Review overnight global markets impact
2. Check pre-market signal queue
3. Verify risk limits and available capital
4. Set day's maximum loss tolerance

**Market Hours:**
1. Monitor real-time signals via dashboard
2. Execute trades as per system recommendations
3. Track position performance and risk levels
4. Adjust stops as market conditions change

**Post-Market:**
1. Review day's trades and performance
2. Analyze any missed opportunities
3. Prepare for next trading session
4. Update risk parameters if needed

## 📊 **How Signals Are Generated**

### **The 4-Layer Signal Generation Process**

#### **1. Real-Time Market Data Collection**
- **Live data streaming** from Upstox API every second
- **Order book analysis** monitoring bid/ask levels up to 5 levels deep
- **Volume analysis** tracking unusual activity patterns
- **Historical context** using 6 months of minute-level price data

#### **2. Multi-Timeframe Technical Analysis**
The system analyzes each stock across multiple timeframes simultaneously:

**Short-term (1-5 minutes):** Entry/exit timing
**Medium-term (1-4 hours):** Trend confirmation
**Long-term (Daily):** Overall market direction

**Key Technical Indicators Used:**
- **Moving Averages**: 5, 10, 20, 50-period SMA/EMA for trend direction
- **RSI (Relative Strength Index)**: Calibrated for Indian market volatility (20/80 levels)
- **MACD**: Optimized parameters (8,24,9) specifically for Indian stocks
- **Bollinger Bands**: 2.5 standard deviation for high-volatility Indian markets
- **ADX**: Market regime detection (trending vs. ranging)
- **Volume Indicators**: OBV, VWAP for institutional activity confirmation

#### **3. Advanced Quantitative Models**

**Trajectory Analysis:**
- Confirms signal strength across 3, 7, 14, and 21-day periods
- Only generates signals when short and long-term trends align
- Statistical significance testing to avoid false signals

**Indian Market Specific Calibrations:**
- **Sector Bias Analysis**: IT stocks stronger during US market hours
- **Currency Impact**: Export sectors (IT, Pharma) benefit from weak rupee
- **Session Timing**: Higher accuracy during 9:30-10:30 AM and 2:30-3:30 PM IST
- **Gap Risk Factor**: Overnight position adjustments for Indian market gaps

**Signal Confidence Scoring (0-100%):**
- **60%+ Confidence**: Basic signals in ranging markets
- **65%+ Confidence**: Trend-following signals
- **80%+ Confidence**: High-probability setups with multiple confirmations

#### **4. Risk-Reward Validation**
Every signal must pass strict risk management criteria:
- **Minimum 2:1 Risk-Reward Ratio**
- **Maximum 3% portfolio risk per trade**
- **Dynamic stop-loss calculation** using 5 different methods combined

---

## 🎯 **Trading Strategies Explained**

### **Strategy 1: Momentum Breakout with Trend Confirmation**

**When It Triggers:**
- Stock breaks above resistance level with high volume
- RSI between 50-70 (not overbought)
- 20-period moving average trending upward
- MACD showing positive momentum
- Order book showing strong buying interest

### **Strategy 2: Mean Reversion in Oversold Conditions**

**When It Triggers:**
- Stock oversold (RSI < 25) but showing signs of reversal
- Price touching lower Bollinger Band
- Support level holding (tested 2+ times in last 30 days)
- Volume increasing on bounce attempts
- Overall market not in severe downtrend

### **Strategy 3: Sector Rotation with Currency Impact**

**When It Triggers:**
- Rupee weakening against USD (benefits IT/Pharma exports)
- Sector showing relative strength vs. Nifty
- Individual stock outperforming sector index
- Technical setup aligning with fundamental catalyst
