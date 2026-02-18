# Cupcake Unified Trade Engine

This project now runs as a single local server focused on one output: the best unified trade decision (BUY/SELL/HOLD) from quant, statistical, technical, orderbook, trajectory, and market-regime signals.

## What this app does

- Ingests live + historical market data.
- Computes one unified signal per symbol.
- Recalculates every minute.
- Applies persistence/risk rules.
- Renders one unified dashboard with rationale, live prices, and signal history.

## Architecture (current)

- `main.py` - only web server entrypoint (Flask + Socket.IO)
- `enhanced_live_fetcher.py` - market stream + feature preparation
- `unified_signal_generator.py` - weighted unified signal engine
- `unified_signal_integration.py` - 1-minute scheduler + socket emission
- `technical_indicators.py`, `practical_quant_engine.py`, `orderbook_analyzer.py`, `enhanced_signal_generator.py` - signal components
- `templates/pages/dashboard.html` + `static/js/app.js` + `static/css/app.css` - unified UI

## Run locally

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Ensure Redis is running on `localhost:6379`.

3. Put valid credentials in `credentials.json`.

4. Start server:

```bash
python3 main.py --mode full --port 5000
```

5. Open:

- Dashboard: `http://localhost:5000`
- Unified API: `http://localhost:5000/api/unified/signals`

## Notes

- This repo no longer uses Docker files or Docker setup docs.
- If port `5000` is occupied, stop that process first and rerun.
