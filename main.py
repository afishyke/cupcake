import argparse
import asyncio
import logging
import os
import sys
import threading

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from flask_socketio import SocketIO

from upstox_client import UpstoxAuthClient


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)

CREDENTIALS_PATH = os.path.join(SCRIPT_DIR, "credentials.json")
SYMBOLS_PATH = os.path.join(SCRIPT_DIR, "symbols.json")


class Config:
    REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
    APP_HOST = os.environ.get("APP_HOST", "localhost")
    APP_PORT = int(os.environ.get("APP_PORT", "5000"))


try:
    import enhanced_live_fetcher
    from enhanced_live_fetcher import fetch_enhanced_market_data
except ImportError as exc:
    print(f"Enhanced live fetcher import error: {exc}")
    enhanced_live_fetcher = None
    fetch_enhanced_market_data = None

try:
    from historical_data_fetcher import UltraFastHistoricalFetcher
except ImportError as exc:
    print(f"Historical fetcher import error: {exc}")
    UltraFastHistoricalFetcher = None


app = Flask(
    __name__,
    template_folder=os.path.join(SCRIPT_DIR, "templates"),
    static_folder=os.path.join(SCRIPT_DIR, "static"),
)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")


if enhanced_live_fetcher:
    enhanced_live_fetcher.socketio = socketio
    if hasattr(enhanced_live_fetcher, "set_socketio_instance"):
        enhanced_live_fetcher.set_socketio_instance(socketio)


def run_enhanced_live_data_feed() -> None:
    if not fetch_enhanced_market_data:
        print("Live feed unavailable")
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(fetch_enhanced_market_data())
    finally:
        loop.close()


def run_historical_backfill() -> None:
    if not UltraFastHistoricalFetcher:
        print("Historical backfill unavailable")
        return

    try:
        fetcher = UltraFastHistoricalFetcher(CREDENTIALS_PATH, SYMBOLS_PATH)
        fetcher.redis.flush_and_prepare()
        result = fetcher.process_all_symbols_ultra_fast(max_workers=8)
        if result.get("success"):
            stats = result["stats"]
            print(f"Backfill complete in {stats['total_time']}")
            print(f"{stats['symbols_successful']}/{stats['symbols_processed']} symbols")
        else:
            print(f"Backfill failed: {result.get('error', 'Unknown error')}")
    except Exception as exc:
        print(f"Backfill error: {exc}")


@app.route("/")
def dashboard():
    return render_template("pages/dashboard.html")


@app.route("/api/unified/signals")
def api_unified_signals():
    manager = getattr(enhanced_live_fetcher, "unified_signal_manager", None)
    if not manager:
        return jsonify([])
    return jsonify(manager.get_all_signals())


@app.route("/api/unified/signals/<symbol_name>")
def api_unified_signal(symbol_name):
    manager = getattr(enhanced_live_fetcher, "unified_signal_manager", None)
    if not manager:
        return jsonify({"error": "Unified signal manager unavailable"}), 503
    signal = manager.get_signal(symbol_name)
    if not signal:
        return jsonify({"error": "Signal not found"}), 404
    return jsonify(signal)


@app.route("/api/unified/live_prices")
def api_unified_live_prices():
    try:
        manager = getattr(enhanced_live_fetcher, "unified_signal_manager", None)
        signals = manager.get_all_signals() if manager else []
        signal_map = {s.get("symbol"): s for s in signals}

        live_market = getattr(enhanced_live_fetcher, "market_data", {}) or {}
        rows = []

        for _, snapshot in live_market.items():
            symbol = snapshot.get("name")
            if not symbol:
                continue

            signal = signal_map.get(symbol, {})
            updated_at = snapshot.get("last_update")
            if hasattr(updated_at, "isoformat"):
                updated_at = updated_at.isoformat()

            rows.append(
                {
                    "symbol": symbol,
                    "ltp": snapshot.get("ltp"),
                    "best_bid": snapshot.get("best_bid"),
                    "best_ask": snapshot.get("best_ask"),
                    "spread_pct": snapshot.get("spread_pct"),
                    "updated_at": updated_at or signal.get("updated_at"),
                    "direction": signal.get("direction"),
                    "confidence": signal.get("confidence"),
                }
            )

        if not rows:
            for signal in signals:
                extra = signal.get("extra") or {}
                ctx = extra.get("price_context") or {}
                rows.append(
                    {
                        "symbol": signal.get("symbol"),
                        "ltp": ctx.get("ltp", signal.get("entry_price")),
                        "best_bid": ctx.get("best_bid"),
                        "best_ask": ctx.get("best_ask"),
                        "spread_pct": ctx.get("spread_pct"),
                        "updated_at": signal.get("updated_at"),
                        "direction": signal.get("direction"),
                        "confidence": signal.get("confidence"),
                    }
                )

        rows.sort(key=lambda item: float(item.get("confidence") or 0), reverse=True)
        return jsonify(rows)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/unified/history")
def api_unified_history():
    manager = getattr(enhanced_live_fetcher, "unified_signal_manager", None)
    if not manager:
        return jsonify([])
    try:
        limit = int(request.args.get("limit", 60))
    except ValueError:
        limit = 60
    limit = max(1, min(limit, 500))
    return jsonify(manager.get_history(limit=limit))


@app.route("/api/unified/history/<symbol_name>")
def api_unified_symbol_history(symbol_name):
    manager = getattr(enhanced_live_fetcher, "unified_signal_manager", None)
    if not manager:
        return jsonify([])
    try:
        limit = int(request.args.get("limit", 60))
    except ValueError:
        limit = 60
    limit = max(1, min(limit, 500))
    return jsonify(manager.get_history(symbol=symbol_name, limit=limit))


@app.route("/api/system_status")
def system_status():
    return jsonify(
        {
            "status": "running",
            "modules": {
                "enhanced_live_fetcher": "active" if enhanced_live_fetcher else "unavailable",
                "ultra_fast_historical": "active" if UltraFastHistoricalFetcher else "unavailable",
                "signal_generator": "active",
            },
            "features": {
                "unified_signals": True,
                "live_prices": True,
                "signal_history": True,
                "multi_timeframe_analysis": True,
            },
        }
    )


@socketio.on("connect")
def handle_connect():
    print("Client connected")


@socketio.on("disconnect")
def handle_disconnect():
    print("Client disconnected")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Cupcake Unified Signal Server")
    parser.add_argument("--mode", choices=["full", "live", "backfill"], default="full")
    parser.add_argument("--skip-auth", action="store_true")
    parser.add_argument("--skip-backfill", action="store_true")
    parser.add_argument("--port", type=int, default=5000)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    if not os.path.exists(CREDENTIALS_PATH):
        print(f"Missing credentials file: {CREDENTIALS_PATH}")
        sys.exit(1)
    if not os.path.exists(SYMBOLS_PATH):
        print(f"Missing symbols file: {SYMBOLS_PATH}")
        sys.exit(1)

    if args.mode == "backfill":
        run_historical_backfill()
        sys.exit(0)

    if not args.skip_auth:
        try:
            auth_client = UpstoxAuthClient(config_file=CREDENTIALS_PATH)
            if not auth_client.authenticate():
                print("Authentication failed")
                sys.exit(1)
            print("Authentication successful")
        except Exception as exc:
            print(f"Authentication error: {exc}")
            if args.mode == "live":
                sys.exit(1)

    if args.mode == "full" and not args.skip_backfill:
        run_historical_backfill()

    if enhanced_live_fetcher:
        feed_thread = threading.Thread(target=run_enhanced_live_data_feed, daemon=True)
        feed_thread.start()

    print(f"Unified Signal Console running at http://{Config.APP_HOST}:{args.port}")
    socketio.run(
        app,
        host="0.0.0.0",
        port=args.port,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
    )
