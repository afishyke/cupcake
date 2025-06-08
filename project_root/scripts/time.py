#!/usr/bin/env python3
"""
Main orchestration script that processes queued data and coordinates analysis
Handles ThreadPool job submissions and periodic triggers
"""
import time
import json
import logging
import threading
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from datetime import datetime, timedelta
import sys
import os

# Import analysis modules
from advanced_indicators import AdvancedIndicators
from talib_indicators import TalibIndicators
from risk_manager import RiskManager
from final_analyzer import FinalAnalyzer
from google_ai_uploader import GoogleAIUploader
from signal_logger import SignalLogger
from sklearn_runner import SklearnRunner
from csv_output import CSVOutput
from dashboard_pusher import DashboardPusher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TimeOrchestrator:
    def __init__(self):
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=8)
        
        # Data queues (shared with upstox_stream_live_data.py)
        self.trade_tick_queue = Queue()
        self.candle_queue = Queue()
        self.live_order_books = {}
        
        # Analysis components
        self.advanced_indicators = AdvancedIndicators()
        self.talib_indicators = TalibIndicators()
        self.risk_manager = RiskManager()
        self.final_analyzer = FinalAnalyzer()
        self.google_uploader = GoogleAIUploader()
        self.signal_logger = SignalLogger()
        self.sklearn_runner = SklearnRunner()
        self.csv_output = CSVOutput()
        self.dashboard_pusher = DashboardPusher()
        
        # Data storage
        self.ohlcv_data = {}  # symbol -> DataFrame
        self.analysis_results = {}
        
        # Load configuration
        self.load_config()
        self.load_historical_data()
        
        # Timing controls
        self.last_upload_time = time.time()
        self.last_dashboard_update = time.time()
        self.upload_interval = 60  # seconds
        self.dashboard_interval = 10  # seconds
        
    def load_config(self):
        """Load subscription and risk configurations"""
        try:
            with open('config/subscriptions.json', 'r') as f:
                self.subscriptions = json.load(f)
            
            with open('config/risk_settings.json', 'r') as f:
                self.risk_settings = json.load(f)
                
            logger.info(f"Loaded {len(self.subscriptions)} subscriptions")
            
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def load_historical_data(self):
        """Bootstrap historical data for indicator calculations"""
        try:
            # Load main historical file
            hist_file = 'config/indicators_history.csv'
            if os.path.exists(hist_file):
                df = pd.read_csv(hist_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                
                # Initialize OHLCV data for each subscribed instrument
                for sub in self.subscriptions:
                    instrument_key = sub['instrumentKeys'][0] if sub['instrumentKeys'] else None
                    if instrument_key:
                        symbol = instrument_key.split('|')[-1]
                        self.ohlcv_data[symbol] = df.copy()
                        
            # Load individual symbol histories from data/history/
            history_dir = 'data/history'
            if os.path.exists(history_dir):
                for filename in os.listdir(history_dir):
                    if filename.endswith('.csv'):
                        instrument_key = filename.replace('.csv', '')
                        filepath = os.path.join(history_dir, filename)
                        
                        df = pd.read_csv(filepath)
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                        
                        symbol = instrument_key.split('|')[-1] if '|' in instrument_key else instrument_key
                        self.ohlcv_data[symbol] = df
                        
            logger.info(f"Loaded historical data for {len(self.ohlcv_data)} symbols")
            
        except Exception as e:
            logger.error(f"Failed to load historical data: {e}")
    
    def process_trade_tick(self, tick_data):
        """Process individual trade tick"""
        try:
            symbol = tick_data.get('symbol', '')
            if not symbol:
                return
                
            # Update live order book if present
            if 'depth' in tick_data:
                self.live_order_books[symbol] = tick_data['depth']
                
            # Add to current candle building logic
            self.update_current_candle(symbol, tick_data)
            
        except Exception as e:
            logger.error(f"Error processing tick for {symbol}: {e}")
    
    def update_current_candle(self, symbol, tick_data):
        """Update current minute candle with tick data"""
        try:
            current_minute = datetime.now().replace(second=0, microsecond=0)
            
            if symbol not in self.ohlcv_data:
                self.ohlcv_data[symbol] = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                
            df = self.ohlcv_data[symbol]
            ltp = tick_data.get('ltp', 0)
            volume = tick_data.get('volume', 0)
            
            # Update or create current candle
            if current_minute in df.index:
                # Update existing candle
                df.loc[current_minute, 'high'] = max(df.loc[current_minute, 'high'], ltp)
                df.loc[current_minute, 'low'] = min(df.loc[current_minute, 'low'], ltp)
                df.loc[current_minute, 'close'] = ltp
                df.loc[current_minute, 'volume'] = volume
            else:
                # Create new candle
                new_row = pd.Series({
                    'open': ltp,
                    'high': ltp,
                    'low': ltp,
                    'close': ltp,
                    'volume': volume
                }, name=current_minute)
                df = pd.concat([df, new_row.to_frame().T])
                self.ohlcv_data[symbol] = df.sort_index()
                
        except Exception as e:
            logger.error(f"Error updating candle for {symbol}: {e}")
    
    def process_completed_candle(self, candle_data):
        """Process completed 1-minute candle"""
        try:
            symbol = candle_data.get('symbol', '')
            if not symbol:
                return
                
            # Add completed candle to historical data
            timestamp = pd.to_datetime(candle_data['timestamp'])
            
            if symbol not in self.ohlcv_data:
                self.ohlcv_data[symbol] = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                
            new_candle = pd.Series({
                'open': candle_data['open'],
                'high': candle_data['high'],
                'low': candle_data['low'],
                'close': candle_data['close'],
                'volume': candle_data['volume']
            }, name=timestamp)
            
            df = self.ohlcv_data[symbol]
            df = pd.concat([df, new_candle.to_frame().T])
            self.ohlcv_data[symbol] = df.sort_index().tail(1000)  # Keep last 1000 candles
            
            # Trigger analysis for this symbol
            self.schedule_analysis(symbol)
            
        except Exception as e:
            logger.error(f"Error processing completed candle for {symbol}: {e}")
    
    def schedule_analysis(self, symbol):
        """Schedule analysis job for symbol"""
        try:
            future = self.executor.submit(self.run_analysis, symbol)
            logger.debug(f"Scheduled analysis for {symbol}")
            
        except Exception as e:
            logger.error(f"Failed to schedule analysis for {symbol}: {e}")
    
    def run_analysis(self, symbol):
        """Run complete analysis pipeline for symbol"""
        start_time = time.time()
        
        try:
            # Get required data
            ohlcv_df = self.ohlcv_data.get(symbol)
            if ohlcv_df is None or len(ohlcv_df) < 20:
                logger.warning(f"Insufficient data for {symbol}")
                return
                
            order_book = self.live_order_books.get(symbol, {})
            
            # Run TA-Lib indicators
            tech_data = self.talib_indicators.calculate_all(ohlcv_df)
            
            # Run advanced indicators
            vwap_bands = self.advanced_indicators.calculate_vwap_bands(ohlcv_df)
            order_flow = self.advanced_indicators.calculate_order_flow_imbalance(order_book)
            cumulative_delta, tick_direction = self.advanced_indicators.calculate_cumulative_delta(ohlcv_df)
            
            # Risk management
            current_price = ohlcv_df['close'].iloc[-1]
            risk_controls = self.risk_manager.calculate_risk_metrics(
                entry_price=current_price,
                current_price=current_price,
                symbol=symbol
            )
            
            # Final analysis
            analysis_result = self.final_analyzer.analyze(
                instrument_key=f"NSE_EQ|{symbol}",
                symbol_name=symbol,
                tech_data=tech_data,
                ohlcv_df=ohlcv_df,
                order_book=order_book,
                cumulative_delta=cumulative_delta,
                risk_controls=risk_controls,
                vwap_bands=vwap_bands,
                order_flow=order_flow
            )
            
            # Store result
            self.analysis_results[symbol] = analysis_result
            
            # Run ML prediction if available
            if hasattr(self.sklearn_runner, 'predict') and len(ohlcv_df) >= 50:
                ml_prediction = self.sklearn_runner.predict(ohlcv_df, tech_data)
                analysis_result['ml_prediction'] = ml_prediction
            
            analysis_time = time.time() - start_time
            logger.info(f"Analysis completed for {symbol} in {analysis_time:.3f}s")
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Analysis failed for {symbol}: {e}")
            return None
    
    def periodic_uploads(self):
        """Handle periodic data uploads and dashboard updates"""
        current_time = time.time()
        
        # Google AI Upload
        if current_time - self.last_upload_time >= self.upload_interval:
            try:
                if self.analysis_results:
                    analysis_list = list(self.analysis_results.values())
                    
                    # Upload to Google AI
                    self.google_uploader.upload_predictions(analysis_list)
                    
                    # Log signals
                    self.signal_logger.log_signals(analysis_list)
                    
                    # Export CSV
                    self.csv_output.export_data(self.ohlcv_data, self.analysis_results)
                    
                    self.last_upload_time = current_time
                    logger.info(f"Uploaded {len(analysis_list)} analysis results")
                    
            except Exception as e:
                logger.error(f"Upload failed: {e}")
        
        # Dashboard Update
        if current_time - self.last_dashboard_update >= self.dashboard_interval:
            try:
                if self.analysis_results:
                    dashboard_data = self.prepare_dashboard_data()
                    self.dashboard_pusher.push_data(dashboard_data)
                    self.last_dashboard_update = current_time
                    
            except Exception as e:
                logger.error(f"Dashboard update failed: {e}")
    
    def prepare_dashboard_data(self):
        """Prepare aggregated data for dashboard"""
        try:
            summary = {
                'timestamp': datetime.now().isoformat(),
                'total_symbols': len(self.analysis_results),
                'active_signals': 0,
                'avg_rsi': 0,
                'market_trend': 'NEUTRAL'
            }
            
            if self.analysis_results:
                rsi_values = []
                signals = 0
                
                for result in self.analysis_results.values():
                    if result.get('technical_indicators', {}).get('RSI'):
                        rsi_values.append(result['technical_indicators']['RSI'])
                    
                    if result.get('signal_strength', 0) > 0.6:
                        signals += 1
                
                if rsi_values:
                    summary['avg_rsi'] = sum(rsi_values) / len(rsi_values)
                    
                summary['active_signals'] = signals
                
                # Determine market trend
                if summary['avg_rsi'] > 70:
                    summary['market_trend'] = 'OVERBOUGHT'
                elif summary['avg_rsi'] < 30:
                    summary['market_trend'] = 'OVERSOLD'
                elif summary['avg_rsi'] > 50:
                    summary['market_trend'] = 'BULLISH'
                else:
                    summary['market_trend'] = 'BEARISH'
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to prepare dashboard data: {e}")
            return {}
    
    def run(self):
        """Main event loop"""
        logger.info("Starting Time Orchestrator...")
        self.running = True
        
        try:
            while self.running:
                # Process trade ticks
                try:
                    while True:
                        tick = self.trade_tick_queue.get_nowait()
                        self.process_trade_tick(tick)
                except Empty:
                    pass
                
                # Process completed candles
                try:
                    while True:
                        candle = self.candle_queue.get_nowait()
                        self.process_completed_candle(candle)
                except Empty:
                    pass
                
                # Handle periodic tasks
                self.periodic_uploads()
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown"""
        logger.info("Shutting down Time Orchestrator...")
        self.running = False
        
        # Wait for running jobs to complete
        self.executor.shutdown(wait=True)
        
        # Final data export
        try:
            if self.analysis_results:
                self.csv_output.export_data(self.ohlcv_data, self.analysis_results)
                logger.info("Final data export completed")
        except Exception as e:
            logger.error(f"Final export failed: {e}")

if __name__ == "__main__":
    orchestrator = TimeOrchestrator()
    orchestrator.run()
    