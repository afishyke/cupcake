"""
Paper Trading Engine for Quantitative Signal Validation
======================================================

Automated paper trading system to validate quantitative signals without real money.
Features:
- Automatic execution of quantitative signals
- Position tracking and P&L calculation
- Performance analytics and model validation
- Risk management controls
- Trade history and audit trail
"""

import os
import json
import redis
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import threading
import time
from collections import defaultdict
import pytz

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TradeType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class TradeStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    STOPPED = "STOPPED"  # Hit stop loss
    TARGET_HIT = "TARGET_HIT"  # Hit target
    EXPIRED = "EXPIRED"  # Expired by time

@dataclass
class PaperTrade:
    """Paper trade representation"""
    trade_id: str
    symbol: str
    trade_type: TradeType
    entry_price: float
    target_price: float
    stop_loss: float
    quantity: int
    entry_time: datetime
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    status: TradeStatus = TradeStatus.OPEN
    signal_confidence: float = 0.0
    signal_factors: Dict = None
    pnl: float = 0.0
    pnl_pct: float = 0.0
    hold_period_hours: float = 0.0
    max_favorable: float = 0.0  # Maximum favorable excursion
    max_adverse: float = 0.0    # Maximum adverse excursion
    commission: float = 0.0
    slippage: float = 0.0

@dataclass
class Portfolio:
    """Paper trading portfolio"""
    initial_capital: float = 1000000.0  # 10 Lakh starting capital
    current_capital: float = 1000000.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    max_drawdown: float = 0.0
    max_capital: float = 1000000.0
    open_positions: int = 0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    last_updated: datetime = None

class PaperTradingEngine:
    """Main paper trading engine"""
    
    def __init__(self, redis_host=None, redis_port=None, initial_capital=1000000.0):
        # Redis connection
        if redis_host is None:
            redis_host = os.environ.get('REDIS_HOST', 'localhost')
        if redis_port is None:
            redis_port = int(os.environ.get('REDIS_PORT', '6379'))
            
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Portfolio
        self.portfolio = Portfolio(initial_capital=initial_capital, current_capital=initial_capital)
        
        # Trade storage
        self.open_trades: Dict[str, PaperTrade] = {}
        self.closed_trades: List[PaperTrade] = []
        self.trade_counter = 0
        
        # Configuration
        self.config = {
            'commission_rate': 0.001,  # 0.1% commission per trade
            'slippage_rate': 0.0005,   # 0.05% slippage
            'max_positions': 10,       # Maximum concurrent positions
            'max_position_size': 0.05, # 5% max per position
            'max_portfolio_risk': 0.20, # 20% max portfolio risk
            'auto_execute': True,      # Automatically execute signals
            'signal_confidence_threshold': 0.3,  # Minimum confidence to trade
            'max_hold_period_days': 30,  # Maximum hold period
            'risk_free_rate': 0.07     # For Sharpe ratio calculation
        }
        
        # Performance tracking
        self.daily_pnl: List[Tuple[datetime, float]] = []
        self.trade_history: List[Dict] = []
        
        # Threading
        self.running = False
        self.monitor_thread = None
        
        logger.info(f"Paper Trading Engine initialized with ₹{initial_capital:,.0f} capital")
    
    def start_monitoring(self):
        """Start the monitoring thread"""
        if not self.running:
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitor_positions, daemon=True)
            self.monitor_thread.start()
            logger.info("Paper trading monitoring started")
    
    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("Paper trading monitoring stopped")
    
    def _monitor_positions(self):
        """Monitor open positions for stop loss/target hits"""
        while self.running:
            try:
                if self.open_trades:
                    self._check_position_exits()
                    self._check_expired_positions()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Error in position monitoring: {e}")
                time.sleep(60)  # Wait longer on error
    
    def _check_position_exits(self):
        """Check if any positions should be closed"""
        current_time = datetime.now(self.ist_timezone)
        positions_to_close = []
        
        for trade_id, trade in self.open_trades.items():
            try:
                current_price = self._get_current_price(trade.symbol)
                if current_price is None:
                    continue
                
                # Update max favorable/adverse excursion
                if trade.trade_type == TradeType.LONG:
                    unrealized_pnl = (current_price - trade.entry_price) / trade.entry_price
                    if unrealized_pnl > trade.max_favorable:
                        trade.max_favorable = unrealized_pnl
                    if unrealized_pnl < trade.max_adverse:
                        trade.max_adverse = unrealized_pnl
                    
                    # Check stop loss and target
                    if current_price <= trade.stop_loss:
                        positions_to_close.append((trade_id, current_price, TradeStatus.STOPPED))
                    elif current_price >= trade.target_price:
                        positions_to_close.append((trade_id, current_price, TradeStatus.TARGET_HIT))
                        
                else:  # SHORT
                    unrealized_pnl = (trade.entry_price - current_price) / trade.entry_price
                    if unrealized_pnl > trade.max_favorable:
                        trade.max_favorable = unrealized_pnl
                    if unrealized_pnl < trade.max_adverse:
                        trade.max_adverse = unrealized_pnl
                    
                    # Check stop loss and target (reversed for short)
                    if current_price >= trade.stop_loss:
                        positions_to_close.append((trade_id, current_price, TradeStatus.STOPPED))
                    elif current_price <= trade.target_price:
                        positions_to_close.append((trade_id, current_price, TradeStatus.TARGET_HIT))
                        
            except Exception as e:
                logger.error(f"Error checking position {trade_id}: {e}")
        
        # Close positions
        for trade_id, exit_price, status in positions_to_close:
            self._close_position(trade_id, exit_price, status)
    
    def _check_expired_positions(self):
        """Check for expired positions"""
        current_time = datetime.now(self.ist_timezone)
        max_hold_period = timedelta(days=self.config['max_hold_period_days'])
        
        positions_to_close = []
        for trade_id, trade in self.open_trades.items():
            if current_time - trade.entry_time > max_hold_period:
                current_price = self._get_current_price(trade.symbol)
                if current_price:
                    positions_to_close.append((trade_id, current_price, TradeStatus.EXPIRED))
        
        for trade_id, exit_price, status in positions_to_close:
            self._close_position(trade_id, exit_price, status)
    
    def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol"""
        try:
            symbol_clean = symbol.replace(' ', '_').replace('.', '').replace('&', 'and')
            close_key = f"stock:{symbol_clean}:close"
            
            # Get latest price
            latest_data = self.redis_client.execute_command('TS.RANGE', close_key, '-', '+', 'COUNT', 1)
            if latest_data and len(latest_data) > 0:
                return float(latest_data[0][1])
            return None
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            return None
    
    def execute_signal(self, signal_data: Dict) -> Optional[str]:
        """Execute a quantitative signal as a paper trade"""
        try:
            # Validate signal
            if not self._should_execute_signal(signal_data):
                return None
            
            # Calculate position size
            position_value = self._calculate_position_value(signal_data)
            if position_value <= 0:
                logger.warning(f"Position value too small for {signal_data['symbol']}")
                return None
            
            quantity = int(position_value / signal_data['entry_price'])
            if quantity <= 0:
                return None
            
            # Apply slippage
            entry_price = self._apply_slippage(signal_data['entry_price'], signal_data['signal_type'])
            
            # Create trade
            trade_id = f"PAPER_{self.trade_counter:06d}"
            self.trade_counter += 1
            
            trade = PaperTrade(
                trade_id=trade_id,
                symbol=signal_data['symbol'],
                trade_type=TradeType(signal_data['signal_type']),
                entry_price=entry_price,
                target_price=signal_data['target_price'],
                stop_loss=signal_data['stop_loss'],
                quantity=quantity,
                entry_time=datetime.now(self.ist_timezone),
                signal_confidence=signal_data.get('confidence', 0.0),
                signal_factors=signal_data.get('factors', {}),
                commission=position_value * self.config['commission_rate'],
                slippage=abs(entry_price - signal_data['entry_price']) * quantity
            )
            
            # Add to open positions
            self.open_trades[trade_id] = trade
            
            # Update portfolio
            self.portfolio.open_positions += 1
            self.portfolio.total_trades += 1
            self.portfolio.current_capital -= trade.commission  # Deduct commission
            
            # Log trade
            logger.info(f"📊 PAPER TRADE EXECUTED: {trade.trade_type.value} {trade.symbol} "
                       f"@ ₹{trade.entry_price:.2f} | Qty: {trade.quantity} | "
                       f"Confidence: {trade.signal_confidence*100:.1f}%")
            
            self._save_trade_to_history(trade, 'OPENED')
            return trade_id
            
        except Exception as e:
            logger.error(f"Error executing signal for {signal_data.get('symbol', 'Unknown')}: {e}")
            return None
    
    def _should_execute_signal(self, signal_data: Dict) -> bool:
        """Check if signal should be executed"""
        # Check confidence threshold
        if signal_data.get('confidence', 0) < self.config['signal_confidence_threshold']:
            return False
        
        # Check maximum positions
        if len(self.open_trades) >= self.config['max_positions']:
            return False
        
        # Check if already have position in this symbol
        for trade in self.open_trades.values():
            if trade.symbol == signal_data['symbol']:
                return False  # Don't double up on same symbol
        
        # Check portfolio risk
        current_risk = self._calculate_current_portfolio_risk()
        if current_risk > self.config['max_portfolio_risk']:
            return False
        
        return True
    
    def _calculate_position_value(self, signal_data: Dict) -> float:
        """Calculate position value based on signal confidence and risk"""
        base_position_pct = signal_data.get('position_size_pct', 0.02)  # Default 2%
        confidence = signal_data.get('confidence', 0.5)
        
        # Adjust position size by confidence
        adjusted_position_pct = base_position_pct * (confidence ** 2)  # Square for conservatism
        
        # Cap at maximum position size
        final_position_pct = min(adjusted_position_pct, self.config['max_position_size'])
        
        return self.portfolio.current_capital * final_position_pct
    
    def _calculate_current_portfolio_risk(self) -> float:
        """Calculate current portfolio risk exposure"""
        total_risk = 0.0
        for trade in self.open_trades.values():
            position_value = trade.quantity * trade.entry_price
            if trade.trade_type == TradeType.LONG:
                risk = (trade.entry_price - trade.stop_loss) * trade.quantity
            else:
                risk = (trade.stop_loss - trade.entry_price) * trade.quantity
            
            risk_pct = risk / self.portfolio.current_capital
            total_risk += risk_pct
        
        return total_risk
    
    def _apply_slippage(self, price: float, signal_type: str) -> float:
        """Apply slippage to entry price"""
        slippage = price * self.config['slippage_rate']
        if signal_type == 'LONG':
            return price + slippage  # Pay slightly more for long
        else:
            return price - slippage  # Get slightly less for short
    
    def _close_position(self, trade_id: str, exit_price: float, status: TradeStatus):
        """Close a position"""
        try:
            trade = self.open_trades.get(trade_id)
            if not trade:
                return
            
            # Apply slippage to exit
            exit_price = self._apply_slippage(exit_price, trade.trade_type.value)
            
            # Calculate P&L
            if trade.trade_type == TradeType.LONG:
                pnl = (exit_price - trade.entry_price) * trade.quantity
            else:
                pnl = (trade.entry_price - exit_price) * trade.quantity
            
            # Deduct commission
            exit_commission = exit_price * trade.quantity * self.config['commission_rate']
            pnl -= exit_commission
            
            # Update trade
            trade.exit_time = datetime.now(self.ist_timezone)
            trade.exit_price = exit_price
            trade.status = status
            trade.pnl = pnl
            trade.pnl_pct = pnl / (trade.entry_price * trade.quantity) * 100
            trade.hold_period_hours = (trade.exit_time - trade.entry_time).total_seconds() / 3600
            trade.commission += exit_commission
            
            # Update portfolio
            self.portfolio.current_capital += pnl
            self.portfolio.total_pnl += pnl
            self.portfolio.total_pnl_pct = (self.portfolio.current_capital - self.portfolio.initial_capital) / self.portfolio.initial_capital * 100
            self.portfolio.open_positions -= 1
            
            # Track max capital and drawdown
            if self.portfolio.current_capital > self.portfolio.max_capital:
                self.portfolio.max_capital = self.portfolio.current_capital
            
            current_drawdown = (self.portfolio.max_capital - self.portfolio.current_capital) / self.portfolio.max_capital
            if current_drawdown > self.portfolio.max_drawdown:
                self.portfolio.max_drawdown = current_drawdown
            
            # Update win/loss statistics
            if pnl > 0:
                self.portfolio.winning_trades += 1
            else:
                self.portfolio.losing_trades += 1
            
            self._update_portfolio_metrics()
            
            # Move to closed trades
            self.closed_trades.append(trade)
            del self.open_trades[trade_id]
            
            # Log closure
            status_emoji = "🎯" if status == TradeStatus.TARGET_HIT else "🛑" if status == TradeStatus.STOPPED else "⏰"
            logger.info(f"{status_emoji} PAPER TRADE CLOSED: {trade.symbol} | "
                       f"P&L: ₹{pnl:,.0f} ({trade.pnl_pct:+.1f}%) | "
                       f"Status: {status.value} | Hold: {trade.hold_period_hours:.1f}h")
            
            self._save_trade_to_history(trade, 'CLOSED')
            
        except Exception as e:
            logger.error(f"Error closing position {trade_id}: {e}")
    
    def _update_portfolio_metrics(self):
        """Update portfolio performance metrics"""
        if self.portfolio.total_trades == 0:
            return
        
        # Win rate
        self.portfolio.win_rate = self.portfolio.winning_trades / self.portfolio.total_trades * 100
        
        # Average win/loss
        winning_trades = [t for t in self.closed_trades if t.pnl > 0]
        losing_trades = [t for t in self.closed_trades if t.pnl < 0]
        
        self.portfolio.avg_win = np.mean([t.pnl for t in winning_trades]) if winning_trades else 0
        self.portfolio.avg_loss = abs(np.mean([t.pnl for t in losing_trades])) if losing_trades else 0
        
        # Profit factor
        total_wins = sum([t.pnl for t in winning_trades])
        total_losses = abs(sum([t.pnl for t in losing_trades]))
        self.portfolio.profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
        
        # Sharpe ratio (simplified)
        if len(self.closed_trades) > 5:
            returns = [t.pnl_pct / 100 for t in self.closed_trades]
            excess_returns = np.mean(returns) - (self.config['risk_free_rate'] / 252)  # Daily risk-free rate
            self.portfolio.sharpe_ratio = excess_returns / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
        
        self.portfolio.last_updated = datetime.now(self.ist_timezone)
    
    def _save_trade_to_history(self, trade: PaperTrade, action: str):
        """Save trade to history for audit trail"""
        history_entry = {
            'timestamp': datetime.now(self.ist_timezone).isoformat(),
            'action': action,
            'trade_data': asdict(trade)
        }
        self.trade_history.append(history_entry)
        
        # Save to Redis for persistence
        try:
            history_key = f"paper_trades:history:{trade.trade_id}"
            self.redis_client.setex(history_key, 86400 * 30, json.dumps(history_entry))  # 30 days TTL
        except Exception as e:
            logger.error(f"Error saving trade history: {e}")
    
    def get_portfolio_summary(self) -> Dict:
        """Get portfolio performance summary"""
        return {
            'capital': {
                'initial': self.portfolio.initial_capital,
                'current': self.portfolio.current_capital,
                'total_pnl': self.portfolio.total_pnl,
                'total_pnl_pct': self.portfolio.total_pnl_pct,
                'max_drawdown': self.portfolio.max_drawdown * 100
            },
            'positions': {
                'open_positions': self.portfolio.open_positions,
                'total_trades': self.portfolio.total_trades,
                'winning_trades': self.portfolio.winning_trades,
                'losing_trades': self.portfolio.losing_trades
            },
            'performance': {
                'win_rate': self.portfolio.win_rate,
                'avg_win': self.portfolio.avg_win,
                'avg_loss': self.portfolio.avg_loss,
                'profit_factor': self.portfolio.profit_factor,
                'sharpe_ratio': self.portfolio.sharpe_ratio
            },
            'last_updated': self.portfolio.last_updated.isoformat() if self.portfolio.last_updated else None
        }
    
    def get_open_positions(self) -> List[Dict]:
        """Get current open positions"""
        positions = []
        for trade in self.open_trades.values():
            current_price = self._get_current_price(trade.symbol)
            if current_price:
                if trade.trade_type == TradeType.LONG:
                    unrealized_pnl = (current_price - trade.entry_price) * trade.quantity
                    unrealized_pnl_pct = (current_price - trade.entry_price) / trade.entry_price * 100
                else:
                    unrealized_pnl = (trade.entry_price - current_price) * trade.quantity
                    unrealized_pnl_pct = (trade.entry_price - current_price) / trade.entry_price * 100
                
                positions.append({
                    'trade_id': trade.trade_id,
                    'symbol': trade.symbol,
                    'trade_type': trade.trade_type.value,
                    'entry_price': trade.entry_price,
                    'current_price': current_price,
                    'target_price': trade.target_price,
                    'stop_loss': trade.stop_loss,
                    'quantity': trade.quantity,
                    'unrealized_pnl': unrealized_pnl,
                    'unrealized_pnl_pct': unrealized_pnl_pct,
                    'signal_confidence': trade.signal_confidence,
                    'hold_period_hours': (datetime.now(self.ist_timezone) - trade.entry_time).total_seconds() / 3600,
                    'max_favorable': trade.max_favorable * 100,
                    'max_adverse': trade.max_adverse * 100
                })
        return positions
    
    def get_recent_trades(self, limit: int = 20) -> List[Dict]:
        """Get recent closed trades"""
        recent_trades = sorted(self.closed_trades, key=lambda x: x.exit_time, reverse=True)[:limit]
        return [asdict(trade) for trade in recent_trades]
    
    def get_performance_analytics(self) -> Dict:
        """Get detailed performance analytics"""
        if not self.closed_trades:
            return {'message': 'No closed trades for analysis'}
        
        trades_df = pd.DataFrame([asdict(trade) for trade in self.closed_trades])
        
        # Monthly returns
        trades_df['exit_month'] = pd.to_datetime(trades_df['exit_time']).dt.to_period('M')
        monthly_pnl = trades_df.groupby('exit_month')['pnl'].sum()
        
        # Trade analysis
        winning_trades = trades_df[trades_df['pnl'] > 0]
        losing_trades = trades_df[trades_df['pnl'] < 0]
        
        # Confidence analysis
        high_conf_trades = trades_df[trades_df['signal_confidence'] > 0.6]
        low_conf_trades = trades_df[trades_df['signal_confidence'] <= 0.4]
        
        return {
            'total_trades': len(trades_df),
            'win_rate': len(winning_trades) / len(trades_df) * 100,
            'avg_hold_period_hours': trades_df['hold_period_hours'].mean(),
            'best_trade': trades_df.loc[trades_df['pnl'].idxmax()]['pnl'] if not trades_df.empty else 0,
            'worst_trade': trades_df.loc[trades_df['pnl'].idxmin()]['pnl'] if not trades_df.empty else 0,
            'confidence_analysis': {
                'high_confidence_win_rate': len(high_conf_trades[high_conf_trades['pnl'] > 0]) / len(high_conf_trades) * 100 if len(high_conf_trades) > 0 else 0,
                'low_confidence_win_rate': len(low_conf_trades[low_conf_trades['pnl'] > 0]) / len(low_conf_trades) * 100 if len(low_conf_trades) > 0 else 0,
            },
            'monthly_pnl': monthly_pnl.to_dict() if not monthly_pnl.empty else {},
            'signal_factor_performance': self._analyze_factor_performance(trades_df)
        }
    
    def _analyze_factor_performance(self, trades_df: pd.DataFrame) -> Dict:
        """Analyze which signal factors correlate with winning trades"""
        try:
            factor_analysis = {}
            
            for _, trade in trades_df.iterrows():
                if trade['signal_factors'] and isinstance(trade['signal_factors'], dict):
                    is_winner = trade['pnl'] > 0
                    
                    for factor, value in trade['signal_factors'].items():
                        if factor not in factor_analysis:
                            factor_analysis[factor] = {'wins': 0, 'losses': 0, 'total_value': 0, 'count': 0}
                        
                        if is_winner:
                            factor_analysis[factor]['wins'] += 1
                        else:
                            factor_analysis[factor]['losses'] += 1
                        
                        factor_analysis[factor]['total_value'] += value
                        factor_analysis[factor]['count'] += 1
            
            # Calculate win rates and averages for each factor
            for factor in factor_analysis:
                total = factor_analysis[factor]['wins'] + factor_analysis[factor]['losses']
                factor_analysis[factor]['win_rate'] = factor_analysis[factor]['wins'] / total * 100 if total > 0 else 0
                factor_analysis[factor]['avg_value'] = factor_analysis[factor]['total_value'] / factor_analysis[factor]['count']
            
            return factor_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing factor performance: {e}")
            return {}

class AutomatedSignalExecutor:
    """Automated executor for quantitative signals"""
    
    def __init__(self, paper_engine: PaperTradingEngine):
        self.paper_engine = paper_engine
        self.running = False
        self.executor_thread = None
        self.last_signal_check = datetime.now()
        
    def start(self):
        """Start automated signal execution"""
        if not self.running:
            self.running = True
            self.executor_thread = threading.Thread(target=self._execute_signals_loop, daemon=True)
            self.executor_thread.start()
            logger.info("🤖 Automated signal executor started")
    
    def stop(self):
        """Stop automated signal execution"""
        self.running = False
        if self.executor_thread:
            self.executor_thread.join()
        logger.info("🤖 Automated signal executor stopped")
    
    def _execute_signals_loop(self):
        """Main execution loop"""
        while self.running:
            try:
                self._check_and_execute_signals()
                time.sleep(60)  # Check for new signals every minute
            except Exception as e:
                logger.error(f"Error in signal execution loop: {e}")
                time.sleep(120)  # Wait longer on error
    
    def _check_and_execute_signals(self):
        """Check for new signals and execute them"""
        try:
            import requests
            
            # Get quantitative signals
            response = requests.get('http://localhost:5000/api/signals/quantitative', timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('signals'):
                    for signal_data in data['signals']:
                        # Convert quantitative signal to paper trading format
                        paper_signal = self._convert_quant_signal_to_paper_format(signal_data)
                        
                        # Check if we already have this signal (basic deduplication)
                        if not self._is_duplicate_signal(paper_signal):
                            trade_id = self.paper_engine.execute_signal(paper_signal)
                            if trade_id:
                                logger.info(f"🤖 AUTO-EXECUTED: {paper_signal['symbol']} | Trade ID: {trade_id}")
                            
        except Exception as e:
            logger.error(f"Error checking signals: {e}")
    
    def _convert_quant_signal_to_paper_format(self, quant_signal: Dict) -> Dict:
        """Convert quantitative signal to paper trading format"""
        try:
            # Extract signal type from quantitative signal
            signal_type = quant_signal.get('signal_type', 'LONG')
            
            # Ensure signal_type is uppercase and valid
            if signal_type.upper() not in ['LONG', 'SHORT']:
                signal_type = 'LONG'
            
            return {
                'symbol': quant_signal.get('symbol', ''),
                'signal_type': signal_type.upper(),
                'confidence': quant_signal.get('confidence', 0.5),
                'entry_price': quant_signal.get('entry_price', 0),
                'target_price': quant_signal.get('target_price', 0),
                'stop_loss': quant_signal.get('stop_loss', 0),
                'position_size_pct': quant_signal.get('position_size_pct', 0.02),
                'factors': quant_signal.get('factors', {}),
                'statistical_metrics': quant_signal.get('statistical_metrics', {})
            }
        except Exception as e:
            logger.error(f"Error converting signal format: {e}")
            return {
                'symbol': quant_signal.get('symbol', ''),
                'signal_type': 'LONG',
                'confidence': 0.3,
                'entry_price': 0,
                'target_price': 0,
                'stop_loss': 0,
                'position_size_pct': 0.02,
                'factors': {},
                'statistical_metrics': {}
            }
    
    def _is_duplicate_signal(self, signal: Dict) -> bool:
        """Check if we already have a position in this symbol"""
        for trade in self.paper_engine.open_trades.values():
            if (trade.symbol == signal['symbol'] and 
                trade.trade_type.value == signal['signal_type']):
                return True
        return False