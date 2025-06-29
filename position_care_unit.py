"""
Position Care Unit - Real-time Risk Management System
===================================================

Professional-grade position monitoring and risk management:
- Real-time P&L tracking
- Risk limit monitoring
- Automated alerts and adjustments
- Portfolio exposure analysis
- Renaissance Technologies-style risk controls
"""

import redis
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import logging
import pytz
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RiskLevel(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class AlertType(Enum):
    POSITION_SIZE = "POSITION_SIZE"
    DRAWDOWN = "DRAWDOWN"
    CONCENTRATION = "CONCENTRATION"
    VAR_BREACH = "VAR_BREACH"
    TIME_LIMIT = "TIME_LIMIT"
    CORRELATION = "CORRELATION"

@dataclass
class RiskAlert:
    alert_id: str
    alert_type: AlertType
    level: RiskLevel
    symbol: str
    message: str
    current_value: float
    threshold: float
    timestamp: datetime
    acknowledged: bool = False

@dataclass
class PositionRisk:
    symbol: str
    current_price: float
    quantity: int
    entry_price: float
    unrealized_pnl: float
    position_value: float
    risk_amount: float
    stop_loss: Optional[float]
    time_in_position: timedelta
    volatility: float
    beta: float
    var_1d: float
    max_drawdown: float
    risk_score: float

class PositionCareUnit:
    """
    Professional position monitoring and risk management system
    """
    
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        
        # Risk limits (configurable)
        self.risk_limits = {
            'max_position_size': 0.20,        # 20% of portfolio per position
            'max_sector_concentration': 0.30,  # 30% per sector
            'max_daily_var': 0.03,            # 3% daily VaR
            'max_drawdown': 0.10,             # 10% max drawdown
            'max_correlation_exposure': 0.50,  # 50% in correlated positions
            'max_time_in_position': 24,       # 24 hours max (intraday focus)
            'stop_loss_buffer': 0.02,         # 2% buffer before stop loss
            'position_size_warning': 0.15     # 15% warning level
        }
        
        # Storage keys
        self.keys = {
            'positions': 'care:positions',
            'alerts': 'care:alerts',
            'risk_metrics': 'care:risk_metrics',
            'portfolio_snapshot': 'care:portfolio_snapshot'
        }
        
        # In-memory caches
        self.current_positions = {}
        self.active_alerts = {}
        self.risk_history = []
        
        # Market data cache for risk calculations
        self.market_data_cache = {}
        
        logger.info("Position Care Unit initialized with risk monitoring")
    
    def update_position(self, symbol: str, position_data: Dict):
        """Update position data and recalculate risks"""
        try:
            current_time = datetime.now(self.ist_timezone)
            
            # Create position risk object
            position_risk = PositionRisk(
                symbol=symbol,
                current_price=position_data.get('current_price', 0),
                quantity=position_data.get('quantity', 0),
                entry_price=position_data.get('entry_price', 0),
                unrealized_pnl=position_data.get('unrealized_pnl', 0),
                position_value=position_data.get('position_value', 0),
                risk_amount=position_data.get('risk_amount', 0),
                stop_loss=position_data.get('stop_loss'),
                time_in_position=current_time - position_data.get('entry_time', current_time),
                volatility=self._calculate_position_volatility(symbol),
                beta=self._calculate_position_beta(symbol),
                var_1d=self._calculate_position_var(symbol, position_data),
                max_drawdown=self._calculate_position_drawdown(symbol, position_data),
                risk_score=0.0  # Will be calculated
            )
            
            # Calculate risk score
            position_risk.risk_score = self._calculate_risk_score(position_risk)
            
            # Store position
            self.current_positions[symbol] = position_risk
            self.redis_client.hset(
                f"{self.keys['positions']}:{symbol}",
                mapping=asdict(position_risk)
            )
            
            # Check for risk violations
            self._check_position_risks(position_risk)
            
            logger.debug(f"Position updated: {symbol} - Risk Score: {position_risk.risk_score:.2f}")
            
        except Exception as e:
            logger.error(f"Error updating position {symbol}: {e}")
    
    def _calculate_position_volatility(self, symbol: str) -> float:
        """Calculate position volatility from market data"""
        try:
            # Get recent price data from Redis or market cache
            if symbol in self.market_data_cache:
                price_history = self.market_data_cache[symbol].get('price_history', [])
                if len(price_history) > 10:
                    prices = [p['price'] for p in price_history[-20:]]
                    returns = np.diff(np.log(prices))
                    return np.std(returns) * np.sqrt(252 * 6.25)  # Annualized volatility
            
            # Default volatility for Indian market
            return 0.25  # 25% default volatility
            
        except Exception as e:
            logger.warning(f"Error calculating volatility for {symbol}: {e}")
            return 0.25
    
    def _calculate_position_beta(self, symbol: str) -> float:
        """Calculate position beta relative to market"""
        try:
            # Simplified beta calculation (would need market index data)
            # For now, return sector-based beta estimates
            sector_betas = {
                'IT': 1.2,
                'BANKING': 1.5,
                'FMCG': 0.8,
                'AUTO': 1.3,
                'PHARMA': 0.9,
                'ENERGY': 1.1
            }
            
            # Try to identify sector from symbol
            for sector, beta in sector_betas.items():
                if any(keyword in symbol.upper() for keyword in self._get_sector_keywords(sector)):
                    return beta
            
            return 1.0  # Market beta
            
        except Exception as e:
            logger.warning(f"Error calculating beta for {symbol}: {e}")
            return 1.0
    
    def _get_sector_keywords(self, sector: str) -> List[str]:
        """Get keywords to identify sector"""
        sector_keywords = {
            'IT': ['TCS', 'INFOSYS', 'HCL', 'WIPRO', 'TECH'],
            'BANKING': ['HDFC', 'ICICI', 'SBI', 'KOTAK', 'BANK'],
            'FMCG': ['HINDUSTAN', 'ITC', 'NESTLE', 'BRITANNIA'],
            'AUTO': ['MARUTI', 'TATA MOTORS', 'BAJAJ', 'HERO'],
            'PHARMA': ['SUN PHARMA', 'DR REDDY', 'CIPLA', 'LUPIN'],
            'ENERGY': ['RELIANCE', 'ONGC', 'NTPC', 'POWER GRID']
        }
        return sector_keywords.get(sector, [])
    
    def _calculate_position_var(self, symbol: str, position_data: Dict) -> float:
        """Calculate 1-day Value at Risk for position"""
        try:
            position_value = abs(position_data.get('position_value', 0))
            volatility = self._calculate_position_volatility(symbol)
            
            # 95% VaR (1.645 z-score)
            daily_vol = volatility / np.sqrt(252)
            var_1d = position_value * daily_vol * 1.645
            
            return var_1d
            
        except Exception as e:
            logger.warning(f"Error calculating VaR for {symbol}: {e}")
            return 0.0
    
    def _calculate_position_drawdown(self, symbol: str, position_data: Dict) -> float:
        """Calculate maximum drawdown for position"""
        try:
            entry_price = position_data.get('entry_price', 0)
            current_price = position_data.get('current_price', 0)
            quantity = position_data.get('quantity', 0)
            
            if entry_price <= 0 or quantity == 0:
                return 0.0
            
            if quantity > 0:  # Long position
                drawdown = max(0, (entry_price - current_price) / entry_price)
            else:  # Short position
                drawdown = max(0, (current_price - entry_price) / entry_price)
            
            return drawdown
            
        except Exception as e:
            logger.warning(f"Error calculating drawdown for {symbol}: {e}")
            return 0.0
    
    def _calculate_risk_score(self, position_risk: PositionRisk) -> float:
        """Calculate comprehensive risk score (0-1, higher = riskier)"""
        try:
            score_components = {}
            
            # Volatility component (0-0.3)
            vol_score = min(0.3, position_risk.volatility / 0.5)  # Normalize by 50% vol
            score_components['volatility'] = vol_score
            
            # Beta component (0-0.2)
            beta_score = min(0.2, abs(position_risk.beta - 1.0) * 0.2)
            score_components['beta'] = beta_score
            
            # Drawdown component (0-0.3)
            dd_score = min(0.3, position_risk.max_drawdown / 0.1)  # Normalize by 10% DD
            score_components['drawdown'] = dd_score
            
            # Time component (0-0.1)
            time_hours = position_risk.time_in_position.total_seconds() / 3600
            time_score = min(0.1, time_hours / self.risk_limits['max_time_in_position'])
            score_components['time'] = time_score
            
            # VaR component (0-0.1)
            var_score = min(0.1, position_risk.var_1d / 100000)  # Normalize by ₹1L VaR
            score_components['var'] = var_score
            
            total_score = sum(score_components.values())
            
            return min(1.0, total_score)
            
        except Exception as e:
            logger.warning(f"Error calculating risk score: {e}")
            return 0.5
    
    def _check_position_risks(self, position_risk: PositionRisk):
        """Check position against risk limits and generate alerts"""
        try:
            current_time = datetime.now(self.ist_timezone)
            
            # Check position size limit
            portfolio_value = self._get_portfolio_value()
            if portfolio_value > 0:
                position_pct = abs(position_risk.position_value) / portfolio_value
                
                if position_pct > self.risk_limits['max_position_size']:
                    self._create_alert(
                        AlertType.POSITION_SIZE,
                        RiskLevel.CRITICAL,
                        position_risk.symbol,
                        f"Position size {position_pct:.1%} exceeds limit {self.risk_limits['max_position_size']:.1%}",
                        position_pct,
                        self.risk_limits['max_position_size']
                    )
                elif position_pct > self.risk_limits['position_size_warning']:
                    self._create_alert(
                        AlertType.POSITION_SIZE,
                        RiskLevel.MEDIUM,
                        position_risk.symbol,
                        f"Position size {position_pct:.1%} approaching limit",
                        position_pct,
                        self.risk_limits['position_size_warning']
                    )
            
            # Check drawdown limit
            if position_risk.max_drawdown > self.risk_limits['max_drawdown']:
                self._create_alert(
                    AlertType.DRAWDOWN,
                    RiskLevel.HIGH,
                    position_risk.symbol,
                    f"Drawdown {position_risk.max_drawdown:.1%} exceeds limit",
                    position_risk.max_drawdown,
                    self.risk_limits['max_drawdown']
                )
            
            # Check time in position
            time_hours = position_risk.time_in_position.total_seconds() / 3600
            if time_hours > self.risk_limits['max_time_in_position']:
                self._create_alert(
                    AlertType.TIME_LIMIT,
                    RiskLevel.MEDIUM,
                    position_risk.symbol,
                    f"Position held for {time_hours:.1f}h, exceeds {self.risk_limits['max_time_in_position']}h limit",
                    time_hours,
                    self.risk_limits['max_time_in_position']
                )
            
            # Check VaR limit
            portfolio_var_limit = portfolio_value * self.risk_limits['max_daily_var']
            if position_risk.var_1d > portfolio_var_limit:
                self._create_alert(
                    AlertType.VAR_BREACH,
                    RiskLevel.HIGH,
                    position_risk.symbol,
                    f"Position VaR ₹{position_risk.var_1d:,.0f} exceeds portfolio limit",
                    position_risk.var_1d,
                    portfolio_var_limit
                )
            
        except Exception as e:
            logger.error(f"Error checking position risks: {e}")
    
    def _create_alert(self, alert_type: AlertType, level: RiskLevel, symbol: str, 
                     message: str, current_value: float, threshold: float):
        """Create and store risk alert"""
        try:
            alert_id = f"{alert_type.value}_{symbol}_{int(datetime.now().timestamp())}"
            
            alert = RiskAlert(
                alert_id=alert_id,
                alert_type=alert_type,
                level=level,
                symbol=symbol,
                message=message,
                current_value=current_value,
                threshold=threshold,
                timestamp=datetime.now(self.ist_timezone)
            )
            
            # Store alert
            self.active_alerts[alert_id] = alert
            self.redis_client.hset(
                f"{self.keys['alerts']}:{alert_id}",
                mapping=asdict(alert)
            )
            
            logger.warning(f"Risk Alert [{level.value}]: {message}")
            
        except Exception as e:
            logger.error(f"Error creating alert: {e}")
    
    def _get_portfolio_value(self) -> float:
        """Get total portfolio value"""
        try:
            total_value = sum(abs(pos.position_value) for pos in self.current_positions.values())
            return total_value
        except Exception as e:
            logger.error(f"Error calculating portfolio value: {e}")
            return 1000000  # Default value
    
    def get_portfolio_risk_summary(self) -> Dict:
        """Get comprehensive portfolio risk summary"""
        try:
            total_positions = len(self.current_positions)
            total_value = self._get_portfolio_value()
            total_pnl = sum(pos.unrealized_pnl for pos in self.current_positions.values())
            total_var = sum(pos.var_1d for pos in self.current_positions.values())
            
            # Risk metrics
            portfolio_var_pct = (total_var / total_value) if total_value > 0 else 0
            max_position_pct = max((abs(pos.position_value) / total_value 
                                  for pos in self.current_positions.values()), default=0)
            avg_risk_score = np.mean([pos.risk_score for pos in self.current_positions.values()]) if self.current_positions else 0
            
            # Active alerts by level
            alerts_by_level = defaultdict(int)
            for alert in self.active_alerts.values():
                if not alert.acknowledged:
                    alerts_by_level[alert.level.value] += 1
            
            # Sector concentration
            sector_exposure = self._calculate_sector_exposure()
            
            return {
                'total_positions': total_positions,
                'total_portfolio_value': total_value,
                'total_unrealized_pnl': total_pnl,
                'portfolio_var_1d': total_var,
                'portfolio_var_pct': portfolio_var_pct,
                'max_position_concentration': max_position_pct,
                'average_risk_score': avg_risk_score,
                'risk_utilization': {
                    'var_utilization': portfolio_var_pct / self.risk_limits['max_daily_var'],
                    'position_size_utilization': max_position_pct / self.risk_limits['max_position_size']
                },
                'active_alerts': dict(alerts_by_level),
                'sector_exposure': sector_exposure,
                'risk_limits': self.risk_limits,
                'timestamp': datetime.now(self.ist_timezone).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating portfolio risk summary: {e}")
            return {}
    
    def _calculate_sector_exposure(self) -> Dict[str, float]:
        """Calculate sector-wise exposure"""
        sector_exposure = defaultdict(float)
        total_value = self._get_portfolio_value()
        
        if total_value <= 0:
            return {}
        
        for symbol, position in self.current_positions.items():
            # Identify sector
            sector = 'OTHER'
            for sector_name in ['IT', 'BANKING', 'FMCG', 'AUTO', 'PHARMA', 'ENERGY']:
                if any(keyword in symbol.upper() for keyword in self._get_sector_keywords(sector_name)):
                    sector = sector_name
                    break
            
            sector_exposure[sector] += abs(position.position_value) / total_value
        
        return dict(sector_exposure)
    
    def get_position_details(self, symbol: str = None) -> Dict:
        """Get detailed position information"""
        if symbol and symbol in self.current_positions:
            return asdict(self.current_positions[symbol])
        elif symbol:
            return None
        else:
            return {sym: asdict(pos) for sym, pos in self.current_positions.items()}
    
    def get_active_alerts(self, level: RiskLevel = None) -> List[Dict]:
        """Get active risk alerts"""
        alerts = []
        for alert in self.active_alerts.values():
            if not alert.acknowledged and (level is None or alert.level == level):
                alerts.append(asdict(alert))
        
        # Sort by timestamp (newest first)
        alerts.sort(key=lambda x: x['timestamp'], reverse=True)
        return alerts
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge a risk alert"""
        try:
            if alert_id in self.active_alerts:
                self.active_alerts[alert_id].acknowledged = True
                self.redis_client.hset(
                    f"{self.keys['alerts']}:{alert_id}",
                    'acknowledged',
                    'true'
                )
                logger.info(f"Alert acknowledged: {alert_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error acknowledging alert {alert_id}: {e}")
            return False
    
    def update_risk_limits(self, new_limits: Dict) -> bool:
        """Update risk limits"""
        try:
            for key, value in new_limits.items():
                if key in self.risk_limits:
                    self.risk_limits[key] = value
            
            # Store updated limits
            self.redis_client.hset(self.keys['risk_metrics'], 'risk_limits', json.dumps(self.risk_limits))
            logger.info("Risk limits updated")
            return True
            
        except Exception as e:
            logger.error(f"Error updating risk limits: {e}")
            return False
    
    def cleanup_old_alerts(self, hours_old: int = 24):
        """Clean up old acknowledged alerts"""
        try:
            cutoff_time = datetime.now(self.ist_timezone) - timedelta(hours=hours_old)
            
            alerts_to_remove = []
            for alert_id, alert in self.active_alerts.items():
                if alert.acknowledged and alert.timestamp < cutoff_time:
                    alerts_to_remove.append(alert_id)
            
            for alert_id in alerts_to_remove:
                del self.active_alerts[alert_id]
                self.redis_client.delete(f"{self.keys['alerts']}:{alert_id}")
            
            logger.info(f"Cleaned up {len(alerts_to_remove)} old alerts")
            
        except Exception as e:
            logger.error(f"Error cleaning up alerts: {e}")

# Global instance
position_care_unit = None

def get_position_care_unit() -> PositionCareUnit:
    """Get or create position care unit instance"""
    global position_care_unit
    if position_care_unit is None:
        position_care_unit = PositionCareUnit()
    return position_care_unit

if __name__ == "__main__":
    # Test the position care unit
    care_unit = PositionCareUnit()
    
    # Test position update
    test_position = {
        'current_price': 2750.50,
        'quantity': 100,
        'entry_price': 2700.00,
        'unrealized_pnl': 5050.0,
        'position_value': 275050.0,
        'risk_amount': 2500.0,
        'stop_loss': 2650.0,
        'entry_time': datetime.now(pytz.timezone('Asia/Kolkata')) - timedelta(hours=2)
    }
    
    care_unit.update_position('Reliance Industries', test_position)
    
    # Get risk summary
    risk_summary = care_unit.get_portfolio_risk_summary()
    print(f"Portfolio Risk Summary: {json.dumps(risk_summary, indent=2, default=str)}")
    
    # Get alerts
    alerts = care_unit.get_active_alerts()
    print(f"Active Alerts: {len(alerts)}")
    for alert in alerts:
        print(f"- {alert['level']}: {alert['message']}")