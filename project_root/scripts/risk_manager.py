#!/usr/bin/env python3
"""
Ultra-fast risk management module for real-time trading
Optimized for speed and reliability with minimal overhead
"""

import json
import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from numba import jit
import numpy as np

logger = logging.getLogger(__name__)

@dataclass
class RiskMetrics:
    """Lightweight risk metrics container"""
    pre_set_stop_level: float
    trailing_stop: float
    position_size: int
    max_loss: float
    profit_target: float
    risk_reward_ratio: float
    current_pnl: float
    drawdown_pct: float

class RiskManager:
    """Ultra-fast risk management with pre-compiled calculations"""
    
    def __init__(self, risk_settings_path: str = "config/risk_settings.json"):
        self.settings = {}
        self.active_positions = {}
        self.max_positions = {}
        self._load_settings(risk_settings_path)
        
    def _load_settings(self, path: str) -> None:
        """Load risk settings with error handling"""
        try:
            with open(path, 'r') as f:
                self.settings = json.load(f)
            logger.info(f"Loaded risk settings for {len(self.settings)} instruments")
        except Exception as e:
            logger.error(f"Failed to load risk settings: {e}")
            # Default fallback settings
            self.settings = {
                "default": {
                    "stop_loss_pct": 2.0,
                    "trailing_stop_pct": 1.5,
                    "max_position_size": 100,
                    "risk_per_trade": 1.0,
                    "profit_target_ratio": 2.0
                }
            }
    
    @jit(nopython=True, cache=True)
    def _calculate_stop_loss(entry_price: float, stop_pct: float, is_long: bool) -> float:
        """JIT-compiled stop loss calculation"""
        if is_long:
            return entry_price * (1.0 - stop_pct / 100.0)
        else:
            return entry_price * (1.0 + stop_pct / 100.0)
    
    @jit(nopython=True, cache=True)
    def _calculate_trailing_stop(current_price: float, entry_price: float, 
                                trailing_pct: float, is_long: bool) -> float:
        """JIT-compiled trailing stop calculation"""
        if is_long:
            return current_price * (1.0 - trailing_pct / 100.0)
        else:
            return current_price * (1.0 + trailing_pct / 100.0)
    
    @jit(nopython=True, cache=True)
    def _calculate_position_size(account_balance: float, risk_pct: float, 
                                entry_price: float, stop_loss: float) -> int:
        """JIT-compiled position sizing"""
        risk_amount = account_balance * (risk_pct / 100.0)
        price_diff = abs(entry_price - stop_loss)
        if price_diff > 0:
            return int(risk_amount / price_diff)
        return 0
    
    def get_risk_params(self, instrument_key: str) -> Dict:
        """Get risk parameters for instrument"""
        return self.settings.get(instrument_key, self.settings.get("default", {}))
    
    def calculate_risk_metrics(self, instrument_key: str, entry_price: float, 
                             current_price: float, is_long: bool = True,
                             account_balance: float = 100000.0) -> RiskMetrics:
        """Calculate comprehensive risk metrics"""
        params = self.get_risk_params(instrument_key)
        
        # Fast calculations using JIT functions
        stop_loss = self._calculate_stop_loss(
            entry_price, params.get("stop_loss_pct", 2.0), is_long
        )
        
        trailing_stop = self._calculate_trailing_stop(
            current_price, entry_price, params.get("trailing_stop_pct", 1.5), is_long
        )
        
        position_size = self._calculate_position_size(
            account_balance, params.get("risk_per_trade", 1.0), 
            entry_price, stop_loss
        )
        
        # PnL calculations
        if is_long:
            current_pnl = (current_price - entry_price) * position_size
        else:
            current_pnl = (entry_price - current_price) * position_size
            
        max_loss = abs(entry_price - stop_loss) * position_size
        profit_target = entry_price + (abs(entry_price - stop_loss) * 
                                     params.get("profit_target_ratio", 2.0))
        
        drawdown_pct = (current_pnl / account_balance) * 100 if current_pnl < 0 else 0.0
        
        return RiskMetrics(
            pre_set_stop_level=stop_loss,
            trailing_stop=trailing_stop,
            position_size=position_size,
            max_loss=max_loss,
            profit_target=profit_target,
            risk_reward_ratio=params.get("profit_target_ratio", 2.0),
            current_pnl=current_pnl,
            drawdown_pct=drawdown_pct
        )
    
    def update_position(self, instrument_key: str, entry_price: float, 
                       quantity: int, is_long: bool) -> None:
        """Track active position"""
        self.active_positions[instrument_key] = {
            "entry_price": entry_price,
            "quantity": quantity,
            "is_long": is_long,
            "max_favorable_price": entry_price
        }
    
    def should_exit_position(self, instrument_key: str, current_price: float) -> Tuple[bool, str]:
        """Fast exit signal detection"""
        if instrument_key not in self.active_positions:
            return False, ""
            
        position = self.active_positions[instrument_key]
        risk_metrics = self.calculate_risk_metrics(
            instrument_key, position["entry_price"], current_price, position["is_long"]
        )
        
        # Stop loss check
        if position["is_long"]:
            if current_price <= risk_metrics.pre_set_stop_level:
                return True, "STOP_LOSS"
            if current_price <= risk_metrics.trailing_stop:
                return True, "TRAILING_STOP"
        else:
            if current_price >= risk_metrics.pre_set_stop_level:
                return True, "STOP_LOSS"
            if current_price >= risk_metrics.trailing_stop:
                return True, "TRAILING_STOP"
        
        # Profit target check
        if position["is_long"] and current_price >= risk_metrics.profit_target:
            return True, "PROFIT_TARGET"
        elif not position["is_long"] and current_price <= risk_metrics.profit_target:
            return True, "PROFIT_TARGET"
            
        return False, ""
    
    def get_portfolio_risk(self) -> Dict:
        """Get overall portfolio risk metrics"""
        total_positions = len(self.active_positions)
        total_exposure = sum(
            pos["entry_price"] * pos["quantity"] 
            for pos in self.active_positions.values()
        )
        
        return {
            "total_positions": total_positions,
            "total_exposure": total_exposure,
            "max_positions": self.max_positions.get("default", 10),
            "can_add_position": total_positions < self.max_positions.get("default", 10)
        }
    
    def remove_position(self, instrument_key: str) -> None:
        """Remove closed position"""
        self.active_positions.pop(instrument_key, None)
    
    def get_risk_summary(self) -> Dict:
        """Get comprehensive risk summary"""
        return {
            "active_positions": len(self.active_positions),
            "risk_settings_loaded": len(self.settings),
            "portfolio_risk": self.get_portfolio_risk()
        }