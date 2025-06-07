"""
Risk Manager Module - Stateful class for tracking trade-specific risk controls
Maintains risk state and calculates dynamic risk levels for each symbol/trade.
"""

import logging
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class RiskManager:
    """
    A stateful risk management class that tracks trade-specific risk controls.
    Each instance should be created per symbol/trade to maintain separate state.
    """
    
    def __init__(self, 
                 entry_price: float,
                 stop_loss_pct: float = 2.0,
                 profit_target_pct: float = 6.0,
                 trailing_stop_pct: float = 1.5):
        """
        Initialize risk manager for a specific trade.
        
        Args:
            entry_price: The entry price of the position
            stop_loss_pct: Stop loss percentage from entry
            profit_target_pct: Profit target percentage from entry
            trailing_stop_pct: Trailing stop percentage from peak
        """
        self.entry_price = float(entry_price)
        self.stop_loss_pct = stop_loss_pct
        self.profit_target_pct = profit_target_pct
        self.trailing_stop_pct = trailing_stop_pct
        
        # State variables
        self.max_price_seen = entry_price
        self.min_price_seen = entry_price
        self.max_drawdown_pct = 0.0
        self.max_profit_pct = 0.0
        
        # Risk levels
        self.stop_level = entry_price * (1 - stop_loss_pct / 100)
        self.target_level = entry_price * (1 + profit_target_pct / 100)
        self.dynamic_trailing_stop = self.stop_level
        
        # Tracking
        self.position_direction = 1  # 1 for long, -1 for short
        self.last_update_time = datetime.now()
        
        logger.info(f"Risk Manager initialized - Entry: {entry_price}, Stop: {self.stop_level}, Target: {self.target_level}")
    
    def update(self, current_price: float) -> Dict[str, float]:
        """
        Update risk manager with current price and recalculate all risk metrics.
        
        Args:
            current_price: Current market price
            
        Returns:
            Dictionary containing all current risk levels and metrics
        """
        try:
            current_price = float(current_price)
            
            if current_price <= 0:
                logger.warning(f"Invalid price received: {current_price}")
                return self._get_current_state()
            
            # Update price extremes
            if current_price > self.max_price_seen:
                self.max_price_seen = current_price
                # Update trailing stop when new high is reached
                self._update_trailing_stop()
            
            if current_price < self.min_price_seen:
                self.min_price_seen = current_price
            
            # Calculate current profit/loss percentage
            current_pnl_pct = ((current_price - self.entry_price) / self.entry_price) * 100
            
            # Update max profit achieved
            if current_pnl_pct > self.max_profit_pct:
                self.max_profit_pct = current_pnl_pct
            
            # Calculate max drawdown from peak
            drawdown_from_peak = ((self.max_price_seen - current_price) / self.max_price_seen) * 100
            if drawdown_from_peak > self.max_drawdown_pct:
                self.max_drawdown_pct = drawdown_from_peak
            
            # Calculate drawdown from entry
            drawdown_from_entry = ((self.entry_price - current_price) / self.entry_price) * 100
            if drawdown_from_entry < 0:
                drawdown_from_entry = 0.0  # No drawdown if price is above entry
            
            self.last_update_time = datetime.now()
            
            return self._get_current_state()
            
        except Exception as e:
            logger.error(f"Error updating risk manager: {e}")
            return self._get_current_state()
    
    def _update_trailing_stop(self):
        """Update the trailing stop level based on the new maximum price."""
        new_trailing_stop = self.max_price_seen * (1 - self.trailing_stop_pct / 100)
        
        # Only update if the new trailing stop is higher than current
        if new_trailing_stop > self.dynamic_trailing_stop:
            self.dynamic_trailing_stop = new_trailing_stop
            logger.debug(f"Trailing stop updated to: {self.dynamic_trailing_stop}")
    
    def _get_current_state(self) -> Dict[str, float]:
        """Get the current state of all risk metrics."""
        return {
            "entry_price": self.entry_price,
            "pre_set_stop_level": self.stop_level,
            "profit_target_level": self.target_level,
            "dynamic_trailing_stop": self.dynamic_trailing_stop,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_profit_pct": self.max_profit_pct,
            "max_price_seen": self.max_price_seen,
            "min_price_seen": self.min_price_seen,
            "current_risk_reward_ratio": self._calculate_risk_reward_ratio()
        }
    
    def _calculate_risk_reward_ratio(self) -> float:
        """Calculate current risk-reward ratio."""
        try:
            potential_loss = self.entry_price - self.dynamic_trailing_stop
            potential_gain = self.target_level - self.entry_price
            
            if potential_loss > 0:
                return potential_gain / potential_loss
            else:
                return 0.0
        except:
            return 0.0
    
    def is_stop_triggered(self, current_price: float) -> bool:
        """
        Check if any stop condition is triggered.
        
        Args:
            current_price: Current market price
            
        Returns:
            True if stop is triggered, False otherwise
        """
        return (current_price <= self.stop_level or 
                current_price <= self.dynamic_trailing_stop)
    
    def is_target_reached(self, current_price: float) -> bool:
        """
        Check if profit target is reached.
        
        Args:
            current_price: Current market price
            
        Returns:
            True if target is reached, False otherwise
        """
        return current_price >= self.target_level
    
    def get_position_status(self, current_price: float) -> str:
        """
        Get current position status.
        
        Args:
            current_price: Current market price
            
        Returns:
            String indicating position status
        """
        if self.is_stop_triggered(current_price):
            return "STOP_TRIGGERED"
        elif self.is_target_reached(current_price):
            return "TARGET_REACHED"
        elif current_price > self.entry_price:
            return "IN_PROFIT"
        else:
            return "IN_LOSS"
    
    def adjust_risk_parameters(self, 
                             new_stop_pct: Optional[float] = None,
                             new_target_pct: Optional[float] = None,
                             new_trailing_pct: Optional[float] = None):
        """
        Dynamically adjust risk parameters.
        
        Args:
            new_stop_pct: New stop loss percentage
            new_target_pct: New profit target percentage  
            new_trailing_pct: New trailing stop percentage
        """
        if new_stop_pct is not None:
            self.stop_loss_pct = new_stop_pct
            self.stop_level = self.entry_price * (1 - new_stop_pct / 100)
            
        if new_target_pct is not None:
            self.profit_target_pct = new_target_pct
            self.target_level = self.entry_price * (1 + new_target_pct / 100)
            
        if new_trailing_pct is not None:
            self.trailing_stop_pct = new_trailing_pct
            self._update_trailing_stop()
            
        logger.info(f"Risk parameters adjusted - Stop: {self.stop_level}, Target: {self.target_level}")
    
    def reset_for_new_trade(self, new_entry_price: float):
        """
        Reset the risk manager for a new trade.
        
        Args:
            new_entry_price: New entry price
        """
        self.__init__(new_entry_price, self.stop_loss_pct, 
                      self.profit_target_pct, self.trailing_stop_pct)
        logger.info(f"Risk manager reset for new trade at entry: {new_entry_price}")