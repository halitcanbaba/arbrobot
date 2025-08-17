"""VWAP calculation for order book depth analysis."""

import numpy as np
from typing import List, Tuple, Optional
from models import DepthLevel, VWAPResult

def calculate_vwap(levels: List[DepthLevel], target_notional: float, 
                  side: str = 'buy') -> VWAPResult:
    """Calculate Volume Weighted Average Price for a target notional amount.
    
    Args:
        levels: List of order book levels (bids for buy, asks for sell)
        target_notional: Target notional amount to fill
        side: 'buy' or 'sell'
        
    Returns:
        VWAPResult with calculated VWAP and fill information
    """
    if not levels or target_notional <= 0:
        return VWAPResult(
            vwap_price=0.0,
            total_volume=0.0,
            levels_used=0,
            fully_filled=False
        )
    
    # Convert to numpy arrays for vectorized operations
    prices = np.array([level.price for level in levels])
    amounts = np.array([level.amount for level in levels])
    
    # Calculate cumulative notional values
    notionals = prices * amounts
    cumulative_notionals = np.cumsum(notionals)
    cumulative_amounts = np.cumsum(amounts)
    
    # Find where we can fill the target notional
    fill_indices = np.where(cumulative_notionals >= target_notional)[0]
    
    if len(fill_indices) == 0:
        # Cannot fully fill - use all available liquidity
        total_notional = cumulative_notionals[-1]
        total_amount = cumulative_amounts[-1]
        vwap = total_notional / total_amount if total_amount > 0 else 0.0
        
        return VWAPResult(
            vwap_price=vwap,
            total_volume=total_amount,
            levels_used=len(levels),
            fully_filled=False
        )
    
    # We can fill the order
    fill_index = fill_indices[0]
    
    if fill_index == 0:
        # Target can be filled within first level
        price = prices[0]
        amount_needed = target_notional / price
        
        return VWAPResult(
            vwap_price=price,
            total_volume=amount_needed,
            levels_used=1,
            fully_filled=True
        )
    
    # Need multiple levels - calculate partial fill of last level
    full_levels_notional = cumulative_notionals[fill_index - 1]
    remaining_notional = target_notional - full_levels_notional
    
    last_level_price = prices[fill_index]
    partial_amount = remaining_notional / last_level_price
    
    # Calculate VWAP for the filled portion
    total_amount_filled = cumulative_amounts[fill_index - 1] + partial_amount
    
    # Weighted average calculation
    if fill_index > 0:
        # Full levels contribution
        full_levels_weighted = np.sum(prices[:fill_index] * amounts[:fill_index])
        # Partial level contribution  
        partial_weighted = last_level_price * partial_amount
        total_weighted = full_levels_weighted + partial_weighted
    else:
        total_weighted = last_level_price * partial_amount
    
    vwap = total_weighted / total_amount_filled if total_amount_filled > 0 else 0.0
    
    return VWAPResult(
        vwap_price=vwap,
        total_volume=total_amount_filled,
        levels_used=fill_index + 1,
        fully_filled=True
    )

def calculate_buy_vwap(asks: List[DepthLevel], target_notional: float) -> VWAPResult:
    """Calculate VWAP for buying (using asks).
    
    Args:
        asks: List of ask levels (sorted by price ascending)
        target_notional: Target notional amount to buy
        
    Returns:
        VWAPResult with buy VWAP
    """
    return calculate_vwap(asks, target_notional, 'buy')

def calculate_sell_vwap(bids: List[DepthLevel], target_notional: float) -> VWAPResult:
    """Calculate VWAP for selling (using bids).
    
    Args:
        bids: List of bid levels (sorted by price descending)
        target_notional: Target notional amount to sell
        
    Returns:
        VWAPResult with sell VWAP
    """
    return calculate_vwap(bids, target_notional, 'sell')

def get_effective_price_after_fees(vwap_result: VWAPResult, fee_rate: float, 
                                  side: str) -> float:
    """Calculate effective price after applying trading fees.
    
    Args:
        vwap_result: VWAP calculation result
        fee_rate: Trading fee rate (e.g., 0.001 for 0.1%)
        side: 'buy' or 'sell'
        
    Returns:
        Effective price after fees
    """
    if not vwap_result.fully_filled or vwap_result.vwap_price <= 0:
        return 0.0
    
    if side == 'buy':
        # When buying, we pay more due to fees
        return vwap_result.vwap_price * (1 + fee_rate)
    else:
        # When selling, we receive less due to fees (but price stays same, amount decreases)
        # For spread calculation, we need the effective price we receive
        return vwap_result.vwap_price * (1 - fee_rate)

def check_sufficient_depth(levels: List[DepthLevel], min_notional: float, 
                          max_levels: int = 10) -> bool:
    """Check if there's sufficient depth for a given notional amount.
    
    Args:
        levels: Order book levels
        min_notional: Minimum required notional
        max_levels: Maximum number of levels to consider
        
    Returns:
        True if sufficient depth exists
    """
    if not levels:
        return False
    
    # Limit to max_levels
    limited_levels = levels[:max_levels]
    
    # Calculate total available notional
    total_notional = sum(level.price * level.amount for level in limited_levels)
    
    return total_notional >= min_notional

def estimate_slippage(levels: List[DepthLevel], target_notional: float) -> float:
    """Estimate slippage compared to best price.
    
    Args:
        levels: Order book levels
        target_notional: Target notional amount
        
    Returns:
        Slippage in basis points (positive means worse price)
    """
    if not levels:
        return float('inf')
    
    best_price = levels[0].price
    vwap_result = calculate_vwap(levels, target_notional)
    
    if not vwap_result.fully_filled or vwap_result.vwap_price <= 0:
        return float('inf')
    
    # Calculate slippage in basis points
    slippage = abs(vwap_result.vwap_price - best_price) / best_price * 10000
    
    return slippage
