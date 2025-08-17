"""Triangular arbitrage detection engine."""

import logging
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime
import asyncio

from models import OrderBook, TriOpportunity
from depth import calculate_buy_vwap, calculate_sell_vwap
from fees import fee_manager
from config import config
from registry import registry

logger = logging.getLogger(__name__)

class TriangularArbitrageEngine:
    """Engine for detecting triangular arbitrage opportunities within exchanges."""
    
    def __init__(self):
        self.order_books: Dict[Tuple[str, str], OrderBook] = {}  # (exchange, symbol) -> OrderBook
        self.triangular_paths: Dict[str, List[Tuple[str, str, str]]] = {}  # exchange -> paths
        self.last_scan_time = datetime.utcnow()
        self.path_cache_time: Dict[str, datetime] = {}
        
    def update_order_book(self, order_book: OrderBook) -> None:
        """Update order book for an exchange/symbol pair.
        
        Args:
            order_book: New order book data
        """
        key = (order_book.exchange, order_book.symbol)
        self.order_books[key] = order_book
        
    async def scan_opportunities(self, exchanges: List[str]) -> List[TriOpportunity]:
        """Scan for triangular arbitrage opportunities.
        
        Args:
            exchanges: List of exchanges to scan
            
        Returns:
            List of triangular arbitrage opportunities
        """
        opportunities = []
        
        for exchange in exchanges:
            exchange_opportunities = await self._scan_exchange_opportunities(exchange)
            opportunities.extend(exchange_opportunities)
        
        self.last_scan_time = datetime.utcnow()
        return opportunities
    
    async def _scan_exchange_opportunities(self, exchange: str) -> List[TriOpportunity]:
        """Scan triangular opportunities for a specific exchange.
        
        Args:
            exchange: Exchange to scan
            
        Returns:
            List of opportunities for this exchange
        """
        opportunities = []
        
        # Update triangular paths for this exchange if needed
        await self._update_triangular_paths(exchange)
        
        if exchange not in self.triangular_paths:
            return opportunities
        
        # Get fees for this exchange
        try:
            fees = await fee_manager.get_fees(exchange)
        except Exception as e:
            logger.debug(f"Failed to get fees for {exchange}: {e}")
            return opportunities
        
        # Scan each triangular path
        paths = self.triangular_paths[exchange]
        
        for base, asset2, asset3 in paths:
            opportunity = await self._check_triangular_opportunity(
                exchange, base, asset2, asset3, fees
            )
            if opportunity:
                opportunities.append(opportunity)
        
        return opportunities
    
    async def _update_triangular_paths(self, exchange: str) -> None:
        """Update triangular paths for an exchange.
        
        Args:
            exchange: Exchange name
        """
        # Check if we need to update paths (cache for 5 minutes)
        now = datetime.utcnow()
        if exchange in self.path_cache_time:
            age = (now - self.path_cache_time[exchange]).total_seconds()
            if age < 300:  # 5 minutes
                return
        
        try:
            # Get triangular paths from registry
            paths = registry.get_triangular_symbols(exchange, config.TRI_BASES)
            
            # Filter paths to only include those with available order books
            valid_paths = []
            
            for base, asset2, asset3 in paths:
                # Check if we have order books for all required symbols
                required_symbols = [
                    f"{base}/{asset2}",     # base -> asset2
                    f"{asset2}/{asset3}",   # asset2 -> asset3  
                    f"{asset3}/{base}",     # asset3 -> base
                ]
                
                # Also check reverse symbols in case exchange uses different format
                alt_symbols = [
                    f"{asset2}/{base}",     # reverse of leg 1
                    f"{asset3}/{asset2}",   # reverse of leg 2
                    f"{base}/{asset3}",     # reverse of leg 3
                ]
                
                has_all_symbols = True
                for i, symbol in enumerate(required_symbols):
                    key = (exchange, symbol)
                    alt_key = (exchange, alt_symbols[i])
                    
                    if key not in self.order_books and alt_key not in self.order_books:
                        has_all_symbols = False
                        break
                
                if has_all_symbols:
                    valid_paths.append((base, asset2, asset3))
            
            self.triangular_paths[exchange] = valid_paths
            self.path_cache_time[exchange] = now
            
            logger.debug(f"Updated {len(valid_paths)} triangular paths for {exchange}")
            
        except Exception as e:
            logger.debug(f"Error updating triangular paths for {exchange}: {e}")
    
    async def _check_triangular_opportunity(self, exchange: str, base: str, 
                                          asset2: str, asset3: str, 
                                          fees) -> Optional[TriOpportunity]:
        """Check for triangular arbitrage opportunity.
        
        Args:
            exchange: Exchange name
            base: Base asset (starting and ending)
            asset2: Second asset in the cycle
            asset3: Third asset in the cycle
            fees: Fee structure for the exchange
            
        Returns:
            TriOpportunity if found, None otherwise
        """
        try:
            # Define the trading path: base -> asset2 -> asset3 -> base
            start_amount = config.MIN_NOTIONAL
            
            # Leg 1: base -> asset2
            leg1_result = await self._execute_leg(
                exchange, base, asset2, start_amount, 'buy', fees
            )
            if not leg1_result:
                return None
            
            asset2_amount, leg1_symbol, leg1_price, leg1_side = leg1_result
            
            # Leg 2: asset2 -> asset3
            leg2_result = await self._execute_leg(
                exchange, asset2, asset3, asset2_amount, 'buy', fees
            )
            if not leg2_result:
                return None
            
            asset3_amount, leg2_symbol, leg2_price, leg2_side = leg2_result
            
            # Leg 3: asset3 -> base
            leg3_result = await self._execute_leg(
                exchange, asset3, base, asset3_amount, 'sell', fees
            )
            if not leg3_result:
                return None
            
            final_base_amount, leg3_symbol, leg3_price, leg3_side = leg3_result
            
            # Calculate gain
            if final_base_amount <= start_amount:
                return None
            
            gain_bps = (final_base_amount / start_amount - 1) * 10000
            
            # Check if gain meets minimum threshold
            if gain_bps < config.MIN_TRI_GAIN_BPS:
                return None
            
            # Create opportunity
            opportunity = TriOpportunity(
                exchange=exchange,
                base_asset=base,
                path=(base, asset2, asset3),
                start_amount=start_amount,
                end_amount=final_base_amount,
                gain_bps=gain_bps,
                notional=start_amount,
                leg1_symbol=leg1_symbol,
                leg1_price=leg1_price,
                leg1_side=leg1_side,
                leg2_symbol=leg2_symbol,
                leg2_price=leg2_price,
                leg2_side=leg2_side,
                leg3_symbol=leg3_symbol,
                leg3_price=leg3_price,
                leg3_side=leg3_side,
                fees=fees.get_fees(),
                timestamp=datetime.utcnow()
            )
            
            logger.info(f"Found triangular opportunity: {opportunity.dedupe_key} - {gain_bps:.2f} bps")
            return opportunity
            
        except Exception as e:
            logger.debug(f"Error checking triangular opportunity {base}->{asset2}->{asset3} on {exchange}: {e}")
            return None
    
    async def _execute_leg(self, exchange: str, from_asset: str, to_asset: str,
                          amount: float, preferred_side: str, fees) -> Optional[Tuple[float, str, float, str]]:
        """Simulate execution of one leg of triangular arbitrage.
        
        Args:
            exchange: Exchange name
            from_asset: Asset to sell/spend
            to_asset: Asset to buy/receive
            amount: Amount of from_asset
            preferred_side: Preferred side ('buy' or 'sell')
            fees: Fee structure
            
        Returns:
            Tuple of (resulting_amount, symbol, price, side) or None if failed
        """
        # Try primary symbol format
        symbol = f"{from_asset}/{to_asset}"
        book = self._get_order_book(exchange, symbol)
        
        if book:
            # Direct symbol exists - determine if we're buying or selling
            if preferred_side == 'buy':
                # Buying to_asset with from_asset (using asks)
                target_notional = amount  # Amount of from_asset to spend
                vwap = calculate_buy_vwap(book.asks, target_notional)
                
                if vwap.fully_filled:
                    # Apply taker fees (we receive less to_asset)
                    taker_fee = fees.get_fees(symbol)[1]
                    gross_to_asset = target_notional / vwap.vwap_price
                    net_to_asset = gross_to_asset * (1 - taker_fee)
                    
                    return net_to_asset, symbol, vwap.vwap_price, 'buy'
            else:
                # Selling from_asset for to_asset (using bids)
                # Convert amount to notional for VWAP calculation
                if not book.bids:
                    return None
                
                best_bid = book.bids[0].price
                target_notional = amount * best_bid
                vwap = calculate_sell_vwap(book.bids, target_notional)
                
                if vwap.fully_filled:
                    # Apply taker fees (we receive less to_asset)
                    taker_fee = fees.get_fees(symbol)[1]
                    gross_to_asset = vwap.total_volume * vwap.vwap_price
                    net_to_asset = gross_to_asset * (1 - taker_fee)
                    
                    return net_to_asset, symbol, vwap.vwap_price, 'sell'
        
        # Try reverse symbol format
        reverse_symbol = f"{to_asset}/{from_asset}"
        book = self._get_order_book(exchange, reverse_symbol)
        
        if book:
            if preferred_side == 'buy':
                # We want to buy to_asset, but symbol is to_asset/from_asset
                # So we're selling from_asset (using bids of reverse symbol)
                if not book.bids:
                    return None
                
                best_bid = book.bids[0].price
                target_notional = amount / best_bid  # Amount of to_asset we want
                vwap = calculate_sell_vwap(book.bids, target_notional)
                
                if vwap.fully_filled:
                    taker_fee = fees.get_fees(reverse_symbol)[1]
                    # We're selling to_asset for from_asset, but we want the reverse
                    gross_to_asset = vwap.total_volume
                    net_to_asset = gross_to_asset * (1 - taker_fee)
                    
                    return net_to_asset, reverse_symbol, vwap.vwap_price, 'sell'
            else:
                # We want to sell from_asset, but symbol is to_asset/from_asset
                # So we're buying to_asset (using asks of reverse symbol)
                target_notional = amount  # Amount of from_asset we have
                vwap = calculate_buy_vwap(book.asks, target_notional)
                
                if vwap.fully_filled:
                    taker_fee = fees.get_fees(reverse_symbol)[1]
                    gross_to_asset = target_notional / vwap.vwap_price
                    net_to_asset = gross_to_asset * (1 - taker_fee)
                    
                    return net_to_asset, reverse_symbol, vwap.vwap_price, 'buy'
        
        return None
    
    def _get_order_book(self, exchange: str, symbol: str) -> Optional[OrderBook]:
        """Get order book for exchange/symbol with recency check.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            OrderBook if available and recent, None otherwise
        """
        key = (exchange, symbol)
        
        if key not in self.order_books:
            return None
        
        book = self.order_books[key]
        
        # Check if data is recent (within last 60 seconds)
        age = (datetime.utcnow() - book.timestamp).total_seconds()
        if age > 60:
            return None
        
        # Check if book has data
        if not book.bids or not book.asks:
            return None
        
        return book
    
    def get_stats(self) -> Dict[str, any]:
        """Get engine statistics.
        
        Returns:
            Dictionary with engine stats
        """
        now = datetime.utcnow()
        
        # Count paths by exchange
        path_counts = {
            exchange: len(paths) 
            for exchange, paths in self.triangular_paths.items()
        }
        
        # Count order books by exchange
        exchange_books = {}
        for (exchange, symbol), book in self.order_books.items():
            if exchange not in exchange_books:
                exchange_books[exchange] = 0
            exchange_books[exchange] += 1
        
        return {
            'triangular_paths': path_counts,
            'order_books_by_exchange': exchange_books,
            'total_paths': sum(path_counts.values()),
            'last_scan_age_seconds': (now - self.last_scan_time).total_seconds()
        }
