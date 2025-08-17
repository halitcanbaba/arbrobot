"""Cross-exchange arbitrage opportunity detection engine."""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import asyncio

from models import OrderBook, Opportunity, VWAPResult
from depth import calculate_buy_vwap, calculate_sell_vwap, get_effective_price_after_fees
from fees import fee_manager
from config import config

logger = logging.getLogger(__name__)

class ArbitrageEngine:
    """Engine for detecting cross-exchange arbitrage opportunities."""
    
    def __init__(self):
        self.order_books: Dict[Tuple[str, str], OrderBook] = {}  # (exchange, symbol) -> OrderBook
        self.last_scan_time = datetime.utcnow()
        
    def update_order_book(self, order_book: OrderBook) -> None:
        """Update order book for an exchange/symbol pair.
        
        Args:
            order_book: New order book data
        """
        key = (order_book.exchange, order_book.symbol)
        self.order_books[key] = order_book
        
    async def scan_opportunities(self, symbols: List[str], 
                               exchanges: List[str]) -> List[Opportunity]:
        """Scan for cross-exchange arbitrage opportunities.
        
        Args:
            symbols: List of symbols to scan
            exchanges: List of exchanges to compare
            
        Returns:
            List of arbitrage opportunities found
        """
        opportunities = []
        
        for symbol in symbols:
            symbol_opportunities = await self._scan_symbol_opportunities(symbol, exchanges)
            opportunities.extend(symbol_opportunities)
        
        self.last_scan_time = datetime.utcnow()
        return opportunities
    
    async def _scan_symbol_opportunities(self, symbol: str, 
                                       exchanges: List[str]) -> List[Opportunity]:
        """Scan opportunities for a specific symbol across exchanges.
        
        Args:
            symbol: Symbol to scan
            exchanges: List of exchanges to compare
            
        Returns:
            List of opportunities for this symbol
        """
        opportunities = []
        
        # Get all available order books for this symbol
        symbol_books: Dict[str, OrderBook] = {}
        
        for exchange in exchanges:
            key = (exchange, symbol)
            if key in self.order_books:
                book = self.order_books[key]
                # Only use recent order books (within last 60 seconds)
                age = (datetime.utcnow() - book.timestamp).total_seconds()
                if age <= 60 and book.bids and book.asks:
                    symbol_books[exchange] = book
        
        if len(symbol_books) < 2:
            return opportunities
        
        # Compare all exchange pairs
        exchange_names = list(symbol_books.keys())
        
        for i in range(len(exchange_names)):
            for j in range(i + 1, len(exchange_names)):
                buy_exchange = exchange_names[i]
                sell_exchange = exchange_names[j]
                
                # Check both directions
                opp1 = await self._check_opportunity(
                    symbol, buy_exchange, sell_exchange, symbol_books
                )
                if opp1:
                    opportunities.append(opp1)
                
                opp2 = await self._check_opportunity(
                    symbol, sell_exchange, buy_exchange, symbol_books
                )
                if opp2:
                    opportunities.append(opp2)
        
        return opportunities
    
    async def _check_opportunity(self, symbol: str, buy_exchange: str, 
                               sell_exchange: str, 
                               symbol_books: Dict[str, OrderBook]) -> Optional[Opportunity]:
        """Check for arbitrage opportunity between two exchanges.
        
        Args:
            symbol: Trading symbol
            buy_exchange: Exchange to buy from
            sell_exchange: Exchange to sell to
            symbol_books: Order books for all exchanges
            
        Returns:
            Opportunity if found, None otherwise
        """
        buy_book = symbol_books.get(buy_exchange)
        sell_book = symbol_books.get(sell_exchange)
        
        if not buy_book or not sell_book:
            return None
        
        if not buy_book.asks or not sell_book.bids:
            return None
        
        try:
            # Get fees for both exchanges
            buy_fees = await fee_manager.get_fees(buy_exchange, symbol)
            sell_fees = await fee_manager.get_fees(sell_exchange, symbol)
            
            # Calculate VWAP for the target notional
            target_notional = config.MIN_NOTIONAL
            
            # Calculate buy VWAP (using asks from buy exchange)
            buy_vwap = calculate_buy_vwap(buy_book.asks, target_notional)
            if not buy_vwap.fully_filled:
                return None
            
            # Calculate sell VWAP (using bids from sell exchange)  
            sell_vwap = calculate_sell_vwap(sell_book.bids, target_notional)
            if not sell_vwap.fully_filled:
                return None
            
            # Apply fees
            buy_price_before_fees = buy_vwap.vwap_price
            sell_price_before_fees = sell_vwap.vwap_price
            
            # For buying: price increases due to fees
            buy_taker_fee = buy_fees.get_fees(symbol)[1]  # taker fee
            buy_price_after_fees = buy_price_before_fees * (1 + buy_taker_fee)
            
            # For selling: we receive less due to fees
            sell_taker_fee = sell_fees.get_fees(symbol)[1]  # taker fee
            sell_price_after_fees = sell_price_before_fees * (1 - sell_taker_fee)
            
            # Check if opportunity exists
            if sell_price_after_fees <= buy_price_after_fees:
                return None
            
            # Calculate spread in basis points
            mid_price = (buy_price_after_fees + sell_price_after_fees) / 2
            spread_bps = (sell_price_after_fees - buy_price_after_fees) / mid_price * 10000
            
            # Check if spread meets minimum threshold
            if spread_bps < config.MIN_SPREAD_BPS:
                return None
            
            # Create opportunity
            opportunity = Opportunity(
                symbol=symbol,
                buy_exchange=buy_exchange,
                sell_exchange=sell_exchange,
                buy_price_before_fees=buy_price_before_fees,
                sell_price_before_fees=sell_price_before_fees,
                buy_price_after_fees=buy_price_after_fees,
                sell_price_after_fees=sell_price_after_fees,
                spread_bps=spread_bps,
                notional=target_notional,
                buy_depth_levels=buy_vwap.levels_used,
                sell_depth_levels=sell_vwap.levels_used,
                buy_fees=buy_fees.get_fees(symbol),
                sell_fees=sell_fees.get_fees(symbol),
                timestamp=datetime.utcnow(),
                mode='ws' if self._is_websocket_data(buy_book, sell_book) else 'rest'
            )
            
            logger.info(f"Found arbitrage opportunity: {opportunity.dedupe_key} - {spread_bps:.2f} bps")
            return opportunity
            
        except Exception as e:
            logger.debug(f"Error checking opportunity for {symbol} {buy_exchange}->{sell_exchange}: {e}")
            return None
    
    def _is_websocket_data(self, *books: OrderBook) -> bool:
        """Check if order books are from WebSocket data.
        
        Args:
            books: Order books to check
            
        Returns:
            True if all books are recent enough to be from WebSocket
        """
        now = datetime.utcnow()
        
        for book in books:
            # If data is older than 5 seconds, assume it's from REST
            age = (now - book.timestamp).total_seconds()
            if age > 5:
                return False
        
        return True
    
    def get_stats(self) -> Dict[str, any]:
        """Get engine statistics.
        
        Returns:
            Dictionary with engine stats
        """
        now = datetime.utcnow()
        
        # Count active order books by exchange
        exchange_counts = {}
        total_books = 0
        recent_books = 0
        
        for (exchange, symbol), book in self.order_books.items():
            age = (now - book.timestamp).total_seconds()
            total_books += 1
            
            if age <= 60:  # Recent data
                recent_books += 1
                
            if exchange not in exchange_counts:
                exchange_counts[exchange] = 0
            exchange_counts[exchange] += 1
        
        return {
            'total_order_books': total_books,
            'recent_order_books': recent_books,
            'exchange_counts': exchange_counts,
            'last_scan_age_seconds': (now - self.last_scan_time).total_seconds()
        }
