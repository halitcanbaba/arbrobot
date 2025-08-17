"""Abstract base class for exchange connectors."""

from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional, List, Dict, Any
from datetime import datetime
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import OrderBook, DepthLevel, FeesPublic

class AbstractExchangeConnector(ABC):
    """Abstract base class for exchange connectors."""
    
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.is_connected = False
        self.last_message_time: Optional[datetime] = None
        self.error_count = 0
        self.reconnect_count = 0
        
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the exchange.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the exchange."""
        pass
    
    @abstractmethod
    async def watch_order_book(self, symbol: str, limit: int = 10) -> AsyncGenerator[OrderBook, None]:
        """Watch order book updates via WebSocket (if supported).
        
        Args:
            symbol: Trading symbol to watch
            limit: Number of price levels to retrieve
            
        Yields:
            OrderBook updates
        """
        pass
    
    @abstractmethod
    async def fetch_order_book(self, symbol: str, limit: int = 10) -> Optional[OrderBook]:
        """Fetch order book snapshot via REST API.
        
        Args:
            symbol: Trading symbol
            limit: Number of price levels to retrieve
            
        Returns:
            OrderBook snapshot or None if failed
        """
        pass
    
    @abstractmethod
    async def get_public_fees(self) -> FeesPublic:
        """Get public fee information for this exchange.
        
        Returns:
            FeesPublic object
        """
        pass
    
    @abstractmethod
    def supports_websocket(self) -> bool:
        """Check if this connector supports WebSocket streaming.
        
        Returns:
            True if WebSocket is supported
        """
        pass
    
    def _parse_order_book_data(self, data: Dict[str, Any], symbol: str) -> Optional[OrderBook]:
        """Parse raw order book data into OrderBook model.
        
        Args:
            data: Raw order book data from exchange
            symbol: Trading symbol
            
        Returns:
            Parsed OrderBook or None if parsing failed
        """
        try:
            # Parse bids and asks
            bids = []
            asks = []
            
            raw_bids = data.get('bids', [])
            raw_asks = data.get('asks', [])
            
            for bid in raw_bids:
                if len(bid) >= 2:
                    price = float(bid[0])
                    amount = float(bid[1])
                    if price > 0 and amount > 0:
                        bids.append(DepthLevel(price=price, amount=amount))
            
            for ask in raw_asks:
                if len(ask) >= 2:
                    price = float(ask[0])
                    amount = float(ask[1])
                    if price > 0 and amount > 0:
                        asks.append(DepthLevel(price=price, amount=amount))
            
            # Sort bids (highest first) and asks (lowest first)
            bids.sort(key=lambda x: x.price, reverse=True)
            asks.sort(key=lambda x: x.price)
            
            timestamp = datetime.utcnow()
            if 'timestamp' in data and data['timestamp']:
                try:
                    timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
                except (ValueError, TypeError):
                    pass
            
            return OrderBook(
                symbol=symbol,
                exchange=self.exchange_name,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                nonce=data.get('nonce')
            )
            
        except Exception as e:
            print(f"Error parsing order book for {symbol} on {self.exchange_name}: {e}")
            return None
    
    def update_connection_stats(self, error: bool = False) -> None:
        """Update connection statistics.
        
        Args:
            error: Whether this update is due to an error
        """
        self.last_message_time = datetime.utcnow()
        
        if error:
            self.error_count += 1
        
    async def _handle_reconnect(self, max_attempts: int = 5) -> bool:
        """Handle reconnection with exponential backoff.
        
        Args:
            max_attempts: Maximum number of reconnection attempts
            
        Returns:
            True if reconnection successful
        """
        for attempt in range(max_attempts):
            try:
                # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                wait_time = min(2 ** attempt, 60)  # Cap at 60 seconds
                
                if attempt > 0:
                    await asyncio.sleep(wait_time)
                
                if await self.connect():
                    self.reconnect_count += 1
                    return True
                    
            except Exception as e:
                print(f"Reconnection attempt {attempt + 1} failed for {self.exchange_name}: {e}")
                continue
        
        return False
