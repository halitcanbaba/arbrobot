"""Generic CCXT-based exchange connector with WebSocket and REST fallback."""

import asyncio
import logging
from typing import AsyncGenerator, Optional, Dict, Any
from datetime import datetime
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import ccxt.pro as ccxtpro
    CCXT_PRO_AVAILABLE = True
except ImportError:
    ccxtpro = None
    CCXT_PRO_AVAILABLE = False

import ccxt

from .base import AbstractExchangeConnector
from models import OrderBook, FeesPublic
from fees import fee_manager
from registry import registry

logger = logging.getLogger(__name__)

class CCXTGenericConnector(AbstractExchangeConnector):
    """Generic connector using CCXT/CCXT.Pro for WebSocket and REST."""
    
    def __init__(self, exchange_name: str):
        super().__init__(exchange_name)
        self.ws_exchange: Optional[ccxtpro.Exchange] = None
        self.rest_exchange: Optional[ccxt.Exchange] = None
        self.supports_ws = False
        self.rate_limit_semaphore: Optional[asyncio.Semaphore] = None
        self._last_rest_call: Dict[str, float] = {}
        
    async def connect(self) -> bool:
        """Connect to the exchange with WebSocket preferred, REST fallback."""
        try:
            # Get REST exchange from registry
            self.rest_exchange = registry.get_exchange(self.exchange_name)
            if not self.rest_exchange:
                logger.error(f"Exchange {self.exchange_name} not found in registry")
                return False
            
            # Set up rate limiting
            rate_limit = getattr(self.rest_exchange, 'rateLimit', 1000)
            # Allow some concurrency but respect rate limits
            max_concurrent = max(1, min(10, 1000 // rate_limit))
            self.rate_limit_semaphore = asyncio.Semaphore(max_concurrent)
            
            # Try to set up WebSocket if supported
            if registry.has_websocket_support(self.exchange_name) and CCXT_PRO_AVAILABLE:
                try:
                    ws_class = getattr(ccxtpro, self.exchange_name, None) if ccxtpro else None
                    if ws_class:
                        self.ws_exchange = ws_class({
                            'enableRateLimit': True,
                            'sandbox': False,
                        })
                        self.supports_ws = True
                        logger.info(f"✓ WebSocket support enabled for {self.exchange_name}")
                    else:
                        logger.debug(f"CCXT.Pro class not found for {self.exchange_name}")
                except Exception as e:
                    logger.debug(f"Failed to initialize WebSocket for {self.exchange_name}: {e}")
                    self.supports_ws = False
            else:
                if not CCXT_PRO_AVAILABLE:
                    logger.debug(f"CCXT.Pro not available - WebSocket disabled for {self.exchange_name}")
                self.supports_ws = False
            
            self.is_connected = True
            logger.info(f"Connected to {self.exchange_name} (WS: {self.supports_ws}, REST: True)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.exchange_name}: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from the exchange."""
        try:
            if self.ws_exchange:
                await self.ws_exchange.close()
            # REST exchange is managed by registry
            self.is_connected = False
            logger.info(f"Disconnected from {self.exchange_name}")
        except Exception as e:
            logger.debug(f"Error disconnecting from {self.exchange_name}: {e}")
    
    async def watch_order_book(self, symbol: str, limit: int = 10) -> AsyncGenerator[OrderBook, None]:
        """Watch order book updates via WebSocket."""
        if not self.supports_ws or not self.ws_exchange:
            raise NotImplementedError(f"WebSocket not supported for {self.exchange_name}")
        
        retries = 0
        max_retries = 3
        
        while retries < max_retries:
            try:
                logger.debug(f"Starting WebSocket order book watch for {symbol} on {self.exchange_name}")
                
                while True:
                    try:
                        # Watch order book via WebSocket
                        raw_book = await self.ws_exchange.watch_order_book(symbol, limit)
                        
                        # Parse and yield order book
                        order_book = self._parse_order_book_data(raw_book, symbol)
                        if order_book:
                            self.update_connection_stats()
                            registry.update_health(
                                self.exchange_name,
                                ws_connected=True,
                                last_ws_message=datetime.utcnow()
                            )
                            yield order_book
                        
                    except Exception as e:
                        if "NetworkError" in str(type(e)) or "Network" in str(e):
                            logger.warning(f"Network error watching {symbol} on {self.exchange_name}: {e}")
                            self.update_connection_stats(error=True)
                            await asyncio.sleep(1)
                            break  # Break inner loop to retry connection
                        else:
                            logger.error(f"Error watching {symbol} on {self.exchange_name}: {e}")
                            self.update_connection_stats(error=True)
                            await asyncio.sleep(5)
                            break
                
                retries += 1
                if retries < max_retries:
                    logger.info(f"Retrying WebSocket connection for {self.exchange_name} (attempt {retries + 1})")
                    await asyncio.sleep(2 ** retries)  # Exponential backoff
                
            except Exception as e:
                logger.error(f"Fatal error in WebSocket watch for {self.exchange_name}: {e}")
                retries += 1
                if retries < max_retries:
                    await asyncio.sleep(5)
        
        logger.error(f"Max retries exceeded for WebSocket on {self.exchange_name}")
        registry.update_health(self.exchange_name, ws_connected=False)
    
    async def fetch_order_book(self, symbol: str, limit: int = 10) -> Optional[OrderBook]:
        """Fetch order book snapshot via REST API with rate limiting."""
        if not self.rest_exchange or not self.rate_limit_semaphore:
            return None
        
        async with self.rate_limit_semaphore:
            try:
                # Implement adaptive rate limiting
                now = asyncio.get_event_loop().time()
                last_call = self._last_rest_call.get(symbol, 0)
                rate_limit_ms = getattr(self.rest_exchange, 'rateLimit', 1000)
                min_interval = rate_limit_ms / 1000.0
                
                time_since_last = now - last_call
                if time_since_last < min_interval:
                    await asyncio.sleep(min_interval - time_since_last)
                
                # Fetch order book
                raw_book = await asyncio.to_thread(
                    self.rest_exchange.fetch_order_book,
                    symbol,
                    limit
                )
                
                self._last_rest_call[symbol] = asyncio.get_event_loop().time()
                
                # Parse order book
                order_book = self._parse_order_book_data(raw_book, symbol)
                if order_book:
                    self.update_connection_stats()
                    registry.update_health(
                        self.exchange_name,
                        rest_ok=True,
                        last_rest_call=datetime.utcnow()
                    )
                
                return order_book
                
            except Exception as e:
                logger.debug(f"Error fetching order book for {symbol} on {self.exchange_name}: {e}")
                self.update_connection_stats(error=True)
                registry.update_health(self.exchange_name, rest_ok=False)
                return None
    
    async def get_public_fees(self) -> FeesPublic:
        """Get public fee information."""
        return await fee_manager.get_fees(self.exchange_name)
    
    def supports_websocket(self) -> bool:
        """Check if WebSocket is supported and available."""
        return self.supports_ws and self.ws_exchange is not None

class ConnectorFactory:
    """Factory for creating exchange connectors."""
    
    @staticmethod
    def create_connector(exchange_name: str) -> AbstractExchangeConnector:
        """Create appropriate connector for an exchange.
        
        Args:
            exchange_name: Name of the exchange
            
        Returns:
            Exchange connector instance
        """
        # Special handling for CoinTR
        if exchange_name.lower() == 'cointr':
            from .cointr import CoinTRConnector
            return CoinTRConnector(exchange_name)
        
        # For all other exchanges, use the generic CCXT connector
        return CCXTGenericConnector(exchange_name)

# Convenience factory function
def create_connector(exchange_name: str) -> AbstractExchangeConnector:
    """Create a connector for the specified exchange."""
    return ConnectorFactory.create_connector(exchange_name)
