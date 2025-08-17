"""CoinTR exchange connector implementation."""

import asyncio
import logging
import json
import websockets
import aiohttp
from typing import AsyncGenerator, Optional, Dict, Any, List
from datetime import datetime
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from .base import AbstractExchangeConnector
from models import OrderBook, DepthLevel, FeesPublic

logger = logging.getLogger(__name__)

class CoinTRConnector(AbstractExchangeConnector):
    """CoinTR exchange connector using native WebSocket and REST APIs."""
    
    def __init__(self, exchange_name: str = "cointr"):
        super().__init__(exchange_name)
        # CoinTR API URLs from documentation
        self.base_url = "https://api.cointr.com"  # Confirmed working
        self.ws_url = "wss://ws.cointr.com/ws/v1/stream"  # WebSocket URL (standard format)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_connection: Optional[websockets.WebSocketServerProtocol] = None
        self.symbol_subscriptions: Dict[str, bool] = {}
        self.supports_ws = True
        
    async def connect(self) -> bool:
        """Connect to CoinTR API."""
        try:
            # Create HTTP session with SSL verification disabled for testing
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            self.session = aiohttp.ClientSession(connector=connector)
            
            # Test REST API connection
            symbols = await self._get_symbols()
            if not symbols:
                logger.error("Failed to get symbols from CoinTR")
                return False
            
            self.is_connected = True
            logger.info(f"Connected to {self.exchange_name} (WS: {self.supports_ws}, REST: True)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.exchange_name}: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from CoinTR."""
        try:
            if self.ws_connection:
                await self.ws_connection.close()
            
            if self.session:
                await self.session.close()
            
            self.is_connected = False
            logger.info(f"Disconnected from {self.exchange_name}")
            
        except Exception as e:
            logger.debug(f"Error disconnecting from {self.exchange_name}: {e}")
    
    async def _get_symbols(self) -> Optional[List[Dict]]:
        """Get available trading symbols."""
        try:
            url = f"{self.base_url}/api/v2/spot/public/symbols"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('code') == '00000' and 'data' in data:
                        return data['data']
            return None
        except Exception as e:
            logger.error(f"Error getting symbols from CoinTR: {e}")
            return None
    
    async def watch_order_book(self, symbol: str, limit: int = 10) -> AsyncGenerator[OrderBook, None]:
        """Watch order book updates via WebSocket."""
        if not self.supports_ws:
            raise NotImplementedError(f"WebSocket not supported for {self.exchange_name}")
        
        retries = 0
        max_retries = 3
        
        while retries < max_retries:
            try:
                logger.debug(f"Starting WebSocket order book watch for {symbol} on {self.exchange_name}")
                
                # Connect to WebSocket
                async with websockets.connect(self.ws_url) as websocket:
                    self.ws_connection = websocket
                    
                    # Subscribe to order book - CoinTR depth channel format
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [{
                            "instType": "SPOT",
                            "channel": "books5",  # books5 = 5 depth levels
                            "instId": symbol.upper()
                        }]
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    # Listen for messages
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            
                            # Skip subscription confirmation and pong messages
                            if 'event' in data or 'pong' in data:
                                continue
                            
                            # Parse order book data - CoinTR depth format
                            if ('data' in data and 'arg' in data and 
                                data.get('arg', {}).get('channel') in ['books', 'books1', 'books5', 'books15']):
                                order_book = self._parse_websocket_order_book(data, symbol)
                                if order_book:
                                    self.update_connection_stats()
                                    yield order_book
                                
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON from {self.exchange_name}: {message}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing WebSocket message: {e}")
                            continue
                
            except Exception as e:
                logger.error(f"WebSocket error for {self.exchange_name}: {e}")
                retries += 1
                if retries < max_retries:
                    logger.info(f"Retrying WebSocket connection (attempt {retries + 1})")
                    await asyncio.sleep(2 ** retries)
        
        logger.error(f"Max retries exceeded for WebSocket on {self.exchange_name}")
    
    async def fetch_order_book(self, symbol: str, limit: int = 10) -> Optional[OrderBook]:
        """Fetch order book snapshot via REST API."""
        if not self.session:
            return None
        
        try:
            url = f"{self.base_url}/api/v2/spot/market/orderbook"
            params = {
                'symbol': symbol.upper(),
                'limit': limit
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('code') == '00000' and 'data' in data:
                        order_book = self._parse_rest_order_book(data['data'], symbol)
                        if order_book:
                            self.update_connection_stats()
                            return order_book
                    
        except Exception as e:
            logger.debug(f"Error fetching order book for {symbol} on {self.exchange_name}: {e}")
            self.update_connection_stats(error=True)
        
        return None
    
    def _parse_websocket_order_book(self, data: Dict, symbol: str) -> Optional[OrderBook]:
        """Parse WebSocket order book data."""
        try:
            if 'data' not in data or not data['data']:
                return None
            
            book_data = data['data'][0]  # CoinTR sends array
            
            # Parse bids and asks
            bids = []
            for bid in book_data.get('bids', []):
                if len(bid) >= 2:
                    bids.append(DepthLevel(
                        price=float(bid[0]),
                        amount=float(bid[1])  # Changed from 'size' to 'amount'
                    ))
            
            asks = []
            for ask in book_data.get('asks', []):
                if len(ask) >= 2:
                    asks.append(DepthLevel(
                        price=float(ask[0]),
                        amount=float(ask[1])  # Changed from 'size' to 'amount'
                    ))
            
            return OrderBook(
                exchange=self.exchange_name,
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            logger.debug(f"Error parsing WebSocket order book: {e}")
            return None
    
    def _parse_rest_order_book(self, data: Dict, symbol: str) -> Optional[OrderBook]:
        """Parse REST API order book data."""
        try:
            # Parse bids and asks
            bids = []
            for bid in data.get('bids', []):
                if len(bid) >= 2:
                    bids.append(DepthLevel(
                        price=float(bid[0]),
                        amount=float(bid[1])  # Changed from 'size' to 'amount'
                    ))
            
            asks = []
            for ask in data.get('asks', []):
                if len(ask) >= 2:
                    asks.append(DepthLevel(
                        price=float(ask[0]),
                        amount=float(ask[1])  # Changed from 'size' to 'amount'
                    ))
            
            return OrderBook(
                exchange=self.exchange_name,
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            logger.debug(f"Error parsing REST order book: {e}")
            return None
    
    async def get_public_fees(self) -> FeesPublic:
        """Get public fee information for CoinTR."""
        # CoinTR typical fees (these should be configured in .env)
        return FeesPublic(
            exchange=self.exchange_name,
            maker_fee=0.001,  # 0.1%
            taker_fee=0.001   # 0.1%
        )
    
    def supports_websocket(self) -> bool:
        """Check if WebSocket is supported."""
        return self.supports_ws
