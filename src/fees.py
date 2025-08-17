"""Fee calculation and management."""

import logging
from typing import Dict, Tuple, Optional
from models import FeesPublic
from config import config
from registry import registry

logger = logging.getLogger(__name__)

class FeeManager:
    """Manages trading fees for different exchanges."""
    
    def __init__(self):
        # Conservative default fees (higher than most exchanges)
        self.default_fees = {
            'maker': 0.0008,  # 0.08%
            'taker': 0.0015,  # 0.15%
        }
        
        # Known fee structures for popular exchanges
        self.known_fees: Dict[str, Dict[str, float]] = {
            'binance': {'maker': 0.0002, 'taker': 0.0005},  # 0.02%/0.05% with BNB
            'okx': {'maker': 0.0008, 'taker': 0.0010},      # 0.08%/0.10%
            'bybit': {'maker': 0.0001, 'taker': 0.0006},    # 0.01%/0.06%
            'coinbase': {'maker': 0.0040, 'taker': 0.0060}, # 0.40%/0.60%
            'kraken': {'maker': 0.0016, 'taker': 0.0026},   # 0.16%/0.26%
            'kucoin': {'maker': 0.0008, 'taker': 0.0010},   # 0.08%/0.10%
            'gateio': {'maker': 0.0015, 'taker': 0.0020},   # 0.15%/0.20%
            'huobi': {'maker': 0.0015, 'taker': 0.0020},    # 0.15%/0.20%
            'bitfinex': {'maker': 0.0010, 'taker': 0.0020}, # 0.10%/0.20%
            'mexc': {'maker': 0.0000, 'taker': 0.0020},     # 0.00%/0.20%
        }
        
        self.fee_cache: Dict[str, FeesPublic] = {}
        self.env_overrides = config.get_fee_overrides()
    
    async def get_fees(self, exchange_name: str, symbol: Optional[str] = None) -> FeesPublic:
        """Get fee information for an exchange and optionally a specific symbol.
        
        Args:
            exchange_name: Name of the exchange
            symbol: Optional specific symbol
            
        Returns:
            FeesPublic object with fee information
        """
        cache_key = f"{exchange_name}_{symbol or 'default'}"
        
        if cache_key in self.fee_cache:
            return self.fee_cache[cache_key]
        
        # Try to get fees from exchange API
        fees = await self._fetch_public_fees(exchange_name)
        
        # Apply environment overrides if any
        if exchange_name in self.env_overrides:
            overrides = self.env_overrides[exchange_name]
            if 'maker' in overrides:
                fees.maker = overrides['maker']
                fees.source = 'env'
            if 'taker' in overrides:
                fees.taker = overrides['taker']
                fees.source = 'env'
        
        self.fee_cache[cache_key] = fees
        return fees
    
    async def _fetch_public_fees(self, exchange_name: str) -> FeesPublic:
        """Fetch fee information from exchange public API.
        
        Args:
            exchange_name: Name of the exchange
            
        Returns:
            FeesPublic object
        """
        exchange = registry.get_exchange(exchange_name)
        
        if not exchange:
            return self._get_fallback_fees(exchange_name)
        
        try:
            # Try to get trading fees from exchange
            if hasattr(exchange, 'fees') and exchange.fees:
                trading_fees = exchange.fees.get('trading', {})
                
                if trading_fees:
                    maker = trading_fees.get('maker', self.default_fees['maker'])
                    taker = trading_fees.get('taker', self.default_fees['taker'])
                    
                    # Also check for symbol-specific fees in markets
                    symbol_specific = {}
                    markets = registry.get_markets(exchange_name)
                    
                    for symbol, market_meta in markets.items():
                        try:
                            # Get the original market data from ccxt
                            ccxt_markets = exchange.markets or {}
                            if symbol in ccxt_markets:
                                market = ccxt_markets[symbol]
                                symbol_maker = market.get('maker', maker)
                                symbol_taker = market.get('taker', taker)
                                
                                if symbol_maker != maker or symbol_taker != taker:
                                    symbol_specific[symbol] = (symbol_maker, symbol_taker)
                        except Exception:
                            continue
                    
                    return FeesPublic(
                        maker=maker,
                        taker=taker,
                        source='public',
                        exchange=exchange_name,
                        symbol_specific=symbol_specific
                    )
            
        except Exception as e:
            logger.debug(f"Failed to fetch public fees for {exchange_name}: {e}")
        
        return self._get_fallback_fees(exchange_name)
    
    def _get_fallback_fees(self, exchange_name: str) -> FeesPublic:
        """Get fallback fees for an exchange.
        
        Args:
            exchange_name: Name of the exchange
            
        Returns:
            FeesPublic object with fallback fees
        """
        # Check if we have known fees for this exchange
        if exchange_name in self.known_fees:
            known = self.known_fees[exchange_name]
            return FeesPublic(
                maker=known['maker'],
                taker=known['taker'],
                source='default',
                exchange=exchange_name
            )
        
        # Use conservative defaults
        return FeesPublic(
            maker=self.default_fees['maker'],
            taker=self.default_fees['taker'],
            source='default',
            exchange=exchange_name
        )
    
    def apply_buy_fees(self, price: float, amount: float, fees: FeesPublic, 
                      symbol: Optional[str] = None, is_maker: bool = False) -> Tuple[float, float]:
        """Apply fees to a buy order.
        
        Args:
            price: Order price
            amount: Order amount
            fees: Fee structure
            symbol: Trading symbol
            is_maker: Whether this is a maker order (default: taker)
            
        Returns:
            Tuple of (effective_price_after_fees, effective_amount)
        """
        maker_fee, taker_fee = fees.get_fees(symbol)
        fee_rate = maker_fee if is_maker else taker_fee
        
        # For buy orders, we pay more (price increases)
        effective_price = price * (1 + fee_rate)
        
        return effective_price, amount
    
    def apply_sell_fees(self, price: float, amount: float, fees: FeesPublic,
                       symbol: Optional[str] = None, is_maker: bool = False) -> Tuple[float, float]:
        """Apply fees to a sell order.
        
        Args:
            price: Order price
            amount: Order amount  
            fees: Fee structure
            symbol: Trading symbol
            is_maker: Whether this is a maker order (default: taker)
            
        Returns:
            Tuple of (effective_price_after_fees, effective_amount_after_fees)
        """
        maker_fee, taker_fee = fees.get_fees(symbol)
        fee_rate = maker_fee if is_maker else taker_fee
        
        # For sell orders, we receive less (amount decreases)
        effective_amount = amount * (1 - fee_rate)
        
        return price, effective_amount
    
    def calculate_round_trip_fee(self, fees: FeesPublic, symbol: Optional[str] = None,
                                is_maker: bool = False) -> float:
        """Calculate the total fee for a round trip (buy + sell).
        
        Args:
            fees: Fee structure
            symbol: Trading symbol
            is_maker: Whether orders are maker orders
            
        Returns:
            Total round trip fee rate
        """
        maker_fee, taker_fee = fees.get_fees(symbol)
        fee_rate = maker_fee if is_maker else taker_fee
        
        # Round trip = buy fee + sell fee (approximately 2x for small fees)
        return 2 * fee_rate
    
    def get_fee_summary(self, exchange_name: str) -> str:
        """Get a human-readable fee summary for an exchange.
        
        Args:
            exchange_name: Name of the exchange
            
        Returns:
            Fee summary string
        """
        if exchange_name in self.fee_cache:
            fees = self.fee_cache[exchange_name]
        else:
            # Get default fees without caching
            if exchange_name in self.known_fees:
                known = self.known_fees[exchange_name]
                fees = FeesPublic(
                    maker=known['maker'],
                    taker=known['taker'],
                    source='default',
                    exchange=exchange_name
                )
            else:
                fees = FeesPublic(
                    maker=self.default_fees['maker'],
                    taker=self.default_fees['taker'],
                    source='default',
                    exchange=exchange_name
                )
        
        maker_pct = fees.maker * 100
        taker_pct = fees.taker * 100
        
        return f"{maker_pct:.3f}%/{taker_pct:.3f}%"

# Global fee manager instance
fee_manager = FeeManager()
