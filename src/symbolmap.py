"""Symbol normalization across exchanges."""

import re
from typing import Dict, Tuple, Optional, Set
from models import MarketMeta

class SymbolMapper:
    """Handles symbol normalization across different exchanges."""
    
    def __init__(self):
        # Common symbol mappings (exchange_symbol -> standard_symbol)
        self.symbol_mappings: Dict[str, Dict[str, str]] = {
            'kraken': {
                'XBTUSD': 'BTC/USD',
                'XBTUSDT': 'BTC/USDT',
                'XETHZUSD': 'ETH/USD',
                'XETHZUSDT': 'ETH/USDT',
                'XXBTZUSD': 'BTC/USD',
                'XXBTZUSDT': 'BTC/USDT',
                'XETHXXBT': 'ETH/BTC',
            },
            'bitfinex': {
                'BTCUSD': 'BTC/USD',
                'BTCUSDT': 'BTC/USDT',
                'ETHUSD': 'ETH/USD',
                'ETHUSDT': 'ETH/USDT',
                'ETHBTC': 'ETH/BTC',
            },
            'binance': {
                'BTCUSDT': 'BTC/USDT',
                'ETHUSDT': 'ETH/USDT',
                'BNBUSDT': 'BNB/USDT',
                'ADAUSDT': 'ADA/USDT',
            }
        }
        
        # Reverse mappings (standard_symbol -> exchange_symbol)
        self.reverse_mappings: Dict[str, Dict[str, str]] = {}
        for exchange, mappings in self.symbol_mappings.items():
            self.reverse_mappings[exchange] = {v: k for k, v in mappings.items()}
    
    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Normalize a symbol from an exchange to standard format.
        
        Args:
            symbol: Exchange-specific symbol
            exchange: Exchange name
            
        Returns:
            Normalized symbol in BASE/QUOTE format
        """
        # Check for explicit mapping first
        if exchange in self.symbol_mappings:
            if symbol in self.symbol_mappings[exchange]:
                return self.symbol_mappings[exchange][symbol]
        
        # If already in BASE/QUOTE format, return as-is
        if '/' in symbol:
            return symbol
        
        # Try to split common formats
        return self._auto_normalize(symbol)
    
    def _auto_normalize(self, symbol: str) -> str:
        """Attempt automatic normalization using common patterns."""
        # Remove common prefixes/suffixes
        clean_symbol = symbol.upper()
        
        # Handle common patterns
        patterns = [
            # BTCUSDT -> BTC/USDT
            (r'^([A-Z]{2,5})(USDT|USDC|USD|EUR|BTC|ETH|BNB)$', r'\1/\2'),
            # XBTUSD -> BTC/USD (Kraken style)
            (r'^X([A-Z]{2,4})Z?(USD|EUR)$', r'\1/\2'),
            # ETHXBT -> ETH/BTC
            (r'^([A-Z]{2,4})XBT$', r'\1/BTC'),
        ]
        
        for pattern, replacement in patterns:
            match = re.match(pattern, clean_symbol)
            if match:
                return re.sub(pattern, replacement, clean_symbol)
        
        # If no pattern matches, return as-is
        return symbol
    
    def get_exchange_symbol(self, standard_symbol: str, exchange: str) -> str:
        """Convert standard symbol to exchange-specific format.
        
        Args:
            standard_symbol: Symbol in BASE/QUOTE format
            exchange: Exchange name
            
        Returns:
            Exchange-specific symbol format
        """
        # Check for explicit reverse mapping
        if exchange in self.reverse_mappings:
            if standard_symbol in self.reverse_mappings[exchange]:
                return self.reverse_mappings[exchange][standard_symbol]
        
        # Default: return as-is (most exchanges support BASE/QUOTE)
        return standard_symbol
    
    def parse_symbol(self, symbol: str) -> Tuple[str, str]:
        """Parse a symbol into base and quote assets.
        
        Args:
            symbol: Symbol to parse (e.g., "BTC/USDT")
            
        Returns:
            Tuple of (base, quote)
        """
        if '/' in symbol:
            parts = symbol.split('/')
            if len(parts) == 2:
                return parts[0].upper(), parts[1].upper()
        
        # Try auto-parsing for symbols without separator
        normalized = self._auto_normalize(symbol)
        if '/' in normalized:
            parts = normalized.split('/')
            return parts[0].upper(), parts[1].upper()
        
        # Fallback: assume last 3-4 chars are quote
        if len(symbol) >= 6:
            for quote_len in [4, 3]:
                if len(symbol) > quote_len:
                    potential_quote = symbol[-quote_len:].upper()
                    if potential_quote in ['USDT', 'USDC', 'USD', 'EUR', 'BTC', 'ETH', 'BNB']:
                        base = symbol[:-quote_len].upper()
                        return base, potential_quote
        
        # Couldn't parse
        raise ValueError(f"Cannot parse symbol: {symbol}")
    
    def create_symbol_map(self, markets: Dict[str, Dict[str, MarketMeta]]) -> Dict[str, Set[str]]:
        """Create a mapping of normalized symbols to exchanges that support them.
        
        Args:
            markets: Markets data from registry
            
        Returns:
            Dict mapping normalized symbols to set of exchange names
        """
        symbol_map: Dict[str, Set[str]] = {}
        
        for exchange, exchange_markets in markets.items():
            for symbol, market in exchange_markets.items():
                if not market.active:
                    continue
                
                # Normalize the symbol
                try:
                    normalized = self.normalize_symbol(symbol, exchange)
                    
                    if normalized not in symbol_map:
                        symbol_map[normalized] = set()
                    
                    symbol_map[normalized].add(exchange)
                    
                except Exception:
                    # Skip symbols we can't normalize
                    continue
        
        return symbol_map
    
    def get_precision_info(self, symbol: str, exchange: str, markets: Dict[str, MarketMeta]) -> Optional[Tuple[int, int]]:
        """Get precision information for a symbol on an exchange.
        
        Args:
            symbol: Standard symbol format
            exchange: Exchange name
            markets: Markets data for the exchange
            
        Returns:
            Tuple of (price_precision, amount_precision) or None if not found
        """
        # Convert to exchange format
        exchange_symbol = self.get_exchange_symbol(symbol, exchange)
        
        if exchange_symbol in markets:
            market = markets[exchange_symbol]
            return market.price_precision, market.amount_precision
        
        return None
    
    def get_min_trade_limits(self, symbol: str, exchange: str, markets: Dict[str, MarketMeta]) -> Optional[Tuple[Optional[float], Optional[float]]]:
        """Get minimum trade limits for a symbol on an exchange.
        
        Args:
            symbol: Standard symbol format
            exchange: Exchange name
            markets: Markets data for the exchange
            
        Returns:
            Tuple of (min_amount, min_notional) or None if not found
        """
        # Convert to exchange format
        exchange_symbol = self.get_exchange_symbol(symbol, exchange)
        
        if exchange_symbol in markets:
            market = markets[exchange_symbol]
            return market.min_amount, market.min_notional
        
        return None

# Global symbol mapper instance
symbol_mapper = SymbolMapper()
