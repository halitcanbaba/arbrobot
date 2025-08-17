"""Exchange registry for discovering and managing exchanges."""

import asyncio
import logging
from typing import Dict, List, Set, Optional, Any, Tuple
import ccxt
from models import MarketMeta, ExchangeHealth
from config import config

logger = logging.getLogger(__name__)

class ExchangeRegistry:
    """Manages exchange discovery and market metadata."""
    
    def __init__(self):
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self.markets: Dict[str, Dict[str, MarketMeta]] = {}
        self.health: Dict[str, ExchangeHealth] = {}
        self._semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_EXCHANGES)
    
    async def discover_exchanges(self) -> List[str]:
        """Discover available exchanges with proper filtering.
        
        Returns:
            List of exchange names that are available and working.
        """
        logger.info("Discovering exchanges...")
        
        # Get all available exchanges
        all_exchanges = ccxt.exchanges
        
        # Add custom exchanges that aren't in CCXT
        custom_exchanges = ['cointr']
        all_exchanges = all_exchanges + custom_exchanges
        
        # Apply filters
        candidate_exchanges = []
        
        if config.INCLUDE_EXCHANGES:
            # Only include specified exchanges
            candidate_exchanges = [
                ex for ex in all_exchanges 
                if ex.lower() in [inc.lower() for inc in config.INCLUDE_EXCHANGES]
            ]
        else:
            # Include all by default
            candidate_exchanges = all_exchanges.copy()
        
        # Remove excluded exchanges
        if config.EXCLUDE_EXCHANGES:
            exclude_lower = [exc.lower() for exc in config.EXCLUDE_EXCHANGES]
            candidate_exchanges = [
                ex for ex in candidate_exchanges 
                if ex.lower() not in exclude_lower
            ]
        
        logger.info(f"Testing {len(candidate_exchanges)} candidate exchanges...")
        
        # Test exchanges concurrently
        tasks = []
        for exchange_name in candidate_exchanges:
            task = asyncio.create_task(
                self._test_exchange(exchange_name)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        working_exchanges = []
        for exchange_name, result in zip(candidate_exchanges, results):
            if isinstance(result, Exception):
                logger.warning(f"Exchange {exchange_name} failed test: {result}")
                continue
            
            if result:  # Exchange is working
                working_exchanges.append(exchange_name)
                logger.info(f"✓ {exchange_name} is available")
            else:
                logger.warning(f"✗ {exchange_name} failed tests")
        
        logger.info(f"Discovered {len(working_exchanges)} working exchanges: {working_exchanges}")
        return working_exchanges
    
    async def _test_exchange(self, exchange_name: str) -> bool:
        """Test if an exchange is working and load its markets.
        
        Args:
            exchange_name: Name of the exchange to test
            
        Returns:
            True if exchange is working, False otherwise
        """
        async with self._semaphore:
            try:
                # Special handling for CoinTR
                if exchange_name.lower() == 'cointr':
                    return await self._test_cointr()
                
                # Create CCXT exchange instance
                exchange_class = getattr(ccxt, exchange_name)
                exchange = exchange_class({
                    'enableRateLimit': True,
                    'sandbox': False,
                    'timeout': 10000,
                })
                
                # Test by loading markets
                markets = await asyncio.wait_for(
                    asyncio.to_thread(exchange.load_markets),
                    timeout=15.0
                )
                
                if not markets:
                    return False
                
                # Store exchange and markets
                self.exchanges[exchange_name] = exchange
                self.markets[exchange_name] = {}
                
                # Convert to our MarketMeta format - spot markets only
                loaded_count = 0
                for symbol, market in markets.items():
                    try:
                        # Basic filters only
                        if ':' in symbol:  # Skip derivatives
                            continue
                            
                        if not market.get('active', False):  # Must be active
                            continue
                            
                        base = market.get('base', '')
                        quote = market.get('quote', '')
                        if not base or not quote:  # Must have base/quote
                            continue
                        
                        market_meta = MarketMeta(
                            symbol=symbol,
                            base=base,
                            quote=quote,
                            active=True,
                            price_precision=8,
                            amount_precision=8,
                            min_amount=None,
                            min_notional=None,
                            exchange=exchange_name
                        )
                        self.markets[exchange_name][symbol] = market_meta
                        loaded_count += 1
                        
                        # Debug first few
                        if loaded_count <= 3:
                            logger.info(f"Loaded {symbol} for {exchange_name}: active={market.get('active')}, base={base}, quote={quote}")
                        
                    except Exception as e:
                        logger.error(f"Failed to parse market {symbol} on {exchange_name}: {e}")
                        continue
                
                # Initialize health tracking
                self.health[exchange_name] = ExchangeHealth(
                    exchange=exchange_name,
                    rest_ok=True
                )
                
                logger.debug(f"Loaded {len(self.markets[exchange_name])} markets for {exchange_name}")
                
                # Debug: Show sample markets loaded
                if self.markets[exchange_name]:
                    sample_symbols = list(self.markets[exchange_name].keys())[:5]
                    logger.info(f"Loaded markets for {exchange_name}: {sample_symbols}")
                else:
                    logger.warning(f"No markets loaded for {exchange_name} - check filtering")
                return True
                
            except Exception as e:
                logger.debug(f"Exchange {exchange_name} test failed: {e}")
                return False
    
    def get_exchange(self, exchange_name: str) -> Optional[ccxt.Exchange]:
        """Get exchange instance by name."""
        return self.exchanges.get(exchange_name)
    
    def get_markets(self, exchange_name: str) -> Dict[str, MarketMeta]:
        """Get markets for an exchange."""
        return self.markets.get(exchange_name, {})
    
    def get_common_symbols(self, min_exchanges: int = 2) -> Set[str]:
        """Get symbols that are available on at least min_exchanges.
        
        Args:
            min_exchanges: Minimum number of exchanges that must support the symbol
            
        Returns:
            Set of symbols available on enough exchanges
        """
        symbol_counts: Dict[str, int] = {}
        
        for exchange_name, markets in self.markets.items():
            for symbol in markets.keys():
                if markets[symbol].active:
                    symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        
        return {
            symbol for symbol, count in symbol_counts.items() 
            if count >= min_exchanges
        }
    
    def get_triangular_symbols(self, exchange_name: str, base_assets: List[str]) -> List[Tuple[str, str, str]]:
        """Get potential triangular arbitrage paths for an exchange.
        
        Args:
            exchange_name: Name of the exchange
            base_assets: List of base assets (e.g., ["USDT", "USDC", "BTC"])
            
        Returns:
            List of (base, asset2, asset3) tuples forming valid triangular paths
        """
        if exchange_name not in self.markets:
            return []
        
        markets = self.markets[exchange_name]
        paths = []
        
        # Get excluded quote assets from config
        exclude_quotes = set(config.TRI_EXCLUDE_QUOTES)
        
        # Get active symbols grouped by base/quote
        symbols_by_base: Dict[str, List[str]] = {}
        symbols_by_quote: Dict[str, List[str]] = {}
        
        for symbol, market in markets.items():
            if not market.active:
                continue
                
            base, quote = market.base, market.quote
            
            # Skip markets with excluded quote assets
            if quote in exclude_quotes:
                continue
            
            if base not in symbols_by_base:
                symbols_by_base[base] = []
            symbols_by_base[base].append(quote)
            
            if quote not in symbols_by_quote:
                symbols_by_quote[quote] = []
            symbols_by_quote[quote].append(base)
        
        # For each base asset, find triangular paths
        for base in base_assets:
            if base not in symbols_by_base:
                continue
            
            # Get assets we can buy with this base
            reachable_from_base = symbols_by_base[base]
            
            for asset2 in reachable_from_base:
                if asset2 == base:
                    continue
                
                # Skip excluded quote assets
                if asset2 in exclude_quotes:
                    continue
                
                # Find assets we can buy with asset2
                if asset2 in symbols_by_base:
                    reachable_from_asset2 = symbols_by_base[asset2]
                    
                    for asset3 in reachable_from_asset2:
                        if asset3 == base or asset3 == asset2:
                            continue
                        
                        # Skip excluded quote assets
                        if asset3 in exclude_quotes:
                            continue
                        
                        # Check if we can sell asset3 back to base
                        if asset3 in symbols_by_quote and base in symbols_by_quote[asset3]:
                            paths.append((base, asset2, asset3))
        
        if exclude_quotes:
            logger.debug(f"Found {len(paths)} triangular paths for {exchange_name} (excluding quotes: {exclude_quotes})")
        else:
            logger.debug(f"Found {len(paths)} triangular paths for {exchange_name}")
        return paths
    
    def has_websocket_support(self, exchange_name: str) -> bool:
        """Check if exchange supports WebSocket order book streaming."""
        # CoinTR has native WebSocket support
        if exchange_name.lower() == 'cointr':
            return True
            
        try:
            # Check CCXT.Pro support for WebSocket
            import ccxt.pro as ccxtpro
            if hasattr(ccxtpro, exchange_name):
                pro_exchange_class = getattr(ccxtpro, exchange_name)
                pro_exchange = pro_exchange_class()
                return getattr(pro_exchange, 'has', {}).get('watchOrderBook', False)
        except ImportError:
            pass
        
        # Fallback to regular CCXT
        exchange = self.get_exchange(exchange_name)
        if not exchange:
            return False
        
        return getattr(exchange, 'has', {}).get('watchOrderBook', False)
    
    def get_health(self, exchange_name: str) -> Optional[ExchangeHealth]:
        """Get health metrics for an exchange."""
        return self.health.get(exchange_name)
    
    def update_health(self, exchange_name: str, **kwargs) -> None:
        """Update health metrics for an exchange."""
        if exchange_name in self.health:
            for key, value in kwargs.items():
                if hasattr(self.health[exchange_name], key):
                    setattr(self.health[exchange_name], key, value)
    
    async def cleanup(self) -> None:
        """Cleanup exchange connections."""
        for exchange in self.exchanges.values():
            try:
                if hasattr(exchange, 'close'):
                    await exchange.close()
            except Exception as e:
                logger.debug(f"Error closing exchange: {e}")
    
    async def _test_cointr(self) -> bool:
        """Test CoinTR exchange connectivity."""
        try:
            import aiohttp
            import ssl
            
            # Test CoinTR REST API with SSL verification disabled
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                url = "https://api.cointr.com/api/v2/spot/public/symbols"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        symbols = data.get('data', [])
                        
                        if symbols:
                            # Create fake exchange object for CoinTR
                            fake_exchange = type('FakeExchange', (), {
                                'name': 'CoinTR',
                                'markets': {},
                                'has': {'watchOrderBook': True}  # WebSocket support
                            })()
                            
                            # Store markets
                            sample_markets = {}
                            for symbol_data in symbols:
                                try:
                                    symbol = symbol_data.get('symbol', '')
                                    base_coin = symbol_data.get('baseCoin', '')
                                    quote_coin = symbol_data.get('quoteCoin', '')
                                    status = symbol_data.get('status', '')
                                    
                                    if symbol and base_coin and quote_coin and status == 'online':
                                        # Convert to standard format
                                        standard_symbol = f"{base_coin}/{quote_coin}"
                                        sample_markets[standard_symbol] = MarketMeta(
                                            symbol=standard_symbol,
                                            base=base_coin,
                                            quote=quote_coin,
                                            active=True,
                                            price_precision=8,
                                            amount_precision=8,
                                            min_amount=None,
                                            min_notional=None,
                                            exchange='cointr'
                                        )
                                except Exception as e:
                                    logger.debug(f"Failed to parse CoinTR symbol {symbol_data}: {e}")
                                    continue
                            
                            self.exchanges['cointr'] = fake_exchange
                            self.markets['cointr'] = sample_markets
                            self.health['cointr'] = ExchangeHealth(
                                exchange='cointr',
                                rest_ok=True
                            )
                            
                            logger.info(f"Loaded {len(sample_markets)} markets for cointr")
                            
                            # Debug: Show some sample symbols
                            sample_symbols = list(sample_markets.keys())[:5]
                            logger.info(f"Sample CoinTR markets: {sample_symbols}")
                            return True
                        
        except Exception as e:
            logger.warning(f"CoinTR test failed: {e}")
            
        return False

# Global registry instance
registry = ExchangeRegistry()
