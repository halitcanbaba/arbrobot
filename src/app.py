"""Main application orchestrator for the arbitrage bot."""

import asyncio
import logging
import signal
import sys
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta
import uvloop

from config import config
from registry import registry
from connectors.ccxt_generic import create_connector
from engine import ArbitrageEngine
from tri_engine import TriangularArbitrageEngine
from alert import alert_manager
from db import db_manager
from health import health_monitor
from symbolmap import symbol_mapper

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('arbitrage_bot.log')
    ]
)

logger = logging.getLogger(__name__)

class ArbitrageBotApp:
    """Main application orchestrator."""
    
    def __init__(self):
        self.running = False
        self.connectors: Dict[str, any] = {}
        self.connector_tasks: Dict[str, List[asyncio.Task]] = {}
        self.arbitrage_engine = ArbitrageEngine()
        self.tri_engine = TriangularArbitrageEngine()
        
        # Scanning tasks
        self.cross_scan_task: Optional[asyncio.Task] = None
        self.tri_scan_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        
        # Order book coalescing
        self.order_book_queues: Dict[Tuple[str, str], asyncio.Queue] = {}
        self.coalesce_tasks: Dict[Tuple[str, str], asyncio.Task] = {}
        
        # Stats
        self.start_time = datetime.utcnow()
        self.stats = {
            'opportunities_found': 0,
            'tri_opportunities_found': 0,
            'alerts_sent': 0,
            'order_book_updates': 0,
        }
    
    async def start(self) -> None:
        """Start the arbitrage bot."""
        logger.info("Starting Crypto Arbitrage Alert Bot")
        
        try:
            # Validate configuration
            if not config.validate():
                logger.warning("Configuration validation failed - continuing with limited functionality")
            
            # Initialize components
            await self._initialize_components()
            
            # Discover and connect to exchanges
            await self._discover_and_connect_exchanges()
            
            # Start data collection
            await self._start_data_collection()
            
            # Start scanning engines
            await self._start_scanning_engines()
            
            # Set up signal handlers
            self._setup_signal_handlers()
            
            self.running = True
            logger.info("Arbitrage bot started successfully")
            
            # Send startup notification
            await alert_manager.send_status_message("Bot started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start arbitrage bot: {e}")
            await self.stop()
            raise
    
    async def stop(self) -> None:
        """Stop the arbitrage bot."""
        logger.info("Stopping arbitrage bot...")
        self.running = False
        
        try:
            # Stop scanning tasks
            tasks_to_cancel = []
            
            if self.cross_scan_task:
                tasks_to_cancel.append(self.cross_scan_task)
            
            if self.tri_scan_task:
                tasks_to_cancel.append(self.tri_scan_task)
            
            if self.cleanup_task:
                tasks_to_cancel.append(self.cleanup_task)
            
            # Cancel coalescing tasks
            for task in self.coalesce_tasks.values():
                tasks_to_cancel.append(task)
            
            # Cancel connector tasks
            for exchange_tasks in self.connector_tasks.values():
                tasks_to_cancel.extend(exchange_tasks)
            
            # Cancel all tasks
            for task in tasks_to_cancel:
                task.cancel()
            
            # Wait for tasks to complete
            if tasks_to_cancel:
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            
            # Disconnect connectors
            for connector in self.connectors.values():
                await connector.disconnect()
            
            # Stop components
            await health_monitor.stop()
            await alert_manager.stop()
            await db_manager.close()
            await registry.cleanup()
            
            logger.info("Arbitrage bot stopped")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def _initialize_components(self) -> None:
        """Initialize all components."""
        logger.info("Initializing components...")
        
        # Initialize database
        await db_manager.initialize()
        
        # Initialize alert manager
        await alert_manager.start()
        
        # Initialize health monitor
        await health_monitor.start()
        
        logger.info("Components initialized")
    
    async def _discover_and_connect_exchanges(self) -> None:
        """Discover exchanges and create connectors."""
        logger.info("Discovering exchanges...")
        
        # Discover working exchanges
        exchanges = await registry.discover_exchanges()
        
        if not exchanges:
            raise RuntimeError("No working exchanges found")
        
        logger.info(f"Creating connectors for {len(exchanges)} exchanges...")
        
        # Create connectors for each exchange
        for exchange_name in exchanges:
            try:
                connector = create_connector(exchange_name)
                
                if await connector.connect():
                    self.connectors[exchange_name] = connector
                    self.connector_tasks[exchange_name] = []
                    logger.info(f"✓ Connected to {exchange_name}")
                else:
                    logger.warning(f"✗ Failed to connect to {exchange_name}")
                    
            except Exception as e:
                logger.error(f"Error connecting to {exchange_name}: {e}")
        
        if not self.connectors:
            raise RuntimeError("Failed to connect to any exchanges")
        
        logger.info(f"Connected to {len(self.connectors)} exchanges")
    
    async def _start_data_collection(self) -> None:
        """Start order book data collection."""
        logger.info("Starting order book data collection...")
        
        # Determine symbols to monitor
        symbols_to_monitor = set(config.SYMBOL_UNIVERSE)
        
        # Add symbols needed for triangular arbitrage
        for exchange_name in self.connectors.keys():
            tri_paths = registry.get_triangular_symbols(exchange_name, config.TRI_BASES)
            for base, asset2, asset3 in tri_paths:
                # Add required symbols for triangular paths
                symbols_to_monitor.update([
                    f"{base}/{asset2}",
                    f"{asset2}/{asset3}",
                    f"{asset3}/{base}",
                    f"{asset2}/{base}",  # Reverse symbols
                    f"{asset3}/{asset2}",
                    f"{base}/{asset3}",
                ])
        
        logger.info(f"Monitoring {len(symbols_to_monitor)} symbols across {len(self.connectors)} exchanges")
        
        # Start data collection for each exchange/symbol pair
        for exchange_name, connector in self.connectors.items():
            exchange_markets = registry.get_markets(exchange_name)
            logger.info(f"Exchange {exchange_name} has {len(exchange_markets)} markets")
            
            # Debug: Show first few markets
            sample_markets = list(exchange_markets.keys())[:5]
            logger.info(f"Sample markets for {exchange_name}: {sample_markets}")
            
            for symbol in symbols_to_monitor:
                # Check if exchange supports this symbol
                normalized_symbol = symbol_mapper.get_exchange_symbol(symbol, exchange_name)
                
                if normalized_symbol not in exchange_markets:
                    logger.debug(f"Symbol {symbol} ({normalized_symbol}) not available on {exchange_name}")
                    continue
                
                logger.info(f"Starting collection for {symbol} on {exchange_name}")
                
                # Create order book queue for coalescing
                queue_key = (exchange_name, symbol)
                self.order_book_queues[queue_key] = asyncio.Queue(maxsize=2)
                
                # Start coalescing task
                self.coalesce_tasks[queue_key] = asyncio.create_task(
                    self._coalesce_order_books(queue_key)
                )
                
                # Start data collection task
                # Force REST polling since WebSocket is not available
                task = asyncio.create_task(
                    self._poll_order_book_rest(exchange_name, symbol, connector)
                )
                
                self.connector_tasks[exchange_name].append(task)
        
        logger.info("Order book data collection started")
    
    async def _watch_order_book_ws(self, exchange_name: str, symbol: str, connector) -> None:
        """Watch order book via WebSocket."""
        queue_key = (exchange_name, symbol)
        
        try:
            normalized_symbol = symbol_mapper.get_exchange_symbol(symbol, exchange_name)
            
            async for order_book in connector.watch_order_book(normalized_symbol, config.DEPTH_LEVELS):
                # Update symbol to standard format
                order_book.symbol = symbol
                
                # Queue for coalescing
                try:
                    self.order_book_queues[queue_key].put_nowait(order_book)
                except asyncio.QueueFull:
                    # Drop oldest update
                    try:
                        self.order_book_queues[queue_key].get_nowait()
                        self.order_book_queues[queue_key].put_nowait(order_book)
                        
                        # Update coalescing stats
                        if exchange_name in registry.health:
                            health = registry.health[exchange_name]
                            health.coalesced_updates += 1
                            
                    except asyncio.QueueEmpty:
                        pass
                
                self.stats['order_book_updates'] += 1
                
        except Exception as e:
            logger.error(f"WebSocket error for {symbol} on {exchange_name}: {e}")
            
            # Update health
            registry.update_health(exchange_name, ws_connected=False)
    
    async def _poll_order_book_rest(self, exchange_name: str, symbol: str, connector) -> None:
        """Poll order book via REST API."""
        queue_key = (exchange_name, symbol)
        
        # Adaptive polling intervals based on symbol liquidity
        high_liquidity_symbols = {'BTC/USDT', 'ETH/USDT', 'BNB/USDT'}
        poll_interval = 1.0 if symbol in high_liquidity_symbols else 3.0
        
        try:
            normalized_symbol = symbol_mapper.get_exchange_symbol(symbol, exchange_name)
            
            while self.running:
                order_book = await connector.fetch_order_book(normalized_symbol, config.DEPTH_LEVELS)
                
                if order_book:
                    # Update symbol to standard format
                    order_book.symbol = symbol
                    
                    # Queue for coalescing
                    try:
                        self.order_book_queues[queue_key].put_nowait(order_book)
                    except asyncio.QueueFull:
                        # Drop oldest update
                        try:
                            self.order_book_queues[queue_key].get_nowait()
                            self.order_book_queues[queue_key].put_nowait(order_book)
                        except asyncio.QueueEmpty:
                            pass
                    
                    self.stats['order_book_updates'] += 1
                
                await asyncio.sleep(poll_interval)
                
        except Exception as e:
            logger.error(f"REST polling error for {symbol} on {exchange_name}: {e}")
    
    async def _coalesce_order_books(self, queue_key: Tuple[str, str]) -> None:
        """Coalesce order book updates to reduce processing load."""
        exchange_name, symbol = queue_key
        
        while self.running:
            try:
                # Wait for order book with timeout
                order_book = await asyncio.wait_for(
                    self.order_book_queues[queue_key].get(),
                    timeout=1.0
                )
                
                # Wait for coalescing window
                await asyncio.sleep(config.COALESCE_MS / 1000.0)
                
                # Get the latest update (drop intermediate ones)
                latest_book = order_book
                while True:
                    try:
                        latest_book = self.order_book_queues[queue_key].get_nowait()
                    except asyncio.QueueEmpty:
                        break
                
                # Update engines with latest order book
                self.arbitrage_engine.update_order_book(latest_book)
                self.tri_engine.update_order_book(latest_book)
                
                # Update queue length stat
                registry.update_health(
                    exchange_name,
                    queue_length=self.order_book_queues[queue_key].qsize()
                )
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in coalescing for {queue_key}: {e}")
                await asyncio.sleep(1)
    
    async def _start_scanning_engines(self) -> None:
        """Start the opportunity scanning engines."""
        logger.info("Starting scanning engines...")
        
        # Start cross-exchange arbitrage scanner
        self.cross_scan_task = asyncio.create_task(self._cross_exchange_scan_loop())
        
        # Start triangular arbitrage scanner
        self.tri_scan_task = asyncio.create_task(self._triangular_scan_loop())
        
        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        logger.info("Scanning engines started")
    
    async def _cross_exchange_scan_loop(self) -> None:
        """Cross-exchange arbitrage scanning loop."""
        while self.running:
            try:
                start_time = asyncio.get_event_loop().time()
                
                # Scan for opportunities
                opportunities = await self.arbitrage_engine.scan_opportunities(
                    symbols=config.SYMBOL_UNIVERSE,
                    exchanges=list(self.connectors.keys())
                )
                
                # Process opportunities
                for opportunity in opportunities:
                    self.stats['opportunities_found'] += 1
                    
                    # Store in database
                    await db_manager.store_opportunity(opportunity)
                    
                    # Send alert
                    if await alert_manager.send_cross_exchange_alert(opportunity):
                        self.stats['alerts_sent'] += 1
                
                # Adaptive scanning interval
                scan_time = (asyncio.get_event_loop().time() - start_time) * 1000
                base_interval = config.TRI_SCAN_MS / 1000.0
                
                # Increase interval if scanning is slow
                if scan_time > base_interval * 1000:
                    sleep_time = base_interval * 1.5
                else:
                    sleep_time = base_interval
                
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in cross-exchange scan loop: {e}")
                await asyncio.sleep(5)
    
    async def _triangular_scan_loop(self) -> None:
        """Triangular arbitrage scanning loop."""
        while self.running:
            try:
                start_time = asyncio.get_event_loop().time()
                
                # Scan for triangular opportunities
                opportunities = await self.tri_engine.scan_opportunities(
                    exchanges=list(self.connectors.keys())
                )
                
                # Process opportunities
                for opportunity in opportunities:
                    self.stats['tri_opportunities_found'] += 1
                    
                    # Store in database
                    await db_manager.store_tri_opportunity(opportunity)
                    
                    # Send alert
                    if await alert_manager.send_triangular_alert(opportunity):
                        self.stats['alerts_sent'] += 1
                
                # Adaptive scanning interval
                scan_time = (asyncio.get_event_loop().time() - start_time) * 1000
                base_interval = config.TRI_SCAN_MS / 1000.0
                
                if scan_time > base_interval * 1000:
                    sleep_time = base_interval * 1.5
                else:
                    sleep_time = base_interval
                
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in triangular scan loop: {e}")
                await asyncio.sleep(5)
    
    async def _cleanup_loop(self) -> None:
        """Periodic cleanup loop."""
        while self.running:
            try:
                # Clean up old database data weekly
                if datetime.utcnow().weekday() == 0:  # Monday
                    await db_manager.cleanup_old_data(days=7)
                
                # Sleep for 1 hour
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(300)  # 5 minutes on error
    
    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            asyncio.create_task(self.stop())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def get_stats(self) -> Dict[str, any]:
        """Get application statistics."""
        uptime = datetime.utcnow() - self.start_time
        
        return {
            'application': {
                'running': self.running,
                'uptime_seconds': uptime.total_seconds(),
                'start_time': self.start_time.isoformat(),
                'connected_exchanges': len(self.connectors),
                'active_symbols': len(self.order_book_queues),
            },
            'opportunities': {
                'cross_exchange_found': self.stats['opportunities_found'],
                'triangular_found': self.stats['tri_opportunities_found'],
                'alerts_sent': self.stats['alerts_sent'],
            },
            'data_flow': {
                'order_book_updates': self.stats['order_book_updates'],
                'active_queues': len([q for q in self.order_book_queues.values() if q.qsize() > 0]),
            },
            'engines': {
                'arbitrage_engine': self.arbitrage_engine.get_stats(),
                'tri_engine': self.tri_engine.get_stats(),
            },
            'components': {
                'alert_manager': alert_manager.get_stats(),
                'db_manager': db_manager.get_stats(),
            }
        }

async def main():
    """Main application entry point."""
    # Install uvloop for better performance
    uvloop.install()
    
    app = ArbitrageBotApp()
    
    try:
        await app.start()
        
        # Keep running until stopped
        while app.running:
            await asyncio.sleep(1)
            
            # Periodically log stats
            if datetime.utcnow().second % 60 == 0:  # Every minute
                stats = app.get_stats()
                logger.info(f"Stats: {stats['opportunities']} | {stats['data_flow']}")
    
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
