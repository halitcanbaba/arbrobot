"""Health monitoring for exchanges and system components."""

import asyncio
import logging
import psutil
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from models import ExchangeHealth
from registry import registry
from config import config
from db import db_manager

logger = logging.getLogger(__name__)

class HealthMonitor:
    """Monitors health of exchanges and system components."""
    
    def __init__(self):
        self.monitor_task: Optional[asyncio.Task] = None
        self.system_stats = {
            'cpu_percent': 0.0,
            'memory_percent': 0.0,
            'event_loop_lag_ms': 0.0,
        }
        
    async def start(self) -> None:
        """Start health monitoring."""
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Health monitor started")
    
    async def stop(self) -> None:
        """Stop health monitoring."""
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Health monitor stopped")
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while True:
            try:
                await self._collect_health_data()
                await asyncio.sleep(config.HEALTH_CHECK_INTERVAL_S)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitor loop: {e}")
                await asyncio.sleep(5)
    
    async def _collect_health_data(self) -> None:
        """Collect health data for all exchanges."""
        try:
            # Update system stats
            await self._update_system_stats()
            
            # Collect health for each exchange
            for exchange_name in registry.exchanges.keys():
                health = await self._collect_exchange_health(exchange_name)
                if health:
                    # Store in database
                    await db_manager.store_health_snapshot(health)
                    
                    # Update registry
                    registry.health[exchange_name] = health
                    
                    # Log unhealthy exchanges
                    if not health.is_healthy():
                        logger.warning(f"Exchange {exchange_name} is unhealthy: {health}")
            
        except Exception as e:
            logger.error(f"Error collecting health data: {e}")
    
    async def _collect_exchange_health(self, exchange_name: str) -> Optional[ExchangeHealth]:
        """Collect health data for a specific exchange.
        
        Args:
            exchange_name: Name of the exchange
            
        Returns:
            ExchangeHealth object or None if collection failed
        """
        try:
            # Get current health from registry or create new
            current_health = registry.get_health(exchange_name)
            if current_health:
                health = current_health
            else:
                health = ExchangeHealth(exchange=exchange_name)
            
            # Update system metrics
            health.event_loop_lag_ms = self.system_stats['event_loop_lag_ms']
            health.last_updated = datetime.utcnow()
            
            return health
            
        except Exception as e:
            logger.debug(f"Error collecting health for {exchange_name}: {e}")
            return None
    
    async def _update_system_stats(self) -> None:
        """Update system performance statistics."""
        try:
            # CPU usage
            self.system_stats['cpu_percent'] = psutil.cpu_percent(interval=None)
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.system_stats['memory_percent'] = memory.percent
            
            # Event loop lag measurement
            start_time = asyncio.get_event_loop().time()
            await asyncio.sleep(0)  # Yield control
            end_time = asyncio.get_event_loop().time()
            lag_ms = (end_time - start_time) * 1000
            self.system_stats['event_loop_lag_ms'] = lag_ms
            
        except Exception as e:
            logger.debug(f"Error updating system stats: {e}")
    
    def get_system_health_summary(self) -> Dict[str, any]:
        """Get overall system health summary.
        
        Returns:
            Dictionary with system health information
        """
        exchange_health = {}
        healthy_exchanges = 0
        total_exchanges = 0
        
        for exchange_name, health in registry.health.items():
            total_exchanges += 1
            is_healthy = health.is_healthy()
            
            if is_healthy:
                healthy_exchanges += 1
            
            exchange_health[exchange_name] = {
                'healthy': is_healthy,
                'ws_connected': health.ws_connected,
                'rest_ok': health.rest_ok,
                'reconnect_count': health.reconnect_count,
                'error_rate': health.error_rate,
                'queue_length': health.queue_length,
                'last_updated': health.last_updated.isoformat() if health.last_updated else None
            }
        
        return {
            'system': {
                'cpu_percent': self.system_stats['cpu_percent'],
                'memory_percent': self.system_stats['memory_percent'],
                'event_loop_lag_ms': self.system_stats['event_loop_lag_ms'],
            },
            'exchanges': {
                'total': total_exchanges,
                'healthy': healthy_exchanges,
                'unhealthy': total_exchanges - healthy_exchanges,
                'details': exchange_health
            },
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_unhealthy_exchanges(self) -> List[str]:
        """Get list of unhealthy exchanges.
        
        Returns:
            List of exchange names that are unhealthy
        """
        unhealthy = []
        
        for exchange_name, health in registry.health.items():
            if not health.is_healthy():
                unhealthy.append(exchange_name)
        
        return unhealthy
    
    async def check_exchange_connectivity(self, exchange_name: str) -> bool:
        """Check if an exchange is currently reachable.
        
        Args:
            exchange_name: Name of the exchange to check
            
        Returns:
            True if exchange is reachable, False otherwise
        """
        try:
            exchange = registry.get_exchange(exchange_name)
            if not exchange:
                return False
            
            # Try a simple API call (get server time or similar)
            if hasattr(exchange, 'fetch_time'):
                await asyncio.wait_for(
                    asyncio.to_thread(exchange.fetch_time),
                    timeout=10.0
                )
                return True
            
            # Fallback: try to load markets
            if hasattr(exchange, 'load_markets'):
                await asyncio.wait_for(
                    asyncio.to_thread(exchange.load_markets),
                    timeout=15.0
                )
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Connectivity check failed for {exchange_name}: {e}")
            return False
    
    def get_performance_metrics(self) -> Dict[str, any]:
        """Get performance metrics for monitoring.
        
        Returns:
            Dictionary with performance metrics
        """
        # Count total order book updates
        total_updates = 0
        total_coalesced = 0
        total_queue_length = 0
        
        for health in registry.health.values():
            total_coalesced += health.coalesced_updates
            total_queue_length += health.queue_length
        
        return {
            'system_performance': {
                'cpu_percent': self.system_stats['cpu_percent'],
                'memory_percent': self.system_stats['memory_percent'],
                'event_loop_lag_ms': self.system_stats['event_loop_lag_ms'],
            },
            'data_flow': {
                'total_coalesced_updates': total_coalesced,
                'total_queue_length': total_queue_length,
            },
            'timestamp': datetime.utcnow().isoformat()
        }

# Global health monitor instance
health_monitor = HealthMonitor()
