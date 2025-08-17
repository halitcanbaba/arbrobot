"""SQLite database for storing opportunities and health data."""

import asyncio
import logging
import aiosqlite
import orjson
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from models import Opportunity, TriOpportunity, ExchangeHealth
from config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages SQLite database operations."""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
        self.db: Optional[aiosqlite.Connection] = None
        self.write_queue = asyncio.Queue()
        self.write_task: Optional[asyncio.Task] = None
        self.batch_size = 10
        self.flush_interval = 5.0  # seconds
        
    async def initialize(self) -> None:
        """Initialize database and create tables."""
        try:
            self.db = await aiosqlite.connect(self.db_path)
            await self._create_tables()
            
            # Start the write queue processor
            self.write_task = asyncio.create_task(self._process_write_queue())
            
            logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def close(self) -> None:
        """Close database connection."""
        if self.write_task:
            self.write_task.cancel()
            try:
                await self.write_task
            except asyncio.CancelledError:
                pass
        
        if self.db:
            await self.db.close()
            logger.info("Database connection closed")
    
    async def _create_tables(self) -> None:
        """Create database tables."""
        
        # Opportunities table for cross-exchange arbitrage
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                buy_exchange TEXT,
                sell_exchange TEXT,
                buy_price_before_fees REAL,
                sell_price_before_fees REAL,
                buy_price_after_fees REAL,
                sell_price_after_fees REAL,
                spread_bps REAL,
                notional REAL NOT NULL,
                buy_depth_levels INTEGER,
                sell_depth_levels INTEGER,
                buy_fees_maker REAL,
                buy_fees_taker REAL,
                sell_fees_maker REAL,
                sell_fees_taker REAL,
                mode TEXT,
                timestamp DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Triangular opportunities table
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS tri_opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                exchange TEXT NOT NULL,
                base_asset TEXT NOT NULL,
                path_asset1 TEXT NOT NULL,
                path_asset2 TEXT NOT NULL,
                path_asset3 TEXT NOT NULL,
                start_amount REAL NOT NULL,
                end_amount REAL NOT NULL,
                gain_bps REAL NOT NULL,
                notional REAL NOT NULL,
                leg1_symbol TEXT NOT NULL,
                leg1_price REAL NOT NULL,
                leg1_side TEXT NOT NULL,
                leg2_symbol TEXT NOT NULL,
                leg2_price REAL NOT NULL,
                leg2_side TEXT NOT NULL,
                leg3_symbol TEXT NOT NULL,
                leg3_price REAL NOT NULL,
                leg3_side TEXT NOT NULL,
                fees_maker REAL NOT NULL,
                fees_taker REAL NOT NULL,
                timestamp DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Exchange health table
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS exchange_health (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                ws_connected INTEGER NOT NULL,
                rest_ok INTEGER NOT NULL,
                last_ws_message DATETIME,
                last_rest_call DATETIME,
                reconnect_count INTEGER DEFAULT 0,
                error_rate REAL DEFAULT 0.0,
                queue_length INTEGER DEFAULT 0,
                coalesced_updates INTEGER DEFAULT 0,
                event_loop_lag_ms REAL DEFAULT 0.0,
                symbols_subscribed TEXT,
                timestamp DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for better query performance
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_opportunities_timestamp 
            ON opportunities(timestamp)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_opportunities_symbol 
            ON opportunities(symbol)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_tri_opportunities_timestamp 
            ON tri_opportunities(timestamp)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_exchange_health_timestamp 
            ON exchange_health(timestamp)
        """)
        
        await self.db.commit()
    
    async def store_opportunity(self, opportunity: Opportunity) -> None:
        """Store cross-exchange opportunity (queued for batch processing).
        
        Args:
            opportunity: Opportunity to store
        """
        await self.write_queue.put(('opportunity', opportunity))
    
    async def store_tri_opportunity(self, opportunity: TriOpportunity) -> None:
        """Store triangular opportunity (queued for batch processing).
        
        Args:
            opportunity: Triangular opportunity to store
        """
        await self.write_queue.put(('tri_opportunity', opportunity))
    
    async def store_health_snapshot(self, health: ExchangeHealth) -> None:
        """Store exchange health snapshot (queued for batch processing).
        
        Args:
            health: Health snapshot to store
        """
        await self.write_queue.put(('health', health))
    
    async def _process_write_queue(self) -> None:
        """Process write queue with batching."""
        batch = []
        last_flush = datetime.utcnow()
        
        while True:
            try:
                # Get item with timeout for periodic flushing
                try:
                    item = await asyncio.wait_for(
                        self.write_queue.get(), 
                        timeout=self.flush_interval
                    )
                    batch.append(item)
                except asyncio.TimeoutError:
                    # Timeout - flush any pending items
                    pass
                
                now = datetime.utcnow()
                should_flush = (
                    len(batch) >= self.batch_size or
                    (batch and (now - last_flush).total_seconds() >= self.flush_interval)
                )
                
                if should_flush and batch:
                    await self._flush_batch(batch)
                    batch.clear()
                    last_flush = now
                
            except asyncio.CancelledError:
                # Flush remaining items before cancellation
                if batch:
                    await self._flush_batch(batch)
                break
            except Exception as e:
                logger.error(f"Error in write queue processor: {e}")
                await asyncio.sleep(1)
    
    async def _flush_batch(self, batch: List[tuple]) -> None:
        """Flush a batch of writes to database.
        
        Args:
            batch: List of (type, object) tuples to write
        """
        if not self.db:
            return
        
        try:
            for item_type, obj in batch:
                if item_type == 'opportunity':
                    await self._insert_opportunity(obj)
                elif item_type == 'tri_opportunity':
                    await self._insert_tri_opportunity(obj)
                elif item_type == 'health':
                    await self._insert_health(obj)
            
            await self.db.commit()
            logger.debug(f"Flushed batch of {len(batch)} items to database")
            
        except Exception as e:
            logger.error(f"Error flushing batch to database: {e}")
            try:
                await self.db.rollback()
            except Exception:
                pass
    
    async def _insert_opportunity(self, opp: Opportunity) -> None:
        """Insert cross-exchange opportunity into database."""
        await self.db.execute("""
            INSERT INTO opportunities (
                type, symbol, buy_exchange, sell_exchange,
                buy_price_before_fees, sell_price_before_fees,
                buy_price_after_fees, sell_price_after_fees,
                spread_bps, notional, buy_depth_levels, sell_depth_levels,
                buy_fees_maker, buy_fees_taker, sell_fees_maker, sell_fees_taker,
                mode, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            opp.type, opp.symbol, opp.buy_exchange, opp.sell_exchange,
            opp.buy_price_before_fees, opp.sell_price_before_fees,
            opp.buy_price_after_fees, opp.sell_price_after_fees,
            opp.spread_bps, opp.notional, opp.buy_depth_levels, opp.sell_depth_levels,
            opp.buy_fees[0], opp.buy_fees[1], opp.sell_fees[0], opp.sell_fees[1],
            opp.mode, opp.timestamp
        ))
    
    async def _insert_tri_opportunity(self, opp: TriOpportunity) -> None:
        """Insert triangular opportunity into database."""
        await self.db.execute("""
            INSERT INTO tri_opportunities (
                type, exchange, base_asset, path_asset1, path_asset2, path_asset3,
                start_amount, end_amount, gain_bps, notional,
                leg1_symbol, leg1_price, leg1_side,
                leg2_symbol, leg2_price, leg2_side,
                leg3_symbol, leg3_price, leg3_side,
                fees_maker, fees_taker, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            opp.type, opp.exchange, opp.base_asset,
            opp.path[0], opp.path[1], opp.path[2],
            opp.start_amount, opp.end_amount, opp.gain_bps, opp.notional,
            opp.leg1_symbol, opp.leg1_price, opp.leg1_side,
            opp.leg2_symbol, opp.leg2_price, opp.leg2_side,
            opp.leg3_symbol, opp.leg3_price, opp.leg3_side,
            opp.fees[0], opp.fees[1], opp.timestamp
        ))
    
    async def _insert_health(self, health: ExchangeHealth) -> None:
        """Insert health snapshot into database."""
        symbols_json = orjson.dumps(health.symbols_subscribed).decode()
        
        await self.db.execute("""
            INSERT INTO exchange_health (
                exchange, ws_connected, rest_ok, last_ws_message, last_rest_call,
                reconnect_count, error_rate, queue_length, coalesced_updates,
                event_loop_lag_ms, symbols_subscribed, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            health.exchange, int(health.ws_connected), int(health.rest_ok),
            health.last_ws_message, health.last_rest_call, health.reconnect_count,
            health.error_rate, health.queue_length, health.coalesced_updates,
            health.event_loop_lag_ms, symbols_json, health.last_updated
        ))
    
    async def get_recent_opportunities(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent opportunities from database.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of opportunity dictionaries
        """
        if not self.db:
            return []
        
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        cursor = await self.db.execute("""
            SELECT * FROM opportunities 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC 
            LIMIT 1000
        """, (cutoff,))
        
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        return [dict(zip(columns, row)) for row in rows]
    
    async def get_recent_tri_opportunities(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent triangular opportunities from database.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of triangular opportunity dictionaries
        """
        if not self.db:
            return []
        
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        cursor = await self.db.execute("""
            SELECT * FROM tri_opportunities 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC 
            LIMIT 1000
        """, (cutoff,))
        
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        return [dict(zip(columns, row)) for row in rows]
    
    async def cleanup_old_data(self, days: int = 7) -> int:
        """Clean up old data from database.
        
        Args:
            days: Number of days to keep
            
        Returns:
            Number of rows deleted
        """
        if not self.db:
            return 0
        
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        # Delete old opportunities
        cursor = await self.db.execute("""
            DELETE FROM opportunities WHERE timestamp < ?
        """, (cutoff,))
        opp_deleted = cursor.rowcount
        
        # Delete old triangular opportunities
        cursor = await self.db.execute("""
            DELETE FROM tri_opportunities WHERE timestamp < ?
        """, (cutoff,))
        tri_deleted = cursor.rowcount
        
        # Delete old health data (keep only 24 hours)
        health_cutoff = datetime.utcnow() - timedelta(hours=24)
        cursor = await self.db.execute("""
            DELETE FROM exchange_health WHERE timestamp < ?
        """, (health_cutoff,))
        health_deleted = cursor.rowcount
        
        await self.db.commit()
        
        total_deleted = opp_deleted + tri_deleted + health_deleted
        logger.info(f"Cleaned up {total_deleted} old records from database")
        
        return total_deleted
    
    def get_stats(self) -> Dict[str, any]:
        """Get database statistics.
        
        Returns:
            Dictionary with database stats
        """
        return {
            'db_path': self.db_path,
            'connected': self.db is not None,
            'write_queue_size': self.write_queue.qsize(),
            'batch_size': self.batch_size,
            'flush_interval_seconds': self.flush_interval
        }

# Global database manager instance
db_manager = DatabaseManager()
