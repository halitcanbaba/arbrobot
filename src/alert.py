"""Telegram alert system with deduplication and throttling."""

import asyncio
import logging
from typing import Dict, Set, Optional
from datetime import datetime, timedelta
from telegram import Bot
from telegram.error import TelegramError

from models import Opportunity, TriOpportunity
from config import config

logger = logging.getLogger(__name__)

class AlertManager:
    """Manages Telegram alerts with deduplication and throttling."""
    
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.chat_id = config.TELEGRAM_CHAT_ID
        self.enabled = False
        
        # Deduplication tracking
        self.sent_alerts: Dict[str, datetime] = {}  # dedupe_key -> timestamp
        self.alert_ttl = timedelta(seconds=30)  # TTL for deduplication
        
        # Rate limiting
        self.last_send_time = datetime.utcnow()
        self.min_send_interval = timedelta(seconds=1)  # Max 1 message per second
        self.send_queue = asyncio.Queue()
        self.send_task: Optional[asyncio.Task] = None
        
        self._initialize_bot()
    
    def _initialize_bot(self) -> None:
        """Initialize Telegram bot if credentials are available."""
        if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
            logger.warning("Telegram credentials not configured - alerts disabled")
            return
        
        try:
            self.bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
            self.enabled = True
            logger.info("Telegram bot initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Telegram bot: {e}")
            self.enabled = False
    
    async def start(self) -> None:
        """Start the alert manager."""
        if not self.enabled:
            logger.info("Alert manager disabled - no Telegram credentials")
            return
        
        # Start the send queue processor
        self.send_task = asyncio.create_task(self._process_send_queue())
        logger.info("Alert manager started")
    
    async def stop(self) -> None:
        """Stop the alert manager."""
        if self.send_task:
            self.send_task.cancel()
            try:
                await self.send_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Alert manager stopped")
    
    async def send_cross_exchange_alert(self, opportunity: Opportunity) -> bool:
        """Send cross-exchange arbitrage alert.
        
        Args:
            opportunity: Cross-exchange opportunity
            
        Returns:
            True if alert was sent (or queued), False if deduplicated
        """
        if not self.enabled:
            return False
        
        # Check deduplication
        if self._is_duplicate(opportunity.dedupe_key):
            return False
        
        # Format alert message
        message = self._format_cross_exchange_message(opportunity)
        
        # Queue the message
        await self.send_queue.put(message)
        
        # Mark as sent for deduplication
        self._mark_sent(opportunity.dedupe_key)
        
        return True
    
    async def send_triangular_alert(self, opportunity: TriOpportunity) -> bool:
        """Send triangular arbitrage alert.
        
        Args:
            opportunity: Triangular opportunity
            
        Returns:
            True if alert was sent (or queued), False if deduplicated
        """
        if not self.enabled:
            return False
        
        # Check deduplication
        if self._is_duplicate(opportunity.dedupe_key):
            return False
        
        # Format alert message
        message = self._format_triangular_message(opportunity)
        
        # Queue the message
        await self.send_queue.put(message)
        
        # Mark as sent for deduplication
        self._mark_sent(opportunity.dedupe_key)
        
        return True
    
    def _format_cross_exchange_message(self, opp: Opportunity) -> str:
        """Format cross-exchange opportunity message.
        
        Args:
            opp: Cross-exchange opportunity
            
        Returns:
            Formatted message string
        """
        timestamp = opp.timestamp.strftime("%H:%M:%S UTC")
        
        message = f"""🔄 [ARB] {opp.symbol} {opp.buy_exchange}→{opp.sell_exchange}
💰 Spread: {opp.spread_bps:.2f} bps | Notional: ${opp.notional:.0f}
📊 Buy@{opp.buy_price_after_fees:.6f} / Sell@{opp.sell_price_after_fees:.6f}
📈 Depth: top{max(opp.buy_depth_levels, opp.sell_depth_levels)} | Fees: taker | Mode: {opp.mode}
🕐 {timestamp}"""
        
        return message
    
    def _format_triangular_message(self, opp: TriOpportunity) -> str:
        """Format triangular opportunity message.
        
        Args:
            opp: Triangular opportunity
            
        Returns:
            Formatted message string
        """
        timestamp = opp.timestamp.strftime("%H:%M:%S UTC")
        path_str = "→".join(opp.path)
        
        message = f"""🔺 [TRI] {opp.exchange} {opp.base_asset} cycle: {path_str}
💎 Gain: {opp.gain_bps:.2f} bps | Start: {opp.start_amount:.0f} {opp.base_asset} → End: {opp.end_amount:.4f} {opp.base_asset}
🔗 Leg1 {opp.leg1_symbol} @ {opp.leg1_price:.6f} | Leg2 {opp.leg2_symbol} @ {opp.leg2_price:.6f} | Leg3 {opp.leg3_symbol} @ {opp.leg3_price:.6f}
📊 Depth: combined | Fees: taker
🕐 {timestamp}"""
        
        return message
    
    def _is_duplicate(self, dedupe_key: str) -> bool:
        """Check if an alert is a duplicate within TTL.
        
        Args:
            dedupe_key: Deduplication key
            
        Returns:
            True if duplicate, False otherwise
        """
        now = datetime.utcnow()
        
        # Clean old entries
        self._clean_old_alerts(now)
        
        # Check if key exists and is within TTL
        if dedupe_key in self.sent_alerts:
            age = now - self.sent_alerts[dedupe_key]
            return age < self.alert_ttl
        
        return False
    
    def _mark_sent(self, dedupe_key: str) -> None:
        """Mark an alert as sent.
        
        Args:
            dedupe_key: Deduplication key
        """
        self.sent_alerts[dedupe_key] = datetime.utcnow()
    
    def _clean_old_alerts(self, now: datetime) -> None:
        """Clean old alert records.
        
        Args:
            now: Current timestamp
        """
        # Remove entries older than TTL
        to_remove = [
            key for key, timestamp in self.sent_alerts.items()
            if now - timestamp > self.alert_ttl
        ]
        
        for key in to_remove:
            del self.sent_alerts[key]
    
    async def _process_send_queue(self) -> None:
        """Process the send queue with rate limiting."""
        while True:
            try:
                # Wait for message
                message = await self.send_queue.get()
                
                # Apply rate limiting
                now = datetime.utcnow()
                time_since_last = now - self.last_send_time
                
                if time_since_last < self.min_send_interval:
                    sleep_time = (self.min_send_interval - time_since_last).total_seconds()
                    await asyncio.sleep(sleep_time)
                
                # Send message
                success = await self._send_message(message)
                
                if success:
                    self.last_send_time = datetime.utcnow()
                    logger.debug("Alert sent successfully")
                else:
                    logger.warning("Failed to send alert")
                
                # Mark task as done
                self.send_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in send queue processor: {e}")
                await asyncio.sleep(1)
    
    async def _send_message(self, message: str) -> bool:
        """Send message via Telegram bot.
        
        Args:
            message: Message to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.bot:
            return False
        
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            return True
            
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
    
    async def send_status_message(self, status: str) -> bool:
        """Send a status message.
        
        Args:
            status: Status message to send
            
        Returns:
            True if sent successfully
        """
        if not self.enabled:
            return False
        
        try:
            await self.send_queue.put(f"🤖 Bot Status: {status}")
            return True
        except Exception as e:
            logger.error(f"Error queueing status message: {e}")
            return False
    
    def get_stats(self) -> Dict[str, any]:
        """Get alert manager statistics.
        
        Returns:
            Dictionary with stats
        """
        now = datetime.utcnow()
        
        return {
            'enabled': self.enabled,
            'queue_size': self.send_queue.qsize(),
            'deduplication_entries': len(self.sent_alerts),
            'last_send_age_seconds': (now - self.last_send_time).total_seconds()
        }

# Global alert manager instance
alert_manager = AlertManager()
