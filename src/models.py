"""Pydantic models for the arbitrage bot."""

from __future__ import annotations
from typing import List, Optional, Dict, Any, Literal, Tuple
from datetime import datetime
from pydantic import BaseModel, Field
import numpy as np

class MarketMeta(BaseModel):
    """Exchange market metadata."""
    symbol: str
    base: str
    quote: str
    active: bool
    price_precision: int
    amount_precision: int
    min_amount: Optional[float] = None
    min_notional: Optional[float] = None
    exchange: str

class DepthLevel(BaseModel):
    """Order book depth level."""
    price: float
    amount: float
    
    def __hash__(self) -> int:
        return hash((self.price, self.amount))

class OrderBook(BaseModel):
    """Order book snapshot."""
    symbol: str
    exchange: str
    bids: List[DepthLevel]
    asks: List[DepthLevel]
    timestamp: datetime
    nonce: Optional[int] = None
    
    class Config:
        arbitrary_types_allowed = True

class FeesPublic(BaseModel):
    """Public fee information for an exchange."""
    maker: float = Field(description="Maker fee rate (0.001 = 0.1%)")
    taker: float = Field(description="Taker fee rate (0.001 = 0.1%)")
    source: Literal["public", "default", "env"] = Field(description="Source of fee data")
    exchange: str = Field(description="Exchange name")
    symbol_specific: Dict[str, Tuple[float, float]] = Field(
        default_factory=dict, 
        description="Symbol-specific (maker, taker) fees"
    )
    
    def get_fees(self, symbol: Optional[str] = None) -> Tuple[float, float]:
        """Get (maker, taker) fees for a symbol or exchange default.
        
        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            
        Returns:
            Tuple of (maker_fee, taker_fee)
        """
        if symbol and symbol in self.symbol_specific:
            return self.symbol_specific[symbol]
        return (self.maker, self.taker)

class Opportunity(BaseModel):
    """Cross-exchange arbitrage opportunity."""
    type: Literal["CROSS"] = "CROSS"
    symbol: str
    buy_exchange: str
    sell_exchange: str
    buy_price_before_fees: float
    sell_price_before_fees: float
    buy_price_after_fees: float
    sell_price_after_fees: float
    spread_bps: float
    notional: float
    buy_depth_levels: int
    sell_depth_levels: int
    buy_fees: Tuple[float, float]  # (maker, taker)
    sell_fees: Tuple[float, float]  # (maker, taker)
    timestamp: datetime
    mode: Literal["ws", "rest"]
    
    @property
    def dedupe_key(self) -> str:
        """Generate deduplication key."""
        return f"CROSS_{self.buy_exchange}_{self.sell_exchange}_{self.symbol}_{int(self.notional)}"

class TriOpportunity(BaseModel):
    """Triangular arbitrage opportunity."""
    type: Literal["TRI"] = "TRI"
    exchange: str
    base_asset: str
    path: Tuple[str, str, str]  # (asset1, asset2, asset3) where asset3 == base_asset
    start_amount: float
    end_amount: float
    gain_bps: float
    notional: float
    leg1_symbol: str
    leg1_price: float
    leg1_side: Literal["buy", "sell"]
    leg2_symbol: str
    leg2_price: float
    leg2_side: Literal["buy", "sell"]
    leg3_symbol: str
    leg3_price: float
    leg3_side: Literal["buy", "sell"]
    fees: Tuple[float, float]  # (maker, taker) for this exchange
    timestamp: datetime
    
    @property
    def dedupe_key(self) -> str:
        """Generate deduplication key."""
        path_str = "_".join(self.path)
        return f"TRI_{self.exchange}_{path_str}_{int(self.notional)}"

class ExchangeHealth(BaseModel):
    """Exchange connection health metrics."""
    exchange: str
    ws_connected: bool = False
    rest_ok: bool = False
    last_ws_message: Optional[datetime] = None
    last_rest_call: Optional[datetime] = None
    reconnect_count: int = 0
    error_rate: float = 0.0  # Errors per minute
    queue_length: int = 0
    coalesced_updates: int = 0
    event_loop_lag_ms: float = 0.0
    symbols_subscribed: List[str] = Field(default_factory=list)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    
    def is_healthy(self) -> bool:
        """Check if exchange connection is healthy."""
        now = datetime.utcnow()
        
        # Check for recent activity (within last 60 seconds)
        if self.last_ws_message:
            ws_recent = (now - self.last_ws_message).total_seconds() < 60
        else:
            ws_recent = False
            
        if self.last_rest_call:
            rest_recent = (now - self.last_rest_call).total_seconds() < 60
        else:
            rest_recent = False
        
        # Healthy if either WS is connected with recent messages OR REST is working
        return (self.ws_connected and ws_recent) or (self.rest_ok and rest_recent)

class VWAPResult(BaseModel):
    """VWAP calculation result."""
    vwap_price: float
    total_volume: float
    levels_used: int
    fully_filled: bool
    
    class Config:
        arbitrary_types_allowed = True
