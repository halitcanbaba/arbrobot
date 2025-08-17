"""Configuration loader for the arbitrage bot."""

import os
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Application configuration."""
    
    # Telegram
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # Thresholds
    MIN_SPREAD_BPS: float = float(os.getenv("MIN_SPREAD_BPS", "50.0"))
    MIN_TRI_GAIN_BPS: float = float(os.getenv("MIN_TRI_GAIN_BPS", "30.0"))
    MIN_NOTIONAL: float = float(os.getenv("MIN_NOTIONAL", "100.0"))
    
    # Symbol Universe
    SYMBOL_UNIVERSE: List[str] = [
        s.strip() for s in os.getenv(
            "SYMBOL_UNIVERSE", 
            "BTC/USDT,ETH/USDT,SOL/USDT,XRP/USDT,BNB/USDT,ADA/USDT,DOGE/USDT,TON/USDT,AVAX/USDT,LINK/USDT"
        ).split(",") if s.strip()
    ]
    
    # Triangular Arbitrage
    TRI_BASES: List[str] = [
        s.strip() for s in os.getenv("TRI_BASES", "USDT,USDC,BTC").split(",") if s.strip()
    ]
    
    TRI_EXCLUDE_QUOTES: List[str] = [
        s.strip() for s in os.getenv("TRI_EXCLUDE_QUOTES", "").split(",") if s.strip()
    ]
    
    # Exchange Filters
    INCLUDE_EXCHANGES: List[str] = [
        s.strip() for s in os.getenv(
            "INCLUDE_EXCHANGES", 
            "binance,okx,bybit,coinbase,kraken,kucoin,gateio,huobi,btcturk,mexc,cointr"
        ).split(",") if s.strip()
    ]
    EXCLUDE_EXCHANGES: List[str] = [
        s.strip() for s in os.getenv("EXCLUDE_EXCHANGES", "").split(",") if s.strip()
    ]
    
    # Performance
    DEPTH_LEVELS: int = int(os.getenv("DEPTH_LEVELS", "10"))
    COALESCE_MS: int = int(os.getenv("COALESCE_MS", "75"))
    TRI_SCAN_MS: int = int(os.getenv("TRI_SCAN_MS", "150"))
    MAX_CONCURRENT_EXCHANGES: int = int(os.getenv("MAX_CONCURRENT_EXCHANGES", "20"))
    
    # Logging & DB
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    DB_PATH: str = os.getenv("DB_PATH", "./arbitrage.db")
    
    # Health Monitoring
    HEALTH_CHECK_INTERVAL_S: int = int(os.getenv("HEALTH_CHECK_INTERVAL_S", "30"))
    MAX_RECONNECT_ATTEMPTS: int = int(os.getenv("MAX_RECONNECT_ATTEMPTS", "5"))
    BACKOFF_MAX_S: int = int(os.getenv("BACKOFF_MAX_S", "60"))
    
    @classmethod
    def get_fee_overrides(cls) -> Dict[str, Dict[str, float]]:
        """Extract fee overrides from environment variables.
        
        Returns:
            Dict mapping exchange names to fee dictionaries.
            Example: {"binance": {"taker": 0.0005, "maker": 0.0008}}
        """
        overrides: Dict[str, Dict[str, float]] = {}
        
        for key, value in os.environ.items():
            if "_TAKER_FEE" in key or "_MAKER_FEE" in key:
                try:
                    parts = key.split("_")
                    if len(parts) >= 3:
                        exchange = parts[0].lower()
                        fee_type = parts[1].lower()  # "taker" or "maker"
                        
                        if exchange not in overrides:
                            overrides[exchange] = {}
                        
                        overrides[exchange][fee_type] = float(value)
                except (ValueError, IndexError):
                    continue
        
        return overrides
    
    @classmethod
    def validate(cls) -> bool:
        """Validate required configuration."""
        if not cls.TELEGRAM_BOT_TOKEN:
            print("Warning: TELEGRAM_BOT_TOKEN not set - alerts disabled")
            return False
        
        if not cls.TELEGRAM_CHAT_ID:
            print("Warning: TELEGRAM_CHAT_ID not set - alerts disabled")
            return False
            
        return True

# Global config instance
config = Config()
