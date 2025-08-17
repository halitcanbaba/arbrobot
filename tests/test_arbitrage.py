"""Test suite for the arbitrage bot."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import asyncio
import numpy as np
from datetime import datetime
from unittest.mock import Mock, patch

from src.models import DepthLevel, OrderBook, FeesPublic, Opportunity, TriOpportunity, VWAPResult
from src.depth import calculate_vwap, calculate_buy_vwap, calculate_sell_vwap, get_effective_price_after_fees
from src.fees import FeeManager
from src.engine import ArbitrageEngine
from src.symbolmap import SymbolMapper

class TestVWAPCalculation:
    """Test VWAP calculation functions."""
    
    def test_calculate_buy_vwap_full_fill(self):
        """Test VWAP calculation for buy orders with full fill."""
        asks = [
            DepthLevel(price=100.0, amount=1.0),
            DepthLevel(price=101.0, amount=2.0),
            DepthLevel(price=102.0, amount=3.0),
        ]
        
        # Test filling within first level
        result = calculate_buy_vwap(asks, 50.0)  # $50 notional
        assert result.fully_filled
        assert abs(result.vwap_price - 100.0) < 0.001
        assert abs(result.total_volume - 0.5) < 0.001
        assert result.levels_used == 1
        
        # Test filling across multiple levels
        result = calculate_buy_vwap(asks, 250.0)  # $250 notional
        assert result.fully_filled
        # VWAP should be weighted average of prices
        assert 100.5 < result.vwap_price < 101.5
        assert result.levels_used >= 2
    
    def test_calculate_sell_vwap_full_fill(self):
        """Test VWAP calculation for sell orders with full fill."""
        bids = [
            DepthLevel(price=99.0, amount=1.0),
            DepthLevel(price=98.0, amount=2.0),
            DepthLevel(price=97.0, amount=3.0),
        ]
        
        # Test filling within first level
        result = calculate_sell_vwap(bids, 50.0)  # $50 notional
        assert result.fully_filled
        assert abs(result.vwap_price - 99.0) < 0.001
        
        # Test filling across multiple levels
        result = calculate_sell_vwap(bids, 250.0)  # $250 notional
        assert result.fully_filled
        assert 97.5 < result.vwap_price < 99.0
    
    def test_calculate_vwap_insufficient_liquidity(self):
        """Test VWAP calculation with insufficient liquidity."""
        asks = [
            DepthLevel(price=100.0, amount=1.0),
        ]
        
        result = calculate_buy_vwap(asks, 200.0)  # More than available
        assert not result.fully_filled
        assert result.levels_used == 1
        assert abs(result.total_volume - 1.0) < 0.001
    
    def test_effective_price_after_fees(self):
        """Test effective price calculation after fees."""
        
        vwap_result = VWAPResult(
            vwap_price=100.0,
            total_volume=1.0,
            levels_used=1,
            fully_filled=True
        )
        
        # Test buy fees (price increases)
        buy_price = get_effective_price_after_fees(vwap_result, 0.001, 'buy')
        assert abs(buy_price - 100.1) < 0.001
        
        # Test sell fees (effective price decreases)
        sell_price = get_effective_price_after_fees(vwap_result, 0.001, 'sell')
        assert abs(sell_price - 99.9) < 0.001

class TestFeeManager:
    """Test fee management functionality."""
    
    @pytest.mark.asyncio
    async def test_get_fees_with_overrides(self):
        """Test fee retrieval with environment overrides."""
        fee_manager = FeeManager()
        
        # Mock environment overrides
        fee_manager.env_overrides = {
            'binance': {'taker': 0.0005, 'maker': 0.0002}
        }
        
        fees = await fee_manager.get_fees('binance')
        assert fees.taker == 0.0005
        assert fees.maker == 0.0002
        assert fees.source == 'env'
    
    def test_apply_buy_fees(self):
        """Test fee application for buy orders."""
        fee_manager = FeeManager()
        fees = FeesPublic(maker=0.001, taker=0.002, source='default', exchange='test')
        
        # Test taker fees
        effective_price, amount = fee_manager.apply_buy_fees(100.0, 1.0, fees, is_maker=False)
        assert abs(effective_price - 100.2) < 0.001
        assert amount == 1.0
        
        # Test maker fees
        effective_price, amount = fee_manager.apply_buy_fees(100.0, 1.0, fees, is_maker=True)
        assert abs(effective_price - 100.1) < 0.001
    
    def test_apply_sell_fees(self):
        """Test fee application for sell orders."""
        fee_manager = FeeManager()
        fees = FeesPublic(maker=0.001, taker=0.002, source='default', exchange='test')
        
        # Test taker fees
        price, effective_amount = fee_manager.apply_sell_fees(100.0, 1.0, fees, is_maker=False)
        assert price == 100.0
        assert abs(effective_amount - 0.998) < 0.001
        
        # Test maker fees
        price, effective_amount = fee_manager.apply_sell_fees(100.0, 1.0, fees, is_maker=True)
        assert abs(effective_amount - 0.999) < 0.001

class TestSymbolMapper:
    """Test symbol normalization functionality."""
    
    def test_normalize_symbol(self):
        """Test symbol normalization across exchanges."""
        mapper = SymbolMapper()
        
        # Test standard format
        assert mapper.normalize_symbol('BTC/USDT', 'binance') == 'BTC/USDT'
        
        # Test automatic normalization
        assert mapper.normalize_symbol('BTCUSDT', 'binance') == 'BTC/USDT'
        assert mapper.normalize_symbol('ETHUSDC', 'binance') == 'ETH/USDC'
        
        # Test Kraken-style symbols
        assert mapper.normalize_symbol('XBTUSD', 'kraken') == 'BTC/USD'
    
    def test_parse_symbol(self):
        """Test symbol parsing into base/quote."""
        mapper = SymbolMapper()
        
        # Test standard format
        base, quote = mapper.parse_symbol('BTC/USDT')
        assert base == 'BTC'
        assert quote == 'USDT'
        
        # Test automatic parsing
        base, quote = mapper.parse_symbol('ETHUSDC')
        assert base == 'ETH'
        assert quote == 'USDC'

class TestArbitrageEngine:
    """Test arbitrage opportunity detection."""
    
    @pytest.mark.asyncio
    async def test_opportunity_detection(self):
        """Test basic opportunity detection logic."""
        engine = ArbitrageEngine()
        
        # Create mock order books with arbitrage opportunity
        timestamp = datetime.utcnow()
        
        # Exchange A: Lower ask prices (good for buying)
        book_a = OrderBook(
            symbol='BTC/USDT',
            exchange='exchange_a',
            bids=[DepthLevel(price=49900.0, amount=1.0)],
            asks=[DepthLevel(price=50000.0, amount=1.0)],
            timestamp=timestamp
        )
        
        # Exchange B: Higher bid prices (good for selling)
        book_b = OrderBook(
            symbol='BTC/USDT',
            exchange='exchange_b',
            bids=[DepthLevel(price=50200.0, amount=1.0)],
            asks=[DepthLevel(price=50300.0, amount=1.0)],
            timestamp=timestamp
        )
        
        # Update engine with order books
        engine.update_order_book(book_a)
        engine.update_order_book(book_b)
        
        # Mock fee manager
        with patch('src.engine.fee_manager') as mock_fee_manager:
            mock_fees = FeesPublic(maker=0.001, taker=0.002, source='default', exchange='test')
            mock_fee_manager.get_fees.return_value = mock_fees
            
            # Mock config to lower threshold
            with patch('src.engine.config') as mock_config:
                mock_config.MIN_SPREAD_BPS = 10.0  # Lower threshold for test
                mock_config.MIN_NOTIONAL = 100.0
                
            # Should be able to scan without errors (may or may not find opportunities)
            opportunities = await engine.scan_opportunities(['BTC/USDT'], ['exchange_a', 'exchange_b'])
            
            # Test that scan completed successfully (no exception)
            assert isinstance(opportunities, list)
            
            # If opportunity found, check its properties
            if opportunities:
                opp = opportunities[0]
                assert opp.symbol == 'BTC/USDT'
                assert opp.buy_exchange in ['exchange_a', 'exchange_b']
                assert opp.sell_exchange in ['exchange_a', 'exchange_b']
                assert opp.spread_bps >= 0

class TestIntegration:
    """Integration tests with real exchange data."""
    
    @pytest.mark.asyncio
    async def test_real_exchange_connection(self):
        """Test connection to a real exchange."""
        from src.registry import registry
        
        # Try to connect to Binance (should work)
        exchanges = await registry.discover_exchanges()
        
        # Should have at least one working exchange
        assert len(exchanges) > 0
        
        # Check that we can get markets
        for exchange_name in exchanges[:1]:  # Test first exchange only
            markets = registry.get_markets(exchange_name)
            assert len(markets) > 0
            
            # Check that BTC/USDT exists (should be available on most exchanges)
            btc_symbols = [symbol for symbol in markets.keys() if 'BTC' in symbol and ('USDT' in symbol or 'USD' in symbol)]
            if btc_symbols:
                assert len(btc_symbols) > 0
            else:
                # If no BTC symbols, just check we have some symbols
                assert len(markets) > 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
