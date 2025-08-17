# Crypto Arbitrage Alert Bot

A high-performance crypto arbitrage alert bot that monitors cross-exchange and triangular arbitrage opportunities using only public APIs. Supports 7+ exchanges including CoinTR.

## ✨ Features

- **Cross-exchange arbitrage**: Monitors spot-to-spot price differences across exchanges with depth-aware VWAP calculations
- **Triangular arbitrage**: Detects 3-leg cycles within individual exchanges with configurable quote asset exclusions
- **Multi-exchange support**: Binance, Bybit, OKX, KuCoin, MEXC, Huobi, and CoinTR
- **CoinTR integration**: Native Turkish exchange support with WebSocket connectivity
- **Telegram alerts**: Real-time notifications when profitable opportunities are found
- **No credentials required**: Uses only public APIs - no exchange API keys needed
- **Performance optimized**: Async/await with uvloop, WebSocket connections where available
- **Production ready**: Comprehensive error handling, health monitoring, and data persistence

## 🚀 Quick Start

### Windows

1. **Install Python 3.11+** from [python.org](https://python.org)

2. **Clone and setup**:
   ```cmd
   git clone https://github.com/halitcanbaba/arbrobot.git
   cd arbrobot
   windows_setup.bat
   ```

3. **Configure**:
   ```cmd
   copy .env.example .env
   # Edit .env with your Telegram bot token and preferences
   ```

4. **Run the bot**:
   ```cmd
   run_windows.bat
   # or manually:
   python run_bot.py
   ```

### Linux/macOS

1. **Install Python 3.11+**

2. **Clone and setup**:
   ```bash
   git clone https://github.com/halitcanbaba/arbrobot.git
   cd arbrobot
   ./setup_and_test.py  # Automated setup
   ```

3. **Configure** (optional):
   ```bash
   cp .env.example .env
   # Edit .env with your Telegram bot token and preferences
   ```

4. **Run the bot**:
   ```bash
   python src/app.py
   # or
   make run
   ```

## ⚙️ Configuration

See `.env.example` for all available configuration options:

### Core Settings
- **MIN_SPREAD_BPS**: Minimum cross-exchange spread threshold (default: 25 bps)
- **MIN_TRI_GAIN_BPS**: Minimum triangular arbitrage gain (default: 15 bps)
- **MIN_NOTIONAL**: Minimum trade size for calculations (default: $100)

### Symbol Universe
- **SYMBOL_UNIVERSE**: Trading pairs to monitor
- **TRI_BASES**: Base assets for triangular arbitrage
- **TRI_EXCLUDE_QUOTES**: Quote assets to exclude from triangular arbitrage (e.g., BTC,ETH for stability)  

### Exchange Controls
- **INCLUDE_EXCHANGES**: Comma-separated list of exchanges to include (binance,okx,bybit,mexc,kucoin,cointr)
- **EXCLUDE_EXCHANGES**: Comma-separated list of exchanges to exclude
- **DEPTH_LEVELS**: Order book depth to analyze

### Performance Tuning  
- **COALESCE_MS**: Order book update batching (default: 100ms)
- **CROSS_SCAN_MS**: Cross-exchange scan interval (default: 1000ms)
- **TRI_SCAN_MS**: Triangular scan interval (default: 2000ms)

### Telegram Alerts
- **TELEGRAM_BOT_TOKEN**: Your bot token from @BotFather
- **TELEGRAM_CHAT_ID**: Your chat/channel ID for alerts

### Fee Overrides (optional)
```env
FEE_OVERRIDE_BINANCE_TAKER=0.0005
FEE_OVERRIDE_KRAKEN_MAKER=0.0015
```

## Architecture

The bot uses a modular architecture with the following components:

- `config.py`: Environment configuration loader
- `models.py`: Pydantic data models
- `registry.py`: Exchange discovery and market loading
- `symbolmap.py`: Symbol normalization across exchanges
- `fees.py`: Fee calculation and management
- `connectors/`: Exchange connection implementations
- `depth.py`: VWAP calculation for order book depth
- `engine.py`: Cross-exchange arbitrage detection
- `tri_engine.py`: Triangular arbitrage detection
- `alert.py`: Telegram notification system
- `db.py`: SQLite data persistence
- `health.py`: System health monitoring
- `app.py`: Main orchestrator

## Performance Guidelines

- Uses uvloop for enhanced async performance
- Prefers WebSocket connections over REST polling
- Implements adaptive backpressure and coalescing
- Uses numpy for vectorized VWAP calculations
- Batches database writes and logs

## Testing

Run tests with:
```bash
python -m pytest tests/
mypy src/
ruff check src/
black --check src/
```

## Disclaimer

This is a monitoring tool only - no actual trading is performed. Use at your own risk and ensure compliance with exchange terms of service.
