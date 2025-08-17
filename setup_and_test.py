#!/usr/bin/env python3
"""Quick setup and test script for the arbitrage bot."""

import sys
import subprocess
import asyncio
from pathlib import Path

def install_dependencies():
    """Install required dependencies."""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install dependencies: {e}")
        return False

def run_tests():
    """Run the test suite."""
    print("\nRunning tests...")
    try:
        subprocess.check_call([sys.executable, "-m", "pytest", "tests/", "-v"])
        print("✓ Tests passed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Tests failed: {e}")
        return False

def check_types():
    """Run type checking."""
    print("\nRunning type checks...")
    try:
        subprocess.check_call([sys.executable, "-m", "mypy", "src/"])
        print("✓ Type checking passed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Type checking failed: {e}")
        return False

def check_style():
    """Run style checks."""
    print("\nRunning style checks...")
    try:
        subprocess.check_call([sys.executable, "-m", "ruff", "check", "src/"])
        subprocess.check_call([sys.executable, "-m", "black", "--check", "src/"])
        print("✓ Style checks passed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Style checks failed: {e}")
        return False

async def test_exchange_connections():
    """Test connections to exchanges."""
    print("\nTesting exchange connections...")
    try:
        # Import after dependencies are installed
        from src.registry import registry
        
        exchanges = await registry.discover_exchanges()
        
        if exchanges:
            print(f"✓ Found {len(exchanges)} working exchanges: {exchanges}")
            
            # Test getting markets for first exchange
            for exchange_name in exchanges[:1]:
                markets = registry.get_markets(exchange_name)
                print(f"✓ {exchange_name}: {len(markets)} markets available")
                
                # Show some sample symbols
                btc_symbols = [s for s in markets.keys() if 'BTC' in s and 'USDT' in s][:3]
                if btc_symbols:
                    print(f"  Sample BTC symbols: {btc_symbols}")
                
            await registry.cleanup()
            return True
        else:
            print("✗ No working exchanges found")
            return False
            
    except Exception as e:
        print(f"✗ Exchange connection test failed: {e}")
        return False

def check_config():
    """Check configuration files."""
    print("\nChecking configuration...")
    
    env_example = Path(".env.example")
    env_file = Path(".env")
    
    if not env_example.exists():
        print("✗ .env.example file not found")
        return False
    
    if not env_file.exists():
        print("⚠ .env file not found")
        print("  Copy .env.example to .env and configure your settings:")
        print("  cp .env.example .env")
        return False
    
    print("✓ Configuration files found")
    return True

def main():
    """Main setup and test function."""
    print("Crypto Arbitrage Bot - Setup and Test")
    print("=" * 40)
    
    # Check if we're in the right directory
    if not Path("src/app.py").exists():
        print("✗ Please run this script from the project root directory")
        return False
    
    # Install dependencies
    if not install_dependencies():
        return False
    
    # Check configuration
    config_ok = check_config()
    
    # Run tests
    if not run_tests():
        return False
    
    # Run type checking
    if not check_types():
        print("⚠ Type checking failed (but continuing...)")
    
    # Run style checking
    if not check_style():
        print("⚠ Style checking failed (but continuing...)")
    
    # Test exchange connections
    try:
        import asyncio
        if not asyncio.run(test_exchange_connections()):
            print("⚠ Exchange connection test failed (but bot may still work)")
    except Exception as e:
        print(f"⚠ Could not test exchange connections: {e}")
    
    print("\n" + "=" * 40)
    if config_ok:
        print("✓ Setup complete! You can now run the bot:")
        print("  python src/app.py")
    else:
        print("⚠ Setup mostly complete, but please configure .env file first:")
        print("  1. cp .env.example .env")
        print("  2. Edit .env with your Telegram bot token and chat ID")
        print("  3. python src/app.py")
    
    print("\nFor more information, see README.md")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
