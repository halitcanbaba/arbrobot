#!/usr/bin/env python3
"""Main application entry point for the arbitrage bot."""

import sys
import os
from pathlib import Path

# Add src directory to Python path
src_dir = Path(__file__).parent / "src"
sys.path.insert(0, str(src_dir))

# Now import and run the app directly
if __name__ == "__main__":
    import asyncio
    from app import main
    asyncio.run(main())
