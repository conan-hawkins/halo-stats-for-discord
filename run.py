#!/usr/bin/env python3
"""
Halo Infinite Discord Stats Bot
================================
Made by Conan Hawkins
Created: 12/02/2025

Main entry point for the Discord bot.

Usage:
    python run.py
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.bot.main import run_bot


if __name__ == "__main__":
    asyncio.run(run_bot())