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
import time
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.bot.main import run_bot


if __name__ == "__main__":
    max_start_attempts = 2
    startup_interrupt_retry_window_seconds = 15

    for attempt in range(1, max_start_attempts + 1):
        started_at = time.monotonic()
        try:
            asyncio.run(run_bot())
            break
        except KeyboardInterrupt:
            elapsed = time.monotonic() - started_at
            # Windows terminals can occasionally surface an early interrupt during initial websocket startup.
            if attempt < max_start_attempts and elapsed <= startup_interrupt_retry_window_seconds:
                print("\nStartup interrupted early; retrying once...")
                continue
            print("\nShutdown requested. Exiting.")
            break
        except asyncio.CancelledError:
            elapsed = time.monotonic() - started_at
            if attempt < max_start_attempts and elapsed <= startup_interrupt_retry_window_seconds:
                print("\nStartup cancelled early; retrying once...")
                continue
            raise