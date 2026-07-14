"""
Utility Functions for Halo Infinite API

Provides thread-safe JSON file operations and token validation utilities.
"""

import json
import os
import asyncio
import time
from typing import Dict, Optional

from src.config import TOKEN_CACHE_FILE, TOKEN_SWAP_MARKER_FILE

# =============================================================================
# FILE LOCKING SETUP
# =============================================================================

try:
    import portalocker
    HAS_FILE_LOCKING = True
except ImportError:
    HAS_FILE_LOCKING = False
    print("Warning: portalocker not installed. Installing for file locking...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'portalocker'])
    import portalocker
    HAS_FILE_LOCKING = True


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


_TOKEN_SWAP_LOCK: Optional[asyncio.Lock] = None


def get_token_swap_lock() -> asyncio.Lock:
    """Return the shared lock that serializes token-cache swap refreshes."""
    global _TOKEN_SWAP_LOCK
    if _TOKEN_SWAP_LOCK is None:
        _TOKEN_SWAP_LOCK = asyncio.Lock()
    return _TOKEN_SWAP_LOCK


def safe_read_json(filepath: str, default=None) -> Optional[Dict]:
    """
    Thread-safe JSON file read with file locking.
    
    Uses shared lock to allow concurrent reads while preventing
    writes during read operations.
    
    Args:
        filepath: Path to the JSON file
        default: Default value to return if file doesn't exist or error occurs
    
    Returns:
        Parsed JSON data or default value
    """
    filepath = str(filepath)  # Convert Path to string if needed
    if not os.path.exists(filepath):
        return default
    
    try:
        with open(filepath, 'r') as f:
            portalocker.lock(f, portalocker.LOCK_SH)  # Shared lock for reading
            try:
                data = json.load(f)
            finally:
                portalocker.unlock(f)
            return data
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return default


def is_token_valid(token_info: Optional[Dict]) -> bool:
    """
    Check if a token is valid (not expired).
    
    Args:
        token_info: Token dictionary containing 'expires_at' timestamp
    
    Returns:
        True if token exists and hasn't expired, False otherwise
    """
    if not token_info:
        return False
    expires_at = token_info.get("expires_at", 0)
    return expires_at > time.time()


def safe_write_json(filepath: str, data: Dict, indent: int = 2) -> None:
    """
    Thread-safe JSON file write with file locking and atomic operation.
    
    Writes to a temporary file first, then atomically replaces the
    target file to prevent corruption from partial writes.
    Includes retry logic for Windows file locking issues.
    
    Args:
        filepath: Path to the JSON file
        data: Data to write
        indent: JSON indentation level
    """
    filepath = str(filepath)  # Convert Path to string if needed
    temp_filepath = filepath + '.tmp'
    max_retries = 5
    
    try:
        with open(temp_filepath, 'w') as f:
            portalocker.lock(f, portalocker.LOCK_EX)  # Exclusive lock for writing
            try:
                json.dump(data, f, indent=indent)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk
            finally:
                portalocker.unlock(f)
        
        # Atomic rename with retry for Windows file locking issues
        for attempt in range(max_retries):
            try:
                if os.path.exists(filepath):
                    os.replace(temp_filepath, filepath)
                else:
                    os.rename(temp_filepath, filepath)
                return  # Success
            except PermissionError as e:
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))  # 0.1s, 0.2s, 0.3s, 0.4s
                    continue
                else:
                    raise  # Re-raise on final attempt
                    
    except Exception as e:
        print(f"Error writing {filepath}: {e}")
        # Clean up temp file if it exists
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except:
                pass


def write_token_swap_marker(backup_cache: Dict, target_cache_file: str) -> None:
    """Persist a swap marker so interrupted refreshes can be recovered on startup."""
    marker = {
        "source_cache_file": str(TOKEN_CACHE_FILE),
        "target_cache_file": str(target_cache_file),
        "backup_cache": backup_cache,
        "created_at": time.time(),
    }
    safe_write_json(TOKEN_SWAP_MARKER_FILE, marker)


def clear_token_swap_marker() -> None:
    """Remove any persisted refresh swap marker."""
    try:
        os.remove(str(TOKEN_SWAP_MARKER_FILE))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Error clearing token swap marker: {e}")


def recover_token_swap_marker() -> bool:
    """Restore Account 1 cache if a previous refresh died mid-swap."""
    marker = safe_read_json(TOKEN_SWAP_MARKER_FILE, default={})
    if not marker:
        return False

    backup_cache = marker.get("backup_cache")
    if not isinstance(backup_cache, dict) or not backup_cache:
        clear_token_swap_marker()
        return False

    safe_write_json(TOKEN_CACHE_FILE, backup_cache)
    clear_token_swap_marker()
    print("Recovered token cache from interrupted refresh swap")
    return True
