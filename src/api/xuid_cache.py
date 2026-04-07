"""
XUID Cache Management for Halo Infinite API.

Provides persistent caching of XUID to gamertag mappings to minimize API
calls for gamertag resolution.

Matching policy:
- Lookup comparisons can normalize whitespace and case for tolerant matching.
- Stored cache values remain the canonical display gamertag value returned by
    Xbox APIs to avoid duplicate stripped/non-stripped entries per XUID.
"""

import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.api.utils import safe_read_json, safe_write_json
from src.config import XUID_CACHE_FILE


XUID_HISTORY_FILE = Path(XUID_CACHE_FILE).with_name("xuid_gamertag_history.json")


def _normalize_gamertag_for_lookup(gamertag: Any) -> str:
    """Normalize gamertag text for strict cache comparisons."""
    if not isinstance(gamertag, str):
        return ""

    # Collapse repeated whitespace and compare case-insensitively.
    return " ".join(gamertag.split()).casefold()


def _normalize_gamertag_alias_key(gamertag: Any) -> str:
    """Normalize gamertag text for optional alias matching without spaces."""
    normalized = _normalize_gamertag_for_lookup(gamertag)
    return normalized.replace(" ", "")


def _normalize_gamertag_value(value: Any) -> Optional[str]:
    """Extract a gamertag string from legacy or metadata-rich cache values."""
    if isinstance(value, str):
        gamertag = value.strip()
        return gamertag or None

    if isinstance(value, dict):
        # Accept both future-facing and legacy sidecar-style field names.
        candidate = value.get("gamertag") or value.get("current_gamertag")
        if isinstance(candidate, str):
            candidate = candidate.strip()
            return candidate or None

    return None


def _flatten_cache(raw_cache: Dict[str, Any]) -> Dict[str, str]:
    """Return a legacy Dict[str, str] view from mixed cache payloads."""
    flattened: Dict[str, str] = {}
    for xuid, value in raw_cache.items():
        if not isinstance(xuid, str):
            continue

        gamertag = _normalize_gamertag_value(value)
        if gamertag:
            flattened[str(xuid)] = gamertag
    return flattened


def _load_history() -> Dict[str, Dict[str, Any]]:
    """Load sidecar XUID history metadata."""
    raw = safe_read_json(XUID_HISTORY_FILE, default={})
    if not isinstance(raw, dict):
        return {}

    cleaned: Dict[str, Dict[str, Any]] = {}
    for xuid, entry in raw.items():
        if not isinstance(xuid, str) or not isinstance(entry, dict):
            continue

        current = _normalize_gamertag_value(entry)
        if not current:
            continue

        previous = entry.get("previous_gamertags", [])
        if not isinstance(previous, list):
            previous = []

        cleaned_previous: List[str] = []
        for item in previous:
            if isinstance(item, str):
                item = item.strip()
                if item and item != current and item not in cleaned_previous:
                    cleaned_previous.append(item)

        cleaned[xuid] = {
            "current_gamertag": current,
            "previous_gamertags": cleaned_previous,
            "updated_at": entry.get("updated_at") or datetime.now(timezone.utc).isoformat(),
        }

    return cleaned


def _update_history(previous_cache: Dict[str, str], new_cache: Dict[str, str]) -> None:
    """Track gamertag changes per XUID in a sidecar file without changing primary cache format."""
    history = _load_history()
    timestamp = datetime.now(timezone.utc).isoformat()
    changed = False

    for xuid, new_gamertag in new_cache.items():
        if not isinstance(xuid, str) or not isinstance(new_gamertag, str):
            continue

        new_gamertag = new_gamertag.strip()
        if not new_gamertag:
            continue

        old_gamertag = previous_cache.get(xuid)
        existing_entry = history.get(xuid)

        if existing_entry is None:
            history[xuid] = {
                "current_gamertag": new_gamertag,
                "previous_gamertags": [],
                "updated_at": timestamp,
            }
            changed = True
            continue

        current = _normalize_gamertag_value(existing_entry) or ""
        previous = existing_entry.get("previous_gamertags", [])
        if not isinstance(previous, list):
            previous = []

        if old_gamertag and old_gamertag != new_gamertag and old_gamertag != current and old_gamertag not in previous:
            previous.append(old_gamertag)
            changed = True

        if current and current != new_gamertag and current not in previous:
            previous.append(current)
            changed = True

        if current != new_gamertag:
            changed = True

        history[xuid] = {
            "current_gamertag": new_gamertag,
            "previous_gamertags": previous,
            "updated_at": timestamp,
        }

    if changed:
        safe_write_json(XUID_HISTORY_FILE, history)


def load_xuid_cache_full() -> Dict[str, Dict[str, Any]]:
    """
    Load cache metadata with current and previous gamertags per XUID.

    This keeps the legacy cache file untouched and merges in sidecar history
    so callers can opt into richer metadata without breaking existing code.
    """
    flat_cache = load_xuid_cache()
    history = _load_history()
    timestamp = datetime.now(timezone.utc).isoformat()

    full: Dict[str, Dict[str, Any]] = {}
    for xuid, gamertag in flat_cache.items():
        entry = history.get(xuid, {})
        previous = entry.get("previous_gamertags", []) if isinstance(entry, dict) else []
        if not isinstance(previous, list):
            previous = []

        full[xuid] = {
            "gamertag": gamertag,
            "updated_at": (entry.get("updated_at") if isinstance(entry, dict) else None) or timestamp,
            "previous_gamertags": [p for p in previous if isinstance(p, str) and p != gamertag],
        }

    return full


def get_gamertag_history(xuid: str) -> List[str]:
    """Return known historical gamertags for a specific XUID."""
    full = load_xuid_cache_full()
    entry = full.get(str(xuid), {})
    previous = entry.get("previous_gamertags", []) if isinstance(entry, dict) else []
    return previous if isinstance(previous, list) else []


def load_xuid_cache() -> Dict[str, str]:
    """
    Load the persistent XUID -> Gamertag cache.
    
    Returns:
        Dictionary mapping XUIDs to gamertags
    """
    raw_cache = safe_read_json(XUID_CACHE_FILE, default={})
    if not isinstance(raw_cache, dict):
        return {}
    return _flatten_cache(raw_cache)


def save_xuid_cache(cache: Dict[str, str]) -> None:
    """
    Save the XUID -> Gamertag cache.
    
    Args:
        cache: Dictionary mapping XUIDs to gamertags
    """
    try:
        previous_cache = load_xuid_cache()
        flattened_cache = _flatten_cache(cache)
        safe_write_json(XUID_CACHE_FILE, flattened_cache)
        _update_history(previous_cache, flattened_cache)
    except Exception as e:
        print(f"Failed to save XUID cache: {e}")
        traceback.print_exc()
