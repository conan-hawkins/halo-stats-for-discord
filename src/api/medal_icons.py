"""Fetch and cache real Halo medal icon images from the official Waypoint CDN.

Medal icons aren't served as individual per-medal URLs - they're packed into
a single sprite sheet, and a separate metadata document maps each medal's id
to a spriteIndex (row/col) into that sheet's 16-column grid. Both files are
fetched once (using the bot's existing Spartan auth) and cached to disk under
MEDAL_ICON_CACHE_DIR, so every icon lookup after the first is a local file
read - no repeated network calls.
"""

import asyncio
import io
import json
import os
from typing import Dict, Optional

import aiohttp

from src.config.settings import (
    MEDAL_ICON_CACHE_DIR,
    MEDAL_METADATA_CACHE_FILE,
    MEDAL_SHEET_CACHE_FILE,
)

METADATA_URL = "https://gamecms-hacs.svc.halowaypoint.com/hi/Waypoint/file/medals/metadata.json"
SHEET_URL = "https://gamecms-hacs.svc.halowaypoint.com/hi/Waypoint/file/medals/images/medal_sheet_xl.png"

SHEET_COLUMNS = 16
SHEET_CELL_PX = 256
ICON_OUTPUT_PX = 64

_sheet_lock = asyncio.Lock()
_metadata: Optional[Dict[int, int]] = None  # medal_id -> spriteIndex
_sheet_bytes: Optional[bytes] = None
_logged_bad_metadata_shape = False


def _spartan_headers() -> Optional[Dict[str, str]]:
    # Local import: avoids a circular import at module load time, since
    # src.api.client is a large module that itself may import from other
    # src.api submodules.
    from src.api.client import api_client

    spartan_token = api_client.get_next_spartan_token()
    if not spartan_token:
        return None
    return {
        "User-Agent": api_client.user_agent,
        "Accept": "application/json",
        "x-343-authorization-spartan": spartan_token,
    }


def _parse_metadata(raw) -> Dict[int, int]:
    """Defensively extract {medal_id: spriteIndex} from the metadata JSON.
    Field names aren't confirmed against a live response, so several
    plausible key spellings are tried before giving up."""
    global _logged_bad_metadata_shape

    entries = None
    if isinstance(raw, list):
        entries = raw
    elif isinstance(raw, dict):
        for key in ("Medals", "medals", "MedalMetadata", "Items", "items"):
            value = raw.get(key)
            if isinstance(value, list):
                entries = value
                break

    if entries is None:
        if not _logged_bad_metadata_shape:
            shape = list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__
            print(f"[medal_icons] Unrecognized medal metadata shape, top-level keys/type: {shape}")
            _logged_bad_metadata_shape = True
        return {}

    parsed: Dict[int, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        medal_id = next((entry[k] for k in ("nameId", "NameId", "id", "Id") if k in entry), None)
        sprite_index = next(
            (entry[k] for k in ("spriteIndex", "SpriteIndex", "index", "Index") if k in entry), None
        )
        if medal_id is None or sprite_index is None:
            continue
        try:
            parsed[int(medal_id)] = int(sprite_index)
        except (TypeError, ValueError):
            continue

    if not parsed and not _logged_bad_metadata_shape:
        sample = entries[0] if entries else None
        print(f"[medal_icons] Medal metadata parsed to zero entries, sample entry: {sample}")
        _logged_bad_metadata_shape = True

    return parsed


def _write_bytes_atomic(filepath, data: bytes) -> None:
    filepath = str(filepath)
    temp_filepath = filepath + ".tmp"
    try:
        with open(temp_filepath, "wb") as f:
            f.write(data)
        os.replace(temp_filepath, filepath)
    except Exception as e:
        print(f"[medal_icons] Error writing {filepath}: {e}")
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError:
                pass


async def _download(url: str, headers: Dict[str, str]) -> Optional[bytes]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    print(f"[medal_icons] GET {url} -> HTTP {response.status}")
                    return None
                return await response.read()
    except Exception as e:
        print(f"[medal_icons] Error fetching {url}: {e}")
        return None


async def _ensure_sheet_and_metadata() -> bool:
    """Populate the in-memory metadata/sheet singletons: disk cache first,
    then a CDN fetch. Never raises - returns False on any failure so callers
    degrade to placeholder icons instead of crashing."""
    global _metadata, _sheet_bytes

    if _metadata and _sheet_bytes:
        return True

    async with _sheet_lock:
        if _metadata and _sheet_bytes:
            return True

        if _metadata is None:
            try:
                if os.path.exists(MEDAL_METADATA_CACHE_FILE):
                    with open(MEDAL_METADATA_CACHE_FILE, "rb") as f:
                        _metadata = _parse_metadata(json.loads(f.read()))
            except Exception as e:
                print(f"[medal_icons] Error reading cached metadata: {e}")

        if _sheet_bytes is None and os.path.exists(MEDAL_SHEET_CACHE_FILE):
            try:
                with open(MEDAL_SHEET_CACHE_FILE, "rb") as f:
                    _sheet_bytes = f.read()
            except Exception as e:
                print(f"[medal_icons] Error reading cached sheet: {e}")

        if _metadata and _sheet_bytes:
            return True

        headers = _spartan_headers()
        if not headers:
            print("[medal_icons] No Spartan token available - cannot fetch medal icons")
            return False

        if not _metadata:
            raw_bytes = await _download(METADATA_URL, headers)
            if raw_bytes is not None:
                try:
                    parsed = _parse_metadata(json.loads(raw_bytes))
                    if parsed:
                        _metadata = parsed
                        MEDAL_ICON_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                        _write_bytes_atomic(MEDAL_METADATA_CACHE_FILE, raw_bytes)
                except Exception as e:
                    print(f"[medal_icons] Error parsing medal metadata: {e}")

        if not _sheet_bytes:
            sheet_bytes = await _download(SHEET_URL, headers)
            if sheet_bytes is not None:
                _sheet_bytes = sheet_bytes
                MEDAL_ICON_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                _write_bytes_atomic(MEDAL_SHEET_CACHE_FILE, sheet_bytes)

        return bool(_metadata) and bool(_sheet_bytes)


async def get_medal_icon_bytes(medal_id: int) -> Optional[bytes]:
    """Return a 64px PNG icon for the given medal id, or None if unavailable.

    Never raises - callers should render a placeholder cell when this
    returns None (missing metadata entry, unreachable CDN, etc).
    """
    icon_cache_file = MEDAL_ICON_CACHE_DIR / f"{medal_id}.png"
    if icon_cache_file.exists():
        try:
            return icon_cache_file.read_bytes()
        except Exception as e:
            print(f"[medal_icons] Error reading cached icon {medal_id}: {e}")

    try:
        if not await _ensure_sheet_and_metadata():
            return None

        sprite_index = _metadata.get(medal_id)
        if sprite_index is None:
            return None

        from PIL import Image

        with Image.open(io.BytesIO(_sheet_bytes)) as sheet:
            col = sprite_index % SHEET_COLUMNS
            row = sprite_index // SHEET_COLUMNS
            left = col * SHEET_CELL_PX
            top = row * SHEET_CELL_PX
            box = (left, top, left + SHEET_CELL_PX, top + SHEET_CELL_PX)
            icon = sheet.crop(box).convert("RGBA").resize(
                (ICON_OUTPUT_PX, ICON_OUTPUT_PX), Image.LANCZOS
            )

            buffer = io.BytesIO()
            icon.save(buffer, format="PNG")
            icon_bytes = buffer.getvalue()

        try:
            MEDAL_ICON_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            icon_cache_file.write_bytes(icon_bytes)
        except Exception as e:
            print(f"[medal_icons] Error caching icon {medal_id}: {e}")

        return icon_bytes
    except Exception as e:
        print(f"[medal_icons] Error extracting icon for medal {medal_id}: {e}")
        return None
