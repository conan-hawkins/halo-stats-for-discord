"""Fetch and cache map artwork from the Halo UGC blob store.

The website shows a thumbnail beside each match, and a browser cannot fetch
that artwork itself: the blob store sits behind the same Spartan auth as the
rest of the Halo API. So the bot - which already owns the credentials - pulls
each image once into MAP_IMAGE_CACHE_DIR, and halo-stats-api serves the files
from disk. Exactly the arrangement medal icons use, and for the same reason.

There is no sprite sheet here. Each MapVariant asset publishes its own image
under a per-asset blob prefix, so images arrive one at a time, keyed on the
asset id. That id's artwork never changes - a reworked map ships as a NEW
asset id, not a new version of the old one - which is what makes "download
once, keep forever" correct rather than merely convenient.

Every function is best-effort and returns rather than raises. A map with no
cached image is a map the site renders without a picture, which is a far
better outcome than a match-ingest run dying over a thumbnail.
"""

import os
from typing import Dict, Optional

import aiohttp

from src.config.settings import MAP_IMAGE_CACHE_DIR

# Cap on a single image. Map thumbnails observed on the blob store are tens of
# kilobytes; a megabyte is generous headroom while still refusing to stream
# something unbounded into the data directory on a wrong or hijacked URL.
MAX_IMAGE_BYTES = 1_048_576

DOWNLOAD_TIMEOUT_SECONDS = 20

# What we will write to disk, mapped to the extension it gets stored under.
# Anything else is discarded: the API serves these straight to browsers, so
# the set of types accepted here is the set of types it can end up serving.
_ALLOWED_CONTENT_TYPES = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}

# Suffix order the API and the backfill both use when looking for an asset's
# cached image, most likely first.
IMAGE_EXTENSIONS = (".jpg", ".png", ".webp")


def cached_image_path(map_asset_id: str) -> Optional[str]:
    """Path of this asset's cached image, or None if nothing is cached.

    The extension is not knowable from the asset id alone - it depends on what
    the blob store served - so this probes the small allowed set rather than
    storing the filename in the database and having two sources of truth.
    """
    for extension in IMAGE_EXTENSIONS:
        candidate = MAP_IMAGE_CACHE_DIR / f"{map_asset_id}{extension}"
        if candidate.exists():
            return str(candidate)
    return None


def _write_bytes_atomic(filepath, data: bytes) -> bool:
    filepath = str(filepath)
    temp_filepath = filepath + ".tmp"
    try:
        with open(temp_filepath, "wb") as f:
            f.write(data)
        os.replace(temp_filepath, filepath)
        return True
    except Exception as e:
        print(f"[map_images] Error writing {filepath}: {e}")
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError:
                pass
        return False


def _spartan_headers(client=None) -> Optional[Dict[str, str]]:
    """Auth headers from `client`, or from the bot's own singleton.

    The client argument is not optional in practice - it is how the offline
    backfill jobs work at all. Those build their OWN HaloAPIClient from whatever
    Spartan tokens are already cached on disk, and never authenticate the module
    singleton; falling back to it there yields no token, so artwork would fail
    silently on every map the backfill named. Callers inside the running bot can
    omit it, where the singleton is the authenticated one.
    """
    if client is None:
        # Local import: src.api.client is a large module that imports from other
        # src.api submodules, so importing it at load time would be circular.
        from src.api.client import api_client

        client = api_client

    spartan_token = client.get_next_spartan_token()
    if isinstance(spartan_token, dict) and "token" in spartan_token:
        spartan_token = spartan_token["token"]
    if not spartan_token:
        return None
    return {
        "Authorization": f"Spartan {spartan_token}",
        "x-343-authorization-spartan": spartan_token,
        "User-Agent": client.user_agent,
        "Accept": "image/*",
    }


async def cache_map_image(map_asset_id: str, image_url: str, client=None) -> Optional[str]:
    """Download one map's artwork into the cache, returning its path.

    A no-op returning the existing path when the image is already cached, so
    this is safe to call on every resolve. Returns None on any failure.

    `client` is the HaloAPIClient whose Spartan tokens to use - see
    _spartan_headers for why an offline job must pass its own.
    """
    if not map_asset_id or not image_url:
        return None

    existing = cached_image_path(map_asset_id)
    if existing:
        return existing

    headers = _spartan_headers(client)
    if headers is None:
        print("[map_images] No Spartan token available - cannot fetch map artwork")
        return None

    try:
        timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(image_url, headers=headers) as response:
                if response.status != 200:
                    print(f"[map_images] GET {image_url} -> HTTP {response.status}")
                    return None

                content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                extension = _ALLOWED_CONTENT_TYPES.get(content_type)
                if extension is None:
                    print(f"[map_images] Refusing {content_type or 'unknown'} for map {map_asset_id}")
                    return None

                # Read with a ceiling rather than response.read(): the length
                # header is the server's claim, not a guarantee.
                data = await response.content.read(MAX_IMAGE_BYTES + 1)
                if len(data) > MAX_IMAGE_BYTES:
                    print(f"[map_images] Map {map_asset_id} artwork exceeds {MAX_IMAGE_BYTES} bytes - skipped")
                    return None
                if not data:
                    return None

        MAP_IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        destination = MAP_IMAGE_CACHE_DIR / f"{map_asset_id}{extension}"
        if _write_bytes_atomic(destination, data):
            return str(destination)
        return None
    except Exception as e:
        print(f"[map_images] Error caching artwork for map {map_asset_id}: {e}")
        return None
