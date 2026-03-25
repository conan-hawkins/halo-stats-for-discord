import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable


@dataclass(frozen=True)
class CacheStatusMetrics:
    xuid_mappings: int
    processed_matches: int
    total_matches: int
    resolved_gamertags: int
    progress_state: str


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_gamertag_count(progress: Dict[str, Any], cache: Dict[str, Any]) -> int:
    resolved_raw = progress.get("resolved_gamertags")
    if isinstance(resolved_raw, list):
        return len(resolved_raw)
    if isinstance(resolved_raw, int):
        return resolved_raw

    # Fallback for older progress files that do not include resolved_gamertags.
    return sum(1 for value in cache.values() if str(value).strip())


def load_cache_status_metrics(cache_file: str | Path, progress_candidates: Iterable[str | Path]) -> CacheStatusMetrics:
    with open(cache_file, "r", encoding="utf-8") as f:
        cache = json.load(f)

    if not isinstance(cache, dict):
        raise ValueError("XUID cache must be a JSON object")

    xuid_mappings = len(cache)
    fallback_resolved = sum(1 for value in cache.values() if str(value).strip())

    progress_path = next((str(path) for path in progress_candidates if os.path.exists(path)), None)
    if not progress_path:
        return CacheStatusMetrics(
            xuid_mappings=xuid_mappings,
            processed_matches=0,
            total_matches=0,
            resolved_gamertags=fallback_resolved,
            progress_state="missing",
        )

    try:
        with open(progress_path, "r", encoding="utf-8") as f:
            progress = json.load(f)
    except Exception:
        return CacheStatusMetrics(
            xuid_mappings=xuid_mappings,
            processed_matches=0,
            total_matches=0,
            resolved_gamertags=fallback_resolved,
            progress_state="unreadable",
        )

    if not isinstance(progress, dict):
        progress = {}

    processed_matches = _to_int(progress.get("processed_matches", progress.get("last_processed_index", 0)), default=0)
    total_matches = _to_int(progress.get("total_matches", 0), default=0)
    resolved_gamertags = _resolve_gamertag_count(progress, cache)

    return CacheStatusMetrics(
        xuid_mappings=xuid_mappings,
        processed_matches=processed_matches,
        total_matches=total_matches,
        resolved_gamertags=resolved_gamertags,
        progress_state="ok",
    )
