from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _protect_real_token_caches(monkeypatch):
    """Fail loudly if a test tries to write the real data/auth token caches.

    A test that patched safe_read_json but not safe_write_json once clobbered
    data/auth/token_cache.json with fixture data, destroying the production
    OAuth refresh token and forcing a manual browser re-auth.
    """
    from src.config import TOKEN_CACHE_DIR
    from src.api import client as client_module
    from src.api import utils as utils_module
    from src.auth import tokens as tokens_module

    protected_dir = Path(TOKEN_CACHE_DIR).resolve()

    def _assert_not_protected(filepath):
        target = Path(str(filepath)).resolve()
        if protected_dir == target.parent or protected_dir in target.parents:
            raise RuntimeError(
                f"Test attempted to write real token cache file: {target}. "
                "Patch safe_write_json (see _install_store in "
                "test_token_refresh_flows.py) so writes stay in memory."
            )

    real_write = utils_module.safe_write_json

    def guarded_write(filepath, data, indent=2):
        _assert_not_protected(filepath)
        return real_write(filepath, data, indent=indent)

    monkeypatch.setattr(utils_module, "safe_write_json", guarded_write)
    monkeypatch.setattr(client_module, "safe_write_json", guarded_write)

    real_save = tokens_module.TokenCache.save

    def guarded_save(self):
        _assert_not_protected(self.cache_file)
        return real_save(self)

    monkeypatch.setattr(tokens_module.TokenCache, "save", guarded_save)


@pytest.fixture
def sample_match_data():
    return {
        "match_id": "match-1",
        "kills": 10,
        "deaths": 5,
        "assists": 7,
        "outcome": 2,
        "duration": "PT12M",
        "start_time": "2026-01-01T12:00:00",
        "is_ranked": True,
        "playlist_id": "playlist-ranked",
        "map_id": "map-1",
        "map_version": "v1",
        "medals": [
            {"NameId": 622331684, "Count": 2, "TotalPersonalScoreAwarded": 0},
            {"NameId": 2758320809, "Count": 1, "TotalPersonalScoreAwarded": 0},
        ],
    }
