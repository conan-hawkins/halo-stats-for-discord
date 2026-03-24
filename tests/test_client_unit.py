from datetime import datetime, timedelta

import pytest

from src.api.client import HaloAPIClient


def test_get_next_spartan_token_round_robin_and_index_selection():
    client = HaloAPIClient()
    client.spartan_accounts = [
        {"token": "tok-1"},
        {"token": "tok-2"},
        {"token": "tok-3"},
    ]

    assert client.get_next_spartan_token(1) == "tok-2"
    assert client.get_next_spartan_token() == "tok-1"
    assert client.get_next_spartan_token() == "tok-2"
    assert client.get_next_spartan_token() == "tok-3"


def test_get_next_spartan_token_falls_back_to_single_token():
    client = HaloAPIClient()
    client.spartan_accounts = []
    client.spartan_token = "fallback"

    assert client.get_next_spartan_token() == "fallback"


def test_is_cache_fresh_true_false_and_invalid():
    client = HaloAPIClient()
    fresh = {"last_update": (datetime.now() - timedelta(minutes=3)).isoformat()}
    stale = {"last_update": (datetime.now() - timedelta(minutes=90)).isoformat()}
    invalid = {"last_update": "not-a-date"}

    assert client.is_cache_fresh(fresh, max_age_minutes=30) is True
    assert client.is_cache_fresh(stale, max_age_minutes=30) is False
    assert client.is_cache_fresh(invalid, max_age_minutes=30) is False
    assert client.is_cache_fresh(None, max_age_minutes=30) is False


def test_calculate_stats_from_matches_filters_stat_types():
    client = HaloAPIClient()
    matches = [
        {"kills": 10, "deaths": 5, "assists": 6, "outcome": 2, "is_ranked": True},
        {"kills": 4, "deaths": 8, "assists": 2, "outcome": 3, "is_ranked": False},
        {"kills": 2, "deaths": 2, "assists": 1, "outcome": 1, "is_ranked": False},
    ]

    overall = client._calculate_stats_from_matches(matches, "overall")
    ranked = client._calculate_stats_from_matches(matches, "ranked")
    social = client._calculate_stats_from_matches(matches, "social")

    assert overall["games_played"] == 3
    assert overall["wins"] == 1
    assert overall["losses"] == 1
    assert overall["ties"] == 1
    assert ranked["games_played"] == 1
    assert social["games_played"] == 2


def test_parse_stats_success_and_error_payloads():
    client = HaloAPIClient()
    payload = {
        "error": 0,
        "stats": {
            "kd_ratio": 1.5,
            "win_rate": "55.0%",
            "avg_kda": 3.2,
            "total_deaths": 10,
            "total_kills": 15,
            "total_assists": 7,
            "games_played": 12,
        },
        "matches_processed": 12,
        "new_matches": 4,
    }

    parsed = client.parse_stats(payload, "overall", "PlayerOne")
    assert parsed["error"] == 0
    assert parsed["gamertag"] == "PlayerOne"
    assert parsed["stats_list"][0] == "1.5"
    assert parsed["stats_list"][6] == "12"

    failure = client.parse_stats({"error": 3, "message": "private"}, "overall", "PlayerOne")
    assert failure == {"error": 3, "message": "private"}


def test_get_cached_match_ids_handles_cache_exceptions(monkeypatch):
    client = HaloAPIClient()

    class BadCache:
        def get_cached_match_ids(self, *args, **kwargs):
            raise RuntimeError("boom")

    client.stats_cache = BadCache()
    assert client.get_cached_match_ids("xuid-1") == set()


@pytest.mark.asyncio
async def test_get_clearance_token_missing_cache_file(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    monkeypatch.setattr(client_module.os.path, "exists", lambda path: False)
    assert await client.get_clearance_token() is False


@pytest.mark.asyncio
async def test_get_clearance_token_loads_valid_spartan(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    token_data = {
        "spartan": {"token": "spartan-token", "expires_at": 9999999999},
        "xsts_xbox": {"token": "xbox-token", "expires_at": 9999999999},
    }

    monkeypatch.setattr(client_module.os.path, "exists", lambda path: True)
    monkeypatch.setattr(client_module, "safe_read_json", lambda *args, **kwargs: token_data)
    monkeypatch.setattr(client_module, "is_token_valid", lambda info: True)

    ok = await client.get_clearance_token()
    assert ok is True
    assert client.spartan_token == "spartan-token"


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_bounded_fetch_stops_at_required_pages(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
            return None

    class _FakeResponse:
        def __init__(self, status, payload):
            self.status = status
            self._payload = payload
            self.headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return self._payload

        async def text(self):
            return ""

    requested_starts = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            requested_starts.append(start)

            if start >= 50:
                return _FakeResponse(200, {"Results": []})

            matches = [{"MatchId": f"m{start + i}"} for i in range(25)]
            return _FakeResponse(200, {"Results": matches})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(client, "load_cached_stats", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)

    async def _fake_match_detail(match_id, player_xuid, session):
        return {
            "match_id": match_id,
            "kills": 1,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-01T00:00:00",
            "csr": None,
            "csr_tier": None,
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=50,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 50
    assert sorted(requested_starts) == [0, 25]
