from datetime import datetime, timedelta
from types import SimpleNamespace

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


@pytest.mark.asyncio
async def test_get_player_stats_forwards_force_full_fetch(monkeypatch):
    client = HaloAPIClient()
    client.clearance_token = "cached-clearance"

    captured = {}

    async def _fake_resolve_gamertag_to_xuid(gamertag):
        assert gamertag == "PlayerOne"
        return "xuid-1"

    async def _fake_calculate_comprehensive_stats(
        xuid,
        stat_type,
        gamertag=None,
        matches_to_process=None,
        force_full_fetch=False,
        _retry_count=0,
    ):
        captured.update(
            {
                "xuid": xuid,
                "stat_type": stat_type,
                "gamertag": gamertag,
                "matches_to_process": matches_to_process,
                "force_full_fetch": force_full_fetch,
            }
        )
        return {"error": 1, "message": "stop after forwarding check"}

    monkeypatch.setattr(client, "resolve_gamertag_to_xuid", _fake_resolve_gamertag_to_xuid)
    monkeypatch.setattr(client, "calculate_comprehensive_stats", _fake_calculate_comprehensive_stats)

    result = await client.get_player_stats(
        "PlayerOne",
        "overall",
        matches_to_process=None,
        force_full_fetch=True,
    )

    assert result["error"] == 1
    assert captured["xuid"] == "xuid-1"
    assert captured["stat_type"] == "overall"
    assert captured["gamertag"] == "PlayerOne"
    assert captured["matches_to_process"] is None
    assert captured["force_full_fetch"] is True


@pytest.mark.asyncio
async def test_statsfind_page_getter_forwards_force_full_fetch(monkeypatch):
    from src.api import client as client_module

    captured = {}

    async def _fake_get_player_stats(gamertag, stat_type, matches_to_process=10, force_full_fetch=False):
        captured.update(
            {
                "gamertag": gamertag,
                "stat_type": stat_type,
                "matches_to_process": matches_to_process,
                "force_full_fetch": force_full_fetch,
            }
        )
        return {
            "error": 0,
            "stats_list": ["1.0", "50.0%", "2.0", "10", "10", "10", "5"],
            "gamertag": gamertag,
            "stat_type": stat_type,
        }

    monkeypatch.setattr(client_module, "api_client", SimpleNamespace(get_player_stats=_fake_get_player_stats))

    finder = client_module.StatsFind()
    result = await finder.page_getter(
        "PlayerOne",
        "stats",
        matches_to_process=None,
        force_full_fetch=True,
    )

    assert result.error_no == 0
    assert captured["gamertag"] == "PlayerOne"
    assert captured["stat_type"] == "overall"
    assert captured["matches_to_process"] is None
    assert captured["force_full_fetch"] is True


def test_get_cached_match_ids_handles_cache_exceptions(monkeypatch):
    client = HaloAPIClient()

    class BadCache:
        def get_cached_match_ids(self, *args, **kwargs):
            raise RuntimeError("boom")

    client.stats_cache = BadCache()
    assert client.get_cached_match_ids("xuid-1") == set()


@pytest.mark.asyncio
async def test_backfill_seed_match_participants_fetches_only_incomplete_verified_matches(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    class _FakeStatsDB:
        def __init__(self):
            self.coverage = {
                "m-1": {"participant_count": 2, "seed_present": True},
                "m-2": {"participant_count": 1, "seed_present": True},
                "m-3": {"participant_count": 0, "seed_present": False},
            }

        def get_seed_verified_match_ids(self, seed_xuid, limit_matches=None):
            all_ids = ["m-1", "m-2", "m-3"]
            if limit_matches:
                return all_ids[: int(limit_matches)]
            return all_ids

        def get_participant_coverage_for_matches(self, match_ids, seed_xuid):
            return {
                match_id: dict(self.coverage.get(match_id, {"participant_count": 0, "seed_present": False}))
                for match_id in match_ids
            }

        def insert_match(self, match_data):
            return True

        def insert_match_participants(self, match_id, participants):
            self.coverage[match_id] = {
                "participant_count": len(participants),
                "seed_present": any(str(p.get("xuid") or "").strip() == "seed-xuid" for p in participants),
            }
            return True

    fake_db = _FakeStatsDB()
    client.stats_cache = SimpleNamespace(
        db=fake_db,
        get_seed_verified_match_ids=fake_db.get_seed_verified_match_ids,
        get_participant_coverage_for_matches=fake_db.get_participant_coverage_for_matches,
    )

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "all_participants": [
                {"xuid": "seed-xuid", "team_id": "1", "outcome": 2},
                {"xuid": f"ally-{match_id}", "team_id": "1", "outcome": 2},
            ],
        }

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    result = await client.backfill_seed_match_participants("seed-xuid", "SeedTag")

    assert result["ok"] is True
    assert result["verified_matches"] == 3
    assert result["complete_matches_before"] == 1
    assert result["incomplete_matches_before"] == 2
    assert result["attempted_backfills"] == 2
    assert result["successful_backfills"] == 2
    assert result["failed_backfills"] == 0
    assert result["complete_matches_after"] == 3
    assert result["incomplete_matches_after"] == 0
    assert set(detail_calls) == {"m-2", "m-3"}


@pytest.mark.asyncio
async def test_backfill_seed_match_participants_skips_when_coverage_already_complete(monkeypatch):
    client = HaloAPIClient()

    class _FakeStatsDB:
        def get_seed_verified_match_ids(self, seed_xuid, limit_matches=None):
            return ["m-1", "m-2"]

        def get_participant_coverage_for_matches(self, match_ids, seed_xuid):
            return {
                "m-1": {"participant_count": 4, "seed_present": True},
                "m-2": {"participant_count": 8, "seed_present": True},
            }

    fake_db = _FakeStatsDB()
    client.stats_cache = SimpleNamespace(
        db=fake_db,
        get_seed_verified_match_ids=fake_db.get_seed_verified_match_ids,
        get_participant_coverage_for_matches=fake_db.get_participant_coverage_for_matches,
    )

    async def _unexpected_match_detail(match_id, player_xuid, session):
        raise AssertionError("match detail fetch should not run when coverage is complete")

    monkeypatch.setattr(client, "get_match_stats_for_match", _unexpected_match_detail)

    result = await client.backfill_seed_match_participants("seed-xuid", "SeedTag")

    assert result["ok"] is True
    assert result["verified_matches"] == 2
    assert result["complete_matches_before"] == 2
    assert result["incomplete_matches_before"] == 0
    assert result["attempted_backfills"] == 0
    assert result["successful_backfills"] == 0
    assert result["failed_backfills"] == 0
    assert result["complete_matches_after"] == 2
    assert result["incomplete_matches_after"] == 0


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


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_cache_hit_with_spacing_normalization(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {"123": "Some   Player"})

    async def _fail_acquire(*args, **kwargs):
        raise AssertionError("rate limiter should not be called on normalized cache hit")

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _fail_acquire)

    result = await client.resolve_gamertag_to_xuid(" some player ")
    assert result == "123"


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_cache_alias_hit_without_spaces(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {"123": "Some Player"})

    async def _fail_acquire(*args, **kwargs):
        raise AssertionError("rate limiter should not be called on alias cache hit")

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _fail_acquire)

    result = await client.resolve_gamertag_to_xuid("SomePlayer")
    assert result == "123"


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_alias_ambiguity_falls_back_to_api(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(
        client_module,
        "load_xuid_cache",
        lambda: {
            "111": "Some Player",
            "222": "Some  Player",
        },
    )
    monkeypatch.setattr(client_module.os.path, "exists", lambda path: False)

    acquire_calls = {"count": 0}
    release_calls = {"count": 0}

    async def _acquire(*args, **kwargs):
        acquire_calls["count"] += 1
        return 0

    def _release(*args, **kwargs):
        release_calls["count"] += 1

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", _release)

    result = await client.resolve_gamertag_to_xuid("SomePlayer")

    assert result is None
    assert acquire_calls["count"] == 1
    assert release_calls["count"] == 1


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_api_success_persists_canonical_gamertag(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    saved = {}
    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {})
    monkeypatch.setattr(client_module, "save_xuid_cache", lambda payload: saved.update(payload))
    monkeypatch.setattr(client_module.os.path, "exists", lambda path: True)

    class _FakeTokenFile:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(client_module, "open", lambda *args, **kwargs: _FakeTokenFile(), raising=False)
    monkeypatch.setattr(
        client_module.json,
        "load",
        lambda _file: {"xsts_xbox": {"token": "token", "uhs": "uhs"}},
    )

    async def _acquire(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", lambda: None)

    class _FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {
                "profileUsers": [
                    {
                        "id": "123",
                        "settings": [
                            {"id": "Gamertag", "value": "Some Player"},
                        ],
                    }
                ]
            }

        async def text(self):
            return ""

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *args, **kwargs):
            return _FakeResponse()

    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())

    result = await client.resolve_gamertag_to_xuid("SomePlayer")
    assert result == "123"
    assert saved == {"123": "Some Player"}


@pytest.mark.asyncio
async def test_resolve_xuid_to_gamertag_cache_hit(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {"123": "CachedTag"})

    async def _fail_acquire(*args, **kwargs):
        raise AssertionError("rate limiter should not be called on cache hit")

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _fail_acquire)

    result = await client.resolve_xuid_to_gamertag("123")
    assert result == "CachedTag"


@pytest.mark.asyncio
async def test_resolve_xuid_to_gamertag_api_success(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    saved = {}
    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {})
    monkeypatch.setattr(client_module, "save_xuid_cache", lambda payload: saved.update(payload))
    monkeypatch.setattr(client_module.os.path, "exists", lambda path: True)
    monkeypatch.setattr(
        client_module,
        "safe_read_json",
        lambda *args, **kwargs: {"xsts_xbox": {"token": "token", "uhs": "uhs"}},
    )

    async def _acquire(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", lambda: None)

    class _FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {
                "profileUsers": [
                    {
                        "settings": [
                            {"id": "Gamertag", "value": "ResolvedTag"},
                        ]
                    }
                ]
            }

        async def text(self):
            return ""

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *args, **kwargs):
            return _FakeResponse()

    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())

    result = await client.resolve_xuid_to_gamertag("123")
    assert result == "ResolvedTag"
    assert saved == {"123": "ResolvedTag"}


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_incremental_topup(monkeypatch):
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

    starts = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                return _FakeResponse(200, {"TotalCount": 2, "Results": [{"MatchId": "new-1"}, {"MatchId": "old-1"}]})
            return _FakeResponse(200, {"TotalCount": 2, "Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {
            "processed_matches": [
                {
                    "match_id": "old-1",
                    "kills": 3,
                    "deaths": 2,
                    "assists": 1,
                    "outcome": 2,
                    "is_ranked": False,
                    "start_time": "2026-01-01T00:00:00",
                }
            ]
        },
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)

    async def _fake_match_detail(match_id, player_xuid, session):
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["new_matches"] == 1
    assert result["stats"]["games_played"] == 2
    assert 0 in starts


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_without_total_hint_uses_incremental_boundary(monkeypatch):
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

    starts = []
    cached_matches = [
        {
            "match_id": f"old-{idx}",
            "kills": 3,
            "deaths": 2,
            "assists": 1,
            "outcome": 2,
            "is_ranked": False,
            "start_time": f"2026-01-{((idx - 1) % 28) + 1:02d}T00:00:00",
        }
        for idx in range(1, 26)
    ]
    first_page_results = [{"MatchId": f"old-{idx}"} for idx in range(1, 26)]

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                # Deliberately omit TotalCount metadata; completeness should be proven by boundary + probe.
                return _FakeResponse(200, {"Results": first_page_results})
            if start == 25:
                # Probe at one position beyond cached 25 finds no additional history.
                return _FakeResponse(200, {"Results": []})
            return _FakeResponse(200, {"Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {"processed_matches": cached_matches},
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)

    async def _fake_match_detail(match_id, player_xuid, session):
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 25
    # Incremental path fetches start=0 for page scan and first-page metadata probe.
    assert starts.count(0) >= 2
    # Probe should run exactly once at one position beyond cached coverage.
    assert starts.count(25) == 1


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_probe_detects_uncached_older_match(monkeypatch):
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

    starts = []
    cached_matches = [
        {
            "match_id": f"old-{idx}",
            "kills": 1,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": f"2026-01-{((idx - 1) % 28) + 1:02d}T00:00:00",
        }
        for idx in range(1, 26)
    ]
    first_page_results = [{"MatchId": f"old-{idx}"} for idx in range(1, 26)]

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                return _FakeResponse(200, {"Results": first_page_results})
            if start == 25:
                # Boundary probe sees an uncached older match, forcing full list traversal.
                return _FakeResponse(200, {"Results": [{"MatchId": "old-26"}]})
            return _FakeResponse(200, {"Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {"processed_matches": cached_matches},
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-02-01T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 26
    # Probe + full traversal should touch start=25 at least twice.
    assert starts.count(25) >= 2
    # Fallback must still fetch details only for uncached matches.
    assert detail_calls == ["old-26"]


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_ignores_ambiguous_count_hint_and_probes(monkeypatch):
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

    starts = []
    cached_matches = [
        {
            "match_id": f"old-{idx}",
            "kills": 1,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": f"2026-01-{((idx - 1) % 28) + 1:02d}T00:00:00",
        }
        for idx in range(1, 27)
    ]

    first_page_results = [{"MatchId": f"old-{idx}"} for idx in range(1, 26)]
    second_page_results = [{"MatchId": "old-26"}, {"MatchId": "old-27"}]

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                # Count is intentionally ambiguous (can be page-size), not a reliable lifetime total.
                return _FakeResponse(200, {"Count": 25, "Results": first_page_results})
            if start == 25:
                return _FakeResponse(200, {"Count": 2, "Results": second_page_results})
            if start == 26:
                # Boundary probe should detect older uncached history.
                return _FakeResponse(200, {"Count": 1, "Results": [{"MatchId": "old-27"}]})
            return _FakeResponse(200, {"Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {"processed_matches": cached_matches},
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-02-01T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 27
    # Probe position should be queried despite Count being present.
    assert 26 in starts
    # Full fallback should still fetch details only for uncached IDs.
    assert detail_calls == ["old-27"]


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_falls_back_when_gap_not_converged(monkeypatch):
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

    starts = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                return _FakeResponse(
                    200,
                    {
                        "TotalCount": 4,
                        "Results": [
                            {"MatchId": "old-1"},
                            {"MatchId": "old-2"},
                            {"MatchId": "new-1"},
                            {"MatchId": "new-2"},
                        ],
                    },
                )
            return _FakeResponse(200, {"TotalCount": 4, "Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {
            "processed_matches": [
                {
                    "match_id": "old-1",
                    "kills": 1,
                    "deaths": 1,
                    "assists": 0,
                    "outcome": 2,
                    "is_ranked": False,
                    "start_time": "2026-01-01T00:00:00",
                },
                {
                    "match_id": "old-2",
                    "kills": 1,
                    "deaths": 1,
                    "assists": 0,
                    "outcome": 2,
                    "is_ranked": False,
                    "start_time": "2026-01-01T00:00:00",
                },
            ]
        },
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 4
    # start=0 is requested once for incremental check and again for full-fetch fallback.
    assert starts.count(0) >= 2
    assert set(detail_calls) == {"new-1", "new-2"}
