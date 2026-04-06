import pytest

from src.api.client import HaloAPIClient


class _FakeResponse:
    def __init__(self, status, headers=None, json_data=None, text_data=""):
        self.status = status
        self.headers = headers or {}
        self._json_data = json_data
        self._text_data = text_data

    async def json(self):
        return self._json_data

    async def text(self):
        return self._text_data


class _FakeGetContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def get(self, *args, **kwargs):
        return _FakeGetContext(self.responses.pop(0))


@pytest.mark.asyncio
async def test_get_match_stats_retries_429_with_account_backoff(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    wait_calls = [0, 1]
    wait_seen = []
    backoffs = []

    async def fake_wait_if_needed(*args, **kwargs):
        idx = wait_calls.pop(0)
        wait_seen.append(idx)
        return idx

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", fake_wait_if_needed)
    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "set_backoff", lambda seconds, account_index=None: backoffs.append((seconds, account_index)))
    monkeypatch.setattr(client, "get_next_spartan_token", lambda idx=None: f"tok-{idx}")

    stats_payload = {
        "Players": [
            {
                "PlayerId": "xuid(123)",
                "Outcome": 2,
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 5, "Deaths": 2, "Assists": 1, "Medals": []}}}],
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT10M",
            "Playlist": {"AssetId": "p1", "VersionId": "v1"},
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }
    session = _FakeSession(
        [
            _FakeResponse(429, headers={"Retry-After": "6"}),
            _FakeResponse(200, json_data=stats_payload),
        ]
    )

    result = await client.get_match_stats_for_match("match-1", "123", session)

    assert result is not None
    assert result["kills"] == 5
    assert result["all_participants"]
    assert result["all_participants"][0]["xuid"] == "123"
    assert result["all_participants"][0]["inferred_team_id"] == "outcome:2"
    assert result["match_category"] == "social"
    assert result["category_source"] == "default_non_ranked"
    assert wait_seen == [0, 1]
    assert (6, 0) in backoffs


@pytest.mark.asyncio
async def test_get_match_stats_extracts_csr_and_tier(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    async def fake_wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", fake_wait_if_needed)
    monkeypatch.setattr(client, "get_next_spartan_token", lambda idx=None: "tok-0")

    stats_payload = {
        "Players": [
            {
                "PlayerId": "xuid(123)",
                "Outcome": 2,
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 8, "Deaths": 4, "Assists": 3, "Medals": []}}}],
                "Skill": {"Csr": 1523, "Tier": "Platinum 3"},
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT12M",
            "Playlist": {"AssetId": "p1", "VersionId": "v1"},
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }

    session = _FakeSession([_FakeResponse(200, json_data=stats_payload)])
    result = await client.get_match_stats_for_match("match-2", "123", session)

    assert result is not None
    assert result["csr"] == 1523
    assert result["csr_tier"] == "Platinum 3"


@pytest.mark.asyncio
async def test_get_match_stats_does_not_infer_csr_from_rank_field(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    async def fake_wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", fake_wait_if_needed)
    monkeypatch.setattr(client, "get_next_spartan_token", lambda idx=None: "tok-0")

    stats_payload = {
        "Players": [
            {
                "PlayerId": "xuid(123)",
                "Outcome": 2,
                "Rank": 5,
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 8, "Deaths": 4, "Assists": 3, "Medals": []}}}],
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT12M",
            "Playlist": {"AssetId": "p1", "VersionId": "v1"},
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }

    session = _FakeSession([_FakeResponse(200, json_data=stats_payload)])
    result = await client.get_match_stats_for_match("match-3", "123", session)

    assert result is not None
    assert result["csr"] is None


@pytest.mark.asyncio
async def test_get_match_stats_retries_500_then_fails(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    async def fake_wait_if_needed(*args, **kwargs):
        return 0

    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", fake_wait_if_needed)
    monkeypatch.setattr(client_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(client, "get_next_spartan_token", lambda idx=None: "tok")

    session = _FakeSession([_FakeResponse(500), _FakeResponse(500)])
    result = await client.get_match_stats_for_match("match-1", "123", session)

    assert result is None
    assert sleeps == [0.3]


@pytest.mark.asyncio
async def test_get_match_stats_classifies_custom_from_playlist_hint(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    async def fake_wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", fake_wait_if_needed)
    monkeypatch.setattr(client, "get_next_spartan_token", lambda idx=None: "tok-0")

    stats_payload = {
        "Players": [
            {
                "PlayerId": "xuid(123)",
                "Outcome": 2,
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 6, "Deaths": 3, "Assists": 2, "Medals": []}}}],
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT10M",
            "Playlist": {"AssetId": "custom-playlist-test", "VersionId": "v1"},
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }

    session = _FakeSession([_FakeResponse(200, json_data=stats_payload)])
    result = await client.get_match_stats_for_match("match-custom", "123", session)

    assert result is not None
    assert result["is_ranked"] is False
    assert result["match_category"] == "custom"
    assert result["category_source"] == "text_heuristic"


@pytest.mark.asyncio
async def test_get_friends_list_429_then_success(monkeypatch):
    client = HaloAPIClient()
    client.xbox_accounts = [{"token": "xtok", "uhs": "u1"}]

    from src.api import client as client_module

    async def fake_acquire(account_index=None):
        return 0

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", fake_acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", lambda: None)
    backoffs = []
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "set_backoff", lambda idx, sec: backoffs.append((idx, sec)))

    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(client_module.asyncio, "sleep", fake_sleep)

    resp_429 = _FakeResponse(
        429,
        json_data={"periodInSeconds": 100, "currentRequests": 30, "maxRequests": 30},
    )
    resp_200 = _FakeResponse(
        200,
        json_data={
            "people": [
                {
                    "xuid": "200",
                    "gamertag": "FriendOne",
                    "displayName": "Friend One",
                    "isFollowingCaller": True,
                    "isFollowedByCaller": True,
                }
            ]
        },
    )

    sessions = [_FakeSession([resp_429]), _FakeSession([resp_200])]

    class _SessionFactory:
        def __init__(self, seq):
            self.seq = seq

        async def __aenter__(self):
            return self.seq.pop(0)

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda: _SessionFactory(sessions))

    result = await client.get_friends_list("100", max_retries=2)

    assert result["error"] is None
    assert len(result["friends"]) == 1
    assert result["friends"][0]["xuid"] == "200"
    assert backoffs
    assert sleeps


@pytest.mark.asyncio
async def test_get_friends_list_retry_drops_fixed_account_hint(monkeypatch):
    client = HaloAPIClient()
    client.xbox_accounts = [
        {"token": "xtok-1", "uhs": "u1"},
        {"token": "xtok-2", "uhs": "u2"},
    ]

    from src.api import client as client_module

    acquire_calls = []

    async def fake_acquire(account_index=None):
        acquire_calls.append(account_index)
        return 0 if len(acquire_calls) == 1 else 1

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", fake_acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", lambda: None)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "set_backoff", lambda idx, sec: None)

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr(client_module.asyncio, "sleep", fake_sleep)

    resp_429 = _FakeResponse(429, json_data={"periodInSeconds": 60, "currentRequests": 30, "maxRequests": 30})
    resp_200 = _FakeResponse(
        200,
        json_data={
            "people": [
                {
                    "xuid": "300",
                    "gamertag": "FriendTwo",
                    "displayName": "Friend Two",
                    "isFollowingCaller": True,
                    "isFollowedByCaller": True,
                }
            ]
        },
    )

    sessions = [_FakeSession([resp_429]), _FakeSession([resp_200])]

    class _SessionFactory:
        def __init__(self, seq):
            self.seq = seq

        async def __aenter__(self):
            return self.seq.pop(0)

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda: _SessionFactory(sessions))

    result = await client.get_friends_list("100", _account_index=0, max_retries=2)

    assert result["error"] is None
    assert acquire_calls == [0, None]


@pytest.mark.asyncio
async def test_get_friends_of_friends_distributes_across_all_accounts(monkeypatch):
    client = HaloAPIClient()
    client.xbox_accounts = [
        {"token": "xtok-1", "uhs": "u1"},
        {"token": "xtok-2", "uhs": "u2"},
        {"token": "xtok-3", "uhs": "u3"},
        {"token": "xtok-4", "uhs": "u4"},
        {"token": "xtok-5", "uhs": "u5"},
    ]

    from src.api import client as client_module

    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {})
    monkeypatch.setattr(client_module, "save_xuid_cache", lambda _cache: None)

    async def fake_resolve(_gamertag):
        return "target-xuid"

    monkeypatch.setattr(client, "resolve_gamertag_to_xuid", fake_resolve)

    seen_account_indexes = []
    direct_friends = [{"xuid": f"friend-{i}", "gamertag": f"Friend{i}"} for i in range(10)]

    async def fake_get_friends_list(xuid, _xuid_cache=None, _cache_stats=None, _account_index=None, max_retries=5):
        if xuid == "target-xuid":
            return {"friends": direct_friends, "is_private": False, "error": None}
        seen_account_indexes.append(_account_index)
        return {"friends": [], "is_private": False, "error": None}

    monkeypatch.setattr(client, "get_friends_list", fake_get_friends_list)

    result = await client.get_friends_of_friends("TargetGT", max_depth=2)

    assert result["error"] is None
    assert set(seen_account_indexes) == {0, 1, 2, 3, 4}
