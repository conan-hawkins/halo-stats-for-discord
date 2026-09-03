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
    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "set_backoff", lambda *, seconds, account_index=None: backoffs.append((seconds, account_index)))
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
async def test_get_match_stats_classifies_custom_from_explicit_flag(monkeypatch):
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
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 7, "Deaths": 4, "Assists": 5, "Medals": []}}}],
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT10M",
            "IsCustom": True,
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }

    session = _FakeSession([_FakeResponse(200, json_data=stats_payload)])
    result = await client.get_match_stats_for_match("match-custom-flag", "123", session)

    assert result is not None
    assert result["is_ranked"] is False
    assert result["match_category"] == "custom"
    assert result["category_source"] == "explicit_custom_flag"


@pytest.mark.asyncio
async def test_get_match_stats_classifies_missing_playlist_as_custom_fallback(monkeypatch):
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
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 9, "Deaths": 2, "Assists": 1, "Medals": []}}}],
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT10M",
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }

    session = _FakeSession([_FakeResponse(200, json_data=stats_payload)])
    result = await client.get_match_stats_for_match("match-custom-fallback", "123", session)

    assert result is not None
    assert result["is_ranked"] is False
    assert result["match_category"] == "custom"
    assert result["category_source"] == "missing_playlist_fallback"


@pytest.mark.asyncio
async def test_get_match_stats_classifies_custom_from_structural_matchinfo(monkeypatch):
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
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 9, "Deaths": 2, "Assists": 1, "Medals": []}}}],
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT10M",
            "LifecycleMode": 1,
            "GameVariantCategory": 6,
            "Playlist": None,
            "PlaylistExperience": None,
            "PlaylistMapModePair": None,
            "UgcGameVariant": {"AssetKind": 6, "AssetId": "ugc-a", "VersionId": "ugc-v"},
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }

    session = _FakeSession([_FakeResponse(200, json_data=stats_payload)])
    result = await client.get_match_stats_for_match("match-custom-struct", "123", session)

    assert result is not None
    assert result["is_ranked"] is False
    assert result["match_category"] == "custom"
    assert result["category_source"] == "matchinfo_structural"


@pytest.mark.asyncio
async def test_get_match_stats_classifies_ranked_playlist_case_insensitively(monkeypatch):
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
                "PlayerTeamStats": [{"Stats": {"CoreStats": {"Kills": 10, "Deaths": 5, "Assists": 0, "Medals": []}}}],
            }
        ],
        "MatchInfo": {
            "StartTime": "2026-01-01T00:00:00",
            "Duration": "PT10M",
            "Playlist": {"AssetId": "6E4E9372-5D49-4F87-B0A7-4489B5E96A0B", "VersionId": "v1"},
            "MapVariant": {"AssetId": "m1", "VersionId": "mv1"},
        },
    }

    session = _FakeSession([_FakeResponse(200, json_data=stats_payload)])
    result = await client.get_match_stats_for_match("match-ranked-upper", "123", session)

    assert result is not None
    assert result["is_ranked"] is True
    assert result["match_category"] == "ranked"
    assert result["category_source"] == "playlist_map"


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
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "set_backoff", lambda *, account_index, seconds: backoffs.append((account_index, seconds)))

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
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "set_backoff", lambda *, account_index, seconds: None)

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


def test_parse_retry_after():
    from src.api.client import _parse_retry_after

    assert _parse_retry_after("1") == 1.0
    assert _parse_retry_after("2.5") == 2.5
    assert _parse_retry_after("0") == 0.0
    # Absent, empty, unparseable and negative all fall back to our own backoff
    # rather than to a guess.
    for bad in (None, "", "soon", "-3", "Wed, 21 Oct 2026 07:28:00 GMT"):
        assert _parse_retry_after(bad) is None


def test_page_listing_backoff_honours_retry_after():
    """A 429 must wait roughly what the server asked, not a fixed 30s.

    Halo answers page-listing 429s with `Retry-After: 1` (verified live). The
    old code did max(int(retry_after), 30), so it slept 30x the requested time;
    five of those turned the page-listing phase of a 670-match crawl into 61 of
    its 105 seconds.
    """
    from src.api.client import (
        RATE_LIMIT_BASE_BACKOFF,
        RATE_LIMIT_MAX_BACKOFF,
        _parse_retry_after,
    )

    def wait_for(retry_after, attempt):
        hint = _parse_retry_after(retry_after)
        backoff = RATE_LIMIT_BASE_BACKOFF * (2 ** attempt)
        return min(max(hint or 0.0, backoff), RATE_LIMIT_MAX_BACKOFF)

    # The measured real-world case: server says 1, first attempt.
    assert wait_for("1", 0) == 1.0

    # Repeated refusals still escalate, so a genuinely angry API is respected.
    ladder = [wait_for("1", a) for a in range(5)]
    assert ladder == [1.0, 2.0, 4.0, 8.0, 16.0]

    # A server asking for longer than our ladder wins...
    assert wait_for("45", 0) == 45.0
    # ...but cannot park a worker indefinitely.
    assert wait_for("3600", 0) == RATE_LIMIT_MAX_BACKOFF

    # No header: fall back to our own escalation, not to zero.
    assert wait_for(None, 0) == RATE_LIMIT_BASE_BACKOFF


@pytest.mark.asyncio
async def test_refresh_survives_the_caller_giving_up():
    """A caller walking away must not kill the crawl.

    The site aborts at 250s and the API at 235s, but a first crawl of a player
    with thousands of matches runs far longer than either - so the request that
    starts one always walks away from it. Awaiting a task normally forwards the
    awaiter's cancellation into it, which killed the crawl (silently, since
    CancelledError is a BaseException and _do_refresh only catches Exception).
    Observed live: a 10,000-match crawl went quiet mid-run and saved nothing.
    """
    import asyncio
    from src.web import internal_api

    internal_api._inflight_lock = asyncio.Lock()
    internal_api._inflight.clear()
    internal_api._global_fetch_times.clear()

    started = asyncio.Event()
    finished = asyncio.Event()

    async def slow_refresh(gamertag, xuid):
        started.set()
        await asyncio.sleep(0.3)          # stands in for a long crawl
        finished.set()
        return {"error": 0, "cache_info": "Processed 5 matches (5 new)"}

    internal_api._do_refresh = slow_refresh

    caller = asyncio.create_task(internal_api._refresh_coalesced("Big", "x"))
    await started.wait()
    caller.cancel()                        # the browser/API gives up
    with pytest.raises(asyncio.CancelledError):
        await caller

    # The crawl itself must still be running, and must still complete.
    await asyncio.wait_for(finished.wait(), timeout=2)
    assert finished.is_set(), "crawl was killed when its caller gave up"

    inflight = internal_api._inflight.get("big")
    assert inflight is not None, "a surviving crawl must stay registered"
    await inflight
    assert inflight.done()


@pytest.mark.asyncio
async def test_surviving_crawl_is_not_duplicated_by_the_next_caller():
    """The in-flight entry must outlive a cancelled awaiter, or the next
    request starts a second crawl of the same player alongside the first."""
    import asyncio
    from src.web import internal_api

    internal_api._inflight_lock = asyncio.Lock()
    internal_api._inflight.clear()
    internal_api._global_fetch_times.clear()

    starts = []

    async def slow_refresh(gamertag, xuid):
        starts.append(gamertag)
        await asyncio.sleep(0.3)
        return {"error": 0, "cache_info": "Processed 1 matches (1 new)"}

    internal_api._do_refresh = slow_refresh

    first = asyncio.create_task(internal_api._refresh_coalesced("Big", "x"))
    await asyncio.sleep(0.05)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    # A second caller arrives while the first crawl is still going.
    second = await internal_api._refresh_coalesced("Big", "x")
    assert second["error"] == 0
    assert starts == ["Big"], f"crawl started {len(starts)} times: {starts}"


# --- Remembered empty-history verdict -------------------------------------
#
# The freshness gate is global across every viewer, so only the FIRST person to
# open a private player gets a refresh that reports "private". Everyone else
# arriving inside the freshness window gets a skip, and their page has nothing
# to explain the empty history with unless the skip carries the last verdict.


def _fresh_request(gamertag="Tester", token="secret"):
    """Minimal stand-in for the aiohttp request handle_refresh actually uses."""

    class _Req:
        headers = {"X-Internal-Token": token}

        async def json(self):
            return {"gamertag": gamertag}

    return _Req()


def test_stamping_a_check_records_its_verdict():
    client = HaloAPIClient()

    client._stamp_history_checked("x1", "private")
    assert client.last_history_visibility("x1") == "private"

    # A later check that saw a real history clears it - this is what stops a
    # player who opens their account back up being called private forever.
    client._stamp_history_checked("x1", None)
    assert client.last_history_visibility("x1") is None

    # Anything the bot did not actually establish is stored as "don't know".
    client._stamp_history_checked("x2", "some_unexpected_value")
    assert client.last_history_visibility("x2") is None

    # An xuid never checked reads the same as one checked and found non-empty;
    # history_checked_age_seconds is what separates them.
    assert client.last_history_visibility("never-seen") is None
    assert client.history_checked_age_seconds("never-seen") is None
    assert client.history_checked_age_seconds("x1") is not None


@pytest.mark.asyncio
async def test_fresh_skip_carries_the_last_private_verdict(monkeypatch):
    import json

    from src.config import settings
    from src.web import internal_api

    monkeypatch.setattr(settings, "INTERNAL_STATS_REFRESH_TOKEN", "secret")
    monkeypatch.setattr(settings, "WEB_AUTOREFRESH_FRESHNESS_SECONDS", 300)
    monkeypatch.setattr(
        internal_api.api_client.stats_cache, "resolve_xuid_by_gamertag", lambda g: "x1"
    )
    monkeypatch.setattr(
        internal_api.api_client, "history_checked_age_seconds", lambda xuid: 12.0
    )
    monkeypatch.setattr(
        internal_api.api_client, "last_history_visibility", lambda xuid: "private"
    )

    resp = await internal_api.handle_refresh(_fresh_request())
    body = json.loads(resp.body)

    assert body["skipped"] is True
    # `reason` still says why we skipped; the verdict rides alongside it, or
    # clients switching on "fresh" would break.
    assert body["reason"] == "fresh"
    assert body["last_reason"] == "private"


@pytest.mark.asyncio
async def test_fresh_skip_omits_the_verdict_when_the_history_was_real(monkeypatch):
    """Absence is the answer here, not a gap: reaching the gate proves a check
    completed, so no verdict means it saw a genuine history."""
    import json

    from src.config import settings
    from src.web import internal_api

    monkeypatch.setattr(settings, "INTERNAL_STATS_REFRESH_TOKEN", "secret")
    monkeypatch.setattr(settings, "WEB_AUTOREFRESH_FRESHNESS_SECONDS", 300)
    monkeypatch.setattr(
        internal_api.api_client.stats_cache, "resolve_xuid_by_gamertag", lambda g: "x1"
    )
    monkeypatch.setattr(
        internal_api.api_client, "history_checked_age_seconds", lambda xuid: 12.0
    )
    monkeypatch.setattr(
        internal_api.api_client, "last_history_visibility", lambda xuid: None
    )

    body = json.loads((await internal_api.handle_refresh(_fresh_request())).body)

    assert body["reason"] == "fresh"
    assert "last_reason" not in body
