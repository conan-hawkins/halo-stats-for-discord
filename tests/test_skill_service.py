"""CSR and MMR from skill.svc.

Payload shapes here are copied from real responses captured against the live
API (a Ranked Arena match, August 2026), not invented:

    playlist csr : csr=1429 tier='Diamond' subTier=4 seasonMax=1542 allTime=1560
    match skill  : TeamMmr=1524.84 preCsr=1423 postCsr=1433 expectedKills=17.0

The two behaviours worth defending are both "absence" cases, because both
would otherwise show up as a plausible-looking rank on a player page:

  - a player with no CSR in a playlist is reported as **-1**, not as an error
    or a null, so -1 must never reach a caller as a rank
  - an unranked match reports **0/0** for pre/post CSR rather than omitting
    the recap, so 0 must not be stored as a CSR either
"""

import asyncio
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlparse

import pytest

from src.api.client import HaloAPIClient
from src.api.rate_limiters import BUCKET_SKILL


class _FakeSkillRateLimiter:
    def __init__(self):
        self.buckets = []

    async def wait_if_needed(self, force_account=None, bucket=None):
        return 0

    @asynccontextmanager
    async def slot(self, force_account=None, bucket=None):
        self.buckets.append(bucket)
        yield await self.wait_if_needed(force_account, bucket)

    def set_backoff(self, seconds, account_index=None):
        return None

    def note_result(self, bucket=None, rate_limited=False):
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


def _install(monkeypatch, handler):
    from src.api import client as client_module

    limiter = _FakeSkillRateLimiter()
    requests = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            requests.append({"path": parsed.path, "query": query})
            return handler(parsed.path, query)

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", limiter)
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *a, **k: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *a, **k: object())
    return limiter, requests


def _client():
    client = HaloAPIClient()
    client.spartan_accounts = [{"id": "account1", "token": "tok-1", "name": "Account 1"}]
    return client


def _csr_entry(xuid, csr, tier="Diamond", sub_tier=4, season_max=1542, all_time=1560):
    return {
        "Id": f"xuid({xuid})",
        "Result": {
            "Current": {"Value": csr, "Tier": tier, "SubTier": sub_tier},
            "SeasonMax": {"Value": season_max},
            "AllTimeMax": {"Value": all_time},
        },
    }


def _skill_entry(xuid, pre, post, tier="Diamond", team_mmr=1524.84):
    return {
        "Id": f"xuid({xuid})",
        "Result": {
            "TeamMmr": team_mmr,
            "TeamId": 0,
            "RankRecap": {
                "PreMatchCsr": {"Value": pre},
                "PostMatchCsr": {"Value": post, "Tier": tier, "SubTier": 4},
            },
            "StatPerformances": {
                "Kills": {"Expected": 17.0, "Actual": 19},
                "Deaths": {"Expected": 15.5, "Actual": 12},
            },
        },
    }


# ---------------------------------------------------------------- playlist CSR

def test_playlist_csr_parses_a_real_response(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(200, {"Value": [_csr_entry("111", 1429)]}))

    result = asyncio.run(_client().get_playlist_csr("playlist-1", ["111"]))

    assert result == {"111": {
        "csr": 1429, "tier": "Diamond", "sub_tier": 4,
        "season_max": 1542, "all_time_max": 1560,
    }}


def test_playlist_csr_batches_every_player_into_one_request(monkeypatch):
    _, requests = _install(
        monkeypatch,
        lambda p, q: _FakeResponse(200, {"Value": [
            _csr_entry(x, 1400) for x in ("1", "2", "3", "4")
        ]}),
    )

    result = asyncio.run(_client().get_playlist_csr("playlist-1", ["1", "2", "3", "4"]))

    assert len(requests) == 1
    assert requests[0]["query"]["players"] == [
        "xuid(1)", "xuid(2)", "xuid(3)", "xuid(4)"
    ]
    assert len(result) == 4


def test_playlist_csr_omits_players_reported_as_minus_one(monkeypatch):
    # -1 means "no CSR in this playlist". Passing it through would render as a
    # rank of -1 for every player who has never touched ranked.
    _install(monkeypatch, lambda p, q: _FakeResponse(200, {"Value": [
        _csr_entry("111", 1429),
        _csr_entry("222", -1, tier="", sub_tier=0, season_max=-1, all_time=-1),
    ]}))

    result = asyncio.run(_client().get_playlist_csr("playlist-1", ["111", "222"]))

    assert "111" in result
    assert "222" not in result


def test_playlist_csr_uses_the_skill_bucket(monkeypatch):
    limiter, _ = _install(monkeypatch, lambda p, q: _FakeResponse(200, {"Value": []}))

    asyncio.run(_client().get_playlist_csr("playlist-1", ["1"]))

    assert limiter.buckets == [BUCKET_SKILL]


def test_playlist_csr_passes_season_when_given(monkeypatch):
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, {"Value": []}))

    asyncio.run(_client().get_playlist_csr("playlist-1", ["1"], season_id="CsrSeason13-3"))

    assert requests[0]["query"]["season"] == ["CsrSeason13-3"]


def test_playlist_csr_treats_404_as_no_data_not_an_error(monkeypatch):
    # A social playlist has no CSR concept and answers 404. That is a normal
    # answer for "does this player have a rank here", not a fault.
    _install(monkeypatch, lambda p, q: _FakeResponse(404, ""))

    assert asyncio.run(_client().get_playlist_csr("playlist-1", ["1"])) == {}


@pytest.mark.parametrize("status", [401, 429, 500])
def test_playlist_csr_returns_empty_on_failure(monkeypatch, status):
    _install(monkeypatch, lambda p, q: _FakeResponse(status, ""))

    assert asyncio.run(_client().get_playlist_csr("playlist-1", ["1"])) == {}


def test_playlist_csr_survives_a_thrown_exception(monkeypatch):
    def handler(path, query):
        raise RuntimeError("connection reset")

    _install(monkeypatch, handler)

    assert asyncio.run(_client().get_playlist_csr("playlist-1", ["1"])) == {}


def test_playlist_csr_ignores_empty_input(monkeypatch):
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, {"Value": []}))

    assert asyncio.run(_client().get_playlist_csr("playlist-1", [])) == {}
    assert asyncio.run(_client().get_playlist_csr("", ["1"])) == {}
    assert requests == []


# ------------------------------------------------------------------ match skill

def test_match_skill_parses_a_real_response(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_skill_entry("111", 1423, 1433)]}))

    result = asyncio.run(_client().get_match_skill("match-1", ["111"]))

    assert result["111"]["pre_csr"] == 1423
    assert result["111"]["post_csr"] == 1433
    assert result["111"]["tier"] == "Diamond"
    assert result["111"]["team_mmr"] == 1524.84
    assert result["111"]["expected_kills"] == 17.0
    assert result["111"]["expected_deaths"] == 15.5


def test_match_skill_covers_a_whole_roster_in_one_request(monkeypatch):
    # The cost model for phase 2 depends on this: one extra request per match,
    # not per player. Verified live at 4 players in one call.
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, {"Value": [
        _skill_entry(x, 1400, 1410) for x in ("1", "2", "3", "4")
    ]}))

    result = asyncio.run(_client().get_match_skill("match-1", ["1", "2", "3", "4"]))

    assert len(requests) == 1
    assert len(result) == 4


def test_match_skill_normalises_an_unranked_match_to_none(monkeypatch):
    # An unranked match returns the recap with 0/0 rather than omitting it.
    # Storing 0 would put every social player at CSR zero.
    _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_skill_entry("111", 0, 0, tier="")]}))

    result = asyncio.run(_client().get_match_skill("match-1", ["111"]))

    assert result["111"]["pre_csr"] is None
    assert result["111"]["post_csr"] is None
    assert result["111"]["tier"] is None
    # MMR is still meaningful for an unranked match.
    assert result["111"]["team_mmr"] == 1524.84


def test_match_skill_returns_empty_on_failure(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(429, ""))

    assert asyncio.run(_client().get_match_skill("match-1", ["1"])) == {}


def test_match_skill_ignores_malformed_entries(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(200, {"Value": [
        {"Id": "not-an-xuid", "Result": {}},
        {"Id": "xuid(111)", "Result": "not-a-dict"},
        _skill_entry("222", 1400, 1410),
    ]}))

    result = asyncio.run(_client().get_match_skill("match-1", ["111", "222"]))

    assert list(result) == ["222"]


# ----------------------------------------------------------------- current CSR

def test_current_csr_stops_at_the_first_playlist_with_a_rank(monkeypatch):
    # An active ranked player should normally cost one request, not one per
    # configured ranked playlist.
    hits = {"playlist-a": -1, "playlist-b": 1429}

    def handler(path, query):
        pid = path.split("/")[3]
        return _FakeResponse(200, {"Value": [_csr_entry("111", hits.get(pid, -1))]})

    _, requests = _install(monkeypatch, handler)

    result = asyncio.run(_client().get_current_csr(
        "111", playlist_ids=["playlist-a", "playlist-b", "playlist-c"]))

    assert result["csr"] == 1429
    assert result["playlist_id"] == "playlist-b"
    assert len(requests) == 2, "kept querying after finding a rank"


def test_current_csr_is_none_when_the_player_has_no_rank_anywhere(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_csr_entry("111", -1)]}))

    result = asyncio.run(_client().get_current_csr(
        "111", playlist_ids=["playlist-a", "playlist-b"]))

    assert result is None


def test_current_csr_defaults_to_the_configured_ranked_playlists(monkeypatch):
    from src.config import CORE_RANKED_PLAYLIST_IDS

    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_csr_entry("111", -1)]}))

    asyncio.run(_client().get_current_csr("111"))

    queried = {r["path"].split("/")[3] for r in requests}
    assert queried == set(CORE_RANKED_PLAYLIST_IDS)


# ------------------------------------------------- wiring into the stats path

def test_fill_missing_csr_only_pays_for_ranked_stat_types(monkeypatch):
    # An ordinary #stats command must not gain a skill.svc request.
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_csr_entry("111", 1429)]}))

    client = _client()
    for stat_type in ("overall", "social"):
        result = asyncio.run(client._fill_missing_csr("111", stat_type,
                                                      {"estimated_csr": None}))
        assert result["estimated_csr"] is None

    assert requests == [], "spent a request on a non-ranked stat type"


def test_fill_missing_csr_leaves_a_scraped_value_alone(monkeypatch):
    # If the match data did carry a CSR, that is the CSR for those matches and
    # must not be overwritten with the player's current rank.
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_csr_entry("111", 1429)]}))

    result = asyncio.run(_client()._fill_missing_csr(
        "111", "ranked", {"estimated_csr": 1200, "csr_tier": "Platinum"}))

    assert result["estimated_csr"] == 1200
    assert requests == []


def test_fill_missing_csr_populates_from_skill_when_absent(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_csr_entry("111", 1429)]}))

    result = asyncio.run(_client()._fill_missing_csr(
        "111", "ranked", {"games_played": 10, "estimated_csr": None}))

    assert result["estimated_csr"] == 1429
    assert result["csr_tier"] == "Diamond"
    assert result["csr_season_max"] == 1542
    assert result["csr_source"] == "skill.svc"
    assert result["games_played"] == 10, "dropped the stats it was enriching"


def test_fill_missing_csr_returns_stats_unchanged_when_skill_fails(monkeypatch):
    # A missing rank must never cost the user their stats.
    _install(monkeypatch, lambda p, q: _FakeResponse(500, ""))

    original = {"games_played": 10, "estimated_csr": None}
    result = asyncio.run(_client()._fill_missing_csr("111", "ranked", original))

    assert result == original


def test_fill_missing_csr_survives_an_exception(monkeypatch):
    def handler(path, query):
        raise RuntimeError("boom")

    _install(monkeypatch, handler)

    original = {"games_played": 10, "estimated_csr": None}
    result = asyncio.run(_client()._fill_missing_csr("111", "ranked", original))

    assert result == original


def test_fill_missing_csr_persists_what_it_fetched(monkeypatch):
    # The request has already been paid for; not storing it would let the
    # backfilled table go stale from the day it lands.
    _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_csr_entry("111", 1429)]}))

    written = []
    client = _client()

    class _FakeDb:
        def upsert_player_playlist_csr(self, *args):
            written.append(args)

    client.stats_cache = type("C", (), {"db": _FakeDb()})()

    result = asyncio.run(client._fill_missing_csr(
        "111", "ranked", {"games_played": 5, "estimated_csr": None}))

    assert result["estimated_csr"] == 1429
    assert len(written) == 1
    xuid, playlist, csr, tier, sub_tier, all_time = written[0]
    assert (xuid, csr, tier, all_time) == ("111", 1429, "Diamond", 1560)
    assert playlist, "playlist id was not carried through to the write"


def test_a_failed_csr_write_does_not_fail_the_command(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(
        200, {"Value": [_csr_entry("111", 1429)]}))

    client = _client()

    class _ExplodingDb:
        def upsert_player_playlist_csr(self, *args):
            raise RuntimeError("disk full")

    client.stats_cache = type("C", (), {"db": _ExplodingDb()})()

    result = asyncio.run(client._fill_missing_csr(
        "111", "ranked", {"games_played": 5, "estimated_csr": None}))

    assert result["estimated_csr"] == 1429
