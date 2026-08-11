"""Service record aggregates from halostats.svc.

Payload shape captured live (August 2026). The number that matters most for
these tests is the disagreement: for the measured player the service record
reported 474 matchmade games, /matches/count said 476, and the match list held
528. All three are "true" in their own terms, which is exactly why the service
record is attached under its own key rather than merged into computed stats -
blending them would make a real upstream disagreement look like a crawl bug.
"""

import asyncio
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlparse

import pytest

from src.api.client import HaloAPIClient


class _FakeRateLimiter:
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

    limiter = _FakeRateLimiter()
    requests = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            parsed = urlparse(url)
            requests.append({"path": parsed.path, "query": parse_qs(parsed.query)})
            return handler(parsed.path, parse_qs(parsed.query))

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", limiter)
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *a, **k: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *a, **k: object())
    return limiter, requests


def _client():
    client = HaloAPIClient()
    client.spartan_accounts = [{"id": "account1", "token": "tok-1", "name": "Account 1"}]
    return client


# Trimmed from a real Matchmade service record response.
RECORD = {
    "MatchesCompleted": 474,
    "Wins": 247,
    "Losses": 210,
    "Ties": 17,
    "TimePlayed": "PT120H30M",
    "CoreStats": {"Kills": 6302, "Deaths": 5800, "Assists": 1500},
    "Subqueries": {
        "SeasonIds": ["Seasons/Season6.json"],
        "PlaylistAssetIds": ["a446725e-b281-414c-a21e-31b8700e95a1"],
        "GameVariantCategories": [6, 15],
        "IsRanked": [False, True],
    },
}


def test_service_record_returns_the_payload(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    result = asyncio.run(_client().get_service_record("111"))

    assert result["MatchesCompleted"] == 474


def test_service_record_rejects_an_unpublished_match_type():
    # "campaign" 404s on the live API; failing loudly here beats a silent None
    # that looks like a network problem.
    with pytest.raises(ValueError):
        asyncio.run(_client().get_service_record("111", match_type="campaign"))


@pytest.mark.parametrize("match_type", ["Matchmade", "Custom", "Local"])
def test_service_record_accepts_each_published_type(monkeypatch, match_type):
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    asyncio.run(_client().get_service_record("111", match_type=match_type))

    assert requests[0]["path"].endswith(f"/{match_type}/servicerecord")


def test_service_record_sends_filters_for_matchmade(monkeypatch):
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    asyncio.run(_client().get_service_record(
        "111", game_variant_category=6, is_ranked=True))

    assert requests[0]["query"]["gameVariantCategory"] == ["6"]
    assert requests[0]["query"]["isRanked"] == ["true"]


def test_service_record_drops_filters_for_non_matchmade_types(monkeypatch):
    # The API only applies them to Matchmade; sending them elsewhere invites a
    # 400 for no benefit.
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    asyncio.run(_client().get_service_record("111", match_type="Custom", is_ranked=True))

    assert requests[0]["query"] == {}


@pytest.mark.parametrize("status", [401, 404, 429, 500])
def test_service_record_returns_none_on_failure(monkeypatch, status):
    _install(monkeypatch, lambda p, q: _FakeResponse(status, ""))

    assert asyncio.run(_client().get_service_record("111")) is None


def test_service_record_survives_an_exception(monkeypatch):
    def handler(path, query):
        raise RuntimeError("connection reset")

    _install(monkeypatch, handler)

    assert asyncio.run(_client().get_service_record("111")) is None


def test_summary_flattens_into_the_bot_stats_shape(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    summary = asyncio.run(_client().get_service_record_summary("111"))

    assert summary["games_played"] == 474
    assert summary["total_kills"] == 6302
    assert summary["kd_ratio"] == round(6302 / 5800, 2)
    assert summary["win_rate"] == "52.1%"
    assert summary["source"] == "servicerecord"


def test_summary_handles_a_player_with_no_games(monkeypatch):
    # Division by zero here would turn "new player" into a crash.
    _install(monkeypatch, lambda p, q: _FakeResponse(200, {
        "MatchesCompleted": 0, "Wins": 0, "Losses": 0, "Ties": 0,
        "CoreStats": {"Kills": 0, "Deaths": 0, "Assists": 0},
    }))

    summary = asyncio.run(_client().get_service_record_summary("111"))

    assert summary["games_played"] == 0
    assert summary["kd_ratio"] == 0
    assert summary["win_rate"] == "0%"


# ------------------------------------------------- wiring into the stats path

def test_attach_skipped_when_the_crawl_already_has_matches(monkeypatch):
    # The whole point is filling a gap. A player with computed stats must not
    # cost an extra request.
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    stats = {"games_played": 120}
    result = asyncio.run(_client()._attach_service_record("111", stats))

    assert result == stats
    assert requests == []


def test_attach_fills_the_gap_for_a_cold_player(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    result = asyncio.run(_client()._attach_service_record("111", {"games_played": 0}))

    assert result["service_record"]["games_played"] == 474
    assert result["games_played"] == 0, "overwrote the computed stats"


def test_attach_keeps_the_two_sources_separate(monkeypatch):
    # 343's accounting and the crawl disagree by design. Merging them would
    # make that look like a bug in the crawl.
    _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    result = asyncio.run(_client()._attach_service_record("111", {"games_played": 0}))

    assert result["service_record"]["source"] == "servicerecord"
    assert "source" not in result


def test_attach_skipped_for_a_private_account(monkeypatch):
    # "private" is already the more useful answer, and the record would only
    # mislead.
    _, requests = _install(monkeypatch, lambda p, q: _FakeResponse(200, RECORD))

    result = asyncio.run(_client()._attach_service_record(
        "111", {"games_played": 0}, history_visibility="private"))

    assert "service_record" not in result
    assert requests == []


def test_attach_is_a_no_op_when_the_record_is_empty(monkeypatch):
    # A genuinely new player has 0 games everywhere; attaching an all-zero
    # block would just be noise.
    _install(monkeypatch, lambda p, q: _FakeResponse(200, {
        "MatchesCompleted": 0, "CoreStats": {}}))

    result = asyncio.run(_client()._attach_service_record("111", {"games_played": 0}))

    assert "service_record" not in result


def test_attach_never_fails_the_command(monkeypatch):
    _install(monkeypatch, lambda p, q: _FakeResponse(500, ""))

    stats = {"games_played": 0}
    assert asyncio.run(_client()._attach_service_record("111", stats)) == stats
