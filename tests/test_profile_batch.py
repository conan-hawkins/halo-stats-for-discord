"""Batch XUID -> gamertag resolution via profile.svc /users?xuids=.

Two measured behaviours of that endpoint drive every test here:

  - it accepts exactly 100 ids per call and returns 400 on 101
  - ONE unknown id returns 400 for the WHOLE batch, rather than being omitted
    from the results

The second is the entire reason `_resolve_chunk` halves instead of failing,
and it is the failure mode nothing else in this suite would catch: losing 99
good gamertags to one bad id looks exactly like "those players had no
gamertag" downstream.
"""

import asyncio
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlparse

import pytest

from src.api.client import HaloAPIClient
from src.api.rate_limiters import BUCKET_PROFILE


class _FakeProfileRateLimiter:
    """Matches the real limiter's shape, and records the bucket used."""

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


def _install(monkeypatch, handler, cache=None):
    """Point the resolver at `handler(xuids) -> _FakeResponse`.

    Also stubs the XUID cache, so no test touches the real 24MB cache file.
    Returns (limiter, requests, cache_state).
    """
    from src.api import client as client_module

    limiter = _FakeProfileRateLimiter()
    requests = []
    cache_state = dict(cache or {})

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            xuids = parse_qs(urlparse(url).query).get("xuids", [])
            requests.append(xuids)
            return handler(xuids)

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", limiter)
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *a, **k: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *a, **k: object())
    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: dict(cache_state))
    monkeypatch.setattr(client_module, "save_xuid_cache", cache_state.update)
    return limiter, requests, cache_state


def _ok(xuids, known=None):
    """A 200 naming every requested xuid, unless `known` restricts it."""
    return _FakeResponse(200, [
        {"xuid": x, "gamertag": f"Player{x}", "gamerpic": ""}
        for x in xuids if known is None or x in known
    ])


def _client():
    client = HaloAPIClient()
    client.spartan_accounts = [{"id": "account1", "token": "tok-1", "name": "Account 1"}]
    return client


def test_chunks_at_the_measured_ceiling(monkeypatch):
    # 250 ids must go out as 100 + 100 + 50. A chunk of 101 would 400, and the
    # endpoint gives no partial credit for the 100 good ids travelling with it.
    _, requests, _ = _install(monkeypatch, _ok)

    resolved = asyncio.run(_client().resolve_xuids_batch([str(1000 + i) for i in range(250)]))

    assert [len(r) for r in requests] == [100, 100, 50]
    assert len(resolved) == 250
    assert resolved["1000"] == "Player1000"


def test_uses_the_profile_bucket_not_a_match_bucket(monkeypatch):
    # Pacing identity lookups on a match bucket would let them eat the crawl's
    # request budget, which is the opposite of the point.
    limiter, _, _ = _install(monkeypatch, _ok)

    asyncio.run(_client().resolve_xuids_batch(["1", "2"]))

    assert limiter.buckets == [BUCKET_PROFILE]


def test_known_ids_come_from_cache_without_any_request(monkeypatch):
    _, requests, _ = _install(monkeypatch, _ok, cache={"1": "CachedOne", "2": "CachedTwo"})

    resolved = asyncio.run(_client().resolve_xuids_batch(["1", "2"]))

    assert resolved == {"1": "CachedOne", "2": "CachedTwo"}
    assert requests == []


def test_new_gamertags_are_written_back_to_the_cache(monkeypatch):
    _, _, cache_state = _install(monkeypatch, _ok, cache={"1": "CachedOne"})

    asyncio.run(_client().resolve_xuids_batch(["1", "2"]))

    assert cache_state["1"] == "CachedOne"   # cached entry survives the write
    assert cache_state["2"] == "Player2"     # newly learned entry persisted


def test_one_bad_id_does_not_cost_the_rest_their_gamertags(monkeypatch):
    bad = "9999"

    def handler(xuids):
        return _FakeResponse(400, "Bad Request") if bad in xuids else _ok(xuids)

    _, requests, _ = _install(monkeypatch, handler)

    client = _client()
    resolved = asyncio.run(client.resolve_xuids_batch([str(i) for i in range(7)] + [bad]))

    assert set(resolved) == {str(i) for i in range(7)}
    assert bad not in resolved
    assert bad in client._unresolvable_xuids
    # Halving, not one request per id.
    assert len(requests) < 8


def test_a_poisoned_id_is_not_resent_on_later_calls(monkeypatch):
    bad = "9999"

    def handler(xuids):
        return _FakeResponse(400, "Bad Request") if bad in xuids else _ok(xuids)

    _, requests, _ = _install(monkeypatch, handler)

    client = _client()
    asyncio.run(client.resolve_xuids_batch(["1", bad]))
    requests.clear()
    asyncio.run(client.resolve_xuids_batch(["2", bad]))

    assert all(bad not in r for r in requests)


def test_an_id_omitted_from_a_200_is_treated_as_unknown(monkeypatch):
    # A 200 that simply does not mention an id is the endpoint saying so.
    _install(monkeypatch, lambda xuids: _ok(xuids, known={"1"}))

    client = _client()
    resolved = asyncio.run(client.resolve_xuids_batch(["1", "2"]))

    assert resolved == {"1": "Player1"}
    assert "2" in client._unresolvable_xuids


@pytest.mark.parametrize("status", [401, 429, 500])
def test_transient_failures_never_mark_ids_unresolvable(monkeypatch, status):
    # 401/429/5xx say nothing about the ids themselves. Poisoning them would
    # let one bad minute lose those players for the rest of the session.
    _install(monkeypatch, lambda xuids: _FakeResponse(status, ""))

    client = _client()
    resolved = asyncio.run(client.resolve_xuids_batch(["1", "2"]))

    assert resolved == {}
    assert client._unresolvable_xuids == set()


def test_a_thrown_exception_leaves_ids_unresolved_rather_than_poisoned(monkeypatch):
    def handler(xuids):
        raise RuntimeError("connection reset")

    _install(monkeypatch, handler)

    client = _client()
    resolved = asyncio.run(client.resolve_xuids_batch(["1", "2"]))

    assert resolved == {}
    assert client._unresolvable_xuids == set()


def test_input_is_deduplicated_and_empty_input_costs_nothing(monkeypatch):
    _, requests, _ = _install(monkeypatch, _ok)

    client = _client()
    resolved = asyncio.run(client.resolve_xuids_batch(["7", "7", "7"]))

    assert requests == [["7"]]
    assert resolved == {"7": "Player7"}
    assert asyncio.run(client.resolve_xuids_batch([])) == {}
    assert asyncio.run(client.resolve_xuids_batch(["", None])) == {}
    assert len(requests) == 1


def test_cache_is_reread_before_writing_so_concurrent_updates_survive(monkeypatch):
    # resolve_xuids_batch writes the whole cache file. If it wrote the snapshot
    # it loaded at the start, a gamertag another task resolved meanwhile would
    # be silently dropped.
    from src.api import client as client_module

    _, _, cache_state = _install(monkeypatch, _ok)

    def _load_with_concurrent_writer():
        # Simulates another task having added an entry mid-flight.
        snapshot = dict(cache_state)
        cache_state["concurrent"] = "OtherTask"
        return snapshot

    monkeypatch.setattr(client_module, "load_xuid_cache", _load_with_concurrent_writer)

    asyncio.run(_client().resolve_xuids_batch(["5"]))

    assert cache_state["concurrent"] == "OtherTask"
    assert cache_state["5"] == "Player5"
