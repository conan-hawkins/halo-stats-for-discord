"""Spartan token expiry provenance, and the refresh livelock it enabled.

Background: 343 returns the spartan token's real expiry as
<ExpiresUtc><ISO8601Date>...</ISO8601Date></ExpiresUtc>. The old parser read
ExpiresUtc's own .text - which is empty, because the instant lives in a child -
and silently fell back to time.time() + 86400. Cached tokens therefore claimed
24h when the grant is ~4h, and a dead token still read as VALID for ~20h.

That lie then jammed the hourly refresh: get_clearance_token saw a "valid"
spartan, took the cached-spartan shortcut, re-requested only clearance, and
returned before the expired XSTS was ever replaced.
"""
import copy
import time
from datetime import datetime, timezone

import pytest

from src.api.client import HaloAPIClient
from src.auth.tokens import HaloAuth, XboxAuth

NS = "http://schemas.datacontract.org/2004/07/Microsoft.Halo.RegisterClient.Bond"
EXPIRES_ISO = "2026-08-14T15:00:49Z"
EXPIRES_EPOCH = datetime(2026, 8, 14, 15, 0, 49, tzinfo=timezone.utc).timestamp()

REAL_XML = f"""<?xml version="1.0" encoding="utf-8"?>
<SpartanTokenV4Response xmlns="{NS}">
  <ExpiresUtc><ISO8601Date>{EXPIRES_ISO}</ISO8601Date></ExpiresUtc>
  <SpartanToken>v4=abc.def</SpartanToken>
  <TokenDuration>PT2H30M59.3046143S</TokenDuration>
</SpartanTokenV4Response>"""

REAL_JSON = ('{"SpartanToken":"v4=abc.def",'
             f'"ExpiresUtc":{{"ISO8601Date":"{EXPIRES_ISO}"}},'
             '"TokenDuration":"PT2H30M59.7S"}')


# ---------------------------------------------------------------- fake aiohttp
class _Response:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def post(self, *a, **k):
        return self._response


def _install_response(monkeypatch, status, body):
    from src.auth import tokens as tokens_module
    monkeypatch.setattr(tokens_module.aiohttp, "ClientSession",
                        lambda **k: _Session(_Response(status, body)))


# ------------------------------------------------- spartan expiry provenance
@pytest.mark.asyncio
@pytest.mark.parametrize("body,label", [(REAL_XML, "xml"), (REAL_JSON, "json")])
async def test_spartan_expiry_comes_from_the_server(monkeypatch, body, label):
    _install_response(monkeypatch, 201, body)
    result = await HaloAuth.request_spartan_token("xsts")

    assert result is not None, "a 201 with a token must never return None"
    assert result["token"] == "v4=abc.def"
    assert result["expires_at"] == pytest.approx(EXPIRES_EPOCH, abs=0.001)


@pytest.mark.asyncio
async def test_spartan_expiry_is_not_the_24h_guess(monkeypatch):
    """The specific regression: a ~2.5h grant recorded as 24h."""
    _install_response(monkeypatch, 201, REAL_XML)
    before = time.time()
    result = await HaloAuth.request_spartan_token("xsts")

    assert abs(result["expires_at"] - (before + 86400)) > 12 * 3600, \
        "expiry still looks like time.time() + 86400"


@pytest.mark.asyncio
async def test_spartan_expiry_survives_a_namespace_change(monkeypatch):
    _install_response(monkeypatch, 201, REAL_XML.replace(NS, "http://moved.invalid"))
    result = await HaloAuth.request_spartan_token("xsts")
    assert result is not None
    assert result["expires_at"] == pytest.approx(EXPIRES_EPOCH, abs=0.001)


@pytest.mark.asyncio
async def test_spartan_without_any_expiry_gets_a_short_ttl(monkeypatch):
    body = (f'<SpartanTokenV4Response xmlns="{NS}">'
            '<SpartanToken>v4=abc.def</SpartanToken></SpartanTokenV4Response>')
    _install_response(monkeypatch, 201, body)
    before = time.time()
    result = await HaloAuth.request_spartan_token("xsts")

    assert result is not None
    assert result["expires_at"] - before < 3600, \
        "an undateable token must be re-minted soon, not trusted for a day"


@pytest.mark.asyncio
@pytest.mark.parametrize("status,body", [
    (401, "unauthorized"), (500, "boom"), (201, "<not-xml"),
    (201, ""), (201, "{}"),
])
async def test_bad_spartan_responses_return_none(monkeypatch, status, body):
    _install_response(monkeypatch, status, body)
    assert await HaloAuth.request_spartan_token("xsts") is None


# --------------------------------------------------------- clearance placeholder
@pytest.mark.asyncio
async def test_failed_clearance_placeholder_is_short_lived(monkeypatch):
    """It may still be cached, but must not read as valid for a whole day."""
    from src.auth import tokens as tokens_module

    class _Ctx:
        async def __aenter__(self):
            raise TimeoutError()

        async def __aexit__(self, *a):
            return False

    class _S:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def get(self, *a, **k):
            return _Ctx()

    import asyncio
    original_sleep = asyncio.sleep
    monkeypatch.setattr(tokens_module.aiohttp, "ClientSession", lambda **k: _S())
    monkeypatch.setattr(tokens_module.asyncio, "sleep", lambda *_: original_sleep(0))

    before = time.time()
    result = await HaloAuth.request_clearance("spartan", "12345")

    assert result["token"] == "skip"          # shape preserved for callers
    assert result["expires_at"] - before < 3600, \
        "a failed clearance cached for 24h masks a persistent auth failure"


# ------------------------------------------------------------------- livelock
def _poisoned_account_cache():
    """The exact production state at 07:00 on 2026-08-14.

    spartan claims 24h of life (the lie) but is really dead; the XSTS that
    proved it has genuinely expired; the user token is still good.
    """
    now = time.time()
    return {
        "oauth": {"refresh_token": "rt-acc2", "expires_at": now - 3600},
        "user": {"token": "user-acc2", "expires_at": now + 5 * 3600},
        "xsts": {"token": "xsts-acc2", "expires_at": now - 6 * 3600, "xuid": "2533"},
        "xsts_xbox": {"token": "xstsx-acc2", "expires_at": now + 5 * 3600},
        "spartan": {"token": "spartan-acc2-DEAD", "expires_at": now + 14 * 3600},
        "clearance": {"token": "skip", "expires_at": now + 23 * 3600},
    }


def _install_store(monkeypatch, store):
    from src.api import client as client_module
    writes = []

    def read_json(path, default=None):
        return copy.deepcopy(store.get(str(path), default))

    def write_json(path, data, indent=2):
        store[str(path)] = copy.deepcopy(data)
        writes.append((str(path), copy.deepcopy(data)))

    monkeypatch.setattr(client_module, "safe_read_json", read_json)
    monkeypatch.setattr(client_module, "safe_write_json", write_json)
    monkeypatch.setattr(client_module, "write_token_swap_marker", lambda b, t: None)
    monkeypatch.setattr(client_module, "clear_token_swap_marker", lambda: None)
    monkeypatch.setattr(client_module, "recover_token_swap_marker", lambda: False)
    return client_module, writes


@pytest.mark.asyncio
async def test_swap_remints_xsts_when_spartan_looks_valid_but_xsts_expired(
        monkeypatch, tmp_path):
    """The livelock: without force-expiry the cascade only re-requests clearance.

    A dead spartan that still reads VALID makes get_clearance_token take the
    cached-spartan branch, so the expired XSTS is never replaced and the account
    can never rejoin the pool - every hour, indefinitely.

    Uses real files in tmp_path rather than an in-memory store: TokenCache
    opens the cache file directly, so a stubbed safe_read_json would leave the
    auth cascade reading an empty cache and the test would prove nothing.
    """
    import json as _json

    client = HaloAPIClient()
    client_module = client_module_ref()
    from src.auth import tokens as tokens_module

    poisoned = _poisoned_account_cache()
    acc1_path = tmp_path / "token_cache.json"
    acc2_path = tmp_path / "token_cache_account2.json"
    acc1_path.write_text(_json.dumps(
        {"spartan": {"token": "acc1-PRIMARY", "expires_at": time.time() + 3600}}))
    acc2_path.write_text(_json.dumps(poisoned))

    monkeypatch.setattr(client_module, "TOKEN_CACHE_FILE", str(acc1_path))
    monkeypatch.setattr(client_module, "get_token_cache_path", lambda n: str(acc2_path))
    monkeypatch.setattr(tokens_module, "TOKEN_CACHE_FILE", str(acc1_path))
    monkeypatch.setattr(client_module, "write_token_swap_marker", lambda b, t: None)
    monkeypatch.setattr(client_module, "clear_token_swap_marker", lambda: None)
    monkeypatch.setattr(client_module, "recover_token_swap_marker", lambda: False)

    calls = {"dual_xsts": 0, "spartan": 0, "clearance": 0}

    def fake_dual(user_token):
        calls["dual_xsts"] += 1
        return {"token": "xsts-FRESH", "expires_at": time.time() + 4 * 3600,
                "xuid": "2533", "uhs": "uhs",
                "xbox_token": "xstsx-FRESH",
                "xbox_expires_at": time.time() + 16 * 3600}

    async def fake_spartan(xsts_token):
        calls["spartan"] += 1
        assert xsts_token == "xsts-FRESH", "must mint from the NEW xsts"
        return {"token": "spartan-FRESH", "expires_at": time.time() + 4 * 3600}

    async def fake_clearance(spartan_token, xuid):
        calls["clearance"] += 1
        if spartan_token != "spartan-FRESH":
            # A dead spartan 401s; the code caches a "skip" placeholder.
            return {"token": "skip", "FlightConfigurationId": "skip",
                    "expires_at": time.time() + 300}
        return {"token": "clearance-FRESH", "FlightConfigurationId": "clearance-FRESH",
                "expires_at": time.time() + 86400}

    from src.auth import tokens as tokens_module
    monkeypatch.setattr(tokens_module.XboxAuth, "get_dual_xsts_tokens", staticmethod(fake_dual))
    monkeypatch.setattr(tokens_module.HaloAuth, "request_spartan_token", staticmethod(fake_spartan))
    monkeypatch.setattr(tokens_module.HaloAuth, "request_clearance", staticmethod(fake_clearance))

    ok = await client._refresh_account_via_swap(2, copy.deepcopy(poisoned))

    assert calls["dual_xsts"] == 1, (
        "LIVELOCK: the cascade never re-minted the XSTS - it short-circuited on "
        "the stale spartan and only re-requested clearance")
    assert ok is True, "refresh should now report success"

    written = _json.loads(acc2_path.read_text())
    assert written["xsts"]["token"] == "xsts-FRESH"
    assert written["spartan"]["token"] == "spartan-FRESH"
    assert written["oauth"]["refresh_token"] == "rt-acc2", \
        "the refresh token must survive the swap"

    # Account 1 must be back in the primary slot.
    assert _json.loads(acc1_path.read_text())["spartan"]["token"] == "acc1-PRIMARY"


def client_module_ref():
    from src.api import client as client_module
    return client_module


@pytest.mark.asyncio
async def test_swap_does_not_mutate_the_callers_cache(monkeypatch):
    """Force-expiry must work on a copy; the caller's dict is shared state."""
    client = HaloAPIClient()
    cm = client_module_ref()
    poisoned = _poisoned_account_cache()
    original = copy.deepcopy(poisoned)

    store = {str(cm.TOKEN_CACHE_FILE): {"spartan": {"token": "acc1"}},
             str(cm.get_token_cache_path(2)): copy.deepcopy(poisoned)}
    client_module, _ = _install_store(monkeypatch, store)

    async def noop_auth(*a, **k):
        return None

    monkeypatch.setattr(client_module, "run_auth_flow", noop_auth)
    await client._refresh_account_via_swap(2, poisoned)

    assert poisoned == original, "caller's cache dict was mutated in place"


@pytest.mark.asyncio
async def test_failed_swap_does_not_persist_broken_tokens(monkeypatch):
    """A refresh that produced nothing usable must leave the cache untouched."""
    client = HaloAPIClient()
    cm = client_module_ref()
    poisoned = _poisoned_account_cache()
    cache_file = str(cm.get_token_cache_path(2))
    store = {str(cm.TOKEN_CACHE_FILE): {"spartan": {"token": "acc1"}},
             cache_file: copy.deepcopy(poisoned)}
    client_module, _ = _install_store(monkeypatch, store)

    async def dead_auth(*a, **k):
        return None  # refresh token dead: nothing gets re-minted

    monkeypatch.setattr(client_module, "run_auth_flow", dead_auth)

    ok = await client._refresh_account_via_swap(2, copy.deepcopy(poisoned))

    assert ok is False
    assert store[cache_file]["spartan"]["token"] == "spartan-acc2-DEAD", (
        "a failed refresh overwrote the account cache with zeroed tokens")
    assert store[cache_file]["oauth"]["refresh_token"] == "rt-acc2", \
        "the refresh token must always survive a failed attempt"


@pytest.mark.asyncio
async def test_account1_cache_is_restored_after_swap(monkeypatch):
    """Account 1 must be put back whatever happens to the secondary."""
    client = HaloAPIClient()
    cm = client_module_ref()
    acc1 = {"spartan": {"token": "acc1-PRIMARY", "expires_at": time.time() + 3600}}
    store = {str(cm.TOKEN_CACHE_FILE): copy.deepcopy(acc1),
             str(cm.get_token_cache_path(2)): _poisoned_account_cache()}
    client_module, _ = _install_store(monkeypatch, store)

    async def boom(*a, **k):
        raise RuntimeError("network died mid-refresh")

    monkeypatch.setattr(client_module, "run_auth_flow", boom)

    with pytest.raises(RuntimeError):
        await client._refresh_account_via_swap(2, _poisoned_account_cache())

    assert store[str(cm.TOKEN_CACHE_FILE)]["spartan"]["token"] == "acc1-PRIMARY"
