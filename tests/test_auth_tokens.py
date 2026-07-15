import asyncio
import time

import pytest

from src.auth.tokens import AuthenticationManager, HaloAuth, TokenCache


class _CancelledRequestContext:
    async def __aenter__(self):
        raise asyncio.CancelledError()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _TimeoutRequestContext:
    async def __aenter__(self):
        raise asyncio.TimeoutError()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, request_context):
        self._request_context = request_context

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, *args, **kwargs):
        return self._request_context


@pytest.mark.asyncio
async def test_request_clearance_returns_placeholder_on_cancelled(monkeypatch):
    from src.auth import tokens as tokens_module

    original_sleep = asyncio.sleep
    monkeypatch.setattr(tokens_module.aiohttp, "ClientSession", lambda **kwargs: _FakeSession(_CancelledRequestContext()))
    monkeypatch.setattr(tokens_module.asyncio, "sleep", lambda *_: original_sleep(0))

    result = await HaloAuth.request_clearance("spartan-token", "12345")

    assert result is not None
    assert result["token"] == "skip"
    assert result["FlightConfigurationId"] == "skip"


def test_token_cache_get_accepts_default(tmp_path):
    cache_file = tmp_path / "cache.json"
    cache = TokenCache(str(cache_file))

    assert cache.get("missing_key") is None
    assert cache.get("missing_key", {}) == {}
    assert cache.get("missing_key", {}).get("xuid") is None


@pytest.mark.asyncio
async def test_get_clearance_token_falls_back_to_cached_spartan(monkeypatch, tmp_path):
    """Spartan valid but clearance/xsts stale should hit the cached-Spartan cascade
    branch (tokens.py get_clearance_token) without raising TypeError from
    TokenCache.get(key, default)."""
    from src.auth import tokens as tokens_module

    cache_file = tmp_path / "cache.json"
    manager = AuthenticationManager("client-id", "client-secret", cache_file=str(cache_file))
    manager.cache.update({
        "spartan": {"token": "spartan-token", "expires_at": time.time() + 3600},
        "xsts": {"token": "xsts-token", "expires_at": time.time() + 3600, "xuid": "12345"},
    })

    async def fake_request_clearance(spartan_token, xuid):
        assert spartan_token == "spartan-token"
        assert xuid == "12345"
        return {"token": "clearance-token", "FlightConfigurationId": "clearance-token", "expires_at": time.time() + 86400}

    monkeypatch.setattr(tokens_module.HaloAuth, "request_clearance", staticmethod(fake_request_clearance))

    result = await manager.get_clearance_token()

    assert result == "clearance-token"
    assert manager.cache.get("clearance")["token"] == "clearance-token"


@pytest.mark.asyncio
async def test_request_clearance_returns_placeholder_on_timeout(monkeypatch):
    from src.auth import tokens as tokens_module

    original_sleep = asyncio.sleep
    monkeypatch.setattr(tokens_module.aiohttp, "ClientSession", lambda **kwargs: _FakeSession(_TimeoutRequestContext()))
    monkeypatch.setattr(tokens_module.asyncio, "sleep", lambda *_: original_sleep(0))

    result = await HaloAuth.request_clearance("spartan-token", "12345")

    assert result is not None
    assert result["token"] == "skip"
    assert result["FlightConfigurationId"] == "skip"
