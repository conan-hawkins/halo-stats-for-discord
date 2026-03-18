import asyncio

import pytest

from src.auth.tokens import HaloAuth


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
