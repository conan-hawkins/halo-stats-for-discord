import copy

import pytest

from src.bot import tasks as bot_tasks


@pytest.mark.asyncio
async def test_auto_refresh_tokens_calls_ensure_valid_tokens(monkeypatch):
    called = {"count": 0}

    class FakeStats:
        async def ensure_valid_tokens(self):
            called["count"] += 1
            return True

    monkeypatch.setattr(bot_tasks, "StatsFind1", FakeStats())

    await bot_tasks.auto_refresh_tokens.coro()
    assert called["count"] == 1


@pytest.mark.asyncio
async def test_auto_refresh_tokens_handles_failed_validation(monkeypatch):
    called = {"count": 0}

    class FakeStats:
        async def ensure_valid_tokens(self):
            called["count"] += 1
            return False

    monkeypatch.setattr(bot_tasks, "StatsFind1", FakeStats())

    await bot_tasks.auto_refresh_tokens.coro()
    assert called["count"] == 1


@pytest.mark.asyncio
async def test_proactive_refresh_restores_primary_cache_on_account_failure(monkeypatch):
    from src import config as config_module
    from src.api import utils as utils_module
    from src.auth import tokens as tokens_module

    token_cache_path = str(config_module.TOKEN_CACHE_FILE)
    acc2_path = str(config_module.get_token_cache_path(2))

    acc1_bundle = {
        "oauth": {"refresh_token": "rt-1"},
        "spartan": {"token": "s1", "expires_at": 9999999999},
        "xsts": {"token": "x1", "expires_at": 9999999999},
        "xsts_xbox": {"token": "xx1", "uhs": "u1", "expires_at": 9999999999},
    }
    acc2_bundle = {
        "oauth": {"refresh_token": "rt-2"},
        "spartan": {"token": "s2", "expires_at": 0},
        "xsts": {"token": "x2", "expires_at": 0},
        "xsts_xbox": {"token": "xx2", "uhs": "u2", "expires_at": 0},
    }

    store = {
        token_cache_path: copy.deepcopy(acc1_bundle),
        acc2_path: copy.deepcopy(acc2_bundle),
    }
    writes = []

    def fake_read(path, default=None):
        return copy.deepcopy(store.get(str(path), default))

    def fake_write(path, data, indent=2):
        store[str(path)] = copy.deepcopy(data)
        writes.append((str(path), copy.deepcopy(data)))

    async def fail_auth(*args, **kwargs):
        raise RuntimeError("proactive refresh failure")

    monkeypatch.setattr(utils_module, "safe_read_json", fake_read)
    monkeypatch.setattr(utils_module, "safe_write_json", fake_write)
    monkeypatch.setattr(tokens_module, "run_auth_flow", fail_auth)

    class FakeClient:
        client_id = "cid"
        client_secret = "secret"

    monkeypatch.setattr(bot_tasks, "api_client", FakeClient())

    await bot_tasks.proactive_token_refresh.coro()

    token_cache_writes = [w for w in writes if w[0] == token_cache_path]
    assert token_cache_writes
    assert token_cache_writes[-1][1]["spartan"]["token"] == "s1"


@pytest.mark.asyncio
async def test_proactive_refresh_force_expires_tokens_before_auth(monkeypatch):
    from src import config as config_module
    from src.api import utils as utils_module
    from src.auth import tokens as tokens_module

    token_cache_path = str(config_module.TOKEN_CACHE_FILE)
    acc2_path = str(config_module.get_token_cache_path(2))

    acc1_bundle = {
        "oauth": {"refresh_token": "rt-1"},
        "spartan": {"token": "s1", "expires_at": 9999999999},
        "xsts": {"token": "x1", "expires_at": 9999999999},
        "xsts_xbox": {"token": "xx1", "uhs": "u1", "expires_at": 9999999999},
    }
    acc2_bundle = {
        "oauth": {"refresh_token": "rt-2"},
        "spartan": {"token": "s2", "expires_at": 9999999999},
        "xsts": {"token": "x2", "expires_at": 9999999999},
        "xsts_xbox": {"token": "xx2", "uhs": "u2", "expires_at": 9999999999},
    }

    store = {
        token_cache_path: copy.deepcopy(acc1_bundle),
        acc2_path: copy.deepcopy(acc2_bundle),
    }
    auth_seen = {"expired_before_auth": False}

    def fake_read(path, default=None):
        return copy.deepcopy(store.get(str(path), default))

    def fake_write(path, data, indent=2):
        store[str(path)] = copy.deepcopy(data)

    def fake_valid(token_info):
        return bool(token_info and token_info.get("expires_at", 0) > 0)

    async def fake_auth(*args, **kwargs):
        current = store[token_cache_path]
        auth_seen["expired_before_auth"] = (
            current.get("spartan", {}).get("expires_at") == 0 and
            current.get("xsts", {}).get("expires_at") == 0 and
            current.get("xsts_xbox", {}).get("expires_at") == 0
        )
        # Simulate refreshed tokens.
        store[token_cache_path] = {
            "oauth": {"refresh_token": "rt-2"},
            "spartan": {"token": "s2-new", "expires_at": 9999999999},
            "xsts": {"token": "x2-new", "expires_at": 9999999999},
            "xsts_xbox": {"token": "xx2-new", "uhs": "u2", "expires_at": 9999999999},
        }

    monkeypatch.setattr(utils_module, "safe_read_json", fake_read)
    monkeypatch.setattr(utils_module, "safe_write_json", fake_write)
    monkeypatch.setattr(utils_module, "is_token_valid", fake_valid)
    monkeypatch.setattr(tokens_module, "run_auth_flow", fake_auth)

    class FakeClient:
        client_id = "cid"
        client_secret = "secret"

    monkeypatch.setattr(bot_tasks, "api_client", FakeClient())

    await bot_tasks.proactive_token_refresh.coro()

    assert auth_seen["expired_before_auth"] is True
    assert store[acc2_path]["spartan"]["token"] == "s2-new"
    assert store[token_cache_path]["spartan"]["token"] == "s1"


@pytest.mark.asyncio
async def test_proactive_refresh_does_not_persist_invalid_refreshed_tokens(monkeypatch):
    from src import config as config_module
    from src.api import utils as utils_module
    from src.auth import tokens as tokens_module

    token_cache_path = str(config_module.TOKEN_CACHE_FILE)
    acc2_path = str(config_module.get_token_cache_path(2))

    acc1_bundle = {
        "oauth": {"refresh_token": "rt-1"},
        "spartan": {"token": "s1", "expires_at": 9999999999},
        "xsts": {"token": "x1", "expires_at": 9999999999},
        "xsts_xbox": {"token": "xx1", "uhs": "u1", "expires_at": 9999999999},
    }
    acc2_bundle = {
        "oauth": {"refresh_token": "rt-2"},
        "spartan": {"token": "s2", "expires_at": 9999999999},
        "xsts": {"token": "x2", "expires_at": 9999999999},
        "xsts_xbox": {"token": "xx2", "uhs": "u2", "expires_at": 9999999999},
    }

    store = {
        token_cache_path: copy.deepcopy(acc1_bundle),
        acc2_path: copy.deepcopy(acc2_bundle),
    }

    def fake_read(path, default=None):
        return copy.deepcopy(store.get(str(path), default))

    def fake_write(path, data, indent=2):
        store[str(path)] = copy.deepcopy(data)

    def fake_valid(token_info):
        return bool(token_info and token_info.get("expires_at", 0) > 0)

    async def fake_auth(*args, **kwargs):
        # Simulate refresh flow returning invalid/expired derived tokens.
        store[token_cache_path] = {
            "oauth": {"refresh_token": "rt-2"},
            "spartan": {"token": "s2-bad", "expires_at": 0},
            "xsts": {"token": "x2-bad", "expires_at": 0},
            "xsts_xbox": {"token": "xx2-bad", "uhs": "u2", "expires_at": 0},
        }

    monkeypatch.setattr(utils_module, "safe_read_json", fake_read)
    monkeypatch.setattr(utils_module, "safe_write_json", fake_write)
    monkeypatch.setattr(utils_module, "is_token_valid", fake_valid)
    monkeypatch.setattr(tokens_module, "run_auth_flow", fake_auth)

    class FakeClient:
        client_id = "cid"
        client_secret = "secret"

    monkeypatch.setattr(bot_tasks, "api_client", FakeClient())

    await bot_tasks.proactive_token_refresh.coro()

    # Account 2 cache should remain unchanged because refreshed bundle was invalid.
    assert store[acc2_path]["spartan"]["token"] == "s2"
    assert store[token_cache_path]["spartan"]["token"] == "s1"
