import asyncio
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


def test_proactive_refresh_invokes_swap_recovery(monkeypatch):
    from src.api import utils as utils_module

    called = {"recover": 0}

    def fake_recover():
        called["recover"] += 1
        return False

    monkeypatch.setattr(utils_module, "recover_token_swap_marker", fake_recover)
    monkeypatch.setattr(utils_module, "safe_read_json", lambda *args, **kwargs: {})

    monkeypatch.setattr(bot_tasks, "api_client", type("FakeClient", (), {"client_id": "cid", "client_secret": "secret"})())

    asyncio.run(bot_tasks.proactive_token_refresh.coro())

    assert called["recover"] == 1


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


def _valid_bundle(tag, spartan_exp=9999999999):
    return {
        "oauth": {"refresh_token": f"rt-{tag}", "expires_at": 9999999999},
        "spartan": {"token": f"s{tag}", "expires_at": spartan_exp},
        "xsts": {"token": f"x{tag}", "expires_at": 9999999999},
        "xsts_xbox": {"token": f"xx{tag}", "uhs": f"u{tag}", "expires_at": 9999999999},
    }


def _make_bare_client():
    """A HaloAPIClient with no __init__ side effects, just the pool attributes."""
    from src.api import client as client_module

    client = client_module.HaloAPIClient.__new__(client_module.HaloAPIClient)
    client.spartan_accounts = []
    client.spartan_token = None
    return client


@pytest.mark.asyncio
async def test_reload_spartan_accounts_from_cache_repopulates_all(monkeypatch):
    from src.api import client as client_module
    from src import config as config_module

    token_cache_path = str(config_module.TOKEN_CACHE_FILE)
    store = {token_cache_path: _valid_bundle(1)}
    for i in range(2, 6):
        store[str(config_module.get_token_cache_path(i))] = _valid_bundle(i)

    monkeypatch.setattr(
        client_module, "safe_read_json",
        lambda path, default=None: copy.deepcopy(store.get(str(path), default)),
    )
    seen = {}
    monkeypatch.setattr(
        client_module.halo_stats_rate_limiter, "set_num_accounts",
        lambda n: seen.__setitem__("n", n),
    )

    client = _make_bare_client()
    count = await client.reload_spartan_accounts_from_cache()

    assert count == 5
    assert [a["id"] for a in client.spartan_accounts] == [
        "account1", "account2", "account3", "account4", "account5",
    ]
    assert client.spartan_token == "s1"
    assert seen["n"] == 5


@pytest.mark.asyncio
async def test_reload_spartan_accounts_from_cache_omits_invalid_accounts(monkeypatch):
    from src.api import client as client_module
    from src import config as config_module

    token_cache_path = str(config_module.TOKEN_CACHE_FILE)
    store = {
        token_cache_path: _valid_bundle(1),
        str(config_module.get_token_cache_path(2)): _valid_bundle(2, spartan_exp=0),  # expired
        str(config_module.get_token_cache_path(3)): _valid_bundle(3),
        str(config_module.get_token_cache_path(4)): {},  # missing/empty
        str(config_module.get_token_cache_path(5)): {},  # missing/empty
    }

    monkeypatch.setattr(
        client_module, "safe_read_json",
        lambda path, default=None: copy.deepcopy(store.get(str(path), default)),
    )
    monkeypatch.setattr(
        client_module.halo_stats_rate_limiter, "set_num_accounts", lambda n: None
    )

    client = _make_bare_client()
    count = await client.reload_spartan_accounts_from_cache()

    assert count == 2
    assert [a["id"] for a in client.spartan_accounts] == ["account1", "account3"]


@pytest.mark.asyncio
async def test_reload_spartan_accounts_keeps_pool_when_nothing_valid(monkeypatch):
    from src.api import client as client_module
    from src import config as config_module

    # No cache files resolve to anything -> reload must not wipe the live pool.
    monkeypatch.setattr(
        client_module, "safe_read_json", lambda path, default=None: default
    )
    monkeypatch.setattr(
        client_module.halo_stats_rate_limiter, "set_num_accounts", lambda n: None
    )

    client = _make_bare_client()
    existing = [{"id": "account1", "token": "s1", "name": "Account 1"}]
    client.spartan_accounts = existing

    count = await client.reload_spartan_accounts_from_cache()

    assert count == 0
    assert client.spartan_accounts is existing  # untouched
