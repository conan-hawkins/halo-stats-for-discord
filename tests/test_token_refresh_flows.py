import copy
import time

import pytest

from src.api.client import HaloAPIClient


def _valid_bundle(token_name: str):
    return {
        "oauth": {"refresh_token": f"rt-{token_name}"},
        "spartan": {"token": f"spartan-{token_name}", "expires_at": time.time() + 3600},
        "xsts": {"token": f"xsts-{token_name}", "expires_at": time.time() + 3600},
        "xsts_xbox": {
            "token": f"xstsx-{token_name}",
            "uhs": f"uhs-{token_name}",
            "expires_at": time.time() + 3600,
        },
    }


def _invalid_bundle(token_name: str, with_refresh: bool = True):
    data = {
        "spartan": {"token": f"spartan-{token_name}", "expires_at": 0},
        "xsts": {"token": f"xsts-{token_name}", "expires_at": 0},
        "xsts_xbox": {
            "token": f"xstsx-{token_name}",
            "uhs": f"uhs-{token_name}",
            "expires_at": 0,
        },
    }
    if with_refresh:
        data["oauth"] = {"refresh_token": f"rt-{token_name}"}
    return data


def _install_store(monkeypatch, store):
    from src.api import client as client_module

    writes = []

    def read_json(path, default=None):
        key = str(path)
        return copy.deepcopy(store.get(key, default))

    def write_json(path, data, indent=2):
        key = str(path)
        store[key] = copy.deepcopy(data)
        writes.append((key, copy.deepcopy(data)))

    monkeypatch.setattr(client_module, "safe_read_json", read_json)
    monkeypatch.setattr(client_module, "safe_write_json", write_json)
    return client_module, writes


@pytest.mark.asyncio
async def test_ensure_valid_tokens_returns_false_when_refresh_already_in_progress():
    client = HaloAPIClient()
    client._refresh_in_progress = True

    assert await client.ensure_valid_tokens() is False


@pytest.mark.asyncio
async def test_ensure_valid_tokens_respects_refresh_cooldown(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    cache = _invalid_bundle("acc1", with_refresh=True)

    monkeypatch.setattr(
        client_module,
        "safe_read_json",
        lambda path, default=None: copy.deepcopy(cache) if str(path) == str(client_module.TOKEN_CACHE_FILE) else {},
    )
    monkeypatch.setattr(client_module, "is_token_valid", lambda info: False)

    called = {"auth": 0}

    async def fake_auth(*args, **kwargs):
        called["auth"] += 1

    monkeypatch.setattr(client_module, "run_auth_flow", fake_auth)
    monkeypatch.setattr(client_module.time, "time", lambda: 200.0)

    client._last_refresh_time = 180.0
    ok = await client.ensure_valid_tokens()

    assert ok is False
    assert called["auth"] == 0


@pytest.mark.asyncio
async def test_ensure_valid_tokens_restores_primary_cache_if_account_refresh_throws(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    acc1 = _valid_bundle("acc1")
    acc2 = _invalid_bundle("acc2", with_refresh=True)

    store = {
        str(client_module.TOKEN_CACHE_FILE): copy.deepcopy(acc1),
        str(client_module.get_token_cache_path(2)): copy.deepcopy(acc2),
    }
    _, writes = _install_store(monkeypatch, store)

    def valid_fn(info):
        return bool(info and info.get("expires_at", 0) > 0)

    monkeypatch.setattr(client_module, "is_token_valid", valid_fn)

    async def fail_auth(*args, **kwargs):
        raise RuntimeError("refresh fail")

    monkeypatch.setattr(client_module, "run_auth_flow", fail_auth)
    monkeypatch.setattr(client, "_load_xbox_accounts", lambda: None)

    ok = await client.ensure_valid_tokens()

    assert ok is True
    primary_path = str(client_module.TOKEN_CACHE_FILE)
    # Last write to token_cache should be restored Account 1 bundle.
    token_cache_writes = [entry for entry in writes if entry[0] == primary_path]
    assert token_cache_writes
    assert token_cache_writes[-1][1]["spartan"]["token"] == acc1["spartan"]["token"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valid_map",
    [
        {"spartan": True, "xsts": False, "xsts_xbox": True},
        {"spartan": False, "xsts": True, "xsts_xbox": True},
        {"spartan": True, "xsts": True, "xsts_xbox": False},
    ],
)
async def test_account1_requires_all_three_tokens(monkeypatch, valid_map):
    client = HaloAPIClient()

    from src.api import client as client_module

    bundle = _valid_bundle("acc1")
    for key in ("spartan", "xsts", "xsts_xbox"):
        if not valid_map[key]:
            bundle[key]["expires_at"] = 0

    store = {str(client_module.TOKEN_CACHE_FILE): bundle}
    _install_store(monkeypatch, store)

    monkeypatch.setattr(client_module, "is_token_valid", lambda info: bool(info and info.get("expires_at", 0) > 0))
    monkeypatch.setattr(client_module.time, "time", lambda: 200.0)
    client._last_refresh_time = 180.0  # forces cooldown branch when invalid

    ok = await client.ensure_valid_tokens()
    assert ok is False


@pytest.mark.asyncio
async def test_invalid_additional_account_without_refresh_token_is_skipped(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    acc1 = _valid_bundle("acc1")
    acc2 = _invalid_bundle("acc2", with_refresh=False)
    store = {
        str(client_module.TOKEN_CACHE_FILE): acc1,
        str(client_module.get_token_cache_path(2)): acc2,
    }
    _install_store(monkeypatch, store)

    monkeypatch.setattr(client_module, "is_token_valid", lambda info: bool(info and info.get("expires_at", 0) > 0))

    called = {"auth": 0}

    async def fake_auth(*args, **kwargs):
        called["auth"] += 1

    monkeypatch.setattr(client_module, "run_auth_flow", fake_auth)
    monkeypatch.setattr(client, "_load_xbox_accounts", lambda: None)

    ok = await client.ensure_valid_tokens()

    assert ok is True
    assert called["auth"] == 0
    assert len(client.spartan_accounts) == 1


@pytest.mark.asyncio
async def test_refresh_success_reloads_accounts_and_sets_primary_token(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    acc1_invalid = _invalid_bundle("acc1", with_refresh=True)
    acc2_valid = _valid_bundle("acc2")

    store = {
        str(client_module.TOKEN_CACHE_FILE): copy.deepcopy(acc1_invalid),
        str(client_module.get_token_cache_path(2)): copy.deepcopy(acc2_valid),
    }
    _install_store(monkeypatch, store)
    monkeypatch.setattr(client_module, "is_token_valid", lambda info: bool(info and info.get("expires_at", 0) > 0))

    async def fake_auth(*args, **kwargs):
        refreshed = _valid_bundle("acc1-refreshed")
        store[str(client_module.TOKEN_CACHE_FILE)] = refreshed

    monkeypatch.setattr(client_module, "run_auth_flow", fake_auth)

    tracker = {"loaded": 0}

    def fake_load_xbox_accounts():
        tracker["loaded"] += 1

    monkeypatch.setattr(client, "_load_xbox_accounts", fake_load_xbox_accounts)
    monkeypatch.setattr(client_module.time, "time", lambda: 999.0)
    client._last_refresh_time = 0.0

    ok = await client.ensure_valid_tokens()

    assert ok is True
    assert client.spartan_token == "spartan-acc1-refreshed"
    assert len(client.spartan_accounts) == 2
    assert tracker["loaded"] == 1
