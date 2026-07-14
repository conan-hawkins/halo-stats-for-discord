import time

from src.api import utils as utils_module
from src.api.utils import is_token_valid, safe_read_json, safe_write_json


def test_safe_read_json_returns_default_for_missing_file(tmp_path):
    missing = tmp_path / "missing.json"
    result = safe_read_json(str(missing), default={"ok": True})
    assert result == {"ok": True}


def test_safe_write_json_then_read_roundtrip(tmp_path):
    target = tmp_path / "cache.json"
    payload = {"a": 1, "nested": {"b": 2}}

    safe_write_json(str(target), payload)
    read_back = safe_read_json(str(target), default={})

    assert read_back == payload


def test_safe_write_json_handles_unserializable_data(tmp_path):
    target = tmp_path / "bad.json"
    payload = {"bad": {1, 2, 3}}

    safe_write_json(str(target), payload)

    assert not target.exists()
    assert not (tmp_path / "bad.json.tmp").exists()


def test_is_token_valid_with_missing_token_info():
    assert is_token_valid(None) is False


def test_is_token_valid_with_expired_and_future_token():
    now = time.time()
    expired = {"expires_at": now - 10}
    valid = {"expires_at": now + 10}

    assert is_token_valid(expired) is False
    assert is_token_valid(valid) is True


def test_recover_token_swap_marker_restores_primary_cache(tmp_path, monkeypatch):
    token_cache = tmp_path / "token_cache.json"
    marker_file = tmp_path / "token_refresh_swap.json"
    backup_cache = {
        "oauth": {"refresh_token": "rt-1"},
        "spartan": {"token": "s1", "expires_at": 1234567899},
        "xsts": {"token": "x1", "expires_at": 1234567899},
        "xsts_xbox": {"token": "xx1", "uhs": "u1", "expires_at": 1234567899},
    }

    monkeypatch.setattr(utils_module, "TOKEN_CACHE_FILE", token_cache)
    monkeypatch.setattr(utils_module, "TOKEN_SWAP_MARKER_FILE", marker_file)

    safe_write_json(str(token_cache), {"spartan": {"token": "swapped"}})
    safe_write_json(
        str(marker_file),
        {
            "source_cache_file": str(token_cache),
            "target_cache_file": str(tmp_path / "token_cache_account2.json"),
            "backup_cache": backup_cache,
            "created_at": time.time(),
        },
    )

    assert utils_module.recover_token_swap_marker() is True
    assert safe_read_json(str(token_cache), default={}) == backup_cache
    assert not marker_file.exists()
