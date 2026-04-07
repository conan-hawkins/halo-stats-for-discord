from src.api import xuid_cache


def test_load_xuid_cache_uses_default(monkeypatch):
    calls = {}

    def fake_read(path, default=None):
        calls["path"] = path
        calls["default"] = default
        return {"1": "PlayerOne"}

    monkeypatch.setattr(xuid_cache, "safe_read_json", fake_read)
    result = xuid_cache.load_xuid_cache()

    assert calls["path"] == xuid_cache.XUID_CACHE_FILE
    assert calls["default"] == {}
    assert result == {"1": "PlayerOne"}


def test_save_xuid_cache_writes(monkeypatch):
    calls = []

    def fake_write(path, payload):
        calls.append((path, payload))

    monkeypatch.setattr(xuid_cache, "safe_write_json", fake_write)
    xuid_cache.save_xuid_cache({"1": "PlayerOne"})

    assert (xuid_cache.XUID_CACHE_FILE, {"1": "PlayerOne"}) in calls


def test_save_xuid_cache_handles_write_error(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(xuid_cache, "safe_write_json", boom)

    # Should not raise because save_xuid_cache catches exceptions.
    xuid_cache.save_xuid_cache({"1": "PlayerOne"})


def test_load_xuid_cache_flattens_metadata_entries(monkeypatch):
    def fake_read(path, default=None):
        if path == xuid_cache.XUID_CACHE_FILE:
            return {
                "1": "PlayerOne",
                "2": {"gamertag": "PlayerTwo", "previous_gamertags": ["OldTwo"]},
                "3": {"current_gamertag": "PlayerThree"},
                "4": {"unknown": "shape"},
                5: "NotStringXuid",
            }
        return default

    monkeypatch.setattr(xuid_cache, "safe_read_json", fake_read)

    result = xuid_cache.load_xuid_cache()
    assert result == {
        "1": "PlayerOne",
        "2": "PlayerTwo",
        "3": "PlayerThree",
    }


def test_save_xuid_cache_updates_sidecar_history_on_rename(monkeypatch):
    writes = {}

    def fake_read(path, default=None):
        if path == xuid_cache.XUID_CACHE_FILE:
            return {"1": "OldTag"}
        if path == xuid_cache.XUID_HISTORY_FILE:
            return {
                "1": {
                    "current_gamertag": "OldTag",
                    "previous_gamertags": [],
                    "updated_at": "2026-01-01T00:00:00+00:00",
                }
            }
        return default

    def fake_write(path, payload):
        writes[path] = payload

    monkeypatch.setattr(xuid_cache, "safe_read_json", fake_read)
    monkeypatch.setattr(xuid_cache, "safe_write_json", fake_write)

    xuid_cache.save_xuid_cache({"1": "NewTag"})

    assert writes[xuid_cache.XUID_CACHE_FILE] == {"1": "NewTag"}
    history_payload = writes[xuid_cache.XUID_HISTORY_FILE]
    assert history_payload["1"]["current_gamertag"] == "NewTag"
    assert history_payload["1"]["previous_gamertags"] == ["OldTag"]


def test_load_xuid_cache_full_merges_history(monkeypatch):
    def fake_read(path, default=None):
        if path == xuid_cache.XUID_CACHE_FILE:
            return {"1": "CurrentTag"}
        if path == xuid_cache.XUID_HISTORY_FILE:
            return {
                "1": {
                    "current_gamertag": "CurrentTag",
                    "previous_gamertags": ["OldTag"],
                    "updated_at": "2026-01-01T00:00:00+00:00",
                }
            }
        return default

    monkeypatch.setattr(xuid_cache, "safe_read_json", fake_read)

    full = xuid_cache.load_xuid_cache_full()
    assert full["1"]["gamertag"] == "CurrentTag"
    assert full["1"]["previous_gamertags"] == ["OldTag"]
    assert full["1"]["updated_at"] == "2026-01-01T00:00:00+00:00"


def test_normalize_gamertag_for_lookup_collapses_whitespace_and_case():
    assert xuid_cache._normalize_gamertag_for_lookup("  Some   Player  ") == "some player"
    assert xuid_cache._normalize_gamertag_for_lookup("SOME player") == "some player"
    assert xuid_cache._normalize_gamertag_for_lookup(None) == ""


def test_normalize_gamertag_alias_key_removes_spaces():
    assert xuid_cache._normalize_gamertag_alias_key("  Some   Player  ") == "someplayer"
    assert xuid_cache._normalize_gamertag_alias_key("SomePlayer") == "someplayer"
    assert xuid_cache._normalize_gamertag_alias_key(1234) == ""
