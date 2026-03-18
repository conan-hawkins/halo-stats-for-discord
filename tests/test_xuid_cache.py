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
    calls = {}

    def fake_write(path, payload):
        calls["path"] = path
        calls["payload"] = payload

    monkeypatch.setattr(xuid_cache, "safe_write_json", fake_write)
    xuid_cache.save_xuid_cache({"1": "PlayerOne"})

    assert calls["path"] == xuid_cache.XUID_CACHE_FILE
    assert calls["payload"] == {"1": "PlayerOne"}


def test_save_xuid_cache_handles_write_error(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(xuid_cache, "safe_write_json", boom)

    # Should not raise because save_xuid_cache catches exceptions.
    xuid_cache.save_xuid_cache({"1": "PlayerOne"})
