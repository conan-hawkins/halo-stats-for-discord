from pathlib import Path

from src.config import settings


def test_get_token_cache_path_for_primary_account():
    assert settings.get_token_cache_path(1) == settings.TOKEN_CACHE_FILE


def test_get_token_cache_path_for_additional_account():
    path = settings.get_token_cache_path(3)
    assert path.name == "token_cache_account3.json"
    assert path.parent == settings.TOKEN_CACHE_DIR


def test_ensure_data_directories_creates_expected_paths(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    token_dir = data_dir / "auth"

    monkeypatch.setattr(settings, "DATA_DIR", data_dir)
    monkeypatch.setattr(settings, "TOKEN_CACHE_DIR", token_dir)

    settings.ensure_data_directories()

    assert data_dir.exists()
    assert token_dir.exists()


def test_get_terminal_admin_password_reads_current_env(monkeypatch):
    monkeypatch.setenv("TERMINAL_ADMIN_PASSWORD", "  test-pass  ")

    assert settings.get_terminal_admin_password() == "test-pass"


def test_get_terminal_admin_password_empty_when_unset(monkeypatch):
    monkeypatch.delenv("TERMINAL_ADMIN_PASSWORD", raising=False)

    assert settings.get_terminal_admin_password() == ""
