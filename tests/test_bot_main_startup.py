import asyncio
import importlib

import pytest

bot_main = importlib.import_module("src.bot.main")


def test_run_bot_recovers_swap_before_token_validation(monkeypatch):
    calls = []

    monkeypatch.setattr(bot_main, "recover_token_swap_marker", lambda: calls.append("recover"))

    class FakeStats:
        async def ensure_valid_tokens(self):
            calls.append("validate")
            return False

    monkeypatch.setattr(bot_main, "StatsFind1", FakeStats())
    monkeypatch.setattr(bot_main, "TOKEN", "discord-token")

    async def fake_load_cogs():
        calls.append("load_cogs")

    async def fake_start(token):
        calls.append(("start", token))

    monkeypatch.setattr(bot_main, "load_cogs", fake_load_cogs)
    monkeypatch.setattr(bot_main.bot, "start", fake_start)

    asyncio.run(bot_main.run_bot())

    assert calls[:2] == ["recover", "validate"]
    assert "load_cogs" not in calls
    assert ("start", "discord-token") not in calls
