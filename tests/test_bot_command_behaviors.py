import pytest

from src.bot.cogs.graph import GraphCog
from src.bot.cogs.stats import StatsCog


class _FakeCtx:
    def __init__(self):
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))
        return _FakeMessage()


class _FakeMessage:
    id = 1

    async def edit(self, *args, **kwargs):
        return None

    async def delete(self):
        return None


@pytest.mark.asyncio
async def test_stats_cog_full_ranked_casual_dispatch(monkeypatch):
    calls = []

    async def fake_fetch(ctx, gamertag, stat_type="stats", matches_to_process=None):
        calls.append((gamertag, stat_type, matches_to_process))

    from src.bot.cogs import stats as stats_module

    monkeypatch.setattr(stats_module, "fetch_and_display_stats", fake_fetch)

    cog = StatsCog(bot=object())
    ctx = _FakeCtx()

    await StatsCog.full.callback(cog, ctx, "A", "B")
    await StatsCog.ranked.callback(cog, ctx, "A", "B")
    await StatsCog.casual.callback(cog, ctx, "A", "B")

    assert calls[0] == ("AB", "stats", None)
    assert calls[1] == ("AB", "ranked", None)
    assert calls[2] == ("AB", "social", None)


@pytest.mark.asyncio
async def test_graph_find_similar_requires_input():
    cog = GraphCog(bot=object())
    ctx = _FakeCtx()

    await GraphCog.find_similar.callback(cog, ctx)

    assert ctx.sent
    msg = ctx.sent[0][0][0]
    assert "Please provide a gamertag" in msg
    cog.db.close()
