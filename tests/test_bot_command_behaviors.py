import pytest
import discord
import json
from types import SimpleNamespace

from src.bot.cogs.graph import GraphCog, NetworkFilterView, NetworkRefreshView
from src.bot.cogs import stats as stats_module
from src.bot.cogs.stats import StatsCog


class _FakeCtx:
    def __init__(self):
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))
        return _FakeMessage()


class _FakeMessage:
    id = 1

    def __init__(self):
        self.last_edit = None

    async def edit(self, *args, **kwargs):
        self.last_edit = {"args": args, "kwargs": kwargs}
        return None

    async def delete(self):
        return None


class _FakeUser:
    def __init__(self, user_id):
        self.id = user_id


class _FakeInteractionResponse:
    def __init__(self):
        self.sent_messages = []
        self.edited_message = None

    async def send_message(self, *args, **kwargs):
        self.sent_messages.append((args, kwargs))

    async def edit_message(self, *args, **kwargs):
        self.edited_message = {"args": args, "kwargs": kwargs}


class _FakeInteraction:
    def __init__(self, user_id, message):
        self.user = _FakeUser(user_id)
        self.message = message
        self.response = _FakeInteractionResponse()


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


def test_stats_cog_excludes_removed_server_command():
    assert not hasattr(StatsCog, "server_stats")


@pytest.mark.asyncio
async def test_graph_find_similar_requires_input():
    cog = GraphCog(bot=object())
    ctx = _FakeCtx()

    await GraphCog.find_similar.callback(cog, ctx)

    assert ctx.sent
    msg = ctx.sent[0][0][0]
    assert "Please provide a gamertag" in msg
    cog.db.close()


@pytest.mark.asyncio
async def test_cache_status_embed_uses_resolved_gamertags_without_unique_players(tmp_path, monkeypatch):
    cache_file = tmp_path / "xuid_gamertag_cache.json"
    progress_file = tmp_path / "cache_progress.json"

    cache_file.write_text(json.dumps({"1": "One", "2": "Two"}), encoding="utf-8")
    progress_file.write_text(
        json.dumps(
            {
                "processed_matches": 25,
                "total_matches": 50,
                "resolved_gamertags": ["A", "B", "C", "D"],
                "unique_players": ["u1", "u2", "u3"],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(stats_module, "XUID_CACHE_FILE", str(cache_file))
    monkeypatch.setattr(stats_module, "CACHE_PROGRESS_FILE", str(progress_file))
    monkeypatch.setattr(stats_module, "PROJECT_ROOT", str(tmp_path))

    cog = StatsCog(bot=object())
    ctx = _FakeCtx()

    await StatsCog.cache_status.callback(cog, ctx)

    assert ctx.sent
    embed = ctx.sent[-1][1]["embed"]
    values = [field.value for field in embed.fields]

    assert any("Total mappings: **2**" in value for value in values)
    assert any("Processed: **25** / **50** matches" in value for value in values)
    assert any("Resolved gamertags: **4**" in value for value in values)
    assert all("Unique players" not in value for value in values)


@pytest.mark.asyncio
async def test_cache_status_embed_no_progress_file_still_shows_resolved_from_cache(tmp_path, monkeypatch):
    cache_file = tmp_path / "xuid_gamertag_cache.json"
    cache_file.write_text(json.dumps({"1": "One", "2": "Two", "3": "Three"}), encoding="utf-8")

    missing_progress_file = tmp_path / "missing_progress.json"

    monkeypatch.setattr(stats_module, "XUID_CACHE_FILE", str(cache_file))
    monkeypatch.setattr(stats_module, "CACHE_PROGRESS_FILE", str(missing_progress_file))
    monkeypatch.setattr(stats_module, "PROJECT_ROOT", str(tmp_path / "other_root"))

    cog = StatsCog(bot=object())
    ctx = _FakeCtx()

    await StatsCog.cache_status.callback(cog, ctx)

    embed = ctx.sent[-1][1]["embed"]
    values = [field.value for field in embed.fields]

    assert any("No active match scan progress file" in value for value in values)
    assert any("Resolved gamertags: **3**" in value for value in values)
    assert all("Unique players" not in value for value in values)


@pytest.mark.asyncio
async def test_network_filter_view_timeout_switches_to_refresh_controls():
    base_embed = discord.Embed(title="Network")
    view = NetworkFilterView(
        cog=object(),
        requester_id=42,
        center_xuid="xuid-center",
        center_gamertag="Center",
        halo_friends=[],
        center_features=None,
        base_embed=base_embed,
    )
    view.min_group_size = 7
    view.min_link_strength = 9.0
    view.clustered = True

    message = _FakeMessage()
    view.message = message

    await view.on_timeout()

    assert message.last_edit is not None
    edited_embed = message.last_edit["kwargs"]["embed"]
    edited_view = message.last_edit["kwargs"]["view"]
    assert "Controls: **INACTIVE**" in (edited_embed.description or "")
    assert "15m timeout" in (edited_embed.description or "")
    assert isinstance(edited_view, NetworkRefreshView)
    assert edited_view.message is message


@pytest.mark.asyncio
async def test_network_refresh_button_restores_active_controls_preserving_filters():
    base_embed = discord.Embed(title="Network")
    source_view = NetworkFilterView(
        cog=object(),
        requester_id=42,
        center_xuid="xuid-center",
        center_gamertag="Center",
        halo_friends=[{"dst_xuid": "friend-1", "social_group_size": 10}],
        center_features=None,
        base_embed=base_embed,
    )
    source_view.min_group_size = 5
    source_view.min_link_strength = 10.0
    source_view.clustered = True
    source_view._sync_select_defaults()

    refresh_view = NetworkRefreshView(requester_id=42, source_view=source_view)
    interaction = _FakeInteraction(user_id=42, message=_FakeMessage())

    refresh_button = refresh_view.children[0]
    await refresh_button.callback(interaction)

    assert interaction.response.edited_message is not None
    edited_kwargs = interaction.response.edited_message["kwargs"]
    edited_embed = edited_kwargs["embed"]
    restored_view = edited_kwargs["view"]

    assert "Controls: **ACTIVE**" in (edited_embed.description or "")
    assert isinstance(restored_view, NetworkFilterView)
    assert restored_view.min_group_size == 5
    assert restored_view.min_link_strength == 10.0
    assert restored_view.clustered is True
    assert restored_view.message is interaction.message


@pytest.mark.asyncio
async def test_crawlgames_run_inline_uses_existing_scope_without_friend_crawl(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.upserts = []

        def get_friends(self, xuid):
            if xuid == "seed-xuid":
                return [{"dst_xuid": "friend-xuid", "halo_active": 1}]
            return []

        def get_player(self, xuid):
            return {"gamertag": xuid}

        def upsert_coplay_edge(self, **kwargs):
            self.upserts.append(kwargs)
            return True

        def close(self):
            return None

    class _FakeStatsDB:
        def get_scope_match_participants(self, scope_xuids):
            assert set(scope_xuids) == {"seed-xuid", "friend-xuid"}
            return {
                "m-1": [
                    {
                        "xuid": "seed-xuid",
                        "team_id": "1",
                        "inferred_team_id": None,
                        "start_time": "2026-01-01T00:00:00",
                    },
                    {
                        "xuid": "friend-xuid",
                        "team_id": "1",
                        "inferred_team_id": None,
                        "start_time": "2026-01-01T00:00:00",
                    },
                ]
            }

    fake_api_client = SimpleNamespace(stats_cache=SimpleNamespace(db=_FakeStatsDB()))

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    fake_api_client.resolve_gamertag_to_xuid = _resolve_gamertag_to_xuid

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(graph_module, "api_client", fake_api_client)

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()

    class _InlineCtx:
        def __init__(self):
            self.sent = []
            self.channel = self

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            return _FakeMessage()

    ctx = _InlineCtx()
    result = await GraphCog.start_crawl_games.callback(
        cog,
        ctx,
        "SeedTag",
        "1",
        run_inline=True,
    )

    assert "rows written" in result
    assert "pairs 1" in result
    assert len(cog.db.upserts) == 2
