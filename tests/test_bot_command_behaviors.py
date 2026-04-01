import pytest
import discord
import json
from io import BytesIO
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

        def _get_connection(self):
            class _Cursor:
                def execute(self, *_args, **_kwargs):
                    return None

                def fetchall(self):
                    return [{"xuid": "seed-xuid"}, {"xuid": "friend-xuid"}]

            class _Conn:
                def cursor(self):
                    return _Cursor()

            return _Conn()

        def upsert_coplay_edge(self, **kwargs):
            self.upserts.append(kwargs)
            return True

        def close(self):
            return None

    class _FakeStatsDB:
        def get_all_match_participants(self):
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
    assert "seed pairs 1" in result
    assert len(cog.db.upserts) == 2


@pytest.mark.asyncio
async def test_halonet_falls_back_to_single_match_threshold(monkeypatch):
    class _FakeGraphDB:
        def get_player(self, xuid):
            return {"xuid": xuid, "gamertag": "HalcyonVidar"}

        def get_halo_features(self, xuid):
            return {"kd_ratio": 1.2, "win_rate": 55.0, "matches_played": 123}

        def get_coplay_neighbors(self, xuid, min_matches=2, limit=59):
            if min_matches >= 2:
                return []
            return [
                {
                    "partner_xuid": "friend-xuid",
                    "matches_together": 1,
                    "wins_together": 0,
                    "total_minutes": 0,
                    "same_team_count": 1,
                    "opposing_team_count": 0,
                    "first_played": "2026-01-01T00:00:00",
                    "last_played": "2026-01-01T00:00:00",
                    "gamertag": "Friend",
                    "halo_active": 1,
                    "kd_ratio": 1.0,
                    "win_rate": 50.0,
                    "matches_played": 20,
                }
            ]

        def get_coplay_edges_within_set(self, xuids, min_matches=1):
            # Force fallback path that builds edges from neighbors list.
            return []

        def close(self):
            return None

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(
        graph_module,
        "api_client",
        SimpleNamespace(resolve_gamertag_to_xuid=_resolve_gamertag_to_xuid),
    )

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()
    cog._render_coplay_graph = lambda *args, **kwargs: BytesIO(b"fake-png")

    class _InlineCtx:
        def __init__(self):
            self.sent = []

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            return _FakeMessage()

    ctx = _InlineCtx()
    await GraphCog.show_halonet.callback(cog, ctx, "HalcyonVidar")

    # First send is loading embed, second is final graph embed+file.
    assert len(ctx.sent) >= 2
    final_kwargs = ctx.sent[-1][1]
    assert "embed" in final_kwargs
    final_embed = final_kwargs["embed"]
    summary_field = next((f for f in final_embed.fields if f.name == "Summary"), None)
    assert summary_field is not None
    assert "Min edge weight: **1**" in summary_field.value
    assert "fallback edges with at least 1 shared match" in (final_embed.description or "")


@pytest.mark.asyncio
async def test_halogroups_returns_overlap_and_membership_csv(monkeypatch):
    class _FakeGraphDB:
        def get_player(self, xuid):
            return {"xuid": xuid, "gamertag": "Seed"}

        def get_coplay_neighbors(self, xuid, min_matches=2, limit=59):
            return [
                {"partner_xuid": "a", "gamertag": "A", "matches_together": 5},
                {"partner_xuid": "b", "gamertag": "B", "matches_together": 5},
                {"partner_xuid": "c", "gamertag": "C", "matches_together": 5},
                {"partner_xuid": "d", "gamertag": "D", "matches_together": 5},
            ]

        def get_coplay_edges_within_set(self, xuids, min_matches=1):
            # Directional rows; command aggregates into undirected weighted edges.
            return [
                {"src_xuid": "seed-xuid", "dst_xuid": "a", "matches_together": 5},
                {"src_xuid": "a", "dst_xuid": "seed-xuid", "matches_together": 5},
                {"src_xuid": "a", "dst_xuid": "b", "matches_together": 8},
                {"src_xuid": "b", "dst_xuid": "a", "matches_together": 8},
                {"src_xuid": "seed-xuid", "dst_xuid": "c", "matches_together": 3},
                {"src_xuid": "c", "dst_xuid": "seed-xuid", "matches_together": 3},
                {"src_xuid": "c", "dst_xuid": "d", "matches_together": 8},
                {"src_xuid": "d", "dst_xuid": "c", "matches_together": 8},
                {"src_xuid": "b", "dst_xuid": "c", "matches_together": 1},
                {"src_xuid": "c", "dst_xuid": "b", "matches_together": 1},
            ]

        def close(self):
            return None

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(
        graph_module,
        "api_client",
        SimpleNamespace(resolve_gamertag_to_xuid=_resolve_gamertag_to_xuid),
    )

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()

    class _InlineCtx:
        def __init__(self):
            self.sent = []

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            return _FakeMessage()

    ctx = _InlineCtx()
    await GraphCog.show_halogroups.callback(cog, ctx, "Seed")

    assert len(ctx.sent) >= 2
    final_kwargs = ctx.sent[-1][1]
    final_embed = final_kwargs["embed"]
    final_files = final_kwargs["files"]

    assert final_embed.title.startswith("Halo Groups:")
    summary_field = next((f for f in final_embed.fields if f.name == "Summary"), None)
    assert summary_field is not None
    assert "Groups:" in summary_field.value

    assert len(final_files) == 2
    filenames = {f.filename for f in final_files}
    assert any(name.startswith("halogroups_overlap_") for name in filenames)
    assert any(name.startswith("halogroups_members_") for name in filenames)

    overlap_file = next(f for f in final_files if f.filename.startswith("halogroups_overlap_"))
    members_file = next(f for f in final_files if f.filename.startswith("halogroups_members_"))

    overlap_file.fp.seek(0)
    members_file.fp.seek(0)
    overlap_csv = overlap_file.fp.read().decode("utf-8")
    members_csv = members_file.fp.read().decode("utf-8")

    assert "Group,G1" in overlap_csv
    assert "group_id,xuid,gamertag,is_center" in members_csv
