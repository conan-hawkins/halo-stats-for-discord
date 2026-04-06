import pytest
import discord
import json
from io import BytesIO
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

from src.bot.cogs.graph import (
    CrawlProgressView,
    GraphCog,
    HaloNetFilterView,
    HaloNetRefreshView,
    NetworkFilterView,
    NetworkRefreshView,
)
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
async def test_halonet_filter_view_timeout_switches_to_refresh_controls():
    base_embed = discord.Embed(title="HaloNet", description="Co-play links weighted by shared matches.")
    view = HaloNetFilterView(
        cog=object(),
        requester_id=42,
        center_xuid="seed-xuid",
        center_gamertag="Seed",
        node_map={
            "seed-xuid": {"xuid": "seed-xuid", "gamertag": "Seed", "is_center": True},
            "friend-xuid": {"xuid": "friend-xuid", "gamertag": "Friend", "is_center": False},
        },
        edges=[{"src_xuid": "seed-xuid", "dst_xuid": "friend-xuid", "matches_together": 3}],
        base_embed=base_embed,
    )
    view.min_node_strength = 7
    view.min_edge_weight = 9
    view.clustered = True

    message = _FakeMessage()
    view.message = message

    await view.on_timeout()

    assert message.last_edit is not None
    edited_embed = message.last_edit["kwargs"]["embed"]
    edited_view = message.last_edit["kwargs"]["view"]
    assert "Controls: **INACTIVE**" in (edited_embed.description or "")
    assert "15m timeout" in (edited_embed.description or "")
    assert isinstance(edited_view, HaloNetRefreshView)
    assert edited_view.message is message


@pytest.mark.asyncio
async def test_halonet_refresh_button_restores_active_controls_preserving_filters():
    base_embed = discord.Embed(title="HaloNet", description="Co-play links weighted by shared matches.")
    source_view = HaloNetFilterView(
        cog=object(),
        requester_id=42,
        center_xuid="seed-xuid",
        center_gamertag="Seed",
        node_map={
            "seed-xuid": {"xuid": "seed-xuid", "gamertag": "Seed", "is_center": True},
            "friend-xuid": {"xuid": "friend-xuid", "gamertag": "Friend", "is_center": False},
        },
        edges=[{"src_xuid": "seed-xuid", "dst_xuid": "friend-xuid", "matches_together": 5}],
        base_embed=base_embed,
    )
    source_view.min_node_strength = 5
    source_view.min_edge_weight = 10
    source_view.clustered = True
    source_view._sync_select_defaults()

    refresh_view = HaloNetRefreshView(requester_id=42, source_view=source_view)
    interaction = _FakeInteraction(user_id=42, message=_FakeMessage())

    refresh_button = refresh_view.children[0]
    await refresh_button.callback(interaction)

    assert interaction.response.edited_message is not None
    edited_kwargs = interaction.response.edited_message["kwargs"]
    edited_embed = edited_kwargs["embed"]
    restored_view = edited_kwargs["view"]

    assert "Controls: **ACTIVE**" in (edited_embed.description or "")
    assert isinstance(restored_view, HaloNetFilterView)
    assert restored_view.min_node_strength == 5
    assert restored_view.min_edge_weight == 10
    assert restored_view.clustered is True
    assert restored_view.message is interaction.message


@pytest.mark.asyncio
async def test_crawlgames_run_inline_uses_existing_scope_without_friend_crawl(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.upserts = []
            self.stub_batches = []

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

        def insert_or_update_players_stub_batch(self, xuids):
            self.stub_batches.append(sorted(str(x).strip() for x in xuids if str(x).strip()))
            return len(self.stub_batches[-1])

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
    assert cog.db.stub_batches


@pytest.mark.asyncio
async def test_crawlgames_run_inline_scoped_uses_scope_participants(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.upserts = []
            self.stub_batches = []

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

        def insert_or_update_players_stub_batch(self, xuids):
            normalized = sorted(str(x).strip() for x in xuids if str(x).strip())
            self.stub_batches.append(normalized)
            return len(normalized)

        def close(self):
            return None

    class _FakeStatsDB:
        def __init__(self):
            self.scoped_calls = []

        def get_scope_match_participants(self, scope_xuids):
            self.scoped_calls.append(list(scope_xuids))
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

    fake_stats_db = _FakeStatsDB()
    fake_api_client = SimpleNamespace(stats_cache=SimpleNamespace(db=fake_stats_db))

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    fake_api_client.resolve_gamertag_to_xuid = _resolve_gamertag_to_xuid

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(graph_module, "api_client", fake_api_client)

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()
    monkeypatch.setattr(cog, "_collect_halo_active_scope", lambda seed_xuid, depth: ["friend-xuid"])

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
        "--scoped",
        run_inline=True,
    )

    assert "Scoped participants" in result
    assert len(cog.db.upserts) == 2
    assert fake_stats_db.scoped_calls
    assert set(fake_stats_db.scoped_calls[0]) == {"seed-xuid", "friend-xuid"}


@pytest.mark.asyncio
async def test_crawlgames_scoped_overlays_seed_rosters_for_seed_matches(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.upserts = []
            self.stub_batches = []

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
                    return []

            class _Conn:
                def cursor(self):
                    return _Cursor()

            return _Conn()

        def upsert_coplay_edge(self, **kwargs):
            self.upserts.append(kwargs)
            return True

        def insert_or_update_players_stub_batch(self, xuids):
            normalized = sorted(str(x).strip() for x in xuids if str(x).strip())
            self.stub_batches.append(normalized)
            return len(normalized)

        def close(self):
            return None

    class _FakeStatsDB:
        def __init__(self):
            self.scoped_calls = []
            self.seed_calls = []

        def get_scope_match_participants(self, scope_xuids):
            self.scoped_calls.append(list(scope_xuids))
            return {
                "seed-match": [
                    {
                        "xuid": "seed-xuid",
                        "team_id": "1",
                        "inferred_team_id": None,
                        "start_time": "2026-01-01T00:00:00",
                    }
                ]
            }

        def get_seed_match_participants(self, seed_xuid, limit_matches=None):
            self.seed_calls.append((seed_xuid, limit_matches))
            return {
                "seed-match": [
                    {
                        "xuid": "seed-xuid",
                        "team_id": "1",
                        "inferred_team_id": None,
                        "start_time": "2026-01-01T00:00:00",
                    },
                    {
                        "xuid": "external-xuid",
                        "team_id": "2",
                        "inferred_team_id": None,
                        "start_time": "2026-01-01T00:00:00",
                    },
                ]
            }

    fake_stats_db = _FakeStatsDB()
    fake_api_client = SimpleNamespace(stats_cache=SimpleNamespace(db=fake_stats_db))

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    fake_api_client.resolve_gamertag_to_xuid = _resolve_gamertag_to_xuid

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(graph_module, "api_client", fake_api_client)

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()
    monkeypatch.setattr(cog, "_collect_halo_active_scope", lambda seed_xuid, depth: ["friend-xuid"])

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
        "--scoped",
        run_inline=True,
    )

    assert "seed rows 2" in result
    assert "seed pairs 1" in result
    assert "seed rosters" in result
    assert fake_stats_db.scoped_calls
    assert fake_stats_db.seed_calls == [("seed-xuid", None)]
    assert len(cog.db.upserts) == 2

    upsert_pairs = {(row["src_xuid"], row["dst_xuid"]) for row in cog.db.upserts}
    assert upsert_pairs == {("seed-xuid", "external-xuid"), ("external-xuid", "seed-xuid")}
    assert cog.db.stub_batches
    assert "external-xuid" in cog.db.stub_batches[-1]


@pytest.mark.asyncio
async def test_crawlgames_default_mode_hydrates_seed_history_before_scan(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.upserts = []
            self.stub_batches = []

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

        def insert_or_update_players_stub_batch(self, xuids):
            normalized = sorted(str(x).strip() for x in xuids if str(x).strip())
            self.stub_batches.append(normalized)
            return len(normalized)

        def close(self):
            return None

    class _FakeStatsDB:
        def __init__(self):
            self.scoped_calls = []

        def get_scope_match_participants(self, scope_xuids):
            self.scoped_calls.append(list(scope_xuids))
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

    fake_stats_db = _FakeStatsDB()
    fake_api_client = SimpleNamespace(stats_cache=SimpleNamespace(db=fake_stats_db))

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    fake_api_client.resolve_gamertag_to_xuid = _resolve_gamertag_to_xuid

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(graph_module, "api_client", fake_api_client)

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()
    monkeypatch.setattr(cog, "_collect_halo_active_scope", lambda seed_xuid, depth: ["friend-xuid"])

    hydrate_calls = []

    async def _fake_hydrate(seed_xuid, seed_gamertag):
        hydrate_calls.append((seed_xuid, seed_gamertag))
        return {
            "ok": True,
            "matches_processed": 12,
            "matches_with_participants": 10,
        }

    monkeypatch.setattr(cog, "_hydrate_seed_match_history", _fake_hydrate)

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

    assert hydrate_calls == [("seed-xuid", "SeedTag")]
    assert "Scoped participants" in result
    assert len(cog.db.upserts) == 2
    assert fake_stats_db.scoped_calls


@pytest.mark.asyncio
async def test_crawl_progress_view_cancel_button_requests_task_cancellation():
    class _FakeTask:
        def __init__(self):
            self.cancel_called = False

        def done(self):
            return False

        def cancel(self):
            self.cancel_called = True

    fake_cog = SimpleNamespace(_crawl_task=_FakeTask())
    view = CrawlProgressView(cog=fake_cog, requester_id=42)
    message = _FakeMessage()
    view.message = message

    interaction = _FakeInteraction(user_id=42, message=message)
    cancel_button = view.children[0]
    await cancel_button.callback(interaction)

    assert view.cancel_requested is True
    assert fake_cog._crawl_task.cancel_called is True
    assert interaction.response.sent_messages
    sent_args, sent_kwargs = interaction.response.sent_messages[-1]
    assert "Cancellation requested" in sent_args[0]
    assert sent_kwargs.get("ephemeral") is True


@pytest.mark.asyncio
async def test_crawl_progress_view_rejects_unauthorized_cancel():
    class _FakeTask:
        def __init__(self):
            self.cancel_called = False

        def done(self):
            return False

        def cancel(self):
            self.cancel_called = True

    fake_cog = SimpleNamespace(_crawl_task=_FakeTask())
    view = CrawlProgressView(cog=fake_cog, requester_id=42)
    message = _FakeMessage()
    view.message = message

    interaction = _FakeInteraction(user_id=99, message=message)
    cancel_button = view.children[0]
    await cancel_button.callback(interaction)

    assert view.cancel_requested is False
    assert fake_cog._crawl_task.cancel_called is False
    assert interaction.response.sent_messages
    sent_args, sent_kwargs = interaction.response.sent_messages[-1]
    assert "Only the requester or an admin" in sent_args[0]
    assert sent_kwargs.get("ephemeral") is True


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
async def test_halonet_auto_heals_missing_seed_edges(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.auto_healed = False

        def get_player(self, xuid):
            return {"xuid": xuid, "gamertag": "HalcyonVidar"}

        def get_halo_features(self, xuid):
            return {"kd_ratio": 1.0, "win_rate": 50.0, "matches_played": 10}

        def get_coplay_neighbors(self, xuid, min_matches=2, limit=59):
            if not self.auto_healed:
                return []
            return [
                {
                    "partner_xuid": "friend-xuid",
                    "matches_together": 2,
                    "wins_together": 0,
                    "total_minutes": 0,
                    "same_team_count": 1,
                    "opposing_team_count": 1,
                    "first_played": "2026-01-01T00:00:00",
                    "last_played": "2026-01-01T00:00:00",
                    "gamertag": "Friend",
                    "halo_active": 1,
                    "kd_ratio": 1.0,
                    "win_rate": 50.0,
                    "matches_played": 20,
                }
            ]

        def get_coplay_edges_within_set(self, xuids, min_matches=2):
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
    fake_db = _FakeGraphDB()
    cog.db = fake_db
    cog._render_coplay_graph = lambda *args, **kwargs: BytesIO(b"fake-png")

    auto_heal_calls = {"count": 0}

    async def _fake_attempt(seed_xuid, seed_gamertag):
        auto_heal_calls["count"] += 1
        fake_db.auto_healed = True
        return {
            "attempted": True,
            "ok": True,
            "message": "Auto-refresh wrote 2 co-play rows across 1 seed pairs.",
        }

    cog._attempt_halonet_auto_heal = _fake_attempt

    class _InlineCtx:
        def __init__(self):
            self.sent = []

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            return _FakeMessage()

    ctx = _InlineCtx()
    await GraphCog.show_halonet.callback(cog, ctx, "HalcyonVidar")

    assert auto_heal_calls["count"] == 1
    assert len(ctx.sent) >= 2

    final_kwargs = ctx.sent[-1][1]
    final_embed = final_kwargs["embed"]
    auto_field = next((f for f in final_embed.fields if f.name == "Auto-refresh"), None)
    assert auto_field is not None
    assert "wrote 2 co-play rows" in auto_field.value


@pytest.mark.asyncio
async def test_halonet_reports_auto_heal_skip_when_no_edges_after_retry(monkeypatch):
    class _FakeGraphDB:
        def get_player(self, xuid):
            return {"xuid": xuid, "gamertag": "HalcyonVidar"}

        def get_halo_features(self, xuid):
            return {"kd_ratio": 1.0, "win_rate": 50.0, "matches_played": 10}

        def get_coplay_neighbors(self, xuid, min_matches=2, limit=59):
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

    async def _fake_attempt(seed_xuid, seed_gamertag):
        return {
            "attempted": False,
            "ok": False,
            "message": "Auto-refresh skipped: recently refreshed (3m ago); retry in 23h 57m.",
        }

    cog._attempt_halonet_auto_heal = _fake_attempt

    class _InlineCtx:
        def __init__(self):
            self.sent = []

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            return _FakeMessage()

    ctx = _InlineCtx()
    await GraphCog.show_halonet.callback(cog, ctx, "HalcyonVidar")

    assert len(ctx.sent) >= 2
    final_args = ctx.sent[-1][0]
    assert final_args
    assert "after auto-refresh" in final_args[0]
    assert "Auto-refresh skipped" in final_args[0]
    assert "#crawlgames HalcyonVidar" in final_args[0]


@pytest.mark.asyncio
async def test_halonet_failed_auto_heal_uses_short_retry_window(monkeypatch):
    cog = GraphCog(bot=object())

    async def _fake_run(seed_xuid, seed_gamertag):
        return {
            "ok": False,
            "message": "Auto-refresh completed, but no seed co-play pairs were found in persisted match participants.",
        }

    monkeypatch.setattr(cog, "_run_halonet_auto_heal", _fake_run)

    attempt = await cog._attempt_halonet_auto_heal("seed-xuid", "Seed")

    assert attempt["attempted"] is True
    assert attempt["ok"] is False

    allowed_now, reason_now = cog._is_halonet_repair_allowed("seed-xuid")
    assert allowed_now is False
    assert reason_now is not None
    assert "retry in" in reason_now
    assert "retry in 23h" not in reason_now

    # Simulate a short wait window passing.
    cog._halonet_repair_cooldowns["seed-xuid"] -= timedelta(minutes=16)
    allowed_later, _ = cog._is_halonet_repair_allowed("seed-xuid")
    assert allowed_later is True


@pytest.mark.asyncio
async def test_halonet_successful_auto_heal_keeps_long_cooldown(monkeypatch):
    cog = GraphCog(bot=object())

    async def _fake_run(seed_xuid, seed_gamertag):
        return {
            "ok": True,
            "message": "Auto-refresh wrote 6 co-play rows across 3 seed pairs.",
        }

    monkeypatch.setattr(cog, "_run_halonet_auto_heal", _fake_run)

    attempt = await cog._attempt_halonet_auto_heal("seed-xuid", "Seed")

    assert attempt["attempted"] is True
    assert attempt["ok"] is True

    allowed_now, reason_now = cog._is_halonet_repair_allowed("seed-xuid")
    assert allowed_now is False
    assert reason_now is not None
    assert "h" in reason_now


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


@pytest.mark.asyncio
async def test_iss_level0_checks_direct_blacklist_and_persists_edges(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.players = {}
            self.edge_batches = []

        def insert_or_update_player(self, xuid, **kwargs):
            self.players[xuid] = {"xuid": xuid, **kwargs}
            return True

        def insert_friend_edges_batch(self, edges):
            self.edge_batches.append(list(edges))
            return len(edges)

        def close(self):
            return None

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "target-xuid"

    async def _get_friends_list(_xuid):
        return {
            "friends": [
                {"xuid": "bad-xuid", "gamertag": "Bad Actor", "is_mutual": True},
                {"xuid": "friend-xuid", "gamertag": "Normal Friend", "is_mutual": False},
            ],
            "is_private": False,
            "error": None,
        }

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(
        graph_module,
        "api_client",
        SimpleNamespace(
            resolve_gamertag_to_xuid=_resolve_gamertag_to_xuid,
            get_friends_list=_get_friends_list,
        ),
    )
    monkeypatch.setattr(GraphCog, "_load_blacklist", lambda _self: {"bad-xuid": "Bad Actor"})

    cog = GraphCog(bot=object())
    cog.db.close()
    fake_db = _FakeGraphDB()
    cog.db = fake_db
    ctx = _FakeCtx()

    result = await cog.iss_level0(ctx, "SeedPlayer")

    assert "ISS level 0 complete" in result
    assert fake_db.edge_batches
    assert len(fake_db.edge_batches[0]) == 2
    embed = ctx.sent[-1][1]["embed"]
    assert embed.title.startswith("ISS Level 0")
    assert "Bad Actor" in embed.fields[-1].value


@pytest.mark.asyncio
async def test_iss_level2_persists_history_and_coplay_for_blacklist_candidates(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.players = {}
            self.edge_batches = []
            self.halo_feature_rows = []
            self.coplay_upserts = []

        def insert_or_update_player(self, xuid, **kwargs):
            row = self.players.get(xuid, {"xuid": xuid, "halo_active": False})
            row.update(kwargs)
            self.players[xuid] = row
            return True

        def insert_friend_edges_batch(self, edges):
            self.edge_batches.append(list(edges))
            return len(edges)

        def insert_or_update_halo_features(self, **kwargs):
            self.halo_feature_rows.append(kwargs)
            return True

        def get_player(self, xuid):
            return self.players.get(xuid)

        def upsert_coplay_edge(self, **kwargs):
            self.coplay_upserts.append(kwargs)
            return True

        def close(self):
            return None

    now = datetime.now(timezone.utc)
    recent_match = (now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old_match = (now - timedelta(days=250)).strftime("%Y-%m-%dT%H:%M:%SZ")
    stats_calls = []

    async def _get_friends_of_friends(_gamertag, max_depth=2, progress_callback=None):
        if progress_callback:
            await progress_callback(0, 1, "friends_found", 0)
            await progress_callback(1, 1, "checking_fof", 1)
        return {
            "target": {"xuid": "target-xuid", "gamertag": "SeedPlayer"},
            "friends": [{"xuid": "bad-xuid", "gamertag": "Bad Actor", "is_mutual": True}],
            "friends_of_friends": [{"xuid": "fof-xuid", "gamertag": "FoF", "via": "Bad Actor", "is_mutual": False}],
            "private_friends": [],
            "error": None,
        }

    async def _calculate_comprehensive_stats(xuid, stat_type, gamertag=None, matches_to_process=None, force_full_fetch=False):
        stats_calls.append(
            {
                "xuid": xuid,
                "stat_type": stat_type,
                "gamertag": gamertag,
                "matches_to_process": matches_to_process,
                "force_full_fetch": force_full_fetch,
            }
        )
        return {
            "error": 0,
            "stats": {
                "games_played": 2,
                "total_kills": 20,
                "total_deaths": 10,
                "total_assists": 5,
                "kd_ratio": 2.0,
                "win_rate": "50.0%",
                "estimated_csr": 1200,
                "csr_tier": "Gold",
            },
            "processed_matches": [
                {
                    "match_id": "m-recent",
                    "start_time": recent_match,
                    "all_participants": [
                        {"xuid": "bad-xuid", "gamertag": "Bad Actor", "team_id": "1"},
                        {"xuid": "ally-xuid", "gamertag": "Ally", "team_id": "1"},
                    ],
                },
                {
                    "match_id": "m-old",
                    "start_time": old_match,
                    "all_participants": [
                        {"xuid": "bad-xuid", "gamertag": "Bad Actor", "team_id": "1"},
                        {"xuid": "old-xuid", "gamertag": "OldPal", "team_id": "2"},
                    ],
                },
            ],
        }

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(
        graph_module,
        "api_client",
        SimpleNamespace(
            get_friends_of_friends=_get_friends_of_friends,
            calculate_comprehensive_stats=_calculate_comprehensive_stats,
        ),
    )
    monkeypatch.setattr(GraphCog, "_load_blacklist", lambda _self: {"bad-xuid": "Bad Actor"})

    cog = GraphCog(bot=object())
    cog.db.close()
    fake_db = _FakeGraphDB()
    cog.db = fake_db
    ctx = _FakeCtx()

    result = await cog.iss_level2(ctx, "SeedPlayer")

    assert "ISS level 2 complete" in result
    assert stats_calls and stats_calls[0]["matches_to_process"] == 120
    assert stats_calls[0]["force_full_fetch"] is False
    assert fake_db.halo_feature_rows
    assert fake_db.coplay_upserts


@pytest.mark.asyncio
async def test_iss_level3_uses_full_history_fetch_for_blacklist_candidates(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.players = {}

        def insert_or_update_player(self, xuid, **kwargs):
            row = self.players.get(xuid, {"xuid": xuid, "halo_active": False})
            row.update(kwargs)
            self.players[xuid] = row
            return True

        def insert_friend_edges_batch(self, edges):
            return len(edges)

        def insert_or_update_halo_features(self, **kwargs):
            return True

        def get_player(self, xuid):
            return self.players.get(xuid)

        def upsert_coplay_edge(self, **kwargs):
            return True

        def close(self):
            return None

    stats_calls = []

    async def _get_friends_of_friends(_gamertag, max_depth=2, progress_callback=None):
        return {
            "target": {"xuid": "target-xuid", "gamertag": "SeedPlayer"},
            "friends": [{"xuid": "bad-xuid", "gamertag": "Bad Actor", "is_mutual": True}],
            "friends_of_friends": [],
            "private_friends": [],
            "error": None,
        }

    async def _calculate_comprehensive_stats(xuid, stat_type, gamertag=None, matches_to_process=None, force_full_fetch=False):
        stats_calls.append(
            {
                "xuid": xuid,
                "matches_to_process": matches_to_process,
                "force_full_fetch": force_full_fetch,
            }
        )
        return {
            "error": 0,
            "stats": {
                "games_played": 1,
                "total_kills": 5,
                "total_deaths": 2,
                "total_assists": 1,
                "kd_ratio": 2.5,
                "win_rate": "100.0%",
            },
            "processed_matches": [
                {
                    "match_id": "m-1",
                    "start_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "all_participants": [
                        {"xuid": "bad-xuid", "gamertag": "Bad Actor", "team_id": "1"},
                        {"xuid": "ally-xuid", "gamertag": "Ally", "team_id": "1"},
                    ],
                }
            ],
        }

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(
        graph_module,
        "api_client",
        SimpleNamespace(
            get_friends_of_friends=_get_friends_of_friends,
            calculate_comprehensive_stats=_calculate_comprehensive_stats,
        ),
    )
    monkeypatch.setattr(GraphCog, "_load_blacklist", lambda _self: {"bad-xuid": "Bad Actor"})

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()
    ctx = _FakeCtx()

    result = await cog.iss_level3(ctx, "SeedPlayer")

    assert "ISS level 3 complete" in result
    assert stats_calls
    assert stats_calls[0]["matches_to_process"] is None
    assert stats_calls[0]["force_full_fetch"] is True
