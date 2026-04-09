import pytest
import discord
import json
from io import BytesIO
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

from src.bot.cogs.graph import (
    CrawlProgressView,
    GraphCog,
)
from src.bot.cogs.graph_commands.display.halonet.ui import (
    HaloNetFilterView,
    HaloNetNodeInfoView,
    HaloNetRefreshView,
)
from src.bot.cogs.graph_commands.display.network.ui import NetworkFilterView, NetworkRefreshView
from src.bot.cogs.graph_commands.display.halonet.cog import HaloNetCog
from src.bot.cogs import graph as graph_module
from src.bot.cogs.graph_commands.display.halonet import cog as halonet_module
from src.bot.cogs.graph_commands.display.network import cog as network_module
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


class _TrackingMessage(_FakeMessage):
    def __init__(self):
        super().__init__()
        self.edits = []

    async def edit(self, *args, **kwargs):
        payload = {"args": args, "kwargs": kwargs}
        self.edits.append(payload)
        self.last_edit = payload
        return None


class _FriendsCtx(_FakeCtx):
    def __init__(self, loading_message=None):
        super().__init__()
        self.loading_message = loading_message or _TrackingMessage()
        self._send_count = 0

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))
        self._send_count += 1
        if self._send_count == 1:
            return self.loading_message
        return _TrackingMessage()


class _FakeUser:
    def __init__(self, user_id):
        self.id = user_id


class _FakeInteractionResponse:
    def __init__(self):
        self.sent_messages = []
        self.edited_message = None
        self.deferred = False

    async def send_message(self, *args, **kwargs):
        self.sent_messages.append((args, kwargs))

    async def edit_message(self, *args, **kwargs):
        self.edited_message = {"args": args, "kwargs": kwargs}

    async def defer(self):
        self.deferred = True


class _FakeInteraction:
    def __init__(self, user_id, message):
        self.user = _FakeUser(user_id)
        self.message = message
        self.response = _FakeInteractionResponse()


@pytest.mark.asyncio
async def test_stats_cog_full_ranked_casual_dispatch(monkeypatch):
    calls = []

    async def fake_fetch(ctx, gamertag, stat_type="stats", matches_to_process=None, force_full_fetch=False):
        calls.append((gamertag, stat_type, matches_to_process, force_full_fetch))

    from src.bot.cogs import stats as stats_module

    monkeypatch.setattr(stats_module, "fetch_and_display_stats", fake_fetch)

    cog = StatsCog(bot=object())
    ctx = _FakeCtx()

    await StatsCog.full.callback(cog, ctx, "A", "B")
    await StatsCog.ranked.callback(cog, ctx, "A", "B")
    await StatsCog.casual.callback(cog, ctx, "A", "B")

    assert calls[0] == ("A B", "stats", None, False)
    assert calls[1] == ("A B", "ranked", None, False)
    assert calls[2] == ("A B", "social", None, False)


def test_stats_cog_excludes_removed_server_command():
    assert not hasattr(StatsCog, "server_stats")


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
async def test_xboxfriends_happy_path_updates_progress_and_final_embed(monkeypatch):
    progress_stages = []

    async def fake_get_friends_of_friends(gamertag, max_depth=2, progress_callback=None):
        if progress_callback:
            await progress_callback(0, 2, "friends_found", 0)
            progress_stages.append("friends_found")
            await progress_callback(1, 2, "checking_fof", 1)
            progress_stages.append("checking_fof")

        return {
            "target": {"xuid": "target-xuid", "gamertag": gamertag},
            "friends": [
                {"xuid": "friend-good", "gamertag": "Good Friend", "is_mutual": True},
                {"xuid": "friend-bad", "gamertag": "Bad Friend", "is_mutual": False},
            ],
            "friends_of_friends": [
                {
                    "xuid": "fof-bad",
                    "gamertag": "FoF Bad",
                    "via": "Good Friend",
                    "is_mutual": False,
                }
            ],
            "private_friends": [{"xuid": "private-1", "gamertag": "Private One"}],
            "error": None,
        }

    class _FakeGraphDB:
        def __init__(self):
            self.players = []
            self.edge_batches = []

        def insert_or_update_player(self, **kwargs):
            self.players.append(kwargs)
            return True

        def insert_friend_edges_batch(self, edges):
            self.edge_batches.append(list(edges))
            return len(edges)

    fake_graph_db = _FakeGraphDB()

    monkeypatch.setattr(
        stats_module,
        "api_client",
        SimpleNamespace(get_friends_of_friends=fake_get_friends_of_friends),
    )
    monkeypatch.setattr(stats_module, "get_graph_db", lambda: fake_graph_db)
    monkeypatch.setattr(stats_module.Path, "exists", lambda _self: True)
    monkeypatch.setattr(
        stats_module.Path,
        "read_text",
        lambda _self, encoding="utf-8-sig": json.dumps(
            {"friend-bad": "Known Bad", "fof-bad": "Known FoF"}
        ),
    )

    cog = StatsCog(bot=object())
    ctx = _FriendsCtx()

    await StatsCog.friends_list.callback(cog, ctx, "Test", "Player")

    assert progress_stages == ["friends_found", "checking_fof"]
    assert len(ctx.sent) == 1
    assert ctx.sent[0][1]["embed"].title == "🔍 Fetching Friends List..."

    edits = ctx.loading_message.edits
    assert len(edits) >= 3

    first_progress = edits[0]["kwargs"]["embed"]
    second_progress = edits[1]["kwargs"]["embed"]
    final_embed = edits[-1]["kwargs"]["embed"]

    assert "Progress: 0/2 friends checked" in (first_progress.description or "")
    assert "Progress: **1/2** friends checked" in (second_progress.description or "")

    assert final_embed.title == "👥 Friends Network: Test Player"
    field_names = [field.name for field in final_embed.fields]
    assert any(name.startswith("📋 Direct Friends") for name in field_names)
    assert "📊 Summary" in field_names
    assert any("Known Bad" in (field.value or "") for field in final_embed.fields)


@pytest.mark.asyncio
async def test_xboxfriends_api_error_updates_loading_message_with_error_embed(monkeypatch):
    async def fake_get_friends_of_friends(_gamertag, max_depth=2, progress_callback=None):
        return {
            "friends": [],
            "friends_of_friends": [],
            "private_friends": [],
            "error": "request failed",
        }

    monkeypatch.setattr(
        stats_module,
        "api_client",
        SimpleNamespace(get_friends_of_friends=fake_get_friends_of_friends),
    )
    monkeypatch.setattr(stats_module.Path, "exists", lambda _self: False)

    cog = StatsCog(bot=object())
    ctx = _FriendsCtx()

    await StatsCog.friends_list.callback(cog, ctx, "Error", "Player")

    assert ctx.loading_message.edits
    error_embed = ctx.loading_message.edits[-1]["kwargs"]["embed"]
    assert error_embed.title == "❌ Error"
    assert error_embed.description == "request failed"


@pytest.mark.asyncio
async def test_xboxfriends_fetch_exception_updates_loading_message_with_error_embed(monkeypatch):
    async def fake_get_friends_of_friends(_gamertag, max_depth=2, progress_callback=None):
        raise RuntimeError("network down")

    monkeypatch.setattr(
        stats_module,
        "api_client",
        SimpleNamespace(get_friends_of_friends=fake_get_friends_of_friends),
    )
    monkeypatch.setattr(stats_module.Path, "exists", lambda _self: False)

    cog = StatsCog(bot=object())
    ctx = _FriendsCtx()

    await StatsCog.friends_list.callback(cog, ctx, "Error", "Player")

    assert ctx.loading_message.edits
    error_embed = ctx.loading_message.edits[-1]["kwargs"]["embed"]
    assert error_embed.title == "❌ Error"
    assert "An error occurred: network down" in (error_embed.description or "")


@pytest.mark.asyncio
async def test_xboxfriends_empty_input_sends_prompt(monkeypatch):
    cog = StatsCog(bot=object())
    ctx = _FriendsCtx()

    await StatsCog.friends_list.callback(cog, ctx)

    assert ctx.sent
    assert "Please provide a gamertag" in ctx.sent[0][0][0]


@pytest.mark.asyncio
async def test_xboxfriends_graph_persistence_failure_still_sends_result_embed(monkeypatch):
    async def fake_get_friends_of_friends(gamertag, max_depth=2, progress_callback=None):
        return {
            "target": {"xuid": "target-xuid", "gamertag": gamertag},
            "friends": [{"xuid": "friend-1", "gamertag": "Friend One", "is_mutual": True}],
            "friends_of_friends": [],
            "private_friends": [],
            "error": None,
        }

    class _FailingGraphDB:
        def insert_or_update_player(self, **kwargs):
            raise RuntimeError("db unavailable")

        def insert_friend_edges_batch(self, edges):
            return 0

    monkeypatch.setattr(
        stats_module,
        "api_client",
        SimpleNamespace(get_friends_of_friends=fake_get_friends_of_friends),
    )
    monkeypatch.setattr(stats_module, "get_graph_db", lambda: _FailingGraphDB())
    monkeypatch.setattr(stats_module.Path, "exists", lambda _self: False)

    cog = StatsCog(bot=object())
    ctx = _FriendsCtx()

    await StatsCog.friends_list.callback(cog, ctx, "Seed", "Player")

    assert ctx.loading_message.edits
    final_embed = ctx.loading_message.edits[-1]["kwargs"]["embed"]
    assert final_embed.title == "👥 Friends Network: Seed Player"


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
async def test_network_cog_show_network_uses_shared_runtime(monkeypatch):
    calls = {}
    fake_db = object()

    async def fake_execute_show_network(cog, ctx, db, inputs):
        calls["args"] = (cog, ctx, db, inputs)

    monkeypatch.setattr(network_module, "get_graph_db", lambda: fake_db)
    monkeypatch.setattr(network_module, "execute_show_network", fake_execute_show_network)

    cog = network_module.NetworkCog(bot=object())
    ctx = _FakeCtx()

    await network_module.NetworkCog.show_network.callback(cog, ctx, "Chief", "117")

    assert "args" in calls
    assert calls["args"][0] is cog
    assert calls["args"][2] is fake_db
    assert calls["args"][3] == ("Chief", "117")


@pytest.mark.asyncio
async def test_graph_show_network_internal_method_uses_shared_runtime(monkeypatch):
    calls = {}
    fake_db = object()

    async def fake_execute_show_network(cog, ctx, db, inputs):
        calls["args"] = (cog, ctx, db, inputs)

    monkeypatch.setattr(graph_module, "get_graph_db", lambda: fake_db)
    monkeypatch.setattr(graph_module, "execute_show_network", fake_execute_show_network)

    cog = GraphCog(bot=object())
    ctx = _FakeCtx()

    await cog.show_network(ctx, "Chief117")

    assert "args" in calls
    assert calls["args"][0] is cog
    assert calls["args"][2] is fake_db
    assert calls["args"][3] == ("Chief117",)


def test_graph_render_network_graph_uses_shared_renderer(monkeypatch):
    fake_db = object()
    calls = {}

    def fake_render_network_graph(
        db,
        center_xuid,
        center_gamertag,
        halo_friends,
        center_features,
        clustered=False,
        min_group_size=0,
        min_link_strength=1.0,
    ):
        calls["args"] = {
            "db": db,
            "center_xuid": center_xuid,
            "center_gamertag": center_gamertag,
            "clustered": clustered,
            "min_group_size": min_group_size,
            "min_link_strength": min_link_strength,
        }
        return BytesIO(b"rendered")

    monkeypatch.setattr(graph_module, "get_graph_db", lambda: fake_db)
    monkeypatch.setattr(graph_module, "render_network_graph", fake_render_network_graph)

    cog = GraphCog(bot=object())
    rendered = cog._render_network_graph(
        "xuid-seed",
        "Seed",
        [],
        None,
        clustered=True,
        min_group_size=5,
        min_link_strength=7.0,
    )

    assert rendered.getvalue() == b"rendered"
    assert calls["args"]["db"] is fake_db
    assert calls["args"]["center_xuid"] == "xuid-seed"
    assert calls["args"]["center_gamertag"] == "Seed"
    assert calls["args"]["clustered"] is True
    assert calls["args"]["min_group_size"] == 5
    assert calls["args"]["min_link_strength"] == 7.0


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
    source_view.game_type_filter = "ranked"
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
    assert restored_view.game_type_filter == "ranked"
    assert restored_view.message is interaction.message


def test_halonet_filter_thresholds_include_200_and_stay_within_select_limit():
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
        edges=[{"src_xuid": "seed-xuid", "dst_xuid": "friend-xuid", "matches_together": 5}],
        base_embed=base_embed,
    )

    node_select = next(
        item for item in view.children if isinstance(item, discord.ui.Select) and "Node Filter" in (item.placeholder or "")
    )
    edge_select = next(
        item for item in view.children if isinstance(item, discord.ui.Select) and "Edge Filter" in (item.placeholder or "")
    )
    game_type_select = next(
        item for item in view.children if isinstance(item, discord.ui.Select) and "Game Type Filter" in (item.placeholder or "")
    )

    node_values = [opt.value for opt in node_select.options]
    edge_values = [opt.value for opt in edge_select.options]
    game_type_values = [opt.value for opt in game_type_select.options]

    assert "200" in node_values
    assert "200" in edge_values
    assert game_type_values == ["all", "ranked", "social", "custom"]
    assert len(node_values) <= 25
    assert len(edge_values) <= 25


def _build_halonet_node_fixture(total_nodes):
    node_map = {
        "seed-xuid": {
            "xuid": "seed-xuid",
            "gamertag": "Seed",
            "is_center": True,
            "weighted_degree": 999,
            "kd_ratio": 1.5,
            "win_rate": 60.0,
            "matches_played": 300,
        }
    }
    edges = []
    for idx in range(1, total_nodes):
        xuid = f"friend-{idx}"
        node_map[xuid] = {
            "xuid": xuid,
            "gamertag": f"Friend {idx}",
            "is_center": False,
            "weighted_degree": max(1, total_nodes - idx),
            "kd_ratio": 1.0 + (idx / 100.0),
            "win_rate": 48.0 + (idx / 10.0),
            "matches_played": 20 + idx,
        }
        edges.append(
            {
                "src_xuid": "seed-xuid",
                "dst_xuid": xuid,
                "matches_together": 3 + idx,
                "wins_together": 1 + (idx // 2),
                "total_minutes": 45 + idx,
                "same_team_count": 1 + (idx % 3),
                "opposing_team_count": idx % 2,
            }
        )
    return node_map, edges


@pytest.mark.asyncio
async def test_halonet_node_info_view_all_setting_paginates_beyond_25_nodes():
    node_map, edges = _build_halonet_node_fixture(total_nodes=31)
    view = HaloNetNodeInfoView(node_map=node_map, edges=edges, requester_id=42)
    interaction = _FakeInteraction(user_id=42, message=_FakeMessage())

    count_select = next(
        item for item in view.children if isinstance(item, discord.ui.Select) and "Node Info Count" in (item.placeholder or "")
    )
    count_select._values = ["all"]
    await count_select.callback(interaction)

    assert view.include_count is None
    assert view._get_page_count() == 2
    assert interaction.response.edited_message is not None
    assert "setting: **All**" in interaction.response.edited_message["kwargs"]["content"]

    next_button = next(
        item for item in view.children if isinstance(item, discord.ui.Button) and item.label == "Next Page"
    )
    await next_button.callback(interaction)

    assert view.page_index == 1
    assert "Page: **2/2**" in interaction.response.edited_message["kwargs"]["content"]


@pytest.mark.asyncio
async def test_halonet_node_info_selector_rejects_non_requester():
    node_map, edges = _build_halonet_node_fixture(total_nodes=6)
    view = HaloNetNodeInfoView(node_map=node_map, edges=edges, requester_id=42)
    interaction = _FakeInteraction(user_id=7, message=_FakeMessage())

    node_select = next(
        item for item in view.children if isinstance(item, discord.ui.Select) and "Select a HaloNet node" in (item.placeholder or "")
    )
    node_select._values = ["friend-1"]
    await node_select.callback(interaction)

    assert interaction.response.sent_messages
    message_args, message_kwargs = interaction.response.sent_messages[0]
    assert "Only the command requester can use this selector." in message_args[0]
    assert message_kwargs.get("ephemeral") is True


@pytest.mark.asyncio
async def test_halonet_node_info_selector_returns_embed_and_partner_file():
    node_map, edges = _build_halonet_node_fixture(total_nodes=6)
    view = HaloNetNodeInfoView(node_map=node_map, edges=edges, requester_id=42)
    interaction = _FakeInteraction(user_id=42, message=_FakeMessage())

    node_select = next(
        item for item in view.children if isinstance(item, discord.ui.Select) and "Select a HaloNet node" in (item.placeholder or "")
    )
    node_select._values = ["friend-1"]
    await node_select.callback(interaction)

    assert interaction.response.sent_messages
    _, message_kwargs = interaction.response.sent_messages[0]
    embed = message_kwargs["embed"]
    partner_file = message_kwargs["file"]

    assert embed.title == "Node Details: Friend 1"
    assert any(field.name == "Co-play Insights" for field in embed.fields)
    assert partner_file.filename == "node_coplay_partners_friend-1.txt"


@pytest.mark.asyncio
async def test_halonet_command_sends_node_info_view_for_multi_node_graph(monkeypatch):
    class _FakeHaloNetDB:
        def get_player(self, xuid):
            return {"xuid": xuid, "gamertag": "Seed"}

        def get_halo_features(self, xuid):
            return {"kd_ratio": 1.42, "win_rate": 55.5, "matches_played": 220}

        def get_coplay_neighbors(self, xuid, min_matches=2, limit=59):
            return [
                {
                    "partner_xuid": "friend-1",
                    "gamertag": "Friend 1",
                    "matches_together": 8,
                    "wins_together": 4,
                    "total_minutes": 120,
                    "same_team_count": 6,
                    "opposing_team_count": 2,
                    "first_played": "2025-01-01T00:00:00Z",
                    "last_played": "2026-01-01T00:00:00Z",
                    "kd_ratio": 1.10,
                    "win_rate": 52.0,
                    "matches_played": 90,
                }
            ]

        def get_coplay_edges_within_set(self, xuids, min_matches=1):
            return [
                {
                    "src_xuid": "seed-xuid",
                    "dst_xuid": "friend-1",
                    "matches_together": 8,
                    "wins_together": 4,
                    "total_minutes": 120,
                    "same_team_count": 6,
                    "opposing_team_count": 2,
                    "first_played": "2025-01-01T00:00:00Z",
                    "last_played": "2026-01-01T00:00:00Z",
                }
            ]

        def close(self):
            return None

    class _InlineHaloNetCtx:
        def __init__(self):
            self.sent = []
            self.author = SimpleNamespace(id=42)

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            return _FakeMessage()

    async def _fake_resolve_gamertag_to_xuid(gamertag):
        return "seed-xuid"

    monkeypatch.setattr(
        halonet_module,
        "api_client",
        SimpleNamespace(resolve_gamertag_to_xuid=_fake_resolve_gamertag_to_xuid),
    )

    cog = HaloNetCog(bot=object())
    cog.db.close()
    cog.db = _FakeHaloNetDB()
    monkeypatch.setattr(cog, "_render_coplay_graph", lambda *args, **kwargs: BytesIO(b"png"))

    ctx = _InlineHaloNetCtx()
    await HaloNetCog.show_halonet.callback(cog, ctx, "Seed")

    sent_views = [kwargs.get("view") for _, kwargs in ctx.sent if kwargs.get("view") is not None]
    assert any(isinstance(view, HaloNetFilterView) for view in sent_views)
    assert any(isinstance(view, HaloNetNodeInfoView) for view in sent_views)


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
async def test_crawlgames_non_inline_completion_edit_failure_emits_fallback_message(monkeypatch):
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
        def get_scope_match_participants(self, _scope_xuids):
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

    class _FlakyProgressMessage(_TrackingMessage):
        def __init__(self):
            super().__init__()
            self.completed_edit_attempts = 0

        async def edit(self, *args, **kwargs):
            embed = kwargs.get("embed")
            if embed is not None:
                status_field = next((field for field in embed.fields if field.name == "Status"), None)
                if status_field and str(status_field.value) == "COMPLETED":
                    self.completed_edit_attempts += 1
                    raise RuntimeError("simulated completed edit failure")
            return await super().edit(*args, **kwargs)

    class _NonInlineCtx:
        def __init__(self, progress_message):
            self.sent = []
            self.channel = self
            self.author = SimpleNamespace(id=777)
            self._progress_message = progress_message

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            if kwargs.get("embed") is not None:
                return self._progress_message
            return _TrackingMessage()

    fake_api_client = SimpleNamespace(stats_cache=SimpleNamespace(db=_FakeStatsDB()))

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    fake_api_client.resolve_gamertag_to_xuid = _resolve_gamertag_to_xuid

    from src.bot.cogs import graph as graph_module

    monkeypatch.setattr(graph_module, "api_client", fake_api_client)

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()
    monkeypatch.setattr(cog, "_collect_halo_active_scope", lambda seed_xuid, depth: ["friend-xuid"])

    progress_message = _FlakyProgressMessage()
    ctx = _NonInlineCtx(progress_message)

    await GraphCog.start_crawl_games.callback(cog, ctx, "SeedTag", "1")
    assert cog._crawl_task is not None
    await cog._crawl_task

    assert progress_message.completed_edit_attempts >= 1

    status_values = []
    for edit in progress_message.edits:
        embed = edit["kwargs"].get("embed")
        if embed is None:
            continue
        status_field = next((field for field in embed.fields if field.name == "Status"), None)
        if status_field:
            status_values.append(str(status_field.value))

    assert status_values
    assert status_values[-1] == "RUNNING"

    text_messages = [args[0] for args, _kwargs in ctx.sent if args and isinstance(args[0], str)]
    assert any("Co-play crawl completed for **SeedTag**." in message for message in text_messages)


@pytest.mark.asyncio
async def test_crawlgames_skips_unchanged_edges_with_snapshot(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.upserts = []
            self.batch_calls = []
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

        def get_coplay_edges_snapshot(self, _pairs):
            base = {
                "matches_together": 1,
                "wins_together": 0,
                "first_played": "2026-01-01T00:00:00",
                "last_played": "2026-01-01T00:00:00",
                "total_minutes": 0,
                "same_team_count": 1,
                "opposing_team_count": 0,
                "source_type": "participants-runtime",
                "is_inferred": 0,
                "is_partial": 0,
                "coverage_ratio": 1.0,
                "is_halo_active_pair": 1,
            }
            return {
                ("seed-xuid", "friend-xuid"): {"src_xuid": "seed-xuid", "dst_xuid": "friend-xuid", **base},
                ("friend-xuid", "seed-xuid"): {"src_xuid": "friend-xuid", "dst_xuid": "seed-xuid", **base},
            }

        def upsert_coplay_edges_batch(self, edges, suppress_errors=False):
            self.batch_calls.append((list(edges), suppress_errors))
            return {"ok": True, "written": len(edges), "failed": 0}

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

    assert "rows written 0" in result
    assert "unchanged skipped 2" in result
    assert "changed rows targeted 0" in result
    assert cog.db.batch_calls == []
    assert cog.db.upserts == []


@pytest.mark.asyncio
async def test_crawlgames_uses_batch_upsert_for_changed_edges(monkeypatch):
    class _FakeGraphDB:
        def __init__(self):
            self.batch_calls = []
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

        def get_coplay_edges_snapshot(self, _pairs):
            return {}

        def upsert_coplay_edges_batch(self, edges, suppress_errors=False):
            self.batch_calls.append((list(edges), suppress_errors))
            return {"ok": True, "written": len(edges), "failed": 0}

        def insert_or_update_players_stub_batch(self, xuids):
            normalized = sorted(str(x).strip() for x in xuids if str(x).strip())
            self.stub_batches.append(normalized)
            return len(normalized)

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

    assert "rows written 2" in result
    assert "changed rows targeted 2" in result
    assert len(cog.db.batch_calls) == 1
    batch_rows, suppress_errors = cog.db.batch_calls[0]
    assert suppress_errors is True
    assert len(batch_rows) == 2
    assert {row["src_xuid"] for row in batch_rows} == {"seed-xuid", "friend-xuid"}
    assert {row["dst_xuid"] for row in batch_rows} == {"seed-xuid", "friend-xuid"}


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
    backfill_calls = []

    async def _resolve_gamertag_to_xuid(_gamertag):
        return "seed-xuid"

    async def _fake_backfill_seed_match_participants(seed_xuid, seed_gamertag):
        backfill_calls.append((seed_xuid, seed_gamertag))
        return {
            "ok": True,
            "verified_matches": 12,
            "complete_matches_before": 10,
            "incomplete_matches_before": 2,
            "attempted_backfills": 2,
            "successful_backfills": 2,
            "failed_backfills": 0,
            "complete_matches_after": 12,
            "incomplete_matches_after": 0,
        }

    fake_api_client.resolve_gamertag_to_xuid = _resolve_gamertag_to_xuid
    fake_api_client.backfill_seed_match_participants = _fake_backfill_seed_match_participants

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
    assert backfill_calls == [("seed-xuid", "SeedTag")]
    assert "Scoped participants" in result
    assert len(cog.db.upserts) == 2
    assert fake_stats_db.scoped_calls


@pytest.mark.asyncio
async def test_crawlgames_seed_hydration_uses_lifetime_db_first(monkeypatch):
    captured = {}

    async def _fake_calculate_comprehensive_stats(**kwargs):
        captured.update(kwargs)
        return {
            "error": 0,
            "processed_matches": [
                {"all_participants": [{"xuid": "seed-xuid"}]},
                {"all_participants": []},
            ],
        }

    monkeypatch.setattr(
        graph_module,
        "api_client",
        SimpleNamespace(calculate_comprehensive_stats=_fake_calculate_comprehensive_stats),
    )

    cog = GraphCog(bot=object())
    cog.db.close()

    result = await cog._hydrate_seed_match_history("seed-xuid", "SeedTag")

    assert result["ok"] is True
    assert result["matches_processed"] == 2
    assert result["matches_with_participants"] == 1
    assert captured["xuid"] == "seed-xuid"
    assert captured["stat_type"] == "overall"
    assert captured["gamertag"] == "SeedTag"
    assert captured["matches_to_process"] is None
    assert captured["force_full_fetch"] is False


@pytest.mark.asyncio
async def test_halonet_seed_hydration_uses_lifetime_db_first(monkeypatch):
    captured = {}

    async def _fake_calculate_comprehensive_stats(**kwargs):
        captured.update(kwargs)
        return {
            "error": 0,
            "processed_matches": [
                {"all_participants": [{"xuid": "seed-xuid"}]},
            ],
        }

    monkeypatch.setattr(
        halonet_module,
        "api_client",
        SimpleNamespace(calculate_comprehensive_stats=_fake_calculate_comprehensive_stats),
    )

    cog = HaloNetCog(bot=object())
    cog.db.close()

    result = await cog._hydrate_seed_match_history("seed-xuid", "SeedTag")

    assert result["ok"] is True
    assert result["matches_processed"] == 1
    assert result["matches_with_participants"] == 1
    assert captured["xuid"] == "seed-xuid"
    assert captured["stat_type"] == "overall"
    assert captured["gamertag"] == "SeedTag"
    assert captured["matches_to_process"] is None
    assert captured["force_full_fetch"] is False


@pytest.mark.asyncio
async def test_halonet_seed_rebuild_reads_unbounded_seed_participants(monkeypatch):
    class _FakeStatsDB:
        def __init__(self):
            self.seed_calls = []

        def get_seed_match_participants(self, seed_xuid, limit_matches=None):
            self.seed_calls.append((seed_xuid, limit_matches))
            return {}

    class _FakeGraphDB:
        def __init__(self):
            self.stub_batches = []

        def insert_or_update_players_stub_batch(self, xuids):
            self.stub_batches.append(sorted(str(x) for x in xuids))
            return len(xuids)

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

        def close(self):
            return None

    fake_stats_db = _FakeStatsDB()
    monkeypatch.setattr(
        halonet_module,
        "api_client",
        SimpleNamespace(stats_cache=SimpleNamespace(db=fake_stats_db)),
    )

    cog = HaloNetCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()

    result = await cog._rebuild_seed_coplay_edges_from_stats_cache("seed-xuid")

    assert result["ok"] is True
    assert fake_stats_db.seed_calls == [("seed-xuid", None)]
    assert cog.db.stub_batches


@pytest.mark.asyncio
async def test_halonet_auto_heal_runs_backfill_before_rebuild(monkeypatch):
    cog = HaloNetCog(bot=object())
    call_order = []

    async def _fake_hydrate(seed_xuid, seed_gamertag):
        call_order.append("hydrate")
        return {
            "ok": True,
            "matches_processed": 5,
            "matches_with_participants": 3,
        }

    async def _fake_backfill(seed_xuid, seed_gamertag, limit_matches=None):
        call_order.append("backfill")
        return {
            "ok": True,
            "verified_matches": 5,
            "complete_matches_before": 3,
            "incomplete_matches_before": 2,
            "attempted_backfills": 2,
            "successful_backfills": 2,
            "failed_backfills": 0,
            "complete_matches_after": 5,
            "incomplete_matches_after": 0,
        }

    async def _fake_rebuild(seed_xuid):
        call_order.append("rebuild")
        return {
            "ok": True,
            "message": "Seed co-play rebuild complete.",
            "seed_pairs": 1,
            "rows_written": 2,
            "write_failures": 0,
        }

    monkeypatch.setattr(cog, "_hydrate_seed_match_history", _fake_hydrate)
    monkeypatch.setattr(cog, "_rebuild_seed_coplay_edges_from_stats_cache", _fake_rebuild)
    monkeypatch.setattr(
        halonet_module,
        "api_client",
        SimpleNamespace(backfill_seed_match_participants=_fake_backfill),
    )

    result = await cog._run_halonet_auto_heal("seed-xuid", "Seed")

    assert call_order == ["hydrate", "backfill", "rebuild"]
    assert result["ok"] is True
    assert result["backfill"]["complete_matches_after"] == 5
    assert "Participant coverage 5/5; repaired 2/2." in result["message"]


@pytest.mark.asyncio
async def test_halonet_attempt_cooldown_uses_rebuild_outcome_even_with_backfill_payload(monkeypatch):
    cog = HaloNetCog(bot=object())

    async def _fake_run(seed_xuid, seed_gamertag):
        return {
            "ok": False,
            "message": "Auto-refresh completed, but no seed co-play pairs were found in persisted match participants.",
            "backfill": {
                "ok": True,
                "verified_matches": 6,
                "complete_matches_before": 3,
                "complete_matches_after": 6,
            },
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


@pytest.mark.asyncio
async def test_graphstats_embed_includes_participant_coverage_field():
    class _FakeGraphDB:
        def get_graph_stats(self):
            return {
                "total_players": 125,
                "halo_active_players": 75,
                "total_friend_edges": 410,
                "total_coplay_edges": 222,
                "players_with_stats": 66,
                "avg_friend_degree": 3.28,
                "avg_halo_friend_degree": 2.14,
                "depth_distribution": {0: 1, 1: 14},
                "db_size_mb": 42.42,
                "participant_coverage": {
                    "total_edges": 13,
                    "complete_edges": 10,
                    "partial_edges": 3,
                    "avg_coverage_ratio": 0.875,
                },
            }

        def close(self):
            return None

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()

    ctx = _FakeCtx()
    await GraphCog.graph_stats.callback(cog, ctx)

    assert ctx.sent
    embed = ctx.sent[-1][1]["embed"]
    coverage_field = next((field for field in embed.fields if field.name == "Participant Coverage"), None)

    assert coverage_field is not None
    assert coverage_field.value == "**10** complete | **3** partial | Avg: **87.5%**"


@pytest.mark.asyncio
async def test_graphstats_embed_defaults_participant_coverage_when_missing():
    class _FakeGraphDB:
        def get_graph_stats(self):
            return {
                "total_players": 5,
                "halo_active_players": 3,
                "total_friend_edges": 4,
                "total_coplay_edges": 2,
                "players_with_stats": 1,
                "avg_friend_degree": 0.8,
                "avg_halo_friend_degree": 0.6,
                "depth_distribution": {},
                "db_size_mb": 1.23,
            }

        def close(self):
            return None

    cog = GraphCog(bot=object())
    cog.db.close()
    cog.db = _FakeGraphDB()

    ctx = _FakeCtx()
    await GraphCog.graph_stats.callback(cog, ctx)

    embed = ctx.sent[-1][1]["embed"]
    coverage_field = next((field for field in embed.fields if field.name == "Participant Coverage"), None)

    assert coverage_field is not None
    assert coverage_field.value == "**0** complete | **0** partial | Avg: **0.0%**"


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

    monkeypatch.setattr(
        halonet_module,
        "api_client",
        SimpleNamespace(resolve_gamertag_to_xuid=_resolve_gamertag_to_xuid),
    )

    cog = HaloNetCog(bot=object())
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
    await HaloNetCog.show_halonet.callback(cog, ctx, "HalcyonVidar")

    # First send is loading embed, second is final graph embed+file.
    assert len(ctx.sent) >= 2
    final_kwargs = next((kwargs for _, kwargs in reversed(ctx.sent) if "embed" in kwargs), None)
    assert final_kwargs is not None
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

    monkeypatch.setattr(
        halonet_module,
        "api_client",
        SimpleNamespace(resolve_gamertag_to_xuid=_resolve_gamertag_to_xuid),
    )

    cog = HaloNetCog(bot=object())
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
    await HaloNetCog.show_halonet.callback(cog, ctx, "HalcyonVidar")

    assert auto_heal_calls["count"] == 1
    assert len(ctx.sent) >= 2

    final_kwargs = next((kwargs for _, kwargs in reversed(ctx.sent) if "embed" in kwargs), None)
    assert final_kwargs is not None
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

    monkeypatch.setattr(
        halonet_module,
        "api_client",
        SimpleNamespace(resolve_gamertag_to_xuid=_resolve_gamertag_to_xuid),
    )

    cog = HaloNetCog(bot=object())
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
    await HaloNetCog.show_halonet.callback(cog, ctx, "HalcyonVidar")

    assert len(ctx.sent) >= 2
    final_args = ctx.sent[-1][0]
    assert final_args
    assert "after auto-refresh" in final_args[0]
    assert "Auto-refresh skipped" in final_args[0]
    assert "#crawlgames HalcyonVidar" in final_args[0]


@pytest.mark.asyncio
async def test_halonet_failed_auto_heal_uses_short_retry_window(monkeypatch):
    cog = HaloNetCog(bot=object())

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
    cog = HaloNetCog(bot=object())

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
