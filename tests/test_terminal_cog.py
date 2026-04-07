import json

import pytest

from src.bot.cogs.terminal import router as terminal_router
from src.bot.cogs.terminal import render as terminal_render
from src.bot.cogs.terminal.router import execute_terminal_action, parse_crawl_input
from src.bot.cogs.terminal.state import TerminalState


def _admin_state() -> TerminalState:
    state = TerminalState(requester_id=1)
    state.set_access_level("admin")
    return state


def test_terminal_state_navigation_wraps_in_menu():
    state = _admin_state()

    assert state.menu_key == "root"
    assert state.current_item().label == "DATABASE STATUS"

    state.move_up()
    assert state.current_item().label == "CRAWL"

    state.move_down()
    assert state.current_item().label == "DATABASE STATUS"


def test_terminal_state_submenu_and_back():
    state = _admin_state()

    state.enter_submenu("stats")
    assert state.menu_key == "stats"
    assert state.current_item().label == "FULL STATS"

    state.move_down()
    assert state.current_item().label == "RANKED STATS"

    state.go_back()
    assert state.menu_key == "root"
    assert state.current_item().label == "DATABASE STATUS"


def test_terminal_stats_menu_excludes_server_leaderboard():
    state = _admin_state()
    state.enter_submenu("stats")

    labels = [item.label for item in state.current_menu()]
    assert "SERVER LEADERBOARD" not in labels


def test_terminal_crawl_menu_includes_new_crawl_actions():
    state = _admin_state()
    state.enter_submenu("crawl")

    labels = [item.label for item in state.current_menu()]
    assert "START FRIEND CRAWL" in labels
    assert "BUILD CO-PLAY EDGES" in labels
    assert "START CRAWL" not in labels


def test_terminal_root_menu_includes_iss():
    state = _admin_state()

    labels = [item.label for item in state.current_menu()]
    assert "ISS" in labels


def test_terminal_iss_menu_includes_all_levels():
    state = _admin_state()
    state.enter_submenu("iss")

    labels = [item.label for item in state.current_menu()]
    assert labels == [
        "ISS LEVEL 0",
        "ISS LEVEL 1",
        "ISS LEVEL 2",
        "ISS LEVEL 3",
    ]


def test_terminal_social_menu_includes_halonet():
    state = _admin_state()
    state.enter_submenu("social")

    labels = [item.label for item in state.current_menu()]
    assert "HALONET" in labels


def test_parse_crawl_input_pipe_delimited_with_depth():
    gamertag, depth = parse_crawl_input("Chief117|3")
    assert gamertag == "Chief117"
    assert depth == 3


def test_parse_crawl_input_space_delimited_with_depth():
    gamertag, depth = parse_crawl_input("Master Chief 2")
    assert gamertag == "Master Chief"
    assert depth == 2


def test_parse_crawl_input_without_depth():
    gamertag, depth = parse_crawl_input("Arbiter")
    assert gamertag == "Arbiter"
    assert depth is None


def test_terminal_state_loading_lifecycle():
    state = _admin_state()
    state.begin_loading("FULL STATS", "Running command")

    assert state.is_loading is True
    assert state.loading_label == "FULL STATS"
    assert state.loading_stage == "Running command"
    assert state.loading_started_at is not None

    tick_before = state.loading_tick
    state.bump_loading_tick()
    assert state.loading_tick == tick_before + 1

    state.end_loading()
    assert state.is_loading is False
    assert state.loading_label == ""
    assert state.loading_stage == ""
    assert state.loading_started_at is None
    assert state.loading_tick == 0


def test_terminal_render_includes_loading_block_when_active():
    state = _admin_state()
    state.begin_loading("NETWORK", "Running command")
    state.bump_loading_tick()
    state.last_output = "Running NETWORK..."

    text = terminal_render._build_lines(state)

    assert "LOADING:" in text
    assert "NETWORK" in text
    assert "Stage: Running command" in text
    assert "Elapsed:" in text


class _FakeBot:
    def get_cog(self, _name):
        return None


class _FakePerms:
    administrator = True


class _FakePermsNonAdmin:
    administrator = False


class _FakeAuthor:
    guild_permissions = _FakePerms()
    display_name = "Admin Tester"


class _FakeAuthorNonAdmin:
    guild_permissions = _FakePermsNonAdmin()
    display_name = "User Tester"


def test_terminal_starts_locked_on_login_menu():
    state = TerminalState(requester_id=1)

    assert state.is_authenticated is False
    assert state.menu_key == "login"
    labels = [item.label for item in state.current_menu()]
    assert labels == ["ENTER USER TERMINAL", "ENTER ADMIN TERMINAL"]


def test_terminal_user_mode_root_shows_only_stats_and_iss():
    state = TerminalState(requester_id=1)
    state.set_access_level("user")

    labels = [item.label for item in state.current_menu()]
    assert labels == ["STATS", "ISS"]


def test_terminal_user_mode_hides_iss_levels_2_and_3():
    state = TerminalState(requester_id=1)
    state.set_access_level("user")
    state.enter_submenu("iss")

    labels = [item.label for item in state.current_menu()]
    assert labels == ["ISS LEVEL 0", "ISS LEVEL 1"]


def test_terminal_render_shows_login_screen_when_locked():
    state = TerminalState(requester_id=1)

    text = terminal_render._build_lines(state)
    assert "ACCESS: LOCKED" in text
    assert "LOGIN:" in text
    assert "ENTER USER TERMINAL" in text
    assert "ENTER ADMIN TERMINAL" in text


class _FakeCtx:
    author = _FakeAuthor()


class _FakeCtxNonAdmin:
    author = _FakeAuthorNonAdmin()


class _FakeGraphCog:
    def __init__(self):
        self.calls = []

    async def show_network(self, ctx, *args, **kwargs):
        self.calls.append(("network", args, kwargs))

    async def show_halonet(self, ctx, *args, **kwargs):
        self.calls.append(("halonet", args, kwargs))

    async def start_crawl(self, ctx, *args, **kwargs):
        self.calls.append(("crawlfriends", args, kwargs))

    async def start_crawl_games(self, ctx, *args, **kwargs):
        self.calls.append(("crawlgames", args, kwargs))

    async def iss_level0(self, ctx, *args, **kwargs):
        self.calls.append(("iss_level0", args, kwargs))
        return "ISS level 0 done"

    async def iss_level1(self, ctx, *args, **kwargs):
        self.calls.append(("iss_level1", args, kwargs))
        return "ISS level 1 done"

    async def iss_level2(self, ctx, *args, **kwargs):
        self.calls.append(("iss_level2", args, kwargs))
        return "ISS level 2 done"

    async def iss_level3(self, ctx, *args, **kwargs):
        self.calls.append(("iss_level3", args, kwargs))
        return "ISS level 3 done"


class _FakeBotWithGraph:
    def __init__(self, graph_cog):
        self.graph_cog = graph_cog

    def get_cog(self, name):
        if name == "Stats":
            return object()
        if name == "Graph":
            return self.graph_cog
        return None


class _FakeNetworkCog:
    def __init__(self):
        self.calls = []

    async def show_network(self, ctx, *args, **kwargs):
        self.calls.append(("network", args, kwargs))


class _FakeHaloNetCog:
    def __init__(self):
        self.calls = []

    async def show_halonet(self, ctx, *args, **kwargs):
        self.calls.append(("halonet", args, kwargs))


class _FakeBotWithStandaloneSocialCogs:
    def __init__(self, graph_cog, network_cog=None, halonet_cog=None):
        self.graph_cog = graph_cog
        self.network_cog = network_cog
        self.halonet_cog = halonet_cog

    def get_cog(self, name):
        if name == "Stats":
            return object()
        if name == "Graph":
            return self.graph_cog
        if name == "Network":
            return self.network_cog
        if name == "HaloNet":
            return self.halonet_cog
        return None


class _FakeStatsCog:
    def __init__(self):
        self.calls = []

    async def run_stats_profile(self, ctx, profile, gamertag):
        self.calls.append((profile.command_name, profile.fetch_stat_type, gamertag))


class _FakeBotWithStats:
    def __init__(self, stats_cog):
        self.stats_cog = stats_cog

    def get_cog(self, name):
        if name == "Stats":
            return self.stats_cog
        if name == "Graph":
            return None
        return None


@pytest.mark.asyncio
async def test_terminal_router_dispatches_stats_profiles():
    stats_cog = _FakeStatsCog()
    bot = _FakeBotWithStats(stats_cog)
    ctx = _FakeCtx()

    out_full = await execute_terminal_action(bot, ctx, "cmd_full", "Chief117")
    out_ranked = await execute_terminal_action(bot, ctx, "cmd_ranked", "Chief117")
    out_casual = await execute_terminal_action(bot, ctx, "cmd_casual", "Chief117")

    assert out_full == "Executed #full for Chief117"
    assert out_ranked == "Executed #ranked for Chief117"
    assert out_casual == "Executed #casual for Chief117"
    assert stats_cog.calls == [
        ("full", "stats", "Chief117"),
        ("ranked", "ranked", "Chief117"),
        ("casual", "social", "Chief117"),
    ]


@pytest.mark.asyncio
async def test_terminal_status_cache_shows_resolved_gamertags_over_xuid_count(tmp_path, monkeypatch):
    cache_file = tmp_path / "xuid_gamertag_cache.json"
    progress_file = tmp_path / "cache_progress.json"

    cache_file.write_text(json.dumps({"1": "One", "2": "Two"}), encoding="utf-8")
    progress_file.write_text(
        json.dumps(
            {
                "processed_matches": 50,
                "total_matches": 100,
                "resolved_gamertags": ["A", "B", "C", "D", "E"],
                "unique_players": ["x1", "x2", "x3"],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(terminal_router, "XUID_CACHE_FILE", str(cache_file))
    monkeypatch.setattr(terminal_router, "CACHE_PROGRESS_FILE", str(progress_file))
    monkeypatch.setattr(terminal_router, "PROJECT_ROOT", str(tmp_path))

    output = await execute_terminal_action(_FakeBot(), None, "status_cache")

    assert "XUID cache mappings: 2" in output
    assert "Resolved GTs: 5" in output
    assert "Match scan: 50/100 (50.0%)" in output
    assert "Unique players" not in output


@pytest.mark.asyncio
async def test_terminal_status_cache_missing_progress_removes_unique_player_line(tmp_path, monkeypatch):
    cache_file = tmp_path / "xuid_gamertag_cache.json"
    cache_file.write_text(json.dumps({"1": "One", "2": "Two", "3": "Three"}), encoding="utf-8")

    missing_progress_file = tmp_path / "not_here.json"

    monkeypatch.setattr(terminal_router, "XUID_CACHE_FILE", str(cache_file))
    monkeypatch.setattr(terminal_router, "CACHE_PROGRESS_FILE", str(missing_progress_file))
    monkeypatch.setattr(terminal_router, "PROJECT_ROOT", str(tmp_path / "other_root"))

    output = await execute_terminal_action(_FakeBot(), None, "status_cache")

    assert "XUID cache mappings: 3" in output
    assert "Resolved GTs: 3" in output
    assert "Progress: No active progress file" in output
    assert "Unique players" not in output


@pytest.mark.asyncio
async def test_terminal_router_blocks_user_mode_for_non_stats_and_non_iss_actions():
    output = await execute_terminal_action(_FakeBot(), _FakeCtx(), "status_cache", access_level="user")
    assert output == "Action not available in user terminal mode."


@pytest.mark.asyncio
async def test_terminal_router_allows_user_mode_for_iss_level0():
    graph_cog = _FakeGraphCog()
    bot = _FakeBotWithGraph(graph_cog)

    output = await execute_terminal_action(bot, _FakeCtx(), "cmd_iss_level0", "Chief117", access_level="user")

    assert output == "ISS level 0 done"


@pytest.mark.asyncio
async def test_terminal_router_dispatches_crawlfriends_and_crawlgames():
    graph_cog = _FakeGraphCog()
    bot = _FakeBotWithGraph(graph_cog)
    ctx = _FakeCtx()

    out1 = await execute_terminal_action(bot, ctx, "cmd_crawlfriends", "Chief117|2")
    out2 = await execute_terminal_action(bot, ctx, "cmd_crawlgames", "Chief117|3")

    assert out1 == "Executed #crawlfriends Chief117 2"
    assert out2 == "Executed #crawlgames Chief117 3"
    assert graph_cog.calls[0] == (
        "crawlfriends",
        ("Chief117", "2"),
        {"progress_callback": None, "run_inline": False},
    )
    assert graph_cog.calls[1] == (
        "crawlgames",
        ("Chief117", "3"),
        {"progress_callback": None, "run_inline": False},
    )


@pytest.mark.asyncio
async def test_terminal_router_requires_standalone_halonet_cog():
    graph_cog = _FakeGraphCog()
    bot = _FakeBotWithGraph(graph_cog)

    output = await execute_terminal_action(bot, _FakeCtx(), "cmd_halonet", "Chief117")

    assert output == "HaloNet cog unavailable"
    assert graph_cog.calls == []


@pytest.mark.asyncio
async def test_terminal_router_prefers_standalone_network_cog():
    graph_cog = _FakeGraphCog()
    network_cog = _FakeNetworkCog()
    bot = _FakeBotWithStandaloneSocialCogs(graph_cog=graph_cog, network_cog=network_cog)

    output = await execute_terminal_action(bot, _FakeCtx(), "cmd_network", "Chief117")

    assert output == "Executed #network for Chief117"
    assert network_cog.calls == [("network", ("Chief117",), {})]
    assert all(call[0] != "network" for call in graph_cog.calls)


@pytest.mark.asyncio
async def test_terminal_router_requires_standalone_network_cog():
    graph_cog = _FakeGraphCog()
    bot = _FakeBotWithStandaloneSocialCogs(graph_cog=graph_cog, network_cog=None)

    output = await execute_terminal_action(bot, _FakeCtx(), "cmd_network", "Chief117")

    assert output == "Network cog unavailable"
    assert graph_cog.calls == []


@pytest.mark.asyncio
async def test_terminal_router_prefers_standalone_halonet_cog():
    graph_cog = _FakeGraphCog()
    halonet_cog = _FakeHaloNetCog()
    bot = _FakeBotWithStandaloneSocialCogs(graph_cog=graph_cog, halonet_cog=halonet_cog)

    output = await execute_terminal_action(bot, _FakeCtx(), "cmd_halonet", "Chief117")

    assert output == "Executed #halonet for Chief117"
    assert halonet_cog.calls == [("halonet", ("Chief117",), {})]
    assert all(call[0] != "halonet" for call in graph_cog.calls)


@pytest.mark.asyncio
async def test_terminal_router_dispatches_iss_levels():
    graph_cog = _FakeGraphCog()
    bot = _FakeBotWithGraph(graph_cog)
    ctx = _FakeCtx()

    out0 = await execute_terminal_action(bot, ctx, "cmd_iss_level0", "Chief117")
    out1 = await execute_terminal_action(bot, ctx, "cmd_iss_level1", "Chief117")
    out2 = await execute_terminal_action(bot, ctx, "cmd_iss_level2", "Chief117")
    out3 = await execute_terminal_action(bot, ctx, "cmd_iss_level3", "Chief117")

    assert out0 == "ISS level 0 done"
    assert out1 == "ISS level 1 done"
    assert out2 == "ISS level 2 done"
    assert out3 == "ISS level 3 done"
    assert graph_cog.calls[0] == (
        "iss_level0",
        ("Chief117",),
        {"progress_callback": None, "run_inline": False},
    )
    assert graph_cog.calls[1] == (
        "iss_level1",
        ("Chief117",),
        {"progress_callback": None, "run_inline": False},
    )
    assert graph_cog.calls[2] == (
        "iss_level2",
        ("Chief117",),
        {"progress_callback": None, "run_inline": False},
    )
    assert graph_cog.calls[3] == (
        "iss_level3",
        ("Chief117",),
        {"progress_callback": None, "run_inline": False},
    )


@pytest.mark.asyncio
async def test_terminal_router_rejects_iss_levels_2_and_3_for_non_admin():
    graph_cog = _FakeGraphCog()
    bot = _FakeBotWithGraph(graph_cog)
    ctx = _FakeCtxNonAdmin()

    out2 = await execute_terminal_action(bot, ctx, "cmd_iss_level2", "Chief117")
    out3 = await execute_terminal_action(bot, ctx, "cmd_iss_level3", "Chief117")

    assert out2 == "Admin permission required for ISS level 2"
    assert out3 == "Admin permission required for ISS level 3"
    assert all(call[0] not in {"iss_level2", "iss_level3"} for call in graph_cog.calls)
