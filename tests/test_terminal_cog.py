import json

import pytest

from src.bot.cogs.terminal import router as terminal_router
from src.bot.cogs.terminal import render as terminal_render
from src.bot.cogs.terminal.router import execute_terminal_action, parse_crawl_input
from src.bot.cogs.terminal.state import TerminalState


def test_terminal_state_navigation_wraps_in_menu():
    state = TerminalState(requester_id=1)

    assert state.menu_key == "root"
    assert state.current_item().label == "DATABASE STATUS"

    state.move_up()
    assert state.current_item().label == "CRAWL"

    state.move_down()
    assert state.current_item().label == "DATABASE STATUS"


def test_terminal_state_submenu_and_back():
    state = TerminalState(requester_id=1)

    state.enter_submenu("stats")
    assert state.menu_key == "stats"
    assert state.current_item().label == "FULL STATS"

    state.move_down()
    assert state.current_item().label == "RANKED STATS"

    state.go_back()
    assert state.menu_key == "root"
    assert state.current_item().label == "DATABASE STATUS"


def test_terminal_stats_menu_excludes_server_leaderboard():
    state = TerminalState(requester_id=1)
    state.enter_submenu("stats")

    labels = [item.label for item in state.current_menu()]
    assert "SERVER LEADERBOARD" not in labels


def test_terminal_crawl_menu_includes_new_crawl_actions():
    state = TerminalState(requester_id=1)
    state.enter_submenu("crawl")

    labels = [item.label for item in state.current_menu()]
    assert "START FRIEND CRAWL" in labels
    assert "BUILD CO-PLAY EDGES" in labels
    assert "START CRAWL" not in labels


def test_terminal_social_menu_includes_halonet():
    state = TerminalState(requester_id=1)
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
    state = TerminalState(requester_id=1)
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
    state = TerminalState(requester_id=1)
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


class _FakeAuthor:
    guild_permissions = _FakePerms()


class _FakeCtx:
    author = _FakeAuthor()


class _FakeGraphCog:
    def __init__(self):
        self.calls = []

    async def show_halonet(self, ctx, *args, **kwargs):
        self.calls.append(("halonet", args, kwargs))

    async def start_crawl(self, ctx, *args, **kwargs):
        self.calls.append(("crawlfriends", args, kwargs))

    async def start_crawl_games(self, ctx, *args, **kwargs):
        self.calls.append(("crawlgames", args, kwargs))


class _FakeBotWithGraph:
    def __init__(self, graph_cog):
        self.graph_cog = graph_cog

    def get_cog(self, name):
        if name == "Stats":
            return object()
        if name == "Graph":
            return self.graph_cog
        return None


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
async def test_terminal_router_dispatches_halonet():
    graph_cog = _FakeGraphCog()
    bot = _FakeBotWithGraph(graph_cog)

    output = await execute_terminal_action(bot, _FakeCtx(), "cmd_halonet", "Chief117")

    assert output == "Executed #halonet for Chief117"
    assert graph_cog.calls[0] == ("halonet", ("Chief117",), {})
