from src.bot.cogs.terminal.router import parse_crawl_input
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
