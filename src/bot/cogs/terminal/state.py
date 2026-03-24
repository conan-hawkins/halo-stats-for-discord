from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class TerminalMenuItem:
    label: str
    action: Optional[str] = None
    submenu: Optional[str] = None
    requires_input: bool = False
    input_hint: str = ""
    description: str = ""


TERMINAL_MENUS: Dict[str, List[TerminalMenuItem]] = {
    "root": [
        TerminalMenuItem("DATABASE STATUS", submenu="status", description="Open graph and cache database status tools."),
        TerminalMenuItem("STATS", submenu="stats", description="Open player stats commands and cache actions."),
        TerminalMenuItem("SOCIAL", submenu="social", description="Open friends, network, and similarity commands."),
        TerminalMenuItem("CRAWL", submenu="crawl", description="Open crawl start/stop controls."),
    ],
    "status": [
        TerminalMenuItem("GRAPH STATS", action="status_graph", description="Show total graph players, active players, and edges."),
        TerminalMenuItem("XUID CACHE STATUS", action="status_cache", description="Show XUID cache totals and active cache progress."),
    ],
    "stats": [
        TerminalMenuItem("FULL STATS", action="cmd_full", requires_input=True, input_hint="Gamertag", description="Run the full stats command for a player."),
        TerminalMenuItem("RANKED STATS", action="cmd_ranked", requires_input=True, input_hint="Gamertag", description="Run ranked-only stats for a player."),
        TerminalMenuItem("CASUAL STATS", action="cmd_casual", requires_input=True, input_hint="Gamertag", description="Run casual-only stats for a player."),
        TerminalMenuItem("SERVER LEADERBOARD", action="cmd_server", description="Show the current server leaderboard."),
        TerminalMenuItem("POPULATE CACHE", action="cmd_populate", requires_input=True, input_hint="Gamertag", description="Fetch and cache player data for faster future lookups."),
    ],
    "social": [
        TerminalMenuItem("XBOX FRIENDS", action="cmd_xboxfriends", requires_input=True, input_hint="Gamertag", description="Show Xbox friends for a player."),
        TerminalMenuItem("NETWORK", action="cmd_network", requires_input=True, input_hint="Gamertag", description="Build and display a player's local network graph."),
        TerminalMenuItem("SIMILAR", action="cmd_similar", requires_input=True, input_hint="Gamertag", description="Find players with similar match-history patterns."),
        TerminalMenuItem("HUBS", action="cmd_hubs", requires_input=True, input_hint="Min friends (optional)", description="Find highly connected hub players in the graph."),
    ],
    "crawl": [
        TerminalMenuItem("START CRAWL", action="cmd_crawl", requires_input=True, input_hint="Gamertag|Depth (depth optional)", description="Start graph crawling from a seed gamertag with optional depth."),
        TerminalMenuItem("STOP CRAWL", action="cmd_crawlstop", description="Stop the active crawl process."),
    ],
}


@dataclass
class TerminalState:
    requester_id: int
    menu_key: str = "root"
    selected_index: int = 0
    menu_stack: List[str] = field(default_factory=list)
    last_output: str = "READY"
    last_error: str = ""

    def current_menu(self) -> List[TerminalMenuItem]:
        return TERMINAL_MENUS.get(self.menu_key, TERMINAL_MENUS["root"])

    def current_item(self) -> TerminalMenuItem:
        menu = self.current_menu()
        if not menu:
            return TerminalMenuItem(label="N/A")
        idx = max(0, min(self.selected_index, len(menu) - 1))
        return menu[idx]

    def move_up(self) -> None:
        menu = self.current_menu()
        if not menu:
            return
        self.selected_index = (self.selected_index - 1) % len(menu)

    def move_down(self) -> None:
        menu = self.current_menu()
        if not menu:
            return
        self.selected_index = (self.selected_index + 1) % len(menu)

    def enter_submenu(self, submenu_key: str) -> None:
        self.menu_stack.append(self.menu_key)
        self.menu_key = submenu_key
        self.selected_index = 0

    def go_back(self) -> None:
        if self.menu_stack:
            self.menu_key = self.menu_stack.pop()
            self.selected_index = 0
        else:
            self.menu_key = "root"
            self.selected_index = 0
