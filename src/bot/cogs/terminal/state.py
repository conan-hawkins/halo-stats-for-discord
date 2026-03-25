from dataclasses import dataclass, field
from datetime import datetime
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
        TerminalMenuItem("STATS", submenu="stats", description="Open full/ranked/casual lookup and cache population commands."),
        TerminalMenuItem("SOCIAL", submenu="social", description="Open live friends lookup, graph visualization, and similarity tools."),
        TerminalMenuItem("CRAWL", submenu="crawl", description="Open background crawl controls for friends expansion and co-play modeling."),
    ],
    "status": [
        TerminalMenuItem("GRAPH STATS", action="status_graph", description="Show total graph players, active players, and edges."),
        TerminalMenuItem("XUID CACHE STATUS", action="status_cache", description="Show XUID cache totals and active cache progress."),
    ],
    "stats": [
        TerminalMenuItem("FULL STATS", action="cmd_full", requires_input=True, input_hint="Gamertag", description="Run the full stats command for a player."),
        TerminalMenuItem("RANKED STATS", action="cmd_ranked", requires_input=True, input_hint="Gamertag", description="Run ranked-only stats for a player."),
        TerminalMenuItem("CASUAL STATS", action="cmd_casual", requires_input=True, input_hint="Gamertag", description="Run casual-only stats for a player."),
        TerminalMenuItem("POPULATE CACHE", action="cmd_populate", requires_input=True, input_hint="Gamertag", description="Fetch and cache player data for faster future lookups."),
    ],
    "social": [
        TerminalMenuItem("XBOX FRIENDS", action="cmd_xboxfriends", requires_input=True, input_hint="Gamertag", description="Show Xbox friends for a player."),
        TerminalMenuItem("NETWORK", action="cmd_network", requires_input=True, input_hint="Gamertag", description="Build and display a player's local network graph."),
        TerminalMenuItem("HALONET", action="cmd_halonet", requires_input=True, input_hint="Gamertag", description="Build and display a player's co-play network graph."),
        TerminalMenuItem("SIMILAR", action="cmd_similar", requires_input=True, input_hint="Gamertag", description="Find players with similar match-history patterns."),
        TerminalMenuItem("HUBS", action="cmd_hubs", requires_input=True, input_hint="Min friends (optional)", description="Find highly connected hub players in the graph."),
    ],
    "crawl": [
        TerminalMenuItem("START FRIEND CRAWL", action="cmd_crawlfriends", requires_input=True, input_hint="Gamertag|Depth (depth optional)", description="Run #crawlfriends to expand Halo-active friend graph from a seed player."),
        TerminalMenuItem("BUILD CO-PLAY EDGES", action="cmd_crawlgames", requires_input=True, input_hint="Gamertag|Depth (depth optional)", description="Run #crawlgames to compute weighted co-play edges from shared match history."),
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
    is_loading: bool = False
    loading_label: str = ""
    loading_stage: str = ""
    loading_started_at: Optional[datetime] = None
    loading_tick: int = 0
    progress_percent: Optional[float] = None
    progress_detail: str = ""

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

    def begin_loading(self, label: str, stage: str = "Starting") -> None:
        self.is_loading = True
        self.loading_label = label
        self.loading_stage = stage
        self.loading_started_at = datetime.now()
        self.loading_tick = 0
        self.progress_percent = None
        self.progress_detail = ""

    def bump_loading_tick(self) -> None:
        self.loading_tick += 1

    def end_loading(self) -> None:
        self.is_loading = False
        self.loading_label = ""
        self.loading_stage = ""
        self.loading_started_at = None
        self.loading_tick = 0
        self.progress_percent = None
        self.progress_detail = ""
