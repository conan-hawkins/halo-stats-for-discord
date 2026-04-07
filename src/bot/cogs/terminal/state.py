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
    "login": [
        TerminalMenuItem("ENTER USER TERMINAL", action="auth_user", description="Enter user mode with STATS and ISS access."),
        TerminalMenuItem("ENTER ADMIN TERMINAL", action="auth_admin", requires_input=True, input_hint="Admin password", description="Enter admin mode (password required)."),
    ],
    "root": [
        TerminalMenuItem("DATABASE STATUS", submenu="status", description="Open graph and cache database status tools."),
        TerminalMenuItem("STATS", submenu="stats", description="Open full/ranked/casual lookup commands."),
        TerminalMenuItem("SOCIAL", submenu="social", description="Open live friends lookup and graph visualization tools."),
        TerminalMenuItem("ISS", submenu="iss", description="Run blacklist-aware ISS checks with optional history analysis levels."),
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
    ],
    "social": [
        TerminalMenuItem("XBOX FRIENDS", action="cmd_xboxfriends", requires_input=True, input_hint="Gamertag", description="Show Xbox friends for a player."),
        TerminalMenuItem("NETWORK", action="cmd_network", requires_input=True, input_hint="Gamertag", description="Build and display a player's local network graph."),
        TerminalMenuItem("HALONET", action="cmd_halonet", requires_input=True, input_hint="Gamertag", description="Build and display a player's co-play network graph."),
        TerminalMenuItem("HUBS", action="cmd_hubs", requires_input=True, input_hint="Min friends (optional)", description="Find highly connected hub players in the graph."),
    ],
    "iss": [
        TerminalMenuItem("ISS LEVEL 0", action="cmd_iss_level0", requires_input=True, input_hint="Gamertag", description="Check direct friends against blacklist."),
        TerminalMenuItem("ISS LEVEL 1", action="cmd_iss_level1", requires_input=True, input_hint="Gamertag", description="Check friends and friends-of-friends against blacklist."),
        TerminalMenuItem("ISS LEVEL 2", action="cmd_iss_level2", requires_input=True, input_hint="Gamertag", description="Run level 1 plus 6-month match-history checks for blacklisted players."),
        TerminalMenuItem("ISS LEVEL 3", action="cmd_iss_level3", requires_input=True, input_hint="Gamertag", description="Run level 2 plus full-history checks for blacklisted players."),
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
    menu_key: str = "login"
    selected_index: int = 0
    menu_stack: List[str] = field(default_factory=list)
    last_output: str = "READY"
    last_error: str = ""
    access_level: Optional[str] = None
    login_error: str = ""
    is_loading: bool = False
    loading_label: str = ""
    loading_stage: str = ""
    loading_started_at: Optional[datetime] = None
    loading_tick: int = 0
    progress_percent: Optional[float] = None
    progress_detail: str = ""

    @property
    def is_authenticated(self) -> bool:
        return self.access_level in {"admin", "user"}

    @property
    def is_admin(self) -> bool:
        return self.access_level == "admin"

    @property
    def is_user(self) -> bool:
        return self.access_level == "user"

    def current_menu(self) -> List[TerminalMenuItem]:
        if not self.is_authenticated:
            return TERMINAL_MENUS["login"]

        menu = TERMINAL_MENUS.get(self.menu_key, TERMINAL_MENUS["root"])
        if not self.is_user:
            return menu

        if self.menu_key == "root":
            return [item for item in menu if item.submenu in {"stats", "iss"}]

        if self.menu_key == "iss":
            return [
                item
                for item in menu
                if item.action in {"cmd_iss_level0", "cmd_iss_level1"}
            ]

        if self.menu_key in {"status", "social", "crawl"}:
            return []

        return menu

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
        if not self.is_authenticated:
            return
        self.menu_stack.append(self.menu_key)
        self.menu_key = submenu_key
        self.selected_index = 0

    def go_back(self) -> None:
        if not self.is_authenticated:
            self.menu_key = "login"
            self.selected_index = 0
            return
        if self.menu_stack:
            self.menu_key = self.menu_stack.pop()
            self.selected_index = 0
        else:
            self.menu_key = "root"
            self.selected_index = 0

    def set_access_level(self, access_level: str) -> None:
        if access_level not in {"admin", "user"}:
            return

        self.access_level = access_level
        self.login_error = ""
        self.last_error = ""
        self.menu_key = "root"
        self.menu_stack.clear()
        self.selected_index = 0

    def logout(self) -> None:
        self.access_level = None
        self.login_error = ""
        self.last_error = ""
        self.menu_key = "login"
        self.menu_stack.clear()
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
