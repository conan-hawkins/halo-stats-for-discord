"""HaloNet view and filter controls for display commands."""

import asyncio
import io
import math
from datetime import datetime
from typing import Dict, List, Optional

import discord


NETWORK_CONTROLS_TIMEOUT_SECONDS = 900
HALONET_NODE_FILTER_THRESHOLDS = [
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    15,
    20,
    25,
    30,
    35,
    40,
    50,
    60,
    75,
    100,
    125,
    150,
    175,
    200,
]
HALONET_EDGE_FILTER_THRESHOLDS = [
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    15,
    20,
    25,
    30,
    35,
    40,
    50,
    60,
    75,
    100,
    125,
    150,
    175,
    200,
]
HALONET_GAME_TYPE_OPTIONS = [
    ("all", "All game types", "Use total shared matches across all categories"),
    ("ranked", "Ranked only", "Reweight edges using ranked matches only"),
    ("social", "Social only", "Reweight edges using social matches only"),
    ("custom", "Custom only", "Reweight edges using custom matches only"),
]
HALONET_GAME_TYPE_LABELS = {
    "all": "All",
    "ranked": "Ranked",
    "social": "Social",
    "custom": "Custom",
}


class HaloNetNodeInfoSelect(discord.ui.Select):
    """Dropdown for inspecting node details from the rendered HaloNet graph."""

    def __init__(self, options: List[discord.SelectOption], disabled: bool = False):
        super().__init__(
            placeholder="Select a HaloNet node to view details",
            min_values=1,
            max_values=1,
            options=options,
            row=1,
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, HaloNetNodeInfoView):
            await interaction.response.send_message("Node info view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use this selector.", ephemeral=True)
            return

        selected_xuid = self.values[0]
        if selected_xuid == "none":
            await interaction.response.send_message("Node details are unavailable.", ephemeral=True)
            return

        embed, partner_file = view.build_node_details(selected_xuid)
        if not embed or not partner_file:
            await interaction.response.send_message("Node details are unavailable.", ephemeral=True)
            return

        await interaction.response.send_message(embed=embed, file=partner_file, ephemeral=True)


class HaloNetNodeCountSelect(discord.ui.Select):
    """Dropdown for controlling how many nodes are included in node info options."""

    def __init__(self, options: List[discord.SelectOption], disabled: bool = False):
        super().__init__(
            placeholder="Node Info Count (5-All): included nodes",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, HaloNetNodeInfoView):
            await interaction.response.send_message("Node info view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        view.set_include_count(self.values[0])
        await interaction.response.edit_message(content=view.build_status_text(), view=view)


class HaloNetNodePageButton(discord.ui.Button):
    """Page navigation for HaloNet node-info selection."""

    def __init__(self, direction: int, disabled: bool = False):
        self.direction = -1 if direction < 0 else 1
        label = "Previous Page" if self.direction < 0 else "Next Page"
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label=label,
            row=2,
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, HaloNetNodeInfoView):
            await interaction.response.send_message("Node info view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        view.change_page(self.direction)
        await interaction.response.edit_message(content=view.build_status_text(), view=view)


class HaloNetNodeInfoView(discord.ui.View):
    """Interactive node inspector for HaloNet with count control and pagination."""

    def __init__(self, node_map: Dict[str, Dict], edges: List[Dict], requester_id: int):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.node_map = node_map
        self.edges = edges
        self.requester_id = requester_id
        self.message: Optional[discord.Message] = None

        self._aggregated_edges: List[Dict[str, object]] = []
        self._partner_edge_lookup: Dict[str, Dict[str, Dict[str, object]]] = {}
        self._ranked_nodes = self._build_ranked_nodes()
        self.include_count: Optional[int] = self._default_include_count()
        self.page_index = 0
        self._rebuild_components()

    def _build_ranked_nodes(self) -> List[Dict]:
        weighted_degree: Dict[str, float] = {
            xuid: float(data.get("weighted_degree") or 0.0)
            for xuid, data in self.node_map.items()
        }
        partner_sets: Dict[str, set] = {xuid: set() for xuid in self.node_map}

        aggregated_by_pair: Dict[tuple, Dict[str, object]] = {}
        for edge in self.edges:
            src = str(edge.get("src_xuid") or "").strip()
            dst = str(edge.get("dst_xuid") or "").strip()
            if not src or not dst or src == dst:
                continue

            key = tuple(sorted((src, dst)))
            bucket = aggregated_by_pair.setdefault(
                key,
                {
                    "src_xuid": key[0],
                    "dst_xuid": key[1],
                    "matches_together": 0,
                    "wins_together": 0,
                    "total_minutes": 0,
                    "same_team_count": 0,
                    "opposing_team_count": 0,
                },
            )
            bucket["matches_together"] += int(edge.get("matches_together") or 0)
            bucket["wins_together"] += int(edge.get("wins_together") or 0)
            bucket["total_minutes"] += int(edge.get("total_minutes") or 0)
            bucket["same_team_count"] += int(edge.get("same_team_count") or 0)
            bucket["opposing_team_count"] += int(edge.get("opposing_team_count") or 0)

        self._aggregated_edges = list(aggregated_by_pair.values())
        self._partner_edge_lookup = {xuid: {} for xuid in self.node_map}

        for edge in self._aggregated_edges:
            src = str(edge.get("src_xuid") or "").strip()
            dst = str(edge.get("dst_xuid") or "").strip()
            if not src or not dst:
                continue

            matches_together = float(edge.get("matches_together") or 0.0)
            weighted_degree[src] = weighted_degree.get(src, 0.0) + matches_together
            weighted_degree[dst] = weighted_degree.get(dst, 0.0) + matches_together

            partner_sets.setdefault(src, set()).add(dst)
            partner_sets.setdefault(dst, set()).add(src)
            self._partner_edge_lookup.setdefault(src, {})[dst] = edge
            self._partner_edge_lookup.setdefault(dst, {})[src] = edge

        for xuid, node in self.node_map.items():
            node["weighted_degree"] = float(weighted_degree.get(xuid, 0.0))
            node["coplay_partner_count"] = len(partner_sets.get(xuid, set()))

        return sorted(
            self.node_map.values(),
            key=lambda n: (
                0 if n.get("is_center") else 1,
                -float(n.get("weighted_degree") or 0.0),
                (n.get("gamertag") or n.get("xuid") or "").lower(),
            ),
        )

    def _default_include_count(self) -> int:
        if not self._ranked_nodes:
            return 0
        return min(25, len(self._ranked_nodes))

    def _get_selectable_count_values(self) -> List[int]:
        total_nodes = len(self._ranked_nodes)
        if total_nodes <= 0:
            return []
        if total_nodes < 5:
            return [total_nodes]

        values = list(range(5, total_nodes + 1, 5))
        if total_nodes <= 25 and total_nodes not in values:
            values.append(total_nodes)
        return sorted(set(values))

    def _get_included_nodes(self) -> List[Dict]:
        if self.include_count is None:
            return self._ranked_nodes

        capped = max(0, min(int(self.include_count or 0), len(self._ranked_nodes)))
        return self._ranked_nodes[:capped]

    def _get_page_count(self) -> int:
        included_nodes = self._get_included_nodes()
        if not included_nodes:
            return 1
        return max(1, math.ceil(len(included_nodes) / 25))

    def _get_page_nodes(self) -> List[Dict]:
        included_nodes = self._get_included_nodes()
        if not included_nodes:
            return []

        page_count = self._get_page_count()
        self.page_index = max(0, min(self.page_index, page_count - 1))
        start = self.page_index * 25
        return included_nodes[start:start + 25]

    def _build_count_options(self) -> List[discord.SelectOption]:
        options: List[discord.SelectOption] = []
        for count in self._get_selectable_count_values():
            options.append(
                discord.SelectOption(
                    label=f"Include top {count} nodes",
                    value=str(count),
                    description="Sorted by center, weighted degree, and name"[:100],
                    default=(self.include_count == count),
                )
            )

        if self._ranked_nodes:
            options.append(
                discord.SelectOption(
                    label="Include all nodes",
                    value="all",
                    description="Use page controls to inspect all nodes"[:100],
                    default=(self.include_count is None),
                )
            )

        if not options:
            options.append(
                discord.SelectOption(
                    label="No nodes available",
                    value="none",
                    description="Node details are unavailable"[:100],
                    default=True,
                )
            )
        return options

    def _build_node_options(self) -> List[discord.SelectOption]:
        page_nodes = self._get_page_nodes()
        if not page_nodes:
            return [
                discord.SelectOption(
                    label="No nodes available",
                    value="none",
                    description="Node details are unavailable"[:100],
                    default=True,
                )
            ]

        options = []
        for node in page_nodes:
            node_xuid = str(node.get("xuid") or "").strip()
            if not node_xuid:
                continue

            name = node.get("gamertag") or node_xuid
            role = "Center" if node.get("is_center") else "Friend"
            weighted_degree = int(round(float(node.get("weighted_degree") or 0.0)))
            partner_count = int(node.get("coplay_partner_count") or 0)
            options.append(
                discord.SelectOption(
                    label=name[:100],
                    value=node_xuid,
                    description=f"{role} | WDeg {weighted_degree} | Partners {partner_count}"[:100],
                )
            )

        return options or [
            discord.SelectOption(
                label="No nodes available",
                value="none",
                description="Node details are unavailable"[:100],
                default=True,
            )
        ]

    def _rebuild_components(self):
        self.clear_items()

        count_options = self._build_count_options()
        count_disabled = len(count_options) == 1 and count_options[0].value == "none"
        self.add_item(HaloNetNodeCountSelect(options=count_options, disabled=count_disabled))

        node_options = self._build_node_options()
        node_disabled = len(node_options) == 1 and node_options[0].value == "none"
        self.add_item(HaloNetNodeInfoSelect(options=node_options, disabled=node_disabled))

        page_count = self._get_page_count()
        self.add_item(
            HaloNetNodePageButton(
                direction=-1,
                disabled=(page_count <= 1 or self.page_index <= 0),
            )
        )
        self.add_item(
            HaloNetNodePageButton(
                direction=1,
                disabled=(page_count <= 1 or self.page_index >= page_count - 1),
            )
        )

    def set_include_count(self, value: str):
        if value == "all":
            self.include_count = None
        elif value == "none":
            self.include_count = 0
        else:
            try:
                parsed = int(value)
            except ValueError:
                parsed = self._default_include_count()

            parsed = max(1, min(parsed, len(self._ranked_nodes))) if self._ranked_nodes else 0
            self.include_count = parsed

        self.page_index = 0
        self._rebuild_components()

    def change_page(self, direction: int):
        page_count = self._get_page_count()
        if page_count <= 1:
            return

        self.page_index = max(0, min(self.page_index + direction, page_count - 1))
        self._rebuild_components()

    def build_status_text(self) -> str:
        total_nodes = len(self._ranked_nodes)
        included_nodes = self._get_included_nodes()
        setting = "All" if self.include_count is None else str(int(self.include_count or 0))
        page_label = f"{self.page_index + 1}/{self._get_page_count()}"
        return (
            "Use the selector below for HaloNet node details.\n"
            f"Included nodes: **{len(included_nodes)}** of **{total_nodes}** (setting: **{setting}**)\n"
            f"Page: **{page_label}**"
        )

    def build_node_details(self, selected_xuid: str):
        node = self.node_map.get(selected_xuid)
        if not node:
            return None, None

        name = node.get("gamertag") or selected_xuid
        role = "Center" if node.get("is_center") else "Friend"
        weighted_degree = float(node.get("weighted_degree") or 0.0)
        partner_map = self._partner_edge_lookup.get(selected_xuid, {})

        partners = []
        for partner_xuid, edge in partner_map.items():
            partner_node = self.node_map.get(partner_xuid, {})
            partners.append(
                {
                    "xuid": partner_xuid,
                    "gamertag": partner_node.get("gamertag") or partner_xuid,
                    "matches_together": int(edge.get("matches_together") or 0),
                    "wins_together": int(edge.get("wins_together") or 0),
                    "total_minutes": int(edge.get("total_minutes") or 0),
                    "same_team_count": int(edge.get("same_team_count") or 0),
                    "opposing_team_count": int(edge.get("opposing_team_count") or 0),
                }
            )

        partners.sort(key=lambda row: (-row["matches_together"], row["gamertag"].lower()))

        total_shared_matches = sum(p["matches_together"] for p in partners)
        total_wins_together = sum(p["wins_together"] for p in partners)
        total_minutes = sum(p["total_minutes"] for p in partners)
        same_team_total = sum(p["same_team_count"] for p in partners)
        opposing_team_total = sum(p["opposing_team_count"] for p in partners)

        member_xuids = {selected_xuid}
        member_xuids.update(p["xuid"] for p in partners)

        internal_links = 0
        for edge in self._aggregated_edges:
            src = edge.get("src_xuid")
            dst = edge.get("dst_xuid")
            if src in member_xuids and dst in member_xuids:
                internal_links += 1

        member_count = len(member_xuids)
        possible_links = (member_count * (member_count - 1)) // 2
        density_pct = (internal_links / possible_links * 100.0) if possible_links else 0.0

        center_node = next((n for n in self._ranked_nodes if n.get("is_center")), None)
        shared_with_center = None
        if center_node and center_node.get("xuid") and center_node.get("xuid") != selected_xuid:
            center_partners = set(self._partner_edge_lookup.get(str(center_node["xuid"]), {}).keys())
            selected_partners = set(partner_map.keys())
            shared_with_center = len(center_partners & selected_partners)

        kd = node.get("kd_ratio")
        kd_str = f"{kd:.2f}" if kd is not None else "N/A"
        win = node.get("win_rate")
        win_str = f"{win:.1f}%" if win is not None else "N/A"
        matches_played = node.get("matches_played")
        matches_str = str(matches_played) if matches_played is not None else "N/A"

        embed = discord.Embed(
            title=f"Node Details: {name}",
            colour=0x1ABC9C,
            timestamp=datetime.now(),
        )
        embed.add_field(name="XUID", value=selected_xuid, inline=False)
        embed.add_field(name="Role", value=role, inline=True)
        embed.add_field(name="Weighted Degree", value=f"{weighted_degree:.0f}", inline=True)
        embed.add_field(name="Co-play Partners", value=str(len(partners)), inline=True)

        embed.add_field(
            name="Halo Stats",
            value=(
                f"K/D: {kd_str}\n"
                f"Win Rate: {win_str}\n"
                f"Matches: {matches_str}"
            ),
            inline=False,
        )

        embed.add_field(
            name="Co-play Insights",
            value=(
                f"Total shared matches: {total_shared_matches}\n"
                f"Wins together: {total_wins_together}\n"
                f"Same-team: {same_team_total} | Opposing-team: {opposing_team_total}\n"
                f"Time together: {total_minutes} minutes\n"
                f"Internal links: {internal_links}\n"
                f"Density: {density_pct:.1f}%"
            ),
            inline=False,
        )

        if shared_with_center is not None:
            embed.add_field(
                name="Overlap With Center",
                value=f"Shared co-play partners: {shared_with_center}",
                inline=False,
            )

        preview_lines = [
            f"- {row['gamertag']}: {row['matches_together']} shared matches"
            for row in partners[:20]
        ]
        if len(partners) > 20:
            preview_lines.append(f"... and {len(partners) - 20} more")
        embed.add_field(
            name="Partners Preview",
            value="\n".join(preview_lines) if preview_lines else "No co-play partners found.",
            inline=False,
        )

        partner_lines = [
            (
                f"{idx}. {row['gamertag']} ({row['xuid']}) | "
                f"shared={row['matches_together']}, same-team={row['same_team_count']}, opposing={row['opposing_team_count']}"
            )
            for idx, row in enumerate(partners, start=1)
        ]
        partner_text = "\n".join(partner_lines) if partner_lines else "No co-play partners found."
        partner_file = discord.File(
            io.BytesIO(partner_text.encode("utf-8")),
            filename=f"node_coplay_partners_{selected_xuid}.txt",
        )

        return embed, partner_file


class HaloNodeStrengthFilterSelect(discord.ui.Select):
    """Select control for minimum node weighted-degree threshold."""

    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Node Filter (0-200): minimum weighted degree",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, HaloNetFilterView):
            await interaction.response.send_message("Filter view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        try:
            view.min_node_strength = int(self.values[0])
        except ValueError:
            view.min_node_strength = 0
        await interaction.response.defer()


class HaloEdgeWeightFilterSelect(discord.ui.Select):
    """Select control for minimum edge shared-match threshold."""

    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Edge Filter (1-200): minimum shared matches",
            min_values=1,
            max_values=1,
            options=options,
            row=1,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, HaloNetFilterView):
            await interaction.response.send_message("Filter view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        try:
            view.min_edge_weight = int(self.values[0])
        except ValueError:
            view.min_edge_weight = 1
        await interaction.response.defer()


class HaloGameTypeFilterSelect(discord.ui.Select):
    """Select control for game-type-based edge reweighting."""

    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Game Type Filter: All/Ranked/Social/Custom",
            min_values=1,
            max_values=1,
            options=options,
            row=2,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, HaloNetFilterView):
            await interaction.response.send_message("Filter view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        selected_mode = str(self.values[0] or "all").strip().lower()
        if selected_mode not in HALONET_GAME_TYPE_LABELS:
            selected_mode = "all"
        view.game_type_filter = selected_mode
        await interaction.response.defer()


class HaloNetFilterView(discord.ui.View):
    """Interactive threshold filters for HaloNet weighted-degree and edge weights."""

    def __init__(
        self,
        cog,
        requester_id: int,
        center_xuid: str,
        center_gamertag: str,
        node_map: Dict[str, Dict],
        edges: List[Dict],
        base_embed: discord.Embed,
    ):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.cog = cog
        self.requester_id = requester_id
        self.center_xuid = center_xuid
        self.center_gamertag = center_gamertag
        self.node_map = node_map
        self.edges = edges
        self.base_embed = base_embed
        self.message: Optional[discord.Message] = None

        self.min_node_strength = 0
        self.min_edge_weight = 1
        self.clustered = False
        self.game_type_filter = "all"

        node_options = []
        for v in HALONET_NODE_FILTER_THRESHOLDS:
            if v == 0:
                label = "Show all nodes"
                description = "No node-strength filtering"
            else:
                label = f"Hide nodes below {v}"
                description = f"Keep nodes with weighted degree >= {v}"
            node_options.append(
                discord.SelectOption(
                    label=label[:100],
                    value=str(v),
                    description=description[:100],
                    default=(v == 0),
                )
            )

        edge_options = []
        for v in HALONET_EDGE_FILTER_THRESHOLDS:
            if v == 1:
                label = "Show all edges"
                description = "No edge-weight filtering"
            else:
                label = f"Hide edges below {v}"
                description = f"Keep edges with shared matches >= {v}"
            edge_options.append(
                discord.SelectOption(
                    label=label[:100],
                    value=str(v),
                    description=description[:100],
                    default=(v == 1),
                )
            )

        game_type_options = [
            discord.SelectOption(
                label=label,
                value=value,
                description=description[:100],
                default=(value == "all"),
            )
            for value, label, description in HALONET_GAME_TYPE_OPTIONS
        ]

        self.add_item(HaloNodeStrengthFilterSelect(node_options))
        self.add_item(HaloEdgeWeightFilterSelect(edge_options))
        self.add_item(HaloGameTypeFilterSelect(game_type_options))

    def _build_description(self, controls_active: bool) -> str:
        """Build embed description with base text and current control state."""
        lines = []
        base_description = (self.base_embed.description or "").strip()
        if base_description:
            lines.append(base_description)

        layout_mode = "Clustered" if self.clustered else "Standard"
        game_type_label = HALONET_GAME_TYPE_LABELS.get(self.game_type_filter, "All")
        state = "ACTIVE" if controls_active else "INACTIVE"
        lines.append(f"Controls: **{state}** ({NETWORK_CONTROLS_TIMEOUT_SECONDS // 60}m timeout)")
        lines.append(f"Layout: **{layout_mode}**")
        lines.append(f"Game Type: **{game_type_label}**")
        lines.append(
            f"Filters: node weighted degree >= {self.min_node_strength}, "
            f"edge shared matches >= {self.min_edge_weight}"
        )
        if self.game_type_filter != "all":
            lines.append("Filtered modes exclude unknown match categories.")
        return "\n".join(lines)

    def _sync_select_defaults(self):
        """Keep dropdown selected values aligned with current filter state."""
        current_node = str(int(self.min_node_strength))
        current_edge = str(int(self.min_edge_weight))
        current_mode = self.game_type_filter if self.game_type_filter in HALONET_GAME_TYPE_LABELS else "all"

        for item in self.children:
            if isinstance(item, HaloNodeStrengthFilterSelect) and item.options:
                item.options = [
                    discord.SelectOption(
                        label=o.label,
                        value=o.value,
                        description=o.description,
                        default=(o.value == current_node),
                    )
                    for o in item.options
                ]
            elif isinstance(item, HaloEdgeWeightFilterSelect) and item.options:
                item.options = [
                    discord.SelectOption(
                        label=o.label,
                        value=o.value,
                        description=o.description,
                        default=(o.value == current_edge),
                    )
                    for o in item.options
                ]
            elif isinstance(item, HaloGameTypeFilterSelect) and item.options:
                item.options = [
                    discord.SelectOption(
                        label=o.label,
                        value=o.value,
                        description=o.description,
                        default=(o.value == current_mode),
                    )
                    for o in item.options
                ]

    async def _send_filtered(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        await interaction.response.defer()
        loop = asyncio.get_event_loop()
        buf = await loop.run_in_executor(
            None,
            lambda: self.cog._render_coplay_graph(
                self.center_xuid,
                self.center_gamertag,
                self.node_map,
                self.edges,
                clustered=self.clustered,
                min_node_strength=self.min_node_strength,
                min_edge_weight=self.min_edge_weight,
                game_type_filter=self.game_type_filter,
            ),
        )
        self._sync_select_defaults()

        file = discord.File(fp=buf, filename="halonet.png")
        embed = self.base_embed.copy()
        embed.description = self._build_description(controls_active=True)
        embed.set_image(url="attachment://halonet.png")
        await interaction.message.edit(embed=embed, attachments=[file], view=self)
        self.message = interaction.message

    async def on_timeout(self):
        if not self.message:
            return
        try:
            embed = self.base_embed.copy()
            embed.description = self._build_description(controls_active=False)
            embed.set_image(url="attachment://halonet.png")

            refresh_view = HaloNetRefreshView(
                requester_id=self.requester_id,
                source_view=self,
            )
            await self.message.edit(embed=embed, view=refresh_view)
            refresh_view.message = self.message
        except Exception:
            return

    @discord.ui.button(label="Apply Filters To Graph", style=discord.ButtonStyle.success, row=3)
    async def apply_filters(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_filtered(interaction)

    @discord.ui.button(label="Reset Filters", style=discord.ButtonStyle.secondary, row=3)
    async def reset_filters(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.min_node_strength = 0
        self.min_edge_weight = 1
        self.clustered = False
        self.game_type_filter = "all"
        self._sync_select_defaults()

        await self._send_filtered(interaction)

    @discord.ui.button(label="Standard Layout", style=discord.ButtonStyle.secondary, row=4)
    async def standard_layout(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.clustered = False
        await self._send_filtered(interaction)

    @discord.ui.button(label="Clustered Layout", style=discord.ButtonStyle.primary, row=4)
    async def clustered_layout(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.clustered = True
        await self._send_filtered(interaction)


class HaloNetRefreshView(discord.ui.View):
    """Minimal fallback view shown after timeout to refresh HaloNet controls in-place."""

    def __init__(self, requester_id: int, source_view: HaloNetFilterView):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.requester_id = requester_id
        self.source_view = source_view
        self.message: Optional[discord.Message] = None

    @discord.ui.button(label="Refresh Controls", style=discord.ButtonStyle.primary)
    async def refresh_controls(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the command requester can refresh controls.", ephemeral=True)
            return

        refreshed_view = HaloNetFilterView(
            cog=self.source_view.cog,
            requester_id=self.source_view.requester_id,
            center_xuid=self.source_view.center_xuid,
            center_gamertag=self.source_view.center_gamertag,
            node_map=self.source_view.node_map,
            edges=self.source_view.edges,
            base_embed=self.source_view.base_embed,
        )
        refreshed_view.min_node_strength = self.source_view.min_node_strength
        refreshed_view.min_edge_weight = self.source_view.min_edge_weight
        refreshed_view.clustered = self.source_view.clustered
        refreshed_view.game_type_filter = self.source_view.game_type_filter
        refreshed_view._sync_select_defaults()

        embed = self.source_view.base_embed.copy()
        embed.description = refreshed_view._build_description(controls_active=True)
        embed.set_image(url="attachment://halonet.png")

        await interaction.response.edit_message(embed=embed, view=refreshed_view)
        refreshed_view.message = interaction.message

    async def on_timeout(self):
        if not self.message:
            return

        try:
            for item in self.children:
                item.disabled = True
            await self.message.edit(view=self)
        except Exception:
            return
