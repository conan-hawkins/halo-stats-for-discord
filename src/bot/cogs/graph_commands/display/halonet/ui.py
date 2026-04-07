"""HaloNet view and filter controls for display commands."""

import asyncio
from typing import Dict, List, Optional

import discord


NETWORK_CONTROLS_TIMEOUT_SECONDS = 900


class HaloNodeStrengthFilterSelect(discord.ui.Select):
    """Select control for minimum node weighted-degree threshold."""

    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Node Filter (0-50): minimum weighted degree",
            min_values=1,
            max_values=1,
            options=options,
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
            placeholder="Edge Filter (1-50): minimum shared matches",
            min_values=1,
            max_values=1,
            options=options,
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

        node_thresholds = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 50]
        node_options = []
        for v in node_thresholds:
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

        edge_thresholds = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 50]
        edge_options = []
        for v in edge_thresholds:
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

        self.add_item(HaloNodeStrengthFilterSelect(node_options))
        self.add_item(HaloEdgeWeightFilterSelect(edge_options))

    def _build_description(self, controls_active: bool) -> str:
        """Build embed description with base text and current control state."""
        lines = []
        base_description = (self.base_embed.description or "").strip()
        if base_description:
            lines.append(base_description)

        layout_mode = "Clustered" if self.clustered else "Standard"
        state = "ACTIVE" if controls_active else "INACTIVE"
        lines.append(f"Controls: **{state}** ({NETWORK_CONTROLS_TIMEOUT_SECONDS // 60}m timeout)")
        lines.append(f"Layout: **{layout_mode}**")
        lines.append(
            f"Filters: node weighted degree >= {self.min_node_strength}, "
            f"edge shared matches >= {self.min_edge_weight}"
        )
        return "\n".join(lines)

    def _sync_select_defaults(self):
        """Keep dropdown selected values aligned with current filter state."""
        current_node = str(int(self.min_node_strength))
        current_edge = str(int(self.min_edge_weight))

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

    @discord.ui.button(label="Apply Filters To Graph", style=discord.ButtonStyle.success)
    async def apply_filters(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_filtered(interaction)

    @discord.ui.button(label="Reset Filters", style=discord.ButtonStyle.secondary)
    async def reset_filters(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.min_node_strength = 0
        self.min_edge_weight = 1
        self.clustered = False
        self._sync_select_defaults()

        await self._send_filtered(interaction)

    @discord.ui.button(label="Standard Layout", style=discord.ButtonStyle.secondary, row=2)
    async def standard_layout(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.clustered = False
        await self._send_filtered(interaction)

    @discord.ui.button(label="Clustered Layout", style=discord.ButtonStyle.primary, row=2)
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
