"""Network-specific Discord view and control components.

These classes are shared by both the standalone Network cog and the Graph
legacy compatibility command.
"""

import asyncio
import io
from datetime import datetime
from typing import Dict, List, Optional

import discord


NETWORK_CONTROLS_TIMEOUT_SECONDS = 900


class NetworkNodeInfoSelect(discord.ui.Select):
    """Dropdown for inspecting node details from the rendered network."""

    def __init__(self, node_map: Dict[str, Dict], requester_id: int, db):
        sorted_nodes = sorted(
            node_map.values(),
            key=lambda n: (0 if n.get("is_center") else 1, -(n.get("group_size") or 0), n.get("gamertag") or ""),
        )

        options = []
        for node in sorted_nodes[:25]:
            name = node.get("gamertag") or node.get("xuid", "Unknown")
            group_size = node.get("group_size") or 0
            role = "Center" if node.get("is_center") else "Friend"
            options.append(
                discord.SelectOption(
                    label=name[:100],
                    value=node.get("xuid", ""),
                    description=f"{role} | Group {group_size}"[:100],
                )
            )

        super().__init__(
            placeholder="Select a node to view details",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.node_map = node_map
        self.requester_id = requester_id
        self.db = db

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the command requester can use this selector.", ephemeral=True)
            return

        selected_xuid = self.values[0]
        node = self.node_map.get(selected_xuid)
        if not node:
            await interaction.response.send_message("Node details are unavailable.", ephemeral=True)
            return

        name = node.get("gamertag") or node.get("xuid", "Unknown")
        group_size = node.get("group_size") or 0
        kd = node.get("kd_ratio")
        kd_str = f"{kd:.2f}" if kd is not None else "N/A"
        win = node.get("win_rate")
        win_str = f"{win:.1f}%" if win is not None else "N/A"
        matches = node.get("matches_played")
        matches_str = str(matches) if matches is not None else "N/A"
        mutual = node.get("is_mutual")
        mutual_str = "Yes" if mutual else "No"
        group_source = node.get("group_size_source") or "direct"
        inferred_group_size = bool(node.get("group_size_inferred"))

        embed = discord.Embed(
            title=f"Node Details: {name}",
            colour=0x2ECC71,
            timestamp=datetime.now(),
        )

        embed.add_field(name="XUID", value=node.get("xuid", "Unknown"), inline=False)
        embed.add_field(name="Role", value="Center" if node.get("is_center") else "Friend", inline=True)
        embed.add_field(name="Halo Social Group Size", value=str(group_size), inline=True)
        embed.add_field(name="Group Size Source", value=group_source, inline=True)

        if not node.get("is_center"):
            embed.add_field(name="Mutual Friend", value=mutual_str, inline=True)

        embed.add_field(
            name="Halo Stats",
            value=(
                f"K/D: {kd_str}\n"
                f"Win Rate: {win_str}\n"
                f"Matches: {matches_str}"
            ),
            inline=False,
        )

        # Node members are this player's verified Halo-active friends (plus the selected player).
        halo_friends = self.db.get_halo_friends(selected_xuid)
        verified_members = [f for f in halo_friends if (f.get("matches_played") or 0) > 0]
        members_inferred = False

        if not verified_members:
            # Fallback: infer visible social-group members from reciprocal evidence
            # (players that list this user as a verified Halo-active friend).
            incoming_verified = self.db.get_verified_halo_incoming_friends(selected_xuid)
            if incoming_verified:
                verified_members = [
                    {
                        "dst_xuid": r.get("src_xuid"),
                        "gamertag": r.get("gamertag"),
                        "matches_played": r.get("matches_played"),
                    }
                    for r in incoming_verified
                ]
                members_inferred = True

        members = [{
            "xuid": selected_xuid,
            "gamertag": name,
        }]
        for m in verified_members:
            members.append({
                "xuid": m.get("dst_xuid") or "",
                "gamertag": m.get("gamertag") or (m.get("dst_xuid") or "Unknown"),
            })

        dedup = {}
        for m in members:
            mx = m.get("xuid")
            if mx and mx not in dedup:
                dedup[mx] = m
        members = sorted(dedup.values(), key=lambda m: (m.get("gamertag") or "").lower())

        member_xuids = [m["xuid"] for m in members if m.get("xuid")]
        edges = self.db.get_edges_within_set(member_xuids) if len(member_xuids) >= 2 else []
        unique_edges = {
            tuple(sorted((e["src_xuid"], e["dst_xuid"])))
            for e in edges
            if e.get("src_xuid") and e.get("dst_xuid") and e["src_xuid"] != e["dst_xuid"]
        }

        n = len(member_xuids)
        possible_edges = (n * (n - 1)) // 2
        density_pct = (len(unique_edges) / possible_edges * 100.0) if possible_edges else 0.0

        center_node = next((v for v in self.node_map.values() if v.get("is_center")), None)
        shared_with_center = None
        if center_node and center_node.get("xuid") and center_node.get("xuid") != selected_xuid:
            center_friends = self.db.get_halo_friends(center_node["xuid"])
            center_verified = {
                f.get("dst_xuid")
                for f in center_friends
                if (f.get("matches_played") or 0) > 0 and f.get("dst_xuid")
            }
            selected_verified = set(member_xuids)
            selected_verified.discard(selected_xuid)
            shared_with_center = len(selected_verified & center_verified)

        embed.add_field(
            name="Node Insights",
            value=(
                f"Members (verified Halo-active): {len(member_xuids)}\n"
                f"Internal links: {len(unique_edges)}\n"
                f"Density: {density_pct:.1f}%"
            ),
            inline=False,
        )

        if shared_with_center is not None:
            embed.add_field(
                name="Overlap With Center",
                value=f"Shared verified Halo-active members: {shared_with_center}",
                inline=False,
            )

        preview = "\n".join(
            f"- {m['gamertag']}" for m in members[:20]
        ) or "No members"
        if len(members) > 20:
            preview += f"\n... and {len(members) - 20} more"
        embed.add_field(name="Members Preview", value=preview, inline=False)

        if members_inferred or inferred_group_size:
            embed.add_field(
                name="Inference Applied",
                value=(
                    "Yes - direct friend list data appears private/empty, so members were populated "
                    "from reciprocal visibility in other verified Halo-active nodes."
                ),
                inline=False,
            )

        member_lines = [
            f"{idx}. {m['gamertag']} ({m['xuid']})"
            for idx, m in enumerate(members, 1)
        ]
        member_text = "\n".join(member_lines) if member_lines else "No members"
        member_file = discord.File(
            io.BytesIO(member_text.encode("utf-8")),
            filename=f"node_members_{selected_xuid}.txt",
        )

        await interaction.response.send_message(embed=embed, file=member_file, ephemeral=True)


class NetworkNodeInfoView(discord.ui.View):
    """View container for the node info dropdown."""

    def __init__(self, node_map: Dict[str, Dict], requester_id: int, db):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.add_item(NetworkNodeInfoSelect(node_map=node_map, requester_id=requester_id, db=db))


class NetworkLayoutToggleView(discord.ui.View):
    """Buttons to render standard vs clustered network layouts from the same command."""

    def __init__(
        self,
        cog,
        requester_id: int,
        center_xuid: str,
        center_gamertag: str,
        halo_friends: List[Dict],
        center_features: Optional[Dict],
    ):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.cog = cog
        self.requester_id = requester_id
        self.center_xuid = center_xuid
        self.center_gamertag = center_gamertag
        self.halo_friends = halo_friends
        self.center_features = center_features

    async def _render_and_send(self, interaction: discord.Interaction, clustered: bool):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the command requester can use these layout controls.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        loop = asyncio.get_event_loop()
        buf = await loop.run_in_executor(
            None,
            lambda: self.cog._render_network_graph(
                self.center_xuid,
                self.center_gamertag,
                self.halo_friends,
                self.center_features,
                clustered=clustered,
            ),
        )
        file = discord.File(fp=buf, filename="network_layout.png")
        mode = "Clustered" if clustered else "Standard"
        embed = discord.Embed(
            title=f"Network Layout: {mode}",
            description="Same network data, alternate arrangement to inspect structure.",
            colour=0x3498DB,
            timestamp=datetime.now(),
        )
        embed.set_image(url="attachment://network_layout.png")
        await interaction.followup.send(embed=embed, file=file, ephemeral=True)

    @discord.ui.button(label="Standard Layout", style=discord.ButtonStyle.secondary)
    async def show_standard(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._render_and_send(interaction, clustered=False)

    @discord.ui.button(label="Clustered Layout", style=discord.ButtonStyle.primary)
    async def show_clustered(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._render_and_send(interaction, clustered=True)


class NodeSizeFilterSelect(discord.ui.Select):
    """Select control for minimum node group-size threshold."""

    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Node Filter (0-50): minimum social group size",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, NetworkFilterView):
            await interaction.response.send_message("Filter view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        try:
            view.min_group_size = int(self.values[0])
        except ValueError:
            view.min_group_size = 0
        await interaction.response.defer()


class EdgeStrengthFilterSelect(discord.ui.Select):
    """Select control for minimum edge-strength threshold."""

    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Edge Filter (1-50): minimum link strength",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, NetworkFilterView):
            await interaction.response.send_message("Filter view is unavailable.", ephemeral=True)
            return
        if interaction.user.id != view.requester_id:
            await interaction.response.send_message("Only the command requester can use these controls.", ephemeral=True)
            return

        try:
            view.min_link_strength = float(self.values[0])
        except ValueError:
            view.min_link_strength = 1.0
        await interaction.response.defer()


class NetworkFilterView(discord.ui.View):
    """Interactive threshold filters for node size and edge strength."""

    def __init__(
        self,
        cog,
        requester_id: int,
        center_xuid: str,
        center_gamertag: str,
        halo_friends: List[Dict],
        center_features: Optional[Dict],
        base_embed: discord.Embed,
    ):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.cog = cog
        self.requester_id = requester_id
        self.center_xuid = center_xuid
        self.center_gamertag = center_gamertag
        self.halo_friends = halo_friends
        self.center_features = center_features
        self.base_embed = base_embed
        self.message: Optional[discord.Message] = None

        self.min_group_size = 0
        self.min_link_strength = 1.0
        self.clustered = False

        # Keep option counts <= 25 (Discord select limit) while allowing thresholds up to 50.
        node_thresholds = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 50]
        node_options = []
        for v in node_thresholds:
            if v == 0:
                label = "Show all nodes"
                description = "No node-size filtering"
            else:
                label = f"Hide nodes below {v}"
                description = f"Keep nodes with group size >= {v}"
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
                description = "No edge-strength filtering"
            else:
                label = f"Hide edges below {v}"
                description = f"Keep links with strength >= {v}"
            edge_options.append(
                discord.SelectOption(
                    label=label[:100],
                    value=str(v),
                    description=description[:100],
                    default=(v == 1),
                )
            )

        self.add_item(NodeSizeFilterSelect(node_options))
        self.add_item(EdgeStrengthFilterSelect(edge_options))

    def _build_description(self, controls_active: bool) -> str:
        """Build a consistent controls status line for the graph embed."""
        layout_mode = "Clustered" if self.clustered else "Standard"
        state = "ACTIVE" if controls_active else "INACTIVE"
        return (
            f"Controls: **{state}** ({NETWORK_CONTROLS_TIMEOUT_SECONDS // 60}m timeout)\\n"
            f"Layout: **{layout_mode}**\\n"
            f"Filters: node group size >= {self.min_group_size}, "
            f"edge strength >= {self.min_link_strength:.0f}"
        )

    def _sync_select_defaults(self):
        """Keep dropdown selected values aligned with current filter state."""
        current_group = str(int(self.min_group_size))
        current_edge = str(int(self.min_link_strength))

        for item in self.children:
            if isinstance(item, NodeSizeFilterSelect) and item.options:
                item.options = [
                    discord.SelectOption(
                        label=o.label,
                        value=o.value,
                        description=o.description,
                        default=(o.value == current_group),
                    )
                    for o in item.options
                ]
            elif isinstance(item, EdgeStrengthFilterSelect) and item.options:
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
            lambda: self.cog._render_network_graph(
                self.center_xuid,
                self.center_gamertag,
                self.halo_friends,
                self.center_features,
                clustered=self.clustered,
                min_group_size=self.min_group_size,
                min_link_strength=self.min_link_strength,
            ),
        )
        self._sync_select_defaults()

        file = discord.File(fp=buf, filename="network.png")
        embed = self.base_embed.copy()
        embed.description = self._build_description(controls_active=True)
        embed.set_image(url="attachment://network.png")
        await interaction.message.edit(embed=embed, attachments=[file], view=self)
        self.message = interaction.message

    async def on_timeout(self):
        if not self.message:
            return
        try:
            embed = self.base_embed.copy()
            embed.description = self._build_description(controls_active=False)
            embed.set_image(url="attachment://network.png")

            refresh_view = NetworkRefreshView(
                requester_id=self.requester_id,
                source_view=self,
            )
            await self.message.edit(embed=embed, view=refresh_view)
            refresh_view.message = self.message
        except Exception:
            # Best-effort timeout cleanup; avoid raising from discord view timeout tasks.
            return

    @discord.ui.button(label="Apply Filters To Graph", style=discord.ButtonStyle.success)
    async def apply_filters(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_filtered(interaction)

    @discord.ui.button(label="Reset Filters", style=discord.ButtonStyle.secondary)
    async def reset_filters(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.min_group_size = 0
        self.min_link_strength = 1.0
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


class NetworkRefreshView(discord.ui.View):
    """Minimal fallback view shown after timeout to refresh controls in-place."""

    def __init__(self, requester_id: int, source_view: NetworkFilterView):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.requester_id = requester_id
        self.source_view = source_view
        self.message: Optional[discord.Message] = None

    @discord.ui.button(label="Refresh Controls", style=discord.ButtonStyle.primary)
    async def refresh_controls(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the command requester can refresh controls.", ephemeral=True)
            return

        refreshed_view = NetworkFilterView(
            cog=self.source_view.cog,
            requester_id=self.source_view.requester_id,
            center_xuid=self.source_view.center_xuid,
            center_gamertag=self.source_view.center_gamertag,
            halo_friends=self.source_view.halo_friends,
            center_features=self.source_view.center_features,
            base_embed=self.source_view.base_embed,
        )
        refreshed_view.min_group_size = self.source_view.min_group_size
        refreshed_view.min_link_strength = self.source_view.min_link_strength
        refreshed_view.clustered = self.source_view.clustered
        refreshed_view._sync_select_defaults()

        embed = self.source_view.base_embed.copy()
        embed.description = refreshed_view._build_description(controls_active=True)
        embed.set_image(url="attachment://network.png")

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
            # Best-effort timeout cleanup; avoid raising from discord view timeout tasks.
            return
