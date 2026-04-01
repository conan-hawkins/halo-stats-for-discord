"""
Graph Commands Cog for Halo Stats Discord Bot

Contains commands for social graph analysis:
- #graphstats - Show current graph statistics
- #similar - Find similar players (KNN)
- #hubs - Find hub players with many connections
- #network - Show a player's Halo-active friend network
- #halonet - Show a player's co-play network graph
- #crawlfriends - Start background crawl from a seed player
- #crawlgames - Build co-play edges from shared match history
"""

import asyncio
import csv
import io
from collections import defaultdict
from datetime import datetime
from itertools import combinations
from typing import Awaitable, Callable, Dict, List, Optional

import discord
from discord.ext import commands

from src.database.graph_schema import get_graph_db
from src.api import api_client


NETWORK_CONTROLS_TIMEOUT_SECONDS = 900


class NetworkNodeInfoSelect(discord.ui.Select):
    """Dropdown for inspecting node details from the rendered network."""

    def __init__(self, node_map: Dict[str, Dict], requester_id: int, db):
        sorted_nodes = sorted(
            node_map.values(),
            key=lambda n: (0 if n.get('is_center') else 1, -(n.get('group_size') or 0), n.get('gamertag') or ''),
        )

        options = []
        for node in sorted_nodes[:25]:
            name = node.get('gamertag') or node.get('xuid', 'Unknown')
            group_size = node.get('group_size') or 0
            role = 'Center' if node.get('is_center') else 'Friend'
            options.append(
                discord.SelectOption(
                    label=name[:100],
                    value=node.get('xuid', ''),
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

        name = node.get('gamertag') or node.get('xuid', 'Unknown')
        group_size = node.get('group_size') or 0
        kd = node.get('kd_ratio')
        kd_str = f"{kd:.2f}" if kd is not None else "N/A"
        win = node.get('win_rate')
        win_str = f"{win:.1f}%" if win is not None else "N/A"
        matches = node.get('matches_played')
        matches_str = str(matches) if matches is not None else "N/A"
        mutual = node.get('is_mutual')
        mutual_str = "Yes" if mutual else "No"
        group_source = node.get('group_size_source') or 'direct'
        inferred_group_size = bool(node.get('group_size_inferred'))

        embed = discord.Embed(
            title=f"Node Details: {name}",
            colour=0x2ECC71,
            timestamp=datetime.now(),
        )

        embed.add_field(name="XUID", value=node.get('xuid', 'Unknown'), inline=False)
        embed.add_field(name="Role", value="Center" if node.get('is_center') else "Friend", inline=True)
        embed.add_field(name="Halo Social Group Size", value=str(group_size), inline=True)
        embed.add_field(name="Group Size Source", value=group_source, inline=True)

        if not node.get('is_center'):
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
        verified_members = [f for f in halo_friends if (f.get('matches_played') or 0) > 0]
        members_inferred = False

        if not verified_members:
            # Fallback: infer visible social-group members from reciprocal evidence
            # (players that list this user as a verified Halo-active friend).
            incoming_verified = self.db.get_verified_halo_incoming_friends(selected_xuid)
            if incoming_verified:
                verified_members = [
                    {
                        'dst_xuid': r.get('src_xuid'),
                        'gamertag': r.get('gamertag'),
                        'matches_played': r.get('matches_played'),
                    }
                    for r in incoming_verified
                ]
                members_inferred = True

        members = [{
            'xuid': selected_xuid,
            'gamertag': name,
        }]
        for m in verified_members:
            members.append({
                'xuid': m.get('dst_xuid') or '',
                'gamertag': m.get('gamertag') or (m.get('dst_xuid') or 'Unknown'),
            })

        dedup = {}
        for m in members:
            mx = m.get('xuid')
            if mx and mx not in dedup:
                dedup[mx] = m
        members = sorted(dedup.values(), key=lambda m: (m.get('gamertag') or '').lower())

        member_xuids = [m['xuid'] for m in members if m.get('xuid')]
        edges = self.db.get_edges_within_set(member_xuids) if len(member_xuids) >= 2 else []
        unique_edges = {
            tuple(sorted((e['src_xuid'], e['dst_xuid'])))
            for e in edges
            if e.get('src_xuid') and e.get('dst_xuid') and e['src_xuid'] != e['dst_xuid']
        }

        n = len(member_xuids)
        possible_edges = (n * (n - 1)) // 2
        density_pct = (len(unique_edges) / possible_edges * 100.0) if possible_edges else 0.0

        center_node = next((v for v in self.node_map.values() if v.get('is_center')), None)
        shared_with_center = None
        if center_node and center_node.get('xuid') and center_node.get('xuid') != selected_xuid:
            center_friends = self.db.get_halo_friends(center_node['xuid'])
            center_verified = {
                f.get('dst_xuid')
                for f in center_friends
                if (f.get('matches_played') or 0) > 0 and f.get('dst_xuid')
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
            io.BytesIO(member_text.encode('utf-8')),
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


class GraphCog(commands.Cog, name="Graph"):
    """Commands for social graph analysis and network discovery"""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_graph_db()
        self._crawl_task: Optional[asyncio.Task] = None
    
    @commands.command(name='graphstats', help='Show current social graph database totals, depth distribution, and size.')
    async def graph_stats(self, ctx: commands.Context):
        """Display statistics about the social graph database"""
        stats = self.db.get_graph_stats()
        
        embed = discord.Embed(
            title="Social Graph Statistics",
            description="Current state of the Halo Infinite social network",
            colour=0x00BFFF,
            timestamp=datetime.now()
        )
        
        # Node counts
        embed.add_field(
            name="Players",
            value=f"Total: **{stats['total_players']:,}**\nHalo Active: **{stats['halo_active_players']:,}**",
            inline=True
        )
        
        # Edge counts
        embed.add_field(
            name="Connections",
            value=f"Friend edges: **{stats['total_friend_edges']:,}**\nCo-play edges: **{stats['total_coplay_edges']:,}**",
            inline=True
        )
        
        # Stats coverage
        embed.add_field(
            name="Feature Store",
            value=f"With stats: **{stats['players_with_stats']:,}**",
            inline=True
        )
        
        # Graph structure
        embed.add_field(
            name="Graph Metrics",
            value=f"Avg degree: **{stats['avg_friend_degree']:.1f}**\nHalo degree: **{stats['avg_halo_friend_degree']:.1f}**",
            inline=True
        )
        
        # Depth distribution
        depth_dist = stats.get('depth_distribution', {})
        if depth_dist:
            depth_str = "\n".join([f"Depth {d}: {c:,}" for d, c in sorted(depth_dist.items())[:5]])
            embed.add_field(
                name="Depth Distribution",
                value=depth_str,
                inline=True
            )
        
        # Database size
        embed.add_field(
            name="Database Size",
            value=f"**{stats.get('db_size_mb', 0):.2f}** MB",
            inline=True
        )
        
        await ctx.send(embed=embed)
    
    @commands.command(name='similar', help='Find players with similar Halo performance profiles. Usage: #similar <gamertag>')
    async def find_similar(self, ctx: commands.Context, *inputs):
        """Find players with similar stats using KNN"""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#similar GAMERTAG`")
            return
        
        gamertag = ' '.join(inputs)
        
        # Loading message
        loading_embed = discord.Embed(
            title="Finding Similar Players...",
            description=f"Searching for players similar to **{gamertag}**",
            colour=0xFFA500,
            timestamp=datetime.now()
        )
        loading_msg = await ctx.send(embed=loading_embed)
        
        try:
            # Resolve gamertag to XUID
            xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await loading_msg.edit(embed=discord.Embed(
                    title="Player Not Found",
                    description=f"Could not find player **{gamertag}**",
                    colour=0xFF0000
                ))
                return
            
            # Check if player is in graph
            player = self.db.get_player(xuid)
            features = self.db.get_halo_features(xuid)
            
            if not features or features.get('matches_played', 0) == 0:
                await loading_msg.edit(embed=discord.Embed(
                    title="No Stats Available",
                    description=f"**{gamertag}** is not in the graph database.\nRun a crawl that includes this player first.",
                    colour=0xFF0000
                ))
                return
            
            # Find similar players
            similar = self.db.get_similar_players_knn(xuid, k=5)
            
            if not similar:
                await loading_msg.edit(embed=discord.Embed(
                    title="No Similar Players Found",
                    description="Not enough players in database to find matches.",
                    colour=0xFF0000
                ))
                return
            
            # Build response
            embed = discord.Embed(
                title=f"Players Similar to {gamertag}",
                description=f"Based on CSR, K/D, and Win Rate",
                colour=0x00FF00,
                timestamp=datetime.now()
            )
            
            # Target player stats
            embed.add_field(
                name=f"Target: {gamertag}",
                value=f"CSR: {features.get('csr', 0):.0f} | K/D: {features.get('kd_ratio', 0):.2f} | Win: {features.get('win_rate', 0):.1f}%",
                inline=False
            )
            
            # Similar players
            for i, p in enumerate(similar, 1):
                gt = p.get('gamertag', p.get('xuid', 'Unknown'))
                embed.add_field(
                    name=f"{i}. {gt}",
                    value=f"CSR: {p.get('csr') or 0:.0f} | K/D: {p.get('kd_ratio') or 0:.2f} | Win: {p.get('win_rate') or 0:.1f}%\nMatches: {p.get('matches_played') or 0}",
                    inline=True
                )
            
            await loading_msg.edit(embed=embed)
            
        except Exception as e:
            await loading_msg.edit(embed=discord.Embed(
                title="Error",
                description=f"Error finding similar players: {str(e)}",
                colour=0xFF0000
            ))
    
    @commands.command(name='hubs', help='Find highly connected hub players. Usage: #hubs [min_friends]')
    async def find_hubs(self, ctx: commands.Context, min_friends: int = 30):
        """Find players with the most Halo-active connections"""
        
        try:
            hubs = self.db.find_hubs(min_degree=min_friends, halo_only=True)
            
            if not hubs:
                await ctx.send(f"No hubs found with {min_friends}+ Halo friends in the database.")
                return
            
            embed = discord.Embed(
                title=f"Hub Players ({min_friends}+ Halo Friends)",
                description="Players with the most Halo-active connections",
                colour=0x9B59B6,
                timestamp=datetime.now()
            )
            
            for i, hub in enumerate(hubs[:10], 1):
                gt = hub.get('gamertag', hub.get('xuid', 'Unknown'))
                csr = hub.get('csr') or 0
                kd = hub.get('kd_ratio') or 0
                
                embed.add_field(
                    name=f"{i}. {gt}",
                    value=f"Friends: **{hub['friend_count']}**\nCSR: {csr:.0f} | K/D: {kd:.2f}",
                    inline=True
                )
            
            embed.set_footer(text=f"Showing top {min(10, len(hubs))} of {len(hubs)} hubs")
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"Error finding hubs: {str(e)}")

    @commands.command(name='halonet', help='Show a co-play network visualization from graph DB data. Usage: #halonet <gamertag>')
    async def show_halonet(self, ctx: commands.Context, *inputs):
        """Show a player's local co-play network from stored graph_coplay data."""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#halonet GAMERTAG`")
            return

        gamertag = ' '.join(inputs)
        min_matches = 2
        max_nodes = 60

        loading_embed = discord.Embed(
            title="Building Co-play Network...",
            description=f"Fetching co-play edges for **{gamertag}**",
            colour=0xFFA500,
            timestamp=datetime.now(),
        )
        loading_msg = await ctx.send(embed=loading_embed)

        try:
            xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await loading_msg.delete()
                await ctx.send(f"Could not find player **{gamertag}**")
                return

            player = self.db.get_player(xuid)
            if not player:
                await loading_msg.delete()
                await ctx.send(
                    f"**{gamertag}** is not in the graph database. Run `#crawlfriends` and `#crawlgames` first."
                )
                return

            center_features = self.db.get_halo_features(xuid)
            active_min_matches = min_matches
            threshold_relaxed = False

            neighbors = self.db.get_coplay_neighbors(xuid, min_matches=active_min_matches, limit=max_nodes - 1)
            if not neighbors:
                relaxed_neighbors = self.db.get_coplay_neighbors(xuid, min_matches=1, limit=max_nodes - 1)
                if relaxed_neighbors:
                    neighbors = relaxed_neighbors
                    active_min_matches = 1
                    threshold_relaxed = True
                else:
                    await loading_msg.delete()
                    await ctx.send(
                        f"No co-play edges found for **{gamertag}**. "
                        f"If this player has stats but still shows zero edges, run `#crawlfriends {gamertag}` "
                        "to expand graph scope and then rerun `#crawlgames`."
                    )
                    return

            node_map: Dict[str, Dict] = {
                xuid: {
                    'xuid': xuid,
                    'gamertag': gamertag,
                    'is_center': True,
                    'kd_ratio': center_features.get('kd_ratio') if center_features else None,
                    'win_rate': center_features.get('win_rate') if center_features else None,
                    'matches_played': center_features.get('matches_played') if center_features else None,
                }
            }

            for row in neighbors:
                partner_xuid = row.get('partner_xuid')
                if not partner_xuid:
                    continue
                node_map[partner_xuid] = {
                    'xuid': partner_xuid,
                    'gamertag': row.get('gamertag') or partner_xuid,
                    'is_center': False,
                    'kd_ratio': row.get('kd_ratio'),
                    'win_rate': row.get('win_rate'),
                    'matches_played': row.get('matches_played'),
                }

            all_xuids = list(node_map.keys())
            raw_edges = self.db.get_coplay_edges_within_set(all_xuids, min_matches=active_min_matches)

            aggregated_edges: Dict[tuple, Dict[str, object]] = {}
            for edge in raw_edges:
                src = edge.get('src_xuid')
                dst = edge.get('dst_xuid')
                if not src or not dst or src == dst:
                    continue
                key = tuple(sorted((src, dst)))
                bucket = aggregated_edges.setdefault(
                    key,
                    {
                        'src_xuid': key[0],
                        'dst_xuid': key[1],
                        'matches_together': 0,
                        'wins_together': 0,
                        'total_minutes': 0,
                    },
                )
                bucket['matches_together'] += int(edge.get('matches_together') or 0)
                bucket['wins_together'] += int(edge.get('wins_together') or 0)
                bucket['total_minutes'] += int(edge.get('total_minutes') or 0)

            if not aggregated_edges:
                for row in neighbors:
                    partner_xuid = row.get('partner_xuid')
                    if not partner_xuid:
                        continue
                    key = tuple(sorted((xuid, partner_xuid)))
                    aggregated_edges[key] = {
                        'src_xuid': key[0],
                        'dst_xuid': key[1],
                        'matches_together': int(row.get('matches_together') or 0),
                        'wins_together': int(row.get('wins_together') or 0),
                        'total_minutes': int(row.get('total_minutes') or 0),
                    }

            edges = [
                edge
                for edge in aggregated_edges.values()
                if int(edge.get('matches_together') or 0) >= active_min_matches
            ]

            if not edges:
                await loading_msg.delete()
                await ctx.send(
                    f"Co-play data exists for **{gamertag}**, but no edges met the current minimum shared-match threshold ({active_min_matches})."
                )
                return

            total_shared_matches = sum(int(edge.get('matches_together') or 0) for edge in edges)
            top_partners = sorted(
                neighbors,
                key=lambda row: int(row.get('matches_together') or 0),
                reverse=True,
            )[:5]

            embed = discord.Embed(
                title=f"HaloNet: {gamertag}",
                description=(
                    "Co-play links weighted by shared matches from graph DB data."
                    + (" Showing fallback edges with at least 1 shared match." if threshold_relaxed else "")
                ),
                colour=0x1ABC9C,
                timestamp=datetime.now(),
            )
            embed.add_field(
                name="Summary",
                value=(
                    f"Nodes: **{len(node_map)}**\n"
                    f"Edges: **{len(edges)}**\n"
                    f"Total shared matches: **{total_shared_matches:,}**\n"
                    f"Min edge weight: **{active_min_matches}**"
                ),
                inline=True,
            )

            if center_features and (center_features.get('matches_played') or 0) > 0:
                kd_val = center_features.get('kd_ratio')
                wr_val = center_features.get('win_rate')
                kd_str = f"{kd_val:.2f}" if kd_val is not None else "N/A"
                wr_str = f"{wr_val:.1f}%" if wr_val is not None else "N/A"
                embed.add_field(
                    name="Player Stats",
                    value=(
                        f"K/D: {kd_str}\n"
                        f"Win Rate: {wr_str}\n"
                        f"Matches: {center_features.get('matches_played') or 0}"
                    ),
                    inline=True,
                )

            if top_partners:
                lines = []
                for row in top_partners:
                    partner_name = row.get('gamertag') or row.get('partner_xuid', 'Unknown')
                    matches_together = int(row.get('matches_together') or 0)
                    same_team = int(row.get('same_team_count') or 0)
                    lines.append(f"**{partner_name}**: {matches_together} shared matches ({same_team} same-team)")
                embed.add_field(name="Top Co-play Partners", value="\n".join(lines), inline=False)

            await loading_msg.delete()

            loop = asyncio.get_event_loop()
            buf = await loop.run_in_executor(
                None,
                lambda: self._render_coplay_graph(xuid, gamertag, node_map, edges),
            )

            file = discord.File(fp=buf, filename="halonet.png")
            embed.set_image(url="attachment://halonet.png")
            embed.set_footer(text=f"XUID: {xuid} | Gold=center | Edge width/color=shared matches | Node size=weighted degree")
            await ctx.send(embed=embed, file=file)

        except Exception as e:
            try:
                await loading_msg.delete()
            except Exception:
                pass
            await ctx.send(f"Error showing halonet: {str(e)}")
            raise

    @commands.command(name='halogroups', help='Show co-play communities and overlap matrix. Usage: #halogroups <gamertag>')
    async def show_halogroups(self, ctx: commands.Context, *inputs):
        """Show detected co-play communities around a player and export overlap CSVs."""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#halogroups GAMERTAG`")
            return

        gamertag = ' '.join(inputs)
        min_matches = 2
        max_nodes = 60

        loading_embed = discord.Embed(
            title="Building Co-play Communities...",
            description=f"Analyzing co-play groups for **{gamertag}**",
            colour=0xFFA500,
            timestamp=datetime.now(),
        )
        loading_msg = await ctx.send(embed=loading_embed)

        try:
            xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await loading_msg.delete()
                await ctx.send(f"Could not find player **{gamertag}**")
                return

            player = self.db.get_player(xuid)
            if not player:
                await loading_msg.delete()
                await ctx.send(
                    f"**{gamertag}** is not in the graph database. Run `#crawlfriends` and `#crawlgames` first."
                )
                return

            active_min_matches = min_matches
            threshold_relaxed = False

            neighbors = self.db.get_coplay_neighbors(xuid, min_matches=active_min_matches, limit=max_nodes - 1)
            if not neighbors:
                relaxed_neighbors = self.db.get_coplay_neighbors(xuid, min_matches=1, limit=max_nodes - 1)
                if relaxed_neighbors:
                    neighbors = relaxed_neighbors
                    active_min_matches = 1
                    threshold_relaxed = True
                else:
                    await loading_msg.delete()
                    await ctx.send(
                        f"No co-play edges found for **{gamertag}**. Run `#crawlgames` and try again."
                    )
                    return

            node_map: Dict[str, Dict] = {
                xuid: {
                    'xuid': xuid,
                    'gamertag': gamertag,
                    'is_center': True,
                }
            }

            for row in neighbors:
                partner_xuid = row.get('partner_xuid')
                if not partner_xuid:
                    continue
                node_map[partner_xuid] = {
                    'xuid': partner_xuid,
                    'gamertag': row.get('gamertag') or partner_xuid,
                    'is_center': False,
                }

            all_xuids = list(node_map.keys())
            raw_edges = self.db.get_coplay_edges_within_set(all_xuids, min_matches=active_min_matches)

            # Collapse directional rows into one undirected edge with summed weights.
            aggregated_edges: Dict[tuple, Dict[str, object]] = {}
            for edge in raw_edges:
                src = edge.get('src_xuid')
                dst = edge.get('dst_xuid')
                if not src or not dst or src == dst:
                    continue
                key = tuple(sorted((src, dst)))
                bucket = aggregated_edges.setdefault(
                    key,
                    {
                        'src_xuid': key[0],
                        'dst_xuid': key[1],
                        'matches_together': 0,
                    },
                )
                bucket['matches_together'] += int(edge.get('matches_together') or 0)

            edges = [
                edge
                for edge in aggregated_edges.values()
                if int(edge.get('matches_together') or 0) >= active_min_matches
            ]

            if not edges:
                await loading_msg.delete()
                await ctx.send(
                    f"Co-play data exists for **{gamertag}**, but no edges met the current threshold ({active_min_matches})."
                )
                return

            import networkx as nx

            G = nx.Graph()
            for xuid_key, node in node_map.items():
                G.add_node(xuid_key, gamertag=node.get('gamertag') or xuid_key)

            for edge in edges:
                src = edge.get('src_xuid')
                dst = edge.get('dst_xuid')
                weight = int(edge.get('matches_together') or 0)
                if not src or not dst or src == dst or weight <= 0:
                    continue
                if G.has_edge(src, dst):
                    G[src][dst]['weight'] += weight
                else:
                    G.add_edge(src, dst, weight=weight)

            if G.number_of_nodes() < 2 or G.number_of_edges() < 1:
                await loading_msg.delete()
                await ctx.send(f"Not enough connected co-play data to compute groups for **{gamertag}**.")
                return

            communities = list(nx.algorithms.community.greedy_modularity_communities(G, weight='weight'))
            communities = sorted((set(c) for c in communities), key=lambda c: (-len(c), sorted(c)[0]))

            community_of: Dict[str, int] = {}
            for idx, members in enumerate(communities, start=1):
                for member in members:
                    community_of[member] = idx

            group_count = len(communities)
            matrix_weights = [[0 for _ in range(group_count)] for _ in range(group_count)]
            matrix_edges = [[0 for _ in range(group_count)] for _ in range(group_count)]

            for u, v, data in G.edges(data=True):
                gu = community_of.get(u)
                gv = community_of.get(v)
                if not gu or not gv:
                    continue
                i = gu - 1
                j = gv - 1
                w = int(data.get('weight') or 0)
                matrix_weights[i][j] += w
                matrix_weights[j][i] += w
                matrix_edges[i][j] += 1
                matrix_edges[j][i] += 1

            strongest_links = []
            for i in range(group_count):
                for j in range(i + 1, group_count):
                    if matrix_weights[i][j] <= 0:
                        continue
                    strongest_links.append((i + 1, j + 1, matrix_weights[i][j], matrix_edges[i][j]))
            strongest_links.sort(key=lambda item: item[2], reverse=True)

            embed = discord.Embed(
                title=f"Halo Groups: {gamertag}",
                description=(
                    "Detected co-play communities from weighted shared-match edges."
                    + (" Using fallback threshold of 1 shared match." if threshold_relaxed else "")
                ),
                colour=0x1ABC9C,
                timestamp=datetime.now(),
            )
            embed.add_field(
                name="Summary",
                value=(
                    f"Nodes: **{G.number_of_nodes()}**\n"
                    f"Edges: **{G.number_of_edges()}**\n"
                    f"Groups: **{group_count}**\n"
                    f"Min edge weight: **{active_min_matches}**"
                ),
                inline=True,
            )

            top_group_lines = []
            for idx, members in enumerate(communities[:5], start=1):
                names = sorted((node_map.get(m, {}).get('gamertag') or m) for m in members)
                preview = ", ".join(names[:6])
                if len(names) > 6:
                    preview += f", +{len(names) - 6} more"
                top_group_lines.append(f"G{idx} ({len(members)}): {preview}")
            embed.add_field(
                name="Top Groups",
                value="\n".join(top_group_lines) if top_group_lines else "No groups",
                inline=False,
            )

            if strongest_links:
                lines = [
                    f"G{a} <-> G{b}: {weight} shared matches across {edge_count} links"
                    for a, b, weight, edge_count in strongest_links[:6]
                ]
                embed.add_field(name="Strongest Inter-Group Links", value="\n".join(lines), inline=False)
            else:
                embed.add_field(
                    name="Strongest Inter-Group Links",
                    value="No cross-group links were detected (single cluster or disconnected communities).",
                    inline=False,
                )

            overlap_buf = io.StringIO()
            overlap_writer = csv.writer(overlap_buf)
            overlap_writer.writerow(["Group"] + [f"G{i}" for i in range(1, group_count + 1)])
            for i in range(group_count):
                overlap_writer.writerow([f"G{i + 1}"] + matrix_weights[i])

            members_buf = io.StringIO()
            members_writer = csv.writer(members_buf)
            members_writer.writerow(["group_id", "xuid", "gamertag", "is_center"])
            for idx, members in enumerate(communities, start=1):
                for member_xuid in sorted(members):
                    members_writer.writerow([
                        idx,
                        member_xuid,
                        node_map.get(member_xuid, {}).get('gamertag') or member_xuid,
                        int(member_xuid == xuid),
                    ])

            overlap_file = discord.File(
                io.BytesIO(overlap_buf.getvalue().encode('utf-8')),
                filename=f"halogroups_overlap_{xuid}.csv",
            )
            members_file = discord.File(
                io.BytesIO(members_buf.getvalue().encode('utf-8')),
                filename=f"halogroups_members_{xuid}.csv",
            )

            embed.set_footer(
                text="Attached CSVs: overlap matrix (shared matches) and full group memberships"
            )

            await loading_msg.delete()
            await ctx.send(embed=embed, files=[overlap_file, members_file])

        except Exception as e:
            try:
                await loading_msg.delete()
            except Exception:
                pass
            await ctx.send(f"Error showing halogroups: {str(e)}")
            raise
    
    @commands.command(name='network', help='Show a player network visualization from graph DB data. Usage: #network <gamertag>')
    async def show_network(self, ctx: commands.Context, *inputs):
        """Show a player's Halo-active friend network with a visual graph image"""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#network GAMERTAG`")
            return

        gamertag = ' '.join(inputs)

        loading_embed = discord.Embed(
            title="Building Network Graph...",
            description=f"Fetching data for **{gamertag}**",
            colour=0xFFA500,
            timestamp=datetime.now()
        )
        loading_msg = await ctx.send(embed=loading_embed)

        try:
            # Resolve gamertag
            xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await loading_msg.delete()
                await ctx.send(f"Could not find player **{gamertag}**")
                return

            # Get player info
            player = self.db.get_player(xuid)
            if not player:
                await loading_msg.delete()
                await ctx.send(f"**{gamertag}** is not in the graph database. Run a crawl first.")
                return

            # Get friends
            halo_friends = self.db.get_halo_friends(xuid)
            halo_flagged_count = len(halo_friends)
            all_friends = self.db.get_friends(xuid)
            features = self.db.get_halo_features(xuid)

            last_crawled_raw = player.get('last_crawled')
            if last_crawled_raw:
                try:
                    last_crawled_dt = datetime.fromisoformat(last_crawled_raw)
                    crawl_status = f"Last crawl: {last_crawled_dt.strftime('%Y-%m-%d %H:%M')}"
                except (ValueError, TypeError):
                    crawl_status = f"Last crawl: {last_crawled_raw}"
            else:
                crawl_status = "Player not crawled"

            # Keep graph nodes to verified Halo-active friends (have recorded Halo matches).
            halo_friends = [f for f in halo_friends if (f.get('matches_played') or 0) > 0]
            halo_verified_count = len(halo_friends)
            friends_with_stats = [f for f in halo_friends if f.get('matches_played') is not None]

            # Social group size comes from persisted snapshot on each friend node.
            # Lazily backfill if no snapshot exists yet for a friend.
            for friend in halo_friends:
                friend_xuid = friend.get('dst_xuid')
                if not friend_xuid:
                    continue

                if friend.get('inference_updated_at') is None:
                    snapshot = self.db.refresh_inferred_group_snapshot(friend_xuid)
                    friend['social_group_size'] = int(snapshot.get('social_group_size') or 0)
                    friend['group_size_inferred'] = bool(snapshot.get('social_group_size_inferred'))
                    friend['group_size_source'] = snapshot.get('social_group_source') or 'unknown'
                else:
                    friend['social_group_size'] = int(friend.get('social_group_size') or 0)
                    friend['group_size_inferred'] = bool(friend.get('social_group_size_inferred'))
                    friend['group_size_source'] = friend.get('social_group_source') or 'unknown'

            # Build summary embed
            embed = discord.Embed(
                title=f"Network: {gamertag} ({crawl_status})",
                colour=0x3498DB,
                timestamp=datetime.now()
            )

            embed.add_field(
                name="Summary",
                value=(
                    f"Total friends: **{len(all_friends)}**\n"
                    f"Halo flagged: **{halo_flagged_count}**\n"
                    f"Halo-active friends (verified): **{halo_verified_count}**\n"
                    f"Node depth (from seed): **{player.get('crawl_depth', '?')}**\n"
                    f"Friends with stats: **{len(friends_with_stats)}/{halo_verified_count}**"
                ),
                inline=True
            )

            if features and (features.get('matches_played') or 0) > 0:
                embed.add_field(
                    name="Player Stats",
                    value=(
                        f"K/D: {features.get('kd_ratio') or 0:.2f}\n"
                        f"Win Rate: {features.get('win_rate') or 0:.1f}%\n"
                        f"Matches: {features.get('matches_played') or 0}"
                    ),
                    inline=True
                )

            if halo_friends:
                sorted_friends = sorted(
                    halo_friends,
                    key=lambda x: ((x.get('social_group_size') or 0), (x.get('matches_played') or 0)),
                    reverse=True
                )
                section_title = "Top Halo Friends (by Social Group Size)"

                top_str = ""
                for f in sorted_friends[:5]:
                    gt = f.get('gamertag') or f.get('dst_xuid', 'Unknown')
                    kd = f.get('kd_ratio')
                    kd_str = f"{kd:.2f}" if kd is not None else "N/A"
                    group_size = f.get('social_group_size') or 0
                    top_str += f"**{gt}**: Group Size {group_size}, K/D {kd_str}\n"
                embed.add_field(
                    name=section_title,
                    value=top_str or "None",
                    inline=False
                )

            embed.set_footer(text=f"XUID: {xuid} | Gold=center | YlOrRd=group size | Greens=link strength | Green outline=direct | Orange outline=inferred | Red outline=private")

            await loading_msg.delete()

            if not halo_friends:
                await ctx.send(embed=embed)
                return

            MAX_FRIENDS = 60
            friends_to_show = halo_friends[:MAX_FRIENDS]

            node_map: Dict[str, Dict] = {
                xuid: {
                    'xuid': xuid,
                    'gamertag': gamertag,
                    'group_size': len(halo_friends),
                    'kd_ratio': features.get('kd_ratio') if features else None,
                    'win_rate': features.get('win_rate') if features else None,
                    'matches_played': features.get('matches_played') if features else None,
                    'is_center': True,
                }
            }
            for f in friends_to_show:
                fxuid = f.get('dst_xuid')
                if not fxuid:
                    continue
                node_map[fxuid] = {
                    'xuid': fxuid,
                    'gamertag': f.get('gamertag') or fxuid,
                    'group_size': f.get('social_group_size') or 0,
                    'group_size_inferred': bool(f.get('group_size_inferred')),
                    'group_size_source': f.get('group_size_source') or 'direct',
                    'kd_ratio': f.get('kd_ratio'),
                    'win_rate': f.get('win_rate'),
                    'matches_played': f.get('matches_played'),
                    'is_mutual': bool(f.get('is_mutual')),
                    'is_center': False,
                }

            # Render graph image in a thread to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            buf = await loop.run_in_executor(
                None,
                lambda: self._render_network_graph(xuid, gamertag, friends_to_show, features)
            )
            file = discord.File(fp=buf, filename="network.png")
            filter_view = NetworkFilterView(
                cog=self,
                requester_id=ctx.author.id,
                center_xuid=xuid,
                center_gamertag=gamertag,
                halo_friends=friends_to_show,
                center_features=features,
                base_embed=embed,
            )
            embed.description = filter_view._build_description(controls_active=True)
            embed.set_image(url="attachment://network.png")
            graph_message = await ctx.send(
                embed=embed,
                file=file,
                view=filter_view,
            )
            filter_view.message = graph_message

            if len(node_map) > 1:
                await ctx.send(
                    "Use the selector below for node details:",
                    view=NetworkNodeInfoView(node_map=node_map, requester_id=ctx.author.id, db=self.db),
                )
                await ctx.send("Use controls on the graph message to switch layout and apply filters. If controls time out, use **Refresh Controls** on that same message.")

        except Exception as e:
            try:
                await loading_msg.delete()
            except Exception:
                pass
            await ctx.send(f"Error showing network: {str(e)}")
            raise
    
    def _render_network_graph(
        self,
        center_xuid: str,
        center_gamertag: str,
        halo_friends: list,
        center_features: Optional[dict],
        clustered: bool = False,
        min_group_size: int = 0,
        min_link_strength: float = 1.0,
    ) -> io.BytesIO:
        """Render the friend network as a PNG and return a BytesIO buffer (sync)."""
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.cm as cm
        import matplotlib.colors as mcolors
        from matplotlib.lines import Line2D
        import matplotlib.patheffects as path_effects
        import networkx as nx

        MAX_FRIENDS = 60
        friends_to_show = [
            f for f in halo_friends[:MAX_FRIENDS]
            if (f.get('social_group_size') or 0) >= min_group_size
        ]

        friend_xuids = [f['dst_xuid'] for f in friends_to_show]
        all_xuids = [center_xuid] + friend_xuids

        # Cross-edges between friends (not touching center node)
        cross_edges_raw = self.db.get_edges_within_set(all_xuids)
        cross_edge_set = {
            (e['src_xuid'], e['dst_xuid'])
            for e in cross_edges_raw
            if e['src_xuid'] != center_xuid and e['dst_xuid'] != center_xuid
        }

        G = nx.Graph()

        # Center node
        center_group_size = len(halo_friends)
        G.add_node(center_xuid, label=center_gamertag, is_center=True, group_size=center_group_size)

        # Friend nodes + spoke edges
        for f in friends_to_show:
            fxuid = f['dst_xuid']
            fgt = f.get('gamertag') or fxuid[:10]
            G.add_node(
                fxuid,
                label=fgt,
                is_center=False,
                group_size=f.get('social_group_size') or 0,
                group_size_inferred=bool(f.get('group_size_inferred')),
            )
            G.add_edge(center_xuid, fxuid, weight=1.0)

        # Cross-edges
        for src, dst in cross_edge_set:
            if G.has_node(src) and G.has_node(dst) and not G.has_edge(src, dst):
                G.add_edge(src, dst, weight=1.4)

        if min_link_strength > 1:
            degree_map = dict(G.degree())
            edges_to_remove = []
            for u, v in G.edges():
                strength = min(degree_map.get(u, 0), degree_map.get(v, 0))
                if strength < min_link_strength:
                    edges_to_remove.append((u, v))
            if edges_to_remove:
                G.remove_edges_from(edges_to_remove)

        # Keep only nodes reachable from the center after filtering.
        if G.has_node(center_xuid):
            reachable = nx.node_connected_component(G, center_xuid)
            nodes_to_remove = [n for n in G.nodes if n not in reachable]
            if nodes_to_remove:
                G.remove_nodes_from(nodes_to_remove)

        # Layout: spread nodes further for readability in dense networks.
        k = 5.8 / max(1, len(G.nodes) ** 0.5)
        pos = nx.spring_layout(G, seed=42, k=k, iterations=120, weight='weight')

        if clustered and len(G.nodes) >= 5 and len(G.edges) >= 4:
            communities = list(nx.algorithms.community.greedy_modularity_communities(G, weight='weight'))
            if len(communities) > 1:
                cluster_of = {}
                for cid, members in enumerate(communities):
                    for n in members:
                        cluster_of[n] = cid

                cluster_graph = nx.Graph()
                for cid in range(len(communities)):
                    cluster_graph.add_node(cid)
                for u, v, data in G.edges(data=True):
                    cu = cluster_of.get(u)
                    cv = cluster_of.get(v)
                    if cu is None or cv is None or cu == cv:
                        continue
                    w = data.get('weight', 1.0)
                    if cluster_graph.has_edge(cu, cv):
                        cluster_graph[cu][cv]['weight'] += w
                    else:
                        cluster_graph.add_edge(cu, cv, weight=w)

                cluster_k = 1.9 / max(1, len(cluster_graph.nodes()) ** 0.5)
                cluster_pos = nx.spring_layout(cluster_graph, seed=42, k=cluster_k, iterations=100, weight='weight')

                clustered_pos = {}
                for cid, members in enumerate(communities):
                    sub = G.subgraph(members)
                    local_k = 1.3 / max(1, len(sub.nodes()) ** 0.5)
                    local_pos = nx.spring_layout(sub, seed=42, k=local_k, iterations=70, weight='weight')
                    center = cluster_pos.get(cid, (0.0, 0.0))
                    radius = 0.18 + 0.025 * min(10, len(sub.nodes()))
                    for n, coords in local_pos.items():
                        clustered_pos[n] = (
                            center[0] + coords[0] * radius,
                            center[1] + coords[1] * radius,
                        )

                if len(clustered_pos) == len(G.nodes):
                    pos = clustered_pos

        # Stretch normalized layout to fill most of the canvas width/height.
        x_values = [p[0] for p in pos.values()]
        y_values = [p[1] for p in pos.values()]
        x_min, x_max = min(x_values), max(x_values)
        y_min, y_max = min(y_values), max(y_values)
        x_span = (x_max - x_min) or 1.0
        y_span = (y_max - y_min) or 1.0

        x_left, x_right = 0.02, 0.84
        # Reserve extra top room so sparse-node layouts do not sit under legend/colorbars.
        y_bottom, y_top = 0.03, 0.87
        pos = {
            node: (
                x_left + ((coords[0] - x_min) / x_span) * (x_right - x_left),
                y_bottom + ((coords[1] - y_min) / y_span) * (y_top - y_bottom),
            )
            for node, coords in pos.items()
        }

        # Single colour scale for group size; confidence comes from outlines.
        friend_groups = [
            G.nodes[n]['group_size']
            for n in friend_xuids
            if G.has_node(n)
        ]

        def _safe_norm(values: list):
            if not values:
                return mcolors.Normalize(vmin=0, vmax=1)
            vmin = min(values)
            vmax = max(values)
            if vmin == vmax:
                vmax = vmin + 1
            return mcolors.Normalize(vmin=vmin, vmax=vmax)

        group_colormap = cm.YlOrRd
        link_colormap = cm.Greens     # edge/link strength
        group_norm = _safe_norm(friend_groups)

        node_colors = []
        node_sizes = []
        node_edge_colors = []
        node_linewidths = []
        labels = {}
        for n in G.nodes:
            data = G.nodes[n]
            labels[n] = data['label']
            if data.get('is_center'):
                node_colors.append('#FFD700')  # gold for center
                node_sizes.append(620)
                node_edge_colors.append('white')
                node_linewidths.append(1.0)
            else:
                node_colors.append(group_colormap(group_norm(data['group_size'])))
                node_sizes.append(120 + G.degree(n) * 28)
                if data.get('group_size_inferred'):
                    # Orange outline indicates inferred group size via reciprocal visibility.
                    node_edge_colors.append('#FFA500')
                    node_linewidths.append(2.4)
                elif (data.get('group_size') or 0) == 0:
                    # Red outline indicates likely private friends list (no visible Halo social group members).
                    node_edge_colors.append('#FF3B30')
                    node_linewidths.append(2.4)
                else:
                    # Green outline for direct friend nodes with visible data
                    node_edge_colors.append('#22dd22')
                    node_linewidths.append(1.2)
        # Label all nodes and add an outline for readability on dense backgrounds.
        visible_labels = labels

        # Plot
        bg = '#1a1a2e'
        fig, ax = plt.subplots(figsize=(14.5, 11), facecolor=bg)
        fig.subplots_adjust(left=0.02, right=0.98, top=0.90, bottom=0.06)
        ax.set_facecolor(bg)

        # Spoke edges (center -> friends)
        spoke_edges = [(u, v) for u, v in G.edges() if center_xuid in (u, v)]
        cross_list = [(u, v) for u, v in G.edges() if center_xuid not in (u, v)]

        # Node link strength in rendered graph: use endpoint degree on each edge.
        edge_strengths = [min(G.degree(u), G.degree(v)) for u, v in G.edges()]
        link_norm = _safe_norm(edge_strengths)

        def _edge_style(edges: list, alpha_base: float):
            colors = []
            widths = []
            for u, v in edges:
                strength = min(G.degree(u), G.degree(v))
                colors.append(link_colormap(link_norm(strength)))
                widths.append(0.7 + 1.8 * link_norm(strength))
            return colors, widths, alpha_base

        spoke_colors, spoke_widths, spoke_alpha = _edge_style(spoke_edges, 0.55)
        cross_colors, cross_widths, cross_alpha = _edge_style(cross_list, 0.35)

        nx.draw_networkx_edges(G, pos, edgelist=spoke_edges,
                               edge_color=spoke_colors, width=spoke_widths, alpha=spoke_alpha, ax=ax)
        if cross_list:
            nx.draw_networkx_edges(G, pos, edgelist=cross_list,
                                   edge_color=cross_colors, width=cross_widths, alpha=cross_alpha, ax=ax)

        nx.draw_networkx_nodes(G, pos, node_color=node_colors,
                               node_size=node_sizes, linewidths=node_linewidths,
                               edgecolors=node_edge_colors, ax=ax)
        label_artists = nx.draw_networkx_labels(
            G,
            pos,
            labels=visible_labels,
            font_size=6,
            font_color='white',
            ax=ax,
        )
        for text_artist in label_artists.values():
            text_artist.set_path_effects([
                path_effects.Stroke(linewidth=2.2, foreground='black'),
                path_effects.Normal(),
            ])

        # Colourbars (group size + link strength), horizontal along the top beside legend.
        group_cax = fig.add_axes([0.36, 0.915, 0.25, 0.018])
        link_cax = fig.add_axes([0.66, 0.915, 0.25, 0.018])
        group_cax.set_facecolor(bg)
        link_cax.set_facecolor(bg)

        group_sm = cm.ScalarMappable(cmap=group_colormap, norm=group_norm)
        group_sm.set_array([])
        group_cbar = fig.colorbar(group_sm, cax=group_cax, orientation='horizontal')
        group_cbar.set_label('Group Size (YlOrRd: low -> high)', color='white', fontsize=8)
        group_cbar.ax.xaxis.set_tick_params(color='white', labelsize=7)
        plt.setp(group_cbar.ax.xaxis.get_ticklabels(), color='white')
        group_cbar.outline.set_edgecolor('white')

        link_sm = cm.ScalarMappable(cmap=link_colormap, norm=link_norm)
        link_sm.set_array([])
        link_cbar = fig.colorbar(link_sm, cax=link_cax, orientation='horizontal')
        link_cbar.set_label('Node Link Strength (Greens: weak -> strong)', color='white', fontsize=9)
        link_cbar.ax.xaxis.set_tick_params(color='white', labelsize=7)
        plt.setp(link_cbar.ax.xaxis.get_ticklabels(), color='white')
        link_cbar.outline.set_edgecolor('white')

        # Graph key for node semantics; outline-only meanings intentionally have no fill.
        legend_handles = [
            Line2D([0], [0], marker='o', color='none', label='Center Player', markerfacecolor='#FFD700',
                   markeredgecolor='white', markeredgewidth=1.0, markersize=9),
            Line2D([0], [0], marker='o', color='none', label='Green Outline Only: direct friend data visible', markerfacecolor='none',
                   markeredgecolor='#22dd22', markeredgewidth=2.2, markersize=8),
            Line2D([0], [0], marker='o', color='none', label='Orange Outline Only: inferred via reciprocal data', markerfacecolor='none',
                   markeredgecolor='#FFA500', markeredgewidth=2.2, markersize=8),
            Line2D([0], [0], marker='o', color='none', label='Red Outline Only: private/empty friend list', markerfacecolor='none',
                   markeredgecolor='#FF3B30', markeredgewidth=2.2, markersize=8),
            Line2D([0], [0], color=link_colormap(0.25), linewidth=1.2, label='Weaker Link Strength'),
            Line2D([0], [0], color=link_colormap(0.85), linewidth=2.6, label='Stronger Link Strength'),
            Line2D([0], [0], marker='o', color='none', label='Smaller Node (Lower Degree)', markerfacecolor='#cccccc',
                   markeredgecolor='white', markeredgewidth=0.6, markersize=5),
            Line2D([0], [0], marker='o', color='none', label='Larger Node (Higher Degree)', markerfacecolor='#cccccc',
                   markeredgecolor='white', markeredgewidth=0.6, markersize=10),
        ]
        legend = ax.legend(
            handles=legend_handles,
            loc='upper left',
            frameon=True,
            facecolor=bg,
            edgecolor='white',
            fontsize=8,
        )
        for text in legend.get_texts():
            text.set_color('white')

        shown = len(friends_to_show)
        total = len(halo_friends)
        # Count private-list nodes (group_size=0) including those that were inferred as such.
        private_list_count = sum(
            1
            for f in friends_to_show
            if (f.get('social_group_size') or 0) == 0
        )
        inferred_count = sum(1 for f in friends_to_show if f.get('group_size_inferred'))
        title = f"Halo Network: {center_gamertag}"
        if total > shown:
            title += f"  (showing {shown} of {total} Halo friends)"
        title += f"  |  Private-list nodes: {private_list_count}"
        title += f"  |  Inferred nodes: {inferred_count}"
        if min_group_size > 0 or min_link_strength > 1:
            title += f"  |  Filter N>={min_group_size}, E>={int(min_link_strength)}"
        if clustered:
            title += "  |  Layout: Clustered"
        # Place title at the bottom per user preference.
        fig.text(0.50, 0.015, title, color='white', fontsize=12, ha='center', va='bottom')
        ax.axis('off')
        # Keep margins minimal after explicit subplot placement.
        ax.margins(x=0.0, y=0.0)

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=120, bbox_inches='tight', facecolor=bg)
        buf.seek(0)
        plt.close(fig)
        return buf

    def _render_coplay_graph(
        self,
        center_xuid: str,
        center_gamertag: str,
        node_map: Dict[str, Dict],
        edges: List[Dict],
    ) -> io.BytesIO:
        """Render a weighted co-play graph as a PNG and return a BytesIO buffer."""
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.cm as cm
        import matplotlib.colors as mcolors
        import matplotlib.pyplot as plt
        import matplotlib.patheffects as path_effects
        import networkx as nx

        G = nx.Graph()

        for xuid, data in node_map.items():
            G.add_node(
                xuid,
                label=data.get('gamertag') or xuid,
                is_center=bool(data.get('is_center')),
            )

        for edge in edges:
            src = edge.get('src_xuid')
            dst = edge.get('dst_xuid')
            matches = int(edge.get('matches_together') or 0)
            if not src or not dst or src == dst or matches <= 0:
                continue
            if not G.has_node(src) or not G.has_node(dst):
                continue
            if G.has_edge(src, dst):
                G[src][dst]['weight'] += matches
            else:
                G.add_edge(src, dst, weight=matches)

        if not G.edges:
            G.add_node(center_xuid, label=center_gamertag, is_center=True)

        weighted_degree = {node: float(G.degree(node, weight='weight')) for node in G.nodes}
        edge_weights = [float(data.get('weight') or 0.0) for _, _, data in G.edges(data=True)]

        def _safe_norm(values: List[float]) -> mcolors.Normalize:
            if not values:
                return mcolors.Normalize(vmin=0, vmax=1)
            vmin = min(values)
            vmax = max(values)
            if vmin == vmax:
                vmax = vmin + 1
            return mcolors.Normalize(vmin=vmin, vmax=vmax)

        node_norm = _safe_norm(list(weighted_degree.values()))
        edge_norm = _safe_norm(edge_weights)
        node_cmap = cm.Blues
        edge_cmap = cm.Greens

        pos = nx.spring_layout(G, seed=42, k=4.5 / max(1, len(G.nodes) ** 0.5), iterations=140, weight='weight')

        node_colors = []
        node_sizes = []
        for node in G.nodes:
            if node == center_xuid:
                node_colors.append('#FFD700')
                node_sizes.append(760)
            else:
                strength = weighted_degree.get(node, 0.0)
                node_colors.append(node_cmap(node_norm(strength)))
                node_sizes.append(180 + 22 * strength)

        edge_colors = []
        edge_widths = []
        for _, _, data in G.edges(data=True):
            weight = float(data.get('weight') or 0.0)
            edge_colors.append(edge_cmap(edge_norm(weight)))
            edge_widths.append(0.8 + 2.8 * edge_norm(weight))

        bg = '#101820'
        fig, ax = plt.subplots(figsize=(14.5, 11), facecolor=bg)
        fig.subplots_adjust(left=0.03, right=0.97, top=0.92, bottom=0.06)
        ax.set_facecolor(bg)

        nx.draw_networkx_edges(
            G,
            pos,
            edge_color=edge_colors,
            width=edge_widths,
            alpha=0.65,
            ax=ax,
        )
        nx.draw_networkx_nodes(
            G,
            pos,
            node_color=node_colors,
            node_size=node_sizes,
            linewidths=1.2,
            edgecolors='white',
            ax=ax,
        )

        labels = {n: G.nodes[n].get('label', n) for n in G.nodes}
        label_artists = nx.draw_networkx_labels(G, pos, labels=labels, font_size=7, font_color='white', ax=ax)
        for text_artist in label_artists.values():
            text_artist.set_path_effects([
                path_effects.Stroke(linewidth=2.2, foreground='black'),
                path_effects.Normal(),
            ])

        edge_cax = fig.add_axes([0.60, 0.93, 0.33, 0.018])
        edge_cax.set_facecolor(bg)
        edge_sm = cm.ScalarMappable(cmap=edge_cmap, norm=edge_norm)
        edge_sm.set_array([])
        edge_cbar = fig.colorbar(edge_sm, cax=edge_cax, orientation='horizontal')
        edge_cbar.set_label('Shared Matches per Edge (low -> high)', color='white', fontsize=9)
        edge_cbar.ax.xaxis.set_tick_params(color='white', labelsize=7)
        plt.setp(edge_cbar.ax.xaxis.get_ticklabels(), color='white')
        edge_cbar.outline.set_edgecolor('white')

        title = (
            f"HaloNet Co-play Graph: {center_gamertag}  |  Nodes: {len(G.nodes)}"
            f"  |  Edges: {len(G.edges)}"
        )
        fig.text(0.5, 0.015, title, color='white', fontsize=12, ha='center', va='bottom')

        ax.axis('off')
        ax.margins(x=0.0, y=0.0)

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=120, bbox_inches='tight', facecolor=bg)
        buf.seek(0)
        plt.close(fig)
        return buf

    def _collect_halo_active_scope(self, seed_xuid: str, max_depth: int) -> List[str]:
        """Collect halo-active players reachable from seed within depth in current graph DB."""
        visited = {seed_xuid}
        frontier = {seed_xuid}

        for _ in range(max(0, max_depth)):
            next_frontier = set()
            for current_xuid in frontier:
                for edge in self.db.get_friends(current_xuid):
                    dst_xuid = edge.get('dst_xuid')
                    if not dst_xuid or dst_xuid in visited:
                        continue
                    if not bool(edge.get('halo_active')):
                        continue
                    visited.add(dst_xuid)
                    next_frontier.add(dst_xuid)
            if not next_frontier:
                break
            frontier = next_frontier

        return sorted(visited)

    @commands.command(name='crawlfriends', help='Start a background Halo-friends crawl from a seed player. Admin only.')
    @commands.has_permissions(administrator=True)
    async def start_crawl(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ):
        """Start a background crawl (admin only)"""
        if not inputs:
            await ctx.send("Usage: `#crawlfriends GAMERTAG [depth]`\nExample: `#crawlfriends YourGamertag 2`\nNote: Wrap gamertags with spaces in quotes: `#crawlfriends \"Possibly Tom\" 2`")
            return
        
        # Handle gamertags with spaces: check if last arg is a number (depth)
        if len(inputs) > 1 and inputs[-1].isdigit():
            # Last arg is depth, everything else is the gamertag
            gamertag = ' '.join(inputs[:-1])
            depth = int(inputs[-1])
        else:
            # No depth specified, entire input is the gamertag
            gamertag = ' '.join(inputs)
            depth = 2
        
        if self._crawl_task and not self._crawl_task.done():
            await ctx.send("A crawl is already running. Wait for it to complete or restart the bot.")
            return

        # Early detection: avoid launching crawl for known/private friend-list profiles.
        seed_xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
        if not seed_xuid:
            await ctx.send(f"Could not resolve **{gamertag}**. Check spelling and try again.")
            return

        seed_player = self.db.get_player(seed_xuid)
        if seed_player and seed_player.get('profile_visibility') == 'private':
            await ctx.send(
                f"Cannot crawl **{gamertag}**: profile is marked private (friends list not visible)."
            )
            return

        try:
            seed_friends_probe = await api_client.get_friends_list(seed_xuid)
        except Exception as e:
            await ctx.send(f"Unable to verify seed profile visibility before crawl: {str(e)}")
            return

        if seed_friends_probe.get('is_private'):
            self.db.insert_or_update_player(
                xuid=seed_xuid,
                gamertag=gamertag,
                profile_visibility='private',
            )
            await ctx.send(
                f"Cannot crawl **{gamertag}**: friends list is private/unavailable."
            )
            return
        
        if not run_inline:
            await ctx.send(f"Starting background friends crawl from **{gamertag}** with depth {depth}...\nUse `#graphstats` to check progress.")

        async def crawl_progress_update(progress):
            if not progress_callback:
                return
            crawled = int(getattr(progress, 'nodes_crawled', 0) or 0)
            discovered = int(getattr(progress, 'nodes_discovered', 0) or 0)
            denominator = max(crawled + 1, discovered, 1)
            crawl_pct = min(84.0, (float(crawled) / float(denominator)) * 84.0)
            await progress_callback(
                {
                    "stage": "Crawling friends",
                    "percent": crawl_pct,
                    "detail": f"Crawled {crawled} nodes, discovered {discovered}",
                }
            )
        
        # Import here to avoid circular imports
        from src.graph.crawler import GraphCrawler, CrawlConfig
        
        async def run_crawl():
            try:
                config = CrawlConfig(
                    max_depth=depth,
                    collect_stats=True,
                    stats_matches_to_process=25,
                    progress_callback=crawl_progress_update if progress_callback else None,
                )
                crawler = GraphCrawler(api_client, config, self.db)
                progress = await crawler.crawl_from_seed(seed_gamertag=gamertag)

                if progress_callback:
                    await progress_callback(
                        {
                            "stage": "Finalizing",
                            "percent": 100.0,
                            "detail": "Friends crawl complete",
                        }
                    )
                
                # Send completion message
                if not run_inline:
                    channel = ctx.channel
                    embed = discord.Embed(
                        title="Crawl Complete",
                        colour=0x00FF00,
                        timestamp=datetime.now()
                    )
                    embed.add_field(name="Seed", value=gamertag, inline=True)
                    embed.add_field(name="Depth", value=str(depth), inline=True)
                    embed.add_field(name="Nodes Discovered", value=str(progress.nodes_discovered), inline=True)
                    embed.add_field(name="Halo Players", value=str(progress.halo_players_found), inline=True)
                    embed.add_field(name="Edges", value=str(progress.edges_discovered), inline=True)
                    embed.add_field(name="With Stats", value=str(progress.nodes_with_stats), inline=True)
                    
                    await channel.send(embed=embed)
                return (
                    f"Friends crawl completed for {gamertag}. "
                    f"Discovered {progress.nodes_discovered} players, "
                    f"halo-active {progress.halo_players_found}, "
                    f"stats on {progress.nodes_with_stats}."
                )
                
            except Exception as e:
                if not run_inline:
                    await ctx.channel.send(f"Crawl error: {str(e)}")
                raise
        
        self._crawl_task = asyncio.create_task(run_crawl())
        if run_inline:
            return await self._crawl_task

    @commands.command(name='crawlgames', help='Crawl and build co-play edges from shared match history. Admin only.')
    @commands.has_permissions(administrator=True)
    async def start_crawl_games(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ):
        """Start background game-history crawl and refresh co-play edge weights."""
        if not inputs:
            await ctx.send("Usage: `#crawlgames GAMERTAG [depth]`\nExample: `#crawlgames YourGamertag 2`")
            return

        if len(inputs) > 1 and inputs[-1].isdigit():
            gamertag = ' '.join(inputs[:-1])
            depth = int(inputs[-1])
        else:
            gamertag = ' '.join(inputs)
            depth = 2

        if self._crawl_task and not self._crawl_task.done():
            await ctx.send("A crawl is already running. Wait for it to complete or restart the bot.")
            return

        if not run_inline:
            await ctx.send(
                f"Starting participant-first co-play build for **{gamertag}** with depth {depth}. "
                "This now builds from global match participants and reports seed-specific coverage."
            )

        async def run_coplay_crawl():
            try:
                if progress_callback:
                    await progress_callback(
                        {
                            "stage": "Preparing scope",
                            "percent": 5.0,
                            "detail": f"Resolving seed player {gamertag}",
                        }
                    )

                seed_xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
                if not seed_xuid:
                    await ctx.channel.send(
                        f"Could not resolve **{gamertag}**; co-play build skipped. "
                        "Run `#crawlfriends` first if this player has not been discovered yet."
                    )
                    return

                if progress_callback:
                    await progress_callback(
                        {
                            "stage": "Reading participants",
                            "percent": 20.0,
                            "detail": "Loading global match participants",
                        }
                    )

                match_edge_counts: Dict[tuple[str, str], int] = defaultdict(int)
                same_team_counts: Dict[tuple[str, str], int] = defaultdict(int)
                opposing_team_counts: Dict[tuple[str, str], int] = defaultdict(int)
                first_played: Dict[tuple[str, str], str] = {}
                last_played: Dict[tuple[str, str], str] = {}
                counted_pair_matches = set()

                players_analyzed = 0
                stats_db = getattr(getattr(api_client, 'stats_cache', None), 'db', None)
                if not stats_db or not hasattr(stats_db, 'get_all_match_participants'):
                    await ctx.channel.send(
                        "Participant-first co-play build requires stats DB participant access, but it is unavailable."
                    )
                    return

                all_match_participants = stats_db.get_all_match_participants() or {}
                analyzed_players = set()
                seed_matches_seen = 0
                seed_qualifying_matches = 0

                total_matches = len(all_match_participants)
                for idx, (match_id, participants) in enumerate(all_match_participants.items(), start=1):
                    if len(participants) < 2:
                        continue

                    normalized_participants = []
                    seen_in_match = set()
                    seed_in_match = False
                    start_time = ''
                    for participant in participants:
                        participant_xuid = str(participant.get('xuid') or '').strip()
                        if not participant_xuid or participant_xuid in seen_in_match:
                            continue
                        seen_in_match.add(participant_xuid)
                        normalized_participants.append(participant)
                        analyzed_players.add(participant_xuid)
                        if participant_xuid == seed_xuid:
                            seed_in_match = True
                        if not start_time:
                            start_time = str(participant.get('start_time') or '')

                    if seed_in_match:
                        seed_matches_seen += 1
                    if len(normalized_participants) < 2:
                        continue
                    if seed_in_match:
                        seed_qualifying_matches += 1

                    for left, right in combinations(normalized_participants, 2):
                        left_xuid = str(left.get('xuid') or '').strip()
                        right_xuid = str(right.get('xuid') or '').strip()
                        if not left_xuid or not right_xuid or left_xuid == right_xuid:
                            continue

                        src_xuid, dst_xuid = sorted((left_xuid, right_xuid))
                        pair_key = (src_xuid, dst_xuid)
                        pair_match_key = (match_id, src_xuid, dst_xuid)
                        if match_id and pair_match_key in counted_pair_matches:
                            continue
                        if match_id:
                            counted_pair_matches.add(pair_match_key)

                        match_edge_counts[pair_key] += 1

                        left_team = left.get('team_id') or left.get('inferred_team_id')
                        right_team = right.get('team_id') or right.get('inferred_team_id')
                        if left_team and right_team:
                            if str(left_team) == str(right_team):
                                same_team_counts[pair_key] += 1
                            else:
                                opposing_team_counts[pair_key] += 1

                        if start_time:
                            existing_first = first_played.get(pair_key)
                            existing_last = last_played.get(pair_key)
                            if not existing_first or start_time < existing_first:
                                first_played[pair_key] = start_time
                            if not existing_last or start_time > existing_last:
                                last_played[pair_key] = start_time

                    if progress_callback and total_matches > 0:
                        analysis_pct = 25.0 + (float(idx) / float(total_matches)) * 60.0
                        await progress_callback(
                            {
                                "stage": "Analyzing co-play",
                                "percent": min(85.0, analysis_pct),
                                "detail": f"Analyzed {idx}/{total_matches} matches",
                            }
                        )

                players_analyzed = len(analyzed_players)
                seed_pairs_found = sum(1 for src_xuid, dst_xuid in match_edge_counts if seed_xuid in (src_xuid, dst_xuid))

                halo_active_xuids: set[str] = set()
                conn = self.db._get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT xuid FROM graph_players WHERE halo_active = 1")
                halo_active_xuids = {str(row['xuid']) for row in cursor.fetchall() if row['xuid']}

                if progress_callback:
                    await progress_callback(
                        {
                            "stage": "Writing edges",
                            "percent": 88.0,
                            "detail": f"Writing {len(match_edge_counts)} co-play pairs",
                        }
                    )

                rows_written = 0
                seed_rows_written = 0
                total_pairs = max(1, len(match_edge_counts))
                for pair_idx, ((src_xuid, dst_xuid), matches_together) in enumerate(match_edge_counts.items(), start=1):
                    first_ts = first_played.get((src_xuid, dst_xuid))
                    last_ts = last_played.get((src_xuid, dst_xuid))
                    is_halo_active_pair = src_xuid in halo_active_xuids and dst_xuid in halo_active_xuids
                    is_seed_pair = seed_xuid in (src_xuid, dst_xuid)

                    if self.db.upsert_coplay_edge(
                        src_xuid=src_xuid,
                        dst_xuid=dst_xuid,
                        matches_together=matches_together,
                        wins_together=0,
                        first_played=first_ts,
                        last_played=last_ts,
                        total_minutes=0,
                        same_team_count=same_team_counts.get((src_xuid, dst_xuid), 0),
                        opposing_team_count=opposing_team_counts.get((src_xuid, dst_xuid), 0),
                        source_type='participants-runtime',
                        is_halo_active_pair=is_halo_active_pair,
                    ):
                        rows_written += 1
                        if is_seed_pair:
                            seed_rows_written += 1

                    if self.db.upsert_coplay_edge(
                        src_xuid=dst_xuid,
                        dst_xuid=src_xuid,
                        matches_together=matches_together,
                        wins_together=0,
                        first_played=first_ts,
                        last_played=last_ts,
                        total_minutes=0,
                        same_team_count=same_team_counts.get((src_xuid, dst_xuid), 0),
                        opposing_team_count=opposing_team_counts.get((src_xuid, dst_xuid), 0),
                        source_type='participants-runtime',
                        is_halo_active_pair=is_halo_active_pair,
                    ):
                        rows_written += 1
                        if is_seed_pair:
                            seed_rows_written += 1

                    if progress_callback and (pair_idx == 1 or pair_idx == total_pairs or pair_idx % 250 == 0):
                        write_pct = 88.0 + (float(pair_idx) / float(total_pairs)) * 10.0
                        await progress_callback(
                            {
                                "stage": "Writing edges",
                                "percent": min(98.0, write_pct),
                                "detail": f"Upserted {pair_idx}/{total_pairs} pairs",
                            }
                        )

                if progress_callback:
                    await progress_callback(
                        {
                            "stage": "Finalizing",
                            "percent": 100.0,
                            "detail": "Co-play crawl complete",
                        }
                    )

                embed = discord.Embed(
                    title="Co-play Crawl Complete",
                    colour=0x00FF88,
                    timestamp=datetime.now(),
                )
                embed.add_field(name="Seed", value=gamertag, inline=True)
                embed.add_field(name="Depth", value=str(depth), inline=True)
                embed.add_field(name="Scope Mode", value="Global participants", inline=True)
                embed.add_field(name="Players Analyzed", value=str(players_analyzed), inline=True)
                embed.add_field(name="Unique Co-play Pairs", value=str(len(match_edge_counts)), inline=True)
                embed.add_field(name="Co-play Rows Written", value=str(rows_written), inline=True)
                embed.add_field(name="Seed Pairs Found", value=str(seed_pairs_found), inline=True)
                embed.add_field(name="Seed Rows Written", value=str(seed_rows_written), inline=True)
                embed.add_field(name="Seed Qualifying Matches", value=str(seed_qualifying_matches), inline=True)
                embed.add_field(name="Participant Source", value="match_participants (global)", inline=True)
                embed.add_field(name="Next Step", value=f"Run `#halonet {gamertag}`", inline=True)
                if seed_matches_seen == 0:
                    embed.add_field(
                        name="Seed Not Found In Participant Rows",
                        value="The resolved seed did not appear in any persisted match participants.",
                        inline=False,
                    )
                elif seed_pairs_found == 0:
                    embed.add_field(
                        name="Seed Has Zero Co-play Pairs",
                        value=(
                            "Global pairs were built, but the seed did not form any pair in qualifying matches. "
                            "This indicates no co-participants were persisted alongside the seed in match_participants."
                        ),
                        inline=False,
                    )

                if len(match_edge_counts) == 0:
                    embed.add_field(
                        name="No Co-play Pairs Found",
                        value=(
                            "No shared-match pairs were discovered in persisted participant history. "
                            "Ensure match_participants ingestion is populated and rerun `#crawlgames`."
                        ),
                        inline=False,
                    )

                if not run_inline:
                    await ctx.channel.send(embed=embed)
                return (
                    f"Co-play crawl completed for {gamertag}. "
                    f"Global participant mode; analyzed {players_analyzed}, pairs {len(match_edge_counts)}, "
                    f"rows written {rows_written}, seed pairs {seed_pairs_found}, seed rows {seed_rows_written}."
                )
            except Exception as e:
                if not run_inline:
                    await ctx.channel.send(f"Co-play crawl error: {str(e)}")
                raise

        self._crawl_task = asyncio.create_task(run_coplay_crawl())
        if run_inline:
            return await self._crawl_task
    
    @commands.command(name='crawlstop', help='Stop the current background crawl. Admin only.')
    @commands.has_permissions(administrator=True)
    async def stop_crawl(self, ctx: commands.Context):
        """Stop the current background crawl"""
        if self._crawl_task and not self._crawl_task.done():
            self._crawl_task.cancel()
            await ctx.send("Crawl task cancelled. Progress has been saved.")
        else:
            await ctx.send("No crawl is currently running.")


async def setup(bot: commands.Bot):
    """Setup function for loading the cog"""
    await bot.add_cog(GraphCog(bot))
