"""
Graph Commands Cog for Halo Stats Discord Bot

Contains commands for social graph analysis:
- #graphstats - Show current graph statistics
- #similar - Find similar players (KNN)
- #hubs - Find hub players with many connections
- #network - Show a player's Halo-active friend network
- #crawl - Start background crawl from a seed player
"""

import asyncio
import io
from datetime import datetime
from typing import Dict, List, Optional

import discord
from discord.ext import commands

from src.database.graph_schema import get_graph_db
from src.api import api_client


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
        super().__init__(timeout=300)
        self.add_item(NetworkNodeInfoSelect(node_map=node_map, requester_id=requester_id, db=db))


class GraphCog(commands.Cog, name="Graph"):
    """Commands for social graph analysis and network discovery"""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_graph_db()
        self._crawl_task: Optional[asyncio.Task] = None
    
    @commands.command(name='graphstats', help='Show current social graph statistics')
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
    
    @commands.command(name='similar', help='Find similar Halo players. Example - #similar XxUK D3STROYxX')
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
    
    @commands.command(name='hubs', help='Find hub players with many Halo friends')
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
    
    @commands.command(name='network', help='Show a player\'s Halo friend network. Example - #network XxUK D3STROYxX')
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
            # Keep graph nodes to verified Halo-active friends (have recorded Halo matches).
            halo_friends = [f for f in halo_friends if (f.get('matches_played') or 0) > 0]
            halo_verified_count = len(halo_friends)
            friends_with_stats = [f for f in halo_friends if f.get('matches_played') is not None]

            # Social group size = known friend edges for each halo friend node.
            for friend in halo_friends:
                direct_count = self.db.get_verified_halo_friend_count(friend['dst_xuid'])
                inferred = False
                source = 'direct'
                if direct_count == 0:
                    reciprocal_count = self.db.get_verified_halo_incoming_friend_count(friend['dst_xuid'])
                    if reciprocal_count > 0:
                        direct_count = reciprocal_count
                        inferred = True
                        source = 'inferred-reciprocal'
                    else:
                        source = 'private-or-empty'
                friend['social_group_size'] = direct_count
                friend['group_size_inferred'] = inferred
                friend['group_size_source'] = source

            # Build summary embed
            embed = discord.Embed(
                title=f"Network: {gamertag}",
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
            embed.set_image(url="attachment://network.png")
            await ctx.send(embed=embed, file=file)

            if len(node_map) > 1:
                await ctx.send(
                    "Use the selector below for node details:",
                    view=NetworkNodeInfoView(node_map=node_map, requester_id=ctx.author.id, db=self.db),
                )

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
        friends_to_show = halo_friends[:MAX_FRIENDS]

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
            G.add_edge(center_xuid, fxuid)

        # Cross-edges
        for src, dst in cross_edge_set:
            if G.has_node(src) and G.has_node(dst) and not G.has_edge(src, dst):
                G.add_edge(src, dst)

        # Layout: better spacing to reduce dense packing
        k = 3.5 / max(1, len(G.nodes) ** 0.5)
        pos = nx.spring_layout(G, seed=42, k=k, iterations=75)

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
                node_sizes.append(700)
                node_edge_colors.append('white')
                node_linewidths.append(1.0)
            else:
                node_colors.append(group_colormap(group_norm(data['group_size'])))
                node_sizes.append(150 + G.degree(n) * 35)
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
        fig, ax = plt.subplots(figsize=(13, 10), facecolor=bg)
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
            font_size=7,
            font_color='white',
            ax=ax,
        )
        for text_artist in label_artists.values():
            text_artist.set_path_effects([
                path_effects.Stroke(linewidth=2.2, foreground='black'),
                path_effects.Normal(),
            ])

        # Colourbars (group size + link strength).
        group_sm = cm.ScalarMappable(cmap=group_colormap, norm=group_norm)
        group_sm.set_array([])
        group_cbar = fig.colorbar(group_sm, ax=ax, fraction=0.025, pad=0.02)
        group_cbar.set_label('Group Size (YlOrRd: low -> high)', color='white', fontsize=8)
        group_cbar.ax.yaxis.set_tick_params(color='white')
        plt.setp(group_cbar.ax.yaxis.get_ticklabels(), color='white')

        link_sm = cm.ScalarMappable(cmap=link_colormap, norm=link_norm)
        link_sm.set_array([])
        link_cbar = fig.colorbar(link_sm, ax=ax, fraction=0.025, pad=0.10)
        link_cbar.set_label('Node Link Strength (Greens: weak -> strong)', color='white', fontsize=9)
        link_cbar.ax.yaxis.set_tick_params(color='white')
        plt.setp(link_cbar.ax.yaxis.get_ticklabels(), color='white')

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
        ax.set_title(title, color='white', fontsize=13, pad=12)
        ax.axis('off')
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=120, bbox_inches='tight', facecolor=bg)
        buf.seek(0)
        plt.close(fig)
        return buf

    @commands.command(name='crawl', help='Start a background crawl from a seed player. Admin only.')
    @commands.has_permissions(administrator=True)
    async def start_crawl(self, ctx: commands.Context, *inputs):
        """Start a background crawl (admin only)"""
        if not inputs:
            await ctx.send("Usage: `#crawl GAMERTAG [depth]`\nExample: `#crawl YourGamertag 2`\nNote: Wrap gamertags with spaces in quotes: `#crawl \"Possibly Tom\" 2`")
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
        
        await ctx.send(f"Starting background crawl from **{gamertag}** with depth {depth}...\nUse `#graphstats` to check progress.")
        
        # Import here to avoid circular imports
        from src.graph.crawler import GraphCrawler, CrawlConfig
        
        async def run_crawl():
            try:
                config = CrawlConfig(
                    max_depth=depth,
                    collect_stats=True,
                    stats_matches_to_process=25
                )
                crawler = GraphCrawler(api_client, config, self.db)
                progress = await crawler.crawl_from_seed(seed_gamertag=gamertag)
                
                # Send completion message
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
                
            except Exception as e:
                await ctx.channel.send(f"Crawl error: {str(e)}")
        
        self._crawl_task = asyncio.create_task(run_crawl())
    
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
