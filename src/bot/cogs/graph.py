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
from typing import Optional

import discord
from discord.ext import commands

from src.database.graph_schema import get_graph_db
from src.api import api_client


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
            all_friends = self.db.get_friends(xuid)
            features = self.db.get_halo_features(xuid)

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
                    f"Halo friends: **{len(halo_friends)}**\n"
                    f"Crawl depth: **{player.get('crawl_depth', '?')}**"
                ),
                inline=True
            )

            if features and (features.get('matches_played') or 0) > 0:
                embed.add_field(
                    name="Player Stats",
                    value=(
                        f"CSR: {features.get('csr') or 0:.0f}\n"
                        f"K/D: {features.get('kd_ratio') or 0:.2f}\n"
                        f"Win Rate: {features.get('win_rate') or 0:.1f}%\n"
                        f"Matches: {features.get('matches_played') or 0}"
                    ),
                    inline=True
                )

            if halo_friends:
                sorted_friends = sorted(
                    halo_friends,
                    key=lambda x: x.get('csr') or 0,
                    reverse=True
                )
                top_str = ""
                for f in sorted_friends[:5]:
                    gt = f.get('gamertag') or f.get('dst_xuid', 'Unknown')
                    csr = f.get('csr') or 0
                    kd = f.get('kd_ratio') or 0
                    top_str += f"**{gt}**: CSR {csr:.0f}, K/D {kd:.2f}\n"
                embed.add_field(
                    name="Top Halo Friends (by CSR)",
                    value=top_str or "None",
                    inline=False
                )

            embed.set_footer(text=f"XUID: {xuid} | Gold node = {gamertag} | Colour = CSR")

            await loading_msg.delete()

            if not halo_friends:
                await ctx.send(embed=embed)
                return

            # Render graph image in a thread to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            buf = await loop.run_in_executor(
                None,
                lambda: self._render_network_graph(xuid, gamertag, halo_friends, features)
            )
            file = discord.File(fp=buf, filename="network.png")
            embed.set_image(url="attachment://network.png")
            await ctx.send(embed=embed, file=file)

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
        center_csr = (center_features.get('csr') or 0) if center_features else 0
        G.add_node(center_xuid, label=center_gamertag, is_center=True, csr=center_csr)

        # Friend nodes + spoke edges
        for f in friends_to_show:
            fxuid = f['dst_xuid']
            fgt = f.get('gamertag') or fxuid[:10]
            G.add_node(fxuid, label=fgt, is_center=False, csr=f.get('csr') or 0)
            G.add_edge(center_xuid, fxuid)

        # Cross-edges
        for src, dst in cross_edge_set:
            if G.has_node(src) and G.has_node(dst) and not G.has_edge(src, dst):
                G.add_edge(src, dst)

        # Layout
        k = 2.5 / max(1, len(G.nodes) ** 0.5)
        pos = nx.spring_layout(G, seed=42, k=k, iterations=50)

        # Colour scale: friends coloured by CSR
        friend_csrs = [G.nodes[n]['csr'] for n in friend_xuids if G.has_node(n)]
        csr_min = min(friend_csrs) if friend_csrs else 0
        csr_max = max(friend_csrs) if friend_csrs else 1
        csr_range = max(csr_max - csr_min, 1)
        colormap = cm.plasma
        norm = mcolors.Normalize(vmin=csr_min, vmax=csr_max)

        node_colors = []
        node_sizes = []
        labels = {}
        for n in G.nodes:
            data = G.nodes[n]
            labels[n] = data['label']
            if data.get('is_center'):
                node_colors.append('#FFD700')  # gold for center
                node_sizes.append(700)
            else:
                node_colors.append(colormap(norm(data['csr'])))
                node_sizes.append(150 + G.degree(n) * 35)

        # Only label center + top-15 friends by CSR to avoid clutter
        top_xuids = {center_xuid} | {
            f['dst_xuid']
            for f in sorted(friends_to_show, key=lambda x: x.get('csr') or 0, reverse=True)[:15]
        }
        visible_labels = {n: labels[n] for n in G.nodes if n in top_xuids}

        # Plot
        bg = '#1a1a2e'
        fig, ax = plt.subplots(figsize=(13, 10), facecolor=bg)
        ax.set_facecolor(bg)

        # Spoke edges (center -> friends)
        spoke_edges = [(u, v) for u, v in G.edges() if center_xuid in (u, v)]
        cross_list = [(u, v) for u, v in G.edges() if center_xuid not in (u, v)]

        nx.draw_networkx_edges(G, pos, edgelist=spoke_edges,
                               edge_color='#4a90d9', width=1.0, alpha=0.5, ax=ax)
        if cross_list:
            nx.draw_networkx_edges(G, pos, edgelist=cross_list,
                                   edge_color='#aaaaaa', width=0.5, alpha=0.25, ax=ax)

        nx.draw_networkx_nodes(G, pos, node_color=node_colors,
                               node_size=node_sizes, linewidths=0.5,
                               edgecolors='white', ax=ax)
        nx.draw_networkx_labels(G, pos, labels=visible_labels,
                                font_size=7, font_color='white', ax=ax)

        # Colourbar (CSR scale)
        sm = cm.ScalarMappable(cmap=colormap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, ax=ax, fraction=0.025, pad=0.02)
        cbar.set_label('CSR', color='white', fontsize=9)
        cbar.ax.yaxis.set_tick_params(color='white')
        plt.setp(cbar.ax.yaxis.get_ticklabels(), color='white')

        shown = len(friends_to_show)
        total = len(halo_friends)
        title = f"Halo Network: {center_gamertag}"
        if total > shown:
            title += f"  (showing {shown} of {total} Halo friends)"
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
